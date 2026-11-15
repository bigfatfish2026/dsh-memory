#!/usr/bin/env node
/**
 * 应急：换嵌入模型 / 向量表坏了，从 memories 表重新算一遍向量。
 *
 * 2026-11-05 改：原来从 memories.jsonl 重算，而且是把 vec 表单独建到 .tmp 再整个盖回
 * memory.db——jsonl 退出运行时之后，这条路会把正文表一起盖掉、还会按冻结的旧 jsonl 重建。
 * 现在只认 memories 表：先把整个 memory.db（含正文）在线备份到 .tmp，再在 .tmp 里
 * 只重建 vec_items / vec_map，最后原子换回去。正文一条不动。
 *
 * 平时不用跑——新记忆是增量 insert 进 db 的。4672 条约 3～5 分钟（受嵌入服务速度限制）。
 *
 * 用法：node scripts/rebuild_vectors.mjs [--dir ...] [--api <嵌入服务地址>] [--batch 32] [--limit N]
 * 退出码：0 成功；1 失败（原库不动）。
 */
import fs from 'node:fs';
import path from 'node:path';
import { backup } from 'node:sqlite';
import { openDb, resolveMemoryRoot } from './lib/db.mjs';

const args = {};
for (let i = 2; i < process.argv.length; i += 2) {
  args[process.argv[i].replace(/^--/, '')] = process.argv[i + 1];
}

const DIR = args.dir || resolveMemoryRoot();
const API = (args.api || process.env.DSH_EMBED_API || 'http://127.0.0.1:8080').replace(/\/$/, '');
const BATCH = Math.max(1, Number(args.batch) || 32);
const LIMIT = Math.max(0, Number(args.limit) || 0);
const DB = path.join(DIR, 'memory.db');

function findExtension() {
  const here = import.meta.dirname;
  const candidates = [
    path.join(here, '../node_modules/sqlite-vec-linux-arm64/vec0.so'),
  ];
  for (const c of candidates) if (fs.existsSync(c)) return c;
  throw new Error('找不到 vec0 扩展');
}

async function embed(texts) {
  const resp = await fetch(`${API}/embed`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ inputs: texts }),
  });
  if (!resp.ok) throw new Error(`embed failed: HTTP ${resp.status}`);
  const data = await resp.json();
  return data.embeddings;
}

async function main() {
  if (!fs.existsSync(DB)) throw new Error(`记忆库不存在：${DB}（先跑 migrate-jsonl-to-sqlite.mjs）`);
  const src = openDb(DB, { readOnly: true });
  const hasMemories = Boolean(
    src.prepare("SELECT 1 AS ok FROM sqlite_master WHERE type='table' AND name='memories'").get(),
  );
  if (!hasMemories) {
    src.close();
    throw new Error(`${DB} 里没有 memories 表：先跑 scripts/migrate-jsonl-to-sqlite.mjs 迁移，再重建向量`);
  }
  let rows = src.prepare('SELECT id, text FROM memories ORDER BY rowSeq').all();
  if (LIMIT > 0) rows = rows.slice(0, LIMIT);
  src.close();
  console.log(`从 memories 表 ${rows.length} 条记忆重建向量，每批 ${BATCH} 条`);

  const tmp = `${DB}.tmp`;
  for (const f of [tmp, `${tmp}-wal`, `${tmp}-shm`]) if (fs.existsSync(f)) fs.rmSync(f);
  const srcForBackup = openDb(DB, { readOnly: true });
  await backup(srcForBackup, tmp);
  srcForBackup.close();

  let db;
  try {
    db = openDb(tmp, { allowExtension: true });
    db.loadExtension(findExtension());
    db.exec('PRAGMA journal_mode = WAL;');
    db.exec('PRAGMA synchronous = OFF;');
    db.exec('DROP TABLE IF EXISTS vec_items;');
    db.exec('DROP TABLE IF EXISTS vec_map;');
    db.exec('CREATE TABLE vec_map(rowid INTEGER PRIMARY KEY, id TEXT UNIQUE NOT NULL);');
    db.exec('CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT);');

    let dim = 0;
    let insV = null;
    const insVec = () => db.prepare('INSERT INTO vec_items(rowid, embedding) VALUES (?, ?)');
    const insMap = db.prepare('INSERT INTO vec_map(rowid, id) VALUES (?, ?)');
    const started = Date.now();
    for (let i = 0; i < rows.length; i += BATCH) {
      const slice = rows.slice(i, i + BATCH);
      const vecs = await embed(slice.map((r) => String(r.text || '').slice(0, 2000)));
      if (!dim) {
        dim = vecs[0].length;
        db.exec(`CREATE VIRTUAL TABLE vec_items USING vec0(embedding float[${dim}] distance_metric=cosine);`);
        insV = insVec();
      }
      db.exec('BEGIN');
      for (let j = 0; j < slice.length; j += 1) {
        const arr = Float32Array.from(vecs[j]);
        insV.run(BigInt(i + j + 1), new Uint8Array(arr.buffer, arr.byteOffset, arr.byteLength));
        insMap.run(i + j + 1, String(slice[j].id));
      }
      db.exec('COMMIT');
      process.stdout.write(`\r${i + slice.length}/${rows.length}`);
    }
    const meta = db.prepare('INSERT INTO meta(key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value');
    meta.run('dim', String(dim));
    meta.run('rows', String(rows.length));
    meta.run('builtAt', new Date().toISOString());
    db.exec('PRAGMA wal_checkpoint(TRUNCATE);');
    db.close();
    db = null;

    for (const f of [`${DB}-wal`, `${DB}-shm`]) {
      try { if (fs.existsSync(f)) fs.rmSync(f); } catch { /* 忽略 */ }
    }
    fs.renameSync(tmp, DB);
    for (const f of [`${tmp}-wal`, `${tmp}-shm`]) {
      try { if (fs.existsSync(f)) fs.rmSync(f); } catch { /* 忽略 */ }
    }
    console.log(`\n向量算完：${rows.length} 条 × ${dim} 维，用时 ${((Date.now() - started) / 1000).toFixed(0)} 秒`);
    console.log(`完成：${DB}｜${(fs.statSync(DB).size / 1048576).toFixed(1)} MB`);
  } catch (error) {
    try { db?.close(); } catch { /* 忽略 */ }
    try { fs.rmSync(tmp, { force: true }); } catch { /* 忽略 */ }
    throw error;
  }
}

main().catch((error) => {
  console.error(`重建失败，原库没动：${error.message}`);
  process.exitCode = 1;
});
