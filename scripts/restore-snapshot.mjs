#!/usr/bin/env node
/**
 * 在活库里用 SQL 事务回滚到某份快照（2026-11-06 t-restore-05 rev2）。
 *
 * 为什么不停服务：真库被 qqbot 常驻读写，换文件会让它继续往旧 inode 写。
 * 这里反过来——照常打开活库，ATTACH 快照（URI `mode=ro`，只读），
 * 在 BEGIN IMMEDIATE 里逐表 DELETE + INSERT，一次提交；失败整体回滚。
 *
 * 只读快照、只写活库；不碰 vec0 的 shadow 表（vec_items_* 由 vec0 自己维护），
 * vec_items 以 (rowid, embedding) 整表重灌。sqlite_sequence 只回写非 shadow 的
 * 自增表（discards / pending_ops），shadow 的由 vec0 自己管。
 *
 *   node scripts/restore-snapshot.mjs --dry-run     # 只读：列计划、核对列与行数
 *   node scripts/restore-snapshot.mjs               # 真做
 *   node scripts/restore-snapshot.mjs --db x.db --snap y.db
 *
 * 事务里每张表 main 行数必须等于 snap 行数，否则 ROLLBACK；
 * BEGIN IMMEDIATE 拿不到写锁就退避重试 5 次（200ms），不静默跳过。
 * 提交后做 integrity_check / quick_check / wal_checkpoint(TRUNCATE)，
 * 再用 lib/db-fingerprint.js 比「活库 vs 快照」的逻辑指纹，逐字段一致才算成功。
 */
import fs from 'node:fs';
import path from 'node:path';
import { openDb, resolveDbPath, resolveMemoryFile } from './lib/db.mjs';
import { logicalFingerprint, diffFingerprint } from '../lib/db-fingerprint.js';

const DEFAULT_DB = resolveDbPath();
const DEFAULT_SNAP = resolveMemoryFile('backup/2026-11-05T21-00-26-memory.db');
const VEC_EXT = path.join(import.meta.dirname, '../node_modules/sqlite-vec-linux-arm64/vec0.so');
const SHADOW_RE = /^vec_items_/; // vec0 的 shadow 表，交给虚拟表自己维护
const BUSY_RETRIES = 5;
const BUSY_DELAY_MS = 200;

export function parseArgs(argv = process.argv.slice(2)) {
  const out = { db: DEFAULT_DB, snap: DEFAULT_SNAP, dryRun: false };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === '--db') out.db = argv[++i];
    else if (arg.startsWith('--db=')) out.db = arg.slice(5);
    else if (arg === '--snap') out.snap = argv[++i];
    else if (arg.startsWith('--snap=')) out.snap = arg.slice(7);
    else if (arg === '--dry-run') out.dryRun = true;
    else if (arg === '-h' || arg === '--help') out.help = true;
    else throw new Error(`未知参数：${arg}`);
  }
  return out;
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function isBusy(error) {
  const msg = String(error?.message || error);
  return /SQLITE_BUSY|database is locked|database table is locked/i.test(msg) || error?.errcode === 5 || error?.errcode === 6;
}

/** 某 schema 下的实体表名（排除 vec0 shadow）；虚拟表 vec_items 保留。 */
function listTables(db, schema) {
  return db
    .prepare(`SELECT name FROM ${schema}.sqlite_master WHERE type='table' ORDER BY name`)
    .all()
    .map((r) => r.name)
    .filter((name) => !SHADOW_RE.test(name));
}

function columnsOf(db, schema, table) {
  return db.prepare(`PRAGMA ${schema}.table_info("${table}")`).all().map((c) => c.name);
}

function countOf(db, schema, table) {
  return Number(db.prepare(`SELECT count(*) AS c FROM ${schema}."${table}"`).get().c);
}

/** 逐表核对列清单：同名同序同数量，否则直接停。 */
function checkColumns(db, tables) {
  const problems = [];
  for (const t of tables) {
    const m = columnsOf(db, 'main', t);
    const s = columnsOf(db, 'snap', t);
    if (m.length !== s.length || m.join(',') !== s.join(',')) {
      problems.push(`${t}：main(${m.join(',')}) ≠ snap(${s.join(',')})`);
    }
  }
  return problems;
}

/** 事务内 DELETE + INSERT SELECT，返回逐表行数。 */
function copyTable(db, table) {
  const cols = columnsOf(db, 'main', table);
  const colList = cols.map((c) => `"${c}"`).join(', ');
  db.exec(`DELETE FROM main."${table}"`);
  db.prepare(`INSERT INTO main."${table}" (${colList}) SELECT ${colList} FROM snap."${table}"`).run();
  return countOf(db, 'main', table);
}

function vecTableInfo(db) {
  // vec0 虚拟表的 table_info 只报 embedding，隐式 rowid 单独带上
  const cols = columnsOf(db, 'main', 'vec_items');
  return { hasEmbedding: cols.includes('embedding') };
}

async function attachSnapshot(db, snap) {
  // URI 只读打开；不认 URI 的实现会报错或落到别的文件，两种都当失败
  db.exec(`ATTACH DATABASE 'file:${snap}?mode=ro' AS snap`);
  const row = db.prepare("SELECT file FROM pragma_database_list WHERE name = 'snap'").get();
  if (!row || row.file !== snap) {
    db.exec('DETACH DATABASE snap');
    throw new Error(`快照没按只读 URI 打开（file=${row?.file ?? '无'}）`);
  }
  // 实证只读：建表必须被拒
  try {
    db.exec('CREATE TABLE snap.__ro_probe(x INTEGER)');
    db.exec('DROP TABLE snap.__ro_probe');
    db.exec('DETACH DATABASE snap');
    throw new Error('快照是可写的，拒绝继续');
  } catch (error) {
    if (/可写的/.test(String(error?.message))) throw error;
    // attempt to write a readonly database → 正确
  }
}

async function restore(args) {
  if (!fs.existsSync(args.db)) throw new Error(`活库不存在：${args.db}`);
  if (!fs.existsSync(args.snap)) throw new Error(`快照不存在：${args.snap}`);

  const db = openDb(args.db, { allowExtension: true });
  db.exec('PRAGMA busy_timeout = 5000;');
  db.exec('PRAGMA foreign_keys = OFF;');
  db.loadExtension(VEC_EXT);
  console.log(`[restore] 活库 ${args.db}（journal_mode=${db.prepare('PRAGMA journal_mode').get().journal_mode}）`);
  console.log(`[restore] vec0 扩展 ${VEC_EXT}：已加载（db.loadExtension，与 lib/memvec.js 同一份 .so）`);

  try {
    await attachSnapshot(db, args.snap);
    console.log(`[restore] ATTACH 'file:${args.snap}?mode=ro' AS snap（只读，写探针被拒）`);

    const mainTables = listTables(db, 'main');
    const snapTables = listTables(db, 'snap');
    console.log(`[restore] 活库表：${mainTables.join(', ')}`);
    console.log(`[restore] 快照表：${snapTables.join(', ')}`);
    const onlyMain = mainTables.filter((t) => !snapTables.includes(t));
    const onlySnap = snapTables.filter((t) => !mainTables.includes(t));
    if (onlyMain.length || onlySnap.length) {
      throw new Error(`表集合不一致：仅活库 [${onlyMain}]，仅快照 [${onlySnap}]`);
    }
    const colProblems = checkColumns(db, mainTables);
    if (colProblems.length) throw new Error(`列清单不一致：\n${colProblems.join('\n')}`);
    console.log(`[restore] 列清单逐表一致（${mainTables.length} 张，含 vec_items 虚拟表；vec_items_* shadow 不直写）`);
    if (!vecTableInfo(db).hasEmbedding) throw new Error('vec_items 没有 embedding 列');

    const before = {};
    for (const t of mainTables) before[t] = { main: countOf(db, 'main', t), snap: countOf(db, 'snap', t) };
    console.log('[restore] 行数对照 ' + mainTables.map((t) => `${t}: ${before[t].main} → ${before[t].snap}`).join(' | '));

    if (args.dryRun) {
      console.log('[restore] --dry-run：只读预演完成，一条没写');
      return { ok: true, dryRun: true };
    }

    const plan = mainTables.filter((t) => t !== 'sqlite_sequence');
    let lastBusy = null;
    let counts = null;
    for (let attempt = 1; attempt <= BUSY_RETRIES; attempt += 1) {
      try {
        db.exec('BEGIN IMMEDIATE');
        try {
          counts = {};
          for (const t of plan) {
            if (t === 'vec_items') {
              db.exec('DELETE FROM main.vec_items');
              db.prepare('INSERT INTO main.vec_items (rowid, embedding) SELECT rowid, embedding FROM snap.vec_items').run();
              counts[t] = countOf(db, 'main', t);
            } else {
              counts[t] = copyTable(db, t);
            }
          }
          // 事务内核对：每张表行数必须等于快照
          const mismatch = plan.filter((t) => counts[t] !== countOf(db, 'snap', t));
          if (mismatch.length) {
            throw new Error(`事务内行数对不上：${mismatch.map((t) => `${t} ${counts[t]}/${countOf(db, 'snap', t)}`).join(', ')}`);
          }
          // 自增序号回写到快照值（只回非 shadow 表；shadow 归 vec0）
          const seqFix = [];
          for (const row of db.prepare('SELECT name, seq FROM snap.sqlite_sequence').all()) {
            if (SHADOW_RE.test(row.name)) continue;
            const upd = db.prepare('UPDATE main.sqlite_sequence SET seq = ? WHERE name = ?').run(row.seq, row.name);
            if (Number(upd.changes) === 0) {
              db.prepare('INSERT INTO main.sqlite_sequence (name, seq) VALUES (?, ?)').run(row.name, row.seq);
            }
            seqFix.push(`${row.name}=${row.seq}`);
          }
          db.exec('COMMIT');
          console.log(`[restore] 提交完成（第 ${attempt} 次尝试）：${plan.map((t) => `${t}=${counts[t]}`).join(' ')}`);
          console.log(`[restore] sqlite_sequence 回写：${seqFix.join(', ')}`);
          lastBusy = null;
          break;
        } catch (error) {
          try { db.exec('ROLLBACK'); } catch { /* 连接坏了由 sqlite 自己回滚 */ }
          throw error;
        }
      } catch (error) {
        if (!isBusy(error) || attempt === BUSY_RETRIES) throw error;
        lastBusy = error;
        console.log(`[restore] 第 ${attempt} 次 BEGIN IMMEDIATE 撞锁（${error.message}），${BUSY_DELAY_MS}ms 后重试`);
        await sleep(BUSY_DELAY_MS);
      }
    }
    if (lastBusy) throw lastBusy;

    // 提交后体检
    console.log(`[restore] integrity_check=${JSON.stringify(db.prepare('PRAGMA integrity_check').all())}`);
    console.log(`[restore] quick_check=${JSON.stringify(db.prepare('PRAGMA quick_check').all())}`);
    try {
      const ck = db.prepare('PRAGMA wal_checkpoint(TRUNCATE)').all();
      console.log(`[restore] wal_checkpoint(TRUNCATE)=${JSON.stringify(ck)}`);
    } catch (error) {
      console.log(`[restore] wal_checkpoint 失败：${error.message}`);
    }
    const live = logicalFingerprint(args.db);
    const snap = logicalFingerprint(args.snap);
    console.log(`[restore] 活库指纹 ${live.canonical} (#${live.digest})`);
    console.log(`[restore] 快照指纹 ${snap.canonical} (#${snap.digest})`);
    const diff = diffFingerprint(snap, live);
    console.log(`[restore] 指纹差异 ${diff.length ? diff.join('; ') : '（无，逐字段一致）'}`);

    const dist = {
      memories: db.prepare('SELECT status, count(*) AS c FROM main.memories GROUP BY status ORDER BY c DESC').all(),
      ops: db.prepare('SELECT status, count(*) AS c FROM main.pending_ops GROUP BY status ORDER BY c DESC').all(),
      vec_items: countOf(db, 'main', 'vec_items'),
      vec_map: countOf(db, 'main', 'vec_map'),
      maxRowSeq: db.prepare('SELECT max(rowSeq) AS m FROM main.memories').get().m,
    };
    console.log(`[restore] 分布 memories=${JSON.stringify(dist.memories)} pending_ops=${JSON.stringify(dist.ops)}`);
    console.log(`[restore] vec_items=${dist.vec_items} vec_map=${dist.vec_map} max(rowSeq)=${dist.maxRowSeq}`);
    if (diff.length) return { ok: false, diff, live, snap };
    return { ok: true, live, snap };
  } finally {
    try { db.close(); } catch { /* 关不掉不影响结果 */ }
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const args = parseArgs();
  if (args.help) {
    console.log('node scripts/restore-snapshot.mjs [--db 活库] [--snap 快照] [--dry-run]');
    process.exit(0);
  }
  restore(args).then((out) => process.exit(out.ok ? 0 : 1)).catch((error) => {
    console.error(`[restore] 失败：${error?.message || error}`);
    process.exit(2);
  });
}
