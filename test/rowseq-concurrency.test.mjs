/**
 * rowSeq 取号并发回归（2026-11-06 t-fix-quality）。
 *
 * 以前 saveMemory 用进程内存里的 `maxRowSeq + 1` 取号，两个连接各自加载过一次 metas
 * 之后都在算同一个号；真库因此攒下 82 对重复 rowSeq（164 行）。
 * 现在取号挪进 BEGIN IMMEDIATE 事务里做 SELECT MAX+1，两个连接并发写 200 行也撞不上。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { applySchema } from '../scripts/apply-pipeline-schema.mjs';
import { createStore } from '../lib/store.js';
import { checkRowSeq } from '../scripts/check-rowseq.mjs';

const VEC_SO = path.join(import.meta.dirname, '../node_modules/sqlite-vec-linux-arm64/vec0.so');

const MEMORY_DDL = `CREATE TABLE memories (
  id TEXT PRIMARY KEY, text TEXT NOT NULL, tags TEXT, source TEXT, ts TEXT, type TEXT,
  entities TEXT, validFrom TEXT, supersedes TEXT, updatedAt TEXT, status TEXT, reason TEXT,
  supersededBy TEXT, validTo TEXT, confidence REAL, extra TEXT, present TEXT, rowSeq INTEGER NOT NULL
);`;

function makeStore(dir) {
  const cfg = {
    storePath: dir,
    storeLogFile: path.join(dir, 'store.log'),
    apiUrl: 'http://127.0.0.1:1',
    timeoutMs: 50,
    provider: 'fake',
    model: 'fake',
    rrfK: 60,
    opinionAlpha: 0.1,
    splitMaxChars: 200,
    embed: async (texts) => texts.map(() => [1, 0, 0, 0]),
    pipeline: { writeGate: false, textLint: false },
    logger: { info() {}, warn() {} },
  };
  return createStore(cfg);
}

test('两个连接并发写 200 行，rowSeq 无重复且连续', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-rowseq-'));
  const dbPath = path.join(dir, 'memory.db');
  const db = new DatabaseSync(dbPath, { allowExtension: true });
  db.loadExtension(VEC_SO);
  db.exec(MEMORY_DDL);
  db.exec('CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);');
  db.exec("INSERT INTO meta (key, value) VALUES ('dim', '4');");
  db.exec('CREATE VIRTUAL TABLE vec_items USING vec0(embedding float[4] distance_metric=cosine);');
  db.exec('CREATE TABLE vec_map (rowid INTEGER PRIMARY KEY, id TEXT);');
  db.close();
  applySchema(dbPath, { logger: () => {} });

  // 两条独立连接，各自在启动时加载一次 metas（maxRowSeq 都是 0）
  const a = makeStore(dir);
  const b = makeStore(dir);

  const total = 200;
  const jobs = [];
  for (let i = 0; i < total; i += 1) {
    const store = i % 2 === 0 ? a : b;
    jobs.push(store.save(`并发取号第 ${i} 条：树莓派温度 45 度`, ['concurrency'], 'test', { type: 'world' }));
  }
  const saved = await Promise.all(jobs);
  assert.equal(saved.length, total);
  assert.equal(new Set(saved.map((r) => r.id)).size, total, 'id 不重复');

  const read = new DatabaseSync(dbPath, { readOnly: true });
  const rows = read.prepare('SELECT id, rowSeq FROM memories ORDER BY rowSeq').all();
  read.close();
  const seqs = rows.map((r) => Number(r.rowSeq));
  assert.equal(seqs.length, total, '200 行都落库');
  assert.equal(new Set(seqs).size, total, 'rowSeq 一个都不许重');
  assert.deepEqual(seqs, Array.from({ length: total }, (_, i) => i + 1), 'rowSeq 从 1 连续到 200');

  const report = checkRowSeq(dbPath);
  assert.equal(report.ok, true);
  assert.equal(report.dupPairs.length, 0);
  assert.equal(report.dupRows, 0);
  assert.equal(report.total, total);
});

test('check-rowseq：重复 rowSeq 全报出来（含内容摘要），只读不改库', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-rowseq-dup-'));
  const dbPath = path.join(dir, 'memory.db');
  const db = new DatabaseSync(dbPath);
  db.exec(MEMORY_DDL);
  const ins = db.prepare('INSERT INTO memories (id, text, tags, type, status, ts, rowSeq) VALUES (?, ?, ?, ?, ?, ?, ?)');
  ins.run('m_a', '同一段正文。', '[]', 'world', 'active', '2026-11-01T00:00:00Z', 1);
  ins.run('m_b', '同一段正文。', '[]', 'world', 'active', '2026-11-01T00:00:01Z', 1);
  ins.run('m_c', '另一段正文。', '[]', 'world', 'active', '2026-11-01T00:00:02Z', 2);
  db.close();

  const report = checkRowSeq(dbPath);
  assert.equal(report.ok, false);
  assert.equal(report.total, 3);
  assert.equal(report.distinct, 2);
  assert.equal(report.dupPairs.length, 1);
  assert.equal(report.dupRows, 2);
  assert.deepEqual(report.dupPairs[0].ids, ['m_a', 'm_b']);
  assert.equal(report.dupPairs[0].sameContent, true, '正文一样也应报同内容');

  const after = new DatabaseSync(dbPath, { readOnly: true });
  assert.equal(Number(after.prepare('SELECT count(*) AS c FROM memories').get().c), 3, '检查器不许删行');
  after.close();
});
