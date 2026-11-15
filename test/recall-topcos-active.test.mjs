/**
 * 语义入选线只按 active 的余弦取最大。
 *
 * 旧行为：topCos = 全候选最大余弦。一条 archived 0.95 会把 line 抬到 0.85，
 * active 最高只有 0.5 的条目全被挤出局——查不到活的，只捞得到影子。
 * 新行为：topCos 只看 active；一条 active 都没有时退回全候选最大值（不许 NaN）。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { applySchema } from '../scripts/apply-pipeline-schema.mjs';
import { createStore } from '../lib/store.js';

const VEC_SO = path.join(import.meta.dirname, '../node_modules/sqlite-vec-linux-arm64/vec0.so');

const MEMORY_DDL = `CREATE TABLE memories (
  id TEXT PRIMARY KEY, text TEXT NOT NULL, tags TEXT, source TEXT, ts TEXT, type TEXT,
  entities TEXT, validFrom TEXT, supersedes TEXT, updatedAt TEXT, status TEXT, reason TEXT,
  supersededBy TEXT, validTo TEXT, confidence REAL, extra TEXT, present TEXT, rowSeq INTEGER NOT NULL
);`;

/** 4 维单位向量：第一维就是余弦（[c, sqrt(1-c²), 0, 0] 与 [1,0,0,0] 的夹角余弦 = c）。 */
const vecOf = (cos) => [cos, Math.sqrt(Math.max(0, 1 - cos * cos)), 0, 0];

const QUERY = 'ZQ 阴天出门带伞吗';

function makeStore(seedRows) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-topcos-'));
  const dbPath = path.join(dir, 'memory.db');
  const db = new DatabaseSync(dbPath, { allowExtension: true });
  db.loadExtension(VEC_SO);
  db.exec(MEMORY_DDL);
  db.exec('CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);');
  db.exec("INSERT INTO meta (key, value) VALUES ('dim', '4');");
  db.exec('CREATE VIRTUAL TABLE vec_items USING vec0(embedding float[4] distance_metric=cosine);');
  db.exec('CREATE TABLE vec_map (rowid INTEGER PRIMARY KEY, id TEXT);');
  const insertMemory = db.prepare("INSERT INTO memories (id, text, tags, type, status, ts, rowSeq) VALUES (?, ?, '[]', 'world', ?, '2026-11-01T00:00:00Z', ?)");
  const insertVecMap = db.prepare('INSERT INTO vec_map (rowid, id) VALUES (?, ?)');
  const insertVec = db.prepare('INSERT INTO vec_items (rowid, embedding) VALUES (?, ?)');
  seedRows.forEach((row, idx) => {
    insertMemory.run(row.id, row.text, row.status || 'active', idx + 1);
    insertVecMap.run(idx + 1, row.id);
    insertVec.run(BigInt(idx + 1), new Uint8Array(Float32Array.from(vecOf(row.cos)).buffer));
  });
  db.close();
  applySchema(dbPath, { logger: () => {} });

  const cfg = {
    storePath: dir,
    storeLogFile: path.join(dir, 'store.log'),
    topK: 8,
    minScore: 0.45,
    relativeGap: 0.1,
    embed: async (texts) => texts.map(() => [1, 0, 0, 0]),
    logger: { info() {}, warn() {} },
  };
  return { store: createStore(cfg), dir, dbPath, cfg };
}

test('archived 0.95 + active 0.5：入选线按 active 算，archived 仍在但排后面', async () => {
  const { store } = makeStore([
    { id: 'm_arch', text: '风扇转速三千转，散热片是铜的。', cos: 0.95, status: 'archived' },
    { id: 'm_act_hi', text: '磁盘还剩百分之十五的空间。', cos: 0.5 },
    { id: 'm_act_lo', text: '内存还剩两吉字节可用。', cos: 0.47 },
  ]);

  const results = await store.recall(QUERY, 8);

  assert.equal(results.length, 3, 'archived 和两条 active 都该被捞到');
  assert.equal(results[0].status, 'active', '入选线按 active 的 0.5 算，active 排在前面');
  assert.equal(results[0].score, 0.5);
  const arch = results.find((r) => r.status === 'archived');
  assert.ok(arch, 'archived 仍要出现在结果里');
  assert.equal(arch.score, 0.95);
  assert.equal(results.at(-1).status, 'archived', 'archived 只当影子，排最后');
});

test('一条 active 都没有：topCos 退回全候选最大值，不死于 NaN', async () => {
  const { store } = makeStore([
    { id: 'm_a1', text: '风扇转速三千转，散热片是铜的。', cos: 0.95, status: 'archived' },
    { id: 'm_a2', text: '磁盘还剩百分之十五的空间。', cos: 0.9, status: 'merged' },
  ]);

  const results = await store.recall(QUERY, 8);

  assert.equal(results.length, 2, '兜底要能捞到影子，而不是被 NaN 全筛掉');
  for (const row of results) {
    assert.ok(Number.isFinite(row.score), `score 必须是有限数，实际 ${row.score}`);
    assert.equal(row.status === 'active', false);
  }
});
