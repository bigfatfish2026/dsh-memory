/**
 * recall 缓存 + KNN 白名单（2026-11-15，审计存储中-4）。
 *
 * 钉三件事：
 *   1. 分词按记录对象缓存：第二次 recall 不再全表分词；改一条只重分那一条，df 随写事务重建；
 *   2. df 缓存跟着快照版本失效：archive 之后 totalActive 变了，下一次 recall 必须重算；
 *   3. memvec.knn 的 allow 白名单：只把允许的 id 放进结果，翻页/凑数按白名单内条数算。
 *
 * 全程 tmpdir 假库，不碰真库、不联网。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { createStore } from '../lib/store.js';
import { openMemVectors } from '../lib/memvec.js';

const VEC_SO = path.join(import.meta.dirname, '../node_modules/sqlite-vec-linux-arm64/vec0.so');

const MEMORY_DDL = `CREATE TABLE memories (
  id TEXT PRIMARY KEY, text TEXT NOT NULL, tags TEXT, source TEXT, ts TEXT, type TEXT,
  entities TEXT, validFrom TEXT, supersedes TEXT, updatedAt TEXT, status TEXT, reason TEXT,
  supersededBy TEXT, validTo TEXT, confidence REAL, extra TEXT, present TEXT, rowSeq INTEGER NOT NULL
);`;

const QUERY = 'ZQ 阴天出门带伞吗';

/** 4 维单位向量：第一维就是余弦（[c, sqrt(1-c²), 0, 0] 与 [1,0,0,0] 的夹角余弦 = c）。 */
const vecOf = (cos) => [cos, Math.sqrt(Math.max(0, 1 - cos * cos)), 0, 0];

function makeStore(seedRows) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-recall-cache-'));
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

  const cfg = {
    storePath: dir,
    storeLogFile: path.join(dir, 'store.log'),
    topK: 8,
    minScore: 0.45,
    relativeGap: 0.1,
    rrfK: 60,
    embed: async (texts) => texts.map(() => [1, 0, 0, 0]),
    logger: { info() {}, warn() {} },
    pipeline: { writeGate: false, textLint: false },
  };
  const store = createStore(cfg);
  return { store, db, dbPath, dir, cfg };
}

const SEEDS = [
  { id: 'm_act1', text: '磁盘还剩百分之十五的空间。', cos: 0.7 },
  { id: 'm_act2', text: '内存还剩两吉字节可用。', cos: 0.65 },
  { id: 'm_arch', text: '风扇转速三千转，散热片是铜的。', cos: 0.95, status: 'archived' },
];

test('分词按记录对象缓存；update 只重分那一条，df 缓存随写事务失效', async () => {
  const { store } = makeStore(SEEDS);

  await store.recall(QUERY, 8);
  const first = store.recallCacheStats();
  assert.equal(first.dfBuilds, 1, '第一次 recall 建一次 df');
  assert.equal(first.tokenBuilds, 3, '第一次把 3 条都分词');

  await store.recall(QUERY, 8);
  const second = store.recallCacheStats();
  assert.equal(second.dfBuilds, 1, '第二次 recall 命中 df 缓存');
  assert.equal(second.tokenBuilds, 3, '第二次 recall 不再重新分词');

  await store.update('m_act1', { text: '磁盘还剩百分之十七的空间。' });
  await store.recall(QUERY, 8);
  const third = store.recallCacheStats();
  assert.equal(third.dfBuilds, 2, '写事务后 df 重建一次');
  assert.equal(third.tokenBuilds, 4, '只有被改的那条换了新对象，只重分一条');

  // 状态变化（archived→active）也走写事务：totalActive 变了，df 必须重算
  await store.archive('m_act2', 'test');
  await store.recall(QUERY, 8);
  const fourth = store.recallCacheStats();
  assert.equal(fourth.dfBuilds, 3, 'archive 之后 df 再重建');
  assert.equal(fourth.tokenBuilds, 5, '被 archive 的那条换了新对象，只多分一条');
});

test('recall 仍然按老口径召回：active 在前、archived 当影子留最后', async () => {
  const { store } = makeStore(SEEDS);
  const results = await store.recall(QUERY, 8);
  assert.equal(results.length, 3);
  assert.equal(results[0].status, 'active');
  assert.equal(results.at(-1).status, 'archived');
  assert.equal(results.at(-1).score, 0.95, '影子的余弦还在（没被 active 过滤掉）');
});

test('memvec.knn allow 白名单：只回允许的 id，按白名单内条数翻页凑数', () => {
  const { db, dbPath } = makeStore(SEEDS);
  db.close();
  const vec = openMemVectors({ dbPath });
  const q = new Float32Array([1, 0, 0, 0]);

  const all = vec.knn(q, 10);
  assert.equal(all.length, 3, '不带白名单还是全量');
  const onlyActive = vec.knn(q, 10, { allow: ['m_act1', 'm_act2'] });
  assert.deepEqual(onlyActive.map((h) => h.id).sort(), ['m_act1', 'm_act2'], '只回白名单里的');
  const limited = vec.knn(q, 1, { allow: ['m_act1', 'm_act2'] });
  assert.equal(limited.length, 1, 'k 小于白名单条数时按 k 截断');
  assert.equal(limited[0].id, 'm_act1', '按相似度取最靠前的那条');
  assert.deepEqual(vec.knn(q, 5, { allow: [] }), [], '空白名单直接空结果，不查库');
  vec.close();
});
