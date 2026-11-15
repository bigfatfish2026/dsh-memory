import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { applySchema } from '../scripts/apply-pipeline-schema.mjs';
import { gate, findCandidates, hashText, judge, normalizeText } from '../lib/write-gate.js';

function tmpDb() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-gate-'));
  const dbPath = path.join(dir, 'memory.db');
  new DatabaseSync(dbPath).close();
  applySchema(dbPath, { logger: () => {} });
  return dbPath;
}

function seed(dbPath, rows) {
  const db = new DatabaseSync(dbPath);
  db.exec('CREATE TABLE IF NOT EXISTS memories (id TEXT PRIMARY KEY, text TEXT NOT NULL, entities TEXT, ts TEXT, status TEXT);');
  const stmt = db.prepare('INSERT INTO memories (id, text, entities, ts, status) VALUES (?, ?, ?, ?, ?)');
  for (const row of rows) stmt.run(row.id, row.text, JSON.stringify(row.entities || []), row.ts || '2026-11-01T00:00:00Z', row.status || 'active');
  db.close();
}

function countDiscards(dbPath) {
  const db = new DatabaseSync(dbPath, { readOnly: true });
  const n = Number(db.prepare('SELECT count(*) AS n FROM discards').get().n);
  db.close();
  return n;
}

test('hashText 规范化空白、全半角和标点', () => {
  assert.equal(hashText('Ｈｅｌｌｏ，世界！'), hashText('hello 世界'));
  assert.equal(hashText('树莓派 温度：45 度。'), hashText('树莓派温度45度'));
  assert.notEqual(hashText('树莓派温度45度'), hashText('树莓派温度46度'));
  assert.equal(normalizeText('  A-B_C  '), 'abc');
});

test('findCandidates 命中字面 hash 并过滤 excludeIds', async () => {
  const dbPath = tmpDb();
  seed(dbPath, [{ id: 'm_1', text: '树莓派温度 45 度', entities: ['树莓派'] }]);
  const db = new DatabaseSync(dbPath, { readOnly: true });
  const hits = await findCandidates(db, '树莓派温度45度。', { k: 10 });
  assert.equal(hits.hash.length, 1);
  assert.equal(hits.hash[0].id, 'm_1');
  assert.equal(hits.hashValue, hashText('树莓派温度45度。'));
  const excluded = await findCandidates(db, '树莓派温度45度。', { excludeIds: ['m_1'] });
  assert.equal(excluded.hash.length, 0);
  db.close();
});

test('完全重复判 DUPLICATE 并落 discards', async () => {
  const dbPath = tmpDb();
  seed(dbPath, [{ id: 'm_1', text: '树莓派温度 45 度' }]);
  const db = new DatabaseSync(dbPath);
  const result = await gate(db, { text: '树莓派温度45度。', sourceFile: '/tmp/session-a.jsonl' });
  db.close();
  assert.equal(result.verdict, 'DUPLICATE');
  assert.equal(result.matchedId, 'm_1');
  assert.equal(result.discarded, true);

  const check = new DatabaseSync(dbPath, { readOnly: true });
  const row = check.prepare('SELECT * FROM discards').get();
  check.close();
  assert.equal(row.candidate_hash, hashText('树莓派温度45度。'));
  assert.equal(row.matched_id, 'm_1');
  assert.equal(row.verdict, 'DUPLICATE');
  assert.equal(row.source_file, '/tmp/session-a.jsonl');
});

test('近似（余弦 ≥0.98）只标 needsLlm，不擅自入库', async () => {
  const dbPath = tmpDb();
  seed(dbPath, [{ id: 'm_2', text: '树莓派今天有点热' }]);
  const db = new DatabaseSync(dbPath);
  const knn = () => [{ id: 'm_2', similarity: 0.991 }];
  const knnOpts = { knn, vector: [0.1, 0.2] };
  const judged = await judge(
    { text: '树莓派今天挺热的' },
    { hash: [], knn: knn(), related: [] },
    {},
  );
  assert.equal(judged.verdict, 'NEEDS_LLM');
  assert.equal(judged.needsLlm, true);

  const result = await gate(db, { text: '树莓派今天挺热的' }, knnOpts);
  db.close();
  assert.equal(result.verdict, 'NEEDS_LLM');
  assert.equal(result.discarded, false);
  assert.equal(countDiscards(dbPath), 0);
});

test('注入假 llm 能判 DUPLICATE / SUPERSEDES / DISTINCT', async () => {
  const dbPath = tmpDb();
  seed(dbPath, [
    { id: 'm_2', text: '树莓派今天有点热' },
    { id: 'm_3', text: '树莓派温度 45 度' },
  ]);
  const db = new DatabaseSync(dbPath);
  const knn = () => [{ id: 'm_2', similarity: 0.99 }, { id: 'm_3', similarity: 0.9 }];
  const knnOpts = { knn, vector: [0.1, 0.2] };

  const dup = await gate(db, { text: '树莓派今天挺热的' }, {
    ...knnOpts,
    llm: () => ({ verdict: 'DUPLICATE', matchedId: 'm_2', reason: '同一件事换个说法' }),
  });
  assert.equal(dup.verdict, 'DUPLICATE');
  assert.equal(dup.discarded, true);

  const sup = await gate(db, { text: '树莓派今天更热了' }, {
    ...knnOpts,
    llm: () => ({ verdict: 'SUPERSEDES', matchedId: 'm_2', reason: '新结论' }),
  });
  assert.equal(sup.verdict, 'SUPERSEDES');
  assert.equal(sup.discarded, false);

  const dis = await gate(db, { text: '树莓派风扇坏了' }, {
    ...knnOpts,
    llm: () => ({ verdict: 'DISTINCT', matchedId: null, reason: '新事实' }),
  });
  assert.equal(dis.verdict, 'DISTINCT');
  assert.equal(dis.discarded, false);
  db.close();
  assert.equal(countDiscards(dbPath), 1);
});

test('低相似度直接 DISTINCT 放行', async () => {
  const dbPath = tmpDb();
  seed(dbPath, [{ id: 'm_9', text: '完全无关的旧事' }]);
  const db = new DatabaseSync(dbPath);
  const result = await gate(db, { text: '新的事实' }, { knn: () => [{ id: 'm_9', similarity: 0.42 }], vector: [0.1, 0.2] });
  db.close();
  assert.equal(result.verdict, 'DISTINCT');
  assert.equal(result.matchedId, null);
  assert.equal(result.discarded, false);
  assert.equal(countDiscards(dbPath), 0);
});

test('llm 返回非法 verdict 或召回外的 matchedId 时抛错', async () => {
  const hints = { hash: [], knn: [{ id: 'm_2', similarity: 0.99 }], related: [] };
  await assert.rejects(
    () => judge({ text: 'x' }, hints, { llm: () => ({ verdict: 'MAYBE' }) }),
    /verdict 非法/,
  );
  await assert.rejects(
    () => judge({ text: 'x' }, hints, { llm: () => ({ verdict: 'DUPLICATE', matchedId: 'm_404' }) }),
    /不在召回集合/,
  );
});
