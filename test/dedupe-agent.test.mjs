import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { applySchema } from '../scripts/apply-pipeline-schema.mjs';
import { createStore } from '../lib/store.js';
import { dedupe, SUBMIT_VERDICT_TOOL } from '../lib/agents/dedupe.js';

const VEC_SO = path.join(import.meta.dirname, '../node_modules/sqlite-vec-linux-arm64/vec0.so');

const MEMORY_DDL = `CREATE TABLE memories (
  id TEXT PRIMARY KEY, text TEXT NOT NULL, tags TEXT, source TEXT, ts TEXT, type TEXT,
  entities TEXT, validFrom TEXT, supersedes TEXT, updatedAt TEXT, status TEXT, reason TEXT,
  supersededBy TEXT, validTo TEXT, confidence REAL, extra TEXT, present TEXT, rowSeq INTEGER NOT NULL
);`;

function tmpDb() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-dedupe-'));
  const dbPath = path.join(dir, 'memory.db');
  new DatabaseSync(dbPath).close();
  applySchema(dbPath, { logger: () => {} });
  const db = new DatabaseSync(dbPath);
  db.exec('CREATE TABLE IF NOT EXISTS memories (id TEXT PRIMARY KEY, text TEXT NOT NULL, entities TEXT, ts TEXT, status TEXT);');
  return { dir, dbPath, db };
}

function seed(db, rows) {
  const stmt = db.prepare('INSERT INTO memories (id, text, entities, ts, status) VALUES (?, ?, ?, ?, ?)');
  for (const row of rows) stmt.run(row.id, row.text, JSON.stringify(row.entities || []), row.ts || '2026-11-01T00:00:00Z', row.status || 'active');
}

/** 假 llm：每个 handler 返回一组 chunk；调用次数按顺序取，超了重复最后一个。 */
function fakeLlm(handlers) {
  const calls = [];
  return {
    calls,
    stream(options) {
      calls.push(options);
      const handler = handlers[Math.min(calls.length - 1, handlers.length - 1)];
      return (async function* streamChunks() {
        for (const chunk of handler(options)) yield chunk;
      })();
    },
  };
}

const toolChunks = (args) => [
  { type: 'tool-call-delta', index: 0, id: 'call_1', name: 'submit_verdict', argumentsDelta: args.slice(0, 12) },
  { type: 'tool-call-delta', index: 0, argumentsDelta: args.slice(12) },
  { type: 'finish', reason: { kind: 'stop' } },
];

const textChunks = (text) => [
  { type: 'text-delta', text },
  { type: 'finish', reason: { kind: 'stop' } },
];

const brokenChunks = () => [
  { type: 'text-delta', text: '这边不是 JSON' },
  { type: 'tool-call-delta', index: 0, name: 'submit_verdict', argumentsDelta: '{oops' },
  { type: 'finish', reason: { kind: 'stop' } },
];

/** store 夹具：真 createStore + 假 embed，4 维向量，避开远端 BGE。 */
function makeStore({ writeGate = true, llm = null, threshold = 0.9, seedRows = [] } = {}) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-gate-store-'));
  const dbPath = path.join(dir, 'memory.db');
  const db = new DatabaseSync(dbPath, { allowExtension: true });
  db.loadExtension(VEC_SO);
  db.exec(MEMORY_DDL);
  db.exec('CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);');
  db.exec("INSERT INTO meta (key, value) VALUES ('dim', '4');");
  db.exec('CREATE VIRTUAL TABLE vec_items USING vec0(embedding float[4] distance_metric=cosine);');
  db.exec('CREATE TABLE vec_map (rowid INTEGER PRIMARY KEY, id TEXT);');
  let seq = 1;
  const insertMemory = db.prepare('INSERT INTO memories (id, text, tags, type, status, ts, rowSeq) VALUES (?, ?, ?, ?, ?, ?, ?)');
  const insertVecMap = db.prepare('INSERT INTO vec_map (rowid, id) VALUES (?, ?)');
  const insertVec = db.prepare('INSERT INTO vec_items (rowid, embedding) VALUES (?, ?)');
  for (const row of seedRows) {
    const id = row.id;
    insertMemory.run(id, row.text, '[]', 'world', 'active', '2026-11-01T00:00:00Z', seq);
    insertVecMap.run(seq, id);
    insertVec.run(BigInt(seq), new Uint8Array(Float32Array.from([1, 0, 0, 0]).buffer));
    seq += 1;
  }
  db.close();
  applySchema(dbPath, { logger: () => {} });

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
    // 所有正文同一个向量：近似判定走 judged，不靠真实嵌入
    embed: async (texts) => texts.map(() => [1, 0, 0, 0]),
    pipeline: { writeGate, writeGateThreshold: threshold, textLint: false },
    logger: { info() {}, warn() {} },
  };
  const store = createStore(cfg);
  if (llm) store.attachLlm(llm);
  return { store, dir, dbPath };
}

function count(dbPath, sql, ...params) {
  const db = new DatabaseSync(dbPath, { readOnly: true });
  try {
    return Number(db.prepare(sql).get(...params)?.n ?? 0);
  } finally {
    db.close();
  }
}

test('工具通道：submit_verdict 的 argumentsDelta 拼成 JSON，source=tool', async () => {
  const { db } = tmpDb();
  seed(db, [{ id: 'm_1', text: '树莓派温度 45 度' }]);
  const llm = fakeLlm([
    () => toolChunks(JSON.stringify({ verdict: 'DUPLICATE', matchedId: 'm_1', reason: '同一件事换个说法' })),
  ]);
  const result = await dedupe({
    target: db,
    candidate: { text: '树莓派今天挺热的', type: 'world', entities: ['树莓派'] },
    llm,
    provider: 'fake',
    model: 'fake',
    maxTokens: 0,
    threshold: 0.9,
    knn: () => [{ id: 'm_1', similarity: 0.95 }],
    vector: [1, 0, 0, 0],
  });
  db.close();
  assert.equal(result.verdict, 'DUPLICATE');
  assert.equal(result.matchedId, 'm_1');
  assert.equal(result.source, 'tool');
  assert.equal(llm.calls.length, 1);
  const req = llm.calls[0];
  assert.equal(req.reasoningEffort, 'low');
  // 2026-11-06 收口：cfg.dedupeMaxTokens 默认 0 = 不传，请求里不许出现 maxTokens 字段
  assert.equal('maxTokens' in req, false, 'maxTokens=0 时请求里不许有 maxTokens');
  assert.equal(req.temperature, 0);
  assert.equal(req.stop, undefined);
  assert.equal(req.signal, undefined);
  assert.equal(req.tools[0].name, SUBMIT_VERDICT_TOOL.name);
  assert.deepEqual(req.tools[0].parameters.properties.verdict.enum, ['DUPLICATE', 'SUPERSEDES', 'DISTINCT']);
  assert.equal(req.tools[0].parameters.properties.reason.maxLength, 40);
});

test('文本回退：没有工具流时解析正文 JSON，source=text', async () => {
  const { db } = tmpDb();
  seed(db, [{ id: 'm_2', text: '树莓派今天有点热' }]);
  const llm = fakeLlm([
    () => textChunks('```json\n{"verdict":"DISTINCT","matchedId":"","reason":"主题相关但事实不同"}\n```'),
  ]);
  const result = await dedupe({
    target: db,
    candidate: { text: '树莓派风扇坏了', type: 'world', entities: ['树莓派'] },
    llm,
    threshold: 0.9,
    knn: () => [{ id: 'm_2', similarity: 0.93 }],
    vector: [1, 0, 0, 0],
  });
  db.close();
  assert.equal(result.verdict, 'DISTINCT');
  assert.equal(result.source, 'text');
});

test('两条通道都坏：重试一次，仍坏返回 undefined', async () => {
  const { db } = tmpDb();
  seed(db, [{ id: 'm_3', text: '树莓派温度 45 度' }]);
  const llm = fakeLlm([brokenChunks]);
  const result = await dedupe({
    target: db,
    candidate: { text: '树莓派今天挺热的', type: 'world' },
    llm,
    threshold: 0.9,
    knn: () => [{ id: 'm_3', similarity: 0.95 }],
    vector: [1, 0, 0, 0],
  });
  db.close();
  assert.equal(result, undefined);
  assert.equal(llm.calls.length, 2);
});

test('第一次坏、第二次好：重试后拿到工具通道裁决', async () => {
  const { db } = tmpDb();
  seed(db, [{ id: 'm_4', text: '树莓派温度 45 度' }]);
  const llm = fakeLlm([
    brokenChunks,
    () => toolChunks(JSON.stringify({ verdict: 'DISTINCT', matchedId: '', reason: '核实后不是同一件事' })),
  ]);
  const result = await dedupe({
    target: db,
    candidate: { text: '树莓派风扇 3 号坏了', type: 'world' },
    llm,
    threshold: 0.9,
    knn: () => [{ id: 'm_4', similarity: 0.95 }],
    vector: [1, 0, 0, 0],
  });
  db.close();
  assert.equal(result.verdict, 'DISTINCT');
  assert.equal(result.source, 'tool');
  assert.equal(llm.calls.length, 2);
});

test('store 写入闸：DUPLICATE 落 discards，不落 memories', async () => {
  const llm = fakeLlm([() => brokenChunks()]);
  const { store, dbPath } = makeStore({
    seedRows: [{ id: 'm_dup', text: '树莓派温度 45 度' }],
    llm,
  });
  const rec = await store.save('树莓派温度45度。', [], 'test', { type: 'world' });
  assert.equal(rec.discarded, true);
  assert.equal(rec.matchedId, 'm_dup');
  assert.equal(llm.calls.length, 0, 'hash 完全相同走规则层，不该花模型调用');
  assert.equal(count(dbPath, 'SELECT count(*) AS n FROM memories'), 1);
  assert.equal(count(dbPath, "SELECT count(*) AS n FROM discards WHERE verdict='DUPLICATE'"), 1);
  assert.equal(store.metas().length, 1);
});

test('store 写入闸：SUPERSEDES 旧条 archived、新条带 supersedes', async () => {
  const llm = fakeLlm([
    () => toolChunks(JSON.stringify({ verdict: 'SUPERSEDES', matchedId: 'm_old', reason: '温度改成 50 度' })),
  ]);
  const { store, dbPath } = makeStore({
    seedRows: [{ id: 'm_old', text: '树莓派温度 45 度' }],
    llm,
  });
  const rec = await store.save('树莓派温度改成 50 度', [], 'test', { type: 'world' });
  assert.equal(rec.discarded, undefined);
  assert.deepEqual(rec.supersedes, ['m_old']);
  assert.equal(count(dbPath, "SELECT count(*) AS n FROM memories WHERE id='m_old' AND status='archived'"), 1);
  const db = new DatabaseSync(dbPath, { readOnly: true });
  const fresh = db.prepare('SELECT supersedes, status FROM memories WHERE id=?').get(rec.id);
  db.close();
  assert.deepEqual(JSON.parse(fresh.supersedes), ['m_old']);
  assert.equal(fresh.status || 'active', 'active');
  assert.equal(count(dbPath, 'SELECT count(*) AS n FROM discards'), 0, 'SUPERSEDES 是替换不是丢弃，不记 discards');
});
