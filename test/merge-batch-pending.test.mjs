import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { applySchema } from '../scripts/apply-pipeline-schema.mjs';
import { createStore } from '../lib/store.js';
import { setupSleep } from '../lib/sleep.js';

const VEC_SO = path.join(import.meta.dirname, '../node_modules/sqlite-vec-linux-arm64/vec0.so');

const MEMORY_DDL = `CREATE TABLE memories (
  id TEXT PRIMARY KEY, text TEXT NOT NULL, tags TEXT, source TEXT, ts TEXT, type TEXT,
  entities TEXT, validFrom TEXT, supersedes TEXT, updatedAt TEXT, status TEXT, reason TEXT,
  supersededBy TEXT, validTo TEXT, confidence REAL, extra TEXT, present TEXT, rowSeq INTEGER NOT NULL
);`;

// 向量两两余弦 <0.9（不触发近重复连边）、最高 0.8（过 clusterQuestionMin），
// 三条同实体落进同一个桶，正好凑一批交给合并 agent。
const SEEDS = [
  { id: 'm_1', text: '树莓派温度 45 度。', vec: [1, 0, 0, 0] },
  { id: 'm_2', text: '树莓派风扇转速 3000 转。', vec: [0.8, 0.6, 0, 0] },
  { id: 'm_3', text: '树莓派温度 45 度，风扇 3000 转，今晚重启过。', vec: [0.8, 0, 0.6, 0] },
];

const MERGE_OP = {
  op: 'merge',
  ids: ['m_1', 'm_2'],
  text: '树莓派温度 45 度，风扇转速 3000 转。',
  evidence: [
    { id: 'm_1', quote: '树莓派温度 45 度。' },
    { id: 'm_2', quote: '风扇转速 3000 转。' },
  ],
  needsSplit: false,
  reason: '同一台机器的两条状态',
};

function makeStore({ agentPipeline = true } = {}) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-merge-pending-'));
  const dbPath = path.join(dir, 'memory.db');
  const db = new DatabaseSync(dbPath, { allowExtension: true });
  db.loadExtension(VEC_SO);
  db.exec(MEMORY_DDL);
  db.exec('CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);');
  db.exec("INSERT INTO meta (key, value) VALUES ('dim', '4');");
  db.exec('CREATE VIRTUAL TABLE vec_items USING vec0(embedding float[4] distance_metric=cosine);');
  db.exec('CREATE TABLE vec_map (rowid INTEGER PRIMARY KEY, id TEXT);');
  let seq = 1;
  const insertMemory = db.prepare('INSERT INTO memories (id, text, tags, type, status, ts, entities, rowSeq) VALUES (?, ?, ?, ?, ?, ?, ?, ?)');
  const insertVecMap = db.prepare('INSERT INTO vec_map (rowid, id) VALUES (?, ?)');
  const insertVec = db.prepare('INSERT INTO vec_items (rowid, embedding) VALUES (?, ?)');
  for (const row of SEEDS) {
    insertMemory.run(row.id, row.text, '[]', 'world', 'active', '2026-11-01T00:00:00Z', JSON.stringify(['树莓派']), seq);
    insertVecMap.run(seq, row.id);
    // 全同向量：聚类一定把这批圈到一起，模型判定不依赖真实嵌入
    insertVec.run(BigInt(seq), new Uint8Array(Float32Array.from(row.vec).buffer));
    seq += 1;
  }
  db.close();
  applySchema(dbPath, { logger: () => {} });

  const cfg = {
    storePath: dir,
    storeLogFile: path.join(dir, 'store.log'),
    sleepLogFile: path.join(dir, 'sleep.log'),
    stateFile: path.join(dir, 'sleep.state.json'),
    apiUrl: 'http://127.0.0.1:1',
    timeoutMs: 50,
    provider: 'fake',
    model: 'fake',
    rrfK: 60,
    opinionAlpha: 0.1,
    splitMaxChars: 200,
    embed: async (texts) => texts.map(() => [0.9, 0.2, 0.2, 0]),
    pipeline: { agentPipeline, writeGate: false, textLint: false },
    clusterMaxSize: 30,
    clusterNearDup: 0.9,
    clusterQuestionMin: 0.78,
    organizeMaxCalls: 5,
    organizeConcurrency: 2,
    clusterChanges: 4,
    logger: { info() {}, warn() {} },
  };
  const store = createStore(cfg);
  return { store, cfg, dir, dbPath };
}

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
  { type: 'tool-call-delta', index: 0, id: 'call_1', name: 'submit_ops', argumentsDelta: args },
  { type: 'finish', reason: { kind: 'stop' } },
];

const textChunks = (text) => [
  { type: 'text-delta', text },
  { type: 'finish', reason: { kind: 'stop' } },
];

function readState(cfg) {
  try {
    return JSON.parse(fs.readFileSync(cfg.stateFile, 'utf8'));
  } catch {
    return {};
  }
}

function query(dbPath, sql, ...params) {
  const db = new DatabaseSync(dbPath, { readOnly: true });
  try {
    return db.prepare(sql).all(...params);
  } finally {
    db.close();
  }
}

test('空动作：有效 noop 写 organizeCache，不落 pending，下一轮命中缓存', async () => {
  const { store, cfg, dbPath } = makeStore();
  const llm = fakeLlm([() => toolChunks('{"ops":[]}')]);
  const sleep = setupSleep({ llm, provide() {}, effect: () => () => {}, tools: { register() {} } }, store, cfg);

  const out = await sleep.organizePass({ dryRun: false });
  assert.equal(out.agent, true);
  assert.equal(out.calls, 1, '这批只问一次，明确空动作不再原地重试');
  assert.equal(out.noop, 1);
  assert.equal(out.empty, 0, '空动作不是失败');
  assert.equal(llm.calls.length, 1);

  const rows = query(dbPath, "SELECT * FROM pending_ops WHERE kind='merge_batch'");
  assert.equal(rows.length, 0, '有效结论不许进 pending 烧钱');

  const cache = readState(cfg).organizeCache || {};
  assert.deepEqual(Object.keys(cache).length, 1);
  assert.equal(Object.values(cache)[0].v, 'noop');
  assert.match(Object.keys(cache)[0], /^[0-9a-f]{16}$/);

  const second = await sleep.organizePass({ dryRun: false });
  assert.equal(second.calls, 0, '同指纹第二轮不再问模型');
  assert.equal(second.cached, 1, 'noop 缓存照命中');
  assert.equal(llm.calls.length, 1);

  const active = query(dbPath, "SELECT id FROM memories WHERE status='active'");
  assert.equal(active.length, 3, '明确不动，一条记忆都不许改');
});

test('解析失败：attempts 每轮累加，到第 5 次判死（不再无限重试）', async () => {
  const { store, cfg, dbPath } = makeStore();
  const broken = () => [
    { type: 'text-delta', text: '不是 JSON' },
    { type: 'tool-call-delta', index: 0, name: 'submit_ops', argumentsDelta: '{oops' },
    { type: 'finish', reason: { kind: 'stop' } },
  ];
  const llm = fakeLlm([broken]);
  const sleep = setupSleep({ llm, provide() {}, effect: () => () => {}, tools: { register() {} } }, store, cfg);
  for (let round = 1; round <= 5; round += 1) await sleep.organizePass({ dryRun: false });

  const rows = query(dbPath, "SELECT * FROM pending_ops WHERE kind='merge_batch'");
  assert.equal(rows.length, 1);
  assert.equal(Number(rows[0].attempts), 5, '每轮记一次失败，不丢');
  assert.equal(rows[0].status, 'dead', 'attempts 到 5 判死，留给人看（不是放弃整个任务）');
  assert.match(fs.readFileSync(cfg.sleepLogFile, 'utf8'), /\[merge\] WARN merge_batch dead/, '判死要留日志');
  assert.match(rows[0].last_error, /^parse_fail /, 'last_error 带摘要前缀');
  assert.match(rows[0].last_error, /finish=stop/);
  assert.match(rows[0].last_error, /raw=|tool=|text=/, '原文片段要留进 last_error');
  assert.equal(llm.calls.length, 5, '每轮只问一次（不再原地重试），同一轮也不重复问');
  assert.deepEqual(Object.keys(readState(cfg).organizeCache || {}), []);
});

test('成功提案：不进 pending，合并落库并写 acted 缓存', async () => {
  const { store, cfg, dbPath } = makeStore();
  const llm = fakeLlm([() => toolChunks(JSON.stringify({ ops: [MERGE_OP] }))]);
  const sleep = setupSleep({ llm, provide() {}, effect: () => () => {}, tools: { register() {} } }, store, cfg);

  const out = await sleep.organizePass({ dryRun: false });
  assert.equal(out.merged.length, 1);
  assert.deepEqual(out.merged[0].ids.sort(), ['m_1', 'm_2']);
  assert.equal(out.invalid, 0);

  const pending = query(dbPath, 'SELECT * FROM pending_ops');
  assert.equal(pending.length, 0, '成功动作不该进 pending_ops');

  const merged = query(dbPath, "SELECT id, status FROM memories WHERE status='merged'");
  assert.equal(merged.length, 2);
  const fresh = query(dbPath, "SELECT text, source FROM memories WHERE source='sleep:merge'");
  assert.equal(fresh.length, 1);
  assert.equal(fresh[0].text, MERGE_OP.text);

  const cache = readState(cfg).organizeCache || {};
  const values = Object.values(cache);
  assert.equal(values.length, 1);
  assert.equal(values[0].v, 'acted');
});

test('dryRun：一个字节都不写 pending_ops，也不写 organizeCache', async () => {
  const { store, cfg, dbPath } = makeStore();
  const llm = fakeLlm([() => toolChunks('{"ops":[]}')]);
  const sleep = setupSleep({ llm, provide() {}, effect: () => () => {}, tools: { register() {} } }, store, cfg);

  const out = await sleep.organizePass({ dryRun: true });
  assert.equal(out.noop, 1, 'dryRun 也识别成空动作，但不写缓存');
  assert.equal(query(dbPath, 'SELECT * FROM pending_ops').length, 0);
  assert.deepEqual(Object.keys(readState(cfg).organizeCache || {}), []);
  assert.equal(query(dbPath, "SELECT id FROM memories WHERE status='active'").length, 3);
});

test('开关默认关：organizePass 仍走老分簇路径（dry 跑，行为未变）', async () => {
  const { store, cfg, dbPath } = makeStore({ agentPipeline: false });
  const legacy = () => textChunks('{"merge":[],"update":[],"archive":[],"add":[]}');
  const llm = fakeLlm([legacy]);
  const sleep = setupSleep({ llm, provide() {}, effect: () => () => {}, tools: { register() {} } }, store, cfg);

  const out = await sleep.organizePass({ dryRun: true });
  assert.equal(out.agent, undefined, '老路径不带 agent 标记');
  assert.equal(out.calls, 1);
  assert.equal(out.failed, 0);
  assert.equal(out.merged.length, 0);
  assert.equal(query(dbPath, 'SELECT * FROM pending_ops').length, 0);
  assert.deepEqual(Object.keys(readState(cfg).organizeCache || {}), []);
  assert.equal(query(dbPath, "SELECT id FROM memories WHERE status='active'").length, 3);
});
