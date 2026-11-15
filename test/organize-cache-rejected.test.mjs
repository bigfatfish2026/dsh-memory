// 2026-11-07 维护者定：organizeCache 只记 acted / noop，rejected 一律不写。
// 理由：写进去下一轮同指纹直接命中（sleep.js `if (cache[fp]) continue`），
// 那批「全被证据校验打回」的条目就永远没人再看一眼；宁可多问一次模型。
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

// 三条同实体、两两余弦 <0.9（不触发近重复连边）、最高 0.8（过 clusterQuestionMin）
const SEEDS = [
  { id: 'm_1', text: '树莓派温度 45 度。', vec: [1, 0, 0, 0] },
  { id: 'm_2', text: '树莓派风扇转速 3000 转。', vec: [0.8, 0.6, 0, 0] },
  { id: 'm_3', text: '树莓派今晚重启过一次。', vec: [0.8, 0, 0.6, 0] },
];

// quote 在原文里找不到 → validateOps 判 invalid，整批证据打回
const BAD_OP = {
  op: 'merge',
  ids: ['m_1', 'm_2'],
  text: '树莓派温度 45 度，风扇转速 3000 转。',
  evidence: [
    { id: 'm_1', quote: '这句话原文里根本没有。' },
    { id: 'm_2', quote: '这句也没有。' },
  ],
  needsSplit: false,
  reason: '同一台机器的两条状态',
};

const GOOD_OP = {
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

function makeStore() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-cache-rejected-'));
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
    pipeline: { agentPipeline: true, writeGate: false, textLint: false },
    clusterMaxSize: 30,
    clusterNearDup: 0.9,
    clusterQuestionMin: 0.78,
    organizeMaxCalls: 5,
    organizeConcurrency: 2,
    clusterChanges: 4,
    logger: { info() {}, warn() {} },
  };
  const store = createStore(cfg);
  return { store, cfg };
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

function readCache(cfg) {
  try {
    const state = JSON.parse(fs.readFileSync(cfg.stateFile, 'utf8'));
    return state.organizeCache || {};
  } catch {
    return {};
  }
}

const sleepOf = (llm, store, cfg) => setupSleep(
  { llm, provide() {}, effect: () => () => {}, tools: { register() {} } },
  store,
  cfg,
);

test('整批被证据校验打回：不写 organizeCache，下一轮还要再问', async () => {
  const { store, cfg } = makeStore();
  const llm = fakeLlm([() => toolChunks(JSON.stringify({ ops: [BAD_OP] }))]);
  const sleep = sleepOf(llm, store, cfg);

  const first = await sleep.organizePass({ dryRun: false });
  assert.equal(first.calls, 1);
  assert.equal(first.invalid, 1, '提案被证据校验打回');
  assert.equal(first.merged.length, 0, '一条都没落地');
  assert.deepEqual(Object.keys(readCache(cfg)), [], 'rejected 不许进 organizeCache');

  const second = await sleep.organizePass({ dryRun: false });
  assert.equal(second.cached, 0, '第二轮不许命中缓存');
  assert.equal(second.calls, 1, '第二轮重新问模型');
  assert.equal(llm.calls.length, 2, '两轮各问一次，共两次调用');
});

test('明确空 ops：noop 照旧写缓存，下一轮命中不再问', async () => {
  const { store, cfg } = makeStore();
  const llm = fakeLlm([() => toolChunks('{"ops":[]}')]);
  const sleep = sleepOf(llm, store, cfg);

  const first = await sleep.organizePass({ dryRun: false });
  assert.equal(first.noop, 1);
  const cache = readCache(cfg);
  assert.equal(Object.keys(cache).length, 1, 'noop 要写缓存');
  assert.equal(Object.values(cache)[0].v, 'noop');

  const second = await sleep.organizePass({ dryRun: false });
  assert.equal(second.cached, 1, 'noop 缓存照命中');
  assert.equal(second.calls, 0);
  assert.equal(llm.calls.length, 1);
});

test('真落地：acted 照旧写缓存', async () => {
  const { store, cfg } = makeStore();
  const llm = fakeLlm([() => toolChunks(JSON.stringify({ ops: [GOOD_OP] }))]);
  const sleep = sleepOf(llm, store, cfg);

  const first = await sleep.organizePass({ dryRun: false });
  assert.equal(first.merged.length, 1);
  const cache = readCache(cfg);
  assert.equal(Object.keys(cache).length, 1, 'acted 要写缓存');
  assert.equal(Object.values(cache)[0].v, 'acted');
});

test('dryRun：rejected 也不留缓存痕迹', async () => {
  const { store, cfg } = makeStore();
  const llm = fakeLlm([() => toolChunks(JSON.stringify({ ops: [BAD_OP] }))]);
  const sleep = sleepOf(llm, store, cfg);

  await sleep.organizePass({ dryRun: true });
  assert.deepEqual(Object.keys(readCache(cfg)), []);
});
