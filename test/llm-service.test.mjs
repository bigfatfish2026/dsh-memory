/**
 * 2026-11-14：merge 阶段整段空转的回归测试。
 *
 * 现场：12:30 那轮被重启打断，start() 补判「接管上一实例的轮」跑起来；跑到一半插件又被重载，
 * 旧一轮手里的 setup 期 ctx 已 dispose，132 次取 llm 全抛
 *   cannot get required service "llm" in inactive context
 * 收尾却写「睡觉完成 合并0 失败132」。这里锁三件事：
 *   ① 失效 ctx 用 ctx.get('llm') 仍能取到在线服务（不是 ctx.llm）；
 *   ② 真取不到就第一批中止 + 一行醒目日志，整轮按失败走（不 markSlot、不写睡觉完成）；
 *   ③ 接管路径与正常定时路径都用同一套取法，merge 照旧。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { applySchema } from '../scripts/apply-pipeline-schema.mjs';
import { createStore } from '../lib/store.js';
import { setupSleep } from '../lib/sleep.js';
import { LlmUnavailableError, contextStateOf, isLlmUnavailable, llmOf } from '../lib/llm-service.js';

const VEC_SO = path.join(import.meta.dirname, '../node_modules/sqlite-vec-linux-arm64/vec0.so');

const MEMORY_DDL = `CREATE TABLE memories (
  id TEXT PRIMARY KEY, text TEXT NOT NULL, tags TEXT, source TEXT, ts TEXT, type TEXT,
  entities TEXT, validFrom TEXT, supersedes TEXT, updatedAt TEXT, status TEXT, reason TEXT,
  supersededBy TEXT, validTo TEXT, confidence REAL, extra TEXT, present TEXT, rowSeq INTEGER NOT NULL
);`;

// 三条同实体 + 高相似，保证一定聚成一批交给 merge agent。
const SEEDS = [
  { id: 'm_1', text: '树莓派温度 45 度。', vec: [1, 0, 0, 0] },
  { id: 'm_2', text: '树莓派风扇转速 3000 转。', vec: [0.8, 0.6, 0, 0] },
  { id: 'm_3', text: '树莓派温度 45 度，风扇 3000 转。', vec: [0.8, 0, 0.6, 0] },
];

function makeStore(extra = {}) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-llm-inactive-'));
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
    backupDir: path.join(dir, 'backup'),
    sessionsRoot: path.join(dir, 'sessions'),
    apiUrl: 'http://127.0.0.1:1',
    timeoutMs: 50,
    provider: 'fake',
    model: 'fake',
    rrfK: 60,
    opinionAlpha: 0.1,
    splitMaxChars: 9999,
    embed: async (texts) => texts.map(() => [0.9, 0.2, 0.2, 0]),
    pipeline: { agentPipeline: true, writeGate: false, textLint: false, lintReview: false },
    clusterMaxSize: 30,
    clusterNearDup: 0.9,
    clusterQuestionMin: 0.78,
    organizeConcurrency: 1,
    sleepEnabled: true,
    sleepPriceProbe: false,
    sleepDeferInPeak: false,
    sleepSummaryNotify: false,
    slots: [{ hour: 0, minute: 0 }],
    selfTestMarker: path.join(dir, 'no-selftest.marker'),
    logger: { info() {}, warn() {} },
    ...extra,
  };
  const store = createStore(cfg);
  return { store, cfg, dir };
}

function fakeLlm(handlers, { onCall } = {}) {
  const calls = [];
  return {
    calls,
    stream(options) {
      calls.push(options);
      if (onCall) onCall(calls.length, options);
      const handler = handlers[Math.min(calls.length - 1, handlers.length - 1)];
      return (async function* streamChunks() {
        for (const chunk of handler(options)) yield chunk;
      })();
    },
  };
}

const textChunks = (text) => [
  { type: 'text-delta', text },
  { type: 'finish', reason: { kind: 'stop' } },
];
const toolChunks = (args) => [
  { type: 'tool-call-delta', index: 0, id: 'call_1', name: 'submit_ops', argumentsDelta: args },
  { type: 'finish', reason: { kind: 'stop' } },
];

/**
 * 假 ctx。默认把 `ctx.llm` 定义成「一读就抛 cordis 原文」——
 * 修复前任何一处漏改的 ctx.llm 都会在这里当场炸出来。
 */
function fakeCtx({ llm, get, fiber = { uid: 1, state: 2 }, runEffects = false } = {}) {
  const disposers = [];
  const ctx = {
    fiber,
    get: get ?? ((name) => (name === 'llm' ? llm : undefined)),
    provide() {},
    tools: { register() {} },
    effect(fn) {
      if (!runEffects) return () => {};
      const dispose = fn();
      if (typeof dispose === 'function') disposers.push(dispose);
      return () => { try { dispose?.(); } catch { /* 忽略 */ } };
    },
    dispose() {
      for (const d of disposers.splice(0)) {
        try { d(); } catch { /* 忽略 */ }
      }
    },
  };
  Object.defineProperty(ctx, 'llm', {
    configurable: true,
    get() { throw new Error('cannot get required service "llm" in inactive context'); },
  });
  return ctx;
}

function readState(cfg) {
  try {
    return JSON.parse(fs.readFileSync(cfg.stateFile, 'utf8'));
  } catch {
    return {};
  }
}

function logText(cfg) {
  try {
    return fs.readFileSync(cfg.sleepLogFile, 'utf8');
  } catch {
    return '';
  }
}

function pendingRows(cfg) {
  const db = new DatabaseSync(path.join(cfg.storePath, 'memory.db'), { readOnly: true });
  try {
    return db.prepare('SELECT * FROM pending_ops').all();
  } finally {
    db.close();
  }
}

async function waitFor(fn, { timeoutMs = 8000, stepMs = 25 } = {}) {
  const until = Date.now() + timeoutMs;
  while (Date.now() < until) {
    if (fn()) return true;
    await new Promise((r) => setTimeout(r, stepMs));
  }
  return false;
}

/** 写一个够 4 条消息的会话文件，让 ingestByTranscript 不判 too-quiet。 */
function seedSession(cfg, { files = 1 } = {}) {
  for (let n = 0; n < files; n += 1) {
    const dir = path.join(cfg.sessionsRoot, '--home-user--', `s${n}`);
    fs.mkdirSync(dir, { recursive: true });
    const now = Date.now() - 3600 * 1000;
    const events = [
      { time: now, type: 'user/message', data: { source: { kind: 'user' }, content: [{ type: 'text', text: '温度多少' }] } },
      { time: now + 1, type: 'assistant/message', data: { message: { content: [{ type: 'text', text: '45 度' }] } } },
      { time: now + 2, type: 'user/message', data: { source: { kind: 'user' }, content: [{ type: 'text', text: '风扇呢' }] } },
      { time: now + 3, type: 'assistant/message', data: { message: { content: [{ type: 'text', text: '3000 转' }] } } },
    ];
    fs.writeFileSync(path.join(dir, 'session.v4.jsonl'), `${events.map((e) => JSON.stringify(e)).join('\n')}\n`, 'utf8');
  }
}

test('失效 ctx：直接读 ctx.llm 会抛 inactive context，llmOf 走 ctx.get 仍能取到', () => {
  const llm = { stream() {} };
  const dead = fakeCtx({ llm, fiber: { uid: null, state: 4 } });
  assert.throws(() => dead.llm, /cannot get required service "llm" in inactive context/);
  assert.equal(llmOf(dead), llm, 'ctx.get 不要求 inject，失效 ctx 也取得到在线服务');

  const lost = fakeCtx({ get: () => undefined, fiber: { uid: null, state: 4 } });
  assert.throws(
    () => llmOf(lost),
    (error) => error instanceof LlmUnavailableError
      && error.code === 'LLM_UNAVAILABLE'
      && /"llm"/.test(error.message)
      && /fiber\.uid=null\(已失效\)/.test(error.message)
      && /fiber\.state=4/.test(error.message),
  );
  assert.equal(contextStateOf(lost), 'fiber.uid=null(已失效) fiber.state=4');
  assert.equal(isLlmUnavailable(new Error('cannot get required service "llm" in inactive context')), true);
  assert.equal(isLlmUnavailable(new Error('别的错')), false);
});

test('接管路径（失效 ctx）：merge 用同一套 llmOf 取 llm，照常问模型', async () => {
  const { store, cfg } = makeStore();
  const llm = fakeLlm([() => toolChunks('{"ops":[]}')]);
  const ctx = fakeCtx({ llm, fiber: { uid: null, state: 4 } });
  const sleep = setupSleep(ctx, store, cfg);

  const out = await sleep.organizePass({ dryRun: false });
  assert.equal(out.calls, 1, '一批照问');
  assert.equal(out.failed, 0);
  assert.equal(llm.calls.length, 1);
});

test('取不到 llm：merge 第一批就中止，一行醒目日志带服务名与 ctx 状态，不落 pending', async () => {
  const { store, cfg } = makeStore();
  const ctx = fakeCtx({ get: () => undefined, fiber: { uid: null, state: 4 } });
  const sleep = setupSleep(ctx, store, cfg);

  await assert.rejects(
    () => sleep.organizePass({ dryRun: false }),
    (error) => error instanceof LlmUnavailableError,
    '整轮要抛失败，不能返回「合并0 失败132」当成功',
  );

  const lines = logText(cfg).split('\n').filter((line) => line.includes('⛔'));
  assert.equal(lines.length, 1, '只写一行，不许每批刷一条');
  assert.match(lines[0], /merge 阶段中止/);
  assert.match(lines[0], /"llm"/);
  assert.match(lines[0], /fiber\.uid=null\(已失效\)/);
  assert.match(lines[0], /1 批一批都没问/);
  assert.equal(pendingRows(cfg).length, 0, '中止的批次不写 pending_ops');
});

test('接管路径整轮：中途 llm 掉了 → merge 第一批中止，整轮 failed，不落 slot', async () => {
  const { store, cfg } = makeStore();
  seedSession(cfg);
  // 上一实例留下的锁：start() 立即补判 → 接管 → scheduledRun。
  fs.writeFileSync(cfg.stateFile, JSON.stringify({
    runningAt: Date.now(), heartbeatAt: Date.now(), owner: 'deadbeef:111:222',
  }), 'utf8');

  let available = true;
  const llm = fakeLlm(
    [() => textChunks('[]'), () => toolChunks('{"ops":[]}')],
    // 入库那一次提问成功后，llm 服务就「没了」，模拟重载/卸载窗口
    { onCall: (n) => { if (n === 1) available = false; } },
  );
  const ctx = fakeCtx({
    get: (name) => (name === 'llm' && available ? llm : undefined),
    fiber: { uid: 1, state: 2 },
    runEffects: true,
  });
  const sleep = setupSleep(ctx, store, cfg);

  const done = await waitFor(() => /睡觉失败/.test(logText(cfg)));
  ctx.dispose();
  assert.ok(done, `应该以整轮失败收场，日志：${logText(cfg).slice(-800)}`);

  const text = logText(cfg);
  assert.match(text, /⛔ merge 阶段中止/);
  assert.match(text, /"llm"/);
  assert.match(text, /睡觉失败/);
  assert.doesNotMatch(text, /睡觉完成/, '失败轮不许写「睡觉完成」');
  assert.equal(Object.keys(readState(cfg).slots || {}).length, 0, '失败轮不 markSlot，下一拍还要重试');
});

test('正常定时路径整轮：merge 照旧跑完并 markSlot', async () => {
  const { store, cfg } = makeStore();
  seedSession(cfg);

  const llm = fakeLlm([() => textChunks('[]'), () => toolChunks('{"ops":[]}')]);
  const ctx = fakeCtx({ llm, fiber: { uid: 1, state: 2 }, runEffects: true });
  const sleep = setupSleep(ctx, store, cfg);

  const done = await waitFor(() => /睡觉完成/.test(logText(cfg)));
  ctx.dispose();
  assert.ok(done, `应该正常跑完，日志：${logText(cfg).slice(-800)}`);

  const text = logText(cfg);
  assert.match(text, /合并 agent 并发 1 路问完 1 批/);
  assert.doesNotMatch(text, /⛔/);
  assert.ok(llm.calls.length >= 2, '入库一次 + merge 一次，都得走 llmOf');
  const slots = readState(cfg).slots || {};
  assert.equal(Object.keys(slots).length, 1, '成功轮要 markSlot');
});

test('预检能取、第一批真取时没了：worker 当场停，剩余批次不再发', async () => {
  const { store, cfg } = makeStore();
  const llm = fakeLlm([() => toolChunks('{"ops":[]}')]);
  let llmGets = 0;
  const ctx = fakeCtx({
    get: (name) => {
      if (name !== 'llm') return undefined;
      llmGets += 1;
      return llmGets === 1 ? llm : undefined;
    },
  });
  const sleep = setupSleep(ctx, store, cfg);

  await assert.rejects(() => sleep.organizePass({ dryRun: false }), (e) => isLlmUnavailable(e));
  const lines = logText(cfg).split('\n').filter((line) => line.includes('⛔'));
  assert.equal(lines.length, 1);
  assert.match(lines[0], /第1批调用时服务掉了/);
  assert.equal(llm.calls.length, 0, '第一批都没发出去');
});

test('老分簇路径同样 fail-loud：一批都不问就整轮失败', async () => {
  const { store, cfg } = makeStore();
  cfg.pipeline = { ...cfg.pipeline, agentPipeline: false };
  const ctx = fakeCtx({ get: () => undefined, fiber: { uid: null, state: 4 } });
  const sleep = setupSleep(ctx, store, cfg);

  await assert.rejects(() => sleep.organizePass({ dryRun: false }), (e) => isLlmUnavailable(e));
  const lines = logText(cfg).split('\n').filter((line) => line.includes('⛔'));
  assert.equal(lines.length, 1);
  assert.match(lines[0], /分簇整理阶段中止/);
  assert.match(lines[0], /1 批一批都没问/);
});
