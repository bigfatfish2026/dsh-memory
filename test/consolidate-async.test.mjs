import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { setupConsolidate } from '../lib/consolidate.js';

/**
 * 2026-11-13：压缩前整理改异步（方案 A）。
 *
 * 改之前钩子是 `await consolidate(options)` 再 `yield* next()`，
 * memory_consolidate.log 13:50:19→13:51:54 那轮 95953ms 全压在用户请求第一颗 token 之前
 * （主会话 turn/start 13:50:18、assistant/message 13:52:04）。
 *
 * 这组用例钉三件事：
 *  ① 整理还在跑，请求照样先把自己的流吐完（触发点不等结果）；
 *  ② 整理失败不再往调用方抛，但必须留日志 + 计数（异步后没人接异常）；
 *  ③ 一轮在跑时再触发，被 inFlight 挡住并记一笔跳过，不排队、不重复打模型。
 */

function tempCfg(extra = {}) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'consolidate-async-'));
  return {
    consolidateEnabled: true,
    consolidateOnlyMain: false, // 这组用例只管异步语义，不掺子会话类判据
    triggerTokens: 1000,
    growthTokens: 0,
    cooldownMinutes: 0, // 并发用例要能连续触发两次才撞得到 inFlight
    maxHistoryChars: 600000,
    consolidateTools: false, // 关工具：一轮就出 JSON，用例不依赖工具轮
    consolidateToolRounds: 0,
    consolidateLogPath: path.join(dir, 'consolidate.log'),
    notifyScript: '/bin/true',
    ...extra,
  };
}

/** estimateTokens 按 2 字符/token 算，2000 字符 = 1000 token，刚好过触发线。 */
function bigMessages() {
  return [{ role: 'user', content: 'x'.repeat(2000) }];
}

/** 假 llm：一轮吐一个合法 JSON 数组就收工。 */
function jsonStream() {
  return (async function* () {
    yield { type: 'text-delta', text: '[{"text":"异步整理测试条目","type":"world"}]' };
    yield { type: 'finish', reason: { kind: 'stop' } };
  })();
}

/**
 * 2026-11-14：这组统一走「实测压力」路径。
 * 无 sessionId 的请求已一律不触发整理（退回估算另有 30 分钟节流），
 * 异步/失败/inFlight 这些语义要拿有会话、有 pressure 的请求来测。
 */
const SESSION = 'session-async-0001';

function fakeSession(id) {
  return {
    id,
    header: undefined,
    requestContext: () => ({ provider: 'magpie', model: 'group/ds', contextWindow: 1048576 }),
    requestHeader: () => ({ config: { maxTokens: 8000 } }),
  };
}

/** 假 harness：会话 + tokenMeter 常驻 high pressure，触发线 104857×0.9。 */
function fakeHarness() {
  const handlers = new Map();
  const sessions = { [SESSION]: fakeSession(SESSION) };
  let streams = 0;
  const ctx = {
    get: (name) => {
      if (name === 'sessions') return { get: (id) => sessions[id] };
      if (name === 'tokenMeter') {
        return { contextPressure: () => ({ pressureTokens: 120000, contextWindow: 1048576 }) };
      }
      if (name === 'compaction') {
        return { config: { thresholdRatio: 0.1, headroomTokens: 65536, maxTokens: 8000 } };
      }
      return undefined;
    },
    on: (event, handler) => handlers.set(event, handler),
    tools: { register: () => {} },
    llm: {
      stream: () => {
        streams += 1;
        return jsonStream();
      },
    },
  };
  return { ctx, handlers, streams: () => streams };
}

/** 原请求自己的流：只吐一颗 chunk，用来观察它有没有被整理挡住。 */
const passthrough = async function* () {
  yield { type: 'text-delta', text: '主回答' };
};

function readLog(cfg) {
  try {
    return fs.readFileSync(cfg.consolidateLogPath, 'utf8');
  } catch {
    return '';
  }
}

test('触发后立刻返回：整理卡在写库，请求照样先拿到自己的流', async () => {
  const cfg = tempCfg();
  let saveStarted = false;
  const store = {
    listTexts: () => [],
    // 永远不 resolve：模拟一轮 60 秒级别的整理正卡在落库
    save: () => {
      saveStarted = true;
      return new Promise(() => {});
    },
  };
  const { ctx, handlers } = fakeHarness();
  setupConsolidate(ctx, store, cfg, {});
  const handler = handlers.get('llm/stream');

  const stream = handler({ sessionId: SESSION, messages: bigMessages() }, passthrough);
  let settled = false;
  stream.pending.then(() => {
    settled = true;
  });

  const got = [];
  for await (const chunk of stream) got.push(chunk.text);

  assert.deepEqual(got, ['主回答'], '原请求的流应该原样吐完');
  assert.equal(settled, false, '整理没跑完，请求这一侧就已经放行了');
  await new Promise((resolve) => setTimeout(resolve, 20));
  assert.equal(saveStarted, true, '整理本身确实在后台跑起来了');
  assert.match(readLog(cfg), /整理输入 \d+ 字符/, '后台这一轮应该真的开了');
});

test('整理失败不抛到调用方，但日志 + 计数都要留', async () => {
  const cfg = tempCfg({ cooldownMinutes: 25 });
  const store = {
    listTexts: () => {
      throw new Error('boom');
    },
  };
  const { ctx, handlers } = fakeHarness();
  setupConsolidate(ctx, store, cfg, {});
  const handler = handlers.get('llm/stream');

  const stream = handler({ sessionId: SESSION, messages: bigMessages() }, passthrough);
  const got = [];
  for await (const chunk of stream) got.push(chunk.text);
  const result = await stream.pending; // 不该 throw

  assert.deepEqual(got, ['主回答'], '整理失败不该影响原请求');
  assert.equal(result.skipped, 'failed');
  assert.match(readLog(cfg), /整理失败（已忽略，累计 1 次）：boom/);
});

test('整理在跑时再触发一次：被 inFlight 挡住，只记跳过、不再打模型', async () => {
  const cfg = tempCfg();
  const store = {
    listTexts: () => [],
    save: () => new Promise(() => {}),
  };
  const { ctx, handlers, streams } = fakeHarness();
  setupConsolidate(ctx, store, cfg, {});
  const handler = handlers.get('llm/stream');

  const first = handler({ sessionId: SESSION, messages: bigMessages() }, passthrough);
  for await (const _ of first) {
    /* 先把原请求放行，整理留在后台 */
  }
  await new Promise((resolve) => setTimeout(resolve, 20)); // 让第一轮跑到卡点

  const second = handler({ sessionId: SESSION, messages: bigMessages() }, passthrough);
  const secondResult = await second.pending;

  assert.equal(secondResult.skipped, 'busy');
  assert.equal(streams(), 1, '第二轮不该再开一次模型调用');
  assert.match(readLog(cfg), /整理跳过（上一轮还在跑，累计跳过 1 次）/);
});

test('异步后整理读的仍是触发那一刻的会话快照，之后的改写不影响它', async () => {
  const cfg = tempCfg();
  const store = {
    listTexts: () => [],
    save: async (text) => ({ text }),
  };
  const { ctx, handlers } = fakeHarness();
  setupConsolidate(ctx, store, cfg, {});
  const handler = handlers.get('llm/stream');

  const options = { sessionId: SESSION, messages: bigMessages() }; // 2000 字符
  const stream = handler(options, passthrough);
  options.messages = []; // 模拟压缩把会话面重置掉
  for await (const _ of stream) {
    /* 放行原请求 */
  }
  await stream.pending;

  const chars = Number(readLog(cfg).match(/整理输入 (\d+) 字符/)?.[1] ?? 0);
  assert.ok(chars >= 2000, `快照应该还是触发时的 2000 字符，实际 ${chars}`);
});

test('整理自己发起的模型调用不会再触发一轮整理（purpose 挡掉，不白记跳过）', async () => {
  const cfg = tempCfg(); // cooldown 也设 0：专测 purpose，不让冷却替它挡
  const store = {
    listTexts: () => [],
    save: async (text) => ({ text }),
  };
  const { ctx, handlers } = fakeHarness();
  setupConsolidate(ctx, store, cfg, {});
  const handler = handlers.get('llm/stream');

  // 线上 ctx.llm.stream 也被同一个钩子包着：这里模拟「整理自己发起的调用」，
  // 且故意塞一份超过 triggerTokens 的 messages——没有 purpose 就会自己套自己。
  const huge = [{ role: 'user', content: 'p'.repeat(200000) }];
  ctx.llm.stream = (options) => handler({ ...options, messages: huge }, () => jsonStream());

  const stream = handler({ sessionId: SESSION, messages: bigMessages() }, passthrough);
  for await (const _ of stream) {
    /* 放行原请求 */
  }
  await stream.pending;

  const logText = readLog(cfg);
  assert.equal((logText.match(/整理输入/g) || []).length, 1, '只该有一轮真整理');
  assert.doesNotMatch(logText, /整理跳过/, '整理自己的调用应被 purpose 挡在 shouldRun，不该走到 inFlight');
});
