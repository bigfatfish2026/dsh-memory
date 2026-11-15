import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { setupConsolidate, ESTIMATE_TRIGGER_MIN_INTERVAL_MS } from '../lib/consolidate.js';
import { consolidateStatePath, readConsolidateState, writeConsolidateState } from '../lib/index.js';

/**
 * 2026-11-14：重启后 6 分钟 11 次整理、每轮 39~59 万 token 输入。
 *
 * 根因两条：
 *  ① (no-session) 请求拿不到 pressure → 退回估算 → 估算 40 万+ 过 triggerTokens 70000，
 *     每个请求都触发一轮整理；这类请求根本没有真实上下文可整理。
 *  ② 估算触发后基准不推进（「没跑成/0 条」就不写），下一个请求拿同一份假估算又超线。
 * 修法：
 *  - 没有 sessionId 的请求一律不触发，只写一行日志；
 *  - 退回估算路径加 30 分钟硬节流，且上一轮没跑完不允许下一次；
 *  - 估算触发即推进基准（哪怕 0 条）；
 *  - 日志区分 pressure 触发 / 退回估算触发（带原因）。
 * 外加：整理状态（按会话 map + 节流窗口）落盘，重启读回。
 */

/** 主会话的实测压缩线：floor(min(1048576*0.1, 1048576-8000-65536)) = 104857。×0.9 = 94371。 */
const WINDOW = 1048576;
const COMPACT_TOKENS = 104857;

function tempCfg(extra = {}) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'consolidate-throttle-'));
  return {
    consolidateEnabled: true,
    consolidateOnlyMain: false, // 这组只管触发判定
    triggerTokens: 70000,
    growthTokens: 40000,
    maxHistoryChars: 600000,
    consolidateTools: false,
    consolidateLogPath: path.join(dir, 'consolidate.log'),
    notifyScript: '/bin/true',
    stateFile: path.join(dir, 'sleep.state.json'),
    ...extra,
  };
}

function fakeSession(id) {
  return {
    id,
    header: undefined,
    requestContext: () => ({ provider: 'magpie', model: 'group/ds', contextWindow: WINDOW }),
    requestHeader: () => ({ config: { maxTokens: 8000 } }),
  };
}

/** 假 harness；meter=false 模拟 tokenMeter 缺席（走退回估算路径）。 */
function fakeHarness({ pressures = {}, meter = true } = {}) {
  const handlers = new Map();
  const sessions = {};
  const ctx = {
    get: (name) => {
      if (name === 'sessions') return { get: (id) => sessions[id] };
      if (name === 'tokenMeter') {
        if (!meter) return undefined;
        return {
          contextPressure: (session) => ({
            pressureTokens: Number(pressures[session.id]) || 0,
            contextWindow: WINDOW,
          }),
        };
      }
      if (name === 'compaction') {
        return { config: { thresholdRatio: 0.1, headroomTokens: 65536, maxTokens: 8000 } };
      }
      return undefined;
    },
    on: (event, handler) => handlers.set(event, handler),
    tools: { register: () => {} },
    llm: {
      stream: () =>
        (async function* () {
          yield { type: 'text-delta', text: '[{"text":"估算节流测试条目","type":"world"}]' };
          yield { type: 'finish', reason: { kind: 'stop' } };
        })(),
    },
  };
  return { ctx, handlers, pressures, sessions };
}

function okStore() {
  const saveCalls = [];
  return {
    listTexts: () => [],
    save: async (text) => {
      saveCalls.push(text);
      return { text, id: 'm1' };
    },
    saveCalls,
  };
}

function request(sessionId, chars = 400) {
  return { sessionId, messages: [{ role: 'user', content: 'x'.repeat(chars) }] };
}

const SENTINEL = { untouched: true };
const passthrough = async function* () {};

/** 触发就把这一轮跑完（成功落库，基准会推进）。 */
async function drain(result) {
  assert.equal(typeof result?.[Symbol.asyncIterator], 'function', '期望这一 tick 触发整理');
  for await (const _ of result) {
    /* 原请求的流内容不重要 */
  }
  await result.pending;
}

function readLog(cfg) {
  try {
    return fs.readFileSync(cfg.consolidateLogPath, 'utf8');
  } catch {
    return '';
  }
}

function countOf(text, needle) {
  return text.split(needle).length - 1;
}

function setup(cfg, store, extras = {}, harness = fakeHarness()) {
  setupConsolidate(harness.ctx, store, cfg, extras);
  return harness.handlers.get('llm/stream');
}

// ------------------------------------------------------------ 1) 无会话请求

test('(no-session) 请求 20 次：触发 0 次，每次只写一行跳过日志', async () => {
  const cfg = tempCfg({ growthTokens: 0 });
  const store = okStore();
  const harness = fakeHarness();
  const handler = setup(cfg, store, {}, harness);

  // 40 万字符 ≈ 20 万估算 token，远超 triggerTokens 70000；旧逻辑每次都触发。
  for (let i = 0; i < 20; i += 1) {
    const r = handler(request('', 400000), () => SENTINEL);
    assert.equal(r, SENTINEL, `第 ${i + 1} 次 no-session 请求不该触发整理`);
  }
  const text = readLog(cfg);
  assert.equal(countOf(text, '触发整理：'), 0, '不该有任何触发');
  assert.equal(countOf(text, '跳过整理：无 sessionId 请求'), 20, '每次请求一行日志');
  assert.equal(store.saveCalls.length, 0, '不该花任何模型钱');
  // 没 sessionId 也不该落账
  assert.doesNotMatch(text, /\(no-session\)/);
});

// ------------------------------------------------------------ 2) 估算路径节流

test('退回估算路径 20 次：同一窗口只触发 1 次，且带原因日志', async () => {
  const cfg = tempCfg({ growthTokens: 0 }); // 让 growth 闸不挡，单看节流
  const store = okStore();
  const harness = fakeHarness({ meter: false });
  const A = 'session-eeeeeeee-1111';
  harness.sessions[A] = fakeSession(A);
  const handler = setup(cfg, store, {}, harness);

  // 143000 字符 ≈ 71500 token，过 70000
  await drain(handler(request(A, 143000), passthrough));
  for (let i = 0; i < 19; i += 1) {
    const r = handler(request(A, 143000), () => SENTINEL);
    assert.equal(r, SENTINEL, `第 ${i + 2} 次估算请求应在节流窗口内被挡`);
  }

  const text = readLog(cfg);
  assert.equal(countOf(text, '触发整理：退回估算'), 1, '20 次只该触发 1 次');
  assert.match(text, /原因：tokenMeter\.contextPressure 缺席/);
  assert.match(text, /触发即推进/);
  assert.match(text, new RegExp(`节流窗口 ${Math.round(ESTIMATE_TRIGGER_MIN_INTERVAL_MS / 60000)} 分钟`));
  // 被挡的日志按 10 分钟去重，19 次只留一行
  assert.equal(countOf(text, '跳过整理：退回估算被节流'), 1);
});

test('退回估算：上一轮没收尾时不允许下一次（estimateDone 闸）', async () => {
  const cfg = tempCfg({ growthTokens: 0 });
  const store = okStore();
  const harness = fakeHarness({ meter: false });
  const A = 'session-eeeeeeee-2222';
  harness.sessions[A] = fakeSession(A);
  const handler = setup(cfg, store, {
    consolidateState: { estimateTriggerAt: Date.now(), estimateDone: false },
  }, harness);

  const r = handler(request(A, 143000), () => SENTINEL);
  assert.equal(r, SENTINEL, '上一轮没跑完，不该触发');
  assert.match(readLog(cfg), /跳过整理：退回估算被节流（上一轮还没跑完/);
});

// ------------------------------------------------------------ 3) 正常压力路径

test('有 pressure 的会话到 0.9 线照常触发，不受估算节流影响', async () => {
  const cfg = tempCfg();
  const store = okStore();
  const harness = fakeHarness();
  const A = 'session-ffffffff-3333';
  harness.sessions[A] = fakeSession(A);
  const handler = setup(cfg, store, {
    // 估算节流账在窗口内，但这一轮走实测压力，不该被挡
    consolidateState: { estimateTriggerAt: Date.now(), estimateDone: false },
  }, harness);

  harness.pressures[A] = 95000;
  await drain(handler(request(A), passthrough));
  const text = readLog(cfg);
  assert.match(text, /触发整理：pressure 95000 >= 压缩线 104857/);
  assert.doesNotMatch(text, /退回估算/);
});

// ------------------------------------------------------------ 4) 状态落盘

test('状态文件默认跟 sleep.state.json 同目录，读写一致且坏文件不抛', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'consolidate-state-'));
  const cfg = { stateFile: path.join(dir, 'sleep.state.json'), consolidateStateFile: '' };
  assert.equal(consolidateStatePath(cfg), path.join(dir, 'consolidate.state.json'));

  const state = { lastTokens: { 'session-a': 71500 }, estimateTriggerAt: 1700000000000, estimateDone: true };
  writeConsolidateState(cfg, state);
  assert.deepEqual(readConsolidateState(cfg), state);

  fs.writeFileSync(consolidateStatePath(cfg), '{半截', 'utf8');
  assert.deepEqual(readConsolidateState(cfg), {});

  // 显式路径优先
  const explicit = path.join(dir, 'sub', 'my-consolidate.json');
  assert.equal(consolidateStatePath({ stateFile: cfg.stateFile, consolidateStateFile: explicit }), explicit);
});

test('状态落盘后重新加载：按会话的基准与节流窗口都能读回', async () => {
  const cfg = tempCfg({ consolidateStateFile: '' }); // 派生到 sleep.state.json 同目录
  const store = okStore();
  const persist = (state) => writeConsolidateState(cfg, state);
  const harness = fakeHarness({ meter: false });
  const A = 'session-gggggggg-4444';
  harness.sessions[A] = fakeSession(A);
  const handler = setup(cfg, store, {
    consolidateState: readConsolidateState(cfg),
    persistState: persist,
  }, harness);

  await drain(handler(request(A, 143000), passthrough));
  const onDisk = readConsolidateState(cfg);
  assert.equal(onDisk.lastTokens[A], 71500, '估算触发即推进的基准要落盘');
  assert.ok(Number(onDisk.estimateTriggerAt) > 0, '节流窗口要落盘');
  assert.equal(onDisk.estimateDone, true, '跑完后收尾标记要落盘');
  assert.equal(onDisk.pendingTokens?.[A], undefined, '成功回写后 pending 清掉');

  // 模拟重启：新 harness 从磁盘读回同一份状态
  const restarted = fakeHarness();
  restarted.sessions[A] = fakeSession(A);
  restarted.pressures[A] = 96000; // 96000 - 71500 = 24500 < growthTokens 40000
  const handler2 = setup(cfg, store, {
    consolidateState: readConsolidateState(cfg),
    persistState: persist,
  }, restarted);
  assert.equal(
    handler2(request(A), () => SENTINEL),
    SENTINEL,
    '读回基准后同会话只涨 24500 不该触发；读不回就是基准 0、直接触发',
  );
});
