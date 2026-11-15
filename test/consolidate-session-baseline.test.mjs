import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { normalizeCooldownState, setupConsolidate, SESSION_TOKEN_TTL_DAYS } from '../lib/consolidate.js';

/**
 * 2026-11-14：压缩前整理乱触发。
 *
 * 根因两条，都在 shouldRun 里：
 *  ① state.lastTokens 是全局一份、不分会话。群会话 04:19/04:20 各请求一次
 *     （体量比主会话小一半以上）就把主会话的基准清 0，主会话下一轮白整理一遍。
 *  ② 判据是「自己估算的 token >= triggerTokens(70000)」，跟 compaction 的压缩线
 *     （主会话 104857）是两套尺子，0.9 线（94371）反而比 70000 高——估到 70k 就跑。
 *
 * 现在的判据：
 *  - 有实测压力时：该会话 pressure >= 压缩线 × 0.9；
 *  - 拿不到实测压力时：退回 cfg.triggerTokens 估算路径，不让整理彻底不跑；
 *  - 基准按 sessionId 分账，「跌一半清零」只清当前会话那一格；
 *  - 会话格 7 天没更新就回收；老 state 单个数字只给第一条会话认领一次。
 *
 * 这组用例钉住上面四条，外加「同会话连涨不重复触发」。
 */

/** 主会话的实测压缩线：floor(min(1048576*0.1, 1048576-8000-65536)) = 104857。×0.9 = 94371。 */
const WINDOW = 1048576;
const COMPACT_TOKENS = 104857;
const TRIGGER_LINE = Math.floor(COMPACT_TOKENS * 0.9);

function tempCfg(extra = {}) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'consolidate-session-'));
  return {
    consolidateEnabled: true,
    consolidateOnlyMain: false, // 这组只管触发判定，不掺子会话类判据
    triggerTokens: 70000,
    growthTokens: 40000,
    maxHistoryChars: 600000,
    consolidateTools: false, // 关工具：一轮就出 JSON
    consolidateLogPath: path.join(dir, 'consolidate.log'),
    notifyScript: '/bin/true',
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

/**
 * 假 harness：会话表 + tokenMeter.contextPressure + compaction 配置，全部可控。
 *
 * @param {object} opts
 * @param {object} [opts.pressures] { [sessionId]: 当前压力 }，可变（测试中途改）
 * @param {boolean} [opts.meter] false = tokenMeter 服务缺席（走 70000 估算退回）
 */
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
          yield { type: 'text-delta', text: '[{"text":"会话分账测试条目","type":"world"}]' };
          yield { type: 'finish', reason: { kind: 'stop' } };
        })(),
    },
  };
  let streams = 0;
  return { ctx, handlers, pressures, sessions, streams: () => streams };
}

/** 主会话请求：messages 只要过 200 字符就够，判定走 pressure 不看体量。 */
function request(sessionId, chars = 400) {
  return { sessionId, messages: [{ role: 'user', content: 'x'.repeat(chars) }] };
}

const SENTINEL = { untouched: true };
const passthrough = async function* () {};

function isTriggered(result) {
  return typeof result?.[Symbol.asyncIterator] === 'function';
}

/** 触发就把这一轮跑完（成功落库，基准会推进）。 */
async function drain(result) {
  assert.equal(isTriggered(result), true, '期望这一 tick 放行');
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

/** 落库成功的假 store：save 返回一条新记录，listTexts 空。 */
function okStore() {
  return {
    listTexts: () => [],
    save: async (text) => ({ text, id: 'm1' }),
  };
}

function setup(cfg, store, extras = {}, harness = fakeHarness()) {
  const handler = (() => {
    setupConsolidate(harness.ctx, store, cfg, extras);
    return harness.handlers.get('llm/stream');
  })();
  return { handler, harness };
}

// ------------------------------------------------------------ A) 按会话分账

test('两个会话交替：B 的请求不把 A 的基准清零', async () => {
  const cfg = tempCfg();
  const store = okStore();
  const harness = fakeHarness();
  const handler = (() => {
    setupConsolidate(harness.ctx, store, cfg, {});
    return harness.handlers.get('llm/stream');
  })();
  const A = 'session-aaaaaaaa-1111';
  const B = 'session-bbbbbbbb-2222';
  harness.sessions[A] = fakeSession(A);
  harness.sessions[B] = fakeSession(B);

  // A 涨到 0.9 线以上：触发一次，基准推到 95000
  harness.pressures[A] = 95000;
  await drain(handler(request(A), passthrough));

  // B 只有 40000（不足 A 基准的一半）：旧代码在这里把全局 lastTokens 清 0
  harness.pressures[B] = 40000;
  assert.equal(handler(request(B), () => SENTINEL), SENTINEL, 'B 没到自己的线，不该触发');

  // A 再涨 1000：基准还在 95000，增长不到 40000，不该白跑
  harness.pressures[A] = 96000;
  assert.equal(handler(request(A), () => SENTINEL), SENTINEL, 'A 的基准被 B 清了才会误触发');
  assert.match(readLog(cfg), /触发整理：pressure 95000 >= 压缩线 104857/);
});

test('同会话连涨：到 0.9 线才触发一次，之后要涨够 growthTokens', async () => {
  const cfg = tempCfg();
  const store = okStore();
  const A = 'session-cccccccc-3333';
  const harness = fakeHarness();
  setupConsolidate(harness.ctx, store, cfg, {});
  const handler = harness.handlers.get('llm/stream');
  harness.sessions[A] = fakeSession(A);

  // 线下面一点：不触发（以前 70000 就跑了）
  harness.pressures[A] = 90000;
  assert.equal(handler(request(A), () => SENTINEL), SENTINEL, `90000 < ${TRIGGER_LINE} 不该触发`);

  // 过线：触发一次
  harness.pressures[A] = 95000;
  await drain(handler(request(A), passthrough));

  // 只涨 3000：基准已推进，growthTokens 没到，不重复触发
  harness.pressures[A] = 98000;
  assert.equal(handler(request(A), () => SENTINEL), SENTINEL, '同会话连涨不该重复整理');

  // 压缩后掉到 30000：跌一半 → 清掉 A 这一格；这一轮本身不触发
  harness.pressures[A] = 30000;
  assert.equal(handler(request(A), () => SENTINEL), SENTINEL, '刚压缩过这一轮不该触发');

  // 重新涨回线上：清过基准，再触发一次
  harness.pressures[A] = 95000;
  await drain(handler(request(A), passthrough));
});

test('会话格过期回收：7 天没更新的基准在启动时清掉', async () => {
  const cfg = tempCfg();
  const store = okStore();
  const old = 'session-oldddddd-4444';
  const harness = fakeHarness();
  const staleAt = Date.now() - (SESSION_TOKEN_TTL_DAYS + 1) * 86400000;
  setupConsolidate(harness.ctx, store, cfg, {
    consolidateState: { lastTokens: { [old]: { tokens: 120000, at: staleAt } } },
  });
  const handler = harness.handlers.get('llm/stream');
  harness.sessions[old] = fakeSession(old);

  assert.match(readLog(cfg), new RegExp(`回收 1 个过期会话基准（>${SESSION_TOKEN_TTL_DAYS} 天没更新）`));

  // 基准被清掉后，96000 直接触发（否则 96000-120000 是负数，也不会误触发；这里看的是日志里「基准 0」）
  harness.pressures[old] = 96000;
  await drain(handler(request(old), passthrough));
  assert.match(readLog(cfg), /基准 0，增长闸 40000/);
});

test('会话格没过期就留着：5 天前更新过，照样挡增长闸', async () => {
  const cfg = tempCfg();
  const store = okStore();
  const fresh = 'session-freshhhh-5555';
  const harness = fakeHarness();
  const at = Date.now() - 5 * 86400000;
  setupConsolidate(harness.ctx, store, cfg, {
    consolidateState: { lastTokens: { [fresh]: { tokens: 1200000, at } } },
  });
  const handler = harness.handlers.get('llm/stream');
  harness.sessions[fresh] = fakeSession(fresh);

  assert.doesNotMatch(readLog(cfg), /回收 .* 个过期会话基准/);
});

// ------------------------------------------------------------ B) 语义对齐压缩

test('pressure 拿不到时退回 70000 估算路径，整理照跑', async () => {
  const cfg = tempCfg({ triggerTokens: 70000 });
  const store = okStore();
  const harness = fakeHarness({ meter: false });
  const A = 'session-dddddddd-6666';
  harness.sessions[A] = fakeSession(A);
  setupConsolidate(harness.ctx, store, cfg, {});
  const handler = harness.handlers.get('llm/stream');

  // 143000 字符 ≈ 71500 估算 token，过 70000
  await drain(handler(request(A, 143000), passthrough));
  const text = readLog(cfg);
  assert.match(text, /触发整理：退回估算 /);
  assert.match(text, />= triggerTokens 70000/);
});

test('老 state 只有一个数字：第一条会话认领一次，第二个会话不继承', async () => {
  const cfg = tempCfg();
  const store = okStore();
  const A = 'session-eeeeeeee-7777';
  const B = 'session-ffffffff-8888';
  const harness = fakeHarness();
  setupConsolidate(harness.ctx, store, cfg, { consolidateState: { lastTokens: 95000 } });
  const handler = harness.handlers.get('llm/stream');
  harness.sessions[A] = fakeSession(A);
  harness.sessions[B] = fakeSession(B);

  // A 认领 95000：涨 1000 不够 growth，不跑
  harness.pressures[A] = 96000;
  assert.equal(handler(request(A), () => SENTINEL), SENTINEL, 'A 应继承老基准 95000');
  // B 没有基准，但 96000 过线 → 触发（不是继承 95000 后被 growth 挡掉）
  harness.pressures[B] = 96000;
  await drain(handler(request(B), passthrough));
});

test('normalizeCooldownState：坏结构不抛，读不出的当没有', () => {
  const legacy = normalizeCooldownState({ lastTokens: 95000 });
  assert.deepEqual(legacy.lastTokens, {});
  assert.equal(legacy.legacyLastTokens, 95000);

  const perSession = normalizeCooldownState({
    lastTokens: { 'session-a': 120000, 'session-b': 'nope', '': 5 },
    lastTokensAt: { 'session-a': 1700000000000 },
    pendingTokens: { 'session-a': 120000 },
  });
  assert.deepEqual(perSession.lastTokens, { 'session-a': 120000 });
  assert.equal(perSession.lastTokensAt['session-a'], 1700000000000);
  assert.deepEqual(perSession.pendingTokens, { 'session-a': 120000 });
  assert.equal(perSession.legacyLastTokens, 0);

  // 结构坏了：当没有，不抛
  const broken = normalizeCooldownState({ lastTokens: { 'session-a': { get tokens() { throw new Error('x'); } } } });
  assert.deepEqual(broken.lastTokens, {});
  assert.equal(normalizeCooldownState(undefined).legacyLastTokens, 0);
});

test('日志带会话归属：整理输入/完成两行都有 sessionId 前缀', async () => {
  const cfg = tempCfg();
  const store = okStore();
  const A = 'session-gggggggg-9999';
  const harness = fakeHarness();
  setupConsolidate(harness.ctx, store, cfg, {});
  const handler = harness.handlers.get('llm/stream');
  harness.sessions[A] = fakeSession(A);
  harness.pressures[A] = 95000;
  await drain(handler(request(A), passthrough));

  const text = readLog(cfg);
  assert.match(text, /\[session-gggggggg\] 整理输入 /);
  assert.match(text, /\[session-gggggggg\] 整理完成 /);
});
