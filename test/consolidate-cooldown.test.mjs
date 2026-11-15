import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { normalizeCooldownState, setupConsolidate } from '../lib/consolidate.js';

/**
 * 2026-11-14：整理轮的 25 分钟冷却删了。
 * 这组用例守住「不再按时间挡」：失败一轮之后紧接着的 tick 照样能起；
 * 防重复跑只剩 inFlight（并发）和 triggerTokens / growthTokens（量够不够）。
 * lastAttemptAt / lastSuccessAt 仍写进 state，只作排障。
 */

function tempCfg(extra = {}) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'consolidate-cooldown-'));
  return {
    consolidateEnabled: true,
    consolidateOnlyMain: false, // 这组用例只管触发判定，不掺子会话类判据
    triggerTokens: 1000,
    growthTokens: 0,
    consolidateLogPath: path.join(dir, 'consolidate.log'),
    ...extra,
  };
}

/** estimateTokens 按字符数/2 算，2000 字符刚好 1000 token，够触发线。 */
function bigMessages() {
  return [{ role: 'user', content: 'x'.repeat(2000) }];
}

const SENTINEL = { untouched: true };
const passthrough = async function* () {};

/** 抓 setupConsolidate 挂上的 llm/stream 处理器。 */
/**
 * 2026-11-14：无 sessionId 请求已一律不触发整理，这组请求统一带会话。
 * meter=true 走实测压力（失败后再触发那条）；meter=false 走退回估算（量不够那条），
 * 两条路径都在触发判定最前面，不受新的估算节流干扰（各自只请求一次）。
 */
const SESSION = 'session-cooldown-0001';

function fakeSession(id) {
  return {
    id,
    header: undefined,
    requestContext: () => ({ provider: 'magpie', model: 'group/ds', contextWindow: 1048576 }),
    requestHeader: () => ({ config: { maxTokens: 8000 } }),
  };
}

function setup(cfg, store, { meter = true } = {}) {
  const handlers = new Map();
  const sessions = meter ? { [SESSION]: fakeSession(SESSION) } : {};
  const ctx = {
    get: (name) => {
      if (name === 'sessions') return { get: (id) => sessions[id] };
      if (!meter) return undefined;
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
  };
  setupConsolidate(ctx, store, cfg, {});
  return handlers.get('llm/stream');
}

/** shouldRun 放行时返回 async generator；被挡时原样返回 next() 的结果。 */
function triggered(result) {
  return typeof result?.[Symbol.asyncIterator] === 'function';
}

/** 把一轮真跑完（wrapped 是 async generator，不消费就不会执行 consolidate）。 */
async function drain(result) {
  assert.equal(triggered(result), true, '期望这一 tick 放行');
  for await (const _ of result) {
    /* 这轮的内容不重要，只看它有没有真的跑起来 */
  }
  await result.pending;
}

test('失败后没有冷却：紧接着的 tick 照样能起下一轮', async () => {
  const cfg = tempCfg();
  const store = { listTexts: () => { throw new Error('boom'); } }; // 让这轮在起点之后立刻失败
  const handler = setup(cfg, store);

  await drain(handler({ sessionId: SESSION, messages: bigMessages() }, passthrough));
  const logText = fs.readFileSync(cfg.consolidateLogPath, 'utf8');
  assert.match(logText, /整理失败（已忽略，累计 1 次）：boom/, '这一轮应该真的失败过');

  // 2026-11-14：冷却删了，失败后 0 秒也允许再起；挡重复跑的是 inFlight（上面已释放）
  assert.equal(triggered(handler({ sessionId: SESSION, messages: bigMessages() }, passthrough)), true, '失败后不该被时间挡住');
});

test('量不够才不跑：没到 triggerTokens 就挡掉（这不是时间闸）', async () => {
  const cfg = tempCfg({ triggerTokens: 5000 });
  const store = { listTexts: () => [] };
  const handler = setup(cfg, store, { meter: false }); // 无实测压力 → 1000 token 估算走 triggerTokens 闸
  assert.equal(handler({ sessionId: SESSION, messages: bigMessages() }, () => SENTINEL), SENTINEL, '1000 token < 5000 不该触发');
});

test('老 state 只有 lastRunAt 时按 lastAttemptAt 处理', () => {
  const legacy = normalizeCooldownState({ lastRunAt: 1_700_000_000_000, runs: 3 });
  assert.equal(legacy.lastAttemptAt, 1_700_000_000_000);
  assert.equal(legacy.lastSuccessAt, 0);
  assert.equal(legacy.runs, 3);
  assert.equal(normalizeCooldownState(undefined).lastAttemptAt, 0);
});
