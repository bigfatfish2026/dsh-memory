import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { isWorkerSession, setupConsolidate } from '../lib/consolidate.js';

/** 主会话头：qqbot 那边主会话是 session-<uuid>，delegationDepth 0、没有 origin。 */
const MAIN_HEADER = { id: 'session-6b203898-f31e-470e-a44d-0497744b697e', delegationDepth: 0, isSeeded: false };
/** 子会话头：实测 subagent 会话的 session 首行。 */
const WORKER_HEADER = {
  id: '1753f90b-2f6d-471a-a807-7419ae3634de',
  origin: 'subagent',
  delegationDepth: 1,
  parentSession: 'session-6b203898-f31e-470e-a44d-0497744b697e',
  isSeeded: false,
};

function tempCfg(extra = {}) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'consolidate-gate-'));
  return {
    consolidateEnabled: true,
    consolidateOnlyMain: true,
    triggerTokens: 1000,
    growthTokens: 0,
    cooldownMinutes: 0,
    consolidateLogPath: path.join(dir, 'consolidate.log'),
    ...extra,
  };
}

/** 造一个够过闸的 messages：estimateTokens 是字符数/2，这里 2000 字符 → 1000 token。 */
function bigMessages() {
  return [{ role: 'user', content: 'x'.repeat(2000) }];
}

/** 抓 setupConsolidate 挂上的 llm/stream 处理器和注册的工具。 */
function setup(cfg, sessions = {}) {
  const handlers = new Map();
  const tools = [];
  const ctx = {
    get: (name) => (name === 'sessions' ? { get: (id) => sessions[id] } : undefined),
    on: (event, handler) => handlers.set(event, handler),
    tools: { register: (tool) => tools.push(tool) },
  };
  setupConsolidate(ctx, {}, cfg, {});
  return { handler: handlers.get('llm/stream'), tools };
}

const SENTINEL = { untouched: true };
const next = () => SENTINEL;

/** shouldRun 放行时返回的是 async generator，拦下时原样返回 next() 的结果。 */
function triggered(result) {
  return typeof result?.[Symbol.asyncIterator] === 'function';
}

test('判据：子会话头认出来，主会话头不算', () => {
  assert.equal(isWorkerSession({ header: WORKER_HEADER, sessionId: WORKER_HEADER.id }), true);
  assert.equal(isWorkerSession({ header: MAIN_HEADER, sessionId: MAIN_HEADER.id }), false);
});

test('判据：origin / delegationDepth / parentSession 任一命中就算子会话', () => {
  assert.equal(isWorkerSession({ header: { origin: 'subagent' } }), true);
  assert.equal(isWorkerSession({ header: { delegationDepth: 1 } }), true);
  assert.equal(isWorkerSession({ header: { parentSession: 'session-x', isSeeded: false } }), true);
  // 普通 fork 也留 parentSession，但它是 isSeeded，不是子会话（dsh-session types.d.ts:70-76）
  assert.equal(isWorkerSession({ header: { parentSession: 'session-x', isSeeded: true } }), false);
  assert.equal(isWorkerSession({ header: { delegationDepth: 0 } }), false);
});

test('判据：拿不到会话头时退回 id 前缀（主会话 session-）', () => {
  assert.equal(isWorkerSession({ sessionId: '1753f90b-2f6d-471a-a807-7419ae3634de' }), true);
  assert.equal(isWorkerSession({ sessionId: 'session-6b203898-f31e-470e-a44d-0497744b697e' }), false);
  assert.equal(isWorkerSession({}), false);
});

test('自动触发：主会话照旧跑，子会话被挡在 shouldRun 外', () => {
  const cfg = tempCfg();
  const { handler } = setup(cfg, {
    [MAIN_HEADER.id]: { header: MAIN_HEADER },
    [WORKER_HEADER.id]: { header: WORKER_HEADER },
  });

  assert.equal(triggered(handler({ sessionId: MAIN_HEADER.id, messages: bigMessages() }, next)), true);
  assert.equal(handler({ sessionId: WORKER_HEADER.id, messages: bigMessages() }, next), SENTINEL);
});

test('自动触发：会话对象拿不到时靠 id 前缀挡子会话', () => {
  const { handler } = setup(tempCfg(), {}); // sessions.get 一律 undefined
  assert.equal(triggered(handler({ sessionId: MAIN_HEADER.id, messages: bigMessages() }, next)), true);
  assert.equal(handler({ sessionId: WORKER_HEADER.id, messages: bigMessages() }, next), SENTINEL);
});

test('开关关掉（consolidateOnlyMain=false）恢复旧行为：子会话也跑', () => {
  const cfg = tempCfg({ consolidateOnlyMain: false });
  const { handler } = setup(cfg, { [WORKER_HEADER.id]: { header: WORKER_HEADER } });
  assert.equal(triggered(handler({ sessionId: WORKER_HEADER.id, messages: bigMessages() }, next)), true);
});

test('不带 sessionId 的调用：2026-11-14 起一律不触发整理，只记一行日志', () => {
  const cfg = tempCfg();
  const { handler } = setup(cfg, {});
  // 以前这条靠退回估算触发；现在无会话请求没有真实上下文，估出来的 40 万是假的，
  // 一律挡在 shouldRun，子会话类判据都不用看了。
  assert.equal(handler({ messages: bigMessages() }, () => SENTINEL), SENTINEL);
  assert.match(fs.readFileSync(cfg.consolidateLogPath, 'utf8'), /跳过整理：无 sessionId 请求/);
});
