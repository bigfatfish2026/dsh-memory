/**
 * CooldownState（2026-11-14，阶段 4）。
 *
 * 最要紧的一条：按 sessionId 分桶——一个会话的请求/基线绝不动另一个会话的。
 * 上次「群聊基线被 c2c 请求打回零」就是分桶搞错的后果，这里专门锁住。
 * 外加：冷启动、字段类型错、老结构认领、TTL 回收、30 分钟节流时间戳唯一。
 */
import assert from 'node:assert/strict';
import test from 'node:test';
import {
  createCooldownState, normalizeCooldownState, ESTIMATE_TRIGGER_MIN_INTERVAL_MS,
} from '../lib/cooldown-state.js';

test('分桶隔离：写 c2c 会话的基线/待定，群聊会话一格都不动', () => {
  const st = createCooldownState({ raw: { lastTokens: { group: 10000 }, lastTokensAt: { group: 1000 } } });
  assert.equal(st.baselineOf('group'), 10000);
  st.setBaseline('c2c', 3000);
  st.setPending('c2c', 3200);
  assert.equal(st.baselineOf('c2c'), 3000);
  assert.equal(st.baselineOf('group'), 10000, '群聊基线不受 c2c 影响');
  st.clearPending('c2c');
  assert.equal(st.pendingOf('group'), 0);
  st.setBaseline('group', 0);
  assert.equal(st.baselineOf('group'), 0, '只清自己那格');
  assert.equal(st.baselineOf('c2c'), 3000);
});

test('冷启动与 id 归一：空 raw 不炸，undefined 会话进 (no-session)', () => {
  const st = createCooldownState({});
  assert.equal(st.baselineOf('x'), 0);
  assert.equal(st.keyOf(undefined), '(no-session)');
  assert.equal(st.keyOf(''), '(no-session)');
  assert.equal(st.keyOf('abc'), 'abc');
});

test('字段类型错容错：字符串/负数/NaN 一律当没有，不抛', () => {
  const bad = normalizeCooldownState({
    lastTokens: { a: '500', b: '不是数字', c: -3 },
    lastTokensAt: { a: 'oops' },
    pendingTokens: { a: 'x', b: 1200 },
    estimateTriggerAt: 'abc',
  });
  assert.equal(bad.lastTokens.a, 500, '数字字符串认');
  assert.equal('b' in bad.lastTokens, false);
  assert.equal('c' in bad.lastTokens, false);
  assert.equal(bad.lastTokensAt.a, 0, '时间戳坏了记 0，交给 prune 补现在');
  assert.deepEqual(bad.pendingTokens, { b: 1200 });
  assert.equal(bad.estimateTriggerAt, 0);
  assert.equal(bad.estimateDone, true, '缺字段默认已收尾');
});

test('老结构：单个数字 lastTokens 只被第一个会话认领一次', () => {
  const st = createCooldownState({ raw: { lastTokens: 4000 } });
  st.adoptLegacyBaseline('first');
  assert.equal(st.baselineOf('first'), 4000);
  st.adoptLegacyBaseline('second');
  assert.equal(st.baselineOf('second'), 0, '第二个会话不许再继承同一份');
  assert.equal(st.get().legacyLastTokens, 0);
});

test('TTL 回收：超 7 天删格；没时间戳补现在不立刻删', () => {
  const now = 1_000_000_000_000;
  const day = 86400000;
  const st = createCooldownState({
    now: () => now,
    raw: {
      lastTokens: { old: 1, fresh: 2, noTs: 3 },
      lastTokensAt: { old: now - 8 * day, fresh: now - day },
      pendingTokens: { old: 9 },
    },
  });
  const dropped = st.prune();
  assert.equal(dropped, 1);
  assert.equal(st.baselineOf('old'), 0, '过期格删掉');
  assert.equal(st.baselineOf('fresh'), 2);
  assert.equal(st.get().lastTokensAt.noTs, now, '没时间戳的补现在');
  assert.equal(st.get().lastTokensAt.fresh, now - day, '已有时间戳不动');
});

test('30 分钟节流：时间戳只有一个来源，running/窗口内/放行三种原因', () => {
  const now = 1_000_000_000_000;
  const st = createCooldownState({ now: () => now });
  assert.equal(st.estimateBlockReason(now), '', '从没触发过就放行');
  st.markEstimate(now);
  assert.equal(st.estimateBlockReason(now + 1000), 'running', '上一轮还没跑完');
  st.markEstimateDone();
  const blocked = st.estimateBlockReason(now + 1000);
  assert.equal(blocked.seconds, 1, '窗口内触发过，报秒数');
  assert.equal(st.estimateBlockReason(now + ESTIMATE_TRIGGER_MIN_INTERVAL_MS + 1), '', '过窗口放行');
});

test('save：落盘回调抛错也只当没写，状态本身照旧生效', () => {
  let calls = 0;
  const st = createCooldownState({
    persist: () => { calls += 1; throw new Error('disk full'); },
  });
  st.setBaseline('a', 10);
  assert.doesNotThrow(() => st.save('x'));
  assert.equal(calls, 1);
  assert.equal(st.baselineOf('a'), 10);
});
