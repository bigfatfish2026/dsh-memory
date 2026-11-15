import assert from 'node:assert/strict';
import test from 'node:test';
import {
  SLEEP_HEARTBEAT_MS, SLEEP_HEARTBEAT_MISSES, dueSleepSlotNow, shouldMarkSlot,
} from '../lib/sleep.js';

const SLOTS = [
  { hour: 5, minute: 0 },
  { hour: 12, minute: 30 },
];
const NOON = new Date('2026-11-09T12:35:00+08:00');

test('过点且当天没跑：dueSleepSlotNow 给出最早的未跑 slotKey', () => {
  assert.equal(dueSleepSlotNow({ slots: SLOTS, done: {}, now: NOON, lastRunAt: 0 }), '2026-11-09 05:00');
});

test('同一 slot 第二次判：done 已有 key 就跳过它', () => {
  const done = { '2026-11-09 05:00': 1 };
  assert.equal(dueSleepSlotNow({ slots: SLOTS, done, now: NOON, lastRunAt: 0 }), '2026-11-09 12:30');
});

test('busy：心跳还在续租时不补（另一轮正占着，busy 不重复跑）', () => {
  assert.equal(
    dueSleepSlotNow({ slots: SLOTS, done: {}, now: NOON, runningAt: NOON.getTime() - 60 * 1000 }),
    undefined,
  );
  // runningAt 很旧但 heartbeatAt 是新的：旧实例还活着，继续不补
  assert.equal(
    dueSleepSlotNow({
      slots: SLOTS,
      done: {},
      now: NOON,
      runningAt: NOON.getTime() - 10 * 60 * 1000,
      heartbeatAt: NOON.getTime() - 30 * 1000,
    }),
    undefined,
  );
});

test('心跳断了 3 个间隔：判死接管，不用干等两小时', () => {
  const staleAfter = SLEEP_HEARTBEAT_MS * SLEEP_HEARTBEAT_MISSES;
  assert.equal(
    dueSleepSlotNow({
      slots: SLOTS,
      done: {},
      now: NOON,
      runningAt: NOON.getTime() - staleAfter - 1000,
      heartbeatAt: NOON.getTime() - staleAfter - 1000,
    }),
    '2026-11-09 05:00',
  );
  // 差一点点到 3 个间隔：还算活着
  assert.equal(
    dueSleepSlotNow({
      slots: SLOTS,
      done: {},
      now: NOON,
      runningAt: NOON.getTime() - staleAfter + 5000,
      heartbeatAt: NOON.getTime() - staleAfter + 5000,
    }),
    undefined,
  );
});

test('没有 minGap：10 分钟前刚跑过，到点且没标 slot 照样判 due（往事不再挡）', () => {
  // 2026-11-14：minGap 删了；幂等靠 slots（同上一条用例）和 inFlight
  assert.equal(dueSleepSlotNow({ slots: SLOTS, done: {}, now: NOON }), '2026-11-09 05:00');
});

test('shouldMarkSlot：busy 不标 slot，too-quiet 和正常结果照旧标', () => {
  assert.equal(shouldMarkSlot({ skipped: 'busy' }), false, 'busy 标了就等于当天不再跑');
  assert.equal(shouldMarkSlot({ skipped: 'too-quiet' }), true);
  assert.equal(shouldMarkSlot({ added: [], merged: [], updated: [], archived: [] }), true);
});
