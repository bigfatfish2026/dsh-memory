// 2026-11-14：启动补判不能把「上一实例 13 秒前写的锁」当成「另一轮在跑」。
// 12:30 那轮就是重启后看到新鲜心跳跳过，等 180s 锁过期时叫醒源已经没了，整天没补。
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import {
  SLEEP_HEARTBEAT_MS, SLEEP_HEARTBEAT_MISSES,
  dueSleepSlotNow, sameSleepOwner, sleepOwnerOf,
} from '../lib/sleep.js';

const SLOTS = [
  { hour: 5, minute: 0 },
  { hour: 12, minute: 30 },
];
// 12:31:10 重启、12:31:16 新进程补判：旧锁只写了 13 秒，在 180s 窗口内。
const NOW = new Date('2026-11-14T12:31:16+08:00');
const OLD_OWNER = 'aa11bb22:3123:987654';
const MINE = 'aa11bb22:4001:999888';
// 05:00 那轮真跑完了，12:30 还空着——正是 12:30 丢轮现场。
const DONE_0500 = { '2026-11-14 05:00': 1794605637452 };
const FRESH_LOCK = { runningAt: NOW.getTime() - 13 * 1000, heartbeatAt: NOW.getTime() - 13 * 1000 };

test('owner 是当前进程：自己的轮还在跑，跳过', () => {
  assert.equal(
    dueSleepSlotNow({
      slots: SLOTS, done: DONE_0500, now: NOW,
      ...FRESH_LOCK, owner: MINE, currentOwner: MINE,
    }),
    undefined,
  );
  // 就算心跳过期也不抢自己的轮：slot 没落标记只可能是这一轮还没跑完
  assert.equal(
    dueSleepSlotNow({
      slots: SLOTS, done: DONE_0500, now: NOW,
      runningAt: NOW.getTime() - 60 * 60 * 1000,
      heartbeatAt: NOW.getTime() - 60 * 60 * 1000,
      owner: MINE, currentOwner: MINE,
    }),
    undefined,
  );
});

test('owner 是别的进程：不等 180s TTL，当场接管并补跑 12:30', () => {
  // 锁只写了 13 秒，按老逻辑（180s 心跳判活）这会被判「在跑」而跳过
  assert.equal(
    dueSleepSlotNow({
      slots: SLOTS, done: DONE_0500, now: NOW,
      ...FRESH_LOCK, owner: OLD_OWNER, currentOwner: MINE,
    }),
    '2026-11-14 12:30',
  );
  // bootId 变了（重启）也算别人：owner 字符串整体不同
  assert.equal(
    dueSleepSlotNow({
      slots: SLOTS, done: DONE_0500, now: NOW,
      ...FRESH_LOCK, owner: 'ff00ff00:3123:987654', currentOwner: MINE,
    }),
    '2026-11-14 12:30',
  );
  // 调用方没给 currentOwner（拿不到自身身份）也按「不是自己」接管
  assert.equal(
    dueSleepSlotNow({
      slots: SLOTS, done: DONE_0500, now: NOW,
      ...FRESH_LOCK, owner: OLD_OWNER,
    }),
    '2026-11-14 12:30',
  );
});

test('owner 缺失（老 state）：退回 180s 心跳判活，两条分支照旧', () => {
  const staleAfter = SLEEP_HEARTBEAT_MS * SLEEP_HEARTBEAT_MISSES;
  // 新鲜：判活着，跳过
  assert.equal(
    dueSleepSlotNow({ slots: SLOTS, done: DONE_0500, now: NOW, ...FRESH_LOCK }),
    undefined,
  );
  // 过期：判死，接管补跑
  assert.equal(
    dueSleepSlotNow({
      slots: SLOTS, done: DONE_0500, now: NOW,
      runningAt: NOW.getTime() - staleAfter - 1000,
      heartbeatAt: NOW.getTime() - staleAfter - 1000,
    }),
    '2026-11-14 12:30',
  );
});

test('slot 已落标记：不管 owner 是谁都不补（防重复跑）', () => {
  const done = { ...DONE_0500, '2026-11-14 12:30': 1794611111000 };
  assert.equal(
    dueSleepSlotNow({
      slots: SLOTS, done, now: NOW,
      ...FRESH_LOCK, owner: OLD_OWNER, currentOwner: MINE,
    }),
    undefined,
  );
  assert.equal(
    dueSleepSlotNow({ slots: SLOTS, done, now: NOW, ...FRESH_LOCK }),
    undefined,
  );
  assert.equal(
    dueSleepSlotNow({ slots: SLOTS, done, now: NOW, owner: MINE, currentOwner: MINE }),
    undefined,
  );
});

test('owner 生成：带 pid 且同一进程稳定，空的 owner 互不相等', () => {
  const a = sleepOwnerOf();
  const b = sleepOwnerOf();
  assert.equal(a, b, '同一进程两次取到的 owner 必须一样');
  assert.ok(a.includes(String(process.pid)), `owner 里要有 pid：${a}`);
  assert.equal(sameSleepOwner('', ''), false, '空 owner 不算同一个实例');
  assert.equal(sameSleepOwner(MINE, ''), false);
  assert.equal(sameSleepOwner(MINE, MINE), true);
  assert.equal(sameSleepOwner(MINE, OLD_OWNER), false);
});

test('接管日志与锁重置写在 runDue 里（源码断言）', () => {
  const src = fs.readFileSync(path.join(import.meta.dirname, '..', 'lib', 'sleep', 'scheduler.js'), 'utf8');
  assert.match(src, /接管上一实例的轮 slot=\$\{key\} 旧 owner=\$\{st\.owner\}/, '接管日志文案');
  // 阶段 4：writeState(cfg, {...st, X}) 换成 stateStore.patch(X)，行为不变（patch 内部先读最新 state）
  // 注意：lib/ 是 tsdown 产物，会重排缩进/换行，断言只认「三个键同处一次 patch」，不认排版。
  assert.match(
    src,
    /stateStore\.patch\(\s*\{\s*runningAt:\s*Date\.now\(\),\s*heartbeatAt:\s*Date\.now\(\),\s*owner:\s*sleepOwner\s*,?\s*\}\s*\)/,
    '接管时把 runningAt/heartbeatAt 重置成现在并改维护者',
  );
  // 同上：产物会丢掉结尾逗号、重排换行，只认「owner 和 currentOwner 一起传给判活」。
  assert.match(src, /owner:\s*st\.owner,\s*currentOwner:\s*sleepOwner\s*,?/, 'runDue 要把 owner 传给判活');
});
