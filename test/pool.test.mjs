/**
 * 通道池的单元测试（2026-11-14 维护者拍板：整理链 = 队列 + worker 池）。
 * 钉六件事：
 *   1) 领活粒度：一次只领「当前并发数」那么多，不整队标 running；
 *   2) 并发上限：批内在途的 prepare 不超过并发数；
 *   3) 串行落地：commit 同时最多 1 个，写闸 peak 也是 1；
 *   4) 空队列：领不到就收摊，不忙等（claim 只被问一次就退）；
 *   5) 租约回收：每轮领活前调一次 reap；
 *   6) 失败收敛：prepare/commit 抛错都交给 release，dead 计数不丢。
 */
import assert from 'node:assert/strict';
import test from 'node:test';
import { createChannelPool, createWriteLock, runListPool, POOL_CONCURRENCY_CAP } from '../lib/pool.js';

function queueOf(n) {
  const rows = Array.from({ length: n }, (_, i) => ({ id: i + 1 }));
  let cursor = 0;
  const claimed = [];
  return {
    claimed,
    claim(limit) {
      const out = rows.slice(cursor, cursor + limit);
      cursor += out.length;
      claimed.push(out.length);
      return out;
    },
    left: () => rows.length - cursor,
  };
}

test('领活粒度：一次只领并发数那么多，不整队标 running', async () => {
  const q = queueOf(10);
  const seen = [];
  const pool = createChannelPool({
    channel: 't',
    concurrency: 3,
    claim: (n) => q.claim(n),
    prepare: async (row) => row,
    commit: async (row) => { seen.push(row.id); },
    release: () => {},
    remaining: () => q.left(),
  });
  const stats = await pool.drain();
  assert.deepEqual(q.claimed.filter((n) => n > 0), [3, 3, 3, 1], '每批最多领并发数（3）');
  assert.deepEqual(seen, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], '按领取顺序落地');
  assert.equal(stats.claimed, 10);
  assert.equal(stats.landed, 10);
  assert.ok(stats.peakRunning <= 3, `在飞条数不许超过并发数，实测 ${stats.peakRunning}`);
});

test('并发上限：批内 prepare 同时在跑的不超过并发数', async () => {
  const q = queueOf(9);
  let live = 0;
  let peak = 0;
  const pool = createChannelPool({
    channel: 't',
    concurrency: 3,
    claim: (n) => q.claim(n),
    prepare: async (row) => {
      live += 1;
      if (live > peak) peak = live;
      await new Promise((r) => setTimeout(r, 5));
      live -= 1;
      return row;
    },
    commit: () => {},
    release: () => {},
  });
  await pool.drain();
  assert.ok(peak <= 3, `prepare 并发峰值 ${peak} 超过 3`);
  assert.ok(peak >= 2, `并发没生效（峰值 ${peak}），池退化成串行了`);
});

test('串行落地：commit 同时最多 1 个，写闸峰值也是 1', async () => {
  const q = queueOf(6);
  const writeLock = createWriteLock();
  let live = 0;
  let peak = 0;
  const pool = createChannelPool({
    channel: 't',
    concurrency: 4,
    writeLock,
    claim: (n) => q.claim(n),
    prepare: async (row) => row,
    commit: async () => {
      live += 1;
      if (live > peak) peak = live;
      await new Promise((r) => setTimeout(r, 2));
      live -= 1;
    },
    release: () => {},
  });
  await pool.drain();
  assert.equal(peak, 1, '写库同一时刻只许一条线');
  assert.equal(writeLock.stats().peak, 1, '写闸自己也得记住峰值 1');
});

test('空队列：领不到就收摊，不忙等', async () => {
  let calls = 0;
  const pool = createChannelPool({
    channel: 't',
    concurrency: 8,
    claim: () => { calls += 1; return []; },
    prepare: () => {},
    commit: () => {},
    release: () => {},
    idleMs: 5000,
  });
  const t0 = Date.now();
  const stats = await pool.drain();
  assert.equal(calls, 1, '空队列只问一次，不许空转');
  assert.equal(stats.drained, true);
  assert.ok(Date.now() - t0 < 500, '空队列要立刻收摊，不许等 idleMs');
});

test('空队列：stopWhenEmpty=false 时等信号，notify 后立刻醒', async () => {
  let calls = 0;
  let fed = false;
  const pool = createChannelPool({
    channel: 't',
    concurrency: 2,
    stopWhenEmpty: false,
    idleMs: 60000,
    claim: () => {
      calls += 1;
      if (!fed) return [];
      fed = false;
      return [{ id: calls }];
    },
    prepare: async (row) => row,
    commit: () => {},
    release: () => {},
  });
  pool.start();
  await new Promise((r) => setTimeout(r, 20));
  assert.equal(calls, 1, '领空后挂起等信号');
  fed = true;
  pool.notify();
  await new Promise((r) => setTimeout(r, 30));
  assert.ok(calls >= 2, `notify 后应当立刻再领一次，实测 ${calls}`);
  assert.equal(pool.stats().landed, 1);
  await pool.stop();
});

test('租约回收：每轮领活前调一次 reap', async () => {
  const q = queueOf(3);
  let reaps = 0;
  const pool = createChannelPool({
    channel: 't',
    concurrency: 2,
    reap: () => { reaps += 1; },
    claim: (n) => q.claim(n),
    prepare: async (row) => row,
    commit: () => {},
    release: () => {},
  });
  await pool.drain();
  // 两批 + 最后空领一次 = 3 次
  assert.equal(reaps, 3, `reap 次数 ${reaps}`);
});

test('失败收敛：prepare/commit 抛错交给 release，dead 计数不丢', async () => {
  const q = queueOf(4);
  const released = [];
  const pool = createChannelPool({
    channel: 't',
    concurrency: 2,
    claim: (n) => q.claim(n),
    prepare: async (row) => {
      if (row.id === 1) throw new Error('prepare boom');
      return row;
    },
    commit: async (row) => {
      if (row.id === 2) throw new Error('commit boom');
      return { ok: true };
    },
    release: (row, error) => {
      released.push({ id: row.id, message: String(error?.message || error) });
      return { dead: row.id === 3 };
    },
  });
  const stats = await pool.drain();
  assert.deepEqual(released.map((r) => r.id), [1, 2], '两条失败都要进 release');
  assert.match(released[0].message, /prepare boom/);
  assert.match(released[1].message, /commit boom/);
  assert.equal(stats.landed, 2, '另外两条照常落地');
  assert.equal(stats.failed, 2);
  assert.equal(stats.dead, 0, 'release 没报 dead 就不许乱记');
});

test('并发上界：concurrency 配到 999 也夹到硬顶', async () => {
  const q = queueOf(200);
  let live = 0;
  let peak = 0;
  const pool = createChannelPool({
    channel: 't',
    concurrency: 999,
    claim: (n) => q.claim(n),
    prepare: async (row) => {
      live += 1;
      if (live > peak) peak = live;
      await new Promise((r) => setTimeout(r, 1));
      live -= 1;
      return row;
    },
    commit: () => {},
    release: () => {},
  });
  await pool.drain();
  assert.ok(peak <= POOL_CONCURRENCY_CAP, `峰值 ${peak} 超过硬顶 ${POOL_CONCURRENCY_CAP}`);
});

test('runListPool：并发 1 时严格 prepare→commit 交替（原串行节奏）', async () => {
  const events = [];
  const results = await runListPool({
    items: [1, 2, 3],
    concurrency: 1,
    prepare: async (item) => { events.push(`p${item}`); return item; },
    commit: async (item) => { events.push(`c${item}`); return item; },
  });
  assert.deepEqual(events, ['p1', 'c1', 'p2', 'c2', 'p3', 'c3']);
  assert.deepEqual(results.map((r) => r.value), [1, 2, 3]);
});
