/**
 * 队列账本（lib/ledger.js）的分支测试（2026-11-14，阶段 2）。
 *
 * 钉的是状态机本身，不重复测业务：
 *   1) claim：空队列不报错、有活只领并发数那么多、第二次不重复领（CAS）；
 *   2) finish：幂等，重复收尾不改状态、不报错；
 *   3) fail：两套 attempts 记账时机（领取时 / 失败时）都按各自阈值转 dead；
 *   4) reap：租约过期捞回待领；pending_ops 回收计一次 attempts（到阈值判死），实体队列领取时 +1；
 *   5) stats：按通道分开计数，互不串。
 *
 * 用真表（curate_queue / pending_ops）+ 真 SQL，不另写 DDL，保证适配器 SQL 也被覆盖。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { applySchema } from '../scripts/apply-pipeline-schema.mjs';
import { applyCurateSchema } from '../scripts/apply-curate-schema.mjs';
import { createLedger } from '../lib/ledger.js';
import { enqueue, claimChannelOps, channelStats, reapChannelOps, markOp } from '../lib/ingest-index.js';
import { enqueueEntityExtract, claimBatch, markDone, failOne, reapExpired, queueStats } from '../lib/entity-queue.js';

const ENTITY_DDL = `
CREATE TABLE entities (
  id INTEGER PRIMARY KEY, norm TEXT NOT NULL, display TEXT NOT NULL,
  etype TEXT NOT NULL DEFAULT 'unknown', aliases_note TEXT NOT NULL DEFAULT '',
  attrs TEXT NOT NULL DEFAULT '{}', first_seen TEXT NOT NULL DEFAULT '',
  last_seen TEXT NOT NULL DEFAULT '', mem_count INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'active', UNIQUE (norm));
CREATE TABLE memory_entity (
  memory_id TEXT NOT NULL, entity_id INTEGER NOT NULL, role TEXT NOT NULL DEFAULT 'about',
  ts TEXT NOT NULL DEFAULT '', status TEXT NOT NULL DEFAULT 'active',
  attrs TEXT NOT NULL DEFAULT '{}', version INTEGER NOT NULL DEFAULT 0,
  prev_memory_id TEXT NOT NULL DEFAULT '', PRIMARY KEY (memory_id, entity_id, role)) WITHOUT ROWID;
CREATE TABLE entity_surface (
  surface TEXT PRIMARY KEY, norm TEXT NOT NULL, entity_id INTEGER NOT NULL,
  source TEXT NOT NULL CHECK (source IN ('migrate','gate','coarse','extract','model','manual','text')),
  count INTEGER NOT NULL DEFAULT 0, first_seen TEXT NOT NULL DEFAULT '',
  last_seen TEXT NOT NULL DEFAULT '', flag TEXT NOT NULL DEFAULT '',
  UNIQUE (norm, surface)) WITHOUT ROWID;
CREATE INDEX idx_surface_norm ON entity_surface(norm);
CREATE TABLE entity_issue (
  memory_id TEXT NOT NULL DEFAULT '', entity_norm TEXT NOT NULL, raw_name TEXT NOT NULL DEFAULT '',
  reason TEXT NOT NULL, detail TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL,
  PRIMARY KEY (memory_id, entity_norm, reason)) WITHOUT ROWID;
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
INSERT INTO meta(key, value) VALUES ('dim', '4');
`;

const MEMORY_DDL = `CREATE TABLE memories (
  id TEXT PRIMARY KEY, text TEXT NOT NULL, tags TEXT, source TEXT, ts TEXT, type TEXT,
  entities TEXT, validFrom TEXT, supersedes TEXT, updatedAt TEXT, status TEXT, reason TEXT,
  supersededBy TEXT, validTo TEXT, confidence REAL, extra TEXT, present TEXT, rowSeq INTEGER NOT NULL
);`;

function freshDb() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-ledger-'));
  const dbPath = path.join(dir, 'memory.db');
  const db0 = new DatabaseSync(dbPath);
  db0.exec(MEMORY_DDL);
  db0.exec(ENTITY_DDL);
  db0.close();
  applySchema(dbPath, { logger: () => {} });
  applyCurateSchema(dbPath, { logger: { info() {}, warn() {} } });
  return new DatabaseSync(dbPath);
}

function seedOps(db, n, kind = 'ingest_file') {
  for (let i = 0; i < n; i += 1) enqueue(db, kind, `${kind}-${i}`, { path: `${kind}-${i}` }, { now: 1 });
}

test('claim：空队列返回空数组，不抛、不占行', () => {
  const db = freshDb();
  assert.deepEqual(claimChannelOps(db, 'ingest', 4, { now: 2 }), []);
  assert.equal(channelStats(db, 'ingest').total, 0);
  db.close();
});

test('claim：有活只领 limit 条，剩下的还在 pending，第二次不重复领', () => {
  const db = freshDb();
  seedOps(db, 10);
  const first = claimChannelOps(db, 'ingest', 3, { now: 2 });
  assert.equal(first.length, 3);
  assert.equal(channelStats(db, 'ingest').running, 3);
  assert.equal(channelStats(db, 'ingest').pending, 7);
  const second = claimChannelOps(db, 'ingest', 3, { now: 3 });
  assert.equal(second.length, 3);
  assert.ok(!first.some((a) => second.some((b) => a.id === b.id)), '同一时刻不同批不重复领');
  db.close();
});

test('finish：幂等，重复收尾不报错、状态不回退', () => {
  const db = freshDb();
  seedOps(db, 1);
  const [op] = claimChannelOps(db, 'ingest', 1, { now: 2 });
  markOp(db, op.id, 'done', null, { now: 3 });
  assert.equal(channelStats(db, 'ingest').done, 1);
  const again = markOp(db, op.id, 'done', null, { now: 4 });
  assert.equal(again.status, 'done', '重复收尾仍是 done');
  assert.equal(channelStats(db, 'ingest').done, 1, '不重复计数');
  db.close();
});

test('fail：pending_ops 失败时才加 attempts，第 5 次转 dead', () => {
  const db = freshDb();
  enqueue(db, 'merge_batch', 'fp-1', { batch: 'fp-1' }, { now: 1 });
  const seen = [];
  for (let i = 0; i < 5; i += 1) {
    const [op] = claimChannelOps(db, 'curate', 1, { kinds: ['merge_batch'], now: 2 + i });
    assert.ok(op, `第 ${i + 1} 次应当领到`);
    const after = markOp(db, op.id, 'pending', `boom-${i}`, { now: 3 + i });
    seen.push({ attempts: after.attempts, status: after.status });
    // 下一次立即到期（pending_ops 不排退避时间）
    db.prepare('UPDATE pending_ops SET next_try_at=0 WHERE id=?').run(op.id);
  }
  assert.deepEqual(seen.map((x) => x.attempts), [1, 2, 3, 4, 5], 'attempts 每次都 +1');
  assert.deepEqual(seen.map((x) => x.status), ['pending', 'pending', 'pending', 'pending', 'dead'], '第 5 次 dead');
  db.close();
});

test('fail：curate_queue 领取时就加 attempts，第 3 次转 dead', () => {
  const db = freshDb();
  enqueueEntityExtract(db, { memoryId: 'm_1', now: 1 });
  const states = [];
  for (let i = 0; i < 3; i += 1) {
    const [op] = claimBatch(db, { batch: 1, now: 2 + i, leaseMs: 60000 });
    assert.ok(op, `第 ${i + 1} 次应当领到`);
    assert.equal(op.attempts, i + 1, '领取即 +1');
    const after = failOne(db, { id: op.id, attempts: op.attempts, error: `boom-${i}`, now: 3 + i });
    states.push(after.state);
    db.prepare('UPDATE curate_queue SET next_try_at=0 WHERE id=?').run(op.id);
  }
  assert.deepEqual(states, ['ready', 'ready', 'dead'], '阈值 3：前两次回 ready，第三次 dead');
  db.close();
});

test('reap：两套表的租约过期都捞回待领；pending_ops 计一次 attempts，实体队列领取时计', () => {
  const db = freshDb();
  seedOps(db, 1);
  const [op] = claimChannelOps(db, 'ingest', 1, { now: 2, leaseMs: 1000 });
  assert.equal(op.attempts, 0);
  assert.equal(reapChannelOps(db, { now: 2 + 60000, leaseMs: 1000, channel: 'ingest' }), 1);
  assert.equal(channelStats(db, 'ingest').pending, 1);
  assert.equal(channelStats(db, 'ingest').running, 0);
  const row = db.prepare('SELECT attempts, last_error FROM pending_ops WHERE id = ?').get(op.id);
  assert.equal(Number(row.attempts), 1, 'pending_ops 的回收也算一次尝试');
  assert.equal(row.last_error, 'lease_expired');
  const [again] = claimChannelOps(db, 'ingest', 1, { now: 3 + 60000, leaseMs: 1000 });
  assert.equal(again.id, op.id, '捞回来还能再领到');
  assert.equal(again.attempts, 1, '领取带回来的 attempts 含这次过期');

  enqueueEntityExtract(db, { memoryId: 'm_1', now: 1 });
  const [c1] = claimBatch(db, { batch: 1, now: 2, leaseMs: 1000 });
  assert.equal(c1.attempts, 1);
  assert.equal(reapExpired(db, { now: 2 + 60000 }), 1);
  const [c2] = claimBatch(db, { batch: 1, now: 2 + 120000, leaseMs: 1000 });
  assert.equal(c2.id, c1.id, '捞回来还能再领到');
  assert.equal(c2.attempts, 2, 'reap 不动 attempts，只有领取时 +1');
  db.close();
});

test('stats：按通道分开计数，互不串', () => {
  const db = freshDb();
  seedOps(db, 2, 'ingest_file');
  seedOps(db, 2, 'merge_batch');
  claimChannelOps(db, 'ingest', 1, { now: 2 });
  claimChannelOps(db, 'curate', 1, { kinds: ['merge_batch'], now: 2 });
  const ingest = channelStats(db, 'ingest');
  const curate = channelStats(db, 'curate');
  assert.equal(ingest.total, 2);
  assert.equal(ingest.running, 1);
  assert.equal(ingest.pending, 1);
  assert.equal(curate.total, 2, 'curate 只算自己通道的 kind');
  assert.equal(curate.running, 1);
  assert.equal(channelStats(db, 'compact').total, 0, 'compact 派生通道不占队列表');
  db.close();
});

test('curate_queue 的 stats 字段兼容 queueStats（ready/leased/done/dead/skipped）', () => {
  const db = freshDb();
  enqueueEntityExtract(db, { memoryId: 'm_1', now: 1 });
  const [op] = claimBatch(db, { batch: 1, now: 2 });
  markDone(db, [op.id], { now: 3 });
  const st = queueStats(db);
  assert.equal(st.done, 1);
  assert.equal(st.ready, 0);
  assert.equal(st.running, 0);
  assert.equal(st.total, 1);
  db.close();
});

test('claim：表不在时返回空数组，不抛（老库没跑 schema）', () => {
  const db = new DatabaseSync(':memory:');
  assert.deepEqual(claimChannelOps(db, 'ingest', 2, { now: 1 }), []);
  assert.equal(channelStats(db, 'ingest').total, 0);
  db.close();
});

test('release：attempts 不重复加，只把 running 放回待领（increment:false）', () => {
  const db = freshDb();
  enqueue(db, 'merge_batch', 'fp-rel', { batch: 'fp-rel' }, { now: 1 });
  const [op] = claimChannelOps(db, 'curate', 1, { kinds: ['merge_batch'], now: 2 });
  const before = Number(db.prepare('SELECT attempts FROM pending_ops WHERE id=?').get(op.id).attempts);
  markOp(db, op.id, 'pending', 'retry-later', { now: 3, increment: false });
  const after = db.prepare('SELECT status, attempts FROM pending_ops WHERE id=?').get(op.id);
  assert.equal(after.status, 'pending');
  assert.equal(Number(after.attempts), before, 'attempts 已在别处加过，这里不许再加');
  db.close();
});

test('skip：适配器没有 setSkipped 时也返回 skipped，不抛', () => {
  const ledger = createLedger({
    channel: 'fake',
    ops: {
      ready: () => true,
      select: () => [],
      markRunning: () => 0,
      mapClaimed: () => ({}),
      setDone: () => 1,
      setBack: () => 1,
      setDead: () => 1,
      reap: () => 0,
      counts: () => ({ total: 0 }),
    },
  });
  assert.equal(ledger.skip(1).state, 'skipped');
});

test('阈值是参数：maxAttempts=1 时第一次失败就 dead，不写死 5', () => {
  const rows = new Map([[1, { id: 1, attempts: 0, state: 'pending' }]]);
  let deadWrites = 0;
  const ledger = createLedger({
    channel: 'fake',
    maxAttempts: 1,
    attemptsAt: 'fail',
    ops: {
      ready: () => true,
      select: ({ limit }) => [...rows.values()].slice(0, limit),
      markRunning: (row) => { row.state = 'running'; return 1; },
      mapClaimed: (row, attempts) => ({ ...row, attempts }),
      setDone: () => 1,
      setBack: () => 1,
      setDead: () => { deadWrites += 1; return 1; },
      reap: () => 0,
      counts: () => ({ total: 1 }),
    },
  });
  const [claimed] = ledger.claim(1);
  assert.equal(claimed.attempts, 0, "attemptsAt='fail'：领取不加");
  assert.equal(ledger.fail(1, { attempts: 0, error: new Error('boom') }).state, 'dead');
  assert.equal(deadWrites, 1, '到阈值走 setDead，不是 setBack');
  assert.equal(ledger.finish(1, { error: 'done-with-note' }), 1, 'finish 带错误摘要也能收尾');
});

test('skip：适配器有 setSkipped 时走它（正文已不存在这类不占重试额度）', () => {
  let skipped = 0;
  const ledger = createLedger({
    channel: 'fake',
    ops: {
      ready: () => true,
      select: () => [],
      markRunning: () => 0,
      mapClaimed: () => ({}),
      setDone: () => 1,
      setBack: () => 1,
      setDead: () => 1,
      setSkipped: () => { skipped += 1; return 1; },
      reap: () => 0,
      counts: () => ({ total: 0 }),
    },
  });
  assert.equal(ledger.skip(7, { error: 'memory_missing' }).state, 'skipped');
  assert.equal(skipped, 1, '真走了适配器的 setSkipped');
});

test('fail：不传 backoffMs 时立刻到期（next_try_at=now），不排退避', () => {
  let seenNext = -1;
  const ledger = createLedger({
    channel: 'fake',
    maxAttempts: 5,
    attemptsAt: 'fail',
    ops: {
      ready: () => true,
      select: () => [],
      markRunning: () => 0,
      mapClaimed: () => ({}),
      setDone: () => 1,
      setBack: (id, { nextTryAt }) => { seenNext = nextTryAt; return 1; },
      setDead: () => 1,
      reap: () => 0,
      counts: () => ({ total: 0 }),
    },
  });
  const out = ledger.fail(1, { attempts: 0, error: 'x', now: 1000 });
  assert.equal(out.state, 'pending');
  assert.equal(seenNext, 1000, '没有退避阶梯：now 到期');
});

test('边界：error=null 与 attempts 传负数/NaN 都不炸，按 0 处理', () => {
  const seen = [];
  const ledger = createLedger({
    channel: 'fake',
    maxAttempts: 3,
    attemptsAt: 'fail',
    ops: {
      ready: () => true,
      select: () => [],
      markRunning: () => 0,
      mapClaimed: () => ({}),
      setDone: (id, { message }) => { seen.push(['done', message]); return 1; },
      setBack: (id, { message, attempts }) => { seen.push(['back', message, attempts]); return 1; },
      setDead: (id, { attempts }) => { seen.push(['dead', attempts]); return 1; },
      reap: () => 0,
      counts: () => ({ total: 0 }),
    },
  });
  assert.equal(ledger.finish(1, {}).state ?? 'done', 'done');
  assert.deepEqual(seen.at(-1), ['done', null], 'error=null 写成 null，不爆');
  const back = ledger.fail(2, { attempts: -5, error: null });
  assert.equal(back.state, 'pending');
  assert.equal(back.attempts, 1, '负 attempts 当 0，失败后算 1');
  assert.deepEqual(seen.at(-1), ['back', null, 1]);
  const dead = ledger.fail(3, { attempts: Number.NaN });
  assert.equal(dead.attempts, 1, 'NaN 当 0');
  assert.equal(ledger.fail(4, { attempts: 2 }).state, 'dead', '2+1=3 到阈值');
});

test('边界：claim(0) 直接空；select/reap/counts 返回假值时兜底不炸', () => {
  const ledger = createLedger({
    channel: 'fake',
    ops: {
      ready: () => true,
      select: () => null,
      markRunning: () => undefined,
      mapClaimed: () => ({}),
      setDone: () => 1,
      setBack: () => 1,
      setDead: () => 1,
      reap: () => null,
      counts: () => null,
    },
  });
  assert.deepEqual(ledger.claim(0), [], 'limit 0 不查库');
  assert.deepEqual(ledger.claim(3), [], 'select 返回 null 按空队列');
  assert.equal(ledger.reap(), 0);
  assert.deepEqual(ledger.stats(), { total: 0 });
});
