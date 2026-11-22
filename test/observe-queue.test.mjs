/**
 * observation 任务队列（src/store/observe-queue.ts + scripts/apply-observe-schema.mjs）。
 *
 * 覆盖票面六条：
 *   1) 同实体连续 enqueue 只有一行、version 递增；
 *   2) claim 按 enqueued_ts 升序、超 limit 的留在 pending；
 *   3) 租约过期重领：version+1、owner 换掉；
 *   4) complete/fail 版本或 owner 不一致返回 false 且行不变；
 *   5) fail 后 tries+1、status 回 pending；
 *   6) enqueue 把 leased 行拉回 pending（证据变更打断在跑的任务）。
 * 另加：缺表时优雅返回、外层事务里可安全调用。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { applyObserveSchema } from '../scripts/apply-observe-schema.mjs';
import {
  enqueueObserveTask,
  claimObserveTasks,
  completeObserveTask,
  failObserveTask,
} from '../lib/store/observe-queue.js';

/** 临时库 + 迁移脚本建表，和 test/queue-channel.test.mjs 的建库方式一致。 */
function freshDb() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-observe-'));
  const dbPath = path.join(dir, 'memory.db');
  new DatabaseSync(dbPath).close();
  applyObserveSchema(dbPath, { logger: () => {} });
  return new DatabaseSync(dbPath);
}

function rowOf(db, key) {
  const row = db.prepare('SELECT * FROM observe_task WHERE entity_key = ?').get(key);
  assert.ok(row, `observe_task 里应有 ${key}`);
  return row;
}

test('同实体连续 enqueue 只有一行，version 递增，enqueued_ts 不动', () => {
  const db = freshDb();
  const first = enqueueObserveTask(db, '树莓派', { entityId: 'e_rpi', reason: 'save', now: 100 });
  assert.deepEqual(first, { entityKey: '树莓派', version: 1, created: true });

  const second = enqueueObserveTask(db, '树莓派', { reason: 'text_update', now: 200 });
  assert.deepEqual(second, { entityKey: '树莓派', version: 2, created: false });

  const third = enqueueObserveTask(db, '树莓派', { reason: 'merge', now: 300 });
  assert.equal(third.version, 3);

  const count = db.prepare('SELECT count(*) AS n FROM observe_task').get().n;
  assert.equal(Number(count), 1, '同一实体只许一行');
  const row = rowOf(db, '树莓派');
  assert.equal(Number(row.version), 3);
  assert.equal(Number(row.enqueued_ts), 100, 'enqueued_ts 保持第一次登记的时间');
  assert.equal(String(row.entity_id), 'e_rpi', '空 entityId 不覆盖已有的');
  assert.equal(String(row.reason), 'merge');
  assert.equal(String(row.status), 'pending');
  db.close();
});

test('claim 按 enqueued_ts 升序领取，超出 limit 的留在 pending', () => {
  const db = freshDb();
  enqueueObserveTask(db, 'b', { now: 200 });
  enqueueObserveTask(db, 'a', { now: 100 });
  enqueueObserveTask(db, 'c', { now: 300 });

  const claimed = claimObserveTasks(db, { limit: 2, owner: 'w1', leaseMs: 1000, now: 400 });
  assert.deepEqual(claimed.map((x) => x.entityKey), ['a', 'b'], '先来先领');
  assert.deepEqual(claimed[0], { entityKey: 'a', entityId: '', version: 1, reason: '', tries: 0 });

  const a = rowOf(db, 'a');
  assert.equal(String(a.status), 'leased');
  assert.equal(String(a.lease_owner), 'w1');
  assert.equal(Number(a.lease_until), 1400);
  const c = rowOf(db, 'c');
  assert.equal(String(c.status), 'pending', '没被领的仍是 pending');
  assert.equal(String(c.lease_owner), '');
  db.close();
});

test('租约过期后能再被领：lease_until 相等时不算过期，过一点就能领', () => {
  const db = freshDb();
  enqueueObserveTask(db, '树莓派', { now: 100 });
  const [first] = claimObserveTasks(db, { limit: 1, owner: 'w1', leaseMs: 1000, now: 200 });
  assert.equal(first.version, 1);
  assert.equal(Number(rowOf(db, '树莓派').lease_until), 1200);

  assert.deepEqual(claimObserveTasks(db, { limit: 1, owner: 'w2', leaseMs: 1000, now: 1199 }), [], '租约没到期不许抢');
  const [again] = claimObserveTasks(db, { limit: 1, owner: 'w2', leaseMs: 1000, now: 1201 });
  assert.equal(again.entityKey, '树莓派');
  assert.equal(again.version, 2, '重领时 version+1，旧 claim 的版本立即失效');
  const row = rowOf(db, '树莓派');
  assert.equal(String(row.lease_owner), 'w2');
  assert.equal(Number(row.lease_until), 2201);
  assert.equal(Number(row.version), 2);
  db.close();
});

test('complete 版本不一致返回 false 且行不变，版本对上才置 done', () => {
  const db = freshDb();
  enqueueObserveTask(db, '树莓派', { now: 100 });
  const [claim] = claimObserveTasks(db, { limit: 1, owner: 'w1', now: 200 });
  assert.equal(claim.version, 1);

  // 模型跑的时候证据变了：enqueue 把 version 顶到 2，任务回 pending
  enqueueObserveTask(db, '树莓派', { reason: 'evidence_changed', now: 300 });
  const before = rowOf(db, '树莓派');
  assert.equal(completeObserveTask(db, '树莓派', 1), false, '旧版本结果作废');
  const after = rowOf(db, '树莓派');
  assert.deepEqual(after, before, '版本不符不许改任何列');
  assert.equal(String(after.status), 'pending');
  assert.equal(Number(after.version), 2);

  assert.equal(completeObserveTask(db, '树莓派', 2), false, 'pending 不是 leased，也不能直接完成');
  const [again] = claimObserveTasks(db, { limit: 1, owner: 'w2', now: 400 });
  assert.equal(again.version, 2);
  assert.equal(completeObserveTask(db, '树莓派', 2, { now: 500 }), true);
  const done = rowOf(db, '树莓派');
  assert.equal(String(done.status), 'done');
  assert.equal(String(done.lease_owner), '');
  assert.equal(Number(done.lease_until), 0);
  db.close();
});

test('fail 后 tries+1、status 回 pending、清租约；版本不符返回 false', () => {
  const db = freshDb();
  enqueueObserveTask(db, '树莓派', { now: 100 });
  const [claim] = claimObserveTasks(db, { limit: 1, owner: 'w1', leaseMs: 1000, now: 200 });

  assert.equal(failObserveTask(db, '树莓派', 999, '错版本'), false);
  const untouched = rowOf(db, '树莓派');
  assert.equal(String(untouched.status), 'leased');
  assert.equal(Number(untouched.tries), 0);

  assert.equal(failObserveTask(db, '树莓派', claim.version, '模型超时', { now: 300 }), true);
  const row = rowOf(db, '树莓派');
  assert.equal(String(row.status), 'pending');
  assert.equal(Number(row.tries), 1);
  assert.equal(String(row.last_error), '模型超时');
  assert.equal(String(row.lease_owner), '');
  assert.equal(Number(row.lease_until), 0);
  db.close();
});

test('enqueue 把 leased 行拉回 pending：证据变更打断在跑的任务', () => {
  const db = freshDb();
  enqueueObserveTask(db, '树莓派', { now: 100 });
  const [claim] = claimObserveTasks(db, { limit: 1, owner: 'w1', leaseMs: 60000, now: 200 });
  assert.equal(String(rowOf(db, '树莓派').status), 'leased');

  const bumped = enqueueObserveTask(db, '树莓派', { reason: 'evidence_changed', now: 300 });
  assert.equal(bumped.version, 2);
  const row = rowOf(db, '树莓派');
  assert.equal(String(row.status), 'pending', '在跑的任务被拉回待领');
  assert.equal(String(row.lease_owner), '');
  assert.equal(Number(row.lease_until), 0);
  assert.equal(Number(row.enqueued_ts), 100);
  assert.equal(completeObserveTask(db, '树莓派', claim.version), false, '旧结果落不进去');

  const [again] = claimObserveTasks(db, { limit: 1, owner: 'w2', now: 400 });
  assert.equal(again.version, 2);
  assert.equal(failObserveTask(db, '树莓派', again.version, 'boom'), true);
  db.close();
});

test('租约过期重领：version+1，旧版本 complete/fail 都落不进去', () => {
  const db = freshDb();
  enqueueObserveTask(db, '树莓派', { now: 100 });
  const [first] = claimObserveTasks(db, { limit: 1, owner: 'w1', leaseMs: 1000, now: 200 });
  assert.equal(first.version, 1);

  const [again] = claimObserveTasks(db, { limit: 1, owner: 'w2', leaseMs: 1000, now: 1201 });
  assert.equal(again.version, 2, '重领涨 version');

  const before = rowOf(db, '树莓派');
  assert.equal(
    completeObserveTask(db, '树莓派', first.version, { now: 1300, owner: 'w1' }),
    false,
    '旧 owner 的迟到 complete 落不进去',
  );
  assert.equal(
    failObserveTask(db, '树莓派', first.version, '旧 owner 超时', { now: 1300, owner: 'w1' }),
    false,
    '旧 owner 的 fail 也不能清掉新租约',
  );
  assert.deepEqual(rowOf(db, '树莓派'), before, '旧 owner 的两次提交都不许改任何列');
  assert.equal(completeObserveTask(db, '树莓派', again.version, { now: 1400, owner: 'w2' }), true);
  assert.equal(String(rowOf(db, '树莓派').status), 'done');
  db.close();
});

test('complete/fail 校验 owner：版本对但 owner 不符返回 false', () => {
  const db = freshDb();
  enqueueObserveTask(db, '树莓派', { now: 100 });
  const [claim] = claimObserveTasks(db, { limit: 1, owner: 'w1', leaseMs: 60000, now: 200 });

  // 绕过 claim 直接把租约换人：version 不动，只留 owner 条件能挡
  db.prepare('UPDATE observe_task SET lease_owner = ? WHERE entity_key = ?').run('w2', '树莓派');
  const before = rowOf(db, '树莓派');
  assert.equal(
    completeObserveTask(db, '树莓派', claim.version, { now: 300, owner: 'w1' }),
    false,
    'owner 不符不许置 done',
  );
  assert.equal(
    failObserveTask(db, '树莓派', claim.version, 'x', { now: 300, owner: 'w1' }),
    false,
    'owner 不符不许回 pending',
  );
  assert.deepEqual(rowOf(db, '树莓派'), before, 'owner 不符时一行不改');
  assert.equal(completeObserveTask(db, '树莓派', claim.version, { now: 400, owner: 'w2' }), true);
  db.close();
});

test('不传 owner 的旧调用照旧工作（兼容）', () => {
  const db = freshDb();
  enqueueObserveTask(db, '树莓派', { now: 100 });
  const [claim] = claimObserveTasks(db, { limit: 1, now: 200 });
  assert.equal(claim.version, 1);
  assert.equal(completeObserveTask(db, '树莓派', claim.version, { now: 300 }), true);

  enqueueObserveTask(db, '树莓派', { now: 400 });
  const [second] = claimObserveTasks(db, { limit: 1, owner: 'w1', now: 500 });
  assert.equal(second.version, 2);
  assert.equal(failObserveTask(db, '树莓派', second.version, 'boom', { now: 600 }), true);
  assert.equal(Number(rowOf(db, '树莓派').tries), 1);
  db.close();
});

test('缺表时四个函数优雅返回，不抛错', () => {
  const db = new DatabaseSync(':memory:');
  assert.equal(enqueueObserveTask(db, '树莓派'), null);
  assert.deepEqual(claimObserveTasks(db, { limit: 5 }), []);
  assert.equal(completeObserveTask(db, '树莓派', 1), false);
  assert.equal(failObserveTask(db, '树莓派', 1, 'x'), false);
  db.close();
});

test('外层事务里可安全调用：多次 enqueue 只涨 version，不产生多行', () => {
  const db = freshDb();
  db.exec('BEGIN IMMEDIATE');
  try {
    enqueueObserveTask(db, '树莓派', { reason: 'save', now: 100 });
    enqueueObserveTask(db, '树莓派', { reason: 'merge', now: 200 });
    const claimed = claimObserveTasks(db, { limit: 1, owner: 'w1', now: 300 });
    assert.equal(claimed.length, 1);
    assert.equal(claimed[0].version, 2);
    db.exec('COMMIT');
  } catch (error) {
    db.exec('ROLLBACK');
    throw error;
  }
  const row = rowOf(db, '树莓派');
  assert.equal(Number(row.version), 2);
  assert.equal(String(row.status), 'leased');
  db.close();
});
