/**
 * 队列通道与统一并发的测试（2026-11-14）：
 *   1) pending_ops 的 channel 列：老库自愈加列 + 回填，领活按通道走；
 *   2) 领活粒度：claimChannelOps 一次只把「并发数」那么多标 running，峰值不超过它；
 *   3) 统一并发口径：自适应关掉 → 各段恒用上限；organizeConcurrency 只压 curate，不动 ingest。
 * 拆分段并发/落地串行已由 test/split-concurrency.test.mjs 覆盖（同一分支不写第二遍），本文件不再重复。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { applySchema, DDL } from '../scripts/apply-pipeline-schema.mjs';
import { claimChannelOps, reapChannelOps, channelStats, enqueue, markOp, ensureChannelColumn, MAX_ATTEMPTS } from '../lib/ingest-index.js';
import { selectConcurrency, selectCurateConcurrency, selectIngestConcurrency, curateCapOf } from '../lib/concurrency.js';

function freshDb() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-chan-'));
  const dbPath = path.join(dir, 'memory.db');
  new DatabaseSync(dbPath).close();
  applySchema(dbPath, { logger: () => {} });
  return new DatabaseSync(dbPath);
}

test('DDL 自带 channel 列，老库自愈加列且回填', () => {
  assert.ok(DDL.join('\n').includes('channel'), 'pending_ops 建表就该有 channel');
  const db = freshDb();
  // 造一条「老库」形态的行（channel 空）→ 自愈时按 kind 回填
  const op = enqueue(db, 'ingest_file', '/tmp/x.jsonl', { path: '/tmp/x.jsonl' });
  db.prepare("UPDATE pending_ops SET channel='' WHERE id=?").run(op.id);
  assert.equal(ensureChannelColumn(db, { force: true }), true);
  const row = db.prepare('SELECT channel FROM pending_ops WHERE id=?').get(op.id);
  assert.equal(row.channel, 'ingest');
  db.close();
});

test('领活粒度：一次只标「并发数」那么多 running，绝不整队标', () => {
  const db = freshDb();
  for (let i = 0; i < 10; i += 1) {
    enqueue(db, 'ingest_file', `file-${i}`, { path: `file-${i}` }, { now: 1 });
  }
  const first = claimChannelOps(db, 'ingest', 3, { now: 2 });
  assert.equal(first.length, 3, '一领只许领并发数那么多');
  assert.equal(channelStats(db, 'ingest').running, 3);
  assert.equal(channelStats(db, 'ingest').pending, 7, '剩下的还在 pending，不许被顺手标 running');

  // 做完一批再领下一批
  for (const op of first) markOp(db, op.id, 'done', null, { now: 3 });
  const second = claimChannelOps(db, 'ingest', 3, { now: 4 });
  assert.equal(second.length, 3);
  assert.ok(!first.some((a) => second.some((b) => a.id === b.id)), '同一轮不许重复领');
  assert.equal(channelStats(db, 'ingest').running, 3, '在飞条数始终不超过领活批次');
  db.close();
});

test('租约回收：running 超时捞回 pending 并计一次 attempts，到阈值转 dead 留原因', () => {
  const db = freshDb();
  enqueue(db, 'ingest_file', 'slow', { path: 'slow' }, { now: 1 });
  const [op] = claimChannelOps(db, 'ingest', 1, { now: 2, leaseMs: 1000 });
  assert.equal(op.attempts, 0);
  for (let i = 1; i <= MAX_ATTEMPTS; i += 1) {
    const at = 2 + i * 60000;
    assert.equal(reapChannelOps(db, { now: at, leaseMs: 1000, channel: 'ingest' }), 1, `第 ${i} 次过期要捞回`);
    const row = db.prepare('SELECT status, attempts, last_error FROM pending_ops WHERE id = ?').get(op.id);
    assert.equal(Number(row.attempts), i, 'reap 也算一次尝试，attempts+1');
    if (i < MAX_ATTEMPTS) {
      assert.equal(row.status, 'pending');
      assert.equal(row.last_error, 'lease_expired');
      const [again] = claimChannelOps(db, 'ingest', 1, { now: at + 1, leaseMs: 1000 });
      assert.equal(again.id, op.id, '捞回来还能再领到');
      assert.equal(again.attempts, i, '领取带回来的 attempts 含这次过期');
    } else {
      assert.equal(row.status, 'dead', '到阈值判死，毒账不会永远活着');
      assert.match(row.last_error, /^lease_expired/);
      assert.equal(channelStats(db, 'ingest').dead, 1);
      assert.equal(channelStats(db, 'ingest').pending, 0, '死了就不回 pending');
    }
  }
  db.close();
});

test('统一口径：自适应关掉各段恒用上限；organizeConcurrency 只压 curate', () => {
  const off = { organizeIngestConcurrency: 64, organizeIngestAdaptive: false, organizeConcurrency: 0 };
  assert.equal(selectIngestConcurrency(off).conc, 64);
  assert.equal(selectCurateConcurrency(off).conc, 64);
  assert.equal(selectConcurrency(off, 7).conc, 7, 'cap 说了算');

  const capped = { organizeIngestConcurrency: 64, organizeIngestAdaptive: false, organizeConcurrency: 3 };
  assert.equal(selectCurateConcurrency(capped).conc, 3, 'curate 通道被压到 3');
  assert.equal(selectIngestConcurrency(capped).conc, 64, 'ingest 通道不许被 organizeConcurrency 牵连');
  assert.equal(curateCapOf(capped), 3);

  // 开着自适应时 cap 是天花板，算出来的路数不超过它
  const hot = { load1: 100, cores: 4, memAvailableBytes: 1_000_000_000, tempC: 20 };
  const pick = selectCurateConcurrency({ organizeIngestConcurrency: 64, organizeIngestAdaptive: true, organizeConcurrency: 4 }, hot);
  assert.ok(pick.conc >= 1 && pick.conc <= 4, `高负载下路数 ${pick.conc} 要夹在 [1,4]`);
});

test('同通道按 kind 领活：merge 池不许领走 split_item（2026-11-15 stale_members 事故根因）', () => {
  const db = freshDb();
  enqueue(db, 'merge_batch', 'fp-a', { batch: 'fp-a', ids: ['m_1', 'm_2'] });
  enqueue(db, 'split_item', 'm_1', { source_id: 'm_1', text: 'A'.repeat(300) });
  enqueue(db, 'split_item', 'm_2', { source_id: 'm_2', text: 'B'.repeat(300) });
  // 两行都有 channel='curate'：领活如果只按 channel 过滤，merge 池会把拆分的活一起吃掉
  const channels = db.prepare("SELECT kind, channel FROM pending_ops ORDER BY kind").all();
  assert.deepEqual(channels.map((r) => `${r.kind}:${r.channel}`), [
    'merge_batch:curate', 'split_item:curate', 'split_item:curate',
  ]);

  const mergeClaimed = claimChannelOps(db, 'curate', 10, { kinds: ['merge_batch'], now: Date.now() + 1000 });
  assert.deepEqual(mergeClaimed.map((op) => op.kind), ['merge_batch'], 'merge 池只领 merge_batch');
  assert.equal(
    db.prepare("SELECT count(*) AS n FROM pending_ops WHERE kind='split_item' AND status='pending'").get().n,
    2,
    'split_item 必须还在 pending，等拆分消费者来领',
  );

  const splitClaimed = claimChannelOps(db, 'curate', 10, { kinds: ['split_item'], now: Date.now() + 2000 });
  assert.deepEqual(splitClaimed.map((op) => op.kind), ['split_item', 'split_item']);
  db.close();
});
