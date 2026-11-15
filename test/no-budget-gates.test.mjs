/**
 * 数据条数闸删掉的证据：
 * maxChanges / clusterChanges / archiveMaxChanges / sleepSplitLimit / ingestBudget /
 * replayBudget 这些「本轮最多处理 N 个」全部删了，本轮该处理多少就多少。
 * 这里锁住最容易回退的两处：写库动作数和 pending_ops 领取条数。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { applySchema } from '../scripts/apply-pipeline-schema.mjs';
import { dueOps, enqueue } from '../lib/ingest-index.js';
import { setupSleep } from '../lib/sleep.js';

function tmpDir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'no-budget-'));
}

function fakeCtx() {
  return { llm: {}, provide() {}, effect: () => () => {}, tools: { register() {} } };
}

test('动作数不设上限：cfg.maxChanges=1 也照样落 12 条', async () => {
  const dir = tmpDir();
  const cfg = {
    maxChanges: 1, // 旧配置键；2026-11-14 起代码不再读它
    sleepLogFile: path.join(dir, 'sleep.log'),
    stateFile: path.join(dir, 'sleep.state.json'),
    provider: 'fake',
    model: 'fake',
  };
  const store = { listAll: () => [], metas: () => [], db: () => null };
  const sleep = setupSleep(fakeCtx(), store, cfg);
  const actions = {
    add: Array.from({ length: 12 }, (_, i) => ({ text: `第 ${i + 1} 条测试记忆，树莓派温度 45 度。` })),
    merge: [],
    update: [],
    archive: [],
  };
  const result = await sleep.__applyActions(actions, { existing: [], dryRun: true });
  assert.equal(result.added.length, 12, '旧上限 20/1 都不能挡，12 条全过');
  assert.equal(result.rejected.length, 0);
});

test('dueOps 传 Infinity：当期到期的账一次全领（不再有单轮条数预算）', () => {
  const dir = tmpDir();
  const dbPath = path.join(dir, 'memory.db');
  const db = new DatabaseSync(dbPath, { allowExtension: true });
  applySchema(dbPath, { logger: () => {} });
  const now = Date.now();
  for (let i = 0; i < 60; i += 1) {
    enqueue(db, 'merge_batch', `fp_${i}`, { batch: `fp_${i}`, ids: ['m_1', 'm_2'] }, { now });
  }
  const got = dueOps(db, Infinity, { kind: 'merge_batch', now });
  assert.equal(got.length, 60, 'replayBudget=50 的旧上限不能截断');
  // 第二次调用时它们已经是 running，不会重复领
  assert.equal(dueOps(db, Infinity, { kind: 'merge_batch', now }).length, 0);
  db.close();
});

test('dueOps 的 exclude：本轮刚失败的账不会在同一轮被再领', () => {
  const dir = tmpDir();
  const dbPath = path.join(dir, 'memory.db');
  const db = new DatabaseSync(dbPath, { allowExtension: true });
  applySchema(dbPath, { logger: () => {} });
  const now = Date.now();
  const a = enqueue(db, 'merge_batch', 'fp_a', { batch: 'fp_a' }, { now });
  enqueue(db, 'merge_batch', 'fp_b', { batch: 'fp_b' }, { now });
  const first = dueOps(db, Infinity, { kind: 'merge_batch', now });
  assert.equal(first.length, 2);
  // 两条都失败回 pending（next_try_at=now）；a 已经被这一轮试过，用 exclude 挡住
  db.prepare("UPDATE pending_ops SET status='pending', next_try_at=?").run(now);
  const second = dueOps(db, Infinity, { kind: 'merge_batch', now, exclude: [a.id] });
  assert.deepEqual(second.map((o) => o.dedupeKey), ['fp_b'], '刚失败的 a 被 exclude 挡住，同一轮只试一次');
  db.close();
});
