/**
 * compact 通道（2026-11-14，阶段 5）。
 *
 * 钉四件事：
 *   1) compact 三类 kind 真入队、channelStats('compact') 能报 pending/running；
 *   2) revocation_sweep 同一天重复入队不加行（dedupe_key 幂等）；
 *   3) 老库（CHECK 不认 revocation_sweep）能就地迁移，旧行一条不丢；
 *   4) channelStats 把「闸的 LLM」与「本地动作」分开报（llm_calls/local_ops）。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { applySchema } from '../scripts/apply-pipeline-schema.mjs';
import { enqueue, claimChannelOps, channelStats, ensureChannelColumn } from '../lib/ingest-index.js';
import { kindsOfChannel } from '../lib/kind-handlers.js';

function freshDb() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-compact-'));
  const dbPath = path.join(dir, 'memory.db');
  new DatabaseSync(dbPath).close();
  applySchema(dbPath, { logger: () => {} });
  return new DatabaseSync(dbPath);
}

test('compact 通道收三类 kind，channelStats 能报 pending/running', () => {
  const db = freshDb();
  assert.deepEqual(kindsOfChannel('compact').sort(), ['dedupe_check', 'entity_tidy', 'merge_op', 'revocation_sweep']);
  for (const kind of ['revocation_sweep', 'merge_op', 'dedupe_check', 'entity_tidy']) {
    enqueue(db, kind, `k-${kind}`, { k: kind }, { now: 1 });
  }
  const before = channelStats(db, 'compact');
  assert.equal(before.pending, 4, '四类都进 compact 队列');
  const [op] = claimChannelOps(db, 'compact', 1, { now: 2, kinds: kindsOfChannel('compact') });
  assert.ok(op);
  const after = channelStats(db, 'compact');
  assert.equal(after.running, 1);
  assert.equal(after.pending, 3);
  db.close();
});

test('revocation_sweep 幂等：同一天重复入队不加行', () => {
  const db = freshDb();
  const ins = () => enqueue(db, 'revocation_sweep', 'revocation:2026-11-14', { day: '2026-11-14' }, { now: 1 });
  ins();
  ins();
  const n = db.prepare("SELECT count(*) AS n FROM pending_ops WHERE kind='revocation_sweep'").get().n;
  assert.equal(Number(n), 1);
  db.close();
});

test('老库迁移：CHECK 不认 revocation_sweep 时就地重建，旧行不丢', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-compact-old-'));
  const dbPath = path.join(dir, 'memory.db');
  const db = new DatabaseSync(dbPath);
  // 模拟老库：CHECK 里没有 revocation_sweep
  db.exec(`CREATE TABLE pending_ops (
    id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL
      CHECK (kind IN ('ingest_file','merge_batch','split_item','dedupe_check','merge_op')),
    dedupe_key TEXT NOT NULL, payload_json TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0, next_try_at INTEGER NOT NULL,
    last_error TEXT, status TEXT NOT NULL DEFAULT 'pending',
    created_ts INTEGER NOT NULL, updated_ts INTEGER NOT NULL, UNIQUE(kind, dedupe_key));`);
  db.prepare(
    "INSERT INTO pending_ops(kind,dedupe_key,payload_json,attempts,next_try_at,status,created_ts,updated_ts) VALUES ('merge_op','old-1','{}',1,0,'pending',1,1)",
  ).run();
  assert.equal(ensureChannelColumn(db), true);
  // 迁移后能插新 kind，旧行还在
  enqueue(db, 'revocation_sweep', 'revocation:2026-11-14', { day: '2026-11-14' }, { now: 2 });
  assert.equal(Number(db.prepare("SELECT count(*) AS n FROM pending_ops WHERE kind='merge_op'").get().n), 1);
  assert.equal(Number(db.prepare("SELECT count(*) AS n FROM pending_ops WHERE kind='revocation_sweep'").get().n), 1);
  db.close();
});

test('channelStats 分开报 llm_calls 与 local_ops（读最近一条 compact run）', () => {
  const db = freshDb();
  const now = Date.now();
  db.exec(`CREATE TABLE curate_run (
    run_id TEXT PRIMARY KEY, channel TEXT NOT NULL, state TEXT NOT NULL, stage TEXT NOT NULL DEFAULT '',
    cursor_json TEXT NOT NULL DEFAULT '{}', budget_json TEXT NOT NULL DEFAULT '{}', owner TEXT NOT NULL DEFAULT '',
    lease_expires INTEGER NOT NULL DEFAULT 0, started_ts INTEGER NOT NULL, heartbeat_ts INTEGER NOT NULL,
    updated_ts INTEGER NOT NULL, stop_reason TEXT NOT NULL DEFAULT '');`);
  db.prepare(
    `INSERT INTO curate_run(run_id,channel,state,stage,cursor_json,budget_json,owner,lease_expires,started_ts,heartbeat_ts,updated_ts)
     VALUES ('c1','compact','done','compact','{}','{"llm_calls":7,"local_ops":3,"ops":5}','x',0,?,?,?)`,
  ).run(now, now, now);
  const st = channelStats(db, 'compact');
  assert.equal(st.llm_calls, 7);
  assert.equal(st.local_ops, 3);
  db.close();
});

test('merge_op/dedupe_check 老账被回填成 curate 也能被 compact 领走（kind 注册表说了算）', () => {
  const db = freshDb();
  // 模拟历史：阶段 5 之前 ensureChannelColumn 把这两个 kind 写死回填成 curate
  const now = Date.now();
  const ins = db.prepare(`INSERT INTO pending_ops
    (kind, channel, dedupe_key, payload_json, attempts, next_try_at, last_error, status, created_ts, updated_ts)
    VALUES (?, ?, ?, '{}', 2, 0, '', 'pending', ?, ?)`);
  ins.run('merge_op', 'curate', 'legacy-merge-op', now, now);
  ins.run('dedupe_check', 'curate', 'legacy-dedupe', now, now);
  assert.equal(ensureChannelColumn(db), true);
  const got = claimChannelOps(db, 'compact', 2, { now: now + 1, kinds: kindsOfChannel('compact') });
  assert.deepEqual(got.map((o) => o.kind).sort(), ['dedupe_check', 'merge_op'], '两个老账都要被 compact 领到');
  assert.equal(Number(db.prepare("SELECT count(*) AS n FROM pending_ops WHERE channel <> 'compact'").get().n), 0);
  db.close();
});
