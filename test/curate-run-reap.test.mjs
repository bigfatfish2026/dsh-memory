/**
 * curate_run 的租约回收（t-mwp0uk01-98eh）。
 *
 * 钉三件事：
 *   ① 过期（state='executing' 且 lease_expires 已过）的行被回收成 aborted，stop_reason=lease_expired；
 *   ② 没过期的 executing 行不动；finishRun 写过 lease_expires=0 的收尾行也不动；
 *   ③ 行不删；回收有两条触发点：进程启动（setupEntityQueue.start）和每轮开跑（runEntityQueueOnce）。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { applySchema } from '../scripts/apply-pipeline-schema.mjs';
import { applyCurateSchema } from '../scripts/apply-curate-schema.mjs';
import { reapStaleRuns, setupEntityQueue, runEntityQueueOnce } from '../lib/entity-worker.js';

const NOW = 1_800_000_000_000;

function tmpDb() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-curate-run-'));
  const dbPath = path.join(dir, 'memory.db');
  new DatabaseSync(dbPath).close();
  applySchema(dbPath, { logger: () => {} });
  const db = new DatabaseSync(dbPath);
  db.exec('CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);');
  // apply-curate-schema 要给 memory_entity 补 origin 列并数 entities：这两张表先摆最小形状
  db.exec('CREATE TABLE IF NOT EXISTS entities (id INTEGER PRIMARY KEY, name TEXT);');
  db.exec('CREATE TABLE IF NOT EXISTS memory_entity (memory_id TEXT, entity_id INTEGER, role TEXT);');
  db.close();
  applyCurateSchema(dbPath, { logger: { info() {}, warn() {} } });
  return { dir, dbPath, db: new DatabaseSync(dbPath) };
}

/** 插一条 curate_run：leaseExpires 相对 NOW 的偏移（0 = finishRun 写过的收尾行）。 */
function seedRun(db, { runId, state = 'executing', leaseIn, channel = 'curate', base = Date.now() }) {
  db.prepare(
    `INSERT INTO curate_run(run_id, channel, state, stage, cursor_json, budget_json, owner,
                            lease_expires, started_ts, heartbeat_ts, updated_ts)
     VALUES (?, ?, ?, 'entity_extract', '{}', '{}', 'boot:999999:1', ?, ?, ?, ?)`,
  ).run(runId, channel, state, leaseIn === 0 ? 0 : base + leaseIn, base - 3600000, base - 3600000, base - 3600000);
}

test('① 过期 executing 回收成 aborted，未过期 / 已收尾的行不动，行不删', () => {
  const { db } = tmpDb();
  seedRun(db, { runId: 'r_dead_1', leaseIn: -7200000, base: NOW }); // 两小时前就过期（owner 进程已 GONE）
  seedRun(db, { runId: 'r_dead_2', leaseIn: -1000, base: NOW });
  seedRun(db, { runId: 'r_live', leaseIn: 180000, base: NOW }); // 还在租约里
  seedRun(db, { runId: 'r_finished', state: 'done', leaseIn: 0, base: NOW }); // finishRun 写过 lease_expires=0
  const before = db.prepare('SELECT count(*) AS n FROM curate_run').get().n;

  const reaped = reapStaleRuns(db, { now: NOW });
  assert.equal(reaped, 2, '两条过期');
  const rows = Object.fromEntries(db.prepare('SELECT run_id, state, stop_reason FROM curate_run').all().map((r) => [r.run_id, r]));
  assert.equal(rows.r_dead_1.state, 'aborted');
  assert.equal(rows.r_dead_1.stop_reason, 'lease_expired');
  assert.equal(rows.r_dead_2.state, 'aborted');
  assert.equal(rows.r_live.state, 'executing', '没过期的绝不动');
  assert.equal(rows.r_finished.state, 'done', '收尾行不动');
  assert.equal(db.prepare('SELECT count(*) AS n FROM curate_run').get().n, before, '只改状态，不删行');
  assert.equal(reapStaleRuns(db, { now: NOW }), 0, '重复跑幂等');
  db.close();
});

test('② 进程启动（setupEntityQueue.start）就回收一次僵尸 run', () => {
  const { dir, db } = tmpDb();
  seedRun(db, { runId: 'r_stale', leaseIn: -600000 });
  const cfg = {
    entityQueue: { tickMs: 60000, logFile: path.join(dir, 'entity.log') },
    sleepLogFile: path.join(dir, 'sleep.log'),
  };
  const handle = setupEntityQueue({}, { db: () => db }, cfg);
  try {
    const row = db.prepare("SELECT state, stop_reason FROM curate_run WHERE run_id='r_stale'").get();
    assert.equal(row.state, 'aborted', '启动时收掉上一个进程留下的 executing');
    assert.equal(row.stop_reason, 'lease_expired');
    const log = fs.readFileSync(path.join(dir, 'entity.log'), 'utf8');
    assert.match(log, /\[entity-queue\] curate_run 僵尸回收：1 条 executing→aborted（进程启动）/);
  } finally {
    handle.dispose();
    db.close();
  }
});

test('③ 每轮开跑（runEntityQueueOnce）也回收一次', async () => {
  const { dir, db } = tmpDb();
  seedRun(db, { runId: 'r_stale_tick', leaseIn: -600000 });
  const logs = [];
  // 队列里没有 ready 的活：一轮只走「回收 → startRun → 领空 → finishRun」
  const out = await runEntityQueueOnce({
    store: { db: () => db },
    cfg: { entityQueue: { logFile: path.join(dir, 'entity.log') }, sleepLogFile: path.join(dir, 'sleep.log') },
    llm: { stream() {} },
    log: (line) => logs.push(line),
    now: NOW,
  });
  assert.equal(out.claimed, 0);
  const row = db.prepare("SELECT state FROM curate_run WHERE run_id='r_stale_tick'").get();
  assert.equal(row.state, 'aborted');
  assert.match(logs.join('\n'), /\[entity-queue\] curate_run 僵尸回收：1 条 executing→aborted（租约过期）/);
  db.close();
});

test('④ 没有 curate_run 表 / 没有 db 时不抛错', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-curate-run-bare-'));
  const dbPath = path.join(dir, 'memory.db');
  const db = new DatabaseSync(dbPath);
  assert.equal(reapStaleRuns(db), 0);
  assert.equal(reapStaleRuns(null), 0);
  assert.equal(reapStaleRuns(undefined), 0);
  db.close();
});
