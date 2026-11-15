/**
 * 5 处「未绑定标识符」的行为回归。
 *
 * 这 5 处都是拆分时漏搬的既有缺陷：类型上被 `declare const` 遮住，运行时才炸，
 * 而且大多炸在 try/catch 里，静默少账/少日志。每条用例都钉「修之前必失败、
 * 修之后有正向可观察行为」：
 *   1) sleep/lint.js  askActionsWithLint：llmOf / ctx 没绑定 → ReferenceError
 *   2) sleep/pool.js  planPoolQueues：CLUSTER_PROMPT_SHELL_TOKENS 没 import → ReferenceError
 *   3) sleep/pool-pass.js pooledSleepPass：sleepOwner 没声明 → curate_run 少一条账
 *   4) sleep/setup.js  setupSleep：windowSessionOf 没绑定 → 少一条启动窗口日志
 *   5) sleep/scheduler.js archive 钩子：this.cfg 不存在 → .md 留底静默丢失
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { applySchema } from '../scripts/apply-pipeline-schema.mjs';
import { createLint } from '../lib/sleep/lint.js';
import { createPool } from '../lib/sleep/pool.js';
import { createPoolPass } from '../lib/sleep/pool-pass.js';
import { createScheduler } from '../lib/sleep/scheduler.js';
import { setupSleep } from '../lib/sleep.js';

function tmpDir(tag) {
  return fs.mkdtempSync(path.join(os.tmpdir(), `dsh-unbound-${tag}-`));
}

/** 建一个带 ingest/pending_ops/discards/curate_run 的库（本地文件，sqlite 要落盘）。 */
function freshDb(tag) {
  const dir = tmpDir(tag);
  const dbPath = path.join(dir, 'memory.db');
  new DatabaseSync(dbPath).close();
  applySchema(dbPath, { logger: () => {} });
  const boot = new DatabaseSync(dbPath);
  boot.exec('CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT)');
  // curate_run 的 DDL 照 scripts/apply-curate-schema.mjs 抄（那个脚本要求 memories/entities 先存在，
  // 本文件只用到这张账表，不为它补一整套存储表）。
  boot.exec(`CREATE TABLE IF NOT EXISTS curate_run (
  run_id        TEXT PRIMARY KEY,
  channel       TEXT NOT NULL,
  state         TEXT NOT NULL,
  stage         TEXT NOT NULL DEFAULT '',
  cursor_json   TEXT NOT NULL DEFAULT '{}',
  budget_json   TEXT NOT NULL DEFAULT '{}',
  owner         TEXT NOT NULL DEFAULT '',
  lease_expires INTEGER NOT NULL DEFAULT 0,
  started_ts    INTEGER NOT NULL,
  heartbeat_ts  INTEGER NOT NULL,
  updated_ts    INTEGER NOT NULL,
  stop_reason   TEXT NOT NULL DEFAULT ''
)`);
  boot.close();
  return { dir, dbPath, db: new DatabaseSync(dbPath) };
}

/** 假 llm：复核那一路回 keep，够 judgeSuspected 走完。 */
function fakeReviewLlm(text) {
  return {
    calls: [],
    stream(options) {
      this.calls.push(options);
      return (async function* gen() {
        yield { type: 'text-delta', text };
        yield { type: 'finish', reason: { kind: 'stop' } };
      })();
    },
  };
}

function emptyPlan(over = {}) {
  return {
    files: 0, ingest: 0, merge: 0, split: 0, cached: 0, batches: 0, legacy: 0, sweep: 0, tidy: 0, ...over,
  };
}

test('lint: askActionsWithLint 拿到 ctx 与 llmOf，不再抛 ReferenceError', async () => {
  const llm = fakeReviewLlm('{"decisions":[{"index":0,"verdict":"keep","reason":"单个对象+路径，能自足看懂"}]}');
  const ctx = { get: (name) => (name === 'llm' ? llm : undefined) };
  const lint = createLint({
    ctx,
    cfg: { pipeline: { lintReview: true } },
    log: () => {},
    // 第一次问模型：正文以「但是」开头，必定被代码 lint 打标 → 进复核那条路
    model: { askModel: async () => '{"add":[{"text":"但是小林把树莓派温度调到了 45 度。"}]}' },
    ledger: { pendingDb: () => null },
  });

  const out = await lint.askActionsWithLint({ transcript: 't', existing: [], effort: 'low', tag: '测试' });
  assert.ok(out.actions, '返回结构里要有 actions');
  assert.equal(out.actions.add.length, 1, '复核判 keep 的条目原样放行');
  assert.equal(out.lintReviewed, 1, '复核真被调到了（llmOf(ctx) 生效）');
  assert.ok(out.actions.add[0].__lintKept, 'keep 的条目带 __lintKept 标记');
  assert.equal(llm.calls.length, 1, '一个批次只发一次复核调用');
});

test('pool: planPoolQueues 认得 CLUSTER_PROMPT_SHELL_TOKENS，规划不再抛 ReferenceError', () => {
  const { dir, db } = freshDb('pool');
  const pool = createPool({
    ctx: {},
    store: { db: () => db, typeOf: () => 'world' },
    cfg: { sessionsRoot: path.join(dir, 'sessions'), storePath: dir, splitMaxChars: 200 },
    stateStore: { load: () => ({}) },
    log: () => {},
    ledger: { pendingDb: () => db, candidateListFor: () => [] },
    agentOps: { applyAgentOps: async () => ({}) },
    replay: { replayPendingOps: async () => ({}) },
    actions: { revocationPass: async () => ({ scanned: 0, archived: [], skipped: [], orphan: [] }) },
    scheduler: { putMergeResult: () => ({}) },
    activeSnap: () => ({ list: [], byId: new Map(), vecOf: new Map() }),
    // 生产窗口 1048576：window 非 0 → 必走扣掉 shell 常量的那一支
    budgetOf: () => ({ window: 1048576, reserve: 0, budget: 1048576 }),
    batchPlannerOf: () => ({ plan: () => ({ batches: [], picked: [], cached: 0, funnel: {} }) }),
    ingest: { shortErr: (e) => String(e) },
  });

  const counts = pool.planPoolQueues(db, { dryRun: false, lastSleepAt: Date.now() });
  assert.equal(counts.files, 0, '空 sessions 目录：0 个文件');
  assert.equal(counts.sweep, 1, '作废清扫每天一条');
  assert.equal(counts.tidy, 1, '实体整理每天一条');
  const kinds = db.prepare('SELECT kind FROM pending_ops ORDER BY kind').all().map((r) => String(r.kind));
  assert.deepEqual(kinds, ['entity_tidy', 'revocation_sweep'], '两条 compact 活真进了 pending_ops');
});

test('pool-pass: pooledSleepPass 记 curate_run 账，owner 是本实例的 sleepOwner', async () => {
  const { dir, db } = freshDb('curate-run');
  const logs = [];
  const SLEEP_OWNER = 'deadbeef:4242:777777';
  const poolPass = createPoolPass({
    ctx: { get: () => undefined },
    store: { db: () => db, listAll: () => [], typeOf: () => 'world', gateStats: () => ({ llm: 0 }) },
    cfg: {
      storePath: dir,
      sessionsRoot: path.join(dir, 'sessions'),
      archiveDir: path.join(dir, 'archive'),
      backupDir: path.join(dir, 'backup'),
      notifyScript: '/bin/true',
      splitMaxChars: 200,
      pipeline: { agentPipeline: false, textLint: false },
    },
    stateStore: { load: () => ({ lastSleepAt: Date.now() }), patch: () => {} },
    sleepOwner: SLEEP_OWNER,
    log: (line) => logs.push(line),
    push: () => {},
    snapshot: async () => '',
    scheduler: {
      beginPass: () => true,
      endPass: () => {},
      setRoundSnapshot: (s) => s,
      mergeCache: () => ({}),
      curatePick: () => ({ conc: 1, cap: 1, adaptive: false }),
    },
    ledger: { pendingDb: () => db },
    organize: { persistOrganizeCache: () => {} },
    ingest: {
      shortErr: (e) => String(e),
      emptyActionResult: () => ({
        added: [], merged: [], updated: [], archived: [], rejected: [],
      }),
      mergeInto: () => {},
      prepareOneFile: async () => ({}),
      commitOneFile: async () => ({}),
    },
    // 规划与 compact 池打桩：本用例只钉收尾那条 curate_run 记账
    pool: {
      planPoolQueues: () => emptyPlan(),
      startCompactPool: () => ({
        pool: { drain: async () => ({ claimed: 0, landed: 0 }) },
        stats: {
          llm: 0, local: 0, ops: 0, revoked: 0, failed: 0, archived: [], replay: {}, tidy: null,
        },
      }),
      prepareMergeBatchOp: async () => ({}),
      commitMergeBatchOp: async () => ({}),
    },
    passes: { legacySleepPass: async () => ({}) },
  });

  const out = await poolPass.pooledSleepPass({ dryRun: true });
  assert.equal(out.ran, true, '这一轮正常收工');
  const rows = db.prepare('SELECT channel, state, owner FROM curate_run').all();
  assert.equal(rows.length, 1, 'curate_run 应记到 1 条账（修之前被 catch 吞掉是 0 条）');
  assert.equal(String(rows[0].channel), 'compact');
  assert.equal(String(rows[0].owner), SLEEP_OWNER, 'owner 是本实例的 sleepOwner，不是空串');
});

test('setup: 启动时打出拼 prompt 窗口日志（windowSessionOf 走 env）', () => {
  const { dir, db } = freshDb('setup');
  const warns = [];
  const ctx = {
    logger: { info: () => {}, warn: (line) => warns.push(String(line)) },
    effect: (fn) => {
      const off = fn();
      if (typeof off === 'function') off();
      return () => {};
    },
    tools: { get: () => null, register: () => {} },
    provide: () => {},
    get: () => null,
  };
  const cfg = {
    storePath: dir,
    sessionsRoot: path.join(dir, 'sessions'),
    archiveDir: path.join(dir, 'archive'),
    backupDir: path.join(dir, 'backup'),
    notifyScript: '/bin/true',
    sleepLogFile: path.join(dir, 'memory_sleep.log'),
    sleepPriceProbe: false,
    sleepEnabled: false,
    sleepSummaryNotify: false,
    contextWindowTokens: 1048576,
    pipeline: { ingestAck: true, textLint: false, writeGate: false, agentPipeline: false },
  };
  const store = {
    db: () => db,
    listAll: () => [],
    typeOf: () => 'world',
    save: async () => ({}),
    update: async () => ({}),
    archive: async () => ({}),
    merge: async () => ({}),
  };

  setupSleep(ctx, store, cfg);
  assert.ok(
    warns.some((line) => line.includes('拼 prompt 窗口') && line.includes('1048576')),
    `ctx.logger.warn 应收到窗口日志，实际 ${JSON.stringify(warns)}`,
  );
});

test('scheduler: archive 钩子真把 .md 写进 archiveDir', () => {
  const dir = tmpDir('archive');
  const archiveDir = path.join(dir, 'archive');
  let service = null;
  const scheduler = createScheduler({
    cfg: { archiveDir, storePath: dir, notifyScript: '/bin/true' },
    ctx: {
      provide: (name, value) => {
        if (name === 'memory-archive') service = value;
      },
    },
    stateStore: { load: () => ({}), patch: () => {} },
    sleepOwner: 'deadbeef:4242:777777',
    log: () => {},
    push: () => {},
    passes: { sleepPass: async () => ({}), archivePass: async () => ({}) },
  });
  assert.ok(scheduler, '调度器建起来了');
  assert.ok(service, 'memory-archive 服务挂上了');

  const out = service.archive('小林把树莓派温度压到 45 度。', { scope: 'unbound' });
  assert.equal(out.queued, true, '有正文就入队');
  const files = fs.readdirSync(archiveDir);
  assert.equal(files.length, 1, `archiveDir 里应有 1 个 .md，实际 ${JSON.stringify(files)}`);
  assert.match(files[0], /-unbound\.md$/, '文件名带 scope');
  assert.equal(fs.readFileSync(path.join(archiveDir, files[0]), 'utf8'), '小林把树莓派温度压到 45 度。');
});
