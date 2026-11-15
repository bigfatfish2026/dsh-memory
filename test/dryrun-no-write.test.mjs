/**
 * dryRun 零写加固（2026-11-05）：
 * 1) 五个账本入口（bootstrapLegacy/ackFile/failFile/enqueue/markOp）传 dryRun 时
 *    只记日志，ingest_files / pending_ops 行数一动不动；
 * 2) 整条 sleepPass(ingestAck=true, dryRun=true) 跑完，两张表还是零行。
 *
 * 2026-11-05 之前的坑：ingestAck 打开后 dryRun 仍会在 bootstrapLegacy 和重放成功后的
 * markOp(done) 写行，演练不再是零写。这里把入口和整条流程都钉住。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { applySchema } from '../scripts/apply-pipeline-schema.mjs';
import {
  ackFile,
  bootstrapLegacy,
  enqueue,
  failFile,
  markOp,
} from '../lib/ingest-index.js';
import { setupSleep } from '../lib/sleep.js';

const T0 = Date.now();

function tmpDb() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-dryrun-'));
  const dbPath = path.join(dir, 'memory.db');
  new DatabaseSync(dbPath).close();
  applySchema(dbPath, { logger: () => {} });
  return { dir, dbPath, open: () => new DatabaseSync(dbPath) };
}

function counts(db) {
  return {
    ingest: Number(db.prepare('SELECT count(*) AS n FROM ingest_files').get()?.n ?? 0),
    pending: Number(db.prepare('SELECT count(*) AS n FROM pending_ops').get()?.n ?? 0),
  };
}

function putSession(root, events) {
  const dir = path.join(root, 'agent-a', 's1');
  fs.mkdirSync(dir, { recursive: true });
  const file = path.join(dir, 'session-dry.jsonl');
  fs.writeFileSync(file, `${events.map((e) => JSON.stringify(e)).join('\n')}\n`, 'utf8');
  return file;
}

// 一个只会回空动作的假模型：dryRun 下它不该引起任何写
const fakeLlm = {
  async *stream() {
    yield { type: 'text-delta', text: '{"add":[],"merge":[],"update":[],"archive":[]}' };
    yield { type: 'finish', reason: { kind: 'stop' } };
  },
};

test('五个账本入口在 dryRun 下都不写 ingest_files / pending_ops', () => {
  const { open, dbPath } = tmpDb();
  const db = open();
  const before = counts(db);
  const logs = [];

  const boot = bootstrapLegacy(
    db,
    [{ path: '/tmp/fake-session-a.jsonl', mtime: T0, size: 10, lastEventTs: T0 + 1000 }],
    T0,
    { dryRun: true, log: (line) => logs.push(line) },
  );
  assert.equal(boot.pending, 1, 'dryRun 也要把账算出来');
  assert.deepEqual(counts(db), before, 'bootstrapLegacy 不许写表');

  const ack = ackFile(db, '/tmp/fake-session-a.jsonl', T0 + 1000, { dryRun: true, log: (line) => logs.push(line) });
  assert.equal(ack.dryRun, true);
  assert.deepEqual(counts(db), before, 'ackFile 不许写表');

  const fail = failFile(db, '/tmp/fake-session-a.jsonl', new Error('boom'), { dryRun: true, log: (line) => logs.push(line) });
  assert.equal(fail.dryRun, true);
  assert.deepEqual(counts(db), before, 'failFile 不许写表');

  const op = enqueue(db, 'ingest_file', '/tmp/fake-session-a.jsonl', { path: '/tmp/fake-session-a.jsonl' }, {
    dryRun: true,
    log: (line) => logs.push(line),
  });
  assert.equal(op.id, null, 'dryRun 入队返回合成行，不给真 id');
  assert.deepEqual(counts(db), before, 'enqueue 不许写表');

  const marked = markOp(db, 1, 'done', null, { dryRun: true, log: (line) => logs.push(line) });
  assert.equal(marked.dryRun, true);
  assert.deepEqual(counts(db), before, 'markOp 不许写表');

  assert.equal(logs.length, 5, '五个入口各留一行日志');
  db.close();
  assert.ok(fs.existsSync(dbPath));
});

test('sleepPass(dryRun=true, ingestAck=true) 跑完两张表仍是零行', async () => {
  const { dir, open } = tmpDb();
  const db = open();
  const sessionsRoot = path.join(dir, 'sessions');
  const file = putSession(sessionsRoot, [
    { time: T0 - 5000, type: 'user/message', data: { source: { kind: 'user' }, content: [{ type: 'text', text: '小林，今天机器几度？' }] } },
    { time: T0 - 4000, type: 'assistant/message', data: { message: { content: [{ type: 'text', text: '45℃，还行。' }] } } },
  ]);

  const writes = [];
  const store = {
    db: () => db,
    listAll: () => [],
    metas: () => [],
    vectorIndex: () => new Map(),
    typeOf: () => 'world',
    async save() { writes.push('save'); throw new Error('dryRun 不许 save'); },
    async merge() { writes.push('merge'); throw new Error('dryRun 不许 merge'); },
    async update() { writes.push('update'); throw new Error('dryRun 不许 update'); },
    async archive() { writes.push('archive'); throw new Error('dryRun 不许 archive'); },
  };
  const ctx = { logger: { info() {} }, llm: fakeLlm, tools: { get: () => null, register() {} }, provide() {}, effect: (fn) => fn() };
  const cfg = {
    storePath: dir,
    sessionsRoot,
    stateFile: path.join(dir, 'sleep.state.json'),
    backupDir: path.join(dir, 'backup'),
    sleepLogFile: path.join(dir, 'memory_sleep.log'),
    splitStateFile: path.join(dir, 'split.state.json'),
    splitLogFile: path.join(dir, 'memory_split.log'),
    splitPreviewFile: path.join(dir, 'split-preview.md'),
    notifyScript: '/bin/true',
    notify: false,
    provider: 'fake',
    model: 'fake',
    maxChanges: 5,
    maxSessions: 5,
    maxMessageChars: 1500,
    sleepMaxTokens: 512,
    mergeMaxTokens: 512,
    splitMaxTokens: 512,
    clusterMaxSize: 30,
    clusterNearDup: 0.9,
    clusterQuestionMin: 0.78,
    clusterChanges: 6,
    organizeMaxCalls: 1,
    organizeConcurrency: 1,
    sleepSplitEnabled: false,
    conflictCheckEnabled: false,
    sleepTools: false,
    pipeline: { ingestAck: true, ingestBudget: 5, replayBudget: 5, writeGate: true, agentPipeline: true },
  };

  const before = counts(db);
  const sleep = setupSleep(ctx, store, cfg);
  const result = await sleep.sleepPass({ dryRun: true });
  const after = counts(db);

  assert.equal(result.dryRun, true);
  assert.deepEqual(after, before, 'dryRun 跑完整条流程也不许写账本表');
  assert.deepEqual(after, { ingest: 0, pending: 0 });
  assert.deepEqual(writes, [], 'dryRun 不许调任何写库函数');
  assert.equal(result.ingest.processed, 1, '应当处理到那个会话文件');
  assert.equal(result.ingest.events, 2, '账目要数到事件数');
  assert.ok(fs.existsSync(file));
  db.close();
});
