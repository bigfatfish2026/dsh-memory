/**
 * 会话入库按文件 ack（docs/sleep-pipeline-redesign.md §3、§4）的离线测试。
 * 全部在临时目录造会话文件 + 临时库，不碰真实会话目录和真库。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import zlib from 'node:zlib';
import { DatabaseSync } from 'node:sqlite';
import { applySchema } from '../scripts/apply-pipeline-schema.mjs';
import {
  MAX_ATTEMPTS,
  LEASE_MS,
  ackFile,
  backfill,
  bootstrapLegacy,
  dueOps,
  enqueue,
  failFile,
  markOp,
  planFiles,
  scanSessions,
} from '../lib/ingest-index.js';
import { eventsToTranscript, lastEventTsOf, readSessionEvents } from '../lib/transcript.js';

const T0 = Date.parse('2026-11-01T00:00:00Z');
const LAST_SLEEP = T0 + 3600 * 1000; // 旧/新文件的分界

function evUser(time, text) {
  return { time, type: 'user/message', data: { source: { kind: 'user' }, content: [{ type: 'text', text }] } };
}

function evAssistant(time, text) {
  return { time, type: 'assistant/message', data: { message: { content: [{ type: 'text', text }] } } };
}

function evTool(time) {
  return { time, type: 'tool/result', data: { ok: true } };
}

function put(file, events, { zstd = false, mtime = 0 } = {}) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  const body = `${events.map((e) => JSON.stringify(e)).join('\n')}\n`;
  if (zstd) fs.writeFileSync(file, zlib.zstdCompressSync(Buffer.from(body, 'utf8')));
  else fs.writeFileSync(file, body);
  if (mtime) fs.utimesSync(file, mtime / 1000, mtime / 1000);
  return file;
}

function tmpSessionRoot() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-ingest-'));
  const base = path.join(dir, 'sessions');
  fs.mkdirSync(path.join(base, 'agent-a', 's1'), { recursive: true });
  return { dir, base };
}

function tmpDb() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-ingest-db-'));
  const dbPath = path.join(dir, 'memory.db');
  new DatabaseSync(dbPath).close();
  applySchema(dbPath, { logger: () => {} });
  return { dir, dbPath, open: () => new DatabaseSync(dbPath) };
}

function seedFour(base, mtimes) {
  const oldJsonl = put(path.join(base, 'agent-a', 's1', 'session-old.jsonl'), [evUser(T0, '旧问题'), evAssistant(T0 + 1000, '旧回答'), evTool(T0 + 1500)], { mtime: mtimes[0] });
  const oldZstd = put(
    path.join(base, 'agent-a', 's1', 'session-old2.jsonl.zstd'),
    [evUser(T0 + 2000, '旧问题二'), evAssistant(T0 + 2500, '旧回答二')],
    { zstd: true, mtime: mtimes[1] },
  );
  const newJsonl = put(path.join(base, 'agent-a', 's1', 'session-new.jsonl'), [evUser(LAST_SLEEP + 60000, '新问题'), evAssistant(LAST_SLEEP + 61000, '新回答')], { mtime: mtimes[2] });
  const newZstd = put(
    path.join(base, 'agent-a', 's1', 'session-new2.jsonl.zstd'),
    [evUser(LAST_SLEEP + 120000, '新问题二'), evAssistant(LAST_SLEEP + 121000, '新回答二')],
    { zstd: true, mtime: mtimes[3] },
  );
  // 非会话文件必须被忽略
  fs.writeFileSync(path.join(base, 'agent-a', 's1', 'notes.txt'), 'x');
  return { oldJsonl, oldZstd, newJsonl, newZstd };
}

test('scanSessions 递归找 jsonl 与 .zstd，忽略非会话文件', () => {
  const { base } = tmpSessionRoot();
  const files = seedFour(base, [T0 + 5000, T0 + 6000, T0 + 7000, T0 + 8000]);
  const scanned = scanSessions(base);
  assert.equal(scanned.length, 4);
  const paths = scanned.map((f) => f.path).sort();
  assert.deepEqual(paths, [files.newJsonl, files.newZstd, files.oldJsonl, files.oldZstd].sort());
  for (const f of scanned) {
    assert.ok(f.size > 0);
    assert.ok(f.mtime > 0);
  }
  // zstd 能被同一套解码读出来
  const events = readSessionEvents(files.newZstd);
  assert.equal(events.length, 2);
  assert.equal(lastEventTsOf(events), LAST_SLEEP + 121000);
});

test('bootstrap 只把最后事件时间 <= lastSleepAt 的文件标 legacy_acked，且幂等', () => {
  const { base } = tmpSessionRoot();
  const files = seedFour(base, [T0 + 5000, T0 + 6000, T0 + 7000, T0 + 8000]);
  const { open } = tmpDb();
  const db = open();
  const files4 = scanSessions(base);

  const boot = bootstrapLegacy(db, files4, LAST_SLEEP, { now: T0 + 9999 });
  assert.equal(boot.scanned, 4);
  assert.equal(boot.legacy, 2);
  assert.equal(boot.pending, 2);
  assert.equal(boot.errors, 0);

  const row = (p) => db.prepare('SELECT * FROM ingest_files WHERE path = ?').get(p);
  assert.equal(row(files.oldJsonl).status, 'legacy_acked');
  assert.equal(row(files.oldJsonl).last_acked_ts, T0 + 1500); // 最后合法事件时间，tool 事件也算
  assert.equal(row(files.oldZstd).status, 'legacy_acked');
  assert.equal(row(files.oldZstd).last_acked_ts, T0 + 2500);
  assert.equal(row(files.newJsonl).status, 'pending');
  assert.equal(row(files.newJsonl).last_acked_ts, LAST_SLEEP); // 不回头补 lastSleepAt 之前
  assert.equal(row(files.newZstd).status, 'pending');

  // 二次 bootstrap 不重复写、不改已有行
  const again = bootstrapLegacy(db, files4, LAST_SLEEP, { now: T0 + 20000 });
  assert.equal(again.legacy, 0);
  assert.equal(again.pending, 0);
  assert.equal(again.skipped, 4);
  assert.equal(Number(db.prepare('SELECT count(*) AS n FROM ingest_files').get().n), 4);
  db.close();

  const plan = planFiles(open(), scanSessions(base), { lastSleepAt: LAST_SLEEP, budget: 20 });
  assert.equal(plan.todo.length, 2);
  assert.equal(plan.legacy.length, 0);
  assert.equal(plan.done.length, 2);
});

test('budget 只截本轮处理量：超额留在 pending，不丢、下轮还能捞回来', () => {
  const { base } = tmpSessionRoot();
  const files = seedFour(base, [T0 + 5000, T0 + 6000, T0 + 7000, T0 + 8000]);
  const { open } = tmpDb();
  const db = open();
  bootstrapLegacy(db, scanSessions(base), LAST_SLEEP);
  const all = scanSessions(base);

  const p1 = planFiles(db, all, { lastSleepAt: LAST_SLEEP, budget: 1 });
  assert.equal(p1.todo.length, 1);
  assert.equal(p1.overBudget.length, 1);
  assert.equal(p1.done.length, 2);
  // 排序：旧账先补（last_acked_ts 小的先）→ 两个新文件的 last_acked_ts 一样，按 mtime
  assert.equal(p1.todo[0].path, files.newJsonl);
  assert.equal(p1.overBudget[0].path, files.newZstd);

  // 表里两条都还是 pending，没被 budget 弄丢
  assert.equal(Number(db.prepare("SELECT count(*) AS n FROM ingest_files WHERE status='pending'").get().n), 2);

  const p2 = planFiles(db, all, { lastSleepAt: LAST_SLEEP, budget: 20 });
  assert.equal(p2.todo.length, 2);
  assert.equal(p2.overBudget.length, 0);

  // 2026-11-14：不传 budget = 不截断（ingestBudget 删了），全部返回
  const pAll = planFiles(db, all, { lastSleepAt: LAST_SLEEP });
  assert.equal(pAll.todo.length, 2);
  assert.equal(pAll.overBudget.length, 0);
  db.close();
});

test('文件被追加后重新 pending，ack 之后又回到 done', () => {
  const { base } = tmpSessionRoot();
  const files = seedFour(base, [T0 + 5000, T0 + 6000, T0 + 7000, T0 + 8000]);
  const { open } = tmpDb();
  const db = open();
  bootstrapLegacy(db, scanSessions(base), LAST_SLEEP);

  const first = scanSessions(base);
  const item = first.find((f) => f.path === files.newJsonl);
  const events = readSessionEvents(item.path);
  ackFile(db, item.path, lastEventTsOf(events), { now: T0 + 30000 });

  let plan = planFiles(db, scanSessions(base), { lastSleepAt: LAST_SLEEP, budget: 20 });
  assert.equal(plan.todo.length, 1); // 只剩 newZstd
  assert.ok(!plan.todo.some((t) => t.path === files.newJsonl));

  // 追加一条新事件（mtime 和 size 都变），必须重新进 pending
  const line = JSON.stringify(evUser(LAST_SLEEP + 900000, '追加的问题'));
  fs.appendFileSync(files.newJsonl, `${line}\n`);
  fs.utimesSync(files.newJsonl, (T0 + 40000) / 1000, (T0 + 40000) / 1000);

  plan = planFiles(db, scanSessions(base), { lastSleepAt: LAST_SLEEP, budget: 20 });
  const back = plan.todo.find((t) => t.path === files.newJsonl);
  assert.ok(back);
  assert.equal(back.reason, 'changed');
  assert.equal(back.lastAckedTs, LAST_SLEEP + 61000); // 只读新增段
  assert.equal(back.lastEventTs, LAST_SLEEP + 900000);

  // 同一文件再 ack 就回到 done
  ackFile(db, files.newJsonl, back.lastEventTs, { now: T0 + 50000 });
  plan = planFiles(db, scanSessions(base), { lastSleepAt: LAST_SLEEP, budget: 20 });
  assert.ok(plan.done.includes(files.newJsonl));

  // legacy_acked 的文件被追加时也要重新进 pending（§3.2 第 2 条）
  const oldEvents = readSessionEvents(files.oldJsonl);
  assert.ok(lastEventTsOf(oldEvents) < LAST_SLEEP);
  fs.appendFileSync(files.oldJsonl, `${JSON.stringify(evUser(LAST_SLEEP + 1800000, '历史文件被写了新东西'))}\n`);
  fs.utimesSync(files.oldJsonl, (T0 + 60000) / 1000, (T0 + 60000) / 1000);
  plan = planFiles(db, scanSessions(base), { lastSleepAt: LAST_SLEEP, budget: 20 });
  const legacyBack = plan.todo.find((t) => t.path === files.oldJsonl);
  assert.ok(legacyBack);
  assert.equal(legacyBack.lastAckedTs, T0 + 1500);
  assert.equal(legacyBack.lastEventTs, LAST_SLEEP + 1800000);
  db.close();
});

test('失败 attempts 递增、不排退避（next_try_at=now），attempts 到 5 转 dead 且不再自动进队', () => {
  const { base } = tmpSessionRoot();
  const files = seedFour(base, [T0 + 5000, T0 + 6000, T0 + 7000, T0 + 8000]);
  const { open } = tmpDb();
  const db = open();
  bootstrapLegacy(db, scanSessions(base), LAST_SLEEP);
  const target = files.newJsonl;
  const now = T0 + 100000;

  for (let i = 1; i <= MAX_ATTEMPTS; i += 1) {
    const at = now + i;
    const f = failFile(db, target, new Error(`boom-${i}`), { now: at });
    assert.equal(f.attempts, i);
    // 2026-11-14：删掉 1/5/15 分钟退避——失败当场可再领，判死只认 attempts=5
    assert.equal(f.nextTryAt, at);
    assert.equal(f.status, i >= MAX_ATTEMPTS ? 'dead' : 'failed');
    const row = db.prepare('SELECT * FROM ingest_files WHERE path = ?').get(target);
    assert.equal(row.attempts, i);
    assert.equal(row.last_error, `boom-${i}`);
    assert.equal(row.last_acked_ts, LAST_SLEEP); // 失败不推进 ack
    assert.equal(row.status, i >= MAX_ATTEMPTS ? 'dead' : 'failed');
    const op = db.prepare("SELECT * FROM pending_ops WHERE kind='ingest_file' AND dedupe_key=?").get(target);
    assert.equal(op.attempts, i);
    assert.equal(op.status, i >= MAX_ATTEMPTS ? 'dead' : 'pending');
    assert.equal(op.next_try_at, at);
  }
  // attempts 到 5 之后 planFiles 不再自动捞它
  const plan = planFiles(db, scanSessions(base), { lastSleepAt: LAST_SLEEP, budget: 20 });
  assert.ok(!plan.todo.some((t) => t.path === target));
  assert.ok(!plan.overBudget.some((t) => t.path === target));
  // dueOps 也不会领 dead
  const due = dueOps(db, 50, { kind: 'ingest_file', now: now + 10 * 60000 });
  assert.ok(!due.some((o) => o.dedupeKey === target));
  db.close();
});

test('pending_ops 幂等入队、按到期领取、失败 attempts 到 5 转 dead', () => {
  const { open } = tmpDb();
  const db = open();
  const now = T0 + 500000;

  const first = enqueue(db, 'ingest_file', '/tmp/session-a.jsonl', { path: '/tmp/session-a.jsonl' }, { now });
  const again = enqueue(db, 'ingest_file', '/tmp/session-a.jsonl', { path: '/tmp/session-a.jsonl' }, { now: now + 1000 });
  assert.equal(first.id, again.id);
  assert.equal(Number(db.prepare('SELECT count(*) AS n FROM pending_ops').get().n), 1);

  // 还没到期不领
  assert.equal(dueOps(db, 10, { kind: 'ingest_file', now: now - 1 }).length, 0);
  const due = dueOps(db, 10, { kind: 'ingest_file', now });
  assert.equal(due.length, 1);
  assert.equal(due[0].payload.path, '/tmp/session-a.jsonl');
  // 领走后是 running，不会被同一轮重复领
  assert.equal(dueOps(db, 10, { kind: 'ingest_file', now }).length, 0);
  assert.equal(db.prepare('SELECT status FROM pending_ops WHERE id = ?').get(first.id).status, 'running');

  // 失败回队：attempts 递增、next_try_at=now（不排退避），attempts 到 5 判死
  let op = markOp(db, first.id, 'pending', 'read_fail', { now: now + 2000 });
  assert.equal(op.status, 'pending');
  assert.equal(op.attempts, 1);
  assert.equal(op.next_try_at, now + 2000);
  // 没到 5 次：当场就又能领
  assert.equal(dueOps(db, 10, { kind: 'ingest_file', now: now + 2000 }).length, 1);
  op = markOp(db, first.id, 'pending', 'parse_fail', { now: now + 80000 });
  assert.equal(op.attempts, 2);
  op = markOp(db, first.id, 'pending', 'read_fail', { now: now + 90000 });
  assert.equal(op.attempts, 3);
  op = markOp(db, first.id, 'pending', 'read_fail', { now: now + 95000 });
  assert.equal(op.attempts, 4);
  assert.equal(op.status, 'pending', '第 4 次还活着');
  op = markOp(db, first.id, 'pending', 'read_fail', { now: now + 100000 });
  assert.equal(op.status, 'dead');
  assert.equal(op.attempts, 5);
  assert.equal(dueOps(db, 10, { kind: 'ingest_file', now: now + 999999 }).length, 0);

  // 成功标 done，入队不会把 done 重置回 pending
  enqueue(db, 'ingest_file', '/tmp/session-a.jsonl', { path: '/tmp/session-a.jsonl' }, { now: now + 200000 });
  assert.equal(db.prepare('SELECT status FROM pending_ops WHERE id = ?').get(first.id).status, 'dead');

  // 别的 kind 不会被 ingest_file 的领取误领
  enqueue(db, 'merge_batch', 'fp_1', { batch: 'fp_1' }, { now });
  assert.ok(dueOps(db, 10, { kind: 'ingest_file', now }).every((o) => o.kind === 'ingest_file'));
  db.close();
});

test('--backfill：path 重置到 0；since 取 min(当前 ack, 该日)，不强制覆盖', () => {
  const { base } = tmpSessionRoot();
  const files = seedFour(base, [T0 + 5000, T0 + 6000, T0 + 7000, T0 + 8000]);
  const { open } = tmpDb();
  const db = open();
  bootstrapLegacy(db, scanSessions(base), LAST_SLEEP);
  ackFile(db, files.newJsonl, LAST_SLEEP + 61000, { now: T0 + 30000 });

  const byPath = backfill(db, files.newJsonl, { files: scanSessions(base) });
  assert.equal(byPath.mode, 'path');
  const row = db.prepare('SELECT * FROM ingest_files WHERE path = ?').get(files.newJsonl);
  assert.equal(row.status, 'pending');
  assert.equal(row.last_acked_ts, 0);
  assert.equal(row.attempts, 0);

  // since 早于当前 ack：ack 退回该日 00:00
  const sinceMs = Date.parse('2026-11-01T00:00:00');
  const bySince = backfill(db, '2026-11-01', { files: scanSessions(base) });
  assert.equal(bySince.mode, 'since');
  assert.equal(bySince.since, sinceMs);
  const legacyRow = db.prepare('SELECT * FROM ingest_files WHERE path = ?').get(files.oldZstd);
  // 2026-11-01 00:00（本地）比 T0 早 8 小时，旧文件也在该日之后 → 一起回捞，ack 压到该日 00:00
  assert.equal(legacyRow.status, 'pending');
  assert.equal(legacyRow.last_acked_ts, sinceMs);
  const changed = db.prepare("SELECT * FROM ingest_files WHERE status='pending'").all();
  assert.ok(changed.length >= 1);
  for (const r of changed) {
    assert.ok(r.last_acked_ts <= sinceMs, `${r.path} 的 ack 没被压到 since 之前`);
  }

  // since 晚于当前 ack：不强制覆盖，取 min
  ackFile(db, files.newJsonl, LAST_SLEEP + 61000, { now: T0 + 40000 });
  backfill(db, '2026-11-02', { files: scanSessions(base) });
  const kept = db.prepare('SELECT * FROM ingest_files WHERE path = ?').get(files.newJsonl);
  assert.equal(kept.last_acked_ts, LAST_SLEEP + 61000);
  db.close();
});

test('eventsToTranscript 只留人机对话，候选文本与老抽取口径一致', () => {
  const cfg = { maxMessageChars: 1500 };
  const events = [
    evUser(T0, '你好'),
    { time: T0 + 1, type: 'user/message', data: { source: { kind: 'subagent' }, content: [{ type: 'text', text: '后台回填' }] } },
    { time: T0 + 2, type: 'user/message', data: { content: [{ type: 'text', text: 'Current runtime context: xxx' }] } },
    evAssistant(T0 + 3, '在的'),
    evTool(T0 + 4),
    evUser(T0 + 5, '旧问题'),
  ];
  const kept = eventsToTranscript(cfg, events, T0 + 3);
  assert.equal(kept.messages, 1); // 你好 / 在的 都被 since 卡掉（> since 才留），只剩旧问题
  assert.equal(kept.lastEventTs, T0 + 5);
  assert.ok(kept.lines[0].includes('用户: 旧问题'));
  const all = eventsToTranscript(cfg, events, 0);
  assert.equal(all.messages, 3); // 你好 / 在的 / 旧问题
});

test('dueOps 回收过期 running：attempts+1，到 5 判 dead 并留 lease_expired（毒账不会长生）', () => {
  const { open } = tmpDb();
  const db = open();
  const now = T0 + 900000;
  const op = enqueue(db, 'ingest_file', '/tmp/poison.jsonl', { path: '/tmp/poison.jsonl' }, { now });
  assert.equal(dueOps(db, 10, { kind: 'ingest_file', now }).length, 1, '先领成 running');
  const lease = LEASE_MS;
  for (let i = 1; i < MAX_ATTEMPTS; i += 1) {
    const at = now + i * (lease + 1);
    const got = dueOps(db, 10, { kind: 'ingest_file', now: at, leaseMs: lease });
    assert.equal(got.length, 1, `第 ${i} 次过期后还能被领到`);
    assert.equal(got[0].attempts, i, '回收也算一次 attempts');
  }
  const deadAt = now + MAX_ATTEMPTS * (lease + 1);
  assert.equal(dueOps(db, 10, { kind: 'ingest_file', now: deadAt, leaseMs: lease }).length, 0, '判死后不再领');
  const row = db.prepare('SELECT status, attempts, last_error FROM pending_ops WHERE id = ?').get(op.id);
  assert.equal(row.status, 'dead');
  assert.equal(Number(row.attempts), MAX_ATTEMPTS);
  assert.match(String(row.last_error), /^lease_expired/);
  db.close();
});

test('dueOps 先占后取：SELECT 之后被抢走的行不派出去（CAS 带 status=pending）', () => {
  const { open } = tmpDb();
  const db = open();
  const now = T0 + 100;
  enqueue(db, 'ingest_file', '/tmp/race.jsonl', { path: '/tmp/race.jsonl' }, { now });
  // 模拟另一个池在 SELECT 与 CAS 之间抢走它：只拦 SELECT，把行原地改成 running 再返回
  const racing = {
    prepare(sql) {
      const stmt = db.prepare(sql);
      if (sql.trim().startsWith('SELECT * FROM pending_ops')) {
        return {
          all: (...args) => {
            const rows = stmt.all(...args);
            if (rows.length) {
              db.prepare("UPDATE pending_ops SET status = 'running', updated_ts = ? WHERE id = ?").run(now, rows[0].id);
            }
            return rows;
          },
        };
      }
      return stmt;
    },
    exec(sql) { return db.exec(sql); },
  };
  assert.deepEqual(dueOps(racing, 10, { kind: 'ingest_file', now }), [], '被抢走的行 changes=0，跳过');
  db.close();
});

test('failFile 三步同事务：pending_ops 写失败时 ingest_files 一起回滚，账不分叉', () => {
  const { base } = tmpSessionRoot();
  const files = seedFour(base, [T0 + 5000, T0 + 6000, T0 + 7000, T0 + 8000]);
  const { open } = tmpDb();
  const db = open();
  bootstrapLegacy(db, scanSessions(base), LAST_SLEEP, { now: T0 + 9999 });
  const before = db.prepare('SELECT * FROM ingest_files WHERE path = ?').get(files.newJsonl);
  const faulty = {
    prepare(sql) {
      if (/^UPDATE pending_ops SET attempts = \?/.test(sql.trim())) throw new Error('inject: pending_ops 写失败');
      return db.prepare(sql);
    },
    exec(sql) { return db.exec(sql); },
  };
  assert.throws(() => failFile(faulty, files.newJsonl, new Error('boom'), { now: T0 + 20000 }), /inject/);
  const after = db.prepare('SELECT * FROM ingest_files WHERE path = ?').get(files.newJsonl);
  assert.equal(Number(after.attempts), Number(before.attempts), 'ingest_files 的 attempts 一起回滚');
  assert.equal(after.status, before.status, 'status 一起回滚');
  assert.equal(after.last_error, before.last_error, 'last_error 一起回滚');
  assert.equal(Number(db.prepare("SELECT count(*) AS n FROM pending_ops WHERE kind='ingest_file'").get().n), 0, '入队也一起回滚');

  // 正常路径：两表 attempts/状态同口径
  const ok = failFile(db, files.newJsonl, new Error('boom'), { now: T0 + 21000 });
  assert.equal(ok.attempts, 1);
  const fileRow = db.prepare('SELECT attempts, status FROM ingest_files WHERE path = ?').get(files.newJsonl);
  const opRow = db.prepare("SELECT attempts, status FROM pending_ops WHERE kind='ingest_file' AND dedupe_key = ?").get(files.newJsonl);
  assert.equal(Number(fileRow.attempts), 1);
  assert.equal(Number(opRow.attempts), 1);
  assert.equal(fileRow.status, 'failed');
  assert.equal(opRow.status, 'pending');
  db.close();
});
