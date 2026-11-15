/**
 * 跨层端到端：复核 agent 判 keep → validateOps → store.assertTextLint → 真的写进 memories。
 *
 * 为什么单开一个文件：merge-agent.test.mjs 只断言到
 * `merge()` 的返回值带 __lintKept，中间隔着 validateOps 那次「重新构造 op」。
 * 那次重建曾经写成 `raw.__lintKept === true`，而复核给了理由时 __lintKept 是**字符串**，
 * 于是标记被吞、store 又按普通不合规拒一次，keep 白判、误杀照样落 discards。
 * 真库里 2026-11-06 23:23 就这么丢掉了一条。这个文件专门盯穿这三层。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { applySchema } from '../scripts/apply-pipeline-schema.mjs';
import { createStore } from '../lib/store.js';
import { merge, validateOps } from '../lib/agents/merge.js';
import { lintMarksOf, lintReviewMeta } from '../lib/lint-review.js';

const VEC_SO = path.join(import.meta.dirname, '../node_modules/sqlite-vec-linux-arm64/vec0.so');

const MEMORY_DDL = `CREATE TABLE memories (
  id TEXT PRIMARY KEY, text TEXT NOT NULL, tags TEXT, source TEXT, ts TEXT, type TEXT,
  entities TEXT, validFrom TEXT, supersedes TEXT, updatedAt TEXT, status TEXT, reason TEXT,
  supersededBy TEXT, validTo TEXT, confidence REAL, extra TEXT, present TEXT, rowSeq INTEGER NOT NULL
);`;

/** 源条正文：留一条合格正文当基线，keep 的误杀条目要靠它改。 */
const SOURCE = '树莓派温度 45 度，2026-11-07 重启后 /srv/app 磁盘用了 90%。';
/** 代码 lint 必打回：「但是」开头（leading_conjunction）；45 在 evidence 里，证据校验能过。 */
const BAD = '但是树莓派温度 45 度。';
/** 另一条不相干的合格正文：判重得有候选，只有自己一条时 dedupe 直接 DISTINCT 放行。 */
const OTHER = '根分区已用 15%，这是 df -h 的输出；备份快照在 /srv/app/memory/backup。';
const KEEP_REASON = '「但是」承接的是同一条记忆里的前半句，这一条独立成篇反而丢了上下文';

function tmpDir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-lint-e2e-'));
}

function makeStore({ writeGate = false } = {}) {
  const workDir = tmpDir();
  const dbPath = path.join(workDir, 'memory.db');
  const db = new DatabaseSync(dbPath);
  db.exec(MEMORY_DDL);
  db.close();
  applySchema(dbPath, { logger: () => {} });

  const ext = new DatabaseSync(dbPath, { allowExtension: true });
  ext.loadExtension(VEC_SO);
  ext.exec("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);");
  if (!ext.prepare("SELECT value FROM meta WHERE key = 'dim'").get()) {
    ext.exec("INSERT INTO meta (key, value) VALUES ('dim', '4');");
  }
  ext.exec('CREATE VIRTUAL TABLE IF NOT EXISTS vec_items USING vec0(embedding float[4] distance_metric=cosine);');
  ext.exec('CREATE TABLE IF NOT EXISTS vec_map (rowid INTEGER PRIMARY KEY, id TEXT);');
  ext.close();

  const cfg = {
    storePath: workDir,
    storeLogFile: path.join(workDir, 'store.log'),
    apiUrl: 'http://127.0.0.1:1',
    timeoutMs: 50,
    provider: 'fake',
    model: 'fake',
    rrfK: 60,
    opinionAlpha: 0.1,
    splitMaxChars: 200,
    embed: async (texts) => texts.map(() => [1, 0, 0, 0]),
    pipeline: writeGate ? { writeGate: true, writeGateThreshold: 0.9 } : {},
    logger: { info() {}, warn() {} },
  };
  return { store: createStore(cfg), dbPath, workDir };
}

function fakeLlm(handlers) {
  const calls = [];
  return {
    calls,
    stream(options) {
      calls.push(options);
      const handler = handlers[Math.min(calls.length - 1, handlers.length - 1)];
      return (async function* streamChunks() {
        for (const chunk of handler(options)) yield chunk;
      })();
    },
  };
}

const toolChunks = (args) => [
  { type: 'tool-call-delta', index: 0, id: 'call_1', name: 'submit_ops', argumentsDelta: args.slice(0, 20) },
  { type: 'tool-call-delta', index: 0, argumentsDelta: args.slice(20) },
  { type: 'finish', reason: { kind: 'stop' } },
];

const textChunks = (text) => [
  { type: 'text-delta', text },
  { type: 'finish', reason: { kind: 'stop' } },
];

function readAll(dbPath, sql, ...params) {
  const db = new DatabaseSync(dbPath, { readOnly: true });
  try {
    return db.prepare(sql).all(...params);
  } finally {
    db.close();
  }
}

/** 造一条 update op：把源条改成 BAD（代码 lint 必打回），证据逐字来自源条。 */
function badUpdateOp(id) {
  return [{
    op: 'update',
    ids: [id],
    text: BAD,
    evidence: [{ id, quote: SOURCE }],
    needsSplit: false,
    reason: '同一件事收短',
  }];
}

test('lintMarksOf：复核理由字符串和 true 两种形态都原样透传', () => {
  assert.deepEqual(lintMarksOf({ __lintKept: KEEP_REASON }), { __lintKept: KEEP_REASON });
  assert.deepEqual(lintMarksOf({ __lintKept: true }), { __lintKept: true });
  assert.deepEqual(lintMarksOf({}), {}, '没标记就是空对象，别凭空造一个');
  assert.deepEqual(lintMarksOf({ __lintKept: false }), {}, 'false 不算标记');
  // 已经是 store 形态的 meta 也要带过去
  const meta = { __lintReview: { verdict: 'keep', reason: 'x' } };
  assert.deepEqual(lintMarksOf(meta), meta);
});

test('validateOps 不许吞 __lintKept：字符串理由要活到 store 那一步', () => {
  const items = [{ id: 'm_1', text: SOURCE }];
  const ops = badUpdateOp('m_1').map((op) => ({ ...op, __lintKept: KEEP_REASON }));
  const checked = validateOps(ops, items, { log: () => {} });
  assert.equal(checked.invalid.length, 0, '证据与事实都过得去，不该进 invalid');
  assert.equal(checked.valid.length, 1);
  assert.equal(checked.valid[0].__lintKept, KEEP_REASON, '这一行以前会被 === true 判掉');
  // 落到 write 那一步的形态
  assert.equal(lintReviewMeta(checked.valid[0]).__lintReview.verdict, 'keep');
  assert.equal(lintReviewMeta(checked.valid[0]).__lintReview.reason, KEEP_REASON);
});

test('端到端：复核 keep → validateOps → store 放行 → memories 增行、原文一字不改', async () => {
  const { store, dbPath } = makeStore();
  const saved = await store.save(SOURCE, [], 'test', { type: 'world' });
  assert.ok(saved?.id, '基线条目要先入库');
  const before = readAll(dbPath, 'SELECT count(*) AS n FROM memories')[0].n;

  const items = [{ id: String(saved.id), text: SOURCE }];
  const llm = fakeLlm([
    () => toolChunks(JSON.stringify({ ops: badUpdateOp(String(saved.id)) })),
    () => textChunks(JSON.stringify({ decisions: [{ index: 0, verdict: 'keep', reason: KEEP_REASON }] })),
  ]);

  // 第一层：agent 产出 + 复核裁决
  const answer = await merge({ items, llm, provider: 'fake', model: 'fake', clusterId: 'e2e' });
  assert.equal(answer.ops.length, 1, 'keep 的 op 要带出来');
  assert.equal(answer.ops[0].__lintKept, KEEP_REASON, '复核理由是字符串，得原样带着');

  // 第二层：证据校验（这一步以前会吞标记）
  const checked = validateOps(answer.ops, items, { maxChars: 200, log: () => {} });
  assert.equal(checked.valid.length, 1);
  assert.equal(checked.valid[0].__lintKept, KEEP_REASON);

  // 第三层：真写库，store 的最后一道闸要认这个 keep
  const op = checked.valid[0];
  const rec = await store.update(String(saved.id), { text: op.text, ...lintReviewMeta(op) });
  assert.equal(rec.text, BAD, '复核放行的正文原样入库');

  const rows = readAll(dbPath, 'SELECT id, text, status FROM memories WHERE id = ?', saved.id);
  assert.equal(rows.length, 1);
  assert.equal(rows[0].text, BAD, '正文一字不改，不做任何机器改写');
  assert.notEqual(rows[0].status, 'archived');
  assert.notEqual(rows[0].status, 'merged');

  assert.equal(readAll(dbPath, 'SELECT count(*) AS n FROM memories')[0].n, before, 'update 不增行');
  assert.equal(readAll(dbPath, "SELECT id FROM discards WHERE verdict = 'LINT_REJECT'").length, 0, 'keep 的条目不许再落 discards');

  const log = fs.readFileSync(path.join(dbPath, '..', 'store.log'), 'utf8');
  assert.match(log, /\[lint\] 复核放行 update/, '放行要留一条 INFO，能追是谁放行的');
  assert.match(log, /unverified_claim|leading_conjunction/, '日志里要看得到被放行的 code');
  store.close?.();
});

test('反例：同一段正文丢了 keep 标记，store 照样拒并落 discards', async () => {
  const { store, dbPath } = makeStore();
  const saved = await store.save(SOURCE, [], 'test', { type: 'world' });
  await assert.rejects(store.update(String(saved.id), { text: BAD }), { code: 'LINT_REJECT' });
  const row = readAll(dbPath, 'SELECT text FROM memories WHERE id = ?', saved.id)[0];
  assert.equal(row.text, SOURCE, '旧正文一个字不动');
  const discards = readAll(dbPath, "SELECT text, source_file FROM discards WHERE verdict = 'LINT_REJECT'");
  assert.equal(discards.length, 1);
  assert.equal(discards[0].text, BAD, '原文一字不动地留证');
  assert.equal(discards[0].source_file, 'lint_reject');
  store.close?.();
});

test('端到端：复核判 redo → 不带 keep 标记，重做后仍不过就落 discards', async () => {
  const { store, dbPath } = makeStore();
  const saved = await store.save(SOURCE, [], 'test', { type: 'world' });
  const items = [{ id: String(saved.id), text: SOURCE }];
  // 落 discards 要 db：merge 不传 db 就不落账
  const mergeDb = new DatabaseSync(dbPath);
  // 第 1 轮：不过 lint；复核判 redo；第 2 轮重做仍然吐同一段 BAD（`retries.lint` 只给一次）
  const llm = fakeLlm([
    () => toolChunks(JSON.stringify({ ops: badUpdateOp(String(saved.id)) })),
    () => textChunks(JSON.stringify({ decisions: [{ index: 0, verdict: 'redo', reason: '「但是」确实没头没尾' }] })),
    () => toolChunks(JSON.stringify({ ops: badUpdateOp(String(saved.id)) })),
  ]);
  const answer = await merge({ items, llm, provider: 'fake', model: 'fake', clusterId: 'e2e-redo', db: mergeDb });
  assert.equal(answer.retries.lint, 1, '重做只给一次');
  assert.equal(answer.lintRejected, 1, '重做后仍不过的要计入 lintRejected');
  assert.equal(answer.ops.length, 0, '重做后仍不过的条目不许带出去');
  const discards = readAll(dbPath, "SELECT text, reason FROM discards WHERE verdict = 'LINT_REJECT'");
  assert.equal(discards.length, 1, '重做仍不过要留证');
  assert.equal(discards[0].text, BAD);
  assert.match(discards[0].reason, /leading_conjunction/);
  assert.equal(readAll(dbPath, 'SELECT count(*) AS n FROM memories')[0].n, 1, '库里只有基线那一条');
  mergeDb.close();
  store.close?.();
});

test('写入闸未决落账时把复核结论一起带走：重放才不会再判死一次', async () => {
  const { store, dbPath } = makeStore({ writeGate: true });
  // 判重 llm 两次都吐垃圾 → store 抛 WRITE_GATE_PENDING，候选落 pending_ops
  const badJudge = {
    stream() {
      return (async function* chunks() {
        yield { type: 'text-delta', text: '这边不是 JSON，也没工具调用' };
        yield { type: 'finish', reason: { kind: 'stop' } };
      })();
    },
  };
  // 先铺两条基线：__gateChecked 只跳过写入闸，lint 照走（闸还没注入 llm，开着也没法判）
  const saved = await store.save(SOURCE, [], 'test', { type: 'world', __gateChecked: true });
  // 库里得还有一条别的，判重才有候选；只有自己一条时 dedupe 直接 DISTINCT 放行，压根不调模型
  await store.save(OTHER, [], 'test', { type: 'world', __gateChecked: true });
  store.attachLlm(badJudge);

  await assert.rejects(
    store.update(String(saved.id), { text: BAD, __lintReview: { verdict: 'keep', reason: KEEP_REASON } }),
    { code: 'WRITE_GATE_PENDING' },
  );

  const rows = readAll(dbPath, "SELECT payload_json AS payload FROM pending_ops WHERE kind = 'dedupe_check'");
  assert.equal(rows.length, 1, '未决要落一条账');
  const payload = JSON.parse(rows[0].payload);
  assert.equal(payload.candidate.text, BAD);
  assert.deepEqual(payload.review, { verdict: 'keep', reason: KEEP_REASON }, '复核结论必须跟着账走');
  assert.equal(readAll(dbPath, "SELECT id FROM discards WHERE verdict = 'LINT_REJECT'").length, 0, '有复核结论就不该先落 discard');
  store.close?.();
});

test('写入闸未决但没复核结论时，账里不留 review 字段（别凭空造标记）', async () => {
  const { store, dbPath } = makeStore({ writeGate: true });
  const badJudge = {
    stream() {
      return (async function* chunks() {
        yield { type: 'text-delta', text: '这边不是 JSON' };
        yield { type: 'finish', reason: { kind: 'stop' } };
      })();
    },
  };
  const saved = await store.save(SOURCE, [], 'test', { type: 'world', __gateChecked: true });
  await store.save(OTHER, [], 'test', { type: 'world', __gateChecked: true });
  store.attachLlm(badJudge);
  const GOOD_NEW = '树莓派温度 45 度，2026-11-07 重启后磁盘用了 90%。';
  await assert.rejects(
    store.update(String(saved.id), { text: GOOD_NEW }),
    { code: 'WRITE_GATE_PENDING' },
  );
  const rows = readAll(dbPath, "SELECT payload_json AS payload FROM pending_ops WHERE kind = 'dedupe_check'");
  assert.equal(rows.length, 1);
  assert.equal('review' in JSON.parse(rows[0].payload), false);
  store.close?.();
});
