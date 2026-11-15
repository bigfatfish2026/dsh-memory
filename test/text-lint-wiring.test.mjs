import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { applySchema } from '../scripts/apply-pipeline-schema.mjs';
import { createStore } from '../lib/store.js';
import { split } from '../lib/agents/split.js';

const VEC_SO = path.join(import.meta.dirname, '../node_modules/sqlite-vec-linux-arm64/vec0.so');

const MEMORY_DDL = `CREATE TABLE memories (
  id TEXT PRIMARY KEY, text TEXT NOT NULL, tags TEXT, source TEXT, ts TEXT, type TEXT,
  entities TEXT, validFrom TEXT, supersedes TEXT, updatedAt TEXT, status TEXT, reason TEXT,
  supersededBy TEXT, validTo TEXT, confidence REAL, extra TEXT, present TEXT, rowSeq INTEGER NOT NULL
);`;

const BAD = '但是小林把闸挪了。';
const GOOD1 = '小林在 2026-11-06 把正文 lint 接进 store.js 的 saveMemory，闸在第 530 行。';
const GOOD2 = '根分区已用 15%，数字来自 df -h 的输出，这块盘还够用一段时间。';

function tmpDir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-lint-wire-'));
}

function makeDb(dir) {
  const dbPath = path.join(dir, 'memory.db');
  const db = new DatabaseSync(dbPath);
  db.exec(MEMORY_DDL);
  db.close();
  applySchema(dbPath, { logger: () => {} });
  return dbPath;
}

/** 最小 store：够 save / update / merge / archive 走完写入路径。 */
function makeStore({ textLint, dir } = {}) {
  const workDir = dir || tmpDir();
  const dbPath = makeDb(workDir);
  const db = new DatabaseSync(dbPath, { allowExtension: true });
  db.loadExtension(VEC_SO);
  db.exec("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);");
  if (!db.prepare("SELECT value FROM meta WHERE key = 'dim'").get()) {
    db.exec("INSERT INTO meta (key, value) VALUES ('dim', '4');");
  }
  db.exec('CREATE VIRTUAL TABLE IF NOT EXISTS vec_items USING vec0(embedding float[4] distance_metric=cosine);');
  db.exec('CREATE TABLE IF NOT EXISTS vec_map (rowid INTEGER PRIMARY KEY, id TEXT);');
  db.close();

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
    pipeline: textLint === undefined ? {} : { textLint },
    logger: { info() {}, warn() {} },
  };
  const store = createStore(cfg);
  return { store, cfg, dbPath, dir: workDir };
}

function readAll(dbPath, sql, ...params) {
  const db = new DatabaseSync(dbPath, { readOnly: true });
  try {
    return db.prepare(sql).all(...params);
  } finally {
    db.close();
  }
}

/* ─────────────── store 层：最后一道闸 ─────────────── */

test('store.save：正文不过 lint 就抛 LINT_REJECT，并落一行 lint_reject', async () => {
  const { store, dbPath } = makeStore();
  await assert.rejects(store.save(BAD, [], 'test', { type: 'world' }), (error) => {
    assert.equal(error.code, 'LINT_REJECT');
    assert.equal(Array.isArray(error.violations), true);
    assert.equal(error.text, BAD);
    return true;
  });
  assert.equal(readAll(dbPath, 'SELECT id FROM memories').length, 0, '一条都不许入库');
  const discards = readAll(dbPath, 'SELECT * FROM discards');
  assert.equal(discards.length, 1);
  assert.equal(discards[0].verdict, 'LINT_REJECT');
  assert.equal(discards[0].source_file, 'lint_reject');
  assert.equal(discards[0].text, BAD, '原文一字不动地留下');
  assert.match(discards[0].reason, /leading_conjunction:/);
});

test('store.save：合格正文照常入库，字数与内容不改', async () => {
  const { store, dbPath } = makeStore();
  const rec = await store.save(GOOD1, [], 'test', { type: 'world' });
  assert.ok(rec && rec.id);
  const rows = readAll(dbPath, 'SELECT text FROM memories WHERE id = ?', rec.id);
  assert.equal(rows[0].text, GOOD1);
});

test('store.save：cfg.pipeline.textLint=false 时同一段短正文照旧入库', async () => {
  const { store, dbPath } = makeStore({ textLint: false });
  const rec = await store.save(BAD, [], 'test', { type: 'world' });
  assert.ok(rec && rec.id);
  assert.equal(readAll(dbPath, 'SELECT id FROM memories').length, 1);
  assert.equal(readAll(dbPath, 'SELECT * FROM discards').length, 0);
});

test('store.update：新正文不过 lint 就拒，旧正文一个字不动', async () => {
  const { store, dbPath } = makeStore();
  const rec = await store.save(GOOD1, [], 'test', { type: 'world' });
  await assert.rejects(store.update(rec.id, { text: BAD }), { code: 'LINT_REJECT' });
  const row = readAll(dbPath, 'SELECT text, status FROM memories WHERE id = ?', rec.id)[0];
  assert.equal(row.text, GOOD1, '旧正文一个字不动');
  assert.notEqual(row.status, 'archived');
  assert.notEqual(row.status, 'merged');
});

test('store.merge：合并后的正文不过 lint 就拒，源条一条不动', async () => {
  const { store, dbPath } = makeStore();
  const a = await store.save(GOOD1, [], 'test', { type: 'world' });
  const b = await store.save(GOOD2, [], 'test', { type: 'world' });
  await assert.rejects(store.merge([a.id, b.id], BAD), { code: 'LINT_REJECT' });
  const rows = readAll(dbPath, 'SELECT id, status, text FROM memories WHERE id IN (?, ?)', a.id, b.id);
  assert.equal(rows.length, 2, '源条一条不少');
  assert.equal(rows.some((row) => row.status === 'merged'), false, '源条不许被标 merged');
  assert.equal(readAll(dbPath, "SELECT id FROM memories WHERE source = 'sleep:merge'").length, 0);
});

/* ─────────────── split：一次重切，二次失败记 lint_reject ─────────────── */

/**
 * 2026-11-15：切分点协议下，「重写一次」改成「换切点一次」。
 * 首版把「收尾。」单独切出来（3 字 → too_short），重做版把切点前移到上一句开头，
 * 两段都够长就通过；正文全程一字未改，都是原文 slice。
 */
const LT1 = '2026-11-05 早上八点，小林把 dsh-memory 的 jsonl 冻结了，只留作历史副本。';
const LT2 = '下午两点，小林把写入口挪到了 store.js 的第 530 行。';
const TAIL = '收尾。';
const SOURCE = `${LT1}${LT2}${TAIL}`;
const SHORT_CUTS = [TAIL];
const REDO_CUTS = [LT2.slice(0, 12)];

const toolChunks = (args) => [
  { type: 'tool-call-delta', index: 0, id: 'call_1', name: 'submit_parts', argumentsDelta: args.slice(0, 20) },
  { type: 'tool-call-delta', index: 0, argumentsDelta: args.slice(20) },
  { type: 'finish', reason: { kind: 'stop' } },
];

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

const cutsOf = (cuts) => JSON.stringify({ cuts, unavoidable: false, reason: '' });

test('split：两轮都切出残句 → 只重发一次，二次失败记 lint_reject，不入库（lintReview=false 走旧路径）', async () => {
  const { dbPath, dir } = makeStore();
  const db = new DatabaseSync(dbPath);
  // 两轮都给同一份切分方案：第一轮 lint 拒 → 重切一次 → 仍拒
  const llm = fakeLlm([
    () => toolChunks(cutsOf(SHORT_CUTS)),
    () => toolChunks(cutsOf(SHORT_CUTS)),
  ]);
  const logs = [];
  // 关掉两级复核，专测旧的「重切一次就收手」退化路径；复核路径见 test/lint-review.test.mjs
  const result = await split({
    sourceId: 'm_src', text: SOURCE, type: 'world', ts: '2026-11-05',
    db, llm, provider: 'fake', model: 'fake', lintReview: false, log: (line) => logs.push(line),
  });
  db.close();

  assert.equal(llm.calls.length, 2, '第一轮 + 重切一次，绝不循环');
  assert.equal(result, undefined, '剔掉不合格段只剩 1 段，整条退拆');
  assert.equal(logs.some((line) => line.includes('段命中代码标记')), true, '日志要留代码标记这一轮');
  assert.equal(readAll(dbPath, 'SELECT id FROM memories').length, 0, 'memories 表不许有行');

  const discards = readAll(dbPath, 'SELECT * FROM discards');
  assert.equal(discards.length >= 1, true, '二次失败要留 lint_reject 证据');
  assert.equal(discards[0].verdict, 'LINT_REJECT');
  assert.equal(discards[0].source_file, 'lint_reject');
  assert.equal(discards[0].text, TAIL);
  assert.match(discards[0].reason, /too_short:/);
  fs.rmSync(dir, { recursive: true, force: true });
});

test('split：重切后的版本合格 → 两段都入库，不记 lint_reject（lintReview=false 走旧路径）', async () => {
  const { dbPath, dir } = makeStore();
  const db = new DatabaseSync(dbPath);
  const llm = fakeLlm([
    () => toolChunks(cutsOf(SHORT_CUTS)),
    () => toolChunks(cutsOf(REDO_CUTS)),
  ]);
  const result = await split({
    sourceId: 'm_src', text: SOURCE, type: 'world', ts: '2026-11-05',
    db, llm, provider: 'fake', model: 'fake', lintReview: false,
  });
  db.close();

  assert.equal(llm.calls.length, 2);
  assert.ok(result);
  assert.equal(result.parts.length, 2);
  assert.equal(result.parts[0].text, LT1);
  assert.equal(result.parts[1].text, `${LT2}${TAIL}`, '通过的那段仍逐字来自原文，不截断');
  assert.equal(readAll(dbPath, 'SELECT * FROM discards').length, 0);
  fs.rmSync(dir, { recursive: true, force: true });
});
