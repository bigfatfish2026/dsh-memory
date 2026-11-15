/**
 * t-fix-first-run 第 3 处：超长正文一律不丢不截，改入队 pending_ops(kind='split_item')。
 *
 * 覆盖四个长度样本（首跑丢掉的那四条）：249 / 444 / 375 / 365 字，都没有句末标点。
 * 三处调用点各来一遍：
 *   1. lib/sleep.js 的 applyActions（add / update / merge）；
 *   2. lib/consolidate.js（memory_consolidate 工具，真走 store.save 前的那条路）；
 *   3. lib/store.js 的 memory_merge 工具（不再是直接抛错）。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { applySchema } from '../scripts/apply-pipeline-schema.mjs';
import { createStore, registerStoreTools } from '../lib/store.js';
import { setupSleep } from '../lib/sleep.js';
import { setupConsolidate } from '../lib/consolidate.js';
import { needsSplitText, previewClampText, MEMORY_TEXT_MAX } from '../lib/text-limit.js';

const VEC_SO = path.join(import.meta.dirname, '../node_modules/sqlite-vec-linux-arm64/vec0.so');

const MEMORY_DDL = `CREATE TABLE memories (
  id TEXT PRIMARY KEY, text TEXT NOT NULL, tags TEXT, source TEXT, ts TEXT, type TEXT,
  entities TEXT, validFrom TEXT, supersedes TEXT, updatedAt TEXT, status TEXT, reason TEXT,
  supersededBy TEXT, validTo TEXT, confidence REAL, extra TEXT, present TEXT, rowSeq INTEGER NOT NULL
);`;

/** 四个样本：全是长句、没有句末标点（首跑就是因为「截不短」被整条丢掉的形状）。 */
const SENTENCE = '树莓派温度四十五度风扇转速三千转磁盘用了百分之十五内存还剩两吉字节负载一点二持续了六小时';
const sample = (chars) => SENTENCE.repeat(Math.ceil(chars / SENTENCE.length)).slice(0, chars);

const SAMPLES = [249, 444, 375, 365].map((n) => ({ n, text: sample(n) }));

function makeStore({ writeGate = false, llm = null } = {}) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-split-queue-'));
  const dbPath = path.join(dir, 'memory.db');
  const db = new DatabaseSync(dbPath, { allowExtension: true });
  db.loadExtension(VEC_SO);
  db.exec(MEMORY_DDL);
  db.exec('CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);');
  db.exec("INSERT INTO meta (key, value) VALUES ('dim', '4');");
  db.exec('CREATE VIRTUAL TABLE vec_items USING vec0(embedding float[4] distance_metric=cosine);');
  db.exec('CREATE TABLE vec_map (rowid INTEGER PRIMARY KEY, id TEXT);');
  db.prepare(`INSERT INTO memories (id, text, tags, type, status, ts, rowSeq) VALUES (?, ?, '[]', 'world', 'active', '2026-11-01T00:00:00Z', 1)`)
    .run('m_src', '树莓派温度 45 度，风扇转速 3000 转');
  db.prepare('INSERT INTO vec_map (rowid, id) VALUES (1, ?)').run('m_src');
  db.prepare('INSERT INTO vec_items (rowid, embedding) VALUES (?, ?)')
    .run(1n, new Uint8Array(Float32Array.from([1, 0, 0, 0]).buffer));
  db.close();
  applySchema(dbPath, { logger: () => {} });

  const cfg = {
    storePath: dir,
    storeLogFile: path.join(dir, 'store.log'),
    sleepLogFile: path.join(dir, 'sleep.log'),
    splitLogFile: path.join(dir, 'split.log'),
    apiUrl: 'http://127.0.0.1:1',
    timeoutMs: 50,
    provider: 'fake',
    model: 'fake',
    splitMaxChars: MEMORY_TEXT_MAX,
    consolidateTools: false,
    notifyScript: '/bin/true',
    embed: async (texts) => texts.map(() => [1, 0, 0, 0]),
    logger: { info() {}, warn() {} },
    pipeline: { writeGate },
  };
  const store = createStore(cfg);
  if (llm) store.attachLlm(llm);
  return { store, dir, dbPath, cfg };
}

function makeSleep(store, cfg) {
  const ctx = {
    logger: { info() {}, warn() {} }, llm: null, tools: { register() {} },
    provide() {}, effect(fn) { const stop = fn(); return () => stop?.(); },
  };
  return setupSleep(ctx, store, cfg);
}

function rows(dbPath, sql, ...params) {
  const db = new DatabaseSync(dbPath, { readOnly: true });
  try {
    return db.prepare(sql).all(...params);
  } finally {
    db.close();
  }
}

function count(dbPath, sql, ...params) {
  return Number(rows(dbPath, sql, ...params)[0]?.n ?? 0);
}

const logged = () => [];

test('长度口径：四个样本都判超长，预览截断只压提示词副本、不返回空串', () => {
  for (const { n, text } of SAMPLES) {
    assert.equal(text.length, n);
    assert.equal(needsSplitText(text), true, `${n} 字应判超长`);
    const preview = previewClampText(text);
    assert.notEqual(preview.text, '', `${n} 字预览不许被丢成空串`);
    assert.ok(preview.text.length <= MEMORY_TEXT_MAX);
    assert.equal(preview.original, n);
  }
  assert.equal(needsSplitText('树莓派温度 45 度。'), false);
});

test('sleep applyActions：add 249 字 → 入队拆分，原样入 payload，不落库', async () => {
  const { store, dbPath, cfg } = makeStore();
  const sleep = makeSleep(store, cfg);
  const { n, text } = SAMPLES[0];
  const result = await sleep.__applyActions({ add: [{ text, tags: ['infra'] }], merge: [], update: [], archive: [] }, {
    existing: store.metas(), dryRun: false, limit: 10,
  });
  assert.equal(result.added.length, 0);
  assert.equal(result.split.length, 1);
  assert.equal(result.split[0].length, n);
  assert.equal(result.rejected.length, 0, '超长不是「坏条」，不许进 rejected');
  const ops = rows(dbPath, "SELECT dedupe_key, payload_json FROM pending_ops WHERE kind='split_item'");
  assert.equal(ops.length, 1);
  const payload = JSON.parse(ops[0].payload_json);
  assert.equal(payload.text, text, '入队正文必须一字不差');
  assert.equal(payload.length, n);
  assert.equal(payload.op, 'add');
  assert.equal(payload.source_id, '');
  assert.equal(count(dbPath, 'SELECT count(*) AS n FROM memories'), 1);
});

test('sleep applyActions：update 444 字 → 原条一个字不动，新正文入队', async () => {
  const { store, dbPath, cfg } = makeStore();
  const sleep = makeSleep(store, cfg);
  const { n, text } = SAMPLES[1];
  const result = await sleep.__applyActions({ add: [], merge: [], update: [{ id: 'm_src', text }], archive: [] }, {
    existing: store.metas(), dryRun: false, limit: 10,
  });
  assert.equal(result.updated.length, 0);
  assert.equal(result.split.length, 1);
  const ops = rows(dbPath, "SELECT dedupe_key, payload_json FROM pending_ops WHERE kind='split_item'");
  assert.equal(ops.length, 1);
  assert.equal(ops[0].dedupe_key, 'm_src');
  const payload = JSON.parse(ops[0].payload_json);
  assert.equal(payload.text, text);
  assert.equal(payload.op, 'update');
  assert.equal(payload.length, n);
  assert.equal(count(dbPath, "SELECT count(*) AS n FROM memories WHERE text='树莓派温度 45 度，风扇转速 3000 转'"), 1);
});

test('sleep applyActions：merge 375 字 → 源条不合并，合并正文入队并带全 ids', async () => {
  const { store, dbPath, cfg } = makeStore();
  const sleep = makeSleep(store, cfg);
  const { n, text } = SAMPLES[2];
  const result = await sleep.__applyActions({ add: [], update: [], archive: [], merge: [{ ids: ['m_src', 'm_other'], text }] }, {
    existing: store.metas(), dryRun: false, limit: 10,
  });
  // m_other 不在库里，只剩 1 个有效 id → 按老规矩「ids<2」拒绝；为了让 merge 路径真的跑到入队，补一条假条目
  const existing = [...store.metas(), { id: 'm_other', text: '树莓派风扇转速 3000 转，今天正常', tags: [], type: 'world', status: 'active' }];
  const result2 = await sleep.__applyActions({ add: [], update: [], archive: [], merge: [{ ids: ['m_src', 'm_other'], text }] }, {
    existing, dryRun: false, limit: 10,
  });
  assert.equal(result.rejected.length, 1, '第一次只有 1 个有效 id，按 ids<2 拒绝');
  assert.equal(result2.merged.length, 0);
  assert.equal(result2.split.length, 1);
  assert.equal(result2.split[0].length, n);
  const ops = rows(dbPath, "SELECT payload_json FROM pending_ops WHERE kind='split_item'");
  assert.equal(ops.length, 1);
  const payload = JSON.parse(ops[0].payload_json);
  assert.equal(payload.text, text);
  assert.equal(payload.op, 'merge');
  assert.deepEqual(payload.ids, ['m_src', 'm_other']);
  assert.equal(count(dbPath, "SELECT count(*) AS n FROM memories WHERE status='merged'"), 0);
});

test('consolidate：365 字条目入队拆分，一条都不写库', async () => {
  const { store, dbPath, cfg } = makeStore();
  const { n, text } = SAMPLES[3];
  const llm = {
    calls: [],
    stream(options) {
      this.calls.push(options);
      return (async function* chunks() {
        yield { type: 'text-delta', text: JSON.stringify([{ text, type: 'world', tags: ['infra'] }]) };
        yield { type: 'finish', reason: { kind: 'stop' } };
      })();
    },
  };
  const tools = [];
  const ctx = { llm, on() {}, tools: { register: (t) => tools.push(t) }, logger: { info() {}, warn() {} } };
  setupConsolidate(ctx, store, cfg);
  const tool = tools.find((t) => t.name === 'memory_consolidate');
  // 整理入口 200 字符以下的资料直接跳过，所以给一段够长的记录
  const transcript = Array.from({ length: 40 }, (_, i) => `第 ${i} 句：树莓派今天有点热，风扇一直在转。`).join('\n');
  const out = await tool.execute({ transcript }, {});
  assert.equal(out.saved, 0);
  assert.equal(out.items, 1);
  const ops = rows(dbPath, "SELECT dedupe_key, payload_json FROM pending_ops WHERE kind='split_item'");
  assert.equal(ops.length, 1);
  const payload = JSON.parse(ops[0].payload_json);
  assert.equal(payload.text, text);
  assert.equal(payload.length, n);
  assert.equal(payload.batch, 'consolidator');
  assert.equal(count(dbPath, 'SELECT count(*) AS n FROM memories'), 1);
});

test('memory_merge 工具：超长不再抛错，入队拆分并给明确提示', async () => {
  const { store, dbPath } = makeStore();
  const tools = [];
  const ctx = { tools: { register: (t) => tools.push(t) }, logger: { info() {}, warn() {} } };
  registerStoreTools(ctx, store, { logger: { info() {}, warn() {} } });
  const tool = tools.find((t) => t.name === 'memory_merge');
  const { n, text } = SAMPLES[0];
  const out = await tool.execute({ ids: 'm_src,m_other', text }, {});
  assert.equal(out.queued, true);
  assert.equal(out.length, n);
  assert.equal(out.merged, 0);
  assert.equal(out.id.startsWith('queued:'), true);
  const rendered = tool.output.render({}, out)[0].text;
  assert.match(rendered, /入队拆分/);
  assert.match(rendered, /原条一条都没动/);
  const ops = rows(dbPath, "SELECT payload_json FROM pending_ops WHERE kind='split_item'");
  assert.equal(ops.length, 1);
  const payload = JSON.parse(ops[0].payload_json);
  assert.equal(payload.text, text);
  assert.deepEqual(payload.ids, ['m_src', 'm_other']);
  assert.equal(payload.batch, 'memory_merge');
  assert.equal(count(dbPath, "SELECT count(*) AS n FROM memories WHERE status='merged'"), 0);
});

test('memory_save 工具：超长不再退回，入队拆分且原文一字不动', async () => {
  const { store, dbPath } = makeStore();
  const tools = [];
  const ctx = { tools: { register: (t) => tools.push(t) }, logger: { info() {}, warn() {} } };
  registerStoreTools(ctx, store, { logger: { info() {}, warn() {} } });
  const tool = tools.find((t) => t.name === 'memory_save');
  const { n, text } = SAMPLES[1];
  const out = await tool.execute({ text, tags: 'infra', type: 'world' }, {});
  assert.equal(out.queued, true);
  assert.equal(out.saved, false);
  assert.equal(out.length, n);
  const rendered = tool.output.render({}, out)[0].text;
  assert.match(rendered, /已入队拆分/);
  assert.match(rendered, /原文没丢也没截/);
  const ops = rows(dbPath, "SELECT payload_json FROM pending_ops WHERE kind='split_item'");
  assert.equal(ops.length, 1);
  const payload = JSON.parse(ops[0].payload_json);
  assert.equal(payload.text, text);
  assert.equal(payload.batch, 'memory_save');
  assert.deepEqual(payload.tags, ['infra']);
  assert.equal(count(dbPath, 'SELECT count(*) AS n FROM memories'), 1);
});
