/**
 * 拆分那一路的计数接进汇总行。
 *
 * 以前 consumeSplitOps 的入库 / 判重 / 归档只在两行日志里出现，不进 result 计数器，
 * 「睡觉完成」那行天生比库小（汇总 新增17/拒绝63，库里实际 新增17+拆分入库93、落账 67）。
 * 这里跑一整轮 sleepPass（临时库 + 假模型）钉住：
 *   - summary 的 splitInserted / splitDeduped / splitArchived = 实际写库 / 判重 / 归档行数；
 *   - 「拒绝」= discards 落账行数（拆分段判重也各落一行）；
 *   - 「新增」不被拆分污染（只算 agent 直接 add）；
 *   - 汇总行同时保留旧的「拆分X条/尝试Y」字段。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { applySchema } from '../scripts/apply-pipeline-schema.mjs';
import { createStore } from '../lib/store.js';
import { setupSleep } from '../lib/sleep.js';
import { runSplitPass } from '../lib/split-pass.js';
import { enqueue } from '../lib/ingest-index.js';

const VEC_SO = path.join(import.meta.dirname, '../node_modules/sqlite-vec-linux-arm64/vec0.so');

const MEMORY_DDL = `CREATE TABLE memories (
  id TEXT PRIMARY KEY, text TEXT NOT NULL, tags TEXT, source TEXT, ts TEXT, type TEXT,
  entities TEXT, validFrom TEXT, supersedes TEXT, updatedAt TEXT, status TEXT, reason TEXT,
  supersededBy TEXT, validTo TEXT, confidence REAL, extra TEXT, present TEXT, rowSeq INTEGER NOT NULL
);`;

const S1 = '2026-11-07 早上八点，小林把 dsh-memory 的汇总行补齐，拆分三样单列计数。';
const S2 = '上午十点，真库 memories 四千七百九十五行，vec_map 四千七百九十八行。';
const S3 = '下午两点，拆分 agent 的提示词定稿，每段不超过两百字。';
const S4 = '晚上八点，日志里 split ratio 零点九八，没有正文被截断。';
const SOURCE = `${S1}${S2}${S3}${S4}`;

const toolChunks = (args) => [
  { type: 'tool-call-delta', index: 0, id: 'call_1', name: 'submit_parts', argumentsDelta: args.slice(0, 20) },
  { type: 'tool-call-delta', index: 0, argumentsDelta: args.slice(20) },
  { type: 'finish', reason: { kind: 'stop' } },
];
const textChunks = (text) => [
  { type: 'text-delta', text },
  { type: 'finish', reason: { kind: 'stop' } },
];
const verdictChunks = (args) => [
  { type: 'tool-call-delta', index: 0, id: 'call_v', name: 'submit_verdict', argumentsDelta: args },
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

function makeStore({ llm, seedRows }) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-sleep-split-'));
  const dbPath = path.join(dir, 'memory.db');
  const db = new DatabaseSync(dbPath, { allowExtension: true });
  db.loadExtension(VEC_SO);
  db.exec(MEMORY_DDL);
  db.exec('CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);');
  db.exec("INSERT INTO meta (key, value) VALUES ('dim', '4');");
  db.exec('CREATE VIRTUAL TABLE vec_items USING vec0(embedding float[4] distance_metric=cosine);');
  db.exec('CREATE TABLE vec_map (rowid INTEGER PRIMARY KEY, id TEXT);');
  let seq = 1;
  const insertMemory = db.prepare("INSERT INTO memories (id, text, tags, type, status, ts, rowSeq) VALUES (?, ?, '[]', 'world', ?, '2026-11-01T00:00:00Z', ?)");
  const insertVecMap = db.prepare('INSERT INTO vec_map (rowid, id) VALUES (?, ?)');
  const insertVec = db.prepare('INSERT INTO vec_items (rowid, embedding) VALUES (?, ?)');
  for (const row of seedRows) {
    insertMemory.run(row.id, row.text, row.status || 'active', seq);
    insertVecMap.run(seq, row.id);
    insertVec.run(BigInt(seq), new Uint8Array(Float32Array.from(row.vec).buffer));
    seq += 1;
  }
  db.close();
  applySchema(dbPath, { logger: () => {} });

  const cfg = {
    storePath: dir,
    storeLogFile: path.join(dir, 'store.log'),
    sleepLogFile: path.join(dir, 'sleep.log'),
    splitLogFile: path.join(dir, 'split.log'),
    splitStateFile: path.join(dir, 'split.state.json'),
    splitPreviewFile: path.join(dir, 'split-preview.md'),
    stateFile: path.join(dir, 'sleep.state.json'),
    backupDir: path.join(dir, 'backup'),
    sessionsRoot: path.join(dir, 'sessions'),
    notifyScript: '/bin/true',
    notify: false,
    apiUrl: 'http://127.0.0.1:1',
    timeoutMs: 50,
    provider: 'fake',
    model: 'fake',
    maxChanges: 20,
    maxSessions: 5,
    maxMessageChars: 1500,
    sleepMaxTokens: 512,
    mergeMaxTokens: 512,
    splitMaxTokens: 512,
    splitMaxChars: 200,
    clusterMaxSize: 30,
    clusterNearDup: 0.9,
    clusterQuestionMin: 0.78,
    clusterChanges: 6,
    organizeMaxCalls: 1,
    organizeConcurrency: 1,
    sleepSplitEnabled: true,
    sleepSplitLimit: 20,
    conflictCheckEnabled: false,
    sleepTools: false,
    embed: async (texts) => texts.map(() => [1, 0, 0, 0]),
    logger: { info() {}, warn() {} },
    pipeline: {
      ingestAck: true,
      ingestBudget: 5,
      replayBudget: 5,
      writeGate: true,
      writeGateThreshold: 0.9,
      textLint: false,
      lintReview: false,
      agentPipeline: true,
    },
  };
  const store = createStore(cfg);
  store.attachLlm(llm);
  return { store, dir, dbPath, cfg };
}

function putSession(root, events) {
  const dir = path.join(root, 'agent-a', 's1');
  fs.mkdirSync(dir, { recursive: true });
  const file = path.join(dir, 'session-split.jsonl');
  fs.writeFileSync(file, `${events.map((e) => JSON.stringify(e)).join('\n')}\n`, 'utf8');
  return file;
}

function rows(dbPath, sql, ...params) {
  const db = new DatabaseSync(dbPath, { readOnly: true });
  try {
    return db.prepare(sql).all(...params);
  } finally {
    db.close();
  }
}

test('睡一整轮：拆分三样进 summary，拒绝=discards 行数，新增不被拆分污染', async () => {
  const T0 = Date.now();
  // 2026-11-15 切分点协议：模型只给第二、三段开头的定位串，正文由代码 slice
  const cuts = [S2.slice(0, 10), S3.slice(0, 10)];
  // 调用顺序：阶段一抽动作 → 切分 submit_parts → 逐段过写入闸（DISTINCT）
  const llm = fakeLlm([
    () => textChunks('{"add":[],"merge":[],"update":[],"archive":[]}'),
    () => toolChunks(JSON.stringify({ cuts, unavoidable: false, reason: '' })),
    () => verdictChunks(JSON.stringify({ verdict: 'DISTINCT', matchedId: '', reason: '不同事实' })),
  ]);
  const { store, dir, dbPath, cfg } = makeStore({
    llm,
    seedRows: [
      { id: 'm_src', text: SOURCE, vec: [1, 0, 0, 0] },
      // 跟 S2 一字不差：写入闸走 hash 规则直接判重，不花模型调用
      { id: 'm_dup', text: S2, vec: [0, 1, 0, 0] },
    ],
  });
  putSession(cfg.sessionsRoot, [
    { time: T0 - 5000, type: 'user/message', data: { source: { kind: 'user' }, content: [{ type: 'text', text: '小林，今天的汇总对上了吗？' }] } },
    { time: T0 - 4000, type: 'assistant/message', data: { message: { content: [{ type: 'text', text: '在查，拆分那三样还没进汇总。' }] } } },
  ]);
  const db = new DatabaseSync(dbPath);
  enqueue(db, 'split_item', 'm_src', { source_id: 'm_src', text: SOURCE, type: 'world' });
  db.close();

  const sleep = setupSleep(
    { logger: { info() {} }, llm, tools: { register() {} }, provide() {}, effect: (fn) => fn() },
    store,
    cfg,
  );
  const result = await sleep.sleepPass({});
  assert.equal(result.ran, true);

  // 实际写库 / 判重 / 归档的行数
  const insertedRows = rows(dbPath, "SELECT id, text FROM memories WHERE source = 'sleep:split:m_src'");
  const discardRows = rows(dbPath, 'SELECT * FROM discards');
  const srcRow = rows(dbPath, 'SELECT status, reason FROM memories WHERE id = ?', 'm_src')[0];

  assert.equal(insertedRows.length, 2, 'S2 被判重，只有 S1/S3 入库');
  assert.equal(discardRows.length, 1, '判重那段在 discards 落一行账');
  assert.equal(srcRow.status, 'archived', '拆完原条要归档');
  assert.match(srcRow.reason, /拆成 2 条/);

  // summary 三样 = 实际行数
  assert.equal(result.splitInserted, insertedRows.length);
  assert.equal(result.splitDeduped, discardRows.length);
  assert.equal(result.splitArchived, 1);

  // 「拒绝」= discards 行数（2026-11-15 起汇总按轮首/轮尾差值单列）；「新增」只算 agent 直接 add
  assert.equal(result.discards, discardRows.length);
  assert.equal(result.rejected.length, discardRows.length, '这个场景里提案打回恰好也是 discards 数');
  assert.equal(result.added.length, 0);

  // 汇总行：三样单列，旧的「拆分X条/尝试Y」字段还在
  const logText = fs.readFileSync(cfg.sleepLogFile, 'utf8');
  const line = logText.split('\n').filter((l) => l.includes('睡觉完成')).at(-1) || '';
  assert.match(line, /新增0 /);
  assert.match(line, /拒绝1\(discards\)/);
  assert.match(line, /打回1\(提案\)/);
  assert.match(line, /拆分0条\/尝试0/);
  assert.match(line, /拆分入库2\/判重1\/归档1/);
  assert.equal(fs.existsSync(path.join(dir, 'memory_sleep.log')), false, '临时库的日志只许写 cfg 指定的文件');
});

test('阶段三 split-pass 也报三样：入库/判重/归档 = 实际行数', async () => {
  const llm = fakeLlm([
    () => textChunks(JSON.stringify({
      items: [
        { text: S1, type: 'world', entities: [], validFrom: '' },
        { text: S2, type: 'world', entities: [], validFrom: '' },
      ],
    })),
  ]);
  // 阶段三只捞正文 >200 字的 active 条目，所以源条要够长
  const longSource = `${SOURCE}${SOURCE}`;
  const { store, dbPath, cfg } = makeStore({
    llm,
    seedRows: [
      { id: 'm_src', text: longSource, vec: [1, 0, 0, 0] },
      // 跟第二个子段一字不差：写入闸 hash 规则判重，落一行 discards
      { id: 'm_dup', text: S2, vec: [0, 1, 0, 0] },
    ],
  });

  const out = await runSplitPass({ store, llm, cfg, dryRun: false, limit: 5 });

  assert.equal(out.split, 1, '源条拆开成功');
  assert.equal(out.inserted, 1, 'S1 入库');
  assert.equal(out.deduped, 1, 'S2 被判重');
  assert.equal(out.archived, 1, '源条归档');
  const discardRows = rows(dbPath, 'SELECT * FROM discards');
  assert.equal(out.inserted, rows(dbPath, "SELECT id FROM memories WHERE source = 'split'").length);
  assert.equal(out.deduped, discardRows.length);
  assert.equal(out.archived, rows(dbPath, 'SELECT id FROM memories WHERE id = ? AND status = ?', 'm_src', 'archived').length);
});
