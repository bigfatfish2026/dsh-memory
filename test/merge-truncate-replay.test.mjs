/**
 * 合并 agent 的切批、留痕与重放（2026-11-05 起，2026-11-14 改）：
 *   1. 一批怎么切由窗口 token 决定（替代 mergeBatchMax=6 的固定条数）；
 *   2. 双通道都坏时模型原文前 600 字留痕（含 finishReason / completionTokens）；
 *   3. 空响应 / 撞墙 / 解析失败**不原地重试固定次数**，直接交上层记账；
 *   4. merge_batch / merge_op / dedupe_check 三种 op 各有一次成功重放；
 *   5. 重放失败不换策略、不转 split，只 attempts+1 + last_error，下一轮再来（不自动放弃）；
 *      老账缺 ids 仍走召回补位，merge_op 仍用 payload.source 再验。
 *
 * 全部在 tmp 目录的副本库上跑，不碰线上 memory.db。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { applySchema } from '../scripts/apply-pipeline-schema.mjs';
import { createStore } from '../lib/store.js';
import { setupSleep, splitAgentBatches, MEMORY_ITEM_OVERHEAD_TOKENS } from '../lib/sleep.js';
import { estimateTokens } from '../lib/token-window.js';
import { enqueue, MAX_ATTEMPTS } from '../lib/ingest-index.js';
import { merge } from '../lib/agents/merge.js';

const VEC_SO = path.join(import.meta.dirname, '../node_modules/sqlite-vec-linux-arm64/vec0.so');

const MEMORY_DDL = `CREATE TABLE memories (
  id TEXT PRIMARY KEY, text TEXT NOT NULL, tags TEXT, source TEXT, ts TEXT, type TEXT,
  entities TEXT, validFrom TEXT, supersedes TEXT, updatedAt TEXT, status TEXT, reason TEXT,
  supersededBy TEXT, validTo TEXT, confidence REAL, extra TEXT, present TEXT, rowSeq INTEGER NOT NULL
);`;

const SEEDS = [
  { id: 'm_1', text: '树莓派温度 45 度。', vec: [1, 0, 0, 0] },
  { id: 'm_2', text: '树莓派风扇转速 3000 转。', vec: [0.8, 0.6, 0, 0] },
  { id: 'm_3', text: '树莓派温度 45 度，风扇 3000 转，今晚重启过。', vec: [0.8, 0, 0.6, 0] },
];

const MERGE_OP = {
  op: 'merge',
  ids: ['m_1', 'm_2'],
  text: '树莓派温度 45 度，风扇转速 3000 转。',
  evidence: [
    { id: 'm_1', quote: '树莓派温度 45 度。' },
    { id: 'm_2', quote: '风扇转速 3000 转。' },
  ],
  needsSplit: false,
  reason: '同一台机器的两条状态',
};

const ITEMS = [
  { id: 'm_1', type: 'world', text: '树莓派温度 45 度。', ts: '2026-11-01T00:00:00Z', entities: ['树莓派'] },
  { id: 'm_2', type: 'world', text: '树莓派风扇转速 3000 转。', ts: '2026-11-01T00:00:00Z', entities: ['树莓派'] },
];

function makeStore({ rows = SEEDS, writeGate = false, clusterQuestionMin = 0.78, contextWindowTokens = 0 } = {}) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-truncate-'));
  const dbPath = path.join(dir, 'memory.db');
  const db = new DatabaseSync(dbPath, { allowExtension: true });
  db.loadExtension(VEC_SO);
  db.exec(MEMORY_DDL);
  db.exec('CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);');
  db.exec("INSERT INTO meta (key, value) VALUES ('dim', '4');");
  db.exec('CREATE VIRTUAL TABLE vec_items USING vec0(embedding float[4] distance_metric=cosine);');
  db.exec('CREATE TABLE vec_map (rowid INTEGER PRIMARY KEY, id TEXT);');
  let seq = 1;
  const insertMemory = db.prepare('INSERT INTO memories (id, text, tags, type, status, ts, entities, rowSeq) VALUES (?, ?, ?, ?, ?, ?, ?, ?)');
  const insertVecMap = db.prepare('INSERT INTO vec_map (rowid, id) VALUES (?, ?)');
  const insertVec = db.prepare('INSERT INTO vec_items (rowid, embedding) VALUES (?, ?)');
  for (const row of rows) {
    insertMemory.run(row.id, row.text, '[]', 'world', 'active', '2026-11-01T00:00:00Z', JSON.stringify(row.entities || ['树莓派']), seq);
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
    stateFile: path.join(dir, 'sleep.state.json'),
    apiUrl: 'http://127.0.0.1:1',
    timeoutMs: 50,
    provider: 'fake',
    model: 'fake',
    rrfK: 60,
    opinionAlpha: 0.1,
    splitMaxChars: 200,
    embed: async (texts) => texts.map(() => [0.9, 0.2, 0.2, 0]),
    pipeline: { agentPipeline: true, writeGate, replayBudget: 50, textLint: false },
    contextWindowTokens,
    clusterNearDup: 0.9,
    clusterQuestionMin,
    organizeConcurrency: 2,
    clusterChanges: 6,
    // 2026-11-14：这几个配置口子已删。故意塞进来，证明代码不再读它们——
    // 真读了的话，下面 30 条那组只会问 1 批 / 只会按 2 条一批切。
    organizeMaxCalls: 1,
    mergeBatchMax: 2,
    sleepMemoryChars: 1000,
    addPassChars: 1000,
    logger: { info() {}, warn() {} },
  };
  const store = createStore(cfg);
  return { store, cfg, dir, dbPath };
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
  { type: 'tool-call-delta', index: 0, id: 'call_1', name: 'submit_ops', argumentsDelta: args },
  { type: 'finish', reason: { kind: 'stop' } },
];

const emptyChunks = () => [
  { type: 'finish', reason: { kind: 'stop' }, usage: { completionTokens: 0 } },
];

const brokenChunks = () => [
  { type: 'text-delta', text: '不是 JSON' },
  { type: 'tool-call-delta', index: 0, name: 'submit_ops', argumentsDelta: '{oops' },
  { type: 'finish', reason: { kind: 'stop' } },
];

const verdictChunks = (args) => [
  { type: 'tool-call-delta', index: 0, id: 'call_v', name: 'submit_verdict', argumentsDelta: args },
  { type: 'finish', reason: { kind: 'stop' } },
];

function itemsOfPrompt(options) {
  const prompt = options.messages[0].content[0].text;
  const m = prompt.match(/待整理记忆\n([\s\S]*?)\n近似候选/);
  assert.ok(m, '提示词里要有待整理记忆段');
  return JSON.parse(m[1]);
}

/** 不管这批有谁，都给第一条一个「原样 update」——只为数调用次数与成员数，不动库内容。 */
function updateFirstItem(options) {
  const items = itemsOfPrompt(options);
  const first = items[0];
  return toolChunks(JSON.stringify({
    ops: [{
      op: 'update',
      ids: [first.id],
      text: first.text,
      evidence: [{ id: first.id, quote: first.text }],
      needsSplit: false,
      reason: '原样重述',
    }],
  }));
}

function query(dbPath, sql, ...params) {
  const db = new DatabaseSync(dbPath, { readOnly: true });
  try {
    return db.prepare(sql).all(...params);
  } finally {
    db.close();
  }
}

function insertPending(dbPath, { kind, key, payload, attempts = 0, nextTryAt = 0, status = 'pending', lastError = null }) {
  const db = new DatabaseSync(dbPath);
  try {
    const now = Date.now();
    db.prepare(`INSERT INTO pending_ops
      (kind, dedupe_key, payload_json, attempts, next_try_at, last_error, status, created_ts, updated_ts)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`)
      .run(kind, key, JSON.stringify(payload), attempts, nextTryAt, lastError, status, now, now);
  } finally {
    db.close();
  }
}

/** 把某类账本改成「立刻可重放」：enqueue 给的是 now+60s，测试不等退避。 */
function makeDue(dbPath, kind) {
  const db = new DatabaseSync(dbPath);
  try {
    db.prepare('UPDATE pending_ops SET next_try_at = 0 WHERE kind = ?').run(kind);
  } finally {
    db.close();
  }
}

function readLog(cfg, needle) {
  try {
    return fs.readFileSync(cfg.sleepLogFile, 'utf8').split('\n').filter((line) => line.includes(needle));
  } catch {
    return [];
  }
}

function makeSleep(store, cfg, llm) {
  return setupSleep({ llm, provide() {}, effect: () => () => {}, tools: { register() {} } }, store, cfg);
}

// ── 1. 满批 30 条不再一批送 ─────────────────────────────────────────────

test('消费侧切批：一批多大由窗口 token 决定（窗口小批多、窗口大一批装完），第二轮全走缓存', async () => {
  const rows = Array.from({ length: 30 }, (_, i) => ({
    id: `m_${i + 1}`,
    text: `树莓派状态第 ${i + 1} 条：温度 ${40 + i} 度。`,
    vec: [1, 0, 0, 0],
    entities: ['树莓派'],
  }));
  // 窗口 1200：输出预留 300、提示词外壳 600，只剩 300 token 给条目 → 必须切多批
  const small = makeStore({ rows, contextWindowTokens: 1200 });
  const llmSmall = fakeLlm([updateFirstItem]);
  const sleepSmall = makeSleep(small.store, small.cfg, llmSmall);
  const out = await sleepSmall.organizePass({ dryRun: false });
  assert.equal(out.clusters, 1, '30 条同实体同向量还是一簇');
  assert.ok(out.batches > 1, `窗口小必须切批，实际 ${out.batches} 批`);
  assert.equal(llmSmall.calls.length, out.batches, '切出几批就问几次，没有额外的固定上限');
  const sizes = llmSmall.calls.map((options) => itemsOfPrompt(options).length);
  assert.equal(sizes.reduce((a, b) => a + b, 0), 30, '30 条一条不丢');
  const cacheSmall = JSON.parse(fs.readFileSync(small.cfg.stateFile, 'utf8')).organizeCache || {};
  assert.equal(Object.keys(cacheSmall).length, out.batches, '每批一个指纹');
  const second = await sleepSmall.organizePass({ dryRun: false });
  assert.equal(second.calls, 0, '指纹没变，第二轮不再问模型');
  assert.equal(second.cached, out.batches);
  assert.equal(llmSmall.calls.length, out.batches);

  // 窗口 200000：同样的 30 条一批全塞进去，不再按条数硬切
  const big = makeStore({ rows, contextWindowTokens: 200000 });
  const llmBig = fakeLlm([updateFirstItem]);
  const sleepBig = makeSleep(big.store, big.cfg, llmBig);
  const bigOut = await sleepBig.organizePass({ dryRun: false });
  assert.equal(bigOut.batches, 1, '窗口装得下就整簇一批');
  assert.equal(bigOut.batchBudget > 0, true);
  assert.equal(itemsOfPrompt(llmBig.calls[0]).length, 30);
});

test('切批助手：按 token 预算切，预算 0（窗口未知）不切', () => {
  const rows = Array.from({ length: 30 }, (_, i) => ({ id: `m_${i + 1}`, text: `第${i + 1}条`, vec: null }));
  const vecOf = new Map(rows.map((r) => [r.id, [1, 0, 0, 0]]));
  const whole = splitAgentBatches({ queue: [{ cluster: rows, score: 1 }], vecOf, budgetTokens: 0 });
  assert.equal(whole.length, 1);
  assert.equal(whole[0].cluster.length, 30, '窗口未知时全塞，不拿拍脑袋条数兜底');
  const cut = splitAgentBatches({ queue: [{ cluster: rows, score: 1 }], vecOf, budgetTokens: 300 });
  assert.ok(cut.length > 1, '预算装不下才切');
  assert.equal(cut.reduce((n, b) => n + b.cluster.length, 0), 30, '一条不丢');
  const cost = (list) => list.reduce((n, m) => n + estimateTokens(m.text) + MEMORY_ITEM_OVERHEAD_TOKENS, 0);
  assert.ok(cut.every((b) => cost(b.cluster) <= 300 || b.cluster.length === 1), '每批都在预算内（单条超预算才独占）');
});

// ── 3. 空响应不再原地重试 ─────────────────────────────────────────────

test('空响应：不原地重试固定次数，一次拿不到就交上层记账', async () => {
  const llm = fakeLlm([emptyChunks, () => toolChunks(JSON.stringify({ ops: [MERGE_OP] }))]);
  const logs = [];
  let summary = '';
  let kind = '';
  const result = await merge({
    items: ITEMS,
    llm,
    provider: 'fake',
    model: 'fake',
    clusterId: 'fp_empty',
    // 这条测的是失败重试策略；正文自足性见 test/text-lint.test.mjs
    textLint: false,
    log: (line) => logs.push(line),
    onFail: (line, k) => { summary = line; kind = k; },
  });
  assert.equal(result, undefined, '一次空响应不自动重试');
  assert.equal(llm.calls.length, 1, '一次调用都不多花');
  assert.equal(kind, 'empty');
  assert.match(summary, /finish=stop/);
  assert.equal(logs.some((line) => line.includes('retry reason=empty')), false, '不再有固定次数的原地重试日志');
  assert.equal('maxTokens' in llm.calls[0], false, '不传 maxTokens，由 provider 封顶');
});

test('撞墙单独记因：finish=length + 双通道空 → reasoning_exhausted，带 completionTokens 与 reasoning 长度', async () => {
  const reasoningChunks = () => [
    { type: 'reasoning-delta', text: '想'.repeat(1800) },
    { type: 'finish', reason: { kind: 'length' }, usage: { completionTokens: 6000, reasoningTokens: 5980 } },
  ];
  const llm = fakeLlm([reasoningChunks]);
  const logs = [];
  let summary = '';
  let kind = '';
  const result = await merge({
    items: ITEMS,
    llm,
    provider: 'fake',
    model: 'fake',
    clusterId: 'fp_wall',
    log: (line) => logs.push(line),
    onFail: (line, k) => { summary = line; kind = k; },
  });
  assert.equal(result, undefined);
  assert.equal(llm.calls.length, 1, '撞墙也不原地重试，交上层记账');
  assert.equal(kind, 'reasoning_exhausted', '与 empty / parse_fail 分开');
  assert.equal(logs.some((line) => line.includes('retry reason=reasoning')), false);
  const rawLine = logs.find((line) => line.startsWith('[merge] raw channel=tool'));
  assert.ok(rawLine.includes('finish=length'));
  assert.ok(rawLine.includes('maxTokens=unset'));
  assert.ok(rawLine.includes('completionTokens=6000'));
  assert.ok(rawLine.includes('reasoningTokens=5980'));
  assert.ok(rawLine.includes('reasoningChars=1800'));
  assert.match(summary, /finish=length/);
  assert.match(summary, /reasoningChars=1800/);

  // 有正文但被截断（不是双通道空）仍算 parse_fail，不能误标撞墙
  const truncated = fakeLlm([() => [
    { type: 'text-delta', text: '{"ops":[{"op":"update"' },
    { type: 'finish', reason: { kind: 'length' }, usage: { completionTokens: 6000, reasoningTokens: 10 } },
  ]]);
  let kind2 = '';
  await merge({ items: ITEMS, llm: truncated, clusterId: 'fp_trunc', onFail: (line, k) => { kind2 = k; } });
  assert.equal(kind2, 'parse_fail');
});

test('撞墙落账：last_error 以 reasoning_exhausted 开头，不再是 empty_response', async () => {
  const { store, cfg, dbPath } = makeStore({ clusterQuestionMin: 0.78 });
  const llm = fakeLlm([() => [
    { type: 'reasoning-delta', text: '想'.repeat(500) },
    { type: 'finish', reason: { kind: 'length' }, usage: { completionTokens: 6000, reasoningTokens: 5980 } },
  ]]);
  const sleep = makeSleep(store, cfg, llm);
  const out = await sleep.organizePass({ dryRun: false });
  assert.equal(out.reasoningExhausted, 1);
  assert.equal(out.empty, 0);
  assert.equal(out.parseFail, 0);

  const rows = query(dbPath, "SELECT * FROM pending_ops WHERE kind='merge_batch'");
  assert.equal(rows.length, 1);
  assert.equal(rows[0].status, 'pending', '不自动放弃：留在 pending 等下一轮');
  assert.equal(Number(rows[0].attempts), 1);
  assert.match(rows[0].last_error, /^reasoning_exhausted finish=length maxTokens=unset completionTokens=6000/);
  assert.match(rows[0].last_error, /reasoningChars=500/);
  assert.doesNotMatch(rows[0].last_error, /empty_response|parse_fail/);
  assert.ok(readLog(cfg, '撞墙').length >= 1, '日志里单独写撞墙');
});

test('空 ops 走 noop：不落 pending、不重试，下一轮命中缓存', async () => {
  const { store, cfg, dbPath } = makeStore({ clusterQuestionMin: 0.78 });
  const llm = fakeLlm([() => toolChunks('{"ops":[]}')]);
  const sleep = makeSleep(store, cfg, llm);

  const first = await sleep.organizePass({ dryRun: false });
  assert.equal(first.noop, 1);
  assert.equal(first.empty, 0, '空动作不算失败');
  assert.equal(first.calls, 1);
  assert.equal(llm.calls.length, 1, '模型明确不动，不再原地重试');
  assert.equal(query(dbPath, 'SELECT * FROM pending_ops').length, 0, '有效结果不进 pending 烧钱');
  const cache = JSON.parse(fs.readFileSync(cfg.stateFile, 'utf8')).organizeCache || {};
  assert.equal(Object.keys(cache).length, 1);
  assert.equal(Object.values(cache)[0].v, 'noop');

  const second = await sleep.organizePass({ dryRun: false });
  assert.equal(second.calls, 0, '同指纹第二轮不再问模型');
  assert.equal(second.cached, 1);
  assert.equal(second.noop, 0);
  assert.equal(llm.calls.length, 1);
  assert.equal(query(dbPath, "SELECT id FROM memories WHERE status='active'").length, 3, '一条都没动');
});

// ── 2. 原文留痕 ────────────────────────────────────────────────────────

test('解析失败：两个通道各留前 600 字，带 finishReason 与 completionTokens', async () => {
  const longTool = `{"ops":[{"op":"merge","ids":["m_1","m_2"],"text":"${'截'.repeat(800)}`;
  const longText = `思考${'x'.repeat(700)}`;
  const llm = fakeLlm([() => [
    { type: 'text-delta', text: longText },
    { type: 'tool-call-delta', index: 0, name: 'submit_ops', argumentsDelta: longTool },
    { type: 'finish', reason: { kind: 'length' }, usage: { completionTokens: 4000 } },
  ]]);
  const logs = [];
  let summary = '';
  const result = await merge({
    items: ITEMS,
    llm,
    provider: 'fake',
    model: 'fake',
    clusterId: 'fp_raw',
    log: (line) => logs.push(line),
    onFail: (line) => { summary = line; },
  });
  assert.equal(result, undefined);
  assert.equal(llm.calls.length, 1, '解析失败不原地重试');

  const toolLine = logs.find((line) => line.startsWith('[merge] raw channel=tool'));
  const textLine = logs.find((line) => line.startsWith('[merge] raw channel=text'));
  assert.ok(toolLine && textLine, '两个通道都要有一行');
  assert.ok(toolLine.includes('finish=length'), 'finishReason 要留');
  assert.ok(toolLine.includes('completionTokens=4000'), 'usage.completionTokens 要留');
  assert.ok(toolLine.includes(`len=${longTool.length}`));
  assert.ok(toolLine.includes(JSON.stringify(longTool.slice(0, 600))), '原文正好前 600 字');
  assert.ok(toolLine.includes(`…(+${longTool.length - 600}字)`), '截掉多少要标出来');
  assert.ok(textLine.includes(JSON.stringify(longText.slice(0, 600))));

  assert.match(summary, /finish=length/);
  assert.match(summary, /completionTokens=4000/);
  assert.match(summary, /tool=\d+字:/);
  assert.match(summary, /text=\d+字:/);
});

test('解析失败落账：last_error 带原文摘要，不只是 parse_fail 四个字', async () => {
  const { store, cfg, dbPath } = makeStore({ clusterQuestionMin: 0.78 });
  const llm = fakeLlm([brokenChunks]);
  const sleep = makeSleep(store, cfg, llm);
  await sleep.organizePass({ dryRun: false });

  const rows = query(dbPath, "SELECT * FROM pending_ops WHERE kind='merge_batch'");
  assert.equal(rows.length, 1);
  assert.match(rows[0].last_error, /^parse_fail finish=stop/);
  assert.match(rows[0].last_error, /tool=5字:"\{oops"/);
  assert.match(rows[0].last_error, /text=7字:/);
  const payload = JSON.parse(rows[0].payload_json);
  assert.match(payload.reason, /completionTokens=/);
});

// ── 4a. merge_batch 重放 ───────────────────────────────────────────────

test('merge_batch 重放：账本成员重新提合并，成功一次标 done', async () => {
  const { store, cfg, dbPath } = makeStore({ clusterQuestionMin: 0.99 });
  insertPending(dbPath, {
    kind: 'merge_batch',
    key: 'a'.repeat(16),
    payload: { batch: 'fp_replay', ids: ['m_1', 'm_2'], items: [], reason: 'empty_response' },
  });
  const llm = fakeLlm([() => toolChunks(JSON.stringify({ ops: [MERGE_OP] }))]);
  const sleep = makeSleep(store, cfg, llm);

  const out = await sleep.organizePass({ dryRun: false });
  assert.equal(out.replay.merge_batch.done, 1);
  const rows = query(dbPath, "SELECT * FROM pending_ops WHERE kind='merge_batch'");
  assert.equal(rows[0].status, 'done');
  assert.equal(query(dbPath, "SELECT id FROM memories WHERE status='merged'").length, 2);
  assert.ok(readLog(cfg, '[replay] kind=merge_batch done=1 failed=0 dead=0').length >= 1);
});

test('merge_batch 重放：失败不换策略、不转 split，只累加 attempts 并留在 pending', async () => {
  const { store, cfg, dbPath } = makeStore({ clusterQuestionMin: 0.99 });
  insertPending(dbPath, {
    kind: 'merge_batch',
    key: 'b'.repeat(16),
    payload: {
      batch: 'fp_stuck',
      ids: ['m_1', 'm_2'],
      items: [
        { id: 'm_1', text: '树莓派温度 45 度。' },
        { id: 'm_2', text: '树莓派风扇转速 3000 转。' },
      ],
      reason: 'parse_fail',
    },
    attempts: 2,
  });
  const llm = fakeLlm([brokenChunks]);
  const sleep = makeSleep(store, cfg, llm);

  const out = await sleep.organizePass({ dryRun: false });
  assert.equal(out.replay.merge_batch.failed, 1, '这一轮算一次失败，不标 done');
  assert.equal(llm.calls.length, 1, '同一轮只问一次（刚失败的账不在收尾重放里再问）');
  const rows = query(dbPath, "SELECT * FROM pending_ops WHERE kind='merge_batch'");
  assert.equal(rows.length, 1);
  assert.equal(rows[0].status, 'pending', '不自动放弃');
  assert.equal(Number(rows[0].attempts), 3, 'attempts 累加');
  assert.match(rows[0].last_error, /^replay_fail /, '失败原因留账（重放前缀 + 模型摘要）');
  assert.equal(query(dbPath, "SELECT * FROM pending_ops WHERE kind='split_item'").length, 0, '不再按次数转 split');
  assert.equal(readLog(cfg, '[replay] escalate').length, 0, '不再有固定次数的换策略日志');
});

test('merge_batch 重放：成员再多也不按固定条数切小批重排，原账重试', async () => {
  const { store, cfg, dbPath } = makeStore({ clusterQuestionMin: 0.99 });
  const ids = ['m_1', 'm_2', 'm_3', 'm_4', 'm_5', 'm_6', 'm_7'];
  insertPending(dbPath, {
    kind: 'merge_batch',
    key: 'e'.repeat(16),
    payload: { batch: 'fp_chunk', ids, items: ids.map((id) => ({ id, text: `第 ${id} 条` })), reason: 'parse_fail' },
    attempts: 2,
  });
  const llm = fakeLlm([brokenChunks]);
  const sleep = makeSleep(store, cfg, llm);

  const out = await sleep.organizePass({ dryRun: false });
  assert.equal(out.replay.merge_batch.failed, 1);
  const escalated = query(dbPath, "SELECT * FROM pending_ops WHERE kind='merge_batch' AND dedupe_key LIKE 'eeeeeeeeeeeeeeee#e%'");
  assert.equal(escalated.length, 0, '不再按 mergeBatchMax 切小批重新入队');
  const original = query(dbPath, "SELECT * FROM pending_ops WHERE dedupe_key = ?", 'e'.repeat(16));
  assert.equal(original[0].status, 'pending');
  assert.equal(Number(original[0].attempts), 3);
  assert.ok(!/escalate/.test(String(original[0].last_error)));
});

// ── 4b. merge_op 重放 ─────────────────────────────────────────────────

test('merge_op 重放：单条修复性重验，通过就落地并标 done', async () => {
  const { store, cfg, dbPath } = makeStore({ clusterQuestionMin: 0.99 });
  insertPending(dbPath, {
    kind: 'merge_op',
    key: 'c'.repeat(16),
    payload: {
      batch: 'fp_op',
      op: 'update',
      ids: ['m_1'],
      text: '树莓派温度 45 度。',
      quote: '树莓派温度 45 度。',
      evidence: [{ id: 'm_1', quote: '树莓派温度 45 度。' }],
      reason: 'fact_not_in_source',
    },
  });
  const llm = fakeLlm([emptyChunks]);
  const sleep = makeSleep(store, cfg, llm);

  const out = await sleep.organizePass({ dryRun: false });
  assert.equal(out.replay.merge_op.done, 1);
  const rows = query(dbPath, "SELECT * FROM pending_ops WHERE kind='merge_op'");
  assert.equal(rows[0].status, 'done');
  assert.equal(llm.calls.length, 0, '单条重放只走本地证据校验，不花模型调用');
  assert.ok(readLog(cfg, '[replay] kind=merge_op done=1 failed=0 dead=0').length >= 1);
});

test('merge_op 重放：验不过就记账重试，不再按次数转 split_item', async () => {
  const { store, cfg, dbPath } = makeStore({ clusterQuestionMin: 0.99 });
  insertPending(dbPath, {
    kind: 'merge_op',
    key: 'd'.repeat(16),
    payload: {
      batch: 'fp_op_stuck',
      op: 'update',
      ids: ['m_1'],
      text: '树莓派温度 45 度，风扇 999 转。',
      quote: '树莓派温度 45 度。',
      evidence: [{ id: 'm_1', quote: '树莓派温度 45 度。' }],
      reason: 'fact_not_in_source',
    },
    attempts: 2,
  });
  const llm = fakeLlm([emptyChunks]);
  const sleep = makeSleep(store, cfg, llm);

  const out = await sleep.organizePass({ dryRun: false });
  assert.equal(out.replay.merge_op.failed, 1, '验不过算一次失败');
  const rows = query(dbPath, "SELECT * FROM pending_ops WHERE kind='merge_op'");
  assert.equal(rows[0].status, 'pending', '不自动放弃');
  assert.equal(Number(rows[0].attempts), 3);
  assert.match(rows[0].last_error, /^replay_invalid /);
  assert.equal(query(dbPath, "SELECT * FROM pending_ops WHERE kind='split_item'").length, 0, '不再按次数转 split');
  assert.equal(readLog(cfg, '[replay] escalate').length, 0);
});

test('merge_op 重放：源条已归档但有 payload.source → 照原文验过并收口 replay_source_gone', async () => {
  const { store, cfg, dbPath } = makeStore({ clusterQuestionMin: 0.99 });
  insertPending(dbPath, {
    kind: 'merge_op',
    key: 'f'.repeat(16),
    payload: {
      batch: 'fp_source',
      op: 'update',
      ids: ['m_gone'],
      text: '树莓派温度 45 度。',
      quote: '树莓派温度 45 度。',
      evidence: [{ id: 'm_gone', quote: '树莓派温度 45 度。' }],
      source: [{ id: 'm_gone', text: '树莓派温度 45 度。' }],
      reason: 'fact_not_in_source',
    },
  });
  const llm = fakeLlm([emptyChunks]);
  const sleep = makeSleep(store, cfg, llm);

  const out = await sleep.organizePass({ dryRun: false });
  assert.equal(out.replay.merge_op.done, 1, '不再 because source_missing 无限重试');
  const rows = query(dbPath, "SELECT * FROM pending_ops WHERE kind='merge_op'");
  assert.equal(rows[0].status, 'done');
  assert.equal(rows[0].last_error, 'replay_source_gone');
  assert.equal(llm.calls.length, 0);
  assert.ok(readLog(cfg, 'action=replay_source_gone').length >= 1);
});

test('merge_op 重放：老账只有 quote、没有 ids/source → 跳过并记 no_source_for_replay', async () => {
  const { store, cfg, dbPath } = makeStore({ clusterQuestionMin: 0.99 });
  insertPending(dbPath, {
    kind: 'merge_op',
    key: 'g'.repeat(16),
    payload: { batch: 'fp_old', op: 'update', ids: [], text: '树莓派温度 45 度。', quote: '树莓派温度 45 度。', reason: 'fact_not_in_source' },
  });
  const llm = fakeLlm([emptyChunks]);
  const sleep = makeSleep(store, cfg, llm);

  const out = await sleep.organizePass({ dryRun: false });
  assert.equal(out.replay.merge_op.done, 1);
  const rows = query(dbPath, "SELECT * FROM pending_ops WHERE kind='merge_op'");
  assert.equal(rows[0].status, 'done');
  assert.equal(rows[0].last_error, 'no_source_for_replay');
  assert.ok(readLog(cfg, 'reason=no_source_for_replay').length >= 1);
});

// ── 4c. dedupe_check 重放 ─────────────────────────────────────────────

test('dedupe_check 重放：重新过写入闸，判得出就补写并标 done', async () => {
  const { store, cfg, dbPath } = makeStore({ writeGate: true, clusterQuestionMin: 0.99 });
  const text = '树莓派今天 46 度，风扇 3100 转。';
  store.attachLlm(fakeLlm([brokenChunks]));
  await assert.rejects(store.save(text, [], 'test', { type: 'world', entities: ['树莓派'] }), /写入闸未决/);
  const before = query(dbPath, "SELECT * FROM pending_ops WHERE kind='dedupe_check'");
  assert.equal(before.length, 1, '未决先落一条 dedupe_check');
  makeDue(dbPath, 'dedupe_check');

  store.attachLlm(fakeLlm([() => verdictChunks(JSON.stringify({ verdict: 'DISTINCT', reason: '说的是另一个时刻的温度' }))]));
  // setupSleep 的 llm 只用于合并/拆分；写入闸走 store 里注入的那个
  const sleep = makeSleep(store, cfg, fakeLlm([emptyChunks]));
  const out = await sleep.organizePass({ dryRun: false });
  assert.equal(out.replay.dedupe_check.done, 1);
  assert.equal(query(dbPath, 'SELECT id FROM memories').length, 4, '候选补写进库');

  const after = query(dbPath, 'SELECT status FROM pending_ops WHERE id = ?', before[0].id);
  assert.equal(after[0].status, 'done');
});

test('dedupe_check 重放：一直判不出，attempts 到顶转 dead', async () => {
  const { store, cfg, dbPath } = makeStore({ writeGate: true, clusterQuestionMin: 0.99 });
  const text = '树莓派今天 47 度，风扇 3200 转。';
  const broken = () => brokenChunks();
  store.attachLlm(fakeLlm([broken]));
  await assert.rejects(store.save(text, [], 'test', { type: 'world', entities: ['树莓派'] }), /写入闸未决/);
  const row = query(dbPath, "SELECT * FROM pending_ops WHERE kind='dedupe_check'")[0];
  assert.equal(Number(row.attempts), 0, '首次未决只落账，不算一次重试');

  const sleep = makeSleep(store, cfg, fakeLlm([emptyChunks]));
  // 召回补位也不许兜住：这条账就是要验证「召回也找不到 → 死」
  store.recall = async () => [];
  // 2026-11-14 起失败不排退避（next_try_at=now）；makeDue 保留只为把上一轮被 markOp 写完的行拉回可领状态
  let last = null;
  for (let round = 0; round < 6; round += 1) {
    makeDue(dbPath, 'dedupe_check');
    last = await sleep.organizePass({ dryRun: false });
    if (last.replay.dedupe_check.dead) break;
    assert.equal(last.replay.dedupe_check.failed, 1, `第 ${round + 1} 次失败仍在退避`);
  }
  assert.equal(last.replay.dedupe_check.dead, 1, 'attempts 到 MAX_ATTEMPTS 转 dead');
  const after = query(dbPath, 'SELECT status, attempts FROM pending_ops WHERE id = ?', row.id);
  assert.equal(after[0].status, 'dead');
  assert.equal(Number(after[0].attempts), MAX_ATTEMPTS, 'attempts=5 判死，和 markOp 的判定对齐');
});

// ── 老账本不重置 attempts ──────────────────────────────────────────────

test('enqueue：同一 key 不重置 attempts，只有 payload 变了（新证据）才复活', () => {
  const { dbPath } = makeStore({ rows: SEEDS });
  const db = new DatabaseSync(dbPath);
  try {
    const first = enqueue(db, 'merge_op', 'k1', { text: 'a' }, { reviveDead: true });
    assert.equal(Number(first.attempts), 0);
    db.prepare("UPDATE pending_ops SET attempts = 3, status = 'dead' WHERE kind = 'merge_op' AND dedupe_key = 'k1'").run();

    const same = enqueue(db, 'merge_op', 'k1', { text: 'a' }, { reviveDead: true });
    assert.equal(same.status, 'dead', 'payload 一模一样的老账继续躺着，不无限重试');
    assert.equal(Number(same.attempts), 3);

    const changed = enqueue(db, 'merge_op', 'k1', { text: 'b' }, { reviveDead: true });
    assert.equal(changed.status, 'pending', '新证据要能重放，别被老 attempts 卡死');
    assert.equal(Number(changed.attempts), 0);
  } finally {
    db.close();
  }
});

// ── 4d. 2026-11-06 账本补 ids / 召回补位 ────────────────────────────────

test('dedupe_check 账本带 ids：update / merge 未决都记得该动哪条', async () => {
  // m_4 的向量和假 embed 输出一致（cos=1.0），正文不同但近似 → 写入闸必须问模型
  const rows4 = [...SEEDS, { id: 'm_4', text: '树莓派风扇转了三千零一转。', vec: [0.9, 0.2, 0.2, 0] }];
  const { store, dbPath } = makeStore({ rows: rows4, writeGate: true, clusterQuestionMin: 0.99 });
  store.attachLlm(fakeLlm([brokenChunks]));

  // 改 m_1 的正文、但内容跟 m_4 撞近似（m_1 自己被排除出召回）→ 必须问模型，模型坏了就未决
  await assert.rejects(store.update('m_1', { text: '树莓派风扇转速 3001 转。' }), /写入闸未决/);
  let rows = query(dbPath, "SELECT * FROM pending_ops WHERE kind='dedupe_check' ORDER BY id");
  let payload = JSON.parse(rows[rows.length - 1].payload_json);
  assert.deepEqual(payload.ids, ['m_1'], 'update 账本必须带目标 id');
  assert.equal(payload.candidate.text, '树莓派风扇转速 3001 转。');
  assert.equal(payload.candidate.type, 'world');

  // 合并 m_2+m_3、结果跟 m_4 近似 → 候选只剩 m_1/m_4，模型坏了就未决
  await assert.rejects(store.merge(['m_2', 'm_3'], '树莓派风扇又转了三千零一转。'), /写入闸未决/);
  rows = query(dbPath, "SELECT * FROM pending_ops WHERE kind='dedupe_check' ORDER BY id");
  payload = JSON.parse(rows[rows.length - 1].payload_json);
  assert.deepEqual(payload.ids, ['m_2', 'm_3'], 'merge 账本要带全部源 id');
  assert.equal(payload.candidate.text, '树莓派风扇又转了三千零一转。');
});

test('dedupe_check 重放：老账没 ids 时按正文 + 实体召回补位，改对条目', async () => {
  const { store, cfg, dbPath } = makeStore({ writeGate: true, clusterQuestionMin: 0.99 });
  insertPending(dbPath, {
    kind: 'dedupe_check',
    key: 'h'.repeat(16),
    payload: {
      action: 'update',
      candidate: { text: '树莓派温度 46 度。', type: 'world', tags: [], entities: ['树莓派'] },
      reason: 'dedupe_no_verdict',
    },
  });
  store.attachLlm(fakeLlm([() => verdictChunks(JSON.stringify({ verdict: 'DISTINCT', reason: '新时刻的温度' }))]));
  const sleep = makeSleep(store, cfg, fakeLlm([emptyChunks]));

  const out = await sleep.organizePass({ dryRun: false });
  assert.equal(out.replay.dedupe_check.done, 1, '召回补上 id 后能重放成功');
  const rows = query(dbPath, "SELECT * FROM pending_ops WHERE kind='dedupe_check'");
  assert.equal(rows[0].status, 'done');
  assert.equal(query(dbPath, 'SELECT text FROM memories WHERE id = ?', 'm_1')[0].text, '树莓派温度 46 度。');
  assert.ok(readLog(cfg, 'action=recall_fallback ids=m_1').length >= 1);
});

test('dedupe_check 重放：老账 action=merge 但只召回一条 → 按 update 落地，不硬走 merge', async () => {
  const { store, cfg, dbPath } = makeStore({ writeGate: true, clusterQuestionMin: 0.99 });
  insertPending(dbPath, {
    kind: 'dedupe_check',
    key: 'i'.repeat(16),
    payload: {
      action: 'merge',
      candidate: { text: '树莓派温度 46 度。', type: 'world', tags: [], entities: ['树莓派'] },
      reason: 'dedupe_no_verdict',
    },
  });
  store.recall = async () => [{ id: 'm_1', entities: ['树莓派'], score: 0.9 }];
  store.attachLlm(fakeLlm([() => verdictChunks(JSON.stringify({ verdict: 'DISTINCT', reason: '只有一条' }))]));
  const sleep = makeSleep(store, cfg, fakeLlm([emptyChunks]));

  const out = await sleep.organizePass({ dryRun: false });
  assert.equal(out.replay.dedupe_check.done, 1);
  assert.equal(query(dbPath, 'SELECT text FROM memories WHERE id = ?', 'm_1')[0].text, '树莓派温度 46 度。');
  assert.ok(readLog(cfg, 'action=merge_fallback_update ids=m_1').length >= 1);
});

test('dedupe_check 重放：召回也找不到 → dead，last_error 写 payload_missing_ids_no_candidate', async () => {
  const { store, cfg, dbPath } = makeStore({ writeGate: true, clusterQuestionMin: 0.99 });
  insertPending(dbPath, {
    kind: 'dedupe_check',
    key: 'j'.repeat(16),
    payload: {
      action: 'update',
      candidate: { text: '一段现库没有的正文。', type: 'world', tags: [], entities: ['树莓派'] },
      reason: 'dedupe_no_verdict',
    },
  });
  store.recall = async () => [];
  store.attachLlm(fakeLlm([brokenChunks]));
  const sleep = makeSleep(store, cfg, fakeLlm([emptyChunks]));

  const out = await sleep.organizePass({ dryRun: false });
  assert.equal(out.replay.dedupe_check.dead, 1);
  const rows = query(dbPath, "SELECT * FROM pending_ops WHERE kind='dedupe_check'");
  assert.equal(rows[0].status, 'dead');
  assert.match(rows[0].last_error, /^payload_missing_ids_no_candidate action=update$/);
  assert.ok(readLog(cfg, 'reason=payload_missing_ids_no_candidate').length >= 1);
});

// ── 5. 重放不再按条数预算────────────────────

test('重放全领：5 条 merge_batch 账在同一轮全部试过，不按 replayBudget 截断', async () => {
  const { store, cfg, dbPath } = makeStore({ clusterQuestionMin: 0.99 });
  for (let i = 0; i < 5; i += 1) {
    insertPending(dbPath, {
      kind: 'merge_batch',
      key: `${'g'.repeat(15)}${i}`,
      payload: { batch: `fp_many_${i}`, ids: ['m_1', 'm_2'], items: [], reason: 'parse_fail' },
    });
  }
  // 每批都失败（空响应），但 5 条都要被领到、各记一次失败
  const llm = fakeLlm([emptyChunks]);
  const sleep = makeSleep(store, cfg, llm);
  const out = await sleep.organizePass({ dryRun: false });
  assert.equal(out.replay.merge_batch.failed, 5, '5 条全试过');
  assert.equal(llm.calls.length, 5, '每条一调，没有被预算截掉');
  const rows = query(dbPath, "SELECT * FROM pending_ops WHERE kind='merge_batch' AND status='pending'");
  assert.equal(rows.length, 5, '全留在 pending，attempts 各加一');
  assert.ok(rows.every((r) => Number(r.attempts) === 1));
});
