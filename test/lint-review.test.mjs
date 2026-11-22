import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { applySchema } from '../scripts/apply-pipeline-schema.mjs';
import { createStore } from '../lib/store.js';
import { split } from '../lib/agents/split.js';
import {
  reviewLintItems, judgeSuspected, buildLintRedoFeedback, lintReviewMeta, LINT_REVIEW_SYSTEM,
  collectStreamWithIdle,
} from '../lib/lint-review.js';
import { lintItems } from '../lib/text-lint.js';

/* ─────────────── 假 llm ─────────────── */

function textChunks(text) {
  return [
    { type: 'text-delta', text },
    { type: 'finish', reason: { kind: 'stop' } },
  ];
}

const toolChunks = (args) => [
  { type: 'tool-call-delta', index: 0, id: 'call_1', name: 'submit_parts', argumentsDelta: args.slice(0, 20) },
  { type: 'tool-call-delta', index: 0, argumentsDelta: args.slice(20) },
  { type: 'finish', reason: { kind: 'stop' } },
];

const reviewChunks = (decisions) => textChunks(JSON.stringify({ decisions }));

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

const promptOf = (call) => String(call?.messages?.[0]?.content?.[0]?.text ?? '');

const VEC_SO = path.join(import.meta.dirname, '../node_modules/sqlite-vec-linux-arm64/vec0.so');

const MEMORY_DDL = `CREATE TABLE memories (
  id TEXT PRIMARY KEY, text TEXT NOT NULL, tags TEXT, source TEXT, ts TEXT, type TEXT,
  entities TEXT, validFrom TEXT, supersedes TEXT, updatedAt TEXT, status TEXT, reason TEXT,
  supersededBy TEXT, validTo TEXT, confidence REAL, extra TEXT, present TEXT, rowSeq INTEGER NOT NULL
);`;

function makeStore() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-lintreview-'));
  const dbPath = path.join(dir, 'memory.db');
  new DatabaseSync(dbPath).close();
  applySchema(dbPath, { logger: () => {} });
  return { dir, dbPath, db: new DatabaseSync(dbPath) };
}

function readAll(dbPath, sql, params = []) {
  const db = new DatabaseSync(dbPath, { readOnly: true });
  try {
    return db.prepare(sql).all(...params);
  } finally {
    db.close();
  }
}

const SENT_A = '2026-11-05 早上八点，小林把 dsh-memory 的 jsonl 冻结了，只留作历史副本。';
const SENT_B = '但是当天下午小林又把写入口挪到了 store.js 的第 530 行。';
const SENT_C = '但是晚饭前小林又给 ingest 加了复核闸。';
const SOURCE = `${SENT_A}${SENT_B}${SENT_C}`;

/* ─────────────── reviewLintItems：一批只发一次调用 ─────────────── */

test('reviewLintItems：3 条可疑合并成 1 次调用，提示词带齐三条原文与规则说明', async () => {
  const items = [
    { index: 0, text: SENT_B, violations: [{ code: 'leading_conjunction', match: '但是' }] },
    { index: 1, text: SENT_C, violations: [{ code: 'leading_conjunction', match: '但是' }] },
    { index: 2, text: SENT_A, violations: [{ code: 'too_short', match: '' }] },
  ];
  let seen = 0;
  const llm = fakeLlm([
    (options) => {
      seen += 1;
      const prompt = promptOf(options);
      assert.match(prompt, /但是当天下午小林又把写入口挪到了/);
      assert.match(prompt, /但是晚饭前小林又给 ingest 加了复核闸/);
      assert.match(prompt, /leading_conjunction/);
      return reviewChunks([
        { index: 0, verdict: 'keep', reason: '这是接着上一条的转折，原文拆开不合适' },
        { index: 1, verdict: 'redo', reason: '「但是」开头，单独看不知所指' },
        { index: 2, verdict: 'redo', reason: '太短，缺上下文' },
      ]);
    },
  ]);
  const logs = [];
  const result = await reviewLintItems({
    cfg: { provider: 'fake', model: 'fake' },
    llm,
    agent: 'split',
    items,
    logger: (line) => logs.push(line),
  });

  assert.equal(llm.calls.length, 1, '一个批次只允许一次复核调用');
  assert.equal(seen, 1);
  assert.equal(result.ok, true);
  assert.equal(result.decisions.length, 3);
  assert.deepEqual(result.decisions.map((d) => d.verdict), ['keep', 'redo', 'redo']);
  assert.equal(logs.some((line) => line.includes('[lint-review] fallback')), false);
  assert.match(LINT_REVIEW_SYSTEM, /不要挑风格毛病/, '提示词里要写明只判自足性');
  assert.equal(llm.calls[0].temperature, 0);
  assert.equal('maxTokens' in llm.calls[0], false, '复核不传 maxTokens');
});

test('reviewLintItems：调用抛错 → 降级 ok=false，条目不丢（交给调用方重做一次）', async () => {
  const logs = [];
  const llm = {
    stream() {
      throw new Error('provider 502');
    },
  };
  const result = await reviewLintItems({
    cfg: { provider: 'fake', model: 'fake' },
    llm,
    agent: 'sleep:ingest',
    items: [{ index: 0, text: SENT_B, violations: [{ code: 'leading_conjunction', match: '但是' }] }],
    logger: (line) => logs.push(line),
  });
  assert.equal(result.ok, false);
  assert.equal(result.fallback, true);
  assert.equal(logs.some((line) => line.includes('[lint-review] fallback')), true, '降级要留日志');
});

test('reviewLintItems：返回非 JSON → 同样降级，不瞎猜', async () => {
  const llm = fakeLlm([() => textChunks('我觉得这两条还行')]);
  const result = await reviewLintItems({
    cfg: {}, llm, agent: 'x', items: [{ index: 0, text: SENT_B, violations: [{ code: 'weird', match: '' }] }],
  });
  assert.equal(result.ok, false);
  assert.equal(result.fallback, true);
  assert.match(String(result.error), /非 JSON/);
});

test('judgeSuspected：lintReview=false 时完全走旧逻辑（0 次调用，全 redo）', async () => {
  const llm = fakeLlm([() => reviewChunks([{ index: 0, verdict: 'keep', reason: '不该问到' }])]);
  const judged = await judgeSuspected({
    cfg: { pipeline: { lintReview: false } },
    llm,
    agent: 'split',
    suspects: [{ index: 0, text: SENT_B, violations: [{ code: 'leading_conjunction', match: '但是' }] }],
  });
  assert.equal(llm.calls.length, 0);
  assert.equal(judged.keep.length, 0);
  assert.equal(judged.redo.length, 1);
});

test('judgeSuspected：复核漏判的条目按 redo 算，不放过', async () => {
  const llm = fakeLlm([() => reviewChunks([{ index: 0, verdict: 'keep', reason: 'ok' }])]);
  const judged = await judgeSuspected({
    cfg: {},
    llm,
    agent: 'split',
    suspects: [
      { index: 0, text: SENT_B, violations: [{ code: 'leading_conjunction', match: '但是' }] },
      { index: 1, text: SENT_C, violations: [{ code: 'leading_conjunction', match: '但是' }] },
    ],
  });
  assert.equal(judged.keep.length, 1);
  assert.equal(judged.redo.length, 1, '没判的那条按 redo');
});

/* ─────────────── split agent：keep / redo / 重做仍不过 ─────────────── */

/**
 * 2026-11-15：切分点协议下「重做」= 换切点，正文仍逐字来自原文。
 * 这里用「尾段太短（too_short）」制造 lint 命中：首版把收尾那句单独切出来（3 字），
 * 重做时把切点前移到上一句开头，两段都够长就通过——正文一个字都不用改。
 */
const LT1 = '2026-11-05 早上八点，小林把 dsh-memory 的 jsonl 冻结了，只留作历史副本。';
const LT2 = '下午两点，小林把写入口挪到了 store.js 的第 530 行。';
const TAIL = '收尾。';
const LINT_SRC = `${LT1}${LT2}${TAIL}`;
/** 首版切点：把「收尾。」单独切一段（3 字，too_short）。 */
const SHORT_CUTS = [TAIL];
/** 重做切点：切在 LT2 开头，尾段 = LT2 + TAIL，够长。 */
const REDO_CUTS = [LT2.slice(0, 12)];

test('split：复核判 keep → 原样放行，不重切（首轮 + 复核共 2 次调用）', async () => {
  const { dbPath, db, dir } = makeStore();
  const llm = fakeLlm([
    () => toolChunks(JSON.stringify({ cuts: SHORT_CUTS, unavoidable: false, reason: '' })),
    () => reviewChunks([
      { index: 1, verdict: 'keep', reason: '这就是本条的收尾，并进上一条读得懂' },
    ]),
  ]);
  const logs = [];
  const result = await split({
    sourceId: 'm_keep', text: LINT_SRC, type: 'world', ts: '2026-11-05',
    db, llm, provider: 'fake', model: 'fake', log: (line) => logs.push(line),
  });
  db.close();

  assert.equal(llm.calls.length, 2, '问复核一次，没重切');
  assert.match(promptOf(llm.calls[1]), /收尾。/, '复核要看得到这一段原文');
  assert.ok(result);
  assert.equal(result.parts.length, 2);
  assert.equal(result.parts[1].text, TAIL, 'keep 的段落一个字不改');
  assert.equal(result.parts[1].__lintKept, '这就是本条的收尾，并进上一条读得懂');
  assert.equal(readAll(dbPath, 'SELECT id FROM discards').length, 0);
  assert.equal(logs.some((line) => line.includes('复核放行')), true);
  fs.rmSync(dir, { recursive: true, force: true });
});

test('split：复核判 redo → 带 violations 重做一次（换切点），合格就入库', async () => {
  const { dbPath, db, dir } = makeStore();
  const llm = fakeLlm([
    () => toolChunks(JSON.stringify({ cuts: SHORT_CUTS, unavoidable: false, reason: '' })),
    () => reviewChunks([{ index: 1, verdict: 'redo', reason: '三个字的残句，单独看不知所云' }]),
    () => toolChunks(JSON.stringify({ cuts: REDO_CUTS, unavoidable: false, reason: '' })),
  ]);
  const result = await split({
    sourceId: 'm_redo', text: LINT_SRC, type: 'world', ts: '2026-11-05',
    db, llm, provider: 'fake', model: 'fake',
  });
  db.close();

  assert.equal(llm.calls.length, 3, '首轮 + 复核 + 重做一次，不许多');
  const redoPrompt = promptOf(llm.calls[2]);
  assert.match(redoPrompt, /too_short/, '重做要带第一轮 violations');
  assert.match(redoPrompt, /不知所云/, '重做要带复核理由');
  assert.ok(result);
  assert.equal(result.parts.length, 2);
  assert.equal(result.parts[0].text, LT1);
  assert.equal(result.parts[1].text, `${LT2}${TAIL}`, '重做版正文仍逐字来自原文');
  assert.equal(readAll(dbPath, 'SELECT id FROM discards').length, 0);
  fs.rmSync(dir, { recursive: true, force: true });
});

test('split：重做后仍不过 → discards 一行，reason 带复核理由，绝不循环第三次', async () => {
  const { dbPath, db, dir } = makeStore();
  const llm = fakeLlm([
    () => toolChunks(JSON.stringify({ cuts: SHORT_CUTS, unavoidable: false, reason: '' })),
    () => reviewChunks([{ index: 1, verdict: 'redo', reason: '转折词开头，独立条目读不懂' }]),
    () => toolChunks(JSON.stringify({ cuts: SHORT_CUTS, unavoidable: false, reason: '' })),
  ]);
  const result = await split({
    sourceId: 'm_dead', text: LINT_SRC, type: 'world', ts: '2026-11-05',
    db, llm, provider: 'fake', model: 'fake',
  });
  db.close();

  assert.equal(llm.calls.length, 3, '重做只有一次，不许循环');
  assert.equal(result, undefined, '只剩一段，整条退拆');
  const discards = readAll(dbPath, 'SELECT * FROM discards');
  assert.equal(discards.length, 1);
  assert.equal(discards[0].verdict, 'LINT_REJECT');
  assert.equal(discards[0].source_file, 'lint_reject');
  assert.equal(discards[0].text, TAIL);
  assert.match(discards[0].reason, /too_short/);
  assert.match(discards[0].reason, /复核理由|独立条目读不懂/, '复核 agent 的理由要落账');
  fs.rmSync(dir, { recursive: true, force: true });
});

test('split：复核调用挂了 → 降级按旧逻辑重做一次（换切点），条目不丢', async () => {
  const { dbPath, db, dir } = makeStore();
  const llm = fakeLlm([
    () => toolChunks(JSON.stringify({ cuts: SHORT_CUTS, unavoidable: false, reason: '' })),
    () => {
      throw new Error('复核 provider 超时');
    },
    () => toolChunks(JSON.stringify({ cuts: REDO_CUTS, unavoidable: false, reason: '' })),
  ]);
  const logs = [];
  const result = await split({
    sourceId: 'm_fb', text: LINT_SRC, type: 'world', ts: '2026-11-05',
    db, llm, provider: 'fake', model: 'fake', log: (line) => logs.push(line),
  });
  db.close();

  assert.equal(logs.some((line) => line.includes('[lint-review] fallback')), true);
  assert.ok(result, '复核挂了也把合格的重做版带出来');
  assert.equal(result.parts.length, 2);
  assert.equal(readAll(dbPath, 'SELECT id FROM discards').length, 0);
  fs.rmSync(dir, { recursive: true, force: true });
});

/* ─────────────── store 最后一道闸 ─────────────── */

test('store.save：复核 keep 的标记让它过闸并留日志，没标记的照样拒', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-lintreview-store-'));
  const dbPath = path.join(dir, 'memory.db');
  const raw = new DatabaseSync(dbPath);
  raw.exec(MEMORY_DDL);
  raw.close();
  applySchema(dbPath, { logger: () => {} });
  const db = new DatabaseSync(dbPath, { allowExtension: true });
  db.loadExtension(VEC_SO);
  db.exec("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);");
  db.exec("INSERT OR REPLACE INTO meta (key, value) VALUES ('dim', '4');");
  db.exec('CREATE VIRTUAL TABLE IF NOT EXISTS vec_items USING vec0(embedding float[4] distance_metric=cosine);');
  db.exec('CREATE TABLE IF NOT EXISTS vec_map (rowid INTEGER PRIMARY KEY, id TEXT);');
  const store = createStore({
    storePath: dir,
    storeLogFile: path.join(dir, 'store.log'),
    apiUrl: 'http://127.0.0.1:1',
    timeoutMs: 50,
    provider: 'fake',
    model: 'fake',
    rrfK: 60,
    opinionAlpha: 0.1,
    splitMaxChars: 200,
    embed: async (texts) => texts.map(() => [1, 0, 0, 0]),
    pipeline: {},
  });
  const text = SENT_B;
  await assert.rejects(
    () => store.save(text, [], 'test'),
    (error) => error.code === 'LINT_REJECT',
    '没复核标记的短正文照旧拒绝',
  );
  const meta = lintReviewMeta({ __lintKept: '转折句在原文里紧跟着上一条，单独看也说得通' });
  assert.equal(meta.__lintReview.verdict, 'keep');
  const rec = await store.save(text, [], 'test', { ...meta });
  assert.ok(rec?.id, '复核放行的条目入库');
  const rows = readAll(dbPath, 'SELECT text FROM memories');
  assert.equal(rows.length, 1);
  assert.equal(rows[0].text, text, '放行不改一个字');
  db.close();
  fs.rmSync(dir, { recursive: true, force: true });
});

test('buildLintRedoFeedback：一段话里带 violations 摘要与复核理由，不被 200 字切掉', () => {
  const feedback = buildLintRedoFeedback([
    {
      index: 1,
      text: SENT_B,
      reviewReason: '「但是」开头，单独看不知所指',
      violations: [{ code: 'leading_conjunction', match: '但是' }],
    },
  ]);
  assert.match(feedback, /第 2 条/);
  assert.match(feedback, /leading_conjunction/);
  assert.match(feedback, /不知所指/);
  assert.ok(feedback.length <= 400, `重做提示别太肥，实际 ${feedback.length}`);
});


/* ─────────────── 超时按响应活性判 ─────────────── */

test('活性超时：流总时长超过阈值但一直有数据 → 不判超时', async () => {
  // 一共 6 块、每块间隔 20ms，总时长 ≈100ms；空闲阈值 40ms。按旧的「总时长」口径必死，
  // 按活跃度口径只要块间隔 < 40ms 就该完整收下来。
  const stream = (async function* slow() {
    for (let i = 0; i < 6; i += 1) {
      await new Promise((r) => setTimeout(r, 20));
      yield { type: 'text-delta', text: `第${i}块` };
    }
    yield { type: 'finish', reason: { kind: 'stop' } };
  })();
  const text = await collectStreamWithIdle(stream, { idleMs: 40, label: '复核' });
  assert.equal(text, '第0块第1块第2块第3块第4块第5块');
});

test('活性超时：连续 idleMs 没有数据才判死，报错说清是空闲超时', async () => {
  const stream = (async function* stuck() {
    yield { type: 'text-delta', text: '开了个头' };
    await new Promise(() => {}); // 永远不再出数据
  })();
  await assert.rejects(
    () => collectStreamWithIdle(stream, { idleMs: 40, label: '复核' }),
    /复核 空闲超时：40ms 没有收到任何数据/,
  );
});

test('复核调用传细流：慢但活着不降级，真卡住才走 fallback', async () => {
  const payload = JSON.stringify({ decisions: [{ index: 0, verdict: 'keep', reason: '误杀' }] });
  const parts = [payload.slice(0, 12), payload.slice(12, 24), payload.slice(24, 40), payload.slice(40)];
  const slowOk = {
    stream() {
      return (async function* () {
        for (const part of parts) {
          await new Promise((r) => setTimeout(r, 20));
          yield { type: 'text-delta', text: part };
        }
        yield { type: 'finish', reason: { kind: 'stop' } };
      })();
    },
  };
  const ok = await reviewLintItems({
    cfg: { provider: 'fake', model: 'fake', pipeline: { lintReviewTimeoutMs: 40 } },
    llm: slowOk,
    agent: 'merge',
    items: [{ index: 0, text: '树莓派温度 45 度。', violations: [] }],
  });
  assert.equal(ok.ok, true, '总时长 80ms > 40ms，但一直有数据，不该被当成超时');
  assert.deepEqual(ok.decisions, [{ index: 0, verdict: 'keep', reason: '误杀' }]);

  const stuck = { stream: () => (async function* () { await new Promise(() => {}); })() };
  const logs = [];
  const bad = await reviewLintItems({
    cfg: { provider: 'fake', model: 'fake', pipeline: { lintReviewTimeoutMs: 40 } },
    llm: stuck,
    agent: 'merge',
    items: [{ index: 0, text: '树莓派温度 45 度。', violations: [] }],
    logger: (line) => logs.push(line),
  });
  assert.equal(bad.ok, false);
  assert.equal(bad.fallback, true);
  assert.ok(logs.some((l) => l.includes('[lint-review] fallback') && l.includes('空闲超时')));
});

test('提示词：文件名/行号只提供定位，缺完整陈述即使带路径也判 redo', () => {
  assert.match(LINT_REVIEW_SYSTEM, /文件名、行号、数字只提供定位/);
  assert.match(LINT_REVIEW_SYSTEM, /缺完整陈述（例如只有「lib\/gate\.js 的」）的片段即使带路径也判 redo/);
});
