/**
 * 2026-11-15：拆分从「模型重写每段」改成「模型只给切分点，代码照原文切」。
 *
 * 钉住的四件事：
 *   ① 正常切：模型给 cuts，代码 indexOf 出偏移、原文 slice，段与原文逐字相同、ratio 恒 1；
 *   ② 拼回不等：spliceEqual 当场判false（切点漏/重/错位），validateParts 走 splice_mismatch 分支；
 *   ③ 超长段：>200 字由代码按句号/换行再切，连句读都没有就整段保留（longKept）；
 *   ④ 定位串找不到：判失败、带 feedback 重问一次，第二次合格就入库、不落 pending。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { applySchema } from '../scripts/apply-pipeline-schema.mjs';
import { createStore } from '../lib/store.js';
import {
  split, validateParts, resolveOffsets, spliceEqual, splitLongPart,
  SUBMIT_PARTS_TOOL, PART_TEXT_MAX, SPLIT_SYSTEM, buildSplitPrompt,
} from '../lib/agents/split.js';
import { consumeSplitOps } from '../lib/pipeline-split.js';
import { hashText } from '../lib/write-gate.js';
import { enqueue } from '../lib/ingest-index.js';

const VEC_SO = path.join(import.meta.dirname, '../node_modules/sqlite-vec-linux-arm64/vec0.so');

const MEMORY_DDL = `CREATE TABLE memories (
  id TEXT PRIMARY KEY, text TEXT NOT NULL, tags TEXT, source TEXT, ts TEXT, type TEXT,
  entities TEXT, validFrom TEXT, supersedes TEXT, updatedAt TEXT, status TEXT, reason TEXT,
  supersededBy TEXT, validTo TEXT, confidence REAL, extra TEXT, present TEXT, rowSeq INTEGER NOT NULL
);`;

function tmpDb() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-split-'));
  const dbPath = path.join(dir, 'memory.db');
  new DatabaseSync(dbPath).close();
  applySchema(dbPath, { logger: () => {} });
  return { dir, dbPath, db: new DatabaseSync(dbPath) };
}

function pendingRows(dbPath, kind) {
  const db = new DatabaseSync(dbPath, { readOnly: true });
  try {
    return db.prepare('SELECT * FROM pending_ops WHERE kind = ?').all(kind);
  } finally {
    db.close();
  }
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
  { type: 'tool-call-delta', index: 0, id: 'call_1', name: 'submit_parts', argumentsDelta: args.slice(0, 20) },
  { type: 'tool-call-delta', index: 0, argumentsDelta: args.slice(20) },
  { type: 'finish', reason: { kind: 'stop' } },
];

const textChunks = (text) => [
  { type: 'text-delta', text },
  { type: 'finish', reason: { kind: 'stop' } },
];

const brokenChunks = () => [
  { type: 'text-delta', text: '这不是 JSON' },
  { type: 'tool-call-delta', index: 0, name: 'submit_parts', argumentsDelta: '{oops' },
  { type: 'finish', reason: { kind: 'stop' } },
];

const SENT1 = '2026-11-05 早上八点，小林把 dsh-memory 的 jsonl 冻结了，只留作历史副本。';
const SENT2 = '上午十点，真库 memories 四千七百九十五行、vec_map 四千七百九十八行，三条历史孤儿漏了 id 前缀。';
const SENT3 = '下午两点，合并 agent 的提示词定稿，maxTokens 两千，温度零，输出只许 JSON。';
const SENT4 = '晚上八点，拆分 agent 的草稿写好，每段不超过两百字，段与段不许重复。';
const SENT5 = '夜里十点，日志里 split ratio 零点九八，没有一条正文被截断。';
const SOURCE = `${SENT1}${SENT2}${SENT3}${SENT4}${SENT5}`;
/** 每段开头的定位串：模型只给这些，正文由代码切。 */
const CUTS = [SENT2, SENT3, SENT4, SENT5].map((s) => s.slice(0, 12));

function cutsOf(list = CUTS) {
  return JSON.stringify({ cuts: list, unavoidable: false, reason: '' });
}

test('参数与 schema：submit_parts 只收 cuts，low/温度 0/不传 maxTokens，提示词写死只许切分点', async () => {
  const { db } = tmpDb();
  const llm = fakeLlm([() => toolChunks(cutsOf())]);
  const result = await split({ sourceId: 'm_src', text: SOURCE, type: 'experience', ts: '2026-11-05', llm, provider: 'fake', model: 'fake', maxTokens: 0 });
  db.close();

  assert.equal(result.source, 'tool');
  assert.equal(result.rounds, 1);
  assert.equal(llm.calls.length, 1);
  const req = llm.calls[0];
  assert.equal(req.reasoningEffort, 'low');
  assert.equal('maxTokens' in req, false, 'maxTokens=0 时请求里不许有 maxTokens');
  assert.equal(req.temperature, 0);
  assert.equal(req.tools[0].name, 'submit_parts');
  const schema = req.tools[0].parameters;
  assert.deepEqual(schema.required, ['cuts']);
  assert.equal(schema.properties.cuts.type, 'array');
  assert.equal(schema.properties.offsets.type, 'array');
  assert.equal(schema.properties.unavoidable.type, 'boolean');
  assert.equal(schema.properties.reason.type, 'string');
  assert.equal(SUBMIT_PARTS_TOOL.name, 'submit_parts');
  assert.equal(SUBMIT_PARTS_TOOL.description.includes('绝不输出正文'), true);
  // prompt 里写死：只许给切分点，不许改写/扩写/补字
  assert.match(SPLIT_SYSTEM, /只给切分点/);
  assert.match(SPLIT_SYSTEM, /不许改写/);
  assert.match(SPLIT_SYSTEM, /不许扩写/);
  assert.match(SPLIT_SYSTEM, /不许补字/);
  assert.match(SPLIT_SYSTEM, /不许输出任何正文/);
  const prompt = req.messages[0].content[0].text;
  assert.match(prompt, /^原条目 m_src（experience，2026-11-05）：/);
  assert.equal(prompt.includes('{id}'), false);
  assert.equal(prompt.includes('{"cuts":["原文里第二段开头的 8-20 字"'), true);
});

test('正常切：模型只给 cuts，正文逐字来自原文，拼回相等，ratio 恒 1', async () => {
  const { db } = tmpDb();
  const llm = fakeLlm([() => toolChunks(cutsOf())]);
  const result = await split({ sourceId: 'm_src', text: SOURCE, llm });
  db.close();

  assert.equal(result.parts.length, 5);
  assert.deepEqual(result.parts.map((p) => p.text), [SENT1, SENT2, SENT3, SENT4, SENT5]);
  assert.equal(result.parts.map((p) => p.text).join(''), SOURCE, '拼回去与原文逐字相等');
  assert.equal(result.charsBefore, SOURCE.length);
  assert.equal(result.charsAfter, SOURCE.length, '正文全来自原文，一个字都不多');
  assert.equal(result.ratio, 1);
  assert.equal(result.longKept, 0);
  assert.equal(result.cuts.length, 4);
});

test('拼回硬校验：切点漏字/改写过的段落一律判 false', () => {
  assert.equal(spliceEqual([SENT1, SENT2, SENT3, SENT4, SENT5], SOURCE), true);
  assert.equal(spliceEqual([SENT1, SENT2], SOURCE), false, '漏了后面的字');
  assert.equal(spliceEqual([SENT1, `${SENT2}（补一句）`, SENT3, SENT4, SENT5], SOURCE), false, '补了原文没有的字');
  assert.equal(spliceEqual([SENT2, SENT1, SENT3, SENT4, SENT5], SOURCE), false, '顺序错位');
  const bad = validateParts({ cuts: [`${SENT2.slice(0, 12)}不存在的后缀`] }, SOURCE);
  assert.equal(bad.ok, false, '锚串带原文没有的字就找不到，进不了切分');
  assert.equal(bad.reason, 'anchor_not_found');
});

test('定位串找不到：判失败带 feedback 重问一次，第二次合格就入库', async () => {
  const { db } = tmpDb();
  const logs = [];
  const llm = fakeLlm([
    () => toolChunks(cutsOf(['原文里根本没有这一句'])),
    () => toolChunks(cutsOf()),
  ]);
  const result = await split({ sourceId: 'm_src', text: SOURCE, db, llm, log: (line) => logs.push(line) });
  db.close();

  assert.equal(llm.calls.length, 2, '首问 + 重问一次，不许多');
  assert.ok(result);
  assert.equal(result.rounds, 2);
  assert.equal(result.parts.length, 5);
  const retryPrompt = String(llm.calls[1].messages[0].content[0].text);
  assert.match(retryPrompt, /找不到/, 'feedback 要说明锚串找不到');
  assert.match(retryPrompt, /只许切分点/);
  assert.equal(logs.some((line) => line.includes('切分方案不合格，带 feedback 重问一次')), true);
});

test('偏移方案同样能切：offsets 越界/不递增直接判失败', async () => {
  const at = [SENT1.length, SENT1.length + SENT2.length];
  const ok = validateParts({ offsets: at }, SOURCE.slice(0, SENT1.length + SENT2.length + 8));
  assert.equal(ok.ok, true);
  assert.equal(ok.parts[0].text, SENT1);

  assert.equal(resolveOffsets(SOURCE, { offsets: [0] }).reason, 'offset_out_of_range', '0 不是合法切点');
  assert.equal(resolveOffsets(SOURCE, { offsets: [SOURCE.length] }).reason, 'offset_out_of_range', '末尾不是合法切点');
  assert.equal(resolveOffsets(SOURCE, { offsets: [40, 20] }).reason, 'offset_not_advancing');
});

test('超长段按句号再切：不问模型，切出的段全部 ≤200 且一字不丢', async () => {
  const LONG_A = `${'甲'.repeat(150)}。`;
  const LONG_B = `${'乙'.repeat(150)}。`;
  const LONG_C = `${'丙'.repeat(150)}。`;
  const LONG = LONG_A + LONG_B + LONG_C;
  const llm = fakeLlm([() => toolChunks(JSON.stringify({ cuts: ['乙乙乙乙乙乙乙乙'] }))]);
  const result = await split({ sourceId: 'm_long', text: LONG, llm });
  assert.equal(llm.calls.length, 1, '再切是纯代码，不再问模型');
  assert.equal(result.parts.length, 3, '300 字的段被句号切成两段');
  assert.deepEqual(result.parts.map((p) => p.text), [LONG_A, LONG_B, LONG_C]);
  assert.equal(result.parts.every((p) => p.text.length <= PART_TEXT_MAX), true);
  assert.equal(result.longKept, 0);

  const paragraph = `${'甲'.repeat(100)}。${'乙'.repeat(100)}。${'丙'.repeat(100)}。`;
  const direct = splitLongPart(paragraph, 200);
  assert.equal(direct.length, 3);
  assert.equal(direct.every((p) => p.text.length <= 200), true);
  assert.equal(direct.map((p) => p.text).join(''), paragraph);
  assert.equal(direct.some((p) => p.longKept), false);
});

test('没有句读的硬长段：整段保留成一整段（longKept），不截断、不许模型补', async () => {
  const HARD = '丁'.repeat(500);
  const logs = [];
  const llm = fakeLlm([() => toolChunks(JSON.stringify({ offsets: [250] }))]);
  const result = await split({ sourceId: 'm_hard', text: HARD, llm, log: (line) => logs.push(line) });
  assert.equal(result.parts.length, 2);
  assert.deepEqual(result.parts.map((p) => p.text), ['丁'.repeat(250), '丁'.repeat(250)]);
  assert.equal(result.longKept, 2, '两段都切不动，整段保留');
  assert.equal(result.parts.map((p) => p.text).join(''), HARD, '一个字都不许丢');
  assert.equal(logs.some((line) => line.includes('段落超限保留 2 段')), true);
});

test('文本通道：没有工具流时解析正文 JSON（cuts）', async () => {
  const { db } = tmpDb();
  const llm = fakeLlm([() => textChunks('```json\n' + cutsOf() + '\n```')]);
  const result = await split({ sourceId: 'm_src', text: SOURCE, llm });
  db.close();
  assert.equal(result.source, 'text');
  assert.equal(result.parts.length, 5);
});

test('两条通道都坏：不原地重试，落 pending 且原文不动', async () => {
  const { dbPath, db } = tmpDb();
  const llm = fakeLlm([brokenChunks]);
  const result = await split({ sourceId: 'm_src', text: SOURCE, db, llm });
  db.close();
  assert.equal(result, undefined);
  assert.equal(llm.calls.length, 1, '一次拿不到就交 pending，下一轮重放再拆');
  const rows = pendingRows(dbPath, 'split_item');
  assert.equal(rows.length, 1);
  assert.equal(rows[0].attempts, 1);
  assert.equal(rows[0].status, 'pending');
  assert.equal(JSON.parse(rows[0].payload_json).source_id, 'm_src');
});

test('两次都切不出来：落 pending attempts=1，原文一个字不动', async () => {
  const { dbPath, db } = tmpDb();
  const llm = fakeLlm([() => toolChunks(cutsOf(['找不到的锚串']))]);
  const result = await split({ sourceId: 'm_src', text: SOURCE, db, llm });
  db.close();

  assert.equal(result, undefined);
  assert.equal(llm.calls.length, 2, '首问 + 重问一次');
  const rows = pendingRows(dbPath, 'split_item');
  assert.equal(rows.length, 1);
  assert.equal(rows[0].attempts, 1);
  assert.equal(rows[0].status, 'pending', '失败不许写 done');
  assert.match(String(rows[0].last_error), /^anchor_not_found/);
  const payload = JSON.parse(rows[0].payload_json);
  assert.equal(payload.text, SOURCE, '原文不许截断');
  assert.equal(payload.length, SOURCE.length);
});

test('unavoidable=true：落 pending + WARN，不硬切', async () => {
  const { dbPath, db } = tmpDb();
  const logs = [];
  const llm = fakeLlm([() => toolChunks(JSON.stringify({
    unavoidable: true,
    reason: '强依赖上下文的推理，拆开就没意义',
  }))]);
  const result = await split({ sourceId: 'm_src', text: SOURCE, db, llm, log: (line) => logs.push(line) });
  db.close();

  assert.equal(result.unavoidable, true);
  assert.equal(result.parts.length, 0);
  assert.equal(llm.calls.length, 1, 'unavoidable 不该硬切重试');
  const rows = pendingRows(dbPath, 'split_item');
  assert.equal(rows.length, 1);
  assert.match(rows[0].last_error, /^unavoidable:/);
  assert.equal(logs.some((line) => line.startsWith('[split] unavoidable item=m_src reason=')), true);
});

test('提示词模板逐字替换，feedback 追加在输出之后', () => {
  const prompt = buildSplitPrompt({ sourceId: 'm_1', type: 'world', ts: 't', text: '正文', feedback: '第2个锚找不到' });
  assert.match(prompt, /^原条目 m_1（world，t）：正文/);
  assert.match(prompt, /上一轮没通过校验：第2个锚找不到/);
  assert.match(prompt, /只许切分点/);
  const clean = buildSplitPrompt({ sourceId: 'm_1', type: 'world', ts: 't', text: '正文' });
  assert.equal(clean.includes('上一轮'), false);
  assert.equal(clean.includes('{text}'), false);
});

/* ─────────────── 接线：每一子段单独过写入闸（真库 + 假 llm） ─────────────── */

function makeStore({ writeGate, llm, seedRows }) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-split-store-'));
  const dbPath = path.join(dir, 'memory.db');
  const db = new DatabaseSync(dbPath, { allowExtension: true });
  db.loadExtension(VEC_SO);
  db.exec(MEMORY_DDL);
  db.exec('CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);');
  db.exec("INSERT INTO meta (key, value) VALUES ('dim', '4');");
  db.exec('CREATE VIRTUAL TABLE vec_items USING vec0(embedding float[4] distance_metric=cosine);');
  db.exec('CREATE TABLE vec_map (rowid INTEGER PRIMARY KEY, id TEXT);');
  let seq = 1;
  const insertMemory = db.prepare('INSERT INTO memories (id, text, tags, type, status, ts, rowSeq) VALUES (?, ?, ?, ?, ?, ?, ?)');
  const insertVecMap = db.prepare('INSERT INTO vec_map (rowid, id) VALUES (?, ?)');
  const insertVec = db.prepare('INSERT INTO vec_items (rowid, embedding) VALUES (?, ?)');
  for (const row of seedRows) {
    insertMemory.run(row.id, row.text, '[]', 'world', 'active', '2026-11-01T00:00:00Z', seq);
    insertVecMap.run(seq, row.id);
    insertVec.run(BigInt(seq), new Uint8Array(Float32Array.from([1, 0, 0, 0]).buffer));
    seq += 1;
  }
  db.close();
  applySchema(dbPath, { logger: () => {} });

  const cfg = {
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
    pipeline: { writeGate, writeGateThreshold: 0.9 },
    logger: { info() {}, warn() {} },
  };
  const store = createStore(cfg);
  store.attachLlm(llm);
  return { store, dir, dbPath };
}

function readAll(dbPath, sql, ...params) {
  const db = new DatabaseSync(dbPath, { readOnly: true });
  try {
    return db.prepare(sql).all(...params);
  } finally {
    db.close();
  }
}

const verdictChunks = (args) => [
  { type: 'tool-call-delta', index: 0, id: 'call_2', name: 'submit_verdict', argumentsDelta: args },
  { type: 'finish', reason: { kind: 'stop' } },
];

test('接线：五条子段逐条过闸，命中 DUPLICATE 的那段记 discards，其余入库，原条 archive', async () => {
  const dupText = SENT2;
  // 拆分调用先来，之后每一段过闸问一次去重模型（hash 相同的那段走规则层，不问）
  const llm = fakeLlm([
    () => toolChunks(cutsOf()),
    () => verdictChunks(JSON.stringify({ verdict: 'DISTINCT', matchedId: '', reason: '不同事实' })),
  ]);
  const { store, dbPath } = makeStore({
    writeGate: true,
    llm,
    seedRows: [{ id: 'm_src', text: SOURCE }, { id: 'm_dup', text: dupText }],
  });
  const db = new DatabaseSync(dbPath);
  enqueue(db, 'split_item', 'm_src', { source_id: 'm_src', text: SOURCE, type: 'world' });

  const stats = await consumeSplitOps({ db, store, llm, limit: 5, maxChars: 200 });
  db.close();

  assert.equal(stats.scanned, 1);
  assert.equal(stats.parts, 5);
  assert.equal(stats.inserted, 4);
  assert.equal(stats.deduped, 1);
  // 5 次模型调用 = 1 次切分 + 4 次逐段判重；批量放行只会有 1 次，这里证明是逐条过的闸
  assert.equal(llm.calls.length, 5);
  assert.equal(llm.calls[1].tools[0].name, 'submit_verdict');

  const discards = readAll(dbPath, 'SELECT * FROM discards');
  assert.equal(discards.length, 1);
  assert.equal(discards[0].verdict, 'DUPLICATE');
  assert.equal(discards[0].matched_id, 'm_dup');
  assert.equal(discards[0].candidate_hash, hashText(dupText));

  const src = readAll(dbPath, 'SELECT status, reason FROM memories WHERE id = ?', 'm_src')[0];
  assert.equal(src.status, 'archived');
  assert.match(src.reason, /拆成 4 条/);

  const inserted = readAll(dbPath, "SELECT id, text, supersedes FROM memories WHERE source = 'sleep:split:m_src'");
  assert.equal(inserted.length, 4);
  for (const row of inserted) {
    assert.deepEqual(JSON.parse(row.supersedes), ['m_src']);
    assert.equal(SOURCE.includes(row.text), true, '入库的每一段都是原文片段');
  }
  const ops = readAll(dbPath, "SELECT status FROM pending_ops WHERE kind = 'split_item'");
  assert.equal(ops[0].status, 'done');
});

test('接线：合并 add 候选没有原条，切完直接入库、不归档、不挂 supersedes', async () => {
  const llm = fakeLlm([
    () => toolChunks(cutsOf()),
    // 5 段向量同向 → 第 2 段起都要问一次去重模型，全是不同事实
    () => verdictChunks(JSON.stringify({ verdict: 'DISTINCT', matchedId: '', reason: '不同事实' })),
  ]);
  const { store, dbPath } = makeStore({ writeGate: true, llm, seedRows: [] });
  const db = new DatabaseSync(dbPath);
  const orphanId = 'merge:fp123:0';
  enqueue(db, 'split_item', orphanId, { source_id: orphanId, text: SOURCE, op: 'add' });

  const stats = await consumeSplitOps({ db, store, llm, limit: 5, maxChars: 200 });
  db.close();

  assert.equal(stats.scanned, 1);
  assert.equal(stats.inserted, 5);
  assert.equal(stats.archived, 0, 'add 候选没有原条，不该归档任何东西');
  assert.equal(stats.skipped, 0, '有 payload.text 就不算 source_missing');

  const rows = readAll(dbPath, 'SELECT text, supersedes, status FROM memories WHERE source = ?', `sleep:split:${orphanId}`);
  assert.equal(rows.length, 5);
  for (const row of rows) {
    assert.notEqual(row.status, 'archived');
    assert.equal(row.supersedes, null, 'add 候选没有原条可挂');
  }
  const hasOrphanOriginal = readAll(dbPath, 'SELECT id FROM memories WHERE id = ?', orphanId).length;
  assert.equal(hasOrphanOriginal, 0);
  const ops = readAll(dbPath, "SELECT status FROM pending_ops WHERE kind = ?", 'split_item');
  assert.equal(ops[0].status, 'done');
});
