import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { applySchema } from '../scripts/apply-pipeline-schema.mjs';
import { merge, normalizeOps, validateOps, SUBMIT_OPS_TOOL, MERGE_SYSTEM, buildMergePrompt } from '../lib/agents/merge.js';

function tmpDb() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-merge-'));
  const dbPath = path.join(dir, 'memory.db');
  new DatabaseSync(dbPath).close();
  applySchema(dbPath, { logger: () => {} });
  const db = new DatabaseSync(dbPath);
  return { dir, dbPath, db };
}

function pendingRows(dbPath, kind) {
  const db = new DatabaseSync(dbPath, { readOnly: true });
  try {
    return db.prepare('SELECT * FROM pending_ops WHERE kind = ?').all(kind);
  } finally {
    db.close();
  }
}

/** 假 llm：按调用顺序取 handler，超了重复最后一个。 */
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

const brokenChunks = () => [
  { type: 'text-delta', text: '这边不是 JSON' },
  { type: 'tool-call-delta', index: 0, name: 'submit_ops', argumentsDelta: '{oops' },
  { type: 'finish', reason: { kind: 'stop' } },
];

const ITEMS = [
  {
    id: 'm_1',
    type: 'world',
    text: '树莓派温度 45 度，2026-11-07 重启后 /srv/app 磁盘用了 90%。',
    ts: '2026-11-07T10:00:00Z',
    entities: ['树莓派'],
  },
  {
    id: 'm_2',
    type: 'world',
    text: '树莓派温度 45 度，风扇转速 3000 转。',
    ts: '2026-11-06T10:00:00Z',
    entities: ['树莓派'],
  },
];

const GOOD_OPS = [{
  op: 'update',
  ids: ['m_1'],
  text: '树莓派温度 45 度，2026-11-07 重启后磁盘用了 90%。',
  evidence: [{ id: 'm_1', quote: '树莓派温度 45 度，2026-11-07 重启后 /srv/app 磁盘用了 90%。' }],
  needsSplit: false,
  reason: '同一件事收短',
}];

test('tool 通道：submit_ops 的 argumentsDelta 拼成 {ops}，source=tool，参数照文档', async () => {
  const { db } = tmpDb();
  const llm = fakeLlm([() => toolChunks(JSON.stringify({ ops: GOOD_OPS }))]);
  const result = await merge({ items: ITEMS, llm, provider: 'fake', model: 'fake' });
  db.close();

  assert.equal(result.source, 'tool');
  assert.equal(result.ops.length, 1);
  assert.equal(result.ops[0].op, 'update');
  assert.deepEqual(result.ops[0].ids, ['m_1']);
  assert.equal(result.ops[0].evidence[0].id, 'm_1');
  assert.equal(llm.calls.length, 1);

  const req = llm.calls[0];
  assert.equal(req.reasoningEffort, 'high');
  assert.equal('maxTokens' in req, false, '2026-11-06 维护者硬规矩：默认不传 maxTokens');
  assert.equal(req.temperature, 0);
  assert.equal(req.tools[0].name, 'submit_ops');
  assert.deepEqual(req.tools[0].parameters.properties.ops.items.properties.op.enum, ['merge', 'update', 'archive', 'add']);
  assert.deepEqual(req.tools[0].parameters.required, ['ops']);
  const opSchema = req.tools[0].parameters.properties.ops.items;
  assert.deepEqual(opSchema.required, ['op', 'ids', 'text', 'evidence']);
  assert.equal(opSchema.properties.ids.type, 'array');
  assert.equal(opSchema.properties.needsSplit.type, 'boolean');
  assert.deepEqual(opSchema.properties.evidence.items.required, ['id', 'quote']);
  assert.equal(MERGE_SYSTEM.includes('你是长期记忆的合并整理员'), true);
  assert.equal(MERGE_SYSTEM.includes('不许截断'), true);
  const prompt = req.messages[0].content[0].text;
  // 2026-11-14：改动上限从提示词里删了，只剩批次 id
  assert.match(prompt, /^本批 未知。\n待整理记忆\n\[/);
  assert.match(prompt, /近似候选（库里可能已有的同类事实）\n\[\]/);
  assert.equal(prompt.includes('{items:'), false);
});

test('maxTokens 一律不传：参数给什么都不进请求，由 provider 封顶', async () => {
  const { db } = tmpDb();
  const off = fakeLlm([() => toolChunks(JSON.stringify({ ops: GOOD_OPS }))]);
  await merge({ items: ITEMS, llm: off, maxTokens: 0 });
  assert.equal('maxTokens' in off.calls[0], false, '0 = 不传');
  db.close();

  // 2026-11-14：配置口子删了，调用方就算传了数字也不再进请求
  const { db: db2 } = tmpDb();
  const on = fakeLlm([() => toolChunks(JSON.stringify({ ops: GOOD_OPS }))]);
  await merge({ items: ITEMS, llm: on, maxTokens: 4321 });
  assert.equal('maxTokens' in on.calls[0], false, '显式给了也不传，服务端按自己的上限走');
  db2.close();
});

test('提示词模板逐字替换：clusterId / 两段 JSON 都落地，改动上限已删', () => {
  const prompt = buildMergePrompt({ items: ITEMS, candidates: [{ id: 'm_9', text: '旧条', score: 0.83 }], clusterId: 'fp_8f3a', maxChanges: 4 });
  assert.match(prompt, /^本批 fp_8f3a。/, '不再写本轮改动上限（动作数不设限）');
  assert.equal(prompt.includes('maxChanges'), false);
  assert.equal(prompt.includes('"id":"m_1"'), true);
  assert.equal(prompt.includes('"score":0.83'), true);
  assert.equal(prompt.includes('{clusterId}'), false);
  assert.equal(prompt.includes('{candidates:'), false);
});

test('文本通道：没有工具流时解析正文 JSON，source=text', async () => {
  const { db } = tmpDb();
  const llm = fakeLlm([() => textChunks('```json\n{"ops":[]}\n```')]);
  const result = await merge({ items: ITEMS, llm });
  db.close();
  assert.equal(result.source, 'text');
  assert.deepEqual(result.ops, []);
});

test('两条通道都坏：不原地重试固定次数，一次就交上层', async () => {
  const { db } = tmpDb();
  const llm = fakeLlm([brokenChunks]);
  const result = await merge({ items: ITEMS, llm });
  db.close();
  assert.equal(result, undefined);
  assert.equal(llm.calls.length, 1, '失败重试交给 pending_ops 下一轮，不在一次调用里循环');
});

test('证据校验正例：数字、日期、路径都在 quote 里 → valid', () => {
  const { valid, invalid } = validateOps(GOOD_OPS, ITEMS);
  assert.equal(invalid.length, 0);
  assert.equal(valid.length, 1);
  assert.equal(valid[0].op, 'update');
  assert.equal(valid[0].needsSplit, false);
  assert.equal(valid[0].text, GOOD_OPS[0].text);
});

test('反例一：quote 不是源文子串 → invalid 并落 pending_ops(merge_op)', () => {
  const { dbPath, db } = tmpDb();
  const ops = [{
    op: 'update',
    ids: ['m_1'],
    text: '树莓派温度 46 度',
    evidence: [{ id: 'm_1', quote: '树莓派温度 46 度' }],
  }];
  const { valid, invalid } = validateOps(ops, ITEMS, { db, batchId: 'fp_test' });
  db.close();
  assert.equal(valid.length, 0);
  assert.equal(invalid.length, 1);
  assert.equal(invalid[0].reason, 'evidence_quote_not_substring');
  assert.equal(invalid[0].quote, '树莓派温度 46 度');
  const rows = pendingRows(dbPath, 'merge_op');
  assert.equal(rows.length, 1);
  assert.equal(rows[0].status, 'pending');
  const payload = JSON.parse(rows[0].payload_json);
  assert.equal(payload.reason, 'evidence_quote_not_substring');
  assert.equal(payload.quote, '树莓派温度 46 度');
  assert.equal(payload.batch, 'fp_test');
  assert.deepEqual(payload.ids, ['m_1']);
});

test('反例二：evidence 的 id 不在本批 → invalid，不修不猜', () => {
  const ops = [{
    op: 'merge',
    ids: ['m_1', 'm_2'],
    text: '树莓派温度 45 度，风扇转速 3000 转。',
    evidence: [{ id: 'm_404', quote: '树莓派温度 45 度' }],
  }];
  const { valid, invalid } = validateOps(ops, ITEMS);
  assert.equal(valid.length, 0);
  assert.equal(invalid[0].reason, 'evidence_id_not_in_batch');
  assert.equal(invalid[0].id, 'm_404');
});

test('反例三：merge 只给 1 个 id → invalid', () => {
  const ops = [{
    op: 'merge',
    ids: ['m_1'],
    text: '树莓派温度 45 度',
    evidence: [{ id: 'm_1', quote: '树莓派温度 45 度' }],
  }];
  const { valid, invalid } = validateOps(ops, ITEMS);
  assert.equal(valid.length, 0);
  assert.equal(invalid[0].reason, 'merge_ids_lt_2');
});

test('反例四：正文里的数字改了、证据里找不到 → fact_not_in_evidence', () => {
  const ops = [{
    op: 'update',
    ids: ['m_1'],
    text: '树莓派温度 46 度',
    evidence: [{ id: 'm_1', quote: '树莓派温度 45 度' }],
  }];
  const { valid, invalid } = validateOps(ops, ITEMS);
  assert.equal(valid.length, 0);
  assert.equal(invalid[0].reason, 'fact_not_in_evidence');
  assert.equal(invalid[0].fact, '46');
});

test('超长正文：强制 needsSplit=true，一个字都没截', () => {
  const sentence = '树莓派温度 45 度。';
  const longText = sentence.repeat(26);
  assert.equal(longText.length > 200, true);
  const ops = [{
    op: 'add',
    ids: [],
    text: longText,
    evidence: [{ id: 'm_1', quote: '树莓派温度 45 度' }],
    needsSplit: false,
  }];
  const { valid, invalid } = validateOps(ops, ITEMS);
  assert.equal(invalid.length, 0);
  assert.equal(valid.length, 1);
  assert.equal(valid[0].needsSplit, true);
  assert.equal(valid[0].text.length, longText.length);
  assert.equal(valid[0].text, longText);
});

test('archive 允许空正文，但 evidence 仍要能对上源文', () => {
  const ops = [{
    op: 'archive',
    ids: ['m_2'],
    text: '',
    evidence: [{ id: 'm_2', quote: '风扇转速 3000 转' }],
    reason: '状态过期',
  }];
  const { valid, invalid } = validateOps(ops, ITEMS);
  assert.equal(invalid.length, 0);
  assert.equal(valid.length, 1);
  assert.equal(valid[0].op, 'archive');
});

test('三层校验①：quote 被截断、事实落在源文里 → 现在通过（首跑误杀 4 条）', () => {
  // 模型把前缀截掉了：quote 只有后半句，日期和路径本来在源文 m_1 里
  const ops = [{
    op: 'update',
    ids: ['m_1'],
    text: '2026-11-07 重启后 /srv/app 磁盘用了 90%。',
    evidence: [{ id: 'm_1', quote: '磁盘用了 90%' }],
  }];
  const { valid, invalid } = validateOps(ops, ITEMS);
  assert.equal(invalid.length, 0);
  assert.equal(valid.length, 1);
  assert.equal(valid[0].text, ops[0].text);
});

test('三层校验②：quote 凭空捏造（源文里也没有）→ 仍然 invalid', () => {
  const ops = [{
    op: 'update',
    ids: ['m_1'],
    text: '2026-12-31 磁盘用了 99%。',
    evidence: [{ id: 'm_1', quote: '2026-12-31 磁盘用了 99%' }],
  }];
  const { valid, invalid } = validateOps(ops, ITEMS);
  assert.equal(valid.length, 0);
  assert.equal(invalid.length, 1);
  assert.equal(invalid[0].reason, 'evidence_quote_not_substring');
});

test('三层校验③：quote 是真子串，但事实两层都找不到 → invalid fact_not_in_evidence', () => {
  const ops = [{
    op: 'update',
    ids: ['m_1'],
    text: '树莓派粉丝转速 77 转。',
    evidence: [{ id: 'm_1', quote: '树莓派温度 45 度' }],
  }];
  const { valid, invalid } = validateOps(ops, ITEMS);
  assert.equal(valid.length, 0);
  assert.equal(invalid.length, 1);
  assert.equal(invalid[0].reason, 'fact_not_in_evidence');
  assert.equal(invalid[0].fact, '77');
});

test('lint 复核判 keep → op 原样放行、打 __lintKept，不重写', async () => {
  const { db } = tmpDb();
  const badOps = [{
    op: 'update',
    ids: ['m_1'],
    text: '但是树莓派温度 45 度。',
    evidence: [{ id: 'm_1', quote: '树莓派温度 45 度，2026-11-07 重启后 /srv/app 磁盘用了 90%。' }],
    needsSplit: false,
    reason: '同一件事收短',
  }];
  const reason = '「但是」承接的是同一条记忆里的前半句，跟原文一起读得通';
  const llm = fakeLlm([
    () => toolChunks(JSON.stringify({ ops: badOps })),
    () => textChunks(JSON.stringify({ decisions: [{ index: 0, verdict: 'keep', reason }] })),
  ]);
  const result = await merge({ items: ITEMS, llm, provider: 'fake', model: 'fake', clusterId: 'abc' });
  db.close();

  assert.equal(llm.calls.length, 2, '问复核一次，不重写');
  assert.equal(result.retries.lint, 0);
  assert.equal(result.ops.length, 1);
  assert.equal(result.ops[0].text, badOps[0].text, 'keep 的 op 一个字不改');
  assert.equal(result.ops[0].__lintKept, reason, '要带着复核理由，写库时才能 __lintReview 放行');
});

test('lint 复核判 redo → 带复核意见重做一次', async () => {
  const { db } = tmpDb();
  const badOps = [{
    op: 'update',
    ids: ['m_1'],
    text: '但是树莓派温度 45 度。',
    evidence: [{ id: 'm_1', quote: '树莓派温度 45 度，2026-11-07 重启后 /srv/app 磁盘用了 90%。' }],
    needsSplit: false,
    reason: '同一件事收短',
  }];
  const llm = fakeLlm([
    () => toolChunks(JSON.stringify({ ops: badOps })),
    () => textChunks(JSON.stringify({
      decisions: [{ index: 0, verdict: 'redo', reason: '「但是」开头，单独看不知道在但是什么' }],
    })),
    () => toolChunks(JSON.stringify({ ops: GOOD_OPS })),
  ]);
  const lines = [];
  const result = await merge({
    items: ITEMS, llm, provider: 'fake', model: 'fake', clusterId: 'abc',
    log: (line) => lines.push(String(line)),
  });
  db.close();

  assert.equal(llm.calls.length, 3, '首轮 + 复核 + 重做一次，不许多');
  const redoPrompt = String(llm.calls[2]?.messages?.[0]?.content?.[0]?.text
    ?? llm.calls[2]?.messages?.[0]?.content ?? '');
  assert.match(redoPrompt, /在但是什么/, '重做要带复核 agent 的原话');
  assert.equal(result.retries.lint, 1);
  assert.equal(result.source, 'tool');
  assert.equal(result.ops[0].text, GOOD_OPS[0].text);
});

test('lint 重试日志带 codes 与 index，不带正文（2026-11-06 t-mw515yk8）', async () => {
  const { db } = tmpDb();
  const badOps = [{
    op: 'update',
    ids: ['m_1'],
    text: '但是树莓派温度 45 度。',
    evidence: [{ id: 'm_1', quote: '树莓派温度 45 度，2026-11-07 重启后 /srv/app 磁盘用了 90%。' }],
    needsSplit: false,
    reason: '同一件事收短',
  }];
  const llm = fakeLlm([
    () => toolChunks(JSON.stringify({ ops: badOps })),
    () => toolChunks(JSON.stringify({ ops: GOOD_OPS })),
  ]);
  const lines = [];
  const result = await merge({
    items: ITEMS, llm, provider: 'fake', model: 'fake', clusterId: 'abc',
    lintReview: false, // 关复核，专测旧的「重写一次」退化路径；复核路径见下一条
    log: (line) => lines.push(String(line)),
  });
  db.close();

  assert.equal(result.source, 'tool');
  assert.equal(result.retries.lint, 1);
  const retryLine = lines.find((l) => l.includes('retry reason=lint'));
  assert.ok(retryLine, `缺 lint 重试日志：${lines.join(' | ')}`);
  assert.match(retryLine, /n=1/);
  assert.match(retryLine, /codes=i0:leading_conjunction/, '要打 op 下标和 code');
  assert.equal(retryLine.includes('树莓派温度 45 度'), false, '不许打正文');
  const stuckLine = lines.find((l) => l.includes('lint 仍拒'));
  assert.equal(stuckLine, undefined, '第二轮通过，不该有仍拒行');
});

/** 假 llm：收完这些 chunk 之后由宿主抛错（模拟 MALFORMED_RESPONSE 这种流尾异常）。 */
function fakeThrowingLlm(chunks, error) {
  const calls = [];
  return {
    calls,
    stream(options) {
      calls.push(options);
      return (async function* streamChunks() {
        for (const chunk of chunks) yield chunk;
        throw error;
      })();
    },
  };
}

const malformedHostError = () => new Error(
  'MALFORMED_RESPONSE: Unexpected non-whitespace character after JSON at position 154',
);

test('模型把同一对象连写两遍：配平取第一个，不报 call_fail', async () => {
  const { db } = tmpDb();
  // 2026-11-14 的现场：`{...} {...}`，宿主解析时抛 Unexpected non-whitespace
  const args = JSON.stringify({ ops: GOOD_OPS }) + JSON.stringify({ ops: [] });
  const llm = fakeLlm([() => toolChunks(args)]);
  const failures = [];
  const result = await merge({ items: ITEMS, llm, onFail: (summary, kind) => failures.push({ summary, kind }) });
  db.close();

  assert.equal(result.source, 'tool');
  assert.equal(result.ops.length, 1, '只取第一个对象的提案');
  assert.equal(result.ops[0].op, 'update');
  assert.deepEqual(failures, [], '不该走到 call_fail');
});

test('正常单对象行为不变：严格解析先命中', async () => {
  const { db } = tmpDb();
  const llm = fakeLlm([() => toolChunks(JSON.stringify({ ops: [] }))]);
  const failures = [];
  const result = await merge({ items: ITEMS, llm, onFail: (summary, kind) => failures.push(kind) });
  db.close();

  assert.equal(result.source, 'tool');
  assert.deepEqual(result.ops, []);
  assert.deepEqual(failures, []);
});

test('配平要跳过字符串里的花括号，只取第一个顶层对象', () => {
  const ops = normalizeOps('{"ops":[],"note":"}"}{"ops":[{"op":"add"}]}');
  assert.deepEqual(ops, [], '字符串里的 } 不算顶层结束');
});

test('真截断仍然失败：配平规则不吞错', async () => {
  const { db } = tmpDb();
  const truncated = JSON.stringify({ ops: GOOD_OPS }).slice(0, 60);
  const llm = fakeLlm([() => toolChunks(truncated)]);
  const failures = [];
  const result = await merge({ items: ITEMS, llm, onFail: (summary, kind) => failures.push(kind) });
  db.close();

  assert.equal(result, undefined);
  assert.deepEqual(failures, ['parse_fail']);
});

test('宿主抛错但 args 已收全：用 partials 走解析成功', async () => {
  const { db } = tmpDb();
  const args = JSON.stringify({ ops: GOOD_OPS });
  const llm = fakeThrowingLlm([
    { type: 'tool-call-delta', index: 0, id: 'call_1', name: 'submit_ops', argumentsDelta: args.slice(0, 30) },
    { type: 'tool-call-delta', index: 0, argumentsDelta: args.slice(30) },
  ], malformedHostError());
  const failures = [];
  const result = await merge({ items: ITEMS, llm, onFail: (summary, kind) => failures.push({ summary, kind }) });
  db.close();

  assert.equal(result.source, 'tool');
  assert.equal(result.ops.length, 1);
  assert.equal(result.ops[0].op, 'update');
  assert.deepEqual(failures, []);
});

test('宿主抛错且输出真坏：不吞错，仍 call_fail，trace 保留已收原文', async () => {
  const { db } = tmpDb();
  const llm = fakeThrowingLlm([
    { type: 'tool-call-delta', index: 0, name: 'submit_ops', argumentsDelta: '{"ops":[' },
  ], malformedHostError());
  const failures = [];
  const result = await merge({ items: ITEMS, llm, onFail: (summary, kind) => failures.push({ summary, kind }) });
  db.close();

  assert.equal(result, undefined);
  assert.equal(failures.length, 1);
  assert.equal(failures[0].kind, 'call_fail', '宿主错误照旧交上层，不降级成 parse_fail');
  assert.match(failures[0].summary, /host:MALFORMED_RESPONSE/, '宿主错误进 last_error');
  assert.match(failures[0].summary, /tool=8字:"/, '已收到的 partials 原文留在摘要里');
});

test('提示词：版本规则段与证据写法保留句都在 MERGE_SYSTEM 里', () => {
  assert.equal(MERGE_SYSTEM.includes('版本规则'), true);
  assert.equal(MERGE_SYSTEM.includes('合并前先核对对象、事件、版本和适用范围'), true);
  assert.equal(MERGE_SYSTEM.includes('改稿后的复核不得借用上一版的理由'), true);
  assert.equal(MERGE_SYSTEM.includes('只有原文明示新版本取代旧版本时才 update'), true);
  assert.equal(MERGE_SYSTEM.includes('分不清版本就跳过这一组'), true);
  assert.equal(MERGE_SYSTEM.includes('避免把一次迁移的基线写成通用要求'), true);
  assert.equal(MERGE_SYSTEM.includes('证据里的路径、行号和单位先按原文写法保留'), true);
});

test('事实校验：文件名与行号同句出现 → 放行（不再拼成单个 token 比）', () => {
  const items = [{
    id: 'm_fl',
    type: 'world',
    text: '入口在 lib/sleep.js 第 382 行，sleep 周期 1800 秒。',
  }];
  const ops = [{
    op: 'update',
    ids: ['m_fl'],
    text: '入口在 lib/sleep.js:382。',
    evidence: [{ id: 'm_fl', quote: '入口在 lib/sleep.js 第 382 行' }],
  }];
  const { valid, invalid } = validateOps(ops, items);
  assert.equal(invalid.length, 0, `同句命中不该判废：${invalid.map((x) => x.reason).join(',')}`);
  assert.equal(valid.length, 1);
});

test('事实校验：文件名与行号分属两句 → 仍拒 fact_not_in_evidence', () => {
  const items = [{
    id: 'm_fl2',
    type: 'world',
    text: '入口在 lib/sleep.js。另记行号 382 在附录里。',
  }];
  const ops = [{
    op: 'update',
    ids: ['m_fl2'],
    text: '入口在 lib/sleep.js:382。',
    evidence: [{ id: 'm_fl2', quote: '入口在 lib/sleep.js' }],
  }];
  const { valid, invalid } = validateOps(ops, items);
  assert.equal(valid.length, 0, '跨句同现不算证据');
  assert.equal(invalid.length, 1);
  assert.equal(invalid[0].reason, 'fact_not_in_evidence');
  assert.equal(invalid[0].fact, 'sleep.js:382');
});
