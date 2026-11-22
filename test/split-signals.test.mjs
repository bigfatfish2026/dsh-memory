/**
 * 拆分自足性段级信号（2026-11-20 P0 阶段一，方案 v3 §3）。
 *
 * 钉住的五件事：
 *   ① 四条信号各自的命中 / 不命中；
 *   ② 阶段一下 validateParts 的 ok 不受信号影响，signals 照常带回；
 *   ③ signalsStrict 打开（阶段二开关，默认关）时才判废、才重问；
 *   ④ text-lint：分句首单字指代只记（bare_reference_clause，warn），不阻断；整条首字仍是 reject；
 *   ⑤ 子段 source 两种来源分开：有记忆 id 写 sleep:split:<id>，队列键写 sleep:split:queue:<key>。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { applySchema } from '../scripts/apply-pipeline-schema.mjs';
import {
  delimiterBalance, deicticOpeners, missingPremise, processState, splitSignalsOf,
  validateParts, split, SPLIT_SYSTEM, SPLIT_DEICTIC_OPENERS,
} from '../lib/agents/split.js';
import { lintMemoryText, BARE_REFERENCES } from '../lib/text-lint.js';
import { SPLIT_RULES } from '../lib/lint-prompt.js';
import { consumeSplitOps } from '../lib/pipeline-split.js';
import { enqueue } from '../lib/ingest-index.js';

const A = '小林去机房巡检时看了风扇的转速，顺手记下当天的温湿度读数。';
// 段首指代只在 validateParts 层面测（split 的段落单独过 lint，「它」在段首就是整条首字，本来就拒）。
const B = '它现在能读到 14551 条记忆，拆分之后一条都不许丢。';
const SOURCE = `${A}${B}`;
// 走 split()/consumeSplitOps 的样本：命中 split_process 信号，且正文 lint 不拒。
const PROCESS_B = '本轮工具用不了，拆分没跑完，但库里 14551 条记忆一条都没丢。';
const SOURCE_PROCESS = `${A}${PROCESS_B}`;
const SRC_ID = 'm_src';

const toolChunks = (args) => [
  { type: 'tool-call-delta', index: 0, id: 'call_1', name: 'submit_parts', argumentsDelta: args.slice(0, 20) },
  { type: 'tool-call-delta', index: 0, argumentsDelta: args.slice(20) },
  { type: 'finish', reason: { kind: 'stop' } },
];

/** 按提示词里的原文在第一个句号后切两段（只给切分点，正文由代码切）。 */
function fakeLlm() {
  const calls = [];
  return {
    calls,
    stream(options) {
      calls.push(options);
      const prompt = String(options?.messages?.[0]?.content?.[0]?.text || '');
      const m = /）：([\s\S]*?)\n\n输出/.exec(prompt);
      const src = m ? m[1] : '';
      const cut = src.indexOf('。') + 1;
      const cuts = src && cut > 0 && cut < src.length ? [src.slice(cut, cut + 8)] : [];
      return (async function* streamChunks() {
        yield* toolChunks(JSON.stringify({ cuts, unavoidable: false, reason: '' }));
      })();
    },
  };
}

function tmpDb() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-split-signals-'));
  const dbPath = path.join(dir, 'memory.db');
  new DatabaseSync(dbPath).close();
  applySchema(dbPath, { logger: () => {} });
  return { dir, dbPath, db: new DatabaseSync(dbPath) };
}

function fakeStore(metas = []) {
  const saved = [];
  return {
    metas: () => metas,
    typeOf: () => 'world',
    async save(text, tags, source, extra) {
      const rec = { id: `new_${saved.length + 1}`, text, source };
      saved.push({ text, source, extra });
      metas.push({ id: rec.id, text, status: 'active', tags: [], entities: [], source });
      return rec;
    },
    async archive(id) {
      const m = metas.find((x) => x.id === id);
      if (m) m.status = 'archived';
    },
    report: () => ({ saved: [...saved] }),
  };
}

// ── ① 四条信号各自命中 / 不命中 ───────────────────────────────

test('split_delimiter：引号/括号/围栏不配平命中，成对不命中', () => {
  const bad = [
    { text: '他说「这条没闭合，然后继续写下去。' },
    { text: '括号没闭合（只剩左边，右边没了。' },
    { text: '代码围栏开了一个 ``` 但没有收尾。' },
    { text: '直引号只有一个 " 也算破。' },
  ];
  assert.deepEqual(delimiterBalance(bad).map((s) => s.code), [
    'split_delimiter', 'split_delimiter', 'split_delimiter', 'split_delimiter',
  ]);
  assert.deepEqual(
    delimiterBalance([
      { text: '他说「这条闭合了」，然后继续写下去。' },
      { text: '括号（左右都有）也成对。' },
      { text: '代码围栏 ```js 和收尾 ``` 成对。' },
      { text: '直引号 "一对" 也算成对。' },
    ]),
    [],
  );
});

test('split_deictic：段首指代命中，具体对象开头不命中', () => {
  const hits = deicticOpeners([
    { text: '这个仓库的 src 只放 .ts 源码。' },
    { text: '该插件默认只记不拒。' },
    { text: '上述两条都落进了信号计数。' },
  ]);
  assert.deepEqual(hits.map((s) => s.code), ['split_deictic', 'split_deictic', 'split_deictic']);
  assert.deepEqual(hits.map((s) => s.index), [0, 1, 2]);
  assert.equal(hits[0].hint.includes('这个仓库'), true);
  assert.deepEqual(
    deicticOpeners([{ text: '用户把 dsh-memory 的 jsonl 冻结了。' }]),
    [],
  );
  // 词表与提示词口径一致：12 项
  assert.equal(SPLIT_DEICTIC_OPENERS.length, 12);
  for (const word of ['这', '该', '它', '此', '那', '上述', '上面', '这份', '本文件', '该目录', '这个仓库', '这个插件']) {
    assert.equal(SPLIT_DEICTIC_OPENERS.includes(word), true, `词表缺 ${word}`);
  }
});

test('split_premise：只有结论没有前提才命中', () => {
  const hits = missingPremise([
    { text: '结论是重启服务就好。' },
    { text: '建议改成按并发数小批领活。' },
    { text: '原因在日志里。' },
  ]);
  assert.deepEqual(hits.map((s) => s.code), ['split_premise', 'split_premise', 'split_premise']);
  assert.deepEqual(
    missingPremise([
      { text: '结论是重启服务就好，因为日志里出现过 OOM。' },
      { text: '做法是先在副本库上跑一遍，因为生产库不能写。' },
      { text: '用户把 jsonl 冻结了，只留作历史副本。' },
    ]),
    [],
  );
});

test('split_process：本轮/这次 + 用不了/不可用/失败 命中', () => {
  const hits = processState([
    { text: '本轮工具用不了，拆分没跑完。' },
    { text: '这次失败是上游限流造成的。' },
  ]);
  assert.deepEqual(hits.map((s) => s.code), ['split_process', 'split_process']);
  assert.deepEqual(
    processState([
      { text: '上次失败的原因是上游限流，已经记在日志里。' },
      { text: '本轮拆分落了 12 条子段，全部进了库。' },
    ]),
    [],
  );
});

test('splitSignalsOf：同一段可命中多条，按段下标排序', () => {
  const signals = splitSignalsOf([
    { text: '正常的一段，讲的是 dsh-memory 的 storePath 落在 /home/julia/fatfish/memory。' },
    { text: '结论是「引号没闭合（还缺一个括号，直接上线。' },
    { text: '这个仓库的 src 只放 .ts 源码，lib 是构建产物。' },
  ]);
  assert.deepEqual(signals.map((s) => s.code).sort(), [
    'split_deictic', 'split_delimiter', 'split_premise',
  ]);
  assert.deepEqual(signals.map((s) => `${s.code}@${s.index}`).sort(), [
    'split_deictic@2', 'split_delimiter@1', 'split_premise@1',
  ]);
});

// ── ②③ validateParts：阶段一只记、开关打开才拒 ─────────────────

test('validateParts 阶段一：信号只进 signals，ok 仍为 true', () => {
  const checked = validateParts({ cuts: [B.slice(0, 12)] }, SOURCE);
  assert.equal(checked.ok, true);
  assert.equal(checked.parts.length, 2);
  assert.equal(checked.parts.map((p) => p.text).join(''), SOURCE);
  assert.equal(checked.signals.some((s) => s.code === 'split_deictic' && s.index === 1), true);
});

test('validateParts 开 signalsStrict：同一份切分被判废，理由带 split_signal', () => {
  const checked = validateParts({ cuts: [B.slice(0, 12)] }, SOURCE, { signalsStrict: true });
  assert.equal(checked.ok, false);
  assert.equal(checked.reason, 'split_signal:split_deictic');
  assert.equal(Array.isArray(checked.signals) && checked.signals.length > 0, true);
});

// ── split() 阶段一不丢段；开关打开才重问/落 pending ─────────────

test('split() 阶段一：命中过程性状态信号仍返回两段，不丢段、不落 pending', async () => {
  const { db } = tmpDb();
  const logs = [];
  const result = await split({
    sourceId: SRC_ID, text: SOURCE_PROCESS, llm: fakeLlm(), db, log: (line) => logs.push(line),
  });
  const pending = db.prepare("SELECT count(*) AS n FROM pending_ops WHERE kind = 'split_item'").get();
  db.close();

  assert.ok(result, '阶段一不许因为信号返回 undefined');
  assert.equal(result.parts.length, 2);
  assert.equal(result.parts.map((p) => p.text).join(''), SOURCE_PROCESS);
  assert.equal(result.signals.some((s) => s.code === 'split_process'), true);
  assert.equal(logs.some((line) => line.includes('signal=split_process')), true);
  assert.equal(logs.some((line) => line.includes('signals=') && line.includes('只记不拒')), true);
  assert.equal(Number(pending.n), 0, '阶段一不许因为信号落 pending');
});

test('split() 开 signalsStrict：重问一次仍不合格就退拆，日志写明 split_signal', async () => {
  const { db } = tmpDb();
  const logs = [];
  const llm = fakeLlm();
  const result = await split({
    sourceId: SRC_ID, text: SOURCE_PROCESS, llm, db, signalsStrict: true, log: (line) => logs.push(line),
  });
  db.close();

  assert.equal(result, undefined);
  assert.equal(llm.calls.length, 2, '结构失败只重问一次');
  assert.equal(logs.some((line) => line.includes('reason=split_signal:split_process')), true);
});

// ── ④ text-lint 分级：warn 只记、reject 照旧 ───────────────────

test('text-lint：分句首单字指代只记（warn），不阻断', () => {
  const result = lintMemoryText('用户把 dsh-memory 的 storePath 设成 /home/julia/fatfish/memory/memory.db，它现在能读到 14551 条记忆。');
  assert.equal(result.ok, true, 'warn 级不许让 ok=false');
  const warn = result.violations.find((v) => v.code === 'bare_reference_clause');
  assert.ok(warn, '分句首「它」要记一条 bare_reference_clause');
  assert.equal(warn.severity, 'warn');
  assert.equal(warn.match, '它');
});

test('text-lint：整条首字的单字指代仍是 reject', () => {
  const result = lintMemoryText('它把 dsh-memory 的 storePath 设成 /home/julia/fatfish/memory/memory.db，用户确认过。');
  assert.equal(result.ok, false);
  assert.equal(result.violations.some((v) => v.code === 'bare_reference' && v.severity === undefined), true);
});

test('text-lint：BARE_REFERENCES 补齐五个新词，短语仍按 reject 判', () => {
  for (const word of ['这份文件', '该目录', '这个仓库', '这个插件', '本文件']) {
    assert.equal(BARE_REFERENCES.includes(word), true, `词表缺 ${word}`);
  }
  const cases = [
    '该目录在 /home/julia/fatfish/memory 下，放着 memory.db 和 backup 两份东西。',
    '本文件是 dsh-memory 的维护说明，改代码之前先读一遍再动手。',
    '这个仓库的 src 目录只放 .ts 源码，lib 是构建产物不许手改。',
  ];
  for (const text of cases) {
    const result = lintMemoryText(text);
    assert.equal(result.ok, false, `应判 reject：${text}`);
    assert.equal(result.violations.some((v) => v.code === 'bare_reference'), true);
  }
});

// ── ⑤ 子段 source：记忆 id 与队列键分开 ────────────────────────

test('子段 source：有 source_id 仍是 sleep:split:<id>', async () => {
  const { db } = tmpDb();
  const store = fakeStore([{ id: SRC_ID, text: SOURCE_PROCESS, status: 'active', tags: [], entities: [], source: 'sleep' }]);
  enqueue(db, 'split_item', SRC_ID, { source_id: SRC_ID, text: SOURCE_PROCESS, type: 'world' }, { now: 1 });
  const stats = await consumeSplitOps({ db, store, llm: fakeLlm(), limit: 5, maxChars: 200 });
  db.close();

  assert.equal(stats.inserted, 2);
  const sources = store.report().saved.map((x) => x.source);
  assert.deepEqual(sources, [`sleep:split:${SRC_ID}`, `sleep:split:${SRC_ID}`]);
});

test('子段 source：只有队列键时写 sleep:split:queue:<key>，不许长成记忆 id', async () => {
  const { db } = tmpDb();
  const store = fakeStore([]);
  const queueKey = 'split:abcdef0123456789';
  enqueue(db, 'split_item', queueKey, { text: SOURCE_PROCESS, type: 'world' }, { now: 1 });
  const stats = await consumeSplitOps({ db, store, llm: fakeLlm(), limit: 5, maxChars: 200 });
  db.close();

  assert.equal(stats.inserted, 2);
  const sources = store.report().saved.map((x) => x.source);
  assert.deepEqual(sources, [
    `sleep:split:queue:${queueKey}`,
    `sleep:split:queue:${queueKey}`,
  ]);
  for (const source of sources) {
    assert.match(source, /^sleep:split:queue:/);
    assert.equal(/^sleep:split:m_/.test(source), false, '队列键来源不许长得和记忆 id 一样');
  }
});

// ── ⑥ 提示词口径同步（三处同一份文本） ─────────────────────────

test('SPLIT_RULES / SPLIT_SYSTEM 同步：删「或分号」、禁定界符内部、段首词表、过程性状态', () => {
  assert.equal(/切点只在句号或换行之后/.test(SPLIT_RULES), true);
  assert.equal(/或分号之后/.test(SPLIT_RULES), false);
  assert.equal(SPLIT_RULES.includes('不许切在分号之后'), true);
  assert.equal(SPLIT_RULES.includes('代码围栏'), true);
  for (const word of ['上述', '上面', '这份', '本文件', '该目录', '这个仓库', '这个插件']) {
    assert.equal(SPLIT_RULES.includes(word), true, `SPLIT_RULES 缺 ${word}`);
  }
  assert.equal(SPLIT_RULES.includes('纯过程性状态'), true);

  assert.equal(SPLIT_SYSTEM.includes('每段的边界只许落在句号、换行之后'), true);
  assert.equal(SPLIT_SYSTEM.includes('不许落在分号之后'), true);
  assert.equal(SPLIT_SYSTEM.includes('代码围栏内部'), true);
  assert.equal(SPLIT_SYSTEM.includes('7. 每段不许以这些词开头'), true);
  assert.equal(SPLIT_SYSTEM.includes('8. 纯过程性状态'), true);
  assert.equal(SPLIT_SYSTEM.includes('9. 只输出 JSON'), true);
});
