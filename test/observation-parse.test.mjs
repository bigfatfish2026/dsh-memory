/**
 * observation 提炼的解析与硬校验（src/sleep/observation-parse.ts）+
 * 输入构造（src/sleep/observation-prompt.ts），t-mwqdo73h-vbpc。
 *
 * 钉住的边界：
 *   ① 代码块包裹 / 裸 JSON / 空 ops 都能解析，坏 JSON 返回 parseError 不抛；
 *   ② 单条字段类型不对只丢该条，不拖垮整批；
 *   ③ create 正常通过；证据不足 2 条、同一根两条、uncertain 来源都不算独立；
 *   ④ 输入外的证据 id、未知 update 目标、超长正文、archive 动作都拒；
 *   ⑤ 已有活观察正文重复、同批 create 重复都拒（同批只留第一条）；
 *   ⑥ user message 按来源根分组标「只算一票」、超长截断并写明原文长度、列出已有观察。
 *
 * 纯函数：不建库、不调模型。
 */
import assert from 'node:assert/strict';
import test from 'node:test';
import {
  parseObservationOps,
  validateObservationOps,
} from '../lib/sleep/observation-parse.js';
import { buildObservationUserMessage, OBSERVATION_REVIEW_NOTE, OBSERVATION_SYSTEM } from '../lib/sleep/observation-prompt.js';

/** 造来源根表：{ id: { roots, uncertain } }。 */
function rootsMap(entries) {
  return new Map(Object.entries(entries));
}

const TWO_ROOTS = rootsMap({
  a: { roots: ['r1'], uncertain: false },
  b: { roots: ['r2'], uncertain: false },
});

function ctx(over = {}) {
  return {
    allowedEvidenceIds: ['a', 'b'],
    rootsOf: TWO_ROOTS,
    existingObservationIds: [],
    maxTextLen: 200,
    ...over,
  };
}

function op(over = {}) {
  return {
    op: 'create',
    text: '小鱼住在树莓派里，负责夜间整理长期记忆。',
    evidence: ['a', 'b'],
    ...over,
  };
}

test('代码块包裹的 JSON 能解析出 op', () => {
  const raw = '```json\n{"ops":[{"op":"create","text":"结论","evidence":["a","b"],"confidence":0.8}]}\n```';
  const { ops, parseError } = parseObservationOps(raw);
  assert.equal(parseError, undefined);
  assert.equal(ops.length, 1);
  assert.deepEqual(ops[0], { op: 'create', text: '结论', evidence: ['a', 'b'], confidence: 0.8 });
});

test('坏 JSON 返回 parseError，不抛', () => {
  assert.doesNotThrow(() => parseObservationOps('模型只写了半句话'));
  const out = parseObservationOps('模型只写了半句话');
  assert.deepEqual(out.ops, []);
  assert.match(out.parseError, /^JSON_PARSE_FAILED$/);
});

test('空输出与空 ops 都正常返回', () => {
  assert.equal(parseObservationOps('   ').parseError, 'EMPTY_OUTPUT');
  const empty = parseObservationOps('{"ops":[]}');
  assert.deepEqual(empty.ops, []);
  assert.equal(empty.parseError, undefined);
});

test('单条字段类型不对只丢该条，后面的照收', () => {
  const raw = '{"ops":[{"op":"create","text":42,"evidence":["a","b"]},{"op":"create","text":"正常","evidence":["a","b"]}]}';
  const { ops, parseError } = parseObservationOps(raw);
  assert.equal(parseError, undefined);
  assert.equal(ops.length, 1);
  assert.equal(ops[0].text, '正常');
});

test('正常 create 通过（两条独立来源）', () => {
  const { accepted, rejected } = validateObservationOps([op()], ctx());
  assert.equal(rejected.length, 0);
  assert.equal(accepted.length, 1);
  assert.equal(accepted[0].op, 'create');
  assert.deepEqual(accepted[0].evidence, ['a', 'b']);
});

test('证据不足 2 条被拒', () => {
  const { accepted, rejected } = validateObservationOps([op({ evidence: ['a'] })], ctx());
  assert.equal(accepted.length, 0);
  assert.equal(rejected[0].reason, 'INSUFFICIENT_INDEPENDENT');
});

test('同一来源根下的两条证据只算一票，被拒', () => {
  const roots = rootsMap({
    a: { roots: ['r1'], uncertain: false },
    a2: { roots: ['r1'], uncertain: false },
  });
  const { accepted, rejected } = validateObservationOps(
    [op({ evidence: ['a', 'a2'] })],
    ctx({ allowedEvidenceIds: ['a', 'a2'], rootsOf: roots }),
  );
  assert.equal(accepted.length, 0);
  assert.equal(rejected[0].reason, 'INSUFFICIENT_INDEPENDENT');
});

test('uncertain 来源不计票，被拒', () => {
  const roots = rootsMap({
    a: { roots: ['r1'], uncertain: false },
    c: { roots: ['r9'], uncertain: true },
  });
  const { accepted, rejected } = validateObservationOps(
    [op({ evidence: ['a', 'c'] })],
    ctx({ allowedEvidenceIds: ['a', 'c'], rootsOf: roots }),
  );
  assert.equal(accepted.length, 0);
  assert.equal(rejected[0].reason, 'INSUFFICIENT_INDEPENDENT');
});

test('不在输入里的证据 id 被拒（防模型编造）', () => {
  const { accepted, rejected } = validateObservationOps(
    [op({ evidence: ['a', 'zzz'] })],
    ctx(),
  );
  assert.equal(accepted.length, 0);
  assert.equal(rejected[0].reason, 'EVIDENCE_NOT_IN_INPUT');
});

test('evidence 非数组 / 空数组被拒', () => {
  const missing = validateObservationOps([op({ evidence: undefined })], ctx());
  assert.equal(missing.rejected[0].reason, 'EVIDENCE_EMPTY');
  const empty = validateObservationOps([op({ evidence: [] })], ctx());
  assert.equal(empty.rejected[0].reason, 'EVIDENCE_EMPTY');
});

test('update 目标不在已有观察里被拒', () => {
  const { accepted, rejected } = validateObservationOps(
    [op({ op: 'update', replaces: 'nope' })],
    ctx({ existingObservationIds: ['obs1'] }),
  );
  assert.equal(accepted.length, 0);
  assert.equal(rejected[0].reason, 'UNKNOWN_OBSERVATION');
});

test('update 目标存在且来源够 2 条时通过，replaces 保留', () => {
  const { accepted, rejected } = validateObservationOps(
    [op({ op: 'update', text: '修订后的结论。', replaces: 'obs1' })],
    ctx({ existingObservationIds: ['obs1'] }),
  );
  assert.equal(rejected.length, 0);
  assert.equal(accepted[0].op, 'update');
  assert.equal(accepted[0].replaces, 'obs1');
});

test('create 与已有活观察正文完全重复被拒', () => {
  const text = '这个实体每次整理都会重算观察。';
  const { accepted, rejected } = validateObservationOps(
    [op({ text })],
    ctx({
      existingObservationIds: ['obs1'],
      existingObservations: [{ id: 'obs1', text, status: 'pending_review' }],
    }),
  );
  assert.equal(accepted.length, 0);
  assert.equal(rejected[0].reason, 'DUPLICATE_OBSERVATION');
});

test('同批两条相同 create 只留第一条', () => {
  const text = '同一批里不要重复写同一条观察。';
  const { accepted, rejected } = validateObservationOps(
    [op({ text }), op({ text })],
    ctx(),
  );
  assert.equal(accepted.length, 1);
  assert.equal(accepted[0].index, 0);
  assert.equal(rejected.length, 1);
  assert.equal(rejected[0].reason, 'DUPLICATE_IN_BATCH');
});

test('超长 text 直接拒，不截断', () => {
  const text = '长'.repeat(21);
  const { accepted, rejected } = validateObservationOps(
    [op({ text })],
    ctx({ maxTextLen: 20 }),
  );
  assert.equal(accepted.length, 0);
  assert.equal(rejected[0].reason, 'TEXT_TOO_LONG');
  assert.equal(rejected[0].text, text.slice(0, 80));
});

test('空 text 与 archive 动作都被拒', () => {
  const empty = validateObservationOps([op({ text: '   ' })], ctx());
  assert.equal(empty.rejected[0].reason, 'TEXT_EMPTY');
  const archive = validateObservationOps([op({ op: 'archive' })], ctx());
  assert.equal(archive.rejected[0].reason, 'OP_NOT_ALLOWED');
});

test('user message 按来源根分组、超长截断、列出已有观察', () => {
  const message = buildObservationUserMessage({
    entity: '树莓派',
    evidences: [
      { id: 'a', text: '短证据', time: '2026-11-20', roots: ['r1'] },
      { id: 'b', text: '长'.repeat(450), time: '2026-11-21', roots: ['r1'] },
      { id: 'c', text: '来历不明的一条', roots: ['r9'], uncertain: true },
      {
        id: 'd',
        record: { text: '从记录里取正文', validFrom: '2026-11-19' },
        roots: ['r2'],
        uncertain: false,
      },
    ],
    existingObservations: [
      { id: 'obs1', text: '旧观察正文', status: 'pending_review', confidence: 0.8, evidenceIds: ['a', 'b'] },
    ],
  });

  assert.match(message, /实体：树莓派/);
  assert.match(message, /【来源根 r1｜本组 2 条，只算一票】/);
  assert.match(message, /已截断/);
  assert.match(message, /原文 450 字/);
  assert.match(message, /来源不明（不计票）/);
  assert.match(message, /- \[d\] 时间=2026-11-19 \| 从记录里取正文/);
  assert.match(message, /observation id=obs1 \| status=pending_review \| conf=0.8 \| 证据=a,b \| 旧观察正文/);
});

test('复核模式在消息末尾追加复核约束，普通 create 模式不带', () => {
  const input = {
    entity: '树莓派',
    evidences: [
      { id: 'a', text: '短证据', time: '2026-11-20', roots: ['r1'] },
      { id: 'b', text: '另一条证据', time: '2026-11-21', roots: ['r2'] },
    ],
    existingObservations: [{ id: 'obs1', text: '旧观察', status: 'pending_review' }],
  };
  const create = buildObservationUserMessage(input);
  const explicitCreate = buildObservationUserMessage({ ...input, mode: 'create' });
  const review = buildObservationUserMessage({ ...input, mode: 'review' });

  assert.equal(explicitCreate, create, '默认 mode=create，显式 create 与不传一致');
  assert.ok(!create.includes(OBSERVATION_REVIEW_NOTE), 'create 消息不带复核约束');
  assert.ok(review.includes(OBSERVATION_REVIEW_NOTE), 'review 消息要带复核约束');
  assert.ok(review.endsWith(OBSERVATION_REVIEW_NOTE), '复核约束拼在消息末尾');
  assert.equal(
    review.slice(0, review.length - OBSERVATION_REVIEW_NOTE.length),
    `${create}\n`,
    '除末尾一句外与 create 消息完全一致',
  );
});

test('提示词：evidence 只填候选证据 ID，同源切片不够，空结论固定 JSON', () => {
  assert.match(OBSERVATION_SYSTEM, /evidence 只复制本轮「候选证据」栏方括号里的 ID/);
  assert.match(OBSERVATION_SYSTEM, /来源根和已有观察 ID 不填进 evidence/);
  assert.match(OBSERVATION_SYSTEM, /同一原条的多个切片不足以单独生成观察/);
  assert.match(OBSERVATION_SYSTEM, /没有合适结论就输出 \{"ops":\[\]\}/);

  const message = buildObservationUserMessage({
    entity: '树莓派',
    evidences: [{ id: 'a', text: '一条证据', roots: ['r1'] }],
  });
  assert.match(message, /候选证据（按来源根分组/, '栏名要与提示词里的「候选证据」对齐');
});
