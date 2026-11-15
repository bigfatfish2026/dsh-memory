/**
 * 合批的 prompt 与解析（2026-11-14，阶段 3 追问）。
 *
 * 钉三件事：
 *   1) 组边界真的进 prompt：groups 存在时 items 是分组的，且写明「不许跨组」；
 *   2) 解析按组分桶：组内 op 留、跨组 op 作废、某组没结论时 byGroup 里没有它（调用方据此降级单问）；
 *   3) 单批路径（groups 为空）的 prompt 一个字段都没变。
 */
import assert from 'node:assert/strict';
import test from 'node:test';
import { buildMergePrompt, groupOpsOf } from '../lib/agents/merge.js';

const A = { id: 'm_a', type: 'world', text: '树莓派温度 45 度', ts: '2026-11-01', entities: ['树莓派'] };
const B = { id: 'm_b', type: 'world', text: '树莓派温度 46 度', ts: '2026-11-03', entities: ['树莓派'] };
const C = { id: 'm_c', type: 'world', text: '风扇 3000 转', ts: '2026-10-01', entities: ['树莓派'] };
const D = { id: 'm_d', type: 'world', text: '风扇 3200 转', ts: '2026-10-05', entities: ['树莓派'] };

test('分组 prompt：items 按 group 输出，并写明不许跨组', () => {
  const p = buildMergePrompt({ items: [A, B, C, D], groups: [[A, B], [C, D]], clusterId: 'p1' });
  assert.match(p, /"group":1/);
  assert.match(p, /"group":2/);
  assert.match(p, /不许跨组合并/);
  assert.ok(p.includes('m_a') && p.includes('m_d'));
});

test('单批 prompt 不变：groups 为空时没有 group 字段与合批提示', () => {
  const p = buildMergePrompt({ items: [A, B], clusterId: 'p1' });
  assert.ok(!p.includes('"group"'), '单批不许出现 group 字段');
  assert.ok(!p.includes('不许跨组合并'));
  assert.ok(p.includes('m_a') && p.includes('m_b'));
});

test('groupOpsOf：组内 op 归组、跨组 op 作废、缺组不补空桶', () => {
  const groups = [[{ id: 'm_a' }, { id: 'm_b' }], [{ id: 'm_c' }, { id: 'm_d' }]];
  const ok1 = { op: 'merge', ids: ['m_a', 'm_b'] };
  const ok2 = { op: 'update', ids: ['m_c'] };
  const cross = { op: 'merge', ids: ['m_b', 'm_c'] }; // 跨组
  const ghost = { op: 'update', ids: ['m_x'] }; // 组外 id
  const { byGroup, cross: bad } = groupOpsOf([ok1, ok2, cross, ghost], groups);
  assert.deepEqual(byGroup.get(0), [ok1]);
  assert.deepEqual(byGroup.get(1), [ok2]);
  assert.equal(byGroup.has(2), false, '没有第 3 组');
  assert.deepEqual(bad.map((o) => o.ids.join('+')), ['m_b+m_c', 'm_x']);
});

test('groupOpsOf：全组没给 op 时返回空桶表，调用方按组降级', () => {
  const groups = [[{ id: 'm_a' }, { id: 'm_b' }], [{ id: 'm_c' }, { id: 'm_d' }]];
  const { byGroup, cross } = groupOpsOf([], groups);
  assert.equal(byGroup.size, 0);
  assert.equal(cross.length, 0);
});
