import assert from 'node:assert/strict';
import test from 'node:test';
import { clusterForSleep } from '../lib/cluster.js';

/** 三条向量完全相同的条目：两两余弦 1，必然连成一个近似重复组。 */
function nearDupSet(n) {
  const items = [];
  const vecOf = new Map();
  for (let i = 0; i < n; i += 1) {
    const id = `m_dup${i}`;
    items.push({ id, text: `树莓派温度四十五度（第 ${i} 条同组记忆）`, tags: [], entities: ['树莓派'] });
    vecOf.set(id, Float32Array.from([1, 0, 0, 0]));
  }
  return { items, vecOf };
}

test('近似重复组 3 条：全部进结果，不再只留第一条', () => {
  const { items, vecOf } = nearDupSet(3);
  const batches = clusterForSleep(items, vecOf, { maxSize: 30, nearDup: 0.9 });
  const flat = batches.flat().map((m) => m.id).sort();
  assert.deepEqual(flat, ['m_dup0', 'm_dup1', 'm_dup2']);
  const dupBatch = batches.find((b) => b.length === 3);
  assert.ok(dupBatch, `应有一批装下 3 条，实际批次：${JSON.stringify(batches.map((b) => b.map((m) => m.id)))}`);
});

test('近似重复组 5 条、maxSize=4：切块后仍一条不丢', () => {
  const { items, vecOf } = nearDupSet(5);
  const batches = clusterForSleep(items, vecOf, { maxSize: 4, nearDup: 0.9 });
  const flat = batches.flat().map((m) => m.id).sort();
  assert.deepEqual(flat, ['m_dup0', 'm_dup1', 'm_dup2', 'm_dup3', 'm_dup4']);
  assert.equal(batches.every((b) => b.length <= 4), true);
});

test('混入不相似的条目：近重复组照样完整，普通条目按实体分桶', () => {
  const { items, vecOf } = nearDupSet(3);
  items.push({ id: 'm_other', text: '风扇转速三千转', tags: [], entities: ['树莓派'] });
  vecOf.set('m_other', Float32Array.from([0, 1, 0, 0]));
  const batches = clusterForSleep(items, vecOf, { maxSize: 30, nearDup: 0.9 });
  const flat = batches.flat().map((m) => m.id).sort();
  assert.deepEqual(flat, ['m_dup0', 'm_dup1', 'm_dup2', 'm_other']);
  const dupBatch = batches.find((b) => b.some((m) => m.id === 'm_dup0'));
  assert.equal(dupBatch.length, 3);
});
