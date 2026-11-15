import assert from 'node:assert/strict';
import test from 'node:test';
import { clusterBySimilarGraph } from '../lib/similar-graph.js';

const DIM = 8;

/** 单位向量：[主方向 + 一点噪声] 单位化后余弦约 0.99 */
function nearVec(axis, noise = 0.05) {
  const v = new Array(DIM).fill(0);
  v[axis] = 1;
  v[(axis + 1) % DIM] = noise;
  return Float32Array.from(v);
}
/** 纯正交基向量 */
function axisVec(axis) {
  const v = new Array(DIM).fill(0);
  v[axis] = 1;
  return Float32Array.from(v);
}

function item(id, extra = {}) {
  return { id, text: String(id), tags: [], entities: [], ...extra };
}

function groupsOf(clusters) {
  return clusters
    .map((c) => c.map((m) => m.id).sort())
    .sort((a, b) => (a[0] < b[0] ? -1 : 1));
}

/**
 * 三组：明显近的 A 组 3 条、明显近的 B 组 2 条、明显远的 2 条（彼此正交）、
 * 缺向量的 3 条（两条同实体、一条不同实体）。
 */
function fixture() {
  const items = [];
  const vecOf = new Map();
  for (let i = 0; i < 3; i += 1) {
    const id = `m_a${i}`;
    items.push(item(id, { text: `树莓派温度四十五度（A 组第 ${i} 条）` }));
    vecOf.set(id, nearVec(0));
  }
  for (let i = 0; i < 2; i += 1) {
    const id = `m_b${i}`;
    items.push(item(id, { text: `风扇转速三千转（B 组第 ${i} 条）` }));
    vecOf.set(id, nearVec(2));
  }
  items.push(item('m_far1', { text: '完全无关的一' }));
  vecOf.set('m_far1', axisVec(4));
  items.push(item('m_far2', { text: '完全无关的二' }));
  vecOf.set('m_far2', axisVec(6));
  items.push(item('m_nv1', { text: '缺向量的一', entities: ['树莓派'] }));
  items.push(item('m_nv2', { text: '缺向量的二', entities: ['树莓派'] }));
  items.push(item('m_nv3', { text: '缺向量的三', entities: ['风扇'] }));
  return { items, vecOf };
}

test('三组向量：近的成组、远的被丢、缺向量走实体回落', () => {
  const { items, vecOf } = fixture();
  const logs = [];
  const { clusters, stats } = clusterBySimilarGraph(items, { vectors: vecOf, log: (m) => logs.push(m) });
  assert.deepEqual(groupsOf(clusters), [
    ['m_a0', 'm_a1', 'm_a2'],
    ['m_b0', 'm_b1'],
    ['m_nv1', 'm_nv2'],
  ]);
  // 远的两条各自是单点分量，不产生候选组
  assert.equal(clusters.some((c) => c.some((m) => m.id.startsWith('m_far'))), false);
  assert.equal(clusters.every((c) => c.length >= 2), true);
  // 缺向量回落日志
  assert.ok(logs.some((m) => m === '[cluster] no_vector fallback=entity count=3'), JSON.stringify(logs));
  assert.equal(stats.withVector, 7);
  assert.equal(stats.noVector, 3);
  assert.equal(stats.singletons, 2);
});

test('封顶生效：40 条近似重复按 clusterMaxSize=30 切成 30+10', () => {
  const items = [];
  const vecOf = new Map();
  for (let i = 0; i < 40; i += 1) {
    const id = `m_cap${String(i).padStart(2, '0')}`;
    items.push(item(id, { text: `同一件事的第 ${i} 种说法（内容够长以便排序）` }));
    vecOf.set(id, nearVec(0, 0.01 + i * 0.001));
  }
  const { clusters, stats } = clusterBySimilarGraph(items, { vectors: vecOf, maxSize: 30 });
  assert.equal(clusters.length, 2);
  const sizes = clusters.map((c) => c.length).sort((a, b) => b - a);
  assert.deepEqual(sizes, [30, 10]);
  assert.equal(clusters.every((c) => c.length <= 30), true);
  assert.equal(clusters.flat().length, 40, '封顶不许丢条目');
  assert.equal(stats.oversized, 1);
  assert.equal(stats.maxComponent, 40);
});

test('单点分量不进候选：互不正交也不够近的条目全被丢掉', () => {
  const items = [];
  const vecOf = new Map();
  for (let i = 0; i < 6; i += 1) {
    const id = `m_solo${i}`;
    items.push(item(id));
    vecOf.set(id, axisVec(i));
  }
  const { clusters, stats } = clusterBySimilarGraph(items, { vectors: vecOf });
  assert.deepEqual(clusters, []);
  assert.equal(stats.singletons, 6);
  assert.equal(stats.groups, 0);
});

test('阈值生效：余弦 0.85 的两条在 0.79 成组、在 0.9 拆开', () => {
  const items = [item('m_p'), item('m_q')];
  const vecOf = new Map([
    ['m_p', axisVec(0)],
    ['m_q', Float32Array.from([0.85, Math.sqrt(1 - 0.85 * 0.85), 0, 0, 0, 0, 0, 0])],
  ]);
  const loose = clusterBySimilarGraph(items, { vectors: vecOf, threshold: 0.79 });
  assert.deepEqual(groupsOf(loose.clusters), [['m_p', 'm_q']]);
  const tight = clusterBySimilarGraph(items, { vectors: vecOf, threshold: 0.9 });
  assert.deepEqual(tight.clusters, []);
  assert.equal(tight.stats.edges, 0);
});

test('缺向量才回落：有条目自带 vector 时也认', () => {
  const items = [
    { id: 'm_x', vector: axisVec(0) },
    { id: 'm_y', vector: axisVec(0) },
  ];
  const { clusters } = clusterBySimilarGraph(items, {});
  assert.deepEqual(groupsOf(clusters), [['m_x', 'm_y']]);
});

test('幂等：同输入两次相等，打乱输入顺序结果不变', () => {
  const { items, vecOf } = fixture();
  const a = clusterBySimilarGraph(items, { vectors: vecOf });
  const b = clusterBySimilarGraph(items, { vectors: vecOf });
  assert.deepEqual(groupsOf(a.clusters), groupsOf(b.clusters));
  const shuffled = items.slice().reverse();
  const c = clusterBySimilarGraph(shuffled, { vectors: vecOf });
  assert.deepEqual(groupsOf(c.clusters), groupsOf(a.clusters));
  assert.deepEqual(c.clusters.map((g) => g.map((m) => m.id)), a.clusters.map((g) => g.map((m) => m.id)));
});
