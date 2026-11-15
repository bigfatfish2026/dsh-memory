/**
 * 候选漏斗 + BatchPlanner 的测试（2026-11-14，阶段 3）。
 *
 * 每一级钉两件事：能砍掉什么、不会误砍什么。
 * 外加 L3 边界（score 刚好卡线、批数刚好 k）与 mode=off 的逐条等价回归锁。
 * 全部用注入的假 clusterOf/splitBatches，不依赖 sleep.js 的真聚类，跑得快且只测漏斗本身。
 */
import assert from 'node:assert/strict';
import test from 'node:test';
import { createBatchPlanner, funnelLevelOf, exactSameFact, cheapSimilarity, pairCosStats, packBatches } from '../lib/batch-planner.js';

function m(id, text, extra = {}) {
  return { id, text, entities: [], type: 'world', ts: '2026-11-01', ...extra };
}

function planner(mode, { clusters = [], minScore = 0.78, cheapMin = 0.3, vectorCeil = 0.88, days = 180, maxBatches = 0, packSize = 0 } = {}) {
  return createBatchPlanner({
    mode,
    clusterOf: () => clusters,
    splitBatches: ({ queue }) => queue.map((x) => ({ cluster: x.cluster, score: x.score })),
    fingerprint: (cluster) => cluster.map((x) => x.id).join(','),
    batchMaxCos: (cluster) => Number(cluster._score ?? 0.8),
    minScore,
    cheapMin,
    vectorCeil,
    timeWindowDays: days,
    maxBatches,
    packSize,
  });
}

test('mode 解析：off/l0/l1/l2/on 各就各位，写错按 off', () => {
  assert.equal(funnelLevelOf('off'), 0);
  assert.equal(funnelLevelOf('l0'), 1);
  assert.equal(funnelLevelOf('l1'), 2);
  assert.equal(funnelLevelOf('l2'), 3);
  assert.equal(funnelLevelOf('on'), 3);
  assert.equal(funnelLevelOf('随便写的'), 0);
});

test('L0：同实体同一天、正文互为前缀 → 摘掉多余成员，只留一条进批', () => {
  const a = m('a1', '树莓派温度 45 度', { entities: ['树莓派'] });
  const a2 = m('a2', '树莓派温度 45 度了', { entities: ['树莓派'] });
  const b = m('b1', '树莓派风扇 3000 转', { entities: ['树莓派'] });
  const plan = planner('l0', { clusters: [[a, a2, b]] }).plan();
  assert.equal(plan.batches.length, 1);
  assert.deepEqual(plan.batches[0].cluster.map((x) => x.id), ['a1', 'b1'], '重复的那条被摘掉');
  assert.equal(plan.funnel.l0DroppedMembers, 1);
});

test('L0：摘完只剩一条 → 整批丢掉，不送模型', () => {
  const a = m('a1', '树莓派温度 45 度', { entities: ['树莓派'] });
  const a2 = m('a2', '树莓派温度 45 度了', { entities: ['树莓派'] });
  const plan = planner('l0', { clusters: [[a, a2]] }).plan();
  assert.equal(plan.batches.length, 0);
  assert.equal(plan.funnel.l0DroppedBatches, 1);
});

test('L0 不误砍：同实体但不同天/正文不互为前缀，照旧保留', () => {
  const a = m('a1', '树莓派温度 45 度', { entities: ['树莓派'], ts: '2026-11-01' });
  const b = m('b1', '树莓派温度 45 度了', { entities: ['树莓派'], ts: '2026-11-03' });
  const c = m('c1', '树莓派换了个风扇', { entities: ['树莓派'], ts: '2026-11-01' });
  const plan = planner('l0', { clusters: [[a, b, c]] }).plan();
  assert.equal(plan.batches[0].cluster.length, 3, '日期不同/正文不相干都不许摘');
  assert.equal(plan.funnel.l0DroppedMembers, 0);
});

test('L1：字面完全不像且向量分刚过线 → 砍批', () => {
  const a = m('a1', '硬盘只剩十五个G', { entities: ['树莓派'] });
  const b = m('b1', '风扇转速三千转', { entities: ['树莓派'] });
  const plan = planner('l1', { clusters: [[a, b]] }).plan();
  assert.equal(plan.batches.length, 0);
  assert.equal(plan.funnel.l1DroppedBatches, 1);
});

test('L1 不误砍：字面像（Jaccard 高）就留；向量分高也不砍', () => {
  const a = m('a1', '树莓派温度四十五度风扇正常', { entities: ['树莓派'] });
  const b = m('b1', '树莓派温度四十五度风扇正常今晚重启过', { entities: ['树莓派'] });
  assert.ok(cheapSimilarity(a, b) >= 0.3, `字面相似度 ${cheapSimilarity(a, b)} 应过线`);
  assert.equal(planner('l1', { clusters: [[a, b]] }).plan().batches.length, 1);

  const c = m('c1', '硬盘只剩十五个G', { entities: ['树莓派'] });
  const d = m('d1', '风扇转速三千转', { entities: ['树莓派'] });
  const high = [[c, d]];
  high[0]._score = 0.95;
  assert.equal(planner('l1', { clusters: high, vectorCeil: 0.88 }).plan().batches.length, 1, '向量分 ≥ 上限的批留给模型');
});

test('L2：没有共享实体、没有同 type、时间跨度超窗 → 砍批', () => {
  const a = m('a1', '硬盘只剩十五个G', { type: 'world', ts: '2025-01-01' });
  const b = m('b1', '风扇转速三千转', { type: 'experience', ts: '2026-11-01' });
  const clusters = [[a, b]];
  clusters[0]._score = 0.95; // 向量高，确保不是被 L1 砍的
  const plan = planner('l2', { clusters, days: 180 }).plan();
  assert.equal(plan.batches.length, 0);
  assert.equal(plan.funnel.l2DroppedBatches, 1);
});

test('L2 不误砍：同实体或同 type 且时间邻近就留', () => {
  const a = m('a1', '树莓派温度 45 度', { entities: ['树莓派'], ts: '2026-11-01' });
  const b = m('b1', '树莓派风扇 3000 转', { entities: ['树莓派'], ts: '2026-11-03' });
  const c1 = [[a, b]]; c1[0]._score = 0.95;
  assert.equal(planner('l2', { clusters: c1 }).plan().batches.length, 1, '同实体近时间');

  const c = m('c1', '今天跑了十公里', { type: 'experience', ts: '2026-11-01' });
  const d = m('d1', '晚饭吃了面', { type: 'experience', ts: '2026-11-02' });
  const c2 = [[c, d]]; c2[0]._score = 0.95;
  assert.equal(planner('l2', { clusters: c2 }).plan().batches.length, 1, '同 type 近时间');

  const e = m('e1', '硬盘只剩十五个G', { type: 'world', ts: '2026-11-01' });
  const f = m('f1', '风扇转速三千转', { type: 'world', ts: '' });
  const c3 = [[e, f]]; c3[0]._score = 0.95;
  assert.equal(planner('l2', { clusters: c3 }).plan().batches.length, 1, 'ts 缺失时保守放行');
});

test('L3 边界：score 恰好等于 minScore 保留（>= 不是 >）', () => {
  const a = m('a1', '树莓派温度 45 度', { entities: ['树莓派'] });
  const b = m('b1', '树莓派温度 46 度', { entities: ['树莓派'] });
  const clusters = [[a, b]];
  clusters[0]._score = 0.78;
  const plan = planner('off', { clusters, minScore: 0.78 }).plan();
  assert.equal(plan.queue.length, 1, '卡线也进候选');
  assert.equal(plan.batches.length, 1);
});

test('L3 边界：maxBatches=k 时只送前 k 批，0=不限', () => {
  const clusters = [1, 2, 3, 4, 5].map((n) => [m(`a${n}`, `树莓派温度 ${n} 度`, { entities: ['树莓派'] }), m(`b${n}`, `树莓派风扇 ${n} 转`, { entities: ['树莓派'] })]);
  for (const c of clusters) c._score = 0.9;
  assert.equal(planner('off', { clusters, maxBatches: 2 }).plan().batches.length, 2);
  assert.equal(planner('off', { clusters, maxBatches: 0 }).plan().batches.length, 5);
});

test('回归锁：mode=off 的 batches/picked 与手算旧路径逐条一致', () => {
  const a = m('a1', '树莓派温度 45 度', { entities: ['树莓派'] });
  const b = m('b1', '树莓派风扇 3000 转', { entities: ['树莓派'] });
  const c = m('c1', '完全不相干的一条', { ts: '2025-01-01' });
  const clusters = [[a, b], [a, c]];
  clusters[0]._score = 0.9;
  clusters[1]._score = 0.8;
  const plan = planner('off', { clusters }).plan();
  // 旧路径：clusterOf → filter≥2 → batchMaxCos ≥ minScore → splitBatches（这里就是 queue 映射）
  const expected = [['a1', 'b1'], ['a1', 'c1']];
  assert.deepEqual(plan.batches.map((x) => x.cluster.map((y) => y.id)), expected);
  assert.deepEqual(plan.picked.map((x) => x.fp), ['a1,b1', 'a1,c1']);
  assert.equal(plan.cached, 0);
  assert.equal(plan.funnel.batchesIn, 2);
  assert.equal(plan.funnel.batchesOut, 2);
  assert.equal(plan.funnel.l0DroppedMembers, 0, 'off 时一级都不许动');
});

test('回归锁：缓存命中的批不进 picked，但仍在 batches 里（池规划要能看到总批数）', () => {
  const a = m('a1', '树莓派温度 45 度', { entities: ['树莓派'] });
  const b = m('b1', '树莓派风扇 3000 转', { entities: ['树莓派'] });
  const clusters = [[a, b]];
  clusters[0]._score = 0.9;
  const plan = planner('off', { clusters }).plan({ cache: { 'a1,b1': { v: 'acted' } } });
  assert.equal(plan.batches.length, 1);
  assert.equal(plan.picked.length, 0);
  assert.equal(plan.cached, 1);
});

test('exactSameFact：短串（<6 字）不做前缀判定，防误合并', () => {
  const a = m('a1', '风扇', { entities: ['树莓派'] });
  const b = m('b1', '风扇转', { entities: ['树莓派'] });
  assert.equal(exactSameFact(a, b), false);
});

/* ── L1 向量闸：minCos 卡离群成员、avgCos 卡整体质量 ───────────────────────── */

const vec = (arr) => new Float32Array(arr);

test('minCos：批里有一条跟谁都不像的离群成员 → 砍（maxCos 看不出来，minCos 能）', () => {
  const a = m('a1', 'alpha 温度四十五度', { vector: vec([1, 0, 0]) });
  const b = m('b1', 'beta 风扇三千转', { vector: vec([0.98, 0.1, 0]) });
  const c = m('c1', 'gamma 晚饭吃面', { vector: vec([0, 0, 1]) });
  const plan = planner('l1', { clusters: [[a, b, c]] }).plan();
  assert.equal(plan.batches.length, 0, '有离群成员就整批不进模型');
  assert.equal(plan.funnel.l1VectorDropped, 1, '归因到向量闸');
});

test('minCos：2 条批余弦低砍、余弦高留（同一形状两个结果）', () => {
  const lo1 = m('lo1', 'alpha 温度四十五度', { vector: vec([1, 0, 0]) });
  const lo2 = m('lo2', 'beta 风扇三千转', { vector: vec([0, 1, 0]) });
  const lo = [[lo1, lo2]];
  lo[0]._score = 0.9; // 向量分高，确保不是被字面闸砍的
  const loPlan = planner('l1', { clusters: lo }).plan();
  assert.equal(loPlan.batches.length, 0, 'cos=0 砍');
  assert.equal(loPlan.funnel.l1VectorDropped, 1, '归因到向量闸');

  const hi1 = m('hi1', 'alpha 温度四十五度', { vector: vec([1, 0, 0]) });
  const hi2 = m('hi2', 'beta 风扇三千转', { vector: vec([0.99, 0.1, 0]) });
  const hi = [[hi1, hi2]];
  hi[0]._score = 0.9;
  assert.equal(planner('l1', { clusters: hi }).plan().batches.length, 1, 'cos≈0.995 留');
});

test('avgCos：min 恰好过线、平均质量不到下限 → 砍（avg 分支单独生效）', () => {
  const s2 = Math.sqrt(1 - 0.78 ** 2);
  const c = (0.78 - 0.78 * 0.78) / s2;
  const d = Math.sqrt(1 - 0.78 ** 2 - c * c);
  const a = m('v1', 'alpha 记忆一', { vector: vec([1, 0, 0]) });
  const b = m('v2', 'beta 记忆二', { vector: vec([0.78, s2, 0]) });
  const c1 = m('v3', 'gamma 记忆三', { vector: vec([0.78, c, d]) });
  const plan = planner('l1', { clusters: [[a, b, c1]] }).plan();
  assert.equal(plan.batches.length, 0, '两两 cos 都是 0.78：min 过线、avg 0.78 < 0.80，按 avg 砍');
  assert.equal(plan.funnel.l1VectorDropped, 1);
});

test('向量统计：空批、单成员、全员缺向量都返回 null（不知道就不砍）', () => {
  assert.equal(pairCosStats([], null), null);
  assert.equal(pairCosStats([m('s1', '树莓派温度 45 度')], null), null);
  assert.equal(pairCosStats([m('n1', 'x'), m('n2', 'y')], null), null);
  const a = m('a1', 'alpha', { vector: vec([1, 0, 0]) });
  const b = m('b1', 'beta', { vector: vec([0, 1, 0]) });
  const st = pairCosStats([a, b], null);
  assert.equal(st.pairs, 1);
  assert.ok(Math.abs(st.min) < 1e-6 && Math.abs(st.avg) < 1e-6);
});

/* ── 合批：只拼跨天 2 条批 ─────────────────────────────────────────────── */

const day = (n) => `2026-11-${String(n).padStart(2, '0')}`;
const pairOf = (id, tsA, tsB) => [
  m(`${id}a`, `${id} 温度四十五度`, { ts: tsA }),
  m(`${id}b`, `${id} 风扇三千转`, { ts: tsB }),
];

test('packBatches：6 个跨天 2 条批按 packSize=3 拼成 2 包，组边界正确', () => {
  const bs = [1, 2, 3, 4, 5, 6].map((n) => ({ cluster: pairOf(`p${n}`, day(1), day(10)), score: 0.8 }));
  const out = packBatches(bs, { packSize: 3 });
  assert.equal(out.length, 2, '6 对拼 3 组/包 = 2 包');
  assert.equal(out[0].groups.length, 3);
  assert.equal(out[0].cluster.length, 6);
  assert.deepEqual(out[0].groups.map((g) => g.map((x) => x.id)), [['p1a', 'p1b'], ['p2a', 'p2b'], ['p3a', 'p3b']]);
  assert.equal(out[0].packed, true);
});

test('packBatches：同天批不拼、3 条批打断当前包', () => {
  const sameDay = { cluster: pairOf('sd', day(1), day(1)), score: 0.8 };
  const wide = { cluster: pairOf('w', day(1), day(10)), score: 0.8 };
  const three = { cluster: [m('x1', 'x'), m('x2', 'y'), m('x3', 'z')], score: 0.8 };
  const out = packBatches([wide, wide, sameDay, three, wide], { packSize: 3 });
  assert.deepEqual(
    out.map((b) => (b.packed ? `pack${b.groups.length}` : b.cluster.length === 3 ? 'three' : 'single')),
    ['pack2', 'single', 'three', 'single'],
    '两个跨天批先拼一包；同天批单走；3 条批打断；最后一个跨天批不包装',
  );
});

test('packBatches：packSize=0/1 原样返回（回归锁）', () => {
  const bs = [1, 2].map((n) => ({ cluster: pairOf(`p${n}`, day(1), day(10)), score: 0.8 }));
  assert.equal(packBatches(bs, { packSize: 0 }), bs);
  assert.equal(packBatches(bs, { packSize: 1 }), bs);
});

test('plan+pckSize：funnel 报合批包数与组数，picked 里的包带 fp', () => {
  const clusters = [1, 2, 3, 4].map((n) => {
    const c = pairOf(`q${n}`, day(1), day(10));
    c._score = 0.9;
    return c;
  });
  const plan = planner('off', { clusters, packSize: 2 }).plan();
  assert.equal(plan.batches.length, 2, '4 对拼 2 组/包');
  assert.equal(plan.funnel.packedBatches, 2);
  assert.equal(plan.funnel.packedGroups, 4);
  assert.ok(plan.picked.every((p) => typeof p.fp === 'string' && p.fp.length > 0));
});
