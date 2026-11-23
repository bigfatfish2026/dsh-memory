/**
 * 条目级复审缓存 + 增量收缩（2026-11-23，t-mwsvibwz-f9ve）。
 *
 * 钉四件事：
 *   1) 全同批只送一次：批里每条都已审过 → 一条不送，整批不进 picked；
 *   2) 部分变化只送变化项 + 最相关的旧邻居；
 *   3) 缓存命中：同 id 同正文命中，成员数组变化不影响（整批指纹失效的根因）；
 *   4) 失效：正文改一个字就换键，不命中，必须重送。
 * 外加关系判定的口径：acted 记 rel，noop 记 v，被拒绝的 id 一条不记。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import {
  itemReviewKey, itemReviewHit, pairReviewed, itemReviewGroupKey,
  shrinkClusterForReview, reviewAdditionsFromOps,
  mergeItemReviewCache, readItemReviewCache, persistItemReviewCache, ITEM_REVIEW_FIELD,
} from '../lib/item-review-cache.js';
import { createBatchPlanner } from '../lib/batch-planner.js';

const m = (id, text, extra = {}) => ({ id, text, entities: [], type: 'world', ts: '2026-11-20', ...extra });

function planner({ clusters = [], itemNeighborLimit = 8, fingerprint } = {}) {
  return createBatchPlanner({
    mode: 'off',
    clusterOf: () => clusters,
    splitBatches: ({ queue }) => queue.map((x) => ({ cluster: x.cluster, score: x.score })),
    fingerprint: fingerprint || ((cluster) => cluster.map((x) => x.id).join(',')),
    batchMaxCos: () => 0.9,
    minScore: 0.78,
    itemNeighborLimit,
  });
}

/** 已审过一条：直接造缓存值（等价于上一轮模型给过结论）。extra 可带 g（同批组键）。 */
const settled = (item, extra = {}) => ({ [itemReviewKey(item)]: { v: 'noop', t: 1, ...extra } });

/** 同一批一起审过的两（或多）条：g 相同 = 批内任意两成员的关系都判过。 */
const settledTogether = (...items) => {
  const g = itemReviewGroupKey(items);
  const out = {};
  for (const item of items) Object.assign(out, settled(item, { g }));
  return out;
};

test('全同批只送一次：同一批一起审过（同 g）→ 0 条送审，整批扔掉', () => {
  const a = m('a', '树莓派温度四十五度');
  const b = m('b', '树莓派风扇三千转');
  const cache = settledTogether(a, b);
  const shrunk = shrinkClusterForReview([a, b], cache, {});
  assert.deepEqual(shrunk.cluster, [], '一条都不该送');
  assert.equal(shrunk.skipped.length, 2);
  const plan = planner({ clusters: [[a, b]] }).plan({ itemCache: cache });
  assert.equal(plan.picked.length, 0, '整批不进 picked');
  assert.equal(plan.batches.length, 0);
  assert.equal(plan.funnel.itemSettledBatches, 1);
  assert.equal(plan.funnel.itemsSettled, 2);
});

test('回归 bug 1：A、B 各自在不同批判过无关 → {A,B} 第一次组对必须送审，不许跳过', () => {
  const a = m('a', '树莓派温度四十五度');
  const b = m('b', '树莓派风扇三千转');
  // 两条都命中缓存，但来自两个不同的旧批（g 不同），也没有任何 rel 指向对方
  const cache = { ...settled(a, { g: 'aaaaaaaaaaaaaaaa' }), ...settled(b, { g: 'bbbbbbbbbbbbbbbb' }) };
  assert.equal(itemReviewHit(cache, a)?.v, 'noop', '两条都算命中');
  assert.equal(itemReviewHit(cache, b)?.v, 'noop');
  assert.equal(pairReviewed(cache, a, b), false, '这一对从来没审过');
  const shrunk = shrinkClusterForReview([a, b], cache, {});
  assert.deepEqual(shrunk.cluster.map((x) => x.id), ['a', 'b'], '缺对就得两条都送审');
  assert.equal(shrunk.skipped.length, 0);
  const plan = planner({ clusters: [[a, b]] }).plan({ itemCache: cache });
  assert.equal(plan.picked.length, 1, '整批不许判 item_settled');
  assert.deepEqual(plan.picked[0].cluster.map((x) => x.id), ['a', 'b']);
  assert.equal(plan.funnel.itemSettledBatches || 0, 0);
});

test('回归 bug 1：同批审过的两条 + 一条新成员 → 新成员只带未审对手，同批那对继续复用', () => {
  const a = m('a', '树莓派温度四十五度风扇正常');
  const b = m('b', '树莓派风扇三千转');
  const c = m('c', '今天换了个电源');
  const cache = settledTogether(a, b);
  const shrunk = shrinkClusterForReview([a, b, c], cache, { neighborLimit: 1 });
  assert.deepEqual(shrunk.changed.map((x) => x.id), ['c'], '只有 c 是变化项');
  // c 与 a、b 的关系都没审过：邻居按批内顺序取前 1 个（a），b 与 c 的对仍在但受上限约束
  assert.deepEqual(shrunk.cluster, [a, c]);
  assert.deepEqual(shrunk.skipped.map((x) => x.id), ['b']);
});

test('部分变化：只送变化项 + 它最相关的旧邻居（邻居上限 1）', () => {
  const a = m('a', '树莓派温度四十五度风扇正常');
  const b = m('b', '今晚把日志目录清了一遍');
  const c = m('c', '树莓派温度四十五度风扇正常了'); // 与 a 字面高度相近
  const cache = { ...settled(a), ...settled(b) };
  const shrunk = shrinkClusterForReview([a, b, c], cache, { neighborLimit: 1 });
  assert.deepEqual(shrunk.changed.map((x) => x.id), ['c']);
  assert.deepEqual(shrunk.neighbors.map((x) => x.id), ['a'], '最相关的旧邻居是 a，不是 b');
  assert.deepEqual(shrunk.cluster.map((x) => x.id), ['a', 'c'], '按原批顺序送审');
  const plan = planner({ clusters: [[a, b, c]], itemNeighborLimit: 1 }).plan({ itemCache: cache });
  assert.equal(plan.picked.length, 1);
  assert.deepEqual(plan.picked[0].cluster.map((x) => x.id), ['a', 'c']);
  assert.equal(plan.picked[0].fp, 'a,c', 'fp 按送审成员重算，不含被摘掉的 b');
  assert.equal(plan.funnel.itemsIn, 3);
  assert.equal(plan.funnel.itemsOut, 2);
  assert.equal(plan.funnel.itemsSettled, 1);
});

test('缓存命中：同 id 同正文命中；成员数组变化不影响（这是整批指纹失效的根因）', () => {
  const a = m('a', '树莓派温度四十五度');
  const b = m('b', '树莓派风扇三千转');
  const c = m('c', '今天换了个电源');
  const cache = settled(a);
  // 同一批 a+b、a+b+c 都命中同一条（键里没有成员数组）
  assert.ok(itemReviewHit(cache, a), '同 id 同正文命中');
  const withMore = shrinkClusterForReview([a, b, c], cache, {});
  assert.ok(withMore.changed.some((x) => x.id === 'a') === false, 'a 仍算已审，不因多了 b/c 失效');
  // 老批（a+b）与新增成员后的批（a+b+c）共用一条 a 的缓存
  assert.equal(itemReviewKey(a), itemReviewKey({ ...a, entities: ['别的'] }), '非正文字段不进键');
});

test('缓存失效：正文改一个字就换键，必须重送', () => {
  const a = m('a', '树莓派温度四十五度');
  const changed = m('a', '树莓派温度四十六度');
  const cache = settled(a);
  assert.equal(itemReviewHit(cache, a)?.v, 'noop');
  assert.equal(itemReviewHit(cache, changed), null, '正文变了就不命中');
  assert.notEqual(itemReviewKey(a), itemReviewKey(changed));
  const shrunk = shrinkClusterForReview([changed, m('b', '树莓派风扇三千转')], cache, {});
  assert.deepEqual(shrunk.cluster.map((x) => x.id), ['a', 'b'], '全批都是变化项 → 整批原样送');
});

test('关系判定：acted 带对方条目键的 rel；显式空 ops 才整批记 noop；被拒/没提到的一条不记', () => {
  const a = m('a', 'x');
  const b = m('b', 'y');
  const c = m('c', 'z');
  const acted = reviewAdditionsFromOps([a, b], [{ op: 'merge', ids: ['a', 'b'] }], { at: 7 });
  assert.equal(acted[itemReviewKey(a)].v, 'acted');
  // rel 的对方记的是条目键（含对方正文哈希），不是裸 id：对方正文一改关系就失效
  assert.deepEqual(acted[itemReviewKey(a)].rel, [[itemReviewKey(b), 'merge']]);
  assert.equal(acted[itemReviewKey(b)].t, 7);
  assert.equal(pairReviewed(acted, a, b), true, '同一条 op 里点到的两条，关系审过');
  const noop = reviewAdditionsFromOps([a, b], [], { at: 8 });
  assert.equal(noop[itemReviewKey(a)].v, 'noop');
  assert.equal(noop[itemReviewKey(b)].v, 'noop');
  assert.ok(noop[itemReviewKey(a)].g, '显式空 ops 带同批组键');
  assert.equal(pairReviewed(noop, a, b), true, '同一批显式判过无关，这一对算审过');
  const withReject = reviewAdditionsFromOps([a, b], [{ op: 'merge', ids: ['a', 'b'] }], { rejected: [{ ids: ['b'] }] });
  assert.ok(withReject[itemReviewKey(a)], '没被打回的照记');
  assert.equal(withReject[itemReviewKey(b)], undefined, '被打回的不许记成已审');
  // 模型没提到的第三条：不记（哪怕它就在送审批里）
  const partial = reviewAdditionsFromOps([a, b, c], [{ op: 'merge', ids: ['a', 'b'] }], {});
  assert.equal(partial[itemReviewKey(c)], undefined, '模型没提到的条目一条都不记');
  // 合并进老表：同键覆盖、异键保留
  const merged = mergeItemReviewCache(noop, acted);
  assert.equal(merged[itemReviewKey(a)].v, 'acted');
  assert.equal(Object.keys(merged).length, 2);
});

test('回归 bug 3：单 id update 记 acted 不记 noop；校验被拒不记；显式 noop 才记', () => {
  const a = m('a', '树莓派温度四十五度');
  const b = m('b', '树莓派风扇三千转');
  // 单 id update：模型对这条有明确结论（改它），但没有「对方」，不许落成 noop
  const single = reviewAdditionsFromOps([a, b], [{ op: 'update', id: 'a', ids: ['a'] }], { at: 3 });
  assert.equal(single[itemReviewKey(a)].v, 'acted', '单 id update 是明确结论，记 acted');
  assert.equal(single[itemReviewKey(a)].rel, undefined, '没有对方就不造关系');
  assert.equal(pairReviewed(single, a, b), false, '单 id 结论不覆盖它与同批别条的关系');
  assert.equal(single[itemReviewKey(b)], undefined, 'b 没被提到，不记');
  // 校验阶段被拒的 id（checked.invalid 交进 rejected）一条都不记
  const rejected = reviewAdditionsFromOps([a, b], [{ op: 'archive', ids: ['b'] }], { rejected: [{ ids: ['b'] }] });
  assert.equal(rejected[itemReviewKey(b)], undefined, '校验被拒的不记');
  // 空响应/解析失败：调用方压根不调用这个函数（answer 为空直接落 pending），这里用空批兜底
  assert.deepEqual(reviewAdditionsFromOps([], []), {});
  // 显式空 ops 才整批记 noop
  const explicit = reviewAdditionsFromOps([a, b], []);
  assert.equal(explicit[itemReviewKey(a)].v, 'noop');
  assert.equal(explicit[itemReviewKey(b)].v, 'noop');
});

test('state 落盘：字段名 itemReviewCache，坏数据当空，写回保留 t 最新的', () => {
  let state = { [ITEM_REVIEW_FIELD]: { bad: 1 } };
  const stateStore = {
    load: () => state,
    patch: (next) => { state = { ...state, ...next }; return state; },
  };
  assert.deepEqual(readItemReviewCache(stateStore), {}, '值不是对象当空');
  const a = m('a', 'x');
  const b = m('b', 'y');
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-item-review-'));
  const cfg = { stateFile: path.join(dir, 'sleep.state.json'), organizeArchiveDir: path.join(dir, 'archive') };
  const written = persistItemReviewCache(
    stateStore,
    { ...settled(a), [itemReviewKey(b)]: { v: 'acted', t: 9 } },
    { max: 1, cfg },
  );
  assert.equal(written, 1, '超过上限按 t 轮转');
  assert.deepEqual(Object.keys(state[ITEM_REVIEW_FIELD]), [itemReviewKey(b)]);
  assert.ok(fs.existsSync(path.join(dir, 'archive')), '溢出的进归档目录，别写仓库 cwd 下的 memory/');
});
