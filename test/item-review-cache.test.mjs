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
  itemReviewKey, itemReviewHit, shrinkClusterForReview, reviewAdditionsFromOps,
  mergeItemReviewCache, readItemReviewCache, persistItemReviewCache, ITEM_REVIEW_FIELD,
} from '../lib/item-review-cache.js';
import { createBatchPlanner } from '../lib/batch-planner.js';

const m = (id, text, extra = {}) => ({ id, text, entities: [], type: 'world', ts: '2026-11-20', ...extra });

function planner({ clusters = [], itemNeighborLimit = 2, fingerprint } = {}) {
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

/** 已审过一条：直接造缓存值（等价于上一轮模型给过结论）。 */
const settled = (item, extra = {}) => ({ [itemReviewKey(item)]: { v: 'noop', t: 1, ...extra } });

test('全同批只送一次：批里每条都审过 → 0 条送审，整批扔掉', () => {
  const a = m('a', '树莓派温度四十五度');
  const b = m('b', '树莓派风扇三千转');
  const cache = { ...settled(a), ...settled(b) };
  const shrunk = shrinkClusterForReview([a, b], cache, {});
  assert.deepEqual(shrunk.cluster, [], '一条都不该送');
  assert.equal(shrunk.skipped.length, 2);
  const plan = planner({ clusters: [[a, b]] }).plan({ itemCache: cache });
  assert.equal(plan.picked.length, 0, '整批不进 picked');
  assert.equal(plan.batches.length, 0);
  assert.equal(plan.funnel.itemSettledBatches, 1);
  assert.equal(plan.funnel.itemsSettled, 2);
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

test('关系判定：acted 带 rel，noop 记 v，被拒绝的 id 一条不记', () => {
  const a = m('a', 'x');
  const b = m('b', 'y');
  const acted = reviewAdditionsFromOps([a, b], [{ op: 'merge', ids: ['a', 'b'] }], { at: 7 });
  assert.equal(acted[itemReviewKey(a)].v, 'acted');
  assert.deepEqual(acted[itemReviewKey(a)].rel, [['b', 'merge']]);
  assert.equal(acted[itemReviewKey(b)].t, 7);
  const noop = reviewAdditionsFromOps([a, b], [], { at: 8 });
  assert.equal(noop[itemReviewKey(a)].v, 'noop');
  assert.equal(noop[itemReviewKey(b)].v, 'noop');
  const withReject = reviewAdditionsFromOps([a, b], [{ op: 'merge', ids: ['a', 'b'] }], { rejected: [{ ids: ['b'] }] });
  assert.ok(withReject[itemReviewKey(a)], '没被打回的照记');
  assert.equal(withReject[itemReviewKey(b)], undefined, '被打回的不许记成已审');
  // 合并进老表：同键覆盖、异键保留
  const merged = mergeItemReviewCache(noop, acted);
  assert.equal(merged[itemReviewKey(a)].v, 'acted');
  assert.equal(Object.keys(merged).length, 2);
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
