/**
 * 活跃快照与向量索引视图。
 *
 * 一轮一次快照：写事务提交后 snapshotVersion +1，下一次 get 自动重取，
 * 不会拿旧视图判「成员还在不在」。metas 与向量都从 state 现读，不复制。
 */
import { createActiveSnapshot } from '../active-snapshot.js';
import { isActive } from './rows.js';
import { listView } from './records.js';
import type { SnapshotSource, StoreState } from './types.js';

/** 活跃条目 id -> 向量视图（subarray，只读；别在外面改它）。 */
export function vecIndexOf(state: StoreState): Map<string, Float32Array> {
  const all = state.vecDb!.viewAll();
  const out: Map<string, Float32Array> = new Map();
  for (const m of state.metas) {
    if (!isActive(m)) continue;
    const v = all.get(m.id);
    if (v) out.set(m.id, v);
  }
  return out;
}

/** 一轮一次的活跃快照：{ list, byId, vecOf, version }。 */
export function createSnapshot(state: StoreState): SnapshotSource {
  return createActiveSnapshot({
    load: () => ({ list: state.metas.filter(isActive).map(listView), vecOf: vecIndexOf(state) }),
    version: () => state.snapshotVersion,
  });
}
