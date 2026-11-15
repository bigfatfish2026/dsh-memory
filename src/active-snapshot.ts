/**
 * 活跃记忆快照（2026-11-14，阶段 4）。
 *
 * 一轮整理里 store.listAll().filter(active) 会被调很多次（planPoolQueues、每批 prepare、
 * 每批 commit 的候选反查、汇总行），每次都是全表映射。这里做成「一轮取一次」：
 *   - get() 有缓存：版本没变就返回同一个对象；
 *   - invalidate() / 版本变化就重新 load——写库后必须失效，否则拿旧视图判「成员还在不在」；
 *   - version() 给日志/测试看这是第几版快照。
 *
 * 迁移期并存：调用方仍可直接用 store.listAll()，想用快照就换成 store.snapshot()；
 * 出问题把调用点换回去即可，不需要动这个文件。
 */
import type { MemoryView, SnapshotView } from './store/types.js';

/** load 的返回面：list 必给，vecOf 可选（睡觉聚类才用）。 */
export interface SnapshotLoad {
  list: MemoryView[];
  vecOf?: Map<string, Float32Array>;
}

/** 快照探针：一轮取一次，版本变了重取。 */
export interface ActiveSnapshot {
  get(): SnapshotView;
  invalidate(): void;
  version(): number;
}

/**
 * @param spec
 * @param spec.load 真正取数据的函数（只在缓存失效时调）
 * @param spec.version 版本源（默认内部自增）
 */
export function createActiveSnapshot({ load, version }: { load?: () => SnapshotLoad | null | undefined; version?: () => number } = {}): ActiveSnapshot {
  if (typeof load !== 'function') throw new Error('active-snapshot: load 必填');
  let cache: SnapshotView | null = null;
  let versionOf: () => number = typeof version === 'function' ? version : () => 0;
  let lastKey: number | null = null;

  function get(): SnapshotView {
    const key = Number(versionOf()) || 0;
    if (cache && lastKey === key) return cache;
    const got = load!() || ({} as SnapshotLoad);
    const list = Array.isArray(got.list) ? got.list : [];
    cache = {
      list,
      byId: new Map(list.map((m): [string, MemoryView] => [String(m?.id ?? ''), m])),
      vecOf: got.vecOf instanceof Map ? got.vecOf : new Map<string, Float32Array>(),
      version: key,
    };
    lastKey = key;
    return cache;
  }

  /** 手动失效：下一轮 get 一定重新 load（版本没变也重取）。 */
  function invalidate(): void {
    lastKey = null;
    cache = null;
  }

  return { get, invalidate, version: () => (cache ? cache.version : Number(versionOf()) || 0) };
}
