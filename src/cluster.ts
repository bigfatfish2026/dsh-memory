/**
 * 无模型的记忆聚类：给「分批喂大模型」生成批次。
 *
 * 为什么不在模型里做：一千多条一次塞进 prompt，注意力被摊薄，重复它根本看不见。
 * 先机械地把「可能是同一件事」的条目圈进同一批，再让模型只在一小批里做取舍。
 *
 * 两步：
 *  1. 近似重复连边：余弦 ≥ nearDup 的条目并进同一簇（同一件事的两种说法必须同批）。
 *  2. 其余按主实体分桶：取这条记忆里 IDF 最高的实体当桶键（维护者 这种上百条的高频实体
 *     不进桶键，否则一个桶吞掉半库），桶内按相似度贪心串起来，再切成不超过 maxSize 的块。
 */

import type { MemoryRecord } from './store/types.js';

/** 聚类选项：maxSize 不限（<=0）就交给 token 预算切。 */
export interface ClusterOptions {
  maxSize?: number;
  nearDup?: number;
}

/** 向量视图：id → Float32Array，可缺。 */
export type VecView = Map<string, Float32Array> | null | undefined;

/** 并查集最小面。 */
interface UnionFind {
  add(x: string): void;
  find(x: string): string;
  union(a: string, b: string): void;
}

const MIN_SIZE = 2;

function cosineAt(va: ArrayLike<number>, vb: ArrayLike<number>): number {
  let dot = 0;
  let na = 0;
  let nb = 0;
  for (let i = 0; i < va.length; i += 1) {
    const a = va[i];
    const b = vb[i];
    dot += a * b;
    na += a * a;
    nb += b * b;
  }
  if (!na || !nb) return 0;
  return dot / Math.sqrt(na * nb);
}

/** 简单并查集 */
function makeUnion(): UnionFind {
  const parent = new Map<string, string>();
  const find = (x: string): string => {
    let r = x;
    while (parent.get(r) !== r) r = parent.get(r) as string;
    let c = x;
    while (parent.get(c) !== c) {
      const next = parent.get(c) as string;
      parent.set(c, r);
      c = next;
    }
    return r;
  };
  return {
    add: (x: string): void => {
      if (!parent.has(x)) parent.set(x, x);
    },
    find,
    union: (a: string, b: string): void => {
      const ra = find(a);
      const rb = find(b);
      if (ra !== rb) parent.set(rb, ra);
    },
  };
}

/**
 * @param {Array} items       活跃记忆（store.listAll 的形状）
 * @param {Map}   vecOf       id -> Float32Array 向量视图（可缺）
 * @param {object} opts       { maxSize, nearDup }
 * @returns {Array<Array>}    批次，每批 1..maxSize 条
 */
export function clusterForSleep(items: MemoryRecord[], vecOf: VecView, opts: ClusterOptions = {}): MemoryRecord[][] {
  // 2026-11-14：不再按条数封顶。不传/<=0 = 不限，切批交给 token 预算。
  const maxSize = Number(opts.maxSize) > 0 ? Number(opts.maxSize) : Infinity;
  const nearDup = Number(opts.nearDup) || 0.9;
  const list = Array.isArray(items) ? items.slice() : [];
  if (list.length <= MIN_SIZE) return list.length ? [list] : [];

  const uf = makeUnion();
  for (const m of list) uf.add(m.id);

  const hasVec = (m: MemoryRecord): Float32Array | null | undefined => vecOf && vecOf.get(m.id);
  const vecCount = list.filter(hasVec).length;

  /* ---------- 1. 近似重复连边 ---------- */
  // 条目不多时两两都算；多了就只在共享实体的对之间算，免得睡觉时卡住
  const fullScan = list.length <= 1500;
  const entitySet = (m: MemoryRecord): Set<string> => new Set((m.entities || []).map(String));
  if (vecCount > 1) {
    if (fullScan) {
      for (let i = 0; i < list.length; i += 1) {
        const vi = hasVec(list[i]);
        if (!vi) continue;
        for (let j = i + 1; j < list.length; j += 1) {
          const vj = hasVec(list[j]);
          if (!vj) continue;
          if (cosineAt(vi, vj) >= nearDup) uf.union(list[i].id, list[j].id);
        }
      }
    } else {
      const byEntity = new Map<string, MemoryRecord[]>();
      for (const m of list) {
        for (const e of m.entities || []) {
          if (!byEntity.has(e)) byEntity.set(e, []);
          byEntity.get(e)!.push(m);
        }
      }
      for (const group of byEntity.values()) {
        for (let i = 0; i < group.length; i += 1) {
          const vi = hasVec(group[i]);
          if (!vi) continue;
          for (let j = i + 1; j < group.length; j += 1) {
            const vj = hasVec(group[j]);
            if (!vj) continue;
            if (cosineAt(vi, vj) >= nearDup) uf.union(group[i].id, group[j].id);
          }
        }
      }
    }
  }

  /* ---------- 2. 剩余按主实体分桶 ---------- */
  const freq = new Map<string, number>();
  for (const m of list) for (const e of entitySet(m)) freq.set(e, (freq.get(e) || 0) + 1);
  const freqLimit = Math.max(3, Math.floor(list.length * 0.02)); // 出现超过 2% 的实体不当桶键

  const root = new Map<string, MemoryRecord[]>(); // union-find 根 -> 条目
  for (const m of list) {
    const r = uf.find(m.id);
    if (!root.has(r)) root.set(r, []);
    root.get(r)!.push(m);
  }

  const buckets = new Map<string, MemoryRecord[]>(); // 桶键 -> 条目数组
  // 必须收整个 rest：近似重复组用 push(key, ...group) 一次塞整组，
  // 2026-11-05 前签名只收了第二个参数，≥3 条的重复组只剩第一条进批，其余直接丢。
  const push = (key: string, ...members: MemoryRecord[]): void => {
    if (!buckets.has(key)) buckets.set(key, []);
    buckets.get(key)!.push(...members);
  };
  for (const group of root.values()) {
    if (group.length >= MIN_SIZE) {
      // 已经因近似重复连起来的，整组进同一批（超大再切）
      push(`__dup__${group[0].id}`, ...group);
      continue;
    }
    const m = group[0];
    let best = '_misc';
    let bestF = Infinity;
    for (const e of entitySet(m)) {
      const f = freq.get(e) || 0;
      if (f <= 1 || f > freqLimit) continue;
      if (f < bestF) {
        bestF = f;
        best = e;
      }
    }
    push(best, m);
  }

  /* ---------- 3. 桶内按相似度排序后切块 ---------- */
  const out: MemoryRecord[][] = [];
  for (const [key, group] of buckets) {
    if (group.length === 1) {
      out.push(group);
      continue;
    }
    const ordered = key.startsWith('__dup__') ? group : orderBySimilarity(group, vecOf);
    for (let i = 0; i < ordered.length; i += maxSize) {
      out.push(ordered.slice(i, i + maxSize));
    }
  }
  return out;
}

/** 一批里最像的两条有多像：低于阈值的批根本没重复，不用送去问模型 */
export function batchMaxCos(cluster: MemoryRecord[], vecOf: VecView): number {
  let mx = 0;
  if (!vecOf || cluster.length < 2) return mx;
  for (let i = 0; i < cluster.length; i += 1) {
    const a = vecOf.get(cluster[i].id);
    if (!a) continue;
    for (let j = i + 1; j < cluster.length; j += 1) {
      const b = vecOf.get(cluster[j].id);
      if (!b) continue;
      const s = cosineAt(a, b);
      if (s > mx) mx = s;
    }
  }
  return mx;
}

/** 贪心最近邻排序：让内容相近的条目在数组里挨着，切块才不会把同类拆开 */
function orderBySimilarity(group: MemoryRecord[], vecOf: VecView): MemoryRecord[] {
  const rest = group.slice();
  const out: MemoryRecord[] = [];
  // 从最长的一条起（信息量最大），每次接上跟它最像的那条
  rest.sort((a, b) => String(b.text || '').length - String(a.text || '').length);
  const first = rest.shift();
  if (!first) return out;
  let cur: MemoryRecord = first;
  out.push(cur);
  while (rest.length) {
    const vc = vecOf && vecOf.get(cur.id);
    let bestIdx = 0;
    let best = -1;
    if (vc) {
      for (let i = 0; i < rest.length; i += 1) {
        const vr = vecOf.get(rest[i].id);
        const s = vr ? cosineAt(vc, vr) : 0;
        if (s > best) {
          best = s;
          bestIdx = i;
        }
      }
    }
    cur = rest.splice(bestIdx, 1)[0];
    out.push(cur);
  }
  return out;
}
