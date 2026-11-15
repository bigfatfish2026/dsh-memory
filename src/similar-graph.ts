/**
 * 候选生成：向量近邻图连通分量（2026-11-06 t-graph-cluster，维护者拍板阈值 0.79）。
 *
 * 为什么换掉实体分桶：老 `clusterForSleep`（src/cluster.ts）按「主实体」凑批，
 * 真库 2988 条 active 上出 896 簇，其中 701 批（2248 条）批内最大余弦 < 0.78，
 * 送去模型也是白跑；467 条空实体条目落 `_misc`，永远没有候选。近邻图按「真的像」
 * 连边，每个候选组都自带 ≥0.79 的边，下游 `clusterQuestionMin` 再也筛不掉整批。
 *
 * 流程（参数见 src/index.ts 的 clusterMode / clusterThreshold / clusterNeighbors / clusterMaxSize）：
 *  1. 取向量：条目自带 `vector`，否则 `opts.vectors`（Map<id, Float32Array>）。
 *     运行时这条 Map 来自 `store.vectorIndex()`，它内部就是 `memvec.viewAll()`（src/memvec.ts:145）
 *     直接扫 vec_items ⋈ vec_map，是现成的「从 vec0 表读向量」函数，这里不另开连接。
 *  2. 分块 512 行算余弦，每条取 top-k 近邻（默认 k=8，排除自身）。
 *  3. 相似度 ≥ threshold 的近邻建边，并查集求连通分量（并查集前按 id 排序，保证幂等）。
 *  4. 单点分量丢弃：一个条目自己一组不算候选。
 *  5. 规模封顶 clusterMaxSize（默认 30）：分量内按度中心性取种子，组内按与种子余弦
 *     降序贪心取满 30，切到剩下的不足 30 为止。
 *  6. 缺向量的条目回落老的分实体桶（复用 `clusterForSleep` 的桶逻辑，传空 vecOf），
 *     日志 `[cluster] no_vector fallback=entity count=N`，不丢条目。
 *
 * 与老模块的关系：老 `clusterForSleep` 一行没删，`clusterMode: 'entity'` 可整体切回。
 */

import type { MemoryRecord } from './store/types.js';
import { clusterForSleep } from './cluster.js';

/** 图聚类选项。 */
export interface SimilarGraphOptions {
  threshold?: number;
  neighbors?: number;
  maxSize?: number;
  block?: number;
  vectors?: Map<string, Float32Array> | null;
  nearDup?: number;
  log?: (line: string) => void;
  logger?: { info?: (line: string) => void };
}

/** 图聚类统计（日志与测试都看这一份）。 */
export interface SimilarGraphStats {
  mode: string;
  threshold: number;
  neighbors: number;
  maxSize: number;
  total: number;
  withVector: number;
  noVector: number;
  edges: number;
  components: number;
  singletons: number;
  maxComponent: number;
  oversized: number;
  cappedGroups: number;
  droppedSingles: number;
  groups: number;
  ms: number;
}

/** 图聚类产物：候选组与统计。 */
export interface SimilarGraphResult {
  clusters: MemoryRecord[][];
  stats: SimilarGraphStats;
}

/** 图里一条：条目 + 原始向量。 */
interface GraphItem {
  m: MemoryRecord;
  raw: ArrayLike<number>;
}

/** 分块行数：512 行 × n 的余弦块张量，2988 条时 6.1MB，8000 条时 16MB */
const DEFAULT_BLOCK = 512;

function byId(a: { id?: unknown } | null | undefined, b: { id?: unknown } | null | undefined): number {
  const x = String(a?.id ?? '');
  const y = String(b?.id ?? '');
  if (x < y) return -1;
  if (x > y) return 1;
  return 0;
}

/** 行内点积（向量已单位化，点积即余弦） */
function dotRows(V: ArrayLike<number>, ia: number, ib: number, dim: number): number {
  const a = ia * dim;
  const b = ib * dim;
  let s = 0;
  for (let d = 0; d < dim; d += 1) s += V[a + d] * V[b + d];
  return s;
}

/**
 * 算余弦 + 每行 top-k（排除自身；同分按 id 序先到先得，保证幂等）。
 *
 * 为什么不是「一块 512 行 × 全库 的余弦矩阵」：那样要 512×n 的浮点张量（2988 条 6MB、
 * 8000 条 16MB），而且同一对余弦要算两遍。余弦对称，这里每对 (i,j) i<j 只算一次、
 * 同时更新两行的 top-k，浮点工作量正好减半，也不需要那块张量；分块仍然保留，
 * 只是块的作用变成「按 512 行一批推进，方便观察进度/限内存抖动」，不再承载矩阵。
 */
function buildTopK(V: ArrayLike<number>, n: number, dim: number, k: number, block: number): { nbrIdx: Int32Array; nbrSim: Float32Array } {
  const nbrIdx = new Int32Array(n * k).fill(-1);
  const nbrSim = new Float32Array(n * k);
  const rows = Math.max(1, Math.min(Math.trunc(block) || DEFAULT_BLOCK, n));

  /** 往 row 的 top-k 里插一条（候选按 j 递增到达，同分保留先到的） */
  function offer(row: number, j: number, s: number): void {
    const base = row * k;
    let cnt = 0;
    while (cnt < k && nbrIdx[base + cnt] >= 0) cnt += 1;
    if (cnt < k) {
      let pos = cnt;
      while (pos > 0 && s > nbrSim[base + pos - 1]) pos -= 1;
      for (let t = cnt; t > pos; t -= 1) {
        nbrSim[base + t] = nbrSim[base + t - 1];
        nbrIdx[base + t] = nbrIdx[base + t - 1];
      }
      nbrSim[base + pos] = s;
      nbrIdx[base + pos] = j;
      return;
    }
    if (s <= nbrSim[base + k - 1]) return;
    let pos = k - 1;
    while (pos > 0 && s > nbrSim[base + pos - 1]) pos -= 1;
    for (let t = k - 1; t > pos; t -= 1) {
      nbrSim[base + t] = nbrSim[base + t - 1];
      nbrIdx[base + t] = nbrIdx[base + t - 1];
    }
    nbrSim[base + pos] = s;
    nbrIdx[base + pos] = j;
  }

  for (let s0 = 0; s0 < n; s0 += rows) {
    const e0 = Math.min(s0 + rows, n);
    for (let i = s0; i < e0; i += 1) {
      const base = i * dim;
      for (let j = i + 1; j < n; j += 1) {
        const jb = j * dim;
        let acc = 0;
        for (let d = 0; d < dim; d += 1) acc += V[base + d] * V[jb + d];
        offer(i, j, acc);
        offer(j, i, acc);
      }
    }
  }
  return { nbrIdx, nbrSim };
}

/** 并查集（Int32Array，路径压缩 + 按秩合并不做，规模够用） */
function makeDsu(n: number): { find(x: number): number; union(a: number, b: number): void } {
  const parent = new Int32Array(n);
  for (let i = 0; i < n; i += 1) parent[i] = i;
  const find = (x: number): number => {
    let r = x;
    while (parent[r] !== r) r = parent[r];
    while (parent[x] !== r) {
      const next = parent[x];
      parent[x] = r;
      x = next;
    }
    return r;
  };
  return {
    find,
    union(a: number, b: number): void {
      const ra = find(a);
      const rb = find(b);
      if (ra !== rb) parent[rb] = ra;
    },
  };
}

/**
 * @param {Array} memories   条目数组（至少 { id }，可带 vector；实体回落要用 entities）
 * @param {object} opts      { threshold, neighbors, maxSize, block, vectors, nearDup, log }
 * @returns {{clusters: Array<Array>, stats: object}}  clusters 每组长 2..maxSize
 */
export function clusterBySimilarGraph(memories: MemoryRecord[], opts: SimilarGraphOptions = {}): SimilarGraphResult {
  const started = Date.now();
  const threshold = Number(opts.threshold ?? 0.79);
  if (!Number.isFinite(threshold)) throw new Error(`clusterThreshold 不是数字：${opts.threshold}`);
  const k = Math.max(1, Math.trunc(Number(opts.neighbors ?? 8)) || 8);
  // 2026-11-14：不再按条数封顶。不传/<=0 = 不限，切批交给 token 预算。
  const maxSize = Number(opts.maxSize) > 0 ? Number(opts.maxSize) : Infinity;
  const block = Math.max(1, Math.trunc(Number(opts.block) ?? DEFAULT_BLOCK) || DEFAULT_BLOCK);
  const vecSource = opts.vectors || null;
  const log = typeof opts.log === 'function' ? opts.log : (msg: string) => opts.logger?.info?.(msg);
  // 并查集前按 id 排序：同一输入、同一阈值的输出必须稳定
  const list = (Array.isArray(memories) ? memories.slice() : []).sort(byId);

  /* ---------- 1. 取向量 + 单位化 ---------- */
  const withRaw: GraphItem[] = [];
  const noVec: MemoryRecord[] = [];
  for (const m of list) {
    const raw = (m && (m.vector as ArrayLike<number> | null)) || (vecSource && vecSource.get(String(m?.id))) || null;
    if (raw && raw.length) withRaw.push({ m, raw });
    else noVec.push(m);
  }
  const dim = withRaw.length ? withRaw[0].raw.length : 0;
  const graphItems: GraphItem[] = [];
  for (const it of withRaw) {
    if (it.raw.length !== dim) { noVec.push(it.m); continue; } // 维度对不上当缺向量
    let norm = 0;
    for (let d = 0; d < dim; d += 1) norm += it.raw[d] * it.raw[d];
    if (!norm) { noVec.push(it.m); continue; } // 零向量跟谁都不像，别留在图里当单点
    graphItems.push(it);
  }
  const n = graphItems.length;
  const V = new Float32Array(n * dim);
  for (let i = 0; i < n; i += 1) {
    const raw = graphItems[i].raw;
    let norm = 0;
    for (let d = 0; d < dim; d += 1) norm += raw[d] * raw[d];
    const inv = 1 / Math.sqrt(norm);
    const base = i * dim;
    for (let d = 0; d < dim; d += 1) V[base + d] = raw[d] * inv;
  }
  return finishWith(V, graphItems, noVec, { threshold, k, maxSize, block, log, started, opts, dim });
}

/** 图聚类主体：0.79 这条路只走这一份实现 */
function finishWith(V: Float32Array, graphItems: GraphItem[], noVec: MemoryRecord[], ctx: { threshold: number; k: number; maxSize: number; block: number; log: ((line: string) => void) | undefined; started: number; opts: SimilarGraphOptions; dim: number }): SimilarGraphResult {
  const { threshold, k, maxSize, block, log, started, opts } = ctx;
  const n = graphItems.length;
  const dim = ctx.dim;
  const out: MemoryRecord[][] = [];
  const stats: SimilarGraphStats = {
    mode: 'graph',
    threshold,
    neighbors: k,
    maxSize,
    total: n + noVec.length,
    withVector: n,
    noVector: noVec.length,
    edges: 0,
    components: 0,
    singletons: 0,
    maxComponent: 0,
    oversized: 0,
    cappedGroups: 0,
    droppedSingles: 0,
    groups: 0,
    ms: 0,
  };

  if (n >= 2) {
    /* ---------- 2. top-k 近邻 ---------- */
    const { nbrIdx, nbrSim } = buildTopK(V, n, dim, k, block);

    /* ---------- 3. 建边 + 并查集 ---------- */
    const deg = new Int32Array(n);
    const dsu = makeDsu(n);
    for (let i = 0; i < n; i += 1) {
      for (let t = 0; t < k; t += 1) {
        const j = nbrIdx[i * k + t];
        if (j < 0) continue;
        if (nbrSim[i * k + t] < threshold) continue;
        deg[i] += 1;
        dsu.union(i, j);
        stats.edges += 1;
      }
    }

    /* ---------- 4. 连通分量（按首条目顺序，稳定） ---------- */
    const comps = new Map<number, number[]>();
    for (let i = 0; i < n; i += 1) {
      const r = dsu.find(i);
      if (!comps.has(r)) comps.set(r, []);
      comps.get(r)!.push(i);
    }
    stats.components = comps.size;

    for (const members of comps.values()) {
      if (members.length < 2) {
        stats.singletons += 1;
        continue; // 单点分量不产生候选组
      }
      if (members.length > stats.maxComponent) stats.maxComponent = members.length;
      if (members.length <= maxSize) {
        out.push(members.map((i) => graphItems[i].m));
        continue;
      }
      /* ---------- 5. 规模封顶 ---------- */
      stats.oversized += 1;
      let rest = members.slice();
      while (rest.length > maxSize) {
        // 度中心性最高的当种子（同度取 id 序靠前的，即下标小的）
        let seed = rest[0];
        for (const i of rest) if (deg[i] > deg[seed]) seed = i;
        const order = rest
          .map((i) => ({ i, s: i === seed ? 2 : dotRows(V, seed, i, dim) }))
          .sort((a, b) => (b.s - a.s) || (a.i - b.i));
        const take = order.slice(0, maxSize).map((x) => x.i);
        const taken = new Set<number>(take);
        out.push(take
          .map((i) => ({ m: graphItems[i].m, i }))
          .sort((a, b) => a.i - b.i)
          .map((x) => x.m));
        stats.cappedGroups += 1;
        rest = rest.filter((i) => !taken.has(i));
      }
      if (rest.length >= 2) {
        out.push(rest.map((i) => graphItems[i].m));
      } else if (rest.length === 1) {
        stats.droppedSingles += 1; // 封顶切出来的孤条不单独成组
      }
    }
  } else {
    stats.singletons = n;
  }

  /* ---------- 6. 缺向量：回落老的分实体桶 ---------- */
  if (noVec.length) {
    log?.(`[cluster] no_vector fallback=entity count=${noVec.length}`);
    const nearDup = Number(opts?.nearDup) || 0.9;
    // 传空 vecOf：老函数就只剩「按主实体分桶 + 桶内按正文长度串」这条纯规则路径
    const fallback = clusterForSleep(noVec.filter(Boolean), new Map(), { maxSize, nearDup });
    for (const group of fallback) {
      if (group.length >= 2) out.push(group);
      else stats.droppedSingles += 1;
    }
  }

  stats.groups = out.length;
  stats.ms = Date.now() - started;
  return { clusters: out, stats };
}

export default clusterBySimilarGraph;
