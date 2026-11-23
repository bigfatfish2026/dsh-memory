/**
 * 候选漏斗 + 切批（2026-11-14，阶段 3）。
 *
 * 整理链原来有三处一模一样的「聚类 → 切批 → 查缓存 → 挑批」：
 *   sleep.js organizePass / organizePassAgent / planPoolQueues。
 * 这里收成一处，顺便把「送模型之前的四级漏斗」加在中间：
 *
 *   L0 精确指纹去重：同实体 + 同一天 + 正文互为前缀（或归一化后完全相同）→ 同一条事实，
 *      只留一条进批，其余从批里摘掉。只摘不写库，判重还是 L3 模型的活。
 *   L1 廉价相似度预筛：批内两两 token Jaccard / 公共前缀 / token 编辑距离取最大值；
 *      字面完全不像、向量分又只刚过线（< vectorCeil）的批，模型也不会判出合并，砍掉。
 *   L2 结构约束：批内至少共享一个实体或同 type，且时间跨度不超过 timeWindowDays；
 *      连「可能是同一件事」的结构条件都不满足的批，砍掉。
 *   L3 送模型：剩下的批按 score 降序，受可选 maxBatches 上限（0=不限）。
 *
 * 过了漏斗还有一道「增量复审」（2026-11-23，t-mwsvibwz-f9ve）：送模型前按条目级缓存
 * （src/item-review-cache.ts）把逐字未变、已审过的成员摘掉，只留变化项 + 它们最相关的旧邻居。
 * 聚类和漏斗本身不动——「全库找邻居」照旧在 clusterOf 里做完。
 *
 * 每一级都由 organizeFunnelMode 控制：off（默认）/ l0 / l1 / l2（'on' 等同 l2）。
 * off 时 plan() 的产物与改造前逐条一致（回归锁见 test/batch-planner.test.mjs）。
 */
import { DEFAULT_ITEM_REVIEW_NEIGHBORS, shrinkClusterForReview } from './item-review-cache.js';
import type { ItemReviewCache, ReviewItem } from './item-review-cache.js';
/** 批次里的一条记忆：候选自带 id/text/vector，其余字段按来源可能有。 */
export interface MemoryClusterItem {
  id?: string | number;
  text?: unknown;
  entities?: unknown[];
  ts?: unknown;
  type?: unknown;
  vector?: Float32Array | number[];
}

/** 一个批次：cluster 是成员，score 是本批最大余弦；合批后多 groups/fp/packed。 */
export interface MemoryBatch {
  cluster: MemoryClusterItem[];
  score: number;
  groups?: MemoryClusterItem[][];
  fp?: string;
  packed?: boolean;
}

/** 合批产出的包：groups 必有、fp 已算、packed=true。 */
export interface PackedBatch extends MemoryBatch {
  groups: MemoryClusterItem[][];
  fp: string;
  packed: true;
}

/** 向量查表：id → 向量；成员自带 vector 时优先用自带的。 */
export interface VecLookup {
  get(id: string): Float32Array | number[] | undefined;
}

/** 切批前的队列项：一簇 + 本簇最大余弦。 */
export interface PlanQueueItem {
  cluster: MemoryClusterItem[];
  score: number;
}

/** 漏斗计数；packed* 三项只在 packSize≥2 时出现。 */
export interface PlanFunnel {
  mode: string;
  level: number;
  batchesIn: number;
  l0DroppedMembers: number;
  l0DroppedBatches: number;
  l1DroppedBatches: number;
  l1VectorDropped: number;
  l2DroppedBatches: number;
  batchesOut: number;
  packedBatches?: number;
  packedGroups?: number;
  packRequests?: number;
  /** 增量复审：送审前的条目数 / 实际送审条目数 / 被复用的已审条目数（962→M 的账）。 */
  itemsIn?: number;
  itemsOut?: number;
  itemsSettled?: number;
  /** 被收缩过 / 整批都已审过不用送的批数。 */
  itemShrunkBatches?: number;
  itemSettledBatches?: number;
  /** 收缩后只剩 1 条、凑不成一对而跳过的批数（不是缓存命中）。 */
  itemSkippedBatches?: number;
}

/** 一轮规划的结果。 */
export interface PlanResult {
  total: number;
  clusters: MemoryClusterItem[][];
  queue: PlanQueueItem[];
  batches: MemoryBatch[];
  picked: MemoryBatch[];
  cached: number;
  funnel: PlanFunnel;
  skippedSingles: number;
  skippedQuiet: number;
}

/** createBatchPlanner 的注入项。 */
export interface BatchPlannerSpec {
  mode?: string;
  clusterOf: (all: MemoryClusterItem[], vecOf: VecLookup | null) => MemoryClusterItem[][];
  splitBatches: (q: { queue: PlanQueueItem[]; vecOf: VecLookup | null; budgetTokens: number; minScore: number }) => MemoryBatch[];
  fingerprint: (cluster: MemoryClusterItem[]) => string;
  batchMaxCos: (cluster: MemoryClusterItem[], vecOf: VecLookup | null) => number;
  budgetTokens?: number;
  minScore?: number;
  cheapMin?: number;
  vectorCeil?: number;
  minPairCos?: number;
  minAvgCos?: number;
  timeWindowDays?: number;
  maxBatches?: number;
  packSize?: number;
  packMinSpanDays?: number;
  packMaxSpanDays?: number;
  /**
   * 增量复审：每个变化项最多带几个旧邻居一起送（默认 DEFAULT_ITEM_REVIEW_NEIGHBORS=8；
   * 0 = 只送变化项）。生产值由 sleepItemReviewNeighbors 配置传入，别在这里写死数字。
   */
  itemNeighborLimit?: number;
  log?: (line: string) => void;
}

/** 规划器对外面。 */
export interface BatchPlanner {
  mode: string;
  level: number;
  plan: (opts?: { all?: MemoryClusterItem[]; vecOf?: VecLookup | null; cache?: Record<string, unknown>; itemCache?: ItemReviewCache }) => PlanResult;
}

export const FUNNEL_MODES = Object.freeze(['off', 'l0', 'l1', 'l2']);

/** 'off'→0 级，'l0'→1 级，'l1'→2 级，'l2'/'on'→3 级；不认识的值按 off。 */
export function funnelLevelOf(mode: string): number {
  const m = String(mode || 'off').toLowerCase();
  if (m === 'on') return FUNNEL_MODES.length - 1;
  const i = FUNNEL_MODES.indexOf(m);
  return i < 0 ? 0 : i;
}

/** 正文归一化：去空白/标点、全半角、小写。指纹和字面相似度都用它，别各写一套。 */
export function normalizeText(text: unknown): string {
  return String(text ?? '')
    .normalize('NFKC')
    .toLowerCase()
    .replace(/[\s\p{P}\p{S}]+/gu, '');
}

/** 中文按字 bigram、英文数字按词切；token 级相似度用。 */
export function tokensOf(text: unknown): string[] {
  const s = normalizeText(text);
  const latin = s.match(/[a-z0-9]+/g) || [];
  const han = s.replace(/[a-z0-9]+/g, '');
  const grams = [];
  for (let i = 0; i < han.length - 1; i += 1) grams.push(han.slice(i, i + 2));
  if (han.length === 1) grams.push(han);
  return [...new Set([...grams, ...latin])];
}

function jaccard(a: string[], b: string[]): number {
  if (!a.length || !b.length) return 0;
  const sa = new Set(a);
  let inter = 0;
  for (const t of new Set(b)) if (sa.has(t)) inter += 1;
  return inter / (sa.size + new Set(b).size - inter);
}

function commonPrefix(a: string[], b: string[]): number {
  let i = 0;
  while (i < a.length && i < b.length && a[i] === b[i]) i += 1;
  return i;
}

function tokenEditRatio(a: string[], b: string[]): number {
  const n = Math.max(a.length, b.length);
  if (!n) return 1;
  const prev = new Array(b.length + 1).fill(0).map((_, j) => j);
  for (let i = 1; i <= a.length; i += 1) {
    let last = prev[0];
    prev[0] = i;
    for (let j = 1; j <= b.length; j += 1) {
      const tmp = prev[j];
      prev[j] = Math.min(prev[j] + 1, prev[j - 1] + 1, last + (a[i - 1] === b[j - 1] ? 0 : 1));
      last = tmp;
    }
  }
  return 1 - prev[b.length] / n;
}

/** 两条记忆的廉价相似度：0..1，越大越像。只用本地字符串，不碰模型/向量。 */
export function cheapSimilarity(a: MemoryClusterItem, b: MemoryClusterItem): number {
  const ta = tokensOf(a?.text);
  const tb = tokensOf(b?.text);
  if (!ta.length || !tb.length) return 0;
  const jac = jaccard(ta, tb);
  const prefix = commonPrefix(ta, tb) / Math.min(ta.length, tb.length);
  const edit = tokenEditRatio(ta, tb);
  return Math.max(jac, prefix, edit);
}

/** 强标识符：路径、代码符号、带数字的 id（长度 ≥6、含非汉字）。两条正文共享一个就说明同一条线，别砍。 */
const STRONG_ID_RE = /[a-z0-9_\-./:]{6,}/g;

export function strongIdsOf(text: unknown): Set<string> {
  const s = String(text ?? '').normalize('NFKC').toLowerCase();
  return new Set(s.match(STRONG_ID_RE) || []);
}

export function sharesStrongId(a: MemoryClusterItem, b: MemoryClusterItem): boolean {
  const sa = strongIdsOf(a?.text);
  if (!sa.size) return false;
  for (const t of strongIdsOf(b?.text)) if (sa.has(t)) return true;
  return false;
}

function batchSharesStrongId(cluster: MemoryClusterItem[]): boolean {
  for (let i = 0; i < cluster.length; i += 1) {
    for (let j = i + 1; j < cluster.length; j += 1) {
      if (sharesStrongId(cluster[i], cluster[j])) return true;
    }
  }
  return false;
}

/** 两条向量的余弦；任一为零向量/缺向量返回 null（不参与判定）。 */
export function cosine(a: ArrayLike<number>, b: ArrayLike<number>): number | null {
  const n = Math.min(a?.length || 0, b?.length || 0);
  if (!n) return null;
  let dot = 0;
  let na = 0;
  let nb = 0;
  for (let i = 0; i < n; i += 1) {
    dot += a[i] * b[i];
    na += a[i] * a[i];
    nb += b[i] * b[i];
  }
  if (!na || !nb) return null;
  return dot / Math.sqrt(na * nb);
}

/**
 * 批内成对余弦：min 卡「批里有没有完全不相关的成员」，avg 卡整体质量。
 * max 就是原来的 batchMaxCos，最大值会把 2 条很像 + 8 条各说各的批伪装成好批，
 * 所以判定要看 min/avg，不看 max。
 * 缺向量的成员跳过；有效向量不足 2 条返回 null（不知道就不砍）。
 */
export function pairCosStats(cluster: unknown, vecOf: VecLookup | null): { min: number; avg: number; max: undefined; pairs: number } | null {
  const vecs = [];
  for (const m of Array.isArray(cluster) ? cluster : []) {
    const v = (m && m.vector) || (vecOf && vecOf.get(String(m?.id)));
    if (v && v.length) vecs.push(v);
  }
  if (vecs.length < 2) return null;
  let min = 1;
  let sum = 0;
  let pairs = 0;
  for (let i = 0; i < vecs.length; i += 1) {
    for (let j = i + 1; j < vecs.length; j += 1) {
      const c = cosine(vecs[i], vecs[j]);
      if (c === null) continue;
      if (c < min) min = c;
      sum += c;
      pairs += 1;
    }
  }
  if (!pairs) return null;
  return { min, avg: sum / pairs, max: undefined, pairs };
}

function dayOf(ts: unknown): string {
  const s = String(ts || '');
  const m = /^(\d{4}-\d{2}-\d{2})/.exec(s);
  return m ? m[1] : '';
}

function msOf(ts: unknown): number | null {
  const t = Date.parse(String(ts || ''));
  return Number.isFinite(t) ? t : null;
}

/**
 * L0：精确重复。
 *   1) 归一化后正文完全相同——不要求实体/日期，那就是同一条；
 *   2) 同实体 + 同一天 + 一方正文是另一方前缀，且长度差 ≤ 2 字（只差「了/的」这种语气词）。
 * 前缀但补了新内容的（长度差大）不算精确重复——补的事实交给 L3 模型判断，别在这层替它决定。
 */
export function exactSameFact(a: MemoryClusterItem, b: MemoryClusterItem): boolean {
  const ta = normalizeText(a?.text);
  const tb = normalizeText(b?.text);
  if (!ta || !tb) return false;
  if (ta === tb) return true;
  if (ta.length < 6 || tb.length < 6) return false;
  if (Math.abs(ta.length - tb.length) > 2) return false;
  const ea = new Set((a?.entities || []).map(String));
  if (!(b?.entities || []).some((e) => ea.has(String(e)))) return false;
  const da = dayOf(a?.ts);
  if (!da || da !== dayOf(b?.ts)) return false;
  return ta.startsWith(tb) || tb.startsWith(ta);
}

/**
 * 合批：把连续的 2 条批按 packSize 拼成一次请求，组边界留给 prompt 标。
 *
 * 只在「两条记忆时间跨度 > packMinSpanDays 天、≤ packMaxSpanDays 天」时拼——
 * 真库对照实测：跨天批拼 6 对一致率 96.1%，同天批只有 85.5%（同天相近事实最容易互相干扰）；
 * 所以合批只吃跨天那一段，同天批一律单问。
 * 3 条以上的批不拼（保持整簇不拆），遇到就先把当前包封口。
 */
export function packBatches(batches: MemoryBatch[], { packSize = 0, minSpanDays = 1, maxSpanDays = 30, fingerprint }: { packSize?: number; minSpanDays?: number; maxSpanDays?: number; fingerprint?: (cluster: MemoryClusterItem[]) => string } = {}): MemoryBatch[] {
  const size = Math.max(0, Math.floor(Number(packSize) || 0));
  if (size < 2) return batches;
  const spanOf = (pair: MemoryClusterItem[]): number | null => {
    const a = msOf(pair[0]?.ts);
    const b = msOf(pair[1]?.ts);
    if (a === null || b === null) return null;
    return Math.abs(a - b) / 86400000;
  };
  const eligible = (pair: MemoryClusterItem[]): boolean => {
    const d = spanOf(pair);
    return d !== null && d > Number(minSpanDays) && d <= Number(maxSpanDays);
  };
  const makePacked = (bucket: MemoryBatch[]): PackedBatch => {
    const cluster = bucket.flatMap((b) => b.cluster);
    const groups = bucket.map((b) => b.cluster);
    const score = bucket.reduce((n, b) => n + (Number(b.score) || 0), 0) / bucket.length;
    const fp = typeof fingerprint === 'function'
      ? fingerprint(cluster)
      : cluster.map((m) => String(m.id)).join(',');
    return { cluster, groups, score, fp, packed: true };
  };
  const out: MemoryBatch[] = [];
  let bucket: MemoryBatch[] = [];
  const flush = () => {
    if (!bucket.length) return;
    // 只剩一个的包不包装：合批是为了省请求，单组包跟原来单问一模一样，别多一层壳
    out.push(bucket.length === 1 ? bucket[0] : makePacked(bucket));
    bucket = [];
  };
  for (const b of batches) {
    const pair = Array.isArray(b?.cluster) && b.cluster.length === 2 ? b.cluster : null;
    if (!pair || !eligible(pair)) {
      flush();
      out.push(b);
      continue;
    }
    bucket.push(b);
    if (bucket.length >= size) flush();
  }
  flush();
  return out;
}

/**
 * @param {object} spec
 * @param {'off'|'l0'|'l1'|'l2'|'on'} [spec.mode='off']
 * @param {Function} spec.clusterOf   (all, vecOf) → clusters
 * @param {Function} spec.splitBatches ({queue,vecOf,budgetTokens,minScore}) → batches
 * @param {Function} spec.fingerprint (cluster) → string（批缓存键）
 * @param {Function} spec.batchMaxCos (cluster, vecOf) → number
 * @param {number} [spec.budgetTokens=0]
 * @param {number} [spec.minScore=0.78]
 * @param {number} [spec.cheapMin=0.3]       L1 字面相似度下限
 * @param {number} [spec.vectorCeil=0.88]    L1 只砍「向量分刚过线」的批
 * @param {number} [spec.timeWindowDays=180] L2 时间窗
 * @param {number} [spec.maxBatches=0]       L3 每轮最多送几批（0=不限）
 */
export function createBatchPlanner({
  mode = 'off',
  clusterOf,
  splitBatches,
  fingerprint,
  batchMaxCos,
  budgetTokens = 0,
  minScore = 0.78,
  cheapMin = 0.15,
  vectorCeil = 0.85,
  minPairCos = 0.78,
  minAvgCos = 0.8,
  timeWindowDays = 180,
  maxBatches = 0,
  packSize = 0,
  packMinSpanDays = 1,
  packMaxSpanDays = 30,
  itemNeighborLimit = DEFAULT_ITEM_REVIEW_NEIGHBORS,
  log = () => {},
}: Partial<BatchPlannerSpec> = {}): BatchPlanner {
  if (typeof clusterOf !== 'function' || typeof splitBatches !== 'function' || typeof fingerprint !== 'function') {
    throw new Error('batch-planner: clusterOf / splitBatches / fingerprint 都是必填');
  }
  const level = funnelLevelOf(mode);
  /** plan() 内的向量表：relateItems 用（每轮重设一次，不跨轮留旧快照）。 */
  let vecOfRef: VecLookup | null = null;

  /** L0：批内精确去重，返回 {cluster, dropped}。 */
  function applyL0(cluster: MemoryClusterItem[]): { cluster: MemoryClusterItem[]; dropped: number } {
    const kept: MemoryClusterItem[] = [];
    let dropped = 0;
    for (const m of cluster) {
      const dup = kept.find((k) => exactSameFact(k, m));
      if (dup) { dropped += 1; continue; }
      kept.push(m);
    }
    return { cluster: kept, dropped };
  }

  /** L1：批内最大廉价相似度；低于下限且向量分不高就砍。 */
  function cheapMax(cluster: MemoryClusterItem[]): number {
    let best = 0;
    for (let i = 0; i < cluster.length; i += 1) {
      for (let j = i + 1; j < cluster.length; j += 1) {
        const s = cheapSimilarity(cluster[i], cluster[j]);
        if (s > best) best = s;
      }
    }
    return best;
  }

  /** L2：同实体/同 type + 时间邻近。ts 缺失时保守放行（不知道就不砍）。 */
  function structurallyCompatible(cluster: MemoryClusterItem[]): boolean {
    const sets = cluster.map((m) => new Set((m.entities || []).map(String)));
    const types = cluster.map((m) => String(m.type || ''));
    let shared = false;
    for (let i = 0; i < sets.length && !shared; i += 1) {
      for (let j = i + 1; j < sets.length; j += 1) {
        if ([...sets[i]].some((e) => sets[j].has(e))) { shared = true; break; }
      }
    }
    const sameType = types.some((t, i) => t && types.some((u, j) => j !== i && u === t));
    // 结构条件不满足，但字面高度像（改个措辞/换行）也放行——实测有「代码在 X」vs「代码位于 X」
    // 这种同实体但 entities 一边为空的对，结构看不出来，字面能看出来。
    if (!shared && !sameType) return cheapMax(cluster) >= 0.5;
    const times = cluster.map((m) => msOf(m.ts)).filter((x) => x !== null);
    if (times.length >= 2) {
      const span = Math.max(...times) - Math.min(...times);
      if (span > timeWindowDays * 86400000) return false;
    }
    return true;
  }

  /**
   * 条目两两相似度：两边都有向量用余弦，缺向量退回廉价字面相似度。
   * 「最相关的旧邻居」就按这个挑（与漏斗里 pairCos/cheap 同一套度量，不另写一版）。
   */
  function relateItems(a: MemoryClusterItem, b: MemoryClusterItem): number {
    const va = a?.vector || (vecOfRef && vecOfRef.get(String(a?.id)));
    const vb = b?.vector || (vecOfRef && vecOfRef.get(String(b?.id)));
    if (va && vb) {
      const c = cosine(va, vb);
      if (c !== null) return c;
    }
    return cheapSimilarity(a, b);
  }

  /**
   * 一轮规划：聚类 → 切批 → 漏斗 → 增量复审 → 查缓存挑批。
   * @returns {{total,clusters,queue,batches,picked,cached,funnel,skippedSingles,skippedQuiet}}
   */
  function plan({ all = [], vecOf = null, cache = {}, itemCache = {} }: { all?: MemoryClusterItem[]; vecOf?: VecLookup | null; cache?: Record<string, unknown>; itemCache?: ItemReviewCache } = {}): PlanResult {
    vecOfRef = vecOf;
    const clusters = clusterOf!(all, vecOf) || [];
    const queue = clusters
      .filter((c) => c.length >= 2)
      .map((c) => ({ cluster: c, score: batchMaxCos!(c, vecOf) }))
      .filter((x) => x.score >= minScore)
      .sort((a, b) => b.score - a.score);
    let batches = splitBatches!({ queue, vecOf, budgetTokens, minScore: 0 }) || [];
    const funnel: PlanFunnel = {
      mode: level === 0 ? 'off' : FUNNEL_MODES[level],
      level,
      batchesIn: batches.length,
      l0DroppedMembers: 0,
      l0DroppedBatches: 0,
      l1DroppedBatches: 0,
      l1VectorDropped: 0,
      l2DroppedBatches: 0,
      batchesOut: batches.length,
      itemsIn: 0,
      itemsOut: 0,
      itemsSettled: 0,
      itemShrunkBatches: 0,
      itemSettledBatches: 0,
    };

    if (level >= 1) {
      const next: MemoryBatch[] = [];
      for (const item of batches) {
        const { cluster, dropped } = applyL0(item.cluster);
        funnel.l0DroppedMembers += dropped;
        if (cluster.length < 2) { funnel.l0DroppedBatches += 1; continue; }
        // L0 只摘成员，不重算向量分：父批的 score 仍是这一簇的相似度证据，
        // 摘掉的是精确重复，不会把批内最大余弦拉低（真重算要再扫一遍向量，白花）。
        next.push({ ...item, cluster, score: Number(item.score) || batchMaxCos!(cluster, vecOf) });
      }
      batches = next;
    }

    if (level >= 2) {
      const next: MemoryBatch[] = [];
      for (const item of batches) {
        const cheap = cheapMax(item.cluster);
        // 共享路径/代码符号的批不砍：实测「护栏重写成 queue(3…)」和「护栏位于 memory-link.js:82」
        // 字面 cheap 只有 0.13，却是同一件事——这种要交给模型，别让字面规则替它判重。
        const protectedByStrongId = batchSharesStrongId(item.cluster);
        const literalBad = cheap < cheapMin && Number(item.score) < vectorCeil && !protectedByStrongId;
        // 向量闸：min 卡「批里不能有完全不相关的成员」，avg 卡整体质量。
        // 有字面/标识符证据（cheap ≥ 0.5 或共享强标识符）就放行——两条正文高度重合时，
        // 向量低可能是换行/改写导致，不能据此判「不相干」。
        const cs = pairCosStats(item.cluster, vecOf);
        const hasLiteralEvidence = cheap >= 0.5 || protectedByStrongId;
        const vectorBad = Boolean(cs) && (cs!.min < minPairCos || cs!.avg < minAvgCos) && !hasLiteralEvidence;
        if (literalBad || vectorBad) {
          funnel.l1DroppedBatches += 1;
          if (vectorBad) funnel.l1VectorDropped = (funnel.l1VectorDropped || 0) + 1;
          continue;
        }
        next.push(item);
      }
      batches = next;
    }

    if (level >= 3) {
      const next: MemoryBatch[] = [];
      for (const item of batches) {
        if (!structurallyCompatible(item.cluster)) {
          funnel.l2DroppedBatches += 1;
          continue;
        }
        next.push(item);
      }
      batches = next;
    }

    if (maxBatches > 0 && batches.length > maxBatches) batches = batches.slice(0, maxBatches);

    // 增量复审（2026-11-23）：逐字未变、已审过的成员不再整批重送。
    // 只动「送进模型的内容」，聚类/漏斗产物与 fp 之外的一切照旧；fp 在下面按送审成员重算。
    {
      const next: MemoryBatch[] = [];
      for (const item of batches) {
        const shrunk = shrinkClusterForReview(
          item.cluster as unknown as ReviewItem[],
          itemCache,
          { relate: relateItems as unknown as (a: ReviewItem, b: ReviewItem) => number, neighborLimit: itemNeighborLimit },
        );
        funnel.itemsIn = (funnel.itemsIn || 0) + item.cluster.length;
        funnel.itemsSettled = (funnel.itemsSettled || 0) + shrunk.skipped.length;
        if (shrunk.cluster.length < 2) {
          // 全批已审过（0 条）记 cached；凑不成一对（1 条）单独记 skipped，别混进缓存命中数
          if (!shrunk.cluster.length) funnel.itemSettledBatches = (funnel.itemSettledBatches || 0) + 1;
          else funnel.itemSkippedBatches = (funnel.itemSkippedBatches || 0) + 1;
          continue;
        }
        funnel.itemsOut = (funnel.itemsOut || 0) + shrunk.cluster.length;
        if (shrunk.cluster.length < item.cluster.length) {
          funnel.itemShrunkBatches = (funnel.itemShrunkBatches || 0) + 1;
          next.push({ ...item, cluster: shrunk.cluster as unknown as MemoryClusterItem[] });
        } else {
          next.push(item);
        }
      }
      batches = next;
      if (funnel.itemShrunkBatches || funnel.itemSettledBatches) {
        log(
          `[merge] 增量复审 条目 ${funnel.itemsIn}→${funnel.itemsOut}`
          + `（已审过 ${funnel.itemsSettled} 条不再重送；收缩批 ${funnel.itemShrunkBatches}、整批已审过 ${funnel.itemSettledBatches}）`,
        );
      }
    }

    if (packSize >= 2) {
      const before = batches.length;
      batches = packBatches(batches, {
        packSize, minSpanDays: packMinSpanDays, maxSpanDays: packMaxSpanDays, fingerprint,
      });
      funnel.packedBatches = batches.filter((b) => b.packed).length;
      funnel.packedGroups = batches.reduce((n, b) => n + (Array.isArray(b.groups) ? b.groups.length : 0), 0);
      funnel.packRequests = batches.filter((b) => b.packed).length;
      if (funnel.packedBatches) {
        log(`[pack] 合批 ${before}→${batches.length} 包 ${funnel.packedBatches} 个（含 ${funnel.packedGroups} 组）`);
      }
    }
    funnel.batchesOut = batches.length;

    const picked: MemoryBatch[] = [];
    // 整批被条目缓存判「已审过」的批也算缓存命中：它们和批指纹命中一样，本轮不问模型。
    let cached = funnel.itemSettledBatches || 0;
    for (const item of batches) {
      const fp = fingerprint!(item.cluster);
      if (cache[fp]) { cached += 1; continue; }
      picked.push({ ...item, fp });
    }
    if (level > 0) {
      log(
        `[funnel] mode=${funnel.mode} 批 ${funnel.batchesIn}→${funnel.batchesOut}`
        + `（L0 摘成员 ${funnel.l0DroppedMembers}、丢批 ${funnel.l0DroppedBatches}；`
        + `L1 丢 ${funnel.l1DroppedBatches}；L2 丢 ${funnel.l2DroppedBatches}）`,
      );
    }
    return {
      total: all.length,
      clusters,
      queue,
      batches,
      picked,
      cached,
      funnel,
      skippedSingles: clusters.filter((c) => c.length === 1).length,
      skippedQuiet: clusters.filter((c) => c.length >= 2).length - queue.length,
    };
  }

  return { mode: level === 0 ? 'off' : FUNNEL_MODES[level], level, plan };
}
