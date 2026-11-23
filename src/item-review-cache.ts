/**
 * 条目级复审缓存（2026-11-23，t-mwsvibwz-f9ve）。
 *
 * 背景：12:30 那笔合并一次带 962 条，吃 115697 未缓存输入 + 31134 输出（logs/memory_sleep.log:64824），
 * 占整轮合并两项用量的 27%；其中 873 条与早上那批逐字相同，只因整批指纹
 * `batchFingerprint`（src/sleep/batching.ts:30）把成员数组算进键，成员一变整批失效，全部重审一遍。
 *
 * 这里只改「送进模型的那一批内容」，聚类（全库找邻居）一点不动：
 *   - 键 = 条目 id + 正文原始字节 sha1（itemReviewKey）。正文改一个字键就变 → 不命中；
 *     成员数组变化不碰任何键 → 已审过的条目仍然命中。这就是缓存失效的唯一条件。
 *   - 值 = { v: 'acted'|'noop', rel: [[对方 id, 关系]], t }：这条和谁比过、当时结论是什么。
 *     只记模型给出的有效结论（acted 或明确 noop）；被证据校验/写入闸打回的条目不记，
 *     免得把「还得重问」的条目永久跳过。
 *   - 收缩（shrinkClusterForReview）：批里已审过的成员摘掉，只留「没审过的（新增/改写）」
 *     ＋「它们最相关的旧邻居」（邻居只从本批已审过的旧成员里挑，余弦优先，缺向量退字面相似）。
 *     全批都审过 → 一条不送；全批都没审过 → 整批原样送（首见不砍）。
 *   - 存放：sleep.state.json 顶层 itemReviewCache，跟 organizeCache / splitVerdictCache 共用
 *     一套 stateStore.load()/patch()；轮转复用 organize-cache.ts 的 trimOrganizeCache
 *     （按 t 保留最近 N 条，溢出的按月归档），不新造文件、不新造表。
 *
 * 语义等价口径：未送审的成员不是「不判了」，而是「它和当时那批邻居的关系已经判过、结论在 rel 里」；
 * 新条目进来时会把最相关的旧邻居一起送审，所以旧成员与新成员之间的关系仍然由模型现判。
 */
import { sha1Hex } from './fingerprint.js';
import { trimOrganizeCache } from './organize-cache.js';
import type { CacheValue, OrganizeCacheConfig } from './organize-cache.js';
import type { SleepStateStore } from './sleep-state.js';

/** state 里的顶层字段名（跟 organizeCache / splitVerdictCache 平级）。 */
export const ITEM_REVIEW_FIELD = 'itemReviewCache';
/**
 * 每个变化项最多带几个旧邻居一起送审。
 *
 * 为什么不是 2：962 那笔回放里，89 个变化项按 top2 只带出 2 条不同的旧邻居，
 * 变化项与其余已审条目的关系根本没进 prompt；旧口径提到变化项的 11 条 op 里，
 * 多条是把变化项和别处的旧条目合在一起。8 条能把「同一实体/同一话题」的旧邻居带全，
 * 送审量仍只有整簇的一小部分（实测见 scripts/replay-item-review-962.mjs）。
 */
export const DEFAULT_ITEM_REVIEW_NEIGHBORS = 8;
/**
 * 主文件保留条数上限：一条约 110 字节，2000 条约 220KB。
 * 条目比批多一个数量级（962 条一笔就有 962 个键），给 2000 才装得下一轮大合并。
 */
export const DEFAULT_ITEM_REVIEW_MAX = 2000;

/** 一条已审关系：关系判定 + 写入时间（t 只用于轮转淘汰）。 */
export interface ItemReviewEntry {
  v: 'acted' | 'noop';
  t: number;
  /** [[对方 id, 关系]，…]；关系取模型 op 的类型（merge/update/archive/add）。 */
  rel?: Array<[string, string]>;
}

/** 缓存表本体：键 -> 条目判定。 */
export type ItemReviewCache = Record<string, ItemReviewEntry>;

/** 本模块只认这两个字段（MemoryClusterItem / MemoryView 都能直接传）。 */
export interface ReviewItem {
  id?: string | number;
  text?: unknown;
  vector?: Float32Array | number[];
  entities?: unknown[];
  type?: unknown;
  ts?: unknown;
}

/** 一条 op 的最小面：只取 id 与类型，别把整条 op 的正文拖进缓存。 */
export interface ReviewOp {
  ids?: unknown;
  id?: unknown;
  op?: unknown;
  kind?: unknown;
}

/** 带 id 的一条被拒提案（打回的不许记成已审）。 */
export interface RejectedOpLike {
  ids?: unknown;
  id?: unknown;
}

/** 条目键：`id#正文sha1前16位`。导出是为了测试能钉住形状。 */
export function itemReviewKey(item: ReviewItem | null | undefined): string {
  return `${String(item?.id ?? '')}#${sha1Hex(String(item?.text ?? ''), { len: 16 })}`;
}

/** 读 state 的字段，坏数据当空（跟 organizeCache 一个口径）。 */
export function readItemReviewCache(stateStore: Pick<SleepStateStore, 'load'>): ItemReviewCache {
  const raw = stateStore.load()?.[ITEM_REVIEW_FIELD];
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return {};
  const out: ItemReviewCache = {};
  for (const [key, value] of Object.entries(raw as Record<string, unknown>)) {
    if (!value || typeof value !== 'object' || Array.isArray(value)) continue;
    const v = value as Record<string, unknown>;
    const kind = v.v === 'acted' ? 'acted' : 'noop';
    const t = Number(v.t);
    const rel = Array.isArray(v.rel)
      ? v.rel
        .filter((x) => Array.isArray(x) && x.length >= 2)
        .slice(0, 8)
        .map((x) => [String(x[0]), String(x[1])] as [string, string])
      : [];
    out[key] = { v: kind, t: Number.isFinite(t) ? t : 0, ...(rel.length ? { rel } : {}) };
  }
  return out;
}

/** 命中返回条目，不命中返回 null。 */
export function itemReviewHit(cache: ItemReviewCache | null | undefined, item: ReviewItem): ItemReviewEntry | null {
  return (cache || {})[itemReviewKey(item)] || null;
}

/** 收缩结果：送审成员 + 被摘掉的已审成员 + 变化项 / 邻居明细（日志用）。 */
export interface ReviewShrink {
  cluster: ReviewItem[];
  skipped: ReviewItem[];
  changed: ReviewItem[];
  neighbors: ReviewItem[];
}

/**
 * 按条目缓存收缩一批。
 *
 * @param cluster 原批成员（聚类产物，顺序不动）
 * @param cache 条目缓存
 * @param opts.relate 两条的相似度（0..1，越大越像）；缺省时按成员在批内原顺序取邻居
 * @param opts.neighborLimit 每个变化项最多带几个旧邻居
 */
export function shrinkClusterForReview(
  cluster: ReviewItem[],
  cache: ItemReviewCache | null | undefined,
  { relate = null, neighborLimit = DEFAULT_ITEM_REVIEW_NEIGHBORS }: {
    relate?: ((a: ReviewItem, b: ReviewItem) => number) | null;
    neighborLimit?: unknown;
  } = {},
): ReviewShrink {
  const members = Array.isArray(cluster) ? cluster : [];
  const changed: ReviewItem[] = [];
  const settled: ReviewItem[] = [];
  for (const m of members) {
    if (itemReviewHit(cache, m)) settled.push(m);
    else changed.push(m);
  }
  // 全批首见 / 全批改写：没有旧结论可复用，整批原样送，不做任何裁剪
  if (!settled.length) return { cluster: members.slice(), skipped: [], changed: members.slice(), neighbors: [] };
  // 全批都审过：一条不送，省掉整次请求
  if (!changed.length) return { cluster: [], skipped: members.slice(), changed: [], neighbors: [] };

  const limit = Math.max(0, Math.floor(Number(neighborLimit) || DEFAULT_ITEM_REVIEW_NEIGHBORS));
  const picked: ReviewItem[] = [];
  // 只对「已选进送审集的邻居」去重（别把全部 settled 预置进来，否则一个都选不上）
  const seen = new Set<string>();
  for (const item of changed) {
    const ranked = settled
      .map((old, index) => ({ old, index, score: relate ? Number(relate(item, old)) || 0 : 0 }))
      .sort((a, b) => b.score - a.score || a.index - b.index)
      .slice(0, limit);
    for (const { old } of ranked) {
      const key = itemReviewKey(old);
      if (seen.has(key)) continue;
      seen.add(key);
      picked.push(old);
    }
  }
  // 保持原批顺序，便于日志/指纹稳定（同一组成员无论从哪条变化项出发都得到同一个送审数组）
  const sendKeys = new Set([...changed, ...picked].map((m) => itemReviewKey(m)));
  const sent = members.filter((m) => sendKeys.has(itemReviewKey(m)));
  const sentSet = new Set(sent.map((m) => itemReviewKey(m)));
  return {
    cluster: sent,
    skipped: members.filter((m) => !sentSet.has(itemReviewKey(m))),
    changed,
    neighbors: picked,
  };
}

/** 从一条 op 里取 id 列表（merge 用 ids，update/archive 用 id）。 */
function idsOfOp(op: ReviewOp | null | undefined): string[] {
  const many = Array.isArray(op?.ids) ? op!.ids!.map(String) : [];
  const one = op?.id === undefined || op?.id === null ? [] : [String(op.id)];
  return [...new Set([...many, ...one])].filter(Boolean);
}

/**
 * 把一批模型的结论摊到条目上。
 *
 * @param cluster 本批实际送审的成员
 * @param ops 模型给出的有效 op（一眼取 id 与类型）
 * @param opts.rejected 被证据校验/写入闸打回的提案；其中的 id 一条都不记（还得重问）
 * @param opts.at 写入时间（t）
 * @returns 可直接并进缓存表的增量
 */
export function reviewAdditionsFromOps(
  cluster: ReviewItem[],
  ops: ReviewOp[],
  { rejected = [], at = Date.now() }: { rejected?: RejectedOpLike[]; at?: number } = {},
): ItemReviewCache {
  const out: ItemReviewCache = {};
  const bad = new Set<string>();
  for (const r of rejected) {
    for (const id of idsOfOp({ ids: r?.ids, id: r?.id })) bad.add(id);
  }
  // 关系表：id -> [[对方 id, op 类型]]
  const relOf = new Map<string, Array<[string, string]>>();
  for (const op of Array.isArray(ops) ? ops : []) {
    const ids = idsOfOp(op);
    if (!ids.length) continue;
    const kind = String(op?.op ?? op?.kind ?? 'op');
    for (const id of ids) {
      const pairs = relOf.get(id) || [];
      for (const other of ids) if (other !== id) pairs.push([other, kind]);
      relOf.set(id, pairs.slice(0, 8));
    }
  }
  for (const m of Array.isArray(cluster) ? cluster : []) {
    const id = String(m?.id ?? '');
    if (!id || bad.has(id)) continue;
    const rel = relOf.get(id);
    out[itemReviewKey(m)] = rel && rel.length
      ? { v: 'acted', t: at, rel }
      : { v: 'noop', t: at };
  }
  return out;
}

/** 老分簇路径的 actions 桶（merge/update/archive）→ 条目增量；与 reviewAdditionsFromOps 同一口径。 */
export function reviewAdditionsFromActions(
  cluster: ReviewItem[],
  actions: { merge?: ReviewOp[]; update?: ReviewOp[]; archive?: ReviewOp[] } | null | undefined,
  opts: { rejected?: RejectedOpLike[]; at?: number } = {},
): ItemReviewCache {
  const ops: ReviewOp[] = [];
  for (const [kind, list] of [['merge', actions?.merge], ['update', actions?.update], ['archive', actions?.archive]] as const) {
    for (const a of Array.isArray(list) ? list : []) ops.push({ op: kind, ids: a?.ids, id: a?.id });
  }
  return reviewAdditionsFromOps(cluster, ops, opts);
}

/** 把增量并进缓存表（同键覆盖、t 刷新）；纯函数，方便测试与多处调用。 */
export function mergeItemReviewCache(
  cache: ItemReviewCache | null | undefined,
  additions: ItemReviewCache | null | undefined,
): ItemReviewCache {
  return { ...(cache || {}), ...(additions || {}) };
}

/**
 * 写回 state：先按 t 保留最近 max 条（复用 organizeCache 的轮转/归档），再 patch。
 * 写失败只 warn，不抛——本轮命中效果仍然有效，下一轮重新积累。
 *
 * @param stateStore sleep.state.json 入口
 * @param cache 全量缓存（调用方负责把老条目和本轮增量合好）
 * @param opts.cfg 轮转用（stateFile / organizeArchiveDir 与原缓存同一套）
 * @param opts.max 保留条数，默认 2000
 */
export function persistItemReviewCache(
  stateStore: Pick<SleepStateStore, 'patch'>,
  cache: ItemReviewCache,
  { cfg = {}, max = DEFAULT_ITEM_REVIEW_MAX, log = null }: {
    cfg?: OrganizeCacheConfig;
    max?: unknown;
    log?: ((line: string) => void) | null;
  } = {},
): number {
  const cap = Math.max(1, Math.floor(Number(max) || DEFAULT_ITEM_REVIEW_MAX));
  const verdict = trimOrganizeCache(
    { ...cfg, organizeCacheMax: cap } as OrganizeCacheConfig,
    cache as unknown as Record<string, CacheValue>,
    { warn: (line: string) => log?.(`[item-review] ${line}`) },
  );
  try {
    stateStore.patch({ [ITEM_REVIEW_FIELD]: verdict.kept });
  } catch (error) {
    log?.(
      `[item-review] WARN 条目缓存写回失败（本轮命中仍有效，下一轮重新积累）：`
      + `${String((error as Error && (error as Error).message) || (error as Error | null))}`,
    );
    return 0;
  }
  return Object.keys(verdict.kept).length;
}

export default persistItemReviewCache;
