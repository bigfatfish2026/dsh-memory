/**
 * 条目级复审缓存（2026-11-23，t-mwsvibwz-f9ve；2026-11-23 t-mwszo19h-pqlj 改成「关系级」）。
 *
 * 背景：12:30 那笔合并一次带 962 条，吃 115697 未缓存输入 + 31134 输出（logs/memory_sleep.log:64824），
 * 占整轮合并两项用量的 27%；其中 873 条与早上那批逐字相同，只因整批指纹
 * `batchFingerprint`（src/sleep/batching.ts:30）把成员数组算进键，成员一变整批失效，全部重审一遍。
 *
 * 这里只改「送进模型的那一批内容」，聚类（全库找邻居）一点不动：
 *   - 键 = 条目 id + 正文原始字节 sha1（itemReviewKey）。正文改一个字键就变 → 不命中；
 *     成员数组变化不碰任何键 → 已审过的条目仍然命中。这就是缓存失效的唯一条件。
 *   - 值 = { v: 'acted'|'noop', rel: [[对方条目键, 关系]], g?: 同批组键, t }。
 *     **判定粒度是「一对」，不是「一条」**：A、B 各自在别的批里判过，不等于 A 与 B 的关系判过。
 *     两条关系已审的证据只有两种：
 *       1) rel 里记着对方的条目键（模型在同一条 op 里同时提到两条，关系明确）；
 *       2) 两条的 g 相同（同一次送审、整批显式给了结论，批内任意两成员都算判过）。
 *     对方键包含对方正文哈希，对方正文一改，这条关系自动失效（不用额外版本号）。
 *   - 收缩（shrinkClusterForReview）：
 *       全批首见 → 整批原样送；
 *       全批命中 → 逐对检查，缺哪条关系就送哪两条的成员（缺一条也不许整批判 settled）；
 *       部分变化 → 变化项 + 它们 top-N 未审对手（已审的对手不重复送）。
 *   - 存放：sleep.state.json 顶层 itemReviewCache，跟 organizeCache / splitVerdictCache 共用
 *     一套 stateStore.load()/patch()；轮转复用 organize-cache.ts 的 trimOrganizeCache
 *     （按 t 保留最近 N 条，溢出的按月归档），不新造文件、不新造表。
 *
 * 记账口径（t6 复核 bug 3）：只有「该 id 真的参与了送审，且模型对它明确给出结论」才写：
 *   - 模型在同一条 op 里提到它 → 记 acted（带 rel，若 op 里还有别的 id）；
 *   - 模型明确给空 ops（整批「看过了，都不用动」）→ 整批记 noop 并带 g；
 *   - 校验阶段被拒、单 id update/archive（没有对方）、模型没提到、空响应/解析失败 →
 *     一条都不记，下一轮还得重问。绝不用「没判过」冒充「判过」。
 */
import { digestParts, sha1Hex } from './fingerprint.js';
import { trimOrganizeCache } from './organize-cache.js';
import type { CacheValue, OrganizeCacheConfig } from './organize-cache.js';
import type { SleepStateStore } from './sleep-state.js';

/** state 里的顶层字段名（跟 organizeCache / splitVerdictCache 平级）。 */
export const ITEM_REVIEW_FIELD = 'itemReviewCache';
/**
 * 每个变化项最多带几个旧邻居一起送审。
 *
 * 生产值只从配置项 `sleepItemReviewNeighbors`（默认 8，见 src/index.ts）来：
 * batch-planner / pool / replay / organize 四处都不许再写死数字。
 * 为什么是 8：962 那笔回放里，89 个变化项按 top2 只带出 2 条不同的旧邻居，
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

/** 一条已审关系：关系判定 + 同批组键 + 写入时间（t 只用于轮转淘汰）。 */
export interface ItemReviewEntry {
  v: 'acted' | 'noop';
  t: number;
  /** [[对方条目键, 关系]，…]；关系取模型 op 的类型（merge/update/archive/add）。 */
  rel?: Array<[string, string]>;
  /** 同批组键（itemReviewGroupKey）：两次记录相同 = 同一次送审、批内关系都判过。 */
  g?: string;
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

/**
 * 同批组键：一次送审的成员键集合（排序后）的 sha1。
 * 只描述「哪些条目在同一批里一起给过结论」，与批 label / 指纹无关：
 * 成员集合相同（哪怕换了个批号）就算同批。
 */
export function itemReviewGroupKey(items: ReviewItem[] | null | undefined): string {
  const keys = (Array.isArray(items) ? items : []).map(itemReviewKey).sort();
  return digestParts(keys, { sep: '\n', len: 16 });
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
    // 旧数据里对方写的是裸 id（不含内容哈希），关系粒度不够，保留原样但判 pair 时不会命中；
    // g 只认 sha1 长度的十六进制串，别的当没有（宁可多送一次，不能误判已审）。
    const g = typeof v.g === 'string' && /^[0-9a-f]{16}$/.test(v.g) ? v.g : undefined;
    out[key] = { v: kind, t: Number.isFinite(t) ? t : 0, ...(rel.length ? { rel } : {}), ...(g ? { g } : {}) };
  }
  return out;
}

/** 命中返回条目，不命中返回 null。 */
export function itemReviewHit(cache: ItemReviewCache | null | undefined, item: ReviewItem): ItemReviewEntry | null {
  return (cache || {})[itemReviewKey(item)] || null;
}

/**
 * 两条之间的关系是否审过。
 *
 * 只认两种证据（见文件头）：同批组键相同，或 rel 里点名了对方的条目键。
 * 裸 id 的老 rel 不算——分不清对方正文是不是还是那一版。
 */
export function pairReviewed(
  cache: ItemReviewCache | null | undefined,
  a: ReviewItem,
  b: ReviewItem,
): boolean {
  const ka = itemReviewKey(a);
  const kb = itemReviewKey(b);
  if (ka === kb) return true;
  const ea = (cache || {})[ka];
  const eb = (cache || {})[kb];
  if (ea?.g && eb?.g && ea.g === eb.g) return true;
  if (ea?.rel?.some(([other]) => other === kb)) return true;
  if (eb?.rel?.some(([other]) => other === ka)) return true;
  return false;
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

  const limit = Math.max(0, Math.floor(Number(neighborLimit) || DEFAULT_ITEM_REVIEW_NEIGHBORS));

  // 全批都命中：不行，逐对查。「自己判过」不等于「这一对判过」——
  // A 在旧批甲里和甲批邻居判过无关、B 在旧批乙里判过无关，{A,B} 仍是第一次组对，
  // 必须送审；只有批内任意两成员的关系都有证据才允许整批跳过。
  if (!changed.length) {
    const needSend = new Set<number>();
    for (let i = 0; i < members.length; i += 1) {
      for (let j = i + 1; j < members.length; j += 1) {
        if (pairReviewed(cache, members[i], members[j])) continue;
        needSend.add(i);
        needSend.add(j);
      }
    }
    if (!needSend.size) return { cluster: [], skipped: members.slice(), changed: [], neighbors: [] };
    // 缺对的两端都送：只送缺的那几条，其余已审且关系齐的继续复用
    return {
      cluster: members.filter((_, i) => needSend.has(i)),
      skipped: members.filter((_, i) => !needSend.has(i)),
      changed: [],
      neighbors: [],
    };
  }

  // 部分变化：变化项必送；对手只挑「关系还没审过」的 top-N（已审的不重复送）。
  const picked: ReviewItem[] = [];
  // 只对「已选进送审集的邻居」去重（别把全部 settled 预置进来，否则一个都选不上）
  const seen = new Set<string>();
  for (const item of changed) {
    const ranked = members
      .filter((old) => old !== item && !pairReviewed(cache, item, old))
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
  const members = Array.isArray(cluster) ? cluster : [];
  const list = Array.isArray(ops) ? ops : [];
  if (!members.length) return {};
  const bad = new Set<string>();
  for (const r of rejected) {
    for (const id of idsOfOp({ ids: r?.ids, id: r?.id })) bad.add(id);
  }
  const keyOf = new Map<string, string>();
  for (const m of members) keyOf.set(String(m?.id ?? ''), itemReviewKey(m));

  // 显式空 ops：模型明确说「这批看过了、都不用动」。这是唯一允许整批记 noop 的情形；
  // 批内任意两成员的关系都算判过（g 相同），下一轮同一批成员一条不送。
  if (!list.length) {
    const out: ItemReviewCache = {};
    const g = itemReviewGroupKey(members);
    for (const m of members) {
      const id = String(m?.id ?? '');
      if (!id || bad.has(id)) continue;
      out[itemReviewKey(m)] = { v: 'noop', t: at, g };
    }
    return out;
  }

  // 关系表：id -> [[对方条目键, op 类型]]；只收 op 里明确点到的成员。
  const relOf = new Map<string, Array<[string, string]>>();
  for (const op of list) {
    const ids = idsOfOp(op);
    if (!ids.length) continue;
    const kind = String(op?.op ?? op?.kind ?? 'op');
    for (const id of ids) {
      const pairs = relOf.get(id) || [];
      for (const other of ids) {
        if (other === id) continue;
        const otherKey = keyOf.get(other);
        // 对方不在本批（候选 id、写库后新 id）就不记关系：键对不上，记了也没用
        if (!otherKey) continue;
        pairs.push([otherKey, kind]);
      }
      relOf.set(id, pairs.slice(0, 8));
    }
  }
  const out: ItemReviewCache = {};
  for (const m of members) {
    const id = String(m?.id ?? '');
    // 没被模型点名的、被打回的、没 id 的，一条都不记：它们没被判过（bug 3）
    if (!id || bad.has(id) || !relOf.has(id)) continue;
    const rel = relOf.get(id) as Array<[string, string]>;
    out[itemReviewKey(m)] = rel.length ? { v: 'acted', t: at, rel } : { v: 'acted', t: at };
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
