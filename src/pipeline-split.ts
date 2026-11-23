/**
 * 拆分流水线的消费端（2026-11-05）。
 *
 * 把 pending_ops(kind='split_item') 里排队的条目领出来：split() → 硬校验 →
 * 每一子段单独过写入闸（store.save → assertWriteAllowed，开关 pipeline.writeGate）→ 入库，
 * 成功后原条 archive、子段 supersedes 挂原条，op 置 done。
 *
 * 顺序（本票定的）：合并 agent 在同一轮里产出 needsSplit 的条目入队，拆分就在
 * organizePassAgent 末尾处理。理由：原文刚在内存里热着，不用等下一轮重新读；
 * 失败的会由 split() 自己 attempts+1 退避，天然顺延到下一轮，不会丢。
 *
 * 日志固定两行：
 *   [split] item= parts= retry= rejected= inserted= deduped= ms=
 *   [split] unavoidable item= reason=（由 agents/split.js 出）
 *
 * 2026-11-14：一段一条问模型太慢（单条 19-24 秒），照 ingest 那套改成
 * 「小批领活 → 批内 N 路并发问模型 → 落地串行」：
 *   - 每批只领 min(并发, 剩余) 条。dueOps 一领就把 op 标 running，一次把几百条全领走等于
 *     把租约占死（之前 599 条同一秒全 running 就是这个后果），所以一批做完再领下一批；
 *   - 批内 prepare（问模型 + 解析出拆分段）并发 N 路，commit（写库 + 标 done/dead +
 *     落 pending_ops）仍由主循环按序一条条做，写库永远只有一条线；
 *   - 并发数由调用方传：sleep.js 用 selectIngestConcurrency（与 ingest 同一个
 *     organizeIngestAdaptive 开关和负载算法，cap 64）算一次，一轮内固定不抖；
 *   - 单条失败只影响它自己：split() 自己 attempts+1，到 MAX_ATTEMPTS=5 转 dead，账不丢；
 *     本轮失败过的 id 不再在同一轮里被重新领到（next_try_at=now 会让它们立刻又到期）。
 */
import type { DatabaseSync } from 'node:sqlite';
import { dueOps, markOp, MAX_ATTEMPTS, type DueOp } from './ingest-index.js';
import { sha1Hex } from './fingerprint.js';
import { hasLegalCutPoint, recordSplitFailure, split } from './agents/split.js';
import { lintReviewMeta } from './lint-review.js';
import { deleteSupersedesLink, hasMemoryLinkTable, isAutoObservation, writeSupersedesLink } from './memory-link.js';
import {
  PARTIAL_SPLIT_REASON, PENDING_SPLIT_STATUS, pendingSplitIds, splitGroupFromRecords, splitGroupGap,
} from './store/split-group.js';
import type { SplitGroupStore, SplitGroupView } from './store/split-group.js';
import type { MemoryRecord, Row } from './store/types.js';
import type { LlmServiceLike } from './llm-service.js';
import type { SplitVerdictCache } from './split-verdict-cache.js';

/** 与 sleep.js 的 ingest 硬顶同口径；调用方给多大这里都再夹一次。 */
const SPLIT_HARD_CAP = 64;

/**
 * 轮末补扫等生产者的超时（2026-11-20）：默认 15 分钟。
 *
 * 正常轮 ingest/merge 两路 drain 几分钟内就完，这个数只给「模型调用永不返回」那种病态情况兜底——
 * 没有它，pipeline-split 的 awaitProducers 会永久挂住，整轮 sleep 不结束。
 */
export const PRODUCER_WAIT_TIMEOUT_MS = 15 * 60 * 1000;

/** split() 的返回面（agents/split() 已类型化，这里收窄成消费端要用的字段）。 */
export interface SplitAnswer {
  unavoidable?: boolean;
  /** 模型给的不可拆理由（写进判定缓存，下次命中时回显）。 */
  reason?: string;
  rounds?: number;
  /** 最终布局的切点（parts[i+1].start），二次切分与 lint 重做之后。 */
  cuts?: number[];
  parts: Array<{ text: string; start?: number; end?: number; [key: string]: unknown }>;
  lintDiscards?: Array<{ text: string; [key: string]: unknown }>;
  dedupeDiscards?: Array<{ text: string; [key: string]: unknown }>;
}

/** 一条拆分活：op + 解析出的原条与正文。 */
export interface SplitTask {
  op: DueOp;
  sourceId: string;
  text: string;
  source: MemoryRecord | null;
  payload: Row;
}

/** prepare 的产物：拆分答案、错误、耗时。 */
export interface SplitPrepared {
  answer?: SplitAnswer | null;
  error?: unknown;
  ms?: number;
}

/** 批内结果盒：prepare 成功带 value，失败带 error；commit 只认这几个字段。 */
export type SplitOutcome =
  | { ok: true; value: SplitPrepared }
  | { ok: false; error?: unknown };

/** split 用到的 store 面：快照、type 判定、写库、归档。 */
export interface SplitStore extends SplitGroupStore {
  metas?: () => MemoryRecord[];
  typeOf?: (m: MemoryRecord) => string;
  save: (text: string, tags: string[], source: string, extra: Record<string, unknown>) => Promise<{ id?: string | number; discarded?: boolean; entities?: string[] }>;
  /** 归档时同一笔事务里写原条的 supersededBy/validTo（子段保存不再碰这两个字段）。 */
  archive: (id: string, reason: string, opts?: {
    supersededBy?: string;
    childValidFrom?: string;
    childTs?: string;
    /** 父条归档那一笔事务里连同子段实体一起登记待观察（reason='split'）。 */
    observeEntities?: string[];
    observeReason?: string;
  }) => Promise<unknown>;
  /** 单条状态补丁（假 store 没实现批量组接口时的退路；真实 store 走 activateSplitGroup）。 */
  update?: (id: string, patch?: Record<string, unknown>) => Promise<unknown>;
}

/** consumeSplitOps 的入参。 */
export interface SplitConsumeOpts {
  db?: DatabaseSync;
  store?: SplitStore;
  llm?: LlmServiceLike | null;
  provider?: string;
  model?: string;
  limit?: number;
  maxChars?: number;
  maxTokens?: number;
  dryRun?: boolean;
  log?: (line: string) => void;
  now?: number;
  textLint?: boolean;
  lintReview?: boolean;
  lintReviewTimeoutMs?: number;
  /** 段级信号是否硬拒（cfg.pipeline.splitSignalsStrict，默认 false = 只记）。 */
  signalsStrict?: boolean;
  concurrency?: number;
  writeLock?: ((fn: () => Promise<void>) => Promise<void>) | null;
  /**
   * 本轮已失败/回 pending 的 op id，跨 pass 共享（2026-11-20）：
   * 轮末补扫时继续用同一份集合当 dueOps 的 exclude，别把上一批刚回队的活原地再领一遍。
   * 不传就是单次调用内部一份新数组（行为与以前一致）。
   */
  skipOpIds?: Array<string | number>;
  /**
   * 「不可拆」判定缓存（2026-11-22）：跨轮存在 sleep.state.json 的 splitVerdictCache 字段。
   * 不传就不启用（其它调用点/测试行为不变）；pool-pass 传真实的 stateStore 包装。
   */
  verdictCache?: SplitVerdictCache | null;
}

/**
 * 轮末补扫的入参（2026-11-20）。
 *
 * consumeSplitOps 一次只领到「当下到期」的活；拆分活的真正产出方是同一轮里并行跑的
 * 合并 / 入库两路池，它们在我们领空之后还会继续入队。所以要一个「等生产者收工再扫一轮」的入口。
 */
export interface SplitRecheckOpts extends SplitConsumeOpts {
  /** 等生产者收工（pool-pass 传合并/入库两路池 drain 完的 promise）；resolve 后再扫一轮。 */
  awaitProducers?: (() => Promise<void>) | null;
  /** 最多扫几轮（含第一轮）；默认 3，硬顶 10。 */
  maxPasses?: number;
  /** 等生产者的超时（毫秒）；默认 PRODUCER_WAIT_TIMEOUT_MS（15 分钟），测试里给小值。 */
  producerTimeoutMs?: number;
}

/** 一轮拆分的统计。 */
export interface SplitStats {
  scanned: number;
  tried: number;
  unavailable: number;
  unavoidable: number;
  archived: number;
  inserted: number;
  deduped: number;
  parts: number;
  rejected: number;
  failed: number;
  skipped: number;
  charsBefore: number;
  charsAfter: number;
  concurrency: number;
  /** 入口/拆中正文版本对不上而停下的条数。 */
  versionMismatch: number;
  /** 覆盖校验没通过、交人工的条数。 */
  coverageBlocked: number;
  /** 真正落库收尾的条数（有子段入库，原条已归档 / 无原条可归档）。 */
  succeeded: number;
  /** 每段都被判重、op 终态但没新增记忆的条数。 */
  dedupedOnly: number;
  /** 回 pending 等下一轮的条数（split_error / 部分落地 / 归档失败 / 不可拆）。 */
  pendingAfter: number;
  /** inflate 到 attempts 上限直接判死的条数。 */
  inflateDead: number;
  /** 命中「不可拆」判定缓存、没问模型的条数；省下的模型调用次数与它相同。 */
  cacheHits: number;
  /** 本地预检判「正文没有合法切点」而没问模型的条数（2026-11-23，t-mwsviczp-epd3）。 */
  precheckSkipped: number;
  /** 自动观察被挡在普通拆分外的条数（入口已挡，这里兜旧队列里已经入队的）。 */
  autoObservationSkipped: number;
}

/**
 * 一轮的分类口径（2026-11-20）：每条领到的 op 恰好落进一类，加起来等于 scanned。
 *   done：succeeded + dedupedOnly + skipped
 *   dead：versionMismatch + coverageBlocked + inflateDead
 *   pending：pendingAfter（unavoidable 也在这里）
 * 以前汇总写 `成功 = scanned - failed`，把 skipped/versionMismatch/no_source_id 全算成成功，
 * 日志「条 245 成功 234 失败 11」而库里实际 done 102 / dead 95 / pending 41 就是这个原因。
 */
export interface SplitStatsBreakdown {
  succeeded: number;
  dedupedOnly: number;
  skipped: number;
  pendingAfter: number;
  versionMismatch: number;
  coverageBlocked: number;
  unavoidable: number;
  inflateDead: number;
}

/**
 * 一条拆活的类型：payload.type → store.typeOf(source) → 'world'。
 * 2026-11-22 从问模型那段的内联表达式收出来：缓存命中回队时也要用同一份判定。
 */
function splitTypeOf(store: SplitStore, payload: Row | null, source: MemoryRecord | null): string {
  return String(payload?.type || '')
    || (source && typeof store.typeOf === 'function' ? String(store.typeOf(source)) : '')
    || 'world';
}

/** 从 store 里取原条：优先 payload.text，库里没有就用 payload 文本（merge 产出的 add 候选没有 id）。 */
function sourceOf(store: SplitStore, sourceId: string): MemoryRecord | null {
  const metas = typeof store.metas === 'function' ? store.metas() : [];
  return metas.find((m) => String(m.id) === sourceId) || null;
}

/**
 * 子段的来源标记（2026-11-20）。
 *
 * 有记忆 id：照旧写 `sleep:split:<sourceId>`，历史格式不变。
 * 没有记忆 id（merge 产出的 add 候选）：写 `sleep:split:queue:<dedupeKey>`。
 * 队列键形状是 `split:<sha1 前16>`（src/split-queue.ts 的 splitQueueKey），不是 memories.id；
 * 以前直接拿它当 id 拼，子段 source 成了 `sleep:split:split:<hex>`，按 `sleep:split:<id>` 回查父条
 * 必然 MISSING。存量数据不动，只保证新数据能区分「记忆 id」与「队列键」。
 */
function partSourceOf(memoryId: string, dedupeKey: string): string {
  const id = String(memoryId || '').trim();
  if (id) return `sleep:split:${id}`;
  return `sleep:split:queue:${String(dedupeKey || '').trim()}`;
}

/**
 * 重试幂等（2026-11-15 存储高-3）：子段一旦落库，重试时靠来源标记 + 正文认出它，直接复用 id。
 * 拆分写库的来源由 partSourceOf() 给：有原条是 `sleep:split:<sourceId>`，
 * 队列键来源是 `sleep:split:queue:<dedupeKey>`；正文 store.save 会 trim，比较前也 trim。
 * 2026-11-22：中间态（pending_split）也要认——它同样是「已落库、还没收尾」的段，
 * 只认 active 会让重试把同一段再写一遍。认不出来就返回空串，走正常入库（宁可多问一次写入闸，也不吞段）。
 */
function savedPartId(store: SplitStore, sourceTag: string, text: unknown): string {
  const want = String(text || '').trim();
  if (!sourceTag || !want) return '';
  let metas = [];
  try {
    metas = typeof store.metas === 'function' ? store.metas() : [];
  } catch {
    return '';
  }
  const hit = metas.find((m) => m
    && m.source === sourceTag
    && String(m.text || '').trim() === want
    && ['active', PENDING_SPLIT_STATUS].includes(String(m.status || 'active').toLowerCase()));
  return hit ? String(hit.id) : '';
}

/**
 * 组的库内现状（2026-11-22）：优先走 store 的库查询（整组判定以库为准），
 * 假 store 只有 metas 时按同一份收口函数 splitGroupFromRecords 收，口径不另写。
 */
function splitGroupViewOf(store: SplitStore, sourceTag: string): SplitGroupView {
  if (typeof store.splitGroupOf === 'function') {
    try {
      return store.splitGroupOf(sourceTag);
    } catch {
      /* 库查询失败就退回内存视图，别让判定本身炸掉拆分 */
    }
  }
  const metas = typeof store.metas === 'function' ? store.metas() : [];
  return splitGroupFromRecords(metas, sourceTag);
}

/** 整组转 active：真实 store 一笔事务；假 store 没批量接口就逐条 update（段本来就 active 时是空操作）。 */
async function activateGroup(store: SplitStore, ids: string[]): Promise<number> {
  if (!ids.length) return 0;
  if (typeof store.activateSplitGroup === 'function') return Number(await store.activateSplitGroup(ids)) || 0;
  if (typeof store.update === 'function') {
    let n = 0;
    for (const id of ids) {
      await store.update(id, { status: 'active' });
      n += 1;
    }
    return n;
  }
  return 0;
}

/** 半成品整组归档：reason 固定 partial-split，组键（source）与 extra.splitGroup 保留。 */
async function archiveGroup(store: SplitStore, ids: string[], reason: string = PARTIAL_SPLIT_REASON): Promise<number> {
  if (!ids.length) return 0;
  if (typeof store.archiveSplitGroup === 'function') return Number(await store.archiveSplitGroup(ids, reason)) || 0;
  if (typeof store.update === 'function') {
    let n = 0;
    for (const id of ids) {
      await store.update(id, { status: 'archived', reason });
      n += 1;
    }
    return n;
  }
  return 0;
}

/**
 * 覆盖校验的入参：最终切分方案 + 每段落库承接 + 两路被剔段 + 当前库快照。
 * 判定只看原文区间和逐字正文，不走 normalizeText（它把 >=5 和 <=5 折成 5）。
 */
export interface CoverageInput {
  /** 送进 split() 的原文（比较前 trim 两端）。 */
  sourceText: string;
  /** split() 返回的最终 parts（每个带 [start,end)）。 */
  parts: Array<{ text: string; start?: number; end?: number; [key: string]: unknown }>;
  /** 与 parts 一一对应的落库 id；没落地（判重/失败）留空串。 */
  landedIds: string[];
  lintDiscards?: Array<{ text: string; [key: string]: unknown }>;
  dedupeDiscards?: Array<{ text: string; [key: string]: unknown }>;
  metas: MemoryRecord[];
  /** 不许拿来当承接的条目（原条自己：它马上就要被归档）。 */
  excludeIds?: string[];
}

/** 覆盖校验结果：ok=false 时 reason 可直接当 last_error。 */
export interface CoverageResult {
  ok: boolean;
  reason?: string;
  detail?: string;
}

/** 去空白后的字符序列：只为判「被剔内容还在不在」，不做全半角/符号折叠。 */
function stripSpaceChars(text: unknown): string {
  return String(text ?? '').replace(/\s+/g, '');
}

/**
 * 归档前的覆盖校验（本票的第三道闸）：
 *   ① 每一段都要有落库承接：memory_id 存在、status 非 archived、正文逐字覆盖该段；
 *      判重丢掉的段允许用「库里已有同正文的活跃条目」承接（写入闸判 DUPLICATE 的正常结果）。
 *   ② lint 与 dedupe 两路被剔正文，必须还能在落库子段（或库里活跃条目）里逐字找到；
 *      找不到就是不归档、交人工——宁可停一条，不许把内容从召回里弄丢。
 */
export function verifySplitCoverage(input: CoverageInput): CoverageResult {
  const { parts, landedIds, metas } = input;
  const excluded = new Set((input.excludeIds || []).map(String));
  // 原条自己也在 activeMetas 里，但它就是等着被归档的那条，不能拿来给自己被剔的内容作承接
  const activeMetas = (Array.isArray(metas) ? metas : [])
    .filter((m) => (m?.status || 'active') !== 'archived' && !excluded.has(String(m?.id)));
  const trimmed = (value: unknown) => String(value ?? '').trim();
  const landedTexts: string[] = [];

  for (let i = 0; i < parts.length; i += 1) {
    const part = parts[i] || ({} as { text?: string });
    const text = trimmed(part.text);
    if (!text) return { ok: false, reason: `part_${i}_empty` };
    const id = String(landedIds?.[i] ?? '');
    let rec = id ? activeMetas.find((m) => String(m.id) === id) : undefined;
    // 判重丢弃的段没有新 id：允许库里已有的同正文活跃条目承接
    if (!rec) rec = activeMetas.find((m) => trimmed(m.text) === text);
    if (!rec) return { ok: false, reason: `part_${i}_not_landed`, detail: text.slice(0, 40) };
    if (trimmed(rec.text) !== text) return { ok: false, reason: `part_${i}_text_mismatch`, detail: String(rec.id) };
    landedTexts.push(text);
  }

  const landedJoined = stripSpaceChars(landedTexts.join(''));
  const allActiveJoined = activeMetas.map((m) => stripSpaceChars(m.text)).join('\n');
  const checkDiscards = (list: unknown, kind: string): CoverageResult | null => {
    for (const entry of (Array.isArray(list) ? list : []) as Array<{ text?: unknown }>) {
      const needle = stripSpaceChars(entry?.text);
      if (!needle) continue;
      if (landedJoined.includes(needle)) continue;
      if (allActiveJoined.includes(needle)) continue;
      return { ok: false, reason: `${kind}_discard_uncovered`, detail: String(entry?.text ?? '').slice(0, 40) };
    }
    return null;
  };
  const lintBad = checkDiscards(input.lintDiscards, 'lint');
  if (lintBad) return lintBad;
  const dedupeBad = checkDiscards(input.dedupeDiscards, 'dedupe');
  if (dedupeBad) return dedupeBad;

  // 位置自检：parts 的顺序区间必须单调、且落在原文范围内
  const sourceLen = trimmed(input.sourceText).length;
  let last = -1;
  for (let i = 0; i < parts.length; i += 1) {
    const start = Number(parts[i]?.start);
    const end = Number(parts[i]?.end);
    if (!Number.isInteger(start) || !Number.isInteger(end)) return { ok: false, reason: `part_${i}_no_range` };
    if (start < 0 || end > sourceLen || start >= end || start < last) {
      return { ok: false, reason: `part_${i}_range_invalid`, detail: `${start}-${end}` };
    }
    last = start;
  }
  return { ok: true };
}

/** 无原条任务的正文覆盖校验入参。 */
export interface TextCoverageInput {
  /** 送进 split() 的原文（payload.text，比较前 trim 两端）。 */
  sourceText: string;
  /** split() 返回的最终 parts（每个带 [start,end)）。 */
  parts: Array<{ text: string; start?: number; end?: number; [key: string]: unknown }>;
  /** 两路被剔段 + 判重丢掉的段：可用来解释原文里没被子段盖住的区间。 */
  discards?: Array<{ text?: unknown; [key: string]: unknown }>;
}

/**
 * 无原条任务的正文覆盖校验（2026-11-20）。
 *
 * 有原条那条路径靠库里 memories 行的区间对账；无原条（payload.source_id 为空、dedupeKey 只是
 * 队列键）没有行可对，以前整段覆盖校验被 `if (source)` 罩住，只要落进 ≥1 段就标 done。
 * 实锤：op 10992（dedupe_key split:73dec0d7e59c4dd6）原文 402 字、两段产物 272 字，
 * 缺的 130 字全库没有承接，op 照样 done。
 *
 * 这里改用 payload 里保存的原文：把 parts 的 [start,end) 从原文上盖一遍，
 * 没被盖住的区间（缝隙 + 尾部）必须能被某条被剔正文解释（去空白后逐字包含），否则就是
 * 拆分把内容丢了 —— 返回 ok=false，调用方标 dead + coverage_unverified。
 */
export function verifySplitTextCoverage(input: TextCoverageInput): CoverageResult {
  const source = String(input.sourceText ?? '').trim();
  const parts = Array.isArray(input.parts) ? input.parts : [];
  if (!source) return { ok: false, reason: 'empty_source' };
  const discards = (Array.isArray(input.discards) ? input.discards : [])
    .map((entry) => stripSpaceChars(entry?.text))
    .filter(Boolean);
  // 原文里没被段盖住的那截，能不能由「被剔正文」解释：去空白后互为子串即可。
  const explainedByDiscard = (gap: string): boolean => {
    const needle = stripSpaceChars(gap);
    if (!needle) return true;
    return discards.some((entry) => entry.includes(needle) || needle.includes(entry));
  };
  let cursor = 0;
  for (let i = 0; i < parts.length; i += 1) {
    const start = Number(parts[i]?.start);
    const end = Number(parts[i]?.end);
    if (!Number.isInteger(start) || !Number.isInteger(end) || start < 0 || start >= end || end > source.length) {
      return { ok: false, reason: `part_${i}_range_invalid`, detail: `${start}-${end}/${source.length}` };
    }
    if (start < cursor) return { ok: false, reason: `part_${i}_overlap`, detail: `${start}<${cursor}` };
    const gap = stripSpaceChars(source.slice(cursor, start));
    if (gap && !explainedByDiscard(gap)) return { ok: false, reason: 'text_gap_uncovered', detail: gap.slice(0, 40) };
    const partText = String(parts[i]?.text ?? '').trim();
    if (partText !== source.slice(start, end).trim()) {
      return { ok: false, reason: `part_${i}_text_mismatch`, detail: partText.slice(0, 40) };
    }
    cursor = end;
  }
  const tail = stripSpaceChars(source.slice(cursor));
  if (tail && !explainedByDiscard(tail)) return { ok: false, reason: 'text_tail_uncovered', detail: tail.slice(0, 40) };
  return { ok: true };
}

/**
 * 拆分归档前补 memory_link 的取代边（origin='curate'，2026-11-20）。
 *
 * memory-link.ts 文件头把「整理轮 origin='curate'」列成三个产出方之一，但全仓只有
 * entity-worker 的 queue 一档真在写；拆分归档只写 memories.supersededBy，memory_link 一条没有
 * （实锤：本轮 28 条合并产物对应 60 条 old→new，一条边都没落）。
 *
 * 三条约束都靠写入口保证，这里不绕：
 *   - dst 必须还活着 → 只能在 store.archive 之前调（归档后 target_not_active）；
 *   - 唯一键 uq_memory_link_edge(src,dst,rel) → 重复写只会判 edge_exists，不会插第二行；
 *   - origin 优先级 curate(2) < queue(3) → 以后 queue 判出别的取代能顶掉，不会被这里锁死。
 *
 * @returns {string[]} 真正写进去的子段 src（dry-run / 没表 / 已存在都不算）；archive 失败时按这份清单回滚
 */
function linkSplitEdges(
  db: DatabaseSync,
  childIds: string[],
  sourceId: string,
  { now, log }: { now: number; log?: ((line: string) => void) | null },
): string[] {
  if (!sourceId || !childIds.length) return [];
  const written: string[] = [];
  for (const child of childIds) {
    const src = String(child || '').trim();
    if (!src || src === sourceId) continue;
    const got = writeSupersedesLink(db, { src, dst: sourceId, origin: 'curate', attrKey: '', now });
    if (got.ok) written.push(src);
    else if (got.reason && got.reason !== 'edge_exists') log?.(`[split] item=${sourceId} memory_link 跳过 ${src}→${sourceId}：${got.reason}`);
  }
  return written;
}

/** 一轮的分类口径：只读 stats，不改它。 */
export function splitBreakdownOf(stats: SplitStats): SplitStatsBreakdown {
  return {
    succeeded: Number(stats.succeeded) || 0,
    dedupedOnly: Number(stats.dedupedOnly) || 0,
    skipped: Number(stats.skipped) || 0,
    pendingAfter: Number(stats.pendingAfter) || 0,
    versionMismatch: Number(stats.versionMismatch) || 0,
    coverageBlocked: Number(stats.coverageBlocked) || 0,
    unavoidable: Number(stats.unavoidable) || 0,
    inflateDead: Number(stats.inflateDead) || 0,
  };
}

/**
 * 分类合计（2026-11-22）：unavoidable 在 :764-765 已经加进 pendingAfter，这里再加一次
 * 就是双计——实测那轮 493 条被算成 740，「差多少条未落账」的差值方向就跟着反了。
 * 口径：不可拆包含在回队里，不单列。
 */
export function splitClassificationSum(b: SplitStatsBreakdown): number {
  return b.succeeded + b.dedupedOnly + b.skipped + b.pendingAfter
    + b.versionMismatch + b.coverageBlocked + b.inflateDead;
}

/** 多轮统计相加（轮末补扫用）：concurrency 取较大值，其余按条累加。 */
function mergeSplitStats(into: SplitStats, from: SplitStats): SplitStats {
  into.scanned += from.scanned;
  into.tried += from.tried;
  into.unavailable += from.unavailable;
  into.unavoidable += from.unavoidable;
  into.archived += from.archived;
  into.inserted += from.inserted;
  into.deduped += from.deduped;
  into.parts += from.parts;
  into.rejected += from.rejected;
  into.failed += from.failed;
  into.skipped += from.skipped;
  into.charsBefore += from.charsBefore;
  into.charsAfter += from.charsAfter;
  into.versionMismatch += from.versionMismatch;
  into.coverageBlocked += from.coverageBlocked;
  into.succeeded += from.succeeded;
  into.dedupedOnly += from.dedupedOnly;
  into.pendingAfter += from.pendingAfter;
  into.inflateDead += from.inflateDead;
  into.cacheHits += from.cacheHits;
  into.precheckSkipped += from.precheckSkipped;
  into.autoObservationSkipped += from.autoObservationSkipped;
  into.concurrency = Math.max(into.concurrency, from.concurrency);
  return into;
}

/**
 * 批内并发池：跟 sleep.js 的 runIngestPool 同构——worker 从 cursor 领一个任务，prepare 并发，
 * 主循环按原始顺序 await 槽位、串行 commit；一个任务 prepare 完没落地就不领下一个，
 * 所以在飞的结果最多 concurrency 个。
 *
 * 为什么不直接 import sleep.js 的 runIngestPool：sleep.js 已经 import 本文件，
 * 反向 import 会绕成环；本池一次只处理一批，批与批之间由外层的 dueOps 重新领活，
 * 跟 runIngestPool 一次吃整个 list 的用法也不一样。
 *
 * prepare 抛错只记在它自己身上（box.error），别的 worker 继续；commit 抛错同样只影响自己。
 *
 * @returns {Promise<{workers:number}>} workers = 本批实际开的 worker 数
 */
async function runSplitBatch(tasks: SplitTask[], concurrency: number, prepare: (task: SplitTask, i: number) => Promise<SplitPrepared>, commit: (task: SplitTask, got: SplitPrepared, i: number) => Promise<void>, writeLock: ((fn: () => Promise<void>) => Promise<void>) | null = null, log: ((line: string) => void) | null = null): Promise<{ workers: number }> {
  const list: SplitTask[] = Array.isArray(tasks) ? tasks : [];
  if (!list.length) return { workers: 0 };
  const conc = Math.min(Math.max(Math.floor(Number(concurrency) || 1), 1), SPLIT_HARD_CAP);
  const workers = Math.min(conc, list.length);
  const ready = list.map(() => {
    let resolve!: (value: SplitOutcome) => void;
    const promise = new Promise<SplitOutcome>((r) => { resolve = r; });
    return { promise, resolve };
  });
  const landed = list.map(() => {
    let resolve!: (value?: void | PromiseLike<void>) => void;
    const promise = new Promise<void>((r) => { resolve = r; });
    return { promise, resolve };
  });
  let cursor = 0;
  const worker = async () => {
    for (;;) {
      const i = cursor;
      cursor += 1;
      if (i >= list.length) return;
      let box: SplitOutcome;
      try {
        box = { ok: true, value: await prepare(list[i], i) };
      } catch (error) {
        box = { ok: false, error };
      }
      ready[i].resolve(box);
      await landed[i].promise; // 等这条落地完再领下一条，未落地的结果最多 workers 个
    }
  };
  const running = Promise.all(Array.from({ length: workers }, () => worker()));
  // 落地串行 + 写闸：批内所有写库（store.save / archive / markOp）都从这条线过，
  // 池与池之间共用同一把 writeLock 时，SQLite 同一时刻只有一个写者。
  for (let i = 0; i < list.length; i += 1) {
    try {
      const box = await ready[i].promise;
      const landedOne = () => commit(list[i], box.ok ? box.value : { error: box.error }, i);
      await (writeLock ? writeLock(landedOne) : landedOne());
    } catch (error) {
      // commit / writeLock 抛错（含 markOp 抛错）绝不能把 landed 卡住：worker 在等它，
      // 漏一次 resolve 整批就永远收不了尾。只记一行，这条 op 留给租约回收（reap 计 attempts）。
      log?.(`[split] commit 抛错 id=${list[i]?.op?.id ?? ''}：${String((error as Error && (error as Error).message) || error as string).slice(0, 120)}`);
    } finally {
      landed[i].resolve();
    }
  }
  await running;
  return { workers };
}

/**
 * 拆分失败必须落成可重试状态：
 * split() 自己会 recordSplitFailure（enqueue + markOp pending，attempts+1）；
 * 但如果它没来得及落（比如外层直接抛错），行还停在 running——这里补一刀。
 * 已经回 pending 的绝不再 markOp：那会把 attempts 加两次。
 * attempts 到 MAX_ATTEMPTS 由 markOp → ledger 判 dead，这里不另设阈值。
 */
function retryIfRunning(db: DatabaseSync, opId: string | number, reason: string, { now, dryRun, log }: { now: number; dryRun: boolean; log?: ((line: string) => void) | null }): Row | null {
  let row = null;
  try {
    row = db.prepare('SELECT status, attempts FROM pending_ops WHERE id = ?').get(opId) as Row;
  } catch {
    return null;
  }
  if (!row || row.status !== 'running') return row;
  log?.(`[split] op=${opId} 失败没有落账（仍在 running），补 pending attempts=${(Number(row.attempts) || 0) + 1}`);
  return markOp(db, opId, 'pending', reason, { now, dryRun, log });
}

/**
 * 消费一批 split_item。领活按小批（= 并发数）走，一批收口完再领下一批；
 * 处理完按结果标 done / pending（split() 或写入闸自己会加 attempts）。
 *
 * @param {object} opts
 * @param {import('node:sqlite').DatabaseSync} opts.db
 * @param {object} opts.store createStore 的返回
 * @param {object} opts.llm ctx.llm
 * @param {string} [opts.provider]
 * @param {string} [opts.model]
 * @param {number} [opts.limit=Infinity] 本轮最多处理多少条；默认全领（分批领，不再一次全标 running）
 * @param {number} [opts.concurrency=1] 并发路数；sleep.js 传 selectIngestConcurrency 算出的值（≤64）
 * @param {number} [opts.maxChars=200]
 * @param {number} [opts.maxTokens=0] 拆分模型输出上限；0 = 不传，由 provider 封顶
 * @param {boolean} [opts.dryRun=false]
 * @param {(line:string)=>void} [opts.log]
 * @param {number} [opts.now] 测试注入；不传就每条活按当时的 Date.now()
 * @returns {Promise<{scanned:number, tried:number, unavailable:number, archived:number, unavoidable:number,
 *   inserted:number, deduped:number, parts:number, rejected:number, failed:number, skipped:number,
 *   concurrency:number}>}
 */
export async function consumeSplitOps(opts: SplitConsumeOpts = {}): Promise<SplitStats> {
  const {
    db, store, llm, provider, model, limit = Infinity, maxChars = 200,
    maxTokens = 0, dryRun = false, log, now: nowOpt,
    // 正文 lint 开关（与 store 同一个口径）；调用方传 cfg.pipeline.textLint !== false
    textLint = true,
    // lint 两级复核；调用方按 cfg.pipeline 的口径传
    lintReview = true,
    lintReviewTimeoutMs = 0,
    // 段级信号是否硬拒（阶段二）；默认 false = 只记
    signalsStrict = false,
    concurrency = 1,
    writeLock = null,
    verdictCache = null,
  } = opts;
  const stats: SplitStats = {
    scanned: 0, tried: 0, unavailable: 0, unavoidable: 0, archived: 0,
    inserted: 0, deduped: 0, parts: 0, rejected: 0, failed: 0, skipped: 0,
    charsBefore: 0, charsAfter: 0, concurrency: 0, versionMismatch: 0, coverageBlocked: 0,
    succeeded: 0, dedupedOnly: 0, pendingAfter: 0, inflateDead: 0,
    cacheHits: 0, precheckSkipped: 0, autoObservationSkipped: 0,
  };
  if (!db || !store) return stats;
  // memory_link 表在不在：一轮判一次（每条都判会刷屏）。缺表只提醒一次，边跳过、拆分照常落库。
  const linkTable = hasMemoryLinkTable(db);
  let linkWarned = false;
  const splitEdgesOf = (childIds: string[], parentId: string): string[] => {
    if (!linkTable) {
      if (!linkWarned) {
        linkWarned = true;
        log?.('[split] memory_link 表不在，取代边跳过（先跑 scripts/apply-curate-schema.mjs）');
      }
      return [];
    }
    return linkSplitEdges(db, childIds, parentId, { now: nowOf(), log });
  };

  // 一轮内固定：进来算一次，批与批之间不抖。
  const conc = Math.min(Math.max(Math.floor(Number(concurrency) || 1), 1), SPLIT_HARD_CAP);
  // 预领粒度：跟并发一样大。领了没做等于把租约占死，一批做完再领下一批。
  const batchSize = conc;
  stats.concurrency = conc;
  // 测试注入固定 now；否则每条活按当时的钟，退避时间算得准。
  const nowOf = () => (Number.isFinite(Number(nowOpt)) ? Number(nowOpt) : Date.now());
  let remaining = limit === Infinity ? Infinity : Math.max(0, Math.floor(Number(limit) || 0));
  // 本轮失败/回 pending 的 id：markOp 把 next_try_at 设成 now，下一批 dueOps 会立刻又领到，
  // 同一轮里不许原地重试同一条（下一轮 sleepPass 再领）。
  // 轮末补扫（consumeSplitOpsWithRecheck）要把这份集合跨 pass 传下来，否则第二趟立刻重领上一趟回队的 op。
  const failedThisRound: Array<string | number> = Array.isArray(opts.skipOpIds) ? opts.skipOpIds : [];
  let peakConc = 0;

  while (remaining > 0) {
    const want = remaining === Infinity ? batchSize : Math.min(batchSize, remaining);
    let ops: DueOp[] = [];
    try {
      ops = dueOps(db, want, { kind: 'split_item', now: nowOf(), exclude: failedThisRound });
    } catch (error) {
      log?.(`[split] WARN 领取 split_item 失败：${String((error as Error && (error as Error).message) || error as string)}`);
      break;
    }
    if (!ops.length) break;
    if (remaining !== Infinity) remaining -= ops.length;

    // ① 前置检查（读库 + 终态标记，都是同步快活，串行做）：决定这条要不要问模型
    const tasks: SplitTask[] = [];
    for (const op of ops) {
      const payload = op.payload || {} as Row;
      // 两种来源分开：payload.source_id 是记忆 id；op.dedupeKey 在缺 source_id 时只是队列键
      // （split:<sha1 前16>）。sourceId 仍两者取一，保证「有键就继续处理」的旧行为不变；
      // 落库时的 source 由 partSourceOf() 区分，别让队列键冒充记忆 id。
      const payloadSourceId = String(payload.source_id || '');
      const dedupeKey = String(op.dedupeKey || '');
      const sourceId = payloadSourceId || dedupeKey;
      stats.scanned += 1;

      if (!sourceId) {
        markOp(db, op.id, 'done', 'no_source_id', { now: nowOf(), dryRun, log });
        stats.skipped += 1;
        continue;
      }

      const source = sourceOf(store, sourceId);
      // 合并 agent 的 add 候选没有对应原条：payload 里自带正文，拆完直接入库，不用归档谁。
      const text = String(payload.text || source?.text || '');

      // 版本闸（入口）：payload.text 与库里 memories.text 逐字比对——
      // 只 trim 两端，正文内部的空格/缩进算内容。对不上就不拆，避免用旧正文拆新条目丢新内容。
      if (source) {
        const payloadText = String(payload.text ?? '');
        const dbText = String(source.text ?? '');
        if (payloadText.trim() && payloadText.trim() !== dbText.trim()) {
          log?.(
            `[split] item=${sourceId} op=${op.id} text_version_mismatch payload_len=${payloadText.length} db_len=${dbText.length} ` +
              `payload_sha1=${sha1Hex(payloadText)} db_sha1=${sha1Hex(dbText)}`,
          );
          markOp(db, op.id, 'dead', 'text_version_mismatch', { now: nowOf(), dryRun, log });
          // 版本不符自己一类（以前顺手算进 skipped，汇总时又被当成「成功」）
          stats.versionMismatch += 1;
          stats.failed += 1;
          continue;
        }
      }

      // 自动观察不进普通拆分（2026-11-22）：它有自己的复核链路（按来源根算独立来源、
      // 够 2 条才升级），拆出来的子段没有 derived_from 证据边，等于凭空造断言。
      // 入口（sleep/pool.ts 的超长入队、split-pass.ts 的 targets）已挡，这里兜旧队列里已入队的。
      if (isAutoObservation(source)) {
        markOp(db, op.id, 'done', 'auto_observation_skip', { now: nowOf(), dryRun, log });
        stats.skipped += 1;
        stats.autoObservationSkipped += 1;
        log?.(`[split] item=${sourceId} 自动观察跳过普通拆分 op=${op.id}`);
        continue;
      }

      if (!source && !text.trim()) {
        // 没 id 也没正文：重试一百次也不会变，终态 done，不静默
        markOp(db, op.id, 'done', 'source_missing', { now: nowOf(), dryRun, log });
        stats.skipped += 1;
        log?.(`[split] item=${sourceId} 原条不在库里且没有正文，op 已标 done`);
        continue;
      }
      if (source && (source.status || 'active') !== 'active') {
        markOp(db, op.id, 'done', 'source_not_active', { now: nowOf(), dryRun, log });
        stats.skipped += 1;
        log?.(`[split] item=${sourceId} 原条 status=${source.status}，op 已标 done`);
        continue;
      }
      if (!text.trim()) {
        markOp(db, op.id, 'done', 'empty_text', { now: nowOf(), dryRun, log });
        stats.skipped += 1;
        log?.(`[split] item=${sourceId} 没有正文可拆，op 已标 done`);
        continue;
      }

      if (dryRun) {
        markOp(db, op.id, 'done', 'dry_run', { now: nowOf(), dryRun, log });
        stats.skipped += 1;
        log?.(`[split] dry-run item=${sourceId} ${text.length}字 待拆`);
        continue;
      }

      // inflate 类失败（拆完总字数超原文 20%）到判死阈值就不再叫模型，直接 dead 等人工捞。
      if (Number(op.attempts) >= MAX_ATTEMPTS) {
        let row = null;
        try {
          row = db.prepare('SELECT last_error FROM pending_ops WHERE id = ?').get(op.id);
        } catch {
          row = null;
        }
        if (/^inflate/.test(String(row?.last_error || ''))) {
          markOp(db, op.id, 'dead', 'inflate_retry_exhausted', { now: nowOf(), dryRun, log });
          stats.failed += 1;
          stats.inflateDead += 1;
          log?.(`[split] item=${sourceId} inflate 已到 attempts=${MAX_ATTEMPTS}，op 标 dead 不再自动重试`);
          continue;
        }
      }

      // 本地预检（2026-11-23，t-mwsviczp-epd3）：正文中间没有任何合法切点（句末标点/换行，
      // 且不在引号、括号、代码围栏内部）时，模型怎么问只会答 unavoidable——直接保留原文、
      // 落 pending 等下一轮，不打这次模型调用。判据与 split() 共用 legalCutPoints，
      // 有切点（哪怕边界模糊）一律照常送审，不在这里替模型做决定。
      if (!hasLegalCutPoint(text)) {
        stats.unavoidable += 1;
        stats.pendingAfter += 1;
        stats.precheckSkipped += 1;
        failedThisRound.push(op.id);
        recordSplitFailure(db, {
          sourceId,
          text,
          type: splitTypeOf(store, payload, source),
          reason: 'precheck_unavoidable: 正文中没有合法切点',
          attempt: Number(op.attempts) || 0,
          log,
          now: nowOf(),
        });
        log?.(`[split] 预检不可拆 跳过模型 item=${sourceId} len=${text.length}`);
        continue;
      }

      // 「不可拆」判定缓存（2026-11-22）：同一正文 + 同一字数上限 + 同一规则版本不重复问模型。
      // 命中仍按不可拆处理：recordSplitFailure 照旧落 pending、attempts+1，原文一个字不动；
      // 不影响 failedThisRound 的同轮去重，也不进 stats.tried（确实没调模型）。
      const cached = verdictCache ? verdictCache.hit(text, maxChars) : null;
      if (cached) {
        stats.unavoidable += 1;
        stats.pendingAfter += 1;
        stats.cacheHits += 1;
        failedThisRound.push(op.id);
        recordSplitFailure(db, {
          sourceId,
          text,
          type: splitTypeOf(store, payload, source),
          reason: `unavoidable: cached: ${cached.reason || 'unavoidable'}`,
          attempt: Number(op.attempts) || 0,
          log,
          now: nowOf(),
        });
        log?.(`[split] item=${sourceId} unavoidable 命中判定缓存，未调模型 ms=0`);
        continue;
      }

      tasks.push({ op, sourceId, text, source, payload });
    }

    if (!tasks.length) continue;

    // ② 批内并发问模型
    const { workers } = await runSplitBatch(
      tasks,
      conc,
      async (task) => {
        stats.tried += 1;
        const startedAt = Date.now();
        let answer = null;
        let error = null;
        try {
          answer = await split({
            sourceId: task.sourceId,
            text: task.text,
            type: splitTypeOf(store, task.payload, task.source),
            ts: task.payload.ts as string || task.source?.ts || '',
            llm,
            provider,
            model,
            db,
            maxChars,
            maxTokens,
            attempt: Number(task.op.attempts) || 0,
            textLint,
            lintReview,
            lintReviewTimeoutMs,
            signalsStrict,
            log,
          });
        } catch (err) {
          error = err;
        }
        return { answer, error, ms: Math.max(0, Date.now() - startedAt) };
      },
      // ③ 串行收口：写库、标 done/dead、落 pending_ops
      async (task, got) => {
        const { op, sourceId, source, payload } = task;
        const ms = Math.max(0, Number(got?.ms) || 0);

        if (got?.error) {
          stats.failed += 1;
          stats.pendingAfter += 1;
          failedThisRound.push(op.id);
          markOp(db, op.id, 'pending', `split_error:${String((got.error as Error && (got.error as Error).message) || got.error as string).slice(0, 100)}`, { now: nowOf(), dryRun, log });
          log?.(`[split] item=${sourceId} 拆分抛错：${String((got.error as Error && (got.error as Error).message) || got.error as string)} ms=${ms}`);
          return;
        }

        const answer = got?.answer;
        if (!answer) {
          // split() 正常会自己落 pending（attempts+1）；万一没落，这里补成可重试，绝不写 done。
          retryIfRunning(db, op.id, 'split_fail', { now: nowOf(), dryRun, log });
          stats.failed += 1;
          stats.pendingAfter += 1;
          failedThisRound.push(op.id);
          log?.(`[split] item=${sourceId} FAIL 拆不出合格分段 ms=${ms}`);
          return;
        }
        if (answer.unavoidable) {
          // 同上：recordSplitFailure 已落 pending；行还在 running 时才补。
          retryIfRunning(db, op.id, 'unavoidable', { now: nowOf(), dryRun, log });
          stats.unavoidable += 1;
          stats.pendingAfter += 1;
          failedThisRound.push(op.id);
          // 记进判定缓存（存 sleep.state.json，跨轮存活）：下一轮同一正文 + 同一上限 +
          // 同一规则版本不再问模型。正文变了键就变，自然重问。
          verdictCache?.remember(task.text, maxChars, String(answer.reason || 'unavoidable'));
          log?.(`[split] item=${sourceId} unavoidable 已落 pending ms=${ms}`);
          return;
        }

        stats.charsBefore += task.text.length;
        stats.charsAfter += answer.parts.reduce((n, part) => n + String(part.text || '').length, 0);
        const tags = Array.isArray(source?.tags) ? source.tags.map(String) : (Array.isArray(payload.tags) ? payload.tags.map(String) : []);
        const entities = Array.isArray(source?.entities) ? source.entities.map(String) : (Array.isArray(payload.entities) ? payload.entities.map(String) : []);
        const type = (source && typeof store.typeOf === 'function' ? store.typeOf(source) : '') || payload.type || 'world';

        let inserted = 0;
        let deduped = 0;
        let rejected = 0;
        let replayed = 0;
        let gatePending = false;
        const newIds: string[] = [];
        // 子段实体（源条 entities ∪ 每段落库返回的 entities）：父条归档那笔事务里一起登记待观察
        const partEntities = new Set<string>(entities);
        // 与 answer.parts 一一对应的落库 id：判重/失败留空串，覆盖校验按位置核对
        const landedIds: string[] = [];
        const dedupedParts: Array<{ text: string; [key: string]: unknown }> = [];
        // 判重承接的段没有新行，组完整性要靠下标补齐（重试时同一段还会再判一次 DUPLICATE）
        const dedupedIndexes: number[] = [];
        // 这一组应该有几段：落在每段行的 extra.splitGroup 上，收尾与重试都按它核
        const expected = answer.parts.length;
        // 子段来源：有记忆 id 用 sleep:split:<id>；队列键来源用 sleep:split:queue:<key>，
        // 别让 split:<sha1> 冒充 memories.id（回查父条会 MISSING）。
        const partSource = partSourceOf(String(payload.source_id || ''), String(op.dedupeKey || ''));
        // 每一条子段单独走 store.save：一次一次过 assertWriteAllowed，绝不批量放行
        for (let partIndex = 0; partIndex < answer.parts.length; partIndex += 1) {
          const part = answer.parts[partIndex];
          // 上一轮已经落过库的段（原条还在 pending 时重试）：复用 id，别再入一次
          const priorId = savedPartId(store, partSource, part.text);
          if (priorId) {
            replayed += 1;
            inserted += 1;
            newIds.push(priorId);
            landedIds.push(priorId);
            continue;
          }
          try {
            const rec = await store.save(part.text, tags, partSource, {
              // 复核判 keep 的段落原样放行：带 __lintReview 过 store 最后一道闸（别写成 __lintChecked，那是内部二次写入的旁路）
              ...lintReviewMeta(part),
              type,
              entities,
              validFrom: source?.validFrom,
              sourceFile: partSource,
              // 写入那一刻就是中间态：同一笔 save 事务里落 status=pending_split，
              // 默认召回硬剔除它；整组齐了才由 activateSplitGroup 一笔事务激活。
              status: PENDING_SPLIT_STATUS,
              // 组键 / expected / 段下标一起进 extra：重试、重启、清理脚本都按库里这几列对账
              __splitGroup: { source: partSource, expected, index: partIndex },
              // 原条只排除出写入闸召回（否则子段会被原条判成 DUPLICATE），
              // 不建立取代关系：supersededBy/validTo 只在归档成功时由 store.archive 同一笔事务写。
              __gateExclude: source ? [sourceId] : undefined,
              // 子段自己这笔 save 事务里按 'split' 登记它挂的实体（docs/observation-plan-v2.md §4）
              __observeReason: 'split',
            });
            if (rec?.discarded) {
              deduped += 1;
              dedupedParts.push(part);
              dedupedIndexes.push(partIndex);
              landedIds.push('');
              continue;
            }
            inserted += 1;
            newIds.push(String(rec.id));
            landedIds.push(String(rec.id));
            for (const e of rec.entities || []) partEntities.add(String(e));
          } catch (error) {
            if (error && (error as { code?: string }).code === 'WRITE_GATE_PENDING') {
              // 闸未决：候选已落 dedupe_check pending，整条拆分会下一轮重来，别再写、别归档
              gatePending = true;
              rejected += 1;
              landedIds.push('');
              continue;
            }
            rejected += 1;
            landedIds.push('');
            log?.(`[split] item=${sourceId} 子段入库失败：${String((error as Error && (error as Error).message) || error as string).slice(0, 120)}`);
          }
        }
        stats.parts += answer.parts.length;
        stats.inserted += inserted;
        stats.deduped += deduped;
        stats.rejected += rejected;

        // 组完整性以库为准（2026-11-22）：按 source 键数库里已落库的段，加上本次判重承接的
        // 下标，必须盖满 expected 个位置。只看本次调用的返回值不够——上一轮已落库的段、
        // 进程重启后重新领到的 op，都只能靠库里的行认出来。
        const group = splitGroupViewOf(store, partSource);
        const gap = splitGroupGap(group, expected, dedupedIndexes);
        // 组视图里一行都认不出来（注入的假 store 不写 source、或存储面没提供组分）：
        // 退回本次调用的落库账，不能因为「看不见组」就判不完整——真实 store 一定写 source。
        const groupSeen = group.rows.length > 0;
        const groupComplete = groupSeen
          ? gap.complete
          : (inserted + deduped >= expected && rejected === 0 && !gatePending);

        if (gatePending || rejected > 0 || !groupComplete) {
          // 存储高-3：有段没落地就不许归档原条（否则那段随原条一起从召回里消失）。
          // 已入库的段留在库里、保持 pending_split、不回收滚；op 回 pending，
          // 下一轮靠 savedPartId 幂等跳过它们，补齐后再整组激活。
          const why = gatePending
            ? 'write_gate_pending'
            : (rejected > 0
              ? `partial_write:${inserted}/${expected}`
              : `split_group_incomplete:${groupSeen ? gap.have : inserted + deduped}/${expected}${gap.missing.length ? ` missing=${gap.missing.join(',')}` : ''}`);
          failedThisRound.push(op.id);
          stats.failed += 1;
          stats.pendingAfter += 1;
          const marked = markOp(db, op.id, 'pending', why, { now: nowOf(), dryRun, log });
          // 重试耗尽：ledger 到 MAX_ATTEMPTS 会直接转 dead。这一刻把已经落库的半成品归档，
          // 不留 pending_split 残段在库里等一个不会到来的下一轮。
          const cleaned = String(marked?.status) === 'dead'
            ? await archiveGroup(store, pendingSplitIds(group), PARTIAL_SPLIT_REASON)
            : 0;
          log?.(
            `[split] item=${sourceId} WARN 子段只落地 ${inserted}/${expected}（${why}），` +
              `原条保持 active，op ${String(marked?.status) === 'dead' ? '已判 dead' : '回 pending'}；` +
              `已入库 ${replayed} 段重试时跳过` +
              (cleaned ? `，半成品 ${cleaned} 段已归档 reason=${PARTIAL_SPLIT_REASON}` : ''),
          );
          return;
        }

        if (source) {
          // 版本闸（拆中）：拆分期间原条被改（含正文内部空格/缩进变化）→ 不归档，交人工。
          const nowSource = sourceOf(store, sourceId);
          const nowText = String(nowSource?.text ?? '');
          if (!nowSource || nowText.trim() !== task.text.trim()) {
            log?.(
              `[split] item=${sourceId} op=${op.id} text_changed_during_split db_len=${nowText.length} split_len=${task.text.length} ` +
                `db_sha1=${sha1Hex(nowText)} split_sha1=${sha1Hex(task.text)}`,
            );
            markOp(db, op.id, 'dead', 'text_version_mismatch', { now: nowOf(), dryRun, log });
            stats.versionMismatch += 1;
            stats.failed += 1;
              failedThisRound.push(op.id);
            return;
          }
        }

        // 覆盖闸：每段都要有落库承接，两路被剔正文都还要能找到，才允许收尾。
        // 判定只用原文区间和逐字正文，不用 normalizeText（它把 >=5 和 <=5 折成 5）。
        //
        // 2026-11-20：这道闸以前整段被 `if (source)` 罩住，无原条（payload.source_id 为空、
        // 退回 dedupeKey 当 sourceId）的 op 走不到这里就标 done，只要落进 ≥1 段就算成功。
        // 现在两种来源都要过，缺内容一律 dead：
        //   - 有原条：拿库里 memories 行对账（行为不变，excludeIds 只排原条自己）；
        //   - 无原条：没有行可对，用 payload 里保存的原文对子段做去空白覆盖比对。
        const discards = [...(answer.lintDiscards || []), ...(answer.dedupeDiscards || []), ...dedupedParts];
        const coverage = verifySplitCoverage({
          sourceText: task.text,
          parts: answer.parts,
          landedIds,
          lintDiscards: answer.lintDiscards,
          dedupeDiscards: [...(answer.dedupeDiscards || []), ...dedupedParts],
          metas: typeof store.metas === 'function' ? store.metas() : [],
          excludeIds: source ? [sourceId] : [],
        });
        const textCoverage: CoverageResult = source
          ? { ok: true }
          : verifySplitTextCoverage({ sourceText: task.text, parts: answer.parts, discards });
        const coverageBad = !coverage.ok ? coverage : (!textCoverage.ok ? textCoverage : null);
        if (coverageBad) {
          log?.(
            `[split] item=${sourceId} op=${op.id} coverage_unverified ${coverageBad.reason}` +
              (coverageBad.detail ? ` detail=${coverageBad.detail}` : '') +
              (source ? ' 原条保持 active，op 标 dead 交人工' : ' 无原条可归档，op 标 dead 交人工'),
          );
          markOp(db, op.id, 'dead', 'coverage_unverified', { now: nowOf(), dryRun, log });
          stats.coverageBlocked += 1;
          stats.failed += 1;
          failedThisRound.push(op.id);
          return;
        }

        let linkedIds: string[] = [];
        if (inserted > 0) {
          // 整组齐了才激活（一笔事务）：先激活再归档父条——反过来的话，archive 成功而激活
          // 失败会留下「父条已归档、子段 pending_split」的不可召回状态，重试还会被入口的
          // source_not_active 判成 done，内容就真没了。激活失败只会整笔回滚、op 留 running 等重领。
          const activated = await activateGroup(store, pendingSplitIds(group));
          if (activated) {
            log?.(`[split] item=${sourceId} 组 ${partSource} 齐 ${expected}/${expected}，激活 ${activated} 段`);
          }
          if (source) {
            // memory_link 的 curate 边：拆分同样是「新条取代旧条」，跟 supersededBy 一起补边。
            // 必须在 archive 之前写：写入口要求 dst 还活着（memory-link.ts 的 targetIsActive）。
            linkedIds = splitEdgesOf(newIds, sourceId);
            try {
              // supersededBy/validTo 跟着归档这一笔事务一起写（子段保存时不碰）
              await store.archive(sourceId, `拆成 ${inserted} 条：${newIds.join(' ')}`.slice(0, 200), {
                supersededBy: newIds[newIds.length - 1],
                childValidFrom: String(source.validFrom || ''),
                childTs: String(source.ts || ''),
                // 父条实体（updateMemory 里还会并上它的旧挂载）+ 全部子段实体，
                // 在归档这一笔事务里统一登记 reason='split'（docs §4）
                observeEntities: [...partEntities],
                observeReason: 'split',
              });
              stats.archived += 1;
              if (linkedIds.length) log?.(`[split] item=${sourceId} memory_link curate 边 ${linkedIds.length} 条（子段→原条）`);
            } catch (error) {
              // 归档没成就不能标 done：原条还 active，下一轮重试（子段全在，savedPartId 会全跳过）。
              // 边是 archive 之前刚写的，这里按 src 清单 + dst + origin='curate' 精确撤掉，别留脏边：
              // 原条还活着、子段也还在，留着边等于凭空说「子段取代了原条」。
              let rolledBack = 0;
              for (const child of linkedIds) {
                rolledBack += deleteSupersedesLink(db, { src: child, dst: sourceId, origin: 'curate' });
              }
              if (rolledBack) log?.(`[split] item=${sourceId} 归档失败，回滚 memory_link curate 边 ${rolledBack} 条`);
              const why = `archive_failed:${String((error as Error && (error as Error).message) || error as string).slice(0, 100)}`;
              failedThisRound.push(op.id);
              stats.failed += 1;
              stats.pendingAfter += 1;
                  markOp(db, op.id, 'pending', why, { now: nowOf(), dryRun, log });
              log?.(`[split] item=${sourceId} 原条归档失败，op 回 pending：${String((error as Error && (error as Error).message) || error as string)}`);
              return;
            }
          }
          // done 前再核一次（以库为准，2026-11-22）：段位必须盖满 expected，有原条时父条
          // 必须已经归档。少了这一步就会出现「done 任务放行残句」——返回值说齐了，库里却没齐。
          const finalView = splitGroupViewOf(store, partSource);
          const finalGap = splitGroupGap(finalView, expected, dedupedIndexes);
          const finalSeen = finalView.rows.length > 0;
          const groupOk = finalSeen ? finalGap.complete : (inserted + deduped >= expected);
          const parentStatus = source ? String(sourceOf(store, sourceId)?.status || '') : '';
          if (!groupOk || (source && parentStatus !== 'archived')) {
            const why = !groupOk
              ? `done_group_incomplete:${finalSeen ? finalGap.have : inserted + deduped}/${expected}${finalGap.missing.length ? ` missing=${finalGap.missing.join(',')}` : ''}`
              : `done_parent_not_archived:${parentStatus || 'missing'}`;
            failedThisRound.push(op.id);
            stats.failed += 1;
            stats.pendingAfter += 1;
            markOp(db, op.id, 'pending', why, { now: nowOf(), dryRun, log });
            log?.(`[split] item=${sourceId} WARN done 前复核没过（${why}），不标 done`);
            return;
          }
          markOp(db, op.id, 'done', null, { now: nowOf(), dryRun, log });
          stats.succeeded += 1;
        } else if (deduped > 0) {
          // 每一段都判重：原条没有新增信息，但也不能贸然归档，留 active，op 终态
          markOp(db, op.id, 'done', 'all_parts_deduped', { now: nowOf(), dryRun, log });
          stats.dedupedOnly += 1;
          log?.(`[split] WARN item=${sourceId} ${deduped} 段全被判重，${source ? '原条保持 active' : '无原条可归档'}`);
        } else {
          markOp(db, op.id, 'pending', 'no_part_inserted', { now: nowOf(), dryRun, log });
          stats.failed += 1;
          stats.pendingAfter += 1;
          failedThisRound.push(op.id);
        }

        // 逐条成功行带 ms（跟 ingest 的 `file= ok= ms=` 一个口径），落地那一刻就报
        log?.(
          `[split] item=${sourceId} parts=${answer.parts.length} retry=${Math.max(0, Number(answer.rounds || 1) - 1)} ` +
            `rejected=${rejected} inserted=${inserted} deduped=${deduped} replayed=${replayed} ms=${ms}`,
        );
      },
      writeLock,
      log,
    );
    peakConc = Math.max(peakConc, workers);
  }

  // 汇总一行：grep `[split] 并发` 就能看出这轮开几路、每条 op 落在哪一类。
  // 2026-11-20：以前「成功 = scanned - failed」，把 skipped / versionMismatch / no_source_id /
  // unavoidable 全算成成功（日志「条 245 成功 234 失败 11」而库里 done 102 / dead 95 / pending 41）。
  // 现在只数真正落库的条数，其余分类单列，相加等于 scanned，能直接和 pending_ops 的
  // done/dead/pending 对上：
  //   done = succeeded + dedupedOnly + skipped
  //   dead = versionMismatch + coverageBlocked + inflateDead
  //   pending = pendingAfter（不可拆已经算在里面，2026-11-22 起不再单列）
  if (stats.scanned > 0) {
    const b = splitBreakdownOf(stats);
    const sum = splitClassificationSum(b);
    log?.(
      `[split] 并发 ${peakConc || Math.min(conc, stats.scanned)} 路 条 ${stats.scanned} ` +
        `落库 ${b.succeeded} 全判重 ${b.dedupedOnly} 跳过 ${b.skipped} 回队 ${b.pendingAfter}（含不可拆 ${b.unavoidable}） ` +
        `版本不符 ${b.versionMismatch} 覆盖未过 ${b.coverageBlocked} 判死 ${b.inflateDead} ` +
        `失败 ${stats.failed} 分类合计 ${sum} 缓存命中 ${stats.cacheHits} ` +
        `预检跳过 ${stats.precheckSkipped} 自动观察跳过 ${stats.autoObservationSkipped}`,
    );
    if (sum !== stats.scanned) {
      // 有 op 没落分类（比如 commit 抛错、行还挂在 running）：当场说清楚，别让汇总看着齐整。
      // 方向分开说：sum 小 = 少记；sum 大 = 重复入账（双计这类 bug 一眼能看出来）。
      const diff = sum < stats.scanned
        ? `少记 ${stats.scanned - sum} 条（多为 commit 抛错留在 running）`
        : `多记 ${sum - stats.scanned} 条（查重复入账）`;
      log?.(`[split] WARN 分类合计 ${sum} 与条 ${stats.scanned} 不符：${diff}`);
    }
  }

  // 本地预检汇总（2026-11-23）：打过模型的条数 = tried，预检省下的调用数 = precheckSkipped。
  // 这行与逐条的 `[split] 预检不可拆 跳过模型` 一起，方便以后按轮核数。
  if (stats.precheckSkipped > 0) {
    log?.(`[split] 预检不可拆 合计 n=${stats.precheckSkipped} saved_llm=${stats.precheckSkipped}`);
  }

  // 判定缓存统计（2026-11-22）：接了缓存就每轮打一行，命中 0 也打。
  // 「不能静默全不命中」靠这行的 size / rules：字段被升级读丢时 size=0，
  // SPLIT_RULES 文本改过时 rules 串变，一眼能看出来。dryRun 不写 state。
  if (verdictCache) {
    const size = dryRun ? verdictCache.size() : verdictCache.flush();
    log?.(
      `[split] verdict-cache hit=${stats.cacheHits} saved_llm=${stats.cacheHits} ` +
        `added=${verdictCache.added()} size=${size} max=${verdictCache.max} ` +
        `rules=${verdictCache.ruleVersion} 自动观察跳过=${stats.autoObservationSkipped}`,
    );
  }
  return stats;
}

/**
 * 等生产者收工，最多等 timeoutMs（2026-11-20）。
 *
 * 为什么要有超时：ingest/merge 两路 drain 的 promise 只在两路都 settle 之后才 resolve。
 * 某一路的模型调用永不返回（provider 挂死、socket 半开）时，这里会永久挂住，整轮 sleep 不结束——
 * 补扫只是「顺手多扫一遍」，不该有把整轮拖死的权力。
 *
 * @returns {Promise<boolean>} true = 生产者已收工；false = 超时，调用方跳过补扫继续收尾
 */
async function waitProducerOrTimeout(wait: () => Promise<void>, timeoutMs: number): Promise<boolean> {
  let timer: ReturnType<typeof setTimeout> | null = null;
  const done = Promise.resolve().then(wait).then(() => true);
  // 超时之后 wait 才 reject 时不算未处理拒绝：结果已经不要了。
  done.catch(() => {});
  try {
    const expired = new Promise<boolean>((resolve) => {
      timer = setTimeout(() => resolve(false), Math.max(20, Math.floor(timeoutMs)));
    });
    return await Promise.race([done, expired]);
  } finally {
    if (timer) clearTimeout(timer);
  }
}

/**
 * 「拆分池领空即退」的补丁（2026-11-20）。
 *
 * consumeSplitOps 的 while 里 `if (!ops.length) break` 让消费者一领空就收工；
 * 但同一轮里合并 / 入库两路池还在往 pending_ops 排 split_item（实锤：本轮 11:08:02 收尾之后
 * 又排了 450 行入队事件，任务数去重后收尾时 502 条里 397 条 pending，一条没领）。
 *
 * 这里在外面套一层，不动 consumeSplitOps 的循环结构：
 *   ① 先扫一轮（跟以前一样，和合并/入库并行）；
 *   ② 等 awaitProducers resolve ——它只在生产者全部 drain 完之后 resolve，不是定时器等；
 *      最多等 producerTimeoutMs（默认 15 分钟），超时就跳过补扫继续收尾，不让整轮挂死；
 *   ③ 再扫一轮，仍然没有新活（scanned=0）就停；最多 maxPasses 轮（默认 3，硬顶 10）。
 * 三条终止条件任意一条成立都收工，不会死等；没有 awaitProducers 时行为与 consumeSplitOps 完全一致。
 *
 * 2026-11-20 加固：本轮失败/回 pending 的 op id 由 skipOpIds 跨 pass 共享——补扫时 dueOps 会跳过它们，
 * 不在同一轮里原地重试（next_try_at=now 会让它们立刻又到期）。
 */
export async function consumeSplitOpsWithRecheck({
  awaitProducers = null,
  maxPasses = 3,
  producerTimeoutMs = PRODUCER_WAIT_TIMEOUT_MS,
  ...opts
}: SplitRecheckOpts = {}): Promise<SplitStats> {
  // 一份集合贯穿所有 pass：consumeSplitOps 往里 push，下一次调用拿它当 exclude。
  const skipOpIds: Array<string | number> = Array.isArray(opts.skipOpIds) ? opts.skipOpIds : [];
  const total = await consumeSplitOps({ ...opts, skipOpIds });
  if (typeof awaitProducers !== 'function') return total;
  const cap = Math.min(Math.max(Math.floor(Number(maxPasses) || 1), 1), 10);
  const waitMs = Math.max(20, Math.floor(Number(producerTimeoutMs) || PRODUCER_WAIT_TIMEOUT_MS));
  for (let pass = 1; pass < cap; pass += 1) {
    try {
      const settled = await waitProducerOrTimeout(awaitProducers, waitMs);
      if (!settled) {
        // 生产者收不了工就别等：补扫跳过，整轮照常收尾（下一条别处的日志能看到这行）
        opts.log?.(`[split] 轮末补扫等待生产者超时，跳过补扫（${waitMs}ms）`);
        break;
      }
    } catch (error) {
      opts.log?.(`[split] WARN 等生产者收工失败，按当前队列收尾：${String((error as Error && (error as Error).message) || error as string).slice(0, 120)}`);
      break;
    }
    const again = await consumeSplitOps({ ...opts, skipOpIds });
    mergeSplitStats(total, again);
    if (!again.scanned) break;
    opts.log?.(`[split] 轮末补扫第 ${pass} 轮：条 ${again.scanned} 落库 ${again.succeeded} 回队 ${again.pendingAfter}`);
  }
  return total;
}

export default consumeSplitOps;
