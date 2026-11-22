/**
 * 拆分组的中间态：组标记、组完整性、整组激活/归档（2026-11-22）。
 *
 * 背景：拆分子段是一段一笔事务写进 memories 的，写到第 2 段失败时第 1 段已经落库。
 * 以前子段落库就是 active，partial_write / 覆盖未过 / fetch failed 之后那半截残句能被
 * 默认召回，原条也还是 active（实锤：11-22 05:00 那轮 309 个活跃子段里 33 个来自
 * 部分写入或覆盖失败的任务）。本模块把「组」变成落库事实：
 *
 *   - 每段落库时（同一笔 save 事务）带 status=PENDING_SPLIT_STATUS，extra 里带
 *     splitGroup={source, expected, index}；落库那一刻就不可召回，不需要第二笔补丁；
 *   - 组键就是 memories.source（sleep:split:<parent> / sleep:split:queue:<key>），
 *     expected 落在每段行上，所以重启、回队、重试都能以库为准重新数；
 *   - 整组覆盖全部 expected 位后，activateSplitGroup 用**一笔**事务把整组置回 active；
 *   - 判定失败/重试耗尽时 archiveSplitGroup 用一笔事务把半成品归档成
 *     status='archived'、reason='partial-split'，组键（source）与 extra.splitGroup 都保留，
 *     清理脚本可二次识别。
 *
 * 为什么不删行：写入时已经付过 embedding、粗实体、观察登记的账，删行会在 vec_map /
 * 实体队列 / 观察边上留孤儿，而且事后无从追查。归档行不进默认召回（memories 状态机
 * 本来就只看 active），同时保留审计线索。
 */
import type { MemoryRecord, StoreState } from './types.js';

/** 子段已落库、但整组还没判完的中间态；默认召回与 memory_entity 都不许看见。 */
export const PENDING_SPLIT_STATUS = 'pending_split';
/** 半成品归档的原因：与 archived 组合成「这组没拆完就停下了」。 */
export const PARTIAL_SPLIT_REASON = 'partial-split';

/** 段行上带的组标记（落进 extra 列，rowToRecord 会平铺回记录顶层）。 */
export interface SplitGroupMark {
  /** 组键 = partSourceOf() 的产物（sleep:split:<parent> / sleep:split:queue:<key>）。 */
  source: string;
  /** 这一组一共应该有几十段（answer.parts.length）。 */
  expected: number;
  /** 本段在 answer.parts 里的下标。 */
  index: number;
}

/** 组里的一行（从库或内存记录收窄出来）。 */
export interface SplitGroupRow {
  id: string;
  text: string;
  status: string;
  /** 段下标；老数据与假 store 可能没有。 */
  index: number | null;
  /** 行上记的 expected。 */
  expected: number | null;
}

/** 一组的库内现状：以库为准判定时只看这份。 */
export interface SplitGroupView {
  source: string;
  /** 行上记的 expected（多行取最大；没有就 0）。 */
  expected: number;
  rows: SplitGroupRow[];
  /** 行上带的下标（去重、升序）。 */
  indexes: number[];
  pending: number;
  active: number;
}

/** 组是否拆完：complete=false 时 missing 给出还缺哪些下标。 */
export interface SplitGroupGap {
  complete: boolean;
  expected: number;
  have: number;
  missing: number[];
}

/**
 * pipeline-split / split-pass 只需要这几个面；MemoryStore 全有，测试里的假 store
 * 可以只实现 splitGroupOf（组统计）与 activate/archive（状态收尾）。
 */
export interface SplitGroupStore {
  metas?: () => MemoryRecord[];
  splitGroupOf?: (source: string) => SplitGroupView;
  activateSplitGroup?: (ids: string[]) => number | Promise<number>;
  archiveSplitGroup?: (ids: string[], reason?: string) => number | Promise<number>;
}

/** 中间态判定：默认召回在打分之前硬剔除（与 isHiddenObservation 同一个位置）。 */
export function isPendingSplit(m: { status?: unknown } | null | undefined): boolean {
  return String(m?.status || '').toLowerCase() === PENDING_SPLIT_STATUS;
}

/** 从记录顶层读组标记（rowToRecord 把 extra 平铺；也认 __splitGroup 这个内部名）。 */
function markOf(m: MemoryRecord): SplitGroupMark | null {
  const raw = (m as Record<string, unknown>).splitGroup ?? (m as Record<string, unknown>).__splitGroup;
  if (!raw || typeof raw !== 'object') return null;
  const mark = raw as Record<string, unknown>;
  const index = Number(mark.index);
  const expected = Number(mark.expected);
  return {
    source: String(mark.source || ''),
    expected: Number.isFinite(expected) ? expected : 0,
    index: Number.isInteger(index) ? index : -1,
  };
}

/**
 * 把一批记录收成组视图（库查询与假 store 的 metas 回退共用这一份）。
 *
 * 组键口径：行上带 splitGroup 的按 mark.source 归组（split-pass 的源列是固定的 'split'，
 * 组键只能放 extra）；没有标记的老数据退回 memories.source 比对（pipeline-split 的组键
 * 就是 source 列，历史行也是这么落的）。
 */
export function splitGroupFromRecords(records: MemoryRecord[], sourceTag: string): SplitGroupView {
  const source = String(sourceTag || '').trim();
  const view: SplitGroupView = { source, expected: 0, rows: [], indexes: [], pending: 0, active: 0 };
  if (!source) return view;
  for (const m of Array.isArray(records) ? records : []) {
    if (!m) continue;
    const mark = markOf(m);
    const groupKey = mark && mark.source ? mark.source : String(m.source || '');
    if (groupKey !== source) continue;
    const status = String(m.status || 'active').toLowerCase();
    // 归档/合并掉的行不再属于「还在等激活」的组：已经收过尾，别再算进来
    if (status !== 'active' && status !== PENDING_SPLIT_STATUS) continue;
    const index = mark && mark.index >= 0 ? mark.index : null;
    const expected = mark && mark.expected > 0 ? mark.expected : null;
    view.rows.push({ id: String(m.id), text: String(m.text ?? ''), status, index, expected });
    if (index !== null && !view.indexes.includes(index)) view.indexes.push(index);
    if (expected !== null && expected > view.expected) view.expected = expected;
    if (status === PENDING_SPLIT_STATUS) view.pending += 1;
    else view.active += 1;
  }
  view.indexes.sort((a, b) => a - b);
  return view;
}

/** 组视图（以库为准）：直接按 memories.source 查，不信调用方这次拿到的内存结果。 */
export function splitGroupOf(state: StoreState, sourceTag: string): SplitGroupView {
  const source = String(sourceTag || '').trim();
  if (!source) return splitGroupFromRecords([], '');
  const d = state.connect();
  const rows = d.prepare(
    'SELECT id, text, status, source, extra, rowSeq FROM memories WHERE source = ? ORDER BY rowSeq, id',
  ).all(source) as Array<Record<string, unknown>>;
  const records: MemoryRecord[] = [];
  for (const row of rows) {
    const rec: MemoryRecord = { id: String(row.id), text: String(row.text ?? ''), status: String(row.status ?? 'active'), source: String(row.source ?? '') };
    const extra = row.extra ? parseExtra(row.extra) : null;
    if (extra) for (const key of Object.keys(extra)) if (!(key in rec)) rec[key] = extra[key];
    records.push(rec);
  }
  return splitGroupFromRecords(records, source);
}

function parseExtra(raw: unknown): Record<string, unknown> | null {
  if (!raw || typeof raw !== 'string') return null;
  try {
    const out = JSON.parse(raw);
    return out && typeof out === 'object' ? out as Record<string, unknown> : null;
  } catch {
    return null;
  }
}

/**
 * 组完整性：expected 位里，库里已落库的下标 + 本次判重承接的下标 = 全部，才算拆完。
 *
 * 判重丢弃的段没有新行（写入闸判 DUPLICATE，正文在库里有承接），重试时还会再判一次
 * DUPLICATE，所以调用方把「本次判重段的下标」当已覆盖传进来。
 *
 * 老数据（本票之前落的子段）行上没有 splitGroup.index：按行数兜底，能判「够不够」但
 * 指不出缺哪段——这类组本来就是清理脚本的扫描对象。
 */
export function splitGroupGap(view: SplitGroupView, expected: number, covered: number[] = []): SplitGroupGap {
  const want = Math.max(0, Math.floor(Number(expected) || 0), Math.floor(Number(view?.expected) || 0));
  if (want <= 0) return { complete: true, expected: 0, have: 0, missing: [] };
  const seen = new Set<number>();
  for (const index of view?.indexes || []) {
    const n = Number(index);
    if (Number.isInteger(n) && n >= 0 && n < want) seen.add(n);
  }
  for (const index of Array.isArray(covered) ? covered : []) {
    const n = Number(index);
    if (Number.isInteger(n) && n >= 0 && n < want) seen.add(n);
  }
  const rowCount = (view?.rows || []).length + (Array.isArray(covered) ? covered.length : 0);
  const hasIndexes = (view?.indexes || []).length > 0;
  if (!hasIndexes) {
    // 老数据：只按行数兜底，不虚构下标
    const complete = rowCount >= want;
    return { complete, expected: want, have: Math.min(rowCount, want), missing: [] };
  }
  const missing: number[] = [];
  for (let i = 0; i < want; i += 1) if (!seen.has(i)) missing.push(i);
  return { complete: missing.length === 0, expected: want, have: seen.size, missing };
}

/** 组里还没收尾（pending_split）的行 id。 */
export function pendingSplitIds(view: SplitGroupView): string[] {
  return (view?.rows || []).filter((r) => r.status === PENDING_SPLIT_STATUS).map((r) => r.id);
}

/**
 * 一笔事务里改整组状态。只动 pending_split 行：
 * 激活不会把父条或别的组带进来，归档也不会覆盖已经 active 的段。
 */
function setGroupStatus(state: StoreState, ids: string[], status: string, reason: string | null, label: string): number {
  const list = [...new Set((Array.isArray(ids) ? ids : []).map((id) => String(id || '').trim()).filter(Boolean))];
  if (!list.length) return 0;
  const now = new Date().toISOString();
  const changed = state.writeTx(label, (db) => {
    const stmt = db.prepare('UPDATE memories SET status = ?, reason = ?, updatedAt = ? WHERE id = ? AND status = ?');
    let n = 0;
    for (const id of list) n += Number(stmt.run(status, reason, now, id, PENDING_SPLIT_STATUS).changes) || 0;
    return n;
  });
  // 内存记录跟库同一口径地改（快照/召回都读 state.metas，不能只在库里改）
  for (const m of state.metas) {
    if (!list.includes(m.id)) continue;
    if (String(m.status || '').toLowerCase() !== PENDING_SPLIT_STATUS) continue;
    m.status = status;
    m.updatedAt = now;
    if (reason) m.reason = reason;
    else delete m.reason;
  }
  return changed;
}

/** 整组转 active（一笔事务）。返回真正改了几行。 */
export function activateSplitGroup(state: StoreState, ids: string[]): number {
  return setGroupStatus(state, ids, 'active', null, 'split-group:activate');
}

/** 半成品整组归档（一笔事务），reason 固定 partial-split，组键与 splitGroup 标记保留。 */
export function archiveSplitGroup(state: StoreState, ids: string[], reason = PARTIAL_SPLIT_REASON): number {
  return setGroupStatus(state, ids, 'archived', String(reason || PARTIAL_SPLIT_REASON), 'split-group:archive');
}
