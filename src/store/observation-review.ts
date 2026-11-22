/**
 * 证据变更时的 observation 就地复核（2026-11-21，docs/observation-plan-v2.md §4）。
 *
 * 触发方：records 的 archive / update（正文改写）/ merge，以及 entity-graph 的独立解挂。
 * 全部在调用方**已有事务**里执行：本文件不开 BEGIN / SAVEPOINT，写失败由外层回滚。
 *
 * 判据：
 *   - 只处理 extra.autoObservation === true 的观察，手工 observation 一行不碰；
 *   - 按 kind 处理证据边：archive 删边；merge 有合法后继就改 dst，否则删边；
 *     rewrite / unmount 边保留但整条观察记 needsModelReview=true；
 *   - 用 sourceRootsOf + independentSourceCount 按「来源根」重算独立来源（同一原条的多片只算一条）；
 *   - 来源不足 2 条，或正文变更后证据全失效 → archived + reason='evidence-<kind>'；
 *     否则 needsModelReview 或证据边有变化 → pending_review + reason='evidence-<kind>'。
 *
 * 防循环：观察自身被归档时**不登记它的实体**。否则 observe_task 会把同一个实体再拉回
 * 下一轮，生成→归档→再生成。只有转 pending_review 的观察才 enqueue(reason='review')。
 *
 * 常量 EVIDENCE_REL / EPOCH_KEY 在这里重复一份，是为了让本文件不导入 observation-records：
 * observation-records → records，而 records / entity-graph 要导入本文件，直接引常量会形成
 * 循环依赖。两处字符串与 observation-records.ts:30、:38 保持一致。
 */
import type { DatabaseSync } from 'node:sqlite';
import { independentSourceCount, sourceRootsOf } from '../sleep/observation-sources.js';
import type { EvidenceLink } from '../sleep/observation-sources.js';
import { hasMemoryLinkTable } from '../memory-link.js';
import { enqueueObserveTask } from './observe-queue.js';
import { entityKeysOf } from './observe-notify.js';
import { isActive, MEMORY_COLUMNS, rowToRecord } from './rows.js';
import { entityList } from './types.js';
import type { MemoryRecord, Row } from './types.js';

/** 与 observation-records.ts:30 一致：观察 → 证据。 */
const EVIDENCE_REL = 'derived_from';
/** 与 observation-records.ts:38 一致：meta 表每库一份的观察版本号。 */
const EPOCH_KEY = 'observation_epoch';

/** 证据变更的四类来源。 */
export type EvidenceChangeKind = 'archive' | 'merge' | 'rewrite' | 'unmount';

export interface EvidenceChange {
  kind: EvidenceChangeKind;
  /** 本次变动的证据 id（归档/改写/解挂的是它自己；合并是全部源条）。 */
  evidenceIds: unknown;
  /** merge 专用：证据 id → 后继 id。 */
  successorOf?: Record<string, string>;
}

export interface ReviewOptions {
  /** observe 配置面；给了且 observe.enabled 不是 true 时只改状态、不登记队列。 */
  cfg?: { observe?: { enabled?: boolean; [key: string]: unknown } | null; [key: string]: unknown } | null;
  /** 本批统一跳过 observe_task 登记（调用方明确不要通知时用）。 */
  skipNotify?: boolean;
  /** 覆盖时间（测试用）。 */
  now?: number;
  /** 出错日志出口；不传就静默（复核失败不许挡主写路径）。 */
  log?: (line: string) => void;
}

export interface ReviewResult {
  /** 命中的自动观察条数（含状态没变的）。 */
  reviewed: number;
  /** 本次转 archived 的条数。 */
  archived: number;
  /** 本次转 pending_review 的条数。 */
  pending: number;
  /** 状态变了的观察行，调用方拿去同步内存视图（commitWrite 的 nextMetas 在事务前已求值）。 */
  updated: MemoryRecord[];
}

const EMPTY: ReviewResult = { reviewed: 0, archived: 0, pending: 0, updated: [] };

function placeholders(n: number): string {
  return new Array(n).fill('?').join(',');
}

function idsOf(value: unknown): string[] {
  return [...new Set(entityList(value).map(String).filter(Boolean))];
}

/** 按 id 读整行（rowToRecord 会把 extra 摊平回顶层，autoObservation 直接可读）。 */
function recordsById(db: DatabaseSync, ids: string[]): Map<string, MemoryRecord> {
  const out = new Map<string, MemoryRecord>();
  if (!ids.length) return out;
  const rows = db.prepare(
    `SELECT * FROM memories WHERE id IN (${placeholders(ids.length)})`,
  ).all(...ids) as Row[];
  for (const row of rows) {
    const rec = rowToRecord(row);
    out.set(String(rec.id), rec);
  }
  return out;
}

/** 一条观察的全部证据边（dst 列表，按建边顺序）。 */
function evidenceEdges(db: DatabaseSync, observationId: string): string[] {
  const rows = db.prepare(
    `SELECT dst FROM memory_link WHERE src = ? AND rel = ? ORDER BY created_ts, dst`,
  ).all(String(observationId), EVIDENCE_REL) as Array<{ dst?: unknown }>;
  return rows.map((row) => String(row.dst)).filter(Boolean);
}

/** 和这批证据有关的全部边：来源根要把拆分/合并血缘一起看到，不能只看 derived_from。 */
function linksTouching(db: DatabaseSync, ids: string[]): EvidenceLink[] {
  if (!ids.length) return [];
  const marks = placeholders(ids.length);
  return db.prepare(
    `SELECT src, dst, rel FROM memory_link WHERE src IN (${marks}) OR dst IN (${marks})`,
  ).all(...ids, ...ids) as unknown as EvidenceLink[];
}

function deleteEdge(db: DatabaseSync, observationId: string, evidenceId: string): number {
  const res = db.prepare(
    `DELETE FROM memory_link WHERE src = ? AND dst = ? AND rel = ?`,
  ).run(String(observationId), String(evidenceId), EVIDENCE_REL);
  return Number(res.changes) || 0;
}

function moveEdge(db: DatabaseSync, observationId: string, from: string, to: string): number {
  const res = db.prepare(
    `UPDATE memory_link SET dst = ? WHERE src = ? AND dst = ? AND rel = ?`,
  ).run(String(to), String(observationId), String(from), EVIDENCE_REL);
  return Number(res.changes) || 0;
}

/** 观察版本号 +1；meta 缺表/写失败只吞掉（与 observation-records 的 bump 同口径）。 */
function bumpEpoch(db: DatabaseSync): void {
  try {
    const row = db.prepare('SELECT value FROM meta WHERE key = ?').get(EPOCH_KEY) as
      | { value?: unknown }
      | undefined;
    const next = (Number.isFinite(Number(row?.value)) ? Math.trunc(Number(row?.value)) : 0) + 1;
    db.prepare(
      `INSERT INTO meta(key, value) VALUES (?, ?)
       ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
    ).run(EPOCH_KEY, String(next));
  } catch {
    // 没有 meta 表的老库：状态照样改，版本号下次启动从 0 起算
  }
}

/** 观察行的动态键（extra 列内容）：除固定列外的全部顶层键。 */
function extraOf(record: MemoryRecord): Row {
  const extra: Row = {};
  for (const key of Object.keys(record)) {
    if (key === 'id' || MEMORY_COLUMNS.includes(key)) continue;
    extra[key] = record[key];
  }
  return extra;
}

/**
 * 只改复核会动的四列：status / reason / updatedAt / extra。
 * 不用 upsertRow 是因为本函数只拿得到 db（entity-graph 的解挂路径没有 StoreState），
 * 其余列保持原样，`present` 也不改。
 */
function writeReviewRow(db: DatabaseSync, record: MemoryRecord): void {
  const extra = extraOf(record);
  db.prepare(
    `UPDATE memories SET status = ?, reason = ?, updatedAt = ?, extra = ? WHERE id = ?`,
  ).run(
    String(record.status || 'active'),
    String(record.reason || ''),
    String(record.updatedAt || ''),
    Object.keys(extra).length ? JSON.stringify(extra) : null,
    String(record.id),
  );
}

/** 观察的实体登记到 observe_task；开关关着或表不在时由 enqueueObserveTask 静默跳过。 */
function enqueueReview(db: DatabaseSync, record: MemoryRecord, opts: ReviewOptions): void {
  if (opts.skipNotify) return;
  if (opts.cfg && opts.cfg.observe?.enabled !== true) return;
  for (const key of entityKeysOf(entityList(record.entities))) {
    let entityId = '';
    try {
      const row = db.prepare('SELECT id FROM entities WHERE norm = ?').get(key) as { id?: unknown } | undefined;
      if (row) entityId = String(row.id);
    } catch {
      entityId = '';
    }
    enqueueObserveTask(db, key, {
      entityId,
      reason: 'review',
      now: opts.now,
      log: opts.log,
    });
  }
}

/** 合并后继能不能接这条边：存在、active、且与观察有实体交集（键口径同证据校验）。 */
function successorUsable(
  observation: MemoryRecord,
  successorId: string,
  records: Map<string, MemoryRecord>,
): boolean {
  const successor = records.get(successorId);
  if (!successor || !isActive(successor)) return false;
  const observationKeys = new Set(entityKeysOf(entityList(observation.entities)));
  const successorKeys = entityKeysOf(entityList(successor.entities));
  return successorKeys.some((key) => observationKeys.has(key));
}

/**
 * 证据被归档 / 合并 / 改写正文 / 解除挂载后，就地复核引用它的自动观察。
 *
 * 调用方必须已经在写事务里；本函数只发单条语句，不自己开事务。
 * 任何异常都吞掉并返回空结果：复核是派生的，不能把证据本身的写路径带崩。
 */
export function reviewObservationsOnEvidenceChange(
  db: DatabaseSync | null | undefined,
  change: EvidenceChange,
  opts: ReviewOptions = {},
): ReviewResult {
  try {
    if (!db || !hasMemoryLinkTable(db)) return EMPTY;
    const kind = change?.kind;
    if (!['archive', 'merge', 'rewrite', 'unmount'].includes(String(kind))) return EMPTY;
    const evidenceIds = idsOf(change.evidenceIds);
    if (!evidenceIds.length) return EMPTY;

    // a. 反查引用这些证据的观察 id（derived_from 的方向是 观察 → 证据）
    const rows = db.prepare(
      `SELECT DISTINCT src FROM memory_link WHERE rel = ? AND dst IN (${placeholders(evidenceIds.length)})`,
    ).all(EVIDENCE_REL, ...evidenceIds) as Array<{ src?: unknown }>;
    const observationIds = [...new Set(rows.map((row) => String(row.src)).filter(Boolean))];
    if (!observationIds.length) return EMPTY;

    const observationRecords = recordsById(db, observationIds);
    const result: ReviewResult = { reviewed: 0, archived: 0, pending: 0, updated: [] };
    const nowIso = new Date(Number.isFinite(Number(opts.now)) ? Number(opts.now) : Date.now()).toISOString();
    const evidenceSet = new Set(evidenceIds);

    for (const observationId of observationIds) {
      const observation = observationRecords.get(observationId);
      // b. 只碰自动观察；手工 observation 没有 autoObservation 标记，直接跳过
      if (!observation || observation.autoObservation !== true) continue;
      result.reviewed += 1;

      let edges = evidenceEdges(db, observationId);
      let edgesChanged = false;
      let needsReview = observation.needsModelReview === true;

      if (kind === 'archive') {
        // c-archive：证据失效，边直接删（memory_link 是硬删，没有软删列）
        for (const evidenceId of [...edges]) {
          if (!evidenceSet.has(evidenceId)) continue;
          if (deleteEdge(db, observationId, evidenceId) > 0) {
            edges = edges.filter((id) => id !== evidenceId);
            edgesChanged = true;
          }
        }
      } else if (kind === 'merge') {
        // c-merge：能接就改指后继，接不了就删边（后继必须 active 且实体相交）
        const successorOf = change.successorOf || {};
        const successorIds = evidenceIds
          .map((id) => String(successorOf[id] || '').trim())
          .filter(Boolean);
        const successorRecords = recordsById(db, successorIds);
        for (const evidenceId of [...edges]) {
          if (!evidenceSet.has(evidenceId)) continue;
          const successorId = String(successorOf[evidenceId] || '').trim();
          const usable = successorId !== observationId
            && successorId !== evidenceId
            && !edges.includes(successorId)
            && successorUsable(observation, successorId, successorRecords);
          if (usable && moveEdge(db, observationId, evidenceId, successorId) > 0) {
            edges = edges.map((id) => (id === evidenceId ? successorId : id));
          } else {
            deleteEdge(db, observationId, evidenceId);
            edges = edges.filter((id) => id !== evidenceId);
          }
          edgesChanged = true;
        }
      } else {
        // c-rewrite / c-unmount：边保留，正文/挂载变了，结论可能不再成立，交给下一轮模型复核
        needsReview = true;
      }

      // d. 用处理后的边重算独立来源（来源根口径，不是条数）
      const edgeRecords = recordsById(db, edges);
      const links = linksTouching(db, edges);
      const rootsMap = sourceRootsOf(edgeRecords.values(), links);
      const count = independentSourceCount(edges, rootsMap);
      const allInvalid = edges.length === 0
        || edges.every((id) => {
          const rec = edgeRecords.get(id);
          return !rec || !isActive(rec);
        });

      let target = '';
      if (count.count < 2 || (needsReview && allInvalid)) target = 'archived';
      else if (needsReview || edgesChanged) target = 'pending_review';
      if (!target) continue;

      const reason = `evidence-${kind}`;
      const currentStatus = String(observation.status || 'active').toLowerCase();
      const currentReason = String(observation.reason || '');
      const flagChanged = needsReview && observation.needsModelReview !== true;
      if (target === currentStatus && reason === currentReason && !flagChanged) continue;

      const next: MemoryRecord = {
        ...observation,
        status: target,
        reason,
        updatedAt: nowIso,
      };
      if (needsReview) next.needsModelReview = true;
      writeReviewRow(db, next);

      // e. 状态变化：版本号 +1；只有转 pending_review 的观察才登记 review 任务
      bumpEpoch(db);
      result.updated.push(next);
      if (target === 'archived') {
        result.archived += 1;
        // f. 防循环：观察自己被归档时不再登记它的实体，否则下一轮同一个实体又被拉起来
        continue;
      }
      result.pending += 1;
      enqueueReview(db, next, opts);
    }

    return result;
  } catch (error) {
    opts.log?.(`[observe] 证据复核失败（主写路径照常）：${String((error && (error as { message?: unknown }).message) || error)}`);
    return EMPTY;
  }
}
