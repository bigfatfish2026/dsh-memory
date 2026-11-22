/**
 * 夜间 observation 提炼（2026-11-21，docs/observation-plan-v2.md §2/§3/§6/§8）。
 *
 * 一轮六步：领 observe_task → 取该实体的 active world/experience 证据与已有观察 →
 * 拼 prompt 问模型 → 解析 + 硬校验 → 落库（create / update）→ 提交任务。
 *
 * 开关 cfg.observe.enabled 默认 false（关着直接 skipped，一个字节都不动）；dryRun 照跑模型、
 * 照计量，但不 claim、不落库、不改状态。dryRun 不 claim 的理由：claimObserveTasks 本身会写
 * status='leased'/lease_until，设计 §6 要求「observe_task 一个字节不改」；而队列没有「归还」
 * 入口（fail 会 tries+1），所以 dryRun 只做同条件的只读 SELECT。
 *
 * 事务口径（为什么不用 withNestedTransaction）：
 *   saveObservation → commitWrite → state.writeTx → withTransaction 无条件发 BEGIN IMMEDIATE
 *   （src/store/state.ts:112-115、src/shared/tx.ts:19-21）。SQLite 不支持嵌套 BEGIN；
 *   withNestedTransaction 只有在 db.isTransaction 为真时才改开 SAVEPOINT（tx.ts:59-62），
 *   而 node:sqlite 的 DatabaseSync 没有这个标记，所以把它包在外层事务里必然抛
 *   「cannot start a transaction within a transaction」。
 * 因此落库与 complete 用**补偿方案**：
 *   ① saveObservation 先用自己的事务提交；
 *   ② completeObserveTask 带 claim 时的 version + owner 做 CAS；
 *   ③ 返回 false（模型跑期间证据变了）→ 归档刚写的那条观察（只归档 created=true 的；
 *      命中去重返回的旧观察不动），任务保持 pending（写侧 enqueue 已经把它拉回 pending、version+1）。
 * 补偿归档失败只记日志：观察初态自 2026-11-21 起是 active（证据已在 saveObservation 的写事务里
 * 校验过），这条脏结果可能在召回里多留到下一轮重算；prefill 注入缓存由观察版本号兜底，
 * 不会一直注入旧结论。
 *
 * 复核分支（2026-11-21，docs/observation-plan-v2.md §4）：写侧把观察置 pending_review 并
 * enqueue(reason='review')，本文件领到该实体后**先消费这条待复核观察**，不顺手做普通提炼：
 *   - 独立来源 < 2（或证据边全失效）→ 程序归档，reason='evidence-review'，不调模型；
 *   - 独立来源 ≥ 2 且 extra.needsModelReview !== true → 程序恢复，reason='review-ok'，不调模型；
 *   - 独立来源 ≥ 2 且 needsModelReview === true → 交给模型，只收 update（create 一律
 *     REVIEW_NO_CREATE 拒掉）；update 落库时旧观察归档，空 ops（或模型没覆盖到的）按
 *     reason='review-keep' 恢复。
 * 状态变更一律在 completeObserveTask 成功之后才写：版本冲突时任务留 pending，观察保持
 * pending_review 等下一轮重判，不会出现「结果作废但观察已被恢复」。状态行直接写库
 * （MemoryPatch 没有 extra，updateMemory 也清不掉 needsModelReview，且会 notify 实体导致
 * 自己触发自己）；每笔写库同事务 bumpObservationEpoch，与 observation-review 同口径。
 * 复核判定从库里读 pending 观察，不读 store 内存视图：本文件刚直接改过状态行，内存可能陈旧。
 */
import type { DatabaseSync } from 'node:sqlite';
import {
  buildObservationUserMessage, OBSERVATION_SYSTEM,
} from './observation-prompt.js';
import { parseObservationOps, validateObservationOps } from './observation-parse.js';
import {
  independentSourceCount, selectEntityEvidence, sourceRootsOf,
} from './observation-sources.js';
import {
  claimObserveTasks, completeObserveTask, entityEtypeOf, failObserveTask, hasObserveTaskTable,
  OBSERVE_LEASE_MS, SKIP_OBSERVE_ETYPES,
} from '../store/observe-queue.js';
import { bumpObservationEpoch, observationEvidence } from '../store/observation-records.js';
import { entityKeysOf } from '../store/observe-notify.js';
import { isActive, MEMORY_COLUMNS, rowToRecord } from '../store/rows.js';
import { dateOnly, entityList } from '../store/types.js';
import { withTransaction } from '../shared/tx.js';
import { normalizeEntity } from '../shared/entity-normalize.js';
import type { EntityEvidenceHit, EvidenceLink, SourceRoots } from './observation-sources.js';
import type { AcceptedObservationOp, ExistingObservationForValidation } from './observation-parse.js';
import type { ObservationExistingRecord } from './observation-prompt.js';
import type {
  ObservationInput, ObservationOptions, ObservationResult,
} from '../store/observation-records.js';
import type { AskObservationResult } from './model-calls.js';
import type { MemoryRecord, Row } from '../store/types.js';
import type { SleepCfg, SleepStore } from './env.js';

/** 本文件读到的 observe 配置面；缺省值按 docs §8 的灰度预算。 */
export interface ObservationCfg {
  enabled?: boolean;
  dryRun?: boolean;
  maxEntities?: number;
  maxInput?: number;
}

/** store 上本阶段要用的面（MemoryStore 全部满足）。 */
export interface ObservationPassStore extends SleepStore {
  metas: () => MemoryRecord[];
  saveObservation: (input: ObservationInput, opts?: ObservationOptions) => Promise<ObservationResult>;
}

/** 模型句柄：createModelCalls 返回的 askObservation。 */
export interface ObservationPassModel {
  askObservation: (system: string, user: string) => Promise<AskObservationResult>;
}

/** runObservationPass 的依赖；形状照 createSleepEnv 的 env 收缩。 */
export interface ObservationPassDeps {
  store: ObservationPassStore;
  cfg: SleepCfg;
  log: (line: string) => void;
  model?: ObservationPassModel;
  /** 写进 lease_owner，便于排障；缺省 'observe-pass'。 */
  owner?: string;
  /** 覆盖 db 句柄（默认 store.db()）。 */
  db?: DatabaseSync;
  /** 覆盖时钟（测试用）。 */
  now?: () => number;
  leaseMs?: number;
}

export interface ObservationPassOptions {
  dryRun?: boolean;
}

export interface ObservationPassResult {
  processed: number;
  created: number;
  updated: number;
  failed: number;
  dryRun: boolean;
  /** 版本在模型跑期间变了、结果被丢弃的任务数（任务仍是 pending，不计 tries）。 */
  versionMismatch: number;
  /** 走了复核分支的任务数（日志里对应 review=1）。 */
  review: number;
  /** 复核分支里归档 / 恢复的观察条数（程序判定与模型判定都算）。 */
  archived: number;
  restored: number;
}

export type ObservationPassOutcome = ObservationPassResult | { skipped: true };

/** 一条待处理任务（claim 与 dryRun 的只读 SELECT 同一形状）。 */
interface ObserveTask {
  entityKey: string;
  entityId: string;
  version: number;
  reason: string;
  tries: number;
}

const DEFAULT_MAX_ENTITIES = 20;
const DEFAULT_MAX_INPUT = 30;
const LAST_ERROR_MAX = 500;

function intOf(value: unknown, fallback: number, min: number): number {
  const n = Math.floor(Number(value));
  return Number.isFinite(n) && n >= min ? n : fallback;
}

function textOf(error: unknown): string {
  return String((error && (error as { message?: unknown }).message) || error).slice(0, LAST_ERROR_MAX);
}

/** 只认显式 true；缺配置/缺 observe 段一律当关（与 observe-notify.ts 的 observeEnabled 同口径）。 */
function observeCfgOf(cfg: SleepCfg): ObservationCfg {
  const raw = (cfg as { observe?: unknown }).observe;
  return raw && typeof raw === 'object' ? raw as ObservationCfg : {};
}

/**
 * dryRun 的只读候选：WHERE / ORDER 与 claimObserveTasks 完全一致，但不写状态。
 *
 * 非实体类（file/config/tool/unknown）按与 claim 相同的口径滤掉；dryRun 不写状态，
 * 所以这些脏行本轮只是不出现，等实跑那一轮才由 claim 标 done，期间仍占候选窗口。
 */
function peekObserveTasks(db: DatabaseSync, limit: number, now: number): ObserveTask[] {
  const rows = db.prepare(
    `SELECT entity_key, entity_id, version, reason, tries
       FROM observe_task
      WHERE status = 'pending' OR (status = 'leased' AND lease_until < ?)
      ORDER BY enqueued_ts ASC, entity_key ASC
      LIMIT ?`,
  ).all(now, limit) as Array<{
    entity_key?: unknown;
    entity_id?: unknown;
    version?: unknown;
    reason?: unknown;
    tries?: unknown;
  }>;
  return rows
    .filter((row) => !SKIP_OBSERVE_ETYPES.has(entityEtypeOf(db, String(row.entity_key ?? ''))))
    .map((row) => ({
      entityKey: String(row.entity_key ?? ''),
      entityId: String(row.entity_id ?? ''),
      version: Math.trunc(Number(row.version) || 0),
      reason: String(row.reason ?? ''),
      tries: Math.trunc(Number(row.tries) || 0),
    }));
}

/** entities.display；查不到退回规范化键。 */
function displayOf(db: DatabaseSync, entityId: string, entityKey: string): string {
  const id = Number(entityId);
  if (entityId && Number.isFinite(id)) {
    try {
      const row = db.prepare('SELECT display FROM entities WHERE id = ?').get(id) as
        | { display?: unknown }
        | undefined;
      const display = String(row?.display ?? '').trim();
      if (display) return display;
    } catch {
      /* entities 表不在就退回 entity_key */
    }
  }
  return entityKey;
}

/** 记忆行实体键里有没有这个实体（规范化后比较）。 */
function recordHasEntity(record: MemoryRecord, entityNorm: string): boolean {
  return entityKeysOf(entityList(record.entities)).includes(entityNorm);
}

/** 证据 id 相关的边：两个方向都取（合并产物的 merged_into 是成员 → 产物）。 */
function evidenceLinksOf(db: DatabaseSync, ids: string[]): EvidenceLink[] {
  if (!ids.length) return [];
  const marks = ids.map(() => '?').join(',');
  try {
    const rows = db.prepare(
      `SELECT src, dst, rel FROM memory_link WHERE src IN (${marks}) OR dst IN (${marks})`,
    ).all(...ids, ...ids) as Array<{ src?: unknown; dst?: unknown; rel?: unknown }>;
    return rows.map((row) => ({
      src: String(row.src ?? ''),
      dst: String(row.dst ?? ''),
      rel: String(row.rel ?? ''),
    }));
  } catch {
    // 老库没有 memory_link：来源根只看记忆行自身的 source/supersedes，不挡流程
    return [];
  }
}

/** 该实体本批证据：active world/experience，按时间倒序取 limit 条，带上来源根。 */
function evidenceFor(
  store: ObservationPassStore,
  db: DatabaseSync,
  entityNorm: string,
  limit: number,
): { selection: ReturnType<typeof selectEntityEvidence>; rootsMap: Map<string, SourceRoots> } {
  const records = store.metas();
  const first = selectEntityEvidence(records, { entityNorm, limit });
  const links = evidenceLinksOf(db, first.items.map((item) => item.id));
  const rootsMap = sourceRootsOf(records, links);
  // 第二遍带上 rootsMap：返回的 items 与 allowedEvidenceIds 是同一批（同一 limit、同一排序）
  const selection = selectEntityEvidence(records, { entityNorm, limit, rootsMap });
  return { selection, rootsMap };
}

/**
 * 该实体已有 active / pending_review 的 observation（带证据 id）。
 *
 * 直接读库、不读 store.metas()：复核分支会就地改状态行，内存视图可能还停在旧状态；
 * 模型输入里的 existing 必须反映库里此刻的真实状态（尤其是「已归档的不许再被拉回来」）。
 */
function existingObservationsOf(
  db: DatabaseSync,
  entityNorm: string,
): Array<ObservationExistingRecord & ExistingObservationForValidation> {
  const out: Array<ObservationExistingRecord & ExistingObservationForValidation> = [];
  let rows: Row[] = [];
  try {
    rows = db.prepare(
      `SELECT * FROM memories WHERE type = 'observation' AND lower(status) IN ('active', 'pending_review')`,
    ).all() as Row[];
  } catch {
    return out; // 老库没有 memories / type 列异常：当作没有已有观察，不挡流程
  }
  for (const row of rows) {
    const meta = rowToRecord(row);
    const status = String(meta.status || 'active').toLowerCase();
    if (status !== 'active' && status !== 'pending_review') continue;
    if (!recordHasEntity(meta, entityNorm)) continue;
    out.push({
      id: String(meta.id),
      text: String(meta.text || ''),
      status,
      confidence: typeof meta.confidence === 'number' ? meta.confidence : null,
      evidenceIds: observationEvidence(db, String(meta.id)),
    });
  }
  // 稳定顺序：先 active 后 pending_review，同状态按 id，prompt 两轮之间不抖
  return out.sort((a, b) => {
    const sa = String(a.status) === 'active' ? 0 : 1;
    const sb = String(b.status) === 'active' ? 0 : 1;
    return sa - sb || String(a.id).localeCompare(String(b.id));
  });
}

/** 观察行要挂的实体名：display 归一后不等于队列键时，把键也带上（证据交集校验靠它）。 */
function observationEntitiesOf(display: string, entityNorm: string): string[] {
  const name = String(display || '').trim() || entityNorm;
  return normalizeEntity(name).norm === entityNorm ? [name] : [name, entityNorm];
}

/**
 * 逐条落库（正常提炼与复核分支共用）：一条坏只记这一条，不把同批别的观察带下去。
 * 返回 created / replaces 供调用方算补偿与「模型覆盖了哪几条」。
 */
async function saveObservationOps(
  deps: ObservationPassDeps,
  entityKey: string,
  display: string,
  entityNorm: string,
  reason: string,
  ops: AcceptedObservationOp[],
  log: (line: string) => void,
): Promise<{ saved: SavedObservation[]; saveError: unknown }> {
  const saved: SavedObservation[] = [];
  let saveError: unknown = null;
  for (const op of ops) {
    try {
      // eslint-disable-next-line no-await-in-loop -- 写路径逐条落地，与 sleep 其它段同节奏
      const res = await deps.store.saveObservation({
        text: op.text,
        entities: observationEntitiesOf(display, entityNorm),
        evidenceIds: op.evidence,
        ...(typeof op.confidence === 'number' ? { confidence: op.confidence } : {}),
        ...(op.op === 'update' && op.replaces ? { replacesObservationId: op.replaces } : {}),
        reason,
        source: 'sleep:observe',
      });
      saved.push({
        id: res.id,
        created: res.created,
        op: op.op,
        ...(op.op === 'update' && op.replaces ? { replaces: op.replaces } : {}),
      });
    } catch (error) {
      saveError = error;
      log(`[observe] entity=${entityKey} op=${op.op} 落库拒绝：${textOf(error)}`);
    }
  }
  return { saved, saveError };
}

/** 复核归档 / 恢复写进 memories.reason 的稳定文案（测试与 grep 按它们判）。 */
export const REVIEW_ARCHIVE_REASON = 'evidence-review';
export const REVIEW_RESTORE_REASON = 'review-ok';
export const REVIEW_KEEP_REASON = 'review-keep';

/** 一条待复核观察：判定与写库只认这几项。 */
interface ReviewTarget {
  id: string;
  needsModelReview: boolean;
  evidenceIds: string[];
  text: string;
  confidence: number | null;
}

/** 复核计划：三种处置 + 判定用的来源根表；判定阶段只读不写。 */
interface ReviewPlan {
  archive: ReviewTarget[];
  restore: ReviewTarget[];
  needModel: ReviewTarget[];
  rootsMap: Map<string, SourceRoots>;
}

type ReviewAction = 'archive' | 'restore-ok' | 'restore-keep';

interface ReviewDecision {
  target: ReviewTarget;
  action: ReviewAction;
}

/**
 * 该实体下等待复核的自动观察（status='pending_review' 且 extra.autoObservation === true）。
 *
 * 直接查库：本文件可能刚在上一个任务里直接改过状态行，store 内存视图不进这条判定。
 * 手工 observation 没有 autoObservation 标记，一律不碰。
 */
function pendingObservationsOf(db: DatabaseSync, entityNorm: string): ReviewTarget[] {
  const out: ReviewTarget[] = [];
  let rows: Row[] = [];
  try {
    rows = db.prepare(
      `SELECT * FROM memories WHERE type = 'observation' AND lower(status) = 'pending_review'`,
    ).all() as Row[];
  } catch {
    return out; // 老库没有 memories：没有待复核观察
  }
  for (const row of rows) {
    const rec = rowToRecord(row);
    if (rec.autoObservation !== true) continue;
    if (String(rec.status || '').toLowerCase() !== 'pending_review') continue;
    if (!recordHasEntity(rec, entityNorm)) continue;
    const id = String(rec.id);
    out.push({
      id,
      needsModelReview: rec.needsModelReview === true,
      evidenceIds: observationEvidence(db, id),
      text: String(rec.text || ''),
      confidence: typeof rec.confidence === 'number' ? rec.confidence : null,
    });
  }
  return out.sort((a, b) => a.id.localeCompare(b.id));
}

/** 这批 id 里还 active 的（以库为准：store 内存视图可能还没跟上别人的归档）。 */
function activeIdsOf(db: DatabaseSync, ids: string[]): Set<string> {
  const out = new Set<string>();
  if (!ids.length) return out;
  const marks = ids.map(() => '?').join(',');
  try {
    const rows = db.prepare(`SELECT id, status FROM memories WHERE id IN (${marks})`).all(...ids) as Row[];
    for (const row of rows) {
      if (String(row.status || 'active').toLowerCase() === 'active') out.add(String(row.id));
    }
  } catch {
    /* 查不到就当这条不算活来源 */
  }
  return out;
}

/**
 * 程序判定：证据够不够、要不要模型。
 *
 *   - 证据边为空，或全部指向已不在 active 的记录 → 归档（来源不足）；
 *   - 独立来源（按来源根，uncertain 不计）< 2 → 归档；
 *   - ≥ 2 且 extra.needsModelReview !== true → 恢复；
 *   - ≥ 2 且 needsModelReview === true → 交模型改写或确认。
 *
 * 判据与 observation-review.ts 的写侧复核同一口径（来源根 + 独立来源计数）。
 */
function planReviewOf(targets: ReviewTarget[], records: MemoryRecord[], db: DatabaseSync): ReviewPlan {
  const plan: ReviewPlan = { archive: [], restore: [], needModel: [], rootsMap: new Map() };
  if (!targets.length) return plan;
  const edgeIds = [...new Set(targets.flatMap((target) => target.evidenceIds))];
  const activeIds = activeIdsOf(db, edgeIds);
  const rootsMap = sourceRootsOf(records, evidenceLinksOf(db, edgeIds));
  plan.rootsMap = rootsMap;

  for (const target of targets) {
    const edges = target.evidenceIds;
    const allInactive = edges.length > 0 && edges.every((id) => !activeIds.has(id));
    if (!edges.length || allInactive) {
      plan.archive.push(target);
      continue;
    }
    if (independentSourceCount(edges, rootsMap).count < 2) {
      plan.archive.push(target);
      continue;
    }
    if (target.needsModelReview) plan.needModel.push(target);
    else plan.restore.push(target);
  }
  return plan;
}

/** 待复核观察自己的证据边：活着的、还没进模型输入的补齐成 EntityEvidenceHit。 */
function reviewEvidenceOf(
  records: MemoryRecord[],
  rootsMap: ReadonlyMap<string, SourceRoots>,
  targets: ReviewTarget[],
  skipIds: Set<string>,
): EntityEvidenceHit[] {
  const byId = new Map<string, MemoryRecord>();
  for (const record of records) byId.set(String(record.id), record);
  const out: EntityEvidenceHit[] = [];
  for (const target of targets) {
    for (const id of target.evidenceIds) {
      if (skipIds.has(id)) continue;
      const record = byId.get(id);
      if (!record || !isActive(record)) continue;
      skipIds.add(id);
      const hit = rootsMap.get(id);
      out.push({
        id,
        record,
        roots: hit ? [...hit.roots] : [id],
        uncertain: hit ? hit.uncertain : true,
      });
    }
  }
  return out;
}

/** 观察行的动态键（extra 列内容）：除固定列外的全部顶层键。口径同 observation-review.ts。 */
function extraOf(record: MemoryRecord): Row {
  const extra: Row = {};
  for (const key of Object.keys(record)) {
    if (key === 'id' || MEMORY_COLUMNS.includes(key)) continue;
    extra[key] = record[key];
  }
  return extra;
}

/**
 * 复核结论落库：归档 / 恢复观察行，清 extra.needsModelReview，同事务 bump 观察版本号。
 *
 * 为什么直接写库而不是 store.update / archive：
 *   - MemoryPatch 没有 extra，updateMemory 清不掉 needsModelReview；
 *   - updateMemory(archived) 会 notifyEntityNamesChanged(reason='archive')，刚复核完的实体
 *     立刻又被登记回 observe_task，形成「复核 → 自己触发自己」的循环；
 *   - upsertRow 需要 StoreState，本文件只拿得到 store 与 db 句柄。
 * 每行都带 `status='pending_review'` 条件（CAS）：期间被别人改过就不覆盖。
 * 整批放在一笔 BEGIN IMMEDIATE 里；调用方必须已经 complete 成功——版本冲突时这段根本不执行。
 */
function applyReviewDecisions(
  db: DatabaseSync,
  decisions: ReviewDecision[],
  at: number,
  log: (line: string) => void,
  skipIds: Set<string> = new Set(),
): { archived: number; restored: number } {
  const out = { archived: 0, restored: 0 };
  if (!decisions.length) return out;
  const stamp = new Date(at).toISOString();
  const today = dateOnly(stamp);
  try {
    withTransaction(db, (tx) => {
      let changed = 0;
      for (const decision of decisions) {
        const id = decision.target.id;
        if (skipIds.has(id)) continue; // 已由 saveObservation(replaces) 归档，别再写一遍
        const row = tx.prepare('SELECT * FROM memories WHERE id = ?').get(id) as Row | undefined;
        if (!row) continue;
        const current = rowToRecord(row);
        if (String(current.status || 'active').toLowerCase() !== 'pending_review') continue;
        const extra = extraOf(current);
        delete extra.needsModelReview;
        const archive = decision.action === 'archive';
        const reason = archive
          ? REVIEW_ARCHIVE_REASON
          : (decision.action === 'restore-keep' ? REVIEW_KEEP_REASON : REVIEW_RESTORE_REASON);
        const validTo = archive ? today : dateOnly(current.validTo);
        const res = tx.prepare(
          `UPDATE memories SET status = ?, reason = ?, validTo = ?, updatedAt = ?, extra = ?
            WHERE id = ? AND lower(status) = 'pending_review'`,
        ).run(
          archive ? 'archived' : 'active',
          reason,
          validTo || null,
          stamp,
          Object.keys(extra).length ? JSON.stringify(extra) : null,
          id,
        );
        if (Number(res.changes) > 0) {
          changed += 1;
          if (archive) out.archived += 1;
          else out.restored += 1;
        }
      }
      if (changed > 0) bumpObservationEpoch(tx);
    });
  } catch (error) {
    log(`[observe] 复核写库失败（本轮状态未改）：${textOf(error)}`);
    return { archived: 0, restored: 0 };
  }
  return out;
}

/** 已落库的那条观察：补偿时要知道是不是本轮新建的。 */
interface SavedObservation {
  id: string;
  created: boolean;
  op: 'create' | 'update';
  /** update 的目标观察 id（复核分支判「模型覆盖了哪几条」用）。 */
  replaces?: string;
}

/**
 * 跑一轮 observation 提炼。
 *
 * @returns 关着 / 缺表 / 缺模型句柄时返回 {skipped:true}；否则逐任务处理，异常都在轮内收口。
 */
export async function runObservationPass(
  deps: ObservationPassDeps,
  opts: ObservationPassOptions = {},
): Promise<ObservationPassOutcome> {
  const cfg = observeCfgOf(deps.cfg);
  const log = deps.log;
  if (cfg.enabled !== true) return { skipped: true };
  if (!deps.model || typeof deps.model.askObservation !== 'function') {
    log('[observe] 开关开着但没拿到模型句柄，跳过');
    return { skipped: true };
  }
  const db = deps.db ?? deps.store.db();
  if (!hasObserveTaskTable(db)) {
    log('[observe] 库里没有 observe_task，跳过（先跑 scripts/apply-observe-schema.mjs）');
    return { skipped: true };
  }

  const now = deps.now ?? Date.now;
  const dryRun = opts.dryRun === true || cfg.dryRun === true;
  const limit = intOf(cfg.maxEntities, DEFAULT_MAX_ENTITIES, 1);
  const maxInput = intOf(cfg.maxInput, DEFAULT_MAX_INPUT, 1);
  const owner = String(deps.owner || 'observe-pass');
  const tasks: ObserveTask[] = dryRun
    ? peekObserveTasks(db, limit, now())
    : claimObserveTasks(db, { limit, owner, leaseMs: deps.leaseMs ?? OBSERVE_LEASE_MS, now: now(), log });

  const result: ObservationPassResult = {
    processed: 0, created: 0, updated: 0, failed: 0, dryRun, versionMismatch: 0,
    review: 0, archived: 0, restored: 0,
  };
  let totalCalls = 0;
  let totalPromptChars = 0;
  let totalCompletionChars = 0;
  const startedAll = now();
  const tag = dryRun ? '[observe][dry-run]' : '[observe]';

  for (const task of tasks) {
    result.processed += 1;
    const started = now();
    const entityKey = String(task.entityKey || '');
    const entityNorm = normalizeEntity(entityKey).norm || entityKey;
    let calls = 0;
    let promptChars = 0;
    let completionChars = 0;
    let acceptedCount = 0;
    let rejectedCount = 0;

    try {
      if (!entityNorm) {
        log(`${tag} entity=${entityKey || '(空)'} version=${task.version} 跳过：实体键为空`);
        continue;
      }

      const display = displayOf(db, task.entityId, entityKey);
      const evidence = evidenceFor(deps.store, db, entityNorm, maxInput);
      const allowed = evidence.selection.items.map((item) => item.id);

      // ── 复核分支（在正常提炼之前）：先消费该实体的 pending_review 自动观察 ──
      const pending = pendingObservationsOf(db, entityNorm);
      if (pending.length) {
        result.review += 1;
        const plan = planReviewOf(pending, deps.store.metas(), db);
        const needModel = plan.needModel;
        const archiveIds = new Set(plan.archive.map((target) => target.id));
        const restoreIds = new Set(plan.restore.map((target) => target.id));

        // 模型输入：当前实体的活证据 + 待复核观察自己的活证据（去重、带上来源根）
        const modelEvidence: EntityEvidenceHit[] = [...evidence.selection.items];
        const seenEvidence = new Set(allowed);
        modelEvidence.push(...reviewEvidenceOf(deps.store.metas(), plan.rootsMap, needModel, seenEvidence));
        const modelAllowed = modelEvidence.map((item) => item.id);
        const mergedRoots = new Map<string, SourceRoots>(evidence.rootsMap);
        for (const [id, roots] of plan.rootsMap) mergedRoots.set(id, roots);

        // 模型看到的已有观察按本轮计划显示真实去向：要归档的不出现，要恢复的显示 active
        const existing = existingObservationsOf(db, entityNorm)
          .filter((item) => !archiveIds.has(String(item.id)))
          .map((item) => (restoreIds.has(String(item.id)) ? { ...item, status: 'active' } : item));

        let accepted: AcceptedObservationOp[] = [];
        let rejectedDetail = '-';
        if (needModel.length) {
          const user = buildObservationUserMessage({
            entity: display,
            evidences: modelEvidence,
            existingObservations: existing,
            mode: 'review',
          });
          const asked = await deps.model.askObservation(OBSERVATION_SYSTEM, user);
          calls += Math.max(1, Math.trunc(Number(asked?.calls) || 0));
          promptChars += Math.trunc(Number(asked?.promptChars) || 0) || user.length + OBSERVATION_SYSTEM.length;
          completionChars += Math.trunc(Number(asked?.completionChars) || 0) || String(asked?.text || '').length;

          const parsed = parseObservationOps(asked?.text);
          if (parsed.parseError) {
            if (!dryRun) failObserveTask(db, entityKey, task.version, `parse:${parsed.parseError}`, { now: now(), owner });
            result.failed += 1;
            log(`${tag} entity=${entityKey} version=${task.version} review=1 FAIL parse=${parsed.parseError} calls=${calls} ms=${now() - started}`);
            continue;
          }

          // 复核只改待复核的那几条：update 目标限制在 needModel 集合内，create 一律拒
          const needModelIds = needModel.map((target) => target.id);
          const validated = validateObservationOps(parsed.ops, {
            allowedEvidenceIds: modelAllowed,
            entityKey: display,
            entityNorm,
            rootsOf: mergedRoots,
            existingObservationIds: needModelIds,
            existingObservations: existing.filter((item) => needModelIds.includes(String(item.id))),
          });
          let noCreate = 0;
          for (const op of validated.accepted) {
            if (op.op === 'create') {
              noCreate += 1; // 复核任务不许顺手长出新观察
              continue;
            }
            accepted.push(op);
          }
          acceptedCount = accepted.length;
          rejectedCount = validated.rejected.length + noCreate;
          const detail = validated.rejected.map((item) => `${item.op}:${item.reason}`);
          for (let i = 0; i < noCreate; i += 1) detail.push('create:REVIEW_NO_CREATE');
          rejectedDetail = detail.length ? detail.join(',') : '-';
        }

        if (dryRun) {
          // 演练：照跑模型与计量，状态一个字节不动
          log(`${tag} entity=${entityKey} version=${task.version} review=1 calls=${calls} promptChars=${promptChars} completionChars=${completionChars} ms=${now() - started} wouldArchive=${plan.archive.length} wouldRestore=${plan.restore.length} needModel=${needModel.length} accepted=${acceptedCount} rejected=${rejectedCount} rejectedDetail=${rejectedDetail}`);
          continue;
        }

        // 先落库（模型 update），再 CAS complete；失败/冲突都不碰观察状态
        const saveResult = accepted.length
          ? await saveObservationOps(deps, entityKey, display, entityNorm, `observe:review:${task.reason || 'review'}`, accepted, log)
          : { saved: [] as SavedObservation[], saveError: null as unknown };
        const saved = saveResult.saved;
        if (accepted.length && !saved.length) {
          failObserveTask(db, entityKey, task.version, saveResult.saveError ?? 'save-rejected', { now: now(), owner });
          result.failed += 1;
          log(`${tag} entity=${entityKey} version=${task.version} review=1 FAIL 落库全失败 accepted=${acceptedCount} rejected=${rejectedCount} calls=${calls} ms=${now() - started}`);
          continue;
        }

        const done = completeObserveTask(db, entityKey, task.version, { now: now(), owner });
        if (!done) {
          for (const item of saved) {
            if (!item.created) continue; // 命中去重返回的是既有观察，不能归档
            try {
              await deps.store.archive(item.id, '版本冲突作废：observation 复核结果在模型跑期间过期');
            } catch (error) {
              log(`[observe] 补偿归档失败 id=${item.id}：${textOf(error)}（初态 active，召回里可能多留到下一轮重算）`);
            }
          }
          result.versionMismatch += 1;
          log(`${tag} entity=${entityKey} version=${task.version} review=1 版本冲突：丢弃 ${saved.length} 条结果，任务留 pending`);
          continue;
        }

        // complete 成功之后才让状态变更生效；被 saveObservation 取代的旧观察已在落库事务里归档
        const coveredIds = new Set(
          saved.filter((item) => item.created && item.replaces).map((item) => String(item.replaces)),
        );
        const decisions: ReviewDecision[] = [
          ...plan.archive.map((target) => ({ target, action: 'archive' as const })),
          ...plan.restore.map((target) => ({ target, action: 'restore-ok' as const })),
          ...needModel
            .filter((target) => !coveredIds.has(target.id))
            .map((target) => ({ target, action: 'restore-keep' as const })),
        ];
        const applied = applyReviewDecisions(db, decisions, now(), log, coveredIds);
        result.archived += applied.archived;
        result.restored += applied.restored;
        for (const item of saved) {
          if (!item.created) continue;
          if (item.op === 'update') result.updated += 1;
          else result.created += 1;
        }
        log(`${tag} entity=${entityKey} version=${task.version} review=1 ms=${now() - started} calls=${calls} promptChars=${promptChars} completionChars=${completionChars} accepted=${acceptedCount} rejected=${rejectedCount} archived=${applied.archived} restored=${applied.restored} needModel=${needModel.length} saved=${saved.filter((item) => item.created).length} rejectedDetail=${rejectedDetail}`);
        continue;
      }

      // 没有 active 证据：不花模型调用，正常收尾。证据回来时写侧会重新 enqueue。
      if (!allowed.length) {
        if (!dryRun) {
          const done = completeObserveTask(db, entityKey, task.version, { now: now(), owner });
          if (!done) result.versionMismatch += 1;
        }
        log(`${tag} entity=${entityKey} version=${task.version} 证据=0 calls=0 ms=${now() - started} 无 active 证据，跳过`);
        continue;
      }

      const existing = existingObservationsOf(db, entityNorm);

      const user = buildObservationUserMessage({
        entity: display,
        evidences: evidence.selection.items,
        existingObservations: existing,
      });
      const asked = await deps.model.askObservation(OBSERVATION_SYSTEM, user);
      calls += Math.max(1, Math.trunc(Number(asked?.calls) || 0));
      promptChars += Math.trunc(Number(asked?.promptChars) || 0) || user.length + OBSERVATION_SYSTEM.length;
      completionChars += Math.trunc(Number(asked?.completionChars) || 0) || String(asked?.text || '').length;

      const parsed = parseObservationOps(asked?.text);
      if (parsed.parseError) {
        // 空响应 / 坏 JSON 都算一次尝试（AGENTS：不许当 noop 静默吞掉）
        if (!dryRun) failObserveTask(db, entityKey, task.version, `parse:${parsed.parseError}`, { now: now(), owner });
        result.failed += 1;
        log(`${tag} entity=${entityKey} version=${task.version} FAIL parse=${parsed.parseError} calls=${calls} promptChars=${promptChars} completionChars=${completionChars} ms=${now() - started}`);
        continue;
      }

      const validated = validateObservationOps(parsed.ops, {
        allowedEvidenceIds: allowed,
        entityKey: display,
        entityNorm,
        rootsOf: evidence.rootsMap,
        existingObservationIds: existing.map((item) => item.id),
        existingObservations: existing,
      });
      acceptedCount = validated.accepted.length;
      rejectedCount = validated.rejected.length;
      const rejectedDetail = validated.rejected.length
        ? validated.rejected.map((item) => `${item.op}:${item.reason}`).join(',')
        : '-';

      if (dryRun) {
        // 照跑模型、照计量；created/updated 是「本应落库」的条数，库与队列一个字节没动
        for (const op of validated.accepted) {
          if (op.op === 'update') result.updated += 1;
          else result.created += 1;
        }
        log(`${tag} entity=${entityKey} version=${task.version} ms=${now() - started} calls=${calls} promptChars=${promptChars} completionChars=${completionChars} accepted=${acceptedCount} rejected=${rejectedCount} evidence=${allowed.length} existing=${existing.length} rejectedDetail=${rejectedDetail}`);
        continue;
      }

      // 落库：逐条 try/catch，一条坏只记这一条，不把同批别的观察带下去
      const { saved, saveError } = await saveObservationOps(
        deps, entityKey, display, entityNorm, `observe:${task.reason || 'refresh'}`, validated.accepted, log,
      );

      if (!saved.length && validated.accepted.length) {
        failObserveTask(db, entityKey, task.version, saveError ?? 'save-rejected', { now: now(), owner });
        result.failed += 1;
        log(`${tag} entity=${entityKey} version=${task.version} FAIL 落库全失败 accepted=${acceptedCount} rejected=${rejectedCount} calls=${calls} ms=${now() - started}`);
        continue;
      }

      // 提交：版本在模型跑/落库期间被写侧 enqueue 顶掉 → 不认这份结果，任务留 pending
      const done = completeObserveTask(db, entityKey, task.version, { now: now(), owner });
      if (!done) {
        for (const item of saved) {
          if (!item.created) continue; // 命中去重返回的是既有观察，不能归档
          try {
            await deps.store.archive(item.id, '版本冲突作废：observation 结果在模型跑期间过期');
          } catch (error) {
            log(`[observe] 补偿归档失败 id=${item.id}：${textOf(error)}（初态 active，召回里可能多留到下一轮重算）`);
          }
        }
        result.versionMismatch += 1;
        log(`${tag} entity=${entityKey} version=${task.version} 版本冲突：丢弃 ${saved.length} 条结果，任务留 pending`);
        continue;
      }

      for (const item of saved) {
        if (!item.created) continue; // 判重命中：没有新行
        if (item.op === 'update') result.updated += 1;
        else result.created += 1;
      }
      log(`${tag} entity=${entityKey} version=${task.version} ms=${now() - started} calls=${calls} promptChars=${promptChars} completionChars=${completionChars} accepted=${acceptedCount} rejected=${rejectedCount} saved=${saved.filter((item) => item.created).length} rejectedDetail=${rejectedDetail}`);
    } catch (error) {
      // 模型/embedding/意外异常：记 trys 账，不让一条坏任务带崩整轮整理
      if (!dryRun) {
        try {
          failObserveTask(db, entityKey, task.version, error, { now: now(), owner });
        } catch {
          /* fail 也失败时只剩这一条日志，任务由租约过期兜底 */
        }
      }
      result.failed += 1;
      log(`${tag} entity=${entityKey} version=${task.version} FAIL ${textOf(error)} calls=${calls} ms=${now() - started}`);
    } finally {
      totalCalls += calls;
      totalPromptChars += promptChars;
      totalCompletionChars += completionChars;
    }
  }

  // 汇总固定字段：调用数 / 字符数 / 耗时单独计量（docs §8），不混进 sleep 预算
  log(
    `${tag} 汇总 processed=${result.processed} created=${result.created} updated=${result.updated} failed=${result.failed}` +
      ` review=${result.review} archived=${result.archived} restored=${result.restored}` +
      ` versionMismatch=${result.versionMismatch} calls=${totalCalls} promptChars=${totalPromptChars}` +
      ` completionChars=${totalCompletionChars} ms=${now() - startedAll}`,
  );
  return result;
}

export default runObservationPass;
