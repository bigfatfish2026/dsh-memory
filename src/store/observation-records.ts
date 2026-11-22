/**
 * observation 的专用写路径（2026-11-21，docs/observation-plan-v2.md §2.2 / §2.3）。
 *
 * 为什么不复用 saveMemory：通用写入闸会把新条和旧条一起裁掉——
 *   - DUPLICATE：整条丢弃，返回 discarded（saveMemory → assertWriteAllowed，gate.ts:207-209）；
 *   - SUPERSEDES：把库里相近的旧条归档（records.ts:199-200、gate.ts:269-281）。
 * observation 是派生视图：正文相近不代表同一条事实，旧条是证据，绝不能因为写观察被归档。
 * 所以这里只借 saveMemory 的落库收尾（buildNewRecord / commitWrite / writeNewRecordTx 那套副作用），
 * 不调 assertWriteAllowed，也不造 supersedes 关系。
 *
 * 一笔事务里完成：证据校验 → 写观察行（rowSeq / 向量 / 粗挂载）→ derived_from 边 →
 * 旧 observation 归档 → （可选）observe_task 登记。任何一步抛错整笔回滚，调用方拿不到半条数据。
 *
 * status 默认 'active'：证据校验（存在 / active / 实体相交）就在本笔写事务里跑过，
 * 落库那一刻观察已经「有据可查」，没有理由再把它挡在召回外。'pending_review' 从 2026-11-21 起
 * 只表示「证据后来变化、等待复核」，由复核链路写；默认召回剔除这类自动观察（isHiddenObservation）。
 */
import type { DatabaseSync } from 'node:sqlite';
import { buildNewRecord, commitWrite } from './records.js';
import { assertTextLint } from './gate.js';
import { writeCoarseEntities } from './entities.js';
import { allocRowSeq, existingRowSeq, isActive, upsertRow } from './rows.js';
import { hasMemoryLinkTable, OBSERVATION_PENDING_REVIEW_STATUS } from '../memory-link.js';
import { entityKeysOf, notifyEntityNamesChanged } from './observe-notify.js';
import { normalizeEntity } from '../shared/entity-normalize.js';
import { dateOnly, entityList } from './types.js';
import type { CoarseHit, LintReview, MemoryRecord, StoreState } from './types.js';

/** 本路径写入的唯一 rel；证据边方向固定为「观察 → 证据」。 */
export const EVIDENCE_REL = 'derived_from';
/** 证据边来源标记；与整理轮其它边区分，排障时可直接按 origin 过滤。 */
export const EVIDENCE_ORIGIN = 'sleep';
/** extra 里的自动标记：{ autoObservation: true, version: 1 }，字段名按票面固定。 */
export const AUTO_OBSERVATION_VERSION = 1;
/** 观察默认初态：生成时证据已在本笔事务内校验过，直接进召回。 */
export const OBSERVATION_INITIAL_STATUS = 'active';
/** meta 表里每库一份的观察版本号：观察写入时 +1，注入缓存按它失效。 */
export const OBSERVATION_EPOCH_KEY = 'observation_epoch';

/** 写入入参：type 固定 observation，不接受调用方改。 */
export interface ObservationInput {
  text: string;
  entities: unknown;
  evidenceIds: unknown;
  confidence?: number;
  /** 被这条新观察取代的旧 observation id；旧条在这笔事务里归档。 */
  replacesObservationId?: string;
  /** 为什么生成（排障/复核用），落 memories.reason。 */
  reason?: string;
  /** 来源前缀，默认 'sleep:observe'；调用方只允许补后缀。 */
  source?: string;
}

/** 可选项。 */
export interface ObservationOptions {
  tags?: string[];
  /** 初态覆盖；默认 active（证据在写事务里已校验），复核链路要重开待复核时显式传 pending_review。 */
  status?: string;
  /** 正文 lint 复核裁决（与 saveMemory 的 __lintReview 同口径）。 */
  lintReview?: LintReview | null;
  /** 已由上层复核过正文，跳过代码 lint。 */
  skipLint?: boolean;
  /**
   * 是否在写事务里登记受影响实体的 observe_task。默认 false：
   * 观察本身不是证据，登记只会让刚算完的实体立刻重算一遍（自己触发自己）。
   */
  enqueueObserve?: boolean;
}

/** 落库结果：created=false 表示命中去重，没有新增行。 */
export interface ObservationResult {
  id: string;
  created: boolean;
  text: string;
  entities: string[];
  evidenceIds: string[];
  /** 被取代并归档的旧 observation id；没传 replacesObservationId 时为 null。 */
  replaced: string | null;
  rowSeq: number | null;
}

/** 观察写入被拒绝的原因码。 */
export type ObservationRejectCode =
  | 'TEXT_EMPTY'
  | 'ENTITIES_EMPTY'
  | 'EVIDENCE_EMPTY'
  | 'EVIDENCE_MISSING'
  | 'EVIDENCE_INACTIVE'
  | 'EVIDENCE_ENTITY_MISMATCH'
  | 'REPLACES_NOT_FOUND'
  | 'REPLACES_NOT_OBSERVATION';

/** 校验失败：调用方按 code / evidenceId 判，库里一行都不会留。 */
export class ObservationRejectedError extends Error {
  code: ObservationRejectCode;
  evidenceId?: string;

  constructor(code: ObservationRejectCode, message: string, evidenceId?: string) {
    super(message);
    this.name = 'ObservationRejectedError';
    this.code = code;
    if (evidenceId) this.evidenceId = evidenceId;
  }
}

function reject(code: ObservationRejectCode, message: string, evidenceId?: string): never {
  throw new ObservationRejectedError(code, message, evidenceId);
}

/** status 的「还活着」口径：active 或待复核都算，archived / merged / deleted 不算。 */
function isLiveObservation(m: MemoryRecord): boolean {
  const status = String(m.status || 'active').toLowerCase();
  return status === OBSERVATION_INITIAL_STATUS || status === OBSERVATION_PENDING_REVIEW_STATUS;
}

function textOf(value: unknown): string {
  return String(value || '').trim();
}

function evidenceRows(d: DatabaseSync, ids: string[]): Map<string, MemoryRecord> {
  const placeholders = ids.map(() => '?').join(',');
  const rows = d.prepare(
    `SELECT id, text, type, status, entities FROM memories WHERE id IN (${placeholders})`,
  ).all(...ids) as Array<Record<string, unknown>>;
  const out = new Map<string, MemoryRecord>();
  for (const row of rows) {
    // memories.entities 是 JSON 文本列；这里逐行解成数组，交给 entityList 时不会再被逗号切开
    let entities: unknown = row.entities;
    if (typeof entities === 'string') {
      try {
        entities = JSON.parse(entities);
      } catch {
        entities = [];
      }
    }
    out.set(String(row.id), { ...row, entities } as unknown as MemoryRecord);
  }
  return out;
}

/**
 * 证据校验：存在、active、且 entities 与本次观察有交集。
 * 全部在写事务里查，BEGIN IMMEDIATE 之后读到的是写前最新状态。
 */
function assertEvidence(d: DatabaseSync, ids: string[], observationKeys: Set<string>): void {
  const rows = evidenceRows(d, ids);
  for (const id of ids) {
    const row = rows.get(id);
    if (!row) reject('EVIDENCE_MISSING', `证据记忆不存在：${id}`, id);
    if (!isActive(row)) {
      reject('EVIDENCE_INACTIVE', `证据记忆不是 active（${String(row.status || 'active')}）：${id}`, id);
    }
    const keys = entityKeysOf(entityList(row.entities));
    if (!keys.some((key) => observationKeys.has(key))) {
      reject(
        'EVIDENCE_ENTITY_MISMATCH',
        `证据记忆与本次观察没有共同实体：${id}（证据 ${keys.join(',') || '-'} / 观察 ${[...observationKeys].join(',') || '-'}）`,
        id,
      );
    }
  }
}

/** 显式实体在 entities 表里的行：粗实体词典可能还没收录新实体，这里兜一层。 */
function entityHits(d: DatabaseSync, names: string[]): CoarseHit[] {
  const norms = [...new Set(names.map((name) => normalizeEntity(name).norm).filter(Boolean))];
  if (!norms.length) return [];
  try {
    const placeholders = norms.map(() => '?').join(',');
    const rows = d.prepare(
      `SELECT id, norm, display FROM entities WHERE norm IN (${placeholders})`,
    ).all(...norms) as Array<Record<string, unknown>>;
    return rows.map((row) => ({
      id: Number(row.id),
      norm: String(row.norm || ''),
      display: String(row.display || row.norm || ''),
    }));
  } catch {
    // 实体表不在/坏了不该挡观察落库：粗挂载是加分项，队列以后会补
    return [];
  }
}

/** 粗实体命中与显式实体行合并，按 entities.id 去重（同一条只挂一次）。 */
function coarseHitsOf(state: StoreState, d: DatabaseSync, text: string, names: string[]): CoarseHit[] {
  const hits = [...(state.coarseMatcher?.match(text) || []), ...entityHits(d, names)];
  const seen = new Set<number>();
  return hits.filter((hit) => {
    const id = Number(hit.id);
    if (!Number.isFinite(id) || seen.has(id)) return false;
    seen.add(id);
    return true;
  });
}

/** 写 derived_from 边：唯一键 (src,dst,rel) 已存在就跳过，同事务。 */
function writeEvidenceEdges(d: DatabaseSync, observationId: string, evidenceIds: string[], now: number): number {
  if (!hasMemoryLinkTable(d)) return 0;
  const stmt = d.prepare(
    `INSERT OR IGNORE INTO memory_link(src, dst, rel, origin, attr_key, ts, created_ts)
     VALUES (?, ?, ?, ?, '', ?, ?)`,
  );
  const iso = new Date(now).toISOString();
  let n = 0;
  for (const evidenceId of evidenceIds) {
    const res = stmt.run(observationId, evidenceId, EVIDENCE_REL, EVIDENCE_ORIGIN, iso, now);
    if (Number(res.changes) > 0) n += 1;
  }
  return n;
}

/** 命中去重时把库里真实的边读回来（别拿这次入参冒充）。 */
export function observationEvidence(db: DatabaseSync | null | undefined, observationId: string): string[] {
  if (!db || !hasMemoryLinkTable(db)) return [];
  try {
    const rows = db.prepare(
      `SELECT dst FROM memory_link WHERE src = ? AND rel = ? ORDER BY created_ts, dst`,
    ).all(String(observationId), EVIDENCE_REL) as Array<{ dst?: unknown }>;
    return rows.map((row) => String(row.dst)).filter(Boolean);
  } catch {
    return [];
  }
}

/**
 * 每库一份的观察版本号（meta 表 key='observation_epoch'）。
 *
 * 为什么放 meta 表而不是 state 文件：版本要跟「这一库的观察」严格同步，
 * 同库同事务写入、同库任何进程读到的是同一个数；state 文件跟着 cwd 走，
 * 多实例/换工作目录会各写一份，版本会分裂。读失败（老库没 meta）返回 0。
 */
export function observationEpoch(db: DatabaseSync | null | undefined): number {
  if (!db) return 0;
  try {
    const row = db.prepare('SELECT value FROM meta WHERE key = ?').get(OBSERVATION_EPOCH_KEY) as
      | { value?: unknown }
      | undefined;
    const n = Number(row?.value);
    return Number.isFinite(n) ? Math.trunc(n) : 0;
  } catch {
    return 0;
  }
}

/** 观察写入时在写事务里 +1；meta 缺表/写失败只吞掉（不能挡观察落库），返回 0 表示没记上。 */
export function bumpObservationEpoch(d: DatabaseSync): number {
  try {
    const next = observationEpoch(d) + 1;
    d.prepare(
      `INSERT INTO meta(key, value) VALUES (?, ?)
       ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
    ).run(OBSERVATION_EPOCH_KEY, String(next));
    return next;
  } catch {
    return 0;
  }
}

/** 同实体下正文完全相同的活观察：直接复用，保证重跑幂等。 */function findDuplicate(state: StoreState, text: string, observationKeys: Set<string>): MemoryRecord | null {
  for (const m of state.metas) {
    if (!isLiveObservation(m)) continue;
    if (String(m.type || '').toLowerCase() !== 'observation') continue;
    if (String(m.text || '').trim() !== text) continue;
    const keys = entityKeysOf(entityList(m.entities));
    if (keys.some((key) => observationKeys.has(key))) return m;
  }
  return null;
}

/**
 * 写一条 observation（观察行 + 证据边），绕开通用写入闸。
 *
 * @param state store 共享状态（commitWrite / 向量 / 粗实体都在它上面，db 句柄不够）
 * @param input text / entities / evidenceIds 必填；type 固定 observation
 * @param opts 见 ObservationOptions
 * @returns 落库结果；命中去重时 created=false、id 为已有观察
 * @throws ObservationRejectedError 证据校验失败（一行不落）
 */
export async function saveObservation(
  state: StoreState,
  input: ObservationInput,
  opts: ObservationOptions = {},
): Promise<ObservationResult> {
  const text = textOf(input.text);
  if (!text) reject('TEXT_EMPTY', 'observation 正文为空');

  const entities = [...new Set(entityList(input.entities))];
  if (!entities.length) reject('ENTITIES_EMPTY', 'observation 至少要有 entities');

  const evidenceIds = [...new Set(entityList(input.evidenceIds))];
  if (!evidenceIds.length) reject('EVIDENCE_EMPTY', 'observation 至少要有 1 条证据');

  // lint 在 embed 之前：不合格的正文不花 embedding 的钱（口径同 saveMemory）
  if (!opts.skipLint) {
    assertTextLint(state, 'save', text, input.source || 'sleep:observe', opts.lintReview ?? null);
  }

  const observationKeys = new Set(entityKeysOf(entities));

  // 去重先于 embedding：重跑命中已有观察时一次模型调用都不该发生
  const duplicate = findDuplicate(state, text, observationKeys);
  if (duplicate) {
    return {
      id: duplicate.id,
      created: false,
      text: String(duplicate.text),
      entities: entityList(duplicate.entities),
      evidenceIds: observationEvidence(state.connect(), duplicate.id),
      replaced: null,
      rowSeq: state.rowSeqById.get(duplicate.id) ?? null,
    };
  }

  const [vec] = await state.embedTexts([text]);
  if (!vec || !vec.length) throw new Error('embedding returned empty vector');
  if (state.dim === 0) state.dim = vec.length;
  if (vec.length !== state.dim) throw new Error(`dimension mismatch: got ${vec.length}, expected ${state.dim}`);

  const d = state.connect();
  const coarseHits = coarseHitsOf(state, d, text, entities);
  const now = Date.now();
  const record = buildNewRecord({
    trimmed: text,
    tagList: Array.isArray(opts.tags) ? opts.tags.map(String) : [],
    type: 'observation',
    source: input.source || 'sleep:observe',
    meta: { confidence: input.confidence },
    entities,
    validFrom: '',
  });
  // 初态 active：证据的存在/active/实体相交在本事务的 ① 已校验过；
  // pending_review 留给「证据后来变化、待复核」，由复核链路显式传。
  record.status = opts.status || OBSERVATION_INITIAL_STATUS;
  if (input.reason) record.reason = String(input.reason);
  // extra 的落法沿用 recordToRow：非列键统一进 extra 列，读侧 rowToRecord 再展开
  record.autoObservation = true;
  record.version = AUTO_OBSERVATION_VERSION;

  const replacesId = String(input.replacesObservationId || '').trim();
  // 旧条状态在内存里定（校验失败直接抛，一行没写）；真正的 upsert 放在事务里
  let archivedRow: MemoryRecord | null = null;
  if (replacesId) {
    const prior = state.metas.find((m) => m.id === replacesId);
    if (!prior) reject('REPLACES_NOT_FOUND', `被取代的 observation 不存在：${replacesId}`);
    if (String(prior.type || '').toLowerCase() !== 'observation') {
      reject('REPLACES_NOT_OBSERVATION', `被取代的条目不是 observation：${replacesId}`);
    }
    if (isLiveObservation(prior)) {
      archivedRow = {
        ...prior,
        status: 'archived',
        supersededBy: record.id,
        validTo: prior.validTo || dateOnly(record.ts) || dateOnly(new Date(now).toISOString()),
        reason: `被新 observation 取代：${record.id}`,
        updatedAt: new Date(now).toISOString(),
      };
    }
  }
  // commitWrite 的 nextMetas 在 work 之前求值，所以归档行必须在这里就定好
  const nextMetas = state.metas
    .map((m) => (archivedRow && m.id === archivedRow.id ? archivedRow : m))
    .concat(record);

  const rowSeq = commitWrite(state, {
    action: 'observation_save',
    work: (tx) => {
      // ① 证据校验：任何一条不合格整笔拒绝，此时还没有任何写
      assertEvidence(tx, evidenceIds, observationKeys);

      // ② 旧观察归档：同一笔事务里直接改行，不走 archiveMemory（它会另开一笔 writeTx）
      if (archivedRow) {
        upsertRow(state, archivedRow, existingRowSeq(state, tx, archivedRow.id) ?? allocRowSeq(tx));
      }

      // ③ 观察行：rowSeq / 粗挂载 / 向量，副作用与 saveMemory 的新条路径同口径
      const seq = allocRowSeq(tx);
      upsertRow(state, record, seq);
      writeCoarseEntities(state, tx, record.id, coarseHits, record.ts);
      state.vecDb!.insert(record.id, Float32Array.from(vec));

      // ④ 证据边：同事务，唯一键天然幂等
      writeEvidenceEdges(tx, record.id, evidenceIds, now);

      // ⑤ 观察版本号 +1：prefill 注入缓存按它失效（同一笔事务，读侧不会看到「观察已写、版本没涨」）
      bumpObservationEpoch(tx);

      // ⑥ 默认不登记 observe_task：观察不是证据，登记只会自己触发自己。
      //    调用方显式要（例如以后按实体做「观察已刷新」通知）才登记，缺表时静默跳过。
      if (opts.enqueueObserve) notifyEntityNamesChanged(tx, state.cfg, entities, 'observation');
      return seq;
    },
    nextMetas,
    rowSeqId: record.id,
  });

  return {
    id: record.id,
    created: true,
    text,
    entities: record.entities || entities,
    evidenceIds,
    replaced: archivedRow ? archivedRow.id : null,
    rowSeq,
  };
}
