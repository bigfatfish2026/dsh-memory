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
 * status 默认 'active'：证据校验（存在 / active / 实体绑定）就在本笔写事务里跑过，
 * 落库那一刻观察已经「有据可查」，没有理由再把它挡在召回外。'pending_review' 有两种来路：
 *   ① 证据后来变化、等待复核（2026-11-21 起，由复核链路写）；
 *   ② 本次落库时证据绑定有歧义（2026-11-23 t-mwsuyr33-2qmv）：不拒也不放行，
 *      在本事务里把初态改成 pending_review 并标 extra.needsModelReview，交复核链路判。
 * 默认召回剔除这类自动观察（isHiddenObservation）。
 */
import type { DatabaseSync } from 'node:sqlite';
import { buildNewRecord, commitWrite } from './records.js';
import { assertTextLint } from './gate.js';
import { writeCoarseEntities } from './entities.js';
import { allocRowSeq, existingRowSeq, isActive, upsertRow } from './rows.js';
import { hasMemoryLinkTable, OBSERVATION_PENDING_REVIEW_STATUS } from '../memory-link.js';
import { entityKeysOf, notifyEntityNamesChanged } from './observe-notify.js';
import { evidenceBindingOf } from '../sleep/observation-sources.js';
import { normalizeEntity } from '../shared/entity-normalize.js';
import { dateOnly, entityList } from './types.js';
import type { EvidenceBasis } from '../sleep/observation-sources.js';
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
  /**
   * 证据：id 字符串，或 `{ id, basis }`（取证据时算出的命中依据，见 observation-sources 的
   * evidenceBindingOf）。带 basis 只是把取证口径带进事务，判定仍以事务里读到的最新行为准。
   */
  evidenceIds: unknown;
  confidence?: number;
  /** 被这条新观察取代的旧 observation id；旧条在这笔事务里归档。 */
  replacesObservationId?: string;
  /** 为什么生成（排障/复核用），落 memories.reason。 */
  reason?: string;
  /** 来源前缀，默认 'sleep:observe'；调用方只允许补后缀。 */
  source?: string;
}

/** 事务复验用的一条证据：id + 取证时的命中依据。 */
export interface ObservationEvidenceRef {
  id: string;
  basis?: EvidenceBasis;
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
  /** 日志出口；证据绑定有歧义、观察被迫转待复核时打一行（前缀 [observe]）。 */
  log?: (line: string) => void;
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
  /** 落库行的实际状态；证据绑定有歧义时会被强制成 pending_review。 */
  status: string;
  /** 因绑定有歧义排进待复核通道的证据 id（调用方拿去打日志）。 */
  pendingReviewEvidence: string[];
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

/** 入参证据归一：字符串或 `{ id, basis }`，按 id 去重保留首次出现。 */
function evidenceRefsOf(value: unknown): ObservationEvidenceRef[] {
  if (!Array.isArray(value)) return [];
  const out: ObservationEvidenceRef[] = [];
  const seen = new Set<string>();
  for (const item of value) {
    const raw = item && typeof item === 'object' && !Array.isArray(item)
      ? (item as Record<string, unknown>)
      : null;
    const id = textOf(raw ? raw.id : item);
    if (!id || seen.has(id)) continue;
    seen.add(id);
    const basis = raw?.basis && typeof raw.basis === 'object' && !Array.isArray(raw.basis)
      ? raw.basis as EvidenceBasis
      : undefined;
    out.push(basis ? { id, basis } : { id });
  }
  return out;
}

/**
 * 证据校验：存在、active、且实体绑定与本次观察同一口径。
 *
 * 逐条 active 校验与「观察实体键」这条底线不变（2026-11-23 t-mwsuyr33-2qmv 只修取证口径）：
 *   - 记录实体键与观察键有交集 → 通过（老路径，行为不变）；
 *   - 记录实体列为空 → 用 evidenceBindingOf 的正文整词兜底（和取证据同一份函数）：
 *       绑不上（只是泛泛提及 / 裸子串）→ 照旧拒绝；
 *       无歧义 → 通过；歧义（词太短、片段太短、同一词命中多个候选实体）→ 交待复核，
 *       不在这里拒，也不静默放行。
 *   - 记录有实体键但无交集 → 照旧拒绝（不许因为前缀相似就挂边）。
 *
 * 返回需要待复核的证据；拒绝仍抛 ObservationRejectedError。
 */
function assertEvidence(
  d: DatabaseSync,
  refs: ObservationEvidenceRef[],
  observationKeys: Set<string>,
): Array<{ id: string; basis: EvidenceBasis }> {
  const rows = evidenceRows(d, refs.map((ref) => ref.id));
  const review: Array<{ id: string; basis: EvidenceBasis }> = [];
  const candidateKeys = [...observationKeys];

  for (const ref of refs) {
    const id = ref.id;
    const row = rows.get(id);
    if (!row) reject('EVIDENCE_MISSING', `证据记忆不存在：${id}`, id);
    if (!isActive(row)) {
      reject('EVIDENCE_INACTIVE', `证据记忆不是 active（${String(row.status || 'active')}）：${id}`, id);
    }
    const keys = entityKeysOf(entityList(row.entities));
    if (keys.some((key) => observationKeys.has(key))) continue;

    // 旧行（实体列为空）：正文整词绑定，规则与 selectEntityEvidence 共用 evidenceBindingOf
    if (keys.length === 0) {
      // 候选实体键一起传进去：同一句里出现多个候选实体词也算歧义（句子级判据在
      // evidenceBindingOf 里，与取证据同一份；2026-11-23 t-mwszo23a-iwxs）。
      const bindings = candidateKeys
        .map((key) => evidenceBindingOf(row, key, { candidates: candidateKeys }))
        .filter((basis): basis is EvidenceBasis => Boolean(basis));
      if (bindings.length === 1 && !bindings[0].ambiguous) continue;
      if (bindings.length >= 1) {
        const basis: EvidenceBasis = bindings.length > 1
          // 同一个词能绑到不止一个候选实体：结论不唯一，排待复核
          ? { ...bindings[0], ambiguous: true, ambiguousReason: 'multiple_entities' }
          : bindings[0];
        review.push({ id, basis });
        continue;
      }
    }
    const hit = ref.basis ? `命中依据 ${ref.basis.rule}:${ref.basis.word}` : '无命中依据';
    reject(
      'EVIDENCE_ENTITY_MISMATCH',
      `证据记忆与本次观察没有共同实体：${id}（证据 ${keys.join(',') || '-'} / 观察 ${candidateKeys.join(',') || '-'} / ${hit}）`,
      id,
    );
  }
  return review;
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

  // 证据既可以是 id 字符串，也可以是取证据时带回的 { id, basis }（命中依据）
  const evidenceRefs = evidenceRefsOf(input.evidenceIds);
  const evidenceIds = evidenceRefs.map((ref) => ref.id);
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
      status: String(duplicate.status || OBSERVATION_INITIAL_STATUS),
      pendingReviewEvidence: [],
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
  // 初态 active：证据的存在/active/实体绑定在本事务的 ① 复验过；
  // 绑定有歧义时 ① 会把这里改成 pending_review（覆盖显式传入的状态：歧义不该静默放行）。
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

  // 事务复验发现的歧义绑定：不拒，转待复核（复用复核链路的 pending_review + needsModelReview）
  let reviewEvidence: Array<{ id: string; basis: EvidenceBasis }> = [];

  const rowSeq = commitWrite(state, {
    action: 'observation_save',
    work: (tx) => {
      // ① 证据校验：任何一条不合格整笔拒绝，此时还没有任何写。
      //    歧义绑定不拒：下面把观察初态改成 pending_review，交复核链路判。
      reviewEvidence = assertEvidence(tx, evidenceRefs, observationKeys);
      if (reviewEvidence.length) {
        record.status = OBSERVATION_PENDING_REVIEW_STATUS;
        record.needsModelReview = true;
      }

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

  if (reviewEvidence.length) {
    const detail = reviewEvidence
      .map((item) => `${item.id}:${item.basis.rule}/${item.basis.ambiguousReason || '-'}`)
      .join(',');
    opts.log?.(`[observe] 证据绑定有歧义，观察转待复核 id=${record.id} evidence=${detail}`);
  }

  return {
    id: record.id,
    created: true,
    text,
    entities: record.entities || entities,
    evidenceIds,
    replaced: archivedRow ? archivedRow.id : null,
    rowSeq,
    status: String(record.status || OBSERVATION_INITIAL_STATUS),
    pendingReviewEvidence: reviewEvidence.map((item) => item.id),
  };
}
