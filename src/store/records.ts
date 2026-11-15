/**
 * 记忆的增删改查：save / update / archive / merge / reflect。
 *
 * 三条写路径（save / update / merge）的事务收尾统一走 commitWrite：
 * 写事务、snapshotVersion 更新、向量索引、挂载维护、metas/rowSeq 同步、异步入队
 * 这七件事只在这里做一遍。事务体本身的差异（新条插入 vs 老条替换）由各路径提供，
 * 但边界与账目不再各写一套——2026-11-15 拆 store.js 时收的口子。
 */
import { hasOriginColumn, deleteMemoryEntity } from '../entity-gate.js';
import { allocRowSeq, existingRowSeq, upsertRow } from './rows.js';
import { assertTextLint, assertWriteAllowed, discardedRecord, supersededRow } from './gate.js';
import { writeCoarseEntities } from './entities.js';
import { clamp01, dateFromHead, dateOnly, entityList, normalizeType } from './types.js';
import { logStore } from './log.js';
import type {
  CoarseHit, DiscardedRecord, MemoryPatch, MemoryRecord, MemoryView, RecordMeta, SavedRecord,
  StoreState, WriteGateResult,
} from './types.js';
import type { DatabaseSync } from 'node:sqlite';

/** commitWrite 的入参：事务体 + 事务外要同步的内存账。 */
interface CommitWriteInput {
  action: string;
  work: (d: DatabaseSync) => number;
  nextMetas: MemoryRecord[];
  rowSeqId?: string | null;
  enqueueId?: string | null;
}

/** 新条落库的事务体入参。 */
interface WriteNewRecordInput {
  record: MemoryRecord;
  supersededUpdates?: MemoryRecord[];
  coarseHits?: CoarseHit[];
  vec: ArrayLike<number>;
}

/** 组装新记录的入参（save / merge 同一口径）。 */
interface NewRecordInput {
  trimmed: string;
  tagList: string[];
  type: string;
  source?: string;
  meta: RecordMeta;
  entities?: string[];
  validFrom?: unknown;
  supersedes?: string[];
}

/** 被取代旧条的入参。 */
interface SupersededInput {
  record: MemoryRecord;
  supersedes?: string[];
  archivedByGate?: string[];
  extraArchive?: string[];
}

/** 正文修订（lint → 向量 → 过闸）的结果。 */
interface TextRevision {
  next: MemoryRecord;
  nextVec: Float32Array;
  gateResult: WriteGateResult | null;
  coarseHits: CoarseHit[] | null;
}

/** 合并时带过去的结构字段。 */
interface MergeFields {
  entities: Set<string>;
  type?: string;
  confidence?: number;
  validFrom: string;
}

/**
 * 唯一的写入收尾。事务体的差异由 work(d) 提供；事务边界、版本号、内存索引、
 * 事件只在这里动，save / update / merge 共用。
 */
function commitWrite(state: StoreState, { action, work, nextMetas, rowSeqId, enqueueId = null }: CommitWriteInput): number {
  const seq = state.writeTx(action, work);
  state.metas = nextMetas;
  if (rowSeqId) {
    state.rowSeqById.set(rowSeqId, seq);
    state.maxRowSeq = Math.max(state.maxRowSeq || 0, seq);
  }
  if (enqueueId) state.enqueueEntityLater(enqueueId);
  return seq;
}

/**
 * 新条落库的事务体（save / merge 共用）：取 rowSeq + 正文 + 被取代的旧条 + 粗实体 + 向量。
 *
 * 2026-11-15（审计存储高-1）：以前 mergeMemories 先调 saveMemory 落新条，
 * 再对每个源条单独 updateMemory，三笔以上事务；中途抛错就留下「新条已落、源条还 active」。
 * 抽成这个函数后 merge 能在同一笔里再写源条的 merged 状态。
 */
export function writeNewRecordTx(state: StoreState, d: DatabaseSync, { record, supersededUpdates = [], coarseHits = [], vec }: WriteNewRecordInput): number {
  const rowSeq = allocRowSeq(d);
  upsertRow(state, record, rowSeq);
  for (const update of supersededUpdates) upsertRow(state, update, existingRowSeq(state, d, update.id) ?? allocRowSeq(d));
  // 粗实体跟记忆同事务：新条落库那一刻就有实体，近邻/实体通道不空窗
  writeCoarseEntities(state, d, record.id, coarseHits, record.ts);
  state.vecDb!.insert(record.id, Float32Array.from(vec));
  return rowSeq;
}

/**
 * 组装要落库的新记录（save / merge 共用）。
 * 两边口径必须一致：id/ts 生成、opinion 置信度、entities 老列、validFrom/validTo、supersedes。
 * 2026-11-15 抽出来——merge 改成一笔事务后不能再借 saveMemory，不抽就会两处漂移。
 */
function buildNewRecord({ trimmed, tagList, type, source, meta, entities = [], validFrom = '', supersedes = [] }: NewRecordInput): MemoryRecord {
  const record: MemoryRecord = {
    id: `m_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 6)}`,
    text: trimmed,
    tags: tagList,
    type,
    source: source || 'agent',
    ts: new Date().toISOString(),
  };
  if (type === 'opinion') record.confidence = clamp01(meta.confidence, 0.7);
  else if (meta.confidence !== undefined) record.confidence = clamp01(meta.confidence, 1);
  // memories.entities 老列留着做双读对照：工具路径写粗实体名，内部路径仍写自己给的
  if (entities.length) record.entities = [...new Set(entities)];
  // validFrom 服务端写：调用方给的（内部路径）优先，工具路径靠正文头部日期反抽
  const from = dateOnly(validFrom) || dateFromHead(trimmed);
  if (from) record.validFrom = from;
  const to = dateOnly(meta.validTo);
  if (to) record.validTo = to;
  if (supersedes.length) record.supersedes = supersedes;
  return record;
}

/**
 * 被取代的旧条目：supersededBy / validTo，跟新条目同一笔事务落库。
 * archivedByGate 里的要真归档（status=archived），其余只补 supersededBy/validTo（老行为）。
 */
function buildSupersededUpdates(state: StoreState, { record, supersedes = [], archivedByGate = [], extraArchive = [] }: SupersededInput): MemoryRecord[] {
  const archived = new Set(archivedByGate);
  const updates: MemoryRecord[] = [];
  for (const id of [...new Set([...supersedes, ...extraArchive])]) {
    if (archived.has(id)) {
      const row = supersededRow(state, id, record.id, record.ts);
      if (row) updates.push(row);
      continue;
    }
    const prior = state.metas.find((m) => m.id === id);
    if (!prior) continue;
    updates.push({
      ...prior,
      supersededBy: record.id,
      validTo: prior.validTo || record.validFrom || dateOnly(record.ts),
    });
  }
  return updates;
}

export async function saveMemory(
  state: StoreState,
  text: unknown,
  tags: unknown,
  source: string | undefined,
  meta: RecordMeta = {},
): Promise<SavedRecord | DiscardedRecord> {
  const trimmed = String(text || '').trim();
  if (!trimmed) throw new Error('text is empty');
  // 正文 lint 在 embed / 过闸之前：不合格的条目不花 embedding 的钱，也不进任何表
  if (!meta.__lintChecked) assertTextLint(state, 'save', trimmed, source, meta.__lintReview);
  const tagList = Array.isArray(tags) ? tags.map(String) : [];
  const type = normalizeType(meta.type, tagList, trimmed);

  let vec = meta.__vector;
  if (!vec) [vec] = await state.embedTexts([trimmed]);
  if (!vec || !vec.length) throw new Error('embedding returned empty vector');
  if (state.dim === 0) state.dim = vec.length;
  if (vec.length !== state.dim) throw new Error(`dimension mismatch: got ${vec.length}, expected ${state.dim}`);

  // 唯一写入闸：mergeMemories 已经用同一份正文过闸，所以传 __gateChecked 免得问两遍
  // 2026-11-05 t-fix-first-run：带 supersedes 时把被取代的 id 也排除出召回，
  // 否则「新条取代旧条」会被旧条自己判成 DUPLICATE，整个 SUPERSEDES 路径走不通。
  const supersedes = entityList(meta.supersedes).filter((id) => state.metas.some((m) => m.id === id));
  // 2026-11-13：实体必须在过闸之前算（写入闸 related 通道吃 entities），
  // 内部路径（sleep/consolidate/split）自带 entities 就照用，工具路径没有就靠粗实体兜。
  const explicitEntities = entityList(meta.entities);
  const coarseHits = explicitEntities.length ? [] : state.coarseMatcher!.match(trimmed);
  const gateEntities = explicitEntities.length ? explicitEntities : coarseHits.map((h) => h.display || h.norm);
  const gateResult = meta.__gateChecked
    ? null
    : await assertWriteAllowed(state, 'save', {
      text: trimmed, type, tags: tagList, entities: gateEntities, source, sourceFile: meta.sourceFile,
    }, { vector: vec, excludeIds: supersedes, ids: [], lintReview: meta.__lintReview });
  if (gateResult?.verdict === 'DUPLICATE') return discardedRecord(trimmed, tagList, type, gateResult);

  // supersedes 上面已经算好（过闸前就要用它排除召回），这里只补写入闸判定的那条
  const archivedByGate = gateResult?.verdict === 'SUPERSEDES' && gateResult.matchedId ? [gateResult.matchedId] : [];
  if (archivedByGate.length && !supersedes.includes(archivedByGate[0])) supersedes.push(archivedByGate[0]);

  const record = buildNewRecord({
    trimmed, tagList, type, source, meta, entities: gateEntities, validFrom: meta.validFrom, supersedes,
  });
  const supersededUpdates = buildSupersededUpdates(state, {
    record, supersedes, archivedByGate, extraArchive: entityList(meta.__archive),
  });
  const nextMetas = state.metas
    .map((m) => supersededUpdates.find((u) => u.id === m.id) || m)
    .concat(record);
  // 向量与正文同事务：vecDb 用的是同一个 sqlite 连接；rowSeq 也在事务里取（见 allocRowSeq）
  commitWrite(state, {
    action: 'save',
    work: (d) => writeNewRecordTx(state, d, { record, supersededUpdates, coarseHits, vec }),
    nextMetas,
    rowSeqId: record.id,
    // 事务外异步入队，交给 entity_extract 补实体（同一进程里排一次就够，dedupe_key=memory_id）
    enqueueId: record.id,
  });
  return record;
}

/**
 * 正文变了才走的那一段：lint → 向量 → 过闸。
 * 抽出来只为让 updateMemory 留在 80 行内，判定顺序与原实现逐行一致。
 */
async function prepareTextRevision(state: StoreState, id: string, prev: MemoryRecord, patch: MemoryPatch): Promise<TextRevision> {
  const trimmed = String(patch.text).trim();
  if (!trimmed) throw new Error('text is empty');
  // 只有正文变了才判（archive / 置信度调整不引入新事实）
  if (!patch.__lintChecked) assertTextLint(state, 'update', trimmed, 'update', patch.__lintReview);
  const [vec] = await state.embedTexts([trimmed]);
  if (!vec || !vec.length) throw new Error('embedding returned empty vector');
  if (state.dim === 0) state.dim = state.vecDb!.dim || vec.length;
  if (vec.length !== state.dim) throw new Error(`dimension mismatch: got ${vec.length}, expected ${state.dim}`);
  // 只有正文变了才需要过闸：archive / 置信度调整这类不引入新事实
  // 2026-11-05 t-fix-first-run：排除它自己与它 supersedes 的 id——否则改一个字就把自己判成重复，
  // 修订和 SUPERSEDES 全部 22/22 撞 DUPLICATE。
  const excludeIds = [id, ...entityList(patch.supersedes)];
  // 正文改了但没显式给实体：现算一遍粗实体喂给写入闸（同 save 的理由）。
  // 2026-11-15（审计实体中-8）：不再只在 entities 列为空时才算——
  // 重写后的挂载要按新正文换，否则旧正文抽出来的 agent 挂载会一直挂着。
  let coarseHits: CoarseHit[] | null = null;
  if (patch.entities === undefined) coarseHits = state.coarseMatcher!.match(trimmed);
  const gateEntities = patch.entities !== undefined
    ? patch.entities
    : (coarseHits?.length ? coarseHits.map((h) => h.display || h.norm) : entityList(prev.entities));
  const gateResult = await assertWriteAllowed(state, 'update', {
    text: trimmed,
    type: patch.type ?? prev.type,
    tags: patch.tags ?? prev.tags,
    entities: gateEntities,
    source: 'update',
    sourceFile: patch.sourceFile,
    ids: [String(id)],
  }, { vector: vec, excludeIds, lintReview: patch.__lintReview });
  const next: MemoryRecord = { ...prev, text: trimmed };
  return { next, nextVec: Float32Array.from(vec), gateResult, coarseHits };
}

/** 字段补丁（不含正文与 lint/向量）：顺序与老实现一致，tag 变了要重推 type。 */
function applyPatchFields(state: StoreState, next: MemoryRecord, patch: MemoryPatch, coarseHits: CoarseHit[] | null): void {
  if (patch.tags !== undefined) {
    next.tags = Array.isArray(patch.tags) ? patch.tags.map(String) : [];
    // 标签变了但没显式给 type：重新推断，别留着旧类型
    if (patch.type === undefined) next.type = normalizeType(undefined, next.tags, next.text);
  }
  if (patch.type !== undefined) next.type = normalizeType(patch.type, next.tags, next.text);
  if (patch.confidence !== undefined) next.confidence = clamp01(patch.confidence, 0.7);
  if (patch.entities !== undefined) {
    const entities = entityList(patch.entities);
    if (entities.length) next.entities = entities;
    else delete next.entities;
  }
  // 正文重写又没给实体：粗实体顺手换掉 memories.entities 老列（新表那几行在事务里换）
  if (coarseHits) {
    if (coarseHits.length) next.entities = [...new Set(coarseHits.map((h) => h.display || h.norm))];
    else delete next.entities;
  }
  if (patch.validFrom !== undefined) {
    const from = dateOnly(patch.validFrom);
    if (from) next.validFrom = from;
  }
  if (patch.validTo !== undefined) {
    const to = dateOnly(patch.validTo);
    if (to) next.validTo = to;
    else delete next.validTo;
  }
  if (patch.supersedes !== undefined) {
    const list = entityList(patch.supersedes).filter((x) => x !== next.id && state.metas.some((m) => m.id === x));
    if (list.length) next.supersedes = list;
  }
  if (patch.supersededBy !== undefined) next.supersededBy = patch.supersededBy;
  if (patch.status !== undefined) next.status = patch.status;
  if (patch.reason !== undefined) next.reason = patch.reason;
  next.updatedAt = new Date().toISOString();
}

export async function updateMemory(state: StoreState, id: string, patch: MemoryPatch): Promise<SavedRecord | DiscardedRecord> {
  const idx = state.metas.findIndex((m) => m.id === id);
  if (idx < 0) throw new Error(`memory not found: ${id}`);
  const prev = state.metas[idx];
  let next: MemoryRecord = { ...prev };
  let nextVec: Float32Array | null = null;
  let gateResult: WriteGateResult | null = null;
  let coarseHits: CoarseHit[] | null = null;
  if (patch.text !== undefined) {
    const rev = await prepareTextRevision(state, id, prev, patch);
    if (rev.gateResult?.verdict === 'DUPLICATE') {
      return {
        ...prev,
        discarded: true,
        verdict: rev.gateResult.verdict,
        matchedId: rev.gateResult.matchedId || null,
        reason: rev.gateResult.reason || '',
      };
    }
    next = rev.next;
    nextVec = rev.nextVec;
    gateResult = rev.gateResult;
    coarseHits = rev.coarseHits;
  }
  applyPatchFields(state, next, patch, coarseHits);
  // 写入闸判 SUPERSEDES：旧条归档跟这次修订同一笔事务，别拆成两次
  let archiveRow: MemoryRecord | null = null;
  if (gateResult?.verdict === 'SUPERSEDES' && gateResult.matchedId && gateResult.matchedId !== id) {
    archiveRow = supersededRow(state, gateResult.matchedId, id, next.updatedAt);
    if (archiveRow) next.supersedes = [...new Set([...(next.supersedes || []), gateResult.matchedId])];
  }
  const nextMetas = state.metas.slice();
  nextMetas[idx] = next;
  if (archiveRow) {
    const at = nextMetas.findIndex((m) => m.id === archiveRow.id);
    if (at >= 0) nextMetas[at] = archiveRow;
  }
  commitWrite(state, {
    action: 'update',
    work: (d) => {
      const rowSeq = existingRowSeq(state, d, id) ?? allocRowSeq(d);
      upsertRow(state, next, rowSeq);
      if (nextVec) state.vecDb!.replace(id, nextVec);
      if (archiveRow) upsertRow(state, archiveRow, existingRowSeq(state, d, archiveRow.id) ?? allocRowSeq(d));
      // 正文重写后旧挂载要跟着换：按 memory_id 全删（rule / agent 一起清），
      // 只删 origin='rule' 的话 agent 从旧正文抽出来的挂载会留在新正文上（2026-11-15，审计实体中-8）。
      // 老库没有 origin 列时整段跳过——宁可留旧行，也不能误删
      if (coarseHits && hasOriginColumn(d)) {
        deleteMemoryEntity(d, { memoryId: id });
        writeCoarseEntities(state, d, id, coarseHits, next.ts || '');
      }
      return rowSeq;
    },
    nextMetas,
    rowSeqId: id,
  });
  return next;
}

export async function archiveMemory(state: StoreState, id: string, reason?: string): Promise<SavedRecord | DiscardedRecord> {
  return updateMemory(state, id, { status: 'archived', reason: reason || '' });
}

/** 合并时把结构字段带过去：取最早的 validFrom、最高置信度、实体并集。 */
function aggregateMergeFields(sources: MemoryRecord[], meta: RecordMeta): MergeFields {
  const entities = new Set<string>();
  let type = meta.type;
  let confidence = meta.confidence;
  let validFrom = dateOnly(meta.validFrom);
  for (const m of sources) {
    for (const e of m.entities || []) entities.add(e);
    if (!type) type = m.type;
    if (m.confidence !== undefined) {
      confidence = confidence === undefined ? m.confidence : Math.max(Number(confidence) || 0, m.confidence);
    }
    const from = dateOnly(m.validFrom) || dateOnly(m.ts);
    if (from && (!validFrom || from < validFrom)) validFrom = from;
  }
  return { entities, type, confidence, validFrom };
}

/**
 * 合并多条记忆：新条 + supersedes 归档 + 源条转 merged + 源条旧挂载清理，全部落在**一笔** writeTx。
 *
 * 2026-11-15（审计存储高-1）：以前是 saveMemory 一笔 + 每个源条 updateMemory 各一笔，
 * 中途崩/抛错会留下新条已落、部分源条还 active，重跑又生成一条。现在：
 *   ① ids 先按内存里的记录过滤，过滤后不足 2 条直接抛，不产生任何写入；
 *   ② 所有 upsert / 挂载删除都在同一个 BEGIN IMMEDIATE 里，失败整笔回滚。
 */
export async function mergeMemories(
  state: StoreState,
  ids: unknown,
  text: unknown,
  tags: unknown,
  meta: RecordMeta = {},
): Promise<SavedRecord | DiscardedRecord> {
  const list = [...new Set(entityList(ids))].filter((id) => state.metas.some((m) => m.id === id));
  if (list.length < 2) throw new Error('merge needs at least 2 existing ids');
  const trimmed = String(text || '').trim();
  if (!trimmed) throw new Error('text is empty');
  if (!meta.__lintChecked) assertTextLint(state, 'merge', trimmed, meta.sourceFile || 'merge', meta.__lintReview);
  const tagList = Array.isArray(tags) ? tags.map(String) : [];
  const sources = list.map((id) => state.metas.find((m) => m.id === id)) as MemoryRecord[];
  const { entities, type, confidence, validFrom } = aggregateMergeFields(sources, meta);

  // 写入闸先于任何落库：DUPLICATE 连新条带旧条都不动
  // 2026-11-05 t-fix-first-run：本批源 id + supersedes 里点名的 id 全部排除出召回。
  // 不排除的话合并结果必然跟自己的成员撞 DUPLICATE（首跑 22/22 全灭，一条没落库）。
  const [vec] = await state.embedTexts([trimmed]);
  if (!vec || !vec.length) throw new Error('embedding returned empty vector');
  if (state.dim === 0) state.dim = vec.length;
  if (vec.length !== state.dim) throw new Error(`dimension mismatch: got ${vec.length}, expected ${state.dim}`);
  const excludeIds = [...new Set([
    ...list.map(String),
    ...entityList(meta.supersedes),
    ...entityList(meta.__archive),
  ])];
  const gateResult = await assertWriteAllowed(state, 'merge', {
    text: trimmed, type, tags: tagList, entities: [...entities], source: 'sleep:merge', sourceFile: meta.sourceFile,
    ids: list.map(String),
  }, { vector: vec, excludeIds, lintReview: meta.__lintReview });
  if (gateResult?.verdict === 'DUPLICATE') {
    return discardedRecord(trimmed, tagList, normalizeType(type, tagList, trimmed), gateResult);
  }

  // 新条按 saveMemory 的老口径组装（这里不能再调 saveMemory，否则又是一笔独立事务）
  const archiveIds = gateResult?.verdict === 'SUPERSEDES' && gateResult.matchedId ? [gateResult.matchedId] : [];
  const supersedes = entityList(meta.supersedes === undefined ? list : meta.supersedes)
    .filter((id) => state.metas.some((m) => m.id === id));
  for (const id of archiveIds) if (!supersedes.includes(id)) supersedes.push(id);
  const record = buildNewRecord({
    trimmed,
    tagList,
    type: normalizeType(type, tagList, trimmed),
    source: 'sleep:merge',
    meta: { ...meta, confidence },
    entities: [...entities],
    validFrom,
    supersedes,
  });
  const supersededUpdates = buildSupersededUpdates(state, {
    record, supersedes, archivedByGate: archiveIds, extraArchive: entityList(meta.__archive),
  });
  const now = new Date().toISOString();
  const mergedUpdates = list.map((id): MemoryRecord => {
    const prior = supersededUpdates.find((u) => u.id === id) || state.metas.find((m) => m.id === id);
    return { ...prior, status: 'merged', reason: `merged into ${record.id}`, updatedAt: now } as MemoryRecord;
  });
  const nextMetas = state.metas
    .map((m) => mergedUpdates.find((u) => u.id === m.id) || supersededUpdates.find((u) => u.id === m.id) || m)
    .concat(record);

  commitWrite(state, {
    action: 'merge',
    work: (d) => {
      const rowSeq = writeNewRecordTx(state, d, { record, supersededUpdates, vec });
      for (const update of mergedUpdates) upsertRow(state, update, existingRowSeq(state, d, update.id) ?? allocRowSeq(d));
      // 源条退场，挂载跟着清：合并掉的记忆不该还挂在实体上占 mem_count / surface.count，
      // 也不该再被实体通道召回（审计实体中-7/中-8/中-9 的删除面）。
      for (const id of list) {
        try {
          deleteMemoryEntity(d, { memoryId: id });
        } catch (error) {
          logStore(state.cfg, 'WARN', '合并时清挂载失败（合并照做）', `${id}：${String((error && (error as { message?: unknown }).message) || error)}`);
        }
      }
      return rowSeq;
    },
    nextMetas,
    rowSeqId: record.id,
    // 事务外异步入队，交给 entity_extract 给新条补实体
    enqueueId: record.id,
  });
  return record;
}

/** opinion 置信度调整，照 Hindsight：矛盾掉得比削弱快一倍 */
export async function reflect(state: StoreState, id: string, signal: unknown): Promise<SavedRecord | DiscardedRecord> {
  const m = state.metas.find((x) => x.id === id);
  if (!m) throw new Error(`memory not found: ${id}`);
  const type = normalizeType(m.type, m.tags, m.text);
  if (type !== 'opinion') throw new Error(`不是 opinion 条目：${id}（${type}）`);
  const alpha = Math.max(0.01, Math.min(0.5, Number(state.cfg.opinionAlpha) || 0.1));
  const before = clamp01(m.confidence, 0.7);
  const kind = String(signal || '').toLowerCase();
  let after: number;
  if (kind === 'reinforce') after = Math.min(before + alpha, 1);
  else if (kind === 'weaken') after = Math.max(before - alpha, 0);
  else if (kind === 'contradict') after = Math.max(before - 2 * alpha, 0);
  else throw new Error('signal 只能是 reinforce / weaken / contradict');
  return updateMemory(state, id, { confidence: Number(after.toFixed(3)) });
}

/** 单条记忆的对外视图（listAll / 快照共用一份，别各写各的字段）。 */
export function listView(m: MemoryRecord): MemoryView {
  return {
    id: m.id,
    text: m.text,
    tags: m.tags || [],
    type: normalizeType(m.type, m.tags, m.text),
    confidence: m.confidence === undefined ? null : m.confidence,
    entities: m.entities || [],
    validFrom: m.validFrom || '',
    validTo: m.validTo || '',
    ts: m.ts,
    status: m.status || 'active',
    reason: m.reason || '',
  };
}

export function listAllItems(state: StoreState): MemoryView[] {
  return state.metas.map(listView);
}
