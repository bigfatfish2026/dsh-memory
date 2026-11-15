/**
 * memories 表的行级映射与 rowSeq 记账。
 *
 * 这里只做「SQL 行 ↔ 内存记录」和取号/读表，不碰业务判断（那在 records.js）。
 * rowSeq 取号必须在写事务**里面**（2026-11-06 t-fix-quality）：
 * 以前 `maxRowSeq + 1` 在进程内存里算，--live 的另一个写入者同时也在算同一个数，
 * 真库因此出现 82 对重复 rowSeq（164 行）。BEGIN IMMEDIATE 已经拿了写锁，
 * 事务里 SELECT MAX(rowSeq)+1 就是全局唯一的下一个号。
 */
import { clamp01, dateOnly, entityList, normalizeType } from './types.js';
import type { MemoryRecord, MemoryRow, Row, StoreState } from './types.js';
import type { DatabaseSync } from 'node:sqlite';

/* ── rows ↔ 内存记录 ────────────────────────────────────────────────────
 * memories 表的列就是 jsonl 那 15 个 key + extra / present / rowSeq。
 * 读进来尽量还原成 jsonl 记录的形状（缺的键就不出现，status 缺失仍是 active），
 * 下游 normalizeRecord 及所有工具都不用改。
 */
export const MEMORY_COLUMNS = [
  'id', 'text', 'tags', 'source', 'ts', 'type', 'entities', 'validFrom',
  'supersedes', 'updatedAt', 'status', 'reason', 'supersededBy', 'validTo',
  'confidence', 'extra', 'present', 'rowSeq',
];
const MEMORY_PLAIN_COLUMNS = [
  'source', 'ts', 'type', 'validFrom', 'updatedAt', 'status', 'reason', 'supersededBy', 'validTo',
];
const MEMORY_JSON_COLUMNS = ['tags', 'entities', 'supersedes'];
export const UPSERT_MEMORY_SQL = `INSERT INTO memories (${MEMORY_COLUMNS.join(', ')})
VALUES (${MEMORY_COLUMNS.map(() => '?').join(', ')})
ON CONFLICT(id) DO UPDATE SET ${MEMORY_COLUMNS.filter((c) => c !== 'id').map((c) => `${c}=excluded.${c}`).join(', ')}`;

function parseJsonColumn(value: unknown, fallback: unknown): unknown {
  if (value === null || value === undefined || value === '') return fallback;
  if (typeof value !== 'string') return value;
  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

export function rowToRecord(row: Row): MemoryRecord {
  const rec: MemoryRecord = { id: row.id as string, text: row.text as string };
  for (const key of MEMORY_PLAIN_COLUMNS) {
    if (row[key] !== null && row[key] !== undefined) rec[key] = row[key];
  }
  for (const key of MEMORY_JSON_COLUMNS) {
    if (row[key] !== null && row[key] !== undefined) rec[key] = parseJsonColumn(row[key], []);
  }
  if (row.confidence !== null && row.confidence !== undefined) rec.confidence = Number(row.confidence);
  if (row.extra) {
    const extra = parseJsonColumn(row.extra, {}) as Row;
    for (const key of Object.keys(extra)) if (!(key in rec)) rec[key] = extra[key];
  }
  return rec;
}

export function recordToRow(rec: MemoryRecord, rowSeq: number): MemoryRow {
  const extra: Row = {};
  for (const key of Object.keys(rec)) {
    if (key === 'id' || MEMORY_COLUMNS.includes(key)) continue;
    extra[key] = rec[key];
  }
  const row = {
    id: rec.id,
    text: rec.text,
    tags: rec.tags === undefined ? null : JSON.stringify(rec.tags),
    source: rec.source ?? null,
    ts: rec.ts ?? null,
    type: rec.type ?? null,
    entities: rec.entities === undefined ? null : JSON.stringify(rec.entities),
    validFrom: rec.validFrom ?? null,
    supersedes: rec.supersedes === undefined ? null : JSON.stringify(rec.supersedes),
    updatedAt: rec.updatedAt ?? null,
    status: rec.status ?? null,
    reason: rec.reason ?? null,
    supersededBy: rec.supersededBy ?? null,
    validTo: rec.validTo ?? null,
    confidence: rec.confidence === undefined ? null : Number(rec.confidence),
    extra: Object.keys(extra).length > 0 ? JSON.stringify(extra) : null,
    present: JSON.stringify(Object.keys(rec)),
    rowSeq,
  };
  return row;
}

/** 从 memories 表按 rowSeq 读出全部记录，内存结构跟原来的 jsonl 记录一致 */
export function loadMemories(state: StoreState): MemoryRecord[] {
  const rows = state.connect().prepare(
    `SELECT ${MEMORY_COLUMNS.join(', ')} FROM memories ORDER BY rowSeq, id`,
  ).all();
  const out = [];
  state.rowSeqById.clear();
  state.maxRowSeq = 0;
  for (const row of rows) {
    const seq = Number(row.rowSeq);
    state.rowSeqById.set(row.id as string, seq);
    if (seq > state.maxRowSeq) state.maxRowSeq = seq;
    out.push(normalizeRecord(rowToRecord(row)));
  }
  return out;
}

export function upsertRow(state: StoreState, rec: MemoryRecord, rowSeq: number): void {
  const row = recordToRow(rec, rowSeq);
  state.connect().prepare(UPSERT_MEMORY_SQL).run(...MEMORY_COLUMNS.map((c) => row[c]));
}

export function allocRowSeq(d: DatabaseSync): number {
  const row = d.prepare('SELECT COALESCE(MAX(rowSeq), 0) + 1 AS n FROM memories').get();
  return Number(row?.n) || 1;
}

/** 老条改正文时保住它原来的 rowSeq；内存映射没这条（别的进程插的）就现查一次。 */
export function existingRowSeq(state: StoreState, d: DatabaseSync, id: string): number | null | undefined {
  if (state.rowSeqById.has(id)) return state.rowSeqById.get(id);
  const row = d.prepare('SELECT rowSeq FROM memories WHERE id = ?').get(String(id));
  return row ? Number(row.rowSeq) : null;
}

export function normalizeRecord(m: MemoryRecord): MemoryRecord {
  const tags = Array.isArray(m.tags) ? m.tags.map(String) : [];
  const type = normalizeType(m.type, tags, m.text);
  const rec: MemoryRecord = { ...m, tags, type };
  if (type === 'opinion') rec.confidence = clamp01(m.confidence, 0.7);
  else if (m.confidence !== undefined && m.confidence !== null) rec.confidence = clamp01(m.confidence, 1);
  const entities = entityList(m.entities);
  if (entities.length) rec.entities = entities;
  const from = dateOnly(m.validFrom) || dateOnly(m.ts) || '';
  if (from) rec.validFrom = from;
  const to = dateOnly(m.validTo);
  if (to) rec.validTo = to;
  if (Array.isArray(m.supersedes) && m.supersedes.length) rec.supersedes = m.supersedes.map(String);
  return rec;
}

export function isActive(m: MemoryRecord): boolean {
  return (m.status || 'active') === 'active';
}

