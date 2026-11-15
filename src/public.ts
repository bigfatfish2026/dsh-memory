/**
 * dsh-memory 的只读公共接口。
 *
 * 别的插件/脚本要读记忆，一律走这里，不许再直接读 memories.jsonl / vectors.bin。
 * 本模块只碰 SQLite（memory.db），不出现 jsonl 路径；缺 memories 表时抛明确错误，
 * 不静默返回空数组。
 *
 * 用法：
 *   import { createMemoryReader } from '<plugin>/lib/public.js';
 *   const reader = createMemoryReader();               // 默认真库
 *   const rows = reader.list({ status: 'active' });
 *   const one = reader.get('m_xxx');
 *   const vec = reader.allWithVectors({ includeMissing: false });
 *   reader.migrated();                                  // true/false
 *
 * 库默认路径可用 opts.dbPath / opts.db 或环境变量 DSH_MEMORY_DB 覆盖（测试指向副本）。
 */
import fs from 'node:fs';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import * as paths from './shared/paths.js';

export const DEFAULT_DB_PATH = paths.dbPath();

/** createMemoryReader 的入参：dbPath 优先，其次 db，再退环境变量。 */
export interface MemoryReaderOptions {
  dbPath?: string;
  db?: string;
}

/** list() 的过滤条件。 */
export interface MemoryListOptions {
  status?: string;
  type?: string;
  limit?: number | null;
}

/** count() 的过滤条件（同 list，不含 limit）。 */
export interface MemoryCountOptions {
  status?: string;
  type?: string;
}

/** allWithVectors() 的入参。 */
export interface MemoryVectorOptions {
  includeMissing?: boolean;
  limit?: number | null;
}

/** list()/get() 返回的一行：字段跟 memories 表列名一一对应。 */
export interface MemoryRowView {
  // 建表 DDL 里只有 text / rowSeq 是 NOT NULL；id（TEXT PRIMARY KEY）与 ts / type 都允许 NULL。
  id: string | null;
  text: string;
  tags: unknown[];
  source: string | null;
  ts: string | null;
  type: string | null;
  entities: unknown[];
  validFrom: string | null;
  supersedes: unknown[];
  updatedAt: string | null;
  status: string;
  statusRaw: string | null;
  reason: string | null;
  supersededBy: string | null;
  validTo: string | null;
  confidence: number | null;
  extra: Record<string, unknown>;
  rowSeq: number;
}

/** allWithVectors() 的一条：正文 + 向量，可能带 skipped 原因。 */
export interface MemoryVectorItem {
  id: string;
  text: string | null;
  tags: unknown[];
  type: string | null;
  ts: string | null;
  status: string;
  embedding: Float32Array | null;
  skipped?: 'no-vector' | 'missing-memory';
}

/** 只读句柄的对外面。 */
export interface MemoryReader {
  dbPath: string;
  migrated(): boolean;
  list(options?: MemoryListOptions): MemoryRowView[];
  count(options?: MemoryCountOptions): number;
  get(id: string): MemoryRowView | null;
  allWithVectors(options?: MemoryVectorOptions): MemoryVectorItem[];
  close(): void;
}

export class MemoryReaderError extends Error {
  declare code: string;

  constructor(message: string, code?: string) {
    super(message);
    this.name = 'MemoryReaderError';
    this.code = code ?? 'EMEMORY';
  }
}

export class MemoryNotMigratedError extends MemoryReaderError {
  declare dbPath: string;

  constructor(dbPath: string) {
    super(
      `库 ${dbPath} 里没有 memories 表：这个库还没迁移，先跑 `
      + 'plugins/dsh-memory/scripts/migrate-jsonl-to-sqlite.mjs。只读接口不读 jsonl。',
      'ENOTMIGRATED',
    );
    this.name = 'MemoryNotMigratedError';
    this.dbPath = dbPath;
  }
}

function findVecExtension(): string | null {
  const candidates = [
    path.join(import.meta.dirname, '../node_modules/sqlite-vec-linux-arm64/vec0.so'),
  ];
  for (const candidate of candidates) if (fs.existsSync(candidate)) return candidate;
  return null;
}

function parseJsonField(value: unknown, fallback: unknown): unknown {
  if (value === null || value === undefined || value === '') return fallback;
  if (typeof value !== 'string') return value;
  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

function toFloat32(value: unknown): Float32Array | null {
  if (!value) return null;
  if (value instanceof Float32Array) return value;
  if (ArrayBuffer.isView(value)) {
    const bytes = new Uint8Array(value.byteLength);
    bytes.set(value as unknown as ArrayLike<number>);
    return new Float32Array(bytes.buffer, 0, Math.floor(bytes.byteLength / 4));
  }
  if (Array.isArray(value)) return Float32Array.from(value as number[]);
  return null;
}

function normalizeStatus(value: unknown): string {
  return value === null || value === undefined || value === '' ? 'active' : value as string;
}

/** TEXT 列可能为 NULL（DDL 如此），原样透传，不替空值编默认值。 */
function asNullableString(value: unknown): string | null {
  return value === null || value === undefined ? null : value as string;
}

/** REAL/INTEGER 列可能为 NULL，原样透传。 */
function asNullableNumber(value: unknown): number | null {
  return value === null || value === undefined ? null : value as number;
}

function mapRow(row: Record<string, unknown>): MemoryRowView {
  // 逐字段构造，让类型跟实际返回值对齐：可空列就写可空，不靠 as unknown as 蒙过去。
  return {
    id: asNullableString(row.id),
    text: row.text as string,
    tags: parseJsonField(row.tags, []) as unknown[],
    source: asNullableString(row.source),
    ts: asNullableString(row.ts),
    type: asNullableString(row.type),
    entities: parseJsonField(row.entities, []) as unknown[],
    validFrom: asNullableString(row.validFrom),
    supersedes: parseJsonField(row.supersedes, []) as unknown[],
    updatedAt: asNullableString(row.updatedAt),
    status: normalizeStatus(row.status),
    statusRaw: asNullableString(row.status),
    reason: asNullableString(row.reason),
    supersededBy: asNullableString(row.supersededBy),
    validTo: asNullableString(row.validTo),
    confidence: asNullableNumber(row.confidence),
    extra: parseJsonField(row.extra, {}) as Record<string, unknown>,
    rowSeq: row.rowSeq as number,
  };
}

const SELECT_FIELDS = 'id, text, tags, source, ts, type, entities, validFrom, supersedes, '
  + 'updatedAt, status, reason, supersededBy, validTo, confidence, extra, rowSeq';

export function createMemoryReader(options: MemoryReaderOptions = {}): MemoryReader {
  const dbPath = path.resolve(options.dbPath || options.db || process.env.DSH_MEMORY_DB || DEFAULT_DB_PATH);
  let db: DatabaseSync | null = null;
  let vecLoaded = false;

  function open(): DatabaseSync {
    if (db) return db;
    if (!fs.existsSync(dbPath)) {
      throw new MemoryReaderError(`记忆库不存在：${dbPath}`, 'ENOENT');
    }
    try {
      db = new DatabaseSync(dbPath, { readOnly: true, allowExtension: true });
      db.exec('PRAGMA busy_timeout = 5000;');
    } catch (error) {
      throw new MemoryReaderError(`打不开记忆库 ${dbPath}（只读）：${(error as Error).message}`, 'EOPEN');
    }
    return db;
  }

  function hasMemories(): boolean {
    return Boolean(open().prepare(
      "SELECT 1 AS ok FROM sqlite_master WHERE type='table' AND name='memories'",
    ).get());
  }

  function ensureMigrated() {
    if (!hasMemories()) throw new MemoryNotMigratedError(dbPath);
  }

  function ensureVec() {
    if (vecLoaded) return;
    const ext = findVecExtension();
    if (!ext) throw new MemoryReaderError('找不到 sqlite-vec 的 vec0.so，读不了向量', 'ENOVEC');
    try {
      open().loadExtension(ext);
      vecLoaded = true;
    } catch (error) {
      throw new MemoryReaderError(`加载 vec0 扩展失败：${(error as Error).message}`, 'ENOVEC');
    }
  }

  return {
    dbPath,

    /** 表在不在、迁移没迁移。库文件不存在直接抛，缺表返回 false。 */
    migrated(): boolean {
      return hasMemories();
    },

    /** 按 status/type 过滤；默认不设上限，按原 jsonl 行序（rowSeq）返回。 */
    list({ status, type, limit = null }: MemoryListOptions = {}): MemoryRowView[] {
      ensureMigrated();
      const where = [];
      const params = [];
      if (status !== undefined && status !== null && status !== '') {
        where.push("COALESCE(NULLIF(status, ''), 'active') = ?");
        params.push(status);
      }
      if (type !== undefined && type !== null && type !== '') {
        where.push('type = ?');
        params.push(type);
      }
      let sql = `SELECT ${SELECT_FIELDS} FROM memories`;
      if (where.length) sql += ` WHERE ${where.join(' AND ')}`;
      sql += ' ORDER BY rowSeq ASC';
      if (Number.isFinite(limit) && (limit as number) >= 0) {
        sql += ' LIMIT ?';
        params.push(Math.trunc(limit as number));
      }
      return open().prepare(sql).all(...params).map(mapRow);
    },

    /** 计数（过滤条件同 list），不把整表拉出来。 */
    count({ status, type }: MemoryCountOptions = {}): number {
      ensureMigrated();
      const where = [];
      const params = [];
      if (status !== undefined && status !== null && status !== '') {
        where.push("COALESCE(NULLIF(status, ''), 'active') = ?");
        params.push(status);
      }
      if (type !== undefined && type !== null && type !== '') {
        where.push('type = ?');
        params.push(type);
      }
      let sql = 'SELECT count(*) AS c FROM memories';
      if (where.length) sql += ` WHERE ${where.join(' AND ')}`;
      return open().prepare(sql).get(...params)!.c as number;
    },

    /** 单条；没有返回 null。 */
    get(id: string): MemoryRowView | null {
      ensureMigrated();
      const row = open().prepare(`SELECT ${SELECT_FIELDS} FROM memories WHERE id = ?`).get(id);
      return row ? mapRow(row) : null;
    },

    /**
     * 正文 + 向量一起拿。默认只返回「正文和向量都在」的条目，embedding 是 Float32Array。
     * includeMissing=true 时补上没向量的记忆（skipped:'no-vector'）和
     * 找不到正文的孤儿向量（skipped:'missing-memory'，embedding 仍有值）。
     */
    allWithVectors({ includeMissing = false, limit = null }: MemoryVectorOptions = {}): MemoryVectorItem[] {
      ensureMigrated();
      const cap = Number.isFinite(limit) && (limit as number) >= 0 ? Math.trunc(limit as number) : null;
      const read = open();
      const mapped: MemoryVectorItem[] = [];
      const add = (row: Record<string, unknown>, skipped: 'no-vector' | 'missing-memory' | null, embedding: Float32Array | null) => {
        if (cap !== null && mapped.length >= cap) return;
        mapped.push({
          id: row.id as string,
          text: row.text as string,
          tags: parseJsonField(row.tags, []) as unknown[],
          type: row.type as string,
          ts: row.ts as string,
          status: normalizeStatus(row.status),
          embedding,
          ...(skipped ? { skipped } : {}),
        });
      };

      let hasVecMap = false;
      try {
        hasVecMap = Boolean(read.prepare(
          "SELECT 1 AS ok FROM sqlite_master WHERE type='table' AND name='vec_map'",
        ).get());
      } catch (err) {
        console.warn('[dsh-memory] 读 vec_map 失败，按无向量处理：', (err as Error)?.message || String(err));
      }
      if (hasVecMap) {
        ensureVec();
        const rows = read.prepare(
          `SELECT m.id, m.text, m.tags, m.type, m.ts, m.status, m.rowSeq,
                  i.embedding
             FROM memories m
             JOIN vec_map v ON v.id = m.id
             JOIN vec_items i ON i.rowid = v.rowid
            ORDER BY m.rowSeq ASC`,
        ).all();
        for (const row of rows) add(row, null, toFloat32(row.embedding));
        if (includeMissing) {
          const noVec = read.prepare(
            `SELECT m.id, m.text, m.tags, m.type, m.ts, m.status, m.rowSeq
               FROM memories m LEFT JOIN vec_map v ON v.id = m.id
              WHERE v.id IS NULL ORDER BY m.rowSeq ASC`,
          ).all();
          for (const row of noVec) add(row, 'no-vector', null);
          const orphan = read.prepare(
            `SELECT v.id, i.embedding, v.rowid AS vecRowid
               FROM vec_map v JOIN vec_items i ON i.rowid = v.rowid
               LEFT JOIN memories m ON m.id = v.id
              WHERE m.id IS NULL ORDER BY v.rowid ASC`,
          ).all();
          for (const row of orphan) {
            add({ id: row.id, text: null, tags: null, type: null, ts: null, status: null },
              'missing-memory', toFloat32(row.embedding));
          }
        }
        return mapped;
      }

      if (includeMissing) {
        for (const row of read.prepare(`SELECT ${SELECT_FIELDS} FROM memories ORDER BY rowSeq ASC`).all()) {
          add(row, 'no-vector', null);
        }
        return mapped;
      }
      throw new MemoryReaderError(`库 ${dbPath} 里没有 vec_map（没有向量表），拿不到向量`, 'ENOVEC');
    },

    close(): void {
      try { db?.close(); } catch {}
      db = null;
      vecLoaded = false;
    },
  };
}
