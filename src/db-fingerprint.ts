/**
 * 真库"有没有被写"的逻辑指纹（2026-11-05 t-fix-first-run）。
 *
 * 为什么不看文件 sha：真库是 WAL 模式，qqbot 常驻读，新写全先落 memory.db-wal，
 * 主文件可以一动不动；反过来 TRUNCATE checkpoint 抢不到锁，拷出来的 sha 也不代表
 * "这一刻没人写"。所以只认逻辑数字：READONLY 连接里开一个读事务，一次取完
 * memories 行数 / vec_map 行数 / max(rowSeq) / ingest_files / pending_ops / discards 行数。
 *
 * 注意：数字变了不一定是我们写的——常驻 qqbot 也在写同一个库。diff 要把变了哪几个
 * 字段打出来，操作者自己判断是谁的手笔。
 */
import crypto from 'node:crypto';
import fs from 'node:fs';
import { DatabaseSync } from 'node:sqlite';

/** 要比的计数列；缺表记 null，别把"表不存在"当成"0 行"糊过去。 */
export const LOGICAL_COUNTS: ReadonlyArray<readonly [string, string]> = [
  ['memories', 'SELECT count(*) AS n FROM memories'],
  ['vec_map', 'SELECT count(*) AS n FROM vec_map'],
  ['ingest_files', 'SELECT count(*) AS n FROM ingest_files'],
  ['pending_ops', 'SELECT count(*) AS n FROM pending_ops'],
  ['discards', 'SELECT count(*) AS n FROM discards'],
];

const DIFF_KEYS = ['memories', 'vec_map', 'maxRowSeq', 'ingest_files', 'pending_ops', 'discards'];

/** 一次逻辑指纹的产物：计数列为 number，表不存在为 null，打不开时只有 path + error。 */
export interface LogicalFingerprint {
  path: string;
  error?: string;
  memories?: number | null;
  vec_map?: number | null;
  ingest_files?: number | null;
  pending_ops?: number | null;
  discards?: number | null;
  maxRowSeq?: number | null;
  canonical?: string;
  digest?: string;
  [key: string]: unknown;
}

/** 单个文件层的参考信息（只打印、不判定）。 */
export interface FileInfo {
  path: string;
  [key: string]: unknown;
}

/**
 * 取一次逻辑指纹。打不开就返回 {error}，绝不退回文件 sha 假装成功。
 * @param file 库路径
 */
export function logicalFingerprint(file: string): LogicalFingerprint {
  let db: DatabaseSync | null = null;
  try {
    db = new DatabaseSync(file, { readOnly: true });
    // 一个读事务里取完几个数，避免"取到一半有人写了"取成拼接货
    db.exec('BEGIN');
    const out: LogicalFingerprint = { path: file };
    for (const [key, sql] of LOGICAL_COUNTS) {
      try {
        out[key] = Number(db.prepare(sql).get()?.n ?? 0);
      } catch {
        out[key] = null;
      }
    }
    try {
      const row = db.prepare('SELECT max(rowSeq) AS seq FROM memories').get();
      out.maxRowSeq = row && row.seq !== null && row.seq !== undefined ? Number(row.seq) : null;
    } catch {
      out.maxRowSeq = null;
    }
    db.exec('COMMIT');
    out.canonical = `${LOGICAL_COUNTS.map(([key]) => `${key}=${out[key]}`).join(' ')} maxRowSeq=${out.maxRowSeq}`;
    out.digest = crypto.createHash('sha256').update(out.canonical).digest('hex').slice(0, 16);
    return out;
  } catch (error) {
    return { path: file, error: String((error as { message?: unknown })?.message || error) };
  } finally {
    try { db?.close(); } catch { /* 关不掉不影响判定 */ }
  }
}

/**
 * 逐字段列出差异；空数组 = 逻辑上没动。取不到指纹时返回一条说明，调用方据此判失败。
 */
export function diffFingerprint(before: LogicalFingerprint, after: LogicalFingerprint): string[] {
  if (before?.error || after?.error) return [`无法比对：${before?.error || after?.error}`];
  return DIFF_KEYS.filter((key) => before[key] !== after[key]).map((key) => `${key}: ${before[key]} → ${after[key]}`);
}

/**
 * 文件层面的参考信息（主库 / -wal / -shm 的大小和 mtime），只打印、不判定。
 * @param file
 */
export function fileInfo(file: string): FileInfo {
  const info: FileInfo = { path: file };
  for (const [key, target] of [['db', file], ['wal', `${file}-wal`], ['shm', `${file}-shm`]]) {
    try {
      const st = fs.statSync(target);
      info[key] = { size: st.size, mtimeMs: Math.round(st.mtimeMs) };
    } catch {
      info[key] = null;
    }
  }
  return info;
}
