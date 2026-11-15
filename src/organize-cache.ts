// 2026-11-09：organizeCache 主文件按条数轮转 + 溢出按月压缩归档。
//
// 主文件（sleep.state.json）里只留最近 organizeCacheMax 条（默认 200），按写入时间 t
// 从新到旧保留；更旧的按 t 归属月份搬进 <state 同级>/organize-archive/organize-YYYY-MM.jsonl.gz，
// 一行一条 {key, value, archivedAt}。
//
// 规矩：
// - 加载路径只读主文件，**不读归档**（readState 里没有半个字提到这个目录）。
// - 轮转只发生在写入处，且只碰溢出条目所属月份那 1~2 个文件，不 stat 整个目录。
// - gzip 追加用「解压旧文件 → 拼新行 → gzipSync → 临时文件 + rename」：
//   gzip 多 member 追加虽然合法，但 zcat 以外有些工具只读第一个 member；
//   读旧+拼新是单 member 单文件语义，失败时旧文件一个字节不动，最稳。
// - 归档文件只追加，绝不删、绝不用新内容覆盖旧内容（同 key 已归档的直接跳过）。
// - 任何一步失败都不从主文件删条目：主文件原样、只 warn，最坏退化成「不轮转」。
import fs from 'node:fs';
import path from 'node:path';
import zlib from 'node:zlib';
import * as paths from './shared/paths.js';

/** 主文件保留条数默认值；cfg.organizeCacheMax 可覆盖（schema 里同名，默认 200）。 */
export const DEFAULT_CACHE_MAX = 200;
/** 归档目录默认名，跟 sleep.state.json 同级。 */
export const DEFAULT_ARCHIVE_DIRNAME = 'organize-archive';

/** 本文件认的配置项：其余字段一概不看。 */
export interface OrganizeCacheConfig {
  organizeCacheMax?: unknown;
  organizeArchiveDir?: unknown;
  stateFile?: unknown;
  [key: string]: unknown;
}

/** 主文件里的一条缓存值：只读 t（写入时间），其余原样带走。 */
export interface CacheValue {
  t?: unknown;
  [key: string]: unknown;
}

/** 归档行：一行一条 {key, value, archivedAt}。 */
export interface ArchiveRow {
  key: string;
  value: unknown;
  archivedAt: string;
}

/** trimOrganizeCache 的产物；failed=true 时 kept 就是传进来的原 cache。 */
export interface TrimResult {
  kept: Record<string, unknown>;
  before: number;
  max: number;
  spilled: number;
  written: number;
  months: Record<string, number>;
  failed: boolean;
  error: string | null;
}

/** 归档目录里一个月份文件的摘要。 */
export interface ArchiveMonth {
  name: string;
  file: string;
  bytes: number;
  count: number;
  error: string | null;
}

export function cacheMaxOf(cfg: OrganizeCacheConfig = {}): number {
  const n = Number(cfg?.organizeCacheMax);
  if (Number.isFinite(n) && n >= 1) return Math.floor(n);
  return DEFAULT_CACHE_MAX;
}

export function archiveDirOf(cfg: OrganizeCacheConfig = {}): string {
  const explicit = String(cfg?.organizeArchiveDir || '').trim();
  if (explicit) return explicit;
  const stateFile = String(cfg?.stateFile || paths.memoryFile('sleep.state.json'));
  return path.join(path.dirname(stateFile), DEFAULT_ARCHIVE_DIRNAME);
}

/** 写入时间归属月份，本地时区（睡觉整理按本地 05:00/12:30 跑，月份边界跟维护者看的一致）。 */
export function monthKeyOf(ts: unknown): string {
  const n = Number(ts);
  if (!Number.isFinite(n) || n <= 0) return 'unknown';
  const d = new Date(n);
  if (Number.isNaN(d.getTime())) return 'unknown';
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
}

export function archiveFileOf(dir: string, month: string): string {
  return path.join(dir, `organize-${month}.jsonl.gz`);
}

/** 读旧归档（解压 + 逐行解析），同时拿到已归档 key 集合。坏文件抛错，交给调用方降级。 */
function readArchive(file: string): { keys: Set<string>; buf: Buffer } {
  if (!fs.existsSync(file)) return { keys: new Set(), buf: Buffer.alloc(0) };
  const buf = zlib.gunzipSync(fs.readFileSync(file));
  const keys = new Set<string>();
  for (const line of buf.toString('utf8').split('\n')) {
    const s = line.trim();
    if (!s) continue;
    let row: { key?: unknown } | null;
    try {
      row = JSON.parse(s);
    } catch {
      throw new Error(`归档文件有坏行：${file}`);
    }
    if (!row || typeof row.key !== 'string') throw new Error(`归档行缺 key：${file}`);
    keys.add(row.key);
  }
  return { keys, buf };
}

/** 把 rows 追加进 file；同 key 跳过。返回新写入行数。失败直接抛，由上层统一降级。 */
function appendArchiveRows(file: string, rows: ArchiveRow[]): number {
  const { keys, buf } = readArchive(file);
  const fresh = rows.filter((r) => !keys.has(r.key));
  if (!fresh.length) return 0;
  const sep = buf.length && buf[buf.length - 1] !== 0x0a ? Buffer.from('\n') : Buffer.alloc(0);
  const body = Buffer.from(`${fresh.map((r) => JSON.stringify(r)).join('\n')}\n`, 'utf8');
  const gz = zlib.gzipSync(Buffer.concat([buf, sep, body]));
  const tmp = `${file}.tmp-${process.pid}-${Date.now()}`;
  fs.writeFileSync(tmp, gz);
  // rename 是原子替换：新文件包含旧文件全部内容 + 新行，不会出现半截 gzip
  fs.renameSync(tmp, file);
  return fresh.length;
}

/**
 * 超阈值就把最旧的条目按月归档，返回主文件该保留的 cache。
 *
 * 返回值：{ kept, before, max, spilled, written, months, failed, error }
 * - kept 是主文件该写回的 cache（对象，按 t 从新到旧）。
 * - failed=true 时 kept 就是传进来的原 cache，一个字都不删。
 */
export function trimOrganizeCache(cfg: OrganizeCacheConfig, cache: Record<string, CacheValue>, { now = Date.now(), warn }: { now?: number; warn?: (line: string) => void } = {}): TrimResult {
  const warnFn = typeof warn === 'function' ? warn : () => {};
  const source: Record<string, CacheValue> = cache && typeof cache === 'object' ? cache : {};
  const entries = Object.entries(source);
  const max = cacheMaxOf(cfg);
  const verdict: TrimResult = {
    kept: source,
    before: entries.length,
    max,
    spilled: 0,
    written: 0,
    months: {},
    failed: false,
    error: null,
  };
  if (entries.length <= max) return verdict;

  const sorted = entries
    .slice()
    .sort((a, b) => (Number(b[1]?.t) || 0) - (Number(a[1]?.t) || 0));
  const keepEntries = sorted.slice(0, max);
  const spill = sorted.slice(max);

  const groups = new Map<string, ArchiveRow[]>();
  for (const [key, value] of spill) {
    const month = monthKeyOf(value?.t);
    if (!groups.has(month)) groups.set(month, []);
    groups.get(month)!.push({ key, value, archivedAt: new Date(now).toISOString() });
  }

  const dir = archiveDirOf(cfg);
  const months: Record<string, number> = {};
  let written = 0;
  try {
    fs.mkdirSync(dir, { recursive: true });
    for (const [month, rows] of groups) {
      const n = appendArchiveRows(archiveFileOf(dir, month), rows);
      months[month] = n;
      written += n;
    }
  } catch (error) {
    const msg = (error as { message?: unknown })?.message as string || String(error);
    verdict.failed = true;
    verdict.error = msg;
    verdict.kept = source;
    // 已写成功的月份也报出来，方便排查；但主文件不删条目
    verdict.months = months;
    verdict.written = written;
    warnFn(`organizeCache 归档失败，主文件保持原样不裁剪：${msg}`);
    return verdict;
  }

  verdict.kept = Object.fromEntries(keepEntries);
  verdict.spilled = spill.length;
  verdict.written = written;
  verdict.months = months;
  return verdict;
}

/** 导出用：列出归档目录里各月份文件与条数（只读，解压计数）。 */
export function listArchiveMonths(dir: string): ArchiveMonth[] {
  let names: string[] = [];
  try {
    names = fs.readdirSync(dir).filter((n) => /^organize-.*\.jsonl\.gz$/.test(n)).sort();
  } catch {
    return [];
  }
  return names.map((name) => {
    const file = path.join(dir, name);
    let count = 0;
    let bytes = 0;
    let error: string | null = null;
    try {
      bytes = fs.statSync(file).size;
      const buf = zlib.gunzipSync(fs.readFileSync(file));
      count = buf.toString('utf8').split('\n').filter((l) => l.trim()).length;
    } catch (e) {
      error = (e as { message?: unknown })?.message as string || String(e);
    }
    return { name, file, bytes, count, error };
  });
}

/** 导出用：读单个归档文件并返回行数组（带月份字段方便打印）。 */
export function readArchiveRows(file: string): unknown[] {
  const buf = zlib.gunzipSync(fs.readFileSync(file));
  const rows: unknown[] = [];
  for (const line of buf.toString('utf8').split('\n')) {
    const s = line.trim();
    if (!s) continue;
    rows.push(JSON.parse(s));
  }
  return rows;
}
