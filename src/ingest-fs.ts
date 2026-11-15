/**
 * 入库链的文件级纯函数（2026-11-15，从 ingest-index.js 抽出来）：
 * 扫会话文件、解压缓存、dry-run 判定、行读取、日期解析。
 *
 * 这些函数不持有任何状态：给路径/给 db 就出结果，所以不包成类，也不塞进 IngestQueue。
 */
import fs from 'node:fs';
import path from 'node:path';
import type { DatabaseSync } from 'node:sqlite';
import { isSessionFile, readSessionEvents, type SessionEvent } from './transcript.js';
import type { Row } from './store/types.js';

/** 会话文件条目：scanSessions 的产物；上游已算好 lastEventTs 时会带上（scanSessions 不填）。 */
export interface SessionFileInfo {
  path: string;
  mtime: number;
  size: number;
  lastEventTs?: number;
}

/**
 * dryRun 的零写开关：五个入口（bootstrapLegacy/ackFile/failFile/enqueue/markOp）
 * 统一在入口判一次，dryRun 时只打日志、一个字节都不碰库。
 * 返回 true 表示调用方应当停在这里。
 */
export function dryRunSkip(dryRun: unknown, log: ((line: string) => void) | null | undefined, what: string): boolean {
  if (!dryRun) return false;
  try {
    log?.(`[dry-run] ${what} 不落库`);
  } catch {
    /* 日志失败也不许反过来写库 */
  }
  return true;
}

export function dryRunRow(kind: string, dedupeKey: string, payloadJson: string, now: number, nextTryAt?: number) {
  return {
    id: null,
    kind,
    dedupe_key: String(dedupeKey),
    payload_json: payloadJson,
    attempts: 0,
    next_try_at: Number(nextTryAt) || now,
    last_error: null,
    status: 'dry_run',
    created_ts: now,
    updated_ts: now,
    dryRun: true,
  };
}

export function statFile(file: string): { mtime: number; size: number } | null {
  try {
    const st = fs.statSync(file);
    return { mtime: st.mtimeMs, size: st.size };
  } catch {
    return null;
  }
}

/**
 * 递归枚举会话文件（§3.2：不依赖层数），返回 [{path, mtime, size}]，按 mtime 升序。
 * 正则和 zstd 解码都走 transcript.js，别在这里复制一份。
 */
export function scanSessions(sessionsRoot: string): SessionFileInfo[] {
  const out: SessionFileInfo[] = [];
  const walk = (dir: string): void => {
    let entries = [];
    try {
      entries = fs.readdirSync(dir, { withFileTypes: true });
    } catch {
      return;
    }
    for (const entry of entries) {
      const full = path.join(dir, entry.name);
      if (entry.isDirectory()) {
        walk(full);
        continue;
      }
      if (!entry.isFile() || !isSessionFile(entry.name)) continue;
      let st;
      try {
        st = fs.statSync(full);
      } catch {
        continue;
      }
      out.push({ path: full, mtime: st.mtimeMs, size: st.size });
    }
  };
  walk(sessionsRoot);
  out.sort((a, b) => a.mtime - b.mtime || (a.path < b.path ? -1 : a.path > b.path ? 1 : 0));
  return out;
}

export function rowOf(db: DatabaseSync, filePath: string): Row | null {
  return db.prepare('SELECT * FROM ingest_files WHERE path = ?').get(filePath) ?? null;
}

/**
 * 解压结果在本轮内复用（§3.2）：cache 是 Map，最多留 retain 个文件，超了丢最老的。
 * 传 cache=null 表示只读一次不留。
 */
export function loadEvents(filePath: string, cache: Map<string, SessionEvent[]> | null, retain = 32): SessionEvent[] | null {
  if (cache && cache.has(filePath)) return cache.get(filePath) as SessionEvent[];
  const events = readSessionEvents(filePath);
  if (cache && events && retain > 0) {
    if (cache.size >= retain) cache.delete(cache.keys().next().value as string);
    cache.set(filePath, events);
  }
  return events;
}

/** '2026-10-01' → 当地 00:00 的毫秒。解析不了返回 null。 */
export function parseSinceDay(spec: unknown): number | null {
  const s = String(spec || '').trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(s)) return null;
  const ts = new Date(`${s}T00:00:00`).getTime();
  return Number.isFinite(ts) ? ts : null;
}
