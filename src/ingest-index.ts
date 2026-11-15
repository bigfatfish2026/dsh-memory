/**
 * 会话入库账本的对外面。
 *
 * 实现搬进 src/ingest-queue.ts 的 IngestQueue 类（db、每通道 Ledger 缓存、channel 自愈标记
 * 都是它的私有字段）。本文件只保留旧导出名和旧函数签名，调用方与测试一个字不用改：
 * 每个函数按 db 取同一个实例（WeakMap 注册表），再转调方法。
 *
 * 纯函数（scanSessions / parseSinceDay / dryRun 判定 / 行拼装）不包成方法，
 * 直接从 ingest-queue.js 转出。
 */
import {
  IngestQueue,
  ingestQueueOf,
  ensurePendingKinds,
  ensureChannelColumn,
  pendingOpsOps,
  hasIngestTables,
  MAX_ATTEMPTS,
  LEASE_MS,
  CHANNELS,
  KINDS_OF_CHANNEL,
  channelOfKind,
} from './ingest-queue.js';
import type { DatabaseSync } from 'node:sqlite';
import type { Row } from './store/types.js';
import type { SessionEvent } from './transcript.js';
import { scanSessions, parseSinceDay, type SessionFileInfo } from './ingest-fs.js';

export {
  IngestQueue,
  ingestQueueOf,
  ensurePendingKinds,
  ensureChannelColumn,
  pendingOpsOps,
  hasIngestTables,
  MAX_ATTEMPTS,
  LEASE_MS,
  CHANNELS,
  KINDS_OF_CHANNEL,
  channelOfKind,
  scanSessions,
  parseSinceDay,
};

/** 一条到期活（dueOps / claim 的产物）：payload 是解析后的对象，payloadJson 是原文。 */
export interface DueOp {
  id: string | number;
  kind: string;
  dedupeKey?: string;
  payload: Row | null;
  payloadJson: string;
  attempts: number;
  nextTryAt?: number;
}

/** planFiles 里待处理的文件条目；lastEventTs 是文件最后一条事件的时间。 */
export interface PlanFileEntry {
  path: string;
  mtime: number;
  size: number;
  lastAckedTs: number;
  lastEventTs: number;
  attempts: number;
  status: string;
  reason: string;
}

/** planFiles 里等 bootstrap 标的旧文件：只有文件三要素与最后事件时间。 */
export interface PlanLegacyEntry {
  path: string;
  mtime: number;
  size: number;
  lastEventTs: number;
}

/** planFiles 的四类结果；done 是路径字符串，其余是条目。 */
export interface PlanFilesResult {
  todo: PlanFileEntry[];
  overBudget: PlanFileEntry[];
  legacy: PlanLegacyEntry[];
  done: string[];
}

/** bootstrapLegacy 的计数结果。 */
export interface BootstrapResult {
  scanned: number;
  legacy: number;
  pending: number;
  skipped: number;
  errors: number;
  dryRun: boolean;
  lastSleepAt: number;
}

/** ackFile 的落库结果（dryRun 时多一个 dryRun:true）。 */
export interface AckResult {
  path: string;
  lastEventTs: number;
  status: string;
  mtime: number;
  size: number;
  dryRun?: boolean;
}

/** failFile 的落库结果（dryRun 时多一个 dryRun:true）。 */
export interface FailResult {
  path: string;
  attempts: number;
  status: string;
  nextTryAt: number;
  lastError: string | null;
  dryRun?: boolean;
}

/** backfill 的改动清单。 */
export interface BackfillResult {
  mode: string;
  changed: string[];
  skipped: Array<{ path: string; reason: string }>;
  since: number | null;
}

/** 通道各状态计数；通道没活（表不在 / kinds 为空）时适配器只回 total，其余字段缺省。 */
export interface ChannelStats {
  total: number;
  pending: number;
  running: number;
  done: number;
  dead: number;
  skipped?: number;
  llm_calls?: number;
  local_ops?: number;
}

/**
 * 按通道领一批（池的唯一取活入口）。limit 由调用方的成本通道并发数决定，
 * 不许一次把整队标 running。
 */
export function claimChannelOps(db: DatabaseSync, channel: string, limit: number, { now = Date.now(), leaseMs = LEASE_MS, kinds: only = null, exclude = [] }: { now?: number; leaseMs?: number; kinds?: string[] | null; exclude?: unknown[] } = {}): DueOp[] {
  return ingestQueueOf(db).claim(channel, limit, { now, leaseMs, kinds: only, exclude });
}

/** 租约回收：running 超过 leaseMs 没更新，视为进程崩了，捞回 pending。 */
export function reapChannelOps(db: DatabaseSync, { now = Date.now(), leaseMs = LEASE_MS, channel = null }: { now?: number; leaseMs?: number; channel?: string | null } = {}): number {
  return ingestQueueOf(db).reap(channel, { now, leaseMs });
}

/** 某通道各状态条数（池汇总、队列可观测用）。 */
export function channelStats(db: DatabaseSync, channel: string): ChannelStats {
  return ingestQueueOf(db).stats(channel);
}

/** bootstrap（§6.3，首次运行）：不重挖历史。 */
export function bootstrapLegacy(db: DatabaseSync, files: SessionFileInfo[], lastSleepAt: number, opts: { cache?: Map<string, SessionEvent[]> | null; now?: number; dryRun?: boolean; log?: ((line: string) => void) | null } = {}): BootstrapResult {
  return ingestQueueOf(db).bootstrapLegacy(files, lastSleepAt, opts);
}

/** 待处理判定（§3.2）：没行 / mtime|size 变了 / last_acked_ts < 文件最后事件时间。 */
export function planFiles(db: DatabaseSync, files: SessionFileInfo[], opts: { lastSleepAt?: number; budget?: number; cache?: Map<string, SessionEvent[]> | null } = {}): PlanFilesResult {
  return ingestQueueOf(db).planFiles(files, opts);
}

/** 整文件 ack：一个事务写 last_acked_ts/mtime/size/status='done'/attempts=0。 */
export function ackFile(db: DatabaseSync, filePath: string, lastEventTs: number, opts: { status?: string; mtime?: number; size?: number; now?: number; dryRun?: boolean; log?: ((line: string) => void) | null } = {}): AckResult {
  return ingestQueueOf(db).ackFile(filePath, lastEventTs, opts);
}

/** 单文件失败（§3.4）：attempts+1、last_error 落库、不推进 last_acked_ts。 */
export function failFile(db: DatabaseSync, filePath: string, err: unknown, opts: { now?: number; dryRun?: boolean; log?: ((line: string) => void) | null } = {}): FailResult {
  return ingestQueueOf(db).failFile(filePath, err, opts);
}

/** 幂等入队（§4.1）：UNIQUE(kind, dedupe_key)。 */
export function enqueue(db: DatabaseSync, kind: string, dedupeKey: string, payload: unknown, opts: { now?: number; nextTryAt?: number; dryRun?: boolean; log?: ((line: string) => void) | null; reviveDead?: boolean; reviveKeys?: string[] | null; channelColumn?: boolean | null } = {}): Row {
  return ingestQueueOf(db).enqueue(kind, dedupeKey, payload, opts);
}

/** 领取到期任务（§4.2）：先回收超租约的 running，再取 limit 条并置 running。 */
export function dueOps(db: DatabaseSync, limit: number, opts: { now?: number; kind?: string | null; leaseMs?: number; exclude?: unknown[] } = {}): DueOp[] {
  return ingestQueueOf(db).dueOps(limit, opts);
}

/** 收尾一个任务：done / pending（attempts+1）/ dead。 */
export function markOp(db: DatabaseSync, id: string | number, status: string, err: unknown = null, opts: { now?: number; dryRun?: boolean; log?: ((line: string) => void) | null; increment?: boolean } = {}): Row | null {
  return ingestQueueOf(db).markOp(id, status, err, opts);
}

/** 手动回捞（§6.3），默认不触发。 */
export function backfill(db: DatabaseSync, spec: string, opts: { files?: SessionFileInfo[] | null; cache?: Map<string, SessionEvent[]> | null; now?: number } = {}): BackfillResult {
  return ingestQueueOf(db).backfill(spec, opts);
}
