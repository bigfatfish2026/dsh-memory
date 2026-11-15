/**
 * entity_extract 队列（2026-11-13，docs/dsh-memory-save-queue-2026.md §2.4）。
 *
 * 本文件只碰 curate_queue 一张表：入队 + 表适配（SQL）/ 业务入口。
 * 状态机（领活 CAS、收尾、attempts 阈值、退避、租约）在 src/ledger.ts，两套队列共用一套。
 * 不调模型（那是 src/agents/entity.ts）、不写实体（那是 src/entity-gate.ts + 消费端）。
 *
 * 状态机照设计稿 §10.3：ready → leased → done，闸全拒 → skipped，3 次失败 → dead。
 * 幂等靠 UNIQUE(channel, kind, dedupe_key)，entity_extract 的 dedupe_key 就是 memory_id：
 * 同一条记忆重复入队只刷新 payload/updated_ts，不加行、不重置状态。
 *
 * 表由 scripts/apply-curate-schema.mjs 建，本文件不建表；缺表由调用方决定跳过。
 */
import os from 'node:os';
import type { DatabaseSync } from 'node:sqlite';
import { createLedger } from './ledger.js';
import type { Ledger, LedgerCounts, LedgerId, LedgerItem, LedgerOps, LedgerRow } from './ledger.js';

/**
 * 领到手的 curate 行：ledger 的 LedgerItem 加这张表的键列。
 *
 * 队列协议类型单点在这里（2026-11-15 刀 6）：entity-worker 只认这个形状，
 * 不自己照着 SQL 再写一份。字段名跟 curate_queue 表走，改列名要一起改。
 */
export interface CurateClaim extends LedgerItem {
  dedupe_key: string;
  payload_json: string;
  next_try_at: number;
}

/** enqueueEntityExtract 的入参；只认这些键，其余一概不读。 */
export interface EnqueueEntityExtractOptions {
  memoryId?: unknown;
  priority?: unknown;
  now?: unknown;
  dryRun?: unknown;
  log?: ((line: string) => void) | null;
}

export const CHANNEL = 'curate';
export const KIND = 'entity_extract';
/** 租约 180s：2-4s × 最多 3 次 + 余量（§10.4 的 L3）。 */
export const LEASE_MS = 180000;
/** 失败上限，超了 dead，不再重试。 */
export const MAX_ATTEMPTS = 3;
/** 退避阶梯（ms）：1 / 5 / 30 分钟，第 4 次 dead。 */
export const BACKOFF_MS = [60000, 5 * 60000, 30 * 60000];
/** 单批 8 条；并发由消费端控制（默认 2）。 */
export const BATCH = 8;

/** 三张表在不在（curate_run 也要，消费端写 run 记录才有意义）。 */
export function hasCurateTables(db: DatabaseSync | null | undefined): boolean {
  if (!db) return false;
  try {
    const names = (db
      .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('curate_queue','curate_run')")
      .all() as Array<{ name: string }>)
      .map((r) => r.name);
    return names.includes('curate_queue') && names.includes('curate_run');
  } catch {
    return false;
  }
}

/** 缺表提示每连接只打一次：表没建时每写一条记忆都 WARN 会把 memory_store.log 刷满。 */
const NO_TABLE_WARNED = new WeakSet<DatabaseSync>();

/** 本进程的租约 owner：host:pid，排障时一眼看出是谁领的。 */
export function leaseOwner(): string {
  return `${os.hostname()}:${process.pid}`;
}

/**
 * curate_queue 的表适配器：SQL 留在这里，状态机在 ledger。
 * 字段/状态名跟表走，ledger 只认「待领 / 在跑 / 完成 / 死 / 跳过」五个语义。
 */
export function curateQueueOps(db: DatabaseSync): LedgerOps {
  return {
    ready: () => hasCurateTables(db),
    select: ({ limit, exclude = [], now }: { limit: number; exclude?: unknown[]; now: number }): LedgerRow[] => {
      const skip = (Array.isArray(exclude) ? exclude : []).map((x) => Number(x)).filter((x) => Number.isFinite(x));
      const skipSql = skip.length ? ` AND id NOT IN (${skip.map(() => '?').join(',')})` : '';
      return db.prepare(
        `SELECT id, dedupe_key, payload_json, attempts, next_try_at
           FROM curate_queue
          WHERE state='ready' AND channel=? AND kind=? AND next_try_at<=?${skipSql}
          ORDER BY priority, next_try_at, id
          LIMIT ?`,
      ).all(CHANNEL, KIND, now, ...skip, Math.max(1, Number(limit) || BATCH)) as LedgerRow[];
    },
    // attempts+1 在这里落值，但「领取即计一次」是 ledger.claim 的 attemptsAt='claim' 语义
    // （2026-11-15 收口）；别把这行改成别的时机。
    markRunning: (row: LedgerRow, { now, owner, leaseMs }: { now: number; owner: string; leaseMs: number }): number | bigint => db.prepare(
      `UPDATE curate_queue
          SET state='leased', lease_owner=?, leased_ts=?, lease_expires=?, updated_ts=?, attempts=attempts+1
        WHERE id=? AND state='ready'`,
    ).run(
      String(owner || leaseOwner()),
      now,
      now + Math.max(1000, Number(leaseMs) || LEASE_MS),
      now,
      row.id as LedgerId,
    ).changes,
    mapClaimed: (row: LedgerRow, attempts: number): CurateClaim => ({ ...row, attempts } as CurateClaim),
    backState: 'ready',
    setDone: (id: LedgerId, { now }: { now: number }): number | bigint => db.prepare(
      `UPDATE curate_queue SET state='done', lease_owner='', lease_expires=0, updated_ts=?, last_error=''
        WHERE id=? AND state='leased'`,
    ).run(now, id).changes,
    setBack: (id: LedgerId, { message, attempts, nextTryAt, now }: { message: string | null; attempts: number | null; nextTryAt: number; now: number }): number | bigint => {
      if (attempts === null) {
        return db.prepare(
          `UPDATE curate_queue SET state='ready', lease_owner='', lease_expires=0, next_try_at=?, updated_ts=?, last_error=?
            WHERE id=?`,
        ).run(nextTryAt, now, message, id).changes;
      }
      return db.prepare(
        `UPDATE curate_queue SET state='ready', lease_owner='', lease_expires=0, next_try_at=?, updated_ts=?, last_error=?, attempts=?
          WHERE id=?`,
      ).run(nextTryAt, now, message, attempts, id).changes;
    },
    setDead: (id: LedgerId, { message, attempts, now }: { message: string | null; attempts: number | null; now: number }): number | bigint => {
      if (attempts === null) {
        return db.prepare(
          `UPDATE curate_queue SET state='dead', lease_owner='', lease_expires=0, updated_ts=?, last_error=? WHERE id=?`,
        ).run(now, message || 'attempts exhausted', id).changes;
      }
      return db.prepare(
        `UPDATE curate_queue SET state='dead', lease_owner='', lease_expires=0, updated_ts=?, last_error=?, attempts=? WHERE id=?`,
      ).run(now, message || 'attempts exhausted', attempts, id).changes;
    },
    setSkipped: (id: LedgerId, { message, now }: { message: string | null; now: number }): number | bigint => db.prepare(
      `UPDATE curate_queue SET state='skipped', lease_owner='', lease_expires=0, updated_ts=?, last_error=?
        WHERE id=? AND state='leased'`,
    ).run(now, String(message ?? '').slice(0, 500), id).changes,
    reap: ({ now }: { now: number }): number | bigint => db.prepare(
      `UPDATE curate_queue
          SET state='ready', lease_owner='', leased_ts=0, lease_expires=0, updated_ts=?
        WHERE state='leased' AND channel=? AND kind=? AND lease_expires>0 AND lease_expires<?`,
    ).run(now, CHANNEL, KIND, now).changes,
    counts: (): LedgerCounts => {
      const out: Record<string, number> = { total: 0 };
      for (const r of db
        .prepare('SELECT state, count(*) AS n FROM curate_queue WHERE channel=? AND kind=? GROUP BY state')
        .all(CHANNEL, KIND) as Array<{ state: unknown; n: unknown }>) {
        out[String(r.state)] = Number(r.n) || 0;
        out.total += Number(r.n) || 0;
      }
      for (const s of ['ready', 'leased', 'done', 'dead', 'skipped']) if (!(s in out)) out[s] = 0;
      // 统一口径别名：ledger 的 pending/running 对应这张表的 ready/leased
      out.pending = out.ready;
      out.running = out.leased;
      return out as unknown as LedgerCounts;
    },
  };
}

/** 建这张表的账本。阈值/退避/attempts 时机都由这里传，ledger 不硬编码。 */
export function curateLedger(db: DatabaseSync, { leaseMs = LEASE_MS }: { leaseMs?: number } = {}): Ledger {
  return createLedger({
    channel: `curate/${KIND}`,
    kinds: [KIND],
    maxAttempts: MAX_ATTEMPTS,
    leaseMs: Number(leaseMs) || LEASE_MS,
    attemptsAt: 'claim',
    backoffMs: (attempts) => BACKOFF_MS[Math.min(Math.max(Number(attempts) || 1, 1) - 1, BACKOFF_MS.length - 1)],
    ops: curateQueueOps(db),
  });
}

/**
 * 入队一条待抽实体的记忆。
 * @param {import('node:sqlite').DatabaseSync} db
 * @param {object} item
 * @param {string} item.memoryId 记忆 id（dedupe_key 就是它）
 * @param {number} [item.priority=50]
 * @param {number} [item.now]
 * @param {boolean} [item.dryRun]
 * @param {(line:string)=>void} [item.log]
 * @returns {{key:string, id:number|null, created:boolean}|null} 没有库/没表/失败返回 null
 */
export function enqueueEntityExtract(db: DatabaseSync | null, item: EnqueueEntityExtractOptions = {}): { key: string; id: number | null; created: boolean } | null {
  const memoryId = String(item.memoryId || '').trim();
  const log = typeof item.log === 'function' ? item.log : null;
  if (!memoryId) return null;
  if (!db) {
    log?.(`[entity-queue] 没有库可入队 memory=${memoryId}`);
    return null;
  }
  if (!hasCurateTables(db)) {
    if (!NO_TABLE_WARNED.has(db)) {
      NO_TABLE_WARNED.add(db);
      log?.('[entity-queue] curate_queue/curate_run 不在，入队静默跳过（先跑 scripts/apply-curate-schema.mjs）');
    }
    return null;
  }
  const payload = JSON.stringify({ memory_id: memoryId });
  if (item.dryRun) {
    log?.(`[entity-queue][dry-run] 不入队 memory=${memoryId}`);
    return { key: memoryId, id: null, created: false };
  }
  const now = Number(item.now) || Date.now();
  try {
    const before = db
      .prepare('SELECT id FROM curate_queue WHERE channel=? AND kind=? AND dedupe_key=?')
      .get(CHANNEL, KIND, memoryId);
    db.prepare(
      `INSERT INTO curate_queue(channel, kind, dedupe_key, payload_json, priority, entity_id,
                                state, attempts, next_try_at, created_ts, updated_ts)
       VALUES (?, ?, ?, ?, ?, 0, 'ready', 0, ?, ?, ?)
       ON CONFLICT(channel, kind, dedupe_key) DO UPDATE
         SET payload_json = excluded.payload_json,
             priority     = excluded.priority,
             updated_ts   = excluded.updated_ts`,
    ).run(CHANNEL, KIND, memoryId, payload, Number(item.priority) || 50, now, now, now);
    const row = db
      .prepare('SELECT id, state FROM curate_queue WHERE channel=? AND kind=? AND dedupe_key=?')
      .get(CHANNEL, KIND, memoryId) as { id: unknown } | undefined;
    return { key: memoryId, id: row ? Number(row.id) : null, created: !before };
  } catch (error) {
    log?.(`[entity-queue] 入队失败 memory=${memoryId}：${String((error && (error as { message?: unknown }).message) || error)}`);
    return null;
  }
}

/**
 * 原子领取：两步 CAS（先选后改，UPDATE 带 state='ready' 条件），逻辑在 ledger。
 * @returns {Array<{id:number, dedupe_key:string, payload_json:string, attempts:number, next_try_at:number}>}
 */
export function claimBatch(db: DatabaseSync | null, { owner = leaseOwner(), batch = BATCH, now = Date.now(), leaseMs = LEASE_MS }: { owner?: string; batch?: number; now?: number; leaseMs?: number } = {}): CurateClaim[] {
  if (!db || !hasCurateTables(db)) return [];
  return curateLedger(db, { leaseMs }).claim(batch, { now, owner }) as CurateClaim[];
}

/** 一批处理完：逐条 state='done'，返回真正改到的条数。 */
export function markDone(db: DatabaseSync | null, ids: LedgerId[] | null | undefined, { now = Date.now() }: { now?: number } = {}): number {
  if (!db || !ids?.length) return 0;
  const ledger = curateLedger(db);
  let n = 0;
  for (const id of ids) n += Number(ledger.finish(id, { now })) || 0;
  return n;
}

/** 一条都没抽到 / 全被闸拒：state='skipped'（不占重试额度）。 */
export function markSkipped(db: DatabaseSync | null, ids: LedgerId[] | null | undefined, { now = Date.now(), reason = '' }: { now?: number; reason?: string } = {}): number {
  if (!db || !ids?.length) return 0;
  const ledger = curateLedger(db);
  let n = 0;
  for (const id of ids) n += Number(ledger.skip(id, { error: reason, now })) || 0;
  return n;
}

/**
 * 一条失败：attempts 到上限就 dead，否则回 ready 并按阶梯退避。
 * attempts 是领取时 +1 后的值（claimBatch 返回的就是它）。
 */
export function failOne(db: DatabaseSync | null, { id, attempts, error, now = Date.now() }: { id?: LedgerId | null; attempts?: number; error?: unknown; now?: number } = {}): { state: string; attempts: number; nextTryAt?: number } {
  if (!db || !id) return { state: 'unknown', attempts: 0 };
  return curateLedger(db).fail(id, { attempts, error, now });
}

/** 租约过期的捞回 ready（attempts 领取时已经 +1，这里不再加）。 */
export function reapExpired(db: DatabaseSync | null, { now = Date.now() }: { now?: number } = {}): number {
  if (!db || !hasCurateTables(db)) return 0;
  return curateLedger(db).reap({ now });
}

/** 队列现状：消费端日志和验收都用它。 */
export function queueStats(db: DatabaseSync | null | undefined): LedgerCounts | null {
  if (!db || !hasCurateTables(db)) return null;
  return curateLedger(db).stats();
}
