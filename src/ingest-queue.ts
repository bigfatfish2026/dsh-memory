/**
 * 会话入库账本（docs/sleep-pipeline-redesign.md §3、§4）：
 *   ingest_files —— 每个会话文件一行，处理到哪、失败几次都记着；
 *   pending_ops  —— 要重试的活，UNIQUE(kind, dedupe_key) 保证幂等入队。
 *
 * 2026-11-05 第二步：把「一个全局 lastSleepAt 游标 + 末 20 个文件」换成按文件 ack。
 * 本模块只碰库和文件，不调模型、不落记忆；模型调用仍在 sleep.js 里。
 *
 * 约定：db 是 node:sqlite 的 DatabaseSync（store.db() 拿得到），表由
 * scripts/apply-pipeline-schema.mjs 建好；这里不建表，缺表由调用方决定回退。
 *
 * 2026-11-15：闭包收成 IngestQueue 类。这个类持有两样可变状态：
 *   1) 每个通道的 Ledger 实例（阈值/租约/退避参数不同，按 channel/kinds/leaseMs 缓存）；
 *   2) 「channel 列自愈做过没」的每库标记（原来是模块级 WeakSet）。
 * 扫描文件、判 dry-run、拼行之类的纯函数留在模块里，不硬包成方法。
 */
import path from 'node:path';
import type { DatabaseSync } from 'node:sqlite';
import { lastEventTsOf } from './transcript.js';
import * as paths from './shared/paths.js';
import type { SessionEvent } from './transcript.js';
import { Ledger, recordAttempt } from './ledger.js';
import type { LedgerCounts, LedgerId, LedgerItem, LedgerOps } from './ledger.js';
import { withTransaction } from './shared/tx.js';
import { kindsOfChannel, channelOfKindDef, defaultKindRegistry } from './kind-handlers.js';
import {
  dryRunRow, dryRunSkip, loadEvents, parseSinceDay, rowOf, scanSessions, statFile,
} from './ingest-fs.js';
import type { SessionFileInfo } from './ingest-fs.js';
import type { Row } from './store/types.js';
import type {
  AckResult, BackfillResult, BootstrapResult, ChannelStats, DueOp, FailResult, PlanFilesResult,
} from './ingest-index.js';

/** pending_ops 的一行（SELECT * 的产物）。 */
export type PendingOpRow = Row & {
  id?: LedgerId;
  kind?: string;
  channel?: string;
  dedupe_key?: string;
  payload_json?: string;
  attempts?: number;
  next_try_at?: number;
  last_error?: string | null;
  status?: string;
  created_ts?: number;
  updated_ts?: number;
};


/**
 * 判死阈值：同一处理单元 attempts 到 5 就不再自动领，标 dead 留给人看。
 * 这是防卡死，不是预算——不是「最多试 5 次就放弃整个任务」，只死这一个单元。
 * 重试不再排时间：next_try_at 就写现在，下一轮有资格再领（本轮靠 exclude 只试一次）。
 */
export const MAX_ATTEMPTS = 5;
/** running 租约，超过 10 分钟没更新视为进程崩了，捞回 pending（§4.1）。 */
export const LEASE_MS = 10 * 60000;

/** 三张表在不在；开关打开了但库没建表时调用方要能发现。 */
export function hasIngestTables(db: DatabaseSync | null | undefined): boolean {
  if (!db) return false;
  try {
    const names = db
      .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('ingest_files','pending_ops')")
      .all()
      .map((r) => r.name as string);
    return names.includes('ingest_files') && names.includes('pending_ops');
  } catch {
    return false;
  }
}

/**
 * 通道划分（2026-11-14 维护者拍板：物理一张 pending_ops 表，按 channel 分三段）。
 * channel 列是后加的（老库靠 ensureChannelColumn 自愈），所以领活时一律
 * 「channel = ? OR (channel = '' AND kind 落在本通道)」，老行不用回填也能被领走。
 */
export const CHANNELS = ['ingest', 'curate', 'compact'];
/** kind → channel 的唯一出处是 src/kind-handlers.ts 的注册表，这里只是按通道取一份。 */
export const KINDS_OF_CHANNEL = Object.freeze({
  ingest: kindsOfChannel('ingest'),
  curate: kindsOfChannel('curate'),
  // compact = 本地重放通道（阶段 5）：revocation_sweep / merge_op / dedupe_check 真入队
  compact: kindsOfChannel('compact'),
});

export function channelOfKind(kind: unknown): string {
  const k = String(kind || '');
  for (const [channel, kinds] of Object.entries(KINDS_OF_CHANNEL)) {
    if (kinds.includes(k)) return channel;
  }
  // entity_extract 这类不落 pending_ops 的 kind，也从同一张表答，别再各写一份映射
  return channelOfKindDef(k);
}

/**
 * 老库的 pending_ops.kind CHECK 还不认识 revocation_sweep（2026-11-14 阶段 5 新加的
 * compact 通道 kind）。SQLite 改不了 CHECK，只能 12-step 重建；只做一次，失败就返回 false
 * （调用方按「没有这个 kind」降级，不要因此把整轮整理拖死）。
 */
export function ensurePendingKinds(db: DatabaseSync | null | undefined): boolean {
  if (!db) return false;
  let sql = '';
  try {
    sql = String(db.prepare("SELECT sql FROM sqlite_master WHERE type='table' AND name='pending_ops'").get()?.sql || '');
  } catch {
    return false;
  }
  // 缺失的 kind 一律问注册表，别再一个个手写：阶段 5 加 revocation_sweep、三期加 entity_tidy
  // 都是「表里 CHECK 不认」，漏一个整条通道就领不到自己的活。
  const wanted = defaultKindRegistry.list().map((d) => String(d.kind));
  const missing = wanted.filter((k) => !sql.includes(`'${k}'`));
  if (!sql || !missing.length) return true;
  let nextSql = sql.replace('CHECK (kind IN (', `CHECK (kind IN (${missing.map((k) => `'${k}'`).join(',')},`);
  if (nextSql === sql) return false;
  let cols: string[] = [];
  try {
    cols = db.prepare('PRAGMA table_info(pending_ops)').all().map((c) => String(c.name));
  } catch {
    return false;
  }
  const hasChannel = cols.includes('channel');
  // 老表的 CREATE 文本里没有 channel 列：重建时顺手补上，否则新表仍缺列
  if (!hasChannel) {
    nextSql = nextSql.replace(/(\bkind\s+TEXT\s+NOT NULL)/i, "$1,\n  channel TEXT NOT NULL DEFAULT ''");
    if (!nextSql.includes('channel TEXT')) return false;
  }
  try {
    withTransaction(db, () => {
      db.exec('ALTER TABLE pending_ops RENAME TO pending_ops_old');
      db.exec(nextSql);
      // 老表可能还没 channel 列（ensureChannelColumn 在重建之后才 ALTER）：按列名显式搬，别 SELECT *
      const target = 'id, kind, channel, dedupe_key, payload_json, attempts, next_try_at, last_error, status, created_ts, updated_ts';
      const source = hasChannel
        ? 'id, kind, channel, dedupe_key, payload_json, attempts, next_try_at, last_error, status, created_ts, updated_ts'
        : "id, kind, '', dedupe_key, payload_json, attempts, next_try_at, last_error, status, created_ts, updated_ts";
      db.exec(`INSERT INTO pending_ops(${target}) SELECT ${source} FROM pending_ops_old`);
      db.exec('DROP TABLE pending_ops_old');
      db.exec('CREATE INDEX IF NOT EXISTS idx_pending_due ON pending_ops(status, next_try_at, attempts)');
      db.exec('CREATE INDEX IF NOT EXISTS idx_pending_channel ON pending_ops(channel, status, next_try_at, attempts)');
    });
    return true;
  } catch {
    return false;
  }
}

/**
 * 租约回收（2026-11-15 存储中-1）：running 超过 leaseMs 没更新，视为这条活的进程没走完。
 * 回收要记账——attempts+1，到阈值直接 dead 并留原因 `lease_expired`，不许白回收：
 * 不然一条永远收不了尾的毒账每轮被捞回、又永远不会死。
 * 返回捞回（含判死）条数。
 */
function reapExpiredOps(db: DatabaseSync, { now, leaseMs, maxAttempts, channel = '' }: { now: number; leaseMs: number; maxAttempts: number; channel?: string }): number {
  // 整批一条 SQL：阈值由 ledger.reap 传进来（maxAttempts），判定口径与 ledger.recordAttempt 相同
  // （+1 后 >= 阈值即 dead），这里不逐行调 JS 是为了保持「一条 UPDATE 原子回收」。
  const lease = Math.max(1000, Number(leaseMs) || LEASE_MS);
  const cap = Math.max(1, Math.floor(Number(maxAttempts) || MAX_ATTEMPTS));
  const chSql = channel ? ' AND channel = ?' : '';
  const args: Array<number | string> = [cap, cap, now, now - lease];
  if (channel) args.push(String(channel));
  const r = db.prepare(
    `UPDATE pending_ops
        SET attempts = attempts + 1,
            status = CASE WHEN attempts + 1 >= ? THEN 'dead' ELSE 'pending' END,
            last_error = CASE WHEN attempts + 1 >= ?
                              THEN 'lease_expired: ' || COALESCE(NULLIF(last_error, ''), '租约过期，没留原因')
                              ELSE 'lease_expired' END,
            updated_ts = ?
      WHERE status = 'running' AND updated_ts < ?${chSql}`,
  ).run(...args);
  return Number(r.changes) || 0;
}

/**
 * pending_ops 的表适配器（2026-11-14 阶段 2）：SQL 留在这里，状态机在 src/ledger.ts。
 * 状态名映射：pending↔待领、running↔在跑、done、dead；没有 skipped。
 * 租约语义跟这张表走：running 的 updated_ts 就是租约起点（没有单独的 lease_expires 列）。
 */
export function pendingOpsOps(db: DatabaseSync, { channel, kinds = [], leaseMs = LEASE_MS, ensure = null }: { channel?: string; kinds?: string[]; leaseMs?: number; ensure?: (() => boolean) | null } = {}): LedgerOps {
  const list = () => (Array.isArray(kinds) && kinds.length ? kinds : []);
  // 自愈入口由账本持有者给（IngestQueue 实例），默认退回按 db 取同一个实例。
  const ready = () => (typeof ensure === 'function' ? ensure() : ensureChannelColumn(db));
  return {
    ready: () => Boolean(db) && hasIngestTables(db),
    select: ({ limit, exclude = [], now, kinds: only }) => {
      const ks = Array.isArray(only) && only.length ? only : list();
      if (!ks.length) return [];
      ready();
      const skip = (Array.isArray(exclude) ? exclude : []).map((x) => Number(x)).filter((x) => Number.isFinite(x));
      const skipSql = skip.length ? ` AND id NOT IN (${skip.map(() => '?').join(',')})` : '';
      // kind 条件必须对「已有 channel 的行」也生效：2026-11-15 12:30 那轮 merge 池按
      // channel='curate' 把同通道的 split_item 一起领走，prepare 拿不到 ids 就当 stale_members
      // 标 done，30 条拆分失败账全被盖成终态。channel 只用来隔离通道，kind 才是池的领活边界。
      return db.prepare(
        `SELECT * FROM pending_ops
           WHERE status = 'pending' AND next_try_at <= ?
             AND kind IN (${ks.map(() => '?').join(',')})
             AND (channel = ? OR channel = '')${skipSql}
           ORDER BY attempts ASC, next_try_at ASC, id ASC
           LIMIT ?`,
      ).all(now, ...ks, String(channel || ''), ...skip, Math.max(1, Number(limit) || 1));
    },
    markRunning: (row, { now }) => db.prepare(
      "UPDATE pending_ops SET status = 'running', channel = ?, updated_ts = ? WHERE id = ? AND status = 'pending'",
    ).run(String(channel || ''), now, row.id as LedgerId).changes,
    mapClaimed: (row, attempts) => {
      let payload = null;
      try { payload = JSON.parse(row.payload_json as string); } catch { payload = null; }
      return {
        id: row.id as LedgerId,
        kind: row.kind as string,
        channel: String(channel || ''),
        dedupeKey: row.dedupe_key as string,
        payload,
        payloadJson: row.payload_json as string,
        attempts,
        nextTryAt: Number(row.next_try_at) || 0,
      };
    },
    setDone: (id, { now, message }) => db.prepare(
      "UPDATE pending_ops SET status = 'done', last_error = ?, updated_ts = ? WHERE id = ?",
    ).run(message, now, id).changes,
    setBack: (id, { message, attempts, nextTryAt, now }) => {
      if (attempts === null) {
        return db.prepare(
          "UPDATE pending_ops SET status = 'pending', last_error = ?, updated_ts = ? WHERE id = ?",
        ).run(message, now, id).changes;
      }
      return db.prepare(
        "UPDATE pending_ops SET status = 'pending', attempts = ?, next_try_at = ?, last_error = ?, updated_ts = ? WHERE id = ?",
      ).run(attempts, nextTryAt, message, now, id).changes;
    },
    setDead: (id, { message, attempts, now }) => {
      if (attempts === null) {
        return db.prepare(
          "UPDATE pending_ops SET status = 'dead', last_error = ?, updated_ts = ? WHERE id = ?",
        ).run(message || 'dead', now, id).changes;
      }
      return db.prepare(
        "UPDATE pending_ops SET status = 'dead', attempts = ?, last_error = ?, updated_ts = ? WHERE id = ?",
      ).run(attempts, message || 'dead', now, id).changes;
    },
    reap: ({ now, maxAttempts }) => reapExpiredOps(db, { now, leaseMs, maxAttempts, channel }),
    counts: ({ kinds: only }): LedgerCounts => {
      const ks = Array.isArray(only) && only.length ? only : list();
      const out: Record<string, number> & { total: number } = { total: 0 };
      if (!ks.length) return out as unknown as LedgerCounts;
      ready();
      try {
        const rows = db.prepare(
          `SELECT state, count(*) AS n FROM (
             SELECT CASE WHEN status = 'running' THEN 'running' ELSE status END AS state
               FROM pending_ops
              WHERE (channel = ? OR (channel = '' AND kind IN (${ks.map(() => '?').join(',')})))
           ) GROUP BY state`,
        ).all(String(channel || ''), ...ks);
        for (const r of rows) {
          out[String(r.state)] = Number(r.n) || 0;
          out.total += Number(r.n) || 0;
        }
        for (const s of ['pending', 'running', 'done', 'dead']) if (!(s in out)) out[s] = 0;
      } catch { /* 表不在/列没加：按空队列报 */ }
      return out as unknown as LedgerCounts;
    },
  };
}

/**
 * 每库一个队列实例：Ledger 缓存与 channel 自愈标记都挂在实例上，同一 db 的调用方看到同一份。
 * 不再用模块级 WeakSet 存可变状态。
 */
const QUEUES = new WeakMap<DatabaseSync, IngestQueue>();

/** 取（或建）某个连接对应的 IngestQueue。 */
export function ingestQueueOf(db: DatabaseSync): IngestQueue {
  let queue = QUEUES.get(db);
  if (!queue) {
    queue = new IngestQueue(db);
    QUEUES.set(db, queue);
  }
  return queue;
}

/** 按 db 取队列并做一次 channel 列自愈（旧导出 ensureChannelColumn 的实现）。 */
export function ensureChannelColumn(db: DatabaseSync | null | undefined, { force = false }: { force?: boolean } = {}): boolean {
  if (!db) return false;
  return ingestQueueOf(db).ensureChannelColumn({ force });
}

export class IngestQueue {
  #db: DatabaseSync;

  #ledgers = new Map<string, Ledger>();

  /** 每个连接只自愈一次：老库 ADD COLUMN + 回填 channel，之后是一条空 UPDATE。 */
  #channelReady = false;

  constructor(db: DatabaseSync) {
    this.#db = db;
  }

  get db(): DatabaseSync {
    return this.#db;
  }

  /** 幂等加列 + 回填。没表/加不上就返回 false，调用方按「没有通道列」退化处理。 */
  ensureChannelColumn({ force = false }: { force?: boolean } = {}): boolean {
    const db = this.#db;
    if (!db) return false;
    if (!force && this.#channelReady) return true;
    // 老库的 pending_ops 没有 revocation_sweep 这个 kind（CHECK 约束挡着）：就地重建一次。
    ensurePendingKinds(db);
    try {
      const cols = db.prepare('PRAGMA table_info(pending_ops)').all().map((c) => String(c.name));
      if (!cols.includes('channel')) {
        db.exec("ALTER TABLE pending_ops ADD COLUMN channel TEXT NOT NULL DEFAULT ''");
      }
      db.exec('CREATE INDEX IF NOT EXISTS idx_pending_channel ON pending_ops(channel, status, next_try_at, attempts)');
      // 2026-11-14：回填 channel 一律问 kind 注册表，不许在这里另写一张表。
      // 原来写死「merge_batch/merge_op/dedupe_check/split_item → curate」，而阶段 5 起
      // merge_op / dedupe_check 归 compact：老账被改成 curate 后两个池都领不到它们，
      // 只能永远躺在 pending（20:00 那轮 4 条 merge_op 一条没被 compact 领走就是这个原因）。
      // 口径：kind 的归属只有 kind-handlers.js 一处；这里顺带纠正历史上被改错的行（幂等）。
      for (const kind of ['ingest_file', 'merge_batch', 'split_item', 'merge_op', 'dedupe_check', 'revocation_sweep']) {
        const ch = channelOfKindDef(kind);
        if (!ch) continue;
        db.prepare('UPDATE pending_ops SET channel = ? WHERE kind = ? AND channel <> ?').run(ch, kind, ch);
      }
      this.#channelReady = true;
      return true;
    } catch {
      return false;
    }
  }

  /** 通道账本：阈值 5、attempts 失败时加（两表历史差异由参数化解决，不写死在 ledger 里）。 */
  ledgerFor(channel: string = '', kinds: string[] = [], leaseMs: number = LEASE_MS): Ledger {
    const key = `${channel}|${(kinds || []).join(',')}|${Math.max(1000, Number(leaseMs) || LEASE_MS)}`;
    let ledger = this.#ledgers.get(key);
    if (!ledger) {
      ledger = new Ledger({
        channel: `ops/${channel || '*'}`,
        kinds,
        maxAttempts: MAX_ATTEMPTS,
        leaseMs,
        attemptsAt: 'fail',
        backoffMs: () => 0,
        ops: pendingOpsOps(this.#db, { channel, kinds, leaseMs, ensure: () => this.ensureChannelColumn() }),
      });
      this.#ledgers.set(key, ledger);
    }
    return ledger;
  }

  /**
   * 按通道领一批（池的唯一取活入口）：领到 limit 条当场标 running（带租约语义：
   * updated_ts 就是租约起点，超 LEASE_MS 由 reap 捞回）。
   * **不许一次把整队标 running**——limit 由调用方给的成本通道并发数决定。
   */
  claim(channel: string, limit: number, { now = Date.now(), leaseMs = LEASE_MS, kinds: only = null, exclude = [] }: { now?: number; leaseMs?: number; kinds?: string[] | null; exclude?: unknown[] } = {}): LedgerItem[] {
    const n = Math.max(0, Math.floor(Number(limit) || 0));
    const db = this.#db;
    if (!db || !n) return [];
    // kinds：同一条通道里只领某几种活（比如 curate 通道的合并池只领 merge_batch，拆分另有人领）
    const kinds = Array.isArray(only) && only.length ? only : ((KINDS_OF_CHANNEL as Record<string, string[]>)[channel] || []);
    if (!kinds.length) return [];
    this.ensureChannelColumn();
    // 领活粒度/并发上限/这是唯一取活入口的语义都在 ledger.claim 里
    return this.ledgerFor(channel, kinds, leaseMs).claim(n, { exclude, now });
  }

  /** 租约回收：running 超过 leaseMs 没更新，视为进程崩了，捞回 pending。 */
  reap(channel: string | null = null, { now = Date.now(), leaseMs = LEASE_MS }: { now?: number; leaseMs?: number } = {}): number {
    const db = this.#db;
    if (!db) return 0;
    try {
      return this.ledgerFor(channel || '', (KINDS_OF_CHANNEL as Record<string, string[]>)[channel as string] || [], leaseMs).reap({ now });
    } catch {
      return 0;
    }
  }

  /** 某通道各状态条数（池汇总、队列可观测用）。 */
  stats(channel: string): ChannelStats {
    const db = this.#db;
    const kinds = (KINDS_OF_CHANNEL as Record<string, string[]>)[channel] || [];
    if (!db || !kinds.length) return { total: 0 } as ChannelStats;
    this.ensureChannelColumn();
    let out: ChannelStats;
    try {
      out = this.ledgerFor(channel, kinds).stats();
    } catch {
      out = { total: 0 } as ChannelStats;
    }
    // compact 的「闸成本」单独报：llm_calls = 写入闸里的模型调用，local_ops = 本地动作数。
    // 这两项来自最近一条 channel='compact' 的 curate_run（由 sleep 的 compact 池收尾写入）。
    if (channel === 'compact') {
      try {
        const row = db.prepare(
          "SELECT budget_json FROM curate_run WHERE channel='compact' ORDER BY started_ts DESC LIMIT 1",
        ).get();
        const b = row ? JSON.parse(row.budget_json as string || '{}') : {};
        out.llm_calls = Number(b.llm_calls) || 0;
        out.local_ops = Number(b.local_ops) || 0;
      } catch {
        out.llm_calls = 0;
        out.local_ops = 0;
      }
    }
    return out;
  }

  /**
   * 待处理判定（§3.2）：没行 / mtime|size 变了 / last_acked_ts < 文件最后事件时间 /
   * status in (pending, failed)；dead 不自动捞，legacy_acked 只在文件变了之后重新进队。
   *
   * budget 只截本轮处理量，截掉的放 overBudget（DB 里仍是 pending，下轮按同样规则继续）。
   * 2026-11-14：不传 budget = 不截断，本轮该处理的全部返回（ingestBudget 删了）。
   * 排序 attempts ASC, last_acked_ts ASC, mtime ASC：先救失败过的、先补旧账。
   *
   * 返回 {todo, legacy, done, overBudget}。todo/overBudget 元素带 path/mtime/size/
   * lastAckedTs/lastEventTs/attempts/status/reason；legacy 是等 bootstrap 标的旧文件。
   */
  planFiles(files: SessionFileInfo[], { lastSleepAt = 0, budget = 0, cache = null }: { lastSleepAt?: number; budget?: number; cache?: Map<string, SessionEvent[]> | null } = {}): PlanFilesResult {
    const db = this.#db;
    const todo = [];
    const legacy = [];
    const done = [];
    const watermark = Number(lastSleepAt) || 0;
    // budget<=0 = 不截断（默认），只有显式给了正数才当上限用（测试/排障）。
    const limit = Number(budget) > 0 ? Math.max(0, Math.floor(Number(budget))) : Infinity;

    for (const file of files || []) {
      const row = rowOf(db, file.path);
      const base = { path: file.path, mtime: file.mtime, size: file.size };

      if (!row) {
        const events = loadEvents(file.path, cache);
        if (!events) {
          todo.push({ ...base, lastAckedTs: watermark, lastEventTs: 0, attempts: 0, status: 'missing', reason: 'unreadable' });
          continue;
        }
        const lastTs = lastEventTsOf(events);
        if (lastTs <= watermark) {
          legacy.push({ ...base, lastEventTs: lastTs });
        } else {
          todo.push({ ...base, lastAckedTs: watermark, lastEventTs: lastTs, attempts: 0, status: 'new', reason: 'new' });
        }
        continue;
      }

      const changed = Number(row.mtime) !== Number(file.mtime) || Number(row.size) !== Number(file.size);
      const status = String(row.status || 'pending');

      if (status === 'dead') continue;
      if (status === 'legacy_acked' && !changed) {
        done.push(file.path);
        continue;
      }

      // 没变的 done 行不用解压：ack 时 last_acked_ts 就等于当时的最后事件时间，
      // 内容没动过就不会有「acked < 最后事件」；省掉一整轮 764 个文件的解压。
      if (status === 'done' && !changed) {
        done.push(file.path);
        continue;
      }

      const events = loadEvents(file.path, cache);
      if (!events) {
        todo.push({
          ...base,
          lastAckedTs: Number(row.last_acked_ts) || 0,
          lastEventTs: 0,
          attempts: Number(row.attempts) || 0,
          status,
          reason: 'unreadable',
        });
        continue;
      }
      const lastTs = lastEventTsOf(events);
      const acked = Number(row.last_acked_ts) || 0;
      if (lastTs <= acked && status === 'done') {
        done.push(file.path);
        continue;
      }
      todo.push({
        ...base,
        lastAckedTs: acked,
        lastEventTs: lastTs,
        attempts: Number(row.attempts) || 0,
        status,
        reason: changed ? 'changed' : status,
      });
    }

    todo.sort(
      (a, b) =>
        (Number(a.attempts) || 0) - (Number(b.attempts) || 0) ||
        (Number(a.lastAckedTs) || 0) - (Number(b.lastAckedTs) || 0) ||
        (Number(a.mtime) || 0) - (Number(b.mtime) || 0) ||
        (a.path < b.path ? -1 : a.path > b.path ? 1 : 0),
    );

    return {
      todo: limit === Infinity ? todo : todo.slice(0, limit),
      overBudget: limit === Infinity ? [] : todo.slice(limit),
      legacy,
      done,
    };
  }

  /**
   * bootstrap（§6.3，首次运行）：不重挖历史。
   *  最后事件时间 <= lastSleepAt → status='legacy_acked'、last_acked_ts=最后事件时间；
   *  最后事件时间 >  lastSleepAt → status='pending'、last_acked_ts=lastSleepAt。
   * 已有行的文件不动（幂等，可重复调）。返回 {scanned, legacy, pending, skipped, errors}。
   */
  bootstrapLegacy(files: SessionFileInfo[], lastSleepAt: number, { cache = null, now = Date.now(), dryRun = false, log = null }: { cache?: Map<string, SessionEvent[]> | null; now?: number; dryRun?: boolean; log?: ((line: string) => void) | null } = {}): BootstrapResult {
    const db = this.#db;
    const out = { scanned: 0, legacy: 0, pending: 0, skipped: 0, errors: 0, dryRun, lastSleepAt: Number(lastSleepAt) || 0 };
    const insert = db.prepare(
      `INSERT INTO ingest_files (path, mtime, size, last_acked_ts, status, attempts, last_error, updated_ts)
       VALUES (?, ?, ?, ?, ?, 0, NULL, ?)
       ON CONFLICT(path) DO NOTHING`,
    );
    for (const file of files || []) {
      out.scanned += 1;
      if (rowOf(db, file.path)) {
        out.skipped += 1;
        continue;
      }
      const st = file.mtime !== undefined ? { mtime: file.mtime, size: file.size } : statFile(file.path);
      if (!st) {
        out.errors += 1;
        continue;
      }
      const events = file.lastEventTs !== undefined ? null : loadEvents(file.path, cache);
      if (file.lastEventTs === undefined && !events) {
        out.errors += 1;
        continue;
      }
      const lastTs = file.lastEventTs !== undefined ? Number(file.lastEventTs) || 0 : lastEventTsOf(events);
      if (lastTs <= out.lastSleepAt) {
        if (!dryRun) insert.run(file.path, st.mtime, st.size, lastTs, 'legacy_acked', now);
        out.legacy += 1;
      } else {
        if (!dryRun) insert.run(file.path, st.mtime, st.size, out.lastSleepAt, 'pending', now);
        out.pending += 1;
      }
    }
    if (dryRun) {
      try {
        log?.(`[dry-run] bootstrap 扫描${out.scanned} 将标 legacy_acked=${out.legacy} pending=${out.pending}（不写 ingest_files）`);
      } catch {
        /* 同上 */
      }
    }
    return out;
  }

  /**
   * 整文件 ack：一个事务写 last_acked_ts/mtime/size/status='done'/attempts=0（§3.3 第 4 步）。
   * 只在所有候选都落定（入库/判重丢弃/终态 rejected）之后调；mtime/size 默认现取。
   */
  ackFile(filePath: string, lastEventTs: number, { status = 'done', mtime, size, now = Date.now(), dryRun = false, log = null }: { status?: string; mtime?: number; size?: number; now?: number; dryRun?: boolean; log?: ((line: string) => void) | null } = {}): AckResult {
    const db = this.#db;
    const st = statFile(filePath);
    const m = Number(mtime ?? st?.mtime ?? 0);
    const s = Number(size ?? st?.size ?? 0);
    const ts = Number(lastEventTs) || 0;
    if (dryRunSkip(dryRun, log, `ackFile ${filePath} ts=${ts}`)) {
      return { path: filePath, lastEventTs: ts, status, mtime: m, size: s, dryRun: true };
    }
    db.prepare(
      `INSERT INTO ingest_files (path, mtime, size, last_acked_ts, status, attempts, last_error, updated_ts)
       VALUES (?, ?, ?, ?, ?, 0, NULL, ?)
       ON CONFLICT(path) DO UPDATE SET
         mtime = excluded.mtime,
         size = excluded.size,
         last_acked_ts = excluded.last_acked_ts,
         status = excluded.status,
         attempts = 0,
         last_error = NULL,
         updated_ts = excluded.updated_ts`,
    ).run(filePath, m, s, ts, status, now);
    return { path: filePath, lastEventTs: ts, status, mtime: m, size: s };
  }

  /**
   * 单文件失败（§3.4）：attempts+1、last_error 落库、不推进 last_acked_ts；
   * 同时把 pending_ops(kind='ingest_file', dedupe_key=path) 置 pending 并带退避时间，
   * attempts>3 双方都转 dead，等人工捞。
   *
   * mtime/size 故意不更新：下一轮文件对不上表，自然重新进 pending。
   */
  failFile(filePath: string, err: unknown, { now = Date.now(), dryRun = false, log = null }: { now?: number; dryRun?: boolean; log?: ((line: string) => void) | null } = {}): FailResult {
    const db = this.#db;
    const row = rowOf(db, filePath);
    // attempts 到 5 标 dead，不再自动捞；next_try_at 就写现在（不排退避时间）。口径在 ledger.recordAttempt。
    const rec = recordAttempt({ attempts: row?.attempts as number | undefined, maxAttempts: MAX_ATTEMPTS, error: err, now });
    const attempts = rec.attempts;
    const status = rec.dead ? 'dead' : 'failed';
    const nextTryAt = now;
    const message = rec.message;
    const st = row ? { mtime: row.mtime, size: row.size } : { mtime: 0, size: 0 };
    if (dryRunSkip(dryRun, log, `failFile ${filePath} attempts=${attempts} error=${message!.slice(0, 80)}`)) {
      return { path: filePath, attempts: 0, status: 'dry_run', nextTryAt: 0, lastError: message, dryRun: true };
    }
    // 存储中-9：ingest_files 的 attempts、pending_ops 的入队与状态三步是一笔账，必须同一事务。
    // channel 列自愈先在事务外做：ensurePendingKinds 自己会 BEGIN/ROLLBACK，套进来会把外层滚掉。
    const hasChannel = this.ensureChannelColumn();
    withTransaction(db, () => {
      db.prepare(
        `INSERT INTO ingest_files (path, mtime, size, last_acked_ts, status, attempts, last_error, updated_ts)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(path) DO UPDATE SET
           status = excluded.status,
           attempts = excluded.attempts,
           last_error = excluded.last_error,
           updated_ts = excluded.updated_ts`,
      ).run(filePath, st.mtime as number, st.size as number, Number(row?.last_acked_ts) || 0, status, attempts, message, now);

      const op = this.enqueue('ingest_file', filePath, { path: filePath }, { now, channelColumn: hasChannel });
      db.prepare(
        'UPDATE pending_ops SET attempts = ?, next_try_at = ?, last_error = ?, status = ?, updated_ts = ? WHERE id = ?',
      ).run(attempts, nextTryAt, message, status === 'dead' ? 'dead' : 'pending', now, op.id as LedgerId);
    });

    return { path: filePath, attempts, status, nextTryAt, lastError: message };
  }

  /**
   * 幂等入队（§4.1）：UNIQUE(kind, dedupe_key)，重复入队只刷新 payload/updated_ts，
   * 不动 attempts、不重置退避、不把 dead 拉回 pending（要捞得走 --retry-dead 或 backfill）。
   *
   * reviveDead（2026-11-05 t-fix-truncate）：merge_batch / merge_op / dedupe_check 三个 agent
   * 账本按 dedupe_key 撞上**老账**时，老 attempts 会把新重放直接顶成 dead。所以给一个显式口子：
   * 仅当老行是 dead 且新 payload_json 与老的不同（内容/证据变了，属新证据）才复活——
   * attempts 清零、status 回 pending、next_try_at 从现在起算；payload 一模一样的老账继续躺着，
   * 该人工捞还是人工捞。默认 false，ingest_file 的老语义一个字没变。
   */
  enqueue(kind: string, dedupeKey: string, payload: unknown, {
    now = Date.now(), nextTryAt = now, dryRun = false, log = null, reviveDead = false, reviveKeys = null,
    channelColumn = null,
  }: {
    now?: number;
    nextTryAt?: number;
    dryRun?: boolean;
    log?: ((line: string) => void) | null;
    reviveDead?: boolean;
    reviveKeys?: string[] | null;
    channelColumn?: boolean | null;
  } = {}): Row {
    const db = this.#db;
    const payloadJson = typeof payload === 'string' ? payload : JSON.stringify(payload ?? {});
    if (dryRunSkip(dryRun, log, `enqueue ${kind}/${String(dedupeKey).slice(0, 80)}`)) {
      return dryRunRow(kind, dedupeKey, payloadJson, now, nextTryAt);
    }
    // 2026-11-14：入队就写 channel（池按通道领活）；老库没列时 ensureChannelColumn 先补上，
    // 补不上（视图/只读库）就退回不带列的写法，功能不受影响。
    // channelColumn（2026-11-15）：调用方已经在一个事务里时，把自愈结果传进来，
    // 别在事务里再做一次 ensurePendingKinds 的 BEGIN/ROLLBACK（那会把外层那笔一起滚掉）。
    const hasChannel = channelColumn === null ? this.ensureChannelColumn() : Boolean(channelColumn);
    const channel = channelOfKind(kind);
    const prior = db.prepare('SELECT payload_json, status FROM pending_ops WHERE kind = ? AND dedupe_key = ?')
      .get(kind, String(dedupeKey));
    // reviveKeys：判「新证据」时只比这几项内容，其余字段（比如
    // 每轮都变的失败摘要 reason）不参与。不传就和以前一样整串逐字节比。
    const contentOf = (json: string): string => {
      if (!Array.isArray(reviveKeys) || !reviveKeys.length) return json;
      try {
        const parsed = JSON.parse(json);
        if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return json;
        const picked: Record<string, unknown> = {};
        for (const key of reviveKeys) if (key in parsed) picked[key] = parsed[key];
        return JSON.stringify(picked);
      } catch {
        return json;
      }
    };
    const revive = Boolean(
      reviveDead && prior && prior.status === 'dead' && contentOf(prior.payload_json as string) !== contentOf(payloadJson),
    );
    if (hasChannel) {
      db.prepare(
        `INSERT INTO pending_ops (kind, channel, dedupe_key, payload_json, attempts, next_try_at, last_error, status, created_ts, updated_ts)
         VALUES (?, ?, ?, ?, 0, ?, NULL, 'pending', ?, ?)
         ON CONFLICT(kind, dedupe_key) DO UPDATE SET
           payload_json = excluded.payload_json,
           channel = excluded.channel,
           updated_ts = excluded.updated_ts`,
      ).run(kind, channel, String(dedupeKey), payloadJson, Number(nextTryAt) || now, now, now);
    } else {
      db.prepare(
        `INSERT INTO pending_ops (kind, dedupe_key, payload_json, attempts, next_try_at, last_error, status, created_ts, updated_ts)
         VALUES (?, ?, ?, 0, ?, NULL, 'pending', ?, ?)
         ON CONFLICT(kind, dedupe_key) DO UPDATE SET
           payload_json = excluded.payload_json,
           updated_ts = excluded.updated_ts`,
      ).run(kind, String(dedupeKey), payloadJson, Number(nextTryAt) || now, now, now);
    }
    if (revive) {
      db.prepare(
        "UPDATE pending_ops SET attempts = 0, status = 'pending', next_try_at = ?, updated_ts = ? WHERE kind = ? AND dedupe_key = ?",
      ).run(Number(nextTryAt) || now, now, kind, String(dedupeKey));
      try { log?.(`[retry] revive kind=${kind} key=${String(dedupeKey).slice(0, 16)} reason=payload_changed`); } catch { /* 日志不许反过来影响入队 */ }
    }
    return db.prepare('SELECT * FROM pending_ops WHERE kind = ? AND dedupe_key = ?').get(kind, String(dedupeKey)) as Row;
  }

  /**
   * 领取到期任务（§4.2）：先回收超租约的 running，再按 attempts/next_try_at 取 limit 条并置 running。
   * kind 可选，没接线的 kind 不会被误领。返回 [{id, kind, dedupeKey, payload, attempts, nextTryAt}]。
   */
  dueOps(limit: number, { now = Date.now(), kind = null, leaseMs = LEASE_MS, exclude = [] }: { now?: number; kind?: string | null; leaseMs?: number; exclude?: unknown[] } = {}): DueOp[] {
    const db = this.#db;
    // Infinity = 不限条数（把当期到期的账全领）；0/负数 = 不领
    const unlimited = limit === Infinity;
    const n = unlimited ? 0 : Math.max(0, Math.floor(Number(limit) || 0));
    if (!unlimited && n <= 0) return [];
    // 先回收超租约的 running：跟 reap 一个账（attempts+1，超限 dead），不然崩在半路的毒账
    // 走这条路会被永远捞回、永远死不了。
    reapExpiredOps(db, { now, leaseMs, maxAttempts: MAX_ATTEMPTS });
    const where = ["status = 'pending'", 'next_try_at <= ?'];
    const args: Array<number | string> = [now];
    if (kind) {
      where.push('kind = ?');
      args.push(kind);
    }
    // exclude：本轮刚失败过的 id（next_try_at=now 会让它们立刻又到期）。同一轮只试一次，
    // 下一轮再领——不是时间闸，只是「这一轮别原地重试」。
    const skip = (Array.isArray(exclude) ? exclude : []).map((x) => Number(x)).filter((x) => Number.isFinite(x));
    if (skip.length) {
      where.push(`id NOT IN (${skip.map(() => '?').join(',')})`);
      args.push(...skip);
    }
    const sql = `SELECT * FROM pending_ops WHERE ${where.join(' AND ')} ORDER BY attempts ASC, next_try_at ASC, id ASC`;
    const rows = unlimited ? db.prepare(sql).all(...args) : db.prepare(`${sql} LIMIT ?`).all(...args, n);
    if (!rows.length) return [];
    // 存储中-6：先占后取。UPDATE 必须带 status='pending'，changes()!==1 说明这一行被别的池
    // 先领走了（或者刚被别人标了别的状态），跳过它，别把别人的活重复派出去。
    const mark = db.prepare("UPDATE pending_ops SET status = 'running', updated_ts = ? WHERE id = ? AND status = 'pending'");
    const out = [];
    for (const row of rows) {
      if (Number(mark.run(now, row.id).changes) !== 1) continue;
      let payload = null;
      try {
        payload = JSON.parse(row.payload_json as string);
      } catch {
        payload = null;
      }
      out.push({
        id: row.id as number,
        kind: row.kind as string,
        dedupeKey: row.dedupe_key as string,
        payload,
        payloadJson: row.payload_json as string,
        attempts: Number(row.attempts) || 0,
        nextTryAt: Number(row.next_try_at) || 0,
      });
    }
    return out;
  }

  /**
   * 收尾一个任务：status='done' 直接标；status='pending' 表示这次没成——attempts+1、
   * 按 1/5/15 分钟退避、attempts>3 转 dead；status='dead' 直接躺平等人工捞。
   *
   * increment:false（2026-11-05 t-fix-truncate）：有些重放路径里 attempts 已经由下面那层
   * （比如 dedupe 的 enqueue ON CONFLICT）加过了，这里只把 running 放回 pending、不重复计数。
   */
  markOp(id: LedgerId, status: string, err: unknown = null, { now = Date.now(), dryRun = false, log = null, increment = true }: { now?: number; dryRun?: boolean; log?: ((line: string) => void) | null; increment?: boolean } = {}): Row | null {
    const db = this.#db;
    if (dryRunSkip(dryRun, log, `markOp id=${id} status=${status}`)) {
      return { id, status: 'dry_run', dryRun: true };
    }
    const row = db.prepare('SELECT * FROM pending_ops WHERE id = ?').get(id);
    if (!row) return null;
    const message = err === null || err === undefined ? row.last_error as string | null : String((err as Error && ((err as Error).message || err)) || err as string).slice(0, 500);
    // 状态迁移交给 ledger：pending→fail（attempts+1，到 5 转 dead）、done→finish、dead→kill。
    // 阈值不再在这里写死；increment:false 是「attempts 已在别处加过」，只放回 pending。
    const ledger = this.ledgerFor(row.channel as string || channelOfKind(row.kind), [], LEASE_MS);
    if (status === 'pending' && increment === false) {
      ledger.release(id, { error: message, now });
    } else if (status === 'pending') {
      ledger.fail(id, { attempts: row.attempts as number | undefined, error: message, now });
    } else if (status === 'done') {
      ledger.finish(id, { error: message, now });
    } else if (status === 'dead') {
      ledger.kill(id, { error: message, now });
    } else {
      // running 这类原始状态迁移不经过状态机（重放时会直接改回 running）
      db.prepare('UPDATE pending_ops SET status = ?, last_error = ?, updated_ts = ? WHERE id = ?').run(status, message, now, id);
    }
    return db.prepare('SELECT * FROM pending_ops WHERE id = ?').get(id) as Row | null;
  }

  /**
   * 手动回捞（§6.3），默认不触发：
   *   --backfill /abs/path.jsonl → 该文件 status='pending'、last_acked_ts=0；
   *   --backfill 2026-10-01     → 最后事件时间晚于该日期的文件置 pending，
   *                               last_acked_ts = min(当前 ack, 该日 00:00)，不强制覆盖。
   * 只改 ingest_files，不跑模型。返回 {mode, changed, skipped, since?}。
   */
  backfill(spec: string, { files = null, cache = null, now = Date.now() }: { files?: SessionFileInfo[] | null; cache?: Map<string, SessionEvent[]> | null; now?: number } = {}): BackfillResult {
    const db = this.#db;
    const raw = String(spec || '').trim();
    if (!raw) throw new Error('--backfill 需要一个绝对路径或 YYYY-MM-DD');
    const out: BackfillResult = { mode: '', changed: [], skipped: [], since: null };

    if (!/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
      const abs = path.resolve(raw);
      const row = rowOf(db, abs);
      if (row) {
        db.prepare(
          "UPDATE ingest_files SET status = 'pending', last_acked_ts = 0, attempts = 0, last_error = NULL, updated_ts = ? WHERE path = ?",
        ).run(now, abs);
      } else {
        const st = statFile(abs);
        if (!st) {
          out.mode = 'path';
          out.skipped.push({ path: abs, reason: 'not_found' });
          return out;
        }
        db.prepare(
          `INSERT INTO ingest_files (path, mtime, size, last_acked_ts, status, attempts, last_error, updated_ts)
           VALUES (?, ?, ?, 0, 'pending', 0, NULL, ?)`,
        ).run(abs, st.mtime, st.size, now);
      }
      out.mode = 'path';
      out.changed.push(abs);
      return out;
    }

    const since = parseSinceDay(raw);
    if (since === null) throw new Error(`看不懂的 --backfill 参数：${raw}`);
    out.mode = 'since';
    out.since = since;
    const list = files || scanSessions(paths.sessionsRoot());
    const update = db.prepare(
      "UPDATE ingest_files SET status = 'pending', last_acked_ts = ?, attempts = 0, last_error = NULL, updated_ts = ? WHERE path = ?",
    );
    for (const file of list || []) {
      const row = rowOf(db, file.path);
      const events = file.lastEventTs !== undefined ? null : loadEvents(file.path, cache);
      if (file.lastEventTs === undefined && !events) {
        out.skipped.push({ path: file.path, reason: 'unreadable' });
        continue;
      }
      const lastTs = file.lastEventTs !== undefined ? Number(file.lastEventTs) || 0 : lastEventTsOf(events);
      if (lastTs <= since) {
        out.skipped.push({ path: file.path, reason: 'before_since' });
        continue;
      }
      if (row) {
        update.run(Math.min(Number(row.last_acked_ts) || 0, since), now, file.path);
      } else {
        const st = file.mtime !== undefined ? { mtime: file.mtime, size: file.size } : statFile(file.path);
        if (!st) {
          out.skipped.push({ path: file.path, reason: 'not_found' });
          continue;
        }
        db.prepare(
          `INSERT INTO ingest_files (path, mtime, size, last_acked_ts, status, attempts, last_error, updated_ts)
           VALUES (?, ?, ?, ?, 'pending', 0, NULL, ?)`,
        ).run(file.path, st.mtime, st.size, since, now);
      }
      out.changed.push(file.path);
    }
    return out;
  }
}

