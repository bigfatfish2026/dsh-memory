/**
 * observation 任务队列（2026-11-21，docs/observation-plan-v2.md §2/§3）。
 *
 * 只碰 observe_task 一张表：写侧登记、按租约领取、版本校验收尾。
 * 表由 scripts/apply-observe-schema.mjs 手建，本文件不建表；缺表时入队返回 null，
 * 领活返回空数组，收尾返回 false，调用方自己决定跳过还是报错。
 *
 * 事务口径：四个函数内部不开 BEGIN/COMMIT/SAVEPOINT，只用单条语句 + 带条件 CAS。
 * 调用方在写事务里可以直接调，出错由外层事务回滚；同一事务里反复 enqueue 只让
 * version 往上涨，不会多出行。
 *
 * 版本防旧结果：claim 返回领取后的 version，complete/fail 都带 version 条件；
 * 期间只要有人 enqueue（证据变了）或租约过期被别人重领，version 就变，旧结果落不进去、
 * 也不会把新任务标 done。complete/fail 传了 owner 时再校验 lease_owner，双保险。
 */
import type { DatabaseSync } from 'node:sqlite';

/** 默认租约：模型一次生成 + 落库 10 分钟够用；过期由 claim 直接重领，不另跑 reap。 */
export const OBSERVE_LEASE_MS = 10 * 60 * 1000;
/** 每轮默认最多 20 个实体（灰度预算，见 docs/observation-plan-v2.md §8）。 */
export const OBSERVE_BATCH = 20;
/** 失败回队列时 last_error 只留前 500 字，和 pending_ops 一个口径。 */
const LAST_ERROR_MAX = 500;

export type ObserveStatus = 'pending' | 'leased' | 'done';

/** 写侧登记参数：只有这几个键会被读。 */
export interface EnqueueObserveOptions {
  /** 实体 id（entities.id）；空串表示暂未解析到实体行。 */
  entityId?: string;
  /** 为什么重新排队：save / archive / merge / split / text_update / unmount …，排障用。 */
  reason?: string;
  /** 覆盖当前时间（毫秒），测试用。 */
  now?: number;
  /** 出错时的日志出口；不传就静默返回 null。 */
  log?: (line: string) => void;
}

/** 入队结果；db 为空 / 键为空 / 缺表时返回 null。 */
export interface EnqueueObserveResult {
  entityKey: string;
  version: number;
  /** true = 新插一行；false = 命中已有行、version 涨了一次。 */
  created: boolean;
}

/** 领活参数。 */
export interface ClaimObserveOptions {
  limit?: number;
  owner?: string;
  leaseMs?: number;
  now?: number;
}

/** 领到手的一项：version 必须原样带回 complete/fail。 */
export interface ObserveClaim {
  entityKey: string;
  entityId: string;
  version: number;
  reason: string;
  tries: number;
}

/** 收尾结果：false = 版本不符 / 状态不符 / owner 不符 / 缺表，旧结果作废，行未被改动。 */
export type ObserveSettleResult = boolean;

/** 收尾选项：owner 只有显式传了才校验，旧调用不传时行为不变。 */
export interface ObserveSettleOptions {
  now?: number;
  /** 领取时的 lease_owner；传了才要求库里一致，不传不校验。 */
  owner?: string;
}

/** 表在不在：老库没跑迁移时调用方要能优雅跳过。 */
export function hasObserveTaskTable(db: DatabaseSync | null | undefined): boolean {
  if (!db) return false;
  try {
    return Boolean(db.prepare("SELECT 1 AS ok FROM sqlite_master WHERE type='table' AND name='observe_task'").get());
  } catch {
    return false;
  }
}

function toInt(value: unknown, fallback: number): number {
  const n = Number(value);
  return Number.isFinite(n) ? Math.trunc(n) : fallback;
}

function messageOf(error: unknown): string {
  return String((error && (error as { message?: unknown }).message) || error);
}

/**
 * 登记一个待重算 observation 的实体。
 *
 * 新建：version=1、status='pending'、enqueued_ts=updated_ts=now、entity_id/reason 取入参。
 * 已有：version=version+1、status 拉回 'pending'、清租约、updated_ts=now、reason 覆盖；
 *       enqueued_ts 不动（排队先后仍按第一次登记算，避免证据一变更就插队）。
 * 证据变更打断在跑的任务就靠这一条：version 变了，旧结果 complete/fail 都会返回 false。
 */
export function enqueueObserveTask(
  db: DatabaseSync | null | undefined,
  entityKey: string,
  opts: EnqueueObserveOptions = {},
): EnqueueObserveResult | null {
  const key = String(entityKey ?? '').trim();
  const log = typeof opts.log === 'function' ? opts.log : null;
  if (!key) return null;
  if (!db || !hasObserveTaskTable(db)) {
    log?.(`[observe] 没有 observe_task，登记跳过 key=${key}（先跑 scripts/apply-observe-schema.mjs）`);
    return null;
  }
  const now = toInt(opts.now, Date.now());
  const entityId = String(opts.entityId ?? '');
  const reason = String(opts.reason ?? '');
  try {
    const before = db.prepare('SELECT version FROM observe_task WHERE entity_key = ?').get(key) as
      | { version?: unknown }
      | undefined;
    db.prepare(
      `INSERT INTO observe_task(entity_key, entity_id, status, version, lease_owner, lease_until,
                                enqueued_ts, updated_ts, tries, last_error, reason)
       VALUES (?, ?, 'pending', 1, '', 0, ?, ?, 0, '', ?)
       ON CONFLICT(entity_key) DO UPDATE SET
         version     = observe_task.version + 1,
         status      = 'pending',
         lease_owner = '',
         lease_until = 0,
         updated_ts  = excluded.updated_ts,
         entity_id   = CASE WHEN excluded.entity_id <> '' THEN excluded.entity_id ELSE observe_task.entity_id END,
         reason      = excluded.reason`,
    ).run(key, entityId, now, now, reason);
    const row = db.prepare('SELECT version FROM observe_task WHERE entity_key = ?').get(key) as { version?: unknown };
    return { entityKey: key, version: toInt(row?.version, 0), created: !before };
  } catch (error) {
    log?.(`[observe] 登记失败 key=${key}：${messageOf(error)}`);
    return null;
  }
}

/**
 * 领一批活：pending 的，或 leased 但租约已过期（lease_until < now）的。
 *
 * 先按 enqueued_ts 升序（同时间按 entity_key 稳定排序）取候选，再逐行 CAS：
 * UPDATE 同时校验 version 与「pending 或已过期」，只有真正改到的行才返回，
 * 同一进程/同一事务里不会被领两次。
 *
 * 过期重领（原来是 leased）同时让 version+1：旧 owner 手里那一版立即失效，
 * 迟到 complete/fail 的版本条件不再命中。pending 首次领取不涨 version。
 * 返回的 version 是领取后行上的版本（重领时已加过 1），必须原样带回 complete/fail。
 */
export function claimObserveTasks(
  db: DatabaseSync | null | undefined,
  { limit = OBSERVE_BATCH, owner = '', leaseMs = OBSERVE_LEASE_MS, now = Date.now() }: ClaimObserveOptions = {},
): ObserveClaim[] {
  if (!db || !hasObserveTaskTable(db)) return [];
  const cap = Math.max(1, toInt(limit, OBSERVE_BATCH));
  const lease = Math.max(1000, toInt(leaseMs, OBSERVE_LEASE_MS));
  const at = toInt(now, Date.now());
  const ownerText = String(owner ?? '');
  const rows = db.prepare(
    `SELECT entity_key, entity_id, status, version, reason, tries
       FROM observe_task
      WHERE status = 'pending' OR (status = 'leased' AND lease_until < ?)
      ORDER BY enqueued_ts ASC, entity_key ASC
      LIMIT ?`,
  ).all(at, cap) as Array<{
    entity_key?: unknown;
    entity_id?: unknown;
    status?: unknown;
    version?: unknown;
    reason?: unknown;
    tries?: unknown;
  }>;
  if (!rows.length) return [];
  // SET 里的表达式都按旧行求值：过期重领的 version+1，pending 首次领取保持原值
  const update = db.prepare(
    `UPDATE observe_task
        SET status = 'leased', lease_owner = ?, lease_until = ?, updated_ts = ?,
            version = version + CASE WHEN status = 'leased' THEN 1 ELSE 0 END
      WHERE entity_key = ? AND version = ?
        AND (status = 'pending' OR (status = 'leased' AND lease_until < ?))`,
  );
  const out: ObserveClaim[] = [];
  for (const row of rows) {
    const entityKey = String(row.entity_key ?? '');
    const version = toInt(row.version, 0);
    const res = update.run(ownerText, at + lease, at, entityKey, version, at);
    if (Number(res.changes) > 0) {
      // 候选取到的 status 与 CAS 修改的是同一行同一版本：leased 的都已涨到 version+1
      const claimedVersion = String(row.status ?? '') === 'leased' ? version + 1 : version;
      out.push({
        entityKey,
        entityId: String(row.entity_id ?? ''),
        version: claimedVersion,
        reason: String(row.reason ?? ''),
        tries: toInt(row.tries, 0),
      });
    }
  }
  return out;
}

/**
 * 提交结果：只有「还在租约里、version 没变、owner 对得上」才置 done。
 * 返回 false 时一行都不改——证据在模型跑期间变了，或租约已被别人重领，
 * 这份结果作废，任务留给当前 owner / 下一轮。
 */
export function completeObserveTask(
  db: DatabaseSync | null | undefined,
  entityKey: string,
  version: number,
  { now = Date.now(), owner }: ObserveSettleOptions = {},
): ObserveSettleResult {
  if (!db || !hasObserveTaskTable(db)) return false;
  const at = toInt(now, Date.now());
  // owner 不传时绑 NULL，条件恒真，旧调用行为不变
  const ownerText = owner === undefined ? null : String(owner);
  const res = db.prepare(
    `UPDATE observe_task
        SET status = 'done', lease_owner = '', lease_until = 0, updated_ts = ?, last_error = ''
      WHERE entity_key = ? AND status = 'leased' AND version = ?
        AND (? IS NULL OR lease_owner = ?)`,
  ).run(at, String(entityKey ?? '').trim(), toInt(version, -1), ownerText, ownerText);
  return Number(res.changes) > 0;
}

/**
 * 失败收尾：version 与 owner 一致才回 pending、tries+1、记 last_error、清租约。
 * 返回 false（版本已被 enqueue 顶掉 / 已过期重领 / owner 不符）时不改任何行，
 * 旧任务的失败账不记在新任务头上，也不会清掉新 owner 的租约。
 */
export function failObserveTask(
  db: DatabaseSync | null | undefined,
  entityKey: string,
  version: number,
  error: unknown,
  { now = Date.now(), owner }: ObserveSettleOptions = {},
): ObserveSettleResult {
  if (!db || !hasObserveTaskTable(db)) return false;
  const at = toInt(now, Date.now());
  const message = messageOf(error).slice(0, LAST_ERROR_MAX);
  const ownerText = owner === undefined ? null : String(owner);
  const res = db.prepare(
    `UPDATE observe_task
        SET status = 'pending', tries = tries + 1, last_error = ?, lease_owner = '', lease_until = 0,
            updated_ts = ?
      WHERE entity_key = ? AND version = ?
        AND (? IS NULL OR lease_owner = ?)`,
  ).run(message, at, String(entityKey ?? '').trim(), toInt(version, -1), ownerText, ownerText);
  return Number(res.changes) > 0;
}
