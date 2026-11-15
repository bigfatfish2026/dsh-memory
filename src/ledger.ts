/**
 * 队列账本（2026-11-14，阶段 2）。
 *
 * 两张队列表（curate_queue / pending_ops）物理不动，状态机收到这里：
 *   pending --claim--> running --finish--> done
 *                          \--fail--> pending（退避到期）/ dead（attempts 到阈值）
 *   running --reap（租约过期）--> pending
 *
 * 表差异全部走 ops 适配器（各表自己的 SQL），本文件只留三件共享逻辑：
 *   1) 领活：按 limit 选行 → 逐行 CAS 标 running → 抢到的才返回（绝不满队标 running）；
 *   2) 收尾：done / 回 pending / dead 的状态迁移，attempts 阈值与退避阶梯；
 *   3) 租约：reap 的语义由适配器实现，入口统一；回收也算一次尝试，阈值到头由适配器判死。
 *
 * attempts 什么时候 +1 是两表的历史差异，不硬编码：attemptsAt='claim'（curate_queue，
 * 领取即计一次）或 'fail'（pending_ops，失败才计一次）。阈值 maxAttempts 也是参数，
 * 两个调用方各传自己的（3 / 5）。
 *
 * 2026-11-15：闭包收成 Ledger 类，状态机状态（阈值/租约/退避/适配器/日志）
 * 全是私有字段；createLedger 保留为工厂（测试与旧引用照用），recordAttempt 仍是纯函数。
 */
export const DEFAULT_MAX_ATTEMPTS = 5;
export const DEFAULT_LEASE_MS = 10 * 60000;

/** 一条队列行的 id：pending_ops 是整数，curate_queue 也是整数，但适配器可能给字符串。 */
export type LedgerId = string | number;

/** 适配器 select 出来的原始行：至少带 attempts，其余列按表自己的来。 */
export type LedgerRow = Record<string, unknown> & { attempts?: number | string; id?: LedgerId };

/** mapClaimed 之后的统一行：调用方只认这几个字段。 */
export interface LedgerItem {
  id: LedgerId;
  kind: string;
  channel?: string;
  dedupeKey?: string;
  payload: Record<string, unknown> | null;
  payloadJson: string;
  attempts: number;
  nextTryAt?: number;
  [key: string]: unknown;
}

/** 分状态计数；表不在 / 通道没活时适配器只回 total，池只拿 pending 判空。 */
export interface LedgerCounts {
  total: number;
  pending: number;
  running: number;
  done: number;
  dead: number;
  skipped?: number;
}

/** 一张表的 SQL 适配器；跨表 SQL 不进 ledger，阈值/租约由 Ledger 传下去。 */
export interface LedgerOps {
  ready: () => boolean;
  select: (q: { limit: number; exclude: unknown[]; now: number; kinds: string[]; channel: string }) => LedgerRow[] | null;
  markRunning: (row: LedgerRow, opts: { now: number; owner: string; leaseMs: number }) => number | bigint;
  mapClaimed: (row: LedgerRow, attempts: number) => LedgerItem;
  setDone: (id: LedgerId, opts: { now: number; message: string | null }) => unknown;
  setBack: (id: LedgerId, opts: { message: string | null; attempts: number | null; nextTryAt: number; now: number }) => unknown;
  setDead: (id: LedgerId, opts: { message: string | null; attempts: number | null; now: number }) => unknown;
  setSkipped?: (id: LedgerId, opts: { message: string | null; now: number }) => unknown;
  reap: (q: { now: number; channel: string; kinds: string[]; maxAttempts: number }) => number | bigint;
  counts: (q: { channel: string; kinds: string[] }) => LedgerCounts;
  backState?: string;
}

/**
 * attempts 记账的唯一口径。
 *
 * 原来 5 个入口各自 `attempts + 1`，阈值和 last_error 截断各写一遍（ingest failFile、
 * merge_batch 入队、dedupe 撞门、租约回收、claim CAS）。SQL 仍留在各表自己的地方
 * （跨表 SQL 不进 ledger），但「+1 / 到阈值判死 / 原因留 500 字」只有这一份。
 *
 * @param {object} opts
 * @param {number} [opts.attempts] 记账前的 attempts
 * @param {number} [opts.maxAttempts] 判死阈值（curate_queue 3 / pending_ops 5）
 * @param {*} [opts.error] 失败原因，落 last_error
 * @param {number} [opts.now]
 * @returns {{attempts:number, dead:boolean, status:'dead'|'pending', message:string|null, now:number}}
 */
export function recordAttempt({ attempts = 0, maxAttempts = DEFAULT_MAX_ATTEMPTS, error = null, now = Date.now() }: { attempts?: number; maxAttempts?: number; error?: unknown; now?: number } = {}): { attempts: number; dead: boolean; status: 'dead' | 'pending'; message: string | null; now: number } {
  const next = Math.max(0, Number(attempts) || 0) + 1;
  const cap = Math.max(1, Math.floor(Number(maxAttempts) || DEFAULT_MAX_ATTEMPTS));
  const message = error === null || error === undefined
    ? null
    : String((error as Error && ((error as Error).message || error)) || 'unknown' as string).slice(0, 500);
  const dead = next >= cap;
  return { attempts: next, dead, status: dead ? 'dead' : 'pending', message, now };
}

function messageOf(error: unknown): string | null {
  if (error === null || error === undefined) return null;
  return String((error as Error && ((error as Error).message || error)) || error as string).slice(0, 500);
}

/**
 * 队列状态机。一个实例 = 一张表的一个通道账本；阈值/租约/退避/适配器都是构造参数。
 */
export class Ledger {
  #channel: string;

  #kinds: string[];

  #maxAttempts: number;

  #leaseMs: number;

  #attemptsAt: 'claim' | 'fail';

  #backoffMs: ((attempts: number) => number) | null;

  #ops: LedgerOps;

  #log: (line: string) => void;

  /**
   * @param {object} spec 见 createLedger 的 JSDoc
   */
  constructor({
    channel = '',
    kinds = [],
    maxAttempts = DEFAULT_MAX_ATTEMPTS,
    leaseMs = DEFAULT_LEASE_MS,
    attemptsAt = 'fail',
    backoffMs = null,
    ops,
    log = () => {},
  }: {
    channel?: string;
    kinds?: string[];
    maxAttempts?: number;
    leaseMs?: number;
    attemptsAt?: 'claim' | 'fail';
    backoffMs?: ((attempts: number) => number) | null;
    ops?: LedgerOps;
    log?: (line: string) => void;
  } = {}) {
    if (!ops) throw new Error('ledger: 缺少 ops 适配器');
    this.#channel = channel;
    this.#kinds = kinds;
    this.#maxAttempts = maxAttempts;
    this.#leaseMs = leaseMs;
    this.#attemptsAt = attemptsAt;
    this.#backoffMs = backoffMs;
    this.#ops = ops;
    this.#log = log;
  }

  get channel() {
    return this.#channel;
  }

  get kinds() {
    return this.#kinds;
  }

  get maxAttempts() {
    return this.#maxAttempts;
  }

  get leaseMs() {
    return this.#leaseMs;
  }

  /**
   * 两表 attempts 的记账时机不同，方向也相反：
   *   claim 返回：curate_queue 在领取时已经 +1（返回 n+1），pending_ops 返回原值（n）；
   *   fail 判定：curate_queue 的 n 已含本次（用 n），pending_ops 用 n+1。
   */
  #claimedAttempts(raw: unknown): number {
    const n = Math.max(0, Number(raw) || 0);
    return this.#attemptsAt === 'claim' ? n + 1 : n;
  }

  #failedAttempts(raw: unknown): number {
    const n = Math.max(0, Number(raw) || 0);
    return this.#attemptsAt === 'claim' ? n : n + 1;
  }

  /**
   * 领一批：limit 由调用方的并发数决定。
   * @returns {Array<object>} 适配器映射后的统一行（id/kind/dedupeKey/payload/attempts/...）
   */
  claim(limit: number, { exclude = [], now = Date.now(), owner = '' }: { exclude?: unknown[]; now?: number; owner?: string } = {}): LedgerItem[] {
    const n = Math.max(0, Math.floor(Number(limit) || 0));
    if (!n || !this.#ops.ready()) return [];
    const rows = this.#ops.select({ limit: n, exclude, now, kinds: this.#kinds, channel: this.#channel }) || [];
    const out = [];
    for (const row of rows) {
      // 两步 CAS：UPDATE 带 state=pending 条件，changes()===1 才算领到；抢不到换下一行。
      const changes = Number(this.#ops.markRunning(row, { now, owner, leaseMs: this.#leaseMs })) || 0;
      if (changes !== 1) continue;
      out.push(this.#ops.mapClaimed(row, this.#claimedAttempts(row.attempts)));
    }
    return out;
  }

  /** 收尾成功：置 done。 */
  finish(id: LedgerId, { now = Date.now(), error = null }: { now?: number; error?: unknown } = {}): unknown {
    return this.#ops.setDone(id, { now, message: messageOf(error) }) ?? null;
  }

  /**
   * 收尾失败：attempts 到阈值转 dead，否则回 pending 并按 backoffMs 排下次到期时间。
   * @param {object} opts.attempts 领取时拿到的 attempts（curate_queue 已含本次，pending_ops 不含）
   */
  fail(id: LedgerId, { attempts = 0, error = null, now = Date.now() }: { attempts?: number; error?: unknown; now?: number } = {}): { state: string; attempts: number; nextTryAt?: number } {
    const effective = this.#failedAttempts(attempts);
    const message = messageOf(error);
    if (effective >= this.#maxAttempts) {
      this.#ops.setDead(id, { message, attempts: effective, now });
      this.#log(`[ledger:${this.#channel}] dead id=${id} attempts=${effective} 到阈值 ${this.#maxAttempts}`);
      return { state: 'dead', attempts: effective };
    }
    const delay = typeof this.#backoffMs === 'function' ? Math.max(0, Number(this.#backoffMs(effective)) || 0) : 0;
    const nextTryAt = now + delay;
    this.#ops.setBack(id, { message, attempts: effective, nextTryAt, now });
    // 对外报这张表自己的状态名（curate_queue 叫 ready，pending_ops 叫 pending）
    return { state: this.#ops.backState || 'pending', attempts: effective, nextTryAt };
  }

  /** 放回 pending 但不记账（attempts 已在别处加过）：只给确实需要的路径用。 */
  release(id: LedgerId, { error = null, now = Date.now() }: { error?: unknown; now?: number } = {}): { state: string } {
    this.#ops.setBack(id, { message: messageOf(error), attempts: null, nextTryAt: now, now });
    return { state: this.#ops.backState || 'pending' };
  }

  /** 直接判死（毒账、payload 缺字段这类重试也不会变好的活）。 */
  kill(id: LedgerId, { error = null, now = Date.now() }: { error?: unknown; now?: number } = {}): { state: 'dead' } {
    this.#ops.setDead(id, { message: messageOf(error), attempts: null, now });
    return { state: 'dead' };
  }

  /** 直接跳过（网关全拒、正文已不存在这类不该占重试额度的活）。 */
  skip(id: LedgerId, { error = null, now = Date.now() }: { error?: unknown; now?: number } = {}): { state: 'skipped' } {
    if (typeof this.#ops.setSkipped !== 'function') return { state: 'skipped' };
    this.#ops.setSkipped(id, { message: messageOf(error), now });
    return { state: 'skipped' };
  }

  /**
   * 租约回收：running 且租约过期 → 由适配器回收（pending_ops：attempts+1，到阈值标 dead 并留
   * `lease_expired`；curate_queue：回待领）。返回捞回（含判死）条数。
   * maxAttempts 传给适配器，判死只在适配器那一层落 SQL。
   */
  reap({ now = Date.now() }: { now?: number } = {}): number {
    return Number(this.#ops.reap({ now, channel: this.#channel, kinds: this.#kinds, maxAttempts: this.#maxAttempts })) || 0;
  }

  /** 分状态计数；通道没活（kinds 为空/表不在）时返回 { total: 0 }。 */
  stats(): LedgerCounts {
    return this.#ops.counts({ channel: this.#channel, kinds: this.#kinds }) || { total: 0 } as LedgerCounts;
  }
}

/**
 * 建一个账本（旧工厂名保持不变）。内部就是 new Ledger(spec)；
 * 结构参数见类构造器：channel / kinds / maxAttempts / leaseMs / attemptsAt / backoffMs / ops / log。
 */
export function createLedger(spec: ConstructorParameters<typeof Ledger>[0] = {}): Ledger {
  return new Ledger(spec);
}

/**
 * 适配器接口（各表在自己的文件里实现，SQL 不跨表共享）：
 *   ready()                        → 表在不在（false 时 claim 直接空）
 *   select({limit, exclude, now, kinds, channel}) → 行数组（按 attempts/next_try_at/id 排序，带 LIMIT）
 *   markRunning(row, {now, owner, leaseMs})       → UPDATE 影响行数（必须是 WHERE ... AND 待领状态 的 CAS）
 *   mapClaimed(row, attempts)      → { id, kind, channel, dedupeKey, payload, payloadJson, attempts, nextTryAt, ... }
 *   setDone(id, {now, message})    → 影响行数或行
 *   setBack(id, {message, attempts, nextTryAt, now}) → attempts=null 表示不动 attempts
 *   setDead(id, {message, attempts, now})            → 同上
 *   reap({now, channel, kinds, maxAttempts}) → 捞回条数（pending_ops 的回收要 attempts+1，到 maxAttempts 标 dead）
 *   counts({channel, kinds})       → { total, pending, running, done, dead, skipped? }
 */
