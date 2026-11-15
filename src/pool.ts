/**
 * 队列 worker 池（2026-11-14 维护者拍板：整理改成「队列 + 池」，不再是一条顺序大函数）。
 *
 * 池的语义只有五条，每个通道都一样：
 *   1) 领活：从队列表按 channel 领一批，批大小 = 本通道当前并发数，领到的当场标 running
 *      （带租约，靠 updated_ts + leaseMs，超时由 reap 捞回 pending）。**绝不满队标 running**。
 *   2) 并发问模型：批内 prepare 最多 N 路同时跑（N = 并发数），只读、不写库。
 *   3) 串行落地：批内按领到的顺序逐条 commit，写库统一过 writeLock（同一时刻只有一条写在跑）。
 *   4) 空了就等：领不到活时 sleep(idleMs) 或等 notify() 信号，不忙等空转。
 *   5) 不收敛不算完：prepare/commit 抛错交给 release（attempts+1，5 次 dead），池继续。
 *
 * stopWhenEmpty=true 时，领不到活就收摊（定时一轮跑完就退）；false 时长期挂着等信号。
 */
import type { Row } from './store/types.js';

/** 池里一条活的行：队列行形状由调用方给，池只认 id。 */
type PoolRow = Row;

/** 写库串行闸：同一时刻只放一条进临界区，排队的不算并发。 */
export interface WriteLock {
  <T>(fn: () => T): Promise<T>;
  stats?: () => { active: number; peak: number };
}

/** commit / release 的返回面：池只认 dead，其余值原样回给调用方。 */
export type PoolOutcome = { dead?: unknown } | null | undefined;

/** 池自己的统计计数（stats() 返回的就是它）。 */
export interface PoolState {
  channel: string | undefined;
  startedAt: number;
  ms: number;
  batches: number;
  claimed: number;
  landed: number;
  failed: number;
  dead: number;
  peakRunning: number;
  running: number;
  idleRounds: number;
  errors: number;
  stopped: boolean;
  drained: boolean;
}

/** 批内 prepare 的结果信封（成功带 box，失败带 error）。 */
type PrepareBox = { ok: true; box: unknown } | { ok: false; error: unknown };

/** ChannelPool 的注入面：各通道自己给，池不认识任何业务。 */
export interface ChannelPoolOptions {
  channel?: string;
  claim?: (limit: number, exclude: number[]) => PoolRow[];
  prepare?: (row: PoolRow, index: number) => unknown;
  commit?: (row: PoolRow, box: unknown, index: number) => PoolOutcome;
  release?: (row: PoolRow, err: unknown, index: number) => PoolOutcome;
  onLanded?: ((row: PoolRow, box: unknown, out: unknown, index: number, ms: number) => void) | null;
  concurrency?: number | (() => number);
  writeLock?: WriteLock | null;
  reap?: (() => void) | null;
  remaining?: (() => number) | null;
  log?: (line: string) => void;
  idleMs?: number;
  stopWhenEmpty?: boolean;
}

/** 列表型并发池的一条结果：成功带 commit 返回值，失败带阶段与错误。 */
/** 单条落地结果；value 是 unknown，类型标注交给调用方。 */
export type PoolResult = { ok: true; value: unknown } | { ok: false; phase: string; error: unknown };

/** runListPool 的注入面。 */
export interface RunListPoolOptions<T> {
  items?: T[];
  concurrency?: number;
  prepare?: (item: T, index: number) => unknown;
  commit?: (value: unknown, index: number) => unknown;
  heartbeat?: () => void;
  onLanded?: ((info: { index: number; item: T; result: PoolResult; ms: number }) => void) | null;
  writeLock?: WriteLock | null;
}

/** 空队列时的等待上限（ms）。有 notify() 会立刻醒，没信号就按这个粒度短轮询。 */
export const POOL_IDLE_MS = 1000;

/** 池内并发的硬顶：跟 ingest 同口径，配大了也不超。 */
export const POOL_CONCURRENCY_CAP = 64;

/**
 * 写库串行闸：所有通道共用同一个实例，保证「同一时刻只有一条写在跑」。
 * commit 内容是同步 SQL（store 的 writeTx 里没有 await），所以闸只是兜底：
 * 真出现跨 await 的长事务时，它能把另一条池挡在门外，而不是让 SQLite 自己 busy。
 */
export function createWriteLock(): WriteLock {
  let tail = Promise.resolve();
  let active = 0;
  let peak = 0;
  const lock: WriteLock = <T>(fn: () => T): Promise<T> => {
    // 只统计「真正在跑」的临界区，排队等闸的不算并发（排队是常态，不能把 peak 记成排队数）
    const run = tail.then(() => {
      active += 1;
      if (active > peak) peak = active;
      try {
        return fn();
      } finally {
        active -= 1;
      }
    });
    // 链条本身不许因为一次失败断掉：失败由调用方接，链继续往下走
    tail = run.then(() => {}, () => {});
    return run;
  };
  lock.stats = () => ({ active, peak });
  return lock;
}

/**
 * 通道池：小批领活 → 批内并发 prepare → 串行 commit（写库过闸）→ 空了等信号。
 *
 * 2026-11-15：闭包收成 ChannelPool 类。池自己的状态（统计计数、
 * waiter、loopPromise、本轮已试 id）都是私有字段；claim/prepare/commit/release 由
 * 调用方按通道注入，池不认识任何业务。createChannelPool 保留为工厂名。
 *
 * @param {object} opts
 * @param {string} opts.channel           通道名（日志里用）
 * @param opts.claim  领一批并标 running；exclude = 本轮已经试过的 id
 * @param opts.prepare  只读/问模型，可并发
 * @param opts.commit  落地，串行
 * @param opts.release  失败收口（attempts+1 / dead）
 * @param [opts.onLanded]
 * @param [opts.concurrency=1]
 * @param [opts.writeLock]
 * @param [opts.reap]  每轮领活前的租约回收
 * @param [opts.remaining]  还剩多少条（日志用）
 * @param [opts.log]
 * @param {number} [opts.idleMs]
 * @param {boolean} [opts.stopWhenEmpty=true]
 */
export class ChannelPool {
  #channel: string | undefined;

  #claim: ((limit: number, exclude: number[]) => PoolRow[]) | undefined;

  #prepare: ((row: PoolRow, index: number) => unknown) | undefined;

  #commit: ((row: PoolRow, box: unknown, index: number) => PoolOutcome) | undefined;

  #release: ((row: PoolRow, err: unknown, index: number) => PoolOutcome) | undefined;

  #onLanded: ((row: PoolRow, box: unknown, out: unknown, index: number, ms: number) => void) | null | undefined;

  #concurrency: number | (() => number) | undefined;

  #writeLock: WriteLock | null | undefined;

  #reap: (() => void) | null | undefined;

  #remaining: (() => number) | null | undefined;

  #log: (line: string) => void;

  #idleMs: number | undefined;

  #stopWhenEmpty: boolean | undefined;

  #state: PoolState = {
    channel: '',
    startedAt: 0,
    ms: 0,
    batches: 0,
    claimed: 0,
    landed: 0,
    failed: 0,
    dead: 0,
    peakRunning: 0,
    running: 0,
    idleRounds: 0,
    errors: 0,
    stopped: false,
    drained: false,
  };

  #waiter: ((reason: string) => void) | null = null;

  #loopPromise: Promise<PoolState> | null = null;

  // 本轮已经收口过的 id：失败的行会被标记回 pending（next_try_at=now），不排除掉的话
  // 同一轮会立刻又领到它、原地重试到 dead——设计要的是「这一轮只试一次，下一轮再来」。
  #triedThisRound = new Set<number>();

  constructor({
    channel,
    claim,
    prepare,
    commit,
    release,
    onLanded = null,
    concurrency = 1,
    writeLock = null,
    reap = null,
    remaining = null,
    log = () => {},
    idleMs = POOL_IDLE_MS,
    stopWhenEmpty = true,
  }: ChannelPoolOptions = {}) {
    this.#channel = channel;
    this.#claim = claim;
    this.#prepare = prepare;
    this.#commit = commit;
    this.#release = release;
    this.#onLanded = onLanded;
    this.#concurrency = concurrency;
    this.#writeLock = writeLock;
    this.#reap = reap;
    this.#remaining = remaining;
    this.#log = log;
    this.#idleMs = idleMs;
    this.#stopWhenEmpty = stopWhenEmpty;
    this.#state.channel = channel;
  }

  get channel(): string | undefined {
    return this.#channel;
  }

  #concOf(): number {
    const raw = typeof this.#concurrency === 'function' ? this.#concurrency() : this.#concurrency;
    return Math.max(1, Math.min(POOL_CONCURRENCY_CAP, Math.floor(Number(raw) || 1)));
  }

  #wake(): void {
    if (!this.#waiter) return;
    const fn = this.#waiter;
    this.#waiter = null;
    fn('signal');
  }

  #idle(): Promise<string> {
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        this.#waiter = null;
        resolve('timeout');
      }, Math.max(50, Number(this.#idleMs) || POOL_IDLE_MS));
      if (typeof timer.unref === 'function') timer.unref();
      this.#waiter = () => {
        clearTimeout(timer);
        resolve('signal');
      };
    });
  }

  /** 批内：N 路 prepare（只读/问模型）→ 按领取顺序串行 commit（写库过闸）。 */
  async #runBatch(rows: PoolRow[], conc: number): Promise<void> {
    const state = this.#state;
    const boxes = new Array<PrepareBox | undefined>(rows.length);
    const startedAt = rows.map(() => Date.now());
    let cursor = 0;
    const worker = async () => {
      for (;;) {
        const i = cursor;
        cursor += 1;
        if (i >= rows.length) return;
        startedAt[i] = Date.now();
        try {
          boxes[i] = { ok: true, box: await this.#prepare!(rows[i], i) };
        } catch (error) {
          boxes[i] = { ok: false, error };
        }
      }
    };
    await Promise.all(Array.from({ length: Math.min(conc, rows.length) }, () => worker()));

    for (let i = 0; i < rows.length; i += 1) {
      const got = boxes[i] as PrepareBox;
      const ms = Math.max(0, Date.now() - (startedAt[i] || Date.now()));
      let out: PoolOutcome;
      const apply = () => (got.ok ? this.#commit!(rows[i], got.box, i) : this.#release!(rows[i], got.error, i));
      try {
        out = this.#writeLock ? await this.#writeLock(apply) : await apply();
        if (got.ok) state.landed += 1;
        else state.failed += 1;
        if (out && out.dead) state.dead += 1;
      } catch (error) {
        state.failed += 1;
        // commit 抛错也算这条失败：交给 release 记一次 attempts，池不许因此停掉
        try {
          const rel = () => this.#release!(rows[i], error, i);
          if (this.#writeLock) await this.#writeLock(rel);
          else await rel();
        } catch (releaseError) {
          state.errors += 1;
          this.#log(`[pool:${this.#channel}] release 也失败了 id=${rows[i]?.id ?? '?'}：${String((releaseError && (releaseError as { message?: unknown }).message) || releaseError)}`);
        }
      }
      state.running = Math.max(0, state.running - 1);
      if (this.#onLanded) {
        // 第三个参数是「这条活的结果信封」：ok/phase/error + commit 的业务返回值放在 value 里，
        // 跟 runListPool 的 results[i] 同一个形状，调用方不用猜 commit 返回了什么。
        const box = got.ok
          ? { ok: true, value: out }
          : { ok: false, phase: 'prepare', error: got.error, value: out };
        try {
          this.#onLanded(rows[i], got.ok ? got.box : null, box, i, ms);
        } catch {
          /* 日志回调不许弄断落地 */
        }
      }
    }
  }

  async #loop(): Promise<PoolState> {
    const state = this.#state;
    state.startedAt = Date.now();
    while (!state.stopped) {
      if (this.#reap) {
        try { this.#reap(); } catch (error) {
          state.errors += 1;
          this.#log(`[pool:${this.#channel}] 租约回收失败：${String((error && (error as { message?: unknown }).message) || error)}`);
        }
      }
      const conc = this.#concOf();
      let rows: PoolRow[] = [];
      try {
        rows = this.#claim!(conc, [...this.#triedThisRound]) || [];
      } catch (error) {
        state.errors += 1;
        rows = [];
        this.#log(`[pool:${this.#channel}] 领活失败：${String((error && (error as { message?: unknown }).message) || error)}`);
      }
      if (!rows.length) {
        state.idleRounds += 1;
        if (this.#stopWhenEmpty) {
          state.drained = true;
          break;
        }
        await this.#idle();
        continue;
      }
      state.batches += 1;
      state.claimed += rows.length;
      for (const row of rows) if (row && row.id !== undefined) this.#triedThisRound.add(Number(row.id));
      state.running += rows.length;
      if (state.running > state.peakRunning) state.peakRunning = state.running;
      const left = typeof this.#remaining === 'function' ? Number(this.#remaining()) || 0 : 0;
      this.#log(`[pool:${this.#channel}] 领 ${rows.length} 条 剩 ${left} 条 并发 ${Math.min(conc, rows.length)}`);
      await this.#runBatch(rows, conc);
    }
    state.ms = Date.now() - state.startedAt;
    return state;
  }

  start(): Promise<PoolState> {
    if (this.#loopPromise) return this.#loopPromise;
    this.#loopPromise = this.#loop();
    return this.#loopPromise;
  }

  /** 等池子把队列吃空（stopWhenEmpty=true）或 stop() 掉。 */
  drain(): Promise<PoolState> {
    return this.start();
  }

  stop(): Promise<PoolState> | null {
    this.#state.stopped = true;
    this.#wake();
    return this.#loopPromise;
  }

  notify(): void {
    this.#wake();
  }

  stats(): PoolState & { writeLock: { active: number; peak: number } | null } {
    return { ...this.#state, writeLock: this.#writeLock ? this.#writeLock.stats!() : null };
  }
}

/** 旧工厂名保持不变；参数见 ChannelPool 构造器。 */
export function createChannelPool(opts: ChannelPoolOptions = {}): ChannelPool {
  return new ChannelPool(opts);
}

/**
 * 列表型并发池（一次给全量 items）：N 路 prepare（只读/问模型）→ 按原顺序串行 commit。
 * 分批领活的队列池（createChannelPool）用的也是这套节奏，只是活是边领边来的。
 * 一个 prepare 完没落地就不领下一个，所以在飞的结果最多 concurrency 个。
 * prepare/commit 抛错只记在它自己身上，不拖累别人。
 *
 * 返回 results[i]：{ ok:true, value:<commit 返回值> } 或 { ok:false, phase, error }。
 * concurrency=1 时就是原来的「prepare→commit→下一个」串行节奏。
 */
export async function runListPool<T>({
  items = [], concurrency = 3, prepare, commit, heartbeat = () => {}, onLanded = null, writeLock = null,
}: RunListPoolOptions<T> = {}): Promise<Array<PoolResult>> {
  const list = Array.isArray(items) ? items : [];
  const conc = Math.min(Math.max(Number(concurrency) || 1, 1), POOL_CONCURRENCY_CAP);
  const results = new Array<PoolResult>(list.length);
  const startedAt = new Array<number>(list.length).fill(0);
  if (!list.length) return results;
  const ready = list.map((): { promise: Promise<PoolResult>; resolve: (value: PoolResult) => void } => {
    let resolve!: (value: PoolResult) => void;
    const promise = new Promise<PoolResult>((r) => { resolve = r; });
    return { promise, resolve };
  });
  const landed = list.map((): { promise: Promise<void>; resolve: () => void } => {
    let resolve!: () => void;
    const promise = new Promise<void>((r) => { resolve = r; });
    return { promise, resolve };
  });
  let cursor = 0;
  const worker = async (): Promise<void> => {
    for (;;) {
      const i = cursor;
      cursor += 1;
      if (i >= list.length) return;
      startedAt[i] = Date.now();
      heartbeat(); // 问模型前续租
      let box: PoolResult;
      try {
        box = { ok: true, value: await prepare!(list[i], i) };
      } catch (error) {
        box = { ok: false, phase: 'prepare', error };
      }
      ready[i].resolve(box);
      await landed[i].promise; // 等自己这条落地完再领下一个，未落地的结果最多 conc 个
    }
  };
  const workers = Promise.all(Array.from({ length: Math.min(conc, list.length) }, () => worker()));
  for (let i = 0; i < list.length; i += 1) {
    const box = await ready[i].promise;
    heartbeat(); // 落地前续租
    if (box.ok) {
      try {
        const apply = () => commit!(box.value, i);
        results[i] = { ok: true, value: writeLock ? await writeLock(apply) : await apply() };
      } catch (error) {
        results[i] = { ok: false, phase: 'commit', error };
      }
    } else {
      results[i] = box;
    }
    if (onLanded) {
      try {
        onLanded({ index: i, item: list[i], result: results[i], ms: Math.max(0, Date.now() - (startedAt[i] || Date.now())) });
      } catch {
        /* 日志回调抛错只影响日志 */
      }
    }
    landed[i].resolve();
  }
  await workers;
  return results;
}
