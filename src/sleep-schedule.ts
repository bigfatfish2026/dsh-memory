/**
 * 「每天几点整理」的时钟源：dsh-timer 的 `memory.sleep.tick` 优先，拿不到就自己数分钟。
 *
 * 背景（第五刀批 4a）：sleep.js 原来自己 setInterval 每 60 秒看一遍时段。现在改成
 * 每个时段注册一条 cron 规则（`30 4 * * *` 这种），到点由 dsh-timer 发 volatile tick，
 * 本模块订阅它。原来的 60 秒定时器不能一刀删：注册没成功、timer 没挂、tick 断流，
 * 都得退回它，否则整理会静默地再也不跑。
 *
 * 四条降级路径，都不能让整理停：
 *
 * - `schedule` 服务不在（dsh-timer 没挂、或还没 apply）：保留自己的定时器，每次
 *   轮询（`onPoll`）再探一次——它中途出现就切过去。
 * - 注册被拒（EV-006：topic 没在 events 注册表里；BAD_RULE / DUP_RULE_ID 同理）：
 *   撤掉这一轮已注册的规则，保留自己的定时器，冷却一段时间再试，别每分钟刷一行 warn。
 * - 服务中途消失（events 或 schedule 被撤）：当场退回自己的定时器并 warn。
 * - tick 断流：watchdog 按「3 个最短周期没收到 tick」判死，退订 + 注销规则 + 恢复
 *   自己的定时器并 warn。
 *
 * 两条硬规矩：
 *
 * - 多时段 = 多条规则，全部注册成功才算成功；任何一条失败就整体回落到定时器，
 *   不留半套规则（半套 = 一个时段靠 tick、一个时段靠定时器，双驱动最容易一天整理两遍）。
 * - tick 只是「叫醒」，到点判定、当天已跑判定、去重键全部还在 sleep.js 里，
 *   本模块不碰业务逻辑。同一 periodKey 重复 tick 由 `state.slots` 的 slotKey 判重挡住。
 */


/** 一个整理时段。 */
export interface SleepSlotDef {
  hour: number;
  minute: number;
}

/** 定时器句柄（Node 的 Timeout，测试里是假对象）。 */
type ClockHandle = ReturnType<typeof setInterval>;

/** 时钟面：watchdog / 轮询用，测试可注入假时钟。 */
export interface SleepClock {
  now: () => Date;
  setInterval: (fn: () => void, ms: number) => ClockHandle;
  clearInterval: (handle: ClockHandle) => void;
}

/** dsh-timer 的 schedule 服务最小面。 */
export interface ScheduleServiceLike {
  register: (rule: { id: string; cron: string; topic: string; catchUp: string }) => unknown;
  unregister: (id: string) => unknown;
  list?: () => unknown;
  listRules?: () => unknown;
}

/** 规则表里的一条。 */
interface ScheduleRuleLike {
  id?: unknown;
  cron?: unknown;
  topic?: unknown;
  catchUp?: unknown;
  [key: string]: unknown;
}

/** dsh-events 的服务最小面（只认本文件用到的）。 */
export interface EventsServiceLike {
  defineEvent: (def: typeof SLEEP_EVENT_DEF) => unknown;
  registry?: { get?: (name: string) => unknown };
  subscribe: (
    topic: string,
    listener: (event: { payload?: unknown } | null | undefined) => unknown,
    opts: { subscriberId: string; replay: { mode: string } },
  ) => { stop?: () => void } | null | undefined;
}

/** createSleepDriver 的入参。 */
export interface SleepDriverOptions {
  slots: () => unknown;
  events: () => EventsServiceLike | undefined;
  schedule: () => ScheduleServiceLike | undefined;
  run: (meta: { periodKey: string; slot: SleepSlotDef | undefined; source: 'interval' | 'tick' }) => unknown;
  checkMs?: number;
  clock?: SleepClock;
  info?: (message: string) => void;
  warn?: (message: string) => void;
}

/** 驱动对外面。 */
export interface SleepDriverApi {
  reconcile: () => void;
  start: () => void;
  onPoll: () => void;
  tick: (payload: unknown) => void;
  noteServiceEvent: (name: string, value: unknown) => boolean;
  dispose: () => void;
  mode: () => string;
  status: () => Record<string, unknown>;
}

/** dsh-timer 发 tick 用的事件名，也是本文件订阅/注册的 topic。 */
export const SLEEP_TOPIC = 'memory.sleep.tick';
/** 规则 id 前缀：`memory.sleep.0500` / `memory.sleep.1230`。 */
export const SLEEP_RULE_PREFIX = 'memory.sleep.';
/**
 * 规则错过时点后怎么办。dsh-timer 只认 `'once'` / `'skip'` 两个值
 * （plugins/dsh-timer/lib/schedule.js:238 的 BAD_RULE 校验，没有第三个字面量）。
 *
 * - `'skip'`：`now - dueAt > graceMs`（3 秒）就永远跳过且不补。2026-11-13 12:30
 *   那轮被 HMR 掐断后，当天再也没补，就是这条。
 * - `'once'`：只对 `'skip'` 做超时过滤（schedule.js:620），所以同一周期（日粒度
 *   即当天）内过点启动会补发一次 tick，跨天不会补昨天的。
 *
 * 补发只是「叫醒」：要不要真跑仍由 sleep.state.json 的 slots 当天判重
 * （sleep.js 的 pickDueSleepSlot），同一档最多跑一次，不存在补发跑两遍。
 */
export const CATCH_UP = 'once';
/** 订阅者 id：dsh-events 的投递游标按它记，重复订阅也只会有一份。 */
export const SLEEP_SUBSCRIBER_ID = 'dsh-memory.sleep';

const MINUTE_MS = 60 * 1000;
const DAY_MS = 24 * 60 * MINUTE_MS;
/** tick 断流判定：这么多个「最短时段间隔」没来就认死。 */
const STALL_FACTOR = 3;
/** watchdog 睁眼间隔：断流阈值再长，也得十分钟看一眼，别傻等到第二天。 */
const QUICK_WATCHDOG_MS = 10 * MINUTE_MS;
/** 注册/订阅失败后的冷却：半小时，别每分钟撞一次同一堵墙。 */
const RETRY_MS = 30 * MINUTE_MS;

/**
 * 事件类型注册项。volatile：tick 是「到点了」的信号，不是业务数据，不该在
 * 事件表里留行，也不需要重放。payload 只放 name/firedAt/periodKey——dsh-timer
 * 自己拼的就是这三样，多一个字段都没有。
 */
export const SLEEP_EVENT_DEF = Object.freeze({
  name: SLEEP_TOPIC,
  version: 1,
  durability: 'volatile',
  payloadSchema: (payload: unknown) => {
    const tick = payload as { name?: unknown; firedAt?: unknown; periodKey?: unknown };
    return (
      typeof tick?.name === 'string' &&
      typeof tick?.firedAt === 'string' &&
      typeof tick?.periodKey === 'string'
    );
  },
});

/** 真时钟：watchdog/轮询用；interval 的句柄 unref 掉，别拽着进程不放。 */
export const realClock = {
  now: (): Date => new Date(),
  setInterval(fn: () => void, ms: number): ClockHandle {
    const handle = setInterval(fn, ms);
    if (typeof handle?.unref === 'function') handle.unref();
    return handle;
  },
  clearInterval(handle: ClockHandle): void {
    clearInterval(handle);
  },
};

function pad2(n: number): string {
  return String(n).padStart(2, '0');
}

/**
 * 「没有 schedule」分三种原因说，别都写成「dsh-timer 未挂」把排障带错方向：
 * 尚未就绪 / 已就绪但本插件没拿到句柄 / 规则被撤。文案跟其它插件逐字一致。
 */
const SCHEDULE_ABSENT = Object.freeze({
  notReady: 'schedule 服务尚未就绪（dsh-timer 未挂或还没 apply）',
  noHandle: 'schedule 服务已就绪过，但本插件没拿到句柄（inject/ctx.get 未生效，或服务正在重载）',
  ruleRevoked: 'schedule 规则被撤（曾注册成功，现不在规则表里）',
});

function scheduleAbsentReason(seenInstance: unknown): string {
  return seenInstance ? SCHEDULE_ABSENT.noHandle : SCHEDULE_ABSENT.notReady;
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function codeOf(error: unknown): string {
  const code = (error as { code?: unknown } | null)?.code;
  return typeof code === 'string' && code ? code : 'ERR';
}

/** 时段归一：小时/分钟都收进合法范围，同 HH:MM 去重，按时间排序。 */
export function normalizeSlots(list: unknown): SleepSlotDef[] {
  const out: SleepSlotDef[] = [];
  const seen = new Set<string>();
  for (const s of Array.isArray(list) ? list : []) {
    if (!s || !Number.isFinite(Number(s.hour))) continue;
    const hour = ((Math.trunc(Number(s.hour)) % 24) + 24) % 24;
    const minute = ((Math.trunc(Number(s.minute) || 0) % 60) + 60) % 60;
    const id = slotIdOf({ hour, minute });
    if (seen.has(id)) continue;
    seen.add(id);
    out.push({ hour, minute });
  }
  out.sort((a, b) => a.hour * 60 + a.minute - (b.hour * 60 + b.minute));
  return out;
}

/**
 * 时段 → 规则 id：`memory.sleep.1230`。
 *
 * 为什么每个时段一条规则，而不是「一条 cron 里小时列表」：一条 cron 只能有
 * 一组分钟集合。`0 5,12 * * *` 能表达 05:00 和 12:00，但 05:00 + 12:30 这种
 * 分钟不同的配法表达不了（分钟列表是笛卡尔积，会多出 05:30/12:00 两拍）。
 * 现在 cfg.schedule 是 {hour,minute} 数组，分钟随时可能不同（2026-10-30 从
 * 11:00 挪到 13:00，2026-11-11 又挪到 12:30），一条规则迟早不够用；多一条规则的代价只是 dsh-timer
 * 里多一行，换来映射无损。
 */
export function slotIdOf(slot: SleepSlotDef): string {
  return `${SLEEP_RULE_PREFIX}${pad2(slot.hour)}${pad2(slot.minute)}`;
}

/** 时段 → 5 段 cron（本地时区）：每天 hour:minute 一次。 */
export function cronOf(slot: SleepSlotDef): string {
  return `${slot.minute} ${slot.hour} * * *`;
}

/** 从 periodKey（`memory.sleep.0430.daily.2026-11-09`）反解出是哪个时段。 */
export function slotFromPeriodKey(periodKey: unknown, slots: unknown): SleepSlotDef | undefined {
  const key = typeof periodKey === 'string' ? periodKey : '';
  if (!key) return undefined;
  for (const slot of Array.isArray(slots) ? slots : []) {
    if (key.startsWith(`${slotIdOf(slot)}.`)) return slot;
  }
  return undefined;
}

/** 最短时段间隔（环形）：多时段时是相邻两拍的最小间隔，单时段就是一天。 */
export function minGapMsOf(slots: unknown): number {
  const list = normalizeSlots(slots);
  if (list.length <= 1) return DAY_MS;
  let min = DAY_MS;
  for (let i = 0; i < list.length; i += 1) {
    const a = list[i].hour * 60 + list[i].minute;
    const b = list[(i + 1) % list.length].hour * 60 + list[(i + 1) % list.length].minute;
    const gap = ((b - a + 1440) % 1440 || 1440) * MINUTE_MS;
    if (gap < min) min = gap;
  }
  return min;
}

/**
 * 惰性探针：`ctx.get('schedule')`。拿不到（没装 dsh-timer、还没 apply、宿主不认）
 * 一律返回 undefined，绝不在这里 throw。
 */
export function probeService(ctx: { get?: (name: string) => unknown } | null | undefined, name: string): unknown {
  const getter = ctx?.get;
  if (typeof getter !== 'function') return undefined;
  try {
    const found = getter.call(ctx, name);
    return found === undefined || found === null ? undefined : found;
  } catch {
    return undefined;
  }
}

/**
 * @param {{
 *   slots:  => Array<{hour:number, minute:number}>,
 *   events:  => object|undefined,
 *   schedule:  => object|undefined,
 *   run: (meta: { periodKey: string, slot: {hour:number,minute:number}|undefined, source: 'interval'|'tick' }) => unknown,
 *   checkMs?: number,
 *   clock?: typeof realClock,
 *   info?: (message: string) => void,
 *   warn?: (message: string) => void,
 * }} options
 */
export function createSleepDriver(options: SleepDriverOptions): SleepDriverApi {
  const { slots: slotsProvider, events, schedule, run } = options;
  const checkMs = Number(options.checkMs) > 0 ? Number(options.checkMs) : 60 * 1000;
  const clock = options.clock ?? realClock;
  const info = options.info ?? (() => {});
  const warn = options.warn ?? (() => {});

  let mode: string = 'interval';
  let subscription: { stop?: () => void } | undefined;
  /** 已注册的规则 id：`registeredIds.size === slots.length` 才算整套都在。 */
  const registeredIds = new Set<string>();
  let lastTickAt = 0;
  let retryAt = 0;
  /** 上次看到的 schedule 服务实例：换实例等于规则全丢，得在新实例上重挂。 */
  let scheduleSeen: ScheduleServiceLike | undefined;
  /** 订阅挂在哪份 events 实例上：换实例就得退订重订，不然 tick 永远收不到。 */
  let subscribedOn: EventsServiceLike | undefined;
  /** 「schedule 不在」只播报一次，服务出现过再消失时重新播报。 */
  let absentAnnounced = false;
  /** 「events 不在」同理。 */
  let eventsAnnounced = false;
  let watchdog: ClockHandle | undefined;
  let poll: ClockHandle | undefined;
  let stopped = false;

  function now(): number {
    return clock.now().getTime();
  }

  function currentSlots(): SleepSlotDef[] {
    return normalizeSlots(slotsProvider());
  }

  function dropSubscription(): boolean {
    const sub = subscription;
    if (!sub) return false;
    subscription = undefined;
    subscribedOn = undefined;
    try {
      sub.stop?.();
    } catch (error) {
      warn(`[memory] 退订 ${SLEEP_TOPIC} 失败：${messageOf(error)}`);
    }
    return true;
  }

  function dropRules(): void {
    const service = scheduleSeen ?? schedule();
    const ids = [...registeredIds];
    registeredIds.clear();
    if (!service) return;
    for (const id of ids) {
      try {
        service.unregister(id);
      } catch (error) {
        warn(`[memory] 注销规则 ${id} 失败：${messageOf(error)}`);
      }
    }
  }

  function stopWatchdog(): void {
    if (watchdog === undefined) return;
    clock.clearInterval(watchdog);
    watchdog = undefined;
  }

  function stopPoll(): void {
    if (poll === undefined) return;
    clock.clearInterval(poll);
    poll = undefined;
  }

  function startPoll(): void {
    if (poll !== undefined || stopped) return;
    poll = clock.setInterval(() => {
      if (stopped) return;
      // 每一拍先探源：dsh-memory 可能比 dsh-timer 先 apply，那会儿 schedule 还没提供，
      // 手上只有一个定时器。服务出现、冷却到期、规则被撤，都在这一拍补上。
      reconcile();
      if (mode === 'tick' || stopped) return;
      fire('interval', '');
    }, checkMs);
  }

  function startWatchdog(): void {
    stopWatchdog();
    const stallMs = minGapMsOf(currentSlots()) * STALL_FACTOR;
    watchdog = clock.setInterval(() => {
      if (stopped || mode !== 'tick') return;
      const idle = now() - lastTickAt;
      if (idle < stallMs) return;
      // 断流不是「少跑一拍」而是「再也不会醒」：必须当场把时钟源换回来。
      fallback(`[memory] ${SLEEP_TOPIC} 断流：${Math.round(idle / MINUTE_MS)} 分钟没收到 tick`);
    }, QUICK_WATCHDOG_MS);
  }

  function fire(source: 'interval' | 'tick', periodKey: string): void {
    const slots = currentSlots();
    const slot = slotFromPeriodKey(periodKey, slots);
    try {
      run({ periodKey, slot, source });
    } catch (error) {
      warn(`[memory] 处理 ${SLEEP_TOPIC}（${source}）抛错：${messageOf(error)}`);
    }
  }

  function handleTick(payload: unknown): void {
    if (stopped) return;
    // 先记时间：这一拍就算业务上被「今天已跑」挡掉，也算流还活着。
    lastTickAt = now();
    const periodKey = typeof (payload as { periodKey?: unknown } | null)?.periodKey === 'string' ? (payload as { periodKey: string }).periodKey : '';
    fire('tick', periodKey);
  }

  function switchToTick(): void {
    mode = 'tick';
    stopPoll();
    lastTickAt = now();
    startWatchdog();
    info(
      `[memory] 整理时段改由 ${SLEEP_TOPIC} 驱动（规则 ${[...registeredIds].join('、')}，catchUp=${CATCH_UP}），自己的 ${Math.round(checkMs / 1000)} 秒定时器已停`,
    );
  }

  /** 降级：退回自己的定时器，并说明原因。只在 tick 模式下有意义。 */
  function fallback(reason: string): void {
    if (stopped) return;
    stopWatchdog();
    dropSubscription();
    dropRules();
    const wasTick = mode === 'tick';
    mode = 'interval';
    retryAt = now() + RETRY_MS;
    startPoll();
    if (wasTick) warn(`${reason}，整理恢复自己的 ${Math.round(checkMs / 1000)} 秒定时器`);
  }

  function ensureSubscription(service: EventsServiceLike): boolean {
    if (subscription && subscribedOn === service) return true;
    if (subscription) {
      // events 换了实例：旧订阅的游标跟着旧实例没了，重订一遍。
      dropSubscription();
      info('[memory] events 服务换了实例，重新订阅 tick');
    }
    try {
      service.defineEvent(SLEEP_EVENT_DEF);
    } catch (error) {
      // 重复注册（热重载、上一份 fiber 已注册过）不算失败：注册表里查得到就行。
      if (service.registry?.get?.(SLEEP_TOPIC) === undefined) {
        warn(
          `[memory] 注册事件类型 ${SLEEP_TOPIC} 失败（${codeOf(error)}）：${messageOf(error)}，整理仍由自己的定时器驱动`,
        );
        return false;
      }
    }
    try {
      const sub = service.subscribe(
        SLEEP_TOPIC,
        (event) => {
          handleTick(event?.payload);
          return undefined;
        },
        { subscriberId: SLEEP_SUBSCRIBER_ID, replay: { mode: 'none' } },
      );
      if (!sub || typeof sub.stop !== 'function') {
        warn(`[memory] 订阅 ${SLEEP_TOPIC} 没拿到 stop()，整理仍由自己的定时器驱动`);
        return false;
      }
      subscription = sub;
      subscribedOn = service;
      info(`[memory] 已订阅 ${SLEEP_TOPIC}（volatile，不落事件表）`);
      return true;
    } catch (error) {
      warn(`[memory] 订阅 ${SLEEP_TOPIC} 失败：${messageOf(error)}，整理仍由自己的定时器驱动`);
      return false;
    }
  }

  /** schedule.list()/listRules() 哪个能用用哪个；没有列表接口就当什么都查不到。 */
  function existingRules(service: ScheduleServiceLike): Map<string, ScheduleRuleLike> | undefined {
    const read = typeof service.list === 'function' ? service.list : service.listRules;
    if (typeof read !== 'function') return undefined;
    let rules: unknown;
    try {
      rules = read.call(service);
    } catch (error) {
      warn(`[memory] 列规则失败（${codeOf(error)}）：${messageOf(error)}`);
      return undefined;
    }
    if (!Array.isArray(rules)) return undefined;
    const map = new Map<string, ScheduleRuleLike>();
    for (const rule of rules) {
      if (rule && typeof rule.id === 'string') map.set(rule.id, rule);
    }
    return map;
  }

  /**
   * 注册全部时段。有一条失败就撤干净返回 false——半套规则比没有更坏。
   *
   * 同 id 规则存在时先 unregister 再 register：dsh-timer 的 id 全局唯一，
   * 重注册不先撤会撞 DUP_RULE_ID。spec 完全一致（cron/topic/catchUp 都对）时
   * 直接复用，既不撞 DUP 也不会把 timer 记的 periodKey 清掉（清了可能重发一拍）。
   */
  function registerAll(service: ScheduleServiceLike, slots: SleepSlotDef[]): boolean {
    const existing = existingRules(service);
    const wanted = slots.map((slot) => ({ id: slotIdOf(slot), cron: cronOf(slot), slot }));
    const wantedIds = new Set(wanted.map((w) => w.id));

    // 时段配置被改掉留下的旧规则：撤掉，别让它继续发 tick
    for (const id of [...registeredIds]) {
      if (wantedIds.has(id)) continue;
      try {
        service.unregister(id);
      } catch (error) {
        warn(`[memory] 注销过期规则 ${id} 失败：${messageOf(error)}`);
      }
      registeredIds.delete(id);
    }

    for (const want of wanted) {
      const have = existing?.get(want.id);
      if (have && have.cron === want.cron && have.topic === SLEEP_TOPIC && have.catchUp === CATCH_UP) {
        registeredIds.add(want.id);
        continue;
      }
      try {
        if (have || registeredIds.has(want.id)) service.unregister(want.id);
        service.register({ id: want.id, cron: want.cron, topic: SLEEP_TOPIC, catchUp: CATCH_UP });
        registeredIds.add(want.id);
      } catch (error) {
        // 整体回落：这一轮已经注册上的全撤掉，免得 tick 和定时器两路同时叫醒。
        for (const id of [...registeredIds]) {
          try {
            service.unregister(id);
          } catch (dropError) {
            warn(`[memory] 回滚注销规则 ${id} 失败：${messageOf(dropError)}`);
          }
        }
        registeredIds.clear();
        retryAt = now() + RETRY_MS;
        warn(
          `[memory] 注册规则 ${want.id} 失败（${codeOf(error)}）：${messageOf(error)}，整理仍由自己的定时器驱动，${Math.round(RETRY_MS / MINUTE_MS)} 分钟后再试`,
        );
        return false;
      }
    }
    return registeredIds.size === wanted.length;
  }

  function reconcile(): void {
    if (stopped) return;

    const eventsService = events();
    if (!eventsService) {
      if (mode === 'tick') {
        fallback('[memory] events 服务不在了');
      } else if (!eventsAnnounced) {
        eventsAnnounced = true;
        info('[memory] 没有 events 服务，整理由自己的定时器驱动（tick 订阅等它出现再挂）');
      }
      return;
    }
    eventsAnnounced = false;

    if (!ensureSubscription(eventsService)) {
      if (mode === 'tick') fallback('[memory] tick 订阅失效');
      return;
    }

    const service = schedule();
    if (!service) {
      if (mode === 'tick') {
        fallback('[memory] schedule 服务不在了');
      } else if (!absentAnnounced) {
        absentAnnounced = true;
        warn(`[memory] ${scheduleAbsentReason(scheduleSeen !== undefined)}，整理由自己的定时器驱动，等服务出现再切换`);
      }
      return;
    }

    if (service !== scheduleSeen) {
      // 新实例（装上、重启、热重载）：旧规则跟着旧实例没了，得重挂。
      if (scheduleSeen !== undefined) info('[memory] schedule 服务换了实例，规则在新实例上重挂');
      scheduleSeen = service;
      registeredIds.clear();
      retryAt = 0;
      absentAnnounced = false;
    }

    const slots = currentSlots();
    if (mode === 'tick' && registeredIds.size === slots.length) return;
    if (now() < retryAt) return;

    if (!registerAll(service, slots)) return;
    if (mode === 'tick') {
      // 已经在 tick 模式但规则少了（被谁 unregister 掉、时段改了）：重挂不换源，
      // 只把存活时间重新起算，别让 watchdog 把刚补好的流当断流。
      lastTickAt = now();
      info(`[memory] ${SCHEDULE_ABSENT.ruleRevoked}，重挂缺的规则`);
      info(`[memory] 整理规则已补齐：${[...registeredIds].join('、')}`);
      return;
    }
    switchToTick();
  }

  return {
    reconcile,
    start() {
      if (stopped) return;
      startPoll();
      reconcile();
      // 启动补判一次：进程在时段之后才起来时，dsh-timer 的补发 tick 可能比我们晚到
      // （甚至因为 events 还没就绪根本不发），而自己的 60 秒定时器第一拍也没到，
      // 随后 reconcile 切 tick 模式又把它 stopPoll 掉——旧逻辑就这么把过点的档整天丢了
      // （2026-11-13 12:30 那轮）。这里立即判一次，过点且当天没跑的档当场补上。
      // 幂等：sleep.js 的 runDue 同步推进 lastRunAt（10 分钟 minGap），同一档靠
      // slots 判重，重复调用不会再跑一遍。
      fire('interval', '');
    },
    /** 定时轮询的每一拍（startPoll 内部用；测试也可以直接调）。 */
    onPoll() {
      if (stopped) return;
      reconcile();
      if (mode === 'tick' || stopped) return;
      fire('interval', '');
    },
    /** dsh-events 的 tick 入口（测试直接喂 payload 用）。 */
    tick(payload) {
      handleTick(payload);
    },
    /**
     * `internal/service` 事件入口：schedule 就绪/销毁。
     *
     * 就绪（value 非空）返回 true，调用方接着 reconcile；销毁（value 为空）当场
     * 清缓存并回落定时器——服务已经没了，tick 不会再来，等 watchdog 判死太慢。
     *
     * @param {string} name
     * @param {unknown} value
     * @returns {boolean} 是否该接着 reconcile
     */
    noteServiceEvent(name, value) {
      if (name !== 'schedule') return false;
      if (value) return true;
      // 不先清 registeredIds：fallback 要靠它把旧实例上的规则撤干净（万一实例还在，
      // 残留规则会继续发 tick，跟回落后的定时器变成两路叫醒）。
      const wasSeen = scheduleSeen !== undefined;
      absentAnnounced = false;
      if (wasSeen) fallback('[memory] schedule 服务被销毁');
      scheduleSeen = undefined;
      return false;
    },
    dispose() {
      if (stopped) return;
      stopped = true;
      stopWatchdog();
      stopPoll();
      dropSubscription();
      dropRules();
      mode = 'interval';
    },
    mode: () => mode,
    status: () => ({
      mode,
      subscribed: subscription !== undefined,
      registered: [...registeredIds],
      lastTickAt,
      checkMs,
      stallMs: minGapMsOf(currentSlots()) * STALL_FACTOR,
    }),
  };
}

/**
 * 挂 `internal/service` 监听：schedule 就绪就重挂规则，销毁就回落。
 *
 * ⚠ 回调整只包 try/catch，这条是硬要求：dsh-events 的 emit 没有兜错，监听器抛错
 * 会冒到 timer 的 provide/effect，可能把 timer 的 apply 弄挂。catch 里只 warn，绝不 rethrow。
 *
 * @param {{ on: (event: string, listener: (...args: any[]) => unknown) =>  => void }} ctx
 * @param {{ noteServiceEvent: (name: string, value: unknown) => boolean, reconcile:  => void }} driver
 * @param {{ warn?: (message: string) => void }} [options]
 * @returns { => void} 摘监听
 */
export function attachServiceWatch(
  ctx: { on?: (event: string, listener: (name: string, value: unknown) => unknown) => (() => void) | undefined } | null | undefined,
  driver: { noteServiceEvent: (name: string, value: unknown) => boolean; reconcile: () => void },
  options: { warn?: (message: string) => void } = {},
): () => void {
  const warn = options.warn ?? (() => {});
  if (typeof ctx?.on !== 'function') return () => {};
  const off = ctx.on('internal/service', (name: string, value: unknown) => {
    try {
      if (name !== 'schedule') return;
      if (!driver.noteServiceEvent(name, value)) return;
      driver.reconcile();
    } catch (error) {
      warn(`[memory] internal/service 监听处理失败（name=${String(name)}）：${messageOf(error)}`);
    }
  });
  return () => {
    try {
      off?.();
    } catch (error) {
      warn(`[memory] 摘 internal/service 监听失败：${messageOf(error)}`);
    }
  };
}
