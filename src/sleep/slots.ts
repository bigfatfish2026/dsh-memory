/**
 * 档位/心跳/owner 判定：定时器和 dsh-timer 的 tick 共用这一份。
 */
import fs from 'node:fs';

/** 一个整理时段。 */
export interface SleepSlot {
  hour: number;
  minute: number;
}

/** 本文件只读 schedule / hour / minute。 */
interface SlotsCfg {
  schedule?: unknown;
  hour?: unknown;
  minute?: unknown;
  [key: string]: unknown;
}

/** 已跑标记：slotKey → 任意值（有值即算跑过）。 */
type SlotDone = Record<string, unknown>;

/** 日志用本地时间带时区，例如 2026-11-05T12:30:00+08:00（跟设计稿 §7 样板一致）。 */
export function isoLocal(d: Date = new Date()): string {
  const pad = (n: number) => String(n).padStart(2, '0');
  const tz = -d.getTimezoneOffset();
  const sign = tz >= 0 ? '+' : '-';
  const abs = Math.abs(tz);
  return (
    `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}` +
    `T${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}` +
    `${sign}${pad(Math.floor(abs / 60))}:${pad(abs % 60)}`
  );
}

export function dayKey(d: Date): string {
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
}

/** 一天里的整理时段：配了 schedule 就按它，没配就用 hour/minute 那一个 */
export function slotsOf(cfg: SlotsCfg): SleepSlot[] {
  const list = Array.isArray(cfg.schedule) ? cfg.schedule : [];
  const valid = list.filter((s) => s && Number.isFinite(Number(s.hour)));
  if (valid.length) return valid.map((s) => ({ hour: Number(s.hour) || 0, minute: Number(s.minute) || 0 }));
  return [{ hour: Number(cfg.hour) || 0, minute: Number(cfg.minute) || 0 }];
}

export function slotKeyFor(d: Date, s: SleepSlot): string {
  return `${dayKey(d)} ${String(s.hour).padStart(2, '0')}:${String(s.minute).padStart(2, '0')}`;
}

/**
 * 已废弃的整理时段：2026-11-11 中午档从 13:00 提前到 12:30 时留下。
 *
 * `sleep.state.json` 的 `slots` 只清非当天的 key，所以部署当天里面还躺着
 * `2026-11-11 13:00`。只认精确 key 的话，重启后 12:30 会被当成「今天没跑过」，
 * 15 点一重启就再整理一遍（13:00 那轮刚跑完）。这里把老时刻当已跑认掉。
 *
 * 它只对「不早于新时段」的判定生效（老时刻 13:00 ≥ 12:30 才算覆盖 12:30），
 * 不会挡住比 13:00 更晚的时段。等线上状态里再也查不到当天 13:00 的 key
 * （一个自然日之后）就能删掉这一项。
 */
export const RETIRED_SLEEP_SLOTS: readonly SleepSlot[] = Object.freeze([{ hour: 13, minute: 0 }]);

/**
 * 这一拍该跑哪个时段：返回 slotKey（`2026-11-09 05:00`），没有就 undefined。
 *
 * 判定：到点 → 当天没跑过，第一条命中就返回。2026-11-14 删掉了
 * 「距上一轮满 10 分钟」的 minGap 去抖——那是「时间到了才跑」的闸；重复 tick 靠
 * slots 幂等 + inFlight 挡。抽出来只为一件事：让定时器和 dsh-timer 的 tick
 * 共用同一份判定，也让「重复 tick 幂等 / 当天已跑不再跑」能单测。
 *
 * `retiredSlots` 是时段改点留下的老 key 兼容项，见 `RETIRED_SLEEP_SLOTS`。
 */
export function pickDueSleepSlot({
  slots,
  done,
  now,
  retiredSlots = RETIRED_SLEEP_SLOTS,
}: {
  slots: SleepSlot[];
  done?: SlotDone | null;
  now: Date;
  retiredSlots?: readonly SleepSlot[];
}): string | undefined {
  const nowMin = now.getHours() * 60 + now.getMinutes();
  const doneMap = done || {};
  for (const s of Array.isArray(slots) ? slots : []) {
    const slotMin = s.hour * 60 + s.minute;
    if (nowMin < slotMin) continue;
    const key = slotKeyFor(now, s);
    if (doneMap[key]) continue;
    // 时段改点：老时刻今天已经跑过就等于这一档跑过，别在同一天再跑一遍。
    const retiredDone = (Array.isArray(retiredSlots) ? retiredSlots : []).some(
      (old) => old && old.hour * 60 + old.minute >= slotMin && doneMap[slotKeyFor(now, old)],
    );
    if (retiredDone) continue;
    return key;
  }
  return undefined;
}

/**
 * 心跳：替代原来 RUNNING_TTL_MS=2 小时的固定锁。
 * 一轮在跑时每 60s 写一次 state.heartbeatAt（外加每批开始/结束各写一次）；
 * 连续 3 个间隔（180s）没有心跳才判它死了、允许接管。
 * 好处：热重载后旧实例真死了 3 分钟就能接上，不用干等两小时；旧实例还活着就一直续租。
 */
export const SLEEP_HEARTBEAT_MS = 60 * 1000;
export const SLEEP_HEARTBEAT_MISSES = 3;

/**
 * 「跑这一轮的是谁」：sleep.state.json 的 `owner`。
 *
 * 只用 pid 不够：进程重启后 pid 会被复用，pid 相同 ≠ 同一个实例。bootId 变了说明
 * 重启过；starttime（/proc/self/stat 第 22 字段，开机以来的 jiffies）在 pid 复用后
 * 也一定不同。三个拼起来才认得出「同一个进程实例」。
 *
 * 读不到 /proc（非 Linux、容器裁了 proc）时退回 `pid:启动时刻`，pid 不同照样能分辨，
 * 只是 pid 复用的极小概率认错——比拿不到 owner 退回 180s 判死好。
 */
export function sleepOwnerOf(): string {
  let boot = '';
  let start = '';
  try {
    boot = fs.readFileSync('/proc/sys/kernel/random/boot_id', 'utf8').trim().slice(0, 8);
    const stat = fs.readFileSync('/proc/self/stat', 'utf8');
    // comm 字段（第 2 个）可能带空格和括号，从最后一个 ')' 之后切；after[0] 是第 3 个字段 state，
    // 所以第 22 个字段 starttime 落在 after[19]。
    const after = stat.slice(stat.lastIndexOf(')') + 2).trim().split(/\s+/);
    start = after[19] || '';
  } catch {
    /* 没有 procfs 就走下面的兜底 */
  }
  if (boot && start) return `${boot}:${process.pid}:${start}`;
  return `${process.pid}:${Math.round(Date.now() - process.uptime() * 1000)}`;
}

/** 两个 owner 是不是同一个进程实例；空值一律不算。 */
export function sameSleepOwner(a: unknown, b: unknown): boolean {
  const x = String(a || '');
  return Boolean(x) && x === String(b || '');
}

/**
 * 启动补判用：在 pickDueSleepSlot 前面再加一道「另一轮正在跑」的闸。
 *
 * 为什么要有：`inFlight` 是 setupSleep 闭包里的，HMR 重载就换新实例、闸门归零；
 * 而 sleep-schedule 的 start() 现在启动就 fire 一次。如果重载发生在 05:00 那轮
 * 还没跑完的时候（磁盘上 slots 还没标、lastSleepAt 还是前一天的），新实例会当场
 * 再起一轮。runningAt/heartbeatAt 写在 sleep.state.json 里，跨重载可见。
 *
 * 判活（2026-11-14 起分三条）：
 *
 * - state 里有 `owner` 且就是当前进程 → 这轮是自己的，跳过（正常并发闸）。
 * - state 里有 `owner` 但不是当前进程（pid 不同 / bootId 变了 / 进程已经没了）→
 *   直接接管，不等 TTL。旧逻辑只看到「13 秒前的心跳」就判「另一轮在跑」跳过，
 *   120 秒后心跳过期时又没人再叫，2026-11-14 12:30 那轮就这么整天丢了。
 *   接管仍受 slots 判重：那一档已经落了标记（真跑完）就不补，防重复跑。
 * - 没有 `owner`（老版本写的 state）→ 退回 180s 心跳判活，行为不变：
 *   取 runningAt 和 heartbeatAt 里较晚的那个，距今不到 3 个心跳间隔就算活着，
 *   超过就接管（旧进程被杀、重载后没跑起来都算死，不会把档锁两小时）。
 */
export function dueSleepSlotNow({
  slots,
  done,
  now,
  runningAt = 0,
  heartbeatAt = 0,
  owner = '',
  currentOwner = '',
  heartbeatMs = SLEEP_HEARTBEAT_MS,
  misses = SLEEP_HEARTBEAT_MISSES,
}: {
  slots: SleepSlot[];
  done?: SlotDone | null;
  now: Date;
  runningAt?: unknown;
  heartbeatAt?: unknown;
  owner?: unknown;
  currentOwner?: unknown;
  heartbeatMs?: unknown;
  misses?: unknown;
}): string | undefined {
  const savedOwner = String(owner || '');
  if (savedOwner) {
    // 自己还在跑：正常跳过。不是自己：上一实例留下的锁，直接接管（slot 已落标记则不补）。
    if (sameSleepOwner(savedOwner, currentOwner)) return undefined;
    return pickDueSleepSlot({ slots, done, now });
  }
  const running = Number(runningAt) || 0;
  const beat = Math.max(running, Number(heartbeatAt) || 0);
  const staleAfter = Math.max(1, Number(heartbeatMs) || SLEEP_HEARTBEAT_MS) * Math.max(1, Number(misses) || SLEEP_HEARTBEAT_MISSES);
  if (running > 0 && now.getTime() - beat < staleAfter) return undefined;
  return pickDueSleepSlot({ slots, done, now });
}

/**
 * 这一轮的结果值不值得把 slot 标成「今天跑过」。
 * busy = 另一轮正占着（同档重复叫醒时的第二发），什么都没干；标了就等于当天不再跑。
 */
export function shouldMarkSlot(result: { skipped?: unknown } | null | undefined): boolean {
  return result?.skipped !== 'busy';
}
