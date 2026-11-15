/**
 * 贵时段闸：补跑只许在便宜时段发生。
 *
 * 为什么有这一层：重启后启动补判会当场补跑当天过点的那一轮（一轮跑 30 分钟以上、
 * 烧真钱）。补跑如果正撞上峰价时段，那一轮就是白烧。规矩定死两条：
 *
 *   1. 便宜时段才允许当场补跑；
 *   2. 贵时段把这一档记成「待补」（sleep.state.json 的 `owedSlots`），
 *      等进入便宜时段的第一拍再补，补完清空。
 *
 * 判据**不在这里写死**。价档表只许有一份，两处各写一张迟早打架：
 * 这里只留一个注入点 `setCheapNowProbe()`，默认「全时段都算便宜」——
 * 也就是没接判据时行为跟改动前完全一致。dsh-price-clock 落地后，把
 * `isCheapNow` 那一行换成 import 它的导出即可，别的都不用动。
 *
 * 另外三条硬规矩，都是为了「别让整理静默停掉」：
 *
 * - 没有判据、判据抛错 → 一律按**便宜**处理（fail-open）。宁可偶尔在贵时段多花一笔，
 *   也不许判据一出毛病就把每天的整理全停掉。判据长期坏掉有第二天的兜底日志兜着。
 * - 待补槽只补**当天**的：跨天的槽作废并记一行日志。那一轮的输入是「昨天以前的会话」，
 *   今天的档反正要读同样的内容，隔天补跑等于白跑一遍。
 * - 待补槽积压超过 `SLEEP_OWED_MAX_AGE_MS`（2 天）还没补上，说明价档判断可能坏死了，
 *   打一行醒目日志作废，不静默丢。
 */

/** 过了多少毫秒算「补跑」而不是「准点触发」。准点有几秒到一分钟的抖动，别把正常到点当成补跑。 */
export const SLEEP_CATCHUP_GRACE_MS = 5 * 60 * 1000;

/** 待补槽最多攒这么久；再久说明价档判断坏了，作废并醒目记一行。 */
export const SLEEP_OWED_MAX_AGE_MS = 2 * 24 * 60 * 60 * 1000;

interface OwedSlot {
  key: string;
  at: number;
}

interface DroppedSlot {
  key: string;
  at: number;
  ageMs: number;
  expired: boolean;
}

interface SplitOwedSlots {
  today: OwedSlot[];
  dropped: DroppedSlot[];
}

interface CatchUpSlotInput {
  key?: unknown;
  now?: Date;
  graceMs?: unknown;
}

interface PlanSlotRunInput {
  key?: unknown;
  cheap?: boolean;
  catchUp?: boolean;
}

type CheapNowProbe = (now: Date) => boolean;

function dayKey(d: Date): string {
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
}

/** 价档判据：undefined = 还没接（等价于「永远是便宜时段」）。 */
let probe: CheapNowProbe | undefined;

/**
 * 接价档判据。`fn(now: Date) => boolean`，true = 便宜（半价），false = 贵（标准价）。
 * 传非函数等于拔掉判据，回到默认的「全时段都便宜」。
 */
export function setCheapNowProbe(fn: unknown): void {
  probe = typeof fn === 'function' ? fn as CheapNowProbe : undefined;
}

/** 拔掉注入的判据（测试之间互不污染用）。 */
export function resetCheapNowProbe(): void {
  probe = undefined;
}

/** 现在是不是便宜时段。没接判据、判据抛错 → true（fail-open，理由见文件头）。 */
export function isCheapNow(now: Date = new Date()): boolean {
  if (!probe) return true;
  try {
    return probe(now) !== false;
  } catch {
    return true;
  }
}

/**
 * slotKey（`2026-11-14 12:30`）→ 那一档的本地时刻。
 * 解析不出来（老 state、手改坏）返回 undefined，调用方按「不是补跑」处理。
 */
export function slotTimeOf(key: unknown): Date | undefined {
  const m = /^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2})$/.exec(String(key ?? ''));
  if (!m) return undefined;
  const [year, month, day, hour, minute] = m.slice(1).map(Number);
  const at = new Date(year, month - 1, day, hour, minute, 0, 0);
  return Number.isNaN(at.getTime()) ? undefined : at;
}

/**
 * 这一档是不是「过点补跑」：now 比档口晚超过 graceMs。
 * 正好 graceMs 不算（边界取「严格大于」，准点抖动留够余量）。
 */
export function isCatchUpSlot({ key, now = new Date(), graceMs = SLEEP_CATCHUP_GRACE_MS }: CatchUpSlotInput): boolean {
  const at = slotTimeOf(key);
  if (!at) return false;
  const grace = Number.isFinite(Number(graceMs)) && Number(graceMs) >= 0
    ? Number(graceMs)
    : SLEEP_CATCHUP_GRACE_MS;
  return now.getTime() - at.getTime() > grace;
}

/**
 * 待补槽分两堆：今天该补的，和已经跨天／积压该扔的。
 *
 * @returns {{today: Array<{key:string, at:number}>,
 *            dropped: Array<{key:string, at:number, ageMs:number, expired:boolean}>}}
 *   `expired` = 积压超过 2 天，调用方要打醒目日志（价档判断可能坏了）。
 */
export function splitOwedSlots(owed: Record<string, unknown> | null | undefined, now: Date = new Date()): SplitOwedSlots {
  const today = dayKey(now);
  const dueToday = [];
  const dropped = [];
  for (const [key, at] of Object.entries(owed && typeof owed === 'object' ? owed : {})) {
    const stamp = Number(at) || 0;
    if (String(key).startsWith(`${today} `)) {
      dueToday.push({ key, at: stamp });
      continue;
    }
    const ageMs = stamp > 0 ? Math.max(0, now.getTime() - stamp) : 0;
    dropped.push({ key, at: stamp, ageMs, expired: ageMs > SLEEP_OWED_MAX_AGE_MS });
  }
  dueToday.sort((a, b) => (a.key < b.key ? -1 : a.key > b.key ? 1 : 0));
  return { today: dueToday, dropped };
}

/**
 * 这一拍该干什么：
 * - `'run'`：当场跑（便宜时段；或者准点触发——定时器行为这轮先不改）；
 * - `'defer'`：贵时段又是在补跑，记待补，等便宜时段；
 * - `'none'`：没有到点的档。
 */
export function planSlotRun({ key, cheap = true, catchUp = false }: PlanSlotRunInput): 'run' | 'defer' | 'none' {
  if (!key) return 'none';
  if (cheap) return 'run';
  return catchUp ? 'defer' : 'run';
}
