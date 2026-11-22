/**
 * 上下文占压缩线提醒（2026-11-13， 改成单次）
 *
 * 原来这活是 ctx-watch.sh + ctx-watch.timer 干的：每 5 分钟读一遍
 * projcache，算会话的自动压缩线，过 80% / 100% / 130% 就推 QQ。它的根因错误是
 * provider/model 取 rows.contextTimeline.val（2026-11-04 08:18 后该键从 projcache
 * 消失），于是永远退到全局 ratio 0.15，主会话的压缩线被算成 157286（真实 104857），
 * 第一档 80% = 125829 永远晚于真实压缩点——结构性永不触发。
 *
 * 搬进插件后不再读盘、不再猜：
 * - 会话对象从 `ctx.sessions.get(options.sessionId)` 拿（llm/stream 的 options 带
 *   sessionId，见 dsh-llm/lib/types/types.d.ts:573）；
 * - 占用量和窗口从 `ctx.tokenMeter.contextPressure(session)` 拿（dsh-token-meter
 *   lib/index.js:818），跟 compaction-basic 判压用同一份投影；
 * - 路由从 `session.requestContext()` 拿（compaction-basic 的 routedTarget 也是这条）；
 * - ratio/headroom/maxTokens 从 `ctx.get('compaction').config` 拿——就是运行中的
 *   那份已解析配置，不用像脚本那样 `dsh --dump-config` + 缓存。
 *
 * 公式照 ctx-watch.sh:236-238（也等于 dsh-compaction-basic/lib/index.js:171-179）：
 *   thresholdTokens = floor(min(窗口*ratio, 窗口 - 请求层maxTokens - headroom))
 * 容量项算崩（<=0）就退回 窗口*ratio，别退化成 0。
 *
 * 2026-11-13 维护者要求：档位全删，只留一个触发点——占用 >= 压缩线
 * 的 95%（含）发一条，说清「下一轮会自动压缩一次」。过线之后再升档已经没有意义
 * （压缩就在下一轮），所以同一会话只发一次。
 *
 * 2026-11-15：有人收到 95%（199.9k/209.7k）和 98%（205.2k/209.7k）
 * 两条，以为是两档。其实只剩一档，两条日志里的提醒线都是 199229；重复来自去重两条
 * 路都漏：①「提醒之后有 compaction/end」判据没要求水位真的降下来，压缩失败/日志重放/
 * 时间戳不可信时会误判成压过；②state 整份覆盖写，另一份内存副本（重载前后）会把刚落盘
 * 的「已提醒」记录抹掉。现在 compacted 判据加「used < atTokens」，落盘改读-合并-写
 * 并带删除墓碑，两处都堵上。
 *
 * 去重落文件：每会话记 { notified, atTokens, at }，只有真的压过一次才清记录——
 * 判据是会话日志里出现比上次提醒更晚的 `compaction/end`，且当前占用已经比上次提醒时低；
 * 拿不到日志时退回水位判据（占用掉到压缩线 45% 以下，或比上次提醒时低一半以上）。
 * 清掉后水位重新涨回 95% 才允许再发一条。旧 state 里的 {level>0} 一律当「已提醒」，
 * 重启不重弹。
 *
 * 判完两路都走：dsh-events 发 `memory.context.pressure`（volatile，宿主内可见），
 * 同时 spawn cfg.notifyScript 推 QQ——通道跟以前一样，没换。
 * 事件注册在 setupContextWatch（apply 里调用）做一次，别每次调用重复注册。
 */
import fs from 'node:fs';
import path from 'node:path';
import * as paths from './shared/paths.js';
import { runNotify } from './shared/notify.js';

/** 事件 topic：必须带点，裸名 dsh-events 直接 EV-001。 */
export const PRESSURE_TOPIC = 'memory.context.pressure';

/** 单会话的提醒记录（老 state 只有 level 也认）。 */
export interface WatchEntry {
  notified?: boolean;
  atTokens?: number;
  at?: number;
  level?: number;
}

/** 落盘的 state：每会话记录 + 删除墓碑。 */
export interface WatchState {
  sessions: Record<string, WatchEntry>;
  tombstones: Record<string, number>;
}

/** 会话日志事件的最小面。 */
interface SessionEventLike {
  time?: unknown;
  type?: unknown;
  data?: Record<string, unknown> | null;
}

/** 宿主会话的最小面。 */
export interface SessionLike {
  log?: SessionEventLike[];
  header?: Record<string, unknown>;
  requestContext?: () => { provider?: unknown; model?: unknown; contextWindow?: unknown } | null;
  requestHeader?: () => { config?: { maxTokens?: unknown } } | null;
  [key: string]: unknown;
}

/** ctx.get() 能返回的服务面：只认本文件用到的几个。 */
interface ServiceLike {
  defineEvent?: (def: unknown) => void;
  registry?: { get?: (name: string) => unknown };
  publish?: (event: { name: string; payload: Record<string, unknown> }) => void;
  get?: (id: string) => SessionLike;
  contextPressure?: (session: unknown) => Record<string, unknown> | null | undefined;
  config?: Record<string, unknown> | null;
  cachedSnapshot?: (header: unknown, keys: string[]) => { values?: Record<string, unknown> } | null | undefined;
}

/** 插件 ctx 的最小面。 */
export interface ContextWatchCtx {
  get?: (name: string) => ServiceLike;
}

/** check(options) 的入参：llm/stream 的 options 带 sessionId，辅助调用带 purpose。 */
export interface CheckOptions {
  purpose?: unknown;
  sessionId?: unknown;
  provider?: unknown;
  model?: unknown;
}

/** 装了属性的 check：跳过/告警只记一次。 */
export type CheckFn = ((options: CheckOptions) => Promise<void>) & {
  _skippedWorkers?: Set<string>;
  _skippedProviders?: Set<string>;
  _warnedConfig?: boolean;
};

/** setupContextWatch 的产物。 */
export interface ContextWatchHandle {
  check: CheckFn;
  stateFile: string;
}

/** 事件类型注册项。volatile：提醒是瞬时信号，不该在事件表里留行。 */
export const PRESSURE_EVENT_DEF = Object.freeze({
  name: PRESSURE_TOPIC,
  version: 1,
  durability: 'volatile',
  payloadSchema: (payload: unknown) => {
    const p = payload as { sessionId?: unknown; origin?: unknown; level: number; pressureTokens?: unknown; thresholdTokens: number; at?: unknown };
    return (
      typeof p?.sessionId === 'string' &&
      p.sessionId.length > 0 &&
      typeof p?.origin === 'string' &&
      (p.origin === 'main' || p.origin === 'worker') &&
      Number.isInteger(p?.level) &&
      p.level > 0 &&
      Number.isFinite(p?.pressureTokens) &&
      Number.isFinite(p?.thresholdTokens) &&
      p.thresholdTokens > 0 &&
      typeof p?.at === 'string'
    );
  },
});

/** 配置里查不到 compaction-basic 时用的兜底，跟 ctx-watch.sh 的 DEF 一致。 */
export const FALLBACK_RATIO = 0.15;
export const FALLBACK_HEADROOM = 65536;
/** 提醒线：压缩线的 95%（含）。2026-11-13：只此一档。 */
export const NOTIFY_RATIO = 0.95;
/**
 * 2026-11-13：子会话（子会话）一律不算线、不发提醒——维护者要求
 * 子会话不限制上下文长度；全局包 compactWorker=false 时子会话只有硬顶保护，
 * 没有这张 95% 的线可报。主会话行为不变。
 */
/** 压过了的判据：占用 < 压缩线 * 0.45（压完 retain 12000，远低于这条）。 */
export const RESET_RATIO = 0.45;

/**
 * 压缩线：floor(min(窗口*ratio, 窗口 - 请求层maxTokens - headroom))。
 *
 * @param {object} input - 计算输入。
 * @param {number} input.contextWindow - 模型窗口。
 * @param {number} [input.thresholdRatio] - 该路由的 ratio，缺省 0.15。
 * @param {number} [input.headroomTokens] - 压缩headroom，缺省 65536。
 * @param {number} [input.reservedTokens] - 请求层输出上限；不给按 headroom 算（照脚本）。
 * @returns {number} 压缩线 token 数；算不出来返回 0。
 */
export function compactThreshold({ contextWindow, thresholdRatio, headroomTokens, reservedTokens }: { contextWindow?: unknown; thresholdRatio?: unknown; headroomTokens?: unknown; reservedTokens?: unknown } = {}): number {
  const win = Math.floor(Number(contextWindow) || 0);
  if (win <= 0) return 0;
  const ratio = Number(thresholdRatio) > 0 ? Number(thresholdRatio) : FALLBACK_RATIO;
  const head = Number(headroomTokens) > 0 ? Number(headroomTokens) : FALLBACK_HEADROOM;
  const reserved = Number(reservedTokens) > 0 ? Number(reservedTokens) : head;
  let thr = Math.floor(Math.min(win * ratio, win - reserved - head));
  // 容量项算崩（headroom 过大、窗口太小）就退回比例项，别退化成 0 变成「永不提醒」。
  if (!(thr > 0)) thr = Math.floor(win * ratio);
  return thr > 0 ? thr : 0;
}

/**
 * 提醒线 = floor(压缩线 * 0.95)；压缩线无效返回 0。
 *
 * @param {number} compactTokens - 压缩线。
 * @returns {number} 提醒线 token 数。
 */
export function notifyThreshold(compactTokens: unknown): number {
  const line = Number(compactTokens) || 0;
  if (!(line > 0)) return 0;
  return Math.floor(line * NOTIFY_RATIO);
}

/**
 * 这一轮该不该发（唯一触发点：压缩线 95%，每会话只发一次）。
 *
 * @param {object} input - 判定输入。
 * @param {number} input.pressureTokens - 当前占用。
 * @param {number} input.compactTokens - 压缩线。
 * @param {object} [input.entry] - state.sessions[会话]（旧格式 {level,at} 也认）。
 * @param {number} [input.compactedAt] - 上次提醒之后最近一次 compaction/end 的时间（ms）。
 * @returns {{emit:boolean, reset:boolean, notified:boolean, threshold:number}} 判定结果。
 */
export function shouldNotify({ pressureTokens, compactTokens, entry, compactedAt }: { pressureTokens?: unknown; compactTokens?: unknown; entry?: WatchEntry | null; compactedAt?: unknown } = {}): { emit: boolean; reset: boolean; notified: boolean; threshold: number } {
  const line = Number(compactTokens) || 0;
  const used = Number(pressureTokens) || 0;
  const threshold = notifyThreshold(line);
  const atTokens = Number(entry?.atTokens) || 0;
  const remindedAt = Number(entry?.at) || 0;
  // 压过了：有比上次提醒更晚的压缩记录（首选判据），或水位掉下去（拿不到日志时兜底）。
  // 2026-11-15：光有压缩记录不够，还要求当前水位比上次提醒时低。
  // 压缩后占用必然低于压缩线，所以真压过不会挡；挡的是「记录说压过、水位还在涨」的误判
  // （当天 03:11 199891 → 03:14 205232 连发两条就是这么来的）。
  const belowReminder = atTokens > 0 && used < atTokens;
  const compactedAfterReminder = remindedAt > 0 && Number(compactedAt) > remindedAt && belowReminder;
  const dropped = line > 0 && (used < line * RESET_RATIO || (atTokens > 0 && used < atTokens * 0.5));
  const reset = compactedAfterReminder || dropped;
  // 旧 state 只有 level，level>0 就算提醒过，别在升级后对同一会话重弹。
  const notified = !reset && (entry?.notified === true || Number(entry?.level) > 0);
  if (reset) {
    // 压完再涨回提醒线才允许再发；这一轮没到线就只清记录、不发。
    return { emit: threshold > 0 && used >= threshold, reset: true, notified: false, threshold };
  }
  if (!(threshold > 0)) return { emit: false, reset: false, notified, threshold: 0 };
  if (notified) return { emit: false, reset: false, notified: true, threshold };
  return { emit: used >= threshold, reset: false, notified: false, threshold };
}

/**
 * 子会话（子会话）整轮跳过提醒：维护者要求子会话不限制上下文长度。
 *
 * compactWorker 不显式为 true 时，全局包
 * @deepseek-ai/dsh-compaction-basic 对子会话只留硬顶保护、不按常规压缩线压，
 * 所以子会话压根没有 95% 这条线，报数只会误导。显式开 compactWorker 才恢复。
 *
 * @param {object} input - 判定输入。
 * @param {string} input.origin - describeSession 判出的 main / worker。
 * @param {boolean} [input.compactWorker] - 运行中的 compaction-basic 配置。
 * @returns {boolean} true = 这一轮不算线也不发提醒。
 */
export function shouldSkipSession({ origin, compactWorker }: { origin?: unknown; compactWorker?: unknown } = {}): boolean {
  return origin === 'worker' && compactWorker !== true;
}

/**
 * 命中「不发提醒的 provider 名单」就整轮跳过（2026-11-21）。
 *
 * 群会话走独立路由（本机 magpie-grp / group/ds），用户要求群里不要再收到
 * 「上下文 XXk / 52.4k 压缩线」这类提醒；主会话（magpie）和子会话（按 origin 另行跳过）
 * 行为不变。判据只看 provider：主会话的 model 同样是 group/ds，按 model 前缀分会误伤。
 *
 * 名单为空（默认）时恒为 false，等于旧行为；匹配口径跟 selectPolicy 一致——精确相等、
 * 区分大小写。
 *
 * @param {object} input - 判定输入。
 * @param {string} input.provider - 会话当前路由 provider（check 里取自 session.requestContext()）。
 * @param {string[]} [input.skipProviders] - 配置 contextWatchSkipProviders。
 * @returns {boolean} true = 这一轮不算线也不发提醒。
 */
export function shouldSkipProvider({ provider, skipProviders }: { provider?: unknown; skipProviders?: unknown } = {}): boolean {
  const name = String(provider || '');
  if (!name || !Array.isArray(skipProviders)) return false;
  return skipProviders.some((item) => String(item) === name);
}

/**
 * 会话日志里最近一次压缩结束的时间；只往回扫到 `since` 为止，日志长了也不贵。
 *
 * @param {object} session - ctx.get('sessions').get(id) 拿到的会话。
 * @param {number} [since] - 只看这个时间（ms）之后的记录；0 = 不限。
 * @returns {number} compaction/end 的 time（ms）；没有就 0。
 */
export function lastCompactionAt(session: SessionLike | null | undefined, since = 0): number {
  const log = session?.log;
  if (!Array.isArray(log)) return 0;
  for (let i = log.length - 1; i >= 0; i -= 1) {
    const event = log[i];
    const time = Number(event?.time) || 0;
    if (since > 0 && time && time < since) break;
    if (event?.type === 'compaction/end') return time;
  }
  return 0;
}

/**
 * 按 provider+model 精确匹配 modelPolicies，匹配不到继承全局（ctx-watch.sh 同款）。
 *
 * @param {object} compactionConfig - ctx.get('compaction').config。
 * @param {object} target - {provider, model}。
 * @returns {object} {thresholdRatio, headroomTokens, retainTokens, maxTokens, source}。
 */
export function selectPolicy(compactionConfig: Record<string, unknown> | null | undefined, { provider, model }: { provider?: unknown; model?: unknown } = {}): { thresholdRatio: number; headroomTokens: number; retainTokens: number; maxTokens: number; source: string } {
  const cfg = compactionConfig || {};
  const policies: Array<Record<string, unknown>> = Array.isArray(cfg.modelPolicies) ? cfg.modelPolicies : [];
  const hit = policies.find(
    (p) => p && p.provider === provider && String(p.model) === String(model),
  );
  const source = hit ? 'policy' : 'global';
  const pick = (key: string, fallback: number): number => {
    const value = hit?.[key] ?? cfg[key];
    return Number(value) > 0 ? Number(value) : fallback;
  };
  return {
    thresholdRatio: pick('thresholdRatio', FALLBACK_RATIO),
    headroomTokens: pick('headroomTokens', FALLBACK_HEADROOM),
    retainTokens: pick('retainTokens', 12000),
    maxTokens: pick('maxTokens', FALLBACK_HEADROOM),
    source,
  };
}

/**
 * 推送文案：数字能对账（当前 / 压缩线 / 百分比），一句说清后面会自动压。
 * 主会话写「主会话」，子会话带自己的 label，跟来源区分一致。
 *
 * @param {object} input - 文案素材。
 * @param {string} input.origin - main / worker。
 * @param {string} [input.label] - 子会话的 session label。
 * @param {string} [input.sessionId] - 兜底短 id。
 * @param {number} input.pressureTokens - 当前占用。
 * @param {number} input.compactTokens - 压缩线。
 * @returns {string} 两行文本。
 */
export function formatPressureMessage({
  origin,
  label,
  sessionId,
  pressureTokens,
  compactTokens,
}: { origin?: unknown; label?: unknown; sessionId?: unknown; pressureTokens?: unknown; compactTokens?: unknown } = {}): string {
  const used = Number(pressureTokens) || 0;
  const line = Number(compactTokens) || 0;
  const pct = line > 0 ? Math.round((used * 100) / line) : 0;
  const who =
    origin === 'worker'
      ? `子会话（${label || String(sessionId || '').slice(0, 8) || '无标签'}）`
      : '主会话';
  const k = (value: number): string => (value / 1000).toFixed(1);
  return (
    `🐟 ${who} · 上下文 ${k(used)}k / ${k(line)}k 压缩线（${pct}%），超过后自动压缩\n` +
    '下一轮会自动压缩一次'
  );
}

/** 状态文件读写：落盘才扛得住重启，别放内存。 */
function loadState(file: string): WatchState {
  try {
    const raw = JSON.parse(fs.readFileSync(file, 'utf8'));
    if (raw && typeof raw === 'object' && raw.sessions && typeof raw.sessions === 'object') {
      return {
        sessions: raw.sessions,
        tombstones: raw.tombstones && typeof raw.tombstones === 'object' ? raw.tombstones : {},
      };
    }
  } catch {
    /* 没文件、坏文件都当空 */
  }
  return { sessions: {}, tombstones: {} };
}

/**
 * 清掉一个会话的提醒记录，并留删除墓碑。
 *
 * 墓碑是给 saveState 的读-合并-写看的：没有它，另一份内存 state 合并时会把刚删掉的
 * 记录原样加回来，压过之后的下一次提醒就永远清不掉（同一条线一直不重发）。
 *
 * @param {object} state - 内存 state。
 * @param {string} sessionId - 会话 id。
 * @param {number} [now] - 删除时间（ms）。
 */
function forget(state: WatchState, sessionId: string, now = Date.now()): void {
  delete state.sessions[sessionId];
  state.tombstones[sessionId] = now;
}

/**
 * 落盘：读-合并-写，不是整份覆盖。
 *
 * 2026-11-15：同一份 state 会被多个内存副本持有（插件重载前后的 fiber、
 * 宿主里并存的实例），整份覆盖会把别人刚写下的「已提醒」记录抹掉，水位没降也再推一条
 * （11-15 03:14 那条 205232 就是被旧副本盖回「未提醒」后重发的）。合并规则：
 * 同一会话取 at 更大的那条；磁盘上有、内存没有的保留；墓碑比记录新就丢弃该记录。
 *
 * @param {string} file - state 文件路径。
 * @param {object} state - 内存 state。
 */
function saveState(file: string, state: WatchState): void {
  try {
    const onDisk = loadState(file);
    const merged = { sessions: { ...onDisk.sessions }, tombstones: { ...onDisk.tombstones } };
    for (const [id, at] of Object.entries(state.tombstones || {})) {
      if (!(Number(merged.tombstones[id]) >= Number(at))) merged.tombstones[id] = at;
    }
    for (const [id, entry] of Object.entries(state.sessions || {})) {
      const disk = merged.sessions[id];
      const mine = Number(entry?.at) || 0;
      if (!disk || mine > (Number(disk.at) || 0)) merged.sessions[id] = entry;
    }
    for (const [id, entry] of Object.entries(merged.sessions)) {
      if (Number(merged.tombstones[id]) >= (Number(entry?.at) || 0)) delete merged.sessions[id];
    }
    fs.mkdirSync(path.dirname(file), { recursive: true });
    // 临时名带 pid：多个进程同时落盘时不互相踩 tmp。
    const tmp = `${file}.${process.pid}.tmp`;
    fs.writeFileSync(tmp, JSON.stringify(merged), 'utf8');
    fs.renameSync(tmp, file);
  } catch {
    /* 落盘失败不影响推送 */
  }
}

/** 活动会话表别无限长：按最后活跃时间裁到 200 条；墓碑多留一倍，够跨重启。 */
function pruneState(state: WatchState, keep = 200): void {
  const rows = Object.entries(state.sessions);
  if (rows.length > keep) {
    rows.sort((a, b) => (Number(b[1]?.at) || 0) - (Number(a[1]?.at) || 0));
    state.sessions = Object.fromEntries(rows.slice(0, keep));
  }
  const graves = Object.entries(state.tombstones || {});
  if (graves.length > keep * 2) {
    graves.sort((a, b) => (Number(b[1]) || 0) - (Number(a[1]) || 0));
    state.tombstones = Object.fromEntries(graves.slice(0, keep * 2));
  }
}

/** 会话来源与标题：主会话 / 子会话（label 取 subagent/descriptor 事件）。 */
function describeSession(session: SessionLike | null | undefined, ctx: ContextWatchCtx): { origin: string; label: string; title: string } {
  const header = session?.header || {};
  const worker =
    header.origin === 'subagent' ||
    (Number.isInteger(header.delegationDepth) && (header.delegationDepth as number) > 0);
  let label = '';
  if (worker) {
    try {
      for (const event of session!.log || []) {
        if (event?.type === 'subagent/descriptor' && typeof event.data?.label === 'string') {
          label = event.data.label;
          break;
        }
      }
    } catch {
      /* 日志读不动就算了，用短 id */
    }
  }
  let title = '';
  try {
    const cached = ctx.get!('sessionProjectionCache')?.cachedSnapshot?.(header, ['title']);
    if (typeof cached?.values?.title === 'string') title = cached.values.title;
  } catch {
    /* 标题只进 payload，拿不到不影响判档 */
  }
  if (!title) title = label;
  return { origin: worker ? 'worker' : 'main', label, title };
}

/**
 * 装上压线提醒：注册事件类型，返回每轮调一次的 check(options)。
 *
 * @param {object} ctx - cordis 上下文（apply 的 ctx）。
 * @param {object} cfg - dsh-memory 配置。
 * @returns {{check: (options: object) => Promise<void>, stateFile: string}} 探针。
 */
export function setupContextWatch(ctx: ContextWatchCtx, cfg: Record<string, unknown>): ContextWatchHandle {
  const stateFile = String(cfg.contextWatchStateFile || paths.memoryFile('context-watch.state.json'));
  const logFile = String(cfg.contextWatchLogFile || paths.logFile('memory_context_watch.log'));
  const state = loadState(stateFile);
  pruneState(state);

  const log = (line: string): void => {
    try {
      fs.mkdirSync(path.dirname(logFile), { recursive: true });
      const stamp = new Date().toISOString().replace('T', ' ').slice(0, 19);
      fs.appendFileSync(logFile, `[${stamp}] ${line}\n`, 'utf8');
      const buf = fs.readFileSync(logFile, 'utf8').split('\n');
      if (buf.length > 800) fs.writeFileSync(logFile, buf.slice(-400).join('\n'), 'utf8');
    } catch {
      /* 日志失败不影响提醒 */
    }
  };

  // 事件类型只注册一次（apply 里），重复注册（热重载/上一份 fiber 已注册）不算失败。
  let events: ServiceLike | undefined;
  try {
    events = ctx.get!('events');
  } catch {
    events = undefined;
  }
  if (events && typeof events.defineEvent === 'function') {
    try {
      events.defineEvent(PRESSURE_EVENT_DEF);
    } catch (error) {
      if (events.registry?.get?.(PRESSURE_TOPIC) === undefined) {
        log(`注册事件类型 ${PRESSURE_TOPIC} 失败（${(error as { code?: unknown })?.code || (error as { message?: unknown })?.message || error}），只推 QQ`);
        events = undefined;
      }
    }
  } else {
    log('events 服务不在，压线提醒只推 QQ 不发事件');
    events = undefined;
  }

  function publish(payload: Record<string, unknown>): void {
    if (!events) return;
    try {
      events.publish!({ name: PRESSURE_TOPIC, payload });
    } catch (error) {
      log(`发布 ${PRESSURE_TOPIC} 失败（${(error as { code?: unknown })?.code || (error as { message?: unknown })?.message || error}）：${payload.sessionId} L${payload.level}`);
    }
  }

  async function notify(message: string): Promise<void> {
    if (!cfg.contextWatchNotify) return;
    // 2026-11-16：spawn 收给 runNotify，ENOENT 由它挂 error 监听记 warn，不再带崩宿主。
    runNotify({ script: String(cfg.notifyScript || paths.notifyScript()), args: [message], label: 'context-watch', log });
  }

  async function check(options: CheckOptions): Promise<void> {
    if (!cfg.contextWatchEnabled) return;
    // 辅助调用（整理/取标题/压缩）不判：它们的 route 不是会话的 route，会把线算歪。
    if (options?.purpose) return;
    const sessionId = options?.sessionId ? String(options.sessionId) : '';
    if (!sessionId) return;
    const session = ctx.get!('sessions')?.get?.(sessionId);
    if (!session) {
      log(`拿不到会话对象 ${sessionId}，本轮跳过`);
      return;
    }
    const meter = ctx.get!('tokenMeter');
    if (typeof meter?.contextPressure !== 'function') {
      log('tokenMeter 服务不在，本轮跳过');
      return;
    }
    const pressure: Record<string, unknown> = meter.contextPressure(session) || {};
    const used = Number(pressure.pressureTokens);
    const route: { provider?: unknown; model?: unknown; contextWindow?: unknown } = session.requestContext?.() || {};
    const window = Number(pressure.contextWindow || route.contextWindow);
    if (!(used >= 0) || !(window > 0)) return;

    const provider = route.provider || options.provider || '';
    const model = route.model || options.model || '';
    // 名单里的 provider（本机是群会话的 magpie-grp）整轮跳过：群里不发压线提醒。
    // 以前按主会话规则发过的记录顺手清掉，别留着以后挡回提醒（同子会话分支口径）。
    if (shouldSkipProvider({ provider, skipProviders: cfg.contextWatchSkipProviders })) {
      if (state.sessions[sessionId]) {
        forget(state, sessionId);
        saveState(stateFile, state);
      }
      if (!(check as CheckFn)._skippedProviders) (check as CheckFn)._skippedProviders = new Set();
      if (!(check as CheckFn)._skippedProviders!.has(sessionId)) {
        (check as CheckFn)._skippedProviders!.add(sessionId);
        log(`会话路由 ${provider}/${model} 在跳过名单里，不发压线提醒`);
      }
      return;
    }
    const compaction = (() => {
      try {
        return ctx.get!('compaction')?.config;
      } catch {
        return undefined;
      }
    })();
    // 子会话（子会话）不算线、不发提醒：维护者要求子会话不限制上下文长度。
    // compactWorker 没显式打开时，全局包对子会话只留硬顶，没有 95% 这条线可报。
    const who = describeSession(session, ctx);
    if (shouldSkipSession({ origin: who.origin, compactWorker: compaction?.compactWorker })) {
      // 以前按主会话规则发过的记录顺手清掉，别留着以后冒出来。
      if (state.sessions[sessionId]) {
        forget(state, sessionId);
        saveState(stateFile, state);
      }
      if (!(check as CheckFn)._skippedWorkers) (check as CheckFn)._skippedWorkers = new Set();
      if (!(check as CheckFn)._skippedWorkers!.has(sessionId)) {
        (check as CheckFn)._skippedWorkers!.add(sessionId);
        log(`子会话（${who.label || sessionId.slice(0, 8)}）不限制上下文，跳过压线提醒`);
      }
      return;
    }
    const policy = selectPolicy(compaction, { provider, model });
    if (!compaction && !(check as CheckFn)._warnedConfig) {
      (check as CheckFn)._warnedConfig = true;
      log('compaction 服务不在，压缩线退回全局 ratio 0.15（跟旧脚本同一个坑，得查为什么没挂）');
    }
    const reserved = Number(session.requestHeader?.()?.config?.maxTokens) || policy.maxTokens;
    const compactTokens = compactThreshold({
      contextWindow: window,
      thresholdRatio: policy.thresholdRatio,
      headroomTokens: policy.headroomTokens,
      reservedTokens: reserved,
    });
    if (!(compactTokens > 0)) return;
    const entry = state.sessions[sessionId];
    // 有记录才去翻日志：找「提醒之后压过一次」的证据，没有就走水位兜底。
    const compactedAt = entry ? lastCompactionAt(session, Number(entry.at) || 0) : 0;
    const verdict = shouldNotify({ pressureTokens: used, compactTokens, entry, compactedAt });
    if (!verdict.emit) {
      // 压过了才需要落盘（清掉已提醒记录），没到线/已提醒过不写，省得每轮刷盘。
      if (verdict.reset && entry) {
        forget(state, sessionId);
        saveState(stateFile, state);
      }
      return;
    }

    const at = new Date().toISOString();
    const payload = {
      sessionId,
      title: who.title,
      origin: who.origin,
      // 事件 schema 要 level > 0：单档提醒固定记 1，读的人当「过线了」就好。
      level: 1,
      pressureTokens: used,
      thresholdTokens: verdict.threshold,
      compactTokens,
      contextWindow: window,
      ratio: used / compactTokens,
      providerModel: `${provider}/${model}`,
      at,
    };
    publish(payload);
    await notify(
      formatPressureMessage({
        origin: who.origin,
        label: who.label,
        sessionId,
        pressureTokens: used,
        compactTokens,
      }),
    );
    state.sessions[sessionId] = { level: 1, notified: true, atTokens: used, at: Date.now() };
    pruneState(state);
    saveState(stateFile, state);
    log(
      `提醒 ${who.origin === 'worker' ? `子会话（${who.label || sessionId.slice(0, 8)}）` : '主会话'} ` +
        `${used}/${compactTokens}（提醒线 ${verdict.threshold}，${provider}/${model} ${policy.source}）只发这一次`,
    );
  }

  return { check, stateFile };
}
