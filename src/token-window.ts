/**
 * 拼 prompt 的容量边界：按 token 动态算。
 *
 * 起因：以前的 sleepMemoryChars=120000 / addPassChars=40000 / pickExisting 40 条 12000 字 /
 * clusterMaxSize=30 / mergeBatchMax=6 全是拍脑袋的固定条数或字符数。窗口 128k 时 12000 字
 * 只用了不到 10%，剩下的空间白白空着；窗口小的模型又可能被 120000 字顶爆。这些都该由
 * 「这次要拼进去多少 token」和「模型窗口多大」两个数决定，而不是常数。
 *
 * 窗口值不写死在代码里，按这个顺序取：
 *   ① cfg.contextWindowTokens（插件配置里显式给模型的实际上下文长度，最优先）；
 *   ② tokenMeter.contextPressure(session).contextWindow（请求层实测的路由窗口，最可靠）；
 *   ③ session.requestContext().contextWindow（路由元数据里的窗口，tokenMeter 没记到时兜底）；
 *   ④ 宿主 compaction 的 modelPolicies（provider+model 精确匹配）或全局 contextWindow；
 *   ⑤ 0 = 谁都没给。调用方按「窗口未知」处理：不切批（全塞），打一条 warn。
 *
 * 2026-11-14：以前只读 compaction.config.contextWindow，但 compaction-basic
 * 的合法键里没有它（settings.yaml 里的 1048576 是 llm 模型定义），所以永远「未知」。
 * 真值在 tokenMeter（和服务端每轮记的 request/context 事件）里，session 得由调用方带进来；
 * 拿不到 session（脚本、无会话上下文）时逐级降级，不抛错。
 *
 * token 估算：中文（含全角标点）按 1 字 1 token，ASCII 按 4 字符 1 token。
 * 中文 1:1 是保守上限（实际 bge/主流分词器中文多在 1~1.7 字/token），宁可少塞也不超窗。
 */
import { keyLog } from './file-log.js';

/** 插件配置里本文件认的几项；其余字段一概不看。 */
interface TokenWindowConfig {
  contextWindowTokens?: unknown;
  provider?: unknown;
  model?: unknown;
  sleepLogFile?: string | null;
  [key: string]: unknown;
}

/** 宿主会话与服务的最小面：拿不到就逐级降级，不抛。 */
interface SessionLike {
  log?: unknown[];
  header?: Record<string, unknown>;
  config?: Record<string, unknown> | null;
  get?: (id: string) => SessionLike;
  contextPressure?: (session: unknown) => Record<string, unknown> | null;
  requestContext?: () => { contextWindow?: unknown; provider?: unknown; model?: unknown } | null;
  requestHeader?: () => { config?: { maxTokens?: unknown } } | null;
  [key: string]: unknown;
}

/** 插件 ctx 的最小面（可为假/空，测试里的假 ctx 没有 get 也不该炸）。 */
export interface TokenWindowCtx {
  get?: (name: string) => SessionLike;
  logger?: { warn?: (line: string) => void };
}

/** 一次窗口读取的结果：tokens=0 表示谁都没给。 */
export interface WindowInfo {
  tokens: number;
  source: string;
}

/** 拼 prompt 的预算：budget=0 表示窗口未知（不切）。 */
export interface PromptBudget {
  window: number;
  reserve: number;
  budget: number;
}

/** 按 token 装批的产物。 */
export interface PackedTokens<T> {
  picked: T[];
  omitted: T[];
  used: number;
}

/** 没拿到宿主的输出上限时，预留窗口的 1/4 给 system/输出。是比例，不是固定 token 数。 */
export const DEFAULT_RESERVE_RATIO = 0.25;

/**
 * 估一段文本的 token 数（保守：中文按字 1:1，ASCII 4:1）。
 * @param {string} text
 * @returns {number}
 */
export function estimateTokens(text: unknown): number {
  const s = String(text ?? '');
  let cjk = 0;
  let ascii = 0;
  for (let i = 0; i < s.length; i += 1) {
    const code = s.codePointAt(i) as number;
    if (code < 0x80) ascii += 1;
    else cjk += 1;
    if (code > 0xffff) i += 1; // 代理对只算一个「字」
  }
  return Math.ceil(cjk + ascii / 4);
}

/** 宿主的 compaction 配置；拿不到返回 null（测试里的假 ctx 没有 get 也不该炸）。 */
function compactionConfigOf(ctx: TokenWindowCtx | null | undefined): Record<string, unknown> | null {
  try {
    if (typeof ctx?.get !== 'function') return null;
    const service = ctx.get('compaction');
    return service?.config || null;
  } catch {
    return null;
  }
}

/** 按 provider+model 找 modelPolicies 里那一条；匹配不到返回 undefined（继承全局）。 */
function policyOf(ctx: TokenWindowCtx | null | undefined, cfg: TokenWindowConfig = {}): { cc: Record<string, unknown> | null; hit: Record<string, unknown> | undefined } {
  const cc = compactionConfigOf(ctx);
  if (!cc) return { cc: null, hit: undefined };
  const policies: Array<Record<string, unknown>> = Array.isArray(cc.modelPolicies) ? cc.modelPolicies : [];
  const hit = policies.find(
    (p) => p && p.provider === cfg.provider && String(p.model) === String(cfg.model),
  );
  return { cc, hit };
}

/**
 * 把调用方给的 session / sessionId 换成会话对象。
 * 拿不到（脚本、无会话、sessions 服务不在）返回 null，不抛。
 * @param {object} ctx 插件 ctx（可为假/空）
 * @param {object|string|null} session 会话对象或 sessionId
 * @returns {object|null}
 */
export function sessionOf(ctx: TokenWindowCtx | null | undefined, session: unknown): SessionLike | null {
  try {
    if (session && typeof session === 'object') return session as SessionLike;
    if (typeof session === 'string' && session) {
      return ctx?.get?.('sessions')?.get?.(session) || null;
    }
    return null;
  } catch {
    return null;
  }
}

/** tokenMeter 记的路由窗口；没会话/服务不在/读不到都返回 0。 */
function tokenMeterWindowOf(ctx: TokenWindowCtx | null | undefined, session: SessionLike | null): number {
  if (!session) return 0;
  try {
    const meter = typeof ctx?.get === 'function' ? ctx.get('tokenMeter') : null;
    if (typeof meter?.contextPressure !== 'function') return 0;
    const pressure: Record<string, unknown> = meter.contextPressure(session) || {};
    const win = Math.floor(Number(pressure.contextWindow) || 0);
    return win > 0 ? win : 0;
  } catch {
    return 0;
  }
}

/** 会话路由元数据里的窗口；没有 requestContext / 没给窗口都返回 0。 */
function requestContextWindowOf(session: SessionLike | null): number {
  if (!session) return 0;
  try {
    const route = session.requestContext?.() || {};
    const win = Math.floor(Number(route.contextWindow) || 0);
    return win > 0 ? win : 0;
  } catch {
    return 0;
  }
}

/** 这次读取命中的来源（日志用）；没命中 tokens=0、source=''。 */
export const WINDOW_SOURCES = Object.freeze({
  config: 'contextWindowTokens',
  tokenMeter: 'tokenMeter',
  requestContext: 'session.requestContext',
  compaction: 'compaction',
});

/**
 * 模型窗口 + 来源。窗口未知时返回 {tokens:0, source:''}。
 * @param {object} ctx 插件 ctx（可为假/空）
 * @param {object} cfg 插件配置
 * @param {object|string|null} [session] 会话对象或 sessionId（拿不到就逐级降级）
 * @returns {{tokens:number, source:string}}
 */
export function windowInfoOf(ctx: TokenWindowCtx | null | undefined, cfg: TokenWindowConfig = {}, session: unknown = null): WindowInfo {
  const direct = Math.floor(Number(cfg?.contextWindowTokens) || 0);
  if (direct > 0) return { tokens: direct, source: WINDOW_SOURCES.config };
  const live = sessionOf(ctx, session);
  const measured = tokenMeterWindowOf(ctx, live);
  if (measured > 0) return { tokens: measured, source: WINDOW_SOURCES.tokenMeter };
  const routed = requestContextWindowOf(live);
  if (routed > 0) return { tokens: routed, source: WINDOW_SOURCES.requestContext };
  const { cc, hit } = policyOf(ctx, cfg);
  const win = Math.floor(Number(hit?.contextWindow ?? cc?.contextWindow) || 0);
  return win > 0 ? { tokens: win, source: WINDOW_SOURCES.compaction } : { tokens: 0, source: '' };
}

/**
 * 模型窗口（token）。返回 0 = 没有可靠来源，调用方别猜。
 * @param {object} ctx 插件 ctx（可为假/空）
 * @param {object} cfg 插件配置
 * @param {object|string|null} [session] 会话对象或 sessionId
 * @returns {number}
 */
export function windowTokensOf(ctx: TokenWindowCtx | null | undefined, cfg: TokenWindowConfig = {}, session: unknown = null): number {
  return windowInfoOf(ctx, cfg, session).tokens;
}

/** 进程内只打一次的窗口日志开关（ctx.logger 的 info/warn 都不进 journald，所以关键行同时写文件）。 */
let windowLogged = false;

/**
 * 打一次窗口日志（进程内只打一次），带来源。
 * 窗口未知且没有会话时先不打、也不标记，等真正算预算 / 第一条带会话的请求再来，
 * 免得重启那会儿把「未知」锁死；窗口未知但有会话且 logUnknown=false 时也不打。
 *
 * @param {object} ctx
 * @param {object} cfg
 * @param {object|string|null} [session]
 * @param {object} [opts]
 * @param {boolean} [opts.logUnknown] 窗口未知也打一条（调用方确实要用预算时）
 * @returns {number} 这次读到的窗口（0=未知/还没会话）
 */
export function logWindowOnce(ctx: TokenWindowCtx | null | undefined, cfg: TokenWindowConfig = {}, session: unknown = null, { logUnknown = false }: { logUnknown?: boolean } = {}): number {
  if (windowLogged) return 0;
  const live = sessionOf(ctx, session);
  const info = windowInfoOf(ctx, cfg, live);
  if (!(info.tokens > 0)) {
    // 还没有会话就先不打「未知」，等第一条带会话的请求/第一次算预算；有会话才认未知。
    if (!logUnknown || !live) return 0;
  }
  windowLogged = true;
  const line = info.tokens > 0
    ? `dsh-memory 拼 prompt 窗口：${info.tokens} token（来源 ${info.source}）`
    : 'dsh-memory 拼 prompt 窗口：未知（tokenMeter / session.requestContext / 宿主 compaction 都没给；不切批，按全塞）';
  // 本机 cordis LoggerService 只注册了内存环形缓冲 exporter：info 和 warn 都进不了
  // journald（app-boot 的 exporter 只在启动失败时兜底）。所以这行必须自己落文件。
  try {
    if (typeof ctx?.logger?.warn === 'function') ctx.logger.warn(line);
  } catch {
    /* ctx.logger 打不出来不影响干活 */
  }
  keyLog(cfg, line);
  return info.tokens;
}

/**
 * 给输出/system 预留多少 token：优先用宿主对该路由声明的请求层输出上限
 * （compaction 的 maxTokens），没有就按窗口比例留。
 * @returns {number}
 */
export function outputReserveOf(ctx: TokenWindowCtx | null | undefined, cfg: TokenWindowConfig = {}, windowTokens = 0, reserveRatio = DEFAULT_RESERVE_RATIO): number {
  const { cc, hit } = policyOf(ctx, cfg);
  const host = Math.floor(Number(hit?.maxTokens ?? cc?.maxTokens) || 0);
  const win = Number(windowTokens) || 0;
  if (host > 0) return Math.min(host, win > 0 ? win : host);
  const ratio = Number(reserveRatio) > 0 ? Number(reserveRatio) : DEFAULT_RESERVE_RATIO;
  return win > 0 ? Math.floor(win * ratio) : 0;
}

/**
 * 本次拼 prompt 能用多少 token。
 * @param {object} ctx
 * @param {object} cfg
 * @param {object} [opts]
 * @param {number} [opts.reserveRatio] 没拿到宿主输出上限时预留窗口的比例
 * @param {number} [opts.reserveTokens] 额外必须留出的 token（如本批 system + 待生成的 ops）
 * @param {object|string|null} [opts.session] 会话对象或 sessionId（窗口从 tokenMeter/route 取）
 * @returns {{window:number,reserve:number,budget:number}} budget=0 表示窗口未知（不切）
 */
export function promptBudgetOf(ctx: TokenWindowCtx | null | undefined, cfg: TokenWindowConfig = {}, { reserveRatio = DEFAULT_RESERVE_RATIO, reserveTokens = 0, session = null }: { reserveRatio?: number; reserveTokens?: number; session?: unknown } = {}): PromptBudget {
  const window = windowTokensOf(ctx, cfg, session);
  if (!(window > 0)) return { window: 0, reserve: 0, budget: 0 };
  const reserve = Math.max(
    Math.floor(Number(reserveTokens) || 0),
    outputReserveOf(ctx, cfg, window, reserveRatio),
  );
  return { window, reserve, budget: Math.max(0, window - reserve) };
}

/** 一条记忆/候选在 prompt 里的估算成本：正文 + id/type/tags/entities 那圈字段。 */
export function itemTokensOf<T>(item: T, textOf: (x: T) => unknown = (x) => (x as { text?: unknown } | null | undefined)?.text, overhead = 0): number {
  const text = typeof textOf === 'function' ? textOf(item) : item;
  return estimateTokens(text) + Math.max(0, Math.floor(Number(overhead) || 0));
}

/**
 * 按 token 装满一批：顺序扫，装得下就收，装不下跳过（继续试后面的短条目）。
 * budget<=0（窗口未知）时全收。
 *
 * @param {Array} items
 * @param {object} opts
 * @param {number} opts.budget 可用 token
 * @param [opts.textOf]
 * @param {number} [opts.overhead] 每条固定开销（字段/分隔符）
 * @returns {{picked:Array, omitted:Array, used:number}}
 */
export function packByTokens<T>(items: T[], { budget = 0, textOf = (x: T) => (x as { text?: unknown } | null | undefined)?.text, overhead = 0 }: { budget?: number; textOf?: (item: T) => unknown; overhead?: number } = {}): PackedTokens<T> {
  const list: T[] = Array.isArray(items) ? items : [];
  if (!(Number(budget) > 0)) return { picked: list.slice(), omitted: [], used: list.reduce((n, it) => n + itemTokensOf(it, textOf, overhead), 0) };
  const picked = [];
  const omitted = [];
  let used = 0;
  for (const item of list) {
    const cost = itemTokensOf(item, textOf, overhead);
    if (used + cost > Number(budget)) {
      omitted.push(item);
      continue;
    }
    picked.push(item);
    used += cost;
  }
  return { picked, omitted, used };
}

/**
 * 按 token 切批：顺序切，每批尽量塞满；单条就超预算的条目自己独占一批（不拆正文）。
 * budget<=0（窗口未知）时不切，整串一批。
 *
 * @param {Array} items
 * @param {object} opts
 * @param {number} opts.budget 每批可用 token
 * @param [opts.textOf]
 * @param {number} [opts.overhead] 每条固定开销
 * @returns {Array<Array>} 批次数组；空输入返回 []
 */
export function chunkByTokens<T>(items: T[], { budget = 0, textOf = (x: T) => (x as { text?: unknown } | null | undefined)?.text, overhead = 0 }: { budget?: number; textOf?: (item: T) => unknown; overhead?: number } = {}): T[][] {
  const list: T[] = Array.isArray(items) ? items : [];
  if (!list.length) return [];
  if (!(Number(budget) > 0)) return [list.slice()];
  const cap = Number(budget);
  const chunks = [];
  let cur = [];
  let used = 0;
  for (const item of list) {
    const cost = itemTokensOf(item, textOf, overhead);
    if (cur.length && used + cost > cap) {
      chunks.push(cur);
      cur = [];
      used = 0;
    }
    cur.push(item);
    used += cost;
  }
  if (cur.length) chunks.push(cur);
  return chunks;
}

export default {
  estimateTokens,
  sessionOf,
  windowInfoOf,
  windowTokensOf,
  logWindowOnce,
  outputReserveOf,
  promptBudgetOf,
  itemTokensOf,
  packByTokens,
  chunkByTokens,
};
