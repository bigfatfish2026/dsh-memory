/**
 * 整理节流状态（2026-11-14，阶段 4）。
 *
 * 原来 consolidate.js 里一张 state 对象 + 十几个闭包函数在改它：按 sessionId 分桶的
 * lastTokens/lastTokensAt/pendingTokens，外加 30 分钟预估节流的 estimateTriggerAt/estimateDone。
 * 这里收成一个对象，几条硬规矩写死在方法里：
 *   - 分桶按 sessionId：一个会话的请求/基线绝不动另一个会话（群聊基线被 c2c 打回零的事故不许重演）；
 *   - 冷启动、字段缺失、字段类型错（字符串/负数/NaN）一律容错成「没有」，不抛；
 *   - 节流时间戳只有 estimateTriggerAt 一个来源，别在调用方再存一份。
 */
export const SESSION_TOKEN_TTL_DAYS = 7;
export const SESSION_TOKEN_TTL_MS = SESSION_TOKEN_TTL_DAYS * 86400000;
/** 退回估算路径的硬节流窗口：同一进程最多这么久触发一次。 */
export const ESTIMATE_TRIGGER_MIN_INTERVAL_MS = 30 * 60 * 1000;

const NO_SESSION = '(no-session)';

/** 落盘 raw 的字段面：老文件可能是字符串/负数/坏结构，一律 unknown 进来再归一。 */
export interface RawCooldownState {
  lastAttemptAt?: unknown;
  lastRunAt?: unknown;
  lastSuccessAt?: unknown;
  runs?: unknown;
  saved?: unknown;
  lastTokens?: unknown;
  lastTokensAt?: unknown;
  pendingTokens?: unknown;
  estimateTriggerAt?: unknown;
  estimateDone?: unknown;
  [key: string]: unknown;
}

/** 归一化后的节流状态：分桶按 sessionId。 */
export interface CooldownState {
  lastAttemptAt: number;
  lastSuccessAt: number;
  runs: number;
  saved: number;
  lastTokens: Record<string, number>;
  lastTokensAt: Record<string, number>;
  pendingTokens: Record<string, number>;
  legacyLastTokens: number;
  estimateTriggerAt: number;
  estimateDone: boolean;
}

/** estimateBlockReason 的产物：'' 放行；'running' 上一轮没跑完；{seconds} 窗口内触发过。 */
export type EstimateBlockReason = '' | 'running' | { seconds: number };

/** createCooldownState 的注入面。 */
export interface CooldownStateOptions {
  raw?: RawCooldownState | null;
  persist?: ((state: CooldownState, reason: string) => void) | null;
  now?: () => number;
}

/** 把任意 raw（老文件/坏文件/字符串字段）归一成可用的状态；读不出来就当没有，不抛。 */
export function normalizeCooldownState(raw: RawCooldownState | null | undefined): CooldownState {
  const src: RawCooldownState = raw && typeof raw === 'object' ? raw : {};
  const attempt = Number(src.lastAttemptAt) || Number(src.lastRunAt) || 0;
  const lastTokens: Record<string, number> = {};
  const lastTokensAt: Record<string, number> = {};
  let legacyLastTokens = 0;
  const rawTokens = src.lastTokens;
  if (rawTokens && typeof rawTokens === 'object') {
    try {
      const rawAt: Record<string, unknown> = src.lastTokensAt && typeof src.lastTokensAt === 'object' ? src.lastTokensAt as Record<string, unknown> : {};
      for (const [sid, value] of Object.entries(rawTokens)) {
        const tokens = Number(value?.tokens ?? value);
        if (!sid || !Number.isFinite(tokens) || tokens <= 0) continue;
        lastTokens[sid] = tokens;
        const at = Number(value?.at ?? rawAt[sid]);
        lastTokensAt[sid] = Number.isFinite(at) && at > 0 ? at : 0;
      }
    } catch {
      // 结构坏了就当没有：宁可多整理一次，也不能启动时抛
    }
  } else {
    const legacy = Number(rawTokens);
    legacyLastTokens = Number.isFinite(legacy) && legacy > 0 ? legacy : 0;
  }
  const pendingTokens: Record<string, number> = {};
  if (src.pendingTokens && typeof src.pendingTokens === 'object') {
    for (const [sid, value] of Object.entries(src.pendingTokens)) {
      const tokens = Number(value);
      if (sid && Number.isFinite(tokens) && tokens > 0) pendingTokens[sid] = tokens;
    }
  }
  return {
    lastAttemptAt: attempt,
    lastSuccessAt: Number(src.lastSuccessAt) || 0,
    runs: Number(src.runs) || 0,
    saved: Number(src.saved) || 0,
    lastTokens,
    lastTokensAt,
    pendingTokens,
    legacyLastTokens,
    // 退回估算路径的节流账：老 state 没有这两个字段 = 从没触发过、上一轮已收尾
    estimateTriggerAt: Number(src.estimateTriggerAt) || 0,
    estimateDone: src.estimateDone !== false,
  };
}

/**
 * @param {object} spec
 * @param {object} [spec.raw] 上次落盘的状态（normalizeCooldownState 的输入）
 * @param {(state:object, reason:string)=>void} [spec.persist] 落盘回调（index.js 注入）
 * @param { => number} [spec.now]
 */
export function createCooldownState({ raw = null, persist = null, now = Date.now }: CooldownStateOptions = {}) {
  const state = normalizeCooldownState(raw);

  const keyOf = (sessionId: unknown): string => String(sessionId || '') || NO_SESSION;

  function baselineOf(key: string): number {
    if (!key) return 0;
    const tokens = Number(state.lastTokens?.[key]);
    return Number.isFinite(tokens) && tokens > 0 ? tokens : 0;
  }

  function setBaseline(key: string, tokens: unknown): void {
    if (!key) return;
    const value = Number(tokens);
    if (Number.isFinite(value) && value > 0) {
      state.lastTokens[key] = value;
      state.lastTokensAt[key] = now();
    } else {
      delete state.lastTokens[key];
      delete state.lastTokensAt[key];
    }
  }

  /** 老 state 只有一个数字 lastTokens：给第一条进来的会话认领一次，认完清零。 */
  function adoptLegacyBaseline(key: string): void {
    const legacy = Number(state.legacyLastTokens) || 0;
    if (!(legacy > 0)) return;
    if (key && !(baselineOf(key) > 0)) {
      state.lastTokens[key] = legacy;
      state.lastTokensAt[key] = now();
    }
    state.legacyLastTokens = 0;
  }

  /** 超过 TTL 没更新的会话格删掉；老格没时间戳就补现在，给它一个完整 TTL。 */
  function prune(): number {
    const t = now();
    let dropped = 0;
    for (const key of Object.keys(state.lastTokens || {})) {
      const at = Number(state.lastTokensAt?.[key]) || 0;
      if (!(at > 0)) {
        state.lastTokensAt[key] = t;
        continue;
      }
      if (t - at > SESSION_TOKEN_TTL_MS) {
        delete state.lastTokens[key];
        delete state.lastTokensAt[key];
        delete state.pendingTokens[key];
        dropped += 1;
      }
    }
    return dropped;
  }

  const pendingOf = (key: string): number => (key && Number(state.pendingTokens?.[key]) > 0 ? Number(state.pendingTokens[key]) : 0);
  const setPending = (key: string, tokens: unknown): void => {
    if (!key) return;
    const value = Number(tokens);
    if (Number.isFinite(value) && value > 0) state.pendingTokens[key] = value;
    else delete state.pendingTokens[key];
  };
  const clearPending = (key: string): void => { if (key) delete state.pendingTokens[key]; };

  const touchAttempt = (t = now()) => { state.lastAttemptAt = t; };
  const touchSuccess = (t = now()) => { state.lastSuccessAt = t; };
  const addRun = (n = 1) => { state.runs = (Number(state.runs) || 0) + n; };
  const addSaved = (n = 1) => { state.saved = (Number(state.saved) || 0) + n; };

  /** 预估节流：上一轮已收尾、且距上次触发超过窗口才允许再触发。 */
  function estimateDue(t = now()): boolean {
    if (state.estimateDone === false) return false;
    const lastAt = Number(state.estimateTriggerAt) || 0;
    return !(t - lastAt < ESTIMATE_TRIGGER_MIN_INTERVAL_MS);
  }

  /** 节流判定 + 原因：'' 放行；'running' 上一轮没跑完；{seconds} 窗口内触发过。 */
  function estimateBlockReason(t = now()): EstimateBlockReason {
    const lastAt = Number(state.estimateTriggerAt) || 0;
    if (state.estimateDone === false && t - lastAt < ESTIMATE_TRIGGER_MIN_INTERVAL_MS) return 'running';
    if (lastAt > 0 && t - lastAt < ESTIMATE_TRIGGER_MIN_INTERVAL_MS) return { seconds: Math.round((t - lastAt) / 1000) };
    return '';
  }

  function markEstimate(t = now()) {
    state.estimateTriggerAt = t;
    state.estimateDone = false;
  }

  function markEstimateDone() {
    state.estimateDone = true;
  }

  function save(reason = ''): CooldownState {
    if (typeof persist !== 'function') return state;
    try {
      persist(state, reason);
    } catch {
      /* 落盘失败只当没写：判定照旧在本进程里生效 */
    }
    return state;
  }

  return {
    keyOf,
    get: () => state,
    baselineOf,
    setBaseline,
    adoptLegacyBaseline,
    prune,
    pendingOf,
    setPending,
    clearPending,
    touchAttempt,
    touchSuccess,
    addRun,
    addSaved,
    estimateDue,
    estimateBlockReason,
    markEstimate,
    markEstimateDone,
    save,
  };
}
