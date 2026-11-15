/**
 * 睡觉链的运行上下文：ctx/store/cfg/stateStore 与「怎么算预算/怎么取快照」这些函数句柄。
 *
 * 状态归属：轮级可变状态 inFlight / roundSnapshot /
 * mergeBatchCache / stageLogged 归 SlotScheduler，env 上不留副本；子模块不许各持一份。
 */
import { createBatchPlanner } from '../batch-planner.js';
import { runNotify } from '../shared/notify.js';
import { batchMaxCos } from '../cluster.js';
import {
  estimateTokens, promptBudgetOf, itemTokensOf, logWindowOnce,
} from '../token-window.js';
import { stateOf } from './state-access.js';
import { snapshot } from './backup.js';
import { sleepOwnerOf } from './slots.js';
import { log } from './log.js';
import {
  batchFingerprint, splitAgentBatches, MEMORY_ITEM_OVERHEAD_TOKENS, SLEEP_PROMPT_SHELL_TOKENS,
} from './batching.js';
import { buildSleepClusters } from './organize.js';
import type { DatabaseSync } from 'node:sqlite';
import type { BatchPlanner, MemoryClusterItem } from '../batch-planner.js';
import type { PromptBudget } from '../token-window.js';
import type { MemoryRecord, MemoryView, SnapshotView } from '../store/types.js';
import type { VecView } from '../cluster.js';
import type { SleepStateStore } from '../sleep-state.js';
import type { createScheduler } from './scheduler.js';
import type { createLint } from './lint.js';
import type { createModelCalls } from './model-calls.js';
import type { createActions } from './actions.js';
import type { createLedgerGlue } from './ledger-glue.js';
import type { createAgentOps } from './agent-ops.js';
import type { createReplay } from './replay.js';
import type { createOrganize } from './organize.js';
import type { createOrganizeAgent } from './organize-agent.js';
import type { createIngest } from './ingest.js';
import type { createPool } from './pool.js';
import type { createPoolPass } from './pool-pass.js';
import type { createPasses } from './passes.js';

/** 一个整理时段（cfg.schedule 的成员）。 */
export interface SleepScheduleSlot {
  hour?: unknown;
  minute?: unknown;
  [key: string]: unknown;
}

/**
 * 睡觉链认的配置面：已知字段给类型，其余原样透传（上层还会加配置项）。
 * 必填项都是 schemastery 里有默认值的（index.ts 的 schema），这里不重复造默认值。
 */
export interface SleepCfg {
  storePath: string;
  notifyScript: string;
  sessionsRoot: string;
  archiveDir: string;
  backupDir: string;
  provider?: string;
  model?: string;
  reasoningEffort?: unknown;
  sleepLogFile?: string | null;
  sleepEnabled?: unknown;
  sleepPriceProbe?: unknown;
  priceClockFile?: string;
  sleepTools?: unknown;
  sleepSplitEnabled?: unknown;
  sleepDeferInPeak?: unknown;
  sleepSummaryNotify?: unknown;
  schedule?: unknown;
  hour?: unknown;
  minute?: unknown;
  checkIntervalMs?: number;
  selfTestMarker?: string;
  archiveNotify?: unknown;
  splitMaxChars?: unknown;
  organizeFunnelMode?: unknown;
  organizePipelineMode?: unknown;
  organizeBatchPackSize?: unknown;
  organizeBatchPackMinSpanDays?: unknown;
  organizeBatchPackMaxSpanDays?: unknown;
  clusterQuestionMin?: unknown;
  funnelCheapMin?: unknown;
  funnelVectorCeil?: unknown;
  funnelMinPairCos?: unknown;
  funnelMinAvgCos?: unknown;
  funnelTimeWindowDays?: unknown;
  funnelMaxBatches?: unknown;
  conflictCheckEnabled?: unknown;
  entityTidy?: { apply?: unknown; [key: string]: unknown };
  pipeline?: {
    lintReview?: boolean;
    lintReviewTimeoutMs?: number;
    textLint?: boolean;
    ingestAck?: boolean;
    agentPipeline?: boolean;
    writeGate?: boolean;
    [key: string]: unknown;
  };
  [key: string]: unknown;
}

/** 宿主会话列表面：只认 id，其余原样透传。 */
export interface SleepSessionInfo {
  id?: string;
  [key: string]: unknown;
}

/** 宿主工具表里 chat_recall 的最小面。 */
export interface SleepToolHit {
  when?: string;
  text?: string;
  [key: string]: unknown;
}

/** 宿主 sessions 服务的最小面：只认 list()。 */
export interface SleepSessions {
  list?: () => SleepSessionInfo[];
  [key: string]: unknown;
}

/** 宿主 ctx 的最小面：取服务、打日志；桩 ctx 没有字段也不该炸。 */
export interface SleepCtx {
  get: (name: string) => SleepSessions;
  provide: (name: string, service: unknown) => unknown;
  logger?: { warn?: (line: string) => void; info?: (line: string) => void };
  tools?: {
    get?: (name: string) => {
      execute: (input: Record<string, unknown>) => Promise<{ hits?: SleepToolHit[] }> | { hits?: SleepToolHit[] };
    } | undefined;
  };
  [key: string]: unknown;
}

/** store 的最小面：睡觉链用到的读写与向量入口。 */
export interface SleepStore {
  db: () => DatabaseSync;
  listAll: () => MemoryView[];
  snapshot?: () => SnapshotView;
  vectorIndex?: () => Map<string, Float32Array>;
  backupTo?: (dst: string) => Promise<unknown>;
  typeOf: (m: MemoryView) => string;
  save: (text: unknown, tags: unknown, source: string | undefined, meta?: Record<string, unknown>) => Promise<unknown>;
  update: (id: string, patch?: Record<string, unknown>) => Promise<unknown>;
  archive: (id: string, reason?: string) => Promise<unknown>;
  merge: (ids: unknown, text: unknown, tags: unknown, meta?: Record<string, unknown>) => Promise<unknown>;
  recall?: (query: unknown, k: unknown, opts?: { caller?: string; session?: unknown; origin?: unknown; [key: string]: unknown }) => Promise<Array<Record<string, unknown>>>;
  gateStats?: () => { llm?: unknown; [key: string]: unknown };
  [key: string]: unknown;
}

/** 核心句柄：createSleepEnv 自己造的那部分。 */
export interface SleepEnvCore {
  ctx: SleepCtx;
  store: SleepStore;
  cfg: SleepCfg;
  stateStore: SleepStateStore;
  state: { lastRunAt: number; runs: number };
  sleepOwner: string;
  log: (line: string) => void;
  snapshot: (cfg: SleepCfg, s?: SleepStore) => Promise<string>;
  activeSnap: () => SnapshotView;
  funnelModeOf: () => string;
  batchPlannerOf: (budgetTokens?: number) => BatchPlanner;
  windowSessionOf: () => SleepSessionInfo | null;
  budgetOf: () => PromptBudget;
  push: (msg: string) => void;
  heartbeat: () => number;
  memoryBudgetTokens: (transcript?: string) => number;
  memoryTexts: (transcript?: string) => MemoryView[];
}

/** 装配完的 env：各段由 setupSleep 挂上，本文件只造核心句柄。 */
export interface SleepEnv extends SleepEnvCore {
  scheduler: ReturnType<typeof createScheduler>;
  lint: ReturnType<typeof createLint>;
  model: ReturnType<typeof createModelCalls>;
  actions: ReturnType<typeof createActions>;
  ledger: ReturnType<typeof createLedgerGlue>;
  agentOps: ReturnType<typeof createAgentOps>;
  replay: ReturnType<typeof createReplay>;
  organize: ReturnType<typeof createOrganize>;
  organizeAgent: ReturnType<typeof createOrganizeAgent>;
  ingest: ReturnType<typeof createIngest>;
  pool: ReturnType<typeof createPool>;
  poolPass: ReturnType<typeof createPoolPass>;
  passes: ReturnType<typeof createPasses>;
}

export function createSleepEnv(ctx: SleepCtx, store: SleepStore, cfg: SleepCfg): SleepEnvCore {
  const stateStore = stateOf(cfg);
  // 轮级可变状态（inFlight / roundSnapshot / mergeBatchCache / stageLogged）不在这里：
  // 2026-11-15 起归 SlotScheduler，env 只放不可变句柄与函数。
  const env = {
    ctx,
    store,
    cfg,
    stateStore,
    state: { lastRunAt: 0, runs: 0 },
    sleepOwner: sleepOwnerOf(),
    log: (line: string) => log(cfg, line),
    snapshot: (_cfg: SleepCfg, s?: SleepStore) => snapshot(cfg, s || store),
  };

  /** 活跃视图：优先 store.snapshot()；假 store / 老接口退回 listAll + vectorIndex。 */
  function activeSnap(): SnapshotView {
    if (typeof store.snapshot === 'function') return store.snapshot();
    const list = store.listAll().filter((m) => (m.status || 'active') === 'active');
    return {
      list,
      byId: new Map(list.map((m) => [String(m.id), m])),
      vecOf: typeof store.vectorIndex === 'function' ? store.vectorIndex() : new Map(),
    } as unknown as SnapshotView;
  }

  /** 候选漏斗开关：只认 off/l0/l1/l2/on，写错按 off（不偷偷改行为）。 */
  function funnelModeOf(): string {
    const m = String(cfg.organizeFunnelMode || 'off').toLowerCase();
    return ['off', 'l0', 'l1', 'l2', 'on'].includes(m) ? m : 'off';
  }

  /** 三处「聚类→切批→查缓存→挑批」共用这一个 planner。 */
  function batchPlannerOf(budgetTokens?: number): BatchPlanner {
    return createBatchPlanner({
      mode: funnelModeOf(),
      clusterOf: (items: MemoryClusterItem[], vecOf) => buildSleepClusters(cfg, items, vecOf),
      splitBatches: (opts) => splitAgentBatches(opts),
      fingerprint: (cluster) => batchFingerprint(cluster),
      batchMaxCos: (cluster, vecOf) => batchMaxCos(cluster as unknown as MemoryRecord[], vecOf as unknown as VecView),
      budgetTokens,
      minScore: Number(cfg.clusterQuestionMin) || 0.78,
      cheapMin: Number(cfg.funnelCheapMin ?? 0.3),
      vectorCeil: Number(cfg.funnelVectorCeil ?? 0.85),
      minPairCos: Number(cfg.funnelMinPairCos ?? 0.78),
      minAvgCos: Number(cfg.funnelMinAvgCos ?? 0.8),
      timeWindowDays: Number(cfg.funnelTimeWindowDays ?? 180),
      maxBatches: Number(cfg.funnelMaxBatches ?? 0),
      packSize: Number(cfg.organizeBatchPackSize ?? 0),
      packMinSpanDays: Number(cfg.organizeBatchPackMinSpanDays ?? 1),
      packMaxSpanDays: Number(cfg.organizeBatchPackMaxSpanDays ?? 30),
      log: (line) => log(cfg, line),
    });
  }

  function windowSessionOf(): SleepSessionInfo | null {
    try {
      const live = ctx.get('sessions')?.list?.();
      if (!Array.isArray(live) || live.length === 0) return null;
      return live.find((s) => String(s?.id || '').startsWith('session-')) || live[live.length - 1] || null;
    } catch {
      return null;
    }
  }

  /** 所有按窗口算预算的地方都走这里：顺手把窗口日志（warn，进程内一次）打出来。 */
  function budgetOf(): PromptBudget {
    const session = windowSessionOf();
    logWindowOnce(ctx, cfg, session, { logUnknown: true });
    return promptBudgetOf(ctx, cfg, { session });
  }

  function push(msg: string): void {
    // 2026-11-16：睡觉链这处同一个洞（同步 try 抓不到异步 ENOENT），一起收给 runNotify。
    runNotify({ script: cfg.notifyScript, args: [msg], label: 'sleep', log: (line) => log(cfg, line) });
  }

  /**
   * 心跳续租：只有本进程写下的 runningAt 才续，不覆盖别的实例的锁。
   * 顺手把缺的 owner 补上（老 state 跑起来后也有主了）。
   */
  function heartbeat(): number {
    try {
      const fresh = stateStore.load();
      if (!Number(fresh.runningAt)) return Date.now();
      stateStore.patch({ heartbeatAt: Date.now(), owner: fresh.owner || env.sleepOwner });
    } catch {
      /* 状态文件写不进不影响干活 */
    }
    return Date.now();
  }

  /** 按 token 算现有记忆能塞多少：窗口 − 输出预留 − 本次会话记录。 */
  function memoryBudgetTokens(transcript = ''): number {
    const { window, budget } = budgetOf();
    if (!window) return Infinity;
    const shell = estimateTokens(transcript) + SLEEP_PROMPT_SHELL_TOKENS;
    return Math.max(0, budget - shell);
  }

  function memoryTexts(transcript = ''): MemoryView[] {
    const budget = memoryBudgetTokens(transcript);
    const costOf = (m: MemoryView) => itemTokensOf(m, (x) => String(x?.text || ''), MEMORY_ITEM_OVERHEAD_TOKENS);
    const all = store.listAll().filter((m) => (m.status || 'active') === 'active');
    const lower = String(transcript || '').toLowerCase();
    const relevant = (m: MemoryView) =>
      (m.entities || []).some((e) => String(e).length >= 2 && lower.includes(String(e).toLowerCase()));
    const picked: MemoryView[] = [];
    const seen = new Set<string>();
    let used = 0;
    const take = (m: MemoryView) => {
      if (!m || seen.has(m.id)) return false;
      const cost = costOf(m);
      if (used + cost > budget) return false;
      seen.add(m.id);
      picked.push(m);
      used += cost;
      return true;
    };

    // 1) 跟本轮会话实体相关的先占位
    for (const m of all) if (relevant(m)) take(m);

    // 2) 其余按时间等距抽样，铺满剩余 token
    const rest = all.filter((m) => !seen.has(m.id));
    if (rest.length) {
      const room = budget - used;
      const avg = rest.reduce((s, m) => s + costOf(m), 0) / rest.length;
      const want = Math.min(rest.length, Math.max(0, Math.floor(room / Math.max(avg, MEMORY_ITEM_OVERHEAD_TOKENS))));
      if (want >= rest.length) {
        for (const m of rest) take(m);
      } else if (want > 0) {
        const step = rest.length / want;
        for (let k = 0; k < want; k += 1) take(rest[Math.floor(k * step)]);
      }
      // 3) 还有余量就从最近往前补
      for (let i = rest.length - 1; i >= 0 && used < budget; i -= 1) take(rest[i]);
    }
    return picked;
  }

  // 段位（scheduler / lint / …）由 setupSleep 按依赖顺序挂上；这里先按最终形状交出去。
  return Object.assign(env, {
    activeSnap,
    funnelModeOf,
    batchPlannerOf,
    windowSessionOf,
    budgetOf,
    push,
    heartbeat,
    memoryBudgetTokens,
    memoryTexts,
  });
}
