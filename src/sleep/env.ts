/**
 * 睡觉链的运行上下文：ctx/store/cfg/stateStore 与「怎么算预算/怎么取快照」这些函数句柄。
 *
 * 状态归属：轮级可变状态 inFlight / roundSnapshot /
 * mergeBatchCache / stageLogged 归 SlotScheduler，env 上不留副本；子模块不许各持一份。
 */
import { createBatchPlanner, cosine } from '../batch-planner.js';
import { runNotify } from '../shared/notify.js';
import { batchMaxCos } from '../cluster.js';
import { foldForMatch } from '../shared/entity-normalize.js';
import {
  estimateTokens, promptBudgetOf, logWindowOnce, packByTokens,
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
  /**
   * 工具循环的累计上下文预算（2026-11-23）：同一次 askModel 里历史工具结果累计超过
   * tokens 后，旧结果只留带出处的相关片段。schema 与默认值在 src/index.ts；这里只列键。
   */
  sleepToolBudget?: {
    enabled?: unknown;
    tokens?: unknown;
    keepRecentRounds?: unknown;
    excerptChars?: unknown;
    maxExcerptLines?: unknown;
    [key: string]: unknown;
  };
  /** 定向查证的轮数提示（2026-11-23）：关掉 = 不注入提示，消息体与改前逐字节一致。 */
  sleepToolCheck?: {
    enabled?: unknown;
    rounds?: unknown;
    maxCallsPerRound?: unknown;
    [key: string]: unknown;
  };
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
  /** 冲突预判（decision 桥）：默认关，关着时 runConflictCheck 与加这层之前一致。 */
  conflictDecide?: {
    enabled?: unknown;
    batchSize?: unknown;
    minConfidence?: unknown;
    timeoutMs?: unknown;
    baseUrl?: unknown;
    model?: unknown;
    [key: string]: unknown;
  };
  entityTidy?: { apply?: unknown; [key: string]: unknown };
  pipeline?: {
    lintReview?: boolean;
    lintReviewTimeoutMs?: number;
    textLint?: boolean;
    ingestAck?: boolean;
    agentPipeline?: boolean;
    writeGate?: boolean;
    /** 段级信号是否硬拒（默认 false = 只记）；阶段二才开。 */
    splitSignalsStrict?: boolean;
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

/**
 * 本批候选上限：只决定「这一份 prompt 最多带多少条记忆」，不是 token 预算。
 * token 上限仍由 memoryBudgetTokens()（真实窗口）算，两者取先撞上的那个。
 */
const MEMORY_CANDIDATE_CAP = 300;

/** 兜底窗口：候选没占满上限时只补最近这几天的条目，不再退回「把整库铺满」。 */
const MEMORY_RECENT_FALLBACK_DAYS = 7;

/** 一组向量的质心：缺向量、维度不一致的丢掉；一条都不可用返回 null。 */
function centroidOf(vecs: Array<Float32Array | undefined>): Float32Array | null {
  let dim = 0;
  const usable: Float32Array[] = [];
  for (const v of vecs) {
    if (!v?.length) continue;
    if (!dim) dim = v.length;
    if (v.length === dim) usable.push(v);
  }
  if (!usable.length) return null;
  const out = new Float32Array(dim);
  for (const v of usable) for (let i = 0; i < dim; i += 1) out[i] += v[i];
  return out;
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

  /**
   * 本批现有记忆：实体字面命中 + 向量邻域取候选，剩余名额给最近条目兜底，再按窗口预算装。
   *
   * 2026-11-20：以前按 token 预算把整库等距铺满（一轮 47 个文件各拼一份，sleep:askModel
   * 单次输入 81.5 万 token，其中 listing 74 万）。现在先筛后装：
   *   - 候选：本批记录里字面出现的实体（foldForMatch 折叠后比较）命中的条目，加上这些条目
   *     的向量邻域（命中条目向量取质心算余弦），最多 MEMORY_CANDIDATE_CAP 条；
   *   - 兜底：候选没占满上限时，按 ts 倒序补最近 MEMORY_RECENT_FALLBACK_DAYS 天的条目，
   *     防止候选为空时 prompt 里一条现有记忆都没有；
   *   - 上限仍走 memoryBudgetTokens()（token-window 的真实窗口），这里不另设条数/字符预算。
   *
   * 跨主题的重复在这一层看不见：不在这里补，阶段二 organizePass（src/sleep/passes.ts:192）
   * 会把全库聚成小批逐批问模型，去重是那一层的活。
   */
  function memoryTexts(transcript = ''): MemoryView[] {
    const budget = memoryBudgetTokens(transcript);
    const snap = activeSnap();
    const listed = snap.list;
    const folded = foldForMatch(transcript);
    // 同一个实体名在库里成百上千条：折叠 + 判命中各算一次，后面的条目直接查表
    const hitCache = new Map<string, boolean>();
    const hitOf = (e: unknown): boolean => {
      const raw = String(e);
      let got = hitCache.get(raw);
      if (got === undefined) {
        const f = foldForMatch(raw);
        got = f.length >= 2 && folded.includes(f);
        hitCache.set(raw, got);
      }
      return got;
    };

    // 1) 实体字面命中：命中实体多的排前面，同分按时间倒序
    const entityHits = listed
      .map((m) => ({ m, hits: (m.entities || []).reduce((n, e) => n + (hitOf(e) ? 1 : 0), 0) }))
      .filter((x) => x.hits > 0)
      .sort((a, b) => b.hits - a.hits || String(b.m.ts || '').localeCompare(String(a.m.ts || '')));

    const picked: MemoryView[] = [];
    const seen = new Set<string>();
    const take = (m: MemoryView | undefined): boolean => {
      if (!m || seen.has(m.id)) return true;
      if (picked.length >= MEMORY_CANDIDATE_CAP) return false;
      seen.add(m.id);
      picked.push(m);
      return true;
    };
    for (const x of entityHits) if (!take(x.m)) break;
    const afterHits = picked.length;

    // 2) 向量邻域：命中条目的向量质心，按余弦把同主题的近邻补满候选
    const centroid = centroidOf(entityHits.map((x) => snap.vecOf.get(x.m.id)));
    if (centroid && picked.length < MEMORY_CANDIDATE_CAP) {
      const near = listed
        .filter((m) => !seen.has(m.id))
        .map((m) => {
          const v = snap.vecOf.get(m.id);
          return { m, cos: v ? cosine(v, centroid) : null };
        })
        .filter((x): x is { m: MemoryView; cos: number } => x.cos !== null && x.cos > 0)
        .sort((a, b) => b.cos - a.cos);
      for (const x of near) if (!take(x.m)) break;
    }
    const afterVec = picked.length;

    // 3) 兜底：候选没占满上限时补最近几天的条目（候选空时至少不让 prompt 空转）
    if (picked.length < MEMORY_CANDIDATE_CAP) {
      const recentCut = Date.now() - MEMORY_RECENT_FALLBACK_DAYS * 86400000;
      const recent = listed
        .filter((m) => {
          const t = Date.parse(String(m.ts || ''));
          return Number.isFinite(t) && t >= recentCut;
        })
        .sort((a, b) => String(b.ts || '').localeCompare(String(a.ts || '')));
      for (const m of recent) if (!take(m)) break;
    }

    // 4) 按窗口预算装：窗口未知（Infinity）时整份候选都带，条数已被上限压住；
    //    窗口已知但一条都装不下时交空表（旧行为：预算 0 什么都取不到）
    let packed: MemoryView[];
    if (!Number.isFinite(budget)) packed = picked;
    else if (budget <= 0) packed = [];
    else packed = packByTokens(picked, {
      budget,
      textOf: (m) => String(m.text || ''),
      overhead: MEMORY_ITEM_OVERHEAD_TOKENS,
    }).picked;

    log(
      cfg,
      `[sleep] 现有记忆候选 实体命中=${afterHits} 向量邻域=${afterVec - afterHits}`
        + ` 兜底=${picked.length - afterVec} 传入=${packed.length} 上限=${MEMORY_CANDIDATE_CAP}`
        + ` 预算=${Number.isFinite(budget) ? Math.round(budget) : '未知'}`,
    );
    return packed;
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
