/**
 * dsh-memory — 长期记忆（存储 + 检索 + 压缩前整理 + 夜间睡觉）
 *
 * 模块分工：
 *   src/store.ts       存储与检索（memory_save / memory_recall / memory_list / memory_archive / memory_merge）
 *   src/consolidate.ts 上下文逼近压缩线时整理（memory_consolidate）
 *   src/sleep.ts       每天定时整理与 /new 前的会话存档（memory_sleep）
 *   src/cluster.ts     睡觉第二阶段用的无模型聚类
 *   src/split-pass.ts  把超长条目拆成原子条目（memory_split）
 *   src/prefill.ts     每轮自动回忆注入（system-prompt/assemble 钩子）
 *
 * 存储全在 memory.db：memories 表（正文）+ sqlite-vec（向量），嵌入走远端 BGE-M3。
 * 2026-11-05 起 memories.jsonl 退出运行时，只在库里还没迁移时当一次性导入源。
 */
import fs from 'node:fs';
import path from 'node:path';
import Schema from '@deepseek-ai/schemastery';
import { defineTool } from '@deepseek-ai/dsh-tools';
import { createStore, registerStoreTools } from './store.js';
import { setupConsolidate } from './consolidate.js';
import { setupContextWatch } from './context-watch.js';
import { setupSleep } from './sleep.js';
import { selectCurateConcurrency } from './concurrency.js';
import * as paths from './shared/paths.js';
import { setupAutoPrefill } from './prefill.js';
import { setupEntityQueue } from './entity-worker.js';
import { entityGraphOf } from './entity-graph.js';
import { assertMayWrite } from './write-guard.js';
import { llmOf } from './llm-service.js';
import type { ToolDefinition } from '@deepseek-ai/dsh-tools';
import type { MemoryStore } from './store/memory-store.js';
import type { StoreConfig } from './store/types.js';
import type { ToolContextLike } from './store/tools.js';
import type { ContextWatchCtx } from './context-watch.js';
import type { ConsolidateCfg, ConsolidateCtx } from './consolidate.js';
import type { PrefillCfg, PrefillCtx, PrefillStore } from './prefill.js';
import type { SleepStore } from './sleep/env.js';
import type { EntityWorkerConfig } from './entity-worker.js';
import type { SplitPassOpts } from './split-pass.js';
import type { ConcurrencyConfig } from './concurrency.js';

/** 插件入口在装配处读到的配置面（schemastery Config 解析后的形状，只列本文件读的键）。 */
export interface MemoryConfig {
  storePath?: string;
  stateFile?: string;
  consolidateStateFile?: string;
  splitPreviewFile?: string;
  /** 管道开关（启动快照读四个布尔 + 阈值）。 */
  pipeline?: {
    ingestAck?: boolean;
    agentPipeline?: boolean;
    writeGate?: boolean;
    writeGateThreshold?: number;
    textLint?: boolean;
    lintReview?: boolean;
    lintReviewTimeoutMs?: number;
    /** 段级信号是否硬拒（默认 false = 只记）；阶段二才开。 */
    splitSignalsStrict?: boolean;
    [key: string]: unknown;
  };
  /** 冲突预判（decision 桥）配置；schema 在下方，默认关。 */
  conflictDecide?: {
    enabled?: boolean;
    batchSize?: number;
    minConfidence?: number;
    timeoutMs?: number;
    baseUrl?: string;
    model?: string;
    [key: string]: unknown;
  };
  /** 工具循环累计上下文预算（2026-11-23）；schema 在下方，默认 4000 token。 */
  sleepToolBudget?: {
    enabled?: boolean;
    tokens?: number;
    keepRecentRounds?: number;
    excerptChars?: number;
    maxExcerptLines?: number;
    [key: string]: unknown;
  };
  /** 定向查证提示（2026-11-23）；关掉 = 不注入提示，消息体与改前逐字节一致。 */
  sleepToolCheck?: {
    enabled?: boolean;
    rounds?: number;
    maxCallsPerRound?: number;
    [key: string]: unknown;
  };
  /** 实体队列（apply 里打启动行读这几个键）。 */
  entityQueue?: {
    enqueue?: boolean;
    consume?: boolean;
    tickMs?: number;
    batch?: number;
    concurrency?: number;
    leaseMs?: number;
    maxBatchesPerTick?: number;
    genericDf?: number;
    genericDfMin?: number;
    coarseMaxWords?: number;
    coarseBudgetMs?: number;
    reasoningEffort?: string;
    logFile?: string;
    [key: string]: unknown;
  };
  /**
   * 实体待观察（observation）登记与夜间提炼（docs/observation-plan-v2.md §4/§6/§8）。
   * enabled 默认 false：写路径不写 observe_task、夜间也不跑提炼，行为与加这项之前一致。
   */
  observe?: {
    enabled?: boolean;
    dryRun?: boolean;
    maxEntities?: number;
    maxInput?: number;
    [key: string]: unknown;
  };
  [key: string]: unknown;
}

/** 宿主插件上下文里本文件直接读到的面（其余段用自己的窄类型接，装配处按需断言）。 */
export interface MemoryPluginContext {
  logger?: { info?: (msg: string) => void; warn?: (line: string) => void };
  effect: (fn: () => (() => void) | void) => unknown;
  tools: { register: (definition: ToolDefinition) => unknown };
  provide?: (name: string, service: unknown) => unknown;
  [key: string]: unknown;
}

export const name = 'dsh-memory';
export const inject = ['llm', 'tools'];

export const Config: Schema<MemoryConfig> = Schema.object({
  // ── 存储与检索 ──
  apiUrl: Schema.string().default(''),
  storePath: Schema.string().default(paths.memoryRoot()),
  topK: Schema.number().step(1).min(1).default(5),
  minScore: Schema.number().default(0.45),
  relativeGap: Schema.number().default(0.1),
  timeoutMs: Schema.number().step(1).min(1).default(30000),
  // 检索融合（RRF）与 opinion 置信度步长，照 Hindsight 的做法
  rrfK: Schema.number().step(1).min(1).default(60),
  // 实体通道权重：实体匹配噪声大，1 太吃重，默认 0.65
  rankWeightEntity: Schema.number().min(0).max(3).default(0.65),
  opinionAlpha: Schema.number().min(0.01).max(0.5).default(0.1),

  // ── 实体队列（2026-11-13，docs/dsh-memory-save-queue-2026.md §2.4）──
  /**
   * memory_save 删掉 entities 参数之后，实体走两条路：
   *   ① 写入时零成本粗匹配（src/entity-coarse.ts，永远开着，事务内落 origin='rule'）；
   *   ② 写入后入队 entity_extract（curate_queue），10 分钟一 tick 由模型补（origin='agent'）。
   * consume 默认 false 是灰度：先只看队列增速、不烧模型调用；确认 dedupe_key 真去重了再开。
   */
  entityQueue: Schema
    .object({
      /** 入队开关：关了写入只做粗实体，不排队列。 */
      enqueue: Schema.boolean().default(true),
      /** 消费开关（默认关）：10 分钟一 tick，只在主会话进程里跑。 */
      consume: Schema.boolean().default(false),
      tickMs: Schema.number().step(1000).min(60000).default(600000),
      /** 单批几条记忆进一次模型。 */
      batch: Schema.number().step(1).min(1).max(32).default(8),
      /**
       * 并发几批：0 = 跟全链自适应（推荐，2026-11-14 起默认 0）；
       * 填了就是这条通道的手工上限，只许往下压，不再写死 2/4。
       */
      concurrency: Schema.number().step(1).min(0).max(64).default(0),
      /** 租约（ms）：过期自动回 ready。 */
      leaseMs: Schema.number().step(1000).min(30000).default(180000),
      /** 一 tick 最多几批。 */
      maxBatchesPerTick: Schema.number().step(1).min(1).max(20).default(3),
      /** 泛词闸：norm 在 memory_entity 的出现次数 / active 记忆数 > 此值就不落库。 */
      genericDf: Schema.number().min(0).max(1).default(0.05),
      /** 泛词闸的最小出现次数：df 不到这个数不算泛词。 */
      genericDfMin: Schema.number().step(1).min(1).default(20),
      /** 粗实体词典条数上限（超预算时代码自己降到 1000）。 */
      coarseMaxWords: Schema.number().step(1).min(100).default(4000),
      /** 单条粗实体耗时预算（ms），连续超就降词表。 */
      coarseBudgetMs: Schema.number().min(0.1).max(50).default(3),
      // 2026-11-14：maxTokens 配置口子删了，请求里永不出现这个字段。
      /**
       * 抽取调用的思考档位：空 = 不传，由 provider 用自己的默认。
       * 不写白名单：不同路由支持的档位不一样，magpie/group/ds 连 'off' 都不认，
       * 写死任何取值都会把整批打成 dead（UNSUPPORTED_REASONING_EFFORT）。
       * 填了也会先跟 llm.resolveModelInfo 对一遍，不支持就退化成不传。
       */
      reasoningEffort: Schema.string().default(''),
      logFile: Schema.string().default(paths.logFile('memory_entity.log')),
    })
    .default({
      enqueue: true,
      consume: false,
      tickMs: 600000,
      batch: 8,
      concurrency: 0,
      leaseMs: 180000,
      maxBatchesPerTick: 3,
      genericDf: 0.05,
      genericDfMin: 20,
      coarseMaxWords: 4000,
      coarseBudgetMs: 3,
      reasoningEffort: '',
      logFile: paths.logFile('memory_entity.log'),
    }),

  // ── 实体待观察（observation）登记与夜间提炼（2026-11-21，docs/observation-plan-v2.md §4/§6/§8）──
  /**
   * enabled：写路径（新建 / 正文改写 / 合并 / 归档 / 拆分 / 挂载晚到）是否把受影响实体登记进
   * observe_task，以及夜间整理收尾是否跑 observation 提炼（src/sleep/observation-pass.ts）。
   * 默认 false —— 关着时一行都不写、一次模型都不调，行为与加这项之前一致；开着但老库没跑
   * scripts/apply-observe-schema.mjs 时队列/提炼内部缺表判定会直接跳过，不影响原写入。
   * dryRun：提炼照跑模型、照计量，但不 claim、不落库、不改 observe_task 一个字节（对账用）。
   * maxEntities：每轮最多领几个实体（默认 20，灰度预算见设计 §8）。
   * maxInput：每个实体最多带几条 active world/experience 证据进 prompt（默认 30）。
   * 召回过滤 / 复核触发 / memory_entity 渲染不在本配置范围（后续票）。
   */
  observe: Schema
    .object({
      enabled: Schema.boolean().default(false),
      dryRun: Schema.boolean().default(false),
      maxEntities: Schema.number().step(1).min(1).default(20),
      maxInput: Schema.number().step(1).min(1).default(30),
    })
    .default({ enabled: false, dryRun: false, maxEntities: 20, maxInput: 30 }),

  // ── 每轮自动回忆注入（2026-11-01 加：分级触发，让「动手前先翻记忆」变成机制）──
  /** 用户消息进来时自动检索长期记忆，拼进 system prompt。 */
  autoPrefill: Schema.boolean().default(true),
  /** 短于此长度且不含问号的消息不查（寒暄、应答一律跳过）。 */
  autoPrefillMinChars: Schema.number().step(1).min(1).default(6),
  /** 自动检索取几条。别调大：注入是给模型垫底的，不是塞满上下文。 */
  autoPrefillTopK: Schema.number().step(1).min(1).max(10).default(3),
  /** 注入段的整段文本上限（字）。只给摘要，不给全文。 */
  autoPrefillMaxChars: Schema.number().step(10).min(80).default(200),
  /** 单次检索超时（ms），超了就放弃注入。默认 1200：票面实测 p50 595 / p90 761，800 贴着线。 */
  autoPrefillTimeoutMs: Schema.number().step(50).min(50).default(1200),
  /** 触发用的实体词表：消息里出现即视为值得查。 */
  autoPrefillEntityWords: Schema
    .array(Schema.string())
    .default(['example', '示例项目', 'demo', 'service', 'database', 'memory', 'git', 'node', 'docker', 'sqlite']),

  // ── 模型与推送（整理和睡觉共用）──
  provider: Schema.string().default('deepseek-official'),
  model: Schema.string().default('deepseek-flash'),
  notifyScript: Schema.string().default(paths.notifyScript()),

  // ── 压缩前整理 ──
  consolidateEnabled: Schema.boolean().default(true),
  /**
   * true = 只对主会话做压缩前整理（默认）。subagent（子会话）会话一律不整理、不推通知：
   * 起因是收到「在 …/dsh-memory 新增 1 条」，是 subagent 会话触发的。
   * 手动工具 memory_consolidate 不受此开关限制。
   */
  consolidateOnlyMain: Schema.boolean().default(true),
  triggerTokens: Schema.number().step(1000).min(1000).default(180000),
  growthTokens: Schema.number().step(1000).min(5000).default(40000),
  // 2026-11-14：cooldownMinutes（整理冷却 25 分钟）删了。
  // 连续触发靠 inFlight 挡并发、靠 triggerTokens/growthTokens 判断「够不够量」，不再按时间等。
  // 2026-11-14：consolidateMaxTokens 配置口子删了，请求里永不出现 maxTokens。
  /** 整理专用推理档位。别跟睡觉/拆分的 reasoningEffort 混用：整理只要抽条目，低档就够，也不烧钱 */
  consolidateReasoningEffort: Schema.union(['off', 'low', 'high', 'max', 'default']).default('low'),
  /**
   * 整理时给模型开只读工具（list_dir / stat_path / read_file / glob / grep / memory_recall / chat_recall）。
   * 2026-10-30 有过这版，2026-11-03 12:09 重写提示词时整段丢了，当天又接回来。
   * 关掉就退回一次性单请求（旧行为），排障时用。
   */
  consolidateTools: Schema.boolean().default(true),
  // 2026-11-14：consolidateToolRounds / consolidateMaxToolCalls 两个次数闸删了。
  // 工具循环收敛改用活性守卫（src/agent-loop.ts：同工具同参连续 3 次 / 连续 20 次无新信息）。
  maxHistoryChars: Schema.number().step(1000).min(10000).default(600000),
  consolidateLogPath: Schema.string().default(paths.logFile('memory_consolidate.log')),
  /**
   * 整理状态落盘。
   * 空 = 与 stateFile（sleep.state.json）同目录的 consolidate.state.json。
   * 按会话的 lastTokens/lastTokensAt/pendingTokens + 退回估算节流窗口都写这里，重启读回。
   * 用独立文件而不是塞进 sleep.state.json：睡觉那边是整份覆盖写，两个写入口共用一个文件容易互相抹掉。
   */
  consolidateStateFile: Schema.string().default(''),

  // ── 上下文占压缩线提醒（从 ctx-watch.sh 搬进来）──
  /**
   * 每轮 llm/stream 判一次占用，过线就发 `memory.context.pressure` 并推 QQ。
   * 占用和窗口走 tokenMeter.contextPressure，压缩线按 compaction 的 modelPolicies 复算；
   * 提醒线只有一档 = 压缩线 × 95%（含），2026-11-15 起确认没有 98% 或更高档。
   */
  contextWatchEnabled: Schema.boolean().default(true),
  /** 去重状态落盘：同会话过 95% 只发一次，重启不能重弹；占用掉到压缩线 45% 以下或真压过一次才重置。 */
  contextWatchStateFile: Schema.string().default(paths.memoryFile('context-watch.state.json')),
  contextWatchLogFile: Schema.string().default(paths.logFile('memory_context_watch.log')),
  /** 关掉就只发事件不推 QQ（排障用）。 */
  contextWatchNotify: Schema.boolean().default(true),
  /**
   * 不发压线提醒的 provider 名单（精确相等、区分大小写，口径同 modelPolicies）。
   * 群会话走独立路由（本机 magpie-grp），群里不需要「上下文 XXk / 压缩线」提醒；
   * 默认空 = 只按会话来源判定，行为与旧版一致。判据只看 provider：主会话的 model
   * 同样是 group/ds，按 model 分会误伤主会话。
   */
  contextWatchSkipProviders: Schema.array(Schema.string()).default([]),

  // ── 睡觉 ──
  /**
   * 三期实体整理：每晚 compact 通道扫一遍实体候选。
   * apply=false（默认）只报不合并，跟改前逐字节一致；true 才把 fold_same / surface_cross
   * 这两类「名字层面确定同义」的实体合并。单复数 / 版本 / 前缀 / 相似度永远只报。
   * 每次合并写一行 entity_merge 当撤销凭据；按批撤回走
   * `node scripts/entity-tidy.mjs --undo-run <run_id>`，单条走 `--undo <mergeId>`。
   */
  entityTidy: Schema
    .object({
      apply: Schema.boolean().default(false),
    })
    .default({ apply: false }),
  sleepEnabled: Schema.boolean().default(true),
  /**
   * 贵时段推迟补跑：重启后的启动补跑只许在便宜时段发生，
   * 贵时段把那一档记成待补（sleep.state.json 的 `owedSlots`），等进了便宜时段再补。
   * 关掉 = 退回旧行为（过点就当场补，不看价）。价档判据由 price-gate 的注入点提供，
   * 没接判据时恒为「便宜」，开关开着也不改变现行为。
   */
  sleepDeferInPeak: Schema.boolean().default(true),
  /**
   * 价档判据来源：判据只有一份，dsh-price-clock 的 isCheapNow。
   * 这里只是路径口子，默认写绝对路径（跟 dsh-ego/lib/window.js:14 一个写法）；插件挪了位置改这行。
   */
  priceClockFile: Schema.string().default(paths.priceClockFile()),
  /** 关掉 = 不注入价档判据，退回「全时段便宜」的老行为（排障口子，默认开）。 */
  sleepPriceProbe: Schema.boolean().default(true),
  /** 睡醒后是否推一条汇总。2026-10-30 维护者 说不用汇报，本机配成 false（失败仍会推） */
  sleepSummaryNotify: Schema.boolean().default(true),
  hour: Schema.number().step(1).min(0).max(23).default(4),
  minute: Schema.number().step(1).min(0).max(59).default(30),
  /** 一天多时段：空则退回 hour/minute 单时段。2026-10-29 维护者 要中午也整理一次，2026-10-30 从 11 点挪到 13 点，2026-11-11 提前到 12:30 */
  schedule: Schema
    .array(
      Schema.object({
        hour: Schema.number().step(1).min(0).max(23),
        minute: Schema.number().step(1).min(0).max(59),
      }),
    )
    .default([]),
  checkIntervalMs: Schema.number().step(1).min(10000).default(60000),
  /** 一次睡觉最多改几条记忆。2026-10-28 从 5 提到 20：记忆涨到 160+ 条，5 条一晚根本追不上 */
  // 2026-11-14：maxChanges（一次睡觉最多改几条）删了，本轮该落多少落多少。
  // 2026-11-14：mergeMaxTokens / dedupeMaxTokens 配置口子删了，不再传 maxTokens。
  // 2026-11-14：mergeBatchMax（固定 6 条一批）删了。切批改由 token 预算决定，
  // 见 src/token-window.ts；窗口从 contextWindowTokens / 宿主 compaction 配置读。
  /**
   * 模型的实际上下文窗口（token）。0 = 不在插件配置里写死，回落到宿主 compaction 配置里
   * 该路由（provider+model）的 contextWindow；两边都没有时按「窗口未知」处理，不切批。
   */
  contextWindowTokens: Schema.number().step(1000).min(0).default(0),
  /** 入口冲突检查：新记忆入库前按实体比对旧条，判冗余 / 状态变更 / 矛盾。2026-10-29 加 */
  conflictCheckEnabled: Schema.boolean().default(true),
  /**
   * 冲突预判（2026-11-20）：入口冲突检查先问本地 decision 桥
   * （tools/gemini-decision-bridge，默认 127.0.0.1:3427/systemone），
   * 高置信判 new 的直接放行，省掉一次生成模型调用（实测 81.1% 判 new，单次 p50 9.7s）。
   * 默认 false —— 关着时 runConflictCheck 不发请求、日志文本与加这层之前逐字节一致；
   * 桥失败/超时/429/答案缺 qid 一律整批退回原 askConflict，主流程无感知。
   */
  conflictDecide: Schema
    .object({
      enabled: Schema.boolean().default(false),
      /** 一批几条；硬上限 10（批内共享 state，条数越多互相污染越重，代码里再夹一次）。 */
      batchSize: Schema.number().step(1).min(1).default(8),
      /** 判 new 的概率门槛：低于它退回原来的 askConflict。 */
      minConfidence: Schema.number().min(0).max(1).default(0.9),
      /** 单批超时（毫秒）：超时整批降级。 */
      timeoutMs: Schema.number().step(100).min(200).default(2500),
      /** decision 桥地址（只认 http://host:port，路径固定 /systemone）。 */
      baseUrl: Schema.string().default('http://127.0.0.1:3427'),
      /** 桥透传给上游的模型名。 */
      model: Schema.string().default('gemini-decision-1'),
    })
    .default({
      enabled: false,
      batchSize: 8,
      minConfidence: 0.9,
      timeoutMs: 2500,
      baseUrl: 'http://127.0.0.1:3427',
      model: 'gemini-decision-1',
    }),
  // 2026-11-14：conflictCheckMaxCalls（一次最多检查 20 条）删了，全部检查。
  /**
   * 工具循环的累计上下文预算（2026-11-23）。
   * 12:30 那轮 27 个文件 202 次请求，平均 7.5 次/文件，每轮都把旧工具结果原文重发。
   * 同一个文件（一次 askModel）的历史工具结果累计超过 tokens 后，旧结果只保留
   * 「出处 + 相关片段」（本地截取，不额外调模型）；最近 keepRecentRounds 轮、以及最新
   * 一条工具结果永远保留原文。候选 listing 与顺序不动。
   * tokens<=0 或 enabled=false = 关闭，消息体与加这层之前逐字节一致。
   */
  sleepToolBudget: Schema
    .object({
      enabled: Schema.boolean().default(true),
      /** 累计工具片段 token 上限（默认 4000，离线回算 187 次请求约少送 50 万输入 token）。 */
      tokens: Schema.number().step(100).min(0).default(4000),
      /** 最近几轮工具结果保留原文（0 = 只保最新一条）。 */
      keepRecentRounds: Schema.number().step(1).min(0).max(20).default(1),
      /** 压缩后每条保留的片段字符数。 */
      excerptChars: Schema.number().step(20).min(40).default(240),
      /** 压缩后每条最多保留几行片段。 */
      maxExcerptLines: Schema.number().step(1).min(1).max(50).default(8),
    })
    .default({
      enabled: true,
      tokens: 4000,
      keepRecentRounds: 1,
      excerptChars: 240,
      maxExcerptLines: 8,
    }),
  /**
   * 定向查证（2026-11-23）：把「一轮问完所有候选」改成先做 rounds 轮定向查证——
   * 第一轮工具结果后注入一条 user 提示，要求先只查最可疑的 1-maxCallsPerRound 条
   * （否定结论、具体路径/服务/版本、状态类），确认会改变归因或状态才继续补查；
   * 轮次用完后注入一条收尾提示。只是提示词层，不硬拦调用；enabled=false 或 rounds=0
   * 时两条提示都不注入，消息体与加这层之前逐字节一致。
   */
  sleepToolCheck: Schema
    .object({
      enabled: Schema.boolean().default(true),
      rounds: Schema.number().step(1).min(0).max(10).default(3),
      maxCallsPerRound: Schema.number().step(1).min(1).max(10).default(3),
    })
    .default({
      enabled: true,
      rounds: 3,
      maxCallsPerRound: 3,
    }),
  /**
   * 候选生成方式（2026-11-06 t-graph-cluster）。
   * 'graph' = 向量近邻图连通分量（默认，src/similar-graph.ts）；'entity' = 老的实体分桶
   * （src/cluster.ts 的 clusterForSleep，一行没删，可整体切回）。缺向量的条目在 graph 模式下
   * 仍回落实体分桶，所以 'entity' 只在排障时用。
   */
  clusterMode: Schema.union(['graph', 'entity']).default('graph'),
  /** 建图的余弦阈值：近邻余弦 ≥ 此值才连边。0.79 是设计决定（原型推荐 0.82）。 */
  clusterThreshold: Schema.number().min(0).max(1).default(0.79),
  /** 每条取几个近邻建图（top-k，排除自身）。 */
  clusterNeighbors: Schema.number().step(1).min(1).default(8),
  // 2026-11-14：clusterMaxSize（单簇 30 条封顶）删了。簇不再按条数截，
  // 装不装得下由 token 切批决定；先截簇会把「同簇同批」这层关系提前砍掉。
  /** 余弦达到这个值必进同簇。 */
  clusterNearDup: Schema.number().min(0).max(1).default(0.9),
  /** 批内最像两条低于此值就不问模型，本地跳过。 */
  clusterQuestionMin: Schema.number().min(0).max(1).default(0.78),
  // 2026-11-14：clusterChanges（单批最多 6 个动作）删了，提案全部落地。
  // 2026-11-14：organizeMaxCalls（一轮最多问 N 批）删了，批数由 token 切出来算。
  // 2026-11-14：sleepMemoryChars（固定字符预算）删了，改成按窗口 token 动态算。
  /** 阶段三拆分开关（2026-11-14 起单轮条数上限 sleepSplitLimit 删了，本轮全部拆）。 */
  sleepSplitEnabled: Schema.boolean().default(true),
  /**
   * 睡觉流水线第二步（docs/sleep-pipeline-redesign.md §3、§4）：会话入库改按文件 ack。
   * 默认 false —— 关着时 sleepPass 走老的「since 之后的全部文件 + 单游标」路径。
   * 2026-11-14：单轮文件预算 ingestBudget / 重放预算 replayBudget 都删了，
   * 打开后本轮能处理的全部处理，pending_ops 当期到期的全部重放。
   */
  pipeline: Schema
    .object({
      ingestAck: Schema.boolean().default(false),
      /**
       * 唯一写入闸（docs/sleep-pipeline-redesign.md §1(a)、§2.3）：所有落 memories 的
       * 路径先过 DedupeAgent。默认 false —— 关着时 save/update/merge 行为与旧版逐字节一致。
       * 打开前先跑 scripts/apply-pipeline-schema.mjs（discards / pending_ops）。
       */
      writeGate: Schema.boolean().default(false),
      /** 余弦到这个值才问模型；低于它由规则层直接 DISTINCT（judge 的 threshold）。 */
      writeGateThreshold: Schema.number().min(0).max(1).default(0.9),
      /**
       * 第三步：分簇整理改走 MergeAgent（双通道 JSON + 证据校验，docs/agent-prompts.md）。
       * 默认 false —— 关着时 organizePass 仍是老的分簇整理，行为与改前一致。
       * 空响应 / 撞墙 / 解析失败落 pending_ops(kind='merge_batch')，第 3 次 dead；
       * 模型明确给 {"ops":[]} 的批算 noop，直接写 organizeCache，不进 pending 烧钱。
       */
      agentPipeline: Schema.boolean().default(false),
      /**
       * 拆分自足性段级信号（split_delimiter / split_deictic / split_premise / split_process）
       * 是否当硬拒。默认 false = 只记日志与 signals 字段，不判废、不重问、不落 dead；
       * 阶段二回放误杀率达标后才打开。textLint 的 warn 级违规不受这个开关影响。
       */
      splitSignalsStrict: Schema.boolean().default(false),
    })
    .default({
      ingestAck: false,
      writeGate: false,
      writeGateThreshold: 0.9,
      agentPipeline: false,
      splitSignalsStrict: false,
    }),
  /**
   * 入库并发：ingest 是整轮第一段，29 个重放文件串行问模型，
   * 一文件约 2.4 分钟，后面所有阶段都在等它。改成 N 路并行问模型、落地串行写库
   * （和 merge 段同一个节奏，见 sleep.js 的 runIngestPool）。
   * 默认 64（2026-11-14 从 3 提到 8：CPU 压力不大，大头在 socket 并发；
   * 2026-11-14 从 8 提到 64：查过上游——DeepSeek 官方账号级并发
   * deepseek-flash 2500 / v4-pro 500，opencode-go 没公开并发数字，本机 magpie 不排队不限速，
   * 历史上 74 路同时在途全 200、0 个 429；本机只多 64 个 socket 和几十 MB 上下文）。
   * 硬顶 64 —— 代码里用 Math.min(..., 64) 再夹一次，配大了也不超。
   * schema 故意不写 .max(64)：Schemastery 的 max 是校验不是夹，配成 99 会让整个插件起不来；
   * 硬顶交给 ingestConcurrencyOf 夹（和 organizeConcurrency 同一个口径，但两个不同的值）。
   * 文件数比 64 少时池只开文件数那么多 worker（runIngestPool 里 Math.min(conc, list.length)）。
   */
  organizeIngestConcurrency: Schema.number().step(1).min(1).default(64),
  /**
   * ingest 并发是否按当时负载自适应：默认 true。
   * organizeIngestConcurrency 从此只当理论上限（硬顶 64）；真正开几路在每轮 ingest 池开跑前
   * 按 /proc/loadavg 的 load1、/proc/meminfo 的 MemAvailable、CPU 核数、/sys thermal 温度算一次，
   * 一轮内固定，下一轮重算。关掉就恒用 organizeIngestConcurrency（回归保护）。
   */
  organizeIngestAdaptive: Schema.boolean().default(true),
  /**
   * 整理链的调度形态（设计决定：队列 + 池）。
   *   'pool'   新的池调度器：ingest / curate / compact 三个通道各领各的活、互不阻塞；
   *   'legacy' 老的一条顺序 await 到底（回滚口子，行为一个字不改）。
   * 默认 'pool'；手写 cfg 没带这个键时按 legacy 跑（embedder/测试不会因为升级突然换路径）。
   */
  organizePipelineMode: Schema.union(['pool', 'legacy']).default('pool'),
  /**
   * 送模型前的候选漏斗（2026-11-14 阶段 3）。默认 off：先对照，不悄悄改行为。
   *   off  与改造前逐条一致；
   *   l0   精确指纹去重（同实体 + 同一天 + 正文互为前缀）；
   *   l1   L0 + 廉价字面相似度预筛（token Jaccard / 公共前缀 / token 编辑距离，不调模型）；
   *   l2   L1 + 结构约束（同实体或同 type、时间邻近）；'on' 等同 l2。
   */
  organizeFunnelMode: Schema.union(['off', 'l0', 'l1', 'l2', 'on']).default('off'),
  /** L1 字面相似度下限：批内最大廉价相似度低于它、且向量分 < funnelVectorCeil 才砍。 */
  funnelCheapMin: Schema.number().min(0).max(1).default(0.15),
  /** L1 的向量上限：只砍「向量分刚过线」的批，高分批留给模型。 */
  funnelVectorCeil: Schema.number().min(0).max(1).default(0.85),
  /** L1 向量闸：批内最小成对余弦下限（卡离群成员）；缺向量/不足 2 条时不判。 */
  funnelMinPairCos: Schema.number().min(0).max(1).default(0.78),
  /** L1 向量闸：批内平均成对余弦下限（卡整体质量）。 */
  funnelMinAvgCos: Schema.number().min(0).max(1).default(0.8),
  /** L2 时间窗（天）：批内最早与最晚记忆超过这个跨度就不进批。 */
  funnelTimeWindowDays: Schema.number().step(1).min(1).default(180),
  /** L3 每轮最多送几批（0 = 不限，保持现状）。 */
  funnelMaxBatches: Schema.number().step(1).min(0).default(0),
  /**
   * 合批（2026-11-14 对照实验后加）：一次请求最多拼几对跨天 2 条批。0 = 关闭（默认）。
   * 真库实测：跨天批拼 6 对一致率 96.1%，同天批只有 85.5%，所以只拼跨天那一段。
   */
  organizeBatchPackSize: Schema.number().step(1).min(0).default(0),
  /** 合批下限：两条记忆时间跨度必须 > 这个天数才拼（默认 1，即跨天）。 */
  organizeBatchPackMinSpanDays: Schema.number().step(1).min(0).default(1),
  /** 合批上限：跨度超过这个天数不拼（默认 30；2-30 天实测 96.4%）。 */
  organizeBatchPackMaxSpanDays: Schema.number().step(1).min(1).default(30),
  /**
   * 全链并发的「手工上限覆盖」：0 = 不覆盖，走 organizeIngestConcurrency + 负载自适应。
   * 2026-11-14 收敛：它以前只被 merge 段当独立并发数用（还额外写死硬顶 8），现在只许往下压，
   * 不再有任何一段自己有独立硬顶。ingest 通道的上限仍只认 organizeIngestConcurrency。
   */
  organizeConcurrency: Schema.number().step(1).min(0).default(0),
  // 2026-11-14：maxSessions（只看末 N 个会话文件）删了，since 之后全读。
  maxTranscriptChars: Schema.number().step(1).min(2000).default(120000),
  maxMessageChars: Schema.number().step(1).min(100).default(1500),
  /** 宿主暂无会话接口（等 session/reader，见 docs），路径只能自己定；DSH_SESSIONS_DIR 可覆盖。 */
  sessionsRoot: Schema.string().default(paths.sessionsRoot()),
  stateFile: Schema.string().default(paths.memoryFile('sleep.state.json')),
  /** organizeCache 主文件保留条数：超出的按写入月份归档，默认 200 */
  organizeCacheMax: Schema.number().step(1).min(1).default(200),
  /** organizeCache 归档目录；空 = 跟 stateFile 同级的 organize-archive/ */
  organizeArchiveDir: Schema.string().default(''),
  archiveDir: Schema.string().default(paths.memoryFile('new-archive')),
  // 2026-11-14：archiveMaxChanges（/new 存档最多改 3 条）删了，该存多少存多少。
  archiveNotify: Schema.boolean().default(true),
  backupDir: Schema.string().default(paths.memoryFile('backup')),
  sleepLogFile: Schema.string().default(paths.logFile('memory_sleep.log')),
  selfTestMarker: Schema.string().default(paths.memoryFile('sleep-selftest.request')),
  /** 睡觉 + 拆分共用的推理档位；压缩前整理另有 consolidateReasoningEffort（默认同为 low） */
  reasoningEffort: Schema.union(['off', 'low', 'high', 'max', 'default']).default('low'),
  // 2026-11-14：sleepMaxTokens 配置口子删了，不再传 maxTokens。

  // ── 拆分（2026-10-29 第三版：一条只说一件事）──
  /** 超过这个字数的活跃条目会被拆；维护者 2026-10-29 把门槛从 100 放宽到 200 */
  splitMaxChars: Schema.number().step(1).min(20).default(200),
  /** 拆出来的单条上限；2026-11-03 跟落库闸门一起收到 200，不再留 220 余量（代码里也按 200 封顶） */
  splitItemMaxChars: Schema.number().step(1).min(20).default(200),
  splitMaxItems: Schema.number().step(1).min(1).default(32),
  // 2026-11-14：splitMaxTokens 配置口子删了，不再传 maxTokens。
  splitStateFile: Schema.string().default(paths.memoryFile('split.state.json')),
  splitLogFile: Schema.string().default(paths.logFile('memory_split.log')),
  splitPreviewFile: Schema.string().default(paths.memoryFile('split-preview.md')),
  /** 检索日志（JSONL）：每调一次 recall 一行，用来跟会话 jsonl 对齐查「该查没查」。超过 8MB 滚成 .1 */
  recallLogFile: Schema.string().default(paths.logFile('memory_recall.log')),
  /** 存储层自己的日志（迁移、load、写事务回滚、备份），格式同其它 memory_*.log */
  storeLogFile: Schema.string().default(paths.logFile('memory_store.log')),

});

/**
 * 整理状态文件路径：默认跟 sleep.state.json 同目录，
 * 独立文件。空配置 = 派生；显式配了就照配。
 */
export function consolidateStatePath(cfg: MemoryConfig | null | undefined): string {
  const explicit = String(cfg?.consolidateStateFile || '').trim();
  if (explicit) return explicit;
  const base = String(cfg?.stateFile || paths.memoryFile('sleep.state.json'));
  return path.join(path.dirname(base), 'consolidate.state.json');
}

/** 启动时读回整理状态；文件不在/半截 JSON 一律当空，不抛。 */
export function readConsolidateState(cfg: MemoryConfig | null | undefined): unknown {
  try {
    return JSON.parse(fs.readFileSync(consolidateStatePath(cfg), 'utf8'));
  } catch {
    return {};
  }
}

/** 落盘：先写 .tmp 再 rename，写一半断电不会留半截 JSON。 */
export function writeConsolidateState(cfg: MemoryConfig | null | undefined, state: unknown): void {
  const file = consolidateStatePath(cfg);
  fs.mkdirSync(path.dirname(file), { recursive: true });
  const tmp = `${file}.tmp-${process.pid}`;
  fs.writeFileSync(tmp, JSON.stringify(state, null, 2), 'utf8');
  fs.renameSync(tmp, file);
}

/** 版本号只给启动快照用；读不到就报 unknown，别让一行日志拖垮启动。 */
function packageVersion(): string {
  try {
    const pkg = JSON.parse(fs.readFileSync(path.join(import.meta.dirname, '../package.json'), 'utf8'));
    return String(pkg.version || 'unknown');
  } catch {
    return 'unknown';
  }
}

/** 存储后端：memory.db 在就是 sqlite（2026-11-05 起唯一运行时），否则仍按旧 jsonl 库报。 */
function storeBackend(cfg: MemoryConfig | null | undefined): string {
  try {
    return fs.existsSync(path.join(String(cfg?.storePath || ''), 'memory.db')) ? 'sqlite' : 'jsonl';
  } catch {
    return 'jsonl';
  }
}

/**
 * 启动快照行。2026-11-05 加：开关状态直接进日志，排障碍不用再 dump 配置反推。
 * 字段名固定，docs/sleep-pipeline-redesign.md §7 有对照说明。
 */
export function pipelineSnapshotLine(cfg: MemoryConfig | null | undefined, { version = packageVersion(), store = storeBackend(cfg) }: { version?: string; store?: string } = {}): string {
  const p = cfg?.pipeline || {};
  const bool = (v: unknown) => (v === true ? 'true' : 'false');
  const num = (v: unknown, fallback: number) => {
    const n = Number(v);
    return Number.isFinite(n) ? n : fallback;
  };
  return (
    `[memory] pipeline ingestAck=${bool(p.ingestAck)} agentPipeline=${bool(p.agentPipeline)} ` +
    `writeGate=${bool(p.writeGate)} threshold=${num(p.writeGateThreshold, 0.9)} ` +
    `store=${store} v=${version}`
  );
}

/** 打印快照并返回这一行，方便测试直接断言。 */
export function logPipelineSnapshot(ctx: MemoryPluginContext, cfg: MemoryConfig | null | undefined): string {
  const line = pipelineSnapshotLine(cfg);
  if (typeof ctx?.logger?.info === 'function') ctx.logger.info(line);
  else console.info(line);
  return line;
}

export function apply(ctx: MemoryPluginContext, config: MemoryConfig): void {
  const cfg = { ...config };
  logPipelineSnapshot(ctx, cfg);
  const store = createStore(cfg as StoreConfig);
  // 写入闸的模型客户端：只注入 getter，每次现取（AGENTS：别在 setup 期存 ctx.llm，
  // 重载后旧句柄会抛 inactive context）。
  store.attachLlm?.(() => llmOf(ctx, { site: 'write_gate', cfg }));
  // observation 写侧登记：records / pipeline-split 走 state.cfg 的同一开关；
  // entity-graph 的「挂载晚到」没有 cfg 入口，装配处按同一份配置设到每库实例（默认 false = 一行不写）。
  entityGraphOf(store.db()).setObserveEnabled(cfg.observe?.enabled === true);
  registerStoreTools(ctx as unknown as ToolContextLike, store, cfg as StoreConfig);
  // 压线提醒跟着已有的 llm/stream 钩子走，不另开定时器；要在 setupConsolidate 之前建好。
  const contextWatch = setupContextWatch(ctx as unknown as ContextWatchCtx, cfg);
  // 2026-11-14：整理状态以前只在内存里，重启就丢（按会话分账白做）。
  // 现在启动读回、每次变账落盘，跟 sleep.state.json 同目录的 consolidate.state.json。
  setupConsolidate(ctx as unknown as ConsolidateCtx, store, cfg as ConsolidateCfg, {
    contextWatch,
    consolidateState: readConsolidateState(cfg),
    persistState: (state) => writeConsolidateState(cfg, state),
  });
  setupSleep(ctx as unknown as Parameters<typeof setupSleep>[0], store as unknown as SleepStore, cfg as unknown as Parameters<typeof setupSleep>[2]);
  setupAutoPrefill(ctx as unknown as PrefillCtx, store as unknown as PrefillStore, cfg as PrefillCfg);
  registerSplitTool(ctx, store, cfg);
  // entity_extract 队列：10 分钟一 tick，只在主会话进程里跑（worker 会话调进来直接 return）
  const entityQueue = setupEntityQueue(ctx, store, cfg as EntityWorkerConfig);
  ctx.effect(() => () => entityQueue.dispose());
  ctx.provide?.('memory-entity-queue', { tick: () => entityQueue.tick('manual'), stats: entityQueue.stats });
  ctx.logger?.info?.(`dsh-memory ready: ${store.count()} 条活跃记忆 @ ${cfg.storePath}`);
  ctx.logger?.info?.(
    `[memory] entityQueue enqueue=${cfg.entityQueue?.enqueue !== false} consume=${cfg.entityQueue?.consume === true} ` +
      `tick=${Math.round(Number(cfg.entityQueue?.tickMs || 600000) / 1000)}s batch=${Number(cfg.entityQueue?.batch || 8)} ` +
      `genericDf=${Number(cfg.entityQueue?.genericDf ?? 0.05)}`,
  );
}

/** memory_split 的单条明细（output.schema 里 details 的形状）。 */
interface SplitToolDetails {
  id?: string;
  action?: string;
  items?: number;
  preview?: string;
  gap?: number;
  missing?: string;
  reason?: string;
  into?: string;
}

/** memory_split 的回执（output.schema 校验的规范值）。 */
interface SplitToolOutput {
  dryRun: boolean;
  scanned: number;
  tried: number;
  split: number;
  failed: number;
  details: SplitToolDetails[];
  previewFile?: string;
}

/** memory_split：把超过 splitMaxChars 的条目拆成「一条只说一件事」，默认演练 */
function registerSplitTool(ctx: MemoryPluginContext, store: MemoryStore, cfg: MemoryConfig): void {
  ctx.tools.register(
    defineTool({
      name: 'memory_split',
      description:
        'Split long-term memory entries longer than splitMaxChars into atomic entries that each state exactly one fact, retyping each as world/experience/opinion. Originals are archived with a reason and the new entries supersede them. Defaults to dryRun (write a preview file, change nothing). Pass dryRun=false to actually split.',
      parameters: {
        dryRun: { type: 'boolean', description: 'Preview only, do not touch the store. Defaults to true.' },
        ids: { type: 'string', description: 'Comma-separated memory ids to limit the run to.' },
        limit: { type: 'number', description: 'At most this many entries this run (0 = all).' },
        maxChars: { type: 'number', description: 'Override the length threshold (default comes from config, 200).' },
        force: { type: 'boolean', description: 'Reprocess entries already handled before.' },
      },
      output: {
        schema: {
          type: 'object',
          additionalProperties: false,
          properties: {
            dryRun: { type: 'boolean', required: true },
            scanned: { type: 'number', required: true },
            tried: { type: 'number', required: true },
            split: { type: 'number', required: true },
            failed: { type: 'number', required: true },
            previewFile: { type: 'string' },
            details: {
              type: 'array',
              items: {
                type: 'object',
                additionalProperties: false,
                properties: {
                  id: { type: 'string' },
                  action: { type: 'string' },
                  items: { type: 'number' },
                  preview: { type: 'string' },
                  gap: { type: 'number' },
                  missing: { type: 'string' },
                  reason: { type: 'string' },
                  into: { type: 'string' },
                },
              },
            },
          },
        },
        render: (_args, v) => [
          {
            type: 'text',
            text:
              `待拆 ${v.scanned} 条，实处理 ${v.tried} 条，${v.dryRun ? '演练' : '拆开'} ${v.dryRun ? v.tried - v.failed : v.split} 条` +
              (v.failed ? `，失败 ${v.failed} 条` : '') +
              (v.dryRun && v.previewFile ? `\n稿子写在 ${v.previewFile}` : ''),
          },
          ...(v.details || []).slice(0, 30).map((d): { type: 'text'; text: string } => ({
            type: 'text',
            text: `· ${d.id} ${d.action}${d.items ? ` ${d.items} 条` : ''}${d.reason ? `（${d.reason}）` : ''}${d.preview ? `「${d.preview}…」` : ''}`,
          })),
        ],
      },
      async execute(args, exec) {
        assertMayWrite(exec, 'memory_split');
        const { runSplitPass } = await import('./split-pass.js');
        const ids = args.ids ? String(args.ids).split(',').map((s) => s.trim()).filter(Boolean) : [];
        const v = await runSplitPass({
          store,
          llm: llmOf(ctx, { site: 'split:manual', cfg }),
          cfg,
          dryRun: args.dryRun !== false,
          ids,
          limit: Number(args.limit) || 0,
          maxChars: Number(args.maxChars) || 0,
          force: Boolean(args.force),
          // 手动触发也走同一套自适应，不再一条条问
          concurrency: selectCurateConcurrency(cfg as ConcurrencyConfig).conc,
        } as unknown as SplitPassOpts);
        // 返回值必须是干净的 JSON：undefined 键会让上层校验直接报 not lossless JSON
        const details = (v.details || []).map((d) => {
          const o: Record<string, unknown> = {};
          for (const k of Object.keys(d)) if (d[k] !== undefined) o[k] = d[k];
          return o;
        }) as unknown as SplitToolDetails[];
        const out: SplitToolOutput = { dryRun: v.dryRun, scanned: v.scanned, tried: v.tried, split: v.split, failed: v.failed, details };
        if (v.dryRun) out.previewFile = cfg.splitPreviewFile;
        return out;
      },
    }),
  );
}
