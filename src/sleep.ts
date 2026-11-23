/**
 * 「睡觉」入口（2026-11-15 拆分后的转发层）。
 *
 * setupSleep 与各段实现都在 src/sleep/ 下；这里只保留原来的导出面，
 * 老引用（src/index.ts、scripts、test/*.mjs）一个字都不用改。
 */
export { setupSleep } from './sleep/setup.js';

export {
  batchFingerprint,
  MEMORY_ITEM_OVERHEAD_TOKENS,
  CLUSTER_PROMPT_SHELL_TOKENS,
  SLEEP_PROMPT_SHELL_TOKENS,
  splitAgentBatches,
} from './sleep/batching.js';

export { readState, stateOf } from './sleep/state-access.js';

export {
  RETIRED_SLEEP_SLOTS,
  pickDueSleepSlot,
  SLEEP_HEARTBEAT_MS,
  SLEEP_HEARTBEAT_MISSES,
  sleepOwnerOf,
  sameSleepOwner,
  dueSleepSlotNow,
  shouldMarkSlot,
} from './sleep/slots.js';

export { mergeToolCalls, collectTurnChunks, repairToolCallIds } from './sleep/tool-calls.js';

// 工具循环累计预算 / 定向查证（2026-11-23）：纯函数，测试与回放脚本直接取
export {
  TOOL_BUDGET_MARK,
  TOOL_BUDGET_DEFAULT_TOKENS,
  TOOL_BUDGET_DEFAULT_EXCERPT_CHARS,
  TOOL_BUDGET_DEFAULT_KEEP_ROUNDS,
  TOOL_CHECK_DEFAULT_ROUNDS,
  TOOL_CHECK_DEFAULT_MAX_CALLS,
  toolBudgetCfgOf,
  toolCheckCfgOf,
  focusTermsOf,
  pickRelevantLines,
  compactToolHistory,
  buildToolCheckHint,
} from './sleep/tool-budget.js';

export { runIngestPool } from './sleep/pool.js';

// 并发口径转发（跟拆分前一样，老引用处不用改）
export {
  INGEST_HARD_CAP, ingestConcurrencyOf, ingestLoadSnapshot, adaptIngestConcurrency,
  curateCapOf, stageCapOf, selectConcurrency, selectCurateConcurrency, selectIngestConcurrency,
  formatIngestAdaptiveLog,
  formatStageAdaptiveLog, formatPoolSummary,
} from './concurrency.js';
