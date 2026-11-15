/**
 * 队列 kind 注册表（2026-11-14，阶段 2）。
 *
 * 目的：kind 归哪个通道、由哪个池消费、阈值多少，只在这里写一次；
 * 池调度和 markOp 不再各自维护一份 kind 列表 / 大 if。
 *
 * 执行入口（prepare/commit/release/replay）暂时还留在 setupSleep 的闭包里，
 * 那里能直接拿到 store/cfg/askMergeOps；阶段 5 把 compact 通道接进池时再随函数一起注册进来。
 */
/** 一个 kind 的归属：哪个通道、哪个池、判死阈值。 */
export interface KindDef {
  kind: string;
  channel: string;
  pool: string;
  maxAttempts: number;
}

/** 注册表对外面；get 未注册返回 null，channelOf 未注册返回空串。 */
export interface KindRegistry {
  register: (def: KindDef) => KindDef;
  get: (kind: string) => KindDef | null;
  channelOf: (kind: string) => string;
  maxAttemptsOf: (kind: string) => number;
  kindsOfChannel: (channel: string) => string[];
  kindsOfPool: (pool: string) => string[];
  list: () => KindDef[];
}

export const KIND_DEFS: readonly KindDef[] = Object.freeze([
  { kind: 'ingest_file', channel: 'ingest', pool: 'ingest', maxAttempts: 5 },
  { kind: 'merge_batch', channel: 'curate', pool: 'merge', maxAttempts: 5 },
  { kind: 'split_item', channel: 'curate', pool: 'split', maxAttempts: 5 },
  // compact = 本地重放通道（2026-11-14 阶段 5）：真领活、真报数。
  // 注意口径：这三类活本身不问模型，但写库会过写入闸（writeGate=true 时闸里会问），
  // 所以 compact 的计数分 llm_calls（闸花的）与 local_ops（纯本地动作），别再写「0 次 LLM」。
  { kind: 'revocation_sweep', channel: 'compact', pool: 'compact', maxAttempts: 5 },
  { kind: 'merge_op', channel: 'compact', pool: 'compact', maxAttempts: 5 },
  { kind: 'dedupe_check', channel: 'compact', pool: 'compact', maxAttempts: 5 },
  // 三期实体整理：纯本地规则，每晚扫一遍实体候选。
  // 默认只报不合并（runEntityTidyPass 的 apply 由调用方显式打开），合库走 scripts/entity-tidy.mjs。
  { kind: 'entity_tidy', channel: 'compact', pool: 'compact', maxAttempts: 5 },
  // entity_extract 单独一张表、单独一个 10 分钟 tick 池
  { kind: 'entity_extract', channel: 'entity', pool: 'entity', maxAttempts: 3 },
]);

export function createKindRegistry(defs: readonly KindDef[] = KIND_DEFS): KindRegistry {
  const map = new Map<string, KindDef>();
  const put = (def: KindDef): KindDef => {
    if (!def?.kind) throw new Error('kind-handlers: kind 必填');
    map.set(String(def.kind), { ...def });
    return map.get(String(def.kind)) as KindDef;
  };
  for (const def of defs) put(def);
  return {
    register: put,
    get: (kind: string) => map.get(String(kind || '')) || null,
    channelOf: (kind: string) => map.get(String(kind || ''))?.channel || '',
    maxAttemptsOf: (kind: string) => Number(map.get(String(kind || ''))?.maxAttempts) || 5,
    kindsOfChannel: (channel: string) => [...map.values()].filter((d) => d.channel === channel).map((d) => d.kind),
    kindsOfPool: (pool: string) => [...map.values()].filter((d) => d.pool === pool).map((d) => d.kind),
    list: () => [...map.values()],
  };
}

/** 插件内共用一份；测试要隔离就自己 createKindRegistry()。 */
export const defaultKindRegistry = createKindRegistry();

export const kindsOfChannel = (channel: string): string[] => defaultKindRegistry.kindsOfChannel(channel);
export const kindsOfPool = (pool: string): string[] => defaultKindRegistry.kindsOfPool(pool);
export const channelOfKindDef = (kind: string): string => defaultKindRegistry.channelOf(kind);
