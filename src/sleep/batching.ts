/**
 * 批次指纹与消费侧切批：按 token 动态切，簇本身不拆不改写。
 */
import { digestParts } from '../fingerprint.js';
import { chunkByTokens } from '../token-window.js';
import { batchMaxCos } from '../cluster.js';
import type { VecView } from '../cluster.js';
import type { MemoryClusterItem, MemoryBatch, VecLookup } from '../batch-planner.js';
import type { MemoryRecord } from '../store/types.js';

/** 一簇 + 本簇最大余弦（planner 的队列项只多不少）。 */
interface SplitQueueItem {
  cluster?: MemoryClusterItem[];
  score?: unknown;
}

/** splitAgentBatches 的入参：队列、向量表、token 预算、最低余弦。 */
interface SplitSpec {
  queue?: SplitQueueItem[];
  vecOf?: VecLookup | null;
  budgetTokens?: number;
  minScore?: number;
}

/** 切出来的一批：在 MemoryBatch 上多带父簇大小（提示词里要报「原簇 N 条」）。 */
export interface SleepBatch extends MemoryBatch {
  parentSize: number;
}

export function batchFingerprint(cluster: MemoryClusterItem[]): string {
  // 排序后的「成员 id + 正文」逐字节拼起来再取 sha1 前 16 位；换原语不改字节序
  const parts = [...cluster]
    .sort((x, y) => ((x.id as string) < (y.id as string) ? -1 : 1))
    .map((m) => `${m.id}\u0000${String(m.text || '')}\u0001`);
  return digestParts(parts, { sep: '', len: 16 });
}

/** 一条记忆塞进 prompt 时的固定开销（id/type/tags/entities 与分隔符），按 token 估。 */
export const MEMORY_ITEM_OVERHEAD_TOKENS = 24;
/** 整理批次的提示词外壳（system + 输出格式 + 收尾话术），按 token 估。 */
export const CLUSTER_PROMPT_SHELL_TOKENS = 600;
/** 会话记录 + 现有记忆拼在一起时的外壳（标题、说明、反馈段），按 token 估。 */
export const SLEEP_PROMPT_SHELL_TOKENS = 800;

export function splitAgentBatches({ queue = [], vecOf = null, budgetTokens = 0, minScore = 0 }: SplitSpec = {}): SleepBatch[] {
  const out: SleepBatch[] = [];
  for (const item of Array.isArray(queue) ? queue : []) {
    const cluster = Array.isArray(item?.cluster) ? item.cluster : [];
    if (cluster.length < 2) continue;
    const parts = chunkByTokens(cluster, {
      budget: budgetTokens,
      textOf: (m) => String(m?.text || ''),
      overhead: MEMORY_ITEM_OVERHEAD_TOKENS,
    });
    for (const part of parts) {
      if (!part.length) continue;
      // 子批自己算相似度：一个大簇里可能只有相邻几条像，别拿整簇的最高分冒充
      const score = part.length >= 2 ? batchMaxCos(part as unknown as MemoryRecord[], vecOf as unknown as VecView) : (Number(item.score) || 0);
      if (part.length >= 2 && score < minScore) continue;
      out.push({ cluster: part, score, parentSize: cluster.length });
    }
  }
  return out.sort((a, b) => b.score - a.score);
}
