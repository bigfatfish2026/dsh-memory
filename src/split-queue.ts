/**
 * 超长正文的唯一出口（2026-11-05，t-fix-first-run）。
 *
 * 本文件只有一件事：把「写记忆时发现正文超过上限」的条目**原样**塞进
 * pending_ops(kind='split_item')，交给拆分 agent（src/pipeline-split.ts + src/agents/split.ts）。
 *
 * 铁律（票里定的）：
 *   - 所有写记忆的路径遇到超长一律**不丢、不截**；
 *   - 超长条目只能走这一条出口，别再各写一套 enqueue；
 *   - 入队失败要大声报（调用方拿到 null 自己记 WARN 并进 rejected），不许静默吞。
 *
 * payload 形状与 pipeline-split 的消费端一致：
 *   {source_id, text, ids, tags, type, op, batch, length, reason}
 *   source_id 为空表示「没有原条可归档」（consolidate / memory_merge 这类 add 候选），
 *   消费端会直接用 payload.text 拆，拆完不归档任何条目。
 */
import type { DatabaseSync } from 'node:sqlite';
import { sha1Hex } from './fingerprint.js';
import { enqueue } from './ingest-index.js';
import type { Row } from './store/types.js';

export const SPLIT_KIND = 'split_item';

/** 入队一条待拆分正文的字段：text 原样必填，其余按来源可选。 */
export interface SplitQueueItem {
  sourceId?: string;
  text?: string;
  ids?: string[];
  tags?: string[];
  type?: string;
  op?: string;
  batch?: string;
  reason?: string;
  dryRun?: boolean;
  log?: (line: string) => void;
  now?: number;
}

/** 入队结果：没有库 / 入队抛错时整个返回 null，row 是 pending_ops 行。 */
export interface SplitQueueResult {
  key: string;
  row: Row | null;
}

/** 没有源 id 时，用正文 sha1 前 16 位当 dedupe_key：同一条超长正文重复入队只刷新，不叠加。 */
export function splitQueueKey({ sourceId = '', text = '' }: { sourceId?: string; text?: string } = {}): string {
  const id = String(sourceId || '').trim();
  if (id) return id;
  // 用原始正文（不归一化）：两条不同长文不能被归一化成同一个键
  return `split:${sha1Hex(text, { len: 16 })}`;
}

/**
 * 入队一条待拆分的超长正文。
 *
 * @param {import('node:sqlite').DatabaseSync|null} db pending_ops 所在的库（store.db()）；没有就返回 null
 * @param {object} item
 * @param {string} [item.sourceId] 原条 id；add 候选没有原条就留空
 * @param {string} item.text 原样正文（一个字都不许截）
 * @param {string[]} [item.ids] 涉及的原条 id（merge 时是一批）
 * @param {string[]} [item.tags]
 * @param {string} [item.type]
 * @param {string} [item.op] merge | update | add
 * @param {string} [item.batch] 哪条路径入队的：sleep / sleep:merge / consolidator / memory_merge
 * @param {string} [item.reason] 为什么入队，默认 over_max
 * @param {boolean} [item.dryRun]
 * @param {(line:string)=>void} [item.log]
 * @param {number} [item.now]
 * @returns {{key:string, row:object|null}|null} 入队成功返回 key；没有库 / 入队抛错返回 null
 */
export function enqueueSplitItem(db: DatabaseSync | null, item: SplitQueueItem = {}): SplitQueueResult | null {
  const text = String(item.text ?? '');
  const sourceId = String(item.sourceId ?? '').trim();
  const log = typeof item.log === 'function' ? item.log : null;
  if (!text.trim()) {
    log?.('[split-queue] 正文为空，不入队');
    return null;
  }
  if (!db) {
    log?.(`[split-queue] 没有 pending_ops 可落，超长正文没处去（id=${sourceId || '-'} ${text.length}字）`);
    return null;
  }
  const key = splitQueueKey({ sourceId, text });
  const payload = {
    source_id: sourceId,
    text,
    ids: (Array.isArray(item.ids) ? item.ids : []).map(String),
    tags: (Array.isArray(item.tags) ? item.tags : []).map(String),
    type: String(item.type || ''),
    op: String(item.op || 'add'),
    batch: String(item.batch || ''),
    length: text.length,
    reason: String(item.reason || 'over_max'),
  };
  try {
    const row = enqueue(db, SPLIT_KIND, key, payload, {
      now: Number(item.now) || Date.now(),
      dryRun: Boolean(item.dryRun),
      log,
    });
    return { key, row: row || null };
  } catch (error) {
    log?.(`[split-queue] 入队失败 id=${sourceId || '-'}：${String((error as Error && (error as Error).message) || error as string)}`);
    return null;
  }
}

export default enqueueSplitItem;
