/**
 * agent 提案落地：merge / update / archive / add，逐条写库、逐条隔离失败。
 */
import { lintReviewMeta } from '../lint-review.js';
import {
  PROTECTED_TAGS, ARCHIVABLE_TAGS, ARCHIVABLE_PATTERN, REVOCATION_PATTERN,
} from './constants.js';
import type { MemoryClusterItem } from '../batch-planner.js';
import type { MemoryRecord, MemoryView, SavedRecord, DiscardedRecord } from '../store/types.js';
import type { KeyLogConfig } from '../file-log.js';

/** 本文件用到的 store 面（MemoryStore 结构性满足）。 */
interface AgentOpsStore {
  listAll: () => MemoryView[];
  typeOf: (m: MemoryRecord) => string;
  save: (text: unknown, tags: unknown, source: string | undefined, meta?: Record<string, unknown>) => Promise<SavedRecord | DiscardedRecord>;
  update: (id: string, patch?: Record<string, unknown>) => Promise<SavedRecord | DiscardedRecord>;
  archive: (id: string, reason?: string) => Promise<SavedRecord | DiscardedRecord>;
  merge: (ids: unknown, text: unknown, tags: unknown, meta?: Record<string, unknown>) => Promise<SavedRecord | DiscardedRecord>;
}

/** 落地时用到的其它段：归档前算 tags、超长转拆分。 */
interface AgentOpsEnv {
  store: AgentOpsStore;
  cfg: KeyLogConfig;
  log: (line: string) => void;
  actions: { tagsOf: (m: MemoryClusterItem) => string[] };
  ledger: { queueMergeAgentSplit: (op: AgentOp, fp: string, opts?: { dryRun?: boolean }) => string | null };
}

/** 本批的一个成员：在 MemoryClusterItem 上补 tags/status（运行时本来就有）。 */
export interface AgentClusterItem extends MemoryClusterItem {
  tags?: unknown[];
  status?: string;
}

/** 证据校验后的一条 op。 */
export interface AgentOp {
  op?: string;
  ids: string[];
  text?: string;
  reason?: string;
  index?: number;
  needsSplit?: boolean;
  tags?: string[];
  type?: string;
  [key: string]: unknown;
}

/** 一批落地的计数与明细。 */
interface AgentOpResult {
  merged: unknown[];
  updated: unknown[];
  archived: unknown[];
  added: unknown[];
  rejected: unknown[];
  split: unknown[];
}

/** 逐条落地时传的上下文。 */
interface OpsCtx {
  byId: Map<string, AgentClusterItem>;
  result: AgentOpResult;
  active: MemoryView[];
  dryRun: boolean;
  fp: string;
}

export function createAgentOps(env: AgentOpsEnv) {
  const { store, cfg } = env;
  const log = (_c: KeyLogConfig, line: string) => env.log(line);
  const tagsOf = (...a: Parameters<AgentOpsEnv['actions']['tagsOf']>) => env.actions.tagsOf(...a);
  const queueMergeAgentSplit = (...a: Parameters<AgentOpsEnv['ledger']['queueMergeAgentSplit']>) => env.ledger.queueMergeAgentSplit(...a);

/**
 * 落地一批通过证据校验的 op。写库仍走 store.save/merge/update/archive：
 * 开关 pipeline.writeGate 打开时 store 内部会逐条过 assertWriteAllowed，两个开关互不牵连。
 * 超长正文这里一个字都不截，只转 split_item。
 */
async function applyAgentOps(ops: AgentOp[], { cluster, dryRun, fp }: { cluster: AgentClusterItem[]; dryRun: boolean; fp: string }): Promise<AgentOpResult> {
  const byId = new Map(cluster.map((m) => [String(m.id), m]));
  const active = store.listAll().filter((m) => (m.status || 'active') === 'active');
  const result: AgentOpResult = { merged: [], updated: [], archived: [], added: [], rejected: [], split: [] };
  // 2026-11-14：本轮动作数上限（clusterChanges）删了，提案全部落地。
  const ctx: OpsCtx = { byId, result, active, dryRun, fp };
  for (const op of ops) {
    if (op.needsSplit) { await applySplitOp(op, ctx); continue; }
    if (op.op === 'merge') { await applyMergeOp(op, ctx); continue; }
    if (op.op === 'update') { await applyUpdateOp(op, ctx); continue; }
    if (op.op === 'archive') { await applyArchiveOp(op, ctx); continue; }
    if (op.op === 'add') { await applyAddOp(op, ctx); }
  }
  return result;
}

async function applySplitOp(op: AgentOp, ctx: OpsCtx): Promise<void> {
  const { result, dryRun, fp } = ctx;
    if (op.needsSplit) {
      const sourceId = op.ids[0] || `merge:${fp}:${op.index}`;
      const queuedId = queueMergeAgentSplit(op, fp, { dryRun });
      result.split.push({ id: sourceId, op: op.op, length: String(op.text || '').length, queued: Boolean(queuedId) });
      if (!queuedId) {
        result.rejected.push({ kind: op.op, reason: '超 200 字且入队失败（没有 pending_ops 可落）', ids: op.ids });
      }
      log(cfg, `[merge] 超长正文转拆分 id=${sourceId} ${String(op.text || '').length}字（未截断）`);
      return;
    }

}

async function applyMergeOp(op: AgentOp, ctx: OpsCtx): Promise<void> {
  const { byId, result, dryRun, fp } = ctx;
    if (op.op === 'merge') {
      const ids = op.ids.filter((id) => byId.has(id));
      if (ids.length < 2) {
        result.rejected.push({ kind: 'merge', reason: 'id 不在本批或不足 2 条', ids: op.ids });
        return;
      }
      const tags = [...new Set(ids.flatMap((id) => (byId.get(id)?.tags || []).map(String)))];
      if (dryRun) {
        result.merged.push({ ids, text: op.text });
        return;
      }
      try {
        const rec = await store.merge(ids, op.text, tags, {
          // 复核判 keep 的 op 原样放行：带 __lintReview 让 store 最后一道闸放行
          ...lintReviewMeta(op),
          sourceFile: `sleep:merge:${fp}`,
        });
        if (rec.discarded) {
          result.rejected.push({ kind: 'merge', reason: '写入闸判重', ids, matchedId: rec.matchedId || null });
          return;
        }
        result.merged.push({ ids, text: rec.text, id: rec.id });
      } catch (error) {
        result.rejected.push({ kind: 'merge', reason: String(((error as Error | null) && (error as Error).message) || error).slice(0, 120), ids });
      }
      return;
    }

}

async function applyUpdateOp(op: AgentOp, ctx: OpsCtx): Promise<void> {
  const { byId, result, dryRun } = ctx;
    if (op.op === 'update') {
      const id = op.ids[0];
      if (!byId.has(id)) {
        result.rejected.push({ kind: 'update', reason: 'id 不在本批', id });
        return;
      }
      const before = String(byId.get(id)?.text || '');
      if (dryRun) {
        result.updated.push({ id, before, after: op.text });
        return;
      }
      try {
        const rec = await store.update(id, { text: op.text, ...lintReviewMeta(op) });
        if (rec.discarded) {
          result.rejected.push({ kind: 'update', reason: '写入闸判重', id });
          return;
        }
        result.updated.push({ id, before, after: rec.text });
      } catch (error) {
        result.rejected.push({ kind: 'update', reason: String(((error as Error | null) && (error as Error).message) || error).slice(0, 120), id });
      }
      return;
    }

}

async function applyArchiveOp(op: AgentOp, ctx: OpsCtx): Promise<void> {
  const { byId, result, dryRun } = ctx;
    if (op.op === 'archive') {
      const id = op.ids[0];
      const target = byId.get(id);
      if (!target) {
        result.rejected.push({ kind: 'archive', reason: 'id 不在本批', id });
        return;
      }
      if (store.typeOf(target as unknown as MemoryRecord) === 'opinion') {
        result.rejected.push({ kind: 'archive', reason: 'opinion 不归档，改用 update 修订', id });
        return;
      }
      const tags = tagsOf(target);
      if (tags.some((t) => PROTECTED_TAGS.includes(t))) {
        result.rejected.push({ kind: 'archive', reason: `受保护条目（${tags.join(',')}）`, id });
        return;
      }
      const archivable = tags.some((t) => ARCHIVABLE_TAGS.includes(t)) ||
        ARCHIVABLE_PATTERN.test(String(target.text || '')) ||
        REVOCATION_PATTERN.test(String(target.text || ''));
      if (!archivable) {
        result.rejected.push({ kind: 'archive', reason: '非状态类条目，不归档', id });
        return;
      }
      if (dryRun) {
        result.archived.push({ id, reason: op.reason || '' });
        return;
      }
      try {
        await store.archive(id, String(op.reason || 'merge agent').slice(0, 200));
        result.archived.push({ id, reason: op.reason || '' });
      } catch (error) {
        result.rejected.push({ kind: 'archive', reason: String(((error as Error | null) && (error as Error).message) || error).slice(0, 120), id });
      }
      return;
    }

}

async function applyAddOp(op: AgentOp, ctx: OpsCtx): Promise<void> {
  const { active, result, dryRun, fp } = ctx;
    if (op.op === 'add') {
      const text = String(op.text || '').trim();
      if (!text) {
        result.rejected.push({ kind: 'add', reason: '文本为空' });
        return;
      }
      if (active.some((m) => String(m.text || '').trim() === text)) {
        result.rejected.push({ kind: 'add', reason: '重复' });
        return;
      }
      if (dryRun) {
        result.added.push(text);
        return;
      }
      try {
        const rec = await store.save(text, [], 'sleep:merge', {
          ...lintReviewMeta(op),
          sourceFile: `sleep:merge:${fp}`,
        });
        if (rec.discarded) {
          result.rejected.push({ kind: 'add', reason: '写入闸判重', matchedId: rec.matchedId || null });
          return;
        }
        result.added.push(rec.text);
      } catch (error) {
        result.rejected.push({ kind: 'add', reason: String(((error as Error | null) && (error as Error).message) || error).slice(0, 120) });
      }
    }
}


  return { applyAgentOps };
}
