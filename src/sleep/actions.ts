/**
 * 睡觉链的动作落库：add / merge / update / archive，以及纯规则的作废清扫。
 */
import { needsSplitText, clampLogLine } from '../text-limit.js';
import { enqueueSplitItem } from '../split-queue.js';
import { lintReviewMeta } from '../lint-review.js';
import {
  PROTECTED_TAGS, ARCHIVABLE_TAGS, ARCHIVABLE_PATTERN, REVOCATION_PATTERN,
} from './constants.js';
import type { DatabaseSync } from 'node:sqlite';
import type { MemoryRecord, MemoryView, SavedRecord, DiscardedRecord } from '../store/types.js';
import type { KeyLogConfig } from '../file-log.js';

/** 本文件用到的 store 面。 */
interface ActionsStore {
  typeOf: (m: MemoryRecord) => string;
  save: (text: unknown, tags: unknown, source: string | undefined, meta?: Record<string, unknown>) => Promise<SavedRecord | DiscardedRecord>;
  update: (id: string, patch?: Record<string, unknown>) => Promise<SavedRecord | DiscardedRecord>;
  archive: (id: string, reason?: string) => Promise<SavedRecord | DiscardedRecord>;
  merge: (ids: unknown, text: unknown, tags: unknown, meta?: Record<string, unknown>) => Promise<SavedRecord | DiscardedRecord>;
  metas: () => MemoryRecord[];
}

/** 本文件用到的 env 面。 */
interface ActionsEnv {
  store: ActionsStore;
  cfg: KeyLogConfig;
  log: (line: string) => void;
  ledger: { pendingDb: () => DatabaseSync | null };
}

/** 一个模型动作（add/merge/update/archive 共用一层皮）。 */
export interface ActionItem {
  text?: string;
  id?: string;
  ids?: string[];
  tags?: string[];
  type?: string;
  confidence?: number;
  entities?: unknown[];
  validFrom?: unknown;
  validTo?: unknown;
  supersedes?: unknown;
  reason?: string;
  [key: string]: unknown;
}

/** 一次 applyActions 的四个桶 + 拆分明细。 */
export interface ApplyResult {
  added: unknown[];
  merged: unknown[];
  updated: unknown[];
  archived: unknown[];
  rejected: unknown[];
  split: unknown[];
}

/** 一轮作废清扫的汇总。 */
export interface RevokeResult {
  scanned: number;
  archived: unknown[];
  skipped: unknown[];
  orphan: unknown[];
}

/** 超长正文转拆分的入参。 */
interface QueueSplitArg {
  sourceId: string;
  text: string;
  ids: string[];
  item: ActionItem;
  op: string;
  reason: string;
}

/** 逐条落地时传的上下文。 */
interface ActionCtx {
  byId: Map<string, MemoryView>;
  result: ApplyResult;
  dryRun: boolean;
  queueSplit: (arg: QueueSplitArg) => boolean;
  existing: MemoryView[];
}

export function createActions(env: ActionsEnv) {
  const { store, cfg } = env;
  const log = (_c: KeyLogConfig, line: string) => env.log(line);
  const pendingDb = (...a: Parameters<ActionsEnv['ledger']['pendingDb']>) => env.ledger.pendingDb(...a);

function tagsOf(m: { tags?: unknown[] }): string[] {
  return (m.tags || []).map((t) => String(t).toLowerCase());
}

const TYPES = ['world', 'experience', 'opinion', 'observation'];

function pickType(value: unknown, tags: unknown): string | undefined {
  const v = String(value || '').toLowerCase().trim();
  if (TYPES.includes(v)) return v;
  for (const t of (tags as unknown[]) || []) {
    const tv = String(t).toLowerCase();
    if (TYPES.includes(tv)) return tv;
  }
  return undefined;
}

function isProtected(m: { tags?: unknown[] }): boolean {
  return tagsOf(m).some((t) => PROTECTED_TAGS.includes(t));
}

async function applyActions(actions: { merge: ActionItem[]; update: ActionItem[]; add: ActionItem[]; archive: ActionItem[] }, { existing, dryRun }: { existing: MemoryView[]; dryRun: boolean }): Promise<ApplyResult> {
  const byId = new Map(existing.map((m) => [m.id, m]));
  const result: ApplyResult = { added: [], merged: [], updated: [], archived: [], rejected: [], split: [] };
  // 2026-11-14：删掉本轮动作数上限（maxChanges / clusterChanges）。
  // 模型给几条就落几条；幂等由 store 的判重、id 存在性校验和 archive 规则各自兜。

  // 2026-11-05 t-fix-first-run 改语义：正文超长一律**不丢不截**，原样进 pending_ops(kind='split_item')
  // 交给拆分 agent。这里的入队是写路径唯一出口，别退回「截断 / 丢弃」。
  const queueSplit = ({ sourceId, text, ids, item, op, reason }: QueueSplitArg): boolean => {
    const queued = enqueueSplitItem(pendingDb(), {
      sourceId,
      text,
      ids,
      tags: item?.tags,
      type: pickType(item?.type, item?.tags),
      op,
      batch: 'sleep',
      reason,
      dryRun,
      log: (line) => log(cfg, line),
    });
    log(cfg, clampLogLine('入队拆分', sourceId || ids?.join('+') || 'pending', text.length, text.length, text));
    result.split.push({ id: sourceId || null, ids: ids || [], op, length: text.length, queued: Boolean(queued) });
    return Boolean(queued);
  };
  const ctx: ActionCtx = { byId, result, dryRun, queueSplit, existing };
  await applyMergeOps(actions.merge, ctx);
  await applyUpdateOps(actions.update, ctx);
  await applyAddOps(actions.add, ctx);
  await applyArchiveOps(actions.archive, ctx);
  return result;
}

async function applyMergeOps(items: ActionItem[], ctx: ActionCtx): Promise<void> {
  const { byId, result, dryRun, queueSplit } = ctx;
  // 1) merge：多条合成一条，并把原条目标记 merged
  for (const item of items) {
    const ids = (item?.ids || []).map(String).filter((id) => byId.has(id));
    let text = String(item?.text || '').trim();
    if (ids.length < 2 || !text) {
      result.rejected.push({ kind: 'merge', reason: 'ids<2 或 text 为空' });
      continue;
    }
    if (needsSplitText(text)) {
        // 合并结果超长：原条一条都不动，合并后的正文原样入队等拆分
      if (!queueSplit({ sourceId: ids[0], text, ids, item, op: 'merge', reason: 'merge_over_max' })) {
        result.rejected.push({ kind: 'merge', reason: '超 200 字且入队失败（没有 pending_ops 可落）', ids });
      }
      continue;
    }
    if (dryRun) {
      result.merged.push({ ids, text });
      continue;
    }
    let rec;
    try {
      rec = await store.merge(ids, text, item.tags, {
        // 复核判 keep（误杀）的条目带 __lintReview 让 store 最后一道闸放行，原样入库
        ...lintReviewMeta(item),
        type: pickType(item.type, item.tags),
        confidence: item.confidence,
        entities: item.entities,
        validFrom: item.validFrom,
        validTo: item.validTo,
      });
    } catch (error) {
      // store 层 lint 兜底拒绝（code=LINT_REJECT）：这一条不入库，其余 op 照常走
      result.rejected.push({
        kind: 'merge',
        reason: (error as { code?: unknown } | null)?.code === 'LINT_REJECT' ? '正文 lint 不通过' : String((error as Error | null)?.message || error).slice(0, 120),
        ids,
      });
      continue;
    }
    if (rec.discarded) {
      result.rejected.push({ kind: 'merge', reason: '写入闸判重', ids });
      continue;
    }
    result.merged.push({ ids, text: rec.text, id: rec.id });
  }
}

async function applyUpdateOps(items: ActionItem[], ctx: ActionCtx): Promise<void> {
  const { byId, result, dryRun, queueSplit } = ctx;
  // 2) update：修订稳定条目的措辞
  for (const item of items) {
    const id = String(item?.id || '');
    const target = byId.get(id);
    let text = String(item?.text || '').trim();
    if (!target || !text) {
      result.rejected.push({ kind: 'update', reason: 'id 不存在或文本非法' });
      continue;
    }
    if (needsSplitText(text)) {
        // 修订后的正文超长：原条保持原文不动，新写法原样入队等拆分
      if (!queueSplit({ sourceId: id, text, ids: [id], item, op: 'update', reason: 'update_over_max' })) {
        result.rejected.push({ kind: 'update', reason: '超 200 字且入队失败（没有 pending_ops 可落）', id });
      }
      continue;
    }
    if (dryRun) {
      result.updated.push({ id, before: target.text, after: text });
      continue;
    }
    let rec;
    try {
      rec = await store.update(id, {
        text,
        ...lintReviewMeta(item),
        tags: item.tags,
        ...(pickType(item.type, item.tags) ? { type: pickType(item.type, item.tags) } : {}),
        ...(item.confidence === undefined ? {} : { confidence: item.confidence }),
        ...(item.entities === undefined ? {} : { entities: item.entities }),
        ...(item.validFrom === undefined ? {} : { validFrom: item.validFrom }),
        ...(item.validTo === undefined ? {} : { validTo: item.validTo }),
        ...(item.supersedes === undefined ? {} : { supersedes: item.supersedes }),
      });
    } catch (error) {
      result.rejected.push({
        kind: 'update',
        reason: (error as { code?: unknown } | null)?.code === 'LINT_REJECT' ? '正文 lint 不通过' : String((error as Error | null)?.message || error).slice(0, 120),
        id,
      });
      continue;
    }
    if (rec.discarded) {
      result.rejected.push({ kind: 'update', reason: '写入闸判重', id });
      continue;
    }
    result.updated.push({ id, before: target.text, after: rec.text });
  }
}

async function applyAddOps(items: ActionItem[], ctx: ActionCtx): Promise<void> {
  const { result, dryRun, queueSplit, existing } = ctx;
  // 3) add：新记忆
  for (const item of items) {
    let text = String(item?.text || '').trim();
    if (!text) {
      result.rejected.push({ kind: 'add', reason: '文本为空' });
      continue;
    }
    if (needsSplitText(text)) {
        // 新条超长：不写库、不截断，原样入队等拆分（拆分后子段才入库）
      if (!queueSplit({ sourceId: '', text, ids: [], item, op: 'add', reason: 'add_over_max' })) {
        result.rejected.push({ kind: 'add', reason: '超 200 字且入队失败（没有 pending_ops 可落）' });
      }
      continue;
    }
    if (existing.some((m) => m.text.trim() === text)) {
      result.rejected.push({ kind: 'add', reason: '重复' });
      continue;
    }
    if (dryRun) {
      result.added.push(text);
      continue;
    }
    let rec;
    try {
      rec = await store.save(text, item.tags, 'sleep', {
        ...lintReviewMeta(item),
        type: pickType(item.type, item.tags),
        confidence: item.confidence,
        entities: item.entities,
        validFrom: item.validFrom,
        validTo: item.validTo,
        supersedes: item.supersedes,
      });
    } catch (error) {
      result.rejected.push({
        kind: 'add',
        reason: (error as { code?: unknown } | null)?.code === 'LINT_REJECT' ? '正文 lint 不通过' : String((error as Error | null)?.message || error).slice(0, 120),
      });
      continue;
    }
    if (rec.discarded) {
      result.rejected.push({ kind: 'add', reason: '写入闸判重', matchedId: rec.matchedId || null });
      continue;
    }
    result.added.push(rec.text);
  }
}

async function applyArchiveOps(items: ActionItem[], ctx: ActionCtx): Promise<void> {
  const { byId, result, dryRun } = ctx;
  // 4) archive：只归档过期状态类条目，稳定条目一律拒绝
  for (const item of items) {
    const id = String(item?.id || '');
    const target = byId.get(id);
    if (!target) {
      result.rejected.push({ kind: 'archive', reason: 'id 不存在' });
      continue;
    }
    if (store.typeOf(target as unknown as MemoryRecord) === 'opinion') {
      result.rejected.push({ kind: 'archive', reason: 'opinion 不归档，改用 update 修订或降置信度' });
      continue;
    }
    if (isProtected(target)) {
      result.rejected.push({ kind: 'archive', reason: `受保护条目（${tagsOf(target).join(',')}）` });
      continue;
    }
    const archivable =
      tagsOf(target).some((t) => ARCHIVABLE_TAGS.includes(t)) ||
      ARCHIVABLE_PATTERN.test(target.text) ||
      // 2026-11-02：宣告式作废条目也算可归档，否则旧说法永远清不掉
      REVOCATION_PATTERN.test(String(target.text || ''));
    if (!archivable) {
      result.rejected.push({ kind: 'archive', reason: '非状态类条目，不归档' });
      continue;
    }
    if (dryRun) {
      result.archived.push({ id, reason: item.reason || '' });
      continue;
    }
    await store.archive(id, String(item.reason || '').slice(0, 200));
    result.archived.push({ id, reason: item.reason || '' });
  }
}


/**
 * 2026-11-02：宣告式作废清扫。纯规则、不调模型，独立预算，不受 applyActions 共享 budget 影响。
 * 起因：模型会写「某说法已作废」的宣告条，但被它作废的旧条还 active；宣告条自己又过不了
 * 「非状态类条目，不归档」那道否决，于是烂账永远清不掉。
 *
 * 2026-11-02 收紧：这里只清「旧条已经归档/合并掉」的空账宣告条，不动旧条。
 * supersedes 是正常换代，旧条留不留是人的判断，自动清会误伤「留备查」这类刻意保留的记录。
 *
 * 取数走 store.metas() 而不是 listAll：listAll 不吐 supersedes/supersededBy，往它里面加字段
 * 会顺着 memoryTexts 把喂模型的 prompt 撑大；metas() 是 store 内部原始记录，只读不改，零 prompt 影响。
 */
async function revocationPass({ dryRun = false }: { dryRun?: boolean } = {}): Promise<RevokeResult> {
  const out: RevokeResult = { scanned: 0, archived: [], skipped: [], orphan: [] };
  // 2026-11-14：宣告条清扫不再限条数（原 limit=20）
  const metas = store.metas();
  const byId = new Map(metas.map((m) => [String(m.id), m]));
  const live = (m: MemoryRecord | undefined) => String((m && m.status) || 'active') === 'active';
  const brief = (m: MemoryRecord | undefined) => String((m && m.text) || '').replace(/\s+/g, ' ').slice(0, 40);
  const archiveOne = async (id: string, reason: string): Promise<void> => {
    if (!dryRun) await store.archive(id, String(reason).slice(0, 200));
    out.archived.push({ id, reason });
    log(cfg, `作废清扫：归档 ${id} ${brief(byId.get(id))}`);
  };

  const decls = metas.filter((m) => live(m) && REVOCATION_PATTERN.test(String(m.text || '')));
  out.scanned = decls.length;
  for (const decl of decls) {
    // opinion 永不归档这条红线先守住
    if (store.typeOf(decl) === 'opinion') {
      out.skipped.push({ id: decl.id, reason: 'opinion 不归档' });
      continue;
    }
    if (isProtected(decl)) {
      out.skipped.push({ id: decl.id, reason: `受保护条目（${tagsOf(decl).join(',')}）` });
      continue;
    }
    const ids = (Array.isArray(decl.supersedes) ? decl.supersedes : String(decl.supersedes || '').split(','))
      .map((x) => String(x).trim())
      .filter((id) => id && id !== String(decl.id));
    const olds = ids.map((id) => byId.get(id)).filter(Boolean) as MemoryRecord[];
    if (!olds.length) {
      // supersedes 为空或指向不存在的条目：不动，只记日志，交给人看
      out.orphan.push({ id: decl.id, supersedes: ids });
      log(cfg, `作废清扫：宣告条 ${decl.id} 找不到旧条（supersedes=${ids.join(',') || '空'}），留给人看：${brief(decl)}`);
      continue;
    }
    const dangling = ids.filter((id) => !byId.has(id));
    if (dangling.length) log(cfg, `作废清扫：宣告条 ${decl.id} 的 supersedes 指向不存在的 ${dangling.join(',')}`);
    const stillActive = olds.filter((m) => live(m));
    if (!stillActive.length) {
      // 旧条早就归档/合并掉了：这条宣告是空账，自己也没什么新事实
      await archiveOne(decl.id, '宣告式空账：被作废的旧条已归档');
      continue;
    }
    // 旧条还活着：一律不动。supersedes 是正常换代，旧条留不留是人的判断；
    // 自动清会误伤「留备查」这类刻意保留的记录。
    out.skipped.push({ id: decl.id, reason: `旧条仍 active：${stillActive.map((m) => m.id).join(',')}` });
    log(cfg, `作废清扫：跳过 ${decl.id}（旧条仍 active ${stillActive.map((m) => m.id).join(',')}）`);
    continue;
  }
  return out;
}

  return { tagsOf, pickType, isProtected, applyActions, revocationPass };
}
