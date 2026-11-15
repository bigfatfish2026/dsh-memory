/**
 * 账本胶水：pending_ops 的连接、merge_batch 记账、超长转 split、候选列表。
 * 判死与复活口径在 src/ledger.ts，这里只负责带上下文调用。
 */
import { enqueue, MAX_ATTEMPTS } from '../ingest-index.js';
import { recordAttempt } from '../ledger.js';
import { enqueueSplitItem } from '../split-queue.js';
import { relatedEntries } from '../related.js';
import type { DatabaseSync } from 'node:sqlite';
import type { MemoryClusterItem } from '../batch-planner.js';
import type { MemoryRecord, MemoryView } from '../store/types.js';
import type { KeyLogConfig } from '../file-log.js';

/** env 里本文件用到的面：一个取库句柄、一份配置、一个日志出口。 */
interface LedgerEnv {
  store: { db: () => DatabaseSync };
  cfg: KeyLogConfig;
  log: (line: string) => void;
}

/** merge agent 产出的一个动作（超长时转 split）。 */
export interface MergeOp {
  ids: string[];
  text?: string;
  tags?: string[];
  type?: string;
  op?: string;
  index?: number;
  reason?: string;
  [key: string]: unknown;
}

/** 记账结果：id 为 null 表示 dryRun 或没有库。 */
interface BumpResult {
  id?: string | number | null;
  attempts: number;
  dead: boolean;
}

export function createLedgerGlue(env: LedgerEnv) {
  const { store, cfg } = env;
  const log = (_c: KeyLogConfig, line: string) => env.log(line);

// ── 合并 agent 路径（2026-11-05，开关 pipeline.agentPipeline，默认关）──────────────

/** pending_ops 落盘用；库连不上就返回 null，调用方只记日志，不静默吞。 */
function pendingDb(): DatabaseSync | null {
  try {
    return store.db();
  } catch {
    return null;
  }
}

/**
 * 模型调用失败 / 空响应 / 撞墙 / 解析失败 → pending_ops(kind='merge_batch')，
 * attempts 累加、last_error 记原文摘要，**不自动放弃**。
 *
 * 旧版第 3 次就转 dead，等于「试固定次数就丢活」；现在只记账，下一轮仍会领到它重试，
 * 直到有人处理。失败绝不写 organizeCache 当 noop；模型明确给 {"ops":[]} 的批算 noop
 * （有效结论），由调用方直接写缓存、不进这里。
 *
 * lastError 带模型原文摘要（finishReason / completionTokens / reasoningTokens / reasoningChars /
 * 两通道前 160 字），05:00 那种「成片解析失败或空响应」事后能直接从账本里看出谁吃光了预算。
 */
function bumpMergeBatch(fp: string, cluster: MemoryClusterItem[], lastError: unknown, { dryRun = false }: { dryRun?: boolean } = {}): BumpResult {
  const db = pendingDb();
  const ids = cluster.map((m) => String(m.id));
  const message = String(lastError).slice(0, 500);
  if (!db) {
    log(cfg, `[merge] WARN 没有 pending_ops，merge_batch 只记日志 fp=${fp} last_error=${message} ids=${ids.join(',')}`);
    return { attempts: 0, dead: false };
  }
  const payload = {
    batch: fp,
    // 成员顺序按 id 排死：同一批人同一批正文，payload_json 就得逐字节一样（见下面 reviveDead）
    ids: [...ids].sort(),
    items: cluster
      .map((m) => ({ id: String(m.id), text: String(m.text || '') }))
      .sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0)),
    // 摘要照旧写进 payload 留给人看（test/merge-truncate-replay 盯着它）；
    // 但 reviveDead 判「新证据」时只比 batch/ids/items（reviveKeys），不看这段每轮都变的摘要
    reason: message,
  };
  try {
    // reviveDead：老账 attempts 不清零，只有 payload 变了（成员/正文变了的新证据）才复活。
    // 2026-11-14：reviveKeys 把 reason（失败摘要，raw/finish/usage 每轮都变）
    // 排除在比较之外——否则判死的账下一轮同样的批再失败一次就被当成「新证据」复活，
    // attempts 归零重来，5 次判死等于不存在，那 75 条就是这么一直转的。
    const row = enqueue(db, 'merge_batch', fp, payload, {
      dryRun,
      reviveDead: true,
      reviveKeys: ['batch', 'ids', 'items'],
      log: (line) => log(cfg, line),
    });
    if (dryRun || row?.dryRun) return { id: null, attempts: 0, dead: false };
    const now = Date.now();
    // 失败判死（2026-11-14）：attempts 到 5 就不自动再领，标 dead 留给人看；
    // 没到就留在 pending，next_try_at=now（不排退避时间），下一轮再试。口径在 ledger.recordAttempt。
    const rec = recordAttempt({ attempts: row?.attempts as number | undefined, maxAttempts: MAX_ATTEMPTS, error: message, now });
    const attempts = rec.attempts;
    const dead = rec.dead;
    db.prepare(
      'UPDATE pending_ops SET attempts = ?, status = ?, last_error = ?, next_try_at = ?, updated_ts = ? WHERE id = ?',
    ).run(attempts, rec.status, rec.message, now, now, row.id as string | number);
    if (dead) {
      log(cfg, `[merge] WARN merge_batch dead fp=${fp} attempts=${attempts} last_error=${message} ids=${ids.join(',')}`);
    } else {
      log(cfg, `[merge] merge_batch pending fp=${fp} attempts=${attempts} last_error=${message} ids=${ids.join(',')}`);
    }
    return { id: row.id as string | number, attempts, dead };
  } catch (error) {
    log(cfg, `[merge] WARN merge_batch 入队失败 fp=${fp}：${String(((error as Error | null) && (error as Error).message) || error)}`);
    return { attempts: 0, dead: false };
  }
}

/**
 * 超长正文交给拆分员：条目 id + 原文进 pending_ops(kind='split_item')，不写库、不截断。
 * 2026-11-05 t-fix-first-run：入队逻辑统一到 src/split-queue.ts，这里只是带上本批指纹的一层包装。
 */
function queueMergeAgentSplit(op: MergeOp, fp: string, { dryRun = false }: { dryRun?: boolean } = {}): string | null {
  const sourceId = op.ids[0] || `merge:${fp}:${op.index}`;
  const queued = enqueueSplitItem(pendingDb(), {
    sourceId,
    text: op.text,
    ids: op.ids,
    tags: op.tags,
    type: op.type,
    op: op.op,
    batch: fp,
    reason: op.reason || 'over_max',
    dryRun,
    log: (line) => log(cfg, line),
  });
  return queued ? sourceId : null;
}

/** 提示词里的「近似候选」：同实体、不在本批、最多 8 条。 */
function candidateListFor(cluster: MemoryClusterItem[], active: MemoryView[]) {
  const inBatch = new Set(cluster.map((m) => String(m.id)));
  const out = new Map<string, { id: string; text: string; score: number }>();
  for (const m of cluster) {
    for (const hit of relatedEntries(String(m.text || ''), (m.entities || []).map(String), active as unknown as MemoryRecord[])) {
      const id = String(hit.id);
      if (inBatch.has(id) || out.has(id)) continue;
      out.set(id, { id, text: String(hit.text || ''), score: 0 });
      if (out.size >= 8) break;
    }
    if (out.size >= 8) break;
  }
  return [...out.values()];
}

  return { pendingDb, bumpMergeBatch, queueMergeAgentSplit, candidateListFor };
}
