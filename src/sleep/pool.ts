/**
 * 队列 + 池的规划与 compact 通道：只入队不消费，池按通道小批领活。
 */
import path from 'node:path';
import { scanSessions, planFiles, bootstrapLegacy, enqueue, markOp, LEASE_MS, claimChannelOps, reapChannelOps, channelStats } from '../ingest-index.js';
import { runEntityTidyPass, writeEntityTidyReport } from '../entity-tidy.js';
import { createChannelPool, runListPool } from '../pool.js';
import { kindsOfPool } from '../kind-handlers.js';
import { merge as askMergeOps, validateOps, groupOpsOf } from '../agents/merge.js';
import { llmOf } from '../llm-service.js';
import { isAutoObservation } from '../memory-link.js';
import { dayKey } from './slots.js';
import { CLUSTER_PROMPT_SHELL_TOKENS } from './batching.js';
import { cheapSimilarity, cosine } from '../batch-planner.js';
import {
  readItemReviewCache, pairReviewed, shrinkClusterForReview, reviewAdditionsFromOps,
} from '../item-review-cache.js';
import type { ReviewItem, ItemReviewCache } from '../item-review-cache.js';
import type { DatabaseSync } from 'node:sqlite';
import type { DueOp } from '../ingest-index.js';
import type { MemoryRecord, MemoryView, SnapshotView, Row } from '../store/types.js';
import type { BatchPlanner } from '../batch-planner.js';
import type { PromptBudget } from '../token-window.js';
import type { SleepCfg, SleepCtx, SleepStore } from './env.js';
import { itemReviewNeighborsOf } from './env.js';
import type { SleepStateStore } from '../sleep-state.js';
import type { AgentOp, AgentClusterItem } from './agent-ops.js';
import type { ChannelPoolOptions, WriteLock, RunListPoolOptions, PoolResult } from '../pool.js';

/** 通道池的一行活（LedgerItem 的结构，够本文件用）。 */
export type PoolRowLike = { id: string | number; kind: string; payload: Row | null; dedupeKey: string; attempts?: number; [key: string]: unknown };

/** 本文件用到的 env 面。 */
interface PoolEnv {
  ctx: SleepCtx;
  store: SleepStore;
  cfg: SleepCfg;
  stateStore: SleepStateStore;
  log: (line: string) => void;
  ledger: {
    pendingDb: () => DatabaseSync | null;
    candidateListFor: (cluster: AgentClusterItem[], active: MemoryView[]) => Array<{ id: string; text: string; score: number }>;
  };
  agentOps: { applyAgentOps: (ops: AgentOp[], opts: { cluster: AgentClusterItem[]; dryRun: boolean; fp: string }) => Promise<{ merged: unknown[]; updated: unknown[]; archived: unknown[]; added: unknown[]; rejected: unknown[]; split: unknown[] }> };
  replay: { replayPendingOps: (opts: { dryRun?: boolean; onlyOps?: DueOp[] | null }) => Promise<Record<string, unknown>> };
  actions: { revocationPass: (opts?: { dryRun?: boolean }) => Promise<{ scanned: number; archived: unknown[]; skipped: unknown[]; orphan: unknown[] }> };
  scheduler: {
    putMergeResult: (label: string, value: { v: string; t: number }) => unknown;
    /** 本轮条目级复审结论（收尾并进 state 的 itemReviewCache）；老装配没这个方法就跳过。 */
    putItemReview?: (additions: ItemReviewCache) => unknown;
  };
  activeSnap: () => SnapshotView;
  budgetOf: () => PromptBudget;
  batchPlannerOf: (budgetTokens?: number) => BatchPlanner;
  ingest: { shortErr: (error: unknown) => string };
}

/** merge_batch 的 prepare 产物。 */
export interface MergeBox {
  batchLabel: string;
  cluster?: MemoryView[];
  groups?: MemoryView[][] | null;
  answer?: { ops?: unknown[] } | null;
  kind?: string;
  summary?: string;
  skip?: string;
  kindMismatch?: boolean;
  error?: unknown;
}

/** 实体整理的汇报面。 */
export interface TidyStats {
  runId: string;
  groups: number;
  merges: number;
  surface: number;
  mount: number;
  report: number;
  applied: number;
  pairs: unknown[];
  skipped: number;
  reportFile: string;
}

/** merge_batch 落地的结果面（commitPackedMerge / commitMergeBatchOp 共用）。 */
export interface MergeCommitResult {
  applied?: { merged: unknown[]; updated: unknown[]; archived: unknown[]; added: unknown[]; split: unknown[]; rejected: unknown[] };
  failed?: unknown;
  acted?: number;
  skip?: string;
  noop?: boolean;
  kindMismatch?: unknown;
  degraded?: number;
}

/** compact 通道的计数。 */
export interface CompactStats {
  llm: number;
  local: number;
  ops: number;
  revoked: number;
  failed: number;
  archived: unknown[];
  replay: Record<string, unknown>;
  tidy?: TidyStats;
}

/** planPoolQueues 的计数。 */
export interface PlanCounts {
  files: number;
  ingest: number;
  merge: number;
  split: number;
  cached: number;
  batches: number;
  legacy: number;
  sweep: number;
  tidy?: number;
  funnel?: unknown;
}

/** 老调用方（测试、脚本）的转发名：文件级并发池实现已搬到 src/pool.ts。 */
export async function runIngestPool<T>(opts: RunListPoolOptions<T> = {}): Promise<Array<PoolResult>> {
  return runListPool(opts);
}

export function createPool(env: PoolEnv) {
  const { ctx, store, cfg, stateStore } = env;
  const log = (_c: SleepCfg, line: string) => env.log(line);
  const pendingDb = (...a: Parameters<PoolEnv['ledger']['pendingDb']>) => env.ledger.pendingDb(...a);
  const candidateListFor = (...a: Parameters<PoolEnv['ledger']['candidateListFor']>) => env.ledger.candidateListFor(...a);
  const applyAgentOps = (...a: Parameters<PoolEnv['agentOps']['applyAgentOps']>) => env.agentOps.applyAgentOps(...a);
  const replayPendingOps = (...a: Parameters<PoolEnv['replay']['replayPendingOps']>) => env.replay.replayPendingOps(...a);
  const revocationPass = (...a: Parameters<PoolEnv['actions']['revocationPass']>) => env.actions.revocationPass(...a);
  const activeSnap = (...a: Parameters<PoolEnv['activeSnap']>) => env.activeSnap(...a);
  const budgetOf = (...a: Parameters<PoolEnv['budgetOf']>) => env.budgetOf(...a);
  const batchPlannerOf = (...a: Parameters<PoolEnv['batchPlannerOf']>) => env.batchPlannerOf(...a);
  const shortErr = (...a: Parameters<PoolEnv['ingest']['shortErr']>) => env.ingest.shortErr(...a);

function pipelineModeOf(): string {
  return cfg.organizePipelineMode === 'pool' ? 'pool' : 'legacy';
}

/**
 * compact 通道：0 次 LLM，纯规则清扫。跟两个模型池同时跑，谁也不等谁。
 * 内容是「作废清扫 + merge_op/dedupe_check 两种本地重放 + organizeCache 轮转」。
 */
/**
 * compact 通道（2026-11-14 阶段 5）：本地重放 + 作废清扫 + cache 轮转。
 * 口径写实：这些活本身不问模型，但写库会过写入闸（writeGate=true 时闸里会问），
 * 所以计数分 llm_calls（闸里的调用）与 local_ops（本地动作），不再写「0 次 LLM，纯规则清扫」。
 */
function startCompactPool({ dryRun, db, writeLock = null }: { dryRun: boolean; db: DatabaseSync; writeLock?: WriteLock | null }): { pool: ReturnType<typeof createChannelPool>; stats: CompactStats } {
  const stats: CompactStats = { llm: 0, local: 0, ops: 0, revoked: 0, failed: 0, archived: [], replay: {} };
  const pool = createChannelPool({
    channel: 'compact',
    concurrency: 1,
    writeLock,
    log: (line: string) => log(cfg, line),
    claim: (n: number, exclude: number[]) => claimChannelOps(db, 'compact', n, { leaseMs: LEASE_MS, kinds: kindsOfPool('compact'), exclude }) as unknown as PoolRowLike[],
    reap: () => reapChannelOps(db, { leaseMs: LEASE_MS, channel: 'compact' }),
    remaining: () => channelStats(db, 'compact').pending,
    prepare: (op: PoolRowLike) => ({ op }),
    commit: async (op: PoolRowLike) => {
      const t0 = Date.now();
      const before = store.gateStats ? store.gateStats() : { llm: 0 };
      let local = 1;
      if (op.kind === 'revocation_sweep') {
        const rev = await revocationPass({ dryRun });
        local = Math.max(1, rev.archived.length);
        stats.revoked += rev.archived.length;
        stats.archived.push(...(rev.archived || []));
        if (!dryRun) markOp(db, op.id, 'done', null, { log: (line: string) => log(cfg, line) });
      } else if (op.kind === 'entity_tidy') {
        // 三期实体整理：每晚扫一遍实体候选。apply=false 只报不合；true 才自动合
        // fold_same / surface_cross 这两类名字层面确定同义的。
        // 合库/手工兜底仍可走 scripts/entity-tidy.mjs（先 dry-run，再 --apply，带备份与撤销凭据）。
        const tidyRunId = `sleep_${dayKey(new Date())}`;
        const tidy = runEntityTidyPass(db, {
          apply: !dryRun && cfg.entityTidy?.apply === true,
          dryRun,
          runId: tidyRunId,
          log: (line: string) => log(cfg, line),
        });
        local = Math.max(1, tidy.stats.autoGroups + tidy.stats.reportPairs);
        // 汇报要求（2026-11-15）：合并了哪些对、跳过哪些候选、撤回命令，都落一份 md。
        // dry-run 轮不写，免得把没发生的合并写进报告。
        let tidyReportFile = '';
        if (!dryRun) {
          try {
            tidyReportFile = writeEntityTidyReport(path.join(cfg.storePath, 'entity-tidy-report.md'), {
              runId: tidyRunId,
              scan: tidy,
              applied: tidy.applied,
              log: (line: string) => log(cfg, line),
            });
          } catch (error) {
            // 报告写不出来不许挡账：错在账上，下一轮会重写。
            log(cfg, `[entity-tidy] 报告写入失败 run_id=${tidyRunId}：${String((error as Error | null)?.message || error)}`);
          }
        }
        stats.tidy = {
          runId: tidyRunId,
          groups: tidy.stats.autoGroups,
          merges: tidy.stats.autoMerges,
          surface: tidy.stats.surfaceMoves,
          mount: tidy.stats.mountMoves,
          report: tidy.stats.reportPairs,
          applied: tidy.applied ? tidy.applied.merged : 0,
          pairs: tidy.applied?.pairs || [],
          skipped: tidy.applied?.skipped || 0,
          reportFile: tidyReportFile,
        };
        if (!dryRun) markOp(db, op.id, 'done', null, { log: (line: string) => log(cfg, line) });
      } else {
        const out = await replayPendingOps({ dryRun, onlyOps: [op] as unknown as DueOp[] });
        stats.replay[op.kind] = out[op.kind] || null;
      }
      const after = store.gateStats ? store.gateStats() : before;
      const llm = Math.max(0, (Number(after.llm) || 0) - (Number(before.llm) || 0));
      stats.llm += llm;
      stats.local += local;
      stats.ops += 1;
      log(cfg, `[compact] op=${op.kind} id=${op.id} llm=${llm} local=${local} ms=${Date.now() - t0}`);
    },
    release: (op: PoolRowLike, error: unknown) => {
      stats.failed += 1;
      markOp(db, op.id, 'pending', shortErr(error), { dryRun, log: (line: string) => log(cfg, line) });
    },
  } as unknown as ChannelPoolOptions);
  return { pool, stats };
}



/**
 * 规划：把这一轮该干的活写进 pending_ops（只入队，不消费）。
 * ingest 通道 = 文件对不上账的会话文件；curate 通道 = 聚类出来的合并批 + 超长待拆条目。
 */
function planPoolQueues(db: DatabaseSync, { dryRun, lastSleepAt }: { dryRun: boolean; lastSleepAt: number }): PlanCounts {
  const out: PlanCounts = { files: 0, ingest: 0, merge: 0, split: 0, cached: 0, batches: 0, legacy: 0, sweep: 0 };
  // ① ingest：扫文件 + bootstrap + planFiles（与 legacy 同一份判定，不另写一套）
  const files = scanSessions(cfg.sessionsRoot);
  out.files = files.length;
  const rows = Number(db.prepare('SELECT count(*) AS n FROM ingest_files').get()?.n ?? 0);
  if (!rows && files.length) {
    const boot = bootstrapLegacy(db, files, lastSleepAt, { dryRun, log: (line: string) => log(cfg, line) });
    out.legacy = boot.legacy;
  }
  const plan = planFiles(db, files, { lastSleepAt });
  if (plan.legacy.length) {
    const boot = bootstrapLegacy(db, plan.legacy, lastSleepAt, { dryRun, log: (line: string) => log(cfg, line) });
    out.legacy += boot.legacy;
  }
  for (const item of plan.todo) {
    enqueue(db, 'ingest_file', item.path, { path: item.path }, { dryRun, log: (line: string) => log(cfg, line) });
    out.ingest += 1;
  }

  // ② curate：聚类 → 合并批。聚类是纯本地 CPU（4020 条约 25s），只在这里跑一次。
  // 一轮一次快照：写库后 store 版本 +1，下一次取自动重读（别在批间拿旧视图）
  const snap = activeSnap();
  const all = snap.list;
  const vecOf = snap.vecOf;
  const { window: promptWindow, budget: promptTokens } = budgetOf();
  const batchBudget = promptWindow ? Math.max(0, promptTokens - CLUSTER_PROMPT_SHELL_TOKENS) : 0;
  // 与两处 organizePass 共用同一份「聚类→切批→漏斗→增量复审→查缓存」；缓存命中的批不进队。
  const batchPlan = batchPlannerOf(batchBudget).plan({
    all,
    vecOf,
    cache: stateStore.load().organizeCache || {},
    itemCache: readItemReviewCache(stateStore),
  });
  out.batches = batchPlan.batches.length;
  out.funnel = batchPlan.funnel;
  out.cached += batchPlan.cached;
  for (const item of batchPlan.picked) {
    const fp = item.fp;
    const payload = {
      batch: fp,
      // 合批包才带 groups（id 对）；单对批不带，prompt 与解析走原路径
      ...(Array.isArray(item.groups)
        ? { groups: item.groups.map((g) => g.map((m) => String(m.id))) }
        : {}),
      ids: item.cluster.map((m) => String(m.id)).sort(),
      items: item.cluster
        .map((m) => ({ id: String(m.id), text: String(m.text || '') }))
        .sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0)),
      reason: 'pool_plan',
    };
    enqueue(db, 'merge_batch', fp as string, payload, {
      dryRun, reviveDead: true, reviveKeys: ['batch', 'ids', 'items'], log: (line: string) => log(cfg, line),
    });
    out.merge += 1;
  }

  // ③ curate 通道的拆分活：超长条目入 split_item（先判状态，别每轮把已完的又塞一遍）。
  // 自动观察不进普通拆分（2026-11-22）：它有自己的复核链路（按来源根算独立来源、够 2 条
  // 才升级），普通拆分产出的子段没有 derived_from 证据边。消费端（pipeline-split）再兜一层。
  const threshold = Number(cfg.splitMaxChars) || 200;
  // MemoryView 是瘦视图、不带 extra 里的 autoObservation 标记，从 store.metas() 取原始记录建 id 集合。
  // 测试桩没 metas 时退化成空集合（不排除），行为跟以前一致。
  const autoObservationIds = new Set<string>(
    (typeof store.metas === 'function' ? (store.metas() as MemoryRecord[]) : [])
      .filter((m) => isAutoObservation(m))
      .map((m) => String(m.id)),
  );
  let autoObservationSkipped = 0;
  for (const m of all) {
    const text = String(m.text || '');
    if (text.length <= threshold) continue;
    if (autoObservationIds.has(String(m.id))) {
      autoObservationSkipped += 1;
      continue;
    }
    enqueue(db, 'split_item', String(m.id), {
      source_id: String(m.id),
      text,
      type: store.typeOf(m),
      tags: (m.tags || []).map(String),
      entities: (m.entities || []).map(String),
      op: 'split',
      batch: 'pool',
    }, { dryRun, log: (line: string) => log(cfg, line) });
    out.split += 1;
  }
  if (autoObservationSkipped) {
    log(cfg, `[pool] split 入队跳过自动观察 ${autoObservationSkipped} 条（走观察复核链路，不进普通拆分）`);
  }

  // ④ compact：作废清扫每天一条（dedupe_key 带日期，幂等）；organizeCache 轮转在池收尾做。
  const today = dayKey(new Date());
  enqueue(db, 'revocation_sweep', `revocation:${today}`, { day: today }, {
    dryRun, reviveDead: true, reviveKeys: ['day'], log: (line: string) => log(cfg, line),
  });
  out.sweep = 1;
  // 三期：实体整理也是「每轮现算的规则活」，跟作废清扫同一档，一天一条。
  enqueue(db, 'entity_tidy', `entity_tidy:${today}`, { day: today }, {
    dryRun, reviveDead: true, reviveKeys: ['day'], log: (line: string) => log(cfg, line),
  });
  out.tidy = 1;
  return out;
}

/** merge_batch 的 prepare：只读 + 问模型（缓存命中/成员已失效的当场判掉，不问）。 */
async function prepareMergeBatchOp(op: DueOp): Promise<MergeBox> {
  // 防御：合并池只认 merge_batch。领活 SQL 已经按 kind 收了，这里再挡一次——
  // 别的 kind 被误领时绝不能按「成员不足 2 条」收口成 done（2026-11-15 的 stale_members 事故）。
  if (op?.kind !== 'merge_batch') return { kindMismatch: true, batchLabel: String(op?.dedupeKey || '') };
  const payload = op.payload || {};
  const ids = Array.isArray(payload.ids) ? payload.ids.map(String) : [];
  const batchLabel = String(payload.batch || op.dedupeKey || '');
  const cache: Record<string, { v?: string }> = stateStore.load().organizeCache as Record<string, { v?: string }> || {};
  const settled = cache[batchLabel];
  if (settled && (settled.v === 'noop' || settled.v === 'acted')) return { skip: `cache_${settled.v}`, batchLabel };
  const snap = activeSnap();
  const active = snap.list;
  const byId = snap.byId;
  let cluster = ids.map((id) => byId.get(id)).filter(Boolean) as unknown as MemoryView[];
  if (cluster.length < 2) return { skip: 'stale_members', batchLabel };
  const itemOf = (m: MemoryView) => ({
    id: String(m.id),
    type: store.typeOf(m),
    text: String(m.text || ''),
    ts: String(m.ts || m.validFrom || ''),
    entities: (m.entities || []).map(String),
  });
  // 合批包（payload.groups 是 id 对）：组内成员必须都还在，掉一条就整包降级成逐组单问，
  // 别让合批把「成员被并掉」的组一起带死。
  const groupIds = Array.isArray(payload.groups) && payload.groups.length > 1
    ? payload.groups.map((g) => (Array.isArray(g) ? g.map(String) : []))
    : null;
  // 增量复审（2026-11-23）：老 pending 行里记的是当时那批成员，重放/消费前按条目缓存再收一次——
  // 逐字未变、已审过的条目不再送审，只送变化项 + 它们最相关的旧邻居。全批都已审过就当场收口。
  const itemCache = readItemReviewCache(stateStore);
  const relate = (a: ReviewItem, b: ReviewItem): number => {
    const va = a?.vector || (snap.vecOf && snap.vecOf.get(String(a?.id)));
    const vb = b?.vector || (snap.vecOf && snap.vecOf.get(String(b?.id)));
    if (va && vb) {
      const c = cosine(va, vb);
      if (c !== null) return c;
    }
    return cheapSimilarity(a as never, b as never);
  };
  let groups = null;
  if (groupIds) {
    const resolved = groupIds.map((g) => g.map((id) => byId.get(id)).filter(Boolean) as unknown as MemoryView[]);
    if (resolved.every((g) => g.length === 2)) {
      // 组内这一对的关系审过才整组不送：两条各自在别的批里判过 ≠ 它们之间的对判过
      // （t6 复核 bug 1，2026-11-23）。没审过的组留下，两条一起送。
      const kept = resolved.filter((g) => !pairReviewed(itemCache, g[0] as unknown as ReviewItem, g[1] as unknown as ReviewItem));
      if (!kept.length) return { skip: 'item_settled', batchLabel };
      groups = kept;
      cluster = [...new Map(kept.flat().map((m) => [String(m.id), m])).values()];
    }
  }
  if (!groups) {
    const shrunk = shrinkClusterForReview(cluster as unknown as ReviewItem[], itemCache, {
      relate,
      neighborLimit: itemReviewNeighborsOf(cfg),
    });
    if (shrunk.cluster.length < 2) return { skip: 'item_settled', batchLabel };
    cluster = shrunk.cluster as unknown as MemoryView[];
  }
  const candidates = candidateListFor(cluster, active);
  const box: MergeBox = { batchLabel, cluster, groups, answer: null, kind: '', summary: '' };
  try {
    box.answer = await askMergeOps({
      items: cluster.map(itemOf),
      groups: groups ? groups.map((g) => g.map(itemOf)) : null,
      candidates,
      clusterId: batchLabel,
      llm: llmOf(ctx, { site: 'merge:pool', log: (line: string) => log(cfg, line), cfg }),
      provider: cfg.provider,
      model: cfg.model,
      log: (line: string) => log(cfg, line),
      db: pendingDb(),
      textLint: cfg.pipeline?.textLint !== false,
      lintReview: cfg.pipeline?.lintReview !== false,
      lintReviewTimeoutMs: cfg.pipeline?.lintReviewTimeoutMs,
      onFail: (summary: string, kind: string) => { box.summary = summary; box.kind = kind; },
    });
  } catch (error) {
    box.error = error;
  }
  return box;
}

/**
 * 合批包的 commit（2026-11-14，packSize 打开后才会走到）：
 * ops 按组分桶，跨组 op 作废；某组没给结论 / 给的 op 全 invalid 时，
 * 只把这一组降级成单对 merge_batch 重新入队，别的组照常落库——「只失败那组」。
 */
async function commitPackedMerge(op: DueOp, box: MergeBox, ops: unknown[]): Promise<MergeCommitResult> {
  const db = store.db();
  const batchLabel = String(box.batchLabel || op.dedupeKey || '');
  const groups = box.groups as MemoryView[][];
  const { byGroup, cross } = groupOpsOf(ops, groups.map((g) => g.map((m) => ({ id: String(m.id) }))));
  if (cross.length) {
    log(cfg, `[pack] batch=${batchLabel.slice(0, 12)} 跨组 op ${cross.length} 条作废（ids=${cross.map((o) => (o.ids || []).join('+')).join(',')}）`);
  }
  let acted = 0;
  const merged: { merged: unknown[]; updated: unknown[]; archived: unknown[]; added: unknown[]; split: unknown[]; rejected: unknown[]; [key: string]: unknown[] } = { merged: [], updated: [], archived: [], added: [], split: [], rejected: [] };
  let degraded = 0;
  for (let i = 0; i < groups.length; i += 1) {
    const gItems = groups[i].map((m) => ({ id: String(m.id), text: String(m.text || '') }));
    const gOps = byGroup.get(i) || [];
    let done = 0;
    if (gOps.length) {
      const checked = validateOps(gOps, gItems, {
        maxChars: Number(cfg.splitMaxChars) || 200,
        db: pendingDb(),
        batchId: `${batchLabel}#g${i + 1}`,
        log: (line: string) => log(cfg, line),
      });
      if (checked.valid.length) {
        const applied = await applyAgentOps(checked.valid, { cluster: groups[i], dryRun: false, fp: `${batchLabel}#g${i + 1}` });
        for (const k of Object.keys(merged)) if (Array.isArray(applied[k as keyof typeof applied])) merged[k].push(...applied[k as keyof typeof applied]);
        done = applied.merged.length + applied.updated.length + applied.archived.length
          + applied.added.length + applied.split.length;
        // 条目级结论：只有真的进过校验、模型明确点到的 id 才记；校验被拒/写入闸打回的一条不记
        env.scheduler.putItemReview?.(reviewAdditionsFromOps(groups[i] as unknown as ReviewItem[], checked.valid, {
          rejected: [
            ...checked.invalid.map((x) => ({ ids: x.ids })),
            ...applied.rejected as { ids?: unknown; id?: unknown }[],
          ],
        }));
        acted += done;
      }
    }
    if (!done) {
      // 这组没落成：降级成单对请求，独立 attempts；整包不因它卡住
      const [a, b] = groups[i];
      const label = `single:${a.id}+${b.id}`;
      enqueue(db, 'merge_batch', label, {
        batch: label,
        ids: [String(a.id), String(b.id)].sort(),
        items: [a, b].map((m) => ({ id: String(m.id), text: String(m.text || '') }))
          .sort((x, y) => (x.id < y.id ? -1 : 1)),
        reason: 'pack_degrade',
      }, {
        dryRun: false, reviveDead: true, reviveKeys: ['batch', 'ids', 'items'], log: (line: string) => log(cfg, line),
      });
      degraded += 1;
    }
  }
  env.scheduler.putMergeResult(batchLabel, { v: acted ? 'acted' : 'noop', t: Date.now() });
  markOp(db, op.id, 'done', degraded ? `pack_degraded=${degraded}` : (acted ? null : 'noop'), { log: (line: string) => log(cfg, line) });
  log(cfg, `[pack] batch=${batchLabel.slice(0, 12)} 组 ${groups.length} 落 ${groups.length - degraded} 降级 ${degraded} 动作 ${acted}`);
  return { acted, applied: merged, degraded };
}

/** merge_batch 的 commit：校验 + 落地 + 写 organizeCache（串行，写库只有这一条线）。 */
async function commitMergeBatchOp(op: DueOp, box: MergeBox | null | undefined): Promise<MergeCommitResult> {
  const batchLabel = String(box?.batchLabel || op.dedupeKey || '');
  if (box?.kindMismatch) {
    // 不是本池的活：原样放回 pending，不记 attempts、不写 done。下一轮由该 kind 的消费者领。
    markOp(store.db(), op.id, 'pending', 'wrong_pool_kind', { increment: false, log: (line: string) => log(cfg, line) });
    log(cfg, `[curate] WARN op id=${op.id} kind=${op.kind} 被合并池领到，已原样放回 pending`);
    return { kindMismatch: op.kind };
  }
  if (box?.skip) {
    markOp(store.db(), op.id, 'done', box.skip, { log: (line: string) => log(cfg, line) });
    return { skip: box.skip };
  }
  if (box?.error) throw box.error;
  const answer = box?.answer;
  if (!answer) {
    const reasonKind = box?.kind === 'empty'
      ? 'empty_response'
      : box?.kind === 'reasoning_exhausted' ? 'reasoning_exhausted' : 'parse_fail';
    markOp(store.db(), op.id, 'pending', `${reasonKind} ${String(box?.summary || '').slice(0, 300)}`, { log: (line: string) => log(cfg, line) });
    return { failed: reasonKind };
  }
  const ops = Array.isArray(answer.ops) ? answer.ops : [];
  if (!ops.length) {
    // 模型明确空 ops = 有效 noop：记缓存、终态 done，不耗 attempts。
    // 条目级也记成「整批显式无关」（noop + 同批组键），这是唯一允许整批记 noop 的情形。
    env.scheduler.putMergeResult(batchLabel, { v: 'noop', t: Date.now() });
    env.scheduler.putItemReview?.(reviewAdditionsFromOps(box.cluster as unknown as ReviewItem[], []));
    markOp(store.db(), op.id, 'done', 'noop', { log: (line: string) => log(cfg, line) });
    return { noop: true };
  }
  if (Array.isArray(box?.groups) && box.groups.length > 1) {
    return commitPackedMerge(op, box, ops);
  }
  const items = [...box.cluster as MemoryView[], ...candidateListFor(box.cluster as MemoryView[], activeSnap().list)]
    .map((m) => ({ id: String(m.id), text: String(m.text || '') }));
  const checked = validateOps(ops, items, {
    maxChars: Number(cfg.splitMaxChars) || 200,
    db: pendingDb(),
    batchId: batchLabel,
    log: (line: string) => log(cfg, line),
  });
  if (!checked.valid.length) {
    markOp(store.db(), op.id, 'pending', 'all_ops_invalid', { log: (line: string) => log(cfg, line) });
    return { failed: 'all_ops_invalid' };
  }
  const applied = await applyAgentOps(checked.valid, { cluster: box.cluster as MemoryView[], dryRun: false, fp: batchLabel });
  const acted = applied.merged.length + applied.updated.length + applied.archived.length
    + applied.added.length + applied.split.length;
  env.scheduler.putMergeResult(batchLabel, { v: acted ? 'acted' : 'noop', t: Date.now() });
  // 条目级结论：只有模型明确点到的 id 才记；校验被拒/被写入闸挡下/没提到的一条不记
  env.scheduler.putItemReview?.(reviewAdditionsFromOps(box.cluster as unknown as ReviewItem[], checked.valid, {
    rejected: [
      ...checked.invalid.map((x) => ({ ids: x.ids })),
      ...applied.rejected as { ids?: unknown; id?: unknown }[],
    ],
  }));
  markOp(store.db(), op.id, acted ? 'done' : 'pending', acted ? null : 'noop', { log: (line: string) => log(cfg, line) });
  return { acted, applied };
}

  return {
    pipelineModeOf, startCompactPool, planPoolQueues,
    prepareMergeBatchOp, commitPackedMerge, commitMergeBatchOp,
  };
}
