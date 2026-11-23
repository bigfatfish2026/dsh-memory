/**
 * 老分簇整理路径：聚类 → 切批 → 查缓存 → 并发问模型 → 逐批落地。
 */
import { parseActions } from '../transcript.js';
import { clusterForSleep } from '../cluster.js';
import { clusterBySimilarGraph } from '../similar-graph.js';
import { trimOrganizeCache, archiveDirOf } from '../organize-cache.js';
import {
  readItemReviewCache, persistItemReviewCache, mergeItemReviewCache, reviewAdditionsFromActions,
} from '../item-review-cache.js';
import { llmOf, isLlmUnavailable } from '../llm-service.js';
import { CLUSTER_PROMPT_SHELL_TOKENS } from './batching.js';
import { log } from './log.js';
import type { MemoryClusterItem, BatchPlanner } from '../batch-planner.js';
import type { MemoryRecord, MemoryView, SnapshotView } from '../store/types.js';
import type { PromptBudget } from '../token-window.js';
import type { SleepCfg, SleepCtx } from './env.js';
import type { SleepState, SleepStateStore } from '../sleep-state.js';
import type { LintRedone, LintRedoOpts, LintActions } from './lint.js';
import type { ActionItem } from './actions.js';
import type { CacheValue } from '../organize-cache.js';

/** 分簇只读的配置面。 */
interface ClusterCfg extends SleepCfg {
  clusterNearDup?: unknown;
  clusterMode?: unknown;
  clusterThreshold?: unknown;
  clusterNeighbors?: unknown;
}

/** 分簇整理用到的 env 面。 */
interface OrganizeEnv {
  ctx: SleepCtx;
  store: { listAll: () => MemoryView[] };
  cfg: ClusterCfg;
  stateStore: SleepStateStore;
  log: (line: string) => void;
  activeSnap: () => SnapshotView;
  budgetOf: () => PromptBudget;
  batchPlannerOf: (budgetTokens?: number) => BatchPlanner;
  scheduler: { curatePick: (channel: string) => { conc: number } };
  heartbeat: () => number;
  model: { askCluster: (cluster: MemoryClusterItem[], feedback?: string) => Promise<string> };
  lint: { lintRedoOnce: (opts: LintRedoOpts) => Promise<LintRedone> };
  actions: {
    applyActions: (
      actions: { merge: ActionItem[]; update: ActionItem[]; archive: ActionItem[]; add: ActionItem[] },
      opts: { existing: MemoryClusterItem[]; dryRun: boolean },
    ) => Promise<{ added: unknown[]; merged: unknown[]; updated: unknown[]; archived: unknown[]; rejected: unknown[] }>;
  };
  organizeAgent: { organizePassAgent: (opts?: { dryRun?: boolean }) => Promise<OrganizeResult> };
}

/** 一轮分簇整理的汇总。 */
interface OrganizeResult {
  total: number;
  clusters: number;
  queued: number;
  batches: number;
  promptWindow: number;
  batchBudget: number;
  calls: number;
  skippedSingles: number;
  skippedQuiet: number;
  cached: number;
  funnel: unknown;
  failed: number;
  merged: unknown[];
  updated: unknown[];
  archived: unknown[];
  rejected: unknown[];
  llmUnavailable?: boolean;
  splitInserted?: number;
  splitDeduped?: number;
  splitArchived?: number;
  [key: string]: unknown;
}

/** 候选生成：默认向量近邻图连通分量，clusterMode=entity 切回老路径。 */
export function buildSleepClusters(cfgArg: ClusterCfg, all: MemoryClusterItem[], vecOf: { get: (id: string) => Float32Array | number[] | undefined } | null): MemoryClusterItem[][] {
  const nearDup = Number(cfgArg.clusterNearDup) || 0.9;
  if (String(cfgArg.clusterMode || 'graph') === 'entity') {
    return clusterForSleep(all as unknown as MemoryRecord[], vecOf as never, { nearDup }) as unknown as MemoryClusterItem[][];
  }
  const got = clusterBySimilarGraph(all as unknown as MemoryRecord[], {
    threshold: Number(cfgArg.clusterThreshold ?? 0.79),
    neighbors: Number(cfgArg.clusterNeighbors ?? 8),
    nearDup,
    vectors: vecOf as never,
    log: (line) => log(cfgArg, line),
  });
  log(
    cfgArg,
    `[cluster] graph n=${got.stats.total} with_vector=${got.stats.withVector} groups=${got.stats.groups} `
      + `edges=${got.stats.edges} max=${got.stats.maxComponent} capped=${got.stats.oversized} `
      + `ms=${got.stats.ms} t=${got.stats.threshold} k=${got.stats.neighbors}`,
  );
  return got.clusters as unknown as MemoryClusterItem[][];
}
export function createOrganize(env: OrganizeEnv) {
  const { ctx, cfg, stateStore } = env;
  const log = (_c: ClusterCfg, line: string) => env.log(line);
  const activeSnap = (...a: Parameters<OrganizeEnv['activeSnap']>) => env.activeSnap(...a);
  const budgetOf = (...a: Parameters<OrganizeEnv['budgetOf']>) => env.budgetOf(...a);
  const batchPlannerOf = (...a: Parameters<OrganizeEnv['batchPlannerOf']>) => env.batchPlannerOf(...a);
  const curatePick = (...a: Parameters<OrganizeEnv['scheduler']['curatePick']>) => env.scheduler.curatePick(...a);
  const heartbeat = (...a: Parameters<OrganizeEnv['heartbeat']>) => env.heartbeat(...a);
  const askCluster = (...a: Parameters<OrganizeEnv['model']['askCluster']>) => env.model.askCluster(...a);
  const lintRedoOnce = (...a: Parameters<OrganizeEnv['lint']['lintRedoOnce']>) => env.lint.lintRedoOnce(...a);
  const applyActions = (...a: Parameters<OrganizeEnv['actions']['applyActions']>) => env.actions.applyActions(...a);
  const organizePassAgent = (...a: Parameters<OrganizeEnv['organizeAgent']['organizePassAgent']>) => env.organizeAgent.organizePassAgent(...a);


/**
 * organizeCache 落盘：只留最近 N 条（默认 200），
 * 溢出的按写入月份归档到 organize-archive/。归档失败时 trimOrganizeCache 返回原 cache，
 * 主文件一条不删，只 warn——最坏退化成「不轮转」。
 */
function persistOrganizeCache(cache: Record<string, CacheValue>) {
  const warn = (line: string) => log(cfg, `[cache] ${line}`);
  const verdict = trimOrganizeCache(cfg, cache, { warn });
  stateStore.patch({ organizeCache: verdict.kept });
  if (verdict.failed) {
    log(cfg, `organizeCache 轮转失败，主文件保留 ${verdict.before} 条未裁剪：${verdict.error}`);
  } else if (verdict.spilled > 0) {
    log(
      cfg,
      `organizeCache 轮转：主文件留 ${Object.keys(verdict.kept).length} 条（上限 ${verdict.max}），`
        + `归档 ${verdict.spilled} 条到 ${archiveDirOf(cfg)}（新写 ${verdict.written} 行，`
        + `月份 ${JSON.stringify(verdict.months)}）`,
    );
  }
  return verdict;
}

async function organizePass({ dryRun = false }: { dryRun?: boolean } = {}): Promise<OrganizeResult> {
  // 2026-11-05 第三步：开关打开走 MergeAgent + 证据校验 + pending 重放；关着仍是老的分簇整理
  if (cfg.pipeline?.agentPipeline === true) return organizePassAgent({ dryRun });
  // 一轮一次快照：写库后 store 版本 +1，下一次取自动重读（别在批间拿旧视图）
  const snap = activeSnap();
  const all = snap.list;
  const vecOf = snap.vecOf;
  const { window: promptWindow, budget: promptTokens } = budgetOf();
  const batchBudget = promptWindow ? Math.max(0, promptTokens - CLUSTER_PROMPT_SHELL_TOKENS) : 0;
  const stateBefore: SleepState = stateStore.load();
  const cache: Record<string, CacheValue> = { ...(stateBefore.organizeCache as Record<string, CacheValue> || {}) };
  // 条目级复审缓存（2026-11-23）：只按条目内容哈希命中，成员数组变化不影响。
  // 送给 plan 的是「已审过的关系」，plan 会把逐字未变的成员摘掉，只留变化项 + 旧邻居。
  let itemNext = readItemReviewCache(stateStore);
  // 聚类 → 切批 → L0-L3 漏斗 → 增量复审 → 查缓存挑批，全在 BatchPlanner（三处共用一份）。
  const plan = batchPlannerOf(batchBudget).plan({ all, vecOf, cache, itemCache: itemNext });
  const { clusters, queue, batches, picked } = plan;
  const out: OrganizeResult = {
    total: plan.total,
    clusters: clusters.length,
    queued: queue.length,
    batches: batches.length,
    promptWindow,
    batchBudget,
    calls: 0,
    skippedSingles: plan.skippedSingles,
    skippedQuiet: plan.skippedQuiet,
    cached: plan.cached,
    funnel: plan.funnel,
    failed: 0,
    merged: [],
    updated: [],
    archived: [],
    rejected: [],
  };
  // 2026-11-14：老分簇路径也 fail-loud，别让 132 批各自静默失败。
  if (picked.length > 0) {
    try {
      llmOf(ctx);
    } catch (error) {
      if (!isLlmUnavailable(error)) throw error;
      out.failed = picked.length;
      out.llmUnavailable = true;
      log(cfg, `[merge] ⛔ 分簇整理阶段中止：${(error as Error).message}——${picked.length} 批一批都没问，整轮按失败处理`);
      throw error;
    }
  }
  // 2026-11-14：跟着全链自适应走，organizeConcurrency 只当上限覆盖，不再有 min(...,8) 这个独立硬顶。
  const pick = curatePick('organize');
  const conc = pick.conc;
  const raws: Array<{ raw?: unknown; error?: unknown }> = new Array(picked.length);
  let cursor = 0;
  const worker = async () => {
    for (;;) {
      const i = cursor;
      cursor += 1;
      if (i >= picked.length) return;
      try { raws[i] = { raw: await askCluster(picked[i].cluster) }; }
      catch (e) { raws[i] = { error: e }; }
    }
  };
  if (picked.length > 0) {
    await Promise.all(Array.from({ length: Math.min(conc, picked.length) }, worker));
    log(cfg, `分簇整理并发 ${Math.min(conc, picked.length)} 路问完 ${picked.length} 批，开始落地`);
  }

  for (let qi = 0; qi < picked.length; qi += 1) {
    heartbeat(); // 每批开始写一次心跳
    const { cluster, fp } = picked[qi];
    out.calls += 1;
    // 2026-11-14：单批动作数上限（clusterChanges）删了，模型给几条落几条
    try {
      const got = raws[qi];
      if (got?.error) throw got.error;
      const raw = got?.raw ?? '';
      let actions = parseActions(raw);
      if (actions) {
        // 三段式：代码标记 → 复核 agent 裁决 →
        // 判 redo 的退回产出这批的 agent **重做一次**；同一批多条可疑只花一次复核调用。
        const done = await lintRedoOnce({
          agent: 'sleep:organize',
          action: 'cluster',
          // 入参只给三个桶（没有 add），lintRedoOnce 对缺桶本来就容忍；断言只为过编译。
          actions: { merge: actions.merge as ActionItem[], update: actions.update as ActionItem[], archive: [] } as unknown as LintActions,
          ask: (feedback) => askCluster(cluster, feedback),
        });
        actions = { ...done.actions, archive: actions.archive || [] };
      }
      if (!actions) {
        out.failed += 1;
        const tail = String(raw || '').slice(-260);
        const dsml = /DSML|antml:|<\|\|/i.test(String(raw || '')) ? '（模型吐了 DSML 标签文本）' : '';
        log(cfg, `分簇整理第${out.calls}批没给出 JSON（${cluster.length}条）${dsml} raw=${JSON.stringify(tail)}`);
        continue;
      }
      const r = await applyActions(
        { merge: actions.merge as ActionItem[] || [], update: actions.update as ActionItem[] || [], archive: actions.archive as ActionItem[] || [], add: [] },
        { existing: cluster, dryRun },
      );
      out.merged.push(...r.merged);
      out.updated.push(...r.updated);
      out.archived.push(...r.archived);
      out.rejected.push(...r.rejected);
      const acted = r.merged.length + r.updated.length + r.archived.length > 0;
      cache[fp as string] = { v: acted ? 'acted' : 'noop', t: Date.now() };
      // 条目级结论：只记模型明确点到的 id（单 id update/archive 记 acted、不记 noop）；
      // 三个桶都空 = 模型明确「这批不动」，按整批 noop + 同批组键记。
      // 被 applyActions 打回的 id 一条都不记，下一轮还得重问（t6 复核 bug 3）。
      if (!dryRun) {
        itemNext = mergeItemReviewCache(itemNext, reviewAdditionsFromActions(cluster, actions as never, { rejected: r.rejected as never[] }));
      }
      if (acted) {
        log(
          cfg,
          `分簇整理第${out.calls}批（${cluster.length}条）→ 合并${r.merged.length} 修订${r.updated.length} 归档${r.archived.length}`,
        );
      }
    } catch (error) {
      out.failed += 1;
      log(cfg, `分簇整理第${out.calls}批失败：${(error as Error | null)?.message || error}`);
    }
  }
  if (!dryRun && out.calls > 0) {
    persistOrganizeCache(cache);
    persistItemReviewCache(stateStore, itemNext, { cfg: cfg as never, log: (line) => log(cfg, line) });
  }
  log(
    cfg,
    `分簇整理结束 记忆${out.total}条/${out.clusters}批 排队${out.queued} 调用${out.calls} 缓存跳过${out.cached} 单条跳过${out.skippedSingles} 无重复跳过${out.skippedQuiet}` +
      ` 合并${out.merged.length} 修订${out.updated.length} 归档${out.archived.length} 失败${out.failed}`,
  );
  return out;
}

  return { buildSleepClusters, persistOrganizeCache, organizePass };
}
