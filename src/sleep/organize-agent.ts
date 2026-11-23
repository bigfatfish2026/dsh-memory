/**
 * 合并 agent 路径：分簇 → merge() → 证据校验 → 逐条落地 → 账本重放 → 超长转拆分。
 */
import { consumeSplitOps } from '../pipeline-split.js';
import { createSplitVerdictCache } from '../split-verdict-cache.js';
import { merge as askMergeOps, validateOps } from '../agents/merge.js';
import { llmOf, isLlmUnavailable } from '../llm-service.js';
import { selectIngestConcurrency, formatIngestAdaptiveLog } from '../concurrency.js';
import { CLUSTER_PROMPT_SHELL_TOKENS } from './batching.js';
import {
  readItemReviewCache, persistItemReviewCache, mergeItemReviewCache, reviewAdditionsFromOps,
} from '../item-review-cache.js';
import type { DatabaseSync } from 'node:sqlite';
import type { MemoryView, SnapshotView } from '../store/types.js';
import type { BatchPlanner } from '../batch-planner.js';
import type { PromptBudget } from '../token-window.js';
import type { SleepCfg, SleepCtx, SleepStore } from './env.js';
import type { SleepStateStore } from '../sleep-state.js';
import type { AgentOp, AgentClusterItem } from './agent-ops.js';
import type { DueOp } from '../ingest-index.js';
import type { CacheValue } from '../organize-cache.js';
import type { createLedgerGlue } from './ledger-glue.js';

/** merge 批的问答盒。 */
interface MergeAskBox {
  answer?: { ops?: unknown[]; source?: unknown } | null;
  kind?: string;
  summary?: string;
  error?: unknown;
}

/** 一轮 agent 整理的汇总。 */
interface AgentOrganizeOut {
  agent: boolean;
  total: number;
  clusters: number;
  queued: number;
  batches: number;
  promptWindow: number;
  batchBudget: number;
  calls: number;
  cached: number;
  funnel: unknown;
  skippedSingles: number;
  skippedQuiet: number;
  failed: number;
  empty: number;
  reasoningExhausted: number;
  noop: number;
  parseFail: number;
  dead: number;
  invalid: number;
  ops: number;
  invalidSamples: unknown[];
  merged: unknown[];
  updated: unknown[];
  archived: unknown[];
  added: unknown[];
  rejected: unknown[];
  split: unknown[];
  splitInserted: number;
  splitDeduped: number;
  splitArchived: number;
  replay: unknown;
  splitAgent?: unknown;
  llmUnavailable?: boolean;
  [key: string]: unknown;
}

/** 本文件用到的 env 面。 */
interface OrganizeAgentEnv {
  ctx: SleepCtx;
  store: SleepStore;
  cfg: SleepCfg;
  stateStore: SleepStateStore;
  log: (line: string) => void;
  ledger: {
    pendingDb: () => DatabaseSync | null;
    bumpMergeBatch: ReturnType<typeof createLedgerGlue>['bumpMergeBatch'];
    candidateListFor: (cluster: AgentClusterItem[], active: MemoryView[]) => Array<{ id: string; text: string; score: number }>;
  };
  agentOps: ReturnType<typeof createPoolShim>;
  activeSnap: () => SnapshotView;
  replay: { replayPendingOps: (opts: { dryRun?: boolean; skipIds?: unknown[]; onlyOps?: DueOp[] | null }) => Promise<Record<string, unknown>> };
  budgetOf: () => PromptBudget;
  batchPlannerOf: (budgetTokens?: number) => BatchPlanner;
  scheduler: { curatePick: (stage: string) => { conc: number; adaptive?: boolean; cap?: number } };
  heartbeat: () => number;
  organize: { persistOrganizeCache: (cache: Record<string, CacheValue>) => unknown };
}

/** 只为拿 applyAgentOps 的类型壳（避免本文件反向依赖 agent-ops 的完整 env）。 */
declare function createPoolShim(env: unknown): { applyAgentOps: (ops: AgentOp[], opts: { cluster: AgentClusterItem[]; dryRun: boolean; fp: string }) => Promise<{ merged: unknown[]; updated: unknown[]; archived: unknown[]; added: unknown[]; rejected: unknown[]; split: unknown[] }> };

export function createOrganizeAgent(env: OrganizeAgentEnv) {
  const { ctx, store, cfg, stateStore } = env;
  const log = (_c: SleepCfg, line: string) => env.log(line);
  const pendingDb = (...a: Parameters<OrganizeAgentEnv['ledger']['pendingDb']>) => env.ledger.pendingDb(...a);
  const bumpMergeBatch = (...a: Parameters<OrganizeAgentEnv['ledger']['bumpMergeBatch']>) => env.ledger.bumpMergeBatch(...a);
  const candidateListFor = (...a: Parameters<OrganizeAgentEnv['ledger']['candidateListFor']>) => env.ledger.candidateListFor(...a);
  const applyAgentOps = (...a: Parameters<OrganizeAgentEnv['agentOps']['applyAgentOps']>) => env.agentOps.applyAgentOps(...a);
  const activeSnap = (...a: Parameters<OrganizeAgentEnv['activeSnap']>) => env.activeSnap(...a);
  const replayPendingOps = (...a: Parameters<OrganizeAgentEnv['replay']['replayPendingOps']>) => env.replay.replayPendingOps(...a);
  const budgetOf = (...a: Parameters<OrganizeAgentEnv['budgetOf']>) => env.budgetOf(...a);
  const batchPlannerOf = (...a: Parameters<OrganizeAgentEnv['batchPlannerOf']>) => env.batchPlannerOf(...a);
  const curatePick = (...a: Parameters<OrganizeAgentEnv['scheduler']['curatePick']>) => env.scheduler.curatePick(...a);
  const heartbeat = (...a: Parameters<OrganizeAgentEnv['heartbeat']>) => env.heartbeat(...a);
  const persistOrganizeCache = (...a: Parameters<OrganizeAgentEnv['organize']['persistOrganizeCache']>) => env.organize.persistOrganizeCache(...a);

/**
 * 新路径（开关 pipeline.agentPipeline=true）：分簇 → merge() → validateOps() → 逐条落地。
 * 模型明确给 {"ops":[]} 是有效 noop：写 organizeCache、不进 pending；
 * 调用失败 / 解析失败 / 撞墙才落 pending_ops(kind='merge_batch')，第 3 次转 dead。
 */
async function organizePassAgent({ dryRun = false } = {}) {
  // 一轮一次快照：写库后 store 版本 +1，下一次取自动重读（别在批间拿旧视图）
  const snap = activeSnap();
  const all = snap.list;
  const vecOf = snap.vecOf;
  const { window: promptWindow, budget: promptTokens } = budgetOf();
  const batchBudget = promptWindow ? Math.max(0, promptTokens - CLUSTER_PROMPT_SHELL_TOKENS) : 0;
  const stateBefore = stateStore.load();
  const cache = { ...(stateBefore.organizeCache || {}) };
  // 条目级复审缓存（2026-11-23）：语义同 legacy 路径——送审前摘掉逐字未变的已审成员。
  let itemNext = readItemReviewCache(stateStore);
  // 与 organizePass / planPoolQueues 共用同一份「聚类→切批→漏斗→增量复审→挑批」。
  const plan = batchPlannerOf(batchBudget).plan({ all, vecOf, cache, itemCache: itemNext }) as unknown as { total: number; clusters: unknown[]; queue: unknown[]; batches: unknown[]; picked: Array<{ cluster: MemoryView[]; fp: string }>; cached: number; funnel: unknown; skippedSingles: number; skippedQuiet: number };
  const { clusters, queue, batches, picked } = plan;
  const out: AgentOrganizeOut = {
    agent: true,
    total: plan.total,
    clusters: clusters.length,
    queued: queue.length,
    batches: batches.length,
    promptWindow,
    batchBudget,
    calls: 0,
    cached: plan.cached,
    funnel: plan.funnel,
    skippedSingles: plan.skippedSingles,
    skippedQuiet: plan.skippedQuiet,
    failed: 0,
    empty: 0,
    reasoningExhausted: 0,
    noop: 0,
    parseFail: 0,
    dead: 0,
    invalid: 0,
    ops: 0,
    invalidSamples: [],
    merged: [],
    updated: [],
    archived: [],
    added: [],
    rejected: [],
    split: [],
    // 拆分段的三样单列：并进 added 会让「新增」既算 agent 直接 add、又算拆出来的子段，
    // 两栏混在一起谁都对不上；这里各记各的，汇总行再一起报。
    splitInserted: 0,
    splitDeduped: 0,
    splitArchived: 0,
    replay: {},
  };


  // 本轮刚失败的 merge_batch 账：收尾重放时跳过它们（dueOps 的 exclude），下一轮再试。
  // 同一轮原地重问等于把「重试次数」藏在一次运行里，和删掉的固定重试没区别。
  const failedThisRound = [];

  // 2026-11-14：llm 取不到就第一批中止，不许 132 批都跑成静默失败。
  // 这里先探一次（服务没挂/卸载时连一批都不发）；worker 每批再取一次，中途掉了也当场停。
  let llmAbort: unknown = null;
  if (picked.length > 0) {
    try {
      llmOf(ctx);
    } catch (error) {
      if (!isLlmUnavailable(error)) throw error;
      llmAbort = error;
      out.failed = picked.length;
      log(cfg, `[merge] ⛔ merge 阶段中止：${(error as Error).message}——${picked.length} 批一批都没问，整轮按失败处理`);
    }
  }
  if (llmAbort) {
    out.llmUnavailable = true;
    throw llmAbort;
  }

  // 先并发问模型，落地再串行——和老的 organizePass 同一个节奏
  // 2026-11-14：并发跟全链自适应同一套口径（cap 64），删掉 min(...,8) 独立硬顶。
  const pick = curatePick('merge');
  const conc = pick.conc;
  const raws = new Array(picked.length);
  let cursor = 0;
  const worker = async () => {
    for (;;) {
      if (llmAbort) return;
      const i = cursor;
      cursor += 1;
      if (i >= picked.length) return;
      heartbeat(); // 问模型前续租
      const { cluster, fp } = picked[i];
      const box: MergeAskBox = { answer: null, kind: '', summary: '' };
      try {
        box.answer = await askMergeOps({
          items: cluster.map((m) => ({
            id: String(m.id),
            type: store.typeOf(m),
            text: String(m.text || ''),
            ts: String(m.ts || m.validFrom || ''),
            entities: (m.entities || []).map(String),
          })),
          candidates: candidateListFor(cluster, all),
          clusterId: fp,
          llm: llmOf(ctx, { site: 'merge', log: (line: string) => log(cfg, line), cfg }),
          provider: cfg.provider,
          model: cfg.model,
          log: (line: string) => log(cfg, line),
          db: pendingDb(),
          textLint: cfg.pipeline?.textLint !== false,
          // lint 两级复核：默认开；false 走旧逻辑
          lintReview: cfg.pipeline?.lintReview !== false,
          lintReviewTimeoutMs: cfg.pipeline?.lintReviewTimeoutMs,
          // 失败摘要（finish/usage/两通道前 160 字）带回来写进 pending_ops.last_error
          onFail: (summary: string, kind: string) => { box.summary = summary; box.kind = kind; },
        });
      } catch (error) {
        box.error = error;
        if (!llmAbort && isLlmUnavailable(error)) {
          llmAbort = error;
          log(cfg, `[merge] ⛔ merge 阶段中止：${(error as Error).message}——第${i + 1}批调用时服务掉了，剩余批次不再发请求，整轮按失败处理`);
        }
      }
      raws[i] = box;
    }
  };
  if (picked.length > 0) {
    await Promise.all(Array.from({ length: Math.min(conc, picked.length) }, worker));
    if (llmAbort) {
      // 一批都没落地：不留半截结果，整轮交给 scheduledRun 标失败（不 markSlot，下一拍重试）。
      out.failed = Math.max(out.failed, picked.length);
      out.llmUnavailable = true;
      throw llmAbort;
    }
    log(cfg, `合并 agent 并发 ${Math.min(conc, picked.length)} 路问完 ${picked.length} 批，开始证据校验与落地`);
  }

  for (let qi = 0; qi < picked.length; qi += 1) {
    heartbeat(); // 每批开始写一次心跳
    const { cluster, fp } = picked[qi];
    out.calls += 1;
    // 2026-11-14：单批动作数上限删了
    const got = raws[qi];
    if (got?.error) {
      out.failed += 1;
      log(cfg, `[merge] 第${out.calls}批调用失败（${cluster.length}条 fp=${fp}）：${String((got.error && got.error.message) || got.error)}`);
      const bump = bumpMergeBatch(fp, cluster, 'call_fail', { dryRun });
      if (bump.id) failedThisRound.push(bump.id);
      if (bump.dead) out.dead += 1;
      continue;
    }
    const answer = got?.answer;
    if (!answer) {
      // 三种失败分开记因：空响应 / 思考吃光预算 / 解析失败，账本里一眼分得清（t-fix-quality）
      const reasonKind = got?.kind === 'empty'
        ? 'empty_response'
        : got?.kind === 'reasoning_exhausted' ? 'reasoning_exhausted' : 'parse_fail';
      if (reasonKind === 'empty_response') out.empty += 1;
      else if (reasonKind === 'reasoning_exhausted') out.reasoningExhausted += 1;
      else out.parseFail += 1;
      const summary = String(got?.summary || 'no_detail').slice(0, 460);
      log(
        cfg,
        `[merge] 第${out.calls}批重试后仍失败（${reasonKind}，${cluster.length}条 fp=${fp}），落 merge_batch 等重放：${summary}`,
      );
      const bump = bumpMergeBatch(fp, cluster, `${reasonKind} ${summary}`, { dryRun });
      if (bump.id) failedThisRound.push(bump.id);
      if (bump.dead) out.dead += 1;
      continue;
    }
    const ops = Array.isArray(answer.ops) ? answer.ops : [];
    out.ops += ops.length;
    if (!ops.length) {
      // 明确空动作 = 这批看过了、不用动：有效结论（2026-11-06 t-fix-quality）。
      // 按 noop 写进 organizeCache，下一轮同指纹直接命中，不再进 pending 烧钱。
      out.noop += 1;
      if (!dryRun) {
        cache[fp] = { v: 'noop', t: Date.now() };
        // 条目级也记：模型明确给空 ops = 整批「看过、不用动」，记 noop + 同批组键；
        // 这是唯一允许整批记 noop 的情形（t6 复核 bug 3）。
        itemNext = mergeItemReviewCache(itemNext, reviewAdditionsFromOps(cluster, []));
      }
      log(cfg, `[merge] 第${out.calls}批明确空 ops（${cluster.length}条 fp=${fp}）：记 noop 缓存，不入队`);
      continue;
    }
    const candidates = candidateListFor(cluster, all);
    const items = [...cluster, ...candidates].map((m) => ({ id: String(m.id), text: String(m.text || '') }));
    const checked = validateOps(ops, items, {
      maxChars: Number(cfg.splitMaxChars) || 200,
      db: dryRun ? null : pendingDb(),
      batchId: fp,
      log: (line: string) => log(cfg, line),
    });
    out.invalid += checked.invalid.length;
    for (const bad of checked.invalid) {
      if (out.invalidSamples.length >= 2) break;
      out.invalidSamples.push({
        index: bad.index,
        op: bad.op,
        ids: bad.ids,
        reason: bad.reason,
        quote: String(bad.quote || '').slice(0, 200),
        text: String(bad.text || '').slice(0, 200),
      });
    }
    if (!checked.valid.length) {
      log(cfg, `[merge] 第${out.calls}批 ${ops.length} 条提案全被证据校验打回（fp=${fp}），整批不动`);
      // 2026-11-07 维护者决定：rejected 不写 organizeCache。写进去下一轮同指纹直接命中，
      // 等于把「这批还得重问」永久跳过；宁可多花一次调用，也让它下一轮重新判一遍。
      continue;
    }
    const applied = await applyAgentOps(checked.valid, { cluster, dryRun, fp });
    out.merged.push(...applied.merged);
    out.updated.push(...applied.updated);
    out.archived.push(...applied.archived);
    out.added.push(...applied.added);
    out.rejected.push(...applied.rejected);
    out.split.push(...applied.split);
    // 条目级结论：只记模型明确点到的 id（单 id update/archive 记 acted、不记 noop）；
    // 校验被拒/写入闸打回/模型没提到的一条不记，下一轮同内容还要再问一遍（t6 复核 bug 3）。
    if (!dryRun) {
      itemNext = mergeItemReviewCache(itemNext, reviewAdditionsFromOps(cluster, checked.valid, {
        rejected: [
          ...checked.invalid.map((x) => ({ ids: x.ids })),
          ...applied.rejected as { ids?: unknown; id?: unknown }[],
        ],
      }));
    }
    const acted = applied.merged.length + applied.updated.length + applied.archived.length +
      applied.added.length + applied.split.length > 0;
    if (!dryRun && acted) {
      cache[fp] = { v: 'acted', t: Date.now() };
    } else if (!dryRun && applied.rejected.length) {
      // 2026-11-14：ops 过了证据校验，但一条都没落地（全被写入闸判重 / 归档规则打回）。
      // 2026-11-07 那条「打回不写缓存」的顾虑是「怕把还得重问的批永久跳过」；可这批的 ops 已经
      // 过了校验、只是被规则挡下，同一指纹同一批正文重问多少遍都是同一批打回。所以改成：
      // 记 noop 缓存（带原因，下一轮整理段直接跳过，不再问模型）+ 落一笔账
      // （attempts+1，到 5 转 dead 的规则照旧兜底，缓存被轮转掉也不会无账重来）。
      // 指纹口径与 replay 段完全一致：都用 batchFingerprint(fp) → cache[fp] / payload.batch。
      const why = applied.rejected.map((r) => `${(r as { kind?: unknown }).kind || 'op'}:${String((r as { reason?: unknown }).reason || '').slice(0, 60)}`);
      const envOnly = applied.rejected.every((r) => /入队失败/.test(String((r as { reason?: unknown }).reason || '')));
      if (envOnly) {
        // 超长正文入队失败是环境问题（pending_ops 不可用），不是「这批不用动」：
        // 不写缓存也不记账，等环境修好下一轮接着问，别把没落地的正文永久跳过。
        log(cfg, `[merge] 第${out.calls}批 ops 全被挡（acted=0，fp=${fp}）原因是环境：${why.join('；')}——不写缓存`);
      } else {
        const reasonText = `acted_none ${why.join('；') || 'no_reason'}`.slice(0, 300);
        cache[fp] = { v: 'noop', t: Date.now(), reason: reasonText };
        const bump = bumpMergeBatch(fp, cluster, reasonText, { dryRun });
        if (bump.id) failedThisRound.push(bump.id);
        if (bump.dead) out.dead += 1;
        log(
          cfg,
          `[merge] 第${out.calls}批 ops 全被挡（acted=0，fp=${fp}）：记 noop 缓存 + 落账 attempts=${bump.attempts}；${reasonText}`,
        );
      }
    }
    if (acted || checked.invalid.length) {
      log(
        cfg,
        `[merge] 第${out.calls}批（${cluster.length}条 source=${answer.source}）→ 合并${applied.merged.length}` +
          ` 修订${applied.updated.length} 归档${applied.archived.length} 新增${applied.added.length}` +
          ` 转拆分${applied.split.length} 打回${checked.invalid.length}`,
      );
    }
  }

  if (!dryRun && out.calls > 0) {
    persistOrganizeCache(cache as Record<string, CacheValue>);
    persistItemReviewCache(stateStore, itemNext, { cfg: cfg as never, log: (line) => log(cfg, line) });
  }

  // 收尾第一步：重放三种 agent 账本（merge_batch → merge_op → dedupe_check）。
  // 2026-11-05 t-fix-truncate：这三种 kind 以前只有写入没有消费，账本越积越厚。
  out.replay = await replayPendingOps({ dryRun, skipIds: failedThisRound });

  // 收尾第二步：拆分本轮刚入队的 needsSplit 条目趁热拆，失败的自己退避到下一轮。
  // 开关 agentPipeline 已在 organizePass 入口把关，这里只在真跑时动手。
  let splitAgent = { scanned: 0, tried: 0, unavoidable: 0, inserted: 0, deduped: 0, parts: 0, failed: 0, rejected: 0, skipped: 0, unavailable: 0, archived: 0 };
  if (!dryRun) {
    try {
      heartbeat(); // 拆分开始续租
      // 2026-11-14：拆分段也开并发，路数走 ingest 同一套自适应
      // （organizeIngestAdaptive 开关 + 本机负载算法，cap 64），开跑前算一次，整段固定不抖。
      const splitPick = selectIngestConcurrency(cfg);
      if (splitPick.adaptive) log(cfg, formatIngestAdaptiveLog(splitPick, '[split]'));
      // 「不可拆」判定缓存（2026-11-22）：legacy 整理路径也接同一份 sleep.state.json 缓存，
      // 两种 organizePipelineMode 行为一致。命中就不再问模型（pipeline-split 内统计）。
      const splitVerdictCache = createSplitVerdictCache(stateStore, {
        log: (line: string) => log(cfg, line),
      });
      splitAgent = await consumeSplitOps({
        textLint: cfg.pipeline?.textLint !== false,
        lintReview: cfg.pipeline?.lintReview !== false,
        lintReviewTimeoutMs: cfg.pipeline?.lintReviewTimeoutMs,
        // 段级信号是否硬拒：默认 false（只记），阶段二才开
        signalsStrict: cfg.pipeline?.splitSignalsStrict === true,
        db: pendingDb(),
        store,
        llm: llmOf(ctx, { site: 'split', log: (line: string) => log(cfg, line), cfg }),
        provider: cfg.provider,
        model: cfg.model,
        // 2026-11-14：拆分与重放都不再吃条数预算，整队都归本轮处理
        // 2026-11-14：但不再 dueOps(Infinity) 一次全领（会把租约占死），
        // 改成 consumeSplitOps 内部按并发数小批领；limit=Infinity 只表示「本轮不封顶」。
        limit: Infinity,
        concurrency: splitPick.conc,
        maxChars: Number(cfg.splitMaxChars) || 200,
        verdictCache: splitVerdictCache,
        log: (line: string) => log(cfg, line),
      } as unknown as Parameters<typeof consumeSplitOps>[0]);
      if (splitAgent.scanned) {
        log(
          cfg,
          `[split] 本轮处理 split_item ${splitAgent.scanned} 条：拆出 ${splitAgent.parts} 段，` +
            `入库 ${splitAgent.inserted} 判重 ${splitAgent.deduped} 被拒 ${splitAgent.rejected}` +
            ` unavoidable ${splitAgent.unavoidable} 失败 ${splitAgent.failed}`,
        );
      }
    } catch (error) {
      log(cfg, `[split] WARN 拆分段整段失败：${String(((error as Error | null) && (error as Error).message) || error)}`);
    }
  }
  out.splitAgent = splitAgent;
  // 拆分段的三样接进 summary：
  // 以前 only 打印，汇总行天生比库小——「新增」只算 agent 直接 add 的条数，拆出来的子段不并进去，
  // 否则「新增」既像 add 又像拆分产物，谁都没法跟库对账；三样单列，汇总行并排报。
  out.splitInserted = Number(splitAgent.inserted) || 0;
  out.splitDeduped = Number(splitAgent.deduped) || 0;
  out.splitArchived = Number(splitAgent.archived) || 0;
  // 判重真的在 discards 落了一行账（写入闸 DUPLICATE），所以计进「拒绝」，
  // 汇总的「拒绝」才能跟 discards 行数对上；它们不进「新增」，新增只认 agent 直接 add。
  for (let n = 0; n < out.splitDeduped; n += 1) out.rejected.push({ kind: 'split', reason: '判重' });

  log(
    cfg,
    `合并 agent 结束 记忆${out.total}条/${out.clusters}批 排队${out.queued} 调用${out.calls} 缓存跳过${out.cached}` +
      ` 单条跳过${out.skippedSingles} 无重复跳过${out.skippedQuiet} noop${out.noop} 空响应${out.empty}` +
      ` 撞墙${out.reasoningExhausted} 解析失败${out.parseFail} dead${out.dead} 证据打回${out.invalid}` +
      ` 合并${out.merged.length} 修订${out.updated.length}` +
      ` 归档${out.archived.length} 新增${out.added.length} 转拆分${out.split.length} 失败${out.failed}` +
      ` 拆分入库${out.splitInserted}/判重${out.splitDeduped}/归档${out.splitArchived}`,
  );
  return out;
}

  return { organizePassAgent };
}
