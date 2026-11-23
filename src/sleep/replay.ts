/**
 * 三种 agent 账本的重放：merge_batch / merge_op / dedupe_check。
 * 领活按并发数小批、串行落地，失败的写 attempts 等下一轮，不原地重试。
 */
import { dueOps, markOp, MAX_ATTEMPTS } from '../ingest-index.js';
import { merge as askMergeOps, validateOps } from '../agents/merge.js';
import { cheapSimilarity, cosine } from '../batch-planner.js';
import {
  readItemReviewCache, shrinkClusterForReview, reviewAdditionsFromOps, mergeItemReviewCache, persistItemReviewCache,
} from '../item-review-cache.js';
import { llmOf } from '../llm-service.js';
import type { DatabaseSync } from 'node:sqlite';
import type { DueOp } from '../ingest-index.js';
import type { Row } from '../store/types.js';
import type { SnapshotView, MemoryView } from '../store/types.js';
import type { SleepCfg, SleepCtx, SleepStore } from './env.js';
import type { AgentOp, AgentClusterItem } from './agent-ops.js';
import type { MergeOp } from './ledger-glue.js';

/** 账本重放用到的 env 面。 */
interface ReplayEnv {
  ctx: SleepCtx;
  store: SleepStore;
  cfg: SleepCfg;
  stateStore: {
    load: () => { organizeCache?: Record<string, unknown> };
    /** 条目级复审缓存写回用；老桩没有 patch 就跳过写回（行为与改前一致）。 */
    patch?: (next: Record<string, unknown>) => unknown;
  };
  log: (line: string) => void;
  ledger: {
    pendingDb: () => DatabaseSync | null;
    queueMergeAgentSplit: (op: MergeOp, fp: string, opts?: { dryRun?: boolean }) => string | null;
    candidateListFor: (cluster: AgentClusterItem[], active: MemoryView[]) => Array<{ id: string; text: string; score: number }>;
  };
  agentOps: { applyAgentOps: (ops: AgentOp[], opts: { cluster: AgentClusterItem[]; dryRun: boolean; fp: string }) => Promise<{ merged: unknown[]; updated: unknown[]; archived: unknown[]; added: unknown[]; rejected: unknown[]; split: unknown[] }> };
  ingest: { shortErr: (error: unknown) => string };
  scheduler: { curatePick: (channel: string) => { conc: number } };
  activeSnap: () => SnapshotView;
  heartbeat: () => number;
}

/** 一种 kind 的计数。 */
interface KindStats { done: number; failed: number; dead: number }

/** 重放收尾助手与段函数的公共依赖。 */
interface ReplayDeps {
  store: SleepStore;
  ask: typeof askMergeOps;
  validate: typeof validateOps;
  apply: ReplayEnv['agentOps']['applyAgentOps'];
  candidates: ReplayEnv['ledger']['candidateListFor'];
  snapshot: () => SnapshotView;
  finish: (kind: string, op: DueOp, status: string, err?: unknown, opts?: { increment?: boolean }) => Row | null;
  bump: (kind: string, key: keyof KindStats) => void;
  dueAll: (kind: string) => Generator<DueOp, void, unknown>;
  heartbeat: () => number;
  activeNow: () => MemoryView[];
  maxChars: number;
  stats: Record<string, KindStats>;
  onlyOps: DueOp[] | null;
}

/** 召回补位的一条。 */
interface RecallRow {
  id?: unknown;
  text?: unknown;
  entities?: unknown;
  score?: unknown;
}

export function createReplay(env: ReplayEnv) {
  const { ctx, store, cfg, stateStore } = env;
  const log = (_c: SleepCfg, line: string) => env.log(line);
  const pendingDb = (...a: Parameters<ReplayEnv['ledger']['pendingDb']>) => env.ledger.pendingDb(...a) as DatabaseSync;
  const queueMergeAgentSplit = (...a: Parameters<ReplayEnv['ledger']['queueMergeAgentSplit']>) => env.ledger.queueMergeAgentSplit(...a);
  const candidateListFor = (...a: Parameters<ReplayEnv['ledger']['candidateListFor']>) => env.ledger.candidateListFor(...a);
  const applyAgentOps = (...a: Parameters<ReplayEnv['agentOps']['applyAgentOps']>) => env.agentOps.applyAgentOps(...a);
  const shortErr = (...a: Parameters<ReplayEnv['ingest']['shortErr']>) => env.ingest.shortErr(...a);
  const curatePick = (...a: Parameters<ReplayEnv['scheduler']['curatePick']>) => env.scheduler.curatePick(...a);
  const activeSnap = (...a: Parameters<ReplayEnv['activeSnap']>) => env.activeSnap(...a);
  const heartbeat = (...a: Parameters<ReplayEnv['heartbeat']>) => env.heartbeat(...a);

/**
 * 三种 agent 账本的重放（2026-11-05 t-fix-truncate）。
 *
 * 以前 merge_batch / merge_op / dedupe_check 只写不读：dueOps 只服务 ingest_file 和 split_item，
 * 账本越积越厚却没人消费。这里在 organizePassAgent 收尾阶段按 kind 依次消费：
 *   merge_batch  → 用账本里的成员重新提合并（成员还在 active 里才有意义）；
 *   merge_op     → 单条修复性重放（证据可能已经变了，重验一遍）；正文仍超长就转 split_item；
 *   dedupe_check → 重新过写入闸（store.save/update/merge 内部再判一次），判得出就补写。
 *
 * 全部走 dueOps / markOp：退避阶梯 1/5/15 分钟、attempts>3 转 dead 原样保留。
 * 日志固定一行：`[replay] kind=<kind> done=<n> failed=<n> dead=<n>`。
 */
async function replayMergeBatchSegment(deps: ReplayDeps): Promise<void> {
  const { finish, dueAll, heartbeat, activeNow, maxChars, stats } = deps;
  const db = pendingDb();
  // ① merge_batch：按账本里的成员重新提合并，再走一遍证据校验与落地
  try {
    // 2026-11-14：重放前先判「这批还有没有活」。organizeCache 里同一指纹
    // 已被整理段判成 noop（批里没活）或 acted（活已经落过），再问一遍模型只会得到同样的空；
    // 这类账直接收口 done，不调用模型、不耗 attempts，也不再每轮白烧一次调用。
    const replayCache: Record<string, { v?: unknown }> = stateStore.load().organizeCache as Record<string, { v?: unknown }> || {};
    // 条目级复审结论段内累积，段尾一次写回（别每批 patch 一次 state 文件）
    const canPersistItems = typeof stateStore.patch === 'function';
    let itemCacheRound = canPersistItems ? readItemReviewCache(stateStore as never) : {};
    let itemDirty = false;
    const singleBatch = deps.onlyOps?.find((o) => o.kind === 'merge_batch');
    for (const op of (singleBatch ? [singleBatch] : dueAll('merge_batch'))) {
      heartbeat();
      try {
        const payload = op.payload || {};
        const ids = Array.isArray(payload.ids) ? payload.ids.map(String) : [];
        const batchLabel = String(payload.batch || op.dedupeKey);
        const settled = replayCache[batchLabel];
        if (settled && (settled.v === 'noop' || settled.v === 'acted')) {
          // 批里本来就没活（noop）或活已经落过（acted）：终态 done，下一轮不再出现
          finish('merge_batch', op, 'done', `cache_${settled.v}`);
          continue;
        }
        // 2026-11-14：删掉「第 3 次（attempts>=2）就换策略 / 转 split」的固定次数逻辑。
        // 失败只累加 attempts + last_error，下一轮照原样再试，不自动放弃、也不静默吞。
        const active = activeNow();
        const byId = new Map(active.map((m) => [String(m.id), m]));
        let cluster = ids.map((id) => byId.get(id)).filter(Boolean) as unknown as MemoryView[];
        if (cluster.length < 2) {
          // 成员被并掉 / 归档了：这条账已经没活可干，终态 done，别每轮重试
          finish('merge_batch', op, 'done', 'stale_members');
          continue;
        }
        // 增量复审（2026-11-23）：账本里的成员逐字没变、已审过的，不再整批重问——
        // 只送变化项 + 最相关的旧邻居；全批都已审过就当场收口 done，不耗 attempts。
        const itemCache = itemCacheRound;
        const snapNow = deps.snapshot();
        const relate = (a: { id?: unknown; text?: unknown; vector?: unknown }, b: { id?: unknown; text?: unknown; vector?: unknown }): number => {
          const va = (a?.vector || (snapNow.vecOf && snapNow.vecOf.get(String(a?.id)))) as ArrayLike<number> | undefined;
          const vb = (b?.vector || (snapNow.vecOf && snapNow.vecOf.get(String(b?.id)))) as ArrayLike<number> | undefined;
          if (va && vb) {
            const c = cosine(va, vb);
            if (c !== null) return c;
          }
          return cheapSimilarity(a as never, b as never);
        };
        const shrunk = shrinkClusterForReview(cluster as unknown as [], itemCache, { relate: relate as never });
        if (shrunk.cluster.length < 2) {
          finish('merge_batch', op, 'done', 'item_settled');
          continue;
        }
        cluster = shrunk.cluster as unknown as MemoryView[];
        const candidates = candidateListFor(cluster, active);
        let answer;
        let summary = '';
        try {
          answer = await askMergeOps({
            items: cluster.map((m) => ({
              id: String(m.id),
              type: store.typeOf(m),
              text: String(m.text || ''),
              ts: String(m.ts || m.validFrom || ''),
              entities: (m.entities || []).map(String),
            })),
            candidates,
            clusterId: batchLabel,
            llm: llmOf(ctx, { site: 'merge:replay', log: (line: string) => log(cfg, line), cfg }),
            provider: cfg.provider,
            model: cfg.model,
            log: (line: string) => log(cfg, line),
            // lint_reject 要能落 discards：把库连接交给 merge agent
            db: pendingDb(),
            textLint: cfg.pipeline?.textLint !== false,
            // lint 两级复核：默认开；false 走旧逻辑
            lintReview: cfg.pipeline?.lintReview !== false,
            lintReviewTimeoutMs: cfg.pipeline?.lintReviewTimeoutMs,
            onFail: (line: string) => { summary = line; },
          });
        } catch (error) {
          answer = undefined;
          summary = shortErr(error);
        }
        if (!answer) {
          finish('merge_batch', op, 'pending', `replay_fail ${summary}`.slice(0, 500));
          continue;
        }
        if (!answer.ops.length) {
          // 两种空要分开：批里本来就没活 → 上面 cache_* / stale_members 已经收口；走到这里
          // 说明批里还有 >=2 条 active 成员、指纹也没被判过，模型却给空 ops：算一次失败，
          // attempts+1，到 MAX_ATTEMPTS(5) 由 markOp 转 dead，不会无限重来。
          finish('merge_batch', op, 'pending', 'replay_empty');
          continue;
        }
        const items = [...cluster, ...candidates].map((m) => ({ id: String(m.id), text: String(m.text || '') }));
        const checked = validateOps(answer.ops, items, {
          maxChars,
          db,
          batchId: batchLabel,
          log: (line: string) => log(cfg, line),
        });
        if (!checked.valid.length) {
          finish('merge_batch', op, 'pending', 'replay_invalid');
          continue;
        }
        const applied = await applyAgentOps(checked.valid, {
          cluster,
          dryRun: false,
          fp: batchLabel,
        });
        const acted = applied.merged.length + applied.updated.length + applied.archived.length +
          applied.added.length + applied.split.length;
        // 条目级结论记进本轮累积表：段尾一次写回，下一轮同内容的条目不再重问
        if (canPersistItems) {
          itemCacheRound = mergeItemReviewCache(
            itemCacheRound,
            reviewAdditionsFromOps(cluster as unknown as [], checked.valid, {
              rejected: applied.rejected as { ids?: unknown; id?: unknown }[],
            }),
          );
          itemDirty = true;
        }
        finish('merge_batch', op, acted ? 'done' : 'pending', acted ? null : 'replay_noop');
      } catch (error) {
        // 单批异常不许把整段拖停：以前抛出去以后剩下的账留在 running，attempts 原地不动，
        // 下一轮租约回收又原样捡起来重跑——那才是真的不收敛。这里按一次失败记上，剩下的照跑。
        finish('merge_batch', op, 'pending', `replay_error ${shortErr(error)}`.slice(0, 500));
      }
    }
    if (canPersistItems && itemDirty) {
      persistItemReviewCache(stateStore as never, itemCacheRound, { cfg: cfg as never, log: (line) => log(cfg, line) });
    }
  } catch (error) {
    log(cfg, `[replay] WARN merge_batch 段失败：${shortErr(error)}`);
  }
  if (stats.merge_batch) {
    log(cfg, `[replay] kind=merge_batch done=${stats.merge_batch.done} failed=${stats.merge_batch.failed} dead=${stats.merge_batch.dead}`);
  }

}

async function replayMergeOpSegment(deps: ReplayDeps): Promise<void> {
  const { finish, dueAll, heartbeat, activeNow, maxChars, stats } = deps;
  // ② merge_op：单条修复性重放；正文仍超长就转 split_item，原文一个字不丢
  try {
    const singleMergeOp = deps.onlyOps?.find((o) => o.kind === 'merge_op');
    for (const op of (singleMergeOp ? [singleMergeOp] : dueAll('merge_op'))) {
      heartbeat();
      const payload = op.payload || {};
      const ids = Array.isArray(payload.ids) ? payload.ids.map(String) : [];
      const active = activeNow();
      const byId = new Map(active.map((m) => [String(m.id), m]));
      const items = ids.map((id) => byId.get(id)).filter(Boolean) as unknown as AgentClusterItem[];
      const text = String(payload.text || '');
      // 2026-11-06 t-fix-quality：源条目原文从账本里兜底（payload.source）。
      // 源条被合并/归档后 items 为空，以前一律 source_missing 重试到 dead；
      // 现在先拿账本里的原文再验一遍证据，验得过但源已不在就记 replay_source_gone 收口。
      const sourceItems = Array.isArray(payload.source)
        ? payload.source
          .map((e) => ({ id: String(e?.id ?? ''), text: String(e?.text ?? ''), type: String(e?.type ?? 'world') }))
          .filter((e) => e.text)
        : [];
      const checkItems = items.length ? items : sourceItems;
      const evidence = Array.isArray(payload.evidence) && payload.evidence.length
        ? payload.evidence.map((e) => ({ id: String(e?.id ?? ''), quote: String(e?.quote ?? '') })).filter((e) => e.quote)
        : ids.slice(0, 1).map((id) => ({ id, quote: String(payload.quote || '') }));
      const candidate = {
        op: String(payload.op || ''),
        ids: ids.length ? ids : checkItems.map((it) => String(it.id)),
        text,
        evidence,
        needsSplit: text.length > maxChars,
        reason: 'merge_op_replay',
      };
      // 2026-11-14：删掉「第 3 次（attempts>=2）转 split」的固定次数逻辑。
      // 验不过就记一次失败（attempts+1、last_error），下一轮照原样再验，不自动放弃。
      const checked = checkItems.length
        ? validateOps([candidate], checkItems, { maxChars, db: null, log: (line: string) => log(cfg, line) })
        : { valid: [], invalid: [{ reason: 'source_missing' }] };
      if (checked.valid.length) {
        if (!items.length) {
          // 只有账本原文、源条已经不在 active：不硬写（会和现库重复），记明原因收口
          log(cfg, `[replay] kind=merge_op id=${op.id} action=replay_source_gone ids=${ids.join(',') || '-'}`);
          finish('merge_op', op, 'done', 'replay_source_gone');
          continue;
        }
        const applied = await applyAgentOps(checked.valid, {
          cluster: items,
          dryRun: false,
          fp: String(payload.batch || op.dedupeKey),
        });
        const acted = applied.merged.length + applied.updated.length + applied.archived.length +
          applied.added.length + applied.split.length;
        finish('merge_op', op, acted ? 'done' : 'pending', acted ? null : 'replay_noop');
        continue;
      }
      // 2026-11-14：这条账当初就是因为 payload.reason 被判 invalid 才入队的
      // （merge.js 的 recordInvalidOps）。拿入队时那份 source 快照再验一遍：
      //   - 快照验出来的原因和当初一样 → 确定性 invalid，再试 5 轮一个字不会变，
      //     只会白推 attempts、每轮白占 compact 的领活名额。当场判死，留 last_error 给人看。
      //   - 快照能过、现库过不了 → 源条已被改/并走，op 没法安全落到现库，同样不空转。
      const recordedReason = String(payload.reason || '');
      const snapCheck = sourceItems.length && items.length
        ? validateOps([candidate], sourceItems, { maxChars, db: null, log: (line: string) => log(cfg, line) })
        : null;
      if (!checked.valid.length && snapCheck && !snapCheck.valid.length) {
        const snapReason = String(snapCheck.invalid[0]?.reason || 'unknown');
        if (!recordedReason || snapReason === recordedReason) {
          log(cfg, `[replay] kind=merge_op id=${op.id} action=dead reason=replay_invalid_permanent ${snapReason}`);
          finish('merge_op', op, 'dead', `replay_invalid_permanent ${snapReason}`);
          continue;
        }
      }
      if (!checked.valid.length && snapCheck && snapCheck.valid.length) {
        log(cfg, `[replay] kind=merge_op id=${op.id} action=dead reason=replay_source_drift ids=${ids.join(',') || '-'}`);
        finish('merge_op', op, 'dead', 'replay_source_drift');
        continue;
      }
      if (text.length > maxChars) {
        const queuedId = queueMergeAgentSplit({ ...candidate, index: 0 }, String(payload.batch || op.dedupeKey), { dryRun: false });
        log(cfg, `[replay] kind=merge_op id=${op.id} action=to_split queued=${queuedId ? 'yes' : 'no'} len=${text.length}`);
        finish('merge_op', op, queuedId ? 'done' : 'pending', queuedId ? null : 'split_enqueue_failed');
        continue;
      }
      // 老账：只有 quote、没有 ids 也没有 source → 重放没有依据，跳过并写明原因，不再无限 pending
      if (!checkItems.length) {
        log(cfg, `[replay] kind=merge_op id=${op.id} action=skip reason=no_source_for_replay quote=${String(payload.quote || '').slice(0, 60)}`);
        finish('merge_op', op, 'done', 'no_source_for_replay');
        continue;
      }
      finish('merge_op', op, 'pending', `replay_invalid ${checked.invalid[0]?.reason || 'unknown'}`);
    }
  } catch (error) {
    log(cfg, `[replay] WARN merge_op 段失败：${shortErr(error)}`);
  }
  if (stats.merge_op) {
    log(cfg, `[replay] kind=merge_op done=${stats.merge_op.done} failed=${stats.merge_op.failed} dead=${stats.merge_op.dead}`);
  }

  /**
   * 老账（2026-11-06 之前）只有 action + candidate，没有 ids：按 candidate 正文 + 实体
   * 召回现库条目补位（2026-11-06 t-fix-quality）。召回也找不到才判死，
   * last_error 写 payload_missing_ids_no_candidate，不再只写 payload_missing_ids。
   */
}

async function replayDedupeCheckSegment(deps: ReplayDeps): Promise<void> {
  const { finish, dueAll, heartbeat, stats } = deps;
  const db = pendingDb();
  const recallIdsFor = async (text: string, entities: unknown, want: number): Promise<string[]> => {
    let hits: RecallRow[] = [];
    try {
      hits = await store.recall!(text, Math.max(3, want + 2), { caller: 'replay:dedupe_check' });
    } catch (error) {
      log(cfg, `[replay] WARN dedupe_check 召回补位失败：${shortErr(error)}`);
      return [];
    }
    // store.recall 对外只给 text/score/entities（记忆工具不给 id），这里用正文反查 id
    const active = store.listAll().filter((m) => (m.status || 'active') === 'active');
    const idByText = new Map(active.map((m) => [String(m.text || ''), String(m.id)]));
    const wanted = new Set(((entities as unknown[]) || []).map((e) => String(e).toLowerCase()));
    const scored = (Array.isArray(hits) ? hits : [])
      .map((h) => {
        const id = h?.id ? String(h.id) : idByText.get(String(h?.text ?? ''));
        if (!id) return null;
        const ents = Array.isArray(h.entities)
          ? h.entities.map((e: unknown) => String(e).toLowerCase())
          : String(h.entities || '').split(/[,，、\s]+/).filter(Boolean).map((e: string) => e.toLowerCase());
        const overlap = wanted.size ? [...wanted].filter((e) => ents.includes(e)).length : 0;
        return { id, overlap, score: Number(h.score) || 0 };
      })
      .filter(Boolean)
      .sort((a, b) => b!.overlap - a!.overlap || b!.score - a!.score);
    return [...new Set(scored.map((x) => x!.id))].slice(0, Math.max(1, want));
  };

  // ③ dedupe_check：重新过写入闸（判得出就补写，判不出就按退避再来，超上限转 dead）
  try {
    if (cfg.pipeline?.writeGate !== true) {
      log(cfg, '[replay] kind=dedupe_check 跳过：写入闸关着（pipeline.writeGate=false）');
    } else {
      const singleDedupe = deps.onlyOps?.find((o) => o.kind === 'dedupe_check');
      for (const op of (singleDedupe ? [singleDedupe] : dueAll('dedupe_check'))) {
        heartbeat();
        const payload = op.payload || {};
        const action = String(payload.action || '');
        const candidate = payload.candidate as Record<string, unknown> || {} as Record<string, unknown>;
        const ids = Array.isArray(payload.ids) ? payload.ids.map(String) : [];
        const text = String(candidate.text || '').trim();
        // 2026-11-06：当初靠复核 keep 才过 lint 的正文，
        // 重放时正文一字不差，得把同一份复核结论带回去，别再判死一次。
        const review = payload.review && typeof payload.review === 'object' ? payload.review : null;
        const markOf = (extra: Record<string, unknown> = {}) => (review ? { ...extra, __lintReview: review } : extra);
        if (!text) {
          finish('dedupe_check', op, 'dead', 'payload_empty_text');
          continue;
        }
        let useIds = ids;
        try {
          if (action === 'save') {
            await store.save(text, candidate.tags || [], candidate.source as string || 'sleep:replay', markOf({
              type: candidate.type,
              entities: candidate.entities,
              sourceFile: candidate.sourceFile,
            }));
            finish('dedupe_check', op, 'done');
          } else {
            if (!useIds.length) {
              useIds = await recallIdsFor(text, candidate.entities, action === 'merge' ? 3 : 1);
              if (useIds.length) {
                log(cfg, `[replay] kind=dedupe_check id=${op.id} action=recall_fallback ids=${useIds.join(',')}`);
              }
            }
            if (action === 'update' && useIds.length === 1) {
              await store.update(useIds[0], markOf({ text }));
              finish('dedupe_check', op, 'done');
            } else if (action === 'merge' && useIds.length >= 2) {
              await store.merge(useIds, text, candidate.tags || [], markOf({ sourceFile: candidate.sourceFile }));
              finish('dedupe_check', op, 'done');
            } else if (action === 'merge' && useIds.length === 1) {
              // 召回只够一条：语义上就是「改这一条」，别硬走 merge（store.merge 要 ≥2 条）
              await store.update(useIds[0], markOf({ text }));
              log(cfg, `[replay] kind=dedupe_check id=${op.id} action=merge_fallback_update ids=${useIds[0]}`);
              finish('dedupe_check', op, 'done');
            } else if (action === 'update' && useIds.length > 1) {
              finish('dedupe_check', op, 'dead', `payload_missing_ids action=update candidates=${useIds.length}`);
            } else {
              // 召回也没找到可改的条目：账本里没带够信息，重试也是空转
              log(cfg, `[replay] kind=dedupe_check id=${op.id} action=skip reason=payload_missing_ids_no_candidate action=${action || '-'}`);
              finish('dedupe_check', op, 'dead', `payload_missing_ids_no_candidate action=${action || '-'}`);
            }
          }
        } catch (error) {
          if ((error as { code?: unknown } | null)?.code === 'WRITE_GATE_PENDING') {
            // store 那一层（enqueueDedupePending）已经 attempts+1、刷新了 next_try_at，这里不重复计数
            const row = db.prepare('SELECT attempts FROM pending_ops WHERE id = ?').get(op.id);
            const attempts = Number(row?.attempts) || 0;
            if (attempts >= MAX_ATTEMPTS) finish('dedupe_check', op, 'dead', shortErr(error));
            else finish('dedupe_check', op, 'pending', shortErr(error), { increment: false });
            continue;
          }
          finish('dedupe_check', op, 'pending', shortErr(error));
        }
      }
    }
  } catch (error) {
    log(cfg, `[replay] WARN dedupe_check 段失败：${shortErr(error)}`);
  }
  if (stats.dedupe_check) {
    log(cfg, `[replay] kind=dedupe_check done=${stats.dedupe_check.done} failed=${stats.dedupe_check.failed} dead=${stats.dedupe_check.dead}`);
  }
}

/** 段表：replay 的调度只认这张表（顺序 = 先批、再单条、最后重过闸）。 */
function replaySegments(): Array<[string, (deps: ReplayDeps) => Promise<void>]> {
  return [
    ['merge_batch', replayMergeBatchSegment],
    ['merge_op', replayMergeOpSegment],
    ['dedupe_check', replayDedupeCheckSegment],
  ];
}

async function replayPendingOps({ dryRun = false, skipIds = [], onlyKinds = null, onlyOps = null }: { dryRun?: boolean; skipIds?: unknown[]; onlyKinds?: string[] | null; onlyOps?: DueOp[] | null } = {}): Promise<Record<string, KindStats>> {
  const stats: Record<string, KindStats> = {};
  const bump = (kind: string, key: keyof KindStats) => {
    if (!stats[kind]) stats[kind] = { done: 0, failed: 0, dead: 0 };
    stats[kind][key] += 1;
  };
  if (dryRun) return stats;
  const db = pendingDb();
  if (!db) {
    log(cfg, '[replay] WARN 没有 pending_ops，三种 agent 账本全部跳过重放');
    return stats;
  }
  const maxChars = Number(cfg.splitMaxChars) || 200;
  const activeNow = () => store.listAll().filter((m) => (m.status || 'active') === 'active');
  // 2026-11-14 维护者拍板：领活粒度 = 当前并发数，不许一次把整队标 running。
  // 这里做成惰性生成器：上一批跑完才领下一批，for...of 用法一个字不用改；
  // 每个处理单元本轮只试一次（失败后 next_try_at=now 会立刻又到期，靠 exclude 挡住同轮重复领）。
  const replayConc = Math.max(1, curatePick('replay').conc);
  const triedThisRound = new Set(
    (Array.isArray(skipIds) ? skipIds : []).map((x) => Number(x)).filter((x) => Number.isFinite(x)),
  );
  function* dueAll(kind: string): Generator<DueOp, void, unknown> {
    // onlyKinds：同一个 compact 通道里只消费本地活（merge_op / dedupe_check），
    // 问模型的 merge_batch 交给 curate 池，不在这儿抢。
    if (Array.isArray(onlyKinds) && !onlyKinds.includes(kind)) return;
    for (;;) {
      const rows = dueOps(db, replayConc, { kind, exclude: [...triedThisRound] });
      if (!rows.length) return;
      for (const row of rows) {
        triedThisRound.add(Number(row.id));
        yield row;
      }
    }
  }
  /** 收尾：写账、记数、打一行。failed 是「还没死、等下一轮」的那部分。 */
  const finish = (kind: string, op: DueOp, status: string, err: unknown = null, opts: { increment?: boolean } = {}): Row | null => {
    // 防御：段的 kind 和账的 kind 对不上时绝不许写终态（2026-11-15 stale_members 事故）。
    // 原样放回 pending，不记 attempts，交给真正认领它的消费者。
    if (op?.kind && op.kind !== kind) {
      const released = markOp(db, op.id, 'pending', 'wrong_replay_kind', { increment: false, log: (line: string) => log(cfg, line) });
      log(cfg, `[replay] WARN kind=${kind} 段领到 kind=${op.kind} 的账 id=${op.id}，已原样放回 pending`);
      return released;
    }
    const after = markOp(db, op.id, status, err, { log: (line: string) => log(cfg, line), ...opts });
    const final = status === 'pending' ? (after?.status || 'pending') : status;
    if (final === 'done') bump(kind, 'done');
    else if (final === 'dead') bump(kind, 'dead');
    else bump(kind, 'failed');
    log(
      cfg,
      `[replay] kind=${kind} id=${op.id} attempts=${after?.attempts ?? op.attempts} status=${final}` +
        `${err ? ` last_error=${shortErr(err)}` : ''}`,
    );
    return after;
  };

  // replay 四段执行体已搬进 handler（2026-11-14 阶段 5）：上下文对象在下面组装，
  // 三个段函数只认 deps（store/ask/validate/apply/candidates/snapshot + 收尾助手），不碰模块级变量。
  const deps: ReplayDeps = {
    store,
    ask: askMergeOps,
    validate: validateOps,
    apply: applyAgentOps,
    candidates: candidateListFor,
    snapshot: activeSnap,
    finish, bump, dueAll, heartbeat, activeNow, maxChars, stats,
    onlyOps: Array.isArray(onlyOps) ? onlyOps : null,
  };
  for (const [kind, run] of replaySegments()) {
    if (Array.isArray(onlyKinds) && !onlyKinds.includes(kind)) continue;
    if (onlyOps && !onlyOps.some((o) => o.kind === kind)) continue;
    await run(deps);
  }
  // 2026-11-14：每轮固定报一行 dead 总数（attempts 到 5 的账），留给人看
  const deadTotal = Object.values(stats).reduce((n, s) => n + (Number(s.dead) || 0), 0);
  log(cfg, `[replay] dead=${deadTotal} 条（attempts=${MAX_ATTEMPTS} 自动停止重试，等人工处理）`);

  return stats;
}

  return { replayPendingOps };
}
