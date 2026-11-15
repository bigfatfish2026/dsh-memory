/**
 * 队列 + 池的调度器：起 ingest / curate / split / compact 四路池 → 等空 → 汇总。
 */
import path from 'node:path';
import { consumeSplitOps } from '../pipeline-split.js';
import {
  claimChannelOps, reapChannelOps, channelStats, markOp, failFile, hasIngestTables, LEASE_MS,
} from '../ingest-index.js';
import { createChannelPool, createWriteLock } from '../pool.js';
import { kindsOfPool } from '../kind-handlers.js';
import { discardCountOf } from '../write-gate.js';
import { llmOf } from '../llm-service.js';
import {
  ingestLoadSnapshot, selectIngestConcurrency, selectCurateConcurrency,
  formatIngestAdaptiveLog, formatStageAdaptiveLog, formatPoolSummary,
} from '../concurrency.js';
import { dayKey } from './slots.js';
import type { DatabaseSync } from 'node:sqlite';
import type { ChannelPoolOptions } from '../pool.js';
import type { ConcurrencyPick } from '../concurrency.js';
import type { SleepCfg, SleepCtx, SleepStore } from './env.js';
import type { SleepStateStore } from '../sleep-state.js';
import type { createPool } from './pool.js';
import type { createOrganize } from './organize.js';
import type { createIngest } from './ingest.js';
import type { createPasses } from './passes.js';
import type { createScheduler } from './scheduler.js';
import type { PoolRowLike, MergeCommitResult } from './pool.js';
import type { DueOp } from '../ingest-index.js';
import type { PreparedFile } from './ingest.js';
import type { MergeBox } from './pool.js';
import type { CacheValue } from '../organize-cache.js';

/** 本轮落库计数。 */
interface TotalResult {
  added: unknown[];
  merged: unknown[];
  updated: unknown[];
  archived: unknown[];
  rejected: unknown[];
  conflictCheck: unknown[];
  [key: string]: unknown;
}

/** ingest 通道的计数。 */

/** curate 通道的计数。 */
interface CurateStats {
  batches: number;
  planned: number;
  cached: number;
  merge: number;
  updated: number;
  archived: number;
  added: number;
  splitQueued: number;
  failed: number;
  split?: Record<string, unknown>;
}

/** 池结果（channelStats 的形状）。 */
interface PoolLanded {
  ok?: boolean;
  phase?: string;
  error?: unknown;
  value?: { one?: { events?: unknown; cand?: unknown; chars?: unknown } };
}

/** 本文件用到的 env 面。 */
interface PoolPassEnv {
  ctx: SleepCtx;
  store: SleepStore;
  cfg: SleepCfg;
  stateStore: SleepStateStore;
  /** 本轮是谁在跑（bootId:pid:starttime），curate_run 记账要写它。 */
  sleepOwner: string;
  log: (line: string) => void;
  push: (msg: string) => void;
  snapshot: (cfg: SleepCfg, store: SleepStore) => Promise<string>;
  scheduler: ReturnType<typeof createScheduler>;
  ledger: { pendingDb: () => DatabaseSync | null };
  organize: ReturnType<typeof createOrganize>;
  ingest: ReturnType<typeof createIngest>;
  pool: ReturnType<typeof createPool>;
  passes: ReturnType<typeof createPasses>;
}

export function createPoolPass(env: PoolPassEnv) {
  const { ctx, store, cfg, stateStore, sleepOwner } = env;
  const log = (_c: SleepCfg, line: string) => env.log(line);
  const snapshot = (...a: Parameters<PoolPassEnv['snapshot']>) => env.snapshot(...a);
  const pendingDb = (...a: Parameters<PoolPassEnv['ledger']['pendingDb']>) => env.ledger.pendingDb(...a);
  const persistOrganizeCache = (...a: Parameters<ReturnType<typeof createOrganize>['persistOrganizeCache']>) => env.organize.persistOrganizeCache(...a);
  const shortErr = (...a: Parameters<ReturnType<typeof createIngest>['shortErr']>) => env.ingest.shortErr(...a);
  const emptyActionResult = (...a: Parameters<ReturnType<typeof createIngest>['emptyActionResult']>) => env.ingest.emptyActionResult(...a);
  const mergeInto = (...a: Parameters<ReturnType<typeof createIngest>['mergeInto']>) => env.ingest.mergeInto(...a);
  const prepareOneFile = (...a: Parameters<ReturnType<typeof createIngest>['prepareOneFile']>) => env.ingest.prepareOneFile(...a);
  const commitOneFile = (...a: Parameters<ReturnType<typeof createIngest>['commitOneFile']>) => env.ingest.commitOneFile(...a);
  const startCompactPool = (...a: Parameters<ReturnType<typeof createPool>['startCompactPool']>) => env.pool.startCompactPool(...a);
  const planPoolQueues = (...a: Parameters<ReturnType<typeof createPool>['planPoolQueues']>) => env.pool.planPoolQueues(...a);
  const prepareMergeBatchOp = (...a: Parameters<ReturnType<typeof createPool>['prepareMergeBatchOp']>) => env.pool.prepareMergeBatchOp(...a);
  const commitMergeBatchOp = (...a: Parameters<ReturnType<typeof createPool>['commitMergeBatchOp']>) => env.pool.commitMergeBatchOp(...a);
  const legacySleepPass = (...a: Parameters<ReturnType<typeof createPasses>['legacySleepPass']>) => env.passes.legacySleepPass(...a);

  let pooledSleepPass: (opts?: { dryRun?: boolean; sinceHours?: number }) => Promise<Record<string, unknown>>;

/**
 * 队列 + 池的调度器（2026-11-14 维护者拍板）：
 * 起几个池 → 等它们都空 → 汇总。三个通道各领各的活，一个池卡住不拖死别的。
 */
pooledSleepPass = async function pooledSleepPass({ dryRun = false, sinceHours = 0 }: { dryRun?: boolean; sinceHours?: number } = {}): Promise<Record<string, unknown>> {
  if (!env.scheduler.beginPass()) return { skipped: 'busy' };
  const started = Date.now();
  const db = store.db();
  // 「拒绝」以 discards 真实落账为准：开头/收尾各数一次，差值就是本轮新增。
  // 提案打回（applied.rejected）另列，别再把两个口径写成同一个数。
  const discardsBefore = discardCountOf(db);
  try {
    const st = stateStore.load();
    const since = sinceHours > 0
      ? Date.now() - sinceHours * 3600000
      : Number(st.lastSleepAt) || Date.now() - 24 * 3600000;
    if (!hasIngestTables(db)) {
      log(cfg, '[pool] 库里缺 ingest_files/pending_ops，pool 模式回退 legacy 路径');
      return await legacySleepPass({ dryRun, sinceHours });
    }

    if (cfg.pipeline?.agentPipeline !== true) {
      log(cfg, '[pool] pipeline.agentPipeline!=true：pool 模式的 curate 通道固定走 MergeAgent（回滚请切 organizePipelineMode=legacy）');
    }
    if (cfg.pipeline?.ingestAck === false) {
      log(cfg, '[pool] pipeline.ingestAck=false：pool 模式统一按文件 ack 入库（队列只认 ingest_files 的账）');
    }
    // 一轮只取一次负载快照：三个通道共用，批与批之间不重算，路数不抖。
    const roundSnapshot = env.scheduler.setRoundSnapshot(ingestLoadSnapshot());
    const ingestPick = selectIngestConcurrency(cfg, roundSnapshot);
    const curatePickV = selectCurateConcurrency(cfg, roundSnapshot);
    const concOf = (pick: ConcurrencyPick) => pick.conc;
    log(
      cfg,
      `[pool] 轮开始 mode=pool ingest=${concOf(ingestPick)}/${ingestPick.cap} curate=${concOf(curatePickV)}/${curatePickV.cap}` +
        ` 自适应=${ingestPick.adaptive ? 'on' : 'off'}`,
    );
    if (ingestPick.adaptive) log(cfg, formatIngestAdaptiveLog(ingestPick));
    if (curatePickV.adaptive) log(cfg, formatStageAdaptiveLog('curate', curatePickV));

    // ① 规划：只入队，不消费
    const planned = planPoolQueues(db, { dryRun, lastSleepAt: since });
    log(
      cfg,
      `[pool] 规划 文件 ${planned.files}（入队 ${planned.ingest}） 合并批 ${planned.batches}（入队 ${planned.merge}，缓存跳过 ${planned.cached}）` +
        ` 待拆 ${planned.split} 实体整理 ${planned.tidy || 0} legacy_acked=${planned.legacy}`,
    );

    const total: TotalResult = { ...emptyActionResult(), conflictCheck: [] };
    const ingestStats = {
      scanned: planned.files, processed: 0, done: 0, failed: 0, queued: 0, dead: 0, events: 0, cand: 0, chars: 0,
    };
    const curateStats: CurateStats = { batches: 0, planned: planned.merge, cached: planned.cached, merge: 0, updated: 0, archived: 0, added: 0, splitQueued: 0, failed: 0 };
    const mergeCache = env.scheduler.mergeCache();

    // ② 有活才备份
    let backup = '';
    const left = {
      ingest: channelStats(db, 'ingest'),
      curate: channelStats(db, 'curate'),
      compact: channelStats(db, 'compact'),
    };
    const hasWork = (left.ingest.pending || 0) + (left.curate.pending || 0) + (left.compact.pending || 0) > 0;
    if (!dryRun && hasWork) backup = await snapshot(cfg, store);

    // ③ 起池：ingest / curate(merge) / split / compact 四路并发，写库共用一把闸
    const writeLock = createWriteLock();
    const poolLog = (line: string) => log(cfg, line);

    const ingestPool = createChannelPool({
      channel: 'ingest',
      concurrency: concOf(ingestPick),
      writeLock,
      log: poolLog,
      claim: (n: number, exclude: number[]) => claimChannelOps(db, 'ingest', n, { leaseMs: LEASE_MS, exclude }) as unknown as PoolRowLike[],
      reap: () => reapChannelOps(db, { leaseMs: LEASE_MS, channel: 'ingest' }),
      remaining: () => channelStats(db, 'ingest').pending,
      prepare: (op: PoolRowLike) => prepareOneFile(String(op.payload?.path || op.dedupeKey || '')),
      commit: async (op: PoolRowLike, prepared: unknown) => {
        const one = await commitOneFile(prepared as PreparedFile, { dryRun });
        markOp(db, op.id, 'done', null, { dryRun, log: poolLog });
        ingestStats.processed += 1;
        ingestStats.done += 1;
        ingestStats.events += Number(one.events) || 0;
        ingestStats.cand += Number(one.cand) || 0;
        ingestStats.chars += Number(one.chars) || 0;
        mergeInto(total, one);
        return { one };
      },
      release: (op: PoolRowLike, error: unknown) => {
        const path0 = String(op.payload?.path || op.dedupeKey || '');
        if (!path0) {
          markOp(db, op.id, 'dead', 'payload 缺 path', { dryRun, log: poolLog });
          ingestStats.dead += 1;
          return { dead: true };
        }
        // failFile 自己会把这个 ingest_file 账 attempts+1、dead 或回 pending，别再补一刀
        const f = failFile(db, path0, error, { dryRun, log: poolLog });
        ingestStats.failed += 1;
        if (f.status === 'dead') { ingestStats.dead += 1; return { dead: true }; }
        ingestStats.queued += 1;
        return { queued: true };
      },
      // 逐文件行在落地那一刻打（和 legacy 同一形状），item/box/result 是池按位置传的
      onLanded: (item: PoolRowLike, _box: unknown, result: PoolLanded | undefined, _index: number, ms: number) => {
        const base = path.basename(String(item?.payload?.path || item?.dedupeKey || '')) || '(unknown)';
        if (result?.ok) log(cfg, `[ingest] file=${base} ok=${Number(result.value?.one?.events) || 0} ms=${ms}`);
        else log(cfg, `[ingest] file=${base} FAIL ${result?.phase || 'error'}: ${shortErr(result?.error)}`);
      },
    } as unknown as ChannelPoolOptions);

    const mergePool = createChannelPool({
      channel: 'curate',
      concurrency: concOf(curatePickV),
      writeLock,
      log: poolLog,
      claim: (n: number, exclude: number[]) => claimChannelOps(db, 'curate', n, { leaseMs: LEASE_MS, kinds: kindsOfPool('merge'), exclude }) as unknown as PoolRowLike[],
      reap: () => reapChannelOps(db, { leaseMs: LEASE_MS, channel: 'curate' }),
      remaining: () => channelStats(db, 'curate').pending,
      prepare: prepareMergeBatchOp,
      commit: async (op: PoolRowLike, box: unknown) => {
        const out: MergeCommitResult = await commitMergeBatchOp(op as unknown as DueOp, box as MergeBox);
        if (out?.applied) {
          curateStats.merge += out.applied.merged.length;
          curateStats.updated += out.applied.updated.length;
          curateStats.archived += out.applied.archived.length;
          curateStats.added += out.applied.added.length;
          curateStats.splitQueued += out.applied.split.length;
          total.merged.push(...out.applied.merged);
          total.updated.push(...out.applied.updated);
          total.archived.push(...out.applied.archived);
          total.added.push(...out.applied.added);
          total.rejected.push(...out.applied.rejected);
        }
        curateStats.batches += 1;
        if (out?.failed) curateStats.failed += 1;
        return out;
      },
      release: (op: PoolRowLike, error: unknown) => {
        markOp(db, op.id, 'pending', shortErr(error), { log: poolLog });
        curateStats.failed += 1;
        return { failed: true };
      },
      onLanded: (item: PoolRowLike, _box: unknown, result: PoolLanded | undefined, _index: number, ms: number) => {
        const fp = String(item?.payload?.batch || item?.dedupeKey || '').slice(0, 12);
        if (result?.ok) log(cfg, `[curate] batch=${fp} op=${item?.id} ms=${ms}`);
        else log(cfg, `[curate] batch=${fp} FAIL ${result?.phase || 'error'}: ${shortErr(result?.error)}`);
      },
    } as unknown as ChannelPoolOptions);

    // split 活是 curate 通道的另一种 kind：单独一个消费者，跟合并池并行，互不阻塞
    const splitTask = (async () => {
      if (dryRun) return null;
      try {
        const sv = await consumeSplitOps({
          textLint: cfg.pipeline?.textLint !== false,
          lintReview: cfg.pipeline?.lintReview !== false,
          lintReviewTimeoutMs: cfg.pipeline?.lintReviewTimeoutMs,
          db: pendingDb() as DatabaseSync,
          store,
          llm: llmOf(ctx, { site: 'split', log: (line) => log(cfg, line), cfg }),
          provider: cfg.provider,
          model: cfg.model,
          concurrency: concOf(curatePickV),
          writeLock,
          maxChars: Number(cfg.splitMaxChars) || 200,
          log: poolLog,
        } as unknown as Parameters<typeof consumeSplitOps>[0]);
        curateStats.split = { scanned: sv.scanned, inserted: sv.inserted, deduped: sv.deduped, archived: sv.archived, failed: sv.failed, concurrency: sv.concurrency };
        return sv;
      } catch (error) {
        log(cfg, `[split] WARN pool 拆分段整段失败：${shortErr(error)}`);
        return null;
      }
    })();

    const compact = startCompactPool({ dryRun, db, writeLock });

    ingestPool.start();
    mergePool.start();
    const [ingestPoolStats, mergePoolStats, splitSv] = await Promise.all([
      ingestPool.drain(), mergePool.drain(), splitTask, compact.pool.drain(),
    ]);
    const compactStats = compact.stats;
    // 写库串行闸的实测峰值：三个池共用一把闸，串行=1。日志里必须有这个数，
    // 不然「写库串行」只能靠读代码相信。
    const lockStats = typeof writeLock.stats === 'function' ? writeLock.stats() : null;
    if (lockStats) log(cfg, `[pool] 写库闸 并发峰值 ${lockStats.peak}（串行=${lockStats.peak <= 1 ? '1' : '否'}）`);
    // 收尾：cache 轮转归 compact（纯本地，0 次 LLM），并写一条 curate_run 让 channelStats 能报 llm/local
    try {
      const nowTs = Date.now();
      db.prepare(
        `INSERT INTO curate_run(run_id, channel, state, stage, cursor_json, budget_json, owner, lease_expires, started_ts, heartbeat_ts, updated_ts)
         VALUES (?, 'compact', 'done', 'compact', '{}', ?, ?, 0, ?, ?, ?)`,
      ).run(
        `c_${nowTs.toString(36)}_${Math.random().toString(36).slice(2, 6)}`,
        JSON.stringify({ llm_calls: compactStats.llm, local_ops: compactStats.local, ops: compactStats.ops }),
        sleepOwner,
        nowTs,
        nowTs,
        nowTs,
      );
    } catch {
      /* curate_run 不在就只靠内存日志 */
    }
    const tidyInfo = compactStats.tidy
      ? ` 实体整理 run_id=${compactStats.tidy.runId} 自动合候选 ${compactStats.tidy.groups} 组/${compactStats.tidy.merges} 次` +
        `（surface ${compactStats.tidy.surface} 行 mount ${compactStats.tidy.mount} 行）只报 ${compactStats.tidy.report} 对` +
        ` 已合 ${compactStats.tidy.applied}` +
        (compactStats.tidy.applied > 0
          ? ` 撤回=node scripts/entity-tidy.mjs --undo-run ${compactStats.tidy.runId} 报告=${compactStats.tidy.reportFile || '-'}`
          : '')
      : '';
    log(cfg, `[compact] 轮 LLM ${compactStats.llm} 次 / 本地 ${compactStats.local} 项（op ${compactStats.ops} 作废 ${compactStats.revoked} 失败 ${compactStats.failed}）${tidyInfo}`);

    // ④ 收尾：organizeCache 合并本轮新结论（persist 里带轮转，纯本地，只做一次）。
    // 必须「老条目 + 本轮新结论」一起写，别拿本轮那几条把主文件整个覆盖掉。
    if (!dryRun && Object.keys(mergeCache).length) {
      persistOrganizeCache({ ...(stateStore.load().organizeCache || {}), ...mergeCache } as Record<string, CacheValue>);
    }

    // ⑤ 汇总（各通道各一行，最后一行总账）
    log(cfg, formatPoolSummary('ingest', ingestPoolStats as unknown as Record<string, unknown>));
    log(cfg, formatPoolSummary('curate', mergePoolStats as unknown as Record<string, unknown>));
    if (splitSv && splitSv.scanned) {
      log(
        cfg,
        `[pool:curate-split] 并发 ${Math.min(splitSv.concurrency || 1, splitSv.scanned)} 路 条 ${splitSv.scanned} ` +
          `成功 ${Math.max(0, splitSv.scanned - (splitSv.failed || 0))} 失败 ${splitSv.failed || 0}`,
      );
    }
    const revokeArchived = compactStats.revoked || 0;
    total.archived.push(...(compactStats.archived || []));

    const changed = total.added.length + total.merged.length + total.updated.length + total.archived.length;
    const ms = Date.now() - started;
    if (!dryRun && changed > 0) {
      stateStore.patch({
        lastSleepAt: Date.now(),
        lastSleepDay: dayKey(new Date()),
        lastRunAt: Date.now(),
        lastSummary: { changed, ms, at: new Date().toISOString() },
      });
    }
    const discardsAdded = Math.max(0, discardCountOf(db) - discardsBefore);
    const org = {
      calls: curateStats.batches,
      clusters: planned.batches,
      total: store.listAll().filter((m) => (m.status || 'active') === 'active').length,
      batches: planned.batches,
      cached: planned.cached,
      failed: curateStats.failed,
      merged: total.merged,
      updated: total.updated,
      archived: total.archived,
      rejected: total.rejected,
      discards: discardsAdded,
    };
    const splitInfo = {
      scanned: splitSv?.scanned || 0,
      tried: splitSv?.tried || 0,
      split: splitSv?.archived || 0,
      failed: splitSv?.failed || 0,
      inserted: splitSv?.inserted || 0,
      deduped: splitSv?.deduped || 0,
      archived: splitSv?.archived || 0,
    };
    log(
      cfg,
      `[pool] 轮完成 新增${total.added.length} 合并${total.merged.length} 修订${total.updated.length}` +
        ` 归档${total.archived.length} 拒绝${discardsAdded}(discards) 打回${total.rejected.length}(提案)` +
        ` 作废清扫${revokeArchived}` +
        ` 整理${curateStats.batches}批/共${planned.batches}批 拆分${splitInfo.split}条` +
        ` 备份${backup ? '有' : '无'} 耗时${ms}ms`,
    );
    return {
      ran: true,
      mode: 'pool',
      dryRun,
      ms,
      backup,
      messages: 0,
      sessions: planned.files,
      chars: ingestStats.chars,
      ...total,
      discards: discardsAdded,
      organize: org,
      split: splitInfo,
      splitInserted: splitInfo.inserted,
      splitDeduped: splitInfo.deduped,
      splitArchived: splitInfo.archived,
      ingest: ingestStats,
      tidy: compactStats.tidy || null,
      pools: {
        ingest: ingestPoolStats,
        curate: mergePoolStats,
        split: splitSv ? { ...splitSv } : null,
        compact: { archive: revokeArchived, llm_calls: compactStats.llm, local_ops: compactStats.local, replay: compactStats.replay },
      },
    };
  } finally {
    env.scheduler.endPass();
  }
}

  return { pooledSleepPass };
}
