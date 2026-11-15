/**
 * entity_extract 队列的消费端。
 *
 * 一轮 = 领一批（默认 8 条）→ 抽实体模型调用 → 过四道闸 + df 闸 → 实体落库（事务一）
 *      → 用已入库实体反查候选 → 关系判定模型调用 → 写边 + 置 done（事务二）。
 *
 * 为什么拆两段：关系候选是「跟本条共享实体的旧记忆」，
 * 而本条的 memory_entity 行只有实体抽取落库后才存在。候选若在抽取前算，第一次进队列
 * 永远拿到空数组，模型按提示词合法地一条边都不给——线上 memory_link 因此一直是 0 行。
 * 铁律：
 *   - 模型永不在事务里（§2.4 事务边界）；事务只包「写 entities/memory_entity + 置 done」；
 *   - 单条失败只回滚这条（savepoint），整批失败只在事务本身坏时发生：attempts+1，3 次转 dead；
 *     模型空返回（items=[]）也算一次尝试，不许静默 markDone；
 *   - 租约 180s 过期自动回 ready；落库幂等靠 INSERT OR IGNORE，唯一键 (memory_id, entity_id, role)。
 *
 * 调度：10 分钟一 tick，只挂在主会话进程（插件 apply 只在主进程里跑一遍）；另外任何带
 * sessionId 的调用先过 isWorkerSession，worker 会话直接 return——上次整理的冷却就是被
 * 子会话会话吃掉的，这里不重演。
 */
import fs from 'node:fs';
import crypto from 'node:crypto';
import type { DatabaseSync } from 'node:sqlite';
import { isWorkerSession } from './consolidate.js';
import * as paths from './shared/paths.js';
import { extractEntities, judgeLinks } from './agents/entity.js';
import type { EntityLink, ExtractedEntity } from './agents/entity.js';
import {
  BATCH,
  LEASE_MS,
  hasCurateTables,
  claimBatch,
  reapExpired,
  markDone,
  markSkipped,
  failOne,
  queueStats,
  leaseOwner,
} from './entity-queue.js';
import type { CurateClaim } from './entity-queue.js';
import { gateEntityName, upsertEntity, insertMemoryEntity, entityIssue } from './entity-gate.js';
import { hasMemoryLinkTable, writeSupersedesLink } from './memory-link.js';
import { keyLog } from './file-log.js';
import type { KeyLogConfig } from './file-log.js';
import { llmOf } from './llm-service.js';
import type { LlmServiceLike } from './llm-service.js';
import { selectCurateConcurrency, formatStageAdaptiveLog } from './concurrency.js';
import type { ConcurrencyConfig } from './concurrency.js';
import { ensureDirFor } from './shared/fs.js';
import { withTransaction, withSavepoint } from './shared/tx.js';
import type { LedgerCounts, LedgerId } from './ledger.js';

/** entityQueue 这一段读到的配置面；其余键照旧透传。 */
export interface EntityQueueConfig {
  enabled?: unknown;
  consume?: unknown;
  batch?: unknown;
  leaseMs?: unknown;
  concurrency?: unknown;
  tickMs?: unknown;
  maxBatchesPerTick?: unknown;
  genericDf?: unknown;
  genericDfMin?: unknown;
  reasoningEffort?: unknown;
  logFile?: string;
}

/** worker 从插件配置里读到的面（日志文件与并发各接自己的接口）。 */
export interface EntityWorkerConfig extends KeyLogConfig, ConcurrencyConfig {
  provider?: string;
  model?: string;
  entityQueue?: EntityQueueConfig;
}

/** 候选记忆的最小行（linkCandidates / getMem 的产物）。 */
export interface LinkCandidateRow {
  id: string;
  text: string;
  ts: string;
}

/** 领到的一条 + 它对应的 memories 行（可能已经被删，故为 undefined）。 */
export interface ClaimedRow extends CurateClaim {
  mem: LinkCandidateRow | undefined;
}

/** 本批一条的落库明细（进度日志用）。 */
export interface LandedRow {
  id: string;
  attempts: number;
  ms: number;
}

/** 本批一条的失败（事务外记账用）。 */
export interface FailedRow {
  row: ClaimedRow & { mem: LinkCandidateRow };
  error: unknown;
}

/** consumeEntityBatch 的产物：每条记忆的去向计数。 */
export interface EntityBatchResult {
  claimed: number;
  accepted: number;
  written?: number;
  linked?: number;
  rejected: Record<string, number>;
  skipped?: number;
  candidates?: number;
  failed: number;
  done: number;
  llmCalls?: number;
  error?: string;
  linkError?: string;
  [key: string]: unknown;
}

/** runEntityQueueOnce 的产物：一批批累加的总账 + 队列现状。 */
export interface EntityQueueOnceResult {
  claimed?: number;
  accepted?: number;
  written?: number;
  linked?: number;
  skipped?: number | string;
  candidates?: number;
  failed?: number;
  done?: number;
  rejected?: Record<string, number>;
  llmCalls?: number;
  queue?: LedgerCounts | null;
  ms?: number;
  error?: string;
  [key: string]: unknown;
}

/** store 只用到 db() 一个方法。 */
export interface QueueStoreLike {
  db?: () => DatabaseSync;
}

/** setupEntityQueue 的返回面：tick / stats / dispose。 */
export interface EntityQueueHandle {
  tick: (reason?: string) => Promise<EntityQueueOnceResult | undefined>;
  stats: () => EntityQueueOnceResult & {
    running: boolean;
    ticks: number;
    last: EntityQueueOnceResult | null;
    lastError: string;
    disabled: boolean;
    tickMs: number;
    queue: LedgerCounts | null;
  };
  dispose: () => void;
}

/** 候选条数：票面「最多 3 条」写死，别让模型看一屏旧记忆。 */
const LINK_CANDIDATES = 3;

/**
 * 跟这条记忆共享实体最多的最近 3 条 active 旧记忆。
 *
 * 依据 memory_entity 的共享实体数排序，同分按 memories.ts 倒序——取代判定要的都是
 * 「同一批实体里时间更早的那条」，聚类和向量相似度都不合适（改写后余弦会掉到 0.5 以下）。
 * 查不到（没有 memory_entity 行 / 表不在）就返回空数组，调用方据此跳过取代判定、只抽实体。
 * 时机：必须在本条 memory_entity 落库之后调用。本条没有
 * 实体行时 me1 join 恒空——第一次进队列的记忆在抽实体之前算候选，永远只能拿到空数组。
 */
export function linkCandidates(db: DatabaseSync | null | undefined, memoryId: unknown, { limit = LINK_CANDIDATES }: { limit?: number } = {}): LinkCandidateRow[] {
  if (!db || !memoryId) return [];
  try {
    return db.prepare(
      `SELECT m.id, m.text, m.ts, COUNT(DISTINCT me2.entity_id) AS shared
         FROM memory_entity me1
         JOIN memory_entity me2
           ON me2.entity_id = me1.entity_id AND me2.memory_id <> me1.memory_id
         JOIN memories m ON m.id = me2.memory_id
        WHERE me1.memory_id = ?
          AND COALESCE(NULLIF(me2.status, ''), 'active') = 'active'
          AND COALESCE(NULLIF(m.status, ''), 'active') = 'active'
        GROUP BY m.id
        ORDER BY shared DESC, m.ts DESC
        LIMIT ?`,
    ).all(String(memoryId), Math.max(1, Number(limit) || LINK_CANDIDATES))
      .map((r) => ({ id: String(r.id), text: String(r.text || ''), ts: String(r.ts || '') }));
  } catch {
    return [];
  }
}

/** 单轮最多几批（默认 3×8=24 条），防一轮把预算烧穿。 */
export const MAX_BATCHES_PER_TICK = 3;
/** 启动后多久先跑一轮（秒级，别跟重启、睡觉抢）。 */
export const FIRST_TICK_MS = 90000;

/**
 * 队列日志：一行一条，跟 memory_store.log 一样按行追加。
 * 2026-11-14：每行同时进关键日志文件（cfg.sleepLogFile，默认
 * memory_sleep.log），因为 ctx.logger 在这台机器上不进 journald，只写 memory_entity.log
 * 又不在 logrotate 白名单里，两个出口各写一份才既好查又不无限长。
 */
export function entityLog(cfg: EntityWorkerConfig | null | undefined, line: string): void {
  const file = cfg?.entityQueue?.logFile || paths.logFile('memory_entity.log');
  try {
    ensureDirFor(file);
    fs.appendFileSync(file, `[${new Date().toISOString()}] ${line}\n`, 'utf8');
  } catch {
    /* 日志失败不影响队列 */
  }
  keyLog(cfg, line);
}

function startRun(db: DatabaseSync, { now = Date.now(), channel = 'curate' }: { now?: number; channel?: string } = {}): string | null {
  const runId = `r_${now.toString(36)}_${crypto.randomBytes(2).toString('hex')}`;
  try {
    db.prepare(
      `INSERT INTO curate_run(run_id, channel, state, stage, cursor_json, budget_json, owner,
                              lease_expires, started_ts, heartbeat_ts, updated_ts)
       VALUES (?, ?, 'executing', 'entity_extract', '{}', '{}', ?, ?, ?, ?, ?)`,
    ).run(runId, channel, leaseOwner(), now + LEASE_MS, now, now, now);
  } catch {
    return null;
  }
  return runId;
}

function finishRun(db: DatabaseSync, runId: string | null, { state = 'done', cursor = {}, budget = {}, now = Date.now(), stopReason = '' }: { state?: string; cursor?: Record<string, unknown>; budget?: Record<string, unknown>; now?: number; stopReason?: unknown } = {}): void {
  if (!runId) return;
  try {
    db.prepare(
      `UPDATE curate_run
          SET state=?, cursor_json=?, budget_json=?, updated_ts=?, heartbeat_ts=?, lease_expires=0, stop_reason=?
        WHERE run_id=?`,
    ).run(state, JSON.stringify(cursor), JSON.stringify(budget), now, now, String(stopReason).slice(0, 300), runId);
  } catch {
    /* run 表坏了不影响队列本身 */
  }
}

/**
 * 消费一批。
 * @returns {Promise<null|{claimed:number, accepted:number, rejected:object, failed:number, done:number}>}
 */
export async function consumeEntityBatch({ db, cfg, llm, log, now = Date.now(), owner = leaseOwner() }: {
  db: DatabaseSync;
  cfg?: EntityWorkerConfig;
  llm: LlmServiceLike | null | undefined;
  log?: ((line: string) => void) | null;
  now?: number;
  owner?: string;
}): Promise<EntityBatchResult | null> {
  const q = cfg?.entityQueue || {};
  const genericDf = Number(q.genericDf ?? 0.05);
  const genericDfMin = Number(q.genericDfMin ?? 20);
  const claimed = claimBatch(db, { owner, batch: Number(q.batch) || BATCH, now, leaseMs: Number(q.leaseMs) || LEASE_MS });
  if (!claimed.length) return null;

  const getMem = db.prepare('SELECT id, text, ts FROM memories WHERE id=?');
  const rows = claimed.map((c) => ({ ...c, mem: getMem.get(String(c.dedupe_key)) as LinkCandidateRow | undefined }));
  const alive = rows.filter((r) => r.mem) as Array<ClaimedRow & { mem: LinkCandidateRow }>;
  for (const r of rows.filter((x) => !x.mem)) {
    // A3 的反面：记忆已经不在了（软删有行，硬删没有），不重试
    markSkipped(db, [r.id], { now, reason: 'memory_missing' });
  }
  if (!alive.length) return { claimed: claimed.length, accepted: 0, rejected: { memory_missing: claimed.length }, failed: 0, done: 0 };

  // 关系候选必须在本条 memory_entity 落库之后才算。
  // 这里只判断表在不在：memory_link 没建就整段跳过，只抽实体——别让新表缺失把老队列带崩。
  const linkTable = hasMemoryLinkTable(db);

  let items: ExtractedEntity[] = [];
  let llmCalls = 1;
  try {
    // 第一阶段只抽实体：候选这时还算不出来（本条自己的实体行还没插），所以不带 candidates。
    const got = await extractEntities({
      llm,
      provider: cfg?.provider,
      model: cfg?.model,
      items: alive.map((r) => ({
        id: r.mem.id,
        text: r.mem.text,
      })),
      log,
      // 2026-11-14：不再传 maxTokens（配置口子已删），由 provider 封顶
      // 思考档位不写死：配置给才传，空 = 不传，
      // 且由 agents/entity.js 先问模型认不认。写死 'off' 曾被 magpie/group/ds 整批拒掉。
      reasoningEffort: q.reasoningEffort as string | undefined,
    });
    items = Array.isArray(got?.items) ? got.items : [];
    // 空返回也记账（AGENTS「重试与判死：空结果也要记账」）：
    // 只有模型明确给 {"items":[]} 才算有效 noop（这批确实没有新实体，继续走 links 判定）；
    // 没给 items 键/结构坏/空响应，一律算一次尝试，不许静默 markDone。
    if (!items.length && got?.emptyAnswer !== true) {
      const msg = 'empty_items';
      for (const r of alive) failOne(db, { id: r.id, attempts: r.attempts, error: msg, now });
      log?.(`[entity-queue] 一批 ${alive.length} 条空返回（没给 items 结构），attempts+1：${msg}`);
      return {
        claimed: claimed.length, accepted: 0, rejected: { empty_items: alive.length }, failed: alive.length, done: 0, llmCalls, error: msg,
      };
    }
  } catch (error) {
    const msg = String((error && (error as { message?: unknown }).message) || error);
    for (const r of alive) failOne(db, { id: r.id, attempts: r.attempts, error: msg, now });
    log?.(`[entity-queue] 一批 ${alive.length} 条模型失败，attempts+1：${msg}`);
    return { claimed: claimed.length, accepted: 0, rejected: {}, failed: alive.length, done: 0, llmCalls, error: msg };
  }

  const rejected: Record<string, number> = {};
  let accepted = 0;
  let written = 0;
  let linked = 0;
  let done = 0;

  // 第一笔事务：只落实体，不置 done。INSERT OR IGNORE 幂等，重跑不加行。
  // 候选一定要等这笔 COMMIT 之后才算：本条自己的 memory_entity 行不在时，
  // linkCandidates 的 me1 join 永远空——这就是「候选恒空、模型一条边都不给」的根。
  // 单条失败隔离（AGENTS「单条失败隔离」）：每条一个 savepoint，脏数据只回滚它自己；
  // 只有事务本身坏了（BEGIN/COMMIT 失败）才整批退回。
  const landed: LandedRow[] = [];
  const rowFailed: FailedRow[] = [];
  try {
    withTransaction(db, () => {
      for (const it of items) {
        const target = alive[it.i - 1];
        if (!target) continue;
        const rowAt = Date.now();
        try {
          withSavepoint(db, 'entity_row', () => {
            const g = gateEntityName({
              db,
              memoryId: target.mem.id,
              text: target.mem.text,
              rawName: it.name,
              etype: it.etype,
              attrs: it.attrs as Record<string, string>,
              genericDf,
              genericDfMin,
            });
            if (!g.ok) {
              rejected[g.reason] = (rejected[g.reason] || 0) + 1;
              if (g.reason === 'etype_unknown' || g.reason === 'not_in_source' || g.reason === 'generic') {
                entityIssue(db, {
                  memoryId: target.mem.id,
                  norm: g.norm,
                  raw: it.name,
                  reason: g.reason,
                  detail: `agent: etype=${it.etype || ''} name=${String(it.name).slice(0, 80)}`,
                  now,
                });
              }
              return;
            }
            const ent = upsertEntity(db, {
              norm: g.norm,
              display: g.display,
              etype: g.etype,
              attrs: g.attrs,
              ts: target.mem.ts,
              now,
              // surface_conflict / 加载失败这类行只有 upsertEntity 内部知道，把出口传进去
              log: (line) => keyLog(cfg, line),
              // 计数统一留给下面的 insertMemoryEntity：那边只在 memory_entity 真插进一行时 +1，
              // 这里再 touch 一次就会让同一次出现记两遍
              countSurface: false,
            });
            if (insertMemoryEntity(db, {
              memoryId: target.mem.id,
              entityId: ent.id,
              ts: target.mem.ts,
              attrs: g.attrs,
              origin: 'agent',
              log: (line) => keyLog(cfg, line),
            })) written += 1;
            accepted += 1;
            landed.push({ id: target.mem.id, attempts: target.attempts, ms: Date.now() - rowAt });
          });
        } catch (error) {
          rowFailed.push({ row: target, error });
        }
      }
    });
  } catch (error) {
    const msg = String((error && (error as { message?: unknown }).message) || error);
    for (const r of alive) failOne(db, { id: r.id, attempts: r.attempts, error: `db: ${msg}`, now });
    log?.(`[entity-queue] 一批实体落库失败（事务整体坏了），整批退回：${msg}`);
    return { claimed: claimed.length, accepted: 0, rejected, failed: alive.length, done: 0, llmCalls, error: msg };
  }
  // 单条失败在事务外记账：事务已提交，failOne 不会被回滚吞掉；这一条只坏它自己。
  for (const f of rowFailed) {
    const reason = `row: ${String((f.error && (f.error as { message?: unknown }).message) || f.error)}`;
    failOne(db, { id: f.row.id, attempts: f.row.attempts, error: reason, now });
    rejected.row_error = (rejected.row_error || 0) + 1;
    log?.(`[entity-queue] fail id=${f.row.mem.id} attempts=${f.row.attempts} reason=${reason}`);
  }

  // 第二阶段：实体已入库，用本条自己的实体反查候选，单独调一次模型只问 links。
  // 一条候选都没有就不调模型：新库/孤立实体不该为「必定空」的关系判定多付一次 round-trip。
  const candidates = new Map<string, LinkCandidateRow[]>();
  if (linkTable) {
    for (const r of alive) {
      const list = linkCandidates(db, r.mem.id);
      if (list.length) candidates.set(r.mem.id, list);
    }
  }
  // 票面要「每轮候选/合并/跳过统计」：候选条数只有这里算得出来，带上一起回给调用方
  let candidateTotal = 0;
  for (const list of candidates.values()) candidateTotal += list.length;
  let links: EntityLink[] = [];
  let linkError = '';
  if (linkTable && candidates.size) {
    llmCalls += 1;
    try {
      const got = await judgeLinks({
        llm,
        provider: cfg?.provider,
        model: cfg?.model,
        items: alive.map((r) => ({
          id: r.mem.id,
          text: r.mem.text,
          candidates: candidates.get(r.mem.id) || [],
        })),
        log,
        // 2026-11-14：不再传 maxTokens
        reasoningEffort: q.reasoningEffort as string | undefined,
      });
      links = Array.isArray(got?.links) ? got.links : [];
    } catch (error) {
      // 实体已经 COMMIT，不能回滚；边只是附加信息，跳过不压死队列。
      linkError = String((error && (error as { message?: unknown }).message) || error);
      log?.(`[entity-queue] links 判定失败（实体已落库，边跳过）：${linkError}`);
    }
  }

  // 第二笔事务：边 + 收尾。边统一走 memory-link.js 的写入口，护栏（target active / 不覆盖 queue 边）不绕。
  // 只认 rel=supersedes；parallel / unrelated 不落边；模型给了候选外的 id 一律丢。
  let failedIds = new Set<LedgerId>();
  try {
    withTransaction(db, () => {
    if (linkTable && links.length) {
      for (const link of links) {
        if (link.rel !== 'supersedes') continue;
        const src = alive[link.i - 1]?.mem?.id;
        if (!src) continue;
        const allowed = candidates.get(src) || [];
        const target = String(link.target_memory_id || '').trim();
        if (!allowed.some((c) => c.id === target)) {
          log?.(`[entity-queue] 取代边跳过 ${src}→${target || '(空)'}：target 不在候选里`);
          continue;
        }
        const w = writeSupersedesLink(db, { src, dst: target, origin: 'queue', attrKey: '', now });
        if (w.ok) linked += 1;
        else log?.(`[entity-queue] 取代边跳过 ${src}→${target}：${w.reason}`);
      }
    }
    failedIds = new Set(rowFailed.map((f) => f.row.id));
    done = markDone(db, alive.filter((r) => !failedIds.has(r.id)).map((r) => r.id), { now });
    });
    // 逐条进度日志（AGENTS「进度日志：落地就打」）：id 用记忆 id（= dedupe_key），一条一行。
    // 明确空返回的批没有实体落地，ms 记 0，但照样留一行「这条收尾了」，别让人以为没跑。
    const landedById = new Map(landed.map((l) => [l.id, l]));
    for (const r of alive) {
      if (failedIds.has(r.id)) continue;
      const one = landedById.get(r.mem.id);
      log?.(`[entity-queue] done id=${r.mem.id} attempts=${r.attempts} ms=${one ? one.ms : 0}`);
    }
  } catch (error) {
    const msg = String((error && (error as { message?: unknown }).message) || error);
    for (const r of alive) failOne(db, { id: r.id, attempts: r.attempts, error: `db: ${msg}`, now });
    log?.(`[entity-queue] 一批收尾失败（实体已落库，重跑幂等）：${msg}`);
    return { claimed: claimed.length, accepted, written, rejected, failed: alive.length, done: 0, llmCalls, error: msg };
  }
  return {
    claimed: claimed.length,
    accepted,
    written,
    linked,
    rejected,
    skipped: claimed.length - alive.length,
    candidates: candidateTotal,
    // 单条失败的条目也要进总账（它们已回 ready/dead，不算 done）
    failed: rowFailed.length,
    done,
    llmCalls,
    ...(linkError ? { linkError } : {}),
  };
}


/**
 * 跑一轮队列（最多 maxBatches 批），并发 concurrency。
 * @param {object} opts
 * @param {object} opts.store createStore 的返回值（只用 db()）
 * @param {object} opts.cfg
 * @param {object} [opts.llm] ctx.llm
 * @param {(line:string)=>void} [opts.log]
 * @param {string} [opts.sessionId] 有就判 worker；没有（定时器调用）按主会话
 * @param {object} [opts.header] session.header
 * @param {number} [opts.maxBatches]
 * @param {number} [opts.now]
 */
export async function runEntityQueueOnce({
  store,
  cfg,
  llm,
  log,
  sessionId = '',
  header,
  maxBatches,
  now = Date.now(),
}: {
  store?: QueueStoreLike | null;
  cfg?: EntityWorkerConfig;
  llm?: LlmServiceLike | null;
  log?: ((line: string) => void) | null;
  sessionId?: string;
  header?: Record<string, unknown> | null;
  maxBatches?: number;
  now?: number;
} = {}): Promise<EntityQueueOnceResult> {
  const slog = typeof log === 'function' ? log : (line: string) => entityLog(cfg, line);
  // worker 会话直接 return：定时器本身没有 sessionId，但任何带会话的调用都不许在 worker 里跑
  if (sessionId || header) {
    if (isWorkerSession({ header, sessionId })) return { skipped: 'worker_session' };
  }
  const db = store?.db ? store.db() : null;
  if (!db) return { skipped: 'no_db' };
  if (!hasCurateTables(db)) return { skipped: 'no_tables' };
  if (!llm || typeof llm.stream !== 'function') return { skipped: 'no_llm' };

  const q = cfg?.entityQueue || {};
  const budget = Math.max(1, Number(maxBatches) || Number(q.maxBatchesPerTick) || MAX_BATCHES_PER_TICK);
  // 2026-11-14：并发跟全链同一套自适应（cap 64），删掉写死 2、硬顶 4；
  // entityQueue.concurrency>0 只当这条通道的手工上限（只许往下压）。
  // 负载快照在每次 tick 开头取一次，tick 内不变（它有自己 10 分钟一 tick 的节奏，不跟整理轮共用快照）。
  const pick = selectCurateConcurrency(cfg);
  const override = Number(q.concurrency) || 0;
  const concurrency = Math.max(1, Math.min(64, override > 0 ? Math.min(override, pick.conc) : pick.conc));
  if (pick.adaptive) slog(formatStageAdaptiveLog('entity-queue', pick));
  const reaped = reapExpired(db, { now });
  if (reaped) slog(`[entity-queue] 租约过期捞回 ready：${reaped} 条`);

  const runId = startRun(db, { now });
  const t0 = Date.now();
  const total: EntityQueueOnceResult & { claimed: number; accepted: number; written: number; linked: number; skipped: number; candidates: number; failed: number; done: number; rejected: Record<string, number> } = { claimed: 0, accepted: 0, written: 0, linked: 0, skipped: 0, candidates: 0, failed: 0, done: 0, rejected: {} };
  let used = 0;
  let calls = 0;

  // 函数声明会被提升，TS 不保留外层 const 的收窄；先把已判空/已收窄的句柄固定成别名再进闭包。
  const runDb = db;
  const runCfg = cfg;
  async function loop(): Promise<void> {
    while (used < budget) {
      used += 1;
      const r = await consumeEntityBatch({ db: runDb, cfg: runCfg, llm, log: slog, now: Date.now() });
      if (!r) return;
      // 一批现在最多两次模型调用：抽实体一次，实体落库后判关系再来一次。
      calls += Number(r.llmCalls) || 1;
      total.claimed += r.claimed;
      total.accepted += r.accepted;
      total.written += r.written || 0;
      total.linked += r.linked || 0;
      total.skipped += r.skipped || 0;
      total.candidates += r.candidates || 0;
      total.failed += r.failed;
      total.done += r.done;
      for (const [k, v] of Object.entries(r.rejected || {})) total.rejected[k] = (total.rejected[k] || 0) + v;
    }
  }

  try {
    await Promise.all(Array.from({ length: concurrency }, () => loop()));
  } catch (error) {
    finishRun(db, runId, { state: 'failed', budget: { llm_calls: calls, ms: Date.now() - t0 }, now: Date.now(), stopReason: String((error as { message?: unknown } | null | undefined)?.message || error) });
    throw error;
  }
  const ms = Date.now() - t0;
  finishRun(db, runId, { state: 'done', cursor: {}, budget: { llm_calls: calls, ms, ops: total.claimed }, now: Date.now() });
  total.ms = ms;
  total.llmCalls = calls;
  total.queue = queueStats(db);
  if (total.claimed) {
    slog(
      `[entity-queue] 一轮 ${ms}ms：领 ${total.claimed} 落 ${total.accepted}（新行 ${total.written}，取代边 ${total.linked}）` +
        ` 候选 ${total.candidates} 跳过 ${total.skipped} 失败 ${total.failed} 拒 ${JSON.stringify(total.rejected)} 模型调用 ${calls} 次`,
    );
  }
  return total;
}

/**
 * 挂 10 分钟一 tick 的调度。只在主会话进程里跑：插件 apply 只在主进程执行一次，
 * timer 自己也不带会话上下文，所以不会被子会话会话"点着"。
 *
 * @param {object} ctx
 * @param {object} store
 * @param {object} cfg
 */
export function setupEntityQueue(ctx: unknown, store: QueueStoreLike, cfg: EntityWorkerConfig): EntityQueueHandle {
  const q = cfg?.entityQueue || {};
  const disabled = q.enabled === false || q.consume === false;
  const tickMs = Math.max(60000, Number(q.tickMs) || 10 * 60000);
  const state: { running: boolean; ticks: number; last: EntityQueueOnceResult | null; lastError: string } = { running: false, ticks: 0, last: null, lastError: '' };
  let timer: ReturnType<typeof setInterval> | null = null;
  let firstTimer: ReturnType<typeof setTimeout> | null = null;
  let disposed = false;

  async function tick(reason = 'timer'): Promise<EntityQueueOnceResult | undefined> {
    if (disposed || state.running) return;
    state.running = true;
    const t0 = Date.now();
    try {
      const r = await runEntityQueueOnce({ store, cfg, llm: llmOf(ctx, { site: 'entity', log: (line) => entityLog(cfg, line), cfg }) });
      state.ticks += 1;
      state.last = r;
      state.lastError = '';
      if (r?.skipped && r.skipped !== 'no_llm') entityLog(cfg, `[entity-queue] tick(${reason}) 跳过：${r.skipped}`);
      return r;
    } catch (error) {
      state.lastError = String((error as { message?: unknown } | null | undefined)?.message || error);
      entityLog(cfg, `[entity-queue] tick(${reason}) 失败：${state.lastError}，${Date.now() - t0}ms`);
      return { error: state.lastError };
    } finally {
      state.running = false;
    }
  }

  const start = (): void => {
    if (disabled || timer) return;
    if (!hasCurateTables(store.db!())) {
      entityLog(cfg, '[entity-queue] curate_queue 不在，调度不挂（先跑 scripts/apply-curate-schema.mjs）');
      return;
    }
    timer = setInterval(() => void tick('timer'), tickMs);
    if (typeof timer.unref === 'function') timer.unref();
    firstTimer = setTimeout(() => void tick('first'), FIRST_TICK_MS);
    if (typeof firstTimer.unref === 'function') firstTimer.unref();
    entityLog(cfg, `[entity-queue] 调度已挂：每 ${Math.round(tickMs / 1000)}s 一 tick，批 ${Number(q.batch) || BATCH}，并发 ${Number(q.concurrency) > 0 ? Number(q.concurrency) : '自适应'}`);
  };

  start();

  return {
    tick,
    stats: () => ({ ...state, disabled, tickMs, queue: hasCurateTables(store.db!()) ? queueStats(store.db!()) : null }),
    dispose() {
      disposed = true;
      if (timer) clearInterval(timer);
      if (firstTimer) clearTimeout(firstTimer);
      timer = null;
      firstTimer = null;
    },
  };
}
