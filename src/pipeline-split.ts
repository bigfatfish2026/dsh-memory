/**
 * 拆分流水线的消费端（2026-11-05）。
 *
 * 把 pending_ops(kind='split_item') 里排队的条目领出来：split() → 硬校验 →
 * 每一子段单独过写入闸（store.save → assertWriteAllowed，开关 pipeline.writeGate）→ 入库，
 * 成功后原条 archive、子段 supersedes 挂原条，op 置 done。
 *
 * 顺序（本票定的）：合并 agent 在同一轮里产出 needsSplit 的条目入队，拆分就在
 * organizePassAgent 末尾处理。理由：原文刚在内存里热着，不用等下一轮重新读；
 * 失败的会由 split() 自己 attempts+1 退避，天然顺延到下一轮，不会丢。
 *
 * 日志固定两行：
 *   [split] item= parts= retry= rejected= inserted= deduped= ms=
 *   [split] unavoidable item= reason=（由 agents/split.js 出）
 *
 * 2026-11-14：一段一条问模型太慢（单条 19-24 秒），照 ingest 那套改成
 * 「小批领活 → 批内 N 路并发问模型 → 落地串行」：
 *   - 每批只领 min(并发, 剩余) 条。dueOps 一领就把 op 标 running，一次把几百条全领走等于
 *     把租约占死（之前 599 条同一秒全 running 就是这个后果），所以一批做完再领下一批；
 *   - 批内 prepare（问模型 + 解析出拆分段）并发 N 路，commit（写库 + 标 done/dead +
 *     落 pending_ops）仍由主循环按序一条条做，写库永远只有一条线；
 *   - 并发数由调用方传：sleep.js 用 selectIngestConcurrency（与 ingest 同一个
 *     organizeIngestAdaptive 开关和负载算法，cap 64）算一次，一轮内固定不抖；
 *   - 单条失败只影响它自己：split() 自己 attempts+1，到 MAX_ATTEMPTS=5 转 dead，账不丢；
 *     本轮失败过的 id 不再在同一轮里被重新领到（next_try_at=now 会让它们立刻又到期）。
 */
import type { DatabaseSync } from 'node:sqlite';
import { dueOps, markOp, MAX_ATTEMPTS, type DueOp } from './ingest-index.js';
import { split } from './agents/split.js';
import { lintReviewMeta } from './lint-review.js';
import type { MemoryRecord, Row } from './store/types.js';
import type { LlmServiceLike } from './llm-service.js';

/** 与 sleep.js 的 ingest 硬顶同口径；调用方给多大这里都再夹一次。 */
const SPLIT_HARD_CAP = 64;

/** split() 的返回面（agents/split() 已类型化，这里收窄成消费端要用的字段）。 */
export interface SplitAnswer {
  unavoidable?: boolean;
  rounds?: number;
  parts: Array<{ text: string; [key: string]: unknown }>;
}

/** 一条拆分活：op + 解析出的原条与正文。 */
export interface SplitTask {
  op: DueOp;
  sourceId: string;
  text: string;
  source: MemoryRecord | null;
  payload: Row;
}

/** prepare 的产物：拆分答案、错误、耗时。 */
export interface SplitPrepared {
  answer?: SplitAnswer | null;
  error?: unknown;
  ms?: number;
}

/** 批内结果盒：prepare 成功带 value，失败带 error；commit 只认这几个字段。 */
export type SplitOutcome =
  | { ok: true; value: SplitPrepared }
  | { ok: false; error?: unknown };

/** split 用到的 store 面：快照、type 判定、写库、归档。 */
export interface SplitStore {
  metas?: () => MemoryRecord[];
  typeOf?: (m: MemoryRecord) => string;
  save: (text: string, tags: string[], source: string, extra: Record<string, unknown>) => Promise<{ id?: string | number; discarded?: boolean }>;
  archive: (id: string, reason: string) => Promise<unknown>;
}

/** consumeSplitOps 的入参。 */
export interface SplitConsumeOpts {
  db?: DatabaseSync;
  store?: SplitStore;
  llm?: LlmServiceLike | null;
  provider?: string;
  model?: string;
  limit?: number;
  maxChars?: number;
  maxTokens?: number;
  dryRun?: boolean;
  log?: (line: string) => void;
  now?: number;
  textLint?: boolean;
  lintReview?: boolean;
  lintReviewTimeoutMs?: number;
  concurrency?: number;
  writeLock?: ((fn: () => Promise<void>) => Promise<void>) | null;
}

/** 一轮拆分的统计。 */
export interface SplitStats {
  scanned: number;
  tried: number;
  unavailable: number;
  unavoidable: number;
  archived: number;
  inserted: number;
  deduped: number;
  parts: number;
  rejected: number;
  failed: number;
  skipped: number;
  charsBefore: number;
  charsAfter: number;
  concurrency: number;
}

/** 从 store 里取原条：优先 payload.text，库里没有就用 payload 文本（merge 产出的 add 候选没有 id）。 */
function sourceOf(store: SplitStore, sourceId: string): MemoryRecord | null {
  const metas = typeof store.metas === 'function' ? store.metas() : [];
  return metas.find((m) => String(m.id) === sourceId) || null;
}

/**
 * 重试幂等（2026-11-15 存储高-3）：子段一旦落库，重试时靠来源标记 + 正文认出它，直接复用 id。
 * 拆分写库的来源固定是 `sleep:split:<sourceId>`；正文 store.save 会 trim，比较前也 trim。
 * 认不出来就返回空串，走正常入库（宁可多问一次写入闸，也不吞段）。
 */
function savedPartId(store: SplitStore, sourceTag: string, text: unknown): string {
  const want = String(text || '').trim();
  if (!sourceTag || !want) return '';
  let metas = [];
  try {
    metas = typeof store.metas === 'function' ? store.metas() : [];
  } catch {
    return '';
  }
  const hit = metas.find((m) => m
    && m.source === sourceTag
    && String(m.text || '').trim() === want
    && (m.status || 'active') === 'active');
  return hit ? String(hit.id) : '';
}

/**
 * 批内并发池：跟 sleep.js 的 runIngestPool 同构——worker 从 cursor 领一个任务，prepare 并发，
 * 主循环按原始顺序 await 槽位、串行 commit；一个任务 prepare 完没落地就不领下一个，
 * 所以在飞的结果最多 concurrency 个。
 *
 * 为什么不直接 import sleep.js 的 runIngestPool：sleep.js 已经 import 本文件，
 * 反向 import 会绕成环；本池一次只处理一批，批与批之间由外层的 dueOps 重新领活，
 * 跟 runIngestPool 一次吃整个 list 的用法也不一样。
 *
 * prepare 抛错只记在它自己身上（box.error），别的 worker 继续；commit 抛错同样只影响自己。
 *
 * @returns {Promise<{workers:number}>} workers = 本批实际开的 worker 数
 */
async function runSplitBatch(tasks: SplitTask[], concurrency: number, prepare: (task: SplitTask, i: number) => Promise<SplitPrepared>, commit: (task: SplitTask, got: SplitPrepared, i: number) => Promise<void>, writeLock: ((fn: () => Promise<void>) => Promise<void>) | null = null, log: ((line: string) => void) | null = null): Promise<{ workers: number }> {
  const list: SplitTask[] = Array.isArray(tasks) ? tasks : [];
  if (!list.length) return { workers: 0 };
  const conc = Math.min(Math.max(Math.floor(Number(concurrency) || 1), 1), SPLIT_HARD_CAP);
  const workers = Math.min(conc, list.length);
  const ready = list.map(() => {
    let resolve!: (value: SplitOutcome) => void;
    const promise = new Promise<SplitOutcome>((r) => { resolve = r; });
    return { promise, resolve };
  });
  const landed = list.map(() => {
    let resolve!: (value?: void | PromiseLike<void>) => void;
    const promise = new Promise<void>((r) => { resolve = r; });
    return { promise, resolve };
  });
  let cursor = 0;
  const worker = async () => {
    for (;;) {
      const i = cursor;
      cursor += 1;
      if (i >= list.length) return;
      let box: SplitOutcome;
      try {
        box = { ok: true, value: await prepare(list[i], i) };
      } catch (error) {
        box = { ok: false, error };
      }
      ready[i].resolve(box);
      await landed[i].promise; // 等这条落地完再领下一条，未落地的结果最多 workers 个
    }
  };
  const running = Promise.all(Array.from({ length: workers }, () => worker()));
  // 落地串行 + 写闸：批内所有写库（store.save / archive / markOp）都从这条线过，
  // 池与池之间共用同一把 writeLock 时，SQLite 同一时刻只有一个写者。
  for (let i = 0; i < list.length; i += 1) {
    try {
      const box = await ready[i].promise;
      const landedOne = () => commit(list[i], box.ok ? box.value : { error: box.error }, i);
      await (writeLock ? writeLock(landedOne) : landedOne());
    } catch (error) {
      // commit / writeLock 抛错（含 markOp 抛错）绝不能把 landed 卡住：worker 在等它，
      // 漏一次 resolve 整批就永远收不了尾。只记一行，这条 op 留给租约回收（reap 计 attempts）。
      log?.(`[split] commit 抛错 id=${list[i]?.op?.id ?? ''}：${String((error as Error && (error as Error).message) || error as string).slice(0, 120)}`);
    } finally {
      landed[i].resolve();
    }
  }
  await running;
  return { workers };
}

/**
 * 拆分失败必须落成可重试状态：
 * split() 自己会 recordSplitFailure（enqueue + markOp pending，attempts+1）；
 * 但如果它没来得及落（比如外层直接抛错），行还停在 running——这里补一刀。
 * 已经回 pending 的绝不再 markOp：那会把 attempts 加两次。
 * attempts 到 MAX_ATTEMPTS 由 markOp → ledger 判 dead，这里不另设阈值。
 */
function retryIfRunning(db: DatabaseSync, opId: string | number, reason: string, { now, dryRun, log }: { now: number; dryRun: boolean; log?: ((line: string) => void) | null }): Row | null {
  let row = null;
  try {
    row = db.prepare('SELECT status, attempts FROM pending_ops WHERE id = ?').get(opId) as Row;
  } catch {
    return null;
  }
  if (!row || row.status !== 'running') return row;
  log?.(`[split] op=${opId} 失败没有落账（仍在 running），补 pending attempts=${(Number(row.attempts) || 0) + 1}`);
  return markOp(db, opId, 'pending', reason, { now, dryRun, log });
}

/**
 * 消费一批 split_item。领活按小批（= 并发数）走，一批收口完再领下一批；
 * 处理完按结果标 done / pending（split() 或写入闸自己会加 attempts）。
 *
 * @param {object} opts
 * @param {import('node:sqlite').DatabaseSync} opts.db
 * @param {object} opts.store createStore 的返回
 * @param {object} opts.llm ctx.llm
 * @param {string} [opts.provider]
 * @param {string} [opts.model]
 * @param {number} [opts.limit=Infinity] 本轮最多处理多少条；默认全领（分批领，不再一次全标 running）
 * @param {number} [opts.concurrency=1] 并发路数；sleep.js 传 selectIngestConcurrency 算出的值（≤64）
 * @param {number} [opts.maxChars=200]
 * @param {number} [opts.maxTokens=0] 拆分模型输出上限；0 = 不传，由 provider 封顶
 * @param {boolean} [opts.dryRun=false]
 * @param {(line:string)=>void} [opts.log]
 * @param {number} [opts.now] 测试注入；不传就每条活按当时的 Date.now()
 * @returns {Promise<{scanned:number, tried:number, unavailable:number, archived:number, unavoidable:number,
 *   inserted:number, deduped:number, parts:number, rejected:number, failed:number, skipped:number,
 *   concurrency:number}>}
 */
export async function consumeSplitOps(opts: SplitConsumeOpts = {}): Promise<SplitStats> {
  const {
    db, store, llm, provider, model, limit = Infinity, maxChars = 200,
    maxTokens = 0, dryRun = false, log, now: nowOpt,
    // 正文 lint 开关（与 store 同一个口径）；调用方传 cfg.pipeline.textLint !== false
    textLint = true,
    // lint 两级复核；调用方按 cfg.pipeline 的口径传
    lintReview = true,
    lintReviewTimeoutMs = 0,
    concurrency = 1,
    writeLock = null,
  } = opts;
  const stats: SplitStats = {
    scanned: 0, tried: 0, unavailable: 0, unavoidable: 0, archived: 0,
    inserted: 0, deduped: 0, parts: 0, rejected: 0, failed: 0, skipped: 0,
    charsBefore: 0, charsAfter: 0, concurrency: 0,
  };
  if (!db || !store) return stats;

  // 一轮内固定：进来算一次，批与批之间不抖。
  const conc = Math.min(Math.max(Math.floor(Number(concurrency) || 1), 1), SPLIT_HARD_CAP);
  // 预领粒度：跟并发一样大。领了没做等于把租约占死，一批做完再领下一批。
  const batchSize = conc;
  stats.concurrency = conc;
  // 测试注入固定 now；否则每条活按当时的钟，退避时间算得准。
  const nowOf = () => (Number.isFinite(Number(nowOpt)) ? Number(nowOpt) : Date.now());
  let remaining = limit === Infinity ? Infinity : Math.max(0, Math.floor(Number(limit) || 0));
  // 本轮失败/回 pending 的 id：markOp 把 next_try_at 设成 now，下一批 dueOps 会立刻又领到，
  // 同一轮里不许原地重试同一条（下一轮 sleepPass 再领）。
  const failedThisRound: Array<string | number> = [];
  let peakConc = 0;
  let failed = 0;

  while (remaining > 0) {
    const want = remaining === Infinity ? batchSize : Math.min(batchSize, remaining);
    let ops: DueOp[] = [];
    try {
      ops = dueOps(db, want, { kind: 'split_item', now: nowOf(), exclude: failedThisRound });
    } catch (error) {
      log?.(`[split] WARN 领取 split_item 失败：${String((error as Error && (error as Error).message) || error as string)}`);
      break;
    }
    if (!ops.length) break;
    if (remaining !== Infinity) remaining -= ops.length;

    // ① 前置检查（读库 + 终态标记，都是同步快活，串行做）：决定这条要不要问模型
    const tasks: SplitTask[] = [];
    for (const op of ops) {
      const payload = op.payload || {} as Row;
      const sourceId = String(payload.source_id || op.dedupeKey || '');
      stats.scanned += 1;

      if (!sourceId) {
        markOp(db, op.id, 'done', 'no_source_id', { now: nowOf(), dryRun, log });
        stats.skipped += 1;
        continue;
      }

      const source = sourceOf(store, sourceId);
      // 合并 agent 的 add 候选没有对应原条：payload 里自带正文，拆完直接入库，不用归档谁。
      const text = String(payload.text || source?.text || '');
      if (!source && !text.trim()) {
        // 没 id 也没正文：重试一百次也不会变，终态 done，不静默
        markOp(db, op.id, 'done', 'source_missing', { now: nowOf(), dryRun, log });
        stats.skipped += 1;
        log?.(`[split] item=${sourceId} 原条不在库里且没有正文，op 已标 done`);
        continue;
      }
      if (source && (source.status || 'active') !== 'active') {
        markOp(db, op.id, 'done', 'source_not_active', { now: nowOf(), dryRun, log });
        stats.skipped += 1;
        log?.(`[split] item=${sourceId} 原条 status=${source.status}，op 已标 done`);
        continue;
      }
      if (!text.trim()) {
        markOp(db, op.id, 'done', 'empty_text', { now: nowOf(), dryRun, log });
        stats.skipped += 1;
        log?.(`[split] item=${sourceId} 没有正文可拆，op 已标 done`);
        continue;
      }

      if (dryRun) {
        markOp(db, op.id, 'done', 'dry_run', { now: nowOf(), dryRun, log });
        stats.skipped += 1;
        log?.(`[split] dry-run item=${sourceId} ${text.length}字 待拆`);
        continue;
      }

      // inflate 类失败（拆完总字数超原文 20%）到判死阈值就不再叫模型，直接 dead 等人工捞。
      if (Number(op.attempts) >= MAX_ATTEMPTS) {
        let row = null;
        try {
          row = db.prepare('SELECT last_error FROM pending_ops WHERE id = ?').get(op.id);
        } catch {
          row = null;
        }
        if (/^inflate/.test(String(row?.last_error || ''))) {
          markOp(db, op.id, 'dead', 'inflate_retry_exhausted', { now: nowOf(), dryRun, log });
          stats.failed += 1;
          failed += 1;
          log?.(`[split] item=${sourceId} inflate 已到 attempts=${MAX_ATTEMPTS}，op 标 dead 不再自动重试`);
          continue;
        }
      }

      tasks.push({ op, sourceId, text, source, payload });
    }

    if (!tasks.length) continue;

    // ② 批内并发问模型
    const { workers } = await runSplitBatch(
      tasks,
      conc,
      async (task) => {
        stats.tried += 1;
        const startedAt = Date.now();
        let answer = null;
        let error = null;
        try {
          answer = await split({
            sourceId: task.sourceId,
            text: task.text,
            type: task.payload.type as string
              || (task.source && typeof store.typeOf === 'function' ? store.typeOf(task.source) : '')
              || 'world',
            ts: task.payload.ts as string || task.source?.ts || '',
            llm,
            provider,
            model,
            db,
            maxChars,
            maxTokens,
            attempt: Number(task.op.attempts) || 0,
            textLint,
            lintReview,
            lintReviewTimeoutMs,
            log,
          });
        } catch (err) {
          error = err;
        }
        return { answer, error, ms: Math.max(0, Date.now() - startedAt) };
      },
      // ③ 串行收口：写库、标 done/dead、落 pending_ops
      async (task, got) => {
        const { op, sourceId, source, payload } = task;
        const ms = Math.max(0, Number(got?.ms) || 0);

        if (got?.error) {
          stats.failed += 1;
          failed += 1;
          failedThisRound.push(op.id);
          markOp(db, op.id, 'pending', `split_error:${String((got.error as Error && (got.error as Error).message) || got.error as string).slice(0, 100)}`, { now: nowOf(), dryRun, log });
          log?.(`[split] item=${sourceId} 拆分抛错：${String((got.error as Error && (got.error as Error).message) || got.error as string)} ms=${ms}`);
          return;
        }

        const answer = got?.answer;
        if (!answer) {
          // split() 正常会自己落 pending（attempts+1）；万一没落，这里补成可重试，绝不写 done。
          retryIfRunning(db, op.id, 'split_fail', { now: nowOf(), dryRun, log });
          stats.failed += 1;
          failed += 1;
          failedThisRound.push(op.id);
          log?.(`[split] item=${sourceId} FAIL 拆不出合格分段 ms=${ms}`);
          return;
        }
        if (answer.unavoidable) {
          // 同上：recordSplitFailure 已落 pending；行还在 running 时才补。
          retryIfRunning(db, op.id, 'unavoidable', { now: nowOf(), dryRun, log });
          stats.unavoidable += 1;
          failedThisRound.push(op.id);
          log?.(`[split] item=${sourceId} unavoidable 已落 pending ms=${ms}`);
          return;
        }

        stats.charsBefore += task.text.length;
        stats.charsAfter += answer.parts.reduce((n, part) => n + String(part.text || '').length, 0);
        const tags = Array.isArray(source?.tags) ? source.tags.map(String) : (Array.isArray(payload.tags) ? payload.tags.map(String) : []);
        const entities = Array.isArray(source?.entities) ? source.entities.map(String) : (Array.isArray(payload.entities) ? payload.entities.map(String) : []);
        const type = (source && typeof store.typeOf === 'function' ? store.typeOf(source) : '') || payload.type || 'world';

        let inserted = 0;
        let deduped = 0;
        let rejected = 0;
        let replayed = 0;
        let gatePending = false;
        const newIds: string[] = [];
        const partSource = `sleep:split:${sourceId}`;
        // 每一条子段单独走 store.save：一次一次过 assertWriteAllowed，绝不批量放行
        for (const part of answer.parts) {
          // 上一轮已经落过库的段（原条还在 pending 时重试）：复用 id，别再入一次
          const priorId = savedPartId(store, partSource, part.text);
          if (priorId) {
            replayed += 1;
            inserted += 1;
            newIds.push(priorId);
            continue;
          }
          try {
            const rec = await store.save(part.text, tags, `sleep:split:${sourceId}`, {
              // 复核判 keep 的段落原样放行：带 __lintReview 过 store 最后一道闸（别写成 __lintChecked，那是内部二次写入的旁路）
              ...lintReviewMeta(part),
              type,
              entities,
              validFrom: source?.validFrom,
              sourceFile: `sleep:split:${sourceId}`,
              // 现有表能力：memories.supersedes 是现成 JSON 列，子段用它挂回原条；
              // 原条由下面的 store.archive 统一归档（不新增列，不迁移）。
              // add 候选没有原条，就没有 supersedes 可挂。
              supersedes: source ? [sourceId] : undefined,
            });
            if (rec?.discarded) {
              deduped += 1;
              continue;
            }
            inserted += 1;
            newIds.push(String(rec.id));
          } catch (error) {
            if (error && (error as { code?: string }).code === 'WRITE_GATE_PENDING') {
              // 闸未决：候选已落 dedupe_check pending，整条拆分会下一轮重来，别再写、别归档
              gatePending = true;
              rejected += 1;
              continue;
            }
            rejected += 1;
            log?.(`[split] item=${sourceId} 子段入库失败：${String((error as Error && (error as Error).message) || error as string).slice(0, 120)}`);
          }
        }
        stats.parts += answer.parts.length;
        stats.inserted += inserted;
        stats.deduped += deduped;
        stats.rejected += rejected;

        if (gatePending || rejected > 0) {
          // 存储高-3：有段没落地就不许归档原条（否则那段随原条一起从召回里消失）。
          // 已入库的段留在库里、不回收滚；op 回 pending，下一轮靠 savedPartId 幂等跳过它们。
          const why = gatePending ? 'write_gate_pending' : `partial_write:${inserted}/${answer.parts.length}`;
          failedThisRound.push(op.id);
          stats.failed += 1;
          failed += 1;
          markOp(db, op.id, 'pending', why, { now: nowOf(), dryRun, log });
          log?.(
            `[split] item=${sourceId} WARN 子段只落地 ${inserted}/${answer.parts.length}（${why}），` +
              `原条保持 active，op 回 pending；已入库 ${replayed} 段重试时跳过`,
          );
          return;
        }

        if (inserted > 0) {
          if (source) {
            try {
              await store.archive(sourceId, `拆成 ${inserted} 条：${newIds.join(' ')}`.slice(0, 200));
              stats.archived += 1;
            } catch (error) {
              // 归档没成就不能标 done：原条还 active，下一轮重试（子段全在，savedPartId 会全跳过）
              const why = `archive_failed:${String((error as Error && (error as Error).message) || error as string).slice(0, 100)}`;
              failedThisRound.push(op.id);
              stats.failed += 1;
              failed += 1;
              markOp(db, op.id, 'pending', why, { now: nowOf(), dryRun, log });
              log?.(`[split] item=${sourceId} 原条归档失败，op 回 pending：${String((error as Error && (error as Error).message) || error as string)}`);
              return;
            }
          }
          markOp(db, op.id, 'done', null, { now: nowOf(), dryRun, log });
        } else if (deduped > 0) {
          // 每一段都判重：原条没有新增信息，但也不能贸然归档，留 active，op 终态
          markOp(db, op.id, 'done', 'all_parts_deduped', { now: nowOf(), dryRun, log });
          log?.(`[split] WARN item=${sourceId} ${deduped} 段全被判重，${source ? '原条保持 active' : '无原条可归档'}`);
        } else {
          markOp(db, op.id, 'pending', 'no_part_inserted', { now: nowOf(), dryRun, log });
          stats.failed += 1;
          failed += 1;
          failedThisRound.push(op.id);
        }

        // 逐条成功行带 ms（跟 ingest 的 `file= ok= ms=` 一个口径），落地那一刻就报
        log?.(
          `[split] item=${sourceId} parts=${answer.parts.length} retry=${Math.max(0, Number(answer.rounds || 1) - 1)} ` +
            `rejected=${rejected} inserted=${inserted} deduped=${deduped} replayed=${replayed} ms=${ms}`,
        );
      },
      writeLock,
      log,
    );
    peakConc = Math.max(peakConc, workers);
  }

  // 汇总一行：grep `[split] 并发` 就能看出这轮开几路、条/成功/失败各几个（跟 ingest 汇总同口径）
  if (stats.scanned > 0) {
    log?.(
      `[split] 并发 ${peakConc || Math.min(conc, stats.scanned)} 路 条 ${stats.scanned} ` +
        `成功 ${Math.max(0, stats.scanned - failed)} 失败 ${failed}`,
    );
  }
  return stats;
}

export default consumeSplitOps;
