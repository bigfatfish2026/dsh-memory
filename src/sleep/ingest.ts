/**
 * 入库两段：老路径（整段 transcript 一次问）/ 文件 ack 路径（prepare 并发、commit 串行）。
 */
import path from 'node:path';
import {
  collectTranscript, readSessionEvents, eventsToTranscript, lastEventTsOf,
} from '../transcript.js';
import {
  scanSessions, planFiles, bootstrapLegacy, ackFile, failFile, dueOps, markOp, hasIngestTables,
} from '../ingest-index.js';
import { selectIngestConcurrency, formatIngestAdaptiveLog } from '../concurrency.js';
import { isoLocal } from './slots.js';
import { runIngestPool } from './pool.js';
import type { DueOp } from '../ingest-index.js';
import type { MemoryView } from '../store/types.js';
import type { SleepCfg, SleepStore } from './env.js';
import type { LintActions, LintAsked } from './lint.js';
import type { TranscriptConfig } from '../transcript.js';

/** 入口冲突检查的汇总。 */
export interface IngestConflictInfo {
  checked: number;
  redundant: unknown[];
  stateChange: unknown[];
  conflict: unknown[];
  failed: number;
}

/** 本轮总账：conflictCheck 单独收。 */
interface IngestTotal {
  added: unknown[];
  merged: unknown[];
  updated: unknown[];
  archived: unknown[];
  rejected: unknown[];
  conflictCheck: unknown[];
  [key: string]: unknown;
}

/** 一段落库动作的结果。 */
export interface IngestActionResult {
  added: unknown[];
  merged: unknown[];
  updated: unknown[];
  archived: unknown[];
  rejected: unknown[];
  conflictCheck?: IngestConflictInfo | null;
  ingest?: unknown;
  [key: string]: unknown;
}

/** 单文件 prepare 的产物。 */
export interface PreparedFile {
  path: string;
  events: number;
  cand: number;
  chars: number;
  ackTs: number;
  actions: LintActions | null;
  existing: MemoryView[];
  conflictInfo: IngestConflictInfo | null;
}

/** 重放池 prepare 的产物：原 op + 单文件面。 */
export interface PreparedReplay extends PreparedFile {
  op: DueOp;
  filePath: string;
  dead?: boolean;
}

/** 两条入口统一的返回面。 */
export interface IngestPassResult {
  skipped?: unknown;
  messages?: number;
  files?: number;
  gathered: { transcript: string; messages: number; files: number };
  existing: MemoryView[];
  existingChars: number;
  backup: string;
  result: IngestActionResult;
  parseFailed: boolean;
  fallback?: boolean;
  stats?: Record<string, unknown>;
  [key: string]: unknown;
}

/** 文件 ack 路径的统计。 */

/** 本文件用到的 env 面。 */
interface IngestEnv {
  store: SleepStore;
  cfg: SleepCfg;
  log: (line: string) => void;
  memoryTexts: (transcript: string) => MemoryView[];
  lint: { askActionsWithLint: (opts: { transcript: string; existing: MemoryView[]; effort: unknown; tag?: string }) => Promise<LintAsked> };
  model: { runConflictCheck: (adds: LintActions['add'], opts?: { dryRun?: boolean }) => Promise<{ adds: LintActions['add']; checked: number; redundant: unknown[]; stateChange: unknown[]; conflict: unknown[]; failed: number }> };
  actions: { applyActions: (actions: LintActions, opts: { existing: MemoryView[]; dryRun: boolean }) => Promise<IngestActionResult> };
  snapshot: (cfg: SleepCfg, store: SleepStore) => Promise<string>;
  heartbeat: () => number;
}

export function createIngest(env: IngestEnv) {
  const { store, cfg } = env;
  const log = (_c: SleepCfg, line: string) => env.log(line);
  const memoryTexts = (...a: Parameters<IngestEnv['memoryTexts']>) => env.memoryTexts(...a);
  const askActionsWithLint = (...a: Parameters<IngestEnv['lint']['askActionsWithLint']>) => env.lint.askActionsWithLint(...a);
  const runConflictCheck = (...a: Parameters<IngestEnv['model']['runConflictCheck']>) => env.model.runConflictCheck(...a);
  const applyActions = (...a: Parameters<IngestEnv['actions']['applyActions']>) => env.actions.applyActions(...a);
  const snapshot = (...a: Parameters<IngestEnv['snapshot']>) => env.snapshot(...a);
  const heartbeat = (...a: Parameters<IngestEnv['heartbeat']>) => env.heartbeat(...a);

/**
 * 老路径（开关关着时）：按 lastSleepAt 读 since 之后的全部会话文件，整段一次问模型。
 * 2026-11-05 第二步只是把它从 sleepPass 里搬进函数，行为一个字没改。
 */
async function ingestByTranscript({ dryRun, since }: { dryRun: boolean; since: number }): Promise<IngestPassResult> {
  const gathered = collectTranscript(cfg as unknown as TranscriptConfig, since);
  if (gathered.messages < 4) {
    return { skipped: 'too-quiet', messages: gathered.messages, files: gathered.files } as unknown as IngestPassResult;
  }
  const existing = memoryTexts(gathered.transcript);
  const existingChars = existing.reduce((s, m) => s + String(m.text || '').length, 0);
  const asked = await askActionsWithLint({
    transcript: gathered.transcript, existing, effort: cfg.reasoningEffort, tag: '会话入库',
  });
  const raw = asked.raw;
  let actions = asked.actions;
  let parseFailed = false;
  if (!actions) {
    // 2026-10-29：阶段一失败不许拖累阶段二——入库可以下次再说，去重整理照跑
    log(
      cfg,
      `阶段一没给出可解析的 JSON，跳过入库、继续分簇整理：${String(raw).replace(/\s+/g, ' ').slice(0, 300)}`,
    );
    actions = { add: [], merge: [], update: [], archive: [] };
    parseFailed = true;
  }

  const backup = dryRun ? '' : await snapshot(cfg, store);

  // 入口检查：新记忆跟同实体旧条的关系（冗余 / 状态变更 / 矛盾），先判再入库
  let conflictInfo = null;
  if (cfg.conflictCheckEnabled !== false && actions.add.length) {
    const checked = await runConflictCheck(actions.add);
    actions.add = checked.adds;
    conflictInfo = {
      checked: checked.checked,
      redundant: checked.redundant,
      stateChange: checked.stateChange,
      conflict: checked.conflict,
      failed: checked.failed,
    };
  }

  const result = await applyActions(actions, { existing, dryRun });
  if (conflictInfo) result.conflictCheck = conflictInfo;
  return { gathered, existing, existingChars, backup, result, parseFailed };
}

function emptyActionResult(): IngestActionResult {
  return { added: [], merged: [], updated: [], archived: [], rejected: [] };
}

function changedCount(result: IngestActionResult | null | undefined): number {
  if (!result) return 0;
  return result.added.length + result.merged.length + result.updated.length + result.archived.length;
}

/**
 * 单文件流程拆两段：
 *   prepareOneFile —— 读文件 → 取 last_acked_ts 之后的事件 → 抽候选 → 问模型 → 解析（可并发）；
 *   commitOneFile  —— applyActions + 整文件 ack（写库，只在主循环里串行调）。
 * 拆开的原因：一个文件约 2.4 分钟全花在问模型上，落地只是几行写；串行的只能是写。
 * 解析失败等可重试错误在 prepare 里直接抛，由调用方 failFile，不推进 ack（和拆开前一致）。
 */
async function prepareOneFile(filePath: string): Promise<PreparedFile> {
  const db = store.db();
  const events = readSessionEvents(filePath);
  if (!events) throw new Error('read_fail');
  const row = db.prepare('SELECT last_acked_ts FROM ingest_files WHERE path = ?').get(filePath);
  const acked = Number(row?.last_acked_ts) || 0;
  const lastTs = lastEventTsOf(events);
  if (lastTs <= acked) {
    // 2026-11-13：没新事件也要 ackFile，把当前 mtime/size 刷进 ingest_files。
    // 以前这个分支直接 return，planFiles 下一轮仍按 size/mtime 判成 changed，
    // 同内容文件每轮重扫、把单轮文件预算占满（连续五天「会话20个/消息0条/字符0」；
    // 那个预算 2026-11-14 已删，但 ack 指纹仍要刷新，否则每轮都判 changed）。
    // last_acked_ts 用 acked 不倒退；只刷新文件指纹，不动内容语义。
    // ack 是写库，挪到 commitOneFile。
    return {
      path: filePath, events: events.length, cand: 0, chars: 0, ackTs: acked,
      actions: null, existing: [], conflictInfo: null,
    };
  }
  const { lines, messages } = eventsToTranscript(cfg as unknown as TranscriptConfig, events, acked);
  const transcript = lines.join('\n');
  if (!messages) {
    return {
      path: filePath, events: events.length, cand: 0, chars: transcript.length, ackTs: lastTs,
      actions: null, existing: [], conflictInfo: null,
    };
  }
  const existing = memoryTexts(transcript);
  const asked = await askActionsWithLint({
    transcript, existing, effort: cfg.reasoningEffort, tag: `单文件入库 ${path.basename(filePath)}`,
  });
  const actions = asked.actions;
  if (!actions) throw new Error('parse_fail');
  let conflictInfo = null;
  if (cfg.conflictCheckEnabled !== false && actions.add.length) {
    // 冲突检查也是问模型 + 读库，不写库，留在并发段
    const checked = await runConflictCheck(actions.add);
    actions.add = checked.adds;
    conflictInfo = {
      checked: checked.checked,
      redundant: checked.redundant,
      stateChange: checked.stateChange,
      conflict: checked.conflict,
      failed: checked.failed,
    };
  }
  return {
    path: filePath, events: events.length, cand: messages, chars: transcript.length, ackTs: lastTs,
    actions, existing, conflictInfo,
  };
}

/** 落地（串行）：applyActions → ackFile；抛错就不推进 ack，由调用方 failFile。 */
async function commitOneFile(prepared: PreparedFile, { dryRun }: { dryRun: boolean }): Promise<PreparedFile & { result: IngestActionResult }> {
  const db = store.db();
  if (!prepared.actions) {
    ackFile(db, prepared.path, prepared.ackTs, { dryRun, log: (line) => log(cfg, line) });
    return { ...prepared, result: emptyActionResult() };
  }
  const result = await applyActions(prepared.actions, { existing: prepared.existing, dryRun });
  if (prepared.conflictInfo) result.conflictCheck = prepared.conflictInfo;
  ackFile(db, prepared.path, prepared.ackTs, { dryRun, log: (line) => log(cfg, line) });
  return { ...prepared, result };
}

/** 把单文件结果并进本轮总账 */
function mergeInto(total: IngestTotal, one: PreparedFile & { result?: IngestActionResult } | null | undefined): void {
  if (!one?.result) return;
  total.added.push(...(one.result.added || []));
  total.merged.push(...(one.result.merged || []));
  total.updated.push(...(one.result.updated || []));
  total.archived.push(...(one.result.archived || []));
  total.rejected.push(...(one.result.rejected || []));
  if (one.result.conflictCheck) total.conflictCheck.push(one.result.conflictCheck);
}

function shortErr(err: unknown): string {
  return String(((err as Error | null) && ((err as Error).message || err)) || 'unknown').replace(/\s+/g, ' ').slice(0, 120);
}

/**
 * 新路径（开关 pipeline.ingestAck=true）：按文件 ack 入库，账记在 ingest_files，
 * 失败落 pending_ops 重放。§3 的 3.2/3.3/3.4 全在这一段。
 */
async function ingestByFilePass({ dryRun, lastSleepAt }: { dryRun: boolean; lastSleepAt: number }): Promise<IngestPassResult> {
  const db = store.db();
  if (!hasIngestTables(db)) {
    log(cfg, '[ingest] 库里缺 ingest_files/pending_ops，开关打开也回退老路径（先跑 scripts/apply-pipeline-schema.mjs）');
    return { fallback: true } as unknown as IngestPassResult;
  }
  // 2026-11-14：单轮文件预算（ingestBudget）和重放预算（replayBudget）删了。
  // 本轮能处理的全部处理：planFiles 不再截断，pending_ops 一次全领。
  const stats = {
    scanned: 0,
    legacy: 0,
    pending: 0,
    overBudget: 0,
    processed: 0,
    done: 0,
    failed: 0,
    dead: 0,
    queued: 0,
    replayed: 0,
    replayFailed: 0,
    events: 0,
    cand: 0,
    chars: 0,
  };
  const total: IngestTotal = { ...emptyActionResult(), conflictCheck: [] };

  // ① 先领本轮到期的重放任务（只领已接线的 ingest_file；其余 kind 等后续票）
  const ops = dueOps(db, Infinity, { kind: 'ingest_file' });

  // ② 扫文件 + 首次 bootstrap + 算待处理清单
  const files = scanSessions(cfg.sessionsRoot);
  stats.scanned = files.length;
  const rows = Number(db.prepare('SELECT count(*) AS n FROM ingest_files').get()?.n ?? 0);
  if (!rows && files.length) {
    const boot = bootstrapLegacy(db, files, lastSleepAt, { dryRun, log: (line) => log(cfg, line) });
    stats.legacy = boot.legacy;
    log(cfg, `[ingest] bootstrap 扫描${boot.scanned} legacy_acked=${boot.legacy} pending=${boot.pending} 读失败=${boot.errors}`);
  }
  const plan = planFiles(db, files, { lastSleepAt });
  if (plan.legacy.length) {
    // 表里已有行、但新冒出来的旧文件：同样按「不重挖历史」口径标掉，别每轮重复判定
    const boot = bootstrapLegacy(db, plan.legacy, lastSleepAt, { dryRun, log: (line) => log(cfg, line) });
    stats.legacy += boot.legacy;
  }
  stats.pending = plan.todo.length;
  stats.overBudget = plan.overBudget.length;
  log(cfg, `[sleep-v2] round=${isoLocal()} todo=${plan.todo.length} replay=${ops.length} bootstrap=${stats.legacy}`);

  // ③ 本轮有活干才备份：账本表不算记忆，写它之前不用备份
  let backup = '';
  if (!dryRun && (ops.length || plan.todo.length)) backup = await snapshot(cfg, store);

  // ④ 重放：和本轮新文件走同一条单文件流程，幂等靠 last_acked_ts 和 dedupe_key
  // 2026-11-14：N 路并发问模型、落地串行（runIngestPool）。
  // 一个文件抛错只记它自己，别的照跑；落地一本一本写，不并发写库。
  // 2026-11-14：一轮只算一次实际路数，重放池和新文件池共用；
  // 自适应开着且本轮真有文件要跑时打一行依据，整轮固定不抖动。
  const ingestPick = selectIngestConcurrency(cfg);
  const ingestConc = ingestPick.conc;
  if (ingestPick.adaptive && (ops.length || plan.todo.length)) {
    log(cfg, formatIngestAdaptiveLog(ingestPick));
  }
  const replayResults = await runIngestPool({
    items: ops,
    concurrency: ingestConc,
    heartbeat,
    prepare: async (op: DueOp) => {
      const filePath = String(op.payload?.path || op.dedupeKey || '');
      if (!filePath) return { op, filePath: '', dead: true };
      const prepared = await prepareOneFile(filePath);
      return { ...prepared, op, filePath };
    },
    commit: async (prepared: unknown) => {
      const op = (prepared as PreparedReplay).op;
      if ((prepared as PreparedReplay).dead) {
        markOp(db, op.id, 'dead', 'payload 缺 path', { dryRun, log: (line) => log(cfg, line) });
        return { dead: true };
      }
      const one = await commitOneFile(prepared as PreparedFile, { dryRun });
      markOp(db, op.id, 'done', null, { dryRun, log: (line) => log(cfg, line) });
      return { one };
    },
    // 重放的文件同样是「本轮落地的文件」，落地那一刻也立刻报一行（replay=1 区分），
    // 汇总行的「文件 N」含重放，这样逐文件行数不会比汇总少。
    onLanded: ({ item, result, ms }) => {
      const base = path.basename(String(item?.payload?.path || item?.dedupeKey || '')) || '(unknown)';
      if (result.ok && !(result.value as { dead?: boolean } | null)?.dead) {
        log(cfg, `[ingest] file=${base} ok=${Number((result.value as { one?: { events?: unknown } } | null)?.one?.events) || 0} ms=${ms} replay=1`);
        return;
      }
      const why = result.ok ? 'payload 缺 path' : `${result.phase || 'error'}: ${shortErr(result.error)}`;
      log(cfg, `[ingest] file=${base} FAIL ${why} replay=1`);
    },
  });
  for (let i = 0; i < replayResults.length; i += 1) {
    const op = ops[i];
    const filePath = String(op.payload?.path || op.dedupeKey || '');
    const got = replayResults[i];
    if (got.ok) {
      if ((got.value as { dead?: boolean } | null)?.dead) {
        stats.replayFailed += 1;
        continue;
      }
      const one = (got.value as { one: PreparedFile & { result: IngestActionResult } }).one;
      mergeInto(total, one);
      stats.replayed += 1;
      stats.events += Number(one.events) || 0;
      log(cfg, `[retry] id=${op.id} kind=ingest_file attempts=${op.attempts} next_try_at=- last_error=- status=done file=${filePath}`);
      continue;
    }
    stats.replayFailed += 1;
    // dryRun 一个字节都不往账本里写，重放失败只记日志
    const error = got.error;
    const after = markOp(db, op.id, 'pending', (error as Error | null)?.message || error, { dryRun, log: (line) => log(cfg, line) });
    log(
      cfg,
      `[retry] id=${op.id} kind=ingest_file attempts=${after?.attempts ?? op.attempts} ` +
        `next_try_at=${dryRun ? '-' : isoLocal(new Date(Number(after?.next_try_at) || Date.now()))} ` +
        `last_error=${shortErr(error)}${dryRun ? ' dryRun=1' : ''}`,
    );
  }

  // ⑤ 本轮新文件：并发读→抽（问模型），落地按原顺序一本一本写；失败不推进 ack，进 pending_ops 等重放
  // 2026-11-14：逐文件日志改由 onLanded 在落地那一刻打，不再等池收尾补打。
  // 行首只放文件基名（session-12.jsonl），不塞绝对路径；ms 是从领走这个文件到 commit 结束的耗时。
  const todoResults = await runIngestPool({
    items: plan.todo,
    concurrency: ingestConc,
    heartbeat,
    prepare: (item) => prepareOneFile(item.path),
    commit: (prepared: unknown) => commitOneFile(prepared as PreparedFile, { dryRun }),
    onLanded: ({ item, result, ms }) => {
      const base = path.basename(String(item?.path || '')) || '(unknown)';
      if (result.ok) {
        log(cfg, `[ingest] file=${base} ok=${Number((result.value as { events?: unknown } | null)?.events) || 0} ms=${ms}`);
        return;
      }
      log(cfg, `[ingest] file=${base} FAIL ${result.phase || 'error'}: ${shortErr(result.error)}`);
    },
  });
  for (let i = 0; i < plan.todo.length; i += 1) {
    const item = plan.todo[i];
    const got = todoResults[i];
    const base = path.basename(String(item?.path || ''));
    stats.processed += 1;
    if (got.ok) {
      const one = got.value as PreparedFile & { result: IngestActionResult };
      mergeInto(total, one);
      stats.done += 1;
      stats.events += Number(one.events) || 0;
      stats.cand += one.cand;
      stats.chars += one.chars || 0;
      // 成功行已经在 onLanded 打过，这里只记账，别重复
      continue;
    }
    stats.failed += 1;
    const error = got.error;
    const f = failFile(db, item.path, error, { dryRun, log: (line) => log(cfg, line) });
    if (dryRun || f.dryRun) continue; // FAIL 行已经即时打过，dryRun 不写账
    if (f.status === 'dead') {
      stats.dead += 1;
      log(cfg, `[ingest] dead file=${base} attempts=${f.attempts} last_error=${f.lastError}`);
    } else {
      stats.queued += 1;
      log(
        cfg,
        `[ingest] pending file=${base} attempts=${f.attempts} ` +
          `next_try_at=${isoLocal(new Date(f.nextTryAt))} error=${shortErr(error)}`,
      );
    }
  }
  stats.queued += stats.replayFailed;

  // 汇总一行：以后 grep `[ingest] 并发` 就能看出这轮开几路、成失败各几个。
  const ingestFiles = ops.length + plan.todo.length;
  if (ingestFiles > 0) {
    log(
      cfg,
      `[ingest] 并发 ${Math.min(ingestConc, ingestFiles)} 路 文件 ${ingestFiles} ` +
        `成功 ${ingestFiles - stats.replayFailed - stats.failed} 失败 ${stats.replayFailed + stats.failed}`,
    );
  }

  log(
    cfg,
    `[sleep-v2] end files_scanned=${stats.scanned} legacy_acked=${stats.legacy} pending=${stats.pending} ` +
      `processed=${stats.processed} done=${stats.done} failed=${stats.failed} dead=${stats.dead} ` +
      `over_budget=${stats.overBudget} queued=${stats.queued} replay_done=${stats.replayed} replay_failed=${stats.replayFailed}`,
  );

  const result = emptyActionResult();
  result.added = total.added;
  result.merged = total.merged;
  result.updated = total.updated;
  result.archived = total.archived;
  result.rejected = total.rejected;
  if (total.conflictCheck.length) result.conflictCheck = total.conflictCheck[0] as IngestConflictInfo;
  result.ingest = { ...stats };
  return {
    gathered: { transcript: '', messages: stats.cand, files: stats.processed },
    existing: [],
    existingChars: stats.chars,
    backup,
    result,
    parseFailed: false,
    stats,
  };
}


  return {
    ingestByTranscript, ingestByFilePass, prepareOneFile, commitOneFile,
    emptyActionResult, changedCount, mergeInto, shortErr,
  };
}
