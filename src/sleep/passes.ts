/**
 * 对外两个 pass：sleepPass（按开关分 pool/legacy）与 archivePass（/new 前存档）。
 */
import { llmOf } from '../llm-service.js';
import { discardCountOf } from '../write-gate.js';
import { dayKey } from './slots.js';
import { runObservationPass } from './observation-pass.js';
import type { ObservationPassOutcome, ObservationPassStore } from './observation-pass.js';
import type { AskObservationResult } from './model-calls.js';
import type { MemoryView } from '../store/types.js';
import type { SleepStateStore } from '../sleep-state.js';
import type { SleepCfg, SleepCtx, SleepStore } from './env.js';
import type { LintAsked } from './lint.js';
import type { ActionItem } from './actions.js';
import type { SplitPassOpts } from '../split-pass.js';
import type { GateStore } from '../write-gate.js';

/** 一段落库动作的计数结果。 */
interface ActionResult {
  added: unknown[];
  merged: unknown[];
  updated: unknown[];
  archived: unknown[];
  rejected: unknown[];
  conflictCheck?: ConflictInfo | null;
  organize?: unknown;
  split?: unknown;
  splitInserted?: number;
  splitDeduped?: number;
  splitArchived?: number;
  revocation?: unknown;
  [key: string]: unknown;
}

/** 入口冲突检查的汇总。 */
interface ConflictInfo {
  checked: number;
  stateChange: unknown[];
  redundant: unknown[];
  conflict: unknown[];
}

/** 一轮 ingest 的产物（两条入口同一形状）。 */
interface IngestPassResult {
  gathered: { transcript: string; messages: number; files: number };
  existing: MemoryView[];
  existingChars: number;
  backup: string;
  result: ActionResult;
  parseFailed?: boolean;
  conflictInfo?: ConflictInfo | null;
  stats?: { processed?: number; replayed?: number; dead?: number; scanned?: number; [key: string]: unknown };
  fallback?: boolean;
  skipped?: unknown;
  [key: string]: unknown;
}

/** 分簇整理的汇总（organizePass）。 */
interface OrganizeSummary {
  total: number;
  clusters: number;
  calls: number;
  merged: unknown[];
  updated: unknown[];
  archived: unknown[];
  rejected: unknown[];
  splitInserted?: number;
  splitDeduped?: number;
  splitArchived?: number;
  [key: string]: unknown;
}

/** 作废清扫的汇总。 */
interface RevokeSummary {
  scanned: number;
  archived: unknown[];
  skipped: unknown[];
  orphan: unknown[];
}

/** 本文件用到的 env 面。 */
interface PassesEnv {
  ctx: SleepCtx;
  store: SleepStore;
  cfg: SleepCfg;
  stateStore: SleepStateStore;
  state: { lastRunAt: number; runs: number };
  log: (line: string) => void;
  push: (msg: string) => void;
  snapshot: (cfg: SleepCfg, store: SleepStore) => Promise<string>;
  memoryTexts: (transcript: string) => MemoryView[];
  scheduler: {
    curatePick: (channel: string) => { conc: number };
    beginPass: () => boolean;
    endPass: () => void;
  };
  lint: { askActionsWithLint: (opts: { transcript: string; existing: MemoryView[]; effort: unknown; tag?: string }) => Promise<LintAsked> };
  actions: {
    applyActions: (actions: { merge: ActionItem[]; update: ActionItem[]; add: ActionItem[]; archive: ActionItem[] }, opts: { existing: MemoryView[]; dryRun: boolean }) => Promise<ActionResult>;
    revocationPass: (opts?: { dryRun?: boolean }) => Promise<RevokeSummary>;
  };
  ingest: {
    ingestByTranscript: (opts: { dryRun: boolean; since: number }) => Promise<IngestPassResult>;
    ingestByFilePass: (opts: { dryRun: boolean; lastSleepAt: number }) => Promise<IngestPassResult>;
  };
  organize: { organizePass: (opts?: { dryRun?: boolean }) => Promise<OrganizeSummary> };
  poolPass: { pooledSleepPass: (opts?: { dryRun?: boolean; sinceHours?: number }) => Promise<unknown> };
  pool: { pipelineModeOf: () => string };
  /** observation 提炼的模型入口（worker 只用到 askObservation）。 */
  model?: { askObservation: (system: string, user: string) => Promise<AskObservationResult> };
  /** 本实例的 sleep owner；observation 任务租约用。 */
  sleepOwner?: string;
}

export function createPasses(env: PassesEnv) {
  const { ctx, store, cfg, stateStore } = env;
  const log = (_c: SleepCfg, line: string) => env.log(line);
  const push = (...a: Parameters<PassesEnv['push']>) => env.push(...a);
  const curatePick = (...a: Parameters<PassesEnv['scheduler']['curatePick']>) => env.scheduler.curatePick(...a);
  const memoryTexts = (...a: Parameters<PassesEnv['memoryTexts']>) => env.memoryTexts(...a);
  const askActionsWithLint = (...a: Parameters<PassesEnv['lint']['askActionsWithLint']>) => env.lint.askActionsWithLint(...a);
  const applyActions = (...a: Parameters<PassesEnv['actions']['applyActions']>) => env.actions.applyActions(...a);
  const snapshot = (...a: Parameters<PassesEnv['snapshot']>) => env.snapshot(...a);
  const ingestByTranscript = (...a: Parameters<PassesEnv['ingest']['ingestByTranscript']>) => env.ingest.ingestByTranscript(...a);
  const ingestByFilePass = (...a: Parameters<PassesEnv['ingest']['ingestByFilePass']>) => env.ingest.ingestByFilePass(...a);
  const organizePass = (...a: Parameters<PassesEnv['organize']['organizePass']>) => env.organize.organizePass(...a);
  const revocationPass = (...a: Parameters<PassesEnv['actions']['revocationPass']>) => env.actions.revocationPass(...a);
  const pooledSleepPass = (...a: Parameters<PassesEnv['poolPass']['pooledSleepPass']>) => env.poolPass.pooledSleepPass(...a);
  const pipelineModeOf = (...a: Parameters<PassesEnv['pool']['pipelineModeOf']>) => env.pool.pipelineModeOf(...a);

/** /new 之前把当前会话存成长期记忆：立刻整理，不等凌晨的睡眠。 */
async function archivePass(transcript: string, meta: { scope?: unknown } = {}): Promise<Record<string, unknown>> {
  // 轮级并发闸归 SlotScheduler（env 上不再有 inFlight 副本）
  if (!env.scheduler.beginPass()) return { skipped: 'busy' };
  const started = Date.now();
  try {
    const existing = memoryTexts(transcript);
    const asked = await askActionsWithLint({
      transcript, existing, effort: cfg.reasoningEffort, tag: '新会话存档',
    });
    const { actions } = asked;
    if (!actions) {
      log(cfg, `新会话存档：模型没给出可解析的 JSON：${String(asked.raw).replace(/\s+/g, ' ').slice(0, 300)}`);
      return { skipped: 'unparsable' };
    }
    const backup = await snapshot(cfg, store);
    const result = await applyActions(actions, { existing, dryRun: false });
    const changed =
      result.added.length + result.merged.length + result.updated.length + result.archived.length;
    const ms = Date.now() - started;
    log(
      cfg,
      `新会话存档完成 新增${result.added.length} 合并${result.merged.length}` +
        ` 修订${result.updated.length} 归档${result.archived.length} 耗时${ms}ms 来源=${meta.scope || '?'}`,
    );
    if (cfg.archiveNotify && changed > 0) {
      push(`🐟 开新会话前把上一段存好了：记了 ${changed} 条。`);
    }
    return { ran: true, backup, ms, ...result };
  } catch (error) {
    log(cfg, `新会话存档失败：${(error as Error | null)?.message || error}`);
    return { error: String((error as Error | null)?.message || error) };
  } finally {
    env.scheduler.endPass();
  }
}

async function sleepPass({ dryRun = false, sinceHours = 0 }: { dryRun?: boolean; sinceHours?: number } = {}): Promise<Record<string, unknown>> {
  if (pipelineModeOf() === 'pool') return pooledSleepPass({ dryRun, sinceHours }) as Promise<Record<string, unknown>>;
  return legacySleepPass({ dryRun, sinceHours });
}

/** 老路径：一条顺序 await 到底（灰度回滚用，行为一个字不改）。 */
async function legacySleepPass({ dryRun = false, sinceHours = 0 }: { dryRun?: boolean; sinceHours?: number } = {}): Promise<Record<string, unknown>> {
  if (!env.scheduler.beginPass()) return { skipped: 'busy' };
  const started = Date.now();
  // 「拒绝」只认 discards 落账（轮首/轮尾行数差）；提案打回不是同一口径。
  const discardsBefore = discardCountOf(store as unknown as GateStore);
  try {
    const st = stateStore.load();
    const since =
      sinceHours > 0
        ? Date.now() - sinceHours * 3600000
        : Number(st.lastSleepAt) || Date.now() - 24 * 3600000;
    // 2026-11-14：maxChanges（本轮最多改几条）删了，本轮该落的全部落。
    // 2026-11-05 第二步：开关默认 false，false 时下面走的是老路径（原样搬进 ingestByTranscript）
    const useIngestAck = cfg.pipeline?.ingestAck === true;
    let pass = useIngestAck
      ? await ingestByFilePass({ dryRun, lastSleepAt: since })
      : await ingestByTranscript({ dryRun, since });
    if (pass.fallback) pass = await ingestByTranscript({ dryRun, since });
    if (pass.skipped) return pass;
    if (useIngestAck && pass.stats?.processed === 0 && pass.stats?.replayed === 0 && pass.stats?.dead === 0) {
      return { skipped: 'too-quiet', messages: 0, files: pass.stats.scanned };
    }
    const { gathered, existing, existingChars, backup, result, parseFailed } = pass;
    const conflictInfo: ConflictInfo | null = pass.conflictInfo ?? result.conflictCheck ?? null;

    // 阶段二：分簇整理——先把记忆聚成小批，再一批一次模型调用，专治「一次塞太多看不见重复」
    const org = await organizePass({ dryRun });
    result.merged.push(...org.merged);
    result.updated.push(...org.updated);
    result.archived.push(...org.archived);
    result.rejected.push(...org.rejected);
    result.organize = org;
    // 分簇整理里消费 pending 拆分单的三样（agent 路径才有）：不并进「新增」，
    // 「新增」只认 agent 直接 add；拆分单列，下面跟阶段三的数一起汇总。
    const orgSplit = {
      inserted: Number(org.splitInserted) || 0,
      deduped: Number(org.splitDeduped) || 0,
      archived: Number(org.splitArchived) || 0,
    };

    // 阶段三：拆分——合并可能把叙事写长，超过 splitMaxChars 的在最后一次拆回「一条一件事」
    let splitInfo = { scanned: 0, tried: 0, split: 0, failed: 0, inserted: 0, deduped: 0, archived: 0, precheckSkipped: 0 };
    if (cfg.sleepSplitEnabled !== false) {
      try {
        const { runSplitPass } = await import('../split-pass.js');
        // 2026-11-14：sleepSplitLimit 删了，本轮超长条目全部拆
        const sv = await runSplitPass({
          store,
          llm: llmOf(ctx, { site: 'split:legacy', log: (line) => log(cfg, line), cfg }),
          cfg,
          dryRun,
          // 2026-11-14：拆分段问模型并发、落地串行，路数跟本轮 curate 通道同一份快照。
          concurrency: curatePick('split').conc,
        } as unknown as SplitPassOpts);
        splitInfo = {
          scanned: sv.scanned || 0,
          tried: sv.tried || 0,
          split: sv.split || 0,
          failed: sv.failed || 0,
          inserted: sv.inserted || 0,
          deduped: sv.deduped || 0,
          archived: sv.archived || 0,
          precheckSkipped: sv.precheckSkipped || 0,
        };
        if (splitInfo.tried || splitInfo.precheckSkipped) {
          log(
            cfg,
            `阶段三拆分：超长${splitInfo.scanned}条 尝试${splitInfo.tried} 拆开${splitInfo.split} 失败${splitInfo.failed}` +
              ` 入库${splitInfo.inserted} 判重${splitInfo.deduped} 归档${splitInfo.archived}` +
              ` 预检跳过${splitInfo.precheckSkipped}`,
          );
        }
      } catch (error) {
        log(cfg, `阶段三拆分失败：${(error as Error | null)?.message || error}`);
      }
    }
    result.split = splitInfo;
    // 两处拆分的三样合到一处报：
    // 「新增」保持只算 agent 直接 add 的条数，拆出来的子段单列成 splitInserted；
    // 判重各在 discards 落了一行账，所以并进「打回」（提案级）；汇总的「拒绝」另按
    // discards 行数差单报，两个口径别混。
    result.splitInserted = orgSplit.inserted + splitInfo.inserted;
    result.splitDeduped = orgSplit.deduped + splitInfo.deduped;
    result.splitArchived = orgSplit.archived + splitInfo.archived;
    for (let n = 0; n < splitInfo.deduped; n += 1) result.rejected.push({ kind: 'split', reason: '判重' });

    // 阶段四：作废清扫——纯规则、不花模型调用，独立预算，别被 archive 那道共享 budget 饿死
    let revokeInfo: RevokeSummary = { scanned: 0, archived: [], skipped: [], orphan: [] };
    try {
      revokeInfo = await revocationPass({ dryRun });
      if (revokeInfo.archived.length || revokeInfo.orphan.length) {
        log(
          cfg,
          `阶段四作废清扫：宣告条${revokeInfo.scanned}条 归档${revokeInfo.archived.length}` +
            ` 无旧条${revokeInfo.orphan.length} 跳过${revokeInfo.skipped.length}`,
        );
      }
    } catch (error) {
      // 清扫出错不许把整轮睡觉带下去
      log(cfg, `阶段四作废清扫失败：${(error as Error | null)?.message || error}`);
    }
    result.revocation = revokeInfo;
    result.archived.push(...revokeInfo.archived);

    // 阶段五：observation 提炼——默认走 pool 路径（重写入口在 pool-pass 的池排空之后），
    // legacy 回滚口子也接一次，保证两种调度下行为一致。失败只记日志；
    // 观察是派生数据，不计入本轮 changed / lastSleepAt 水位，原有语义不变。
    let observe: ObservationPassOutcome | null = null;
    try {
      observe = await runObservationPass(
        {
          store: store as unknown as ObservationPassStore,
          cfg,
          log: (line) => log(cfg, line),
          model: env.model,
          owner: env.sleepOwner,
        },
        { dryRun },
      );
      result.observe = observe;
    } catch (error) {
      log(cfg, `[observe] WARN 整段失败（不影响本轮睡觉）：${(error as Error | null)?.message || error}`);
    }

    const changed =
      result.added.length + result.merged.length + result.updated.length + result.archived.length;
    const ms = Date.now() - started;

    if (!dryRun && changed > 0) {
      env.state.lastRunAt = Date.now();
      env.state.runs += 1;
      stateStore.patch({
        // patch 内部先读最新 state，别拿前文的旧快照盖掉阶段二刚写的 organizeCache
        lastSleepAt: Date.now(),
        lastSleepDay: dayKey(new Date()),
        lastSummary: { changed, ms, at: new Date().toISOString() },
      });
    }

    const discardsAdded = Math.max(0, discardCountOf(store as unknown as GateStore) - discardsBefore);
    log(
      cfg,
      `睡觉完成 dryRun=${dryRun} 新增${result.added.length} 合并${result.merged.length}` +
        ` 修订${result.updated.length} 归档${result.archived.length} 拒绝${discardsAdded}(discards)` +
        ` 打回${result.rejected.length}(提案)` +
        ` 作废清扫${revokeInfo.archived.length}` +
        ` 动作上限=无 记忆${existing.length}条/${existingChars}字` +
        ` 整理${org.calls}批/共${org.clusters}批/记忆${org.total}条` +
        ` 拆分${splitInfo.split}条/尝试${splitInfo.tried}` +
        ` 拆分入库${result.splitInserted}/判重${result.splitDeduped}/归档${result.splitArchived}` +
        (conflictInfo
          ? ` 入口检查${conflictInfo.checked}条/状态变更${conflictInfo.stateChange.length}/冗余丢弃${conflictInfo.redundant.length}/矛盾${conflictInfo.conflict.length}`
          : '') +
        ` 耗时${ms}ms 会话${gathered.files}个/消息${gathered.messages}条/字符${gathered.transcript.length}`,
    );

    return {
      ran: true,
      dryRun,
      parseFailed,
      ms,
      discards: discardsAdded,
      backup,
      messages: gathered.messages,
      sessions: gathered.files,
      chars: gathered.transcript.length,
      ...result,
    };
  } finally {
    env.scheduler.endPass();
  }
}

  return { archivePass, sleepPass, legacySleepPass };
}
