/**
 * 时钟侧：定时器/tick 驱动的 scheduledRun、欠账补齐 sweepOwedSlots、due 判定 runDue。
 *
 * 2026-11-15：闭包收成 SlotScheduler 类。sleep 链上三份轮级可变状态
 * 明确归这里，env 上不再留副本：
 *   - inFlight         一轮/一次存档的并发闸（beginPass/endPass）；
 *   - roundSnapshot    一轮只取一次的负载快照（setRoundSnapshot/roundSnapshot）；
 *   - mergeBatchCache  本轮 merge 结论缓存（mergeCache/putMergeResult）。
 * 另外 stageLogged（阶段自适应日志去重）也归这里，因为它是按轮清空的。
 * 不可变的 ctx/store/cfg/stateStore 与「怎么跑一轮」（env.passes）仍从 env 取。
 */
import fs from 'node:fs';
import path from 'node:path';
import { ensureDir } from '../shared/fs.js';
import { isCheapNow, isCatchUpSlot, splitOwedSlots, planSlotRun } from '../price-gate.js';
import { selectCurateConcurrency, formatStageAdaptiveLog } from '../concurrency.js';
import {
  slotsOf, slotKeyFor, dayKey, dueSleepSlotNow, shouldMarkSlot, sameSleepOwner, SLEEP_HEARTBEAT_MS,
} from './slots.js';
import type { LoadSnapshot, ConcurrencyConfig, ConcurrencyPick } from '../concurrency.js';
import type { SleepCfg, SleepCtx } from './env.js';
import type { SleepState, SleepStateStore } from '../sleep-state.js';
import type { ItemReviewCache } from '../item-review-cache.js';

/** 调度器用到的 env 面（装配完才有 passes）。 */
export interface SchedulerEnv {
  cfg: SleepCfg;
  ctx: SleepCtx;
  stateStore: SleepStateStore;
  sleepOwner: string;
  log: (line: string) => void;
  push: (msg: string) => void;
  passes: {
    sleepPass: (opts: { dryRun?: boolean; sinceHours?: number }) => Promise<Record<string, unknown>>;
    archivePass: (transcript: string, meta?: { scope?: unknown }) => Promise<Record<string, unknown>>;
  };
}

/** state 写失败时的短错误文本：日志一行能读，别把整个 stack 塞进去。 */
function shortStateErr(error: unknown): string {
  const msg = (error as Error | null)?.message || String(error);
  return msg.replace(/\s+/g, ' ').slice(0, 200);
}

/** 睡觉结果的宽面：真正字段由 passes 产出，这里只取调度要用的几项。 */
interface SleepRunResult {
  skipped?: unknown;
  tidy?: { applied?: unknown; runId?: unknown; report?: unknown; reportFile?: unknown };
  added?: unknown[];
  merged?: unknown[];
  updated?: unknown[];
  archived?: unknown[];
  [key: string]: unknown;
}

/** 本轮 merge 结论。 */
interface MergeVerdict {
  v: string;
  t: number;
}

export class SlotScheduler {
  #env: SchedulerEnv;

  #cfg: SleepCfg;

  #stateStore: SleepStateStore;

  #sleepOwner: string;

  /** 同一时刻只许一轮整理（含 /new 存档）。跨实例靠 sleep.state.json 的 runningAt/owner。 */
  #inFlight = false;

  /** 一轮只取一次负载快照，全链各段都拿这一份算并发（免得每批重算导致路数抖动）。 */
  #roundSnapshot: LoadSnapshot | null = null;

  /** 本轮 merge 结论缓存：批指纹 -> {v:'acted'|'noop', t}。收尾并进 organizeCache。 */
  #mergeBatchCache: Record<string, MergeVerdict> = {};

  /** 本轮条目级复审结论：条目键 -> {v,rel,t}。收尾并进 state 的 itemReviewCache。 */
  #itemReviewCache: ItemReviewCache = {};

  /** 阶段自适应日志每轮只打一次。 */
  #stageLogged = new Set<string>();

  constructor(env: SchedulerEnv) {
    this.#env = env;
    this.#cfg = env.cfg;
    this.#stateStore = env.stateStore;
    this.#sleepOwner = env.sleepOwner;
  }

  /** 抢本轮并发闸：false = 已有一轮在跑（调用方回 {skipped:'busy'}，行为与改前一致）。 */
  beginPass(): boolean {
    if (this.#inFlight) return false;
    this.#inFlight = true;
    return true;
  }

  /** 放闸；顺手清掉轮级缓存（原 finally 里的三行清理）。 */
  endPass(): void {
    this.#roundSnapshot = null;
    this.#stageLogged.clear();
    this.#mergeBatchCache = {};
    this.#itemReviewCache = {};
    this.#inFlight = false;
  }

  get inFlight(): boolean {
    return this.#inFlight;
  }

  get roundSnapshot(): LoadSnapshot | null {
    return this.#roundSnapshot;
  }

  /** 一轮开始时落一份负载快照并返回它。 */
  setRoundSnapshot(snapshot: LoadSnapshot): LoadSnapshot {
    this.#roundSnapshot = snapshot;
    return this.#roundSnapshot;
  }

  /** 本轮 merge 结论缓存（整体视图；调用方只读后合并进 organizeCache）。 */
  mergeCache(): Record<string, MergeVerdict> {
    return this.#mergeBatchCache;
  }

  /** 记一条本轮的 merge 结论。 */
  putMergeResult(label: string, value: MergeVerdict): MergeVerdict {
    this.#mergeBatchCache[label] = value;
    return value;
  }

  /** 本轮条目级复审结论（整体视图；pool 收尾并进 state 的 itemReviewCache）。 */
  itemReviewCache(): ItemReviewCache {
    return this.#itemReviewCache;
  }

  /** 并入一批条目级结论（同键覆盖，后到的 t 更新）。 */
  putItemReview(additions: ItemReviewCache): ItemReviewCache {
    this.#itemReviewCache = { ...this.#itemReviewCache, ...(additions || {}) };
    return this.#itemReviewCache;
  }

  /** 某段的并发口径：cap 走统一口径，快照用本轮的；独立调用时没有轮级快照就现取一次。 */
  curatePick(stage: string): ConcurrencyPick {
    const pick = selectCurateConcurrency(this.#cfg as unknown as ConcurrencyConfig, this.#roundSnapshot);
    if (pick.adaptive && !this.#stageLogged.has(stage)) {
      this.#stageLogged.add(stage);
      this.#env.log(formatStageAdaptiveLog(stage, pick));
    }
    return pick;
  }

  async scheduledRun(reason: unknown, slotKey = ''): Promise<Record<string, unknown>> {
    const cfg = this.#cfg;
    const stateStore = this.#stateStore;
    const log = (line: string) => this.#env.log(line);
    const push = (...a: Parameters<SchedulerEnv['push']>) => this.#env.push(...a);
    const startedAt = Date.now();
    // 跑这轮的是谁：启动补判靠它认「上一实例留下的锁」，不是自己就直接接管。
    const owner = this.#sleepOwner;
    // 跨 HMR 的并发闸：inFlight 是实例里的，重载就丢；runningAt+heartbeatAt+owner 落 state，重载后新实例也看得见。
    try {
      stateStore.patch({ runningAt: startedAt, heartbeatAt: startedAt, owner });
    } catch (error) {
      // 写不进状态也要跑，但必须留痕：state 写失败是「下一拍会重跑同一档」的前兆。
      log(`[sleep] WARN 写 state 失败（runningAt）：${shortStateErr(error)}；本轮照跑`);
    }
    // 定时心跳：单批调用再久也不会超过 3 个间隔没心跳
    let beatFailures = 0;
    const beatTimer = setInterval(() => {
      try {
        const fresh = stateStore.load();
        if (Number(fresh.runningAt) !== startedAt) return;
        stateStore.patch({ heartbeatAt: Date.now(), owner });
      } catch (error) {
        // 每 60 秒一次，坏盘时会刷屏：只打第一次，后面的等收尾再算。
        beatFailures += 1;
        if (beatFailures === 1) log(`[sleep] WARN 写 state 失败（心跳）：${shortStateErr(error)}`);
      }
    }, SLEEP_HEARTBEAT_MS);
    if (typeof beatTimer.unref === 'function') beatTimer.unref();
    const clearRunning = () => {
      try { clearInterval(beatTimer); } catch { /* 清不掉不影响 */ }
      try {
        const fresh = stateStore.load();
        if (Number(fresh.runningAt) !== startedAt) return;
        stateStore.update((s) => {
          const out = { ...s };
          delete out.runningAt;
          delete out.heartbeatAt;
          delete out.owner;
          return out;
        });
      } catch (error) {
        // 清不掉就等心跳过期（3 个间隔），但要留一行，别让「轮还在跑」变成幽灵锁。
        log(`[sleep] WARN 写 state 失败（清 runningAt）：${shortStateErr(error)}`);
      }
    };
    const markSlot = () => {
      if (!slotKey) return;
      const now = new Date();
      const today = dayKey(now);
      const nowMin = now.getHours() * 60 + now.getMinutes();
      const fresh = stateStore.load();
      const slots: Record<string, unknown> = {};
      for (const [key, ts] of Object.entries(fresh.slots || {})) {
        if (key.startsWith(`${today} `)) slots[key] = ts;
      }
      slots[slotKey] = Date.now();
      // 今天已经过点、还没标记的时段一起标掉：部署当天别连着跑第二遍
      for (const s of slotsOf(cfg)) {
        if (nowMin >= s.hour * 60 + s.minute) {
          const k = slotKeyFor(now, s);
          if (!slots[k]) slots[k] = Date.now();
        }
      }
      const at = Date.now();
      const written = stateStore.patch({ slots, lastSleepAt: at, lastSleepDay: today });
      // 落地就打：这一行是「state 真的写进去了」的唯一凭据（核对 state 时先 grep 它）。
      log(
        `[sleep] slot 已标记 ${slotKey} slots=${Object.keys(written.slots || {}).sort().join(',')} ` +
          `lastSleepAt=${written.lastSleepAt} state=${stateStore.file || '(内存态)'}`,
      );
    };
    try {
      const result = await this.#env.passes.sleepPass({}) as SleepRunResult;
      if (!shouldMarkSlot(result)) {
        // busy：另一轮正占着，这一发什么都没干。标了 slot 就等于当天不再跑，
        // 而真正在跑的那一轮会自己 markSlot，这里不动。
        log(`睡觉跳过：${result.skipped}`);
        return result;
      }
      markSlot();
      if (result.skipped) {
        log(`睡觉跳过：${result.skipped}`);
        return result;
      }
      // 实体自动合并单独汇报：维护者要求「仍然要汇报」，
      // 不能只报候选。这条不受 sleepSummaryNotify=false 影响：只有真合并了才发，
      // 只带对数、候选数与撤回命令，不刷条目正文。
      const tidy = result?.tidy;
      if (tidy && Number(tidy.applied) > 0) {
        push(
          `🧹 实体自动合并 ${tidy.applied} 对（run_id=${tidy.runId}，跳过候选 ${tidy.report} 对）\n` +
            `撤回：node scripts/entity-tidy.mjs --undo-run ${tidy.runId}\n` +
            (tidy.reportFile ? `报告：${tidy.reportFile}` : ''),
        );
      }
      if (cfg.sleepSummaryNotify) {
        push(
          `🛏️ 睡醒了（${reason}）\n` +
            `新增 ${result.added!.length} / 合并 ${result.merged!.length} / 修订 ${result.updated!.length} / 归档 ${result.archived!.length}\n` +
            (result.added!.length ? `新记：${result.added!.map((t) => (t as string).slice(0, 30)).join('；')}` : '没有新东西，只是把旧的理了理'),
        );
      }
      return result;
    } catch (error) {
      log(`睡觉失败 耗时${Date.now() - startedAt}ms：${(error as Error | null)?.message || error}`);
      push(`🛏️ 睡觉失败了：${(error as Error | null)?.message || error}`);
      return { error: String((error as Error | null)?.message || error) };
    } finally {
      clearRunning();
    }
  }

  /**
   * 待补槽：贵时段推迟下来的档记在 sleep.state.json 的
   * `owedSlots`（slotKey → 记下待补的时刻），这里是唯一的读写落地口。
   *
   * 三条清理规矩，都是为了「不丢也不重复跑」：
   * - 跨天的作废：那一轮的输入今天的档会重新读一遍，隔天补跑等于白跑一遍；
   * - 已经落了 slot 标记的清掉不补：那一档真跑完了，补第二遍就是重复花钱；
   * - 积压超过 2 天打醒目日志再作废：说明价档判断可能坏死了，不许静默丢。
   *
   * @returns {{state: object, today: Array<{key: string, at: number}>}} 今天还欠着的槽，按时间升序
   */
  sweepOwedSlots(now: Date): { state: SleepState; today: Array<{ key: string; at: number }> } {
    const stateStore = this.#stateStore;
    const log = (line: string) => this.#env.log(line);
    const st = stateStore.load();
    const owedIn: Record<string, unknown> = st.owedSlots && typeof st.owedSlots === 'object' ? st.owedSlots : {};
    const { today, dropped } = splitOwedSlots(owedIn, now);
    const owed: Record<string, unknown> = { ...owedIn };
    let dirty = false;
    for (const one of dropped) {
      delete owed[one.key];
      dirty = true;
      if (one.expired) {
        log(
          `[sleep] ⚠️ 待补槽 ${one.key} 积压 ${Math.round(one.ageMs / 3600000)} 小时还没补上，价档判断可能坏了，先作废`,
        );
      } else {
        log(`[sleep] 待补槽 ${one.key} 已跨天，作废不补`);
      }
    }
    const doneMap: Record<string, unknown> = st.slots || {};
    const pending: Array<{ key: string; at: number }> = [];
    for (const one of today) {
      if (doneMap[one.key]) {
        // 已经真跑过并落了标记：清掉，别当欠账再补一遍
        delete owed[one.key];
        dirty = true;
        continue;
      }
      pending.push(one);
    }
    if (dirty) {
      try {
        stateStore.patch({ owedSlots: owed });
      } catch {
        /* 写不进就下一拍再试，内存里这份照用 */
      }
    }
    return { state: { ...st, owedSlots: owed }, today: pending };
  }

  /**
   * 到点就跑：判定只有一份（pickDueSleepSlot），定时器轮询和 dsh-timer 的 tick
   * 都走这里。当天已跑、去重键、scheduledRun 内部一律照旧。
   *
   * 2026-11-14 加价档闸：**补跑**只许在便宜时段发生。贵时段把槽记成
   * 待补（sweepOwedSlots），进了便宜时段的第一拍再补。准点触发的定时档这轮不动
   * （`planSlotRun` 里 cheap=false 也放行非补跑的档）。价档判据是注入的，没接时恒为
   * 「便宜」，行为与改动前完全一致。
   */
  runDue(reason: unknown): boolean {
    const cfg = this.#cfg;
    const stateStore = this.#stateStore;
    const sleepOwner = this.#sleepOwner;
    const log = (line: string) => this.#env.log(line);
    if (!cfg.sleepEnabled) return false;
    const now = new Date();
    // 判据没接、判据抛错都算便宜（fail-open）：宁可偶尔多花一笔，也不许把整理停掉。
    const cheap = cfg.sleepDeferInPeak === false || isCheapNow(now);
    const swept = this.sweepOwedSlots(now);
    const st = swept.state;
    const owed = st.owedSlots || {};
    // 待补优先：便宜时段的第一拍就把它补掉，别等下一个定时档（贵时段就继续等，不刷日志）。
    const owedKey = swept.today.length ? swept.today[0].key : '';
    if (owedKey && cheap) {
      const rest = { ...owed };
      delete rest[owedKey];
      try {
        stateStore.patch({ owedSlots: rest });
      } catch {
        /* 写不进也照跑：scheduledRun 起跑会再写一次 */
      }
      log(`[sleep] 便宜时段，补跑待补的槽 ${owedKey}`);
      void this.scheduledRun(reason, owedKey);
      return true;
    }
    const key = dueSleepSlotNow({
      slots: slotsOf(cfg),
      done: st.slots || {},
      now,
      runningAt: st.runningAt,
      heartbeatAt: st.heartbeatAt,
      owner: st.owner,
      currentOwner: sleepOwner,
    });
    if (!key) return false;
    // 上一实例（重启前的进程）留下的轮锁：它已经不在了，当场接管，不等 180s TTL。
    // 旧逻辑等到锁过期时叫醒源已经没了（12:30 的 tick 早消费掉、60s 轮询也停了），
    // 这一轮就永远补不上——2026-11-14 12:30 那轮正是这么丢的。
    const takingOver = Boolean(st.owner) && !sameSleepOwner(st.owner, sleepOwner);
    const catchUp = takingOver || isCatchUpSlot({ key, now });
    if (planSlotRun({ key, cheap, catchUp }) === 'defer') {
      // 已经在待补里就别每 60 秒刷一行：只在这一档刚被推迟时记一次。
      if (owed[key]) return false;
      try {
        stateStore.patch({ owedSlots: { ...owed, [key]: Date.now() } });
      } catch {
        /* 写不进下一拍还会再判一次，不会丢这一档 */
      }
      log(`[sleep] 贵时段，槽 ${key} 推迟到下一个便宜时段`);
      return false;
    }
    if (takingOver) {
      log(`[sleep] 接管上一实例的轮 slot=${key} 旧 owner=${st.owner}`);
      try {
        stateStore.patch({ runningAt: Date.now(), heartbeatAt: Date.now(), owner: sleepOwner });
      } catch {
        /* 写不进也照跑：scheduledRun 起跑会再写一次 */
      }
    }
    void this.scheduledRun(reason, key);
    return true;
  }

  /**
   * 把存档钩子挂到 ctx 上（原来在 createScheduler 闭包里）。
   * 只有一支预算：/new 之前把会话正文交给这里，后台整理进长期记忆。
   */
  installArchiveHook(): void {
    const cfg = this.#cfg;
    const archivePass = (...a: Parameters<SchedulerEnv['passes']['archivePass']>) => this.#env.passes.archivePass(...a);
    this.#env.ctx.provide('memory-archive', {
      archive(text: unknown, meta: { scope?: unknown } = {}) {
        const body = String(text || '').trim();
        if (!body) return { queued: false };
        try {
          ensureDir(cfg.archiveDir);
          const stamp = new Date().toISOString().replace(/[:.]/g, '-');
          fs.writeFileSync(path.join(cfg.archiveDir, `${stamp}-${String(meta.scope || 'x')}.md`), body, 'utf8');
        } catch {
          /* 留底失败不挡主流程 */
        }
        const timer = setTimeout(() => {
          void archivePass(body, meta);
        }, 1500);
        if (typeof timer.unref === 'function') timer.unref();
        return { queued: true };
      },
    });
  }
}

/** sleep 链的调度器（旧工厂名保留）。 */
export function createScheduler(env: SchedulerEnv): SlotScheduler {
  const scheduler = new SlotScheduler(env);
  scheduler.installArchiveHook();
  return scheduler;
}
