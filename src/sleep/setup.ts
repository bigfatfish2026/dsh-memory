/**
 * 睡觉链装配：先建唯一上下文，再按依赖顺序装各段，最后挂钩子/工具。
 */
import fs from 'node:fs';
import path from 'node:path';
import { defineTool } from '@deepseek-ai/dsh-tools';
import { assertMayWrite } from '../write-guard.js';
import { attachServiceWatch, createSleepDriver, probeService } from '../sleep-schedule.js';
import type { SleepDriverOptions } from '../sleep-schedule.js';


/** 睡觉结果的视图（sleepPass 返回的宽对象）。 */
interface SleepPassView {
  skipped?: unknown;
  error?: unknown;
  dryRun?: unknown;
  ms?: unknown;
  messages?: unknown;
  sessions?: unknown;
  chars?: unknown;
  added?: unknown[];
  merged?: unknown[];
  updated?: unknown[];
  archived?: unknown[];
  rejected?: unknown[];
  raw?: unknown;
  [key: string]: unknown;
}
import { attachPriceProbe } from '../price-probe.js';
import { logWindowOnce } from '../token-window.js';
import { createSleepEnv } from './env.js';
import { log } from './log.js';
import { slotsOf } from './slots.js';
import { createLint } from './lint.js';
import { createModelCalls } from './model-calls.js';
import { createActions } from './actions.js';
import { createLedgerGlue } from './ledger-glue.js';
import { createAgentOps } from './agent-ops.js';
import { createReplay } from './replay.js';
import { createOrganize } from './organize.js';
import { createOrganizeAgent } from './organize-agent.js';
import { createIngest } from './ingest.js';
import { createPool } from './pool.js';
import { createPoolPass } from './pool-pass.js';
import { createPasses } from './passes.js';
import { createScheduler } from './scheduler.js';
import type { SleepCfg, SleepStore, SleepEnv, SleepSessions } from './env.js';

/** 宿主 ctx 在装配处用到的面。 */
interface SetupCtx {
  logger?: { info?: (line: string) => void; warn?: (line: string) => void };
  effect: (fn: () => (() => void) | void) => unknown;
  tools: NonNullable<SleepEnv['ctx']['tools']> & { register: (tool: unknown) => unknown };
  on?: (event: string, listener: (name: string, value: unknown) => unknown) => (() => void) | undefined;
  get: (name: string) => SleepSessions;
  provide: (name: string, service: unknown) => unknown;
  [key: string]: unknown;
}

export function setupSleep(ctx: SetupCtx, store: SleepStore, cfg: SleepCfg): Record<string, unknown> {
  // 价档判据接 dsh-price-clock：必须在最前面同步接上，sleepDriver.start() 会立即补判一次。
  if (cfg.sleepPriceProbe !== false) attachPriceProbe(cfg, cfg.priceClockFile);
  const env = createSleepEnv(ctx, store, cfg) as SleepEnv;
  // state 文件路径必须打出来：它跟着 cwd 走（paths.memoryFile → cwd/memory），
  // 而库路径来自 storePath（这台机器上是绝对路径）。两者分家时，核对的人会去翻库里那份旧
  // state——2026-11-16 05:00 那轮跑完被误判成「没写 state」就是这个原因。落地就打。
  try {
    const stateFile = String(cfg.stateFile || '');
    const stateDir = stateFile ? path.dirname(path.resolve(stateFile)) : '';
    const storeDir = cfg.storePath ? path.resolve(String(cfg.storePath)) : '';
    log(cfg, `[sleep] state 文件=${stateFile || '(未配)'} cwd=${process.cwd()}`);
    if (stateDir && storeDir && stateDir !== storeDir) {
      log(cfg, `[sleep] WARN state 目录 ${stateDir} 与库目录 ${storeDir} 不一致：核对 state 只认上面那行的路径`);
    }
  } catch {
    /* 日志打不出来不影响整理 */
  }
  // 调度器先建：轮级状态（inFlight/roundSnapshot/mergeBatchCache）归它，
  // 各段构造时捕获的是转发函数，真正调用发生在 passes 挂好之后。
  // 各段按依赖顺序装到同一个 env 上；构造发生在段挂齐之前，所以这里 env 的类型是最终形状 SleepEnv。
  // 各段 Env 接口比 SleepEnv 窄（MemoryView 写成 MemoryRecord、tags 收成必填），lint / agentOps 两段单转即可；
  // actions / organize / ingest / passes 四段与 SleepEnv 重叠不足（SleepStore 缺 metas、askCluster 吃
  // MemoryClusterItem、lint.askActionsWithLint 的 existing），只能保留 unknown 双转——运行时传的是同一个对象。
  env.scheduler = createScheduler(env);
  // 各段只拿 env，不另持状态；装完再挂到 env 上供彼此按名调用（调用都发生在挂完之后）。
  env.lint = createLint(env as Parameters<typeof createLint>[0]);
  env.model = createModelCalls(env);
  env.actions = createActions(env as unknown as Parameters<typeof createActions>[0]);
  env.ledger = createLedgerGlue(env);
  env.agentOps = createAgentOps(env as Parameters<typeof createAgentOps>[0]);
  env.replay = createReplay(env);
  env.organize = createOrganize(env as unknown as Parameters<typeof createOrganize>[0]);
  env.organizeAgent = createOrganizeAgent(env);
  env.ingest = createIngest(env as unknown as Parameters<typeof createIngest>[0]);
  env.pool = createPool(env);
  env.poolPass = createPoolPass(env);
  env.passes = createPasses(env as unknown as Parameters<typeof createPasses>[0]);

  const push = (...a: Parameters<SleepEnv['push']>) => env.push(...a);
  const runDue = (...a: Parameters<SleepEnv['scheduler']['runDue']>) => env.scheduler.runDue(...a);
  const sleepPass = (...a: Parameters<SleepEnv['passes']['sleepPass']>) => env.passes.sleepPass(...a);
  const organizePass = (...a: Parameters<SleepEnv['organize']['organizePass']>) => env.organize.organizePass(...a);
  const organizePassAgent = (...a: Parameters<SleepEnv['organizeAgent']['organizePassAgent']>) => env.organizeAgent.organizePassAgent(...a);
  const archivePass = (...a: Parameters<SleepEnv['passes']['archivePass']>) => env.passes.archivePass(...a);
  const revocationPass = (...a: Parameters<SleepEnv['actions']['revocationPass']>) => env.actions.revocationPass(...a);
  const replayPendingOps = (...a: Parameters<SleepEnv['replay']['replayPendingOps']>) => env.replay.replayPendingOps(...a);

try {
  ctx.logger?.info?.(
    `dsh-memory 整理时段：${slotsOf(cfg)
      .map((s) => `${String(s.hour).padStart(2, '0')}:${String(s.minute).padStart(2, '0')}`)
      .join(' / ')}`,
  );
  // 窗口来源：contextWindowTokens → tokenMeter → session.requestContext → 宿主 compaction。
  // 日志用 warn 而非 info：这台 dsh 的 info 只进内存环形缓冲，journal 里看不见。
  // 重启时还没有活会话就一直不打（不把「未知」锁死），等第一条带会话的请求/第一次算预算。
  logWindowOnce(ctx, cfg, env.windowSessionOf());
} catch {
  /* 日志打不出来不影响 */
}

// 时钟源：优先 dsh-timer 的 memory.sleep.tick（每个时段一条 cron 规则），
// 注册不齐/服务不在/tick 断流都退回自己的 checkIntervalMs 定时器。
const sleepDriver = createSleepDriver({
  slots: () => slotsOf(cfg),
  events: () => probeService(ctx, 'events') as ReturnType<SleepDriverOptions['events']>,
  schedule: () => probeService(ctx, 'schedule') as ReturnType<SleepDriverOptions['schedule']>,
  run: ({ source, periodKey }) => {
    if (source === 'tick' && periodKey) log(cfg, `收到 tick ${periodKey}`);
    runDue('定时');
  },
  checkMs: cfg.checkIntervalMs,
  info: (line) => log(cfg, line),
  warn: (line) => log(cfg, line),
});

ctx.effect(() => {
  sleepDriver.start();
  return () => sleepDriver.dispose();
});

// schedule 服务一就绪就重挂规则，销毁就回落定时器，不用等下一拍轮询。
// 监听器内部整只包了 try/catch：events 的 emit 不兜错，冒出去会弄挂 timer 的 apply。
ctx.effect(() => {
  const off = attachServiceWatch(ctx, sleepDriver, { warn: (line) => log(cfg, line) });
  return () => off();
});

// 自检钩子：重启前留一个 marker，插件加载后跑一遍演练并把结果推出来
ctx.effect(() => {
  let marker = false;
  try {
    marker = fs.existsSync(cfg.selfTestMarker as string);
  } catch {
    marker = false;
  }
  if (!marker) return () => {};
  try {
    fs.unlinkSync(cfg.selfTestMarker as string);
  } catch {
    /* 删不掉就算了 */
  }
  const timer = setTimeout(async () => {
    try {
      const r: SleepPassView = await sleepPass({ dryRun: true, sinceHours: 12 });
      push(
        `🧪 睡觉自检（演练）：读 ${r.sessions || 0} 个会话 / ${r.messages || 0} 条消息；` +
          `计划 新增${r.added?.length || 0} 合并${r.merged?.length || 0} 修订${r.updated?.length || 0} 归档${r.archived?.length || 0}` +
          (r.skipped ? `（跳过：${r.skipped}）` : '') +
          (r.error ? `（出错：${r.error}）` : ''),
      );
    } catch (error) {
      push(`🧪 睡觉自检失败：${(error as Error | null)?.message || error}`);
    }
  }, 30000);
  if (typeof timer.unref === 'function') timer.unref();
  return () => clearTimeout(timer);
});

ctx.tools.register(
  defineTool({
    name: 'memory_sleep',
    description:
      'Run the nightly memory "sleep" pass now: read session transcripts since the last sleep, then add/merge/update/archive long-term memories. Use dryRun to preview without writing.',
    parameters: {
      dryRun: { type: 'boolean', description: 'Preview the planned changes without writing.' },
      sinceHours: { type: 'number', description: 'Override the lookback window in hours (default: since last sleep).' },
    },
    output: {
      schema: {
        type: 'object',
        additionalProperties: false,
        properties: {
          ran: { type: 'boolean', required: true },
          skipped: { type: 'string' },
          dryRun: { type: 'boolean' },
          ms: { type: 'number' },
          messages: { type: 'number' },
          sessions: { type: 'number' },
          chars: { type: 'number' },
          added: { type: 'array', items: { type: 'string' } },
          merged: { type: 'array', items: { type: 'object', additionalProperties: true } },
          updated: { type: 'array', items: { type: 'object', additionalProperties: true } },
          archived: { type: 'array', items: { type: 'object', additionalProperties: true } },
          rejected: { type: 'array', items: { type: 'object', additionalProperties: true } },
          error: { type: 'string' },
          raw: { type: 'string' },
        },
      },
      render: (_args: unknown, v: SleepPassView) => {
        if (!v.ran) {
          return [{ type: 'text', text: `没睡成：${v.skipped || v.error || '未知'}${v.raw ? `\n原始输出：${v.raw}` : ''}` }];
        }
        const lines = [
          `${v.dryRun ? '演练' : '睡觉'}完成，${(((v.ms as number) || 0) / 1000).toFixed(1)}s，读 ${v.sessions} 个会话 / ${v.messages} 条消息 / ${v.chars} 字符`,
          `新增 ${v.added?.length || 0}，合并 ${v.merged?.length || 0}，修订 ${v.updated?.length || 0}，归档 ${v.archived?.length || 0}`,
        ];
        for (const t of v.added || []) lines.push(`+ ${(t as string).slice(0, 80)}`);
        for (const m of v.merged || []) lines.push(`≈ ${(m as { ids?: string[] }).ids?.join('+')} → ${String((m as { text?: unknown }).text).slice(0, 60)}`);
        for (const u of v.updated || []) lines.push(`~ ${(u as { id?: string }).id}: ${String((u as { after?: unknown }).after).slice(0, 60)}`);
        for (const a of v.archived || []) lines.push(`- ${(a as { id?: string }).id}（${(a as { reason?: string }).reason}）`);
        for (const r of v.rejected || []) lines.push(`✗ ${(r as { kind?: string }).kind}: ${(r as { reason?: string }).reason}`);
        return [{ type: 'text', text: lines.join('\n') }];
      },
    },
    async execute(args, exec) {
      assertMayWrite(exec, 'memory_sleep');
      const result: SleepPassView = await sleepPass({
        dryRun: Boolean(args.dryRun),
        sinceHours: Number(args.sinceHours) || 0,
      });
      const out: Record<string, unknown> = {
        ran: !result.skipped,
        dryRun: Boolean(result.dryRun),
        ms: Number(result.ms) || 0,
        messages: Number(result.messages) || 0,
        sessions: Number(result.sessions) || 0,
        chars: Number(result.chars) || 0,
        added: result.added || [],
        merged: result.merged || [],
        updated: result.updated || [],
        archived: result.archived || [],
        rejected: result.rejected || [],
      };
      // undefined 会让工具返回值无法通过 JSON 校验，只在有值时补字段
      if (result.skipped) out.skipped = String(result.skipped);
      if (result.error) out.error = String(result.error);
      if (result.raw) out.raw = String(result.raw).slice(0, 300);
      return out as never;
    },
  }),
);


  // 给 node -e 做只读演练用（index.js 不接这个返回值）
  return {
    sleepPass, organizePass, organizePassAgent, archivePass, revocationPass,
    __applyActions: env.actions.applyActions,
    __replay: (opts = {}) => replayPendingOps(opts),
  };
}
