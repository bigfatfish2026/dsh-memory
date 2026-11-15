/**
 * 正文 lint 三段：代码打标记 → 复核 agent 裁决 → 退回重做一次（只一次）。
 */
import { parseActions } from '../transcript.js';
import { lintMemoryText } from '../text-lint.js';
import { recordLintRejects } from '../lint-reject.js';
import { llmOf } from '../llm-service.js';
import {
  judgeSuspected, buildLintRedoFeedback, mergeReviewRejects, lintKeptMarker,
} from '../lint-review.js';
import type { ParsedActions } from '../transcript.js';
import type { LintViolation } from '../text-lint.js';
import type { ReviewConfig, ReviewItem, ReviewLlm } from '../lint-review.js';
import type { LlmContextLike } from '../llm-service.js';
import type { MemoryRecord } from '../store/types.js';
import type { DatabaseSync } from 'node:sqlite';

/** env 里本文件用到的面：配置、日志、问模型、取 pending 库句柄、复核要用的 ctx。 */
interface LintEnv {
  /** 复核要现取 llm 服务（llmOf(ctx)），所以 ctx 必须从 env 带进来。 */
  ctx: LlmContextLike;
  cfg: ReviewConfig & { sleepLogFile?: string | null };
  log: (line: string) => void;
  model: {
    askModel: (transcript: string, existing: MemoryRecord[], effort: unknown, feedback?: string) => Promise<string>;
  };
  ledger: { pendingDb: () => DatabaseSync | null };
}

/** actions 里带正文的三类（archive 没正文，不判）。 */
type ActionKind = 'merge' | 'update' | 'add';

/** 一个待写库动作：只保证能取 text/id，其余字段原样透传。 */
interface LintItem {
  text?: string;
  id?: string;
  [key: string]: unknown;
}

/** 一轮 actions 的四个桶。 */
export interface LintActions {
  add: LintItem[];
  archive: LintItem[];
  merge: LintItem[];
  update: LintItem[];
}

/** 代码 lint 打回的一条：kind 桶 + 原 item + 违规项。 */
interface LintRejected {
  kind: ActionKind;
  item: LintItem;
  text: string;
  violations: LintViolation[];
  [key: string]: unknown;
}

/** lintActions 的产物。 */
interface LintFirst {
  actions: LintActions;
  rejected: LintRejected[];
}

/** lintRedoOnce 的入参。 */
export interface LintRedoOpts {
  agent: string;
  action: string;
  actions: LintActions;
  ask: (feedback: string) => Promise<string>;
  trace?: (line: string) => void;
}

/** 三段式走完的结果。 */
export interface LintRedone {
  actions: LintActions;
  raw?: string;
  lintRejected: ReviewItem[];
  lintRetried: number;
  lintReviewed: number;
  lintReviewFallback: boolean;
}

/** askActionsWithLint 的结果：没有可解析的 actions 时为 null（raw 照旧带回去）。 */
export type LintAsked = Omit<LintRedone, 'actions'> & { actions: LintActions | null };

/** askActionsWithLint 的入参。 */
interface LintAskOpts {
  transcript: string;
  existing: MemoryRecord[];
  effort: unknown;
  tag?: string;
}

export function createLint(env: LintEnv) {
  const { cfg, ctx } = env;
  const log = (_c: ReviewConfig, line: string) => env.log(line);
  const askModel = (...a: Parameters<LintEnv['model']['askModel']>) => env.model.askModel(...a);
  const pendingDb = (...a: Parameters<LintEnv['ledger']['pendingDb']>) => env.ledger.pendingDb(...a);

const actionKey = (text: unknown) => String(text ?? '').replace(/\s+/g, '');

/**
 * 逐条判 actions 里要写进 memories 的正文。
 * archive 没有正文，不判；空文本交给后面的老校验去拒。
 *
 * 2026-11-06：这一层只**打标记**，不再判生死——
 * 命中的条目进「可疑集」，交给复核 agent 裁决（见 lintRedoOnce）。
 */
function lintActions(actions: ParsedActions): LintFirst {
  const out: LintActions = {
    add: [], archive: Array.isArray(actions?.archive) ? actions.archive as LintItem[] : [], merge: [], update: [],
  };
  const rejected: LintRejected[] = [];
  for (const kind of ['merge', 'update', 'add'] as const) {
    for (const item of Array.isArray(actions?.[kind]) ? actions[kind] as LintItem[] : []) {
      const text = String(item?.text ?? '').trim();
      if (!text) {
        out[kind].push(item);
        continue;
      }
      const result = lintMemoryText(text);
      if (result.ok) out[kind].push(item);
      else rejected.push({ kind, item, text, violations: result.violations });
    }
  }
  return { actions: out, rejected };
}

/** 第一轮通过的 + 第二轮（重写后）通过的，按归一化正文去重；第二轮的版本优先。 */
function mergeLintRounds(first: LintActions, second: LintActions | null): LintActions {
  const out: LintActions = { add: [], archive: [], merge: [], update: [] };
  for (const kind of ['merge', 'update', 'add'] as const) {
    const seen = new Set<string>();
    for (const item of [...(second?.[kind] || []), ...(first?.[kind] || [])]) {
      const key = actionKey(item?.text);
      if (seen.has(key)) continue;
      seen.add(key);
      out[kind].push(item);
    }
  }
  const archived = new Map<string, LintItem>();
  for (const item of [...(first?.archive || []), ...(second?.archive || [])]) {
    const key = String(item?.id ?? JSON.stringify(item));
    if (!archived.has(key)) archived.set(key, item);
  }
  out.archive = [...archived.values()];
  return out;
}

/**
 * 三段式的第②③段（ingest 与 organize 两处共用，2026-11-06）：
 *   ① 代码 lint 已经打完标记（first.rejected 就是可疑集）；
 *   ② 复核 agent 裁决：一个批次**只发一次**调用，多条可疑合并问；
 *      keep = 误杀 → 打上 __lintKept 原样放行，写库时带 __lintChecked 过 store 最后一道闸；
 *   ③ redo = 真有问题 → 带着 violations + 复核理由退回产出它的 agent **重做一次**；
 *      重做后再过一遍代码 lint：通过就入库，仍不过才落 discards(LINT_REJECT)。
 *
 * **重做只给一次机会，不许循环**：这里没有 while，也没有第二次重做请求。
 * 复核挂了（抛错/超时/非 JSON）→ 全部按 redo 走旧行为，条目不丢，日志打 [lint-review] fallback。
 *
 * @param opts.agent 复核提示词里的产出方名字
 * @param opts.action 落 discards 时的 action 标签
 * @param opts.actions 第一轮解析出来的 actions
 * @param opts.ask 带着 feedback 再问一次原 agent
 * @param opts.trace 日志（organize 那条走的是同一个 log）
 */
async function lintRedoOnce({ agent, action, actions, ask, trace = (line: string) => log(cfg, line) }: LintRedoOpts): Promise<LintRedone> {
  const clean: LintRedone = { actions, lintRejected: [], lintRetried: 0, lintReviewed: 0, lintReviewFallback: false };
  const first = lintActions(actions);
  if (!first.rejected.length) return clean;

  const suspects = first.rejected.map((entry, index) => ({ ...entry, index }));
  trace(
    `[lint] ${action}：${suspects.length} 条正文命中代码标记（${
      [...new Set(suspects.flatMap((r) => r.violations.map((v) => v.code)))].join(',')}），交复核 agent 裁决`,
  );
  const judged = await judgeSuspected({ cfg, llm: llmOf(ctx, { site: 'lint:review', log: (line) => log(cfg, line), cfg }) as unknown as ReviewLlm, agent, suspects, logger: trace });

  // keep = 误杀：原样放行，带 __lintKept 让写库那一步带 __lintChecked 过 store 的闸
  const kept = first.actions;
  for (const entry of judged.keep) {
    const bucket = kept[entry.kind as ActionKind];
    if (Array.isArray(bucket)) bucket.push({ ...entry.item, ...lintKeptMarker(entry) });
  }
  const reviewed = { ...clean, actions: kept, lintReviewed: judged.keep.length, lintReviewFallback: judged.fallback };
  if (!judged.redo.length) {
    trace(`[lint] ${action}：复核放行 ${judged.keep.length} 条（误杀），不用重写`);
    return reviewed;
  }

  // ③ 退回原 agent 重做**一次**（只有这一次，不循环）
  trace(
    `[lint] ${action}：复核判 ${judged.redo.length} 条要重做，退回 ${agent} 重写一次` +
      (judged.fallback ? '（复核降级，走旧逻辑）' : ''),
  );
  let secondRaw = '';
  try {
    secondRaw = await ask(buildLintRedoFeedback(judged.redo));
  } catch (error) {
    trace(`[lint] ${action}：重做调用失败（${String(((error as Error | null) && (error as Error).message) || error).slice(0, 120)}）`);
  }
  const second = secondRaw ? parseActions(secondRaw) : null;
  const secondLint = second ? lintActions(second) : null;
  const stillRejected = mergeReviewRejects(secondLint ? secondLint.rejected : judged.redo, judged.redo);
  const merged = secondLint ? mergeLintRounds(kept, secondLint.actions) : kept;
  recordLintRejects(pendingDb(), stillRejected, { action, log: trace });
  const passed = merged.merge.length + merged.update.length + merged.add.length;
  trace(
    `[lint] ${action}：重做后仍拒 ${stillRejected.length} 条（已记 lint_reject，带复核理由），通过 ${passed} 条`,
  );
  return { ...reviewed, actions: merged, raw: secondRaw, lintRejected: stillRejected, lintRetried: 1 };
}

/**
 * 问模型要 actions，走「代码标记 → 复核 → 退回重做一次」三段。
 * 重做后仍不合格的条目：不入库，落 discards(verdict=LINT_REJECT, source_file=lint_reject)，
 * 原文 + 两轮 violations + 复核理由都留在 discards.text / reason 里。
 */
async function askActionsWithLint({ transcript, existing, effort, tag = '睡觉' }: LintAskOpts): Promise<LintAsked> {
  const raw = await askModel(transcript, existing, effort);
  const actions = parseActions(raw) as LintActions | null;
  // 2026-11-14：删掉「空文本就缩短材料关掉思考重试一次」的固定重试。
  // 空响应就按失败收工，账记在日志里（raw 会带回去），下一轮睡觉再问，不原地循环。
  if (!raw.trim()) {
    log(cfg, `${tag}：模型没吐出文本（空响应），本轮不重试；原始输出长度=0`);
  }
  if (!actions) {
    return { actions: null, raw, lintRejected: [], lintRetried: 0, lintReviewed: 0, lintReviewFallback: false };
  }
  const done = await lintRedoOnce({
    agent: `sleep:${tag}`,
    action: 'sleep',
    actions,
    ask: (feedback) => askModel(transcript, existing, effort, feedback),
  });
  return { ...done, raw: done.raw || raw };
}

  return { lintActions, mergeLintRounds, lintRedoOnce, askActionsWithLint };
}
