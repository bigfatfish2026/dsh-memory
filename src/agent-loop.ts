/**
 * 模型工具循环的「活性」守卫。
 *
 * 维护者要求不给模型调用设次数上限（toolRounds / maxToolCalls 这类全删）。删掉次数闸之后，
 * 唯一要防的是模型原地转圈、永远不收敛出 JSON；判断依据换成「有没有进展」，不是「调了几次」：
 *
 *   ① 同一个工具 + 同一组参数**连续**出现 3 次 → 中断这一次调用。工具在重复转圈，
 *      再执行第 4 次也不会带来新信息，纯烧钱。
 *   ② 连续 20 次工具调用的返回内容**全量和之前重复**（没有一条新信息进 context）→
 *      「无进展中断」。判的是进展：只要有一次返回是新的，计数就清零。
 *
 * 两个数都是「防循环」阈值（维护者 2026-11-14 明确认可），不是调用预算：
 * 正常使用工具时永远不会碰到，碰到了说明这次调用已经卡死。
 */

/** 同工具同参数连续出现几次算原地转圈。 */
export const REPEAT_STREAK = 3;
/** 连续多少次「返回内容全是旧的」算无进展。 */
export const NO_PROGRESS_STREAK = 20;

/** 一次工具调用的形状：调用前给 name/args，调用后补 result。字段都按 unknown 收，进来先归一化。 */
export interface ToolLoopCall {
  name?: unknown;
  args?: unknown;
  result?: unknown;
}

/** 守卫对一次调用的裁决。 */
export interface ToolLoopDecision {
  stop: boolean;
  reason?: 'repeat' | 'no_progress';
  detail?: string;
}

/** 守卫计数：calls 总调用数，repeats 撞「同参重复」次数，noProgress 当前连续无进展计数。 */
export interface ToolLoopStats {
  calls: number;
  repeats: number;
  noProgress: number;
}

/** 守卫对外面：record 记一次调用，stats 取计数。 */
export interface ToolLoopGuard {
  record(call?: ToolLoopCall): ToolLoopDecision;
  stats(): ToolLoopStats;
}

/** 建守卫的入参。 */
export interface ToolLoopGuardOptions {
  log?: (line: string) => void;
  label?: string;
}

/**
 * 建一个守卫。每个 agent 循环（一次模型会话）建一个。
 */
export function createToolLoopGuard({ log, label = 'agent' }: ToolLoopGuardOptions = {}): ToolLoopGuard {
  const say = typeof log === 'function' ? log : () => {};
  let prevKey = '';
  let repeatStreak = 0;
  const seenResults = new Set<string>();
  let noProgressStreak = 0;
  let calls = 0;
  let repeats = 0;

  const keyOf = (call: ToolLoopCall) => {
    const args = typeof call?.args === 'string' ? call.args : JSON.stringify(call?.args ?? {});
    return `${String(call?.name ?? '')}\u0000${args}`;
  };

  return {
    /**
     * 记一次工具调用（调用前或调用后都可以，result 给了就算一次「进 context」）。
     */
    record(call: ToolLoopCall = {}): ToolLoopDecision {
      calls += 1;
      const key = keyOf(call);
      const result = call?.result === undefined || call?.result === null ? '' : String(call.result);

      // ① 同工具同参数连续重复
      if (key === prevKey) {
        repeatStreak += 1;
      } else {
        prevKey = key;
        repeatStreak = 1;
      }
      if (repeatStreak >= REPEAT_STREAK) {
        repeats += 1;
        const detail = `${String(call?.name ?? '?')} 连续 ${repeatStreak} 次同参数`;
        say(`[agent-loop] 中断 reason=repeat ${label} 调用=${calls} detail=${detail}`);
        return { stop: true, reason: 'repeat', detail };
      }

      // ② 连续多次没有任何新信息
      if (result) {
        if (seenResults.has(result)) {
          noProgressStreak += 1;
        } else {
          seenResults.add(result);
          noProgressStreak = 0;
        }
        if (noProgressStreak >= NO_PROGRESS_STREAK) {
          const detail = `连续 ${noProgressStreak} 次返回内容全是旧的`;
          say(`[agent-loop] 中断 reason=no_progress ${label} 调用=${calls} detail=${detail}`);
          return { stop: true, reason: 'no_progress', detail };
        }
      }
      return { stop: false };
    },

    stats(): ToolLoopStats {
      return { calls, repeats, noProgress: noProgressStreak };
    },
  };
}

export default { createToolLoopGuard, REPEAT_STREAK, NO_PROGRESS_STREAK };
