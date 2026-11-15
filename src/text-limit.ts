/**
 * 记忆正文长度口径（2026-11-03 立，2026-11-05 改语义）。
 *
 * 起因：记忆库里长条越来越多，一条正文里塞三四件事，recall 命中一条等于抱回一堆噪声。
 * 口径统一到 200 字（MEMORY_TEXT_MAX），但**写库路径再也不许丢、不许截**：
 *   - 超长 → 调用方用 needsSplitText 判一下，交给 src/split-queue.ts 入队
 *     pending_ops(kind='split_item')，由拆分 agent 拆成原子条目；
 *   - 写库前把正文改短这件事，2026-11-05 起彻底不做（首跑 4/9 候选就是这么丢的）。
 *
 * 唯一允许「截断」的地方是**喂模型前的预览**：提示词里的现有记忆副本压短一点省 token，
 * 压的是副本、不是落库正文，用 previewClampText。调用点必须写一句注释说明是预览。
 *
 * 调用点（改语义后）：
 *   1. src/sleep.ts applyActions 的 merge / update / add；
 *   2. src/consolidate.ts 入库前；
 *   3. src/store.ts 的 memory_merge 工具。
 * 三处都只判长度 + 入队，不截不丢。
 */

export const MEMORY_TEXT_MAX = 200;

/** 句子边界：中文句号问号叹号、英文对应符号、省略号、换行。分号不算——那只是半句。 */
const SENT_END = /[。！？!?…\n]/;

/**
 * 这条正文是不是超长、需要交给拆分 agent。
 * @param input 待落库正文
 * @param max 上限，默认 200
 */
export function needsSplitText(input: unknown, max = MEMORY_TEXT_MAX): boolean {
  return String(input ?? '').trim().length > max;
}

/** 预览截断的结果：action 说明切法，original/result 是截前截后长度。 */
export interface PreviewClampResult {
  action: 'ok' | 'truncate' | 'hard';
  text: string;
  original: number;
  result: number;
}

/**
 * 预览截断：**只给「喂模型前」的提示词用，绝不进库**。
 *
 * 写库路径一律不丢不截（见 needsSplitText + split-queue），这里能截是因为截的只是
 * 提示词里的一份副本。优先切在句末标点上，整句保留；找不到句末就硬切到 max，不返回空串。
 *
 * @param input
 * @param max 上限，默认 200
 */
export function previewClampText(input: unknown, max = MEMORY_TEXT_MAX): PreviewClampResult {
  const text = String(input ?? '').trim();
  const original = text.length;
  if (original <= max) return { action: 'ok', text, original, result: original };
  let cut = -1;
  for (let i = max - 1; i >= 0; i -= 1) {
    if (SENT_END.test(text[i])) {
      cut = i + 1;
      break;
    }
  }
  if (cut > 0) {
    const safe = text.slice(0, cut).trim();
    return { action: 'truncate', text: safe, original, result: safe.length };
  }
  const hard = text.slice(0, max);
  return { action: 'hard', text: hard, original, result: hard.length };
}

/**
 * 统一的日志行，超长入队 / 预览截断共用，别各写一套格式。
 * 输出形如：[超长] 入队拆分 id=m_abc 原长=249 结果长=249 首40字=……
 */
export function clampLogLine(
  action: string,
  id: string | number | null | undefined,
  original: number,
  result: number,
  text: unknown,
): string {
  const head = String(text ?? '').replace(/\s+/g, ' ').slice(0, 40);
  return `[超长] ${action} id=${id || 'pending'} 原长=${original} 结果长=${result} 首40字=${head}`;
}
