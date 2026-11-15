/**
 * 模型输出解析的公共件。
 *
 * merge / split / dedupe 三个 agent 原来各抄一份 parseJsonLoose，三份还不一样：
 * merge 版多一个「按括号配平切第一个对象」的候选。合并时取并集，候选顺序保持硬规则——
 * 严格解析 → 配平取第一个对象 → 围栏 → 首尾花括号 → 首尾方括号，别把本来能解析的搞坏。
 */

/**
 * 从第一个 `{` 起按括号配平，切出第一个完整的顶层对象。
 * 模型把同一个对象连写两遍（`{...} {...}`）时，JSON.parse 会报
 * Unexpected non-whitespace character after JSON at position N；切出第一个就能读。
 * 配不平（截断）返回 null，交给后面的候选和失败路径。
 */
function firstBalancedObject(source: string): string | null {
  const start = source.indexOf('{');
  if (start < 0) return null;
  let depth = 0;
  let inString = false;
  let escaped = false;
  for (let i = start; i < source.length; i += 1) {
    const ch = source[i];
    if (inString) {
      if (escaped) escaped = false;
      else if (ch === '\\') escaped = true;
      else if (ch === '"') inString = false;
      continue;
    }
    if (ch === '"') inString = true;
    else if (ch === '{') depth += 1;
    else if (ch === '}') {
      depth -= 1;
      if (depth === 0) return source.slice(start, i + 1);
    }
  }
  return null;
}

/** 模型返回的 JSON 可能是裸 JSON、带 ```json 围栏，或前后混了解释文字。 */
export function parseJsonLoose(raw: unknown): unknown {
  const source = String(raw ?? '').trim();
  if (!source) throw new Error('空字符串');
  const candidates: string[] = [source];
  const balanced = firstBalancedObject(source);
  if (balanced) candidates.push(balanced);
  const fenced = source.match(/```(?:json)?\s*([\s\S]*?)```/i);
  if (fenced) candidates.push(fenced[1].trim());
  const first = source.indexOf('{');
  const last = source.lastIndexOf('}');
  if (first >= 0 && last > first) candidates.push(source.slice(first, last + 1));
  const firstBracket = source.indexOf('[');
  const lastBracket = source.lastIndexOf(']');
  if (firstBracket >= 0 && lastBracket > firstBracket) candidates.push(source.slice(firstBracket, lastBracket + 1));
  let lastError: { message?: string } | null = null;
  for (const candidate of candidates) {
    try {
      return JSON.parse(candidate);
    } catch (error) {
      lastError = error as { message?: string };
    }
  }
  throw new Error(`不是合法 JSON：${lastError?.message || 'parse failed'}`);
}
