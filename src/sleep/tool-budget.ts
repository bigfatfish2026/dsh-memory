/**
 * 工具循环的累计上下文预算 + 定向查证提示（2026-11-23）。
 *
 * 线上事实：12:30 那轮 27 个文件发了 202 次请求（平均 7.5 次/文件），每个文件都带满
 * 300 条候选；工具循环里每一轮都把前几轮的工具结果原文重新发一遍，229.2 万未缓存
 * 输入+输出 token 里有一大块是这批重复的旧工具结果。
 *
 * 这里只做两件事，全是本地计算，不额外调模型：
 *   ① compactToolHistory：同一次 askModel（= 同一个文件）的往返里，历史工具结果累计
 *      token 超过阈值后，旧结果换成「出处 + 相关片段」。最近 keepRecentRounds 轮保留原文，
 *      最新一条工具结果永远保留原文；已经压过的条目不重算（幂等），这样上一轮发过的
 *      消息不会被反复改写，前缀缓存只在「这一轮新压掉的那一条」处断开。
 *      只改 role='tool' 的消息，候选 listing（user 首条）与顺序一个字不碰。
 *   ② buildToolCheckHint：定向查证的两条 user 提示——先只查最可疑的 1-3 条，
 *      轮次用完后只有会改变归因/状态的补查才做。开关关掉就不注入，
 *      消息体与加这层之前逐字节一致。
 */
import { estimateTokens } from '../token-window.js';
import { clip } from '../shared/read-tools.js';
import type { ChatMessage } from './tool-calls.js';

/** 压缩标记：压缩过的工具结果以此开头；重入时据此跳过（幂等）。 */
export const TOOL_BUDGET_MARK = '〔压缩〕';
/** 累计工具片段预算的默认值（token）。 */
export const TOOL_BUDGET_DEFAULT_TOKENS = 4000;
/** 压缩后每条工具结果保留的片段字符数。 */
export const TOOL_BUDGET_DEFAULT_EXCERPT_CHARS = 240;
/** 默认保留最近几轮工具结果原文。 */
export const TOOL_BUDGET_DEFAULT_KEEP_ROUNDS = 1;
/** 默认最多保留多少条相关片段。 */
export const TOOL_BUDGET_DEFAULT_MAX_LINES = 8;
/** 定向查证默认先做几轮。 */
export const TOOL_CHECK_DEFAULT_ROUNDS = 3;
/** 定向查证每轮最多查几条。 */
export const TOOL_CHECK_DEFAULT_MAX_CALLS = 3;

/** 累计工具上下文预算（schema 在 src/index.ts，默认值只在那里和这里各写一份口径）。 */
export interface ToolBudgetCfg {
  enabled: boolean;
  /** 累计工具片段 token 上限；<=0 视为不压缩（enabled=true 时也不动手）。 */
  tokens: number;
  /** 最近几轮工具结果保留原文。 */
  keepRecentRounds: number;
  /** 压缩后每条保留的片段字符数。 */
  excerptChars: number;
  /** 压缩后每条最多保留几行片段。 */
  maxExcerptLines: number;
}

/** 定向查证开关。 */
export interface ToolCheckCfg {
  enabled: boolean;
  /** 前几轮做定向查证；0 = 不注入任何提示。 */
  rounds: number;
  /** 每轮最多查几条（写进提示词，不硬拦调用）。 */
  maxCallsPerRound: number;
}

/** 一次压缩的统计（日志与测试用）。 */
export interface ToolBudgetStats {
  /** 本次看到的工具结果轮数。 */
  rounds: number;
  /** 消息里工具结果总条数。 */
  tools: number;
  /** 本次新压缩了几条。 */
  compacted: number;
  /** 仍然是原文的条数（含最近轮与已压缩过的）。 */
  kept: number;
  /** 压缩前工具结果估算 token。 */
  beforeTokens: number;
  /** 压缩后工具结果估算 token。 */
  afterTokens: number;
  /** 本次生效的上限。 */
  budget: number;
}

/** 一条 tool-call 的出处信息：压缩后写进片段头，模型仍能看出这是哪次查询的结果。 */
interface CallMeta {
  name: string;
  args: string;
}

function intOr(value: unknown, fallback: number, min = 0, max = Number.MAX_SAFE_INTEGER): number {
  const n = Math.floor(Number(value));
  if (!Number.isFinite(n)) return fallback;
  return Math.min(Math.max(n, min), max);
}

function boolOn(value: unknown, fallback = true): boolean {
  // 只认明确 false；字符串 'false' / 0 之类不偷偷改行为
  if (value === false) return false;
  if (value === true) return true;
  return fallback;
}

/** 归一 sleepToolBudget 配置；缺省 = 开、4000 token、保留最近 1 轮。 */
export function toolBudgetCfgOf(raw: unknown): ToolBudgetCfg {
  const o = raw && typeof raw === 'object' ? raw as Record<string, unknown> : {};
  return {
    enabled: boolOn(o.enabled, true),
    tokens: intOr(o.tokens, TOOL_BUDGET_DEFAULT_TOKENS, 0),
    keepRecentRounds: intOr(o.keepRecentRounds, TOOL_BUDGET_DEFAULT_KEEP_ROUNDS, 0, 20),
    excerptChars: intOr(o.excerptChars, TOOL_BUDGET_DEFAULT_EXCERPT_CHARS, 40),
    maxExcerptLines: intOr(o.maxExcerptLines, TOOL_BUDGET_DEFAULT_MAX_LINES, 1, 50),
  };
}

/** 归一 sleepToolCheck 配置；缺省 = 开、前 3 轮、每轮最多 3 条。 */
export function toolCheckCfgOf(raw: unknown): ToolCheckCfg {
  const o = raw && typeof raw === 'object' ? raw as Record<string, unknown> : {};
  return {
    enabled: boolOn(o.enabled, true),
    rounds: intOr(o.rounds, TOOL_CHECK_DEFAULT_ROUNDS, 0, 10),
    maxCallsPerRound: intOr(o.maxCallsPerRound, TOOL_CHECK_DEFAULT_MAX_CALLS, 1, 10),
  };
}

/**
 * 压缩时用来判断「相关」的词：本批候选的实体名与 id。
 * 只取前几十个，避免 scoring 变成全表扫描。
 */
export function focusTermsOf(
  existing: Array<{ id?: unknown; entities?: unknown; tags?: unknown }> = [],
  limit = 40,
): string[] {
  const out: string[] = [];
  const seen = new Set<string>();
  const add = (value: unknown): void => {
    const s = String(value ?? '').trim();
    if (s.length < 2 || seen.has(s)) return;
    seen.add(s);
    out.push(s);
  };
  for (const m of existing.slice(0, 80)) {
    const ents = Array.isArray(m?.entities) ? m.entities : [];
    for (const e of ents) add(e);
  }
  for (const m of existing.slice(0, 80)) add(m?.id);
  return out.slice(0, limit);
}

/** 相关行判据：否定结论、具体路径、票号、条目 id、日期、带单位的数字。 */
const NEGATIVE_RE = /装不上|没装|没有|找不到|不存在|删了|删掉|已删|不能用|不行|未核实|失效|过时/i;
const PATH_RE = /\/[\w.-]+\//;
const TICKET_RE = /\bt-[a-z0-9]{6,}\b/i;
const ID_RE = /\bid=[\w-]+/i;
const DATE_RE = /\b20\d{2}-\d{2}-\d{2}\b/;
const NUMBER_RE = /\d+(?:\.\d+)?\s*(?:ms|s|秒|分钟|万|条|个|%|GB|MB|KB|度|℃|token)/i;

function scoreLine(line: string, focus: string[]): number {
  let score = 0;
  const lower = line.toLowerCase();
  for (const f of focus) {
    if (f.length >= 2 && lower.includes(f.toLowerCase())) {
      score += 6;
      break;
    }
  }
  if (PATH_RE.test(line)) score += 3;
  if (TICKET_RE.test(line)) score += 3;
  if (ID_RE.test(line)) score += 3;
  if (NEGATIVE_RE.test(line)) score += 2;
  if (DATE_RE.test(line)) score += 1;
  if (NUMBER_RE.test(line)) score += 1;
  return score;
}

/**
 * 一行里「为什么被选中」的位置（2026-11-23 t-mwszo23a-iwxs）：focus 词优先，其次具体路径、
 * 票号、id=、否定结论。返回 -1 表示没有可定位的命中，长度裁剪就退回旧行为（留开头）。
 *
 * 只用于截取窗口，不参与打分——打分的口径仍是 scoreLine。
 */
function hitAtOf(line: string, focus: string[]): number {
  const lower = line.toLowerCase();
  let best = -1;
  const consider = (at: number): void => {
    if (at >= 0 && (best < 0 || at < best)) best = at;
  };
  for (const f of focus) {
    if (f.length < 2) continue;
    consider(lower.indexOf(f.toLowerCase()));
  }
  if (best >= 0) return best;
  for (const re of [PATH_RE, TICKET_RE, ID_RE, NEGATIVE_RE]) {
    const at = re.exec(line)?.index ?? -1;
    consider(at);
  }
  return best;
}

/**
 * 长行按命中位置取前后窗口（片段不再固定取开头）：命中在 chars 之后时窗口跟着挪，
 * 命中的那一词一定留在窗口里；窗口两侧被截断就加省略号。
 *
 * 只在行比 chars 长时动手；短行原样返回。总长度 ≤ chars（两个省略号占 2 字）。
 */
function windowAt(line: string, at: number, chars: number): string {
  const room = Math.max(1, chars - 2); // 两侧省略号各算 1 字
  if (at < 0 || at + 1 <= room) return clip(line, chars);
  const before = Math.min(at, Math.floor(room / 3));
  let from = Math.max(0, at - before);
  let to = Math.min(line.length, from + room);
  if (to - from < room) from = Math.max(0, to - room);
  const head = from > 0 ? '…' : '';
  const tail = to < line.length ? '…' : '';
  return `${head}${line.slice(from, to)}${tail}`;
}

/**
 * 从一段工具结果里挑相关行：打分从高到低、按原顺序输出，直到字符或行数上限。
 * 纯本地，稳定可复现（同样的输入永远同样的输出，缓存不受抖动影响）。
 *
 * 长单行（整段没有换行）以前只留开头 chars 字，靠末尾内容得分最高的那一行等于丢掉关键修订；
 * 现在按命中位置取前后窗口（2026-11-23 t-mwszo23a-iwxs），token 预算逻辑不在这里改。
 */
export function pickRelevantLines(
  text: unknown,
  focus: string[] = [],
  chars = TOOL_BUDGET_DEFAULT_EXCERPT_CHARS,
  maxLines = TOOL_BUDGET_DEFAULT_MAX_LINES,
): string {
  const lines = String(text ?? '')
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean);
  if (!lines.length) return '（空结果）';
  const scored = lines.map((line, index) => ({ line, index, score: scoreLine(line, focus) }));
  const ordered = [...scored].sort((a, b) => b.score - a.score || a.index - b.index);
  const picked: typeof scored = [];
  let used = 0;
  for (const item of ordered) {
    if (picked.length >= maxLines) break;
    if (picked.length && used + item.line.length > chars) break;
    picked.push(item);
    used += item.line.length + 1;
    if (used >= chars) break;
  }
  picked.sort((a, b) => a.index - b.index);
  const parts = picked.map((x) => windowAt(x.line, hitAtOf(x.line, focus), chars));
  // 单行已经按窗口裁过（没命中时 clip 自带「截断」后缀），别再裁第二刀。
  if (parts.length === 1) return parts[0];
  const out = parts.join('\n');
  return out.length > chars ? clip(out, chars) : out;
}

function toolTextOf(msg: ChatMessage | undefined): string {
  if (!msg || !Array.isArray(msg.content)) return '';
  return msg.content
    .map((part) => (part?.type === 'text' ? String(part.text ?? '') : ''))
    .filter(Boolean)
    .join('\n');
}

function setToolText(msg: ChatMessage, text: string): void {
  msg.content = [{ type: 'text', text }];
}

/** toolCallId → 工具名 + 参数摘要；压缩后仍能看出出处。 */
function callMetaOf(msgs: ChatMessage[]): Map<string, CallMeta> {
  const map = new Map<string, CallMeta>();
  for (const m of msgs) {
    if (m?.role !== 'assistant' || !Array.isArray(m.content)) continue;
    for (const part of m.content) {
      if (part?.type !== 'tool-call') continue;
      const id = part.id ? String(part.id) : '';
      if (!id) continue;
      let args = '{}';
      try {
        args = JSON.stringify(part.arguments ?? {}).slice(0, 80);
      } catch {
        args = '{}';
      }
      map.set(id, { name: String(part.name ?? '?'), args });
    }
  }
  return map;
}

/** 连续的一串 role='tool' 消息算一轮（中间插进来的 user 提示会自然分轮）。 */
function toolRoundsOf(msgs: ChatMessage[]): number[][] {
  const rounds: number[][] = [];
  let cur: number[] = [];
  msgs.forEach((m, index) => {
    if (m?.role === 'tool') {
      cur.push(index);
      return;
    }
    if (cur.length) {
      rounds.push(cur);
      cur = [];
    }
  });
  if (cur.length) rounds.push(cur);
  return rounds;
}

/** 一条工具结果的压缩形态：出处头 + 相关片段。 */
function excerptOf(
  text: string,
  meta: CallMeta,
  focus: string[],
  cfg: ToolBudgetCfg,
): string {
  const args = meta.args && meta.args !== '{}' ? ` args=${meta.args}` : '';
  const header = `${TOOL_BUDGET_MARK} tool=${meta.name}${args}｜原文 ${text.length} 字，超累计预算只留相关片段：`;
  const body = pickRelevantLines(text, focus, cfg.excerptChars, cfg.maxExcerptLines);
  return `${header}\n${body}`;
}

/**
 * 按累计预算压缩历史工具结果。就地改 msgs，返回统计。
 *
 * 规则：
 *   - enabled=false 或 tokens<=0：一条都不动（回退口子，消息体与加这层之前一致）；
 *   - 最近 keepRecentRounds 轮、以及最后一条工具结果保留原文；
 *   - 从最旧的开始压，压到累计 token ≤ tokens 为止；
 *   - 已带 TOOL_BUDGET_MARK 的条目不重算（幂等）；
 *   - 只改 role='tool' 的 content，其余消息与顺序不动。
 */
export function compactToolHistory(
  msgs: ChatMessage[] = [],
  { budget, focus = [] }: { budget: ToolBudgetCfg; focus?: string[] },
): ToolBudgetStats {
  const list = Array.isArray(msgs) ? msgs : [];
  const rounds = toolRoundsOf(list);
  const indexes = rounds.flat();
  const metaOf = callMetaOf(list);
  const cfg = budget;

  let beforeTokens = 0;
  for (const index of indexes) beforeTokens += estimateTokens(toolTextOf(list[index]));
  const stats: ToolBudgetStats = {
    rounds: rounds.length,
    tools: indexes.length,
    compacted: 0,
    kept: indexes.length,
    beforeTokens,
    afterTokens: beforeTokens,
    budget: cfg?.tokens ?? 0,
  };
  if (!cfg?.enabled || cfg.tokens <= 0 || !indexes.length) return stats;

  // 最近 N 轮 + 最新一条：原文保留，模型这几轮的结论还靠它们
  const protect = new Set<number>();
  const keep = Math.min(cfg.keepRecentRounds, rounds.length);
  for (const round of rounds.slice(rounds.length - keep)) for (const i of round) protect.add(i);
  const last = indexes[indexes.length - 1];
  if (last !== undefined) protect.add(last);

  let total = beforeTokens;
  let compactedAlready = 0;
  for (const index of indexes) {
    if (total <= cfg.tokens) break;
    if (protect.has(index)) continue;
    const msg = list[index];
    const text = toolTextOf(msg);
    if (!text) continue;
    if (text.startsWith(TOOL_BUDGET_MARK)) {
      compactedAlready += 1;
      continue;
    }
    const meta = metaOf.get(String(msg?.toolCallId ?? '')) || { name: '?', args: '{}' };
    const next = excerptOf(text, meta, focus, cfg);
    const before = estimateTokens(text);
    const after = estimateTokens(next);
    if (after >= before) continue; // 压不动就不动，别把预算算反
    setToolText(msg, next);
    total += after - before;
    stats.compacted += 1;
  }
  stats.afterTokens = total;
  stats.kept = Math.max(0, indexes.length - compactedAlready - stats.compacted);
  return stats;
}

/**
 * 定向查证的提示文本。
 * start：第一轮工具结果之后注入一次，要求先查最可疑的 1-3 条；
 * wrap：定向轮次用完后注入一次，之后只做会改变归因/状态的补查。
 */
export function buildToolCheckHint(phase: 'start' | 'wrap', cfg: ToolCheckCfg): string {
  if (phase === 'start') {
    return `定向查证：先在 ${cfg.rounds} 轮里只查最可疑的 1-${cfg.maxCallsPerRound} 条`
      + '（否定结论、具体路径/服务/版本、状态类条目），一轮问一个问题，别一次把所有候选都翻一遍。'
      + '只有确认会改变归因（该改/该并哪一条）或状态（active/archived）的，才在下一轮继续补查。';
  }
  return `定向查证轮次已到（${cfg.rounds} 轮）：接下来只有会改变归因或状态的补查才值得做，`
    + '查过的别重复查；按已有结果直接输出 JSON。';
}

export default {
  TOOL_BUDGET_MARK,
  TOOL_BUDGET_DEFAULT_TOKENS,
  TOOL_BUDGET_DEFAULT_EXCERPT_CHARS,
  TOOL_BUDGET_DEFAULT_KEEP_ROUNDS,
  TOOL_CHECK_DEFAULT_ROUNDS,
  TOOL_CHECK_DEFAULT_MAX_CALLS,
  toolBudgetCfgOf,
  toolCheckCfgOf,
  focusTermsOf,
  pickRelevantLines,
  compactToolHistory,
  buildToolCheckHint,
};
