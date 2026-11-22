/**
 * SplitAgent（2026-11-05，2026-11-15 换法）。
 *
 * 拆一条超长记忆。2026-11-15 起**模型只给切分点，正文由代码照原文切**：
 * 模型输出 {"cuts":["原文里唯一可定位的 8-20 字", ...]} 或 {"offsets":[数字偏移]}，
 * 代码 indexOf 还原偏移、在原文上 slice。模型一个字都不许写。
 *
 * 为什么换：旧协议让模型每段重写正文，12:30 那轮 curate-split 48 条只成 18、
 * 25 条 inflate，输出比原文长 37%（均值），原文越短越吃亏；1.05 红线只是收紧了，
 * 治不了「模型每段都在重写」这个根。切分点协议下 charsAfter 恒等于 charsBefore。
 *
 * 硬校验（validateParts）：
 *   - 每个定位串必须在上一个切点之后 indexOf 得到，偏移严格递增；
 *   - 切出来的原文片段按顺序拼回去必须与原文逐字相等（先 raw join === source，
 *     再比去空白后的字符序列，两道网），不等判 splice_mismatch；
 *   - 段长沿用 200 字上限，超了**由代码**按句号/换行边界再切，切不动（无句读）就整段保留；
 *   - 切不出来 / 锚串找不到 → 判失败，带 feedback 重问**一次**，仍不行落
 *     pending_ops(kind='split_item', attempts+1)，原文一个字不截、整条不丢。
 *
 * 双通道结构化输出（submit_parts 工具流 + 文本 JSON 回退），都坏返回 undefined，
 * 由上层落 pending_ops。设计见 docs/sleep-pipeline-redesign.md §2.2。
 */
import { normalizeText } from '../write-gate.js';
import { parseJsonLoose } from '../shared/json.js';
import { enqueue, markOp } from '../ingest-index.js';
import { maxTokensOption } from '../llm-params.js';
import { lintItems } from '../text-lint.js';
import { SELF_CONTAINED_RULES, SPLIT_RULES } from '../lint-prompt.js';
import { recordLintRejects } from '../lint-reject.js';
import {
  judgeSuspected, buildLintRedoFeedback, mergeReviewRejects, lintKeptMarker,
} from '../lint-review.js';
import type { DatabaseSync } from 'node:sqlite';
import type { JudgeResult, ReviewItem, ReviewOpts } from '../lint-review.js';
import type { LlmServiceLike } from '../llm-service.js';
import type { Row } from '../store/types.js';

/** 切分方案的读取面：模型可能只给 cuts 或只给 offsets。 */
export interface SplitCutsInput {
  cuts?: unknown;
  offsets?: unknown;
  unavoidable?: unknown;
  reason?: unknown;
}

/** 收窄后的切分方案。 */
export interface SplitCuts {
  cuts: string[];
  offsets: number[];
  unavoidable: boolean;
  reason: string;
}

/** 定位结果：成功给 offsets，失败给 reason/hint（外加定位失败的位置）。 */
export interface SplitOffsetsResult {
  ok: boolean;
  offsets?: number[];
  reason?: string;
  index?: number;
  anchor?: string;
  hint?: string;
}

/** 一段拆出来的正文（永远逐字来自原文）。 */
export interface SplitPart {
  text: string;
  evidence: string;
  keptFacts: unknown[];
  /** 在原文（trim 后）里的区间 [start,end)；validateParts 之后必有。 */
  start?: number;
  end?: number;
  [key: string]: unknown;
}

/** 段级自足性信号（2026-11-20 P0 阶段一）：只记不拒，code 固定四种。 */
export interface SplitSignal {
  code: 'split_delimiter' | 'split_deictic' | 'split_premise' | 'split_process';
  /** 命中的段下标（parts 里的位置）。 */
  index: number;
  /** 给人看的一行：判据 + 段首 30 字。 */
  hint: string;
}

/** validateParts 的结果：通过时带段与字数账，失败时带原因。 */
export interface ValidatePartsResult {
  ok: boolean;
  reason?: string;
  hint?: string;
  parts?: SplitPart[];
  offsets?: number[];
  ratio?: number;
  charsBefore?: number;
  charsAfter?: number;
  longKept?: number;
  unavoidable?: boolean;
  lostFacts?: string[];
  /** 段级信号：阶段一只记录，ok 不受影响；signalsStrict 打开时才会拒。 */
  signals?: SplitSignal[];
}

/** 模型流的一块：只认下面这些字段。 */
interface SplitStreamChunk {
  type?: string;
  text?: string;
  index?: number;
  id?: string;
  name?: string;
  argumentsDelta?: string;
  arguments?: unknown;
  reason?: { kind?: string; failure?: unknown };
}

/** buildSplitPrompt 的入参。 */
interface BuildSplitPromptInput {
  sourceId?: string;
  type?: string;
  ts?: string;
  text?: unknown;
  feedback?: string;
}

/** askParts 的入参。 */
interface AskPartsArgs {
  llm?: LlmServiceLike | null;
  provider?: string;
  model?: string;
  sourceId: string;
  type: string;
  ts: string;
  text: string;
  feedback: string;
  log?: ((line: string) => void) | null;
  maxTokens?: number;
}

/** 一次 lint 复核重切之间共享的账。 */
interface LintState {
  retried: number;
  lintRejected: number;
}

/** applyLintReview 的入参。 */
interface ApplyLintReviewArgs {
  firstParts: SplitPart[];
  source: string;
  sourceId: string;
  type: string;
  ts: string;
  round: number;
  llm?: LlmServiceLike | null;
  provider?: string;
  model?: string;
  maxTokens: number;
  db: DatabaseSync | null;
  maxChars: number;
  log?: ((line: string) => void) | null;
  lintCfg: ReviewOpts['cfg'];
  textLint: boolean;
  state: LintState;
}

/** redoAfterLint 的入参。 */
interface RedoAfterLintArgs {
  judged: JudgeResult;
  source: string;
  sourceId: string;
  type: string;
  ts: string;
  llm?: LlmServiceLike | null;
  provider?: string;
  model?: string;
  maxTokens: number;
  db: DatabaseSync | null;
  maxChars: number;
  log?: ((line: string) => void) | null;
  keepParts: SplitPart[];
  rejectAll: (entries: ReviewItem[]) => void;
  state: LintState;
}

/** recordSplitFailure 的入参。 */
export interface RecordSplitFailureOptions {
  sourceId: string;
  text?: string;
  type?: string;
  reason?: string;
  attempt?: number;
  log?: ((line: string) => void) | null;
  now?: number;
}

/** split() 的入参。 */
export interface SplitOptions {
  sourceId?: string;
  text?: string;
  type?: string;
  ts?: string;
  llm?: LlmServiceLike | null;
  provider?: string;
  model?: string;
  db?: DatabaseSync | null;
  maxChars?: number;
  attempt?: number;
  log?: ((line: string) => void) | null;
  maxTokens?: number;
  textLint?: boolean;
  lintReview?: boolean;
  lintReviewTimeoutMs?: number;
  /** 阶段二开关（pipeline.splitSignalsStrict，默认 false）：打开才把段级信号当硬拒。 */
  signalsStrict?: boolean;
}

/** split() 的产物：通过校验的段与字数账。 */
export interface SplitResult {
  parts: SplitPart[];
  lostFacts?: unknown[];
  unavoidable?: boolean;
  reason?: string;
  source?: 'tool' | 'text';
  rounds?: number;
  ratio?: number;
  charsBefore?: number;
  charsAfter?: number;
  longKept?: number;
  /** 最终布局的切点（parts[i+1].start），二次切分与 lint 重做之后的位置。 */
  cuts?: number[];
  lintRejected?: number;
  /** 被正文 lint 剔掉、不再入库的段（覆盖校验用）。 */
  lintDiscards?: SplitPart[];
  /** 被判重丢掉、不再入库的段（覆盖校验用）。 */
  dedupeDiscards?: SplitPart[];
  /** 段级自足性信号（阶段一只记，供回放统计；signalsStrict 打开时命中即判废）。 */
  signals?: SplitSignal[];
}

export const SPLIT_SYSTEM = `你是记忆切分员。给你一条超长记忆，你只给切分点，正文由程序照原文切。

硬规矩
1. 只给切分点，不许改写、不许扩写、不许补字、不许输出任何正文。程序会按你的切分点从原文
   slice，你写的字除了定位串以外一个都用不上。
2. cuts 每项是原文里连续 8-20 字，必须在上一个切点之后唯一能找到（逐字抄，别改标点）。
3. 第一段从原文开头算，不用给它的锚；最后一段到原文结尾，不用给；正常拆分时 cuts 至少一项。
4. 别切太碎：一段尽量 30~200 字。切粗了程序会按句号/换行再切，不算错；切碎了会退拆。
5. 每段的边界只许落在句号、换行之后；不许落在分号之后，不许插在句子中间；不许切在引号、括号、代码围栏内部。
6. 每段单独拿出来，必须能看出对象和完整的动作或结论。否定、条件和结论要留在同一段。需要补字、借用上一段主语或跨段补括号时，换切点或把相邻段并起来。仍然拆不开就输出 {"cuts":[],"unavoidable":true,"reason":"一句话说明原因"}。空 cuts 不能配 unavoidable:false。
7. 每段不许以这些词开头：这、该、它、此、那、上述、上面、这份、本文件、该目录、这个仓库、这个插件；要写成本段真正的对象。
8. 纯过程性状态（本轮、本轮工具不可用、这次失败）不是记忆，不要切出来；整条都是这种就输出 unavoidable=true 并写明理由。
9. 只输出 JSON。

${SELF_CONTAINED_RULES}

${SPLIT_RULES}`;

export const SPLIT_USER_TEMPLATE = `原条目 {id}（{type}，{ts}）：{text}

输出（只给切分点，不给正文）
{"cuts":["原文里第二段开头的 8-20 字","原文里第三段开头的 8-20 字"],"unavoidable":false,"reason":""}

可拆：两段各有对象和结论，切点在句号之后 → {"cuts":["第二段开头的原文 8-20 字"],"unavoidable":false,"reason":""}
不可拆：否定、条件或结论被切断就读不懂，换切点也补不回来 → {"cuts":[],"unavoidable":true,"reason":"条件与结论不可分"}`;

/** 工具通道的 schema：顶层 {cuts,offsets,unavoidable,reason}，字段固定，别让模型自由发挥。 */
export const SUBMIT_PARTS_TOOL = {
  name: 'submit_parts',
  description: '提交这一条记忆的切分方案。只调用这个工具，只给切分点，绝不输出正文。',
  parameters: {
    type: 'object',
    additionalProperties: false,
    properties: {
      cuts: {
        type: 'array',
        description: '从第二段开始的每段开头定位串（原文里连续 8-20 字，必须能在上一切点之后唯一找到）；不许给正文',
        items: { type: 'string' },
      },
      offsets: {
        type: 'array',
        description: '二选一：切点字符偏移（升序整数，落在 1..原文长度-1，不含 0 和末尾）',
        items: { type: 'integer' },
      },
      unavoidable: {
        type: 'boolean',
        description: '200 字内确实装不下时置 true，不要硬切；会落 pending 交人看',
      },
      reason: { type: 'string', description: 'unavoidable 的理由' },
    },
    required: ['cuts'],
  },
};

export const PART_TEXT_MAX = 200;
/** 拼回校验的等值线：切分点协议下 charsAfter 必须等于 charsBefore，多一个字都算失败。
 *  2026-11-15 之前这里叫「扩写红线」= 1.05，治不了模型重写正文；换法后恒 1。 */
export const INFLATE_RATIO = 1;

/** 切长段时的硬边界：优先句末，其次分句，最后一个都没有就整段保留。 */
const SENTENCE_MARKS = ['。', '！', '？', '；', '\n', '…'];
const SOFT_MARKS = ['，', '、', '：', ',', ';', ' '];


function fail(reason: string, extra: Record<string, unknown> = {}): ValidatePartsResult {
  return { ok: false, reason, ...extra };
}

/** 段首指代词表（split 段级口径；不动 SELF_CONTAINED_RULES 的措辞）。 */
export const SPLIT_DEICTIC_OPENERS = [
  '这', '该', '它', '此', '那', '上述', '上面', '这份', '本文件', '该目录', '这个仓库', '这个插件',
];

/** 缺前提的结论式段首：只有结论没有「因为/由于/…」的段落。 */
const PREMISE_LEADS = ['改法', '做法', '结论', '思路', '原因', '建议'];
/** 前提标记：段内出现任一个就算说明了前提。 */
const PREMISE_MARKS = ['因为', '由于', '前提', '基于', '原文'];
/** 「在…下」形状的限定条件也算前提。 */
const PREMISE_UNDER_RE = /在[^，。；！？\n]{0,24}下/;

/** 过程性状态：本轮/这次 + 用不了/不可用/失败 这类不成记忆的段落。 */
const PROCESS_STATE_RE = /(本轮|这次|这一轮)/;
const PROCESS_STATE_BAD_RE = /(用不了|不可用|失败|无法|不能)/;

/** 每一段开头都截同样的长度进 hint，方便日志与人工判读对齐。 */
const SIGNAL_HEAD = 30;

function headOf(text: string): string {
  return String(text ?? '').trim().slice(0, SIGNAL_HEAD);
}

function countOf(text: string, needle: string): number {
  let n = 0;
  let at = 0;
  while ((at = text.indexOf(needle, at)) !== -1) {
    n += 1;
    at += needle.length;
  }
  return n;
}

const DELIMITER_PAIRS: Array<[string, string]> = [
  ['「', '」'], ['『', '』'], ['“', '”'], ['（', '）'], ['(', ')'],
];

/**
 * 定界符配平（2026-11-20 P0 阶段一信号）：每段内引号、括号成对，代码围栏出现偶数次。
 * 只看段正文，不看 offsets；命中只记 code，不判废。
 */
export function delimiterBalance(parts: Array<{ text: string }>): SplitSignal[] {
  const out: SplitSignal[] = [];
  (Array.isArray(parts) ? parts : []).forEach((part, index) => {
    const text = String(part?.text ?? '');
    const broken: string[] = [];
    for (const [open, close] of DELIMITER_PAIRS) {
      if (countOf(text, open) !== countOf(text, close)) broken.push(`${open}${close}`);
    }
    if (countOf(text, '```') % 2 !== 0) broken.push('```');
    // 直双引号没有配对字符：出现奇数次就算破
    if (countOf(text, '"') % 2 !== 0) broken.push('"');
    if (!broken.length) return;
    out.push({ code: 'split_delimiter', index, hint: `定界符不配平（${broken.join(' ')}）：${headOf(text)}` });
  });
  return out;
}

/** 段首指代：trim 后前 4 字命中词表，就算离开上文读不懂。 */
export function deicticOpeners(parts: Array<{ text: string }>): SplitSignal[] {
  const out: SplitSignal[] = [];
  (Array.isArray(parts) ? parts : []).forEach((part, index) => {
    const text = String(part?.text ?? '');
    const head = text.trim().slice(0, 4);
    const hit = SPLIT_DEICTIC_OPENERS.find((word) => head.startsWith(word));
    if (!hit) return;
    out.push({ code: 'split_deictic', index, hint: `段首指代「${hit}」：${headOf(text)}` });
  });
  return out;
}

/** 缺前提：段首是改法/做法/结论/思路/原因/建议，且段内没有前提标记。 */
export function missingPremise(parts: Array<{ text: string }>): SplitSignal[] {
  const out: SplitSignal[] = [];
  (Array.isArray(parts) ? parts : []).forEach((part, index) => {
    const text = String(part?.text ?? '');
    const head = text.trim().slice(0, 6);
    const lead = PREMISE_LEADS.find((word) => head.startsWith(word));
    if (!lead) return;
    const premised = PREMISE_MARKS.some((mark) => text.includes(mark)) || PREMISE_UNDER_RE.test(text);
    if (premised) return;
    out.push({ code: 'split_premise', index, hint: `段首「${lead}」没有前提：${headOf(text)}` });
  });
  return out;
}

/** 过程性状态：本轮/这次 + 用不了/不可用/失败，这种段不是记忆。 */
export function processState(parts: Array<{ text: string }>): SplitSignal[] {
  const out: SplitSignal[] = [];
  (Array.isArray(parts) ? parts : []).forEach((part, index) => {
    const text = String(part?.text ?? '');
    if (!PROCESS_STATE_RE.test(text) || !PROCESS_STATE_BAD_RE.test(text)) return;
    out.push({ code: 'split_process', index, hint: `过程性状态：${headOf(text)}` });
  });
  return out;
}

/** 四条段级信号一起收：按段下标排序，同一段可命中多条。 */
export function splitSignalsOf(parts: Array<{ text: string }>): SplitSignal[] {
  return [
    ...delimiterBalance(parts),
    ...deicticOpeners(parts),
    ...missingPremise(parts),
    ...processState(parts),
  ].sort((a, b) => a.index - b.index);
}

/** 去掉所有空白后的字符序列：段界空白允许被 trim 掉，字一个都不许丢。 */
function stripSpaces(text: unknown): string {
  return String(text ?? '').replace(/\s+/g, '');
}

/** 收窄到最小形状：{cuts,offsets,unavoidable,reason}。结构坏直接抛错，交给上层当「通道坏」处理。 */
export function normalizeCuts(raw: unknown): SplitCuts {
  const value = typeof raw === 'string' ? parseJsonLoose(raw) as SplitCutsInput | null : raw as SplitCutsInput | null;
  if (!value || typeof value !== 'object') {
    throw new Error(`切分方案不是对象：${String(JSON.stringify(value)).slice(0, 120)}`);
  }
  const cuts = Array.isArray(value?.cuts)
    ? (value.cuts as unknown[])
      .map((entry) => (entry && typeof entry === 'object' ? (entry as { anchor?: unknown }).anchor : entry))
      .map((entry) => String(entry ?? ''))
      .filter(Boolean)
    : [];
  const offsets = Array.isArray(value?.offsets)
    ? (value.offsets as unknown[]).map((entry) => Number(entry)).filter((n) => Number.isInteger(n))
    : [];
  if (!cuts.length && !offsets.length && value?.unavoidable !== true) {
    throw new Error(`切分方案里既没有 cuts 也没有 offsets：${String(JSON.stringify(value)).slice(0, 120)}`);
  }
  return {
    cuts,
    offsets,
    unavoidable: value?.unavoidable === true,
    reason: String(value?.reason ?? '').slice(0, 160),
  };
}

/**
 * 把切分方案还原成原文偏移。cuts 走 indexOf（每次从上一个切点之后找），offsets 直接用。
 * 定位串必须能找到、切点必须严格递增，否则判失败（调用方带 feedback 重问一次）。
 */
export function resolveOffsets(source: unknown, answer: SplitCutsInput | null | undefined): SplitOffsetsResult {
  const text = String(source ?? '');
  const offsets = Array.isArray(answer?.offsets) ? answer.offsets : [];
  const anchors = Array.isArray(answer?.cuts) ? answer.cuts : [];

  if (offsets.length) {
    const out = [];
    for (let i = 0; i < offsets.length; i += 1) {
      const at = Number(offsets[i]);
      if (!Number.isInteger(at) || at <= 0 || at >= text.length) {
        return fail('offset_out_of_range', {
          index: i,
          hint: `第 ${i + 1} 个偏移 ${offsets[i]} 不在 1..${text.length - 1} 里，偏移必须落在原文内部`,
        });
      }
      if (out.length && at <= out[out.length - 1]) {
        return fail('offset_not_advancing', {
          index: i,
          hint: `第 ${i + 1} 个偏移 ${at} 没有比上一个切点 ${out[out.length - 1]} 大，切点必须严格递增`,
        });
      }
      out.push(at);
    }
    return { ok: true, offsets: out };
  }

  const out = [];
  let cursor = 1;
  for (let i = 0; i < anchors.length; i += 1) {
    const anchor = String(anchors[i] ?? '');
    if (!anchor) return fail('anchor_empty', { index: i, hint: `第 ${i + 1} 个定位串是空的` });
    const at = text.indexOf(anchor, cursor);
    if (at < 0) {
      return fail('anchor_not_found', {
        index: i,
        anchor,
        hint: `第 ${i + 1} 个定位串「${anchor.slice(0, 40)}」在上一个切点之后找不到，必须逐字抄原文里的连续 8-20 字，不许改标点`,
      });
    }
    if (at >= text.length - 1) {
      return fail('anchor_at_end', {
        index: i,
        hint: `第 ${i + 1} 个定位串落在原文末尾，切出来是空段；锚要指向下一段的开头`,
      });
    }
    out.push(at);
    cursor = at + 1;
  }
  if (!out.length) return fail('cuts_missing', { hint: '没给任何切分点，至少给一个 cuts 定位串' });
  return { ok: true, offsets: out };
}

/** 按偏移把原文切成区间，返回原样片段（不 trim，保留切点处的空白用于拼回校验）。 */
export function sliceParts(source: unknown, offsets: number[]): string[] {
  const text = String(source ?? '');
  const bounds = [0, ...[...offsets].sort((a, b) => a - b), text.length];
  const parts = [];
  for (let i = 0; i < bounds.length - 1; i += 1) {
    parts.push(text.slice(bounds[i], bounds[i + 1]));
  }
  return parts;
}

/** 拼回硬校验：切出的原样片段按顺序拼回去，必须与原文（去首尾空白）逐字相等。 */
export function spliceEqual(rawParts: unknown, sourceText: unknown): boolean {
  const list = Array.isArray(rawParts) ? rawParts : [];
  return list.join('') === String(sourceText ?? '').trim();
}

/** 在 maxChars 内找最后一个句末/分句边界；找不到返回 -1（这一段切不动，整段保留）。 */
function boundaryOf(text: string, maxChars: number): number {
  const head = text.slice(0, maxChars + 1);
  for (const mark of SENTENCE_MARKS) {
    const at = head.lastIndexOf(mark);
    if (at >= Math.floor(maxChars / 4)) return at + mark.length;
  }
  for (const mark of SOFT_MARKS) {
    const at = head.lastIndexOf(mark);
    if (at >= Math.floor(maxChars / 2)) return at + mark.length;
  }
  return -1;
}

/**
 * 纯代码再把一段切到 maxChars 以内：按句号/换行/分句边界切，不用问模型。
 * 连一个边界都没有的硬长段整段保留（longKept=true），不截断、不许模型补写。
 * @returns {Array<{text:string,longKept?:boolean}>} 原样片段（不 trim）
 */
export function splitLongPart(text: unknown, maxChars = PART_TEXT_MAX): Array<{ text: string; longKept?: boolean }> {
  const limit = Number(maxChars) > 0 ? Number(maxChars) : PART_TEXT_MAX;
  const out = [];
  let rest = String(text ?? '');
  let guard = 0;
  while (rest.length > limit && guard < 200) {
    guard += 1;
    const cut = boundaryOf(rest, limit);
    if (cut <= 0) {
      out.push({ text: rest, longKept: true });
      return out;
    }
    out.push({ text: rest.slice(0, cut) });
    rest = rest.slice(cut);
  }
  if (rest) out.push({ text: rest, longKept: rest.length > limit });
  return out;
}

/**
 * 硬校验一份切分方案，并把它落成真正的段（正文全部由代码 slice 出来）。
 *
 * @param {object} answer normalizeCuts 的结果（cuts / offsets / unavoidable / reason）
 * @param {string} sourceText 原条目正文
 * @param {object} [opts]
 * @param {number} [opts.maxChars=200]
 * @returns {{ok:boolean, reason?:string, hint?:string, parts?:Array, offsets?:number[],
 *            ratio?:number, charsBefore?:number, charsAfter?:number, longKept?:number,
 *            unavoidable?:boolean, lostFacts?:string[]}}
 */
export function validateParts(
  answer: SplitCutsInput | null | undefined,
  sourceText: unknown,
  opts: { maxChars?: number; signalsStrict?: boolean } = {},
): ValidatePartsResult {
  const maxChars = Number(opts.maxChars) > 0 ? Number(opts.maxChars) : PART_TEXT_MAX;
  const source = String(sourceText ?? '').trim();
  const unavoidable = answer?.unavoidable === true;
  const reasonText = String(answer?.reason ?? '').slice(0, 160);

  // 模型自己说装不下：不硬切，交 pending 让人看
  if (unavoidable) {
    return fail('unavoidable', {
      unavoidable: true,
      reason: reasonText || '模型判定 200 字内装不下',
      hint: 'unavoidable=true，不硬切',
    });
  }

  const resolved = resolveOffsets(source, answer);
  if (!resolved.ok) return resolved;

  // 第一道网：切的片段按顺序原样拼回去，必须与原文逐字相等（漏切、重复切、切点错位当场抓）
  const raw = sliceParts(source, resolved.offsets as number[]);
  if (!spliceEqual(raw, source)) {
    return fail('splice_mismatch', {
      hint: '按切分点拼回去和原文对不上，切点有漏/重/错位；重给一份 cuts，只许切分点，不许改写原文',
    });
  }

  // 超长段由代码按句读再切（不问模型）；切不动的整段保留，记 longKept。
  // 二次切分后的每个子片都要带原文区间，切点账按**最终布局**算，不再沿用首轮 offsets。
  const pieces: Array<{ text: string; start: number; end: number; longKept?: boolean }> = [];
  let longKept = 0;
  let at = 0;
  for (const piece of raw) {
    const from = at;
    at += piece.length;
    if (piece.length <= maxChars) {
      pieces.push({ text: piece, start: from, end: at });
      continue;
    }
    const sliced = splitLongPart(piece, maxChars);
    if (sliced.map((sub) => sub.text).join('') !== piece) {
      return fail('splice_mismatch', { hint: '超长段按句读再切后拼不回该段原文，切点错位' });
    }
    let cursor = from;
    for (const sub of sliced) {
      if (sub.longKept) longKept += 1;
      pieces.push({ text: sub.text, start: cursor, end: cursor + sub.text.length, longKept: sub.longKept });
      cursor += sub.text.length;
    }
  }

  // 第二道网：段界空白允许 trim，但字一个都不许丢、不许重复
  const parts: SplitPart[] = [];
  for (const piece of pieces) {
    const text = piece.text.trim();
    if (!text) continue;
    const lead = piece.text.length - piece.text.trimStart().length;
    const start = piece.start + lead;
    parts.push({ text, evidence: text.slice(0, 24), keptFacts: [], start, end: start + text.length });
  }
  const charsBefore = source.length;
  const charsAfter = parts.reduce((sum, part) => sum + part.text.length, 0);
  if (stripSpaces(parts.map((part) => part.text).join('')) !== stripSpaces(source)) {
    return fail('splice_mismatch', {
      charsBefore,
      charsAfter,
      hint: '切出的段落去掉空白后和原文对不上，有字被丢或被重复；重给 cuts，不许补字',
    });
  }
  if (parts.length < 2) {
    return fail('parts_lt_2', {
      hint: '只切出一段，必须至少切两段；锚串要指向第二段的开头',
    });
  }
  // 段级自足性信号（2026-11-20 P0）：阶段一只记录，ok 不受影响；
  // signalsStrict（配置 pipeline.splitSignalsStrict，默认 false）打开时才当硬拒。
  const signals = splitSignalsOf(parts);
  if (opts.signalsStrict === true && signals.length) {
    const first = signals[0];
    return fail(`split_signal:${first.code}`, {
      signals,
      hint: `${first.hint}（splitSignalsStrict 打开，段级信号按失败处理）`,
    });
  }
  return {
    ok: true,
    parts,
    // 切点按最终 parts 的起点算：二次切分出的每个子片都有自己的 [start,end)
    offsets: parts.slice(1).map((part) => Number(part.start)),
    ratio: charsBefore > 0 ? charsAfter / charsBefore : 0,
    charsBefore,
    charsAfter,
    longKept,
    unavoidable: false,
    lostFacts: [],
    signals,
  };
}

/** 逐字模板替换；{id}/{type}/{ts}/{text} 落地，feedback 作为重切提示追加在末尾。 */
export function buildSplitPrompt({ sourceId = '', type = '', ts = '', text = '', feedback = '' }: BuildSplitPromptInput = {}): string {
  const prompt = SPLIT_USER_TEMPLATE
    .replace('{id}', String(sourceId || '未知'))
    .replace('{type}', String(type || 'world'))
    .replace('{ts}', String(ts || ''))
    .replace('{text}', String(text ?? ''));
  if (!feedback) return prompt;
  // 900 字：结构退拆的提示很短，正文 lint 重做要多带「violations + 复核意见」两段，
  // 按条数最多几条也塞得下（原先 200 字会把复核理由切掉一半）。
  return `${prompt}\n\n上一轮没通过校验：${String(feedback).slice(0, 900)}\n请只重出 JSON（只许切分点，不许改写原文），别解释。`;
}

/**
 * 一次模型调用，双通道取切分方案。
 * @returns {Promise<{cuts:Array, offsets:Array, unavoidable:boolean, reason:string, source:'tool'|'text'}|null>}
 */
async function askParts({ llm, provider, model, sourceId, type, ts, text, feedback, log, maxTokens = 0 }: AskPartsArgs): Promise<(SplitCuts & { source: 'tool' | 'text' }) | null> {
  if (!llm || typeof llm.stream !== 'function') throw new Error('split: llm 不可用');
  const prompt = buildSplitPrompt({ sourceId, type, ts, text, feedback });
  const stream = (llm.stream as (options: Record<string, unknown>) => AsyncIterable<SplitStreamChunk>)({
    provider,
    model,
    system: SPLIT_SYSTEM,
    messages: [{ role: 'user', content: [{ type: 'text', text: prompt }] }],
    tools: [SUBMIT_PARTS_TOOL],
    // 切分点是短输出，low 够用，省 token
    reasoningEffort: 'low',
    // 2026-11-06 维护者硬规矩：默认 0 = 不传 maxTokens，长度由提示词和 provider 上限管；
    // 2026-11-14 起不传 maxTokens（配置口子已删），统一走恒空的 maxTokensOption。
    ...(maxTokensOption as (maxTokens?: number) => Record<string, never>)(maxTokens),
    temperature: 0,
  });

  let body = '';
  let complete = null;
  const partials = new Map();
  for await (const chunk of stream) {
    if (chunk?.type === 'text-delta') {
      body += chunk.text ?? '';
    } else if (chunk?.type === 'tool-call-delta') {
      const index = chunk.index ?? 0;
      const partial = partials.get(index) ?? { id: chunk.id, name: '', args: '' };
      if (chunk.id) partial.id = chunk.id;
      if (chunk.name) partial.name = chunk.name;
      partial.args += chunk.argumentsDelta ?? '';
      partials.set(index, partial);
    } else if (chunk?.type === 'tool-call') {
      if (String(chunk.name || '') === SUBMIT_PARTS_TOOL.name) {
        complete = typeof chunk.arguments === 'string' ? chunk.arguments : JSON.stringify(chunk.arguments ?? {});
      }
    } else if (chunk?.type === 'finish' && chunk.reason?.kind === 'error') {
      throw new Error(`split: 模型调用失败：${JSON.stringify(chunk.reason.failure || {})}`);
    }
  }

  // 通道一：工具流里出现过 submit_parts
  const toolArgs = complete
    ?? [...partials.values()].find((p) => String(p.name || '') === SUBMIT_PARTS_TOOL.name)?.args;
  if (toolArgs) {
    try {
      return { ...normalizeCuts(toolArgs), source: 'tool' };
    } catch (error) {
      log?.(`split: 工具通道解析失败（${(error as Error).message}），回退文本`);
    }
  }

  // 通道二：正文 JSON
  if (String(body).trim()) {
    try {
      return { ...normalizeCuts(body), source: 'text' };
    } catch (error) {
      log?.(`split: 文本通道解析失败（${(error as Error).message}）`);
    }
  }

  return null;
}

/**
 * 一条超长记忆的拆分失败：落 pending_ops(kind='split_item')，attempts+1，按 1/5/15 分钟退避，
 * attempts>3 转 dead。原文一个字都不动。
 *
 * @returns {object|null} pending_ops 行；没表/写失败返回 null（只记日志）
 */
export function recordSplitFailure(db: DatabaseSync | null, { sourceId, text = '', type = '', reason = 'split_fail', attempt = 0, log = null, now = Date.now() }: RecordSplitFailureOptions = {} as RecordSplitFailureOptions): Row | null {
  if (!db) {
    log?.(`[split] WARN 没有 pending_ops，split_item 只记日志 item=${sourceId} reason=${reason}`);
    return null;
  }
  try {
    const row = enqueue(db, 'split_item', String(sourceId), {
      source_id: String(sourceId),
      text: String(text ?? ''),
      type: String(type ?? ''),
      length: String(text ?? '').length,
      reason: String(reason).slice(0, 160),
      attempt: Number(attempt) || 0,
    }, { now });
    const after = markOp(db, row.id as string | number, 'pending', reason, { now });
    log?.(
      `[split] item=${sourceId} 未通过校验 reason=${reason} attempts=${after?.attempts ?? '?'} ` +
        `status=${after?.status || 'pending'} 原文未动`,
    );
    return after;
  } catch (error) {
    log?.(`[split] WARN split_item 入队失败 item=${sourceId}：${String((error && (error as Error).message) || error)}`);
    return null;
  }
}

/** 段正文合计字数。 */
function sumChars(parts: unknown): number {
  return (Array.isArray(parts) ? parts : []).reduce((sum, part) => sum + String(part?.text ?? '').length, 0);
}

/** 正文去重：normalizeText 后同一段只留先出现的；丢掉的段一并带回给覆盖校验。 */
function dedupeParts(parts: unknown): { parts: SplitPart[]; dropped: SplitPart[] } {
  const seen = new Set();
  const kept: SplitPart[] = [];
  const dropped: SplitPart[] = [];
  for (const part of (Array.isArray(parts) ? parts : []) as SplitPart[]) {
    const key = normalizeText(part?.text);
    if (seen.has(key)) {
      dropped.push(part);
      continue;
    }
    seen.add(key);
    kept.push(part);
  }
  return { parts: kept, dropped };
}

/** 复核判输的一条落成可核对的被剔段（正文 + 原区间，有就带上）。 */
function discardOf(entry: { text?: unknown; item?: unknown } | null | undefined): SplitPart {
  const item = (entry?.item || {}) as SplitPart;
  const text = String(entry?.text ?? item?.text ?? '');
  return {
    ...item,
    text,
    evidence: String(item?.evidence ?? text.slice(0, 24)),
    keptFacts: [],
  };
}

/** lint 复核的产物：留下来的段 + 两路被剔段（都带回给覆盖校验）。 */
interface LintReviewResult {
  ok: boolean;
  parts: SplitPart[];
  charsAfter: number;
  lintDiscards: SplitPart[];
  dedupeDiscards: SplitPart[];
}

/**
 * 正文 lint 两级复核 + 最多一次重切（2026-11-06 /）。
 *   ① 代码 lint 只打标记，命中的段落进可疑集；
 *   ② 复核 agent 整批**只发一次**调用：keep = 误杀原样放行，redo = 真有问题；
 *   ③ 判 redo 的换切点重做**一次**（不占结构轮次，也不循环）；切分点协议下「重做」= 换切点，
 *      正文仍由原文切，不许改写。重做后再过一遍代码 lint，仍不过落 discards(LINT_REJECT)。
 *
 * @returns {Promise<{ok:boolean, parts:Array, charsAfter:number, lintDiscards:Array, dedupeDiscards:Array}>}
 *          ok=false = 剔完不足两段，整条退拆；两路被剔段无论 ok 与否都带回。
 */
async function applyLintReview({
  firstParts, source, sourceId, type, ts, round, llm, provider, model, maxTokens,
  db, maxChars, log, lintCfg, textLint, state,
}: ApplyLintReviewArgs): Promise<LintReviewResult> {
  const firstLint = textLint ? lintItems<SplitPart>(firstParts) : { accepted: [], rejected: [] };
  if (!textLint || !firstLint.rejected.length) {
    return { ok: true, parts: firstParts, charsAfter: sumChars(firstParts), lintDiscards: [], dedupeDiscards: [] };
  }
  const codes = [...new Set(firstLint.rejected.flatMap((entry) => entry.violations.map((v) => v.code)))];
  log?.(`[split] item=${sourceId} 第 ${round} 轮 ${firstLint.rejected.length} 段命中代码标记（${codes.join(',')}），交复核`);

  const judged = await judgeSuspected({
    cfg: lintCfg,
    llm,
    agent: 'split',
    suspects: firstLint.rejected.map((entry) => ({ ...entry, sourceId })),
    logger: log,
  } as ReviewOpts);
  // keep = 误杀：原样放行，写库时带 __lintKept 过 store 最后一道闸
  const keepParts = [
    ...firstLint.accepted.map((entry) => entry.item),
    ...judged.keep.map((entry) => ({ ...entry.item, ...lintKeptMarker(entry) })),
  ] as SplitPart[];
  const dedupedKeep = dedupeParts(keepParts);
  let finalParts = dedupedKeep.parts;
  let lintDiscards: SplitPart[] = [];
  let dedupeDiscards: SplitPart[] = [...dedupedKeep.dropped];
  const rejectAll = (entries: ReviewItem[]) => {
    const list = mergeReviewRejects(entries, judged.redo);
    recordLintRejects(db, list.map((r) => ({ ...r, sourceId })), { action: 'split', sourceId, log } as unknown as { action?: string; sourceId?: string; log?: (line: string) => void });
    state.lintRejected += list.length;
  };

  if (!judged.redo.length) {
    log?.(`[split] item=${sourceId} 复核放行 ${judged.keep.length} 段（误杀），不用重切`);
  } else if (state.retried === 0) {
    const redone = await redoAfterLint({
      judged, source, sourceId, type, ts, llm, provider, model, maxTokens, db, maxChars, log, keepParts, rejectAll, state,
    });
    finalParts = redone.parts;
    lintDiscards = redone.lintDiscards;
    dedupeDiscards = redone.dedupeDiscards;
  } else {
    // 已经重做过一次：不再循环，直接落账
    rejectAll(judged.redo);
    lintDiscards = judged.redo.map(discardOf);
  }
  return {
    ok: finalParts.length >= 2,
    parts: finalParts,
    charsAfter: sumChars(finalParts),
    lintDiscards,
    dedupeDiscards,
  };
}

/** lint 判 redo 后的那一次重切：只换切分点，正文仍逐字来自原文；仍不合格就按复核结果落账。 */
async function redoAfterLint({
  judged, source, sourceId, type, ts, llm, provider, model, maxTokens, db, maxChars, log, keepParts, rejectAll, state,
}: RedoAfterLintArgs): Promise<{ parts: SplitPart[]; lintDiscards: SplitPart[]; dedupeDiscards: SplitPart[] }> {
  const firstRoundDiscards = judged.redo.map(discardOf);
  let retryAnswer = null;
  try {
    retryAnswer = await askParts({
      llm, provider, model, sourceId, type, ts, text: source,
      feedback: `${buildLintRedoFeedback(judged.redo)}\n重做只许换切分点：把被指的段并到相邻段或换切点，正文仍逐字来自原文，不许改写。`,
      log, maxTokens,
    });
  } catch (error) {
    log?.(`[split] item=${sourceId} lint 重做调用失败：${String((error && (error as Error).message) || error).slice(0, 120)}`);
  }
  if (!retryAnswer) {
    rejectAll(judged.redo);
    const kept = dedupeParts(keepParts);
    return { parts: kept.parts, lintDiscards: firstRoundDiscards, dedupeDiscards: kept.dropped };
  }
  const retryChecked = validateParts(retryAnswer, source, { maxChars });
  if (!retryChecked.ok) {
    // 重做那版切分不合格：第一轮被打回的按复核结果落账，不入库
    log?.(`[split] item=${sourceId} lint 重做版切分不合格（${retryChecked.reason}），按第一轮结果处理`);
    rejectAll(judged.redo);
    const kept = dedupeParts(keepParts);
    return { parts: kept.parts, lintDiscards: firstRoundDiscards, dedupeDiscards: kept.dropped };
  }
  const retryLint = lintItems(retryChecked.parts);
  const passed = retryLint.accepted.map((entry) => entry.item);
  if (retryLint.rejected.length) {
    const list = mergeReviewRejects(retryLint.rejected.map((r) => ({ ...r, sourceId })), judged.redo);
    recordLintRejects(db, list, { action: 'split', sourceId, log } as unknown as { action?: string; sourceId?: string; log?: (line: string) => void });
    // 重做版里仍被拒的段已经落账，这里只累加计数
    state.lintRejected += list.length;
  }
  log?.(
    `[split] item=${sourceId} lint 重做后通过 ${passed.length} 段，` +
      `仍拒 ${retryLint.rejected.length} 段（已记 lint_reject，带复核理由）`,
  );
  // 切分点协议下重做给的是**整条**切分方案（cuts 覆盖全文），所以整体替换，
  // 不再和第一轮的 keep 段拼——拼起来会出两段重叠正文。
  const kept = dedupeParts(passed.length ? passed : keepParts);
  return {
    parts: kept.parts,
    // 第一轮被判 redo 的正文也要纳入覆盖判定：重做方案没接住就是丢内容
    lintDiscards: [...firstRoundDiscards, ...retryLint.rejected.map(discardOf)],
    dedupeDiscards: kept.dropped,
  };
}

/**
 * 跑一次拆分。不落库、不归档，只返回校验过的 parts（正文全部取自原文）。
 *
 * @param {object} opts
 * @param {string} opts.sourceId 原条目 id（提示词 {id}，也是 pending 的 dedupe_key）
 * @param {string} opts.text 原文
 * @param {string} [opts.type]
 * @param {string} [opts.ts]
 * @param {object} opts.llm ctx.llm（或测试里的假 llm）
 * @param {string} [opts.provider]
 * @param {string} [opts.model]
 * @param {object|import('node:sqlite').DatabaseSync} [opts.db] 给了就落 split_item pending
 * @param {number} [opts.maxChars=200]
 * @param {boolean} [opts.textLint=true] 正文 lint；关掉只给「测轮次/退拆策略」的用例
 * @param {(line:string)=>void} [opts.log]
 * @returns {Promise<{
 *   parts?:Array, lostFacts?:Array, unavoidable?:boolean, reason?:string,
 *   source?:'tool'|'text', rounds?:number, ratio?:number, charsBefore?:number, charsAfter?:number,
 *   longKept?:number, cuts?:number[], lintRejected?:number
 * }|undefined>} 校验不合格/两条通道都坏 → undefined（并已落 pending）。
 */
export async function split(opts: SplitOptions = {}): Promise<SplitResult | undefined> {
  const {
    sourceId = '', text = '', type = '', ts = '', llm, provider, model, db = null,
    maxChars = PART_TEXT_MAX,
    attempt = 0, log, maxTokens = 0,
    // textLint=false 只给「测的是轮次/退拆策略、正文自足性另有专门测试」的用例用
    textLint = true,
    // lint 两级复核；false = 完全走旧逻辑
    lintReview = true,
    lintReviewTimeoutMs = 0,
    // 段级信号是否硬拒（pipeline.splitSignalsStrict）；默认 false = 只记
    signalsStrict = false,
  } = opts;
  const source = String(text ?? '').trim();
  if (!source) throw new Error('split: text 为空');
  // 结构校验只做一次；正文 lint 的重做仍是**独立的一次** askParts（内容纠错，不是失败重试），
  // 不加多轮循环——每个 agent 只支持这一次重做（ 再确认）。
  const rounds = 1;
  // lint 重切次数与 lint_reject 账（applyLintReview 里改）
  const lintState = { retried: 0, lintRejected: 0 };
  // 复核 agent 的判据（同一条路由，不传 maxTokens）
  const lintCfg = {
    provider,
    model,
    pipeline: { lintReview, ...(Number(lintReviewTimeoutMs) > 0 ? { lintReviewTimeoutMs: Number(lintReviewTimeoutMs) } : {}) },
  };

  let feedback = '';
  let lastReason = 'parse_fail';
  let lastHint = '';
  // 切分方案不合格（拼回不等 / 锚串找不到 / 段数不够）原地重问一次：不占结构轮次，
  // 也不额外记 attempts；仍不合格才落 pending 等下一轮。
  let cutRetried = 0;
  for (let round = 1; ; round += 1) {
    let answer = null;
    try {
      answer = await askParts({ llm, provider, model, sourceId, type, ts, text: source, feedback, log, maxTokens });
    } catch (error) {
      lastReason = 'call_fail';
      lastHint = String((error && (error as Error).message) || error).slice(0, 120);
      log?.(`[split] 第 ${round} 轮调用失败：${lastHint}`);
      answer = null;
    }

    if (answer) {
      const checked = validateParts(answer, source, { maxChars, signalsStrict });
      if (checked.ok) {
        // 段级自足性信号（2026-11-20 P0 阶段一）：一字不拒，只落一行日志与返回字段。
        // 每条 signal 一行（code + 段首 30 字），末尾汇总一行便于 grep 统计。
        const signals = Array.isArray(checked.signals) ? checked.signals : [];
        for (const signal of signals) {
          log?.(`[split] item=${sourceId} signal=${signal.code} at=${signal.index} ${signal.hint}`);
        }
        if (signals.length) {
          const counts = signals.reduce<Record<string, number>>((acc, signal) => {
            acc[signal.code] = (acc[signal.code] || 0) + 1;
            return acc;
          }, {});
          const codeText = Object.entries(counts).map(([code, n]) => `${code}:${n}`).join(',');
          log?.(`[split] item=${sourceId} signals=${signals.length} codes=${codeText}（只记不拒）`);
        }
        // 正文 lint（2026-11-06；2026-11-06 改两级）：
        // ① 代码 lint 只打标记，命中的段落进可疑集；
        // ② 复核 agent 裁决，整批**只发一次**调用：keep = 误杀原样放行，redo = 真有问题；
        // ③ 判 redo 的退回本 agent 重做**一次**（不占结构轮次，也不循环）；切分点协议下
        //    「重做」= 换切点，正文仍由原文切，不许改写。重做后再过一遍代码 lint：
        //    通过就入库，仍不过落 discards(LINT_REJECT)，两轮 violations + 复核理由都留着。
        if (checked.longKept! > 0) {
          log?.(`[split] item=${sourceId} 段落超限保留 ${checked.longKept!} 段（连句读都没有，整段留，不截断）`);
        }
        const linted = await applyLintReview({
          firstParts: checked.parts!, source, sourceId, type, ts, round, llm, provider, model, maxTokens,
          db, maxChars, log, lintCfg, textLint, state: lintState,
        });
        if (!linted.ok) {
          // 剔掉不合格的段后不够两段：整条退拆，原文一个字不动
          log?.(`[split] item=${sourceId} lint 后只剩 ${linted.parts.length} 段，整条退拆`);
          recordSplitFailure(db, { sourceId, text: source, type, reason: 'lint_reject_remaining_lt_2', attempt, log });
          return undefined;
        }
        return {
          parts: linted.parts,
          lostFacts: [],
          unavoidable: false,
          source: answer.source,
          rounds: round,
          ratio: checked.charsBefore! > 0 ? linted.charsAfter / checked.charsBefore! : 0,
          charsBefore: checked.charsBefore!,
          charsAfter: linted.charsAfter,
          longKept: checked.longKept,
          // 最终布局：二次切分与 lint 重做之后每段的起点，与 parts 一一对应
          cuts: linted.parts.slice(1).map((part) => Number(part.start)),
          lintRejected: lintState.lintRejected,
          lintDiscards: linted.lintDiscards,
          dedupeDiscards: linted.dedupeDiscards,
          // 段级信号：阶段一只供统计，不参与成败
          signals,
        };
      }
      if (checked.unavoidable) {
        const reason = checked.reason || 'unavoidable';
        log?.(`[split] unavoidable item=${sourceId} reason=${reason}`);
        recordSplitFailure(db, { sourceId, text: source, type, reason: `unavoidable: ${reason}`, attempt, log });
        return { unavoidable: true, reason, parts: [], rounds: round, source: answer.source };
      }
      lastReason = checked.reason!;
      lastHint = checked.hint || checked.reason!;
      feedback = lastHint;
      log?.(`[split] item=${sourceId} 第 ${round} 轮退拆 reason=${checked.reason} ${lastHint}`);
      if (cutRetried < 1) {
        cutRetried += 1;
        log?.(`[split] item=${sourceId} 切分方案不合格，带 feedback 重问一次（不额外记 attempts）`);
        continue;
      }
    } else {
      lastReason = lastReason === 'call_fail' ? 'call_fail' : 'parse_fail';
      feedback = '上一条不是可解析的 JSON，只输出 {"cuts":["原文里的定位串"]}，不许输出正文';
      log?.(`[split] item=${sourceId} 第 ${round} 轮双通道都没出 JSON，重试`);
    }
    if (round >= rounds) break;
  }

  log?.(
    `[split] WARN item=${sourceId} ${rounds + cutRetried} 次没切合格（${lastReason}${lastHint ? `：${lastHint}` : ''}），` +
      '原文保持不动，落 pending 等下一轮',
  );
  recordSplitFailure(db, { sourceId, text: source, type, reason: lastReason, attempt, log });
  return undefined;
}

export default split;
