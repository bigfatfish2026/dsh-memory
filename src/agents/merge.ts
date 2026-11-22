/**
 * MergeAgent（2026-11-05）。
 *
 * 看一小批同簇记忆和它们的近似候选，产出 merge / update / archive / add 提案，不落库。
 * 提示词逐字来自 docs/agent-prompts.md「合并 agent」，参数也照文档：不传 maxTokens、温度 0。
 * 双通道结构化输出（submit_ops 工具流 + 文本 JSON 回退），都坏重试一次，仍坏返回 undefined，
 * 由上层落 pending_ops(kind='merge_batch')。
 *
 * 本文件还有 validateOps()：模型输出只是提案，证据校验才是入库前的硬闸——正文里的数字、
 * 日期、路径、配置键值必须在 evidence[].quote 里逐字找得到，quote 又必须是源条目正文的子串。
 * 对不上的 op 整条作废（不修、不猜），记日志并落 pending_ops(kind='merge_op') 供人工查。
 *
 * 设计见 docs/sleep-pipeline-redesign.md §2.1、§4.4。
 */
import { sha1Hex } from '../fingerprint.js';
import { parseJsonLoose } from '../shared/json.js';
import { normalizeText } from '../write-gate.js';
import { maxTokensOption } from '../llm-params.js';
import { lintMemoryText } from '../text-lint.js';
import { SELF_CONTAINED_RULES, MERGE_RULES } from '../lint-prompt.js';
import { recordLintRejects } from '../lint-reject.js';
import {
  judgeSuspected, buildLintRedoFeedback, mergeReviewRejects, lintKeptMarker, lintMarksOf,
} from '../lint-review.js';
import type { DatabaseSync } from 'node:sqlite';
import type { ReviewItem, ReviewOpts } from '../lint-review.js';
import type { LlmServiceLike } from '../llm-service.js';

/** 一条证据：源条目 id + 原文里逐字找得到的片段。 */
export interface MergeEvidence {
  id: string;
  quote: string;
}

/** 模型给的一条 op 的原始读取面（normalizeOps 收窄前）。 */
interface MergeOpInput {
  op?: unknown;
  ids?: unknown;
  text?: unknown;
  evidence?: unknown;
  needsSplit?: unknown;
  reason?: unknown;
}

/** 收窄后的提案（normalizeOps 的产物）。 */
export interface MergeOp {
  op: string;
  ids: string[];
  text: string;
  evidence: MergeEvidence[];
  needsSplit: boolean;
  reason: string;
}

/** 流里 usage 的几种宿主写法。 */
interface MergeUsage {
  completionTokens?: unknown;
  outputTokens?: unknown;
  output_tokens?: unknown;
  reasoningTokens?: unknown;
  reasoning_tokens?: unknown;
}

/** 模型流的一块：只认下面这些字段。 */
interface MergeStreamChunk {
  type?: string;
  text?: string;
  index?: number;
  id?: string;
  name?: string;
  argumentsDelta?: string;
  arguments?: unknown;
  usage?: MergeUsage | null;
  completionTokens?: unknown;
  reasoningTokens?: unknown;
  reason?: { kind?: string; failure?: unknown; usage?: MergeUsage | null };
}

/** 一次调用的留痕：两个通道原文 + usage + 错误摘要。 */
export interface MergeTrace {
  finishReason: string;
  maxTokens: number | null;
  completionTokens: number | null;
  reasoningTokens: number | null;
  reasoningChars: number;
  toolRaw: string;
  textRaw: string;
  errors: string[];
}

/** 留痕的读取面：trace 可能来自流错误挂上来的半截对象。 */
interface MergeTraceLike {
  finishReason?: unknown;
  maxTokens?: unknown;
  completionTokens?: unknown;
  reasoningTokens?: unknown;
  reasoningChars?: unknown;
  toolRaw?: unknown;
  textRaw?: unknown;
  errors?: unknown[];
}

/** askOps 的结构化结果（ok=false 时带原因，不再用 null 糊掉）。 */
export type AskOpsResult =
  | { ok: true; ops: MergeOp[]; source: 'tool' | 'text'; trace: MergeTrace }
  | { ok: false; kind: 'empty' | 'reasoning_exhausted' | 'parse_fail' | 'call_fail'; trace: MergeTrace };

/** 一批里的一条记忆。 */
export interface MergeItem {
  id?: unknown;
  type?: unknown;
  text?: unknown;
  ts?: unknown;
  validFrom?: unknown;
  entities?: unknown;
}

/** 近似候选。 */
export interface MergeCandidate {
  id?: unknown;
  text?: unknown;
  score?: unknown;
}

/** merge() 的入参。 */
export interface MergeOptions {
  items?: MergeItem[];
  candidates?: MergeCandidate[];
  groups?: MergeItem[][] | null;
  clusterId?: string;
  maxTokens?: number;
  llm?: LlmServiceLike | null;
  provider?: string;
  model?: string;
  log?: (line: string) => void;
  onFail?: (summary: string, kind: string) => void;
  db?: DatabaseSync | null;
  textLint?: boolean;
  lintReview?: boolean;
  lintReviewTimeoutMs?: number;
}

/** merge() 的产物。 */
export interface MergeResult {
  ops: MergeOp[];
  source: 'tool' | 'text';
  retries: { empty: number; parse: number; reasoning: number; lint: number };
  calls: number;
  lintRejected?: number;
  lintReviewed?: number;
}

/** 一条被代码 lint 打回的提案。 */
interface MergeLintReject {
  index: number;
  kind: string;
  item: unknown;
  text: string;
  ids: string[];
  violations: unknown[];
}

/** buildMergePrompt 的入参。 */
interface BuildMergePromptInput {
  items?: MergeItem[];
  candidates?: MergeCandidate[];
  groups?: MergeItem[][] | null;
  clusterId?: string;
  feedback?: string;
}

/** formatRawTrace 的入参。 */
interface RawTraceInput {
  channel?: unknown;
  raw?: unknown;
  finishReason?: unknown;
  maxTokens?: unknown;
  completionTokens?: unknown;
  reasoningTokens?: unknown;
  reasoningChars?: unknown;
  error?: unknown;
}

/** validateOps 的入参。 */
interface ValidateOpsOptions {
  maxChars?: number;
  db?: DatabaseSync | null;
  batchId?: string;
  log?: (line: string) => void;
}

/** 通过证据校验的一条 op（给写库层吃）。 */
export interface ValidMergeOp {
  op: string;
  ids: string[];
  text: string;
  evidence: MergeEvidence[];
  needsSplit: boolean;
  reason: string;
  index: number;
  [key: string]: unknown;
}

/** 证据校验作废的一条 op（落 pending_ops）。 */
export interface InvalidMergeOp {
  index: number;
  op: string;
  ids: string[];
  reason: string;
  quote?: unknown;
  evidence: MergeEvidence[];
  source: Array<{ id: string; text: string }>;
  text: string;
  id?: unknown;
  fact?: unknown;
  factKind?: unknown;
  [key: string]: unknown;
}

/** validateOps 的结果。 */
export interface ValidateOpsResult {
  valid: ValidMergeOp[];
  invalid: InvalidMergeOp[];
}

export const MERGE_SYSTEM = `你是长期记忆的合并整理员。输入一批已有记忆条目和它们的近似候选。你只做三件事：把同一事实的重复表述合并、把过时事实改成新版本、把已失效的条目归档。

硬规矩
1. 不许新增原条目里没有的事实。任何数字、日期、路径、配置值都必须能在某条原文里逐字找到，并在 evidence 里标来源 id 和原句。证据里的路径、行号和单位先按原文写法保留。
2. 正文超过 200 字不许截断、不许丢：整段放进 text，置 needsSplit=true，交给拆分员处理。
3. 合并只在「同一件事的不同表述」之间做。不同事件即使主题相近也不许并。合并后若会丢失条件、期限、边界值，就不许合并，改 op=update 或者不动。
4. 拿不准就输出空 ops，宁可不动。
5. 只输出 JSON，不要解释。

版本规则
合并前先核对对象、事件、版本和适用范围。改稿后的复核不得借用上一版的理由。某项 pass 只代表该版本的该项通过，有最终 ask 或 abort 时一并保留。只有原文明示新版本取代旧版本时才 update。分不清版本就跳过这一组。测试数、产物数和验收标准要带项目与批次，避免把一次迁移的基线写成通用要求。

${SELF_CONTAINED_RULES}

${MERGE_RULES}`;

// 2026-11-14：删掉「本轮改动上限 {maxChanges} 条」——动作数不设上限。
export const MERGE_USER_TEMPLATE = `本批 {clusterId}。
待整理记忆
{items: [{id, type, text, ts, entities}]}
近似候选（库里可能已有的同类事实）
{candidates: [{id, text, score}]}

输出
{"ops":[{"op":"merge|update|archive|add","ids":["..."],"text":"合并后正文","evidence":[{"id":"来源id","quote":"原文片段"}],"needsSplit":false,"reason":"不超过30字"}]}`;

/** 工具通道的 schema：顶层 {ops:[...]}，每个 op 的字段固定，enum 收窄动作。 */
export const SUBMIT_OPS_TOOL = {
  name: 'submit_ops',
  description: '提交这一批记忆的整理提案。只调用这个工具，不要输出正文。',
  parameters: {
    type: 'object',
    additionalProperties: false,
    properties: {
      ops: {
        type: 'array',
        description: '提案列表；拿不准就给空数组',
        items: {
          type: 'object',
          additionalProperties: false,
          properties: {
            op: {
              type: 'string',
              enum: ['merge', 'update', 'archive', 'add'],
              description: 'merge=合并重复表述；update=修订成新版本；archive=归档失效条目；add=新增原条目没有的事实',
            },
            ids: {
              type: 'array',
              items: { type: 'string' },
              description: '涉及的源条目 id：merge ≥2 个，update/archive 恰好 1 个，add 留空',
            },
            text: { type: 'string', description: '合并/修订后的完整正文；archive 可以为空字符串' },
            evidence: {
              type: 'array',
              description: '事实来源：每条给源条目 id 和原文片段，片段必须能在原文里逐字找到',
              items: {
                type: 'object',
                additionalProperties: false,
                properties: {
                  id: { type: 'string', description: '源条目 id' },
                  quote: { type: 'string', description: '源条目正文里的连续片段' },
                },
                required: ['id', 'quote'],
              },
            },
            needsSplit: { type: 'boolean', description: '正文超过 200 字时必须为 true，不许截断', },
            reason: { type: 'string', maxLength: 30, description: '不超过 30 字的理由' },
          },
          required: ['op', 'ids', 'text', 'evidence'],
        },
      },
    },
    required: ['ops'],
  },
};

export const OP_NAMES = ['merge', 'update', 'archive', 'add'];
const OP_SET = new Set(OP_NAMES);
export const OP_TEXT_MAX = 200;

// 2026-11-14：MERGE_MAX_TOKENS / MERGE_BATCH_MAX 两个常量删了。
// 输出上限不再由本插件传（由 provider 封顶）；一批多少条由 token 预算切（src/token-window.ts）。

/** 模型原文留痕宽度：每个通道各记前 600 字符（写日志 + last_error 摘要都用它）。 */
export const RAW_TRACE_CHARS = 600;

/** 把一段模型原文按 RAW_TRACE_CHARS 压到可留痕的宽度，返回字符数与是否截过。 */
export function clampRaw(raw: unknown, limit = RAW_TRACE_CHARS): { text: string; original: number; truncated: boolean } {
  const text = String(raw ?? '');
  const len = text.length;
  if (len <= limit) return { text, original: len, truncated: false };
  return { text: text.slice(0, limit), original: len, truncated: true };
}

/**
 * 单通道留痕行（2026-11-05 t-fix-truncate，2026-11-06 补 reasoning）。
 * 05:00 那轮「批成片解析失败」查不出原因，就是因为模型原文一个字都没留。
 * 这一行固定带 channel / finish / completionTokens / reasoningTokens / reasoningChars / 长度 / 前 600 字。
 *
 * 形如：
 *   [merge] raw channel=tool finish=length maxTokens=unset completionTokens=393216 reasoningTokens=393000 reasoningChars=152340 len=0 err=- raw=""
 */
export function formatRawTrace({
  channel, raw, finishReason, maxTokens, completionTokens, reasoningTokens, reasoningChars, error,
}: RawTraceInput = {}): string {
  const { text, original, truncated } = clampRaw(raw);
  const err = error ? ` err=${JSON.stringify(String((error && (error as Error).message) || error).slice(0, 160))}` : ' err=-';
  const tail = truncated ? `…(+${original - text.length}字)` : '';
  return `[merge] raw channel=${channel || '-'} finish=${finishReason || '-'}` +
    ` maxTokens=${Number(maxTokens) > 0 ? maxTokens : 'unset'}` +
    ` completionTokens=${completionTokens ?? '-'} reasoningTokens=${reasoningTokens ?? '-'}` +
    ` reasoningChars=${reasoningChars ?? '-'} len=${original}${err} raw=${JSON.stringify(text)}${tail}`;
}

/** 一次调用失败的全部留痕：两个通道各一行，合计最多 2×600 字。 */
export function rawTraceLines(trace: MergeTraceLike = {}): string[] {
  const shared = {
    finishReason: trace.finishReason,
    maxTokens: trace.maxTokens,
    completionTokens: trace.completionTokens,
    reasoningTokens: trace.reasoningTokens,
    reasoningChars: trace.reasoningChars,
  };
  return [
    formatRawTrace({ ...shared, channel: 'tool', raw: trace.toolRaw, error: (trace.errors || []).find((e) => String(e).startsWith('tool:')) }),
    formatRawTrace({ ...shared, channel: 'text', raw: trace.textRaw, error: (trace.errors || []).find((e) => String(e).startsWith('text:')) }),
  ];
}

/**
 * last_error 摘要：两个通道都带，各自只留 160 字开头，进 DB 也能一眼看出截在哪、finish 是什么。
 * 形如：finish=length completionTokens=2000 tool=2413字:"{\"ops\":[{\"op\":\"merge\"…" text=0字:""
 */
export function failureSummary(trace: MergeTraceLike = {}, { headChars = 160 }: { headChars?: number } = {}): string {
  const head = (raw: unknown) => {
    const { text, original, truncated } = clampRaw(raw, headChars);
    return `${original}字:${JSON.stringify(text)}${truncated ? '…' : ''}`;
  };
  // 2026-11-14：400 字，够看到 position N 附近到底多了什么
  const errs = Array.isArray(trace.errors) && trace.errors.length
    ? ` err=${trace.errors.join(' ; ').slice(0, 400)}`
    : '';
  const reasoning = ` reasoningTokens=${trace.reasoningTokens ?? '-'} reasoningChars=${trace.reasoningChars ?? '-'}`;
  return `finish=${trace.finishReason || '-'} maxTokens=${Number(trace.maxTokens) > 0 ? trace.maxTokens : 'unset'}` +
    ` completionTokens=${trace.completionTokens ?? '-'}${reasoning}` +
    ` tool=${head(trace.toolRaw)} text=${head(trace.textRaw)}${errs}`;
}

function asStringList(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return value.map((v) => String(v ?? '')).filter(Boolean);
}

/** 收窄到最小形状；结构坏直接抛错，交给上层当「通道坏」处理。 */
export function normalizeOps(raw: unknown): MergeOp[] {
  const value: unknown = typeof raw === 'string' ? parseJsonLoose(raw) : raw;
  const list = Array.isArray(value) ? value : (value as { ops?: unknown } | null)?.ops;
  if (!Array.isArray(list)) throw new Error(`ops 不是数组：${JSON.stringify(value)?.slice(0, 120)}`);
  const ops = (list as unknown[]).map((entry) => {
    const op = String((entry as MergeOpInput | null)?.op ?? '').toLowerCase().trim();
    if (!OP_SET.has(op)) throw new Error(`op 非法：${(entry as MergeOpInput | null)?.op}`);
    const evidence = Array.isArray((entry as MergeOpInput | null)?.evidence)
      ? ((entry as MergeOpInput).evidence as unknown[])
        .map((e) => ({ id: String((e as { id?: unknown } | null)?.id ?? ''), quote: String((e as { quote?: unknown } | null)?.quote ?? '') }))
        .filter((e) => e.id && e.quote)
      : [];
    return {
      op,
      ids: asStringList((entry as MergeOpInput | null)?.ids),
      text: String((entry as MergeOpInput | null)?.text ?? ''),
      evidence,
      needsSplit: (entry as MergeOpInput | null)?.needsSplit === true,
      reason: String((entry as MergeOpInput | null)?.reason ?? '').slice(0, 80),
    };
  });
  return ops;
}

function itemLine(item: MergeItem | null | undefined) {
  return {
    id: String(item?.id ?? ''),
    type: String(item?.type ?? ''),
    text: String(item?.text ?? ''),
    ts: String(item?.ts ?? item?.validFrom ?? ''),
    entities: Array.isArray(item?.entities) ? item.entities.map(String) : [],
  };
}

/** 逐字模板替换；items / candidates 两段占位符换成 JSON。feedback 追加在末尾（lint 重写轮用）。 */
export function buildMergePrompt({ items = [], candidates = [], groups = null, clusterId = '', feedback = '' }: BuildMergePromptInput = {}): string {
  // 合批（2026-11-14 阶段 3 追问）：items 按组输出，组边界显式标出来；
  // 单批路径（groups 为空）跟原来一字不差。
  const packed = Array.isArray(groups) && groups.length > 1;
  const itemsJson = packed
    ? JSON.stringify(groups.map((g, i) => ({ group: i + 1, items: g.map(itemLine) })))
    : JSON.stringify(items.map(itemLine));
  const candidatesJson = JSON.stringify((candidates || []).map((c) => ({
    id: String(c?.id ?? ''),
    text: String(c?.text ?? ''),
    score: Number(Number(c?.score ?? 0).toFixed(4)),
  })));
  const prompt = MERGE_USER_TEMPLATE
    .replace('{clusterId}', String(clusterId || '未知'))
    .replace('{items: [{id, type, text, ts, entities}]}', itemsJson)
    .replace('{candidates: [{id, text, score}]}', candidatesJson);
  const packHint = packed
    ? '\n\n注意：items 已按 group 分组。同一个 op 的 ids 只能来自同一组，不许跨组合并；'
      + '每组最多给一个 merge/update/archive 提案，组内没有可做的就给空 ops。'
    : '';
  const withHint = `${prompt}${packHint}`;
  return feedback ? `${withHint}\n\n${feedback}` : withHint;
}

/**
 * 逐条判提案正文：archive 没有正文，不判；
 * 一次出多条时只打回不合格的那几条，通过的照常交上层。
 *
 * 2026-11-06：这层只**打标记**——命中的进「可疑集」，
 * 由 merge() 交复核 agent 裁决，不再直接判生死。
 *
 * @returns {{kept:Array, rejected:Array<{kind:string, item:object, text:string, ids:string[], violations:Array}>}}
 */
export function lintOps(ops: unknown): { kept: MergeOp[]; rejected: MergeLintReject[] } {
  const kept: MergeOp[] = [];
  const rejected: MergeLintReject[] = [];
  ((Array.isArray(ops) ? ops : []) as MergeOp[]).forEach((op, index) => {
    const kind = String(op?.op ?? '').toLowerCase().trim();
    const text = String(op?.text ?? '').trim();
    if (kind === 'archive' || !text) {
      kept.push(op);
      return;
    }
    const result = lintMemoryText(text);
    if (result.ok) kept.push(op);
    else {
      rejected.push({
        index,
        kind,
        item: op,
        text,
        ids: Array.isArray(op?.ids) ? op.ids.map(String) : [],
        violations: result.violations,
      });
    }
  });
  return { kept, rejected };
}

/**
 * lint 重试日志用的一行摘要：只打 op 下标与命中的 code，绝不带正文（2026-11-06 t-mw515yk8）。
 * 起因：这轮 7 条合并重试只打了 n=，没法按规则分类。
 */
export function lintCodesBrief(rejected: unknown): string {
  return ((Array.isArray(rejected) ? rejected : []) as MergeLintReject[])
    .map((entry) => {
      const codes = [...new Set(((entry.violations || []) as Array<{ code?: unknown }>).map((v) => v.code))];
      return `i${entry.index}:${codes.join('+') || '-'}`;
    })
    .join(';');
}

/**
 * 复核放行的 op 和本轮通过的 op 可能重在一起（模型偶尔会把「别重写」的那几条又吐一遍），
 * 按 op + 归一化正文去重，先出现的说话（复核先入，保持原样）。
 */
export function dedupeOps(ops: unknown): MergeOp[] {
  const seen = new Set();
  const out = [];
  for (const op of Array.isArray(ops) ? ops : []) {
    const key = `${String(op?.op ?? '')}\u0000${normalizeText(String(op?.text ?? ''))}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(op);
  }
  return out;
}

/** usage 里的输出 token 数：不同宿主字段名不一样，认到的第一个算数。 */
function tokenCountOf(chunk: MergeStreamChunk): number | null {
  const usage = chunk?.usage || chunk?.reason?.usage || null;
  const n = usage?.completionTokens ?? usage?.outputTokens ?? usage?.output_tokens ?? chunk?.completionTokens;
  return Number.isFinite(Number(n)) ? Number(n) : null;
}

/** usage 里的思考 token 数（宿主有就给，没有留 null，别拿 0 冒充）。 */
export function reasoningTokenCountOf(chunk: MergeStreamChunk): number | null {
  const usage = chunk?.usage || chunk?.reason?.usage || null;
  const n = usage?.reasoningTokens ?? usage?.reasoning_tokens ?? chunk?.reasoningTokens;
  return Number.isFinite(Number(n)) ? Number(n) : null;
}

/**
 * 一次模型调用，双通道取提案。
 *
 * 返回结构化结果，不再用 null 糊掉「为什么没拿到」：
 *   {ok:true, ops, source, trace}                拿到提案（ops 可能为空数组，那是模型的空动作）
 *   {ok:false, kind:'empty'|'reasoning_exhausted'|'parse_fail', trace}
 *     empty              = 两个通道一个字节都没有，且不是撞上输出上限；
 *     reasoning_exhausted= finish=length 且两个通道都空（思考把预算吃光了，t-fix-quality 单列）；
 *     parse_fail         = 有输出但 JSON 解析不了。
 * trace 带 finishReason、completionTokens、reasoningTokens/reasoningChars 和两个通道的原文（留痕用）。
 *
 * @param {object} opts
 * @param {number} [opts.maxTokens=0] 已废弃：只为兼容旧调用点，实际永不进请求
 * @returns {Promise<{ok:boolean, ops?:Array, source?:string, kind?:string, trace:object}>}
 */
async function askOps({
  llm, provider, model, items, candidates, clusterId,
  maxTokens = 0, log, feedback = '',
}: {
  llm?: LlmServiceLike | null;
  provider?: string;
  model?: string;
  items: MergeItem[];
  candidates: MergeCandidate[];
  groups?: MergeItem[][] | null;
  clusterId: string;
  maxTokens?: number;
  log?: (line: string) => void;
  feedback?: string;
}): Promise<AskOpsResult> {
  if (!llm || typeof llm.stream !== 'function') throw new Error('merge: llm 不可用');
  const prompt = buildMergePrompt({ items, candidates, clusterId, feedback });
  const stream = (llm.stream as (options: Record<string, unknown>) => AsyncIterable<MergeStreamChunk>)({
    provider,
    model,
    system: MERGE_SYSTEM,
    messages: [{ role: 'user', content: [{ type: 'text', text: prompt }] }],
    tools: [SUBMIT_OPS_TOOL],
    // 宿主只认 low/high/max；合并要看清数字和边界值，实测 low 太省，按提示词文档用 high
    reasoningEffort: 'high',
    ...(maxTokensOption as (maxTokens?: number) => Record<string, never>)(maxTokens),
    temperature: 0,
  });

  let text = '';
  let complete: string | null = null;
  const partials = new Map<number, { id?: string; name: string; args: string }>();
  let finishReason = '';
  let completionTokens: number | null = null;
  // 2026-11-06 t-fix-quality：思考长度也要留痕——撞墙时只有 completionTokens 看不出谁吃光了预算
  let reasoningChars = 0;
  let reasoningTokens: number | null = null;
  // 2026-11-14：宿主可能在收完 toolcall args 之后才抛错
  // （模型把同一个对象连写两遍 → MALFORMED_RESPONSE Unexpected non-whitespace character after JSON）。
  // text/partials 这时已经收全，不能丢：记下错误继续走下面的两通道解析，解不出来才原样抛回上层。
  let streamError: (Error & { trace?: MergeTrace }) | null = null;
  try {
    for await (const chunk of stream) {
      if (chunk?.type === 'reasoning-delta') {
        reasoningChars += String(chunk.text ?? '').length;
      } else if (chunk?.type === 'text-delta') {
        text += chunk.text ?? '';
      } else if (chunk?.type === 'tool-call-delta') {
        const index = chunk.index ?? 0;
        const partial = partials.get(index) ?? { id: chunk.id, name: '', args: '' };
        if (chunk.id) partial.id = chunk.id;
        if (chunk.name) partial.name = chunk.name;
        partial.args += chunk.argumentsDelta ?? '';
        partials.set(index, partial);
      } else if (chunk?.type === 'tool-call') {
        if (String(chunk.name || '') === SUBMIT_OPS_TOOL.name) {
          complete = typeof chunk.arguments === 'string' ? chunk.arguments : JSON.stringify(chunk.arguments ?? {});
        }
      } else if (chunk?.type === 'finish') {
        finishReason = String(chunk.reason?.kind || '');
        completionTokens = tokenCountOf(chunk) ?? completionTokens;
        reasoningTokens = reasoningTokenCountOf(chunk) ?? reasoningTokens;
        if (chunk.reason?.kind === 'error') {
          throw new Error(`merge: 模型调用失败：${JSON.stringify(chunk.reason.failure || {})}`);
        }
      }
      if (chunk?.usage) {
        completionTokens = tokenCountOf(chunk) ?? completionTokens;
        reasoningTokens = reasoningTokenCountOf(chunk) ?? reasoningTokens;
      }
    }
  } catch (error) {
    streamError = error as Error & { trace?: MergeTrace };
  }

  const toolRaw = complete
    ?? [...partials.values()].find((p) => String(p.name || '') === SUBMIT_OPS_TOOL.name)?.args
    ?? '';
  const trace = {
    finishReason,
    // 记「实际传了什么」：0/不填就是 unset，撞墙时 completionTokens 才是 provider 的真实上限
    maxTokens: Number(maxTokens) > 0 ? Number(maxTokens) : null,
    completionTokens, reasoningTokens, reasoningChars, toolRaw, textRaw: text,
    errors: streamError ? [`host:${streamError?.message || streamError}`] : [],
  };

  // 通道一：工具流里出现过 submit_ops
  if (toolRaw) {
    try {
      return { ok: true, ops: normalizeOps(toolRaw), source: 'tool', trace };
    } catch (error) {
      trace.errors.push(`tool:${(error as Error).message}`);
      log?.(`merge: 工具通道解析失败（${(error as Error).message}），回退文本`);
    }
  }

  // 通道二：正文 JSON
  if (String(text).trim()) {
    try {
      return { ok: true, ops: normalizeOps(text), source: 'text', trace };
    } catch (error) {
      trace.errors.push(`text:${(error as Error).message}`);
      log?.(`merge: 文本通道解析失败（${(error as Error).message}）`);
    }
  }

  // 两个通道都没输出：撞上输出上限（finish=length）就是思考吃光预算，单独记 reasoning_exhausted；
  // 其余算空响应。有输出但解析不了 = 解析失败。三种重试策略不同。
  let kind: 'empty' | 'reasoning_exhausted' | 'parse_fail' = 'parse_fail';
  if (!toolRaw && !String(text).trim()) {
    kind = finishReason === 'length' ? 'reasoning_exhausted' : 'empty';
  }
  // 宿主自己抛的错：两通道都没解出来，就原样抛回（上层记 call_fail），不吞
  if (streamError) {
    if (streamError && typeof streamError === 'object') (streamError as Error & { trace?: MergeTrace }).trace = trace;
    throw streamError;
  }
  return { ok: false, kind, trace };
}

/**
 * 跑一次合并提案。不落库，只返回提案。
 *
 * 重试策略（2026-11-14 改）：
 *   - 模型调用失败 / 空响应 / 撞墙 / 解析失败 → **不原地重试固定次数**，直接返回 undefined；
 *     上层落 pending_ops(kind='merge_batch')，attempts+1、last_error 记原文摘要，
 *     下一轮重新领出来再问。不自动放弃，也不会静默吞掉失败。
 *   - 模型明确给 {"ops":[]} 是有效结论（「看过了，这批不用动」），上层按 noop 写 organizeCache。
 *   - lint 复核判 redo 的「重做一次」保留：那是内容纠错，不是失败重试（见 merge() 内 retries.lint）。
 *
 * @param {object} opts
 * @param {Array} opts.items 本批条目 [{id, text, type?, ts?, entities?}]
 * @param {Array} [opts.candidates] 近似候选 [{id, text, score}]
 * @param {string} [opts.clusterId] 批指纹（提示词里的 {clusterId}）
 * @param {number} [opts.maxTokens=0] 已废弃：不再进请求（maxTokensOption 恒空）
 * @param {object} opts.llm ctx.llm（或测试里的假 llm）
 * @param {string} [opts.provider]
 * @param {string} [opts.model]
 * @param {(line:string)=>void} [opts.log]
 * @param {(summary:string, kind:string)=>void} [opts.onFail] 失败时把 last_error 摘要交回上层
 * @returns {Promise<{ops:Array, source:'tool'|'text', retries:{empty:number,parse:number}, calls:number}|undefined>}
 */
export async function merge(opts: MergeOptions = {}): Promise<MergeResult | undefined> {
  const {
    items = [], candidates = [], groups = null, clusterId = '',
    maxTokens = 0, llm, provider, model, log, onFail, db = null,
    // textLint=false 只给「测的是重试/落账策略、正文自足性另有专门测试」的用例用
    textLint = true,
    // lint 两级复核；false = 完全走旧逻辑，不花复核调用
    lintReview = true,
    lintReviewTimeoutMs = 0,
  } = opts;
  if (!Array.isArray(items) || items.length === 0) throw new Error('merge: items 为空');

  const batch = String(clusterId || '-');
  const retries = { empty: 0, parse: 0, reasoning: 0, lint: 0 };
  let calls = 0;
  let last = null;
  let feedback = '';
  // 复核 agent 的判据（跟其它 agent 同一条路由；不传 maxTokens）
  const lintCfg = {
    provider,
    model,
    pipeline: { lintReview, ...(Number(lintReviewTimeoutMs) > 0 ? { lintReviewTimeoutMs: Number(lintReviewTimeoutMs) } : {}) },
  };
  // 复核判 keep（误杀）的 op：原样带出去，写库时靠 __lintKept 过 store 最后一道闸
  const keptByReview: unknown[] = [];
  // 第一轮被判 redo 的可疑条目：它们带复核理由，重做仍不过时并进 discards
  let redoSuspects: ReviewItem[] = [];

  for (;;) {
    calls += 1;
    let got: AskOpsResult;
    try {
      got = await askOps({
        llm, provider, model, items, candidates, groups, clusterId, maxTokens, log, feedback,
      });
    } catch (error) {
      got = {
        ok: false,
        kind: 'call_fail',
        // askOps 在流错误时把已收到的原文挂在 error.trace 上，这里别再造一个空 trace 丢掉
        trace: (error as { trace?: MergeTrace } | null)?.trace
          ?? { finishReason: 'error', completionTokens: null, toolRaw: '', textRaw: '', errors: [`call:${(error as Error)?.message || error}`] } as MergeTrace,
      };
    }

    if (got.ok) {
      // ── 三段式─────────────────────────────
      // ① 代码 lint 只打标记：linted.rejected 就是可疑集；
      // ② 复核 agent 裁决：整批**只发一次**调用（多条可疑合并问）；
      // ③ 判 redo 的退回本 agent 重做**一次**；重做后只再过代码 lint，不再问复核。
      const linted = textLint ? lintOps(got.ops) : { kept: got.ops, rejected: [] };
      if (linted.rejected.length && retries.lint < 1) {
        const judged = await judgeSuspected({
          cfg: lintCfg, llm, agent: 'merge', suspects: linted.rejected, logger: log,
        } as ReviewOpts);
        for (const entry of judged.keep) keptByReview.push({ ...entry.item, ...lintKeptMarker(entry) });
        if (judged.redo.length) {
          retries.lint += 1;
          redoSuspects = judged.redo;
          feedback = buildLintRedoFeedback(judged.redo)
            + '\nevidence 仍必须是源条目正文的逐字片段，仍输出 {"ops":[...]}。';
          log?.(
            `[merge] retry reason=lint batch=${batch} n=${judged.redo.length} attempt=${calls} `
            + `keep=${judged.keep.length} codes=${lintCodesBrief(judged.redo)}`
            + (judged.fallback ? ' fallback=1' : ''),
          );
          // 重做只给一次：retries.lint 已经 +1，下一轮不管好坏都收敛到入库 / discards
          continue;
        }
        // 全判 keep：误杀，原样放行
        log?.(`[merge] 复核放行 ${judged.keep.length} 条误杀，不用重写 batch=${batch}`);
        return {
          ops: dedupeOps([...keptByReview, ...linted.kept]),
          source: got.source,
          retries,
          calls,
          lintRejected: 0,
          lintReviewed: judged.keep.length,
        };
      }
      if (linted.rejected.length) {
        // 已经重做过一次：仍不过就落 discards，两轮 violations + 复核理由都留着，**不再循环**
        const rejects = mergeReviewRejects(linted.rejected, redoSuspects);
        recordLintRejects(db, rejects.map((entry) => ({ ...entry, sourceId: (entry as { ids: string[] }).ids.join('+') })), {
          action: 'merge', log,
        });
        log?.(
          `[merge] lint 重做后仍拒 ${linted.rejected.length} 条，剔除后交上层落地 batch=${batch} `
          + `codes=${lintCodesBrief(linted.rejected)}`,
        );
        // 空动作是有效结论（「看过了，不动」），直接交上层写 noop 缓存，不重试、不落 pending
        return {
          ops: dedupeOps([...keptByReview, ...linted.kept]),
          source: got.source,
          retries,
          calls,
          lintRejected: linted.rejected.length,
        };
      }
      return {
        ops: dedupeOps([...(keptByReview || []), ...got.ops]),
        source: got.source,
        retries,
        calls,
      };
    }

    last = got;
    // 2026-11-14：删掉「空响应/撞墙/解析失败各原地重试 1 次」的固定次数。
    // 一次拿不到就交上层：pending_ops 累加 attempts、记 last_error，下一轮照原样再问，
    // 不自动放弃、也不静默吞（日志和账本都留着）。
    break;
  }

  // 失败也要能自查：两个通道的原文各留前 600 字（05:00 那轮就是这里一片黑）
  for (const line of rawTraceLines(last.trace)) log?.(line);
  const summary = failureSummary(last.trace);
  log?.(`[merge] 连续 ${calls} 次没拿到可解析提案（${last.kind}），交上层落 pending（${summary}）`);
  onFail?.(summary, last.kind);
  return undefined;
}

/**
 * 合批的结果按组分桶：op 的 ids 必须全在同一组里，跨组的一律作废（记 cross）。
 * 某组没有有效 op 时，调用方据此把这组降级成单对请求，其余组照常落库。
 * @returns {{byGroup: Map<number, Array>, cross: Array}}
 */
export function groupOpsOf(ops: unknown, groups: unknown): { byGroup: Map<number, MergeOp[]>; cross: MergeOp[] } {
  const groupOf = new Map<string, number>();
  ((Array.isArray(groups) ? groups : []) as Array<Array<{ id?: unknown } | null | undefined>>).forEach((g, i) => {
    for (const m of g || []) groupOf.set(String(m?.id ?? ''), i);
  });
  const byGroup = new Map<number, MergeOp[]>();
  const cross: MergeOp[] = [];
  for (const op of Array.isArray(ops) ? ops as MergeOp[] : []) {
    const ids = Array.isArray(op?.ids) ? op.ids.map(String) : [];
    const gs = new Set(ids.map((id) => groupOf.get(id)).filter((x) => x !== undefined));
    if (gs.size !== 1) { cross.push(op); continue; }
    const g = [...gs][0] as number;
    if (!byGroup.has(g)) byGroup.set(g, []);
    byGroup.get(g)!.push(op);
  }
  return { byGroup, cross };
}

// ---------------------------------------------------------------- 证据校验

/**
 * 引用比较用的归一化：在 write-gate 的 normalizeText 上再抹掉中文日期字，
 * 这样「2026-11-07」和「2026 年 11 月 7 日」能落到同一串数字上。
 */
export function normalizeQuote(text: unknown): string {
  return normalizeText(text).replace(/[年月日号]/g, '');
}

const DATE_RE = /\d{4}[-/年.]\d{1,2}[-/月.]\d{1,2}日?/g;
const PATH_RE = /(?:~|\/)[\w.@%+-]+(?:\/[\w.@%+-]+)+/g;
const KV_RE = /\b[A-Za-z_][\w.-]{0,40}\s*(?:=|:|：)\s*(?:true|false|null|-?\d+(?:\.\d+)?|"[^"\n]{0,80}"|'[^'\n]{0,80}'|[A-Za-z0-9_./:%+-]{1,80})/g;
const NUMBER_RE = /\d+(?:\.\d+)?/g;
/** 「文件名:行号」。两个部件必须分开比对：拼成一个 token（如 `sleepjs382`）永远对不上
 *  证据里「lib/sleep.js 文件第 382 行」这种写法（2026-11-22 误杀）。 */
const FILE_LINE_RE = /[\w.@%+-]+\.[A-Za-z]{1,8}\s*[:：]\s*\d+/g;

/** 一条必须被证据覆盖的事实。 */
export interface ExtractedFact {
  kind: string;
  raw: string;
  forms: string[];
  /** kind='file_line' 时：文件名与行号分开存，校验时要求落在同一句里。 */
  file?: string;
  line?: string;
}

function dateForms(raw: unknown): string[] {
  const m = String(raw).match(/^(\d{4})[-/年.](\d{1,2})[-/月.](\d{1,2})/);
  if (!m) return [normalizeQuote(raw)];
  const [, y, mo, d] = m;
  const padded = `${y}${String(mo).padStart(2, '0')}${String(d).padStart(2, '0')}`;
  const unpadded = `${y}${Number(mo)}${Number(d)}`;
  return [...new Set([normalizeQuote(raw), padded, unpadded])].filter(Boolean);
}

/**
 * 从正文里抠出必须被证据覆盖的事实：日期、路径、配置键值、剩余数字。
 * 日期/路径/键值先占位，落在它们里面的数字不再单独要求（否则「07」和「7」永远对不上）。
 */
export function extractFacts(text: unknown): ExtractedFact[] {
  const body = String(text ?? '');
  const facts: ExtractedFact[] = [];
  const covered: Array<[number, number]> = [];
  const overlaps = (start: number, end: number) => covered.some(([s, e]) => start < e && end > s);
  const scan = (re: RegExp, kind: string, formsOf: (raw: string) => string[]) => {
    re.lastIndex = 0;
    let m;
    while ((m = re.exec(body)) !== null) {
      const start = m.index;
      const end = start + m[0].length;
      if (overlaps(start, end)) continue;
      covered.push([start, end]);
      facts.push({ kind, raw: m[0].trim(), forms: formsOf(m[0].trim()) });
    }
  };

  scan(DATE_RE, 'date', dateForms);
  // 先于 KV/PATH：否则 `sleep.js:382` 会被 KV_RE 整段吃成一条键值事实
  scan(FILE_LINE_RE, 'file_line', () => []);
  scan(PATH_RE, 'path', (raw) => [normalizeQuote(raw)]);
  scan(KV_RE, 'kv', (raw) => [normalizeQuote(raw)]);
  scan(NUMBER_RE, 'number', (raw) => [...new Set([normalizeQuote(raw), normalizeQuote(String(Number(raw)))])]);

  for (const fact of facts) {
    if (fact.kind !== 'file_line') continue;
    const cut = fact.raw.search(/[:：]/);
    if (cut <= 0) continue;
    fact.file = fact.raw.slice(0, cut).trim();
    fact.line = fact.raw.slice(cut + 1).trim();
  }

  // 去重：同一事实只说一次
  const seen = new Set<string>();
  return facts.filter((f) => {
    const key = `${f.kind}\u0000${f.forms.join('\u0001')}\u0000${f.file ?? ''}\u0000${f.line ?? ''}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

/** 按句末标点和换行切句，切完再归一化：文件名与行号必须在同一句里对得上。 */
function sentencesOf(text: unknown): string[] {
  return String(text ?? '')
    .split(/[。！？!?；;\n]+/)
    .map((sentence) => normalizeQuote(sentence))
    .filter(Boolean);
}

/** file_line 事实：同一句里同时出现文件名和行号才算命中，不拼成单个 token 比。 */
function fileLineHit(fact: ExtractedFact, text: unknown): boolean {
  const file = normalizeQuote(fact.file);
  const line = normalizeQuote(fact.line);
  if (!file || !line) return false;
  return sentencesOf(text).some((sentence) => sentence.includes(file) && sentence.includes(line));
}

function factInQuotes(fact: ExtractedFact, quotes: string[]): boolean {
  if (fact.kind === 'file_line') return quotes.some((quote) => fileLineHit(fact, quote));
  for (const quote of quotes) {
    const normalized = normalizeQuote(quote);
    if (fact.forms.some((form) => form && normalized.includes(form))) return true;
  }
  return false;
}

/** 第二层：在源条目正文里找（quote 是摘录，模型常把前缀截掉，事实可能落在被截的那段）。 */
function factInSources(fact: ExtractedFact, texts: string[]): boolean {
  if (fact.kind === 'file_line') return texts.some((text) => fileLineHit(fact, text));
  for (const text of texts) {
    const normalized = normalizeQuote(text);
    if (fact.forms.some((form) => form && normalized.includes(form))) return true;
  }
  return false;
}

function idsOfOp(op: MergeOp | null | undefined): string[] {
  return asStringList(op?.ids);
}

/**
 * 证验一批提案。对不上的整条作废，不修不猜。
 *
 * 规则（docs/sleep-pipeline-redesign.md §2.1 + 本票要求）：
 *   1. op.evidence 每条 id 必须在 items 里，quote 必须是该条正文（归一化后）的子串；
 *   2. op.text 里的日期/路径/配置键值/数字，必须能在某条 evidence.quote 里找到，
 *      或在 op.ids 对应源条目正文里找到（2026-11-05 三层校验；两层都没有才 invalid）；
 *   3. op=merge 的 ids 少于 2 个 → invalid；update/archive 必须恰好 1 个 id，add 不许带 id；
 *   4. 正文超过 200 字且 needsSplit!==true → 强制置 true，原文一个字不截；
 *   5. 任何一条 invalid 都落 pending_ops(kind='merge_op') 供人工查，同时写日志。
 *
 * @param {Array} ops MergeAgent 返回的提案
 * @param {Array} items 本批条目（含候选）[{id, text}]
 * @param {object} [opts]
 * @param {number} [opts.maxChars=200] 超长阈值
 * @param {object|import('node:sqlite').DatabaseSync} [opts.db] 给了就落 pending_ops
 * @param {string} [opts.batchId] 批指纹，写进 pending payload
 * @param {(line:string)=>void} [opts.log]
 * @returns {{valid:Array, invalid:Array}}
 */
export function validateOps(ops: unknown, items: MergeItem[], opts: ValidateOpsOptions = {}): ValidateOpsResult {
  const log = typeof opts.log === 'function' ? opts.log : null;
  const maxChars = Number(opts.maxChars) > 0 ? Number(opts.maxChars) : OP_TEXT_MAX;
  const byId = new Map<string, MergeItem>((Array.isArray(items) ? items : []).map((item) => [String(item?.id ?? ''), item]));
  const valid: ValidMergeOp[] = [];
  const invalid: InvalidMergeOp[] = [];
  const list = Array.isArray(ops) ? ops as MergeOp[] : [];

  for (let index = 0; index < list.length; index += 1) {
    const raw = list[index] || ({} as MergeOp);
    const op = String(raw.op ?? '').toLowerCase().trim();
    const ids = idsOfOp(raw);
    const text = String(raw.text ?? '');
    const evidenceOf = (entry: unknown) => (Array.isArray((entry as { evidence?: unknown } | null)?.evidence)
      ? (entry as { evidence: unknown[] }).evidence.map((e) => ({ id: String((e as { id?: unknown })?.id ?? ''), quote: String((e as { quote?: unknown })?.quote ?? '') })).filter((e) => e.quote)
      : []);
    const fail = (reason: string, extra: Record<string, unknown> = {}) => {
      invalid.push({
        index,
        op,
        ids,
        reason,
        ...extra,
        // 原 quote / 原事实一起留档，人工能直接照着核
        quote: extra.quote ?? (Array.isArray(raw.evidence) ? raw.evidence.map((e) => String(e?.quote ?? '')).join(' | ') : ''),
        // 2026-11-05 t-fix-truncate：evidence 原样留档，merge_op 重放要能照着原样再验一遍
        evidence: evidenceOf(raw),
        // 2026-11-06 t-fix-quality：源条目原文一起存，源条后来被改/归档也能照原样再验，
        // 不再因为 source_missing 无脑重试到 dead
        source: ids.map((id) => ({ id, text: String(byId.get(id)?.text ?? '') })),
        text,
      });
    };

    if (!OP_SET.has(op)) {
      fail('op_unknown');
      continue;
    }
    if (op === 'merge' && ids.length < 2) {
      fail('merge_ids_lt_2');
      continue;
    }
    if ((op === 'update' || op === 'archive') && ids.length !== 1) {
      fail('ids_count_not_one');
      continue;
    }
    if (op === 'add' && ids.length > 0) {
      fail('add_ids_not_empty');
      continue;
    }
    for (const id of ids) {
      if (!byId.has(id)) {
        fail('id_not_in_batch', { id });
        break;
      }
    }
    if (invalid.length && invalid[invalid.length - 1].index === index) continue;
    if (op !== 'archive' && !text.trim()) {
      fail('empty_text');
      continue;
    }

    // 证据：id 在本批、quote 是源文子串
    const evidence = Array.isArray(raw.evidence)
      ? (raw.evidence as unknown[]).map((e) => ({ id: String((e as { id?: unknown })?.id ?? ''), quote: String((e as { quote?: unknown })?.quote ?? '') })).filter((e) => e.quote)
      : [];
    if (!evidence.length) {
      fail('evidence_missing');
      continue;
    }
    let evidenceBad = null;
    for (const ev of evidence) {
      const source = byId.get(ev.id);
      if (!source) {
        evidenceBad = { reason: 'evidence_id_not_in_batch', id: ev.id, quote: ev.quote };
        break;
      }
      const haystack = normalizeQuote(source.text);
      if (!haystack.includes(normalizeQuote(ev.quote))) {
        evidenceBad = { reason: 'evidence_quote_not_substring', id: ev.id, quote: ev.quote };
        break;
      }
    }
    if (evidenceBad) {
      fail(evidenceBad.reason, { id: evidenceBad.id, quote: evidenceBad.quote });
      continue;
    }

    // 事实核查（2026-11-05 t-fix-first-run 改三层）：
    //   ① 先在本 op 的 evidence[].quote 里找；
    //   ② 找不到再在 op.ids（含 evidence 点到的 id）对应**源条目正文**里找——
    //      模型把前缀截掉是常事，日期/路径/数字就落在被截掉的那段，首跑因此误杀 4 条；
    //   ③ 两层都没有才判 invalid。
    // 「quote 必须是源文 normalize 后的子串」这条在上一段 evidenceBad 里，一个字都没放松。
    const quotes = evidence.map((e) => e.quote);
    const sourceTexts = [...new Set(
      [...ids, ...evidence.map((e) => e.id)]
        .map((id) => String(byId.get(id)?.text ?? ''))
        .filter(Boolean),
    )];
    const facts = extractFacts(text);
    const missing = facts.find((fact) => !factInQuotes(fact, quotes) && !factInSources(fact, sourceTexts));
    if (missing) {
      fail('fact_not_in_evidence', { fact: missing.raw, factKind: missing.kind, quote: quotes.join(' | ') });
      continue;
    }

    const needsSplit = text.length > maxChars ? true : raw.needsSplit === true;
    valid.push({
      op,
      ids,
      text,
      evidence,
      needsSplit,
      reason: String(raw.reason ?? '').slice(0, 80),
      index,
      // 复核判 keep 的标记要一路带到写库那一步（store 靠它放行误杀条目）。
      // 别写成 `=== true`：有理由时 __lintKept 是字符串，判掉就等于 keep 白判、误杀照样落 discards。
      ...lintMarksOf(raw as unknown as ReviewItem),
    });
  }

  for (const entry of invalid) {
    log?.(
      `[merge] op 作废 op=${entry.op || '-'} ids=${entry.ids.join('+') || '-'} reason=${entry.reason}` +
        ` quote=${JSON.stringify(String(entry.quote || '').slice(0, 120))}` +
        (entry.fact ? ` fact=${JSON.stringify(entry.fact)}` : ''),
    );
  }
  if (invalid.length && opts.db) recordInvalidOps(opts.db, invalid, { batchId: opts.batchId, log });
  return { valid, invalid };
}

/**
 * invalid 落 pending_ops(kind='merge_op')，留给 organizePassAgent 收尾阶段重放（2026-11-05 t-fix-truncate 起）。
 * 重放成功标 done；仍对不上、正文又超长的转 split_item。表缺列或约束不收就只记日志。
 */
export function recordInvalidOps(db: DatabaseSync, invalid: InvalidMergeOp[], { batchId = '', log = null, now = Date.now() }: { batchId?: string; log?: ((line: string) => void) | null; now?: number } = {}): string[] {
  const written = [];
  for (const entry of invalid) {
    // 键序按对象字面量固定（不是排序键）：换成 digestSorted 会让旧行换键、重放永远 miss
    const key = sha1Hex(JSON.stringify({
      op: entry.op, ids: entry.ids, reason: entry.reason, quote: entry.quote, fact: entry.fact || '',
    }));
    const payload = JSON.stringify({
      batch: batchId || null,
      op: entry.op,
      ids: entry.ids,
      reason: entry.reason,
      quote: entry.quote,
      evidence: Array.isArray(entry.evidence) ? entry.evidence : [],
      source: Array.isArray(entry.source) ? entry.source : [],
      fact: entry.fact || null,
      factKind: entry.factKind || null,
      text: String(entry.text || ''),
    });
    try {
      // 老账本不重置 attempts；但同一 dedupe_key 上来了**不同的 payload**（quote/text 变了）
      // 就是新证据，把 dead 拉回 pending、attempts 清零，否则一条死账会把新重放永久卡住。
      db.prepare(`INSERT INTO pending_ops
        (kind, dedupe_key, payload_json, attempts, next_try_at, last_error, status, created_ts, updated_ts)
        VALUES ('merge_op', ?, ?, 0, ?, ?, 'pending', ?, ?)
        ON CONFLICT(kind, dedupe_key) DO UPDATE SET
          payload_json = excluded.payload_json,
          updated_ts = excluded.updated_ts,
          attempts = CASE
            WHEN pending_ops.status = 'dead' AND pending_ops.payload_json <> excluded.payload_json
            THEN 0 ELSE pending_ops.attempts END,
          status = CASE
            WHEN pending_ops.status = 'dead' AND pending_ops.payload_json <> excluded.payload_json
            THEN 'pending' ELSE pending_ops.status END,
          next_try_at = CASE
            WHEN pending_ops.status = 'dead' AND pending_ops.payload_json <> excluded.payload_json
            THEN excluded.updated_ts ELSE pending_ops.next_try_at END`)
        .run(key, payload, now, entry.reason, now, now);
      written.push(key);
    } catch (error) {
      log?.(
        `[merge] merge_op 入队失败（${String((error && (error as Error).message) || error)}）：` +
          `op=${entry.op} ids=${entry.ids.join('+') || '-'} reason=${entry.reason}` +
          '，先跑 scripts/apply-pipeline-schema.mjs 把 pending_ops.kind 补上 merge_op',
      );
    }
  }
  return written;
}

export default merge;
