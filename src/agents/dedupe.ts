/**
 * DedupeAgent（2026-11-05）。
 *
 * 对一条待写候选做最后裁决，提示词逐字来自 docs/agent-prompts.md「去重 agent」。
 * 候选召回与规则层复用 src/write-gate.ts 的 findCandidates / judge，这里只补
 * 模型那一层：双通道结构化输出（submit_verdict 工具流 + 文本 JSON 回退）。
 *
 * 宿主没有 toolChoice，所以工具通道只能「看流里有没有出现 submit_verdict」：
 *   - tool-call-delta 里出现 → 按 index 拼 argumentsDelta，拼完解析；
 *   - 没出现 / 拼出来不是合法 JSON → 回退解析正文 JSON；
 *   - 两条都坏 → 再问一次（总共最多 2 次），仍坏返回 undefined，由上层落 pending。
 * 绝不降级成 DISTINCT 直接放行。
 *
 * 返回 {verdict, matchedId, reason, source, hints}：
 *   source='tool'  工具通道
 *   source='text'  文本 JSON 回退
 *   source='rule'  规则层直接命中（字面 hash 完全相同 / 相似度没到阈值），没花模型调用
 */
import type { DatabaseSync } from 'node:sqlite';
import { findCandidates, judge } from '../write-gate.js';
import type { CandidateHints, GateStore, GateVerdict, KnnFn } from '../write-gate.js';
import { maxTokensOption } from '../llm-params.js';
import { SELF_CONTAINED_RULES } from '../lint-prompt.js';
import { parseJsonLoose } from '../shared/json.js';

/** 待裁决的候选：只读这些键，其余不碰。 */
export interface DedupeCandidate {
  text?: unknown;
  type?: unknown;
  tags?: unknown;
  entities?: string[] | null;
  source?: unknown;
  sourceFile?: unknown;
}

/** 一个通道的失败记录：source 是哪条通道，rawHead 是模型原文前 120 字符。 */
export interface DedupeAttemptNote {
  source?: unknown;
  rawHead?: unknown;
  error?: unknown;
}

/** 未决诊断：一次裁决里每个通道的失败原文（写日志 / 落 pending 都用它）。 */
export interface DedupeDiagnosticsLike {
  attempts?: DedupeAttemptNote[];
  lastError?: unknown;
}

/** 注入的 llm 面：只保证可能有 stream（store 侧传的是 StoreLlm，测试里是假 llm）。 */
export interface DedupeLlmLike {
  stream?: unknown;
  [key: string]: unknown;
}

/** 提示词里的召回行（三路候选合成一份）。 */
export interface DedupeRecallEntry {
  id: string;
  text: string;
  score: number;
  status: string;
}

/** 裁决结果：source 标记哪条通道给的（tool / text / rule）。 */
export interface DedupeVerdict {
  verdict: string;
  matchedId: string | null;
  reason: string;
  source: string;
}

/** dedupe() 的完整产物：裁决 + 这次用的召回。 */
export interface DedupeResult extends DedupeVerdict {
  hints: CandidateHints;
}

/** dedupe() 的入参。 */
export interface DedupeOptions {
  target?: unknown;
  candidate?: DedupeCandidate | null;
  llm?: DedupeLlmLike | null;
  provider?: string;
  model?: string;
  log?: ((line: string) => void) | null;
  threshold?: number;
  k?: number;
  vector?: Float32Array | number[];
  embed?: (text: string) => Promise<Float32Array | number[]> | Float32Array | number[];
  knn?: KnnFn;
  excludeIds?: string[];
  maxTokens?: number;
  diagnostics?: DedupeDiagnosticsLike;
}

/** 流式 chunk 的最小面（工具通道 + 正文通道）。 */
interface DedupeStreamChunk {
  type?: string;
  text?: string;
  index?: number;
  id?: string;
  name?: string;
  argumentsDelta?: string;
  arguments?: unknown;
  reason?: { kind?: string; failure?: unknown };
  [key: string]: unknown;
}

/** 一次工具调用在流里的拼装态。 */
interface ToolPartial {
  id?: string;
  name: string;
  args: string;
}

export const DEDUPE_SYSTEM = `你是记忆去重判定员。给你一条待写入的候选和它召回的近似条目，判断是否重复。
同事实不同措辞 → DUPLICATE
同事实但候选是更新版本（旧值被新值取代）→ SUPERSEDES
只是主题相关、事实不同 → DISTINCT
判不准一律 DISTINCT，不许保守丢数据。matchedId 只在 DUPLICATE 和 SUPERSEDES 时给。
只输出一行 JSON。

${SELF_CONTAINED_RULES}`;

export const DEDUPE_USER_TEMPLATE = `候选：{text}（{type}，entities: {entities}）
召回（相似度降序）：[{id, text, score, status}]

输出
{"verdict":"DUPLICATE|SUPERSEDES|DISTINCT","matchedId":"","reason":"不超过40字"}`;

/** 工具通道的 schema：宿主没有 toolChoice，只能靠工具名 + enum 收窄输出。 */
export const SUBMIT_VERDICT_TOOL = {
  name: 'submit_verdict',
  description: '提交去重裁决。只调用这个工具，不要输出正文。',
  parameters: {
    type: 'object',
    additionalProperties: false,
    properties: {
      verdict: {
        type: 'string',
        enum: ['DUPLICATE', 'SUPERSEDES', 'DISTINCT'],
        description: 'DUPLICATE=同一事实重复表述；SUPERSEDES=候选是更新版本；DISTINCT=事实不同',
      },
      matchedId: { type: 'string', description: 'DUPLICATE/SUPERSEDES 时填召回里的条目 id，其余留空' },
      reason: { type: 'string', maxLength: 40, description: '不超过 40 字的理由' },
    },
    required: ['verdict'],
  },
};

const VERDICTS = new Set(['DUPLICATE', 'SUPERSEDES', 'DISTINCT']);

/** 校验并收到最小形状；verdict 非法直接抛错，交给上层重试。 */
function parseVerdict(raw: unknown): { verdict: string; matchedId: string | null; reason: string } {
  const value = parseJsonLoose(raw) as { verdict?: unknown; matchedId?: unknown; reason?: unknown } | null;
  const verdict = String(value?.verdict || '').toUpperCase().trim();
  if (!VERDICTS.has(verdict)) throw new Error(`verdict 非法：${value?.verdict}`);
  const matchedId = value?.matchedId === undefined || value?.matchedId === null || value?.matchedId === ''
    ? null
    : String(value.matchedId);
  const reason = String(value?.reason ?? '').slice(0, 40);
  return { verdict, matchedId, reason };
}

function dbOf(target: unknown): DatabaseSync | null {
  if (target && typeof (target as DatabaseSync).prepare === 'function') return target as DatabaseSync;
  if (target && typeof (target as GateStore).db === 'function') return ((target as GateStore).db as () => DatabaseSync)();
  if (target && (target as GateStore).db && typeof ((target as GateStore).db as DatabaseSync).prepare === 'function') return (target as GateStore).db as DatabaseSync;
  return null;
}

/** 给提示词的召回列表：knn / related / hash 合成一份，带 id、text、score、status。 */
function recallList(target: unknown, hints: CandidateHints): DedupeRecallEntry[] {
  const db = dbOf(target);
  const meta = new Map<string, { text: unknown; status: unknown }>();
  if (db) {
    try {
      for (const row of db.prepare('SELECT id, text, status FROM memories').all() as Array<{ id: unknown; text: unknown; status: unknown }>) {
        meta.set(String(row.id), { text: row.text, status: row.status || 'active' });
      }
    } catch {
      /* 只影响提示词里的 status，不影响裁决 */
    }
  }
  const out = new Map<string, DedupeRecallEntry>();
  const push = (id: unknown, text: unknown, score: unknown): void => {
    const key = String(id);
    const known = meta.get(key);
    const entry = {
      id: key,
      text: String(text ?? known?.text ?? ''),
      score: Number(score ?? 0),
      status: String(known?.status || 'active'),
    };
    const prev = out.get(key);
    if (!prev || entry.score > prev.score) out.set(key, entry);
  };
  for (const hit of hints?.hash || []) push(hit.id, hit.text, 1);
  for (const hit of hints?.knn || []) push(hit.id, (hit as { text?: unknown }).text, hit.similarity);
  for (const hit of hints?.related || []) push(hit.id, hit.text, hit.score);
  return [...out.values()].sort((a, b) => b.score - a.score);
}

function buildUserPrompt(candidate: DedupeCandidate | null | undefined, recall: DedupeRecallEntry[]): string {
  const text = String(candidate?.text ?? '').trim();
  const type = String(candidate?.type || 'world');
  const entities = (candidate?.entities || []).map(String).filter(Boolean).join(', ') || '（空）';
  const list = JSON.stringify(recall.map((r) => ({
    id: r.id,
    text: r.text,
    score: Number(r.score.toFixed(4)),
    status: r.status,
  })));
  return DEDUPE_USER_TEMPLATE
    .replace('{text}', text)
    .replace('{type}', type)
    .replace('{entities}', entities)
    .replace('[{id, text, score, status}]', list);
}

/**
 * 一次模型调用，双通道取裁决。
 * @returns {Promise<{raw:object, source:'tool'|'text'}|null>} 两条通道都坏就是 null
 */
async function askVerdict({ llm, provider, model, candidate, recall, log, diagnostics, maxTokens = 0 }: {
  llm: DedupeLlmLike | null | undefined;
  provider?: string;
  model?: string;
  candidate: DedupeCandidate | null | undefined;
  recall: DedupeRecallEntry[];
  log?: ((line: string) => void) | null;
  diagnostics?: DedupeDiagnosticsLike;
  maxTokens?: number;
}): Promise<{ raw: { verdict: string; matchedId: string | null; reason: string }; source: 'tool' | 'text' } | null> {
  if (!llm || typeof llm.stream !== 'function') throw new Error('dedupe: llm 不可用');
  const prompt = buildUserPrompt(candidate, recall);
  const stream = (llm.stream as (options: Record<string, unknown>) => AsyncIterable<DedupeStreamChunk>)({
    provider,
    model,
    system: DEDUPE_SYSTEM,
    messages: [{ role: 'user', content: [{ type: 'text', text: prompt }] }],
    tools: [SUBMIT_VERDICT_TOOL],
    reasoningEffort: 'low',
    // 2026-11-06 维护者硬规矩：默认 0 = 不传 maxTokens，由 provider 封顶。以前写死 200/300
    // 都被思考吃光、判「未决」；裁决要短靠提示词约束，不靠 token 上限。
    // 只在排障时显式压一个小值，统一走 maxTokensOption。
    ...(maxTokensOption as (maxTokens?: number) => Record<string, never>)(maxTokens),
    temperature: 0,
  });

  let text = '';
  let complete: string | null = null;
  const partials = new Map<number, ToolPartial>();
  for await (const chunk of stream) {
    if (chunk?.type === 'text-delta') {
      text += chunk.text ?? '';
    } else if (chunk?.type === 'tool-call-delta') {
      const index = chunk.index ?? 0;
      const partial = partials.get(index) ?? { id: chunk.id, name: '', args: '' };
      if (chunk.id) partial.id = chunk.id;
      if (chunk.name) partial.name = chunk.name;
      partial.args += chunk.argumentsDelta ?? '';
      partials.set(index, partial);
    } else if (chunk?.type === 'tool-call') {
      if (String(chunk.name || '') === SUBMIT_VERDICT_TOOL.name) {
        complete = typeof chunk.arguments === 'string' ? chunk.arguments : JSON.stringify(chunk.arguments ?? {});
      }
    } else if (chunk?.type === 'finish' && chunk.reason?.kind === 'error') {
      throw new Error(`dedupe: 模型调用失败：${JSON.stringify(chunk.reason.failure || {})}`);
    }
  }

  // 通道一：工具流里出现过 submit_verdict
  const toolArgs = complete
    ?? [...partials.values()].find((p) => String(p.name || '') === SUBMIT_VERDICT_TOOL.name)?.args;
  // 未决时要能判：哪个通道、模型原文前 120 字符。diagnostics.attempts 带回去给上层日志和 pending payload。
  const note = (source: string, raw: unknown, error: unknown): void => {
    const head = String(raw ?? '').replace(/\s+/g, ' ').slice(0, 120);
    diagnostics?.attempts?.push({ source, rawHead: head, error: (error as { message?: string } | null | undefined)?.message || '' });
    log?.(`dedupe: ${source} 通道解析失败（${(error as { message?: string } | null | undefined)?.message || '空输出'}）raw=${JSON.stringify(head)}`);
  };
  if (toolArgs) {
    try {
      return { raw: parseVerdict(toolArgs), source: 'tool' };
    } catch (error) {
      note('tool', toolArgs, error);
    }
  }

  // 通道二：正文 JSON
  if (String(text).trim()) {
    try {
      return { raw: parseVerdict(text), source: 'text' };
    } catch (error) {
      note('text', text, error);
    }
  } else {
    note('text', '', new Error(toolArgs ? '工具通道坏了、正文又是空的' : '两个通道都没有输出'));
  }

  return null;
}

/**
 * 跑一次去重裁决。候选召回和规则层都在 src/write-gate.ts。
 *
 * @param {object} opts
 * @param {object|import('node:sqlite').DatabaseSync} opts.target store 或 sqlite 连接
 * @param {object} opts.candidate {text, type?, entities?, tags?, source?, sourceFile?}
 * @param {object} opts.llm ctx.llm（或测试里的假 llm）
 * @param {string} [opts.provider] 模型 provider
 * @param {string} [opts.model] 模型名
 * @param {number} [opts.threshold=0.98] judge 的余弦阈值，≥ 才问模型
 * @param {number} [opts.k=10] knn 取几条
 * @param {Float32Array|number[]} [opts.vector] 预计算向量
 * @param {(texts:string[])=>Promise<number[][]>} [opts.embed] 向量化函数
 * @param {(vec:number[], k:number)=>Array<{id:string,similarity:number}>} [opts.knn]
 * @param {(line:string)=>void} [opts.log]
 * @param {number} [opts.maxTokens=0] 显式输出上限；0/不填 = 不传，由 provider 封顶
 * @returns {Promise<{verdict:string, matchedId:string|null, reason:string, source:string, hints:object}|undefined>}
 */
export async function dedupe(opts: DedupeOptions = {}): Promise<DedupeResult | undefined> {
  const {
    target, candidate, llm, provider, model, log,
    threshold = 0.98, k = 10, vector, embed, knn, excludeIds, maxTokens = 0,
  } = opts;
  // 未决时把「哪个通道、模型原文前 120 字符」带回去：调用方写日志 / 落 pending payload 都用这一份。
  const diagnostics: DedupeDiagnosticsLike & { attempts: DedupeAttemptNote[] } = opts.diagnostics && typeof opts.diagnostics === 'object' ? opts.diagnostics as DedupeDiagnosticsLike & { attempts: DedupeAttemptNote[] } : {} as DedupeDiagnosticsLike & { attempts: DedupeAttemptNote[] };
  diagnostics.attempts = [];
  const text = String(candidate?.text ?? '').trim();
  if (!text) throw new Error('dedupe: candidate.text 为空');

  const hints = await findCandidates(target as GateStore, text, {
    k,
    vector,
    embed,
    knn,
    excludeIds,
    entities: candidate?.entities || [],
  });

  let lastError: unknown = null;
  for (let attempt = 1; attempt <= 2; attempt += 1) {
    let source = 'rule';
    try {
      const result = await judge(
        { ...candidate, text },
        hints,
        {
          threshold,
          llm: async (input: { candidate: unknown; recall: unknown }) => {
            const answer = await askVerdict({
              llm,
              provider,
              model,
              candidate: input.candidate as DedupeCandidate,
              recall: recallList(target, input.recall as CandidateHints ?? hints),
              log,
              diagnostics,
              maxTokens,
            });
            if (!answer) {
              const error = new Error('双通道都没拿到裁决') as Error & { code?: string };
              error.code = 'DEDUPE_NO_VERDICT';
              throw error;
            }
            source = answer.source;
            return answer.raw;
          },
        },
      ) as GateVerdict & { source?: string };
      return {
        verdict: result.verdict,
        matchedId: result.matchedId ?? null,
        reason: result.reason || '',
        source: result.source || source,
        hints,
      };
    } catch (error) {
      lastError = error;
      log?.(`dedupe: 第 ${attempt} 次裁决失败：${(error as { message?: string }).message}`);
    }
  }

  const last = diagnostics.attempts[diagnostics.attempts.length - 1] || {};
  diagnostics.lastError = (lastError as { message?: string } | null | undefined)?.message || '';
  log?.(
    `dedupe: 连续 2 次没拿到裁决，交上层落 pending（source=${last.source || '-'}` +
      ` raw=${JSON.stringify(last.rawHead || '')} err=${(lastError as { message?: string } | null | undefined)?.message || '未知'}）`,
  );
  return undefined;
}

export default dedupe;
