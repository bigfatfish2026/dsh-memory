/**
 * 每次 LLM 调用的 usage 账。
 *
 * 以前只有 consolidate 的失败摘要里拼 in/out/cacheRead，成功路径一行都不记——
 * 12:30 那轮日志里 0 条 usage，花了多少 token 事后查不出来。现在改成：
 *   - 在 llm-service.llmOf(ctx, { site, log, cfg }) 这一层包一层流（所有调用点共用）；
 *   - 每次调用收尾（成功 / 抛错 / 提前 return）都写一行，固定可 grep：
 *       `[usage] site=split provider=magpie model=group/ds in=123 out=45 cacheRead=0 reason=0 ms=12345 ok=1`
 *   - 拿不到 usage 的宿主也记这一行（数值写 `?`），至少调用次数和耗时不会丢。
 *
 * 只在「调用方明确给了 log 或 cfg」时才包：测试里的假 llm 直接传进来，不经过这里，
 * 也不会往真实日志文件写东西。
 */
import { keyLog } from './file-log.js';
import type { KeyLogConfig } from './file-log.js';

/** 一行 usage 里认得出的四个数；认不出的字段是 null。 */
export interface UsageNumbers {
  input?: number | null;
  output?: number | null;
  cacheRead?: number | null;
  reasoning?: number | null;
}

/** stream 的入参：provider/model 由调用点给，其余字段照旧透传给宿主。 */
export interface LlmStreamOptions {
  provider?: string;
  model?: string;
  [key: string]: unknown;
}

/** 只要能 stream 的服务就能包；其余属性原样透传。 */
export interface LlmUsageTarget {
  stream: (options?: LlmStreamOptions) => unknown;
  [key: string]: unknown;
}

/** tapLlmUsage 的记账上下文：site/provider/model 给日志，log 或 cfg 决定往哪写。 */
export interface LlmUsageMeta {
  site?: string;
  provider?: string;
  model?: string;
  log?: (line: string) => void;
  cfg?: unknown;
}

/** formatUsageLine 的入参。 */
export interface UsageLineInput {
  site?: string;
  provider?: string;
  model?: string;
  usage?: UsageNumbers | null;
  ms?: number;
  ok?: boolean;
  error?: { message?: unknown } | null;
}

function numOf(value: unknown): number | null {
  return Number.isFinite(Number(value)) && value !== null && value !== undefined ? Number(value) : null;
}

/** 从流 chunk 里认 usage。不同宿主字段名不一样，这里一次认全。 */
export function usageFrom(chunk: unknown): UsageNumbers | null {
  const usage = (chunk as { usage?: Record<string, unknown> | null; reason?: { usage?: Record<string, unknown> | null } | null } | null | undefined)?.usage || (chunk as { reason?: { usage?: Record<string, unknown> | null } | null } | null | undefined)?.reason?.usage || null;
  if (!usage) return null;
  const input = numOf(usage.inputTokens ?? usage.promptTokens ?? usage.input_tokens ?? usage.prompt_tokens);
  const output = numOf(
    usage.outputTokens ?? usage.completionTokens ?? usage.output_tokens ?? usage.completion_tokens,
  );
  const cacheRead = numOf(
    usage.cacheReadTokens ?? usage.cache_read_tokens ?? usage.cacheReadInputTokens
      ?? usage.cache_read_input_tokens ?? usage.promptCacheHitTokens ?? usage.cachedTokens,
  );
  const reasoning = numOf(usage.reasoningTokens ?? usage.reasoning_tokens);
  if (input === null && output === null && cacheRead === null && reasoning === null) return null;
  return { input, output, cacheRead, reasoning };
}

/** 多轮 chunk 的 usage 合并：后到的非空字段覆盖前面的，缺的保持。 */
export function mergeUsage(base: UsageNumbers | null, next: UsageNumbers | null): UsageNumbers | null {
  if (!next) return base;
  if (!base) return { ...next };
  const out = { ...base };
  for (const key of ['input', 'output', 'cacheRead', 'reasoning'] as const) {
    if (next[key] !== null && next[key] !== undefined) out[key] = next[key];
  }
  return out;
}

const show = (value: unknown): string => (value === null || value === undefined || value === '' ? '?' : String(value));

/** 一行 usage。site 是调用点（merge / split / consolidate / sleep:askModel / write_gate …）。 */
export function formatUsageLine({ site = 'unknown', provider = '', model = '', usage = null, ms = 0, ok = true, error = null }: UsageLineInput = {}): string {
  const u: UsageNumbers = usage || {};
  const err = error ? ` error=${String((error && error.message) || error).replace(/\s+/g, ' ').slice(0, 160)}` : '';
  return `[usage] site=${site} provider=${show(provider)} model=${show(model)}` +
    ` in=${show(u.input)} out=${show(u.output)} cacheRead=${show(u.cacheRead)}` +
    ` reason=${show(u.reasoning)} ms=${Math.max(0, Number(ms) || 0)} ok=${ok ? 1 : 0}${err}`;
}

/**
 * 给 llm 包一层 usage 记账，返回代理对象。只有 stream 被换掉，其余属性/方法原样透传。
 * @param llm llm 服务
 * @param meta { site, provider, model, log?, cfg? }
 */
export function tapLlmUsage<T extends LlmUsageTarget>(llm: T, meta?: LlmUsageMeta): T;
export function tapLlmUsage(llm: null | undefined, meta?: LlmUsageMeta): null | undefined;
export function tapLlmUsage<T extends LlmUsageTarget>(
  llm: T | null | undefined,
  meta: LlmUsageMeta = {},
): T | null | undefined {
  if (!llm || typeof llm.stream !== 'function') return llm;
  const emit = typeof meta.log === 'function'
    ? meta.log
    : (meta.cfg ? (line: string) => keyLog(meta.cfg as KeyLogConfig | null | undefined, line) : null);
  if (!emit) return llm;
  return new Proxy(llm, {
    get(target, prop) {
      if (prop !== 'stream') return Reflect.get(target, prop, target);
      return function tappedStream(options: LlmStreamOptions = {}) {
        const base = target.stream.call(target, options);
        return (async function* usageTapped() {
          const startedAt = Date.now();
          let usage: UsageNumbers | null = null;
          let ok = true;
          let failure: { message?: unknown } | null = null;
          try {
            for await (const chunk of base as AsyncIterable<unknown>) {
              usage = mergeUsage(usage, usageFrom(chunk));
              yield chunk;
            }
          } catch (error) {
            ok = false;
            failure = error as { message?: unknown } | null;
            throw error;
          } finally {
            try {
              emit(formatUsageLine({
                site: meta.site,
                provider: options.provider ?? meta.provider,
                model: options.model ?? meta.model,
                usage,
                ms: Date.now() - startedAt,
                ok,
                error: failure,
              }));
            } catch {
              /* usage 记账不许反过来影响模型调用 */
            }
          }
        })();
      };
    },
  });
}

export default tapLlmUsage;
