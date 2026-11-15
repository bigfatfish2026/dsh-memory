/**
 * 取 llm 服务的唯一入口。
 *
 * 为什么不再直接写 `ctx.llm`：
 * dsh-memory 的整理轮是「后台长任务」，一轮能跑十几二十分钟。插件中途被 HMR 重载/卸载后，
 * 旧一轮仍拿着 setup 期的 ctx 继续跑，而那个 ctx 所属的 fiber 已经 dispose：
 * cordis 的 proxy 在 `prop in fiber.inject` 且找不到实现时，抛的就是
 *   cannot get required service "llm" in inactive context
 * （抛出点见 @deepseek-ai/cordis lib/index.js 的 ReflectService.handler.get 里
 *  `error.message = \`cannot get required service "${prop}" in inactive context\``）。
 * 2026-11-14 12:30 那轮接管后正好撞上又一次重载，132 批全打成这个错，merge 0 条。
 *
 * 正确取法：`ctx.get(name)`（ReflectService.get）不要求 inject、也不走「失效即抛」那条分支；
 * 它按服务实现所属 fiber 是否 active 过滤，服务还在就返回实现值。实测（cordis 4.1.0）：
 *   deadCtx.llm          → throw cannot get required service "llm" in inactive context
 *   deadCtx.get('llm')   → 正常返回 llm 服务
 * 这样旧一轮在重载后能接着用仍在线的 llm，不会再整段空转。
 *
 * 真取不到（llm 插件没挂/卸载/不提供 stream）时抛 LlmUnavailableError，调用方据此 fail-loud，
 * 别把一批批失败当成「跑完了」。
 *
 * 2026-11-15：llmOf(ctx, { site, log, cfg }) 会给返回的 llm 包一层 usage 记账
 * （src/llm-usage.ts）。不传 site 就原样返回，测试里的身份断言、直接注入的假 llm 都不受影响。
 */
import { tapLlmUsage } from './llm-usage.js';

/** stream 的入参：provider/model 由调用点给，其余字段照旧透传给宿主。 */
export interface LlmStreamOptions {
  provider?: string;
  model?: string;
  [key: string]: unknown;
}

/** llm 服务的最小面：只保证有 stream，别的属性原样透传。 */
export interface LlmServiceLike {
  stream: (options?: LlmStreamOptions) => unknown;
  [key: string]: unknown;
}

/** ctx 里这一层真正读到的字段；桩 ctx / 失效 ctx 取不到就按缺省处理。 */
export interface LlmContextLike {
  get?: (name: string) => unknown;
  llm?: unknown;
  fiber?: { uid?: number | null; state?: unknown };
}

/** llmOf 的可选参数：给了 site 才包 usage 记账。 */
export interface LlmOfOptions {
  site?: string;
  log?: (line: string) => void;
  cfg?: unknown;
  provider?: string;
}

export interface LlmUnavailableOptions {
  ctxState?: string;
  cause?: { message?: string } | null;
}

/** llm 服务取不到时抛这个；调用方按 code 判，不靠 message 文本。 */
export class LlmUnavailableError extends Error {
  declare code: string;
  declare service: string;
  declare ctxState: string;

  constructor(message: string, { ctxState = '', cause = null }: LlmUnavailableOptions = {}) {
    super(message);
    this.name = 'LlmUnavailableError';
    this.code = 'LLM_UNAVAILABLE';
    this.service = 'llm';
    this.ctxState = ctxState;
    if (cause) this.cause = cause;
  }
}

/**
 * ctx 的状态，写进日志用。失效的 fiber：uid=null、state 已不是 2（active）。
 * 拿不到 fiber 的假 ctx/桩 ctx 也不抛，返回可读文本即可。
 */
export function contextStateOf(ctx: unknown): string {
  try {
    const fiber = (ctx as LlmContextLike | null | undefined)?.fiber;
    if (!fiber) return 'fiber=未知';
    const uid = fiber.uid === null ? 'null(已失效)' : String(fiber.uid);
    return `fiber.uid=${uid} fiber.state=${String(fiber.state)}`;
  } catch {
    return 'fiber=读取失败';
  }
}

/**
 * 每次用 llm 时现取，别在 setup 期把服务值存下来。
 *
 * @param ctx 插件 ctx（活的、失效的都可以；失效时靠 ctx.get 仍能拿到在线服务）
 * @param opts site 给了才包 usage 记账（不给=原样返回）；log/cfg 决定 usage 行写哪
 * @returns llm 服务
 * @throws {LlmUnavailableError} 服务不在、或没有 stream 方法
 */
export function llmOf(ctx: unknown, opts: LlmOfOptions = {}): LlmServiceLike {
  const ctxState = contextStateOf(ctx);
  let llm: LlmServiceLike | undefined;
  let cause: { message?: string } | null = null;
  if (ctx && typeof (ctx as LlmContextLike).get === 'function') {
    try {
      llm = ((ctx as LlmContextLike).get as (name: string) => unknown)('llm') as LlmServiceLike | undefined;
    } catch (error) {
      cause = error as { message?: string };
      llm = undefined;
    }
  }
  if (!llm && ctx) {
    // 兜底：测试桩/老接口的假 ctx 只有 llm 属性；真实 ctx 若把 llm 提供在 isolate 作用域，
    // root 视角的 ctx.get 看不到，属性读取仍是对的。失效 ctx 这里会抛，被吞进 cause。
    try {
      llm = (ctx as LlmContextLike).llm as LlmServiceLike | undefined;
    } catch (error) {
      cause ||= error as { message?: string };
    }
  }
  if (!llm) {
    throw new LlmUnavailableError(
      `取不到服务 "llm"（ctx ${ctxState}）${cause ? `：${cause.message}` : ''}`,
      { ctxState, cause },
    );
  }
  if (typeof llm.stream !== 'function') {
    throw new LlmUnavailableError(`服务 "llm" 没有 stream 方法（ctx ${ctxState}）`, { ctxState });
  }
  if (!opts?.site) return llm;
  return tapLlmUsage(llm, { ...opts, provider: opts.provider || '' });
}

/** 这个错误是不是「llm 取不到」。兼容别处仍以 ctx.llm 触发时 cordis 的原文。 */
export function isLlmUnavailable(error: unknown): boolean {
  if (!error) return false;
  if (error instanceof LlmUnavailableError) return true;
  if ((error as { code?: unknown }).code === 'LLM_UNAVAILABLE') return true;
  return /cannot get required service "llm" in inactive context/.test(String((error as { message?: unknown }).message || error));
}
