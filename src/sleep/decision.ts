/**
 * 冲突预判的 decision 桥客户端（2026-11-20）。
 *
 * 入口冲突检查（model-calls.ts 的 runConflictCheck）现在逐条问生成模型：今天实测 359 次，
 * 其中 81.1% 判 new，这些条白白花掉一次几十秒的生成调用。这里把「这条新记忆跟这些同实体
 * 旧条都不冲突、可以直接作为新条入库」交给本地 decision 桥
 * （/home/julia/fatfish/tools/gemini-decision-bridge，默认 127.0.0.1:3427）批量判：
 * 一批一次请求，高置信判 new 的直接放行，其余退回原来的 askConflict。
 *
 * 桥的硬约束（都实测过）：
 *   - questions 必须是对象 map（qid→题目），传数组直接 400；
 *   - 每道题必须是对象 `{ type: 'choice', instructions, criteria }`，传字符串 400
 *     （上游原话：Expected an object, got string / The 'type' parameter is required）；
 *   - answers 不保序，必须按 qid 取；置信度是 probabilities[choice]，不是顶层 confidence；
 *   - 批内共享 state 会互相污染（16 条批判 redundant 从 25% 涨到 56%），所以一批硬上限 10 条；
 *   - Free Tier 每分钟 5 万 input token（6 个 16 条批全 429），8 路 ingest 文件池同时打必然超，
 *     这里用进程级串行闸，一次只放一个在途请求。
 *
 * 失败口径只有一条：整批降级。非 200 / 超时 / 网络错 / JSON 坏 / 缺 qid / 概率非法，
 * 都返回空 decided，调用方把这批全部退回 askConflict，主流程与没加这层一样。
 */
const DECIDE_PATH = '/systemone';
const DEFAULT_BASE_URL = 'http://127.0.0.1:3427';
const DEFAULT_MODEL = 'gemini-decision-1';
const DEFAULT_TIMEOUT_MS = 2500;
const DEFAULT_MIN_CONFIDENCE = 0.9;
const DEFAULT_BATCH_SIZE = 8;
/** 一批硬上限：批内共享 state，条数越多互相污染越重。 */
export const DECIDE_BATCH_CAP = 10;
/** 题目里的正文裁剪：只为控 token，判定只分 new / not_new，不需要全文。 */
const NEW_TEXT_LIMIT = 300;
const OLD_TEXT_LIMIT = 200;
const OLD_ITEMS_LIMIT = 6;

/** 所有题目共用的 state：只放通用判据，不放任何一条记忆正文（避免跨题污染）。 */
const DECIDE_STATE = [
  '你在做长期记忆入库前的冲突预判。下面每个问题都给一条准备写入的新记忆和若干同实体的旧记忆。',
  '判断这条新记忆跟这些旧记忆是不是同一属性、同一件事的另一个说法：',
  '如果可能冗余、是旧值被新值替代（状态变更）、或与旧记忆矛盾，答 not_new；',
  '如果讲的是不同属性/不同事情、可以跟旧记忆并存入库，答 new。',
  '每题只看该题给出的内容，不要跨题参考，也不要臆造题目里没有的事实。',
].join('\n');

/** conflictDecide 配置（schema 在 src/index.ts，默认值只在那里写一份）。 */
export interface ConflictDecideCfg {
  enabled: boolean;
  batchSize: number;
  minConfidence: number;
  timeoutMs: number;
  baseUrl: string;
  model: string;
}

/** 待判的一条：新记忆正文 + 同实体旧记忆（只要 id/text/validFrom）。 */
export interface DecideCandidate {
  text: string;
  related: Array<{ id?: unknown; text?: unknown; validFrom?: unknown }>;
}

/** 一批的判定结果：decided 是 candidates 的下标；降级时为空集。 */
export interface DecideBatchResult {
  decided: Set<number>;
  degraded: number;
  /** ok / http_429 / timeout / network / bad_body / missing_answer / bad_choice / bad_confidence */
  reason: string;
  ms: number;
}

function finiteOr(value: unknown, fallback: number): number {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

/** 配置归一化：开关只认严格 true（yml 里写成字符串不许误开），数值越界按硬边界夹。 */
export function conflictDecideCfgOf(raw: unknown): ConflictDecideCfg {
  const src = (raw && typeof raw === 'object' ? raw : {}) as Record<string, unknown>;
  const baseUrl = String(src.baseUrl ?? '').trim() || DEFAULT_BASE_URL;
  const model = String(src.model ?? '').trim() || DEFAULT_MODEL;
  return {
    enabled: src.enabled === true,
    batchSize: Math.min(DECIDE_BATCH_CAP, Math.max(1, Math.floor(finiteOr(src.batchSize, DEFAULT_BATCH_SIZE)))),
    minConfidence: Math.min(1, Math.max(0, finiteOr(src.minConfidence, DEFAULT_MIN_CONFIDENCE))),
    timeoutMs: Math.max(200, Math.floor(finiteOr(src.timeoutMs, DEFAULT_TIMEOUT_MS))),
    baseUrl: baseUrl.replace(/\/+$/, ''),
    model,
  };
}

function clip(value: unknown, limit: number): string {
  const text = String(value ?? '').replace(/\s+/g, ' ').trim();
  return text.length > limit ? `${text.slice(0, limit)}…` : text;
}

function qidOf(index: number): string {
  return `c${index}`;
}

/** 一道题：桥只认对象型 choice 题；criteria 的键就是 answer.choice 的候选值。 */
export interface DecideQuestion {
  type: 'choice';
  instructions: string;
  criteria: Record<string, string>;
}

const CRITERIA_NEW = '新记忆与旧记忆可并存：讲的是不同属性或不同事情，不构成冗余、替代或矛盾。';
const CRITERIA_NOT_NEW = '新记忆与旧记忆有替代或矛盾：可能冗余、旧值被新值替代，或与旧记忆冲突，需要进一步判断。';

/** 一道题：新记忆 + 这 6 条最相似的旧记忆，选项固定成 new / not_new 两个词。 */
function questionOf(candidate: DecideCandidate): DecideQuestion {
  const old = candidate.related.slice(0, OLD_ITEMS_LIMIT)
    .map((m) => {
      const from = m.validFrom ? `from=${String(m.validFrom)} ` : '';
      return `- ${from}${clip(m.text, OLD_TEXT_LIMIT)}`;
    })
    .join('\n');
  return {
    type: 'choice',
    instructions: `新记忆：${clip(candidate.text, NEW_TEXT_LIMIT)}\n\n同实体的旧记忆：\n${old}\n\n`
      + '这条新记忆跟上面的旧记忆可以并存入库吗？',
    criteria: { new: CRITERIA_NEW, not_new: CRITERIA_NOT_NEW },
  };
}

/**
 * 进程级串行闸：桥按每分钟 token 配额限流，ingest 文件池 8 路一起打就是 8 个请求，
 * 必然 429。排队等待不计入单批超时（超时从请求发出算），所以晚到的批只会慢一点，不会被误降级。
 */
let inFlight: Promise<unknown> = Promise.resolve();
function serialize<T>(fn: () => Promise<T>): Promise<T> {
  const run = inFlight.then(fn, fn);
  inFlight = run.then(() => undefined, () => undefined);
  return run;
}

function degrade(count: number, reason: string, startedAt: number): DecideBatchResult {
  return { decided: new Set(), degraded: count, reason, ms: Date.now() - startedAt };
}

/** 失败分类：只认这几种，调用方拿去打日志，不参与重试决策。 */
function reasonOfError(error: unknown): string {
  const name = String((error as { name?: unknown } | null)?.name || '');
  return name === 'TimeoutError' || name === 'AbortError' ? 'timeout' : 'network';
}

async function postDecisions(
  url: string,
  body: unknown,
  timeoutMs: number,
): Promise<{ status: number; text: string }> {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const resp = await fetch(url, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(body),
      signal: ctrl.signal,
    });
    return { status: resp.status, text: await resp.text() };
  } finally {
    clearTimeout(timer);
  }
}

/**
 * 判一批（≤ DECIDE_BATCH_CAP 条）。不重试：桥抖一下，下次整理再判，正确性由 askConflict 兜底。
 * 任何异常都在这里收成降级结果，不往外抛。
 */
export async function decideNewBatch(
  candidates: DecideCandidate[],
  cfg: ConflictDecideCfg,
): Promise<DecideBatchResult> {
  const startedAt = Date.now();
  const count = Math.min(candidates.length, DECIDE_BATCH_CAP);
  if (!count) return { decided: new Set(), degraded: 0, reason: 'ok', ms: 0 };
  const questions: Record<string, DecideQuestion> = {};
  for (let i = 0; i < count; i += 1) questions[qidOf(i)] = questionOf(candidates[i]);
  const url = `${cfg.baseUrl}${DECIDE_PATH}`;

  let resp: { status: number; text: string };
  try {
    resp = await serialize(() => postDecisions(
      url,
      { model: cfg.model, state: DECIDE_STATE, questions },
      cfg.timeoutMs,
    ));
  } catch (error) {
    return degrade(count, reasonOfError(error), startedAt);
  }
  if (resp.status !== 200) return degrade(count, `http_${resp.status}`, startedAt);

  let parsed: unknown;
  try {
    parsed = JSON.parse(resp.text);
  } catch {
    return degrade(count, 'bad_body', startedAt);
  }
  const answers = (parsed as { answers?: unknown } | null)?.answers;
  if (!answers || typeof answers !== 'object' || Array.isArray(answers)) {
    return degrade(count, 'bad_body', startedAt);
  }

  const decided = new Set<number>();
  for (let i = 0; i < count; i += 1) {
    const answer = (answers as Record<string, unknown>)[qidOf(i)];
    if (!answer || typeof answer !== 'object') return degrade(count, 'missing_answer', startedAt);
    const record = answer as Record<string, unknown>;
    const choice = String(record.choice ?? '').trim().toLowerCase();
    // 候选键只有题目 criteria 里的两个；认不出来就整批降级，不猜。
    if (choice !== 'new' && choice !== 'not_new') return degrade(count, 'bad_choice', startedAt);
    const probabilities = record.probabilities;
    const confidence = probabilities && typeof probabilities === 'object' && !Array.isArray(probabilities)
      ? Number((probabilities as Record<string, unknown>)[choice])
      : NaN;
    if (!Number.isFinite(confidence) || confidence < 0 || confidence > 1) {
      return degrade(count, 'bad_confidence', startedAt);
    }
    if (choice === 'new' && confidence >= cfg.minConfidence) decided.add(i);
  }
  return { decided, degraded: count - decided.size, reason: 'ok', ms: Date.now() - startedAt };
}
