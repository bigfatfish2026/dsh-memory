/**
 * lint 两级复核。
 *
 * 起因：维护者看懂了一件事——`src/text-lint.ts` 那五条规则是**正则**，它打的是标记，
 * 不是判决。存量里被它误杀的条目（「用户把 qq_img 目录改名为 img/qq。」这种 24 字带路径的）
 * 一条条掉进 discards，代码越硬，误伤越钝。
 *
 * 于是改成三段：
 *   ① 代码标记（src/text-lint.ts，不变）：命中五条规则的条目进「可疑集」；
 *   ② 复核 agent 裁决（本文件）：原文 + 违规项 + 每条规则在防什么，一次问清楚，
 *      只判不改写，回答 {index, verdict:'keep'|'redo', reason}；
 *      keep = 误杀，原样放行入库；redo = 真有问题，退回产出它那个 agent 重做；
 *   ③ 退回重做：带 violations + 复核理由重发一次给原 agent；重做后再过一遍代码 lint，
 *      通过就入库，仍不过才落 discards(verdict=LINT_REJECT)。
 *
 * **重做只给一次机会，不许循环**：这一层的任何调用点都只发一次重做请求，
 * 重做后的结果无论好坏都收敛到「入库 / discards」，不再回头问第二遍。
 *
 * 降级：复核调用抛错、超时、返回非 JSON → 退回旧行为（带 violations 重写一次），
 * 并打 `[lint-review] fallback` 日志。**不许因为复核挂了就丢条目。**
 *
 * 开关：`cfg.pipeline.lintReview`，默认 true；false 时完全走旧逻辑（不花复核调用）。
 */

import { lintRejectReason, type LintViolation } from './text-lint.js';

/** 复核看到的一条可疑条目：text-lint 的 rejected entry，外加来源与复核标记。 */
export interface ReviewItem {
  index?: number;
  text?: unknown;
  violations?: LintViolation[];
  sourceId?: string;
  reviewReason?: string;
  __lintKept?: unknown;
  __lintReview?: { verdict: string; reason: string };
  item?: Record<string, unknown>;
  [key: string]: unknown;
}

/** 一条复核裁决。 */
export interface ReviewDecision {
  index: number;
  verdict: string;
  reason: string;
}

/** 复核用到的配置面：provider/model 走同一条路由，pipeline 里是两个开关。 */
export interface ReviewConfig {
  provider?: string;
  model?: string;
  pipeline?: { lintReview?: boolean; lintReviewTimeoutMs?: number; textLint?: boolean; [key: string]: unknown };
  [key: string]: unknown;
}

/** 复核只要一个 stream；不传就当不可用，走降级。 */
export interface ReviewLlm {
  stream: (req: unknown) => AsyncIterable<unknown>;
}

/** reviewLintItems / judgeSuspected 的入参。 */
export interface ReviewOpts {
  cfg?: ReviewConfig;
  llm?: ReviewLlm | null;
  agent?: string;
  items?: unknown;
  suspects?: unknown;
  violations?: unknown;
  logger?: (line: string) => void;
}

/** 模型流的一块：只认 type/text 与 finish 里的 reason。 */
interface StreamChunk {
  type?: string;
  text?: unknown;
  reason?: { kind?: string; failure?: unknown };
}

/** reviewLintItems 的结果：ok=false 时 decisions 为空，调用方必须降级。 */
export interface ReviewResult {
  ok: boolean;
  decisions: ReviewDecision[];
  fallback: boolean;
  error?: string;
}

/** judgeSuspected 的结果：keep 放行、redo 退回重做。 */
export interface JudgeResult {
  keep: ReviewItem[];
  redo: ReviewItem[];
  fallback: boolean;
  decisions: ReviewDecision[];
  reviewed: boolean;
}

/** 复核调用超时（毫秒），可通过 cfg.pipeline.lintReviewTimeoutMs 覆盖。 */
export const LINT_REVIEW_TIMEOUT_MS = 90_000;

/** 降级日志前缀，写测试和排障都按这个找。 */
export const LINT_REVIEW_FALLBACK_TAG = '[lint-review] fallback';

/**
 * 五条规则各自在防什么：复核 agent 只看条件，看不到 text-lint.js 的源码，
 * 这段就是它的判据。逐条对上 text-lint.js 的 code。
 */
export const LINT_RULE_NOTES = [
  {
    code: 'leading_conjunction',
    title: '开头连接词',
    why: '正文以「但是/所以/因此/另外/此外/并且/而/且/同时/于是/然后/还有/以及/总之」开头，'
      + '说明这一句是上一句的后半截，单独放进记忆库读不出发生了什么。',
  },
  {
    code: 'bare_reference',
    title: '指代词',
    why: '正文靠「这一步/该步/上述/上面/前者/后者/该结论/这个改动/那条/该条/它/此」指代本条之外的对象，'
      + '离开原对话就没有指代对象了。',
  },
  {
    code: 'too_short',
    title: '过短',
    why: '正文不足 25 字（去掉空白后算），且不带路径、文件名、行号、id 或数字，'
      + '这种一条往往是碎片，没有对象也没有结果。',
  },
  {
    code: 'meta_reference',
    title: '元说明',
    why: '正文里写了「此条被…取代 / 见上一条 / 正文见… / 同上 / （略）」这类说明自己的话，'
      + '取代关系该进 supersedes 字段，省略的字该补全。',
  },
  {
    code: 'unverified_claim',
    title: '无坐标的结论',
    why: '正文写了「已验证/已确认/确认过/实测/实际测过」，但全文没有一个数字、路径分隔符或 URL，'
      + '结论钉不住任何可核对的东西。',
  },
];

/** 复核 agent 的 system 提示词：只判不改写，判据是「能不能自足地被人看懂」。 */
export const LINT_REVIEW_SYSTEM = `你是长期记忆库的正文复核员。代码 lint 已经给一批记忆打了标记，标记是规则（正则）打的，可能是误杀。你只判，不改写。

五条规则各自在防什么：
${LINT_RULE_NOTES.map((rule, i) => `${i + 1}. ${rule.code}（${rule.title}）：${rule.why}`).join('\n')}

判据只有一条：**这条记忆离开这次对话、离开其它条目，单独读能不能看懂**——看出说的是哪个对象、做了什么、结果如何。
不要挑风格毛病：不要求文采，不要求句子更短，不要求补规则里没提的信息，也不要因为「可以写得更好」就判 redo。

输出一个 JSON 对象，不要写别的字：
{"decisions":[{"index":0,"verdict":"keep","reason":"一句话中文理由"}]}
- verdict 只能是 keep 或 redo；每一条都要给，index 用我给的编号，不许漏、不许合并。
- keep = 误杀，原样放行入库；redo = 真有硬伤，退回产出这条的那个 agent 重做一次。`;

/** 重做请求的提示头：**一次机会**，所以要把「只重写这几条」说死。 */
export const LINT_REDO_HEADER =
  '上一轮你产出的记忆里有下面的条目被复核判定不合格（不是风格问题，是单条读不懂）。'
  + '按【自足性硬规则】**只重写这几条**，保持原意、不截断、不超过 200 字；'
  + '已经通过的条目不要重复输出，其余字段照原格式给。';

function asList<T = unknown>(value: unknown): T[] {
  return Array.isArray(value) ? value : [];
}

/** 违规项归一：只留 code / match / hint / index，别把整条正文塞进日志。 */
export function violationLine(violation: LintViolation): string {
  const code = String(violation?.code ?? '');
  const match = String(violation?.match ?? '');
  const hint = String(violation?.hint ?? '');
  return `${code}（命中「${match}」）${hint ? `：${hint}` : ''}`;
}

/**
 * 把一条可疑条目的「原文 + 违规项 + 规则说明」拼成人话，给复核 agent 看。
 * 违规项逐条带规则中文说明，复核员不用猜 code 是什么意思。
 */
export function formatReviewItem(entry: ReviewItem | null | undefined): string {
  const notes = new Map(LINT_RULE_NOTES.map((rule) => [rule.code, rule] as [string, typeof rule]));
  const detail = asList<LintViolation>(entry?.violations).map((v) => {
    const rule = notes.get(String(v?.code ?? ''));
    const mark = violationLine(v);
    return rule ? `  - ${mark}｜这条规则防的是：${rule.why}` : `  - ${mark}`;
  });
  return [
    `[${entry?.index}] 正文：${String(entry?.text ?? '')}`,
    detail.length ? '  代码标记：' : '  代码标记：（无细节）',
    ...detail,
  ].join('\n');
}

/** 一次复核调用的 user 提示词：一个批次多条可疑时拼在一起，**只发一次**。 */
export function buildReviewPrompt({ agent = '', items = [] }: { agent?: string; items?: unknown } = {}): string {
  const list = asList<ReviewItem>(items);
  return [
    `产出方：${agent || '未知 agent'}`,
    `可疑条目：${list.length} 条`,
    '',
    ...list.map(formatReviewItem),
    '',
    `逐条裁决，输出 {"decisions":[{"index":0,"verdict":"keep"|"redo","reason":"..."}]}，共 ${list.length} 条。`,
  ].join('\n');
}

/** 从模型输出里抠 JSON：容忍 ```json 围栏、前后的废话。 */
function extractJson(raw: unknown): Record<string, unknown> | null {
  const text = String(raw ?? '').trim();
  if (!text) return null;
  const fenced = text.match(/```(?:json)?\s*([\s\S]*?)```/i);
  const body = fenced ? fenced[1] : text;
  const start = body.indexOf('{');
  const end = body.lastIndexOf('}');
  if (start < 0 || end <= start) return null;
  try {
    return JSON.parse(body.slice(start, end + 1));
  } catch {
    return null;
  }
}

/** verdict 归一：只认 keep / redo，认不出返回 ''（调用方按 redo 处理，宁可重做不放过）。 */
export function normalizeVerdict(value: unknown): string {
  const v = String(value ?? '').trim().toLowerCase();
  if (['keep', 'ok', 'pass', 'retain', '放行', '保留'].includes(v)) return 'keep';
  if (['redo', 'retry', 'rewrite', 'reject', '重做', '重写'].includes(v)) return 'redo';
  return '';
}

/**
 * 解析复核回答。非 JSON / 没有 decisions 数组 → null（调用方降级）。
 * 单条 decision 认不出来不算整体失败，只是这条没裁决（调用方按 redo 处理）。
 */
export function parseReviewAnswer(raw: unknown): ReviewDecision[] | null {
  const parsed = extractJson(raw);
  if (!parsed || typeof parsed !== 'object') return null;
  const list = asList<{ index?: unknown; verdict?: unknown; reason?: unknown }>(parsed.decisions ?? parsed.items ?? parsed.results);
  if (!list.length) return null;
  const decisions = [];
  for (const entry of list) {
    if (!entry || typeof entry !== 'object') continue;
    const index = Number(entry.index);
    if (!Number.isInteger(index) || index < 0) continue;
    const verdict = normalizeVerdict(entry.verdict);
    if (!verdict) continue;
    decisions.push({ index, verdict, reason: String(entry.reason ?? '').trim().slice(0, 200) });
  }
  return decisions.length ? decisions : null;
}

/** 单次「等下一块数据」的空闲超时；不是整条流的总时长上限。 */
function raceIdle(promise: Promise<IteratorResult<unknown>>, ms: number, label: string): Promise<IteratorResult<unknown>> {
  let timer: NodeJS.Timeout | null = null;
  const idle = new Promise((_, reject) => {
    timer = setTimeout(
      () => reject(new Error(`${label} 空闲超时：${ms}ms 没有收到任何数据`)),
      Number(ms),
    );
    if (typeof timer.unref === 'function') timer.unref();
  });
  return Promise.race([promise, idle]).finally(() => { if (timer) clearTimeout(timer); }) as Promise<IteratorResult<unknown>>;
}

/**
 * 流式收集文本增量。
 *
 * 旧版是「整条流超过 ms 就失败」的固定总时长超时：模型慢慢吐、只要总时长过了线，
 * 明明一直在出字也被判失败，白丢一批条目。现在按**响应活性**判：每收到一块数据就把
 * 计时器重置，只有连续 idleMs 一个字节都没来才算超时；总时长不设限。
 *
 * finish=error 仍直接抛，交给调用方降级。
 *
 * @param {AsyncIterable} stream llm.stream 的返回
 * @param {object} [opts]
 * @param {number} [opts.idleMs=0] 空闲多久算超时；<=0 = 不判
 * @param {string} [opts.label='复核'] 报错前缀
 * @returns {Promise<string>} 收集到的文本
 */
export async function collectStreamWithIdle(stream: unknown, { idleMs = 0, label = '复核' }: { idleMs?: number; label?: string } = {}): Promise<string> {
  const idle = Math.max(0, Number(idleMs) || 0);
  const it = typeof (stream as AsyncIterable<unknown> | null | undefined)?.[Symbol.asyncIterator] === 'function'
    ? (stream as AsyncIterable<unknown>)[Symbol.asyncIterator]()
    : null;
  if (!it) throw new Error(`${label} 调用拿不到流`);
  let text = '';
  try {
    for (;;) {
      const step = it.next();
      const got = idle > 0 ? await raceIdle(step, idle, label) : await step;
      if (got?.done) break;
      const chunk = got?.value as StreamChunk | undefined;
      if (chunk?.type === 'text-delta') text += String(chunk.text ?? '');
      else if (chunk?.type === 'reasoning-delta') {
        // 思考也在出数据：算活性，但不算正文
      } else if (chunk?.type === 'tool-call' || chunk?.type === 'tool-call-delta') {
        // 工具调用同样是「有数据在流」，不重置就只能靠正文增量判活，误杀
      } else if (chunk?.type === 'finish' && chunk.reason?.kind === 'error') {
        throw new Error(`复核调用失败：${JSON.stringify(chunk.reason.failure || {})}`);
      }
    }
  } finally {
    // 超时/抛错后主动关流，别把上游连接晾着。
    // 不能 await：流卡在不返回的 await 里时，async generator 的 return() 会跟着一起卡死，
    // 超时就变成「等上游回话」，活性判定也就没意义了。
    try { void it.return?.(); } catch { /* 关不掉不影响结果 */ }
  }
  return text;
}

function violationIndexOf(violation: LintViolation, fallback: number | undefined): number | undefined {
  const n = Number(violation?.index);
  return Number.isInteger(n) && n >= 0 ? n : fallback;
}

/**
 * 把 items / violations 两路输入合到「按 index 分组的违规项」上。
 * 支持三种写法（各调用点手上的形状不一样）：
 *   ① items[i].violations 自带；
 *   ② violations 是平铺数组 [{index, code, match}]（text-lint 的原始产物）；
 *   ③ violations 是按 items 顺序的数组的数组。
 */
function collectViolations(items: ReviewItem[], violations: unknown): Map<number, LintViolation[]> {
  const byIndex = new Map<number, LintViolation[]>();
  const add = (index: number | undefined, violation: LintViolation): void => {
    if (!index && index !== 0) return;
    const list = byIndex.get(index) ?? [];
    list.push(violation);
    byIndex.set(index, list);
  };
  const list = asList<LintViolation | LintViolation[]>(violations);
  const pairForm = list.some((v) => Array.isArray(v));
  if (pairForm) {
    items.forEach((item, i) => {
      const index = Number.isInteger(Number(item?.index)) ? Number(item.index) : i;
      for (const violation of asList<LintViolation>(list[i])) add(index, violation);
    });
  } else {
    for (const violation of list) {
      if (!violation || typeof violation !== 'object') continue;
      add(violationIndexOf(violation as LintViolation, items.length === 1 ? 0 : undefined), violation as LintViolation);
    }
  }
  items.forEach((item, i) => {
    const index = Number.isInteger(Number(item?.index)) ? Number(item.index) : i;
    for (const violation of asList<LintViolation>(item?.violations)) add(index, violation);
  });
  return byIndex;
}

/**
 * 复核一批可疑条目。**一个批次只发一次调用**，无论里面几条。
 *
 * @param {object} opts
 * @param {object} opts.cfg 插件配置（用 cfg.provider / cfg.model 走同一条路由）
 * @param {object} [opts.llm] ctx.llm；没有 stream 就当复核不可用，走降级
 * @param {string} [opts.agent] 产出这批条目的 agent 名（写进提示词，便于它知道上下文）
 * @param {Array<{index:number, text:string, violations?:Array}>} opts.items 可疑集
 * @param {Array} [opts.violations] 平铺的违规项（可选，见 collectViolations）
 * @param {(line:string)=>void} [opts.logger]
 * @returns {Promise<{ok:boolean, decisions:Array<{index:number, verdict:string, reason:string}>,
 *   fallback:boolean, error?:string}>} ok=false 时 decisions 为空，调用方必须降级
 */
export async function reviewLintItems({ cfg = {}, llm, agent = '', items = [], violations = [], logger }: ReviewOpts = {}): Promise<ReviewResult> {
  const log = typeof logger === 'function' ? logger : () => {};
  const list = asList<ReviewItem>(items);
  if (!list.length) return { ok: true, decisions: [], fallback: false };
  if (!llm || typeof llm.stream !== 'function') {
    log(`${LINT_REVIEW_FALLBACK_TAG} reason=llm_unavailable n=${list.length}`);
    return { ok: false, decisions: [], fallback: true, error: 'llm 不可用' };
  }

  const byIndex = collectViolations(list, violations);
  const promptItems = list.map((item, i) => {
    const index = Number.isInteger(Number(item?.index)) ? Number(item.index) : i;
    return { index, text: String(item?.text ?? ''), violations: byIndex.get(index) ?? asList(item?.violations) };
  });

  // 2026-11-14：这个数现在是「空闲多久算超时」，不是整条流的总时长上限。
  // 键名没改（配置里还是 lintReviewTimeoutMs），语义变宽：慢回答只要有数据在流就不会被杀。
  const idleMs = Number(cfg?.pipeline?.lintReviewTimeoutMs) > 0
    ? Number((cfg as ReviewConfig).pipeline!.lintReviewTimeoutMs)
    : LINT_REVIEW_TIMEOUT_MS;

  let raw = '';
  try {
    // 路由跟其它 agent 一致；**不传 maxTokens**（维护者的硬规矩：由 provider 封顶）
    const stream = llm.stream({
      provider: cfg.provider,
      model: cfg.model,
      system: LINT_REVIEW_SYSTEM,
      messages: [{
        role: 'user',
        content: [{ type: 'text', text: buildReviewPrompt({ agent, items: promptItems }) }],
      }],
      temperature: 0,
    });
    raw = await collectStreamWithIdle(stream, { idleMs, label: '复核' });
  } catch (error) {
    log(`${LINT_REVIEW_FALLBACK_TAG} reason=call_fail n=${list.length} err=${String((error as Error && (error as Error).message) || error as string).slice(0, 160)}`);
    return { ok: false, decisions: [], fallback: true, error: String((error as Error && (error as Error).message) || error as string) };
  }

  const decisions = parseReviewAnswer(raw);
  if (!decisions) {
    log(`${LINT_REVIEW_FALLBACK_TAG} reason=bad_json n=${list.length} raw=${JSON.stringify(String(raw).slice(-160))}`);
    return { ok: false, decisions: [], fallback: true, error: '复核返回非 JSON' };
  }
  return { ok: true, decisions, fallback: false };
}

/**
 * 三段式里的「裁决」段：把代码标记出来的一批可疑条目交给复核 agent 分堆。
 *
 * 返回两堆，调用方照这个走：
 *   keep → 原样放行入库（写库时带 __lintChecked 让 store 的最后一道闸放行）；
 *   redo → 带着 violations + reason 退回产出它的那个 agent **重做一次**。
 *
 * 两种降级（都不许丢条目）：
 *   - `pipeline.lintReview === false` → 全部 redo，完全走旧行为，不花复核调用；
 *   - 复核抛错 / 超时 / 非 JSON → 全部 redo，打 `[lint-review] fallback`。
 * 复核返回了但某几条没裁决 → 那几条按 redo（宁可重做，不放过）。
 *
 * @param {object} opts 同 reviewLintItems，另加 suspects: [{index, text, violations}]
 * @returns {Promise<{keep:Array, redo:Array, fallback:boolean, decisions:Array, reviewed:boolean}>}
 */
export async function judgeSuspected({ cfg = {}, llm, agent = '', suspects = [], logger }: ReviewOpts = {}): Promise<JudgeResult> {
  const log = typeof logger === 'function' ? logger : () => {};
  const list = asList<ReviewItem>(suspects);
  if (!list.length) return { keep: [], redo: [], fallback: false, decisions: [], reviewed: false };

  if (cfg?.pipeline?.lintReview === false) {
    log(`[lint-review] off n=${list.length}，全部按旧逻辑重写一次`);
    return { keep: [], redo: list, fallback: false, decisions: [], reviewed: false };
  }

  const result = await reviewLintItems({ cfg, llm, agent, items: list, logger: log });
  if (!result.ok) {
    return { keep: [], redo: list, fallback: true, decisions: [], reviewed: false };
  }

  const verdictOf = new Map(result.decisions.map((d) => [d.index, d]));
  const keep: ReviewItem[] = [];
  const redo: ReviewItem[] = [];
  let missing = 0;
  list.forEach((entry, i) => {
    const index = Number.isInteger(Number(entry?.index)) ? Number(entry.index) : i;
    const decision = verdictOf.get(index);
    if (!decision) {
      missing += 1;
      redo.push({ ...entry, index });
      return;
    }
    const withReason = { ...entry, index, reviewReason: decision.reason };
    if (decision.verdict === 'keep') keep.push(withReason);
    else redo.push(withReason);
  });
  if (missing) log(`[lint-review] 复核漏判 ${missing} 条，按 redo 处理 n=${list.length}`);
  log(
    `[lint-review] agent=${agent || '-'} n=${list.length} keep=${keep.length} redo=${redo.length}` +
      (redo.length ? ` codes=${[...new Set(redo.flatMap((r) => asList<LintViolation>(r.violations).map((v) => v.code)))].join(',')}` : ''),
  );
  // 逐条记账：只打汇总的话，事后没法回答「这条凭什么放行」。
  // 只打 code + 一句理由 + 正文首 40 字，不打全文。
  const brief = (entry: ReviewItem, verdict: string): string => {
    const codes = [...new Set(asList<LintViolation>(entry?.violations).map((v) => String(v?.code ?? '')))].filter(Boolean).join(',') || '-';
    const reason = String(entry?.reviewReason ?? '').trim().slice(0, 120) || '-';
    return `[lint-review] INFO ${verdict} agent=${agent || '-'} idx=${entry?.index} code=${codes} `
      + `text40=${String(entry?.text ?? '').replace(/\s+/g, ' ').slice(0, 40)} reason=${reason}`;
  };
  for (const entry of keep) log(brief(entry, 'keep'));
  for (const entry of redo) log(brief(entry, 'redo'));
  return { keep, redo, fallback: false, decisions: result.decisions, reviewed: true };
}

/**
 * 退回重做用的提示词：逐条写清「第 N 条、原文、违规项、复核理由」，外加 200 字上限。
 * 这是**最后一次机会**，所以理由要写全，别让模型猜。
 */
export function buildLintRedoFeedback(suspects: unknown): string {
  const list = asList<ReviewItem>(suspects);
  const lines = list.map((entry) => {
    const index = Number.isInteger(Number(entry?.index)) ? Number(entry.index) : 0;
    const detail = asList<LintViolation>(entry?.violations).map(violationLine).join('；') || '代码 lint 不通过';
    // 原文只截个引子（够模型认出是哪条就行），字数留给「原因 + 复核意见」——
    // 复核 agent 的理由是这一轮重做的主要依据，截掉它就白问了。
    const reason = String(entry?.reviewReason ?? '').trim().slice(0, 90);
    return `第 ${index + 1} 条（原文：「${String(entry?.text ?? '').slice(0, 60)}」）被判定不合格，`
      + `原因：${detail}${reason ? `；复核意见：${reason}` : ''}。请重写这一条，保持原意、不截断、不超过 200 字。`;
  });
  return [LINT_REDO_HEADER, ...lines].join('\n');
}

/**
 * 复核判 keep 的条目带在身上的标记：值是复核理由（没有就 true）。
 * 调用方 spread 进 store 的 meta：`...lintReviewMeta(item)`，
 * store 拿 `__lintReview.verdict === 'keep'` 放行并打日志，最后一道闸仍然看得见这条是谁放行的。
 */
export function lintKeptMarker(entry: ReviewItem | null | undefined): { __lintKept: string | true } {
  return { __lintKept: String(entry?.reviewReason ?? '').trim() || true };
}

/** 把 __lintKept 翻成 store 认识的 __lintReview（只有 keep 才带，其余返回空对象）。 */
export function lintReviewMeta(item: ReviewItem | null | undefined): { __lintReview?: { verdict: string; reason: string } } {
  if (!item?.__lintKept) return {};
  const reason = item.__lintKept === true ? '' : String(item.__lintKept);
  return { __lintReview: { verdict: 'keep', reason: reason.slice(0, 160) } };
}

/**
 * 把复核标记原样抠出来，给「重建对象、只挑白名单字段」的地方透传。
 *
 * 为什么单独立一个函数：`__lintKept` 有两种形态——
 * 复核给了理由就是**字符串**，没给理由才是 `true`。merge.js 的 validateOps 曾经写成
 * `raw.__lintKept === true ? {__lintKept:true} : {}`，字符串被判掉，重建出来的 op 不带标记，
 * sleep.js 的 lintReviewMeta 拿空，store 最后一道闸又拒一次——**复核判 keep 的误杀照样落 discards**。
 * 所以任何 reshape 都不许自己判断，统一走这里。
 *
 * @returns {{__lintKept?: any, __lintReview?: object}} 没标记就返回空对象
 */
export function lintMarksOf(item: ReviewItem | null | undefined): { __lintKept?: unknown; __lintReview?: unknown } {
  const out: { __lintKept?: unknown; __lintReview?: unknown } = {};
  const kept = item?.__lintKept;
  if (kept !== undefined && kept !== null && kept !== false) out.__lintKept = kept;
  if (item?.__lintReview) out.__lintReview = item.__lintReview;
  return out;
}

/** 落 discards 时把复核理由并进 reason（两轮 violations 由调用方合并传入）。目前未接线，保留给后续落账路径。 */
export function withReviewerReason(entry: ReviewItem | null | undefined, reason: unknown): ReviewItem | null | undefined {
  const reviewer = String(reason ?? entry?.reviewReason ?? '').trim();
  return reviewer ? { ...entry, reviewerReason: reviewer } : entry;
}

/**
 * 重做之后仍不合格 → 拼落 discards 的那条账：
 * violations 是**两轮合并**（可疑轮的 + 重做轮的），reviewerReason 是复核 agent 的原话。
 * 重做那条正文跟原条对不上号（模型重写了），所以理由按整批拼，不硬配到某一条。
 */
export function mergeReviewRejects(stillRejected: unknown, redoSuspects: unknown): ReviewItem[] {
  const reviewers = [...new Set(asList<ReviewItem>(redoSuspects).map((s) => String(s?.reviewReason ?? '').trim()).filter(Boolean))];
  const reviewerReason = reviewers.join(' / ').slice(0, 200);
  const previous = asList<ReviewItem>(redoSuspects).flatMap((s) => asList<LintViolation>(s?.violations));
  if (!reviewerReason && !previous.length) return asList<ReviewItem>(stillRejected);
  return asList<ReviewItem>(stillRejected).map((entry) => ({
    ...entry,
    violations: [...previous, ...asList<LintViolation>(entry?.violations)],
    ...(reviewerReason ? { reviewerReason } : {}),
  }));
}

/** 给日志用的一行摘要：code 列表 + 复核理由，不带正文。目前未接线，保留给后续复核日志。 */
export function lintReviewBrief(entries: unknown): string {
  return asList<ReviewItem>(entries)
    .map((entry) => {
      const codes = [...new Set(asList<LintViolation>(entry?.violations).map((v) => v.code))].join('+') || '-';
      const why = String(entry?.reviewReason ?? '').slice(0, 40);
      return `i${entry?.index ?? '?'}:${codes}${why ? `(${why})` : ''}`;
    })
    .join(';');
}

/** 一条正文的完整判据串（复核员理由 + violations），给 discards.reason 用。目前未接线，保留给后续落账路径。 */
export function lintReviewReasonOf(entry: ReviewItem | null | undefined): string {
  const base = lintRejectReason(entry?.violations);
  const reviewer = String(entry?.reviewReason ?? '').trim();
  return reviewer ? `${base} | 复核：${reviewer}`.slice(0, 500) : base;
}

export default reviewLintItems;
