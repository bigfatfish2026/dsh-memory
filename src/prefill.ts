/**
 * 每轮自动回忆注入（autoPrefill，分级触发版）
 *
 * 三层：
 *   1) 触发层：纯本地规则，先判这条用户消息值不值得查。不值得就零检索零延迟跳过。
 *   2) 检索层：只对触发的消息跑一次 store.recall(query, topK)，超时/失败静默放弃。
 *   3) 注入层：每条压成「[type] 摘要（id）」，总长受 autoPrefillMaxChars 限制，
 *      作为一节挂进 system prompt（order 9500，subagent 会话不注入）。
 *
 * 为什么要分级：Pi5 上一次 recall 要打远端 bge-m3，每条消息都查等于每轮都交一次
 * 网络税，而「嗯」「哈哈」这种消息根本不可能命中什么。
 *
 * 硬约束：任何异常都必须静默跳过 —— 检索挂了也绝不能让 prompt 构建失败。
 */
import { createHash } from 'node:crypto';
import { appendFileSync, mkdirSync } from 'node:fs';
import { dirname } from 'node:path';
import * as paths from './shared/paths.js';

/** 会话里的一条消息：只取判定要用的字段。 */
export interface ChatMessage {
  role?: string;
  content?: unknown;
  source?: { kind?: string };
}

/** 会话：只认 id / header.origin / deriveMessages 投影。 */
export interface PrefillSession {
  id?: string;
  header?: { origin?: string };
  deriveMessages?: () => unknown;
}

/** 召回项：id/text/type/score 都可能缺，kwHits 是本文件重排时加的。 */
export interface RecallItem {
  id?: string | number;
  text?: unknown;
  type?: unknown;
  score?: number;
  kwHits?: number;
  [key: string]: unknown;
}

/** 触发判定结果。 */
export interface TriggerVerdict {
  trigger: boolean;
  reason: string;
}

/** prefill 读到的配置面。 */
export interface PrefillCfg {
  autoPrefill?: boolean;
  autoPrefillTopK?: number;
  autoPrefillMaxChars?: number;
  autoPrefillTimeoutMs?: number;
  autoPrefillMinChars?: number;
  autoPrefillEntityWords?: string[];
  autoPrefillLogPath?: string;
  [key: string]: unknown;
}

/** 会话内状态：连续失败数、熔断截止、已注入记忆（id → 时间）。 */
export interface PrefillSessionState {
  failures: number;
  blockedUntil: number;
  injectedIds: Map<string, number>;
}

/** 一行注入日志的字段。 */
export interface PrefillLogFields {
  session?: string;
  verdict: string;
  reason: string;
  hits?: number;
  ms?: number;
}

/** prefill 用到的 store 面：召回 + 全表（补 id 用）+ 观察版本（缓存失效用）。 */
export interface PrefillStore {
  recall: (query: string, k: number, opts?: Record<string, unknown>) => Promise<RecallItem[]>;
  listAll?: () => Array<{ id?: string | number; text?: unknown }>;
  /** 每库一份的观察版本号（meta.observation_epoch）；缺这个方法的假 store 按 0 处理。 */
  observationVersion?: () => number;
}

/** system-prompt/assemble 的上下文面。 */
export interface AssembleContext {
  agent?: { session?: PrefillSession };
}

/** 插件上下文里用到的两面：on 与 logger。 */
export interface PrefillCtx {
  on: (
    event: string,
    fn: (assembly: unknown, context: AssembleContext | null | undefined, next: () => Promise<{ sections?: unknown }>) => Promise<unknown>,
    opts?: { prepend?: boolean },
  ) => unknown;
  logger?: { info?: (msg: string) => void };
}

/** auto-prefill 的返回值（测试拿 stats 与 cache 用）。 */
export interface PrefillHandle {
  stats: { injected: number; skipped: number; failed: number; cached: number };
  cache: Map<string, string>;
  sessionState: Map<string, PrefillSessionState>;
}

/** 注入节的 name，稳定，便于在 assembly 里认出并替换。 */
export const PREFILL_SECTION = 'memory-auto-prefill';
/** 注入段标题，写死（改字等于改注入行为）。 */
export const PREFILL_HEADER =
  '【可能相关的记忆（自动检索，未必对得上；要细节用 memory_recall 拉全文；与本轮事实冲突以本轮为准）】';
/** 默认日志路径。 */
export const PREFILL_LOG = paths.logFile('memory-prefill.log');

const CACHE_LIMIT = 50;
/** 单条摘要字数。 */
const SUMMARY_MAX_CHARS = 40;
/** 送去嵌入的 query 上限。 */
const QUERY_MAX_CHARS = 500;
/** 中文停用 bigram：这些命中不算相关度。 */
const STOP_BIGRAMS = new Set([
  '的了', '是的', '我们', '你们', '他们', '这个', '那个', '一个', '可以', '就是', '不是',
  '什么', '怎么', '因为', '所以', '如果', '但是', '而且', '然后', '还是', '已经', '现在',
  '这样', '那样', '这些', '那些', '自己', '时候', '问题', '东西', '一块', '这块', '那块',
  '一下', '没有', '上面', '下面', '里面', '外面', '之后', '之前', '需要', '一次', '这块',
]);
/** 连续失败几次后本会话闭嘴。 */
const FAIL_STREAK_LIMIT = 3;
/** 熔断时长（毫秒）。 */
const FAIL_COOLDOWN_MS = 5 * 60 * 1000;
/** 同一条记忆重复注入的冷却（毫秒）。 */
const MEMORY_COOLDOWN_MS = 10 * 60 * 1000;
/** 检索时多取几倍候选，只为给关键词重排留位置（不增加检索调用）。 */
const CANDIDATE_FACTOR = 2;

/** 寒暄/应答：整条就是这些，查了也没用。 */
const GREETING_WORDS = [
  '哈哈', '哈哈哈', '好的', '好', '嗯', '嗯嗯', '晚安', '早安', '收到', '哦', '噢',
  'ok', 'okay', '在吗', '在？', '在?', '谢谢', '好的呀', '行', '好嘞', '嗯呢',
];

/** 可配置的默认实体词表。 */
export const DEFAULT_ENTITY_WORDS = [
  'example', '示例项目', 'demo', 'service', 'database', 'memory',
  'git', 'node', 'docker', 'sqlite',
];

const QUESTION_WORDS = ['怎么', '为什么', '哪', '啥', '谁', '多少', '是不是', '能不能', '有没有', '吗', '？', '?'];
const ACTION_WORDS = ['查', '看', '改', '装', '删', '跑', '编', '发', '找', '写', '测', '重启', '拆', '做'];
const TIME_WORDS = ['以前', '上次', '之前', '历史', '记得', '那个'];

/** 按 order 语义该排在 9500 之后的已知 section（找到第一个就插在它前面）。 */
const ANCHORS_AFTER_9500 = [
  'tool:structured_output',
  'harness:source',
  'app:web-surface',
  'deployment:persona-suffix',
];
/** order 小于 9500 的已知 section（兜底插在它后面）。 */
const ANCHORS_BEFORE_9500 = ['ui:deliverable-file-references'];

/** 取一条消息里的纯文本。 */
function messageText(message: ChatMessage | null | undefined): string {
  const blocks = message?.content;
  if (!Array.isArray(blocks)) return '';
  const parts: string[] = [];
  for (const block of blocks) {
    if (block?.type === 'text' && typeof block.text === 'string' && block.text.trim()) parts.push(block.text);
  }
  return parts.join('\n').trim();
}

/**
 * harness 每轮往会话里插的「运行时快照」用户消息（t-prefill-v042）。
 *
 * 实测 shape（session.v4.jsonl.zstd，user/message 事件）：
 *   data = { id, role: 'user', content: [{type:'text', text:'Current runtime context. ...'}],
 *            source: { kind: 'runtime-context', form: 'snapshot', sections: [...] } }
 * 而 deriveMessages() 对 user/message 是直接返回 event.data（dsh-session/lib/index.js:222），
 * 所以消息对象本身就是 data，kind 挂在 message.source.kind 上。
 * ego 注入的念头则是 message.source.kind === 'dsh-ego'，属于真该拿去召回的内容。
 */
function isRuntimeContext(message: ChatMessage | null | undefined): boolean {
  return message?.source?.kind === 'runtime-context';
}

/**
 * 最近一条「不是运行时快照」的 user 消息文本；取不到返回空串。
 *
 * 之前取的是「最后一条 role=user」，结果拿到 harness 的 runtime-context 快照
 * （整段环境文字，无括号可剥），召回词永远是那堆字数。现在倒序跳过快照，
 * 落回用户真发言或 ego/插件注入的念头。全是快照时回退原来的最后一条，不返回空。
 */
export function lastUserText(session: PrefillSession | null | undefined): string {
  try {
    const messages = session?.deriveMessages?.();
    if (!Array.isArray(messages)) return '';
    let fallback = '';
    for (let i = messages.length - 1; i >= 0; i -= 1) {
      const message = messages[i];
      if (message?.role !== 'user') continue;
      const text = messageText(message);
      if (!text) continue;
      if (!fallback) fallback = text; // 兜底＝改动前的语义：最后一条 user 消息
      if (isRuntimeContext(message)) continue;
      return text;
    }
    return fallback;
  } catch {
    /* 会话还没成形 / 投影失败：当没有 */
  }
  return '';
}

/**
 * 召回词清理（2026-11-08 t-prefill-fix）：长指令消息里，harness 的注入模板
 * （speak-up / dsh-ego 那类）整段用括号包住，一行就是一条指令。它们占了前 70%
 * 字符，真意图排在最后又被 QUERY_MAX_CHARS 截掉，召回 top3 于是永远是插件规则条文。
 *
 * 规则：一行 trim 后整体以 (（ 开头、以 )） 结尾，整行丢掉；剔完为空就回退原文
 * （绝不把 query 变空）。只作用于 prefill 这一条路径，不动 store.recall 的通用语义。
 */
export function recallQuery(raw: unknown): string {
  const text = String(raw ?? '');
  const kept: string[] = [];
  for (const line of text.split('\n')) {
    const trimmed = line.trim();
    const bracketed = trimmed.length >= 2 && /^[（(]/.test(trimmed) && /[）)]$/.test(trimmed);
    if (bracketed) continue;
    kept.push(line);
  }
  const stripped = kept.join('\n').trim();
  return stripped || text;
}

/** 最后一条 user 消息带的非文本块（图片/文件）——纯图消息不必查记忆。 */
function lastUserHasOnlyMedia(session: PrefillSession | null | undefined): boolean {
  try {
    const messages = session?.deriveMessages?.();
    if (!Array.isArray(messages)) return false;
    for (let i = messages.length - 1; i >= 0; i -= 1) {
      const message = messages[i];
      if (message?.role !== 'user') continue;
      const blocks = Array.isArray(message.content) ? message.content : [];
      const hasMedia = blocks.some((b: { type?: string }) => b?.type === 'image' || b?.type === 'file');
      return hasMedia && blocks.every((b: { type?: string; text?: unknown }) => b?.type !== 'text' || !String(b.text || '').trim());
    }
  } catch {
    /* 读不到就按普通消息处理 */
  }
  return false;
}

/** 去掉表情包标记、标点、emoji 后是否还剩东西。 */
function hasRealText(raw: unknown): boolean {
  const stripped = String(raw || '')
    .replace(/\[[^\]]{1,12}\]/g, '') // [表情]/[图片] 这类 QQ 标记
    .replace(/[\s\p{P}\p{S}\p{Emoji_Presentation}\p{Extended_Pictographic}]/gu, '');
  return stripped.length > 0;
}

/** 从 query 里认出已知实体（小写包含匹配）。 */
function matchEntities(raw: unknown, words: unknown): string[] {
  const lower = String(raw || '').toLowerCase();
  const hits: string[] = [];
  for (const word of words as unknown[]) {
    const w = String(word || '').trim().toLowerCase();
    if (w && lower.includes(w) && !hits.includes(w)) hits.push(w);
  }
  return hits;
}

/**
 * 分级触发的判定：返回 { trigger: boolean, reason: string }。
 * 纯本地、零 IO、零模型。
 */
export function judgeTrigger(raw: unknown, cfg: PrefillCfg = {}): TriggerVerdict {
  const text = String(raw || '').trim();
  if (!text) return { trigger: false, reason: 'empty' };
  const compact = text.replace(/\s+/g, '');
  const minChars = Math.max(1, Number(cfg.autoPrefillMinChars) || 6);

  // ── 跳过：纯表情 / 只有图片 ──
  if (!hasRealText(text)) return { trigger: false, reason: 'emoji-only' };

  // ── 跳过：寒暄应答 ──
  const bare = compact.replace(/[\p{P}\p{S}]/gu, '').toLowerCase();
  for (const word of GREETING_WORDS) {
    const w = word.replace(/[\p{P}\p{S}]/gu, '').toLowerCase();
    if (!w) continue;
    if (bare === w || bare === `${w}${w}` || (bare.length <= w.length + 2 && bare.startsWith(w))) {
      return { trigger: false, reason: 'greeting' };
    }
  }

  // ── 跳过：太短且没问号 ──
  if (compact.length < minChars && !/[？?]/.test(compact)) return { trigger: false, reason: 'too-short' };

  const lower = text.toLowerCase();

  // ── 触发：路径（最明确的信号，先判；只认 /x 或 ~/x，斜杠前必须是行首/空白/括号引号）──
  if (/(^|[\s(（"'`])(?:~\/|\/)[^\s/]/.test(text)) return { trigger: true, reason: 'path' };

  // ── 触发：疑问词 ──
  for (const word of QUESTION_WORDS) if (compact.includes(word)) return { trigger: true, reason: 'question' };

  // ── 触发：动作词 ──
  for (const word of ACTION_WORDS) if (compact.includes(word)) return { trigger: true, reason: 'action' };

  // ── 触发：时间指代 ──
  for (const word of TIME_WORDS) if (compact.includes(word)) return { trigger: true, reason: 'time-ref' };

  // ── 触发：实体信号（实体名先判，比「英文单词」精确，日志里也看得懂）──
  const entities = matchEntities(text, cfg.autoPrefillEntityWords || DEFAULT_ENTITY_WORDS);
  if (entities.length) return { trigger: true, reason: `entity:${entities[0]}` };
  if (/\d+\s*(?:℃|°c|%|gb|mb|kb|tb|ms|s|秒|分钟|小时|天|万|条|次)/i.test(text)) {
    return { trigger: true, reason: 'number+unit' };
  }
  if (/(^|\s)-{1,2}[a-z]/i.test(text)) return { trigger: true, reason: 'flag' };
  if (/(^|[^a-z])[a-z]{3,}/i.test(lower)) return { trigger: true, reason: 'word' };

  return { trigger: false, reason: 'no-signal' };
}

/**
 * query / 文本切成判别用 token：英文词与数字（≥3 字符）+ 中文 bigram（去掉停用）。
 * 只用来给召回结果重排，不参与嵌入。
 */
export function tokensOf(text: unknown): Set<string> {
  const lower = String(text || '').toLowerCase();
  const out = new Set<string>();
  for (const word of lower.match(/[a-z][a-z0-9_.-]{2,}|\d{2,}/g) || []) out.add(word);
  const cjk = lower.replace(/[^\u4e00-\u9fa5]/g, ' ');
  for (const segment of cjk.split(/\s+/)) {
    for (let i = 0; i < segment.length - 1; i += 1) {
      const bigram = segment.slice(i, i + 2);
      if (!STOP_BIGRAMS.has(bigram)) out.add(bigram);
    }
  }
  return out;
}

/**
 * 召回结果按「本轮消息的关键词命中」加权重排。
 * 语义分是主权重，关键词命中数做 0.65~1.0 的系数：命中多的往前挪，
 * 一条都不命中的会被压下去（语义像但用词不搭，多半是噪声）。
 * @param items recall 返回项
 * @param query 本轮用户消息
 */
export function rankByKeywords(items: RecallItem[] | null | undefined, query: unknown): RecallItem[] {
  if (!Array.isArray(items) || items.length <= 1) return items || [];
  const queryTokens = tokensOf(query);
  const scored = items.map((item: RecallItem) => {
    const textTokens = tokensOf(item?.text);
    let hits = 0;
    for (const token of queryTokens) if (textTokens.has(token)) hits += 1;
    const kw = hits === 0 ? 0 : hits / Math.sqrt(Math.max(8, textTokens.size));
    const cos = Math.max(0, Number(item?.score) || 0);
    return { item, hits, kw, cos };
  }) as Array<{ item: RecallItem; hits: number; kw: number; cos: number; final: number }>;
  const maxKw = Math.max(...scored.map((s) => s.kw), 0);
  for (const s of scored) s.final = s.cos * (0.65 + 0.35 * (maxKw > 0 ? s.kw / maxKw : 0));
  scored.sort((a, b) => b.final - a.final || b.cos - a.cos);
  return scored.map((s) => ({ ...s.item, kwHits: s.hits }));
}

/** 一条记忆的稳定身份：有 id 用 id，没有就拿正文摘要。 */
function memoryKey(item: RecallItem | null | undefined): string {
  const id = String(item?.id || '').trim();
  if (id) return id;
  return `t:${createHash('sha256').update(String(item?.text || '')).digest('hex').slice(0, 12)}`;
}

/** 这条记忆是不是刚注入过（10 分钟内）。 */
function isRecentlyInjected(state: PrefillSessionState, item: RecallItem | null | undefined): boolean {
  const at = state.injectedIds.get(memoryKey(item));
  return at !== undefined && Date.now() - at < MEMORY_COOLDOWN_MS;
}

/** 记住这批记忆的注入时间，顺手清掉过期的。 */
function rememberInjected(state: PrefillSessionState, items: RecallItem[]): void {
  const now = Date.now();
  for (const item of items) state.injectedIds.set(memoryKey(item), now);
  if (state.injectedIds.size > 100) {
    for (const [key, at] of state.injectedIds) if (now - at > MEMORY_COOLDOWN_MS) state.injectedIds.delete(key);
    while (state.injectedIds.size > 100) state.injectedIds.delete(state.injectedIds.keys().next().value as string);
  }
}

/** 一条记忆压成一行摘要 + 短 id。 */
function summaryLine(item: RecallItem | null | undefined): string {
  const flat = String(item?.text || '').replace(/\s+/g, ' ').trim();
  if (!flat) return '';
  const short = flat.length > SUMMARY_MAX_CHARS ? `${flat.slice(0, SUMMARY_MAX_CHARS - 1)}…` : flat;
  const id = String(item?.id || '').slice(0, 9);
  return `[${item?.type || 'world'}] ${short}${id ? `（${id}）` : ''}`;
}

/**
 * 把命中结果拼成注入文本；一条都塞不进去（或没有命中）返回空串。
 * @param items recall 的返回项（可带 id）
 * @param maxChars 整段文本上限
 */
export function buildPrefillText(items: unknown, maxChars: unknown): string {
  if (!Array.isArray(items) || items.length === 0) return '';
  const limit = Math.max(PREFILL_HEADER.length + 20, Number(maxChars) || 200);
  let out = PREFILL_HEADER;
  for (const item of items) {
    const line = summaryLine(item);
    if (!line) continue;
    const candidate = `\n- ${line}`;
    if (out.length + candidate.length > limit) break;
    out += candidate;
  }
  return out === PREFILL_HEADER ? '' : out;
}

/** 把注入节按 9500 的位置塞进 sections；已存在同名就替换文本。 */
export function insertPrefillSection(sections: unknown, text: unknown): boolean {
  if (!Array.isArray(sections) || !text) return false;
  const existing = sections.find((section) => section?.name === PREFILL_SECTION);
  if (existing) {
    existing.text = text;
    existing.interpolate = false;
    return true;
  }
  const entry = { name: PREFILL_SECTION, text, interpolate: false };
  for (const anchor of ANCHORS_AFTER_9500) {
    const index = sections.findIndex((section) => section?.name === anchor);
    if (index >= 0) {
      sections.splice(index, 0, entry);
      return true;
    }
  }
  for (let i = sections.length - 1; i >= 0; i -= 1) {
    if (ANCHORS_BEFORE_9500.includes(sections[i]?.name)) {
      sections.splice(i + 1, 0, entry);
      return true;
    }
  }
  sections.push(entry);
  return true;
}

/** 给召回结果补上 id：recall 不返回 id，按正文对上 listAll。 */
function attachIds(store: PrefillStore, items: RecallItem[]): RecallItem[] {
  if (!items.length) return items;
  let byText = null;
  try {
    byText = new Map<unknown, string | number | undefined>();
    for (const m of store.listAll?.() || []) byText.set(m.text, m.id);
  } catch {
    return items;
  }
  return items.map((item) => (item?.id ? item : { ...item, id: byText.get(item?.text) || '' }));
}

/** 写一行日志；失败就算了，绝不影响 prompt。 */
function writeLog(file: string, line: string): void {
  try {
    mkdirSync(dirname(file), { recursive: true });
    appendFileSync(file, `${line}\n`);
  } catch {
    /* 日志写不进去也不许影响主流程 */
  }
}

/** 本地时间戳，日志好读。 */
function stamp(now: Date = new Date()): string {
  const p = (n: number): string => String(n).padStart(2, '0');
  return `${now.getFullYear()}-${p(now.getMonth() + 1)}-${p(now.getDate())} ${p(now.getHours())}:${p(now.getMinutes())}:${p(now.getSeconds())}`;
}

/** 给 recall 套一层超时。 */
function withTimeout<T>(promise: Promise<T>, ms: number): Promise<T> {
  let timer: NodeJS.Timeout;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(`recall 超时 (>${ms}ms)`)), ms);
    timer.unref?.();
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer)) as Promise<T>;
}

/**
 * 挂 system-prompt/assemble 钩子。
 * @param ctx 插件上下文
 * @param store createStore 的返回值
 * @param cfg 配置
 */
export function setupAutoPrefill(ctx: PrefillCtx, store: PrefillStore, cfg: PrefillCfg = {}): PrefillHandle | undefined {
  if (cfg?.autoPrefill === false) return undefined;
  const topK = Math.max(1, Math.min(Number(cfg.autoPrefillTopK) || 3, 10));
  const maxChars = Math.max(PREFILL_HEADER.length + 20, Number(cfg.autoPrefillMaxChars) || 200);
  // 默认 1200ms：实测召回 p50 595ms / p90 761ms，
  // 旧的 800ms 贴着 p90，正常慢一下就误判超时白丢一次注入。键名沿用 autoPrefillTimeoutMs。
  const timeoutMs = Math.max(50, Number(cfg.autoPrefillTimeoutMs) || 1200);
  const entityWords = (cfg.autoPrefillEntityWords || DEFAULT_ENTITY_WORDS).map((w) => String(w));
  const logFile = cfg.autoPrefillLogPath || PREFILL_LOG;

  /** key(会话id:消息hash) -> 注入文本（空串也算结果：这条消息没命中） */
  const cache = new Map<string, string>();
  /** 会话 id -> { failures, blockedUntil, injectedIds } */
  const sessionState = new Map<string, PrefillSessionState>();
  const stats = { injected: 0, skipped: 0, failed: 0, cached: 0 };

  const stateOf = (sid: string): PrefillSessionState => {
    let state = sessionState.get(sid);
    if (!state) {
      state = { failures: 0, blockedUntil: 0, injectedIds: new Map() };
      sessionState.set(sid, state);
    }
    return state;
  };

  const cacheGet = (key: string): string | undefined => (cache.has(key) ? cache.get(key) : undefined);
  const cacheSet = (key: string, value: string): void => {
    if (cache.has(key)) cache.delete(key);
    cache.set(key, value);
    while (cache.size > CACHE_LIMIT) cache.delete(cache.keys().next().value as string);
  };

  const log = (fields: PrefillLogFields): void =>
    writeLog(
      logFile,
      `${stamp()} | ${fields.session || '-'} | ${fields.verdict} | ${fields.reason} | hits:${fields.hits ?? 0} | ${fields.ms ?? 0}ms`,
    );

  const info = (msg: string): void => {
    try {
      ctx.logger?.info?.(`dsh-memory auto-prefill: ${msg}`);
    } catch {
      /* 日志失败也不能影响 prompt */
    }
  };

  ctx.on(
    'system-prompt/assemble',
    async (_assembly, context, next) => {
      const startedAt = Date.now();
      let text = '';
      let sessionId = '';
      let verdict = 'skip';
      let reason = '';
      let hits = 0;

      try {
        const session = context?.agent?.session;
        if (session && session.header?.origin !== 'subagent') {
          sessionId = String(session.id || '');
          const state = stateOf(sessionId);
          const now = Date.now();

          if (now < state.blockedUntil) {
            reason = 'fail-cooldown';
          } else {
            const raw = lastUserText(session);
            if (!raw) {
              reason = 'no-message';
            } else if (lastUserHasOnlyMedia(session)) {
              reason = 'media-only';
            } else {
              const judge = judgeTrigger(raw, { autoPrefillMinChars: cfg.autoPrefillMinChars, autoPrefillEntityWords: entityWords });
              if (!judge.trigger) {
                reason = judge.reason;
              } else {
                reason = judge.reason;
                // 召回词：先剔掉整行括号包裹的 harness 指令行（剔空了回退原文），再截断
                const query = recallQuery(raw);
                // 缓存 key = 会话 + 消息原文 hash + 观察版本号。
                // 观察写入（saveObservation）在写事务里把 meta.observation_epoch +1，
                // 版本一变旧 key 就不再命中，等价于「命中前比版本」；不需要另建失效接口，
                // 也不会被普通记忆写入误伤（那些不动观察版本）。读不到版本就退 0，
                // 只影响失效灵敏度，绝不影响注入本身。
                let epoch = 0;
                try {
                  epoch = Number(store.observationVersion?.() ?? 0) || 0;
                } catch {
                  epoch = 0;
                }
                const key = `${sessionId}:${createHash('sha256').update(raw).digest('hex').slice(0, 16)}:${epoch}`;
                const cached = cacheGet(key);
                // 缓存优先：同一回合里多步工具循环会反复 assemble，
                // 同一条消息必须每次都注入同一段文本，否则提示词前缀每步都变。
                if (cached !== undefined) {
                  text = cached;
                  hits = cached ? cached.split('\n').length - 1 : 0;
                  reason = `${judge.reason}+cache`;
                  verdict = text ? 'inject' : 'miss';
                  stats.cached += 1;
                } else {
                  try {
                    // 多取几倍候选只为给关键词重排留位置：嵌入只算一次，不多花检索调用
                    const candidateK = Math.min(8, Math.max(topK * CANDIDATE_FACTOR, topK));
                    // caller/session 只进检索日志（2026-11-01），不改变检索本身
                    // 送进去的是清理后的 query：memory_recall.log 里 caller=prefill 的 query 也就只剩真意图
                    const items = await withTimeout(
                      store.recall(query.slice(0, QUERY_MAX_CHARS), candidateK, {
                        caller: 'prefill',
                        session: sessionId || null,
                        origin: session ? 'user' : null,
                      }),
                      timeoutMs,
                    );
                    const ranked = rankByKeywords(attachIds(store, items || []), query);
                    // 冷却只压「同一条记忆」：刚注入过的 10 分钟内不再重复，检索照跑
                    const fresh = ranked.filter((item) => !isRecentlyInjected(state, item));
                    const picked = fresh.slice(0, topK);
                    text = buildPrefillText(picked, maxChars);
                    hits = text ? text.split('\n').length - 1 : 0;
                    cacheSet(key, text);
                    state.failures = 0;
                    if (text) {
                      stats.injected += 1;
                      rememberInjected(state, picked);
                    } else if (ranked.length > 0 && fresh.length === 0) {
                      reason = `${judge.reason}+all-repeated`;
                    }
                  } catch (error) {
                    text = '';
                    state.failures += 1;
                    stats.failed += 1;
                    if (state.failures >= FAIL_STREAK_LIMIT) {
                      state.blockedUntil = Date.now() + FAIL_COOLDOWN_MS;
                      reason = `${judge.reason}+failed(${(error as Error)?.message || error})+muted`;
                    } else {
                      reason = `${judge.reason}+failed(${(error as Error)?.message || error})`;
                    }
                    info(reason);
                  }
                  verdict = text ? 'inject' : 'miss';
                }
              }
            }
          }
        } else {
          reason = session ? 'subagent' : 'no-session';
        }
      } catch (error) {
        text = '';
        reason = `error:${(error as Error)?.message || error}`;
        info(reason);
      }

      const result = await next();
      if (text) {
        try {
          insertPrefillSection(result?.sections, text);
        } catch (error) {
          info(`注入失败，忽略：${(error as Error)?.message || error}`);
        }
      } else {
        stats.skipped += 1;
      }
      log({
        session: sessionId,
        verdict: text ? 'inject' : verdict === 'miss' ? 'miss' : 'skip',
        reason: reason || 'unknown',
        hits,
        ms: Date.now() - startedAt,
      });
      return result;
    },
    { prepend: true },
  );

  ctx.logger?.info?.(
    `dsh-memory auto-prefill 已挂载（分级触发，topK=${topK}，maxChars=${maxChars}，timeout=${timeoutMs}ms，日志 ${logFile}）`,
  );
  return { stats, cache, sessionState };
}
