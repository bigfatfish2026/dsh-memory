/**
 * observation 的「证据来源根」计算（2026-11-21，docs/observation-plan-v2.md）。
 *
 * 要解决的问题：拆分把一个原条切成 2～n 个片段后，片段各自都成了 memories 行。
 * 「两条独立证据」如果其实是同一个原条切出来的两片，就不能算两条；合并产物与它的成员
 * 同时出现在证据里也一样。所以计数不能按 memories.id 数，要按「来源根」数。
 *
 * 真库口径（行号见 src/pipeline-split.ts 与本票报告）：
 *   - 片段 source = `sleep:split:<父id>`（父是 memories.id）或 `sleep:split:queue:<dedupeKey>`；
 *   - 拆分不写 memories.supersedes，但 live 边会写 memory_link(src=子段, dst=父条,
 *     rel='supersedes', origin='curate')；历史回填脚本还写过 rel='split_of'（同为子段→父条）；
 *   - 合并新条 source='sleep:merge'，成员清单落在新条自己的 memories.supersedes 列，
 *     extra.sources 目前**没有**写（src/store/records.ts mergeMemories / buildNewRecord），
 *     memory_link 也没有合并边——只有历史回填脚本写过 rel='merged_into'。
 *     所以这里三种线索都认：extra.sources/extra.sourceRoots（方案约定字段）、
 *     merged_from/merged_into 边、sleep:merge 行的 supersedes 列。
 *
 * 纯函数：入参只有记忆行与边列表，不碰 store 实例、不查库、不调模型，可直接单测。
 */

import { foldForMatch, normalizeEntity } from '../shared/entity-normalize.js';
import { entityKeysOf } from '../store/observe-notify.js';
import { isActive } from '../store/rows.js';
import { entityList, normalizeType } from '../store/types.js';
import type { MemoryRecord } from '../store/types.js';

/** 片段来源前缀：`sleep:split:<父id>` / `sleep:split:queue:<dedupeKey>`（pipeline-split.ts partSourceOf）。 */
export const SPLIT_SOURCE_PREFIX = 'sleep:split:';
/** 队列键来源里的二级前缀；根写成 `queue:<dedupeKey>`。 */
export const SPLIT_QUEUE_PREFIX = 'queue:';
/** 合并产物的 source 前缀（src/store/records.ts mergeMemories 写死 'sleep:merge'）。 */
export const MERGE_SOURCE_PREFIX = 'sleep:merge';
/** 合并血缘落过的边：merged_from（产物→成员）/ merged_into（历史回填：成员→…）。 */
export const MERGE_RELS = ['merged_from', 'merged_into'] as const;
/** 拆分/取代边：都是 src=子段、dst=父条。 */
export const PARENT_RELS = ['supersedes', 'split_of'] as const;
/** extra 里约定的成员来源字段名（merge 产物带成员清单时用）。 */
export const MEMBER_SOURCE_KEYS = ['sources', 'sourceRoots'] as const;

/** memory_link 的最小面：只读 src/dst/rel，其它列不参与判断。 */
export interface EvidenceLink {
  src: string;
  dst: string;
  rel?: string;
}

/** 一条证据的来源根：roots 去重排序；uncertain 表示来历不明，只能单独算。 */
export interface SourceRoots {
  roots: string[];
  uncertain: boolean;
}

/** 独立来源计数结果：uncertain 的条目不计入 count，单列给调用方判断。 */
export interface IndependentSourceCount {
  count: number;
  roots: string[];
  uncertainIds: string[];
  uncertainCount: number;
}

/** selectEntityEvidence 的可选项。 */
export interface SelectEntityEvidenceOptions {
  /** 原始实体名（display/别名都行）；匹配口径见本文件 matchesEntity（归一化键 + 词边界）。 */
  entityKey?: string;
  /** 已归一化的实体键（normalizeEntity().norm）；不给就用 entityKey 现算。 */
  entityNorm?: string;
  /** 最多几条，默认 30。 */
  limit?: number;
  /** 完整边表；给了就按它算根，不给只按记忆行自身的线索算。 */
  links?: Iterable<EvidenceLink>;
  /** 已算好的根表；给了就复用，不再自己算。 */
  rootsMap?: ReadonlyMap<string, SourceRoots>;
  /**
   * 本次观察的候选实体词（display/别名都行）：正文命中句里同时出现别的候选实体词时，
   * 这条「提及」不唯一，交复核（2026-11-23 t-mwszo23a-iwxs）。不给就只按单个实体判，
   * 与加这条之前的行为一致。
   */
  candidateNorms?: string[];
}

/** evidenceBindingOf 的可选项。 */
export interface EvidenceBindingOptions {
  /** 同一批候选实体词；只用于「一句里多个候选」这条歧义判据，不参与命中规则。 */
  candidates?: Iterable<string>;
}

/** 绑定命中的规则：精确键 / 整词前缀 / 旧行正文整词。 */
export type EvidenceBindingRule = 'exact' | 'prefix' | 'text';

/**
 * 一条证据是靠什么绑到某个实体上的（t-mwsuyr33-2qmv）。
 *
 * 取证据（本文件 selectEntityEvidence）和落库复验（store/observation-records）共用
 * `evidenceBindingOf` 这一份判定，并把结果原样带进事务：事务不再只翻 memories.entities 一列。
 */
export interface EvidenceBasis {
  rule: EvidenceBindingRule;
  /** 判定用的观察实体键（normalizeEntity().norm）。 */
  entityNorm: string;
  /** 正文里命中的词（归一化后）。 */
  word: string;
  /** 可定位正文片段：命中词前后各留 BINDING_SNIPPET_PAD 个字符（exact 规则为空串）。 */
  snippet: string;
  /** 证据记录自己的实体键（旧行是空数组）。 */
  entityKeys: string[];
  /** 命中不唯一 / 片段过短：不直接拒、也不直接放行，交待复核通道。 */
  ambiguous: boolean;
  /** 歧义原因（word_too_short / snippet_too_short / multiple_entities…），写日志用。 */
  ambiguousReason: string;
}

/** selectEntityEvidence 命中的一条。 */
export interface EntityEvidenceHit {
  id: string;
  record: MemoryRecord;
  roots: string[];
  uncertain: boolean;
  /** 命中依据；事务复验直接用这份（来源根计算等老调用方可以不给）。 */
  basis?: EvidenceBasis;
}

/** 可定位片段窗口：命中词前后各留这么多归一化字符。 */
export const BINDING_SNIPPET_PAD = 16;
/** 命中词之外至少要有这么多字正文，才算「可定位片段」。 */
export const BINDING_MIN_LOCATE_CONTEXT = 4;
/** 短于这个长度的整词命中太容易撞词：不直接放行，交复核。 */
export const BINDING_REVIEW_WORD_LEN = 3;

/**
 * 句子级「顺带提及」判据（2026-11-23 t-mwszo23a-iwxs）。
 *
 * 整词 + 长度够只说明「正文里出现过这个词」，不说明这句话在讲它。命中所在句子带否定 /
 * 排除 / 对比语气时（「本轮只修水泵，未检查 Redis。」），只是顺口带一句，不能当主题命中
 * 直接放行——降级为待复核（ambiguous），既不直接拒也不直接放行。
 *
 * 只加严：这些规则只把 ambiguous 从 false 抬到 true，不改 exact / prefix 命中，也不放宽
 * 任何长度/定位判据。
 */
export const MENTION_NEGATION_RE = /未(?!来|成年)|没(?:有|检查|查|测|改|修|动|碰|处理|涉及|确认|验证|看|核对|跟进|管|用|做)|不是|并非|而非|而不是|除了|除[^。！？；\n]{0,24}外|排除|不算/;
/** 「只修 X」这类范围声明：本句在讲处理/修理的对象，别的词只是被拿来对照。 */
export const MENTION_SCOPE_RE = /只(?:修|改|动|碰|处理|管|查|看|测|负责|涉及|覆盖|针对)/;
/** 断句符：句号类 + 分号 + 换行。逗号不算句界（「只修水泵，未检查 Redis」还得在同一句里）。 */
const SENTENCE_BREAK = /[。！？!?；;\n]/;

/** 命中位置所在的整句（按 SENTENCE_BREAK 切）。 */
function sentenceAt(text: string, at: number): string {
  let from = Math.max(0, Math.min(at, text.length));
  while (from > 0 && !SENTENCE_BREAK.test(text[from - 1])) from -= 1;
  let to = Math.max(0, Math.min(at, text.length));
  while (to < text.length && !SENTENCE_BREAK.test(text[to])) to += 1;
  return text.slice(from, to);
}

/** 同一句里还出现了哪些别的候选实体词（整词、长度 ≥2）；空数组表示不歧义。 */
function otherCandidatesIn(sentence: string, key: string, candidates: Iterable<string> | undefined): string[] {
  if (!candidates) return [];
  const hay = foldForMatch(sentence);
  const out: string[] = [];
  for (const raw of candidates) {
    const norm = foldForMatch(String(raw ?? ''));
    if (!norm || norm.length < 2 || norm === key || out.includes(norm)) continue;
    if (firstWholeWordAt(hay, norm) >= 0) out.push(norm);
  }
  return out;
}

/** selectEntityEvidence 的返回：items 是截断后的，total 是截断前的命中数。 */
export interface EntityEvidenceSelection {
  entityKey: string;
  entityNorm: string;
  total: number;
  items: EntityEvidenceHit[];
}

/** 拆分来源解析结果。 */
type SplitSource = { kind: 'parent'; id: string } | { kind: 'queue'; key: string };

/** 解析 source 里的拆分层级；不是拆分来源（或只有空标记）返回 null。 */
function splitSourceOf(source: unknown): SplitSource | null {
  const tag = String(source ?? '').trim();
  if (!tag.startsWith(SPLIT_SOURCE_PREFIX)) return null;
  const rest = tag.slice(SPLIT_SOURCE_PREFIX.length).trim();
  if (!rest) return null;
  if (rest.startsWith(SPLIT_QUEUE_PREFIX)) {
    const key = rest.slice(SPLIT_QUEUE_PREFIX.length).trim();
    return key ? { kind: 'queue', key } : null;
  }
  // 历史脏数据 `sleep:split:split:<hex>` 也照父 id 处理：回查不到父条就把它当根，
  // 同一来源的两片仍然落在同一个根上（不会各算一条）。
  return { kind: 'parent', id: rest };
}

function isMergeSource(source: unknown): boolean {
  return String(source ?? '').trim().startsWith(MERGE_SOURCE_PREFIX);
}

function relOf(link: EvidenceLink): string {
  return String(link.rel ?? '').trim().toLowerCase();
}

function stringArrayOf(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return value.map((item) => String(item ?? '').trim()).filter(Boolean);
}

function sortRoots(values: Iterable<string>): string[] {
  return [...new Set([...values].map((v) => String(v).trim()).filter(Boolean))].sort();
}

/** 内部解析结果：cycle 只用于在递归里传播「环上的来历不明」，不暴露给调用方。 */
interface Resolution extends SourceRoots {
  cycle?: boolean;
}

interface ResolveContext {
  byId: Map<string, MemoryRecord>;
  linksFrom: Map<string, EvidenceLink[]>;
  linksInto: Map<string, EvidenceLink[]>;
  memo: Map<string, Resolution>;
}

/**
 * 一条记录自己声明的成员来源（合并产物）。
 *
 * 两个来源都看：
 *   - `record.extra.sources` / `record.extra.sourceRoots`（方案约定字段）；
 *   - 顶层 `record.sources` / `record.sourceRoots`——rows.ts 的 rowToRecord 会把 extra
 *     的键摊平回记录顶层（extra 不是记录里的普通字段），只查 extra 会漏掉真库读出来的行。
 */
function declaredMemberSources(record: MemoryRecord): string[] {
  const extra = record.extra && typeof record.extra === 'object' && !Array.isArray(record.extra)
    ? record.extra as Record<string, unknown>
    : null;
  const out: string[] = [];
  for (const key of MEMBER_SOURCE_KEYS) {
    out.push(...stringArrayOf(extra?.[key]));
    out.push(...stringArrayOf(record[key]));
  }
  return sortRoots(out);
}

/** 递归解析一条 id 的来源根：memo 缓存 + 调用栈防环，环上的条目记 uncertain。 */
function resolveRoots(id: string, ctx: ResolveContext, stack: Set<string>): Resolution {
  const cached = ctx.memo.get(id);
  if (cached) return cached;
  // 环：A 的 source 指向 B、B 又指回 A（或自指）。不再往下追，记一条来历不明。
  if (stack.has(id)) return { roots: [id], uncertain: true, cycle: true };

  const record = ctx.byId.get(id);
  if (!record) {
    // 父条/成员不在入参里：它是谁已经写在 source/边里了，就把这个 id 当根。
    const outside: Resolution = { roots: [id], uncertain: false };
    ctx.memo.set(id, outside);
    return outside;
  }

  stack.add(id);
  const roots: string[] = [];
  let hasClue = false;
  let cycle = false;

  /** 把一条线索指向的目标并进来；目标就在当前调用栈上就是环，标出来不再往下追。 */
  const inherit = (target: string): void => {
    const key = String(target ?? '').trim();
    if (!key) return;
    hasClue = true;
    if (stack.has(key)) {
      roots.push(key);
      cycle = true;
      return;
    }
    const sub = resolveRoots(key, ctx, stack);
    roots.push(...sub.roots);
    if (sub.cycle) cycle = true;
  };

  // ① 拆分来源：父是根候选，父本身也是片段就继续往上追到最顶层（防孙段算新来源）。
  const split = splitSourceOf(record.source);
  if (split?.kind === 'queue') {
    roots.push(`${SPLIT_QUEUE_PREFIX}${split.key}`);
    hasClue = true;
  } else if (split?.kind === 'parent') {
    inherit(split.id);
  }

  // ② 合并产物自带的成员来源：extra.sources / extra.sourceRoots（含摊平后的顶层键）。
  //    集合里若是成员 id 而不是根 id，跟着追一层，避免「产物 + 成员」被算成两条。
  const declared = declaredMemberSources(record);
  for (const member of declared) inherit(member);

  // ③ memory_link 的合并边：merged_from（产物→成员）与历史 merged_into 两个方向都认。
  for (const link of ctx.linksFrom.get(id) ?? []) {
    const rel = relOf(link);
    if ((MERGE_RELS as readonly string[]).includes(rel)) inherit(String(link.dst ?? ''));
  }
  for (const link of ctx.linksInto.get(id) ?? []) {
    const rel = relOf(link);
    if ((MERGE_RELS as readonly string[]).includes(rel)) inherit(String(link.src ?? ''));
  }

  // ④ mergeMemories 不写 extra.sources、也不写边：成员清单只落在产物自己的 supersedes 列。
  //    只对 source='sleep:merge*' 的行这么读，普通 update/save 的 supersedes 不当合并血缘。
  if (!declared.length && isMergeSource(record.source)) {
    for (const member of entityList(record.supersedes)) inherit(member);
  }

  // ⑤ 历史 live 边：src=子段、dst=父条（supersedes / split_of）。父条的根并进来。
  for (const link of ctx.linksFrom.get(id) ?? []) {
    const rel = relOf(link);
    if (!(PARENT_RELS as readonly string[]).includes(rel)) continue;
    inherit(String(link.dst ?? ''));
  }

  stack.delete(id);
  const sourceTag = String(record.source ?? '').trim();
  // 没有任何来源线索：自己就是根；source 也空说明来历不明，调用方不能拿它凑独立来源。
  const out: Resolution = hasClue
    ? { roots: sortRoots(roots), uncertain: cycle }
    : { roots: [id], uncertain: !sourceTag };
  if (cycle) out.cycle = true;
  ctx.memo.set(id, out);
  return out;
}

function indexLinks(links: Iterable<EvidenceLink>, side: 'src' | 'dst'): Map<string, EvidenceLink[]> {
  const out = new Map<string, EvidenceLink[]>();
  for (const link of links) {
    const key = String(link?.[side] ?? '').trim();
    if (!key) continue;
    const list = out.get(key);
    if (list) list.push(link);
    else out.set(key, [link]);
  }
  return out;
}

/**
 * 一组记忆行 + 边表 → 每条的来源根。
 *
 * 根的口径：
 *   - `sleep:split:<父id>`：父条（沿 source 递归到最顶层）是根；
 *   - `sleep:split:queue:<key>`：根是 `queue:<key>`；
 *   - 合并产物：extra.sources / extra.sourceRoots，或 merged_from/merged_into 边，
 *     或 sleep:merge 行的 supersedes 列；
 *   - supersedes / split_of 边指向父条：父条的根并进来；
 *   - 以上都没有：roots=[自身 id]；source 也空则 uncertain=true（来历不明）。
 *
 * @param records 记忆行（至少 id/source/supersedes/extra/status）
 * @param links   memory_link 边（src/dst/rel）；不给就只用记忆行自身的线索
 */
export function sourceRootsOf(
  records: Iterable<MemoryRecord>,
  links: Iterable<EvidenceLink> = [],
): Map<string, SourceRoots> {
  const byId = new Map<string, MemoryRecord>();
  for (const record of records) {
    const id = String(record?.id ?? '').trim();
    if (!id) continue;
    byId.set(id, record);
  }
  const linkList = [...links];
  const ctx: ResolveContext = {
    byId,
    linksFrom: indexLinks(linkList, 'src'),
    linksInto: indexLinks(linkList, 'dst'),
    memo: new Map(),
  };
  const out = new Map<string, SourceRoots>();
  for (const id of byId.keys()) {
    const resolved = resolveRoots(id, ctx, new Set());
    out.set(id, { roots: resolved.roots, uncertain: resolved.uncertain });
  }
  return out;
}

/**
 * 把证据 id 的来源根取并集后计数（同一原条的多片、多级拆分都只算一条）。
 *
 * uncertain 的条目不参与并集计数，单独列在 uncertainIds 里（调用方决定能不能凑两条）。
 * 不在 rootsMap 里的 id 也一样按 uncertain 处理——说明拿不到它的来历，不能冒充独立来源。
 * 空 id 忽略，重复 id 去重（rootsMap 取并集本来就幂等）。
 */
export function independentSourceCount(
  ids: Iterable<unknown>,
  rootsMap: ReadonlyMap<string, SourceRoots>,
): IndependentSourceCount {
  const roots = new Set<string>();
  const uncertain = new Set<string>();
  for (const raw of ids) {
    const id = String(raw ?? '').trim();
    if (!id) continue;
    const hit = rootsMap.get(id);
    if (!hit) {
      uncertain.add(id);
      continue;
    }
    if (hit.uncertain) {
      uncertain.add(id);
      continue;
    }
    for (const root of hit.roots) {
      const key = String(root ?? '').trim();
      if (key) roots.add(key);
    }
  }
  return {
    count: roots.size,
    roots: sortRoots(roots),
    uncertainIds: [...uncertain].sort(),
    uncertainCount: uncertain.size,
  };
}

/** 词内字符：归一化后只把 ASCII 字母/数字/下划线算词内；空格、标点、汉字、路径分隔符都算词边界。 */
const WORD_CHAR = /[0-9a-z_]/;

/** text[at, at+len) 两侧都不粘词内字符：'read' 不命中 'readonly'，命中 'read 的' / '-read'。 */
function wholeWordAt(text: string, at: number, len: number): boolean {
  const before = at > 0 ? text[at - 1] : '';
  const after = at + len < text.length ? text[at + len] : '';
  return !WORD_CHAR.test(before) && !WORD_CHAR.test(after);
}

/** key 以 word 开头且 word 之后正好是词边界：'anri的' 命中 'anri'，'readonly' 不命中 'read'。 */
function startsWithWholeWord(key: string, word: string): boolean {
  return key.startsWith(word) && !WORD_CHAR.test(key.slice(word.length, word.length + 1));
}

/** 第一个整词出现位置；没有返回 -1。 */
function firstWholeWordAt(text: string, word: string): number {
  if (!text || !word) return -1;
  for (let at = text.indexOf(word); at >= 0; at = text.indexOf(word, at + 1)) {
    if (wholeWordAt(text, at, word.length)) return at;
  }
  return -1;
}

/** 命中词前后各留 BINDING_SNIPPET_PAD 个字符：落库复验与日志都拿这段判断「可定位」。 */
function snippetAt(text: string, at: number, len: number): string {
  const from = Math.max(0, at - BINDING_SNIPPET_PAD);
  const to = Math.min(text.length, at + len + BINDING_SNIPPET_PAD);
  return text.slice(from, to);
}

/** 片段里命中词之外的字符数：太少就定位不到，按歧义交复核。 */
function locateContextOf(text: string, at: number, len: number): number {
  const from = Math.max(0, at - BINDING_SNIPPET_PAD);
  const to = Math.min(text.length, at + len + BINDING_SNIPPET_PAD);
  return Math.max(0, to - from - len);
}

/**
 * 一条记录绑到观察实体键的依据；绑不上返回 null。
 *
 * 规则与 selectEntityEvidence 完全同一份（取证据和事务复验共用，2026-11-23 t-mwsuyr33-2qmv）：
 * 1) 记录实体键与观察键归一化后相等 → exact；
 * 2) 记录实体键以观察键为「完整词前缀」且正文里观察键作为独立词出现（观察键长度 ≥3）→ prefix；
 * 3) 老记录实体列为空：正文整词兜底 → text。
 *
 * text 规则额外算歧义标记（2026-11-23 t-mwszo23a-iwxs）：
 *   - 观察键只有 2 个字符 / 片段之外没有足够正文可定位 → word_too_short / snippet_too_short；
 *   - 命中所在句带否定、排除、对比或「只修 X」范围语气 → negated_mention / scope_limited_mention；
 *   - 同一句里还有别的候选实体词 → multiple_entities_in_sentence。
 * 歧义只排待复核（调用方），不直接拒也不直接放行；正文里连整词都没出现（例如 'read'
 * 只出现在 'readonly' 里、或只是泛泛提及）则返回 null，照旧拒绝。
 *
 * 前缀规则只在「记录键是观察键的完整词前缀 + 正文同样有整词」时成立，
 * 单独的前缀相似（'tree' vs 'treehouse'）拿不到 basis；已带实体键的行以实体列为准，
 * 不因为在否定句里提了一句就被降级。
 */
export function evidenceBindingOf(record: MemoryRecord, norm: string, options: EvidenceBindingOptions = {}): EvidenceBasis | null {
  const key = String(norm ?? '').trim();
  if (!key) return null;
  const keys = entityKeysOf(entityList(record?.entities));
  if (keys.includes(key)) {
    return {
      rule: 'exact',
      entityNorm: key,
      word: key,
      snippet: '',
      entityKeys: keys,
      ambiguous: false,
      ambiguousReason: '',
    };
  }
  const text = foldForMatch(String(record?.text ?? ''));
  const at = firstWholeWordAt(text, key);
  if (at < 0) return null;
  const snippet = snippetAt(text, at, key.length);
  if (key.length >= 3 && keys.some((item) => startsWithWholeWord(item, key))) {
    return {
      rule: 'prefix',
      entityNorm: key,
      word: key,
      snippet,
      entityKeys: keys,
      ambiguous: false,
      ambiguousReason: '',
    };
  }
  // 有实体键的行不走正文兜底：纯正文命中拿不到精确键交集，口径同落库复验的老路径
  if (keys.length > 0) return null;
  // 正文兜底至少要 2 个字符（1 个字符太容易撞），2 个字符命中本身算歧义
  if (key.length < 2) return null;
  const reasons: string[] = [];
  if (key.length < BINDING_REVIEW_WORD_LEN) reasons.push('word_too_short');
  if (locateContextOf(text, at, key.length) < BINDING_MIN_LOCATE_CONTEXT) reasons.push('snippet_too_short');
  // 句中语气：顺带提及（否定/排除/对比/只修 X）不是主题命中，降级交复核。
  const sentence = sentenceAt(text, at);
  if (MENTION_NEGATION_RE.test(sentence)) reasons.push('negated_mention');
  if (MENTION_SCOPE_RE.test(sentence)) reasons.push('scope_limited_mention');
  if (otherCandidatesIn(sentence, key, options.candidates).length) reasons.push('multiple_entities_in_sentence');
  return {
    rule: 'text',
    entityNorm: key,
    word: key,
    snippet,
    entityKeys: keys,
    ambiguous: reasons.length > 0,
    ambiguousReason: reasons.join('+'),
  };
}

function evidenceTimeOf(record: MemoryRecord): string {
  return String(record.validFrom || record.ts || '');
}

/**
 * 取某个实体下可以作为 observation 证据的条目：active 的 world/experience，按
 * validFrom||ts 倒序（同 store/entities.ts 的 entityView 口径）取 limit 条，并带上来源根。
 *
 * active 口径同 rows.ts isActive：status 空/NULL 也算 active。roots 默认按入参全部记录
 * 现算（这样父条即使不在命中集里也能追到），也可以传 rootsMap 复用已有结果。
 */
export function selectEntityEvidence(
  records: Iterable<MemoryRecord>,
  options: SelectEntityEvidenceOptions = {},
): EntityEvidenceSelection {
  const list = [...records];
  const entityKey = String(options.entityKey ?? '').trim();
  const entityNorm = normalizeEntity(
    String(options.entityNorm ?? '').trim() || entityKey,
  ).norm;
  const rawLimit = Number(options.limit);
  const limit = Number.isFinite(rawLimit) && rawLimit > 0 ? Math.floor(rawLimit) : 30;
  const rootsMap = options.rootsMap
    ?? sourceRootsOf(list, options.links ? [...options.links] : []);

  // 命中依据与命中项一起排：同一次 evidenceBindingOf 的结论既用来筛，也随 items 带回调用方
  const matched = list
    .map((record) => ({ record, basis: evidenceBindingOf(record, entityNorm, { candidates: options.candidateNorms }) }))
    .filter((item): item is { record: MemoryRecord; basis: EvidenceBasis } => {
      if (!item.basis) return false;
      const record = item.record;
      if (!isActive(record)) return false;
      return ['world', 'experience'].includes(normalizeType(record.type, record.tags, record.text));
    });
  matched.sort((a, b) => {
    const byTime = evidenceTimeOf(b.record).localeCompare(evidenceTimeOf(a.record));
    return byTime !== 0 ? byTime : String(a.record.id).localeCompare(String(b.record.id));
  });

  const items = matched.slice(0, limit).map(({ record, basis }) => {
    const hit = rootsMap.get(record.id);
    return {
      id: record.id,
      record,
      roots: hit ? [...hit.roots] : [record.id],
      uncertain: hit ? hit.uncertain : true,
      basis,
    };
  });
  return { entityKey, entityNorm, total: matched.length, items };
}
