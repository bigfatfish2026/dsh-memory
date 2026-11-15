/**
 * 记忆正文 lint。
 *
 * 起因：维护者审 141 条新记忆，指出拆分/合并会产出两类碎片——
 *   ① 「但是/所以/因此」开头的半句，单条读不出发生了什么；
 *   ② 「这一步」「它」「该条」这种指代，离开上文就没头没尾。
 * 目标只有一条：记忆库里每一条单独读都能看懂。
 *
 * 这一层是代码兜底，比提示词硬，但它只做**拒绝**：
 *   - 纯函数、无副作用、不改写正文、不截断正文、不补词；
 *   - 打回后的重写交给模型（各 agent 自己重发一次请求），这里不管；
 *   - 200 字上限和拆分逻辑不在这个文件里，一个字都不动。
 */

/** 正文开头不许出现的连接词（长的排前面，命中时 match 取最长那个）。 */
export const LEADING_CONJUNCTIONS = [
  '但是', '但', '所以', '因此', '另外', '此外', '并且', '而', '且',
  '同时', '于是', '然后', '还有', '以及', '总之',
];

/**
 * 只认「句首 / 分句首」的指代词。
 * 「它」「此」单字太容易撞词（其它、此前、此外……），分句首命中后再看后一个字是否成词。
 */
export const BARE_REFERENCES = [
  '这一步', '该步', '上述', '上面', '前者', '后者', '该结论', '这个改动',
  '那条', '该条', '它', '此',
];

/** 「此」后面紧跟这些字时是时间/方位词，不是指代，不算命中。 */
const CI_COMPOUND = new Set(['前', '后', '时', '刻', '间', '外', '中', '余', '他', '么', '起']);

/**
 * 「它」「此」这种单字指代只在**整条正文的第一个字**才算命中（2026-11-06 t-mw515yk8）。
 * 起因：存量误杀「用户安装了 dsh-github-mcp 插件，它是官方远程 MCP…」——
 * 同一个句子里对象已经写清了，逗号后面接个「它」读得懂，不该按分句首一视同仁地打回。
 * 「这一步」「上述」这类短语照旧按分句首判。
 */
const SINGLE_CHAR_REFS = new Set(['它', '此']);

/** 「而」后面接这些字构成复合词，不是转折/承接连接词，不当开头连接词。 */
const ER_COMPOUNDS = ['而且', '而已', '而后', '而况'];

const CLAUSE_BREAK = /[。！？；，,;!?]/;
const WHITESPACE = /\s/;

/**
 * 引号字符：正文在引用规则字面串时必然带引号（2026-11-06 t-mw515yk8）。
 * 命中的元说明串紧邻引号 = 在举例/引用，不算真的写了元说明。
 */
const QUOTE_CHARS = new Set(['「', '」', '『', '』', '“', '”', '"', '‘', '’', "'", '`']);

/** match 前一个字或后一个字是引号 → 视为在引用，不算命中。 */
function nearQuote(text: string, start: number, len: number): boolean {
  const before = start > 0 ? text[start - 1] : '';
  const after = text[start + len] || '';
  return QUOTE_CHARS.has(before) || QUOTE_CHARS.has(after);
}

/** 元说明：取代说明、见上一条、正文见某节、同上、（略）都不该进正文。 */
const META_PATTERNS: Array<{ re: RegExp; hint: string }> = [
  { re: /此条被/g, hint: '删掉元说明，取代关系写进 supersedes 字段' },
  { re: /被[^。；，,！？!?\n]{0,30}?取代/g, hint: '删掉元说明，取代关系写进 supersedes 字段' },
  { re: /见上一条/g, hint: '删掉元说明，把对象写成具体名词' },
  { re: /正文见/g, hint: '删掉元说明，本条要能自己说清' },
  { re: /同上/g, hint: '删掉「同上」，把这一条的内容写全' },
  { re: /（略）/g, hint: '不许省略，缺的内容补全或删掉这个标记' },
  { re: /\(略\)/g, hint: '不许省略，缺的内容补全或删掉这个标记' },
];

const CLAIM_RE = /已验证|已确认|确认过|实测|实际测过/g;
/** 可核对的坐标：数字、路径分隔符、URL。「word.js:123」里的数字和冒号都落到这条上。 */
const COORD_RE = /\d|[\\/]/;

const MIN_CHARS = 25;

/**
 * 短句豁免用的坐标（2026-11-06 t-mw515yk8）：不足 25 字但带路径、文件名、行号、id 或数字的，
 * 说明这句已经钉在一个可核对的对象上，不该按「碎片」打回。
 * 存量误杀样例：「用户把 qq_img 目录改名为 img/qq。」——24 字，带路径。
 */
const SHORT_COORD_RE = /\d|[\\/]|[A-Za-z][\w.-]*\.[A-Za-z0-9]{1,8}\b|\b[A-Za-z][\w]*(?:[-_][\w]+)+\b/;

const HINTS: Record<string, string> = {
  leading_conjunction: '去掉这个连接词，直接从本条真正的对象写起',
  bare_reference: '把指代词换成具体名词：哪个文件、哪个参数、哪张表、哪个服务',
  too_short: `不足 ${MIN_CHARS} 字，补对象和结果，别只留半句结论`,
  meta_reference: '删掉元说明，只写事实本身',
  unverified_claim: '补坐标或删掉判断词',
};

/** 一条 lint 违规：code 是机器判据，match/index 指出命中位置，hint 是给人看的话。 */
export interface LintViolation {
  code: string;
  match: string;
  index: number;
  hint: string;
}

/** 单条正文的判定结果；ok 为真时 violations 必空。 */
export interface LintResult {
  ok: boolean;
  violations: LintViolation[];
}

/** 通过的一条：原 item 与它在批里的下标。 */
export interface AcceptedEntry<T> {
  item: T;
  index: number;
}

/** 打回的一条：额外带正文与违规清单，落 lint_reject 用。 */
export interface RejectedEntry<T> {
  item: T;
  index: number;
  text: string;
  violations: LintViolation[];
}

/** 一批的判定结果，accepted/rejected 各归各。 */
export interface LintBatchResult<T> {
  accepted: Array<AcceptedEntry<T>>;
  rejected: Array<RejectedEntry<T>>;
}

function pushViolation(list: LintViolation[], code: string, match: unknown, index: unknown, hint?: string): void {
  list.push({ code, match: String(match), index: Math.max(0, Number(index) || 0), hint: hint || HINTS[code] || '' });
}

/** 分句起点：正文开头，以及每个句读标点之后第一个非空白字符的绝对下标。 */
function clauseStarts(text: string): number[] {
  const starts = [];
  let i = 0;
  while (i < text.length) {
    if (i === 0 || CLAUSE_BREAK.test(text[i - 1])) {
      let j = i;
      while (j < text.length && WHITESPACE.test(text[j])) j += 1;
      if (j < text.length) starts.push(j);
      i = j + 1;
    } else {
      i += 1;
    }
  }
  return starts;
}

function leadingConjunctionOf(text: string): { word: string; index: number } | null {
  const body = text.replace(/^\s+/, '');
  const offset = text.length - body.length;
  for (const word of LEADING_CONJUNCTIONS) {
    if (!body.startsWith(word)) continue;
    // 「而且不对称」里的「而」是复合词的一半，不是转折词（2026-11-06 t-mw515yk8）
    if (word === '而' && ER_COMPOUNDS.some((w) => body.startsWith(w))) return null;
    return { word, index: offset };
  }
  return null;
}

function bareReferenceAt(text: string, start: number, firstCharIndex: number): string | null {
  for (const ref of BARE_REFERENCES) {
    if (!text.startsWith(ref, start)) continue;
    // 单字指代只认整条正文的第一个字，分句首不算（2026-11-06 t-mw515yk8）
    if (SINGLE_CHAR_REFS.has(ref) && start !== firstCharIndex) continue;
    if (ref === '此') {
      const next = text[start + 1] || '';
      if (CI_COMPOUND.has(next)) continue;
    }
    return ref;
  }
  return null;
}

/**
 * 记一条正文的 lint 结果。
 *
 * @param text 待入库的正文
 */
export function lintMemoryText(text: unknown): LintResult {
  const body = typeof text === 'string' ? text : String(text ?? '');
  const violations: LintViolation[] = [];

  const leading = leadingConjunctionOf(body);
  if (leading) pushViolation(violations, 'leading_conjunction', leading.word, leading.index);

  const firstCharIndex = (() => {
    let i = 0;
    while (i < body.length && WHITESPACE.test(body[i])) i += 1;
    return i < body.length ? i : -1;
  })();
  for (const start of clauseStarts(body)) {
    const ref = bareReferenceAt(body, start, firstCharIndex);
    if (ref) pushViolation(violations, 'bare_reference', ref, start);
  }

  const dense = body.replace(/\s+/g, '');
  if (dense.length < MIN_CHARS && !SHORT_COORD_RE.test(body)) pushViolation(violations, 'too_short', dense, 0);

  for (const { re, hint } of META_PATTERNS) {
    re.lastIndex = 0;
    let hit;
    while ((hit = re.exec(body)) !== null) {
      // 紧邻引号的命中是在引用规则字面串/举例，不是真写元说明（2026-11-06 t-mw515yk8）
      if (!nearQuote(body, hit.index, hit[0].length)) {
        pushViolation(violations, 'meta_reference', hit[0], hit.index, hint);
      }
      if (!re.global) break;
    }
  }

  if (CLAIM_RE.test(body) && !COORD_RE.test(body)) {
    CLAIM_RE.lastIndex = 0;
    const hit = CLAIM_RE.exec(body);
    if (hit) pushViolation(violations, 'unverified_claim', hit[0], hit.index);
  }
  CLAIM_RE.lastIndex = 0;

  return { ok: violations.length === 0, violations };
}

/** 一次判一批（merge/split 一次出好几条）：逐条判，通过的照常入库。 */
export function lintItems<T = unknown>(
  items: unknown,
  textOf: (item: T) => unknown = (item) => (item as unknown as { text?: unknown } | null | undefined)?.text,
): LintBatchResult<T> {
  const accepted: Array<AcceptedEntry<T>> = [];
  const rejected: Array<RejectedEntry<T>> = [];
  (Array.isArray(items) ? items : []).forEach((item: T, index: number) => {
    const result = lintMemoryText(textOf(item));
    if (result.ok) accepted.push({ item, index });
    else rejected.push({ item, index, text: String(textOf(item) ?? ''), violations: result.violations });
  });
  return { accepted, rejected };
}

/** 拼成一句给模型看的纠正提示（重发请求时用），不夹带正文改写。 */
export function formatLintFeedback(rejected: unknown): string {
  return ((Array.isArray(rejected) ? rejected : []) as Array<RejectedEntry<unknown>>)
    .map((entry) => {
      const detail = (entry.violations || [])
        .map((v) => `${v.code}（命中「${v.match}」）：${v.hint}`)
        .join('；');
      return `「${String(entry.text || '').slice(0, 120)}」→ ${detail}`;
    })
    .join('\n');
}

/** 落 lint_reject 时留证用的原因串：code + hint 一行，原文另存 discards.text。 */
export function lintRejectReason(violations: unknown): string {
  return ((Array.isArray(violations) ? violations : []) as LintViolation[])
    .map((v) => `${v.code}:${v.hint}`)
    .join(' | ')
    .slice(0, 500);
}

export const MEMORY_LINT_MIN_CHARS = MIN_CHARS;
export default lintMemoryText;
