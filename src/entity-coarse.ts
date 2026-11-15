/**
 * 写入时的零成本粗实体（2026-11-13，docs/dsh-memory-save-queue-2026.md §2.4.1）。
 *
 * 为什么必须有：memory_save 的 entities 参数删掉之后，写入闸的 related 通道
 * （write-gate.js → relatedEntriesScored）在 entities 为空时直接返回 []，近邻信息一断，
 * 去重和合并的候选质量当场掉。所以实体要在**过闸之前**算，不能等队列。
 *
 * 做法：entities 表现有 norm 建词典，正文折叠（NFKC + 小写 + 空白折叠）后最长匹配，最多 4 个。
 * 词典常驻进程内存，5 分钟过期或活跃记忆数变化 >100 条才重建；单条正文不查全表。
 *
 * 泛词不进来：词典建成时按 df = 该实体在 memory_entity 里的出现次数 / active 记忆数 过滤，
 * 超过阈值（默认 0.05，即「合并/去重/agent」这类）直接不进词典。回填那轮实测这类词占了
 * 匹配命中里的一大半噪声，见票面「四、泛词过滤」。
 */
import type { DatabaseSync } from 'node:sqlite';
import { foldForMatch, dropReason, isGenericName } from './shared/entity-normalize.js';

/** 词典里的一行：norm 的折叠名 fl 用来扫正文，df 记出现次数。 */
export interface CoarseWord {
  id: number;
  norm: string;
  display: string;
  etype: string;
  fl: string;
  df: number;
}

/** 粗实体命中：at 是正文里的折叠下标（排序用）。 */
export interface CoarseHit {
  id: number;
  norm: string;
  display: string;
  etype: string;
  at: number;
}

/** 建词典时被丢掉的行数，按原因分。 */
export interface CoarseDropped {
  single: number;
  generic: number;
  stopword: number;
  dirty: number;
  dup: number;
  capped: number;
}

/** buildCoarseDict 的产物；checkedAt 只在内层缓存里出现（TTL 内查活跃数的时间戳）。 */
export interface CoarseDict {
  words: CoarseWord[];
  builtAt: number;
  activeCount: number;
  dropped: CoarseDropped;
  checkedAt?: number;
}

/** 实例上的累计计数（stats 的原料）。 */
export interface CoarseCounters {
  builds: number;
  matches: number;
  totalMs: number;
  maxMs: number;
  overBudget: number;
  shrunk: number;
  hits: number;
  misses: number;
}

/** stats() 的产物：计数 + 词表快照。 */
export interface CoarseStats extends CoarseCounters {
  [key: string]: unknown;
  avgMs: number;
  words: number;
  activeCount: number;
  builtAt: number;
  maxWords: number;
}

/** CoarseMatcher 的构造参数。 */
export interface CoarseMatcherOptions {
  db?: () => DatabaseSync;
  genericDf?: number;
  genericDfMin?: number;
  maxWords?: number;
  budgetMs?: number;
  log?: ((line: string) => void) | null;
  now?: () => number;
}

/** 词典 TTL：5 分钟。比这更频繁地重建没必要，entities 表不是快变表。 */
export const COARSE_TTL_MS = 5 * 60000;
/** 活跃记忆数变化超过这个条数就重建（新写入的实体名要尽快可用）。 */
export const COARSE_MEMORY_DELTA = 100;
/** 单条最多取几个实体（§2.4.1）。 */
export const COARSE_MAX_HITS = 4;
/** 耗时超预算时把词表降到多少条（票面：超了就降到 1000 条高频的）。 */
export const COARSE_SHRINK_WORDS = 1000;
/** 泛词闸的最小出现次数：df 不到这个数不算泛词（小库/新词不会被比例一刀切）。 */
export const COARSE_GENERIC_MIN_DF = 20;

const WORD = /[a-z0-9_]/;
/** df 闸只对这两类生效：它们是「合并/去重/agent/ask」这类泛词的落点。 */
const GENERIC_TYPES = new Set(['concept', 'unknown']);

/**
 * 建词典。
 * @param {import('node:sqlite').DatabaseSync} db
 * @param {{genericDf?:number, maxWords?:number}} opts
 * @returns {{words:Array<{id:number,norm:string,display:string,etype:string,fl:string,df:number}>, builtAt:number, activeCount:number, dropped:string[]}}
 */
export function buildCoarseDict(db: DatabaseSync, { genericDf = 0.05, genericDfMin = COARSE_GENERIC_MIN_DF, maxWords = 4000 }: { genericDf?: number; genericDfMin?: number; maxWords?: number } = {}): CoarseDict {
  const activeCount = Number(
    (db.prepare("SELECT count(*) AS n FROM memories WHERE coalesce(status,'') IN ('','active')").get() as { n?: unknown } | undefined)?.n || 0,
  );
  const rows = db
    .prepare(`SELECT e.id, e.norm, e.display, e.etype,
                     (SELECT count(*) FROM memory_entity me WHERE me.entity_id = e.id) AS df
                FROM entities e
               WHERE e.status = 'active'
               ORDER BY df DESC, e.id`)
    .all() as Array<{ id: unknown; norm: string; display: unknown; etype: unknown; df: unknown }>;
  const words: CoarseWord[] = [];
  const dropped: CoarseDropped = { single: 0, generic: 0, stopword: 0, dirty: 0, dup: 0, capped: 0 };
  const seen = new Set();
  for (const r of rows) {
    const fl = foldForMatch(r.norm);
    if (!fl || fl.length < 2) { dropped.single += 1; continue; }
    if (dropReason(fl)) { dropped.dirty += 1; continue; }
    // 黑名单先于 df：这些词在库里 df 还低，按比例永远拦不到，但正文里到处都是
    if (isGenericName(r.norm)) { dropped.stopword = (dropped.stopword || 0) + 1; continue; }
    const df = Number(r.df) || 0;
    // df 闸只拦泛概念词（concept / unknown）。纯按比例砍会误杀最该命中的专名：
    // 真库 4139 条时树莓派 df=642（15.5%）、dsh-memory df=1131（27%），全被 0.05 砍掉，
    // 正文只剩「memory」「温度」这类碎片。票面「跳过 df 最高的泛词」指的是
    // 「合并/去重/agent/ask」这种概念词，device/repo/tool 命中就该写。
    if (GENERIC_TYPES.has(String(r.etype || 'unknown')) && df >= genericDfMin && activeCount > 0 && df / activeCount > genericDf) {
      dropped.generic += 1;
      continue;
    }
    if (seen.has(fl)) { dropped.dup += 1; continue; }
    seen.add(fl);
    if (words.length >= Math.max(1, Number(maxWords) || 4000)) { dropped.capped += 1; continue; }
    words.push({ id: Number(r.id), norm: String(r.norm), display: String(r.display || r.norm), etype: String(r.etype || 'unknown'), fl, df });
  }
  // 匹配按长度降序扫（长名优先，避免「dsh」吃掉「dsh-memory」）
  words.sort((a, b) => b.fl.length - a.fl.length || a.fl.localeCompare(b.fl));
  return { words, builtAt: Date.now(), activeCount, dropped };
}

/**
 * 最长匹配。纯内存字符串扫描，不碰库。
 * @param {string} text 正文
 * @param {Array} words buildCoarseDict 的 words
 * @param {number} maxHits
 * @returns {Array<{id:number,norm:string,display:string,etype:string,at:number}>}
 */
export function matchCoarse(text: string, words: CoarseWord[], maxHits = COARSE_MAX_HITS): CoarseHit[] {
  const folded = foldForMatch(text);
  if (!folded || !words.length) return [];
  const hits: CoarseHit[] = [];
  const used: Array<[number, number]> = [];
  for (const w of words) {
    if (hits.length >= maxHits) break;
    let from = 0;
    while (from <= folded.length - w.fl.length) {
      const i = folded.indexOf(w.fl, from);
      if (i < 0) break;
      from = i + 1;
      // 英文/数字名不许切在词中间：dsh 不能命中 dsh-memory 里的 dsh
      const before = i > 0 ? folded[i - 1] : '';
      const after = folded[i + w.fl.length] || '';
      if (WORD.test(w.fl[0]) && before && WORD.test(before)) continue;
      if (WORD.test(w.fl[w.fl.length - 1]) && after && WORD.test(after)) continue;
      if (used.some(([s, e]) => i < e && i + w.fl.length > s)) continue;
      hits.push({ id: w.id, norm: w.norm, display: w.display, etype: w.etype, at: i });
      used.push([i, i + w.fl.length]);
      break;
    }
  }
  return hits.sort((a, b) => a.at - b.at);
}

/**
 * 粗实体匹配器：词典缓存 + TTL + 预算保护（2026-11-15 收成类）。
 *
 * 状态全在实例上：cache（词表 + builtAt/activeCount）、maxWordsNow、预算统计。
 * buildCoarseDict / matchCoarse 仍是纯函数（给定 db 与词表就出结果），不塞进类里。
 */
export class CoarseMatcher {
  #cache: CoarseDict | null = null;

  #maxWordsNow: number;

  #stats: CoarseCounters = { builds: 0, matches: 0, totalMs: 0, maxMs: 0, overBudget: 0, shrunk: 0, hits: 0, misses: 0 };

  #db: (() => DatabaseSync) | undefined;

  #genericDf: number;

  #genericDfMin: number;

  #budgetMs: number;

  #log: ((line: string) => void) | null | undefined;

  #now: () => number;

  /**
   * @param {object} opts
   * @param { => import('node:sqlite').DatabaseSync} opts.db 取连接的函数（懒建，别在模块加载时连库）
   * @param {number} [opts.genericDf=0.05] 泛词闸：df / active > 此值不进词典
   * @param {number} [opts.genericDfMin=20] df 不到这个数不判泛词
   * @param {number} [opts.maxWords=4000] 词典词数上限
   * @param {number} [opts.budgetMs=3] 单条耗时预算，连续超标就降到 1000 条高频词
   * @param {(line:string)=>void} [opts.log]
   * @param { => number} [opts.now]
   */
  constructor({
    db,
    genericDf = 0.05,
    genericDfMin = COARSE_GENERIC_MIN_DF,
    maxWords = 4000,
    budgetMs = 3,
    log,
    now = Date.now,
  }: CoarseMatcherOptions = {}) {
    this.#db = db;
    this.#genericDf = genericDf;
    this.#genericDfMin = genericDfMin;
    this.#maxWordsNow = Math.max(1, Number(maxWords) || 4000);
    this.#budgetMs = budgetMs;
    this.#log = log;
    this.#now = now;
  }

  #activeCount(): number {
    try {
      return Number((this.#db!().prepare("SELECT count(*) AS n FROM memories WHERE coalesce(status,'') IN ('','active')").get() as { n?: unknown } | undefined)?.n || 0);
    } catch {
      return 0;
    }
  }

  refresh(force = false): CoarseDict {
    const t = this.#now();
    // TTL 内才复用；过了 5 分钟必须重建（否则旧词表会一直被续命，新实体永远进不来）。
    // 记忆数变化 >100 条是 TTL 内的提前重建信号；每 30s 才查一次 count，省得每条都扫。
    if (!force && this.#cache) {
      if (t - this.#cache.builtAt < COARSE_TTL_MS) {
        if (t - (this.#cache.checkedAt || 0) < 30000) return this.#cache;
        const n = this.#activeCount();
        if (Math.abs(n - this.#cache.activeCount) <= COARSE_MEMORY_DELTA) {
          this.#cache = { ...this.#cache, checkedAt: t };
          return this.#cache;
        }
      }
    }
    const dict = buildCoarseDict(this.#db!(), {
      genericDf: this.#genericDf, genericDfMin: this.#genericDfMin, maxWords: this.#maxWordsNow,
    });
    this.#cache = dict;
    this.#stats.builds += 1;
    this.#log?.(`[entity-coarse] 词典重建：${dict.words.length} 个 norm（active ${dict.activeCount}，泛词跳过 ${dict.dropped.generic}）`);
    return this.#cache;
  }

  /** 单条正文的粗实体；任何异常都返回 []，绝不因为实体匹配挡住写记忆。 */
  match(text: string): CoarseHit[] {
    const t0 = this.#now();
    try {
      const dict = this.refresh(false);
      const hits = matchCoarse(text, dict.words);
      const ms = this.#now() - t0;
      this.#stats.matches += 1;
      this.#stats.totalMs += ms;
      this.#stats.maxMs = Math.max(this.#stats.maxMs, ms);
      if (hits.length) this.#stats.hits += hits.length;
      else this.#stats.misses += 1;
      if (ms > Math.max(1, Number(this.#budgetMs) || 3)) {
        this.#stats.overBudget += 1;
        // 连续超标（20 条里 5 条以上）且词表还大于 1000：降级到 1000 条高频词再试一次
        if (this.#stats.overBudget >= 5 && this.#maxWordsNow > COARSE_SHRINK_WORDS) {
          this.#maxWordsNow = COARSE_SHRINK_WORDS;
          this.#stats.shrunk += 1;
          this.#log?.(`[entity-coarse] 单条 ${ms}ms 超预算，词表降到 ${COARSE_SHRINK_WORDS} 条高频`);
          this.#cache = null;
          this.refresh(true);
        }
      }
      return hits;
    } catch (error) {
      this.#log?.(`[entity-coarse] 匹配失败（按无实体处理）：${String((error && (error as { message?: unknown }).message) || error)}`);
      return [];
    }
  }

  /** 只读：给排障/测试看词表大小和耗时。 */
  stats(): CoarseStats {
    const s = this.#stats;
    return {
      ...s,
      avgMs: s.matches ? Number((s.totalMs / s.matches).toFixed(3)) : 0,
      words: this.#cache?.words.length ?? 0,
      activeCount: this.#cache?.activeCount ?? 0,
      builtAt: this.#cache?.builtAt ?? 0,
      maxWords: this.#maxWordsNow,
    };
  }
}

/** 旧工厂名保持不变；参数见 CoarseMatcher 构造器。 */
export function createCoarseMatcher(opts: CoarseMatcherOptions = {}): CoarseMatcher {
  return new CoarseMatcher(opts);
}
