/**
 * 唯一去重闸（未接线版本，2026-11-05）。
 *
 * 只做三件事：召回候选（字面 hash 全表 + memvec top10 + 同实体 related）、
 * 裁决（规则层 + 可注入 LLM）、DUPLICATE 落 discards。真正写 memories 的
 * 动作不在这里，等 tick 二接线；现在默认开关关着，sleep 流程一行没接。
 *
 * 设计见 docs/sleep-pipeline-redesign.md 第 1(a)、2.3 节。
 */
import type { DatabaseSync } from 'node:sqlite';
import { sha1Hex } from './fingerprint.js';
import { relatedEntriesScored } from './related.js';
import type { MemoryRecord } from './store/types.js';

/** 向量近邻函数：给向量和条数，返回 id + 相似度。 */
export type KnnFn = (vec: Float32Array | number[], k: number) => Array<{ id: string; similarity: number }>;

/** findCandidates 的召回结果：三路候选 + 正文 hash。 */
export interface CandidateHints {
  hash: Array<{ id: string; text?: unknown; hash: string }>;
  knn: Array<{ id: string; similarity: number }>;
  related: Array<{ id: string; text?: unknown; score: number; source: string }>;
  hashValue?: string;
}

/** 裁决结果；NEEDS_LLM 表示高相似但没注入 llm，没擅自放行。 */
export interface GateVerdict {
  verdict: string;
  matchedId: string | null;
  reason: string;
  needsLlm: boolean;
}

/** 闸的落点：裸 sqlite 连接（有 prepare）、带 db() 的 store，或只带 db() 属性的 store 壳。 */
export interface GateStore {
  prepare?: (sql: string) => unknown;
  db?: (() => DatabaseSync) | DatabaseSync;
  listAll?: () => MemoryRecord[];
  metas?: () => MemoryRecord[];
  vectors?: { knn?: KnnFn };
  knn?: KnnFn;
}

/** findCandidates / judge / gate 的注入项，各自取用得上的那几项。 */
export interface FindOpts {
  k?: number;
  excludeIds?: string[];
  entities?: string[];
  vector?: Float32Array | number[];
  embed?: (text: string) => Promise<Float32Array | number[]> | Float32Array | number[];
  knn?: KnnFn;
  llm?: (input: { candidate: unknown; recall: unknown }) => Promise<unknown> | unknown;
  threshold?: number;
}

/** 全角标点/字母数字转半角，再把空白、标点、符号全部去掉，最后小写。 */
export function normalizeText(text: unknown): string {
  return String(text ?? '')
    .replace(/[\uFF01-\uFF5E]/g, (ch) => String.fromCharCode(ch.charCodeAt(0) - 0xfee0))
    .replace(/\u3000/g, ' ')
    .replace(/[\p{P}\p{S}\s]+/gu, '')
    .toLowerCase();
}

/** sha1(规范化正文)，discards.candidate_hash 用的就是它。 */
export function hashText(text: unknown): string {
  // 只换原语，归一化口径不变：写入门要的是「同一条不同排版能撞上」
  return sha1Hex(normalizeText(text));
}

function dbOf(target: GateStore): DatabaseSync {
  if (target && typeof target.prepare === 'function') return target as DatabaseSync;
  if (target && typeof target.db === 'function') return (target.db as () => DatabaseSync)();
  if (target && target.db && typeof (target.db as DatabaseSync).prepare === 'function') return target.db as DatabaseSync;
  throw new Error('write-gate: 需要 sqlite 连接或带 db() 的 store');
}

function parseEntities(value: unknown): string[] {
  if (Array.isArray(value)) return value.map(String);
  if (typeof value !== 'string' || !value) return [];
  try {
    const parsed = JSON.parse(value);
    return Array.isArray(parsed) ? parsed.map(String) : [];
  } catch {
    return [];
  }
}

function metasOf(target: GateStore, db: DatabaseSync): MemoryRecord[] {
  if (target && typeof target.listAll === 'function') return target.listAll();
  if (target && typeof target.metas === 'function') return target.metas();
  return db.prepare('SELECT id, text, entities, ts, status FROM memories').all()
    .map((row) => ({ ...row, entities: parseEntities(row.entities) })) as MemoryRecord[];
}

function knnOf(target: GateStore, opts: FindOpts): KnnFn | null {
  if (typeof opts.knn === 'function') return opts.knn;
  if (target && target.vectors && typeof target.vectors.knn === 'function') return target.vectors.knn.bind(target.vectors);
  if (target && typeof target.knn === 'function') return target.knn.bind(target);
  return null;
}

/**
 * 召回候选。
 *
 * @param {object|import('node:sqlite').DatabaseSync} target store 或 sqlite 连接
 * @param {string} text 候选正文
 * @param {object} [opts]
 * @param {number} [opts.k=10]   knn 取几条
 * @param {string[]} [opts.excludeIds] 已判重的旧条 id，不重复返回
 * @param {string[]} [opts.entities]   候选实体，relatedEntries 用
 * @param {Float32Array|number[]} [opts.vector] 预计算向量
 * @param {(text:string)=>Promise<Float32Array|number[]>} [opts.embed] 向量化函数
 * @param {(vec:Float32Array|number[], k:number)=>Array<{id:string,similarity:number}>} [opts.knn]
 * @returns {Promise<{hash:Array, knn:Array, related:Array, hashValue:string}>}
 */
export async function findCandidates(target: GateStore, text: unknown, opts: FindOpts = {}): Promise<CandidateHints> {
  const db = dbOf(target);
  const k = Math.max(1, Number(opts.k) || 10);
  const exclude = new Set((opts.excludeIds || []).map(String));
  const body = String(text ?? '').trim();
  const hashValue = hashText(body);

  const hash = db.prepare('SELECT id, text FROM memories').all()
    .map((row) => ({ id: String(row.id), text: row.text, hash: hashText(row.text) }))
    .filter((row) => row.hash === hashValue && !exclude.has(row.id));

  let knn: Array<{ id: string; similarity: number }> = [];
  const knnFn = knnOf(target, opts);
  if (knnFn) {
    let vec = opts.vector;
    if (!vec && typeof opts.embed === 'function') vec = await opts.embed(body);
    if (vec) {
      knn = (knnFn(vec, k) || [])
        .map((hit) => ({ id: String(hit.id), similarity: Number(hit.similarity) }))
        .filter((hit) => !exclude.has(hit.id))
        .slice(0, k);
    }
  }

  const related = relatedEntriesScored(body, opts.entities || [], metasOf(target, db), 8)
    .filter(({ m }) => !exclude.has(String(m.id)))
    .map(({ m, score }) => ({ id: String(m.id), text: m.text, score, source: 'related' }));

  return { hash, knn, related, hashValue };
}

const VERDICTS = new Set(['DUPLICATE', 'SUPERSEDES', 'DISTINCT']);

function normalizeVerdict(raw: unknown, hints: CandidateHints): GateVerdict {
  const verdict = String((raw as { verdict?: unknown } | null | undefined)?.verdict || '').toUpperCase();
  if (!VERDICTS.has(verdict)) throw new Error(`write-gate: llm verdict 非法：${(raw as { verdict?: unknown } | null | undefined)?.verdict}`);
  const known = new Set([...(hints.hash || []), ...(hints.knn || []), ...(hints.related || [])].map((h) => String(h.id)));
  const matchedId = (raw as { matchedId?: unknown } | null | undefined)?.matchedId ? String((raw as { matchedId?: unknown }).matchedId) : null;
  if ((verdict === 'DUPLICATE' || verdict === 'SUPERSEDES') && !matchedId) {
    throw new Error(`write-gate: ${verdict} 必须给 matchedId`);
  }
  if (matchedId && !known.has(matchedId)) {
    throw new Error(`write-gate: matchedId=${matchedId} 不在召回集合里`);
  }
  return { verdict, matchedId, reason: String((raw as { reason?: unknown } | null | undefined)?.reason || ''), needsLlm: false };
}

/**
 * 裁决一层：hash 完全相同直接 DUPLICATE；最高余弦 ≥0.98 交 LLM；
 * 否则 DISTINCT。没有注入 llm 时高相似只标 needsLlm，不擅自放行。
 *
 * @param {object} candidate {text, hash?}
 * @param {{hash?:Array, knn?:Array, related?:Array}} hints findCandidates 的返回
 * @param {object} [opts]
 * @param {(input:{candidate:object,recall:object})=>Promise<object>|object} [opts.llm]
 * @param {number} [opts.threshold=0.98]
 */
export async function judge(candidate: { text?: unknown; hash?: string }, hints: CandidateHints, opts: FindOpts = {}): Promise<GateVerdict> {
  const text = String(candidate?.text ?? '').trim();
  if (!text) throw new Error('write-gate: candidate.text 为空');
  const hashValue = candidate?.hash || hashText(text);
  const exact = (hints?.hash || []).find((hit) => hit.hash === hashValue || hashText(hit.text) === hashValue);
  if (exact) {
    return { verdict: 'DUPLICATE', matchedId: String(exact.id), reason: 'hash 完全相同', needsLlm: false };
  }

  const threshold = Number(opts.threshold ?? 0.98);
  const top = [...(hints?.knn || [])].sort((a, b) => Number(b.similarity) - Number(a.similarity))[0];
  if (top && Number(top.similarity) >= threshold) {
    if (typeof opts.llm !== 'function') {
      return {
        verdict: 'NEEDS_LLM',
        matchedId: String(top.id),
        reason: `余弦 ${Number(top.similarity).toFixed(4)} ≥ ${threshold}，需 LLM 裁决`,
        needsLlm: true,
      };
    }
    const raw = await opts.llm({ candidate, recall: hints });
    return normalizeVerdict(raw, hints);
  }

  return { verdict: 'DISTINCT', matchedId: null, reason: '规则层未命中', needsLlm: false };
}

/**
 * 记一条 discards（只记 DUPLICATE；SUPERSEDES 是替换不是丢弃）。store 层写入闸和这里的
 * gate() 共用，别各写一套 SQL。
 *
 * @param {object|import('node:sqlite').DatabaseSync} target store 或 sqlite 连接
 * @param {object} item {text, sourceFile?, source_file?}
 * @param {{verdict:string, matchedId?:string|null, reason?:string}} result
 * @returns {{hash:string, id:number|bigint}}
 */
export function recordDiscard(target: GateStore, item: { text?: unknown; sourceFile?: string; source_file?: string }, result: { verdict?: string; matchedId?: string | null; reason?: string }): { hash: string; id: number | bigint } {
  const db = dbOf(target);
  const text = String(item?.text ?? '').trim();
  const hash = hashText(text);
  const info = db.prepare(`INSERT INTO discards
    (candidate_hash, text, matched_id, verdict, reason, source_file, created_ts)
    VALUES (?, ?, ?, ?, ?, ?, ?)`)
    .run(
      hash, text, result?.matchedId || null, result?.verdict || null,
      result?.reason || null, item?.sourceFile || item?.source_file || null, Date.now(),
    );
  return { hash, id: info.lastInsertRowid };
}

/**
 * discards 表当前行数。
 *
 * 整理汇总里的「拒绝」以前数的是 agent 提案被打回的条数（applied.rejected），
 * 和 discards 落账不是同一口径：12:30 那轮报「拒绝 8」而 discards 当日新增 0。
 * 现在汇总直接用「轮开始时/收尾时」两次行数差，报的是真实落账条数；提案打回另列。
 * 表不在就返回 0，不让对账把整轮拖死。
 */
export function discardCountOf(target: GateStore): number {
  try {
    return Number(dbOf(target).prepare('SELECT count(*) AS n FROM discards').get()?.n) || 0;
  } catch {
    return 0;
  }
}

/**
 * 闸门：召回 → 裁决 → DUPLICATE 落 discards。不写 memories。
 *
 * @param {object|import('node:sqlite').DatabaseSync} target store 或 sqlite 连接
 * @param {object} item {text, entities?, type?, tags?, source?, sourceFile?}
 * @param {object} [opts] 透传 findCandidates/judge 的选项（llm/embed/knn/k/excludeIds）
 * @returns {Promise<{verdict:string, matchedId:string|null, reason:string, discarded:boolean, needsLlm:boolean}>}
 */
export async function gate(target: GateStore, item: { text?: unknown; entities?: string[]; type?: string; tags?: string[]; source?: string; sourceFile?: string }, opts: FindOpts = {}): Promise<GateVerdict & { discarded: boolean }> {
  const text = String(item?.text ?? '').trim();
  if (!text) throw new Error('write-gate: item.text 为空');
  const hints = await findCandidates(target, text, {
    ...opts,
    entities: item?.entities || opts.entities || [],
  });
  const result = await judge({ ...item, text }, hints, opts);
  let discarded = false;
  if (result.verdict === 'DUPLICATE') {
    recordDiscard(target, item, result);
    discarded = true;
  }
  return { ...result, discarded };
}
