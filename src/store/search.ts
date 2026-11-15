/**
 * 检索：四路（语义 / 关键词 / 实体 / 时间）+ RRF 融合。
 *
 * 语义那一路靠 sqlite-vec 的 knn；关键词的分词按记录对象缓存在 state.tokenCache，
 * df 跟着 snapshotVersion 走（一笔写事务之后重算），别在这里另存一份 metas。
 */
import { brief, logRecall } from './log.js';
import { isActive } from './rows.js';
import { clamp01, dateOnly, normalizeType } from './types.js';
import type { DfCache, MemoryRecord, StoreState } from './types.js';

/** 一条候选的打分（四路分 + 记录本身）。 */
interface ScoredRow {
  i: number;
  m: MemoryRecord;
  cos: number;
  kw: number;
  kwHits: number;
  entityScore: number;
  timeScore: number;
}

/** 单路排序项：下标 + 分数。 */
interface RankedRow {
  i: number;
  v: number;
}

/** 融合后的命中（带着它是哪几路捞出来的）。 */
interface PickedHit {
  r: ScoredRow;
  fused: number;
  match: string[];
}

/** 对外命中结构（logRecall 与工具共用）。 */
export interface HitView {
  text: string;
  score: number;
  type: string;
  confidence?: number;
  tags: string;
  entities: string;
  date: string;
  match: string;
  ts?: string;
  status: string;
}

/** recall 的可选项：type/asOf 参与检索，其余只进日志。 */
export interface RecallOptions {
  type?: unknown;
  asOf?: unknown;
  caller?: unknown;
  session?: unknown;
  origin?: unknown;
  callId?: unknown;
  [key: string]: unknown;
}

/** 检索日志要用的 trace：query 截断、ids 顺手记。 */
interface RecallTrace {
  query: string;
  k: number | null;
  type: string | null;
  caller: string;
  session: unknown;
  origin: unknown;
  callId: unknown;
  ids: string[];
}

/** scoreCandidates 的输入（四路权重与候选池都在这里）。 */
interface ScoreInput {
  query: string;
  wantType: string;
  dates: string[];
  qTokens: string[];
  tokenWeight: (t: string) => number;
  qWeightSum: number;
  semById: Map<string, number>;
}

// ── 检索用的分词与打分 ──────────────────────────────────────────────────

export function tokensOf(text: unknown): string[] {
  const out = new Set<string>();
  const s = String(text || '').toLowerCase();
  for (const w of s.match(/[a-z0-9][a-z0-9_.\-/]*/g) || []) {
    if (w.length >= 2) out.add(w);
  }
  const cjk = s.replace(/[^\u4e00-\u9fa5]+/g, ' ').split(' ').filter(Boolean);
  for (const seg of cjk) {
    if (seg.length === 1) out.add(seg);
    for (let i = 0; i + 1 < seg.length; i += 1) out.add(seg.slice(i, i + 2));
  }
  return [...out];
}

export function queryDates(query: unknown): string[] {
  const out: string[] = [];
  const re = /(20\d{2})[-年/.](\d{1,2})(?:[-月/.](\d{1,2}))?/g;
  let m;
  while ((m = re.exec(String(query || '')))) {
    out.push(`${m[1]}-${String(m[2]).padStart(2, '0')}-${String(m[3] || '1').padStart(2, '0')}`);
  }
  const md = /(?<!\d)(\d{1,2})月(\d{1,2})日?/g;
  const year = new Date().getFullYear();
  while ((m = md.exec(String(query || '')))) {
    out.push(`${year}-${String(m[1]).padStart(2, '0')}-${String(m[2]).padStart(2, '0')}`);
  }
  return out;
}

export function dayDiff(a: string, b: string): number {
  const ta = Date.parse(`${a}T00:00:00Z`);
  const tb = Date.parse(`${b}T00:00:00Z`);
  if (!Number.isFinite(ta) || !Number.isFinite(tb)) return Number.POSITIVE_INFINITY;
  return Math.abs(ta - tb) / 86400000;
}

function tokensForRecord(state: StoreState, m: MemoryRecord): string[] {
  let tokens = state.tokenCache.get(m);
  if (!tokens) {
    tokens = tokensOf(`${m.text} ${(m.entities || []).join(' ')}`);
    state.tokenBuilds += 1;
    state.tokenCache.set(m, tokens);
  }
  return tokens;
}

function corpusStats(state: StoreState): DfCache {
  if (state.dfCache && state.dfCache.version === state.snapshotVersion) return state.dfCache;
  const df = new Map<string, number>();
  let totalActive = 0;
  for (const m of state.metas) {
    if (isActive(m)) totalActive += 1;
    for (const t of new Set(tokensForRecord(state, m))) df.set(t, (df.get(t) || 0) + 1);
  }
  state.dfBuilds += 1;
  state.dfCache = { version: state.snapshotVersion, totalActive: totalActive || 1, df };
  return state.dfCache;
}

/**
 * 语义通道：active 只取前 max(limit*8, 128) 条，非 active 只留 max(limit*4, 32) 条影子。
 * 真库 9024 条全量塞 Map 一次约 190ms，Pi 上同步跑会把召回链路卡住（审计存储中-4）。
 */
function semanticHits(state: StoreState, qvec: Float32Array, limit: number): Map<string, number> {
  const activeIds: string[] = [];
  const inactiveIds: string[] = [];
  for (const m of state.metas) (isActive(m) ? activeIds : inactiveIds).push(m.id);
  const semById = new Map<string, number>();
  if (activeIds.length) {
    const semK = Math.min(activeIds.length, Math.max(limit * 8, 128));
    for (const h of state.vecDb!.knn(qvec, semK, { allow: activeIds })) semById.set(h.id, h.similarity);
  }
  if (inactiveIds.length) {
    const shadowK = Math.min(inactiveIds.length, Math.max(limit * 4, 32));
    for (const h of state.vecDb!.knn(qvec, shadowK, { allow: inactiveIds })) semById.set(h.id, h.similarity);
  }
  return semById;
}

/** 逐条打分：语义余弦、关键词加权命中、实体命中、时间邻近。 */
function scoreCandidates(state: StoreState, { query, wantType, dates, qTokens, tokenWeight, qWeightSum, semById }: ScoreInput): ScoredRow[] {
  const rows: ScoredRow[] = [];
  for (let i = 0; i < state.metas.length; i += 1) {
    const m = state.metas[i];
    if (wantType && normalizeType(m.type, m.tags, m.text) !== wantType) continue;
    const cos = semById.get(m.id) ?? -1;
    const recSet = new Set(tokensForRecord(state, m));
    let kwHits = 0;
    let kwWeight = 0;
    for (const t of qTokens) {
      if (recSet.has(t)) {
        kwHits += 1;
        kwWeight += tokenWeight(t);
      }
    }
    const kw = kwHits === 0 ? 0 : kwWeight / qWeightSum;
    const ents = (m.entities || []).filter((e) =>
      String(query || '').toLowerCase().includes(e.toLowerCase()),
    );
    const entityScore = ents.length;
    let timeScore = 0;
    if (dates.length) {
      const mine = [m.validFrom, m.validTo, dateOnly(m.ts)].filter(Boolean);
      let best = Number.POSITIVE_INFINITY;
      for (const d of mine) for (const target of dates) best = Math.min(best, dayDiff(d as string, target));
      timeScore = Number.isFinite(best) ? 1 / (1 + best / 30) : 0;
    } else {
      const age = (Date.now() - Date.parse(m.ts || '')) / 86400000;
      timeScore = Number.isFinite(age) ? 1 / (1 + Math.max(0, age) / 240) : 0;
    }
    rows.push({ i, m, cos, kw, kwHits, entityScore, timeScore });
  }
  return rows;
}

/**
 * RRF 融合。同分并列取「平均名次」：
 * 旧版按数组序给 idx+1，entityScore 这种整数分大量并列时，只有最靠前那条拿 rank1，
 * 后面的并列条目白白吃亏，名次完全由 metas 的排列顺序决定。
 * 选平均名次而不是 dense rank：RRF 的 1/(k+rank) 依赖名次幅度，dense rank 会把
 * 「并列第 1、并列第 2」之后的条目名次整体前压，等于偷偷抬高所有条目的分数；
 * 平均名次保持名次总量与排序尺度不变，同分组共享同一个分，谁也不多拿。
 * 同分判定用 ===：实体分是整数计数，浮点余弦真撞到完全相等时也确实该算同分。
 */
function fuseRows(state: StoreState, rows: ScoredRow[], { dates, limit }: { dates: string[]; limit: number }): PickedHit[] {
  const { cfg, rrfK } = state;
  const rank = (list: RankedRow[]): Map<number, number> => {
    const sorted = list.slice().sort((a, b) => b.v - a.v || a.i - b.i);
    const out: Map<number, number> = new Map();
    let k = 0;
    while (k < sorted.length) {
      let j = k;
      while (j + 1 < sorted.length && sorted[j + 1].v === sorted[k].v) j += 1;
      const avg = (k + j) / 2 + 1; // 0-based 区间 [k, j] 的 1-based 平均名次
      if (sorted[k].v > 0) for (let t = k; t <= j; t += 1) out.set(sorted[t].i, avg);
      k = j + 1;
    }
    return out;
  };
  const wSem = 1;
  const wKw = 0.9;
  // 实体匹配噪声大：能撞上但精度不够，1 太吃重，默认 0.65（2026-11-13 实测后下调）
  const wEntRaw = cfg.rankWeightEntity == null ? NaN : Number(cfg.rankWeightEntity);
  const wEnt = Number.isFinite(wEntRaw) ? wEntRaw : 0.65;
  const wTime = dates.length ? 0.8 : 0.4;

  const semRank = rank(rows.map((r) => ({ i: r.i, v: Math.max(0, r.cos) })));
  const kwRank = rank(rows.map((r) => ({ i: r.i, v: r.kw })));
  const entRank = rank(rows.map((r) => ({ i: r.i, v: r.entityScore })));
  const timeRank = rank(rows.map((r) => ({ i: r.i, v: r.timeScore })));

  // 语义入选线只按 active 的余弦取最大值：
  // archived / merged 的条目只是「能查到的影子」，它们的高余弦不该抬高 active 的门槛——
  // 以前全候选一起取 max，一条 archived 0.95 能把 active 最高的 0.5 顶得出局。
  // 边界：一条 active 都没有（极端：库全是 archived）时退回全候选最大值，
  // 保证 topCos 永远是有限数，不会让 NaN/undefined 参与后面的减法。
  const activeRows = rows.filter((r) => isActive(r.m));
  const topCos = activeRows.length
    ? Math.max(...activeRows.map((r) => r.cos))
    : Math.max(...rows.map((r) => r.cos));
  const picked = [];
  for (const r of rows) {
    const semOk = r.cos >= (cfg.minScore as number) && r.cos >= topCos - (cfg.relativeGap as number);
    const otherOk = r.kw >= 0.15 || r.entityScore > 0 || (dates.length && r.timeScore > 0.6);
    if (!semOk && !otherOk) continue;
    if (!isActive(r.m) && !semOk) continue;
    // 归档/合并掉的记忆：只保留一个能查到的影子，别跟 active 抢名额（0.85 太温和，2026-10-31 实测归档条目还能排进前 6）
    const statusWeight = isActive(r.m) ? 1 : 0.3;
    const conf = r.m.type === 'opinion' ? 0.85 + 0.15 * clamp01(r.m.confidence, 0.7) : 1;
    const fused =
      statusWeight *
      conf *
      (wSem / (rrfK + (semRank.get(r.i) || 999)) +
        wKw / (rrfK + (kwRank.get(r.i) || 999)) +
        wEnt / (rrfK + (entRank.get(r.i) || 999)) +
        wTime / (rrfK + (timeRank.get(r.i) || 999)));
    const match: string[] = [];
    if (semRank.has(r.i) && r.cos >= (cfg.minScore as number)) match.push('语义');
    if (kwRank.has(r.i) && r.kw >= 0.15) match.push('关键词');
    if (r.entityScore > 0) match.push('实体');
    if (dates.length && r.timeScore > 0.6) match.push('时间');
    picked.push({ r, fused, match });
  }
  picked.sort((a, b) => b.fused - a.fused || b.r.cos - a.r.cos);
  return picked.slice(0, limit);
}

/** 命中条目转对外结构；score 仍是余弦（老口径），排序看融合分。 */
function formatHit({ r, match }: PickedHit): HitView {
  return {
    text: r.m.text,
    score: Number(r.cos.toFixed(4)),
    type: normalizeType(r.m.type, r.m.tags, r.m.text),
    ...(r.m.confidence === undefined ? {} : { confidence: Number(r.m.confidence) }),
    tags: (r.m.tags || []).join(','),
    entities: (r.m.entities || []).join(','),
    date: r.m.validFrom || dateOnly(r.m.ts) || '',
    match: match.join('+'),
    ts: r.m.ts,
    status: r.m.status || 'active',
  };
}

/**
 * 四路检索 + RRF 融合。
 * 返回的 score 仍然是余弦（老口径，能看出语义相近程度），
 * 排序由融合分数决定，match 字段说明这条是哪些通道捞出来的。
 * trace 只给上面的日志外壳用（记录实际生效的 k 和命中条目的 id），可以为空。
 */
async function recallCore(
  state: StoreState,
  query: unknown,
  k: unknown,
  opts: RecallOptions = {},
  trace: RecallTrace | null = null,
): Promise<HitView[]> {
  if (state.metas.length === 0) return [];
  const q = String(query || '');
  const base = q.split('\n')[0] || q;
  const [qv] = await state.embedTexts([base]);
  if (state.dim && qv.length !== state.dim) {
    throw new Error(`查询向量维度 ${qv.length} 和库里的 ${state.dim} 不一致，嵌入服务换模型了？`);
  }
  const limit = Math.max(1, Math.min(Number(k) || (state.cfg.topK as number), 20));
  if (trace) trace.k = limit;
  const wantType = String(opts.type || '').toLowerCase();
  const qTokens = tokensOf(q);
  const dates = queryDates(q);
  const { df, totalActive } = corpusStats(state);
  const tokenWeight = (t: string) => Math.log(1 + totalActive / (1 + (df.get(t) || 0)));
  const qWeightSum = qTokens.reduce((s, t) => s + tokenWeight(t), 0) || 1;

  const qvec = Float32Array.from(qv);
  // 实体通道按查询原文匹配
  const scoreInput = {
    query: q, wantType, dates, qTokens, tokenWeight, qWeightSum, semById: semanticHits(state, qvec, limit),
  };
  const rows = scoreCandidates(state, scoreInput);
  if (rows.length === 0) return [];

  const top = fuseRows(state, rows, { dates, limit });
  // 只给日志用：返回结构里没有 id，这里顺手把 id 记到 trace
  if (trace) trace.ids = top.map(({ r }) => r.m.id);
  return top.map(formatHit);
}

/**
 * recall 的日志外壳（2026-11-01）：掐表、记一行，然后把 recallCore 的结果
 * 原样透传——不复制、不改字段、不动 k 和排序。opts 里的 session / origin /
 * caller 只进日志，不参与检索。
 */
export async function recall(state: StoreState, query: unknown, k: unknown, opts: RecallOptions = {}): Promise<HitView[]> {
  const t0 = Date.now();
  const trace: RecallTrace = {
    // 只留前 300 字：prefill 的 query 可能是一整段运行时上下文，不截会把日志撑爆
    query: brief(query, 300),
    k: null,
    type: opts.type ? String(opts.type) : null,
    caller: opts.caller as string || 'unknown',
    session: opts.session ?? null,
    origin: opts.origin ?? null,
    callId: opts.callId ?? null,
    ids: [],
  };
  try {
    const items = await recallCore(state, query, k, opts, trace);
    logRecall(state.cfg, {
      ts: new Date().toISOString(),
      session: trace.session,
      origin: trace.origin,
      callId: trace.callId,
      query: trace.query,
      k: trace.k,
      type: trace.type,
      caller: trace.caller,
      hits: Array.isArray(items) ? items.length : 0,
      top: (Array.isArray(items) ? items : []).slice(0, 3).map((it, i) => ({
        id: trace.ids[i] ?? null,
        type: it?.type ?? null,
        score: typeof it?.score === 'number' ? it.score : null,
        text: brief(it?.text),
      })),
      ms: Date.now() - t0,
    });
    return items;
  } catch (error) {
    logRecall(state.cfg, {
      ts: new Date().toISOString(),
      session: trace.session,
      origin: trace.origin,
      callId: trace.callId,
      query: trace.query,
      k: trace.k,
      type: trace.type,
      caller: trace.caller,
      hits: null,
      top: [],
      ms: Date.now() - t0,
      error: String((error && (error as { message?: unknown }).message) || error),
    });
    throw error;
  }
}
