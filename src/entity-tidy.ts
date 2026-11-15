/**
 * 实体整理（三期，2026-11-14）。
 *
 * 每晚安静阶段里跑一遍：扫 entities + entity_surface，把「确定同义」的实体合成一个，
 * 不确定的只出候选清单，一行不动。
 *
 * 三条判定口径（bucket 名沿用 entity_dedup_candidate 表的注释：sep|dsh|plural|suffix|version|prefix|lang）：
 *
 *   自动合（AUTO）——名字层面的等价，不需要语义判断：
 *     fold_same     折叠后完全同名：NFKC + 小写 + 去掉空白/下划线/连字符/中点/点号。
 *                   例：majsoul-watch ≡ majsoul_watch、HA_TOKEN ≡ HA token、zuru ≡ zu ru。
 *                   数字之间的分隔符例外（3.15 / 3-15 是版本号的一部分）：这种名字只进只报。
 *     surface_cross 写法层指向同一实体但规范名不同：某条 surface 行的原文折叠后等于另一个实体的
 *                   display（这条 surface 挂在 B 上，A 却叫这个名字）——同一句话被两个实体认领。
 *                   只认干净写法：路径/点文件/开关名的折叠行不算（.github ≠ GitHub）；
 *                   而且必须双向成立（A 那边也有 B 的写法行），单链方向的不传递，只进只报。
 *
 *   名字里以 / \ ~ . - _ 开头、或含路径分隔符、或数字之间夹着 . _ : · - 的，一律不进自动合
 *   （cleanName）：折叠会抹掉这些前缀，.dsh 撞 dsh、--apply 撞 apply、3.15 撞 315，
 *   撞出来的不是同一个东西。
 *
 *   只报（REPORT）——名字像，但不能保证是同一个东西，合并要人看：
 *     plural        单复数（末尾 s/es）
 *     version       版本/型号剥壳后同名（树莓派 vs 树莓派Pi5 不并，走 splitVersion 的 base 判）
 *     prefix        一方是另一方的前缀（差 1-6 字）
 *     similar       折叠后 Jaccard ≥ 0.8 但不完全相等
 *     surface_cross 单向的写法层交叉（只有一边认领对方的名字）：像，但不敢合
 *
 * 自动合的落地规矩：
 *   - dst 选 mem_count 最大、同分选 id 最小（稳定，重跑结果一致）；
 *   - 迁 entity_surface 挂接、迁 memory_entity 挂载、清空 src 的挂载、src 置 status='merged'；
 *   - 每次合并写一行 entity_merge（src/dst/reason/evidence/run_id），evidence 里记下搬走的
 *     surface 与挂载行，undoEntityMerge() 能原样搬回去；
 *   - 不动 memories 正文一个字；mem_count 一律按 memory_entity 实数重算。
 */

import fs from 'node:fs';
import path from 'node:path';
import type { DatabaseSync, SQLInputValue, StatementSync } from 'node:sqlite';
import { splitVersion } from './shared/entity-normalize.js';
import { withTransaction } from './shared/tx.js';

/** 参与整理的一个实体（entities 表 active 行）。 */
export interface TidyEntity {
  id: number;
  norm: string;
  display: string;
  etype: string;
  memCount: number;
}

/** 一条依据边：a/b 是实体 id，basis 是 bucket 名。 */
export interface TidyEdge {
  a: number;
  b: number;
  basis: string;
}

/** entity_surface 的一行（整理只需要原文、折叠名、次数、来源标记）。 */
export interface SurfaceRow {
  surface: string;
  norm: string;
  count: number;
  flag: string;
}

/** 报告里的一对实体。 */
export interface TidyPairSide {
  id: number;
  display: string;
  memCount: number;
}

/** 只报候选：basis/score/detail 都是给人看的。 */
export interface TidyReportEntry {
  key: string;
  basis: string;
  score: number;
  a: TidyPairSide;
  b: TidyPairSide;
  detail: string;
}

/** 一条依据的判定结果（score 只用于报告排序）。 */
export interface ReportBasis {
  basis: string;
  score: number;
  detail: string;
}

/** 自动合组里的一步：src 并进 dst。 */
export interface TidyPlanned {
  src: number;
  dst: number;
  srcName: string;
  dstName: string;
}

/** 一个自动合组。 */
export interface TidyAutoGroup {
  key: string;
  basis: string[];
  entities: TidyEntity[];
  dst: TidyEntity;
  planned: TidyPlanned[];
  surfaceRows: number;
  mountRows: number;
  surfaceMoves: number;
  mountMoves: number;
}

/** 扫描统计。 */
export interface TidyScanStats {
  entities: number;
  surfaceRows: number;
  autoGroups: number;
  autoEntities: number;
  autoMerges: number;
  surfaceMoves: number;
  mountMoves: number;
  reportPairs: number;
}

/** scanEntityTidy 的产物。 */
export interface TidyScanResult {
  auto: TidyAutoGroup[];
  report: TidyReportEntry[];
  stats: TidyScanStats;
}

/** 并查集（只在本文件用）。 */
export interface UnionFind {
  add: (x: number) => void;
  union: (a: number, b: number) => void;
  find: (x: number) => number;
}

/** apply 用的语句集合：缺表时整段返回 null，由调用方按「全部跳过」处理。 */
export interface TidyStmts {
  hasOrigin: boolean;
  selectMounts: StatementSync;
  insertMount: StatementSync;
  moveSurfaces: StatementSync;
  surfacesOf: StatementSync;
  deleteMount: StatementSync;
  mountOf: StatementSync;
  entityOf: StatementSync;
  setStatus: StatementSync;
  recalc: StatementSync;
  recordMerge: StatementSync;
}

/** 一步合并的落库明细。 */
export interface TidyStepRow {
  src: number;
  dst: number;
  surfaces: number;
  mounts: number;
  /** dry-run 时只记计划，不落库。 */
  dryRun?: boolean;
}

/** 一步合并的回执（报告 / 撤回定位都用它）。 */
export interface TidyMergePair {
  mergeId: number;
  src: number;
  dst: number;
  srcName: string;
  dstName: string;
  reason: string;
  surfaces: number;
  mounts: number;
}

/** applyOneStep 的结果。 */
export interface TidyStepResult {
  status: 'merged' | 'stale' | 'failed';
  row?: TidyStepRow;
  pair?: TidyMergePair;
}

/** applyEntityTidy 的产物。 */
export interface TidyApplyResult {
  groups: number;
  merged: number;
  surfaceMoved: number;
  mountsMoved: number;
  skipped: number;
  stale: number;
  rows: TidyStepRow[];
  pairs: TidyMergePair[];
}

/** undoEntityMerge 的回执（含真实搬回的条数）。 */
export interface TidyUndoResult {
  ok: boolean;
  reason?: string;
  mergeId?: number;
  src?: number;
  dst?: number;
  restored?: number;
  skipped?: number;
  surfaces?: number;
  mounts?: number;
  surfaceSkipped?: number;
  mountSkipped?: number;
  blockingMergeId?: number;
}

/** undoEntityTidyRun 的回执。 */
export interface TidyUndoRunResult {
  ok: boolean;
  runId: string;
  undone: number;
  failed: number;
  mergeIds: number[];
  reason?: string;
  blockingMergeId?: number;
}

/** undo 时临时准备的语句（在事务里 prepare，失败整段回滚）。 */
export interface TidyUndoStmts {
  hasOrigin: boolean;
  restoreSurface: StatementSync | null;
  insertMount: StatementSync | null;
  dropMount: StatementSync | null;
}

/** 自动合的三种依据；其余 bucket 一律只报。 */
export const AUTO_BUCKETS = Object.freeze(['fold_same', 'surface_cross']);
/** 折叠时一律丢掉的分隔符（空白、下划线、点、连字符、中点、冒号）。 */
const SEP_CHAR_RE = /[\s\u3000_.\-·:：]/;
/**
 * 数字之间的分隔符（3.15 / 3-15 / 3:15 / 3·15）。
 *
 * 这是版本号的一部分，不许折叠抹掉：旧写法把 `Python-3.15.1` 抹成 `python3151`，
 * 再撞上 `python` 实体那条 `Python 3.15.1` 的写法行，两个不同版本就被合成一个。
 * 命中的名字不进自动合（cleanName / cleanSurface 各拦一次），只进「只报」。
 */
const NUM_SEP_RE = /\d[._:·\-]\d/;

/**
 * 名字是否「干净」：以 / \ ~ . - _ 开头、或含路径分隔符、或像 URI 的，都不进自动合；
 * 数字之间夹分隔符的（3.15.1 / v1-2）也不进——抹掉分隔符就把不同版本折成同一个名字。
 * 理由：折叠会把这些前缀抹掉，.dsh 撞 dsh、.json 撞 JSON、--apply 撞 apply，
 * 撞出来的都不是同一个东西。这类只进「只报」。
 */
export function cleanName(raw: unknown): boolean {
  const name = String(raw ?? '').trim();
  if (!name) return false;
  if (/^[.\-_~]/.test(name)) return false;
  if (name.includes('/') || name.includes('\\')) return false;
  if (name.includes('://')) return false;
  if (NUM_SEP_RE.test(name)) return false;
  return true;
}

/**
 * 名字折叠：只用于「判断是不是同一个写法」，不写库、不改 norm。
 * 分隔符一律丢掉，但数字之间那个留着（3.15 不能变成 315）。
 */
export function foldName(raw: unknown): string {
  const s = String(raw ?? '').normalize('NFKC').toLowerCase();
  let out = '';
  for (let i = 0; i < s.length; i += 1) {
    const ch = s[i];
    if (!SEP_CHAR_RE.test(ch)) { out += ch; continue; }
    const prev = s[i - 1] || '';
    const next = s[i + 1] || '';
    if (/\d/.test(prev) && /\d/.test(next)) out += ch;
  }
  return out.trim();
}

/**
 * 老口径折叠（分隔符全抹）：只用来找「抹完会撞名」的版本对，命中的只进报告。
 * 自动合一律用 foldName（数字之间那个分隔符留着）。
 */
function foldLoose(raw: unknown): string {
  return String(raw ?? '').normalize('NFKC').toLowerCase().replace(/[\s\u3000_.\-·:：]+/g, '').trim();
}

/**
 * 版本号剥壳取 base：先用 entity-normalize 的 splitVersion（要求有空格），
 * 再兜一层没有空格的写法（Python-3.15.1 / node_22.1.0）。只给「只报」用，不参与合并。
 */
function versionBaseOf(raw: unknown): string {
  const s = String(raw ?? '');
  const v = splitVersion(s);
  if (v?.base) return v.base;
  const m = s.match(/^(.+?)[\s\-_.:·]*v?(\d+(?:[._]\d+)+)$/);
  if (m && m[1] && !/^\d/.test(m[1])) return m[1];
  return '';
}

/** 简单并查集：把各种依据连出来的实体收成一个连通分量。 */
function makeUnion(): UnionFind {
  const parent = new Map<number, number>();
  const find = (x: number): number => {
    let cur = x;
    while (parent.get(cur) !== cur) cur = parent.get(cur)!;
    let now = x;
    while (parent.get(now) !== cur) { const next = parent.get(now)!; parent.set(now, cur); now = next; }
    return cur;
  };
  const add = (x: number): void => { if (!parent.has(x)) parent.set(x, x); };
  const union = (a: number, b: number): void => {
    add(a); add(b);
    const ra = find(a); const rb = find(b);
    if (ra !== rb) parent.set(rb < ra ? ra : rb, rb < ra ? rb : ra);
  };
  return { add, union, find };
}

/** 给 entity_surface 缓存一个跨进程失效信号：epoch +1。没有 meta 表的库跳过，不拖累合并。 */
function bumpSurfaceEpoch(db: DatabaseSync): void {
  try {
    const epoch = Number(db.prepare("SELECT value FROM meta WHERE key='entity_surface_epoch'").get()?.value || 0) + 1;
    db.prepare("INSERT INTO meta(key,value) VALUES ('entity_surface_epoch',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value")
      .run(String(epoch));
  } catch {
    /* 老库没有 meta 表：跳过，缓存失效只能等进程重启 */
  }
}

/** 实体 → 它挂着的 surface 行（原文 + 出现次数）。 */
function surfaceMapOf(db: DatabaseSync): Map<number, SurfaceRow[]> {
  const out = new Map<number, SurfaceRow[]>();
  let rows: Array<{ surface: unknown; norm: unknown; entity_id: unknown; count: unknown; flag: unknown }> = [];
  try {
    rows = db.prepare('SELECT surface, norm, entity_id, count, flag FROM entity_surface').all() as Array<{ surface: unknown; norm: unknown; entity_id: unknown; count: unknown; flag: unknown }>;
  } catch {
    rows = [];
  }
  for (const r of rows) {
    const id = Number(r.entity_id);
    if (!out.has(id)) out.set(id, []);
    out.get(id)!.push({
      surface: String(r.surface), norm: String(r.norm), count: Number(r.count) || 0, flag: String(r.flag || ''),
    });
  }
  return out;
}

/** 某实体挂了多少条 memory_entity（真数，不信 mem_count 列）。 */
function mountCount(db: DatabaseSync, entityId: number, cache?: Map<number, number> | null): number {
  if (cache && cache.has(entityId)) return cache.get(entityId)!;
  let n = 0;
  try {
    n = Number((db.prepare('SELECT count(*) AS n FROM memory_entity WHERE entity_id=?').get(entityId) as { n?: unknown } | undefined)?.n || 0);
  } catch {
    n = 0;
  }
  if (cache) cache.set(entityId, n);
  return n;
}

/** 组内选 dst：mem_count 大优先，同分 id 小的优先（稳定）；空数组返回 null。 */
export function pickDest(entities: TidyEntity[]): TidyEntity | null {
  return [...entities].sort((a, b) => (b.memCount - a.memCount) || (a.id - b.id))[0] ?? null;
}

/** 实体这一侧的折叠名索引（只有干净名字才进：点文件/开关名/版本名一律不进）。 */
function entityFoldIndex(entities: TidyEntity[]): { displayFold: Map<number, string>; byFold: Map<string, TidyEntity[]> } {
  const displayFold = new Map<number, string>(); // entity id → 折叠名
  const byFold = new Map<string, TidyEntity[]>(); // 折叠名 → [entity]
  for (const e of entities) {
    if (!cleanName(e.display)) continue;
    const k = foldName(e.display);
    if (!k) continue;
    displayFold.set(e.id, k);
    if (!byFold.has(k)) byFold.set(k, []);
    byFold.get(k)!.push(e);
  }
  return { displayFold, byFold };
}

/** ① 折叠同名：两边都过 cleanName 才连边（AUTO 组要求每条边都过这条更严规则）。 */
function foldGroupEdges(byFold: Map<string, TidyEntity[]>): TidyEdge[] {
  const edges: TidyEdge[] = [];
  for (const list of byFold.values()) {
    if (list.length < 2) continue;
    for (let i = 1; i < list.length; i += 1) edges.push({ a: list[0].id, b: list[i].id, basis: 'fold_same' });
  }
  return edges;
}

/** 排序后的实体对键：交叉边去重、组成员校验都用它。 */
function pairKeyOf(a: number, b: number): string {
  return `${Math.min(a, b)}|${Math.max(a, b)}`;
}

/** 报告条目：形状统一，key 用排序后的 id 对，方便去重。 */
function reportEntryOf(a: TidyEntity, b: TidyEntity, { basis, score, detail }: ReportBasis, mountOf: (id: number) => number): TidyReportEntry {
  const [lo, hi] = a.id <= b.id ? [a, b] : [b, a];
  return {
    key: `report:${lo.id}|${hi.id}`,
    basis,
    score,
    a: { id: a.id, display: a.display, memCount: mountOf(a.id) },
    b: { id: b.id, display: b.display, memCount: mountOf(b.id) },
    detail,
  };
}

/**
 * ③ 写法层交叉：双向成立（A 的写法行是 B 的名字，B 的写法行也是 A 的名字）才算自动合边。
 * 单向的只进候选清单——顺着单链 X→Y→Z 传下去会把一整片不相干的实体串成一组合掉。
 */
function crossEdgesOf({ byId, surfaces, displayFold, byFoldEntity, mountOf }: {
  byId: Map<number, TidyEntity>;
  surfaces: Map<number, SurfaceRow[]>;
  displayFold: Map<number, string>;
  byFoldEntity: Map<string, TidyEntity[]>;
  mountOf: (id: number) => number;
}): { edges: TidyEdge[]; pairs: Set<string>; oneWay: TidyReportEntry[] } {
  // 路径/折叠类写法（flag='path'/'basename_collapsed'）与以 / . _ ~ 开头的基名不进交叉证据：
  // 它们的折叠名会撞上完全不相干的实体（.github → GitHub、_archive → archive、archive/ → archive）。
  // 数字间夹分隔符的写法（Python 3.15.1）同样不算：那是版本，不是同一句话的另一种写法。
  // 这类撞名只能进「只报」，不能自动合——合错了比不合贵得多。
  const cleanSurface = (s: SurfaceRow): boolean => {
    if (s.flag === 'path' || s.flag === 'basename_collapsed' || s.flag === 'unmapped') return false;
    const raw = String(s.surface || '');
    if (!raw) return false;
    if (raw.includes('/') || raw.includes('\\')) return false;
    if (/^[._~]/.test(raw)) return false;
    if (NUM_SEP_RE.test(raw)) return false;
    return true;
  };
  // 每条实体自己的干净写法行的折叠名：判「双向成立」用
  const surfaceFolds = new Map<number, Set<string>>();
  for (const [entityId, list] of surfaces) {
    const set = new Set<string>();
    for (const s of list) {
      if (!cleanSurface(s)) continue;
      const k = foldName(s.surface);
      if (k) set.add(k);
    }
    surfaceFolds.set(entityId, set);
  }
  const edges: TidyEdge[] = [];
  const pairs = new Set<string>();
  const oneWay: TidyReportEntry[] = [];
  for (const [entityId, list] of surfaces) {
    if (!byId.has(entityId)) continue;
    const own = displayFold.get(entityId);
    if (!own) continue;
    for (const s of list) {
      if (!cleanSurface(s)) continue;
      const k = foldName(s.surface);
      if (!k) continue;
      for (const other of byFoldEntity.get(k) || []) {
        if (other.id === entityId) continue;
        const key = pairKeyOf(other.id, entityId);
        if (pairs.has(key)) continue;
        const mutual = surfaceFolds.get(other.id)?.has(own) === true;
        if (!mutual) {
          // 反向没有：单向交叉只报，不传递
          if (!oneWay.some((x) => x.key === `report:${key}`)) {
            const owner = byId.get(entityId);
            oneWay.push(reportEntryOf(other, owner!, {
              basis: 'surface_cross',
              score: 0.6,
              detail: `${owner?.display || entityId} 有一条写法行「${String(s.surface)}」折成 ${other.display}；`
                + `对方没有反过来的写法行，不传递`,
            }, mountOf));
          }
          continue;
        }
        pairs.add(key);
        edges.push({ a: other.id, b: entityId, basis: 'surface_cross' });
      }
    }
  }
  return { edges, pairs, oneWay };
}

/**
 * 连通分量 → 自动合组。
 * 组内每条边都要过更严规则：每个成员要么和 dst 折叠同名，要么和 dst 双向交叉成立；
 * 靠链式传递混进来的成员踢出去，只报不合。
 */
function buildAutoGroups({ comps, edges, surfaces, displayFold, crossPairs, mountOf }: {
  comps: Iterable<TidyEntity[]>;
  edges: TidyEdge[];
  surfaces: Map<number, SurfaceRow[]>;
  displayFold: Map<number, string>;
  crossPairs: Set<string>;
  mountOf: (id: number) => number;
}): { auto: TidyAutoGroup[]; autoOf: Map<number, number>; extras: TidyReportEntry[] } {
  const auto: TidyAutoGroup[] = [];
  const autoOf = new Map<number, number>(); // entity id → 组下标（报告里跳过同组的对）
  const extras: TidyReportEntry[] = [];
  for (const list of comps) {
    if (list.length < 2) continue;
    const dst = pickDest(list.map((e) => ({ ...e, memCount: mountOf(e.id) })));
    if (!dst) continue;
    const dstFold = displayFold.get(dst.id) || '';
    const kept = list.filter((e) => e.id === dst.id
      || (dstFold && displayFold.get(e.id) === dstFold)
      || crossPairs.has(pairKeyOf(e.id, dst.id)));
    for (const e of list) {
      if (kept.includes(e)) continue;
      extras.push(reportEntryOf(e, dst, {
        basis: 'surface_cross',
        score: 0.6,
        detail: `${e.display} 只靠链式传递连到 ${dst.display}，没有直接依据，不自动合`,
      }, mountOf));
    }
    if (kept.length < 2) continue;
    const ids = new Set(kept.map((e) => e.id));
    const basis = [...new Set(edges.filter((x) => ids.has(x.a) && ids.has(x.b)).map((x) => x.basis))].sort();
    const groupIndex = auto.length;
    for (const e of kept) autoOf.set(e.id, groupIndex);
    auto.push({
      key: `auto:${kept.map((e) => e.id).sort((a, b) => a - b).join('+')}`,
      basis,
      entities: kept.map((e) => ({ ...e, memCount: mountOf(e.id) })).sort((a, b) => a.id - b.id),
      dst,
      planned: kept.filter((e) => e.id !== dst.id).map((e) => ({ src: e.id, dst: dst.id, srcName: e.display, dstName: dst.display })),
      surfaceRows: kept.reduce((n, e) => n + (surfaces.get(e.id) || []).length, 0),
      mountRows: kept.reduce((n, e) => n + mountOf(e.id), 0),
      // 真正要动多少：src 的 surface/挂载行数（dst 自己那部分不动）
      surfaceMoves: kept.filter((e) => e.id !== dst.id)
        .reduce((n, e) => n + (surfaces.get(e.id) || []).length, 0),
      mountMoves: kept.filter((e) => e.id !== dst.id).reduce((n, e) => n + mountOf(e.id), 0),
    });
  }
  auto.sort((a, b) => a.entities[0].id - b.entities[0].id);
  return { auto, autoOf, extras };
}

/** 一对实体的只报依据：单复数 → 前缀 → 版本剥壳 → 字符 bigram 相似度，命中一个就返回。 */
function reportBasisOf(x: TidyEntity, y: TidyEntity, fx: string, fy: string): ReportBasis | null {
  if (fx === fy) {
    // 折叠后完全一样却不在自动合组：只剩「数字间分隔符被 cleanName 拦下」这一种（版本名）。
    // 这种对只报不合——抹掉分隔符就分不清 3.15 和 315 了；点文件/开关名那类照旧不报。
    if (!NUM_SEP_RE.test(x.display) && !NUM_SEP_RE.test(y.display)) return null;
    return { basis: 'version', score: 0.7, detail: `${x.display} / ${y.display}（折叠同名，但含版本数字分隔，不自动合）` };
  }
  const shorter = fx.length <= fy.length ? fx : fy;
  const longer = fx.length <= fy.length ? fy : fx;
  // 单复数只认精确关系：短的就是长的去掉 s/es，或者反过来。
  // 旧写法 `longer.startsWith(shorter.slice(0,-1))` 会把 cordis / cordis.patch.yml 判成单复数。
  const pluralPair = (shortF: string, longF: string): boolean => longF === `${shortF}s` || longF === `${shortF}es`
    || shortF === `${longF}s` || shortF === `${longF}es`;
  const tail = longer.slice(shorter.length);
  const sepTail = /^[-_.·: ]/.test(tail) || /^\d/.test(tail) || tail.length <= 1;
  if (shorter.length >= 2 && pluralPair(shorter, longer)) {
    return { basis: 'plural', score: 0.7, detail: `${shorter} → ${longer}` };
  }
  if (shorter.length >= 4 && longer.startsWith(shorter) && tail.length <= 4 && sepTail) {
    return {
      basis: 'prefix',
      score: Number((0.5 + 0.3 * (shorter.length / longer.length)).toFixed(2)),
      detail: `${shorter} ⊂ ${longer}`,
    };
  }
  // 版本剥壳只进「只报」：base 同名（python 3.11 / python 3.12）、
  // 或一边是「名字+版本」另一边正好是它的 base（Python-3.15.1 / python）。
  // versionBaseOf / foldLoose 都是纯函数，解析不了返回空串/原名，不用包 try/catch。
  const bx = versionBaseOf(x.display);
  const by = versionBaseOf(y.display);
  // 老口径折叠（抹掉数字间分隔符）会撞名的对：v1.2 / v12、python-3.15.1 / python3151
  const looseHit = foldLoose(x.display) === foldLoose(y.display)
    && (NUM_SEP_RE.test(x.display) || NUM_SEP_RE.test(y.display));
  if ((!!bx && !!by && foldName(bx) === foldName(by))
    || (!!bx && foldName(bx) === fy)
    || (!!by && foldName(by) === fx)
    || looseHit) {
    return { basis: 'version', score: 0.75, detail: `${x.display} / ${y.display}（base=${bx || by}）` };
  }
  // 字符 bigram Jaccard，只是给报告排个序，不做合并依据
  const grams = (s: string): Set<string> => {
    const out = new Set<string>();
    for (let k = 0; k < s.length - 1; k += 1) out.add(s.slice(k, k + 2));
    return out;
  };
  const gx = grams(fx);
  const gy = grams(fy);
  if (!gx.size || !gy.size) return null;
  let hit = 0;
  for (const g of gx) if (gy.has(g)) hit += 1;
  const j = hit / (gx.size + gy.size - hit);
  if (j < 0.8) return null;
  const score = Number(j.toFixed(2));
  return { basis: 'similar', score, detail: `bigram jaccard=${score}` };
}

/** 只报候选：按 id 两两比（实体才几千条，两两比够快，规则也简单）。 */
function reportCandidatesOf({ entities, autoOf, mountOf }: {
  entities: TidyEntity[];
  autoOf: Map<number, number>;
  mountOf: (id: number) => number;
}): { report: TidyReportEntry[]; reported: Set<string> } {
  const report: TidyReportEntry[] = [];
  const reported = new Set<string>();
  const foldOf = new Map<number, string>(entities.map((e) => [e.id, foldName(e.display)] as [number, string]));
  for (let i = 0; i < entities.length; i += 1) {
    for (let j = i + 1; j < entities.length; j += 1) {
      const x = entities[i];
      const y = entities[j];
      // 只跳过「同一自动合组」的对；被组内更严规则踢掉的成员还要进候选清单
      const gx = autoOf.get(x.id);
      if (gx !== undefined && gx === autoOf.get(y.id)) continue;
      const fx = foldOf.get(x.id);
      const fy = foldOf.get(y.id);
      if (!fx || !fy) continue;
      const pairKey = `${x.id}|${y.id}`;
      if (reported.has(pairKey)) continue;
      const hit = reportBasisOf(x, y, fx, fy);
      if (!hit) continue;
      reported.add(pairKey);
      report.push(reportEntryOf(x, y, hit, mountOf));
    }
  }
  return { report, reported };
}

/**
 * 扫候选。纯读，一次都不写库。
 *
 * @returns {{auto: Array, report: Array, stats: object}}
 *   auto[i]   = { key, basis: string[], entities: [{id,display,norm,etype,memCount}], dst, surfaceRows, mountRows,
 *                planned: [{src, dst}], surfaceMoves, mountMoves }
 *   report[i] = { key, basis, score, a: {id,display,memCount}, b: {id,display,memCount}, detail }
 */
export function scanEntityTidy(db: DatabaseSync, { log = null }: { log?: ((line: string) => void) | null } = {}): TidyScanResult {
  const emit: (line: string) => void = typeof log === 'function' ? log : () => {};
  const rows = db.prepare(
    "SELECT id, norm, display, etype, mem_count FROM entities WHERE status='active' ORDER BY id",
  ).all() as Array<{ id: unknown; norm: unknown; display: unknown; etype: unknown; mem_count: unknown }>;
  const entities = rows.map((r) => ({
    id: Number(r.id),
    norm: String(r.norm || ''),
    display: String(r.display || ''),
    etype: String(r.etype || 'unknown'),
    memCount: Number(r.mem_count) || 0,
  }));
  const byId = new Map<number, TidyEntity>(entities.map((e) => [e.id, e] as [number, TidyEntity]));
  const surfaces = surfaceMapOf(db);
  const mountCache = new Map<number, number>();
  const mountOf = (id: number) => mountCount(db, id, mountCache);

  const union = makeUnion();
  for (const e of entities) union.add(e.id);
  const edges: TidyEdge[] = []; // {a, b, basis}

  // ① 折叠同名
  const { displayFold, byFold } = entityFoldIndex(entities);
  for (const edge of foldGroupEdges(byFold)) {
    edges.push(edge);
    union.union(edge.a, edge.b);
  }

  // ② 规范名归一（normalizeEntity）这一段故意没做自动合：
  // 它只能补出「norm 相等但折叠不同」的写法，而折叠（去空白/分隔符/大小写）比 norm 更粗，
  // 这种对实际不存在；反过来 normalizeEntity 会剥版本号（python 3.11 / 3.12 都归成 python），
  // 照 norm 合会把不同版本并成一个。版本类只进「只报」。

  // ③ 写法层交叉（双向成立才算边）
  const cross = crossEdgesOf({ byId, surfaces, displayFold, byFoldEntity: byFold, mountOf });
  for (const edge of cross.edges) {
    if (union.find(edge.a) === union.find(edge.b)) continue;
    edges.push(edge);
    union.union(edge.a, edge.b);
  }

  // 连通分量 → 自动合组
  const comps = new Map<number, TidyEntity[]>();
  for (const e of entities) {
    const root = union.find(e.id);
    if (!comps.has(root)) comps.set(root, []);
    comps.get(root)!.push(e);
  }
  const { auto, autoOf, extras } = buildAutoGroups({
    comps: comps.values(), edges, surfaces, displayFold, crossPairs: cross.pairs, mountOf,
  });

  // 只报候选 + 单向交叉 / 组内被踢掉的成员，一并进候选清单（不自动合）
  const { report, reported } = reportCandidatesOf({ entities, autoOf, mountOf });
  for (const extra of [...cross.oneWay, ...extras]) {
    const key = extra.key.replace(/^report:/, '');
    if (reported.has(key)) continue;
    reported.add(key);
    report.push(extra);
  }
  report.sort((x, y) => y.score - x.score || x.a.id - y.a.id);

  const stats = {
    entities: entities.length,
    surfaceRows: [...surfaces.values()].reduce((n, list) => n + list.length, 0),
    autoGroups: auto.length,
    autoEntities: auto.reduce((n, g) => n + g.entities.length, 0),
    autoMerges: auto.reduce((n, g) => n + g.planned.length, 0),
    surfaceMoves: auto.reduce((n, g) => n + g.surfaceMoves, 0),
    mountMoves: auto.reduce((n, g) => n + g.mountMoves, 0),
    reportPairs: report.length,
  };
  emit(
    `[entity-tidy] 扫描 实体 ${stats.entities} 条 / surface ${stats.surfaceRows} 行：` +
      `自动合 ${stats.autoGroups} 组（${stats.autoMerges} 次合并，搬 surface ${stats.surfaceMoves} 行、挂载 ${stats.mountMoves} 行），` +
      `只报 ${stats.reportPairs} 对`,
  );
  return { auto, report, stats };
}

/** memory_entity 有哪些列（老库没有 origin）。表不在时 PRAGMA 返回空集，由调用方按「没有 origin」处理。 */
function memoryEntityColumns(db: DatabaseSync): Set<string> {
  return new Set(db.prepare("PRAGMA table_info('memory_entity')").all().map((r) => String(r.name)));
}

/**
 * apply 要用的语句集合：缺 memory_entity / 账本表就返回 error，由调用方按「全部跳过」处理。
 * 语句准备本身就可能炸（老库缺表），这里整段兜住，不把异常冒到 compact 池。
 * @returns {{stmts: object|null, error: string}}
 */
function prepareApplyStatements(db: DatabaseSync): { stmts: TidyStmts | null; error: string } {
  const hasOrigin = memoryEntityColumns(db).has('origin');
  const stmts: TidyStmts = { hasOrigin } as TidyStmts;
  try {
    stmts.selectMounts = db.prepare('SELECT memory_id, role, ts, status, attrs, version, prev_memory_id FROM memory_entity WHERE entity_id=?');
  } catch (error) {
    return { stmts: null, error: `memory_entity 读不了（${String((error as { message?: unknown } | null | undefined)?.message || error)}）` };
  }
  try {
    stmts.insertMount = hasOrigin
      ? db.prepare(`INSERT OR IGNORE INTO memory_entity(memory_id, entity_id, role, ts, status, attrs, version, prev_memory_id, origin)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`)
      : db.prepare(`INSERT OR IGNORE INTO memory_entity(memory_id, entity_id, role, ts, status, attrs, version, prev_memory_id)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?)`);
    // 搬移语句一律带 entity_id 双约束：只搬「确实还挂在 src 上」的那几行，
    // 中途被别人挪走的行不许顺手改走（审计 实体高-1 / 高-2）。
    stmts.moveSurfaces = db.prepare('UPDATE entity_surface SET entity_id=? WHERE surface=? AND entity_id=?');
    stmts.surfacesOf = db.prepare('SELECT surface, norm FROM entity_surface WHERE entity_id=?');
    stmts.deleteMount = db.prepare('DELETE FROM memory_entity WHERE entity_id=? AND memory_id=? AND role=?');
    stmts.mountOf = db.prepare('SELECT 1 AS ok FROM memory_entity WHERE entity_id=? AND memory_id=? AND role=?');
    stmts.entityOf = db.prepare('SELECT id, display, status FROM entities WHERE id=?');
    stmts.setStatus = db.prepare('UPDATE entities SET status=?, last_seen=? WHERE id=?');
    stmts.recalc = db.prepare('UPDATE entities SET mem_count=(SELECT count(*) FROM memory_entity WHERE entity_id=?) WHERE id=?');
    stmts.recordMerge = db.prepare(`INSERT INTO entity_merge(src_entity, dst_entity, reason, evidence, run_id, created_at)
      VALUES (?, ?, ?, ?, ?, ?)`);
  } catch (error) {
    return { stmts: null, error: `账本表不全（${String((error as { message?: unknown } | null | undefined)?.message || error)}）` };
  }
  return { stmts, error: '' };
}

/**
 * 一个合并步（必须在事务里调）：拍快照 → 搬 surface → 搬挂载 → 逐条核数 → src 退场。
 * 快照在 BEGIN 之后拍，所以账上记的行就是实际搬的行；条数对不上直接 throw，由调用方回滚——
 * 宁可这一组不合，也不留半截凭据（审计 实体高-2）。
 * @returns {{surfaces:string[], mounts:object[], moved:number, inserted:number}}
 */
function mergeStepOf(stmts: TidyStmts, { src, dst, created }: { src: number; dst: number; created: string }): {
  surfaces: string[];
  mounts: Array<Record<string, unknown>>;
  moved: number;
  inserted: number;
} {
  const surfaces = stmts.surfacesOf.all(src).map((r) => String(r.surface));
  const mounts = stmts.selectMounts.all(src).map((r) => ({ ...r }));
  // 1) 迁写法层挂接（surface 的 norm 不变，只换 entity_id），按快照键逐行搬
  let moved = 0;
  for (const surface of surfaces) moved += Number(stmts.moveSurfaces.run(dst, surface, src).changes) || 0;
  if (moved !== surfaces.length) throw new Error(`surface 实搬 ${moved} 行 ≠ 快照 ${surfaces.length} 行`);
  // 2) 迁挂载；撞上 dst 已挂同一条记忆时 INSERT OR IGNORE 跳过，随后按快照键删 src 那行
  let inserted = 0;
  for (const m of mounts) {
    const r = stmts.hasOrigin
      ? stmts.insertMount.run(m.memory_id, dst, m.role, m.ts, m.status, m.attrs, m.version, m.prev_memory_id, 'manual')
      : stmts.insertMount.run(m.memory_id, dst, m.role, m.ts, m.status, m.attrs, m.version, m.prev_memory_id);
    inserted += Number(r.changes) || 0;
  }
  let removed = 0;
  for (const m of mounts) removed += Number(stmts.deleteMount.run(src, m.memory_id, m.role).changes) || 0;
  if (removed !== mounts.length) throw new Error(`mount 实删 ${removed} 行 ≠ 快照 ${mounts.length} 行`);
  // 每条快照键搬完必须真的落在 dst 上（撞键被 IGNORE 也算落住），否则整步回滚
  for (const m of mounts) {
    if (!stmts.mountOf.get(dst, m.memory_id, m.role)) throw new Error(`mount 没落到 dst：${m.memory_id}/${m.role}`);
  }
  // 3) 老实体退场（不删行：entity_merge 靠 id 回滚，历史账要留着）
  stmts.setStatus.run('merged', created, src);
  stmts.recalc.run(src, src); // 清空后必然 0
  stmts.recalc.run(dst, dst);
  return { surfaces, mounts, moved, inserted };
}

/**
 * 一个合并步的落地：事务内核状态 → 合并 → 写撤销凭据。失败只回滚这一步。
 * @returns {{status:'merged'|'stale'|'failed', row?:object, pair?:object}}
 */
function applyOneStep({ db, stmts, group, src, dst, dstExists, srcEntity, created, runId, emit }: {
  db: DatabaseSync;
  stmts: TidyStmts;
  group: TidyAutoGroup;
  src: number;
  dst: number;
  dstExists: Record<string, unknown>;
  srcEntity: Record<string, unknown>;
  created: string;
  runId: string;
  emit: (line: string) => void;
}): TidyStepResult {
  try {
    const out = withTransaction(db, () => {
      // 事务里再核一遍状态：扫完到落地之间可能被别的合并 / 手工改动吃过
      const dstNow = stmts.entityOf.get(dst);
      const srcNow = stmts.entityOf.get(src);
      if (!dstNow || !srcNow || String(dstNow.status) !== 'active' || String(srcNow.status) !== 'active') {
        const stale = new Error('stale_plan') as Error & { tidyAbort?: TidyStepResult };
        stale.tidyAbort = { status: 'stale' };
        throw stale;
      }
      const merged = mergeStepOf(stmts, { src, dst, created });
      // 撤销凭据：surface 原文 + 搬过来的挂载行，undo 照着搬回去
      const reason = `rule:${(group.basis || []).join('+') || 'fold_same'}`;
      const rec = stmts.recordMerge.run(
        src,
        dst,
        reason,
        JSON.stringify({ surfaces: merged.surfaces, mounts: merged.mounts, names: [srcEntity.display, dstExists.display] }),
        String(runId || ''),
        created,
      );
      return {
        status: 'merged' as const,
        row: { src, dst, surfaces: merged.moved, mounts: merged.inserted },
        pair: {
          mergeId: Number(rec?.lastInsertRowid) || 0,
          src,
          dst,
          srcName: String(srcEntity.display || ''),
          dstName: String(dstExists.display || ''),
          reason,
          surfaces: merged.moved,
          mounts: merged.inserted,
        },
      };
    });
    const p = out.pair;
    emit(`[entity-tidy] merge id=${p.mergeId} src=${src}(${p.srcName}) dst=${dst}(${p.dstName}) surface=${p.surfaces} mount=${p.mounts}`);
    return out;
  } catch (error) {
    if ((error as { tidyAbort?: TidyStepResult } | null | undefined)?.tidyAbort) {
      emit(`[entity-tidy] stale_plan src=${src} dst=${dst} reason=status_changed_in_txn`);
      return (error as { tidyAbort: TidyStepResult }).tidyAbort;
    }
    emit(`[entity-tidy] merge 失败 src=${src} dst=${dst}：${String((error as { message?: unknown } | null | undefined)?.message || error)}`);
    return { status: 'failed' };
  }
}

/**
 * 执行合并。每个合并步一个事务；一步坏了只回滚这一步，别的照合。
 *
 * @param {object} spec
 * @param {object} spec.plan scanEntityTidy() 的 auto
 * @param {boolean} [spec.dryRun] true 只打计划，不写库
 * @param {string} [spec.runId]
 * @returns {{groups:number, merged:number, surfaceMoved:number, mountsMoved:number,
 *   skipped:number, stale:number, rows:Array, pairs:Array}}
 */
export function applyEntityTidy(db: DatabaseSync, { plan = [], dryRun = false, runId = '', log = null, now = Date.now() }: { plan?: TidyAutoGroup[]; dryRun?: boolean; runId?: string; log?: ((line: string) => void) | null; now?: number } = {}): TidyApplyResult {
  const emit: (line: string) => void = typeof log === 'function' ? log : () => {};
  // pairs 是给人看的账：每对记 mergeId + 双方名字，
  // 报告和撤回都靠它定位；rows 是内部落库明细，形状不一样，别混用。
  const out: TidyApplyResult = { groups: 0, merged: 0, surfaceMoved: 0, mountsMoved: 0, skipped: 0, stale: 0, rows: [], pairs: [] };
  const created = new Date(now).toISOString();

  const { stmts, error } = prepareApplyStatements(db);
  if (!stmts) {
    emit(`[entity-tidy] apply 跳过：${error}`);
    out.skipped = plan.length;
    return out;
  }

  for (const group of plan) {
    out.groups += 1;
    const dst = Number(group.dst?.id);
    const dstExists = dst ? stmts.entityOf.get(dst) : null;
    // 计划是扫描时的快照：dst 已经不是 active（被别的合并吃掉 / 手工改过）就整组跳过（审计 实体中-4）
    if (!dstExists || String(dstExists.status) !== 'active') {
      out.skipped += 1;
      out.stale += 1;
      emit(`[entity-tidy] stale_plan dst=${dst || 0} reason=dst_not_active`);
      continue;
    }
    for (const step of group.planned || []) {
      const src = Number(step.src);
      const srcEntity = src ? stmts.entityOf.get(src) : null;
      if (!src || src === dst || !srcEntity || String(srcEntity.status) !== 'active') {
        out.skipped += 1;
        out.stale += 1;
        emit(`[entity-tidy] stale_plan src=${src} dst=${dst} reason=src_not_active`);
        continue;
      }
      if (dryRun) {
        out.rows.push({
          src, dst, surfaces: stmts.surfacesOf.all(src).length, mounts: stmts.selectMounts.all(src).length, dryRun: true,
        });
        continue;
      }
      const stepOut = applyOneStep({
        db, stmts, group, src, dst, dstExists, srcEntity, created, runId, emit,
      });
      if (stepOut.status === 'stale') { out.skipped += 1; out.stale += 1; continue; }
      if (stepOut.status === 'failed') { out.skipped += 1; continue; }
      out.merged += 1;
      out.surfaceMoved += stepOut.row!.surfaces;
      out.mountsMoved += stepOut.row!.mounts;
      out.rows.push(stepOut.row!);
      out.pairs.push(stepOut.pair!);
    }
  }
  if (!dryRun && out.merged > 0) bumpSurfaceEpoch(db);
  emit(
    `[entity-tidy] ${dryRun ? 'dry-run' : 'apply'}：组 ${out.groups} 合并 ${out.merged} 次 ` +
      `surface ${out.surfaceMoved} 行 mount ${out.mountsMoved} 行 跳过 ${out.skipped}（stale ${out.stale}）`,
  );
  return out;
}

/**
 * 撤销一次合并（entity_merge 是唯一凭据）。
 * 只允许撤最近一次未被撤过的合并（uq_entity_merge_src 保证一个 src 同时只有一条有效记录）。
 *
 * 2026-11-15 审计（实体高-1 / 中-5 / 中-6）之后的三条硬规矩：
 *   1) 读写同一个事务：SELECT 挪到 BEGIN IMMEDIATE 之后，undone_at 的 UPDATE 带 AND undone_at=''
 *      并核 changes=1，否则 ROLLBACK 报 already_undone；
 *   2) 链上的合并先撤新的：src/dst 还被某条 id 更大的未撤销合并占着时拒绝，
 *      否则搬回去的行会落在已经并走的实体上，再撤新的那条就造出重复挂载；
 *   3) 回执只报真实搬回的条数（restored/skipped），不拿 evidence 条数充成功数。
 */
function recalcChain(db: DatabaseSync, ids: unknown[]): number[] {
  // 合并链上（含已撤销的历史行）所有碰过的实体一起重算 mem_count：链式合并会让
  // 中间实体的计数在撤一条之后仍然虚高，只重算 src/dst 两个数是不够的。
  const set = new Set(ids.map((n) => Number(n)).filter((n) => Number.isFinite(n) && n > 0));
  const rows = db.prepare('SELECT src_entity, dst_entity FROM entity_merge').all();
  let grew = true;
  while (grew) {
    grew = false;
    for (const r of rows) {
      const a = Number(r.src_entity);
      const b = Number(r.dst_entity);
      if (!set.has(a) && !set.has(b)) continue;
      if (!set.has(a)) { set.add(a); grew = true; }
      if (!set.has(b)) { set.add(b); grew = true; }
    }
  }
  const recalc = db.prepare('UPDATE entities SET mem_count=(SELECT count(*) FROM memory_entity WHERE entity_id=?) WHERE id=?');
  for (const id of set) recalc.run(id, id);
  return [...set];
}

/**
 * 撤销一次合并。
 * @returns {{ok:boolean, reason?:string, mergeId?:number, src?:number, dst?:number,
 *   restored?:number, skipped?:number, surfaces?:number, mounts?:number, blockingMergeId?:number}}
 */
/**
 * 把一条 evidence 里的 surface / 挂载行搬回 src（必须在事务里调）。
 * 只搬「现在确实挂在 dst 上」的行，搬不动的算 skipped——回执照实报，不拿 evidence 条数充数。
 * @returns {{surfaceRestored:number, surfaceSkipped:number, mountRestored:number, mountSkipped:number}}
 */
function restoreEvidenceOf(stmts: TidyUndoStmts, { src, dst, surfaces, mounts }: { src: number; dst: number; surfaces: string[]; mounts: Array<Record<string, SQLInputValue>> }): {
  surfaceRestored: number;
  surfaceSkipped: number;
  mountRestored: number;
  mountSkipped: number;
} {
  let surfaceRestored = 0;
  let surfaceSkipped = 0;
  for (const surface of surfaces) {
    const n = Number(stmts.restoreSurface!.run(src, surface, dst).changes) || 0;
    if (n) surfaceRestored += n; else surfaceSkipped += 1;
  }
  let mountRestored = 0;
  let mountSkipped = 0;
  for (const m of mounts) {
    const ins = stmts.hasOrigin
      ? stmts.insertMount!.run(m.memory_id, src, m.role, m.ts, m.status, m.attrs, m.version, m.prev_memory_id, 'manual')
      : stmts.insertMount!.run(m.memory_id, src, m.role, m.ts, m.status, m.attrs, m.version, m.prev_memory_id);
    // dst 那边只有「src 已经立住」时才删，否则先把 dst 的行删了就把挂载弄丢了
    const dropped = Number(stmts.dropMount!.run(dst, m.memory_id, m.role).changes) || 0;
    if (Number(ins.changes) || dropped) mountRestored += 1; else mountSkipped += 1;
  }
  return { surfaceRestored, surfaceSkipped, mountRestored, mountSkipped };
}

export function undoEntityMerge(db: DatabaseSync, { mergeId = 0, log = null, now = Date.now() }: { mergeId?: number; log?: ((line: string) => void) | null; now?: number } = {}): TidyUndoResult {
  const emit: (line: string) => void = typeof log === 'function' ? log : () => {};
  const hasOrigin = memoryEntityColumns(db).has('origin');
  const undoAt = new Date(now).toISOString();
  const stmts: TidyUndoStmts = { hasOrigin, restoreSurface: null, insertMount: null, dropMount: null };
  let row: Record<string, unknown> = null as unknown as Record<string, unknown>;
  // 事务里要提前收手的分支：抛一个带结果的哨兵，由 withTransaction 回滚，再在外层原样返回。
  const abort = (result: TidyUndoResult, line?: string): never => {
    const error = new Error('tx_abort') as Error & { tidyAbort?: unknown; tidyLog?: string };
    error.tidyAbort = result;
    error.tidyLog = line || '';
    throw error;
  };
  try {
    const out: TidyUndoResult & { logLine: string } = withTransaction(db, () => {
      // 读也进事务（审计 实体中-5）：先拿写锁再读，读完到写完之间别人插不进来
      row = mergeId
        ? db.prepare("SELECT * FROM entity_merge WHERE id=? AND undone_at=''").get(Number(mergeId)) as Record<string, unknown>
        : db.prepare("SELECT * FROM entity_merge WHERE undone_at='' ORDER BY id DESC LIMIT 1").get() as Record<string, unknown>;
      if (!row) abort({ ok: false, reason: 'no_merge', restored: 0, skipped: 0 });
      const src = Number(row.src_entity);
      const dst = Number(row.dst_entity);
      // 链上还有更新的合并压着这条时先别撤（审计 实体高-1）：先撤 id 更大的那条，
      // 否则我们搬回去的行会落在已经并走的实体上，再撤新的那条就是重复挂载。
      const newer = db.prepare(`SELECT id FROM entity_merge
          WHERE undone_at='' AND id>? AND (src_entity IN (?,?) OR dst_entity IN (?,?))
          ORDER BY id DESC LIMIT 1`).get(Number(row.id), src, dst, src, dst);
      if (newer) {
        const blockingMergeId = Number(newer.id);
        abort(
          { ok: false, reason: 'undo_newer_first', mergeId: Number(row.id), src, dst, blockingMergeId, restored: 0, skipped: 0 },
          `[entity-tidy] undo 拒绝 merge=${row.id}：链上还有更新的 merge=${blockingMergeId}，先撤它`,
        );
      }
      let evidence: { surfaces?: unknown; mounts?: unknown } = {};
      try { evidence = JSON.parse(row.evidence as string || '{}'); } catch { evidence = {}; }
      const surfaces = Array.isArray(evidence.surfaces) ? evidence.surfaces.map(String) : [];
      const mounts = Array.isArray(evidence.mounts) ? evidence.mounts : [];
      // 搬回一律带 entity_id 约束：只把「现在还挂在 dst 上」的行搬回 src
      stmts.restoreSurface = db.prepare('UPDATE entity_surface SET entity_id=? WHERE surface=? AND entity_id=?');
      stmts.insertMount = hasOrigin
        ? db.prepare(`INSERT OR IGNORE INTO memory_entity(memory_id, entity_id, role, ts, status, attrs, version, prev_memory_id, origin)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`)
        : db.prepare(`INSERT OR IGNORE INTO memory_entity(memory_id, entity_id, role, ts, status, attrs, version, prev_memory_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)`);
      stmts.dropMount = db.prepare('DELETE FROM memory_entity WHERE entity_id=? AND memory_id=? AND role=?');
      const counts = restoreEvidenceOf(stmts, { src, dst, surfaces, mounts });
      db.prepare("UPDATE entities SET status='active', last_seen=? WHERE id=?").run(undoAt, src);
      // 链上所有碰过的实体一起重算：只算 src/dst 两个数，中间实体的 mem_count 会虚高
      recalcChain(db, [src, dst]);
      const marked = Number(db.prepare("UPDATE entity_merge SET undone_at=? WHERE id=? AND undone_at=''")
        .run(undoAt, Number(row.id)).changes) || 0;
      if (marked !== 1) {
        abort(
          { ok: false, reason: 'already_undone', mergeId: Number(row.id), restored: 0, skipped: 0 },
          `[entity-tidy] undo 放弃 merge=${row.id}：这条已被别人撤掉（already_undone）`,
        );
      }
      bumpSurfaceEpoch(db);
      const restored = counts.surfaceRestored + counts.mountRestored;
      const skipped = counts.surfaceSkipped + counts.mountSkipped;
      return {
        ok: true,
        mergeId: Number(row.id),
        src,
        dst,
        restored,
        skipped,
        surfaces: counts.surfaceRestored,
        mounts: counts.mountRestored,
        surfaceSkipped: counts.surfaceSkipped,
        mountSkipped: counts.mountSkipped,
        logLine:
          `[entity-tidy] undo merge=${row.id} src=${src} dst=${dst} ` +
          `surface=${counts.surfaceRestored}/${surfaces.length} mount=${counts.mountRestored}/${mounts.length} ` +
          `restored=${restored} skipped=${skipped}`,
      };
    });
    const { logLine, ...result } = out;
    emit(logLine);
    return result;
  } catch (error) {
    if ((error as { tidyAbort?: TidyUndoResult } | null | undefined)?.tidyAbort) {
      if ((error as { tidyLog?: string }).tidyLog) emit((error as { tidyLog?: string }).tidyLog!);
      return (error as { tidyAbort: TidyUndoResult }).tidyAbort;
    }
    emit(`[entity-tidy] undo 失败 merge=${row?.id || mergeId || '-'}：${String((error as { message?: unknown } | null | undefined)?.message || error)}`);
    return { ok: false, reason: String((error as { message?: unknown } | null | undefined)?.message || error), restored: 0, skipped: 0 };
  }
}

/**
 * 按批撤回一轮自动合并。
 *
 * run_id 是 nightly 的 `sleep_<day>`，或脚本的 `tidy_<ts>`：把这一批里还没撤过的
 * entity_merge 倒序逐条 undo。倒序是因为同一 dst 可能被链式合并多次，后合的必须先撤，
 * 否则先把早期 merge 撤回去、后期那条的 evidence 会指向一个又变回 active 的 src。
 *
 * @returns {{ok:boolean, runId:string, undone:number, failed:number, mergeIds:number[], reason?:string}}
 */
export function undoEntityTidyRun(db: DatabaseSync, { runId = '', log = null, now = Date.now() }: { runId?: string; log?: ((line: string) => void) | null; now?: number } = {}): TidyUndoRunResult {
  const emit: (line: string) => void = typeof log === 'function' ? log : () => {};
  const key = String(runId || '').trim();
  if (!key) return { ok: false, reason: 'run_id_required', runId: '', undone: 0, failed: 0, mergeIds: [] };
  let rows: Array<{ id: unknown }> = [];
  try {
    rows = db.prepare("SELECT id FROM entity_merge WHERE run_id=? AND undone_at='' ORDER BY id DESC").all(key) as Array<{ id: unknown }>;
  } catch (error) {
    return { ok: false, reason: String((error as { message?: unknown } | null | undefined)?.message || error), runId: key, undone: 0, failed: 0, mergeIds: [] };
  }
  if (!rows.length) return { ok: false, reason: 'no_merge', runId: key, undone: 0, failed: 0, mergeIds: [] };
  const out: TidyUndoRunResult = { ok: true, runId: key, undone: 0, failed: 0, mergeIds: [] };
  for (const row of rows) {
    const r = undoEntityMerge(db, { mergeId: Number(row.id), log: emit, now });
    if (r.ok) {
      out.undone += 1;
      out.mergeIds.push(Number(row.id));
    } else {
      out.failed += 1;
      out.ok = false;
      // 链上被更新的合并压着：这一批更早的行同样撤不动，直接停手，等人先撤新的那条
      if (r.reason === 'undo_newer_first') {
        out.reason = 'undo_newer_first';
        out.blockingMergeId = Number(r.blockingMergeId) || 0;
        break;
      }
    }
  }
  emit(`[entity-tidy] undo-run run_id=${key} 撤回 ${out.undone} 次 失败 ${out.failed}`);
  return out;
}

/**
 * 写一份人能直接看的 nightly 报告。
 *
 * 汇报要求：这轮自动合了哪些对、跳过了哪些候选、撤回命令是什么。
 * 口径说明必须写进去：本机制只合并实体（entities / entity_surface / memory_entity），
 * 不动 memories 正文，所以没有「合并正文 token 校验」这一项；撤销凭据是
 * entity_merge.evidence 里的 surface 原文与挂载行快照，撤回是把它们原样搬回旧实体。
 *
 * @returns {string} 实际写入的文件路径
 */
export function writeEntityTidyReport(file: string, { runId = '', scan = null, applied = null, now = Date.now(), log = null }: { runId?: string; scan?: TidyScanResult | null; applied?: TidyApplyResult | null; now?: number; log?: ((line: string) => void) | null } = {}): string {
  const emit: (line: string) => void = typeof log === 'function' ? log : () => {};
  const target = String(file || '');
  if (!target) throw new Error('writeEntityTidyReport: file 必填');
  const stats = scan?.stats || {} as TidyScanStats;
  const pairs = Array.isArray(applied?.pairs) ? applied.pairs : [];
  const skippedPairs = Array.isArray(scan?.report) ? scan.report : [];
  const esc = (v: unknown): string => String(v ?? '').replace(/\|/g, '\\|').replace(/\n/g, ' ');
  const at = new Date(now).toISOString();
  const lines = [];
  lines.push('# 实体整理自动合并报告');
  lines.push('');
  lines.push(`- 时间：${at}`);
  lines.push(`- run_id：${runId || '-'}`);
  lines.push(`- 扫描：实体 ${Number(stats.entities) || 0} 条 / surface ${Number(stats.surfaceRows) || 0} 行`);
  lines.push(`- 自动合候选：${Number(stats.autoGroups) || 0} 组 / ${Number(stats.autoMerges) || 0} 次；本轮实际合并 ${pairs.length} 次`);
  lines.push(`- 跳过（只报不合并）候选：${skippedPairs.length} 对`);
  lines.push('');
  lines.push('## 本轮自动合并');
  lines.push('');
  if (!pairs.length) {
    lines.push('无。');
  } else {
    lines.push('| mergeId | 旧实体 | 并入 | 依据 | surface | mount | 单条撤回 |');
    lines.push('| --- | --- | --- | --- | --- | --- | --- |');
    for (const p of pairs) {
      lines.push(
        `| ${p.mergeId || '-'} | ${esc(p.src)} ${esc(p.srcName)} | ${esc(p.dst)} ${esc(p.dstName)} | ` +
          `${esc(p.reason)} | ${Number(p.surfaces) || 0} | ${Number(p.mounts) || 0} | ` +
          `\`node scripts/entity-tidy.mjs --undo ${p.mergeId || 0}\` |`,
      );
    }
  }
  lines.push('');
  lines.push('## 跳过（只报不合并）');
  lines.push('');
  if (!skippedPairs.length) {
    lines.push('无。');
  } else {
    lines.push('| 依据 | 分数 | 实体 A | 实体 B | 说明 |');
    lines.push('| --- | --- | --- | --- | --- |');
    for (const r of skippedPairs.slice(0, 100)) {
      lines.push(
        `| ${esc(r.basis)} | ${Number(r.score) || 0} | ${esc(r.a?.id)} ${esc(r.a?.display)} | ` +
          `${esc(r.b?.id)} ${esc(r.b?.display)} | ${esc(r.detail)} |`,
      );
    }
    if (skippedPairs.length > 100) lines.push(`| - | - | - | - | 另外 ${skippedPairs.length - 100} 对，跑 --json 看全量 |`);
  }
  lines.push('');
  lines.push('## 撤回');
  lines.push('');
  lines.push('按批撤回本轮（run_id 上表）：');
  lines.push('');
  lines.push('```bash');
  lines.push(`node scripts/entity-tidy.mjs --undo-run ${runId || '<run_id>'}`);
  lines.push('```');
  lines.push('');
  lines.push('单条撤回：`node scripts/entity-tidy.mjs --undo <entity_merge.id>`（不给 id 撤最近一条）。');
  lines.push('');
  lines.push('## 口径说明');
  lines.push('');
  lines.push('- 本机制只合并实体层（entities / entity_surface / memory_entity），不改 memories 正文，因此没有「合并正文 token 校验」这一步。');
  lines.push('- 撤销凭据是 entity_merge.evidence：记录被搬走的 surface 原文与挂载行快照，撤回时原样搬回旧实体并把它恢复 active。');
  lines.push('- 自动合只吃 fold_same / surface_cross；单复数、版本、前缀、相似度仍只报不合并。');
  lines.push('');
  fs.mkdirSync(path.dirname(target), { recursive: true });
  fs.writeFileSync(target, lines.join('\n'), 'utf8');
  emit(`[entity-tidy] 报告 ${target} run_id=${runId || '-'} 合并 ${pairs.length} 对 候选 ${skippedPairs.length} 对`);
  return target;
}

/**
 * 一次实体整理：状态收进 TidyRun 类。
 *
 * 一轮 = 一个 runId + 一次扫描 + （可选）一次落地 + 一份报告。类持有这一轮的状态：
 * db、apply/dryRun、runId、扫描结果、落地结果；扫描/合并算法仍是纯函数
 * （scanEntityTidy / applyEntityTidy 给定 db 与 plan 就出结果，不持有轮级状态）。
 * 撤销是事后动作，不依赖本轮实例，做成静态方法（TidyRun.undo / TidyRun.undoRun）。
 */
export class TidyRun {
  #db: DatabaseSync;

  #runId: string;

  #apply: boolean;

  #dryRun: boolean;

  #now: number;

  #log: (line: string) => void;

  #scan: TidyScanResult | null = null;

  #applied: TidyApplyResult | null = null;

  constructor(db: DatabaseSync, { apply = false, dryRun = false, log = null, runId = '', now = Date.now() }: { apply?: boolean; dryRun?: boolean; log?: ((line: string) => void) | null; runId?: string; now?: number } = {}) {
    this.#db = db;
    this.#runId = String(runId || '');
    this.#apply = apply === true;
    this.#dryRun = dryRun === true;
    this.#now = now;
    this.#log = typeof log === 'function' ? log : () => {};
  }

  get runId(): string {
    return this.#runId;
  }

  /** 扫描结果（run() 前后都可读；扫描前为 null）。 */
  get scan(): TidyScanResult | null {
    return this.#scan;
  }

  /** 落地结果（没合过就是 null）。 */
  get applied(): TidyApplyResult | null {
    return this.#applied;
  }

  /** 扫一遍实体候选，结果留在实例上。 */
  scanCandidates(): TidyScanResult {
    this.#scan = scanEntityTidy(this.#db, { log: this.#log });
    return this.#scan!;
  }

  /** 真合（只有 apply=true 且非 dryRun 才动库），返回落地结果或 null。 */
  applyMerges(): TidyApplyResult | null {
    const scan = this.#scan || this.scanCandidates();
    if (this.#apply && !this.#dryRun && scan.auto.length) {
      this.#applied = applyEntityTidy(this.#db, { plan: scan.auto, runId: this.#runId, log: this.#log, now: this.#now });
    }
    return this.#applied;
  }

  /** 一整轮：扫描 →（按开关）落地，返回与老 runEntityTidyPass 逐字段一致的形状。 */
  run(): TidyScanResult & { applied: TidyApplyResult | null } {
    const scan = this.scanCandidates();
    this.applyMerges();
    return { ...scan, applied: this.#applied };
  }

  /** 写 nightly 报告；返回实际写入的文件路径。 */
  writeReport(file: string): string {
    return writeEntityTidyReport(file, {
      runId: this.#runId, scan: this.#scan, applied: this.#applied, now: this.#now, log: this.#log,
    });
  }

  /** 单条撤回（照 entity_merge.id）。 */
  static undo(db: DatabaseSync, opts: { mergeId?: number; log?: ((line: string) => void) | null; now?: number } = {}): TidyUndoResult {
    return undoEntityMerge(db, opts);
  }

  /** 按 run_id 整批撤回。 */
  static undoRun(db: DatabaseSync, opts: { runId?: string; log?: ((line: string) => void) | null; now?: number } = {}): TidyUndoRunResult {
    return undoEntityTidyRun(db, opts);
  }
}

/**
 * 每晚安静阶段调这一个：扫 + 打账。默认只报不合（apply 需要显式打开），
 * 因为合实体不可逆感强，先用 scripts/entity-tidy.mjs 出 dry-run 账，人看过再放。
 */
export function runEntityTidyPass(db: DatabaseSync, { apply = false, dryRun = false, log = null, runId = '', now = Date.now() }: { apply?: boolean; dryRun?: boolean; log?: ((line: string) => void) | null; runId?: string; now?: number } = {}): TidyScanResult & { applied: TidyApplyResult | null } {
  return new TidyRun(db, { apply, dryRun, log, runId, now }).run();
}
