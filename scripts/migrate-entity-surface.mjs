#!/usr/bin/env node
/**
 * 把现有真实写法迁进 entity_surface（实体去重一期，数据层）。
 *
 * 依据 docs/dsh-memory-entity-dedup-2026-11-14.md §1.3 / §3.2 / §11.1 第 2 项：
 *   ① memories.entities 里所有原文写法 → entity_surface（source='migrate'），**一种原文一行**
 *   ② 找不到对应实体的写法 → entity_issue(reason='surface_unmapped')
 *   ③ 没有任何原文写法的实体，补一条 display 当 surface
 *   ④ 路径写法 norm 保留全路径（§3.2 不变式 3，R8 basename 折叠废除）
 *
 * 表结构：surface 是主键（原文唯一），同一 norm 允许多行（大小写/别名/版本变体各自留原文），
 * 但同一个 norm 的所有行必须指同一个 entity_id（A1，写入时报冲突跳过）。
 *
 * norm 算法就是 src/shared/entity-normalize.ts 的 normalizeEntity（2026-11-14 起两处已合并：
 * R8 basename 折叠废除、硬编码别名表删除，路径整个保留）。本脚本是历史迁移工具，
 * 跑过一次就不再需要；保留 normKeepPath 只是为了不再复制一份规则。
 *
 * 幂等：surface 主键 + INSERT OR IGNORE，重复跑不产生重复行。
 * 默认 --dry-run，只算不写；确认后才 --apply。
 *
 * 用法：
 *   node scripts/migrate-entity-surface.mjs [--dry-run] [--apply] [--db /path/memory.db]
 */
import { openDb, resolveDbPath } from './lib/db.mjs';
import { normalizeEntity } from '../lib/shared/entity-normalize.js';

const argv = process.argv.slice(2);
const DB = argv.includes('--db') ? argv[argv.indexOf('--db') + 1] : resolveDbPath();
const APPLY = argv.includes('--apply');
const NOW = new Date().toISOString();

/**
 * 一期 norm：直接委托 normalizeEntity（路径保留全路径、无硬编码别名）。
 * 历史名字留着，免得调用点和本脚本一起大改。
 */
export function normKeepPath(raw) {
  return normalizeEntity(raw).norm;
}

const db = openDb(DB);
db.exec('PRAGMA busy_timeout = 10000');

const version = (db.prepare(`SELECT value FROM meta WHERE key='entity_schema_version'`).get() || {}).value;
const surfExists = !!db.prepare(`SELECT 1 FROM sqlite_master WHERE type='table' AND name='entity_surface'`).get();
const pkCols = surfExists
  ? db.prepare(`PRAGMA index_list(entity_surface)`).all().filter((r) => r.origin === 'pk')
      .flatMap((r) => db.prepare(`PRAGMA index_info('${r.name}')`).all().map((x) => x.name))
  : [];
if (!surfExists || Number(version || 0) < 2) {
  console.error(JSON.stringify({
    ok: false,
    error: 'entity_surface 表或 meta.entity_schema_version=2 还没就位，先跑 apply-entity-surface.mjs --apply',
    entity_schema_version: version,
    surfaceTable: surfExists,
  }));
  db.close();
  process.exit(1);
}
if (!(pkCols.length === 1 && pkCols[0] === 'surface')) {
  console.error(JSON.stringify({
    ok: false,
    error: 'entity_surface 主键不是 surface（一种原文一行的新结构），先跑 apply-entity-surface.mjs --rebuild-surface',
    surfacePk: pkCols,
  }));
  db.close();
  process.exit(1);
}

// ---------- 1) 从 memories.entities 解析全部原文写法 ----------
const ENT = db.prepare(`SELECT id, norm, display, status, first_seen, last_seen FROM entities`).all();
const entByNorm = new Map(ENT.map((e) => [e.norm, e]));
const entById = new Map(ENT.map((e) => [e.id, e]));

/** surface 原文 → { count, first, last, memId } */
const rawMap = new Map();
let pairs = 0;
for (const row of db.prepare(`SELECT id, entities, ts FROM memories
                              WHERE entities IS NOT NULL AND entities NOT IN ('', '[]')`).iterate()) {
  let arr;
  try { arr = JSON.parse(row.entities); } catch { continue; }
  if (!Array.isArray(arr)) continue;
  for (const item of arr) {
    const s = String(item ?? '').trim();
    if (!s) continue;
    pairs += 1;
    const cur = rawMap.get(s) || { count: 0, first: '', last: '', memId: row.id };
    cur.count += 1;
    if (row.ts) {
      if (!cur.first || row.ts < cur.first) cur.first = row.ts;
      if (!cur.last || row.ts > cur.last) cur.last = row.ts;
    }
    rawMap.set(s, cur);
  }
}

// ---------- 2) 逐原文算 norm、映射实体，先定每个 norm 的唯一维护者 ----------
const rows = [];          // 待插 surface 行，一条原文一行
const unmapped = [];      // 无实体的原文
const normVotes = new Map(); // norm -> Map(entity_id -> count)
const skippedEmpty = [];

for (const [surface, v] of rawMap) {
  const norm = normKeepPath(surface);
  if (!norm) { skippedEmpty.push(surface); continue; }
  const e = entByNorm.get(normalizeEntity(surface).norm);
  const id = e ? e.id : 0;
  const votes = normVotes.get(norm) || new Map();
  votes.set(id, (votes.get(id) || 0) + v.count);
  normVotes.set(norm, votes);
}

const conflicts = [];   // 同一 norm 指向多个实体（迁移期就报，不静默挑一个）
const loserRows = [];   // 因冲突被跳过的原文
const normOwner = new Map(); // norm -> entity_id（赢家：票数最多，平手取小 id）
for (const [norm, votes] of normVotes) {
  const sorted = [...votes].sort((a, b) => b[1] - a[1] || a[0] - b[0]);
  normOwner.set(norm, sorted[0][0]);
  if (votes.size > 1) {
    conflicts.push({ norm, winner: sorted[0][0], votes: sorted.map(([entity_id, count]) => ({ entity_id, count })) });
  }
}

for (const [surface, v] of rawMap) {
  const norm = normKeepPath(surface);
  if (!norm) continue;
  const e = entByNorm.get(normalizeEntity(surface).norm);
  const id = e ? e.id : 0;
  const winner = normOwner.get(norm);
  if (id !== winner) {
    // 同 norm 不同实体：守 A1，这行不插，原文记进冲突清单
    loserRows.push({ surface, norm, entity_id: id, winner });
    continue;
  }
  if (id === 0) {
    unmapped.push({ norm, surface, count: v.count });
    rows.push({ norm, surface, entity_id: 0, count: v.count,
      first: v.first, last: v.last, flag: 'unmapped' });
    continue;
  }
  const status = entById.get(id)?.status || 'active';
  const flag = status === 'archived' ? 'needs_review' : norm.includes('/') ? 'path' : '';
  rows.push({ norm, surface, entity_id: id, count: v.count,
    first: v.first, last: v.last, flag });
}

// ---------- 3) 没有原文写法的实体补一条 display ----------
const covered = new Set(rows.map((r) => r.entity_id).filter((id) => id > 0));
const normOwners = new Map(); // norm -> Set(entity_id)，含已排好的行
for (const r of rows) {
  if (!normOwners.has(r.norm)) normOwners.set(r.norm, new Set());
  normOwners.get(r.norm).add(r.entity_id);
}
const displayRows = [];
const displayConflicts = [];
for (const e of ENT) {
  if (covered.has(e.id)) continue;
  const norm = normKeepPath(e.display);
  if (!norm) { displayConflicts.push({ entity_id: e.id, display: e.display, norm, reason: 'empty_norm' }); continue; }
  const owners = normOwners.get(norm);
  if (owners && [...owners].some((x) => x !== e.id)) {
    displayConflicts.push({ entity_id: e.id, display: e.display, norm, reason: 'norm_owned_by_other_entity' });
    continue;
  }
  displayRows.push({ norm, surface: e.display, entity_id: e.id, count: 0,
    first: e.first_seen || '', last: e.last_seen || '',
    flag: e.status === 'archived' ? 'needs_review' : norm.includes('/') ? 'path' : '' });
}

// ---------- 4) 写库 / 干跑 ----------
const insSurface = db.prepare(`INSERT OR IGNORE INTO entity_surface
  (surface, norm, entity_id, source, count, first_seen, last_seen, flag)
  VALUES (?, ?, ?, 'migrate', ?, ?, ?, ?)`);
const insIssue = db.prepare(`INSERT OR IGNORE INTO entity_issue
  (memory_id, entity_norm, raw_name, reason, detail, created_at)
  VALUES (?, ?, ?, 'surface_unmapped', ?, ?)`);

let inserted = 0;
let insertedDisplay = 0;
let existing = 0;
let issueInserted = 0;
if (APPLY) {
  db.exec('BEGIN IMMEDIATE');
  try {
    for (const r of rows) {
      const c = Number(insSurface.run(r.surface, r.norm, r.entity_id, r.count, r.first, r.last, r.flag).changes);
      if (c) inserted += 1; else existing += 1;
    }
    for (const d of displayRows) {
      const c = Number(insSurface.run(d.surface, d.norm, d.entity_id, d.count, d.first, d.last, d.flag).changes);
      if (c) insertedDisplay += 1; else existing += 1;
    }
    // unmapped 按 norm 归一条 issue（PK 是 memory_id+entity_norm+reason）
    const byNorm = new Map();
    for (const u of unmapped) {
      const cur = byNorm.get(u.norm) || { surfaces: [], count: 0, raw: u.surface };
      cur.surfaces.push(u.surface); cur.count += u.count;
      byNorm.set(u.norm, cur);
    }
    for (const [norm, u] of byNorm) {
      const detail = JSON.stringify({ surfaces: u.surfaces, count: u.count,
        note: 'memories.entities 原文在，entities 里没有对应 norm' });
      issueInserted += Number(insIssue.run('', norm, u.raw, detail, NOW).changes);
    }
    db.exec('COMMIT');
  } catch (err) {
    db.exec('ROLLBACK');
    console.error(JSON.stringify({ ok: false, error: String(err) }));
    db.close();
    process.exit(1);
  }
}

const total = db.prepare(`SELECT count(*) c FROM entity_surface`).get().c;
const sumCount = db.prepare(`SELECT coalesce(sum(count),0) c FROM entity_surface`).get().c;
const normMulti = db.prepare(`SELECT count(*) c FROM (SELECT norm FROM entity_surface GROUP BY norm HAVING count(*) > 1)`).get().c;
const normToMultiEntity = db.prepare(`SELECT count(*) c FROM (SELECT norm FROM entity_surface
  GROUP BY norm HAVING count(DISTINCT entity_id) > 1)`).get().c;
const bySource = db.prepare(`SELECT source, count(*) c FROM entity_surface GROUP BY source ORDER BY c DESC`).all();
const byFlag = db.prepare(`SELECT flag, count(*) c FROM entity_surface GROUP BY flag ORDER BY c DESC`).all();
const out = {
  db: DB,
  mode: APPLY ? 'apply' : 'dry-run',
  surfacePk: pkCols,
  memoriesWithEntities: db.prepare(`SELECT count(*) c FROM memories
    WHERE entities IS NOT NULL AND entities NOT IN ('', '[]')`).get().c,
  pairs,
  rawSurfaces: rawMap.size,          // 设计稿口径 2668
  normGroups: normVotes.size,        // 去掉 R8 后归到多少个 norm
  planned: {
    surfaceRows: rows.length,        // 一种原文一行
    displayRows: displayRows.length,
    unmappedRows: unmapped.length,
    unmappedNorms: new Set(unmapped.map((u) => u.norm)).size,
    conflicts: conflicts.length,
    loserRows: loserRows.length,
    displayConflicts: displayConflicts.length,
    archivedFlagged: rows.filter((r) => r.flag === 'needs_review').length,
    emptyNormSkipped: skippedEmpty.length,
  },
  written: APPLY ? { inserted, insertedDisplay, existingSkipped: existing, issueInserted } : null,
  surfaceTotal: total,
  surfaceSumCount: sumCount,         // 应等于 pairs
  normsWithMultipleRows: normMulti,
  normsPointingToMultipleEntities: normToMultiEntity,
  surfaceBySource: bySource,
  surfaceByFlag: byFlag,
  unmapped: unmapped.map((u) => ({ norm: u.norm, surface: u.surface, count: u.count })),
  conflicts,
  loserRows,
  displayConflicts,
};
console.log(JSON.stringify(out, null, 2));
db.close();
