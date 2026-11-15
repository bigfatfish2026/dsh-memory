#!/usr/bin/env node
/**
 * 建 surface 层三张表（实体去重一期，数据层）。
 *
 * 依据 docs/dsh-memory-entity-dedup-2026-11-14.md §3.1（DDL）/ §3.2（三条不变式）/ §11.1（一期清单）：
 *   entity_surface           写法表，norm 主键全局唯一，surface 存原文（带路径就带全路径）
 *   entity_merge             合并审计，可回滚的唯一凭据
 *   entity_dedup_candidate   四期候选表（一期只建表，不产候选）
 * 另把 meta.entity_schema_version 抬到 2。
 *
 * 一期不重建 entities（旧 norm 列留着，三期切换时再按 §3.1 重建），
 * 所以这里一个字都不动 entities / memories。
 *
 * 全部 CREATE ... IF NOT EXISTS，可重复跑。
 *
 * 用法：
 *   node scripts/apply-entity-surface.mjs --dry-run   只打印 SQL 与将建的表/行数，不写库
 *   node scripts/apply-entity-surface.mjs --apply     真建
 *   node scripts/apply-entity-surface.mjs --verify    跑三条不变式断言，不过就退出码 1
 * 可选 --db /path/memory.db
 */
import { openDb, resolveDbPath } from './lib/db.mjs';

const argv = process.argv.slice(2);
const DB = argv.includes('--db') ? argv[argv.indexOf('--db') + 1] : resolveDbPath();
const MODE = argv.includes('--verify')
  ? 'verify'
  : argv.includes('--apply') || argv.includes('--rebuild-surface') ? 'apply' : 'dry-run';

/** 每条语句单独放，dry-run 能逐条打印。 */
const DDL = [
  // 1) 写法表：唯一真相。一种原文一行；同一 norm 可有多行，但只能指一个 entity_id
  //    （「一个 norm 只指一个实体」由 verify A1 与写入校验守，不靠主键压）。
  `CREATE TABLE IF NOT EXISTS entity_surface (
  surface    TEXT PRIMARY KEY,                   -- 原文本身唯一，带路径就带全路径
  norm       TEXT NOT NULL,                      -- 归一化查表键（路径不折叠，见 §3.2 不变式 3）
  entity_id  INTEGER NOT NULL,                   -- 0 = 未映射（flag='unmapped'）
  source     TEXT NOT NULL
             CHECK (source IN ('migrate','gate','coarse','extract','model','manual','text')),
  count      INTEGER NOT NULL DEFAULT 0,
  first_seen TEXT NOT NULL DEFAULT '',
  last_seen  TEXT NOT NULL DEFAULT '',
  flag       TEXT NOT NULL DEFAULT '',           -- ''|'path'|'basename_collapsed'|'needs_review'|'unmapped'
  UNIQUE (norm, surface)
) WITHOUT ROWID`,
  `CREATE INDEX IF NOT EXISTS idx_surface_norm ON entity_surface(norm)`,
  `CREATE INDEX IF NOT EXISTS idx_surface_entity ON entity_surface(entity_id, count DESC)`,

  // 2) 合并审计。
  `CREATE TABLE IF NOT EXISTS entity_merge (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  src_entity INTEGER NOT NULL,
  dst_entity INTEGER NOT NULL,
  reason     TEXT NOT NULL,                      -- rule:*|model|manual|fake_split
  evidence   TEXT NOT NULL DEFAULT '{}',
  run_id     TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  undone_at  TEXT NOT NULL DEFAULT ''
)`,
  `CREATE UNIQUE INDEX IF NOT EXISTS uq_entity_merge_src ON entity_merge(src_entity) WHERE undone_at=''`,

  // 3) 候选表（四期用，一期只建）。
  `CREATE TABLE IF NOT EXISTS entity_dedup_candidate (
  pair_key   TEXT PRIMARY KEY,                   -- sha1(min_id|max_id|bucket)
  a_entity   INTEGER NOT NULL,
  b_entity   INTEGER NOT NULL,
  bucket     TEXT NOT NULL,                      -- sep|dsh|plural|suffix|version|prefix|lang
  state      TEXT NOT NULL DEFAULT 'ready'
             CHECK (state IN ('ready','auto','judge','approved','rejected','merging','done','skipped')),
  score      REAL NOT NULL DEFAULT 0,
  evidence   TEXT NOT NULL DEFAULT '{}',
  attempts   INTEGER NOT NULL DEFAULT 0,
  last_error TEXT NOT NULL DEFAULT '',
  created_ts INTEGER NOT NULL,
  updated_ts INTEGER NOT NULL
)`,
  `CREATE INDEX IF NOT EXISTS idx_edc_claim ON entity_dedup_candidate(state, bucket)`,
];

const TABLES = ['entity_surface', 'entity_merge', 'entity_dedup_candidate'];

function tableExists(db, name) {
  return !!db.prepare(`SELECT 1 FROM sqlite_master WHERE type='table' AND name=?`).get(name);
}
function countOf(db, name) {
  if (!tableExists(db, name)) return null;
  return db.prepare(`SELECT count(*) c FROM ${name}`).get().c;
}
function metaValue(db, key) {
  const r = db.prepare(`SELECT value FROM meta WHERE key=?`).get(key);
  return r ? r.value : null;
}

/** 四条不变式（§3.2）。每条返回 {id, name, ok, detail, rows}。 */
export function verifySurface(db) {
  const out = [];
  const q = (sql, ...args) => db.prepare(sql).all(...args);

  // A1 一个 norm 只能指一个 entity_id（同 norm 多行 surface 允许，指向必须一致）。
  const a1 = q(`SELECT norm, count(DISTINCT entity_id) n FROM entity_surface
                GROUP BY norm HAVING n > 1`);
  out.push({
    id: 'A1',
    name: '一个 norm 只指一个 entity_id（同 norm 可有多行原文）',
    ok: a1.length === 0,
    rows: a1.length,
    detail: a1.length ? JSON.stringify(a1.slice(0, 20)) : 'ok',
  });

  // A2 surface 指向合法：实体存在、不指 merged 墓碑、archived 与 needs_review 双向对齐、unmapped 对齐。
  const a2 = [];
  a2.push(...q(`SELECT s.surface, s.entity_id FROM entity_surface s
                LEFT JOIN entities e ON e.id = s.entity_id
                WHERE s.entity_id <> 0 AND (e.id IS NULL OR e.status = 'merged')`));
  a2.push(...q(`SELECT s.surface, s.entity_id FROM entity_surface s
                JOIN entities e ON e.id = s.entity_id
                WHERE e.status = 'archived' AND s.flag <> 'needs_review'`));
  a2.push(...q(`SELECT s.surface, s.entity_id FROM entity_surface s
                JOIN entities e ON e.id = s.entity_id
                WHERE s.flag = 'needs_review' AND e.status <> 'archived'`));
  a2.push(...q(`SELECT s.surface, s.entity_id FROM entity_surface s
                WHERE (s.entity_id = 0) <> (s.flag = 'unmapped')`));
  out.push({
    id: 'A2',
    name: 'surface 指向有效实体（active；archived↔needs_review 双向；unmapped↔entity_id=0）',
    ok: a2.length === 0,
    rows: a2.length,
    detail: a2.length ? JSON.stringify(a2.slice(0, 10)) : 'ok',
  });

  // A3 memory_entity 只指 active 实体；mem_count 与挂载行数零漂移。
  const a3 = q(`SELECT me.memory_id, me.entity_id FROM memory_entity me
                LEFT JOIN entities e ON e.id = me.entity_id
                WHERE e.id IS NULL OR e.status = 'merged'`);
  const drift = q(`SELECT e.id, e.mem_count,
                          (SELECT count(*) FROM memory_entity me WHERE me.entity_id = e.id) real_count
                   FROM entities e
                   WHERE e.mem_count <> (SELECT count(*) FROM memory_entity me WHERE me.entity_id = e.id)`);
  out.push({
    id: 'A3',
    name: 'memory_entity 只指 active 实体，mem_count 零漂移',
    ok: a3.length === 0 && drift.length === 0,
    rows: a3.length + drift.length,
    detail: a3.length || drift.length ? JSON.stringify({ orphans: a3.slice(0, 10), drift: drift.slice(0, 10) }) : 'ok',
  });

  // A4 路径写法的 norm 必须含 /，禁止 basename 折叠（R8 废除）。
  const a4 = q(`SELECT norm, surface FROM entity_surface
                WHERE instr(surface, '/') > 0
                  AND surface NOT LIKE '@%'
                  AND instr(norm, '/') = 0`);
  const collapsed = q(`SELECT norm, surface FROM entity_surface
                       WHERE instr(surface, '/') > 0
                         AND norm = substr(surface, instr(surface, '/') + 1)`);
  out.push({
    id: 'A4',
    name: '路径写法 norm 保留全路径（含 /），无 basename 折叠',
    ok: a4.length === 0 && collapsed.length === 0,
    rows: a4.length + collapsed.length,
    detail: a4.length || collapsed.length ? JSON.stringify([...a4, ...collapsed].slice(0, 10)) : 'ok',
  });
  return out;
}

const db = openDb(DB);

/** entity_surface 的主键列；表不存在返回 null。 */
function surfacePkCols() {
  if (!tableExists(db, 'entity_surface')) return null;
  const idx = db.prepare(`PRAGMA index_list(entity_surface)`).all().filter((r) => r.origin === 'pk')[0];
  if (!idx) return [];
  return db.prepare(`PRAGMA index_info('${idx.name}')`).all().map((r) => r.name);
}

if (MODE === 'verify') {
  const missing = TABLES.filter((t) => !tableExists(db, t));
  if (missing.length) {
    console.error(JSON.stringify({ ok: false, missingTables: missing }));
    process.exit(1);
  }
  const results = verifySurface(db);
  const counts = Object.fromEntries(TABLES.map((t) => [t, countOf(db, t)]));
  const sources = db.prepare(`SELECT source, count(*) c FROM entity_surface GROUP BY source ORDER BY c DESC`).all();
  const flags = db.prepare(`SELECT flag, count(*) c FROM entity_surface GROUP BY flag ORDER BY c DESC`).all();
  const payload = {
    db: DB,
    ok: results.every((r) => r.ok),
    entitySchemaVersion: metaValue(db, 'entity_schema_version'),
    entities: countOf(db, 'entities'),
    surfacePk: surfacePkCols(),
    counts,
    surfaceBySource: sources,
    surfaceByFlag: flags,
    invariants: results,
  };
  console.log(JSON.stringify(payload, null, 2));
  db.close();
  process.exit(payload.ok ? 0 : 1);
}

if (MODE === 'dry-run') {
  const plan = DDL.map((sql) => ({
    sql: sql.replace(/\s+/g, ' ').slice(0, 120),
    table: (sql.match(/(?:TABLE|INDEX) IF NOT EXISTS (\w+)/) || [])[1],
    exists: tableExists(db, (sql.match(/(?:TABLE|INDEX) IF NOT EXISTS (\w+)/) || [])[1]),
    rows: countOf(db, (sql.match(/(?:TABLE|INDEX) IF NOT EXISTS (\w+)/) || [])[1]),
  }));
  console.log(JSON.stringify({
    db: DB,
    mode: 'dry-run',
    changes: 'none',
    surfacePk: surfacePkCols(),
    plan,
    tables: Object.fromEntries(TABLES.map((t) => [t, { exists: tableExists(db, t), rows: countOf(db, t) }])),
    meta: {
      entity_schema_version: { now: metaValue(db, 'entity_schema_version'), after: '2' },
    },
  }, null, 2));
  db.close();
  process.exit(0);
}

// --apply / --rebuild-surface：一个事务包住 DDL + meta，失败整体回滚。
const REBUILD = argv.includes('--rebuild-surface');
const pkNow = surfacePkCols();
if (pkNow && !(pkNow.length === 1 && pkNow[0] === 'surface') && !REBUILD) {
  console.error(JSON.stringify({
    ok: false,
    error: 'entity_surface 主键还是旧结构，不是 surface；先跑 --rebuild-surface（会 DROP 表，surface 行可由 migrate 脚本重生成）',
    surfacePk: pkNow,
  }));
  db.close();
  process.exit(1);
}
const before = Object.fromEntries(TABLES.map((t) => [t, countOf(db, t)]));
const beforeMeta = metaValue(db, 'entity_schema_version');
db.exec('PRAGMA busy_timeout = 10000');
db.exec('BEGIN IMMEDIATE');
try {
  if (REBUILD) db.exec('DROP TABLE IF EXISTS entity_surface');
  for (const sql of DDL) db.exec(sql);
  db.prepare(`INSERT INTO meta(key, value) VALUES ('entity_schema_version', '2')
              ON CONFLICT(key) DO UPDATE SET value = excluded.value`).run();
  db.exec('COMMIT');
} catch (err) {
  db.exec('ROLLBACK');
  console.error(JSON.stringify({ ok: false, error: String(err) }));
  db.close();
  process.exit(1);
}
const after = Object.fromEntries(TABLES.map((t) => [t, countOf(db, t)]));
console.log(JSON.stringify({
  db: DB,
  mode: REBUILD ? 'rebuild-surface' : 'apply',
  surfacePk: surfacePkCols(),
  tables: Object.fromEntries(TABLES.map((t) => [t, { before: before[t], after: after[t] }])),
  meta: { entity_schema_version: { before: beforeMeta, after: metaValue(db, 'entity_schema_version') } },
  entities: countOf(db, 'entities'),
}, null, 2));
db.close();
