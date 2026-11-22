#!/usr/bin/env node
/**
 * P1：实体按关联迁移到路径级（v3 方案 §4）。
 *
 * 做法：一条关联（memory_id）只有当正文/entities 原文里能抽出**唯一**的
 * `/plugins/<repo>/<相对路径>` 证据时才迁；目标键 `<repo>/<相对路径>` 只用于分组，
 * 实体 norm / display / surface 一律写展开后的完整路径（R21，不取 basename）。
 * 抽不出唯一仓的关联不迁，按批在 memory_entity.attrs.needs_review 留
 * `path_split:<run_id>`，undo 按账本精确摘掉。
 *
 * 四张表各动什么：
 *   entities        新建目标实体（norm=完整路径，attrs={split_from, path_split:<run>, canonical, key}）；
 *                   已有同 norm 实体时不硬撞，整组关联标 review（单独记账，见 review_only）
 *   entity_surface  已有对应 surface 的（index.js 的 dsh-home 那条）UPDATE entity_id 改指；
 *                   没有的 INSERT，source='migrate'（合法值，见 scripts/lib/path-split.mjs 文件头 2)），
 *                   flag='path'，count=memories.entities 里该原文的出现数
 *   memory_entity   逐条 (memory_id, entity_id, role) 从旧实体搬到新实体，role/ts/status/attrs 原样带
 *   entity_merge    一条目标实体一条账本：reason='path_split'、run_id=本票号、
 *                   evidence 记 mounts（memory_id+role）、surfaces.{updated,inserted}、review_memory_ids
 *
 * undo：只认未 undo 的 path_split 记录，按上面记的精确键逐项恢复；目标实体后来新增的
 * 关联不搬回（它们不在 evidence.mounts 里）。
 *
 * 用法：
 *   node scripts/migrate-entity-paths.mjs --dry-run  [--db path] [--entity 465[,id]] [--run-id x]
 *   node scripts/migrate-entity-paths.mjs --apply    --db <副本> --run-id <票号> [--entity ...] [--backup path]
 *   node scripts/migrate-entity-paths.mjs --undo     --db <副本> [--run-id <票号>]
 *   node scripts/migrate-entity-paths.mjs --verify   [--db path] [--run-id x]
 *   node scripts/migrate-entity-paths.mjs --snapshot [--db path] [--run-id x]
 * --apply / --undo 默认拒绝写生产库（DEFAULT_DB_PATH 或 DSH_MEMORY_DB 指的库），要写必须再给 --allow-prod。
 * --dry-run / --verify / --snapshot 只读开库。
 */
import { backup } from 'node:sqlite';
import { createHash } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { openDb, DEFAULT_DB_PATH, resolveDbPath, resolveMemoryFile } from './lib/db.mjs';
import {
  PATH_SURFACE_SOURCE, SURFACE_SOURCES, requireTables, classifyByPathEvidence,
  surfaceOccurrences, applyBatch, undoBatch, markReviewOnly, nowIso,
} from './lib/path-split.mjs';

const REASON = 'path_split';
const DEFAULT_RUN_ID = 't-mwohffhl-1mk4';

const argv = process.argv.slice(2);
const flag = (name) => argv.includes(name);
const opt = (name) => (argv.includes(name) ? argv[argv.indexOf(name) + 1] : null);
const MODE = flag('--apply') ? 'apply'
  : flag('--undo') ? 'undo'
    : flag('--verify') ? 'verify'
      : flag('--snapshot') ? 'snapshot' : 'dry-run';
const DB = path.resolve(opt('--db') || resolveDbPath());
const RUN_ID = opt('--run-id') || DEFAULT_RUN_ID;
const BACKUP_PATH = path.resolve(opt('--backup') || (DB === DEFAULT_DB_PATH
  ? resolveMemoryFile(`backup/memory-before-pathmigrate-${RUN_ID}.db`)
  : `${DB}.before-pathmigrate.db`));

/** 生产库清单：跟 cwd 走的默认库 + DSH_MEMORY_DB 指的库；不写死任何本机路径。 */
const PROD_DBS = new Set([
  path.resolve(DEFAULT_DB_PATH),
  path.resolve(resolveDbPath()),
]);

function assertWritableTarget() {
  if (!flag('--db')) {
    throw new Error(`--${MODE} 必须显式给 --db（先演练副本），别落到默认路径`);
  }
  if (PROD_DBS.has(DB) && !flag('--allow-prod')) {
    throw new Error(`--${MODE} 指向生产库 ${DB}；迁移要先在副本上演练，确需写生产加 --allow-prod`);
  }
}

const md5File = (file) => createHash('md5').update(readFileSync(file)).digest('hex');

async function ensureBackup() {
  if (existsSync(BACKUP_PATH)) {
    console.log(`备份已存在，不覆盖：${BACKUP_PATH}`);
    return { path: BACKUP_PATH, md5: md5File(BACKUP_PATH), created: false };
  }
  mkdirSync(path.dirname(BACKUP_PATH), { recursive: true });
  const src = openDb(DB, { readOnly: true });
  try {
    await backup(src, BACKUP_PATH);
  } finally {
    src.close();
  }
  const md5 = md5File(BACKUP_PATH);
  writeFileSync(`${BACKUP_PATH}.md5`, `${md5}  ${path.basename(BACKUP_PATH)}\n`);
  return { path: BACKUP_PATH, md5, created: true };
}

/** 候选实体：显式 --entity 优先；否则裸文件名实体（norm 无 /、etype=file）里正文含 /plugins/ 的。 */
function candidateEntityIds(db) {
  const raw = opt('--entity');
  if (raw) {
    return [...new Set(String(raw).split(',').map((x) => Number(String(x).trim())).filter((x) => Number.isFinite(x) && x > 0))];
  }
  return db.prepare(`
    SELECT DISTINCT e.id FROM entities e
    JOIN memory_entity me ON me.entity_id = e.id
    JOIN memories m ON m.id = me.memory_id
    WHERE e.etype='file' AND instr(e.norm,'/')=0 AND instr(m.text,'/plugins/')>0
    ORDER BY e.id`).all().map((r) => Number(r.id));
}

/**
 * 为一个旧实体生成迁移计划。groups 只放 unique 且无 norm/surface 冲突的目标；
 * 其余（ambiguous / none / 冲突）进 review，跟第一个 group 一起记账。
 */
function buildEntityPlan(db, entityId, occurrences) {
  const ent = db.prepare('SELECT id,norm,display,etype FROM entities WHERE id=?').get(entityId);
  if (!ent) throw new Error(`实体 ${entityId} 不存在`);
  const c = classifyByPathEvidence(db, { entityId, mustBasename: ent.norm });
  const groups = new Map();
  const review = [];
  for (const m of c.mounts) {
    if (m.mode !== 'unique') {
      review.push({ memoryId: m.memoryId, role: m.role, mode: m.mode, keys: m.keys });
      continue;
    }
    const normClash = db.prepare('SELECT entity_id FROM entity_surface WHERE norm=? AND surface<>?').get(m.canonical, m.canonical);
    if (normClash && Number(normClash.entity_id) !== Number(entityId)) {
      review.push({ memoryId: m.memoryId, role: m.role, mode: 'norm_conflict', conflictEntity: Number(normClash.entity_id) });
      continue;
    }
    const surfaceClash = db.prepare('SELECT entity_id FROM entity_surface WHERE surface=?').get(m.canonical);
    if (surfaceClash && Number(surfaceClash.entity_id) !== Number(entityId)) {
      review.push({ memoryId: m.memoryId, role: m.role, mode: 'surface_conflict', conflictEntity: Number(surfaceClash.entity_id) });
      continue;
    }
    if (!groups.has(m.key)) {
      groups.set(m.key, {
        key: m.key, canonical: m.canonical, etype: ent.etype, moved: [], review: [],
        surfaces: [surfaceClash
          ? { kind: 'update', surface: m.canonical }
          : {
            kind: 'insert', surface: m.canonical, norm: m.canonical,
            count: occurrences.get(m.canonical) || 0, flag: 'path',
          }],
      });
    }
    groups.get(m.key).moved.push({ memoryId: m.memoryId, role: m.role });
  }
  return { ent, classify: c, groups: [...groups.values()], review };
}

function buildPlan(db, entityIds) {
  const occurrences = surfaceOccurrences(db, collectCandidateSurfaces(db, entityIds));
  const plans = [];
  for (const id of entityIds) {
    const p = buildEntityPlan(db, id, occurrences);
    plans.push(p);
  }
  return plans;
}

/** 先粗扫一遍候选 surface 键，供 count 统计一次走完。 */
function collectCandidateSurfaces(db, entityIds) {
  const keys = new Set();
  for (const id of entityIds) {
    const ent = db.prepare('SELECT norm FROM entities WHERE id=?').get(id);
    if (!ent) continue;
    for (const m of classifyByPathEvidence(db, { entityId: id, mustBasename: ent.norm }).mounts) {
      if (m.mode === 'unique' && m.canonical) keys.add(m.canonical);
    }
  }
  return [...keys];
}

function printPlan(plans) {
  let moved = 0; let review = 0; let newEntities = 0; let newSurfaces = 0; let updSurfaces = 0;
  const entityRows = [];
  for (const p of plans) {
    const entityMoved = p.groups.reduce((a, g) => a + g.moved.length, 0);
    if (!entityMoved) {
      entityRows.push({ id: p.ent.id, norm: p.ent.norm, groups: 0, moved: 0, review: p.review.length, detail: p.classify.bucket, skip: '没有唯一证据，不迁也不留痕' });
      continue;
    }
    moved += entityMoved;
    review += p.review.length;
    newEntities += p.groups.length;
    for (const g of p.groups) for (const sp of g.surfaces) (sp.kind === 'insert' ? newSurfaces++ : updSurfaces++);
    entityRows.push({
      id: p.ent.id, norm: p.ent.norm, groups: p.groups.length, moved: entityMoved,
      review: p.review.length, detail: p.classify.bucket,
      groupsDetail: p.groups.map((g) => ({ key: g.key, canonical: g.canonical, moved: g.moved.length, surfaces: g.surfaces.map((x) => x.kind) })),
    });
  }
  console.log(`=== dry-run（只读，不写库） ===`);
  console.log(`db ${DB}`);
  console.log(`run_id ${RUN_ID} / reason ${REASON} / surface source ${PATH_SURFACE_SOURCE}`);
  console.log(`规则：只迁唯一 /plugins/<repo>/<rel> 证据；多义、无证据、norm/surface 冲突一律 needs_review，不迁`);
  console.log(`可唯一迁：关联 ${moved} 条 / 涉及实体 ${entityRows.filter((x) => x.moved).length} 个 / 目标实体 ${newEntities} 个 / 新 surface ${newSurfaces} 行 / 改指 surface ${updSurfaces} 行 / needs_review ${review} 条`);
  console.log(`实体明细：`);
  for (const r of entityRows) {
    const head = `  #${String(r.id).padEnd(5)} ${String(r.norm).padEnd(22)} 目标 ${String(r.groups).padEnd(3)} 迁 ${String(r.moved).padEnd(3)} 复核 ${String(r.review).padEnd(4)}`;
    console.log(`${head} [unique ${r.detail.unique || 0}/amb ${r.detail.ambiguous || 0}/none ${r.detail.none || 0}]${r.skip ? ` —— ${r.skip}` : ''}`);
    for (const g of r.groupsDetail || []) {
      console.log(`        -> ${g.key}  （${g.canonical}，迁 ${g.moved} 条，surface ${g.surfaces.join('+')}）`);
    }
  }
  return { moved, review, newEntities, newSurfaces, updSurfaces, entities: entityRows };
}

async function apply() {
  assertWritableTarget();
  const backupInfo = await ensureBackup();
  const db = openDb(DB);
  db.exec('PRAGMA busy_timeout=10000');
  const stats = { created: 0, skipped: 0, moved: 0, review: 0, reviewOnly: [], groups: [] };
  try {
    requireTables(db);
    db.exec('BEGIN IMMEDIATE');
    try {
      const plans = buildPlan(db, candidateEntityIds(db));
      for (const p of plans) {
        if (!p.groups.length) continue;
        const groups = p.groups.map((g, i) => ({
          ...g,
          entityAttrs: { [REASON]: RUN_ID, key: g.key, old_norm: p.ent.norm },
          evidence: { group: `${p.ent.norm}->${g.key}`, old_norm: p.ent.norm, old_mem_count_before: p.classify.mountTotal },
          reviewMarker: `${REASON}:${RUN_ID}`,
          // 复核标记只挂一次：挂到第一个组上，undo 按那一条账本摘。
          review: i === 0 ? p.review : [],
        }));
        const r = applyBatch(db, {
          reason: REASON, runId: RUN_ID, source: PATH_SURFACE_SOURCE, oldEntityId: p.ent.id, groups,
        });
        stats.created += r.created;
        stats.skipped += r.skipped;
        stats.moved += r.moved;
        stats.review += r.review;
        for (const g of r.groups) {
          stats.groups.push({ entity: p.ent.id, norm: p.ent.norm, ...g });
          if (g.skipped) console.log(`[skip] #${p.ent.id} ${p.ent.norm} -> ${g.key}：${g.why}`);
          else console.log(`[path] #${p.ent.id} ${p.ent.norm} -> 新实体 ${g.newId} ${g.canonical}：搬 ${g.moved} / 复核 ${g.review} / surface 改指 ${g.surfaces.updated} 库内新增 ${g.surfaces.inserted}`);
        }
        // 目标实体已存在的组：单独 review_only 账本，undo 能摘掉标记。
        const existing = r.groups.filter((g) => g.skipped && g.existingId);
        const mounts = existing.flatMap((g) => g.mounts || []);
        if (mounts.length) {
          const m = markReviewOnly(db, {
            reason: REASON, runId: RUN_ID, entityId: p.ent.id, mounts,
            marker: `${REASON}:exists:${RUN_ID}`,
            evidence: { group: `${p.ent.norm}->exists`, existing_targets: existing.map((g) => g.existingId) },
          });
          stats.reviewOnly.push({ entity: p.ent.id, mounts: mounts.length, ok: m.ok, error: m.error });
          stats.review += mounts.length;
          console.log(`${m.ok ? '[review]' : '[review!]'} #${p.ent.id} ${p.ent.norm} 目标实体已存在 ${mounts.length} 条关联留 needs_review${m.ok ? '' : `（账本写不进：${m.error}）`}`);
        }
      }
      db.exec('COMMIT');
    } catch (e) {
      db.exec('ROLLBACK');
      throw e;
    }
  } finally {
    db.close();
  }
  console.log(`应用完成：新实体 ${stats.created} / 跳过组 ${stats.skipped} / 搬关联 ${stats.moved} / 复核 ${stats.review}`);
  console.log(`备份 ${backupInfo.path} md5 ${backupInfo.md5}`);
  console.log('下一步：--verify，再 --undo 演练回滚');
}

function undo() {
  assertWritableTarget();
  const db = openDb(DB);
  db.exec('PRAGMA busy_timeout=10000');
  let stats;
  try {
    requireTables(db);
    db.exec('BEGIN IMMEDIATE');
    try {
      stats = undoBatch(db, { reason: REASON, runId: opt('--run-id') ? RUN_ID : null });
      db.exec('COMMIT');
    } catch (e) {
      db.exec('ROLLBACK');
      throw e;
    }
  } finally {
    db.close();
  }
  for (const r of stats.records) console.log(`[undo] 新实体 ${r.src} -> 原实体 ${r.dst}（${r.group}）`);
  console.log(`还原完成：undo ${stats.undone} 笔 / 回搬关联 ${stats.restoredMounts} 行 / surface 改回 ${stats.surfacesBack} 行 / 删本批 surface ${stats.surfacesDeleted} 行 / 删新实体 ${stats.deletedEntities} 个`);
  if (stats.keptEntities.length) console.log(`保留的实体（还有本批之外的挂载/surface）：${JSON.stringify(stats.keptEntities)}`);
}

function verify() {
  const db = openDb(DB, { readOnly: true });
  const out = { db: DB, runId: opt('--run-id') || null, ok: true, checks: [] };
  const push = (id, name, ok, rows, detail) => {
    out.checks.push({ id, name, ok, rows, detail });
    if (!ok) out.ok = false;
  };
  try {
    requireTables(db);
    const active = db.prepare(`SELECT * FROM entity_merge WHERE reason=? AND undone_at=''${opt('--run-id') ? ' AND run_id=?' : ''}`)
      .all(...(opt('--run-id') ? [REASON, RUN_ID] : [REASON]));
    const ev = active.map((r) => ({ row: r, ev: JSON.parse(r.evidence || '{}') }));
    const created = ev.filter((x) => !x.ev.review_only && x.ev.new_id);
    const reviewOnly = ev.filter((x) => x.ev.review_only);

    const badSource = db.prepare(`SELECT count(*) c FROM entity_surface WHERE source NOT IN (${SURFACE_SOURCES.map(() => '?').join(',')})`).get(...SURFACE_SOURCES).c;
    push('V1', 'source 全在 CHECK 合法值里', badSource === 0, badSource, badSource ? '有非法 source' : 'ok');

    const movedSurfaces = created.flatMap((x) => (x.ev.surfaces?.updated || []).map((sf) => ({ sf, newId: Number(x.ev.new_id) })));
    const insertedSurfaces = created.flatMap((x) => (x.ev.surfaces?.inserted || []).map((sf) => ({ sf, newId: Number(x.ev.new_id) })));
    const wrongOwner = [...movedSurfaces, ...insertedSurfaces]
      .filter(({ sf, newId }) => {
        const row = db.prepare('SELECT entity_id FROM entity_surface WHERE surface=?').get(sf);
        return !row || Number(row.entity_id) !== newId;
      });
    push('V2', '本批 surface（改指+新增）都指到新实体', wrongOwner.length === 0, wrongOwner.length, wrongOwner.length ? JSON.stringify(wrongOwner.slice(0, 10)) : 'ok');

    const noSurface = created.filter((x) => !db.prepare('SELECT 1 FROM entity_surface WHERE entity_id=?').get(Number(x.ev.new_id)));
    push('V3', '每个新建实体至少一行 surface', noSurface.length === 0, noSurface.length, noSurface.length ? JSON.stringify(noSurface.map((x) => x.ev.new_id)) : 'ok');

    const occ = surfaceOccurrences(db, insertedSurfaces.map((x) => x.sf));
    const drift = insertedSurfaces
      .map(({ sf, newId }) => {
        const row = db.prepare('SELECT count, entity_id FROM entity_surface WHERE surface=?').get(sf);
        return { surface: sf, count: row?.count, actual: occ.get(sf) || 0, entity_id: row?.entity_id, newId };
      })
      .filter((x) => x.count !== x.actual || Number(x.entity_id) !== x.newId);
    push('V4', '本批新增 surface 的 count = memories.entities 原文命中数', drift.length === 0, drift.length, drift.length ? JSON.stringify(drift.slice(0, 10)) : 'ok');

    const memCountWrong = [];
    const touched = new Set([...created.map((x) => Number(x.ev.new_id)), ...created.map((x) => Number(x.ev.old_id))]);
    for (const id of touched) {
      const e = db.prepare('SELECT mem_count FROM entities WHERE id=?').get(id);
      const actual = db.prepare('SELECT count(*) c FROM memory_entity WHERE entity_id=?').get(id).c;
      if (!e || e.mem_count !== actual) memCountWrong.push({ id, mem_count: e?.mem_count, actual });
    }
    push('V5', '本批新旧实体 mem_count = memory_entity 实数', memCountWrong.length === 0, memCountWrong.length, memCountWrong.length ? JSON.stringify(memCountWrong) : 'ok');

    const dupNorm = db.prepare(`SELECT norm, count(DISTINCT entity_id) n FROM entity_surface GROUP BY norm HAVING n > 1`).all();
    push('V6', '同一 surface norm 只指一个实体（A1）', dupNorm.length === 0, dupNorm.length, dupNorm.length ? JSON.stringify(dupNorm.slice(0, 5)) : 'ok');

    const reviewMarked = db.prepare(`SELECT count(*) c FROM memory_entity WHERE json_array_length(json_extract(attrs,'$.needs_review')) > 0`).get().c;
    out.counts = {
      active_path_split: active.length, created_entities: created.length, review_only: reviewOnly.length,
      needs_review_rows: reviewMarked,
      entities: db.prepare('SELECT count(*) c FROM entities').get().c,
      memory_entity: db.prepare('SELECT count(*) c FROM memory_entity').get().c,
      entity_surface: db.prepare('SELECT count(*) c FROM entity_surface').get().c,
      entity_merge: db.prepare('SELECT count(*) c FROM entity_merge').get().c,
    };
  } catch (e) {
    out.ok = false;
    out.error = String((e && e.stack) || e);
  } finally {
    db.close();
  }
  console.log(JSON.stringify(out, null, 2));
  process.exit(out.ok ? 0 : 1);
}

/** 四张表行数 + 关键字段摘要（复盘零差异用）。 */
function snapshot() {
  const db = openDb(DB, { readOnly: true });
  try {
    requireTables(db);
    const digest = (sql) => createHash('sha256').update(JSON.stringify(db.prepare(sql).all())).digest('hex');
    const out = {
      db: DB,
      counts: {
        entities: db.prepare('SELECT count(*) c FROM entities').get().c,
        memory_entity: db.prepare('SELECT count(*) c FROM memory_entity').get().c,
        entity_surface: db.prepare('SELECT count(*) c FROM entity_surface').get().c,
        entity_merge: db.prepare('SELECT count(*) c FROM entity_merge').get().c,
        path_split_active: db.prepare(`SELECT count(*) c FROM entity_merge WHERE reason=? AND undone_at=''`).get(REASON).c,
        needs_review_rows: db.prepare(`SELECT count(*) c FROM memory_entity WHERE json_array_length(json_extract(attrs,'$.needs_review'))>0`).get().c,
      },
      digests: {
        entities: digest('SELECT id,norm,display,etype,aliases_note,attrs,first_seen,last_seen,mem_count,status FROM entities ORDER BY id'),
        memory_entity: digest('SELECT memory_id,entity_id,role,ts,status,attrs,version,prev_memory_id,origin FROM memory_entity ORDER BY memory_id,entity_id,role'),
        entity_surface: digest('SELECT surface,norm,entity_id,source,count,first_seen,last_seen,flag FROM entity_surface ORDER BY surface'),
        entity_merge: digest('SELECT id,src_entity,dst_entity,reason,run_id,created_at,undone_at,evidence FROM entity_merge ORDER BY id'),
      },
    };
    console.log(JSON.stringify(out, null, 2));
  } finally {
    db.close();
  }
}

try {
  if (MODE === 'dry-run') {
    const db = openDb(DB, { readOnly: true });
    try {
      requireTables(db);
      printPlan(buildPlan(db, candidateEntityIds(db)));
    } finally {
      db.close();
    }
  } else if (MODE === 'apply') await apply();
  else if (MODE === 'undo') undo();
  else if (MODE === 'verify') verify();
  else if (MODE === 'snapshot') snapshot();
} catch (e) {
  console.error(`失败：${String((e && e.stack) || e)}`);
  process.exit(1);
}
