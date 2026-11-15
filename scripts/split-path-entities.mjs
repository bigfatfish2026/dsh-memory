#!/usr/bin/env node
/**
 * 路径假合并拆解（二期第二票）。
 *
 * 背景：归一化以前的 R8 把路径取 basename，不同目录同名文件被折成一个实体。
 * 勘察票 把真跨目录的 13 组找全了；本脚本只拆证据足的 10 组：
 *   lib / bin / self / python3 / sessions / config / * / current / messages / mod.rs
 * 不拆 index.js / pkgconfig / 75（票面给了理由）；mod.rs 按「10 组」口径纳入，
 * 票面第 3 条又写它不动，两处冲突，报告里单独说明。
 *
 * 拆法：
 *   1) 原实体不动（不标 merged、不删），只把它 display 之外的某条写法改指新实体；
 *   2) 新实体 norm/display 都用那条写法本身（R21：路径保留全路径，~ 不展开）；
 *   3) 挂载只搬「正文/entities 里点名了这条写法」的，判定顺序：
 *        a. 原文与 surface 完全相等（大小写敏感）；
 *        b. 否则两边 expandPath（~ 展开、去尾斜杠、去 ENV= 前缀）后相等；
 *        c. 只认路径写法（surface 含 /），裸写法不参与拆分判定；
 *        命中的目标写法只有一个才搬，多义/裸词/无证据都留原实体并标 needs_review。
 *   4) needs_review 落 memory_entity.attrs（不落 entity_surface.flag，A2 会当场违例），
 *      形状是数组：attrs.needs_review = ["fake_split:<组>", ...]，同一条记忆被两组复核也不会互相覆盖。
 *   5) 拆解审计落 entity_merge：src=新实体、dst=原实体、reason='fake_split'，
 *      evidence 里存 group / keys / memory_ids / review_memory_ids，--undo 一条命令回环。
 *
 * 用法：
 *   node scripts/split-path-entities.mjs --dry-run [--db path]
 *   node scripts/split-path-entities.mjs --apply   [--db path] [--backup path]
 *   node scripts/split-path-entities.mjs --undo    [--db path]
 *   node scripts/split-path-entities.mjs --verify  [--db path]
 *
 * --apply 单事务，先备份（默认 memory/backup/memory-before-pathsplit-20261114.db）并写 .md5。
 * 重复 --apply 幂等：已有未 undo 的同组 fake_split 记录就跳过，不会再建实体。
 */
import { backup } from 'node:sqlite';
import { openDb, DEFAULT_DB_PATH, resolveMemoryFile } from './lib/db.mjs';
import { createHash } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import path from 'node:path';
import os from 'node:os';
import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const REAL_DB = DEFAULT_DB_PATH;
const DEFAULT_BACKUP = resolveMemoryFile('backup/memory-before-pathsplit-20261114.db');
const RUN_ID = 't-mwfw6ne2-3b15';
const HOME = os.homedir();

const argv = process.argv.slice(2);
const flag = (name) => argv.includes(name);
const opt = (name) => (argv.includes(name) ? argv[argv.indexOf(name) + 1] : null);
const MODE = flag('--apply') ? 'apply' : flag('--undo') ? 'undo' : flag('--verify') ? 'verify' : 'dry-run';
const DB = path.resolve(opt('--db') || REAL_DB);
const BACKUP_PATH = path.resolve(opt('--backup') || (DB === REAL_DB ? DEFAULT_BACKUP : `${DB}.before-pathsplit.db`));

/** 10 组：norm 定位原实体，surface 是要拆出去的那条写法。 */
const GROUPS = [
  { name: 'lib', norm: 'lib', expectId: 358, surface: '/opt/openssl-4.0.3/lib' },
  { name: 'bin', norm: 'bin', expectId: 349, surface: '/opt/rust/bin' },
  { name: 'self', norm: 'self', expectId: 1431, surface: '/api/log/self' },
  { name: 'python3', norm: 'python3', expectId: 167, surface: '/usr/bin/python3' },
  { name: 'sessions', norm: 'sessions', expectId: 17, surface: '~/fatfish/sbi/sky/notes/sessions/' },
  { name: 'config', norm: 'config', expectId: 432, surface: '~/.ssh/config' },
  { name: '*', norm: '*', expectId: 1499, surface: '/v1/*' },
  { name: 'current', norm: 'current', expectId: 619, surface: '/opt/llvm/current' },
  { name: 'messages', norm: 'messages', expectId: 851, surface: '/v2/groups/<openid>/messages' },
  { name: 'mod.rs', norm: 'mod.rs', expectId: 325, surface: 'src/viewmodel/mod.rs' },
];

const NOW = () => new Date().toISOString();
const s = (x) => String(x ?? '');

/** ~ 展开、去 ENV= 前缀、去尾斜杠；大小写不动（精确匹配优先）。 */
function expandPath(p) {
  let t = s(p).trim().replace(/^[A-Z][A-Z0-9_]*=/, '');
  if (t === '~') t = HOME;
  else if (t.startsWith('~/')) t = HOME + t.slice(1);
  return t.replace(/\/+$/, '');
}

const baseOf = (p) => {
  const x = s(p).replace(/\/+$/, '');
  return x.slice(x.lastIndexOf('/') + 1);
};

const TOKEN_SPLIT = /[\s，。、；：！？（）()【】\[\]{}"'`<>|—…·]+/u;
const TOKEN_TRAIL = /[.,:;!?，。；：！？、）】」》*]+$/u;

/**
 * 正文只取带 / 的 token（全路径才算证据）。raw 和去尾标点后的形式都留：
 * raw 用于和 surface 原文精确相等，trim 后用于展开比较（/api/* 这种尾 * 不被误砍）。
 */
function textTokens(text) {
  const out = [];
  for (const raw of s(text).split(TOKEN_SPLIT)) {
    if (!raw.includes('/')) continue;
    out.push(raw);
    const t = raw.replace(TOKEN_TRAIL, '');
    if (t && t !== raw && t.includes('/')) out.push(t);
  }
  return out;
}

/** surface 行 -> 归属目标键。路径写法按展开后的路径归族；裸写法单独一桶。 */
function targetKey(surface, norm) {
  return s(norm).includes('/') ? expandPath(norm) : `<bare:${s(norm)}>`;
}

function tableExists(db, name) {
  return !!db.prepare(`SELECT 1 FROM sqlite_master WHERE type='table' AND name=?`).get(name);
}

function requireTables(db) {
  for (const t of ['entities', 'entity_surface', 'memory_entity', 'memories', 'entity_merge']) {
    if (!tableExists(db, t)) throw new Error(`缺表 ${t}：${DB}`);
  }
}

function loadGroup(db, spec) {
  const old = db.prepare('SELECT id,norm,display,etype,mem_count,status FROM entities WHERE norm=?').get(spec.norm);
  if (!old) throw new Error(`找不到实体 norm=${spec.norm}`);
  if (Number(old.id) !== spec.expectId) {
    throw new Error(`实体 ${spec.norm} 的 id 变成 ${old.id}，票面写的是 ${spec.expectId}，先核库再跑`);
  }
  const rows = db.prepare('SELECT surface,norm,count,flag FROM entity_surface WHERE entity_id=? ORDER BY count DESC').all(old.id);
  const splitKey = expandPath(spec.surface);
  const splitRows = rows.filter((r) => expandPath(r.norm) === splitKey);
  const active = db.prepare(`
    SELECT id FROM entity_merge
    WHERE dst_entity=? AND reason='fake_split' AND undone_at='' AND json_extract(evidence,'$.group')=?`).get(old.id, spec.name);
  if (!splitRows.length && !active) throw new Error(`${spec.name} 组找不到要拆的写法 ${spec.surface}`);
  return { spec, old, rows, splitKey, splitRows, alreadySplit: !!active };
}

/** 逐条挂载判归属；只有唯一命中目标写法才算定。 */
function classify(db, g) {
  const mounts = db.prepare(`
    SELECT me.memory_id, me.status AS me_status, m.status AS mem_status, m.text, m.entities
    FROM memory_entity me JOIN memories m ON m.id = me.memory_id
    WHERE me.entity_id = ? ORDER BY me.memory_id`).all(g.old.id);

  const out = [];
  for (const m of mounts) {
    const tokens = new Set(textTokens(m.text));
    let raws = [];
    try { raws = JSON.parse(m.entities || '[]'); } catch { raws = []; }
    if (Array.isArray(raws)) for (const t of raws) if (typeof t === 'string' && t) tokens.add(t);

    const hits = new Set();
    for (const t of tokens) {
      // a) 原文精确相等优先
      const exact = g.rows.filter((r) => t === r.surface);
      if (exact.length) {
        for (const r of exact) hits.add(targetKey(r.surface, r.norm));
        continue;
      }
      // b) 展开后相等（~、尾斜杠、ENV= 前缀）
      const et = expandPath(t);
      for (const r of g.rows) if (expandPath(r.norm) === et) hits.add(targetKey(r.surface, r.norm));
    }

    let mode;
    let target = null;
    if (hits.size === 1) {
      mode = 'unique';
      target = [...hits][0];
    } else if (hits.size > 1) {
      mode = 'ambiguous';
    } else {
      const bareHit = [...tokens].some((t) =>
        g.rows.some((r) => baseOf(t).toLowerCase() === baseOf(r.norm).toLowerCase()));
      mode = bareHit ? 'bare' : 'none';
    }
    out.push({ memory_id: m.memory_id, me_status: m.me_status, mem_status: m.mem_status, mode, target });
  }

  const moved = out.filter((d) => d.target === g.splitKey);
  const review = out.filter((d) => d.target === null);
  const bucket = out.reduce((a, d) => ((a[d.mode] = (a[d.mode] || 0) + 1), a), {});
  return { mounts: out, moved, review, bucket, mountTotal: out.length };
}

function summarize(db) {
  return GROUPS.map((spec) => {
    const g = loadGroup(db, spec);
    const c = classify(db, g);
    return {
      ...g,
      ...c,
      afterOriginal: c.mountTotal - c.moved.length,
      canonical: spec.surface.replace(/\/+$/, ''),
    };
  });
}

function fmtGroup(g, width = 10) {
  const name = g.spec.name.padEnd(width);
  return `${name} ${String(g.old.id).padEnd(5)} 拆前=${String(g.mountTotal).padEnd(4)} 留原实体=${String(g.afterOriginal).padEnd(4)}` +
    `(复核 ${String(g.review.length).padEnd(3)}) 搬出=${String(g.moved.length).padEnd(3)}` +
    ` 新实体=${g.canonical} 挂载=${g.moved.length}` +
    `  [unique ${g.bucket.unique || 0}/amb ${g.bucket.ambiguous || 0}/bare ${g.bucket.bare || 0}/none ${g.bucket.none || 0}]`;
}

function md5File(file) {
  return createHash('md5').update(readFileSync(file)).digest('hex');
}

async function ensureBackup() {
  if (existsSync(BACKUP_PATH)) {
    const md5 = md5File(BACKUP_PATH);
    console.log(`备份已存在，不覆盖：${BACKUP_PATH}`);
    console.log(`md5 ${md5}`);
    return { path: BACKUP_PATH, md5, created: false };
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
  console.log(`备份：${BACKUP_PATH}`);
  console.log(`md5 ${md5}`);
  return { path: BACKUP_PATH, md5, created: true };
}

function dryRun() {
  const db = openDb(DB, { readOnly: true });
  try {
    requireTables(db);
    const beforeEntities = db.prepare('SELECT count(*) c FROM entities').get().c;
    const beforeMounts = db.prepare('SELECT count(*) c FROM memory_entity').get().c;
    const beforeSurface = db.prepare('SELECT count(*) c FROM entity_surface').get().c;
    const gs = summarize(db);
    const moved = gs.reduce((a, g) => a + g.moved.length, 0);
    const review = gs.reduce((a, g) => a + g.review.length, 0);
    const newEntities = gs.filter((g) => g.moved.length > 0).length;

    console.log(`=== dry-run（只读，不写库） ===`);
    console.log(`db ${DB}`);
    console.log(`拆前：实体 ${beforeEntities} / 挂载 ${beforeMounts} / surface ${beforeSurface} 行`);
    console.log(`组         原id  拆前挂载  留原实体(复核)      搬出   新实体`);
    for (const g of gs) console.log(fmtGroup(g));
    console.log(`拆后：实体 ${beforeEntities + newEntities} / 挂载 ${beforeMounts}（一条不丢）/ 搬出 ${moved} / needs_review ${review} 条`);
    console.log(`不拆：index.js(465，coarse 未改，87 条靠相对写法)、pkgconfig(1748，3 条证据不足)、75(1982，模式串不是路径)`);
    console.log(`改指 surface 行：${gs.reduce((a, g) => a + g.splitRows.length, 0)} 行；新实体 ${gs.map((g) => g.canonical).filter((x, i) => gs[i].moved.length > 0).join(' / ')}`);
  } finally {
    db.close();
  }
}

/** 在事务里逐组拆；返回统计。 */
function applyAll(db, gs) {
  const stats = { created: 0, skipped: 0, noop: 0, moved: 0, review: 0, groups: [] };
  for (const g of gs) {
    const name = g.spec.name;
    const existing = db.prepare(`
      SELECT id FROM entity_merge
      WHERE dst_entity=? AND reason='fake_split' AND undone_at='' AND json_extract(evidence,'$.group')=?`).get(g.old.id, name);
    if (existing) {
      stats.skipped++;
      stats.groups.push({ name, oldId: g.old.id, skipped: true, reason: '已有未 undo 的 fake_split 记录' });
      console.log(`[skip] ${name} 已拆过（entity_merge#${existing.id}），幂等跳过`);
      continue;
    }
    if (!g.moved.length) {
      stats.noop++;
      stats.groups.push({ name, oldId: g.old.id, skipped: true, reason: '没有可搬的挂载' });
      console.log(`[noop] ${name} 没有可搬挂载，不建空实体`);
      continue;
    }
    const canon = g.canonical;
    const clash = db.prepare('SELECT id FROM entities WHERE norm=?').get(canon);
    if (clash) throw new Error(`${name} 的新实体 norm=${canon} 已存在（id=${clash.id}），不硬撞`);

    const ts = NOW();
    const ins = db.prepare(`
      INSERT INTO entities(norm,display,etype,attrs,first_seen,last_seen,mem_count,status)
      VALUES(?,?,?,?,?,?,0,'active')`).run(
      canon, canon, g.old.etype,
      JSON.stringify({ split_from: g.old.id, fake_split: name, canonical: canon }),
      ts, ts,
    );
    const newId = Number(ins.lastInsertRowid);

    for (const r of g.splitRows) {
      const u = db.prepare('UPDATE entity_surface SET entity_id=? WHERE surface=?').run(newId, r.surface);
      if (u.changes !== 1) throw new Error(`${name} 改指 surface=${r.surface} 影响 ${u.changes} 行`);
    }

    for (const d of g.moved) {
      const insMe = db.prepare(`
        INSERT OR IGNORE INTO memory_entity(memory_id,entity_id,role,ts,status,attrs,version,prev_memory_id,origin)
        SELECT memory_id, ?, role, ts, status, attrs, version, prev_memory_id, origin
        FROM memory_entity WHERE entity_id=? AND memory_id=?`).run(newId, g.old.id, d.memory_id);
      if (insMe.changes !== 1 && !db.prepare('SELECT 1 FROM memory_entity WHERE entity_id=? AND memory_id=?').get(newId, d.memory_id)) {
        throw new Error(`${name} 搬挂载 ${d.memory_id} 失败（insert changes=${insMe.changes}）`);
      }
      const del = db.prepare('DELETE FROM memory_entity WHERE entity_id=? AND memory_id=?').run(g.old.id, d.memory_id);
      if (del.changes < 1) throw new Error(`${name} 删旧挂载 ${d.memory_id} 影响 0 行`);
    }

    for (const d of g.review) {
      const row = db.prepare('SELECT attrs FROM memory_entity WHERE entity_id=? AND memory_id=?').get(g.old.id, d.memory_id);
      if (!row) throw new Error(`${name} 复核挂载 ${d.memory_id} 不见了`);
      let attrs = {};
      try { attrs = JSON.parse(row.attrs || '{}') || {}; } catch { attrs = {}; }
      const marker = `fake_split:${name}`;
      const cur = attrs.needs_review;
      const arr = Array.isArray(cur) ? cur.slice() : (typeof cur === 'string' && cur ? [cur] : []);
      if (!arr.includes(marker)) arr.push(marker);
      attrs.needs_review = arr;
      db.prepare('UPDATE memory_entity SET attrs=? WHERE entity_id=? AND memory_id=?')
        .run(JSON.stringify(attrs), g.old.id, d.memory_id);
    }

    const recalc = db.prepare('UPDATE entities SET mem_count=(SELECT count(*) FROM memory_entity WHERE entity_id=?) WHERE id=?');
    recalc.run(g.old.id, g.old.id);
    recalc.run(newId, newId);

    const evidence = {
      group: name,
      old_id: g.old.id,
      new_id: newId,
      keys: g.splitRows.map((r) => r.surface),
      memories: g.splitRows.map((r) => r.norm),
      canonical: canon,
      memory_ids: g.moved.map((d) => d.memory_id),
      review_memory_ids: g.review.map((d) => d.memory_id),
      review_marker: `fake_split:${name}`,
      old_mem_count_before: g.mountTotal,
    };
    db.prepare(`
      INSERT INTO entity_merge(src_entity,dst_entity,reason,evidence,run_id,created_at)
      VALUES(?,?,'fake_split',?,?,?)`).run(newId, g.old.id, JSON.stringify(evidence), RUN_ID, ts);

    stats.created++;
    stats.moved += g.moved.length;
    stats.review += g.review.length;
    stats.groups.push({ name, oldId: g.old.id, newId, canonical: canon, moved: g.moved.length, review: g.review.length });
    console.log(`[split] ${name} ${g.old.id} -> 新实体 ${newId} ${canon}：搬 ${g.moved.length} / 复核 ${g.review.length}`);
  }
  return stats;
}

async function apply() {
  const backupInfo = await ensureBackup();
  const db = openDb(DB);
  db.exec('PRAGMA busy_timeout=10000');
  let stats;
  try {
    requireTables(db);
    db.exec('BEGIN IMMEDIATE');
    let gs;
    try {
      // 分类也在写锁内做：避免备份后、提交前新挂载/新记忆插进来造成判据漂移。
      gs = summarize(db);
      stats = applyAll(db, gs);
      db.exec('COMMIT');
    } catch (e) {
      db.exec('ROLLBACK');
      throw e;
    }
  } finally {
    db.close();
  }
  const after = openDb(DB, { readOnly: true });
  try {
    const entities = after.prepare('SELECT count(*) c FROM entities').get().c;
    const mounts = after.prepare('SELECT count(*) c FROM memory_entity').get().c;
    console.log(`应用完成：新实体 ${stats.created} 个 / 跳过 ${stats.skipped} 组 / 无挂载 ${stats.noop} 组 / 搬出 ${stats.moved} / 复核 ${stats.review}`);
    console.log(`库现在：实体 ${entities} / 挂载 ${mounts}`);
  } finally {
    after.close();
  }
  console.log(`备份 ${backupInfo.path} md5 ${backupInfo.md5}`);
  if (stats.created) console.log('下一步：node scripts/split-path-entities.mjs --verify');
}

function undo() {
  const db = openDb(DB);
  db.exec('PRAGMA busy_timeout=10000');
  db.exec('BEGIN IMMEDIATE');
  let n = 0;
  let restoredMounts = 0;
  try {
    requireTables(db);
    const rows = db.prepare(`
      SELECT * FROM entity_merge
      WHERE reason='fake_split' AND undone_at=''
      ORDER BY id DESC`).all();
    if (!rows.length) {
      console.log('没有未 undo 的 fake_split 记录，什么都不用做');
    }
    for (const r of rows) {
      const ev = JSON.parse(r.evidence || '{}');
      const src = Number(r.src_entity);
      const dst = Number(r.dst_entity);
      const dstRow = db.prepare('SELECT id FROM entities WHERE id=?').get(dst);
      if (!dstRow) throw new Error(`undo 目标原实体 ${dst} 不存在，停`);
      const srcRow = db.prepare('SELECT id FROM entities WHERE id=?').get(src);
      if (srcRow) {
        const ins = db.prepare(`
          INSERT OR IGNORE INTO memory_entity(memory_id,entity_id,role,ts,status,attrs,version,prev_memory_id,origin)
          SELECT memory_id, ?, role, ts, status, attrs, version, prev_memory_id, origin
          FROM memory_entity WHERE entity_id=?`).run(dst, src);
        restoredMounts += ins.changes;
        db.prepare('DELETE FROM memory_entity WHERE entity_id=?').run(src);
        db.prepare('UPDATE entity_surface SET entity_id=? WHERE entity_id=?').run(dst, src);
        db.prepare('DELETE FROM entities WHERE id=?').run(src);
      }
      const marker = ev.review_marker || `fake_split:${ev.group}`;
      for (const mid of Array.isArray(ev.review_memory_ids) ? ev.review_memory_ids : []) {
        const row = db.prepare('SELECT attrs FROM memory_entity WHERE entity_id=? AND memory_id=?').get(dst, mid);
        if (!row) continue;
        let attrs = {};
        try { attrs = JSON.parse(row.attrs || '{}') || {}; } catch { attrs = {}; }
        const cur = attrs.needs_review;
        const arr = (Array.isArray(cur) ? cur : (typeof cur === 'string' && cur ? [cur] : [])).filter((x) => x !== marker);
        if (arr.length) attrs.needs_review = arr;
        else delete attrs.needs_review;
        db.prepare('UPDATE memory_entity SET attrs=? WHERE entity_id=? AND memory_id=?')
          .run(JSON.stringify(attrs), dst, mid);
      }
      db.prepare('UPDATE entities SET mem_count=(SELECT count(*) FROM memory_entity WHERE entity_id=?) WHERE id=?').run(dst, dst);
      db.prepare('UPDATE entity_merge SET undone_at=? WHERE id=?').run(NOW(), r.id);
      n++;
      console.log(`[undo] 新实体 ${src} -> 原实体 ${dst}（${ev.group}）`);
    }
    db.exec('COMMIT');
  } catch (e) {
    db.exec('ROLLBACK');
    throw e;
  } finally {
    db.close();
  }
  const after = openDb(DB, { readOnly: true });
  try {
    const entities = after.prepare('SELECT count(*) c FROM entities').get().c;
    const mounts = after.prepare('SELECT count(*) c FROM memory_entity').get().c;
    const surface = after.prepare('SELECT count(*) c FROM entity_surface').get().c;
    const active = after.prepare(`SELECT count(*) c FROM entity_merge WHERE reason='fake_split' AND undone_at=''`).get().c;
    const marks = after.prepare(`SELECT count(*) c FROM memory_entity WHERE json_array_length(json_extract(attrs,'$.needs_review')) > 0`).get().c;
    console.log(`还原完成：undo ${n} 笔 / 回搬挂载 ${restoredMounts} 行`);
    console.log(`库现在：实体 ${entities} / 挂载 ${mounts} / surface ${surface} 行 / 未 undo 的 fake_split ${active} / needs_review 标记 ${marks}`);
  } finally {
    after.close();
  }
}

function verify() {
  const db = openDb(DB, { readOnly: true });
  const out = { db: DB, ok: true, invariants: [], extras: {}, counts: {} };
  try {
    requireTables(db);
    out.counts = {
      entities: db.prepare('SELECT count(*) c FROM entities').get().c,
      memory_entity: db.prepare('SELECT count(*) c FROM memory_entity').get().c,
      entity_surface: db.prepare('SELECT count(*) c FROM entity_surface').get().c,
      entity_merge_fake_split: db.prepare(`SELECT count(*) c FROM entity_merge WHERE reason='fake_split'`).get().c,
      entity_merge_fake_split_active: db.prepare(`SELECT count(*) c FROM entity_merge WHERE reason='fake_split' AND undone_at=''`).get().c,
    };

    // A1-A4 直接复用一期校验脚本，口径只有一份。
    const r = spawnSync(process.execPath, [path.join(HERE, 'apply-entity-surface.mjs'), '--verify', '--db', DB], { encoding: 'utf8' });
    let base = null;
    try { base = JSON.parse(r.stdout); } catch { /* 下面按失败处理 */ }
    if (!base || !Array.isArray(base.invariants)) {
      out.ok = false;
      out.invariants.push({ id: 'A1-A4', ok: false, rows: -1, detail: `校验脚本没回 JSON：${(r.stderr || r.stdout || '').slice(0, 300)}` });
    } else {
      for (const it of base.invariants) out.invariants.push({ id: it.id, name: it.name, ok: it.ok, rows: it.rows, detail: it.detail });
    }

    // A5 每条挂载都有对应 surface 行（只要求该实体至少有一行 surface，且新实体有拆出去的 surface）。
    const missing = db.prepare(`
      SELECT count(*) c FROM (SELECT DISTINCT entity_id FROM memory_entity) me
      LEFT JOIN entity_surface s ON s.entity_id = me.entity_id
      WHERE s.entity_id IS NULL`).get().c;
    const activeSplits = db.prepare(`
      SELECT src_entity,dst_entity,evidence FROM entity_merge
      WHERE reason='fake_split' AND undone_at=''`).all();
    const noSurfaceNew = activeSplits.filter((x) =>
      !db.prepare('SELECT 1 FROM entity_surface WHERE entity_id=?').get(x.src_entity)).length;
    out.invariants.push({
      id: 'A5',
      name: '每条挂载的实体都有 entity_surface 行；每个拆出的新实体都有 surface',
      ok: missing === 0 && noSurfaceNew === 0,
      rows: missing + noSurfaceNew,
      detail: missing === 0 && noSurfaceNew === 0 ? 'ok' : JSON.stringify({ entitiesWithoutSurface: missing, newEntitiesWithoutSurface: noSurfaceNew }),
    });
    if (missing || noSurfaceNew) out.ok = false;

    // A6 本次拆出去的 surface.count 与 memories.entities 原文命中数一致。
    // 只校验拆解改过的行：原实体其他写法的 count 漂移是历史基线（本脚本不碰），单独报数。
    const occ = new Map();
    for (const m of db.prepare('SELECT entities FROM memories').iterate()) {
      let arr = [];
      try { arr = JSON.parse(m.entities || '[]'); } catch { arr = []; }
      if (!Array.isArray(arr)) continue;
      const seen = new Set();
      for (const t of arr) {
        if (typeof t !== 'string' || !t || seen.has(t)) continue;
        seen.add(t);
        occ.set(t, (occ.get(t) || 0) + 1);
      }
    }
    const scopeKeys = new Set();
    for (const x of activeSplits) {
      const ev = JSON.parse(x.evidence || '{}');
      for (const k of Array.isArray(ev.keys) ? ev.keys : []) scopeKeys.add(k);
    }
    // 没有拆解记录时（比如 undo 后）按票面 10 条目标写法校验，避免空集直接过。
    if (!scopeKeys.size) for (const spec of GROUPS) scopeKeys.add(spec.surface);
    const scoped = db.prepare(`SELECT surface,count,entity_id FROM entity_surface
      WHERE surface IN (${[...scopeKeys].map(() => '?').join(',')})`).all(...scopeKeys);
    const drift = scoped
      .map((x) => ({ surface: x.surface, count: x.count, actual: occ.get(x.surface) || 0, entity_id: x.entity_id }))
      .filter((x) => x.count !== x.actual);
    // 全库漂移只报数，不判失败：本次没动那 11 行，属于基线遗留。
    let globalDrift = 0;
    for (const x of db.prepare('SELECT surface,count FROM entity_surface').iterate()) {
      if ((occ.get(x.surface) || 0) !== x.count) globalDrift++;
    }
    out.invariants.push({
      id: 'A6',
      name: '拆解涉及 surface 的 count = memories.entities 原文命中数',
      ok: drift.length === 0,
      rows: drift.length,
      detail: drift.length === 0 ? `ok（scope ${scoped.length} 行；全库基线漂移 ${globalDrift} 行，未动）` : JSON.stringify(drift.slice(0, 20)),
    });
    if (drift.length) out.ok = false;

    out.extras = {
      globalSurfaceCountDrift: globalDrift,
      scopedSurfaceRows: scoped.length,
      newEntities: activeSplits.map((x) => ({ new: x.src_entity, old: x.dst_entity, group: JSON.parse(x.evidence || '{}').group })),
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

try {
  if (MODE === 'dry-run') dryRun();
  else if (MODE === 'apply') await apply();
  else if (MODE === 'undo') undo();
  else if (MODE === 'verify') verify();
} catch (e) {
  console.error(`失败：${String((e && e.stack) || e)}`);
  process.exit(1);
}
