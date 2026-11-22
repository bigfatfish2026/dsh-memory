#!/usr/bin/env node
/**
 * 迁移：memories.entities（TEXT JSON 数组）→ entities / memory_entity（影子表）。
 *
 * 纯搬运，不改 memories 一个字：旧列继续照原样用，这里只写新表。
 * 每批 500 条一个事务，容忍库在跑（并发新增只影响下一轮增量）；重跑幂等。
 *
 * 用法：
 *   node scripts/migrate-entities.mjs                 # 全量一轮
 *   node scripts/migrate-entities.mjs --incremental   # 再扫一轮，只补新增（幂等）
 *   node scripts/migrate-entities.mjs --db /path.db --batch 500
 *
 * 对账口径：
 *   jsonElems  非空 JSON 元素总数
 *   pairCount  同条内按 norm 去重后的对数（memory_entity 期望行数）
 *   rows       实际写入/已存在的 memory_entity 行数
 *   dupWithin  同条同 norm 被折叠掉的元素数（版本剥离后同一条记忆两个写法撞同一实体）
 *   empty      空名元素数（不进表）
 */
import { openDb, resolveDbPath } from './lib/db.mjs';
import { normalizeEntity } from '../lib/shared/entity-normalize.js';

const argv = process.argv.slice(2);
const argOf = (k, d) => (argv.includes(k) ? argv[argv.indexOf(k) + 1] : d);
const DB = argOf('--db', resolveDbPath());
const BATCH = Number(argOf('--batch', '500'));
const INCREMENTAL = argv.includes('--incremental');

const db = openDb(DB);
db.exec('PRAGMA busy_timeout = 15000; PRAGMA journal_mode = WAL;');

const sel = db.prepare(`SELECT rowid AS rid, id, text, entities, ts, status FROM memories
  WHERE rowid > ? ORDER BY rowid LIMIT ?`);
const insEntity = db.prepare(`INSERT INTO entities(norm, display, etype, attrs, first_seen, last_seen, mem_count, status)
  VALUES (?, ?, ?, ?, ?, ?, 0, 'active')
  ON CONFLICT(norm) DO UPDATE SET
    first_seen = CASE WHEN excluded.first_seen < entities.first_seen OR entities.first_seen = ''
                      THEN excluded.first_seen ELSE entities.first_seen END,
    last_seen  = CASE WHEN excluded.last_seen > entities.last_seen THEN excluded.last_seen
                      ELSE entities.last_seen END,
    attrs      = CASE WHEN entities.attrs = '{}' AND excluded.attrs <> '{}'
                      THEN excluded.attrs ELSE entities.attrs END`);
const getEntity = db.prepare('SELECT id, attrs FROM entities WHERE norm = ?');
const insME = db.prepare(`INSERT OR IGNORE INTO memory_entity(memory_id, entity_id, role, ts, status, attrs, version, prev_memory_id)
  VALUES (?, ?, 'about', ?, ?, ?, 0, '')`);

const stats = {
  db: DB, incremental: INCREMENTAL, batch: BATCH,
  scanned: 0, rowsWithEntities: 0, jsonElems: 0, empty: 0, dupWithin: 0, pairCount: 0,
  inserted: 0, existed: 0, drops: {}, etypes: {}, entityRowsNew: 0,
};
const displayVotes = new Map(); // norm -> Map(display -> count)
const seenNorms = new Set();

function voteDisplay(norm, display) {
  let m = displayVotes.get(norm);
  if (!m) { m = new Map(); displayVotes.set(norm, m); }
  m.set(display, (m.get(display) || 0) + 1);
}

function pickDisplay(norm) {
  const m = displayVotes.get(norm);
  if (!m) return norm;
  let best = null;
  for (const [s, c] of m) {
    if (!best) { best = [s, c]; continue; }
    const [bs, bc] = best;
    if (c > bc) { best = [s, c]; continue; }
    if (c === bc) {
      const upper = (x) => x === x.toUpperCase() && x !== x.toLowerCase();
      if (upper(bs) && !upper(s)) best = [s, c];
      else if (upper(bs) === upper(s) && [...s].length < [...bs].length) best = [s, c];
    }
  }
  return best ? best[0] : norm;
}

let lastRid = 0;
for (;;) {
  const rows = sel.all(lastRid, BATCH);
  if (!rows.length) break;
  db.exec('BEGIN IMMEDIATE');
  try {
    for (const r of rows) {
      lastRid = r.rid;
      stats.scanned += 1;
      const raw = r.entities;
      if (!raw || !String(raw).trim()) continue;
      let arr;
      try { arr = JSON.parse(raw); } catch { continue; }
      if (!Array.isArray(arr)) arr = [arr];
      stats.rowsWithEntities += 1;
      const perMem = new Map(); // norm -> {attrs, display}
      for (const item of arr) {
        const s = String(item ?? '').trim();
        if (!s) { stats.empty += 1; continue; }
        stats.jsonElems += 1;
        const n = normalizeEntity(s);
        if (n.drop) stats.drops[n.drop] = (stats.drops[n.drop] || 0) + 1;
        stats.etypes[n.etype] = (stats.etypes[n.etype] || 0) + 1;
        voteDisplay(n.norm, n.display);
        const prev = perMem.get(n.norm);
        if (prev) {
          stats.dupWithin += 1;
          // 同条内同 norm：属性合并，display 取先到的
          if (n.attrs && Object.keys(n.attrs).length) Object.assign(prev.attrs, n.attrs);
          continue;
        }
        perMem.set(n.norm, { attrs: n.attrs, display: n.display });
      }
      if (!perMem.size) continue;
      const ts = r.ts || '';
      // memories.status：NULL / ''（活跃）读作 'active'，archived / merged 原样带过去。
      // 别把空串写进 memory_entity.status——读侧按 status='active' 过滤时会静默漏行。
      const st = String(r.status ?? '').trim() || 'active';
      for (const [norm, info] of perMem) {
        stats.pairCount += 1;
        const attrsJson = JSON.stringify(info.attrs || {});
        let ent = getEntity.get(norm);
        if (!ent) {
          insEntity.run(norm, info.display, normalizeEntity(norm).etype, attrsJson, ts, ts);
          stats.entityRowsNew += 1;
          ent = getEntity.get(norm);
          if (ent) seenNorms.add(norm);
        } else {
          insEntity.run(norm, info.display, normalizeEntity(norm).etype, attrsJson, ts, ts);
        }
        const res = insME.run(r.id, ent.id, ts, st, attrsJson);
        if (res.changes) stats.inserted += 1; else stats.existed += 1;
      }
    }
    db.exec('COMMIT');
  } catch (e) {
    try { db.exec('ROLLBACK'); } catch { /* ignore */ }
    throw e;
  }
  process.stderr.write(`[migrate] scanned=${stats.scanned}\n`);
}

// R19：display 取组内最高频写法；mem_count / first_seen / last_seen 重算
db.exec('BEGIN IMMEDIATE');
try {
  const upd = db.prepare(`UPDATE entities SET display=?, mem_count=?, first_seen=?, last_seen=? WHERE id=?`);
  const rows = db.prepare('SELECT id, norm FROM entities').all();
  const agg = db.prepare(`SELECT count(*) c,
      coalesce(min(CASE WHEN ts<>'' THEN ts END),'') f,
      coalesce(max(CASE WHEN ts<>'' THEN ts END),'') l
    FROM memory_entity WHERE entity_id = ?`);
  for (const row of rows) {
    const a = agg.get(row.id);
    const d = pickDisplay(row.norm) || row.norm;
    upd.run(d, a.c, a.f, a.l, row.id);
  }
  db.exec('COMMIT');
} catch (e) {
  try { db.exec('ROLLBACK'); } catch { /* ignore */ }
  throw e;
}

const out = {
  ...stats,
  entities: db.prepare('SELECT count(*) c FROM entities').get().c,
  memory_entity: db.prepare('SELECT count(*) c FROM memory_entity').get().c,
  jsonElemsNow: db.prepare(`SELECT coalesce(sum(json_array_length(entities)),0) c FROM memories
    WHERE entities IS NOT NULL AND entities <> '' AND json_valid(entities)`).get().c,
};
console.log(JSON.stringify(out, null, 2));
db.close();
