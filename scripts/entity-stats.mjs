#!/usr/bin/env node
/**
 * 实体层一期影子对账/验收指标（只读）。
 * 输出：表行数、active 覆盖、W4 比例（迁移前用旧 JSON，迁移后用新表）、issue/excluded 分布、
 * JSON 元素对账、integrity、库大小。
 */
import fs from 'node:fs';
import { openDb, resolveDbPath } from './lib/db.mjs';
import { normalizeEntity, foldForMatch } from '../lib/shared/entity-normalize.js';

const DB = process.argv.includes('--db')
  ? process.argv[process.argv.indexOf('--db') + 1]
  : resolveDbPath();
const db = openDb(DB);
db.exec('PRAGMA busy_timeout = 15000;');

const rows = db.prepare('SELECT id, text, entities, status, ts FROM memories').all();
const active = rows.filter((r) => !r.status);
const entityById = new Map(db.prepare('SELECT id, norm FROM entities').all().map((e) => [e.id, e.norm]));
const meRows = db.prepare('SELECT memory_id, entity_id FROM memory_entity').all();
const memById = new Map(rows.map((r) => [r.id, r]));

function oldJsonStats() {
  let elems = 0; let found = 0;
  let activeElems = 0; let activeFound = 0;
  for (const r of rows) {
    if (!r.entities || !String(r.entities).trim()) continue;
    let arr; try { arr = JSON.parse(r.entities); } catch { continue; }
    if (!Array.isArray(arr)) arr = [arr];
    const folded = foldForMatch(r.text);
    for (const it of arr) {
      const raw = String(it ?? '').trim();
      if (!raw) continue;
      const n = normalizeEntity(raw);
      const ok = folded.includes(foldForMatch(n.norm)) || folded.includes(foldForMatch(raw));
      elems += 1; if (ok) found += 1;
      if (!r.status) { activeElems += 1; if (ok) activeFound += 1; }
    }
  }
  return { elems, found, rate: +(found / Math.max(1, elems)).toFixed(4), activeElems, activeFound, activeRate: +(activeFound / Math.max(1, activeElems)).toFixed(4) };
}

function tableStats() {
  let all = 0; let found = 0; let act = 0; let actFound = 0;
  let actMems = new Set(); let actFoundMems = new Set(); const allMems = new Set(); const allFoundMems = new Set();
  for (const me of meRows) {
    const m = memById.get(me.memory_id);
    if (!m) continue;
    const norm = entityById.get(me.entity_id) || '';
    const ok = foldForMatch(m.text).includes(foldForMatch(norm));
    all += 1; allMems.add(m.id);
    if (ok) { found += 1; allFoundMems.add(m.id); }
    if (!m.status) {
      act += 1; actMems.add(m.id);
      if (ok) { actFound += 1; actFoundMems.add(m.id); }
    }
  }
  return {
    all, found, rate: +(found / Math.max(1, all)).toFixed(4),
    active: act, activeFound: actFound, activeRate: +(actFound / Math.max(1, act)).toFixed(4),
    memsAll: allMems.size, memsAllFound: allFoundMems.size,
    memsActive: actMems.size, memsActiveFound: actFoundMems.size,
  };
}

const q = (sql, ...p) => db.prepare(sql).get(...p) || {};
const counts = {};
for (const t of ['entities', 'memory_entity', 'entity_surface', 'entity_issue', 'entity_excluded']) {
  counts[t] = q(`SELECT count(*) c FROM ${t}`).c;
}
const activeTotal = active.length;
const activeHasEntity = q(`SELECT count(*) c FROM memories m WHERE coalesce(m.status,'')=''
  AND EXISTS (SELECT 1 FROM memory_entity me WHERE me.memory_id=m.id)`).c;
const keep = q(`SELECT count(*) c FROM memories m WHERE coalesce(m.status,'')=''
  AND NOT EXISTS (SELECT 1 FROM entity_excluded ex WHERE ex.memory_id=m.id)`).c;
const keepHasEntity = q(`SELECT count(*) c FROM memories m WHERE coalesce(m.status,'')=''
  AND NOT EXISTS (SELECT 1 FROM entity_excluded ex WHERE ex.memory_id=m.id)
  AND EXISTS (SELECT 1 FROM memory_entity me WHERE me.memory_id=m.id)`).c;
const excludedNoEntity = q(`SELECT count(*) c FROM memories m WHERE coalesce(m.status,'')=''
  AND EXISTS (SELECT 1 FROM entity_excluded ex WHERE ex.memory_id=m.id)
  AND NOT EXISTS (SELECT 1 FROM memory_entity me WHERE me.memory_id=m.id)`).c;

const jsonElems = q(`SELECT coalesce(sum(json_array_length(entities)),0) c FROM memories
  WHERE entities IS NOT NULL AND entities <> '' AND json_valid(entities)`).c;
const origMems = q(`SELECT count(*) c FROM memories WHERE coalesce(status,'')='' OR 1=1`).c;
const migPairs = q(`SELECT count(*) c FROM memory_entity me JOIN memories m ON m.id=me.memory_id
  WHERE m.entities IS NOT NULL AND m.entities <> ''`).c;

const issueByReason = Object.fromEntries(db.prepare('SELECT reason, count(*) c FROM entity_issue GROUP BY reason').all().map((x) => [x.reason, x.c]));
const issueElems = q('SELECT count(*) c FROM entity_issue').c;
const exclByReason = Object.fromEntries(db.prepare('SELECT reason, count(*) c FROM entity_excluded GROUP BY reason').all().map((x) => [x.reason, x.c]));

const out = {
  db: DB,
  dbSizeBytes: fs.statSync(DB).size,
  integrity: db.prepare('PRAGMA integrity_check').get()['integrity_check'],
  tables: counts,
  activeTotal, activeHasEntity,
  activeRate: +(activeHasEntity / Math.max(1, activeTotal)).toFixed(4),
  keep, keepHasEntity,
  keepRate: +(keepHasEntity / Math.max(1, keep)).toFixed(4),
  excludedNoEntity,
  jsonElems, migPairs,
  migratedCoverage: oldJsonStats(),
  tableCoverage: tableStats(),
  issueByReason, issueRows: issueElems,
  excludedByReason: exclByReason,
  entitiesByEtype: Object.fromEntries(db.prepare('SELECT etype, count(*) c FROM entities GROUP BY etype ORDER BY c DESC').all().map((x) => [x.etype, x.c])),
  entityStatus: Object.fromEntries(db.prepare('SELECT status, count(*) c FROM entities GROUP BY status').all().map((x) => [x.status, x.c])),
};
console.log(JSON.stringify(out, null, 2));
db.close();
