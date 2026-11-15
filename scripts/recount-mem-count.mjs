#!/usr/bin/env node
/**
 * 校准：按 memory_entity 实际行数全表重算 entities.mem_count。
 *
 * 为什么单独成脚本：代码里只维护增量（insert +1 / delete -1），
 * 历史漂移、脚本直插、进程被杀留下的半笔都靠这条全表重算兜底。不在代码里跑——
 * 每次写入都全表 count 会把 memory_save 拖死。
 *
 * 口径：mem_count = 该实体在 memory_entity 里的行数（不分 role/origin/status）。
 * 写库只在 --apply 时发生，且整批一个事务；不带 --apply（或 --dry-run）只读、只报。
 *
 * 用法：node scripts/recount-mem-count.mjs [--db /path/memory.db] [--apply|--dry-run] [--limit 20]
 */
import { openDb, resolveDbPath } from './lib/db.mjs';

const argv = process.argv.slice(2);
const argOf = (k, d) => (argv.includes(k) ? argv[argv.indexOf(k) + 1] : d);
const DB = argOf('--db', resolveDbPath());
const APPLY = argv.includes('--apply') && !argv.includes('--dry-run');
const LIMIT = Math.max(1, Number(argOf('--limit', 20)) || 20);

const db = openDb(DB);
db.exec('PRAGMA busy_timeout = 15000;');

const actual = new Map(); // entity_id -> memory_entity 行数
for (const r of db.prepare('SELECT entity_id AS id, count(*) AS n FROM memory_entity GROUP BY entity_id').all()) {
  actual.set(Number(r.id), Number(r.n) || 0);
}

const entities = db.prepare('SELECT id, norm, mem_count FROM entities ORDER BY id').all();
const drift = [];
let deltaAbs = 0;
for (const e of entities) {
  const want = actual.get(Number(e.id)) || 0;
  const have = Number(e.mem_count) || 0;
  if (want === have) continue;
  drift.push({ id: Number(e.id), norm: String(e.norm), have, want, delta: want - have });
  deltaAbs += Math.abs(want - have);
}

const st = {
  mode: APPLY ? 'apply' : 'dry-run',
  db: DB,
  entities: entities.length,
  memoryEntityRows: [...actual.values()].reduce((a, b) => a + b, 0),
  mismatched: drift.length,
  deltaAbs,
  fixed: 0,
  samples: drift.slice(0, LIMIT),
};

if (APPLY && drift.length) {
  const upd = db.prepare('UPDATE entities SET mem_count=? WHERE id=?');
  db.exec('BEGIN IMMEDIATE');
  try {
    for (const d of drift) st.fixed += Number(upd.run(d.want, d.id).changes) || 0;
    db.exec('COMMIT');
  } catch (e) {
    try { db.exec('ROLLBACK'); } catch { /* ignore */ }
    throw e;
  }
}

console.log(JSON.stringify(st, null, 2));
db.close();
