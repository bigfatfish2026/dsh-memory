#!/usr/bin/env node
/**
 * 建实体层影子表（一期）。
 *
 * 依据 docs/dsh-memory-entity-rework-2026.md §3.1 / §3.2 / §3.5：
 *   entities / memory_entity 两张主表，memory_entity 带 §3.5 的
 *   attrs / version / prev_memory_id 三列（一期 version/prev 留默认值）。
 * 历史：别名表 2026-11-14 已退场，实体身份改由 entity_surface 表达，
 *       本脚本不再建别名表、不再播种别名，也不再写 meta.entity_schema_version。
 * 另加两张只记不删的辅助表（本票自定，设计稿没有）：
 *   entity_issue     违规/脏名待人工看：{memory_id, entity_norm, raw_name, reason}
 *   entity_excluded  §12.1 D 类划线，不进回填：{memory_id, scope, reason}
 *
 * memories 表一个字不动；旧 entities 列继续照原样用。
 * 幂等：全部 CREATE ... IF NOT EXISTS；重复跑不报错。
 *
 * 用法：node scripts/apply-entity-schema.mjs [--db /path/memory.db]
 */
import { openDb, resolveDbPath } from './lib/db.mjs';

const DB = process.argv.includes('--db')
  ? process.argv[process.argv.indexOf('--db') + 1]
  : resolveDbPath();

const DDL = `
PRAGMA busy_timeout = 10000;

-- 1) 实体主表。id 稳定；norm 全库唯一。
CREATE TABLE IF NOT EXISTS entities (
  id           INTEGER PRIMARY KEY,
  norm         TEXT NOT NULL,
  display      TEXT NOT NULL,
  etype        TEXT NOT NULL DEFAULT 'unknown'
               CHECK (etype IN ('person','account','device','repo','file','service',
                                'ticket','tool','config','org','project','host','concept','unknown')),
  aliases_note TEXT NOT NULL DEFAULT '',
  attrs        TEXT NOT NULL DEFAULT '{}',
  first_seen   TEXT NOT NULL DEFAULT '',
  last_seen    TEXT NOT NULL DEFAULT '',
  mem_count    INTEGER NOT NULL DEFAULT 0,
  status       TEXT NOT NULL DEFAULT 'active'
               CHECK (status IN ('active','merged','archived')),
  UNIQUE (norm)
);

-- 2) 记忆↔实体。ts/status 冗余，为了时间线不 join memories。
CREATE TABLE IF NOT EXISTS memory_entity (
  memory_id TEXT NOT NULL,
  entity_id INTEGER NOT NULL,
  role      TEXT NOT NULL DEFAULT 'about'
            CHECK (role IN ('about','subject','object','mentioned')),
  ts        TEXT NOT NULL DEFAULT '',
  status    TEXT NOT NULL DEFAULT 'active',
  attrs     TEXT NOT NULL DEFAULT '{}',
  version   INTEGER NOT NULL DEFAULT 0,
  prev_memory_id TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (memory_id, entity_id, role)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_me_entity_ts ON memory_entity(entity_id, ts);
CREATE INDEX IF NOT EXISTS idx_me_memory    ON memory_entity(memory_id);
CREATE INDEX IF NOT EXISTS idx_me_attr      ON memory_entity(entity_id, attrs);

-- 4) 违规与脏名（只记不删，等人工过名单）。
CREATE TABLE IF NOT EXISTS entity_issue (
  memory_id   TEXT NOT NULL DEFAULT '',
  entity_norm TEXT NOT NULL,
  raw_name    TEXT NOT NULL DEFAULT '',
  reason      TEXT NOT NULL,
  detail      TEXT NOT NULL DEFAULT '',
  created_at  TEXT NOT NULL,
  PRIMARY KEY (memory_id, entity_norm, reason)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_issue_reason ON entity_issue(reason);

-- 5) 划线：D 类 839 条（724 无具体值 + 115 零独立佐证），不回填、不删。
CREATE TABLE IF NOT EXISTS entity_excluded (
  memory_id  TEXT PRIMARY KEY,
  scope      TEXT NOT NULL,
  reason     TEXT NOT NULL,
  detail     TEXT NOT NULL DEFAULT '',
  ts         TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_excluded_reason ON entity_excluded(reason);
`;

const db = openDb(DB);
db.exec(DDL);
// meta.entity_schema_version 归 apply-entity-surface.mjs 管（surface 层要 2），本脚本不碰。

// 去重口径（2026-11-15，审计实体中-9）：
// 表上 PK 还是 (memory_id, entity_id, role)，历史数据不迁移、PK 不动；代码层
// （lib/entity-gate.js insertMemoryEntity）已经按 (memory_id, entity_id) 先查后插，
// 同一记忆同一实体只留一行。这里补一条物理约束，前提是现有数据真的没重复——
// 有重复就跳过（不硬建、不删数据），把重复对数打出来给人看过再处理。
const dupes = db.prepare(
  `SELECT memory_id, entity_id, count(*) AS n FROM memory_entity
    GROUP BY memory_id, entity_id HAVING n > 1 LIMIT 5`,
).all();
const dupeTotal = db.prepare(
  `SELECT count(*) AS n FROM (
     SELECT 1 FROM memory_entity GROUP BY memory_id, entity_id HAVING count(*) > 1)`,
).get();
if (Number(dupeTotal?.n || 0) === 0) {
  db.exec('CREATE UNIQUE INDEX IF NOT EXISTS uq_memory_entity_mem ON memory_entity(memory_id, entity_id)');
} else {
  console.warn(JSON.stringify({
    warn: '(memory_id, entity_id) 有重复，跳过唯一索引（代码层已去重）',
    duplicatePairs: Number(dupeTotal?.n || 0),
    sample: dupes.map((r) => ({ ...r })),
  }));
}

const counts = {};
for (const t of ['entities', 'memory_entity', 'entity_issue', 'entity_excluded']) {
  counts[t] = db.prepare(`SELECT count(*) c FROM ${t}`).get().c;
}
console.log(JSON.stringify({ db: DB, counts }, null, 2));
db.close();
