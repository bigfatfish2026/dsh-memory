#!/usr/bin/env node
/**
 * 整理队列建表（docs/dsh-memory-entity-rework-2026.md §3.6 原样 + 一列追加 + 一张关系表）。
 *
 * 建 curate_queue / curate_run / memory_link 三表（CREATE IF NOT EXISTS，可重复跑）。
 * 另外给 memory_entity 补一列 origin（rule / agent / backfill / manual）：
 * 写入时的粗实体与队列 agent 抽出来的实体要能分开认，否则「这条是谁补的」永远说不清。
 * 表已经建好了（entities 2375 / memory_entity 22024），本脚本不重建任何表，
 * 也不碰 data——只加表、加列、写一行 meta 版本。
 *
 * memory_link：取代关系不再靠调用方传 supersedes 参数，
 * 由 entity_extract 队列（origin='queue'）和整理轮（origin='sleep'/'consolidate'）判完后写这张表。
 * 唯一索引钉两条规矩：同一条边只留一份；一条新记忆只许一条 supersedes 出边。
 *
 * 2026-11-13 改口：第二条规矩撤了（DROP uq_memory_link_supersedes_src）。
 * 取代是一对多，一条总述被两条细分各自推翻很常见；去重只按 (src,dst,rel)。来源优先级交给
 * lib/memory-link.js 的 queue > curate > legacy，不再靠索引物理锁死。
 *
 * 用法：node scripts/apply-curate-schema.mjs [--db /path/to/memory.db] [--dry-run]
 */
import fs from 'node:fs';
import { openDb, resolveDbPath } from './lib/db.mjs';

const argv = process.argv.slice(2);
const argOf = (k, d) => (argv.includes(k) ? argv[argv.indexOf(k) + 1] : d);
const DB = argOf('--db', resolveDbPath());
const DRY = argv.includes('--dry-run');

export const DDL = `
-- 待处理工作单元。ingest / curate / compact 共用，按 channel 分预算。
CREATE TABLE IF NOT EXISTS curate_queue (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  channel       TEXT NOT NULL CHECK (channel IN ('ingest','curate','compact')),
  kind          TEXT NOT NULL,              -- entity_extract | normalize | pair | split | archive | verify
  dedupe_key    TEXT NOT NULL,              -- 幂等键：entity_extract 用 memory_id
  payload_json  TEXT NOT NULL,
  priority      INTEGER NOT NULL DEFAULT 100,   -- 小优先
  entity_id     INTEGER NOT NULL DEFAULT 0,     -- 分片键；0=不参与分片
  state         TEXT NOT NULL DEFAULT 'ready'
                CHECK (state IN ('ready','leased','done','dead','skipped')),
  attempts      INTEGER NOT NULL DEFAULT 0,
  next_try_at   INTEGER NOT NULL DEFAULT 0,
  lease_owner   TEXT NOT NULL DEFAULT '',
  leased_ts     INTEGER NOT NULL DEFAULT 0,
  lease_expires INTEGER NOT NULL DEFAULT 0,
  created_ts    INTEGER NOT NULL,
  updated_ts    INTEGER NOT NULL,
  last_error    TEXT NOT NULL DEFAULT '',
  UNIQUE (channel, kind, dedupe_key)
);
CREATE INDEX IF NOT EXISTS idx_cq_claim ON curate_queue(state, channel, priority, next_try_at, id);
CREATE INDEX IF NOT EXISTS idx_cq_lease ON curate_queue(state, lease_expires);
CREATE INDEX IF NOT EXISTS idx_cq_entity ON curate_queue(entity_id, state);

-- 一轮 = 一个 run；断点 = run 上的 cursor + 每个批的完成记录（batch 表本票先不建，§10.4 那票再补）。
CREATE TABLE IF NOT EXISTS curate_run (
  run_id       TEXT PRIMARY KEY,            -- r_<ts36>_<rand4>
  channel      TEXT NOT NULL,
  state        TEXT NOT NULL,               -- §10.3 状态机
  stage        TEXT NOT NULL DEFAULT '',
  cursor_json  TEXT NOT NULL DEFAULT '{}',  -- {last_queue_id, cluster_cursor, stage_budget}
  budget_json  TEXT NOT NULL DEFAULT '{}',  -- {llm_calls, ms, ops}
  owner        TEXT NOT NULL DEFAULT '',
  lease_expires INTEGER NOT NULL DEFAULT 0,
  started_ts   INTEGER NOT NULL,
  heartbeat_ts INTEGER NOT NULL,
  updated_ts   INTEGER NOT NULL,
  stop_reason  TEXT NOT NULL DEFAULT ''
);

-- 记忆之间的有向关系。目前只落 rel='supersedes'（新条 -> 被取代的旧条），
-- parallel / unrelated 是判定结果，不落库（没东西可记）。
CREATE TABLE IF NOT EXISTS memory_link (
  src        TEXT NOT NULL,                -- 新记忆 id
  dst        TEXT NOT NULL,                -- 被取代的旧记忆 id
  rel        TEXT NOT NULL,                -- supersedes
  origin     TEXT NOT NULL DEFAULT '',     -- queue | sleep | consolidate | manual
  attr_key   TEXT NOT NULL DEFAULT '',     -- 同一属性的键；判不出就空
  ts         TEXT NOT NULL DEFAULT '',     -- 建边时间（ISO）
  created_ts INTEGER NOT NULL DEFAULT 0    -- 建边时间（epoch ms）
);
-- 同一条边（不管谁写的）只留一份：谁先写谁算，后来者跳过。
CREATE UNIQUE INDEX IF NOT EXISTS uq_memory_link_edge ON memory_link(src, dst, rel);
-- 2026-11-13 t-mwf4a70e-u4uv：取代本来是一对多（一条总述被两条细分各自推翻很常见），
-- 原来按 src 的 partial unique 把多目标边全挡掉了，改成只按 (src,dst,rel) 去重。
DROP INDEX IF EXISTS uq_memory_link_supersedes_src;
CREATE INDEX IF NOT EXISTS idx_memory_link_rel_origin ON memory_link(rel, origin);
CREATE INDEX IF NOT EXISTS idx_memory_link_dst ON memory_link(dst, rel);
`;

/** memory_entity 缺 origin 就 ALTER 加一列；SQLite 加列不重建表、不动已有行。 */
export function ensureEntityOrigin(db) {
  const cols = db.prepare("PRAGMA table_info('memory_entity')").all().map((r) => r.name);
  if (!cols.length) return { added: false, reason: 'memory_entity 不存在（先跑 apply-entity-schema.mjs）' };
  if (cols.includes('origin')) return { added: false, reason: '已有 origin 列' };
  db.exec("ALTER TABLE memory_entity ADD COLUMN origin TEXT NOT NULL DEFAULT ''");
  db.exec("CREATE INDEX IF NOT EXISTS idx_me_origin ON memory_entity(origin, memory_id)");
  return { added: true, reason: 'ALTER TABLE ADD COLUMN origin' };
}

export function applyCurateSchema(dbPath, { logger = console, dryRun = false } = {}) {
  if (!fs.existsSync(dbPath)) throw new Error(`记忆库不存在：${dbPath}`);
  const db = openDb(dbPath);
  db.exec('PRAGMA busy_timeout = 15000;');
  try {
    const before = {};
    for (const t of ['curate_queue', 'curate_run', 'memory_link', 'memory_entity', 'entities']) {
      before[t] = db.prepare("SELECT count(*) AS n FROM sqlite_master WHERE type='table' AND name=?").get(t).n;
    }
    if (dryRun) {
      logger.info(`[dry-run] 将建 curate_queue/curate_run/memory_link；memory_entity 存在=${before.memory_entity}`);
      return { dryRun: true, before };
    }
    db.exec('BEGIN IMMEDIATE');
    try {
      db.exec(DDL);
      db.exec(`INSERT INTO meta(key, value) VALUES ('curate_schema_version', '2')
        ON CONFLICT(key) DO UPDATE SET value = excluded.value`);
      db.exec('COMMIT');
    } catch (error) {
      try { db.exec('ROLLBACK'); } catch { /* 事务自己坏了 */ }
      throw error;
    }
    const origin = ensureEntityOrigin(db);
    const after = {
      curate_queue: db.prepare('SELECT count(*) AS n FROM curate_queue').get().n,
      curate_run: db.prepare('SELECT count(*) AS n FROM curate_run').get().n,
      memory_link: db.prepare('SELECT count(*) AS n FROM memory_link').get().n,
      memory_entity: db.prepare('SELECT count(*) AS n FROM memory_entity').get().n,
      entities: db.prepare('SELECT count(*) AS n FROM entities').get().n,
    };
    return { dryRun: false, before, after, origin };
  } finally {
    db.close();
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const out = applyCurateSchema(DB, { dryRun: DRY });
  console.log(JSON.stringify({ db: DB, ...out }, null, 2));
}
