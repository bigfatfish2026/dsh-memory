#!/usr/bin/env node
/**
 * observation 任务队列表（docs/observation-plan-v2.md §2）。
 *
 * 只建一张表 + 一个索引：observe_task 记「哪些实体等着重算 observation」。
 * 写侧登记（enqueue）、夜间按租约领取（claim）、版本校验（complete/fail）的语义
 * 在 src/store/observe-queue.ts，本脚本只管建表。
 *
 * 幂等：全用 CREATE TABLE / CREATE INDEX IF NOT EXISTS，可重复执行；
 * 已有行一个不碰，只补 meta 版本号。手动跑，不挂启动、不进 ensureSchema。
 *
 * 用法：
 *   node scripts/apply-observe-schema.mjs [--db /path/to/memory.db] [--dry-run]
 *   不给 --db 时按 DSH_MEMORY_DB → $DSH_MEMORY_ROOT/memory.db → cwd/memory/memory.db 解析
 *   （与 scripts/lib/db.mjs 的 resolveDbPath 一致）。
 *
 * 跑完可用 sqlite3 核对：
 *   SELECT name FROM sqlite_master WHERE name IN ('observe_task','idx_observe_task_pick');
 *   SELECT count(*) FROM observe_task;
 */
import fs from 'node:fs';
import { openDb, resolveDbPath } from './lib/db.mjs';

const argv = process.argv.slice(2);
const argOf = (k, d) => (argv.includes(k) ? argv[argv.indexOf(k) + 1] : d);
const DB = argOf('--db', resolveDbPath());
const DRY = argv.includes('--dry-run');

export const OBSERVE_DDL = `
-- 待重算 observation 的实体。entity_key 是规范化实体键（src/shared/entity-normalize.ts 的口径），
-- 一个实体一行；version 每次 enqueue（新证据/证据变更）加一，用来作废在跑的旧结果。
CREATE TABLE IF NOT EXISTS observe_task (
  entity_key   TEXT PRIMARY KEY,                   -- 规范化实体键
  entity_id    TEXT NOT NULL DEFAULT '',
  status       TEXT NOT NULL DEFAULT 'pending',    -- pending | leased | done
  version      INTEGER NOT NULL DEFAULT 0,         -- 每次 enqueue/证据变更 +1
  lease_owner  TEXT NOT NULL DEFAULT '',
  lease_until  INTEGER NOT NULL DEFAULT 0,         -- epoch ms
  enqueued_ts  INTEGER NOT NULL DEFAULT 0,
  updated_ts   INTEGER NOT NULL DEFAULT 0,
  tries        INTEGER NOT NULL DEFAULT 0,
  last_error   TEXT NOT NULL DEFAULT '',
  reason       TEXT NOT NULL DEFAULT ''
);
-- 领活只按「待领优先、先来先领」扫，状态 + 入队时间足够；租约是否过期由 claim 的 WHERE 判。
CREATE INDEX IF NOT EXISTS idx_observe_task_pick ON observe_task(status, enqueued_ts);
`;

/** meta 表在不在：老库/测试库可能没有 meta，缺了就只建表，不硬塞版本行。 */
function hasMetaTable(db) {
  return Boolean(db.prepare("SELECT 1 AS ok FROM sqlite_master WHERE type='table' AND name='meta'").get());
}

/** 建表结果核对：表与索引都在、列齐全才算成功。 */
export function inspectObserveSchema(db) {
  const table = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='observe_task'").get();
  const index = db.prepare("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_observe_task_pick'").get();
  const columns = table
    ? db.prepare("PRAGMA table_info('observe_task')").all().map((r) => String(r.name))
    : [];
  const rows = table ? Number(db.prepare('SELECT count(*) AS n FROM observe_task').get().n) || 0 : 0;
  return { table: Boolean(table), index: Boolean(index), columns, rows };
}

/**
 * 建 observe_task。幂等：重复跑只补缺的表/索引，不重建、不清行。
 * @param {string} dbPath 库路径（文件必须已存在，不替调用方造库）
 * @param {{logger?: {info?: Function, warn?: Function}, dryRun?: boolean}} [opts]
 */
export function applyObserveSchema(dbPath, { logger = console, dryRun = false } = {}) {
  if (!fs.existsSync(dbPath)) throw new Error(`记忆库不存在：${dbPath}`);
  const log = (line) => { if (typeof logger?.info === 'function') logger.info(line); };
  const db = openDb(dbPath);
  db.exec('PRAGMA busy_timeout = 15000;');
  try {
    const before = inspectObserveSchema(db);
    if (dryRun) {
      log(`[dry-run] 将建 observe_task + idx_observe_task_pick（当前 table=${before.table} rows=${before.rows}）`);
      return { dryRun: true, db: dbPath, before };
    }
    db.exec('BEGIN IMMEDIATE');
    try {
      db.exec(OBSERVE_DDL);
      const meta = hasMetaTable(db)
        ? "INSERT INTO meta(key, value) VALUES ('observe_schema_version', '1') ON CONFLICT(key) DO UPDATE SET value = excluded.value"
        : null;
      if (meta) db.exec(meta);
      db.exec('COMMIT');
    } catch (error) {
      try { db.exec('ROLLBACK'); } catch { /* 事务自己坏了 */ }
      throw error;
    }
    const after = inspectObserveSchema(db);
    const metaNote = hasMetaTable(db) ? 'meta.observe_schema_version=1' : 'meta 表不在，版本号未写';
    log(`[apply-observe-schema] observe_task 就绪 table=${after.table} index=${after.index} rows=${after.rows}（${metaNote}）`);
    return { dryRun: false, db: dbPath, before, after, created: !before.table };
  } finally {
    db.close();
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const out = applyObserveSchema(DB, { dryRun: DRY });
  console.log(JSON.stringify(out, null, 2));
}
