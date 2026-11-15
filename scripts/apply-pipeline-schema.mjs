#!/usr/bin/env node
/**
 * 建睡觉流水线 v2 的前三张表：ingest_files / pending_ops / discards。
 *
 * 2026-11-05 设计稿第 3.1、4.1、5.4 节的 DDL 原样落地，全部
 * CREATE ... IF NOT EXISTS，可重复跑。split_done 属于下一票，这里不建。
 *
 *   node scripts/apply-pipeline-schema.mjs --db <path>            # 建表
 *   node scripts/apply-pipeline-schema.mjs --db <path> --dry-run  # 只打印 DDL
 *   node scripts/apply-pipeline-schema.mjs --db <path> --verify   # 只核对
 */
import fs from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { openDb, resolveDbPath } from './lib/db.mjs';

export const DEFAULT_DB = resolveDbPath();

export const TABLES = ['ingest_files', 'pending_ops', 'discards'];

/** 顺序与 docs/sleep-pipeline-redesign.md 5.4 一致 */
export const DDL = [
  `CREATE TABLE IF NOT EXISTS ingest_files (
  path          TEXT PRIMARY KEY,
  mtime         REAL NOT NULL,
  size          INTEGER NOT NULL,
  last_acked_ts INTEGER NOT NULL DEFAULT 0,
  status        TEXT NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending','done','legacy_acked','failed','dead')),
  attempts      INTEGER NOT NULL DEFAULT 0,
  last_error    TEXT,
  updated_ts    INTEGER NOT NULL
)`,
  `CREATE INDEX IF NOT EXISTS idx_ingest_status ON ingest_files(status, updated_ts)`,
  `CREATE TABLE IF NOT EXISTS pending_ops (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  kind         TEXT NOT NULL
               CHECK (kind IN ('ingest_file','merge_batch','split_item','dedupe_check','merge_op','revocation_sweep')),
  -- 设计：一张表按通道分三段（ingest / curate / compact），池按 channel 领活。
  -- '' 是给老库留的自愈位：ensureChannelColumn 会按 kind 回填，领活 SQL 也兼容空值。
  channel      TEXT NOT NULL DEFAULT '',
  dedupe_key   TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  attempts     INTEGER NOT NULL DEFAULT 0,
  next_try_at  INTEGER NOT NULL,
  last_error   TEXT,
  status       TEXT NOT NULL DEFAULT 'pending'
               CHECK (status IN ('pending','running','done','dead')),
  created_ts   INTEGER NOT NULL,
  updated_ts   INTEGER NOT NULL,
  UNIQUE(kind, dedupe_key)
)`,
  `CREATE INDEX IF NOT EXISTS idx_pending_due ON pending_ops(status, next_try_at, attempts)`,
  `CREATE TABLE IF NOT EXISTS discards (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  candidate_hash TEXT NOT NULL,
  text         TEXT NOT NULL,
  matched_id   TEXT,
  verdict      TEXT NOT NULL CHECK (verdict IN ('DUPLICATE','SUPERSEDES','LINT_REJECT')),
  reason       TEXT,
  source_file  TEXT,
  created_ts   INTEGER NOT NULL
)`,
  `CREATE INDEX IF NOT EXISTS idx_discards_hash ON discards(candidate_hash)`,
  `CREATE INDEX IF NOT EXISTS idx_discards_matched ON discards(matched_id)`,
];

function parseArgs(argv) {
  const out = { db: DEFAULT_DB, dryRun: false, verify: false };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === '--db') {
      const value = argv[i + 1];
      if (!value) throw new Error('--db 需要路径');
      out.db = path.resolve(value);
      i += 1;
    } else if (arg.startsWith('--db=')) {
      out.db = path.resolve(arg.slice(5));
    } else if (arg === '--dry-run') {
      out.dryRun = true;
    } else if (arg === '--verify') {
      out.verify = true;
    } else if (arg === '--help' || arg === '-h') {
      out.help = true;
    } else {
      throw new Error(`未知参数：${arg}`);
    }
  }
  return out;
}

function open(dbPath, readOnly = false) {
  const db = openDb(dbPath, { readOnly });
  db.exec('PRAGMA busy_timeout = 5000;');
  return db;
}

/**
 * 老库自愈：pending_ops 建表在 channel 列之前，这里幂等补列 + 按 kind 回填。
 * 必须在 DDL 之后、同一事务里跑（老库没有 channel 列时，DDL 里的索引会先炸）；
 * 加不上（视图/只读）就跳过，领活 SQL 对 '' 也有兼容分支。
 */
export function applyChannelColumn(db) {
  const cols = db.prepare('PRAGMA table_info(pending_ops)').all().map((c) => String(c.name));
  const added = !cols.includes('channel');
  if (added) db.exec("ALTER TABLE pending_ops ADD COLUMN channel TEXT NOT NULL DEFAULT ''");
  db.exec('CREATE INDEX IF NOT EXISTS idx_pending_channel ON pending_ops(channel, status, next_try_at, attempts)');
  const n1 = db.prepare("UPDATE pending_ops SET channel='ingest' WHERE channel='' AND kind='ingest_file'").run();
  const n2 = db.prepare(
    "UPDATE pending_ops SET channel='curate' WHERE channel='' AND kind IN ('merge_batch','merge_op','dedupe_check','split_item')",
  ).run();
  return { added, backfilled: Number(n1.changes) + Number(n2.changes) };
}

/** 单事务建表；返回执行过的语句。 */
export function applySchema(dbPath, { dryRun = false, logger = console.log } = {}) {
  if (!fs.existsSync(dbPath)) throw new Error(`库不存在：${dbPath}`);
  if (dryRun) {
    logger(`[dry-run] ${dbPath}，将执行 ${DDL.length} 条 DDL：`);
    for (const sql of DDL) logger(`${sql};`);
    return { applied: false, statements: DDL.length };
  }
  const db = open(dbPath);
  try {
    db.exec('BEGIN IMMEDIATE;');
    try {
      for (const sql of DDL) db.exec(`${sql};`);
      // 老库补 channel 列（新库已经带了，这里是一次空 UPDATE）
      try { applyChannelColumn(db); } catch { /* 补不上就别挡建表 */ }
      db.exec('COMMIT;');
    } catch (error) {
      try { db.exec('ROLLBACK;'); } catch { /* 交给 sqlite */ }
      throw error;
    }
    logger(`已应用 ${DDL.length} 条 DDL：${dbPath}`);
    return { applied: true, statements: DDL.length };
  } finally {
    db.close();
  }
}

/** 核对三张表、列、索引、行数、integrity_check。 */
export function verifySchema(dbPath) {
  if (!fs.existsSync(dbPath)) throw new Error(`库不存在：${dbPath}`);
  const db = open(dbPath, true);
  try {
    const report = { dbPath, tables: {}, integrity: null, journalMode: null };
    try { report.journalMode = db.prepare('PRAGMA journal_mode').get()?.journal_mode ?? null; } catch { /* 忽略 */ }
    for (const table of TABLES) {
      const exists = Boolean(db.prepare("SELECT 1 AS ok FROM sqlite_master WHERE type='table' AND name=?").get(table));
      if (!exists) {
        report.tables[table] = { exists: false, columns: [], indexes: [], rows: null };
        continue;
      }
      const columns = db.prepare(`PRAGMA table_info(${table})`).all()
        .map((c) => `${c.name}:${c.type || ''}${c.notnull ? ' NOT NULL' : ''}${c.dflt_value !== null ? ` DEFAULT ${c.dflt_value}` : ''}`);
      const indexes = db.prepare(`PRAGMA index_list(${table})`).all().map((i) => i.name);
      const rows = Number(db.prepare(`SELECT count(*) AS n FROM ${table}`).get()?.n ?? 0);
      report.tables[table] = { exists: true, columns, indexes, rows };
    }
    report.integrity = db.prepare('PRAGMA integrity_check').get()?.integrity_check ?? null;
    return report;
  } finally {
    db.close();
  }
}

export function printVerify(report, logger = console.log) {
  logger(`库：${report.dbPath}`);
  logger(`journal_mode=${report.journalMode} integrity_check=${report.integrity}`);
  let ok = report.integrity === 'ok';
  for (const table of TABLES) {
    const t = report.tables[table];
    if (!t?.exists) {
      ok = false;
      logger(`${table}: 缺失`);
      continue;
    }
    logger(`${table}: rows=${t.rows} indexes=${t.indexes.join(',') || '(none)'}`);
    logger(`  columns: ${t.columns.join(' | ')}`);
  }
  return ok;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    console.log('用法：node scripts/apply-pipeline-schema.mjs [--db <path>] [--dry-run|--verify]');
    return;
  }
  if (args.dryRun) {
    applySchema(args.db, { dryRun: true });
    return;
  }
  if (args.verify) {
    const report = verifySchema(args.db);
    const ok = printVerify(report);
    if (!ok) process.exitCode = 1;
    return;
  }
  applySchema(args.db);
  const report = verifySchema(args.db);
  const ok = printVerify(report);
  if (!ok) process.exitCode = 1;
}

const isCli = process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href;
if (isCli) {
  main().catch((error) => {
    console.error(`失败：${error?.message || error}`);
    process.exitCode = 1;
  });
}
