#!/usr/bin/env node
/**
 * 把线上老库的 discards.verdict CHECK 补上 'LINT_REJECT'（2026-11-06）。
 *
 * 背景：正文 lint 二次仍不合格的条目要不入库、留证，落在
 * discards(verdict='LINT_REJECT', source_file='lint_reject')。早期 DDL 的 CHECK 只有
 * ('DUPLICATE','SUPERSEDES')，老库直接写会撞约束——证据静默丢掉，等于没有留证。
 *
 * SQLite 改不了 CHECK，还是那套：新表 → 拷行 → 删旧表 → 改名 → 重建索引，一个事务。
 * discards 的 id / candidate_hash / text 一个字都不动，memories 表连写语句都不执行。
 *
 *   node scripts/migrate-discard-verdicts.mjs [--db <path>]            # 迁移
 *   node scripts/migrate-discard-verdicts.mjs [--db <path>] --dry-run  # 只打印计划
 *   node scripts/migrate-discard-verdicts.mjs [--db <path>] --verify   # 只核对
 *
 * 顺序：这个脚本要在新代码接管写入**之前**跑，否则那段时间的 lint_reject 落不了库。
 */
import fs from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { openDb, resolveDbPath } from './lib/db.mjs';

export const DEFAULT_DB = resolveDbPath();

/** verdict 的完整枚举，和 scripts/apply-pipeline-schema.mjs 的 DDL 保持一致。 */
export const VERDICTS = ['DUPLICATE', 'SUPERSEDES', 'LINT_REJECT'];

const DISCARDS_DDL = `CREATE TABLE discards__new (
  id             INTEGER PRIMARY KEY AUTOINCREMENT,
  candidate_hash TEXT NOT NULL,
  text           TEXT NOT NULL,
  matched_id     TEXT,
  verdict        TEXT NOT NULL
                 CHECK (verdict IN (${VERDICTS.map((v) => `'${v}'`).join(',')})),
  reason         TEXT,
  source_file    TEXT,
  created_ts     INTEGER NOT NULL
)`;

const COLUMNS = ['id', 'candidate_hash', 'text', 'matched_id', 'verdict', 'reason', 'source_file', 'created_ts'];

const INDEXES = [
  'CREATE INDEX IF NOT EXISTS idx_discards_hash ON discards(candidate_hash)',
  'CREATE INDEX IF NOT EXISTS idx_discards_matched ON discards(matched_id)',
];

function parseArgs(argv) {
  const out = { db: DEFAULT_DB, dryRun: false, verify: false, help: false };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === '--db') {
      const value = argv[i + 1];
      if (!value) throw new Error('--db 需要路径');
      out.db = path.resolve(value);
      i += 1;
    } else if (arg.startsWith('--db=')) out.db = path.resolve(arg.slice(5));
    else if (arg === '--dry-run') out.dryRun = true;
    else if (arg === '--verify') out.verify = true;
    else if (arg === '--help' || arg === '-h') out.help = true;
    else throw new Error(`未知参数：${arg}`);
  }
  return out;
}

function open(dbPath, readOnly = false) {
  const db = openDb(dbPath, { readOnly });
  db.exec('PRAGMA busy_timeout = 10000;');
  return db;
}

function tableSql(db, name) {
  return String(db.prepare("SELECT sql FROM sqlite_master WHERE type='table' AND name=?").get(name)?.sql || '');
}

/** 当前 CHECK 里缺哪些 verdict（幂等判定就看这个）。 */
export function missingVerdicts(dbPath) {
  const db = open(dbPath, true);
  try {
    const sql = tableSql(db, 'discards');
    if (!sql) return { exists: false, missing: VERDICTS.slice(), sql: '' };
    const missing = VERDICTS.filter((verdict) => !sql.includes(`'${verdict}'`));
    return { exists: true, missing, sql };
  } finally {
    db.close();
  }
}

function counts(dbPath) {
  const db = open(dbPath, true);
  try {
    const row = db.prepare('SELECT count(*) AS n, coalesce(max(id), 0) AS maxId FROM discards').get();
    return { discards: Number(row?.n ?? 0), maxId: Number(row?.maxId ?? 0) };
  } finally {
    db.close();
  }
}

/**
 * 在给定库上重建 discards，一个事务。已是最新时直接返回 {migrated:false}。
 * @returns {{migrated:boolean, from:string[], to:string[], before:object, after:object}}
 */
export function migrateDiscardVerdicts(dbPath, { logger = console.log } = {}) {
  const state = missingVerdicts(dbPath);
  if (!state.exists) throw new Error(`discards 不存在：${dbPath}（先跑 scripts/apply-pipeline-schema.mjs）`);
  if (!state.missing.length) {
    logger(`[skip] ${dbPath} 的 discards.verdict 已包含全部 ${VERDICTS.length} 个值，无需迁移`);
    return { migrated: false, from: VERDICTS.slice(), to: VERDICTS.slice(), before: counts(dbPath), after: counts(dbPath) };
  }

  const before = counts(dbPath);
  const db = open(dbPath);
  try {
    db.exec('BEGIN IMMEDIATE;');
    try {
      db.exec('DROP TABLE IF EXISTS discards__new;');
      db.exec(`${DISCARDS_DDL};`);
      db.exec(
        `INSERT INTO discards__new (${COLUMNS.join(', ')})
         SELECT ${COLUMNS.join(', ')} FROM discards ORDER BY id;`,
      );
      const copied = Number(db.prepare('SELECT count(*) AS n FROM discards__new').get()?.n ?? 0);
      if (copied !== before.discards) throw new Error(`行数对不上：迁移前 ${before.discards}，拷贝后 ${copied}`);
      db.exec('DROP TABLE discards;');
      db.exec('ALTER TABLE discards__new RENAME TO discards;');
      for (const sql of INDEXES) db.exec(`${sql};`);
      db.exec('COMMIT;');
    } catch (error) {
      try { db.exec('ROLLBACK;'); } catch { /* 交给 sqlite */ }
      throw error;
    }
  } finally {
    db.close();
  }

  const after = counts(dbPath);
  if (after.discards !== before.discards || after.maxId !== before.maxId) {
    throw new Error(`迁移后账不对：rows ${before.discards}→${after.discards} maxId ${before.maxId}→${after.maxId}`);
  }
  logger(
    `[ok] ${dbPath} discards.verdict 已补齐：${state.missing.join(',')}（rows=${after.discards} maxId=${after.maxId}）`,
  );
  return { migrated: true, from: state.missing.slice(), to: VERDICTS.slice(), before, after };
}

/** 核对：integrity_check、CHECK 是否覆盖全部 verdict、行数、索引。 */
export function verifyDiscardVerdicts(dbPath) {
  const db = open(dbPath, true);
  try {
    const state = missingVerdicts(dbPath);
    const rows = counts(dbPath);
    const indexes = db.prepare('PRAGMA index_list(discards)').all().map((row) => row.name);
    return {
      dbPath,
      exists: state.exists,
      missing: state.missing,
      rows: rows.discards,
      maxId: rows.maxId,
      indexes,
      integrity: db.prepare('PRAGMA integrity_check').get()?.integrity_check ?? null,
    };
  } finally {
    db.close();
  }
}

export function printVerify(report, logger = console.log) {
  logger(`库：${report.dbPath}`);
  logger(`discards: rows=${report.rows} maxId=${report.maxId} integrity_check=${report.integrity}`);
  logger(`  verdict 缺失：${report.missing.length ? report.missing.join(',') : '(无)'}`);
  logger(`  索引：${report.indexes.join(',') || '(none)'}`);
  return report.integrity === 'ok' && report.missing.length === 0;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    console.log('用法：node scripts/migrate-discard-verdicts.mjs [--db <path>] [--dry-run|--verify]');
    return;
  }
  if (!fs.existsSync(args.db)) throw new Error(`库不存在：${args.db}`);
  if (args.verify) {
    const ok = printVerify(verifyDiscardVerdicts(args.db));
    if (!ok) process.exitCode = 1;
    return;
  }
  if (args.dryRun) {
    const state = missingVerdicts(args.db);
    const rows = counts(args.db);
    console.log(`[dry-run] ${args.db}`);
    console.log(`  discards 行数：${rows.discards}，maxId=${rows.maxId}`);
    console.log(`  verdict 缺失：${state.missing.join(',') || '(无，无需迁移)'}`);
    console.log(`  将执行：重建表 discards，拷贝 ${COLUMNS.length} 列，重建 2 个索引，一个事务`);
    return;
  }
  const result = migrateDiscardVerdicts(args.db);
  const ok = printVerify(verifyDiscardVerdicts(args.db));
  if (!ok) {
    console.error('迁移后核对不通过');
    process.exitCode = 1;
  }
  return result;
}

const isCli = process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href;
if (isCli) {
  main().catch((error) => {
    console.error(`失败：${error?.message || error}`);
    process.exitCode = 1;
  });
}
