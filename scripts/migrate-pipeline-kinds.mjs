#!/usr/bin/env node
/**
 * 把线上老库的 pending_ops.kind CHECK 补齐（2026-11-05）。
 *
 * 背景：早期 DDL 的 CHECK 只有 ('ingest_file','merge_batch','split_item','dedupe_check')，
 * 后来合并 agent 的证据打回要落 kind='merge_op'，老库没有这个值，写入会撞约束。
 * 这张票把 5 个 kind 一次补齐；其中 split_item 老库已有，缺的是 merge_op
 * （真库实测，见 --verify 输出）。
 *
 * 做法是重建表：SQLite 改不了 CHECK，只能 新表 → 拷行 → 删旧表 → 改名 → 建索引，
 * 整个过程一个事务。pending_ops 的行原样保留（id / attempts / 退避都不动）。
 *
 * 安全：默认先 VACUUM INTO 一个副本，在副本上跑一遍并 verify，通过了才动真库。
 * memories 表连读都只读统计行数，不写一个字节。
 *
 *   node scripts/migrate-pipeline-kinds.mjs [--db <path>]             # 副本先验，再迁真库
 *   node scripts/migrate-pipeline-kinds.mjs [--db <path>] --dry-run   # 只打印计划
 *   node scripts/migrate-pipeline-kinds.mjs [--db <path>] --verify    # 只核对
 *   node scripts/migrate-pipeline-kinds.mjs ... --skip-copy           # 不动副本（不推荐）
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { openDb, resolveDbPath } from './lib/db.mjs';

export const DEFAULT_DB = resolveDbPath();

/** kind 的完整枚举，和 scripts/apply-pipeline-schema.mjs 的 DDL 保持一致。 */
export const KINDS = ['ingest_file', 'merge_batch', 'split_item', 'dedupe_check', 'merge_op'];

const PENDING_OPS_DDL = `CREATE TABLE pending_ops__new (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  kind         TEXT NOT NULL
               CHECK (kind IN (${KINDS.map((k) => `'${k}'`).join(',')})),
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
)`;

const COLUMNS = ['id', 'kind', 'dedupe_key', 'payload_json', 'attempts', 'next_try_at', 'last_error', 'status', 'created_ts', 'updated_ts'];

function parseArgs(argv) {
  const out = { db: DEFAULT_DB, dryRun: false, verify: false, skipCopy: false, help: false };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === '--db') {
      const value = argv[i + 1];
      if (!value) throw new Error('--db 需要路径');
      out.db = path.resolve(value);
      i += 1;
    } else if (arg.startsWith('--db=')) {
      out.db = path.resolve(arg.slice(5));
    } else if (arg === '--dry-run') out.dryRun = true;
    else if (arg === '--verify') out.verify = true;
    else if (arg === '--skip-copy') out.skipCopy = true;
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

/** 当前 CHECK 里缺哪些 kind（幂等判定就看这个）。 */
export function missingKinds(dbPath) {
  const db = open(dbPath, true);
  try {
    const sql = tableSql(db, 'pending_ops');
    if (!sql) return { exists: false, missing: KINDS.slice(), sql: '' };
    const missing = KINDS.filter((kind) => !sql.includes(`'${kind}'`));
    return { exists: true, missing, sql };
  } finally {
    db.close();
  }
}

function counts(dbPath) {
  const db = open(dbPath, true);
  try {
    const pending = db.prepare('SELECT count(*) AS n, coalesce(max(id), 0) AS maxId FROM pending_ops').get();
    let memories = null;
    try {
      memories = Number(db.prepare('SELECT count(*) AS n FROM memories').get()?.n ?? 0);
    } catch {
      memories = null;
    }
    return { pendingOps: Number(pending?.n ?? 0), maxId: Number(pending?.maxId ?? 0), memories };
  } finally {
    db.close();
  }
}

/**
 * 在给定库上重建 pending_ops，一个事务。已是最新时直接返回 {migrated:false}。
 * @returns {{migrated:boolean, from:string[], to:string[], before:object, after:object}}
 */
export function migrateKinds(dbPath, { logger = console.log } = {}) {
  const state = missingKinds(dbPath);
  if (!state.exists) throw new Error(`pending_ops 不存在：${dbPath}（先跑 scripts/apply-pipeline-schema.mjs）`);
  if (!state.missing.length) {
    logger(`[skip] ${dbPath} 的 pending_ops.kind 已包含全部 ${KINDS.length} 个值，无需迁移`);
    return { migrated: false, from: KINDS.slice(), to: KINDS.slice(), before: counts(dbPath), after: counts(dbPath) };
  }

  const before = counts(dbPath);
  const db = open(dbPath);
  try {
    db.exec('BEGIN IMMEDIATE;');
    try {
      db.exec('DROP TABLE IF EXISTS pending_ops__new;');
      db.exec(`${PENDING_OPS_DDL};`);
      db.exec(
        `INSERT INTO pending_ops__new (${COLUMNS.join(', ')})
         SELECT ${COLUMNS.join(', ')} FROM pending_ops ORDER BY id;`,
      );
      const copied = Number(db.prepare('SELECT count(*) AS n FROM pending_ops__new').get()?.n ?? 0);
      if (copied !== before.pendingOps) throw new Error(`行数对不上：迁移前 ${before.pendingOps}，拷贝后 ${copied}`);
      db.exec('DROP TABLE pending_ops;');
      db.exec('ALTER TABLE pending_ops__new RENAME TO pending_ops;');
      db.exec('CREATE INDEX IF NOT EXISTS idx_pending_due ON pending_ops(status, next_try_at, attempts);');
      db.exec('COMMIT;');
    } catch (error) {
      try { db.exec('ROLLBACK;'); } catch { /* 交给 sqlite */ }
      throw error;
    }
  } finally {
    db.close();
  }

  const after = counts(dbPath);
  if (after.pendingOps !== before.pendingOps || after.maxId !== before.maxId) {
    throw new Error(`迁移后账不对：rows ${before.pendingOps}→${after.pendingOps} maxId ${before.maxId}→${after.maxId}`);
  }
  if (after.memories !== before.memories) {
    // 活库可能有别的会话在写 memories；本次迁移一条 memories 语句都没执行，
    // 这里只记账不拦（否则会被别人的正常写入误判成迁移出错）。
    logger(
      `[note] memories 行数在迁移窗口内由 ${before.memories} 变为 ${after.memories}，` +
        '属别的连接并发写入；本次迁移只重建 pending_ops，未执行任何 memories 写语句',
    );
  }
  logger(`[ok] ${dbPath} pending_ops.kind 已补齐：${state.missing.join(',')}（rows=${after.pendingOps} maxId=${after.maxId}）`);
  return { migrated: true, from: KINDS.filter((k) => state.missing.includes(k)), to: KINDS.slice(), before, after };
}

/** 核对：integrity_check、CHECK 是否覆盖全部 kind、行数、索引、memories 统计。 */
export function verifyKinds(dbPath) {
  if (!fs.existsSync(dbPath)) throw new Error(`库不存在：${dbPath}`);
  const db = open(dbPath, true);
  try {
    const report = { dbPath, integrity: null, sql: '', missing: [], rows: null, maxId: null, index: false, memories: null };
    report.integrity = db.prepare('PRAGMA integrity_check').get()?.integrity_check ?? null;
    report.sql = tableSql(db, 'pending_ops');
    if (!report.sql) return report;
    report.missing = KINDS.filter((kind) => !report.sql.includes(`'${kind}'`));
    const row = db.prepare('SELECT count(*) AS n, coalesce(max(id), 0) AS maxId FROM pending_ops').get();
    report.rows = Number(row?.n ?? 0);
    report.maxId = Number(row?.maxId ?? 0);
    report.index = Boolean(db.prepare("SELECT 1 AS ok FROM sqlite_master WHERE type='index' AND name='idx_pending_due'").get());
    try {
      report.memories = Number(db.prepare('SELECT count(*) AS n FROM memories').get()?.n ?? 0);
    } catch {
      report.memories = null;
    }
    return report;
  } finally {
    db.close();
  }
}

export function printVerify(report, logger = console.log) {
  logger(`库：${report.dbPath}`);
  logger(`integrity_check=${report.integrity} pending_ops.rows=${report.rows} maxId=${report.maxId} memories=${report.memories}`);
  logger(`CHECK kinds=${report.missing.length ? `缺 ${report.missing.join(',')}` : '完整（' + KINDS.join(',') + '）'}`);
  logger(`idx_pending_due=${report.index ? '有' : '缺'}`);
  return report.integrity === 'ok' && report.missing.length === 0 && report.index;
}

/** 副本路径：跟真库同目录，带时间戳，不动 WAL 活库（VACUUM INTO 出的是一致快照）。 */
export function copyForTrial(dbPath, { stamp = Date.now() } = {}) {
  const target = path.join(
    path.dirname(dbPath),
    `${path.basename(dbPath)}.kindmig-${stamp}.bak`,
  );
  const db = open(dbPath);
  try {
    db.exec(`VACUUM INTO '${target.replace(/'/g, "''")}';`);
  } finally {
    db.close();
  }
  return target;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    console.log('用法：node scripts/migrate-pipeline-kinds.mjs [--db <path>] [--dry-run|--verify] [--skip-copy]');
    return;
  }
  if (!fs.existsSync(args.db)) throw new Error(`库不存在：${args.db}`);

  if (args.verify) {
    const ok = printVerify(verifyKinds(args.db));
    if (!ok) process.exitCode = 1;
    return;
  }

  const state = missingKinds(args.db);
  console.log(`当前 CHECK：${state.exists ? (state.missing.length ? `缺 ${state.missing.join(',')}` : '已完整') : 'pending_ops 缺失'}`);
  if (!state.missing.length) {
    printVerify(verifyKinds(args.db));
    return;
  }

  if (args.dryRun) {
    console.log(`[dry-run] 将重建 pending_ops（${args.db}），CHECK 补齐为：${KINDS.join(',')}`);
    console.log(PENDING_OPS_DDL);
    console.log('[dry-run] 未写库');
    return;
  }

  // 先在副本上验一遍：副本迁完 verify 通过，才碰真库
  if (!args.skipCopy) {
    const trial = copyForTrial(args.db);
    console.log(`副本：${trial}`);
    const trialResult = migrateKinds(trial);
    const trialReport = verifyKinds(trial);
    const trialOk = printVerify(trialReport, (line) => console.log(`[副本] ${line}`));
    if (!trialResult.migrated && !trialOk) throw new Error('副本上迁移没成功，停手，真库一个字节没动');
    if (!trialOk) throw new Error('副本 verify 不通过，停手，真库一个字节没动');
    try { fs.rmSync(trial); } catch { /* 留着也无害，删不掉就留着 */ }
    console.log('[副本] 验证通过，开始动真库');
  }

  migrateKinds(args.db);
  const ok = printVerify(verifyKinds(args.db));
  if (!ok) process.exitCode = 1;
}

const isCli = process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href;
if (isCli) {
  main().catch((error) => {
    console.error(`失败：${error?.message || error}`);
    process.exitCode = 1;
  });
}
