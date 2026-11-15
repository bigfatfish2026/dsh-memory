#!/usr/bin/env node
/**
 * 会话入库账本 CLI（docs/sleep-pipeline-redesign.md §3.2、§6.3）。
 *
 * 只动 ingest_files / pending_ops 两张表，不跑模型、不写记忆、不碰 sleep.state.json。
 * 默认 dry-run：只打印待处理清单和账目，加 --apply 才落库。
 *
 *   node scripts/memory-ingest.mjs                      # 看账：扫到多少、legacy 多少、pending 多少
 *   node scripts/memory-ingest.mjs --budget 20           # 按单轮预算看清单
 *   node scripts/memory-ingest.mjs --apply --bootstrap   # 首次 bootstrap（legacy_acked 标记）
 *   node scripts/memory-ingest.mjs --apply --backfill /abs/path/session-x.jsonl
 *   node scripts/memory-ingest.mjs --apply --backfill 2026-10-01
 *   node scripts/memory-ingest.mjs --db <path> --sessions <dir> --verify
 *
 * 选它而不是塞进 scripts/memory-query.mjs：query 是查记忆的只读脚本，入库账本要写表，
 * 混在一起容易手滑在查记忆时把账改了。
 */
import fs from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { openDb, resolveMemoryFile, resolveSessionsRoot } from './lib/db.mjs';
import { applySchema, DEFAULT_DB, TABLES } from './apply-pipeline-schema.mjs';
import { backfill, bootstrapLegacy, hasIngestTables, planFiles, scanSessions } from '../lib/ingest-index.js';

export const DEFAULT_SESSIONS = resolveSessionsRoot();
export const DEFAULT_STATE = resolveMemoryFile('sleep.state.json');

function parseArgs(argv) {
  const out = {
    db: DEFAULT_DB,
    sessions: DEFAULT_SESSIONS,
    stateFile: DEFAULT_STATE,
    budget: 20,
    apply: false,
    bootstrap: false,
    verify: false,
    backfill: '',
    help: false,
  };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === '--db') {
      out.db = path.resolve(argv[++i] || '');
    } else if (arg.startsWith('--db=')) {
      out.db = path.resolve(arg.slice(5));
    } else if (arg === '--sessions') {
      out.sessions = path.resolve(argv[++i] || '');
    } else if (arg.startsWith('--sessions=')) {
      out.sessions = path.resolve(arg.slice(11));
    } else if (arg === '--state') {
      out.stateFile = path.resolve(argv[++i] || '');
    } else if (arg === '--budget') {
      out.budget = Number(argv[++i]) || 20;
    } else if (arg.startsWith('--budget=')) {
      out.budget = Number(arg.slice(9)) || 20;
    } else if (arg === '--backfill') {
      out.backfill = argv[++i] || '';
    } else if (arg.startsWith('--backfill=')) {
      out.backfill = arg.slice(11);
    } else if (arg === '--apply') {
      out.apply = true;
    } else if (arg === '--bootstrap') {
      out.bootstrap = true;
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

function readLastSleepAt(stateFile) {
  try {
    const st = JSON.parse(fs.readFileSync(stateFile, 'utf8'));
    return Number(st.lastSleepAt) || 0;
  } catch {
    return 0;
  }
}

function open(dbPath, readOnly = false) {
  if (!fs.existsSync(dbPath)) throw new Error(`库不存在：${dbPath}`);
  const db = openDb(dbPath, { readOnly });
  db.exec('PRAGMA busy_timeout = 5000;');
  return db;
}

export function run(args, logger = console.log) {
  const lastSleepAt = readLastSleepAt(args.stateFile);
  const files = scanSessions(args.sessions);
  logger(`[ingest] 库=${args.db} sessions=${args.sessions}`);
  logger(`[ingest] 扫到 ${files.length} 个会话文件，lastSleepAt=${lastSleepAt || '(无)'}`);

  let db = open(args.db, !args.apply);
  try {
    if (!hasIngestTables(db)) {
      if (!args.apply) {
        logger(`[ingest] 表不全（${TABLES.join('/')}），dry-run 不建表；加 --apply 会先建表`);
        return { applied: false, files: files.length, missingTables: true };
      }
      db.close();
      applySchema(args.db, { logger });
      db = open(args.db);
    }

    if (args.backfill) {
      const result = backfill(db, args.backfill, { files });
      if (!args.apply) {
        logger(`[ingest] dry-run: --backfill ${args.backfill} 会改 ${result.changed.length} 个文件（加 --apply 才写）`);
        for (const p of result.changed.slice(0, 20)) logger(`  待回捞 ${p}`);
        logger(`[ingest] 跳过 ${result.skipped.length} 个`);
        return { applied: false, ...result };
      }
      logger(`[ingest] --backfill ${args.backfill} 已改 ${result.changed.length} 个文件（mode=${result.mode}）`);
      for (const p of result.changed.slice(0, 20)) logger(`  回捞 ${p}`);
      logger(`[ingest] 跳过 ${result.skipped.length} 个`);
      return { applied: true, ...result };
    }

    if (args.bootstrap && args.apply) {
      const boot = bootstrapLegacy(db, files, lastSleepAt);
      logger(
        `[ingest] bootstrap 扫描${boot.scanned} legacy=${boot.legacy} pending=${boot.pending} 已有${boot.skipped} 读失败${boot.errors}`,
      );
    }
    return runBody(db, args, files, lastSleepAt, logger);
  } finally {
    try {
      if (db.isOpen !== false) db.close();
    } catch {
      /* 已经关了 */
    }
  }
}

function runBody(db, args, files, lastSleepAt, logger) {
  const plan = planFiles(db, files, { lastSleepAt, budget: args.budget });
  logger(
    `[ingest] 账目：scanned=${files.length} legacy待标=${plan.legacy.length} pending=${plan.todo.length} ` +
      `超额留=${plan.overBudget.length} done=${plan.done.length} budget=${args.budget}`,
  );
  for (const item of plan.todo) {
    logger(
      `  todo ${item.path} reason=${item.reason} last_acked_ts=${item.lastAckedTs} last_event_ts=${item.lastEventTs} attempts=${item.attempts}`,
    );
  }
  if (args.bootstrap && !args.apply) {
    logger(`[ingest] dry-run: --bootstrap 会标 ${plan.legacy.length} 个 legacy_acked（加 --apply 才写）`);
  }
  return {
    applied: Boolean(args.apply),
    files: files.length,
    budget: args.budget,
    todo: plan.todo.length,
    legacy: plan.legacy.length,
    overBudget: plan.overBudget.length,
    done: plan.done.length,
  };
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) {
    console.log('用法：node scripts/memory-ingest.mjs [--db <path>] [--sessions <dir>] [--budget 20]');
    console.log('       [--apply] [--bootstrap] [--backfill <path|YYYY-MM-DD>] [--verify]');
    return;
  }
  run(args);
}

const isCli = process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href;
if (isCli) {
  main().catch((error) => {
    console.error(`失败：${error?.message || error}`);
    process.exitCode = 1;
  });
}
