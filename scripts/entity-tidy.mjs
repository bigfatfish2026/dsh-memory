#!/usr/bin/env node
/**
 * 实体整理（三期）真库入口：node scripts/entity-tidy.mjs [--dry-run|--apply|--undo <id>|--undo-run <run_id>] [--db 路径]
 *
 *   --dry-run（默认）  只读扫一遍，打印「会合多少组、每组是哪几个实体、依据、搬多少 surface/mount 行」，
 *                      以及不确定的那批候选清单。一个字节都不写库。
 *   --apply            先把 memory.db 复制成 backup/memory-before-entity-tidy-<时间戳>.db，
 *                      再按扫描结果合确定同义的那批，每次合并写一行 entity_merge 当撤销凭据，
 *                      最后写一份 /memory/entity-tidy-report.md（合了哪些对 / 跳过哪些 / 怎么撤）。
 *   --undo <id>        按 entity_merge.id 撤销一次合并（不给 id 就撤最近一次未撤销的）。
 *   --undo-run <id>    按 run_id 整批撤回（nightly 的 run_id 形如 sleep_2026-11-15）。
 *
 * 阈值口径见 lib/entity-tidy.js 文件头：折叠同名 / 归一同名 / 写法层交叉才自动合；
 * 单复数、版本剥壳、前缀、Jaccard 相似一律只报。
 */
import fs from 'node:fs';
import path from 'node:path';
import { openDb, resolveDbPath } from './lib/db.mjs';
import {
  scanEntityTidy, applyEntityTidy, undoEntityMerge, undoEntityTidyRun, writeEntityTidyReport, AUTO_BUCKETS,
} from '../lib/entity-tidy.js';

const argv = process.argv.slice(2);
const argOf = (k, d) => (argv.includes(k) ? argv[argv.indexOf(k) + 1] : d);
const DB = path.resolve(argOf('--db', resolveDbPath()));
const MODE = argv.includes('--apply')
  ? 'apply'
  : argv.includes('--undo-run')
    ? 'undo-run'
    : argv.includes('--undo')
      ? 'undo'
      : 'dry-run';
const UNDO_ID = argv.includes('--undo') ? Number(argOf('--undo', 0)) : 0;
const UNDO_RUN = argv.includes('--undo-run') ? String(argOf('--undo-run', '')) : '';
const JSON_OUT = argv.includes('--json');

const say = (line) => { if (!JSON_OUT) console.log(line); };

function backup(dbPath) {
  const dir = path.join(path.dirname(dbPath), 'backup');
  fs.mkdirSync(dir, { recursive: true });
  const stamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
  const dst = path.join(dir, `memory-before-entity-tidy-${stamp}.db`);
  fs.copyFileSync(dbPath, dst);
  // WAL 里没 checkpoint 的内容也要带上，否则备份可能是旧页
  for (const suffix of ['-wal', '-shm']) {
    if (fs.existsSync(`${dbPath}${suffix}`)) fs.copyFileSync(`${dbPath}${suffix}`, `${dst}${suffix}`);
  }
  return dst;
}

function main() {
  const db = openDb(DB);
  db.exec('PRAGMA busy_timeout = 15000;');

  if (MODE === 'undo' || MODE === 'undo-run') {
    if (MODE === 'undo-run' && !UNDO_RUN) {
      console.error('--undo-run 需要 run_id，例如 --undo-run sleep_2026-11-15');
      db.close();
      return 2;
    }
    const out = MODE === 'undo-run'
      ? undoEntityTidyRun(db, { runId: UNDO_RUN, log: say })
      : undoEntityMerge(db, { mergeId: UNDO_ID, log: say });
    if (JSON_OUT) console.log(JSON.stringify(out));
    if (!out.ok) say(`[entity-tidy] 撤回失败：${out.reason || 'unknown'}`);
    db.close();
    return out.ok ? 0 : 1;
  }

  const scan = scanEntityTidy(db, { log: say });
  const { stats } = scan;

  if (MODE === 'dry-run') {
    say('');
    say(`会合并 ${stats.autoMerges} 组 / 涉及 ${stats.autoEntities} 个实体（${stats.autoGroups} 个连通组）`);
    say(`影响：entity_surface ${stats.surfaceMoves} 行、memory_entity ${stats.mountMoves} 行（合计 ${stats.surfaceMoves + stats.mountMoves} 行）`);
    say('');
    for (const g of scan.auto) {
      const dst = g.dst;
      say(`组 ${g.entities.map((e) => e.id).join('/')}  依据 ${g.basis.join('+')}  保留 → ${dst.id}(${dst.display})`);
      for (const e of g.entities) {
        const mark = e.id === dst.id ? '★' : '→';
        say(`  ${mark} ${e.id} ${e.display}  norm=${e.norm} etype=${e.etype} mount=${e.memCount}`);
      }
      say(`  搬 surface ${g.surfaceMoves} 行 / mount ${g.mountMoves} 行`);
    }
    if (scan.report.length) {
      const byBasis = {};
      for (const r of scan.report) byBasis[r.basis] = (byBasis[r.basis] || 0) + 1;
      say('');
      say(`只报不合并：${scan.report.length} 对（${Object.entries(byBasis).map(([k, n]) => `${k} ${n}`).join(' / ')}）`);
      const top = scan.report.slice(0, 30);
      for (const r of top) {
        say(`  ${r.basis} score=${r.score}  ${r.a.id}(${r.a.display})  vs  ${r.b.id}(${r.b.display})  ${r.detail}`);
      }
      if (scan.report.length > top.length) say(`  ……另外 ${scan.report.length - top.length} 对，加 --json 看全量`);
    }
    if (JSON_OUT) console.log(JSON.stringify({ mode: 'dry-run', stats, auto: scan.auto, report: scan.report }));
    db.close();
    return 0;
  }

  // apply
  const file = backup(DB);
  say(`[entity-tidy] 备份 ${file}`);
  const runId = `tidy_${Date.now().toString(36)}`;
  // entity_surface_epoch 由 applyEntityTidy 自己 bump（撤销也 bump），这里不再单独写。
  const applied = applyEntityTidy(db, { plan: scan.auto, runId, log: say });
  say(`[entity-tidy] 完成 run=${runId} 合并 ${applied.merged} 次 surface ${applied.surfaceMoved} 行 mount ${applied.mountsMoved} 行 跳过 ${applied.skipped}`);
  // 报告与 nightly 共用一份格式（合了哪些对 / 跳过哪些候选 / 怎么撤），别维护两套。
  let reportFile = '';
  try {
    reportFile = writeEntityTidyReport(path.join(path.dirname(DB), 'entity-tidy-report.md'), {
      runId, scan, applied, log: say,
    });
  } catch (error) {
    say(`[entity-tidy] 报告写入失败：${String(error?.message || error)}`);
  }
  say(`[entity-tidy] 撤销（按批）：node scripts/entity-tidy.mjs --undo-run ${runId}；单条：--undo <entity_merge.id>`);
  if (reportFile) say(`[entity-tidy] 报告 ${reportFile}`);
  if (JSON_OUT) console.log(JSON.stringify({ mode: 'apply', runId, stats, applied, backup: file, report: reportFile }));
  db.close();
  return applied.skipped && !applied.merged ? 1 : 0;
}

process.exitCode = main();
