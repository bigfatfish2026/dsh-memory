#!/usr/bin/env node
/**
 * 一次性迁移：把 sleep.state.json 的 organizeCache 从「只增不减」切成「主文件留 N 条 +
 * 溢出按月归档」。
 *
 * 迁移前先备份 <state>.bak-organize-rotate-<时间戳>；然后调 lib/organize-cache.js 的
 * trimOrganizeCache 把最旧、超出 organizeCacheMax（默认 200）的条目按写入月份搬进
 * 同级 organize-archive/organize-YYYY-MM.jsonl.gz，最后原子写回主文件。
 *
 * 幂等：条目搬走就不再出现在主文件里；归档里同 key 重复也会跳过。重复跑直接返回未轮转。
 * 安全：归档任何一步失败 → 主文件一个字节不动、退出码 1；真文件路径必须加 --yes 才动。
 *
 *   node scripts/migrate-organize-cache.mjs --state /tmp/sleep.state.json            # 副本演练
 *   node scripts/migrate-organize-cache.mjs --state <真文件> --yes                    # 主会话批准后才跑
 *   node scripts/migrate-organize-cache.mjs --state ... --dry-run                     # 只看计划
 */
import fs from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { resolveMemoryFile } from './lib/db.mjs';
import {
  trimOrganizeCache, archiveDirOf, cacheMaxOf, archiveFileOf, readArchiveRows,
} from '../lib/organize-cache.js';

export const LIVE_STATE = resolveMemoryFile('sleep.state.json');

function usage(code = 1) {
  console.error(
    '用法：node scripts/migrate-organize-cache.mjs --state <sleep.state.json> [--max 200] '
      + '[--archive-dir <dir>] [--dry-run] [--yes]',
  );
  process.exit(code);
}

export function parseArgs(argv = process.argv.slice(2)) {
  const out = { state: '', max: 0, archiveDir: '', dryRun: false, yes: false };
  for (let i = 0; i < argv.length; i += 1) {
    const a = argv[i];
    if (a === '--state') out.state = argv[++i] || '';
    else if (a === '--max') out.max = Number(argv[++i]) || 0;
    else if (a === '--archive-dir') out.archiveDir = argv[++i] || '';
    else if (a === '--dry-run') out.dryRun = true;
    else if (a === '--yes') out.yes = true;
    else if (a === '-h' || a === '--help') usage(0);
    else {
      console.error(`未知参数：${a}`);
      usage();
    }
  }
  return out;
}

function stamp() {
  return new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
}

function readJson(file) {
  return JSON.parse(fs.readFileSync(file, 'utf8'));
}

function writeJsonAtomic(file, value) {
  const tmp = `${file}.tmp-${process.pid}`;
  fs.writeFileSync(tmp, JSON.stringify(value, null, 2), 'utf8');
  fs.renameSync(tmp, file);
}

/** 归档目录里全部 key（跨月份），用来核对守恒。 */
function archivedKeys(dir) {
  const keys = new Set();
  let months = [];
  try {
    months = fs.readdirSync(dir).filter((n) => /^organize-.*\.jsonl\.gz$/.test(n));
  } catch {
    return { keys, months };
  }
  for (const name of months) {
    for (const row of readArchiveRows(path.join(dir, name))) {
      if (row && typeof row.key === 'string') keys.add(row.key);
    }
  }
  return { keys, months };
}

export function migrate({ stateFile, max = 0, archiveDir = '', dryRun = false, log = console.log } = {}) {
  const statePath = path.resolve(stateFile);
  const state = readJson(statePath);
  const cache = state.organizeCache && typeof state.organizeCache === 'object' ? state.organizeCache : {};
  const before = Object.keys(cache).length;
  const cfg = {
    stateFile: statePath,
    organizeCacheMax: max || cacheMaxOf({}),
    organizeArchiveDir: archiveDir,
  };
  const dir = archiveDirOf(cfg);
  const limit = cacheMaxOf(cfg);
  log(`迁移对象：${statePath}`);
  log(`迁移前：organizeCache ${before} 条，主文件 ${fs.statSync(statePath).size} 字节，上限 ${limit}，归档目录 ${dir}`);

  if (before <= limit) {
    log(`未超过上限（${before} ≤ ${limit}），什么都不用做；重复跑是安全的。`);
    return { changed: false, before, after: before, spilled: 0, months: {}, dir };
  }
  if (dryRun) {
    const planned = {};
    for (const [, v] of Object.entries(cache)) {
      const t = Number(v?.t) || 0;
      const d = new Date(t);
      const m = t > 0 && !Number.isNaN(d.getTime())
        ? `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`
        : 'unknown';
      planned[m] = (planned[m] || 0) + 1;
    }
    const spill = Math.max(0, before - limit);
    log(`dry-run：计划搬走最旧 ${spill} 条，按月份（含主文件现存，粗算）${JSON.stringify(planned)}`);
    return { changed: false, dryRun: true, before, after: before, spilled: spill, months: planned, dir };
  }

  const backup = `${statePath}.bak-organize-rotate-${stamp()}`;
  fs.copyFileSync(statePath, backup, fs.constants.COPYFILE_EXCL);
  log(`已备份：${backup}（${fs.statSync(backup).size} 字节）`);

  const verdict = trimOrganizeCache(cfg, cache, { warn: (l) => log(`WARN ${l}`) });
  if (verdict.failed) {
    log(`归档失败，主文件保持原样未写：${verdict.error}`);
    log(`备份保留在 ${backup}，原文件一个字节没动。`);
    return { changed: false, failed: true, error: verdict.error, before, after: before, spilled: 0, months: {}, dir, backup };
  }

  const next = { ...state, organizeCache: verdict.kept };
  writeJsonAtomic(statePath, next);
  const after = Object.keys(verdict.kept).length;
  const bytesAfter = fs.statSync(statePath).size;

  const { keys: allArchived, months } = archivedKeys(dir);
  const keptKeys = new Set(Object.keys(verdict.kept));
  const overlap = [...keptKeys].filter((k) => allArchived.has(k)).length;
  const conserved = before === after + verdict.spilled && overlap === 0;
  for (const month of Object.keys(verdict.months)) {
    const file = archiveFileOf(dir, month);
    log(`归档 ${month}：本月新写 ${verdict.months[month]} 行，文件累计 ${allArchived.size} 个 key / ${fs.statSync(file).size} 字节`);
  }
  log(`迁移后：主文件 ${after} 条，${bytesAfter} 字节（${bytesAfter < fs.statSync(backup).size ? '更小' : '没变小'}）`);
  log(`守恒核对：${before} = ${after} + ${verdict.spilled} → ${conserved ? '通过' : '不通过'}；主文件与归档 key 重叠 ${overlap} 个；归档月份文件 ${months.length} 个`);
  return {
    changed: true,
    before,
    after,
    spilled: verdict.spilled,
    written: verdict.written,
    months: verdict.months,
    dir,
    backup,
    bytesBefore: fs.statSync(backup).size,
    bytesAfter,
    conserved,
    overlap,
  };
}

if (globalThis.process?.argv?.[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const args = parseArgs();
  if (!args.state) usage();
  if (path.resolve(args.state) === LIVE_STATE && !args.yes) {
    console.error('这是线上真文件。确认要迁移再加 --yes（先拿副本演练过再说）。');
    process.exit(2);
  }
  try {
    const r = migrate({
      stateFile: args.state,
      max: args.max,
      archiveDir: args.archiveDir,
      dryRun: args.dryRun,
    });
    process.exit(r?.failed ? 1 : 0);
  } catch (error) {
    console.error(`迁移失败：${error?.message || error}`);
    process.exit(1);
  }
}
