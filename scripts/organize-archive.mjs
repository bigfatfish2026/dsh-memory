#!/usr/bin/env node
/**
 * 独立导出：回看 organizeCache 的历史指纹。
 *
 * 主文件的正常加载路径**不读归档**，这里就是「想翻旧账」时唯一的路：
 * 解压 organize-archive/organize-YYYY-MM.jsonl.gz，按月份 / key 前缀过滤后逐行打印。
 *
 *   node scripts/organize-archive.mjs                                  # 列各月份文件条数
 *   node scripts/organize-archive.mjs --month 2026-09                   # 看某月
 *   node scripts/organize-archive.mjs --month 2026-09,2026-10 --key-prefix c8ca
 *   node scripts/organize-archive.mjs --key-prefix c8ca --json --limit 20
 *
 * 只读：不写、不解压到磁盘、不碰 sleep.state.json。
 */
import fs from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import {
  archiveDirOf, listArchiveMonths, readArchiveRows,
} from '../lib/organize-cache.js';

function usage(code = 1) {
  console.error(
    '用法：node scripts/organize-archive.mjs [--archive-dir <dir>] [--state <sleep.state.json>] '
      + '[--month YYYY-MM[,YYYY-MM]] [--key-prefix <hex>] [--limit N] [--json]',
  );
  process.exit(code);
}

export function parseArgs(argv = process.argv.slice(2)) {
  const out = { archiveDir: '', state: '', months: [], prefix: '', limit: 0, json: false };
  for (let i = 0; i < argv.length; i += 1) {
    const a = argv[i];
    if (a === '--archive-dir') out.archiveDir = argv[++i] || '';
    else if (a === '--state') out.state = argv[++i] || '';
    else if (a === '--month') out.months = String(argv[++i] || '').split(',').map((s) => s.trim()).filter(Boolean);
    else if (a === '--key-prefix') out.prefix = String(argv[++i] || '').trim();
    else if (a === '--limit') out.limit = Math.max(0, Number(argv[++i]) || 0);
    else if (a === '--json') out.json = true;
    else if (a === '-h' || a === '--help') usage(0);
    else {
      console.error(`未知参数：${a}`);
      usage();
    }
  }
  return out;
}

function monthOfName(name) {
  const m = /^organize-(.+)\.jsonl\.gz$/.exec(name);
  return m ? m[1] : '';
}

export function exportArchive({ archiveDir = '', state = '', months = [], prefix = '', limit = 0, json = false, log = console.log } = {}) {
  const dir = archiveDir || archiveDirOf({ stateFile: state || undefined });
  const files = listArchiveMonths(dir);
  if (!files.length) {
    log(`归档目录里没有月份文件：${dir}`);
    return { dir, files: [], rows: [] };
  }
  const picked = months.length ? files.filter((f) => months.includes(monthOfName(f.name))) : files;
  if (!picked.length) {
    log(`没有匹配的月份：${months.join(',')}；目录里有 ${files.map((f) => monthOfName(f.name)).join(', ')}`);
    return { dir, files, rows: [] };
  }
  if (!months.length && !prefix && !limit && !json) {
    log(`归档目录：${dir}`);
    for (const f of files) {
      const month = monthOfName(f.name);
      log(f.error
        ? `  ${month}  ${f.bytes} 字节  ⚠ 解压失败：${f.error}`
        : `  ${month}  ${f.count} 条  ${f.bytes} 字节  ${f.name}`);
    }
    return { dir, files, rows: [] };
  }

  const rows = [];
  for (const f of picked) {
    if (f.error) {
      log(`⚠ ${f.name} 解压失败，跳过：${f.error}`);
      continue;
    }
    const month = monthOfName(f.name);
    for (const row of readArchiveRows(f.file)) {
      if (prefix && !String(row.key || '').startsWith(prefix)) continue;
      rows.push({ month, ...row });
    }
  }
  const sliced = limit > 0 ? rows.slice(0, limit) : rows;
  if (json) {
    log(JSON.stringify({ dir, months: picked.map((f) => monthOfName(f.name)), count: sliced.length, rows: sliced }, null, 2));
    return { dir, files, rows: sliced };
  }
  log(`共 ${rows.length} 条${limit ? `，显示前 ${sliced.length}` : ''} 条（目录 ${dir}）`);
  for (const row of sliced) {
    const t = Number(row.value?.t) || 0;
    const at = t ? new Date(t).toISOString() : '-';
    log(`${row.month}  ${row.key}  v=${row.value?.v ?? '?'}  t=${at}  archivedAt=${row.archivedAt || '-'}`);
  }
  return { dir, files, rows: sliced };
}

if (globalThis.process?.argv?.[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const args = parseArgs();
  try {
    exportArchive(args);
  } catch (error) {
    console.error(`读取归档失败：${error?.message || error}`);
    process.exit(1);
  }
}
