#!/usr/bin/env node
/**
 * 一次性清理：拆分子段的部分写入残片（2026-11-22）。
 *
 * 背景：拆分子段是一段一笔事务写进 memories 的。写到第 2 段失败（fetch failed / 覆盖未过 /
 * 写闸未决）时第 1 段已经落库；本票之前那段就是 active，能被默认召回，原条也还是 active。
 * 实锤：11-22 05:00 那轮写入 309 个活跃子段，其中 33 个来自部分写入或覆盖失败的任务。
 *
 * 本票之后新写的子段带 status='pending_split' + extra.splitGroup={source,expected,index}，
 * 不会再漏；这个脚本负责扫**存量**：
 *   1) 按组键（sleep:split:<parent> / sleep:split:queue:<key>，行上带 splitGroup 的按
 *      extra.splitGroup.source 归组）把行收组；
 *   2) 组不完整 =
 *      a. 行上记了 expected，但已落段位（按 splitGroup.index）盖不满；
 *      b. 或同组的 split_item op 还没收尾（status=pending/running，或 last_error 命中
 *         partial_write / write_gate_pending / split_group_incomplete / archive_failed /
 *         done_group_incomplete / done_parent_not_archived）；
 *   3) 组不完整时，组里「召回可见的 active」或「中间态 pending_split」的行就是「残片」，
 *      默认只列不写。可见性判据不在这里另写一份 status 清单：直接复用
 *      src/store/rows.ts 的 isActive 与 src/store/split-group.ts 的 isPendingSplit
 *      （旧版本写死 status IN ('active','pending_split')，而本库 active 存的是 NULL，
 *      导致 dry-run 恒为 0 组、--apply 空跑）。
 *   4) 完整性只看段位：have < expected 才算不完整。op 的 status/last_error 只作附注，
 *      不参与判定——done/dead 上残留的 partial_write:i/n 不许再把完整组打成残片。
 *      expected 仍可从 op 的 partial_write:i/n 里取（老数据行上没有 splitGroup 标记）。
 *
 * 用法：
 *   node scripts/clean-partial-splits.mjs                 # 默认只读 dry-run，只输出清单
 *   node scripts/clean-partial-splits.mjs --json          # 机器可读清单
 *   node scripts/clean-partial-splits.mjs --apply         # 单事务归档（reason='partial-split'）+ 先备份
 *   node scripts/clean-partial-splits.mjs --db /path/memory.db
 *   node scripts/clean-partial-splits.mjs --sample 20     # 每组最多列几条正文
 *
 * 归档不删行：向量、粗实体、观察登记在写入时已经落过，删行会留孤儿且不可追查。
 * 归档行保留来源键与 extra.splitGroup，本脚本再跑一次会按「archived + partial-split」跳过。
 * --apply 只改库，当前插件进程的内存视图要等下一轮加载/重启才跟上。
 */
import fs from 'node:fs';
import path from 'node:path';
import { openDb, resolveDbPath, withTx } from './lib/db.mjs';
// 可见性判据复用召回侧那一份，别在脚本里另写 status 清单（口径见 src/store/rows.ts:137-139、
// src/store/split-group.ts:82-84）；以后 status 换取值，这里跟着召回一起变。
import { isActive } from '../lib/store/rows.js';
import { isPendingSplit } from '../lib/store/split-group.js';

export const PARTIAL_SPLIT_REASON = 'partial-split';

/** 扫描/归档的候选：召回可见的 active（含 NULL / 空串）或中间态 pending_split。 */
function isCandidate(row) {
  return isActive(row) || isPendingSplit(row);
}

const argv = process.argv.slice(2);
const has = (k) => argv.includes(k);
const argOf = (k, d) => (argv.includes(k) ? argv[argv.indexOf(k) + 1] : d);

function parseJson(raw) {
  if (!raw || typeof raw !== 'string') return null;
  try {
    const out = JSON.parse(raw);
    return out && typeof out === 'object' ? out : null;
  } catch {
    return null;
  }
}

/** 行上的组标记（extra 平铺在 extra 列里）。 */
function markOfRow(row) {
  const extra = parseJson(row.extra);
  const mark = extra && (extra.splitGroup || extra.__splitGroup);
  if (!mark || typeof mark !== 'object') return null;
  const index = Number(mark.index);
  const expected = Number(mark.expected);
  return {
    source: String(mark.source || ''),
    expected: Number.isInteger(expected) && expected > 0 ? expected : 0,
    index: Number.isInteger(index) && index >= 0 ? index : null,
  };
}

function opGroupKey(op) {
  const payload = parseJson(op.payload_json) || {};
  const sourceId = String(payload.source_id || '').trim();
  if (sourceId) return `sleep:split:${sourceId}`;
  const key = String(op.dedupe_key || '').trim();
  return key ? `sleep:split:queue:${key}` : '';
}

/** 组里还活着的 op（pending / running）：只做附注，不参与完整性判定。 */
function opAlive(op) {
  const status = String(op.status || '').toLowerCase();
  return status === 'pending' || status === 'running';
}

function opNote(ops) {
  if (!ops.length) return '';
  return ops
    .map((op) => `#${op.id} status=${op.status}${op.last_error ? ` last_error=${String(op.last_error).slice(0, 60)}` : ''}`)
    .join('；');
}

/**
 * expected 的另一个来源：pending_ops 的 last_error 里 partial_write:i/n。
 * 只取 n 当段位总数（老数据行上没有 splitGroup 标记），不拿它判收尾与否。
 */
function expectedFromOps(ops) {
  let best = 0;
  for (const op of ops) {
    const m = /partial_write:\d+\/(\d+)/.exec(String(op.last_error || ''));
    if (m) best = Math.max(best, Number(m[1]) || 0);
  }
  return best;
}

/**
 * 只读扫描：返回 { groups, rows, scanned }。
 * 每个 group：{ key, expected, complete, have, missing, causes, rows:[...] }
 */
export function scanPartialSplits(db, { sample = 10 } = {}) {
  const out = { scanned: 0, archived: 0, groups: [], rows: [] };
  let rows = [];
  try {
    // 状态过滤交给 isActive / isPendingSplit 在 JS 里做，SQL 只按来源圈范围，
    // 这样 status 换取值也不用改这条 SQL。
    rows = db.prepare(
      `SELECT id, text, source, status, reason, extra, ts, rowSeq FROM memories
        WHERE source LIKE 'sleep:split:%' OR source = 'split' OR extra LIKE '%"splitGroup"%'`,
    ).all().filter(isCandidate);
  } catch (error) {
    throw new Error(`memories 读取失败（库结构不对？）：${String(error?.message || error)}`);
  }
  if (!rows.length) return out;

  let ops = [];
  try {
    ops = db.prepare("SELECT id, dedupe_key, payload_json, status, last_error, attempts FROM pending_ops WHERE kind = 'split_item'").all();
  } catch {
    ops = []; // 老库没有 pending_ops：只按行上的 expected 判
  }

  const groups = new Map();
  const groupOf = (key) => {
    if (!groups.has(key)) groups.set(key, { key, rows: [], expected: 0, ops: [] });
    return groups.get(key);
  };

  for (const row of rows) {
    const mark = markOfRow(row);
    const key = (mark && mark.source) || String(row.source || '');
    if (!key) continue;
    const g = groupOf(key);
    g.rows.push({
      id: String(row.id),
      status: row.status === null || row.status === undefined ? null : String(row.status),
      text: String(row.text || ''),
      index: mark ? mark.index : null,
      reason: String(row.reason || ''),
      ts: String(row.ts || ''),
    });
    if (mark && mark.expected > g.expected) g.expected = mark.expected;
  }

  const opsByGroup = new Map();
  for (const op of ops) {
    const key = opGroupKey(op);
    if (!key) continue;
    if (!opsByGroup.has(key)) opsByGroup.set(key, []);
    opsByGroup.get(key).push(op);
  }

  for (const [key, g] of groups) {
    const groupOps = opsByGroup.get(key) || [];
    const expected = Math.max(g.expected, expectedFromOps(groupOps));
    const causes = [];
    let complete = false;
    let have = 0;
    const missing = [];
    const indexed = new Set(g.rows.filter((r) => r.index !== null).map((r) => r.index));
    if (expected > 0) {
      if (indexed.size > 0) {
        have = indexed.size;
        for (let i = 0; i < expected; i += 1) if (!indexed.has(i)) missing.push(i);
      } else {
        // 老数据没有 index：只能按行数数
        have = g.rows.length;
      }
      complete = have >= expected;
      if (!complete) causes.push(`段位 ${have}/${expected}${missing.length ? ` missing=${missing.join(',')}` : ''}`);
    } else {
      // 段位总数不知道就没法按 have<expected 判，op 说了也不算：不碰。
      complete = true;
      causes.push('expected=unknown（没有组标记，op 的 last_error 不参判），跳过');
    }
    // op 只作附注：活着的 op 提示这组可能还会被重试，done/dead 的 last_error 残留不参判。
    const alive = groupOps.filter(opAlive);
    if (groupOps.length) causes.push(`op 附注（不参判）${alive.length ? '，其中未收尾' : ''}：${opNote(groupOps)}`);
    const candidates = g.rows.filter(isCandidate);
    out.scanned += candidates.length;
    if (complete || !candidates.length) continue;
    out.groups.push({
      key,
      expected,
      have,
      complete,
      missing,
      causes,
      rows: candidates.slice(0, Math.max(1, Number(sample) || 10)),
      rowCount: candidates.length,
    });
    out.rows.push(...candidates);
  }
  out.archived = out.rows.length;
  return out;
}

/** 单事务归档（--apply 的唯一写入口）。 */
export function applyPartialSplitCleanup(db, rows, { now = Date.now() } = {}) {
  const ids = [...new Set((rows || []).map((r) => String(r.id)))];
  if (!ids.length) return 0;
  const at = now;
  return withTx(db, () => {
    const read = db.prepare('SELECT status FROM memories WHERE id = ?');
    const stmt = db.prepare('UPDATE memories SET status = ?, reason = ?, updatedAt = ? WHERE id = ?');
    let n = 0;
    for (const id of ids) {
      const current = read.get(id);
      // 扫描到落地之间状态被改过的行一律跳过；可见性判据同召回（isActive / isPendingSplit）。
      if (!current || !isCandidate(current)) continue;
      n += Number(stmt.run('archived', PARTIAL_SPLIT_REASON, new Date(at).toISOString(), id).changes) || 0;
    }
    // 可追溯：把这次归档的 id 清单记进 meta（有 meta 表才写，老库没有就跳过）
    try {
      db.prepare('INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)')
        .run(`clean_partial_splits_${at.toString(36)}`, JSON.stringify({ at, reason: PARTIAL_SPLIT_REASON, ids }));
    } catch {
      /* meta 表不在：审计信息只在输出里 */
    }
    return n;
  });
}

function backupDb(dbPath) {
  const stamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
  const dst = `${dbPath}.clean-partial-${stamp}.bak`;
  fs.copyFileSync(dbPath, dst);
  for (const suffix of ['-wal', '-shm']) {
    if (fs.existsSync(`${dbPath}${suffix}`)) fs.copyFileSync(`${dbPath}${suffix}`, `${dst}${suffix}`);
  }
  return dst;
}

function brief(text, n = 40) {
  const s = String(text || '').replace(/\s+/g, ' ');
  return s.length > n ? `${s.slice(0, n)}…` : s;
}

function main() {
  const dbPath = resolveDbPath(argOf('--db', null));
  const mode = has('--apply') ? 'apply' : 'dry-run';
  const asJson = has('--json');
  const sample = Math.max(1, Number(argOf('--sample', 10)) || 10);
  const db = openDb(dbPath, { timeout: 5000 });
  try {
    db.exec('PRAGMA busy_timeout = 5000');
    const report = scanPartialSplits(db, { sample });
    if (asJson) {
      process.stdout.write(`${JSON.stringify({ db: dbPath, mode, ...report }, null, 2)}\n`);
    } else {
      process.stdout.write(
        `[clean-partial-splits] db=${dbPath} mode=${mode} 残片组 ${report.groups.length} 个 / 行 ${report.archived}\n`,
      );
      for (const g of report.groups) {
        process.stdout.write(
          `\n组 ${g.key} expected=${g.expected || 'unknown'} 已落=${g.have}/${g.expected || '?'}` +
            `${g.missing.length ? ` 缺段=[${g.missing.join(',')}]` : ''}\n`,
        );
        for (const cause of g.causes) process.stdout.write(`  因由：${cause}\n`);
        for (const r of g.rows) {
          process.stdout.write(`  行 ${r.id} status=${r.status === null ? 'active(null)' : r.status} idx=${r.index === null ? '?' : r.index} 「${brief(r.text)}」\n`);
        }
        if (g.rowCount > g.rows.length) process.stdout.write(`  …还有 ${g.rowCount - g.rows.length} 行（--sample 调整）\n`);
      }
      if (!report.groups.length) process.stdout.write('没有发现组不完整且仍可召回的残片。\n');
      if (mode === 'dry-run') process.stdout.write('\n[clean-partial-splits] 只读：没有改库。加 --apply 才归档。\n');
    }
    if (mode !== 'apply' || !report.rows.length) return 0;
    const backup = backupDb(dbPath);
    const changed = applyPartialSplitCleanup(db, report.rows);
    if (asJson) {
      process.stdout.write(`${JSON.stringify({ applied: changed, backup, reason: PARTIAL_SPLIT_REASON })}\n`);
    } else {
      process.stdout.write(
        `\n[clean-partial-splits] 已归档 ${changed} 行 reason=${PARTIAL_SPLIT_REASON}，备份 ${backup}\n` +
          '（当前插件进程的内存视图要等下一轮加载/重启才跟上）\n',
      );
    }
    return 0;
  } finally {
    db.close();
  }
}

const isMain = process.argv[1] && path.resolve(process.argv[1]) === path.resolve(new URL(import.meta.url).pathname);
if (isMain) {
  try {
    process.exitCode = main();
  } catch (error) {
    process.stderr.write(`[clean-partial-splits] 失败：${String(error?.message || error)}\n`);
    process.exitCode = 1;
  }
}
