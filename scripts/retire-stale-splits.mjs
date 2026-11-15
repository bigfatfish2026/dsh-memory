#!/usr/bin/env node
/**
 * 退役「重试也不会变」的陈年 split_item。
 *
 * 背景：pending_ops(kind='split_item') 里有一批 attempts>=2 的条目，last_error 是
 * inflate / unavoidable / lint_reject_remaining_lt_2 / part_too_long 这类确定性失败：
 * 原文结构不变、规则不变，下一轮重放只会再叫一次模型（20-30 秒），结果一样。
 * 早先的判死线是 attempts>=5（lib/ingest-index.js MAX_ATTEMPTS），inflate 另有
 * lib/pipeline-split.js 的 attempts>=5 兜底；冲到 5 之前它们会一轮轮重叫模型。
 * 本脚本把这批提前标 dead，留审计、留 undo，不删行。
 *
 * 判定（默认）：
 *   kind='split_item' AND status='pending' AND attempts>=2
 *   AND last_error 命中确定不会变的组：
 *     inflate                         拆完总字数 > 原文 120%，同原文同规则重拆无解
 *     unavoidable                     原文没有合法切点（句号/分号），模型明确拒绝硬拆
 *     lint_reject_remaining_lt_2      lint 重写一次后仍凑不满 2 段
 *     part_too_long                   子段本身仍超 200 字
 *   不碰：attempts<=1（正常排队/刚到 1 次）、status 已是 done/dead/running、
 *         last_error 为 NULL/空（那是新入队、还没试过的条目）。
 *
 * 写库：status='dead'，last_error 前缀 `retired:`，updated_ts=now；
 * 原 status/attempts/last_error/updated_ts 原样存 meta（key=retire_splits_<ts36>），
 * --undo 照这份记录一条条改回。
 *
 * 用法：
 *   node scripts/retire-stale-splits.mjs                     # dry-run（默认）
 *   node scripts/retire-stale-splits.mjs --apply             # 单事务 + 备份
 *   node scripts/retire-stale-splits.mjs --verify            # 断言 apply 结果
 *   node scripts/retire-stale-splits.mjs --undo [--id <metaKey>]  # 逐条还原
 *   公共参数：--db /path/to/memory.db  --backup-dir <dir>  --sample 10  --seed <n>
 */
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { openDb, resolveDbPath } from './lib/db.mjs';

const DEFAULT_DB = resolveDbPath();
const argv = process.argv.slice(2);
const has = (k) => argv.includes(k);
const argOf = (k, d) => (argv.includes(k) ? argv[argv.indexOf(k) + 1] : d);

const DB_PATH = argOf('--db', DEFAULT_DB);
const BACKUP_DIR = argOf('--backup-dir', path.dirname(DB_PATH));
const SAMPLE_N = Math.max(1, Number(argOf('--sample', 10)) || 10);
const SEED = Number(argOf('--seed', 20261114)) || 20261114;
const MODE = has('--apply') ? 'apply' : has('--verify') ? 'verify' : has('--undo') ? 'undo' : 'dry-run';
/** --only=inflate,unavoidable 收窄组；不传 = RETIRE_RULES 全组。 */
const ONLY = (argOf('--only', '') || '')
  .split(',')
  .map((s) => s.trim())
  .filter(Boolean);

/**
 * 确定性失败组。规则按 last_error 前缀匹配；顺序即输出顺序。
 * 只认「原文/规则不变，重放必然同果」的组，split_error / write_gate_pending 这类偶发错误不在内。
 */
export const RETIRE_RULES = [
  { key: 'inflate', test: (e) => /^inflate/.test(e) },
  { key: 'unavoidable', test: (e) => /^unavoidable/.test(e) },
  { key: 'format_broken', test: (e) => /^(lint_reject_remaining_lt_2|part_too_long)\b/.test(e) },
  { key: 'empty_text', test: (e) => /^(empty_text|no_text|empty_content|source_missing)\b/.test(e) },
];

export function classifyError(err, only = null) {
  const e = String(err ?? '');
  if (!e.trim()) return null; // NULL/空 = 没试过，绝不动
  const allow = Array.isArray(only) && only.length ? new Set(only) : null;
  for (const rule of RETIRE_RULES) {
    if (allow && !allow.has(rule.key)) continue;
    if (rule.test(e)) return rule.key;
  }
  return null;
}

const db = () => {
  const d = openDb(DB_PATH, { timeout: 5000 });
  d.exec('PRAGMA busy_timeout = 5000');
  return d;
};
const ro = () => {
  const d = db();
  d.exec('PRAGMA query_only = 1');
  return d;
};

function localStamp(d = new Date()) {
  const p = (n) => String(n).padStart(2, '0');
  return `${d.getFullYear()}${p(d.getMonth() + 1)}${p(d.getDate())}-${p(d.getHours())}${p(d.getMinutes())}${p(d.getSeconds())}`;
}
const ts36 = (d = new Date()) => d.getTime().toString(36) + Math.random().toString(36).slice(2, 6);

function countsOf(d, kind = 'split_item') {
  const out = {};
  for (const row of d.prepare('SELECT status, count(*) AS n FROM pending_ops WHERE kind = ? GROUP BY status').all(kind)) {
    out[row.status] = row.n;
  }
  out.total = d.prepare('SELECT count(*) AS n FROM pending_ops WHERE kind = ?').get(kind).n;
  return out;
}

/**
 * 候选：attempts>=2、pending、错误命中确定性组。
 * pending_ops 没有 lease 字段：租约就是 status='running' + updated_ts（LEASE_MS=10 分钟，
 * lib/ingest-index.js:23/385）。running 的行不算候选，单独统计成 leased 报出来。
 */
export function selectCandidates(d, { minAttempts = 2, only = ONLY } = {}) {
  const rows = d
    .prepare(
      "SELECT id, attempts, status, last_error, updated_ts, payload_json, dedupe_key FROM pending_ops " +
        "WHERE kind = 'split_item' AND status = 'pending' AND attempts >= ? ORDER BY id",
    )
    .all(minAttempts);
  const picked = [];
  const skipped = [];
  for (const row of rows) {
    const group = classifyError(row.last_error, only);
    if (group) picked.push({ ...row, group });
    else skipped.push({ id: row.id, attempts: row.attempts, last_error: String(row.last_error ?? '') });
  }
  const leased = d
    .prepare(
      "SELECT id, kind, status, attempts, updated_ts, last_error FROM pending_ops " +
        "WHERE kind = 'split_item' AND status = 'running' AND attempts >= ? ORDER BY id",
    )
    .all(minAttempts);
  return { picked, skipped, leased };
}

function snippetOf(row) {
  let text = '';
  try {
    const p = JSON.parse(row.payload_json || '{}');
    text = String(p.text || '');
  } catch {
    text = '';
  }
  return text.replace(/\s+/g, ' ').trim().slice(0, 60);
}

/** 固定种子抽样，同一份数据反复跑结果一致，便于维护者对照。 */
function sample(rows, n, seed = SEED) {
  let s = seed >>> 0;
  const rand = () => {
    s |= 0;
    s = (s + 0x6d2b79f5) | 0;
    let t = Math.imul(s ^ (s >>> 15), 1 | s);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
  const copy = rows.map((r) => ({ ...r }));
  for (let i = copy.length - 1; i > 0; i -= 1) {
    const j = Math.floor(rand() * (i + 1));
    [copy[i], copy[j]] = [copy[j], copy[i]];
  }
  return copy.slice(0, n);
}

function groupBy(rows, keyOf) {
  const m = new Map();
  for (const r of rows) {
    const k = keyOf(r);
    m.set(k, (m.get(k) || 0) + 1);
  }
  return [...m.entries()].sort((a, b) => b[1] - a[1]);
}

function pct(n, total) {
  return total ? `${((n / total) * 100).toFixed(1)}%` : '0%';
}

function latestRetirement(d, key = null) {
  if (key) {
    const row = d.prepare('SELECT key, value FROM meta WHERE key = ?').get(key);
    return row ? { key: row.key, ...JSON.parse(row.value) } : null;
  }
  const row = d
    .prepare("SELECT key, value FROM meta WHERE key LIKE 'retire_splits_%' ORDER BY key DESC LIMIT 1")
    .get();
  return row ? { key: row.key, ...JSON.parse(row.value) } : null;
}

function loadRetiredIds(d, entry) {
  return entry.rows.map((r) => r.id);
}

/** 除目标 id 外整张 pending_ops 的指纹：apply 前存、verify 时重算，证明别的行一条没动。 */
function fingerprintOutside(d, ids) {
  const skip = new Set(ids);
  const h = crypto.createHash('sha256');
  for (const row of d.prepare('SELECT * FROM pending_ops ORDER BY id').all()) {
    if (skip.has(row.id)) continue;
    h.update(
      [row.id, row.kind, row.dedupe_key, row.status, row.attempts, row.next_try_at, row.last_error, row.created_ts, row.updated_ts].join('\u0001'),
    );
    h.update('\n');
  }
  return h.digest('hex');
}

function doDryRun() {
  const d = ro();
  const { picked, skipped, leased } = selectCandidates(d);
  const allPending = d
    .prepare("SELECT id, attempts, status, last_error FROM pending_ops WHERE kind='split_item' AND status='pending'")
    .all();
  const pendingGroups = groupBy(
    allPending.map((r) => ({ ...r, group: classifyError(r.last_error) || (String(r.last_error ?? '').trim() ? 'other' : 'untried') })),
    (r) => r.group,
  );

  console.log(`[dry-run] db=${DB_PATH}`);
  console.log(`[dry-run] split_item pending 总数=${allPending.length}；候选（attempts>=2 且确定性错误）=${picked.length}；attempts>=2 但不命中规则${ONLY.length ? `（--only=${ONLY.join(',')}）` : ''}=${skipped.length}；attempts>=2 但正在被领（running，有主不动）=${leased.length}`);
  console.log('[dry-run] pending 全量按错误组：');
  for (const [g, n] of pendingGroups) console.log(`  ${g}: ${n} 条 (${pct(n, allPending.length)})`);

  console.log('[dry-run] 将标 dead 的按 last_error 组：');
  for (const [g, n] of groupBy(picked, (r) => r.group)) console.log(`  ${g}: ${n} 条 (${pct(n, picked.length)})`);

  console.log('[dry-run] 将标 dead 的按 attempts：');
  for (const [a, n] of groupBy(picked, (r) => `attempts=${r.attempts}`)) console.log(`  ${a}: ${n} 条`);

  if (leased.length) {
    console.log(`[dry-run] attempts>=2 正在 running 的行（租约期内，不动）${leased.length} 条：`);
    for (const r of leased) console.log(`  id=${r.id} attempts=${r.attempts} updated=${new Date(Number(r.updated_ts)).toISOString()}`);
  }

  const others = skipped.filter((r) => r.attempts >= 2);
  if (others.length) {
    console.log(`[dry-run] attempts>=2 但错误不命中规则（保留不动）${others.length} 条：`);
    for (const r of others) console.log(`  id=${r.id} attempts=${r.attempts} err=${String(r.last_error).slice(0, 80)}`);
  }

  console.log(`[dry-run] 随机抽样 ${Math.min(SAMPLE_N, picked.length)} 条（seed=${SEED}）：`);
  for (const r of sample(picked, SAMPLE_N)) {
    console.log(`  id=${r.id} attempts=${r.attempts} err=${String(r.last_error).slice(0, 40)} | ${snippetOf(r)}`);
  }

  // 每条还要被重领的次数 = MAX_ATTEMPTS(5) - 当前 attempts；每次一条一次模型调用。
  const MAX_ATTEMPTS = 5;
  const calls = picked.reduce((n, r) => n + Math.max(0, MAX_ATTEMPTS - Number(r.attempts)), 0);
  const lo = Math.round((calls * 20) / 60);
  const hi = Math.round((calls * 30) / 60);
  console.log(
    `[dry-run] 预计执行后省下的模型调用：${calls} 次（${picked.length} 条跑到 attempts=5 之前还要被重领的总次数）` +
      `，每次 20-30 秒 ≈ ${lo}-${hi} 分钟（约 ${(calls * 20 / 3600).toFixed(2)}-${(calls * 30 / 3600).toFixed(2)} 小时）`,
  );
  d.close();
}

function doApply() {
  const d = db();
  const { picked, leased } = selectCandidates(d);
  if (!picked.length) {
    console.log('[apply] 没有候选，什么都没写');
    d.close();
    return;
  }
  const before = countsOf(d);
  const stamp = localStamp();
  const backupPath = path.join(BACKUP_DIR, `memory-before-retire-splits-${stamp}.db`);
  // 活库是 WAL：VACUUM INTO 拿一致快照，不用裸拷 -wal/-shm。
  // 只做这一份快照备份，不在活库上跑全库 VACUUM（会锁库，卡住正在跑的整理轮）。
  d.exec(`VACUUM INTO '${backupPath.replace(/'/g, "''")}'`);
  const backupSize = fs.statSync(backupPath).size;
  console.log(`[apply] 备份完成 ${backupPath} (${backupSize} bytes)`);

  const now = Date.now();
  const appliedAt = new Date(now).toISOString();
  const metaKey = `retire_splits_${ts36(new Date(now))}`;
  const targetIds = picked.map((r) => r.id);
  const rows = [];
  const skippedLease = [];
  let updated = 0;
  d.exec('BEGIN IMMEDIATE');
  try {
    // 事务内重查：apply 前一刻还在 pending 且 attempts 没变才动。
    // pending_ops 没 lease 字段，租约 = status='running'（LEASE_MS=10 分钟）。
    const freshOf = d.prepare('SELECT id, status, attempts, last_error, updated_ts FROM pending_ops WHERE id = ?');
    const eligible = [];
    for (const r of picked) {
      const fresh = freshOf.get(r.id);
      if (!fresh || fresh.status !== 'pending' || Number(fresh.attempts) !== Number(r.attempts)) {
        skippedLease.push({
          id: r.id,
          status: fresh ? fresh.status : 'missing',
          attempts: fresh ? fresh.attempts : null,
          reason: fresh ? `status=${fresh.status}` : 'row_gone',
        });
        continue;
      }
      eligible.push({ ...r, fresh });
    }
    const outsideFp = fingerprintOutside(d, eligible.map((r) => r.id));
    const stmt = d.prepare(
      "UPDATE pending_ops SET status = 'dead', last_error = ?, updated_ts = ? " +
        "WHERE id = ? AND kind = 'split_item' AND status = 'pending' AND attempts = ?",
    );
    for (const r of eligible) {
      const prevError = String(r.fresh.last_error ?? '');
      const newError = `retired:${r.group}|${appliedAt}|${prevError.slice(0, 400)}`;
      const res = stmt.run(newError, now, r.id, r.fresh.attempts);
      if (res.changes !== 1) throw new Error(`id=${r.id} 更新行数=${res.changes}（事务内重查后仍不匹配）`);
      updated += 1;
      rows.push({
        id: r.id,
        prev_status: r.fresh.status,
        prev_attempts: r.fresh.attempts,
        prev_last_error: prevError,
        prev_updated_ts: r.fresh.updated_ts,
        group: r.group,
        retired_at: appliedAt,
      });
    }
    // 事务内自证：目标行之外整表指纹更新前后必须一致（本事务没碰别的行）。
    const fpAfter = fingerprintOutside(d, rows.map((r) => r.id));
    if (fpAfter !== outsideFp) throw new Error('事务内非目标行指纹发生变化，整批回滚');
    const after = countsOf(d);
    const record = {
      appliedAt,
      db: DB_PATH,
      backup: backupPath,
      backupSize,
      rule: 'attempts>=2 && status=pending && last_error in (inflate|unavoidable|lint_reject_remaining_lt_2|part_too_long)',
      before,
      after,
      outsideFingerprint: fpAfter,
      skippedLease,
      rows,
    };
    d.prepare('INSERT INTO meta (key, value) VALUES (?, ?)').run(metaKey, JSON.stringify(record));
    d.exec('COMMIT');
    console.log(`[apply] 单事务提交：${updated} 条标 dead；被租约/状态变化跳过 ${skippedLease.length} 条；meta=${metaKey}`);
    console.log(`[apply] 备份 md5 用 md5sum 另算；状态计数 before=${JSON.stringify(before)} after=${JSON.stringify(after)}`);
    if (skippedLease.length) {
      for (const r of skippedLease) console.log(`  skip id=${r.id} status=${r.status} attempts=${r.attempts} (${r.reason})`);
    }
    if (leased.length) {
      console.log(`[apply] 另有 attempts>=2 的 running 行 ${leased.length} 条（租约期内，未选为候选）`);
      for (const r of leased) console.log(`  running id=${r.id} attempts=${r.attempts}`);
    }
    console.log(`[apply] 回滚：node scripts/retire-stale-splits.mjs --undo --id ${metaKey}`);
  } catch (error) {
    d.exec('ROLLBACK');
    console.error(`[apply] 失败已回滚：${String((error && error.message) || error)}`);
    console.error(`[apply] 备份仍在 ${backupPath}，库未被改动`);
    process.exitCode = 1;
  } finally {
    d.close();
  }
}

function doVerify() {
  const d = ro();
  const entry = latestRetirement(d, argOf('--id', null));
  if (!entry) {
    console.error('[verify] meta 里没有 retire_splits_* 记录，先跑 --apply');
    process.exitCode = 2;
    d.close();
    return;
  }
  const cur = countsOf(d);
  const ids = loadRetiredIds(d, entry);
  const undone = Boolean(entry.undoneAt);
  const placeholders = ids.map(() => '?').join(',');
  let checks;
  if (undone) {
    // undo 之后再 verify：逐条比对 apply 前存的四个字段，计数回到基线。
    const dbRows = new Map(
      d.prepare(`SELECT id, status, attempts, last_error, updated_ts FROM pending_ops WHERE id IN (${placeholders})`).all(...ids).map((r) => [r.id, r]),
    );
    const mismatched = entry.rows.filter((r) => {
      const cur = dbRows.get(r.id);
      return (
        !cur ||
        cur.status !== r.prev_status ||
        Number(cur.attempts) !== Number(r.prev_attempts) ||
        String(cur.last_error ?? '') !== String(r.prev_last_error ?? '') ||
        Number(cur.updated_ts) !== Number(r.prev_updated_ts)
      );
    });
    checks = [
      ['目标条目逐条回到原值（status/attempts/last_error/updated_ts）', mismatched.length === 0, `${mismatched.length} 条不符`],
      ['dead 总数回到基线', Number(cur.dead || 0) === Number(entry.before.dead || 0), `${cur.dead} vs ${entry.before.dead}`],
      ['pending 总数回到基线', Number(cur.pending || 0) === Number(entry.before.pending || 0), `${cur.pending} vs ${entry.before.pending}`],
      ['总行数不变', Number(cur.total) === Number(entry.before.total), `${cur.total} vs ${entry.before.total}`],
    ];
  } else {
    const nowDead = d
      .prepare(`SELECT count(*) AS n FROM pending_ops WHERE id IN (${placeholders}) AND status = 'dead' AND last_error LIKE 'retired:%'`)
      .get(...ids).n;
    const expectedDead = Number(entry.before.dead || 0) + ids.length;
    const expectedPending = Number(entry.before.pending || 0) - ids.length;
    const fpOk = entry.outsideFingerprint ? fingerprintOutside(d, ids) === entry.outsideFingerprint : null;
    checks = [
      ['目标条目全部为 dead 且带 retired: 前缀', nowDead === ids.length, `${nowDead}/${ids.length}`],
      ['dead 总数 = 基线 dead + 本轮退役数', Number(cur.dead || 0) === expectedDead, `${cur.dead} vs ${expectedDead}`],
      ['pending 总数 = 基线 pending - 本轮退役数', Number(cur.pending || 0) === expectedPending, `${cur.pending} vs ${expectedPending}`],
      ['总行数不变', Number(cur.total) === Number(entry.before.total), `${cur.total} vs ${entry.before.total}`],
      ['非目标行指纹一致（别的行一条没动）', fpOk !== false, fpOk === null ? '无指纹（老记录，跳过）' : fpOk ? 'match' : 'MISMATCH'],
    ];
    for (const st of ['done', 'running']) {
      checks.push([
        `${st} 计数与基线一致`,
        Number(cur[st] || 0) === Number(entry.before[st] || 0),
        `${cur[st] || 0} vs ${entry.before[st] || 0}`,
      ]);
    }
  }
  let ok = true;
  console.log(
    `[verify] meta=${entry.key} appliedAt=${entry.appliedAt} ids=${ids.length}` +
      (undone ? ` undoneAt=${entry.undoneAt}（按还原后断言）` : ''),
  );
  for (const [name, pass, detail] of checks) {
    if (!pass) ok = false;
    console.log(`  ${pass ? 'PASS' : 'FAIL'} ${name} (${detail})`);
  }
  console.log(ok ? '[verify] 全部断言通过' : '[verify] 断言失败');
  if (!ok) process.exitCode = 1;
  d.close();
}

function doUndo() {
  const d = db();
  const entry = latestRetirement(d, argOf('--id', null));
  if (!entry) {
    console.error('[undo] meta 里没有 retire_splits_* 记录');
    process.exitCode = 2;
    d.close();
    return;
  }
  if (entry.undoneAt) {
    console.log(`[undo] ${entry.key} 已于 ${entry.undoneAt} 还原过，不重复执行`);
    d.close();
    return;
  }
  const stmt = d.prepare(
    "UPDATE pending_ops SET status = ?, attempts = ?, last_error = ?, updated_ts = ? " +
      "WHERE id = ? AND status = 'dead' AND last_error LIKE 'retired:%'",
  );
  let restored = 0;
  d.exec('BEGIN IMMEDIATE');
  try {
    for (const r of entry.rows) {
      const res = stmt.run(r.prev_status, r.prev_attempts, r.prev_last_error, r.prev_updated_ts, r.id);
      if (res.changes !== 1) throw new Error(`id=${r.id} 已不是 retired dead（行数=${res.changes}），整批回滚等人工看`);
      restored += 1;
    }
    const record = { ...entry, undoneAt: new Date().toISOString(), undoneRows: restored };
    delete record.key;
    d.prepare('UPDATE meta SET value = ? WHERE key = ?').run(JSON.stringify(record), entry.key);
    d.exec('COMMIT');
    console.log(`[undo] ${entry.key} 逐条还原 ${restored} 条（status/attempts/last_error/updated_ts 回原值）`);
  } catch (error) {
    d.exec('ROLLBACK');
    console.error(`[undo] 失败已回滚：${String((error && error.message) || error)}`);
    process.exitCode = 1;
  } finally {
    d.close();
  }
}

if (MODE === 'dry-run') doDryRun();
else if (MODE === 'apply') doApply();
else if (MODE === 'verify') doVerify();
else doUndo();
