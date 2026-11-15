#!/usr/bin/env node
/**
 * 一次性回填：把老账（payload 缺 ids / 缺 source）补上，并把 dead 拉回 pending
 * （2026-11-06 t-fix-quality）。
 *
 * 背景：2026-11-06 之前写入闸未决只存 `{action, candidate, reason}`，没有 ids；
 * merge_op 只存 quote，没有 source。重放段现在有「按正文 + 实体召回」的兜底，
 * 但仍要 payload 里带完整 candidate / 空 ids 数组才走得通；老行原样重放会一直缺字段。
 *
 *   node scripts/revive-dedupe-check.mjs                 # 预演（只读，打印会改什么）
 *   node scripts/revive-dedupe-check.mjs --apply         # 真写 pending_ops
 *   node scripts/revive-dedupe-check.mjs --db /tmp/x.db --apply
 *
 * 只动 pending_ops 的 payload_json / status / attempts / next_try_at / last_error，
 * 不碰 memories，不调模型。dedupe_check 的 ids 留空 []，交给重放段召回补位；
 * merge_op 的 source 用当前库里同 id 的正文回填（找不到就留空，重放会记 no_source_for_replay）。
 */
import { openDb, resolveDbPath } from './lib/db.mjs';

const DEFAULT_DB = resolveDbPath();

export function parseArgs(argv = process.argv.slice(2)) {
  const out = { db: DEFAULT_DB, apply: false, limit: 0 };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === '--db') out.db = argv[++i];
    else if (arg.startsWith('--db=')) out.db = arg.slice(5);
    else if (arg === '--apply') out.apply = true;
    else if (arg === '--limit') out.limit = Number(argv[++i]) || 0;
    else if (arg === '-h' || arg === '--help') out.help = true;
    else throw new Error(`未知参数：${arg}`);
  }
  return out;
}

function parseJson(raw) {
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

/**
 * 计算一条账的新 payload；不需要改就返回 null。
 * dedupe_check：补 ids 数组 + 补全 candidate 结构；
 * merge_op：补 source（源条目原文）。
 */
export function backfillRow(row, lookupSource) {
  const payload = parseJson(row.payload_json);
  if (!payload || typeof payload !== 'object') return null;
  const out = { ...payload };
  let changed = false;
  if (row.kind === 'dedupe_check') {
    if (!Array.isArray(out.ids)) {
      out.ids = [];
      changed = true;
    }
    if (!out.candidate || typeof out.candidate !== 'object') return null; // 没正文，补不了
    const cand = { ...out.candidate };
    for (const key of ['text', 'type', 'tags', 'entities', 'source', 'sourceFile']) {
      if (cand[key] === undefined) {
        cand[key] = key === 'tags' || key === 'entities' ? [] : '';
        changed = true;
      }
    }
    if (changed) out.candidate = cand;
  } else if (row.kind === 'merge_op') {
    if (!Array.isArray(out.source) || !out.source.length) {
      const ids = Array.isArray(out.ids) ? out.ids.map(String) : [];
      const source = ids.map((id) => ({ id, text: lookupSource(id) })).filter((e) => e.text);
      if (source.length) {
        out.source = source;
        changed = true;
      }
    }
  }
  return changed ? out : null;
}

export function revive({ dbPath = DEFAULT_DB, apply = false, limit = 0, logger = console } = {}) {
  const db = openDb(dbPath, apply ? {} : { readOnly: true });
  const out = { dbPath, apply, scanned: 0, changed: 0, revived: 0, skipped: [], rows: [] };
  try {
    const rows = db
      .prepare("SELECT id, kind, dedupe_key, payload_json, attempts, status, last_error FROM pending_ops WHERE status = 'dead' AND kind IN ('dedupe_check','merge_op') ORDER BY id")
      .all();
    const getText = db.prepare('SELECT text FROM memories WHERE id = ?');
    const lookupSource = (id) => String(getText.get(String(id))?.text || '');
    const upd = apply
      ? db.prepare("UPDATE pending_ops SET payload_json = ?, status = 'pending', attempts = 0, next_try_at = ?, last_error = ?, updated_ts = ? WHERE id = ?")
      : null;
    for (const row of rows) {
      if (limit && out.scanned >= limit) break;
      out.scanned += 1;
      const next = backfillRow(row, lookupSource);
      if (!next) {
        out.skipped.push({ id: row.id, kind: row.kind, why: 'nothing_to_backfill' });
        continue;
      }
      out.changed += 1;
      out.revived += 1;
      out.rows.push({ id: row.id, kind: row.kind, key: String(row.dedupe_key).slice(0, 12) });
      if (apply) {
        const now = Date.now();
        upd.run(JSON.stringify(next), now + 60000, 'revived_payload_backfill', now, row.id);
        logger.log(`[revive] id=${row.id} kind=${row.kind} status=dead → pending attempts=0`);
      } else {
        logger.log(`[revive][dry] id=${row.id} kind=${row.kind} 会补 payload 并拉回 pending`);
      }
    }
    return out;
  } finally {
    db.close();
  }
}

function main() {
  let args;
  try {
    args = parseArgs();
  } catch (error) {
    console.error(String(error.message || error));
    process.exit(2);
  }
  if (args.help) {
    console.log('用法：node scripts/revive-dedupe-check.mjs [--db <path>] [--apply] [--limit N]');
    return;
  }
  let report;
  try {
    report = revive({ dbPath: args.db, apply: args.apply });
  } catch (error) {
    console.error(`[revive] 失败：${String((error && error.message) || error)}`);
    process.exit(2);
  }
  const mode = report.apply ? 'apply' : 'dry-run（加 --apply 才写）';
  console.log(
    `[revive] db=${report.dbPath} mode=${mode} dead=${report.scanned} 可补=${report.changed} 已复活=${report.apply ? report.revived : 0}` +
      (report.skipped.length ? ` 跳过=${report.skipped.length}` : ''),
  );
}

if (import.meta.url === (process.argv[1] ? new URL(`file://${process.argv[1]}`).href : '')) main();
