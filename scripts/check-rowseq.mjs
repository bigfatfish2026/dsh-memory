#!/usr/bin/env node
/**
 * 只读体检：memories 表里的 rowSeq 有没有撞号（2026-11-06 t-fix-quality）。
 *
 * 真库在 2026-11-06 之前用「进程内存 maxRowSeq + 1」取号，--live 的另一个写入者
 * 同时写就会拿到同一个号；这里只报不修，改号/删行一律人来做。
 *
 *   node scripts/check-rowseq.mjs                       # 默认真库，只读打开
 *   node scripts/check-rowseq.mjs --db /tmp/x.db        # 查副本
 *   node scripts/check-rowseq.mjs --json                # 机器可读
 *
 * 退出码：0 = 没有重复；1 = 有重复；2 = 打不开/没有表。
 * 连接用 readOnly:true，任何写操作都会直接报错，不会碰真库。
 */
import crypto from 'node:crypto';
import { openDb, resolveDbPath } from './lib/db.mjs';

const DEFAULT_DB = resolveDbPath();

export function parseArgs(argv = process.argv.slice(2)) {
  const out = { db: DEFAULT_DB, json: false };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === '--db') out.db = argv[++i];
    else if (arg.startsWith('--db=')) out.db = arg.slice(5);
    else if (arg === '--json') out.json = true;
    else if (arg === '-h' || arg === '--help') out.help = true;
    else throw new Error(`未知参数：${arg}`);
  }
  return out;
}

/** 正文摘要：sha1(去空白) 前 12 位，用来判断重复 rowSeq 的两行是不是同一段内容。 */
function digestOf(text) {
  return crypto.createHash('sha1').update(String(text ?? '').replace(/\s+/g, ' ').trim()).digest('hex').slice(0, 12);
}

/**
 * 只读打开并统计。导出成函数，测试和 CLI 共用一份逻辑。
 * @returns {{total:number, distinct:number, dupPairs:Array, dupRows:number, ok:boolean}}
 */
export function checkRowSeq(dbPath = DEFAULT_DB) {
  const db = openDb(dbPath, { readOnly: true });
  try {
    const hasTable = db
      .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='memories'")
      .get();
    if (!hasTable) throw new Error('没有 memories 表');
    const total = Number(db.prepare('SELECT count(*) AS c FROM memories').get()?.c ?? 0);
    const distinct = Number(db.prepare('SELECT count(DISTINCT rowSeq) AS c FROM memories').get()?.c ?? 0);
    const groups = db
      .prepare(`SELECT rowSeq, count(*) AS n FROM memories GROUP BY rowSeq HAVING count(*) > 1 ORDER BY rowSeq`)
      .all();
    const rowsOf = db.prepare(
      'SELECT id, rowSeq, length(text) AS len, text, status, ts FROM memories WHERE rowSeq = ? ORDER BY ts, id',
    );
    const dupPairs = groups.map((g) => {
      const rows = rowsOf.all(g.rowSeq).map((r) => ({
        id: String(r.id),
        len: Number(r.len) || 0,
        status: String(r.status || ''),
        ts: String(r.ts || ''),
        digest: digestOf(r.text),
      }));
      const digests = new Set(rows.map((r) => r.digest));
      return {
        rowSeq: Number(g.rowSeq),
        count: rows.length,
        sameContent: digests.size === 1,
        ids: rows.map((r) => r.id),
        rows,
      };
    });
    const dupRows = dupPairs.reduce((n, p) => n + p.count, 0);
    return { dbPath, total, distinct, dupPairs, dupRows, ok: dupPairs.length === 0 };
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
    console.log('用法：node scripts/check-rowseq.mjs [--db <path>] [--json]');
    return;
  }
  let report;
  try {
    report = checkRowSeq(args.db);
  } catch (error) {
    console.error(`[rowseq] 打不开 ${args.db}：${String((error && error.message) || error)}`);
    process.exit(2);
  }
  if (args.json) {
    console.log(JSON.stringify(report, null, 2));
  } else {
    console.log(`[rowseq] db=${report.dbPath} 只读`);
    console.log(`[rowseq] 总行 ${report.total}，不同 rowSeq ${report.distinct}，重复 ${report.dupPairs.length} 对 / ${report.dupRows} 行`);
    for (const pair of report.dupPairs) {
      console.log(
        `[rowseq] rowSeq=${pair.rowSeq} 次数=${pair.count} 同内容=${pair.sameContent ? 'yes' : 'no'} ` +
          `ids=${pair.ids.join(',')} 长度=${pair.rows.map((r) => r.len).join('/')}`,
      );
      console.log(`[rowseq]   摘要 ${pair.rows.map((r) => `${r.digest}${r.status === 'active' ? '' : `(${r.status})`}`).join(' ')}`);
    }
    if (report.ok) console.log('[rowseq] 没有重复 rowSeq');
    else console.log('[rowseq] 结论：有重复。只报不修，改号/删行走人工。');
  }
  process.exit(report.ok ? 0 : 1);
}

if (import.meta.url === (process.argv[1] ? new URL(`file://${process.argv[1]}`).href : '')) main();
