#!/usr/bin/env node
/**
 * 受控重编号：清掉 memories.rowSeq 的撞号（2026-11-06 t-rowseq-fix）。
 *
 * 背景：2026-11-05T21:25Z 之前，rowSeq 由进程内存里的 maxRowSeq + 1 发号，
 * --live 的第二个写入者同时取号会拿到同一个号；真库因此留下 82 组重复
 * （每组 2 行 / 共 164 行）。新代码已把取号挪进 BEGIN IMMEDIATE 事务
 * （lib/store.js allocRowSeq），不再新增。
 *
 * 本脚本只做一件事：**只改 memories.rowSeq**。正文、状态、向量数据一律不动。
 * 先摸过引用面：vec_map 按 id（TEXT）关联，不按 rowSeq；memories 之外没有表
 * 带 rowSeq 列（脚本里每次都会现查一遍，不靠记忆）。
 *
 *   node scripts/fix-rowseq-dupes.mjs              # --dry-run（默认，只读）
 *   node scripts/fix-rowseq-dupes.mjs --apply      # 备份 → 单事务重编号
 *   node scripts/fix-rowseq-dupes.mjs --verify     # 对照备份断言
 *
 * 规则：每组里 id 前缀（base36 毫秒时间戳）更早的那行保住原号，
 * 后写入的那行改到 max(rowSeq)+1 起，按 (原 rowSeq, id 时间戳) 顺序连续发号。
 * 幂等：没有重复时 --apply 什么都不做，--dry-run 打「已无重复」。
 */
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { backup } from 'node:sqlite';
import { openDb, resolveDbPath, resolveMemoryFile } from './lib/db.mjs';
import { logicalFingerprint, diffFingerprint } from '../lib/db-fingerprint.js';

const DEFAULT_DB = resolveDbPath();
const DEFAULT_BACKUP = resolveMemoryFile('backup/pre-rowseq-fix-20261106/memory.db');
const BASELINE_NAME = 'pre-fix-baseline.json';

/** 比 rowSeq 之外的整行内容，用来证明「只换了序号」。 */
const CONTENT_COLUMNS = [
  'id', 'text', 'tags', 'source', 'ts', 'type', 'entities', 'validFrom', 'supersedes',
  'updatedAt', 'status', 'reason', 'supersededBy', 'validTo', 'confidence', 'extra', 'present',
];

export function parseArgs(argv = process.argv.slice(2)) {
  const out = { mode: 'dry-run', db: DEFAULT_DB, backupPath: DEFAULT_BACKUP, json: false };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === '--dry-run') out.mode = 'dry-run';
    else if (arg === '--apply') out.mode = 'apply';
    else if (arg === '--verify') out.mode = 'verify';
    else if (arg === '--db') out.db = argv[++i];
    else if (arg.startsWith('--db=')) out.db = arg.slice(5);
    else if (arg === '--backup') out.backupPath = argv[++i];
    else if (arg.startsWith('--backup=')) out.backupPath = arg.slice(9);
    else if (arg === '--json') out.json = true;
    else if (arg === '-h' || arg === '--help') out.help = true;
    else throw new Error(`未知参数：${arg}`);
  }
  return out;
}

/** id 形如 m_<base36 毫秒时间戳>_<随机>；解析不出来返回 NaN。 */
export function idTimestamp(id) {
  const m = /^m_([0-9a-z]+)_([0-9a-z]+)$/.exec(String(id));
  return m ? parseInt(m[1], 36) : Number.NaN;
}

function iso(ms) {
  return Number.isFinite(ms) ? new Date(ms).toISOString() : '(无法解析)';
}

function brief(text, n = 40) {
  return String(text ?? '').replace(/\s+/g, ' ').trim().slice(0, n);
}

function sha256(s) {
  return crypto.createHash('sha256').update(s).digest('hex');
}

/** 所有带 rowSeq / row_seq 列的表（引用面现查，产物里会打印）。 */
export function rowSeqTables(db) {
  const out = [];
  const tables = db.prepare("SELECT name FROM sqlite_master WHERE type IN ('table','view') ORDER BY name").all();
  for (const t of tables) {
    let cols = [];
    try {
      cols = db.prepare(`PRAGMA table_info(${JSON.stringify(t.name)})`).all().map((c) => String(c.name));
    } catch {
      continue;
    }
    const hit = cols.filter((c) => /^row_?seq$/i.test(c));
    if (hit.length) out.push({ table: String(t.name), columns: hit });
  }
  return out;
}

/** 读重复组，返回可直接渲染的计划。只读，不写。 */
export function buildPlan(db, { withText = false } = {}) {
  const total = Number(db.prepare('SELECT count(*) AS c FROM memories').get()?.c ?? 0);
  const distinct = Number(db.prepare('SELECT count(DISTINCT rowSeq) AS c FROM memories').get()?.c ?? 0);
  const maxSeq = Number(db.prepare('SELECT max(rowSeq) AS m FROM memories').get()?.m ?? 0);
  const groups = db
    .prepare('SELECT rowSeq, count(*) AS n FROM memories GROUP BY rowSeq HAVING count(*) > 1 ORDER BY rowSeq')
    .all();
  const rowsOf = db.prepare(
    `SELECT ${CONTENT_COLUMNS.join(', ')} FROM memories WHERE rowSeq = ?`,
  );
  const plan = [];
  let next = maxSeq + 1;
  for (const g of groups) {
    const rows = rowsOf.all(g.rowSeq)
      .map((r) => ({ ...r, id: String(r.id), ts: String(r.ts ?? ''), tsMs: idTimestamp(r.id) }))
      .sort((a, b) => a.tsMs - b.tsMs || String(a.id).localeCompare(String(b.id)));
    const keep = rows[0];
    const moves = rows.slice(1).map((row) => ({ row, newSeq: next++ }));
    plan.push({
      seq: Number(g.rowSeq),
      count: Number(g.n),
      keep: { id: keep.id, ts: keep.ts, tsMs: keep.tsMs, ...(withText ? { brief: brief(keep.text) } : {}) },
      moves: moves.map(({ row, newSeq }) => ({
        id: row.id, ts: row.ts, tsMs: row.tsMs, newSeq, ...(withText ? { brief: brief(row.text) } : {}),
      })),
      sameContent: new Set(rows.map((r) => sha256(String(r.text ?? '')))).size === 1,
    });
  }
  const movedCount = plan.reduce((n, p) => n + p.moves.length, 0);
  return { total, distinct, maxSeq, plan, movedCount, newMaxSeq: maxSeq + movedCount };
}

/** 除 rowSeq 外的整行内容指纹：证明没动正文/状态/向量关联键。 */
export function contentDigest(db) {
  const rows = db.prepare(
    `SELECT ${CONTENT_COLUMNS.join(', ')} FROM memories ORDER BY id`,
  ).all();
  const h = crypto.createHash('sha256');
  for (const r of rows) {
    h.update(crypto.createHash('sha256').update(JSON.stringify(CONTENT_COLUMNS.map((c) => r[c] ?? null))).digest('hex'));
    h.update('\n');
  }
  return { rows: rows.length, sha256: h.digest('hex') };
}

function counts(db) {
  const out = {};
  for (const [key, sql] of [
    ['memories', 'SELECT count(*) AS n FROM memories'],
    ['vec_map', 'SELECT count(*) AS n FROM vec_map'],
    ['discards', 'SELECT count(*) AS n FROM discards'],
    ['pending_ops', 'SELECT count(*) AS n FROM pending_ops'],
    ['ingest_files', 'SELECT count(*) AS n FROM ingest_files'],
  ]) {
    try {
      out[key] = Number(db.prepare(sql).get()?.n ?? 0);
    } catch {
      out[key] = null;
    }
  }
  out.orphanVec = Number(
    db.prepare('SELECT count(*) AS c FROM vec_map v LEFT JOIN memories m ON m.id = v.id WHERE m.id IS NULL').get()?.c ?? 0,
  );
  out.noVec = Number(
    db.prepare('SELECT count(*) AS c FROM memories m LEFT JOIN vec_map v ON v.id = m.id WHERE v.id IS NULL').get()?.c ?? 0,
  );
  return out;
}

function printPlan(report, db) {
  console.log(`[rowseq] db=${report.db} 模式=${report.mode}${report.mode === 'dry-run' ? '（只读）' : ''}`);
  console.log(`[rowseq] 总行 ${report.total}，不同 rowSeq ${report.distinct}，重复 ${report.plan.length} 组 / ${report.movedCount} 行待改号；当前 max(rowSeq)=${report.maxSeq}`);
  const refs = rowSeqTables(db).map((r) => `${r.table}(${r.columns.join(',')})`).join(', ');
  console.log(`[rowseq] 带 rowSeq 列的表：${refs || '（无）'}；vec_map 关联列：${db.prepare('PRAGMA table_info(vec_map)').all().map((c) => c.name).join(',')}`);
  console.log(`[rowseq] 计划：每组保 id 时间戳更早那行的原号，其余按序改到 ${report.maxSeq + 1}..${report.maxSeq + report.movedCount}`);
  for (const g of report.plan) {
    const same = g.sameContent ? '同内容' : '不同内容';
    console.log(`[rowseq] rowSeq=${g.seq}（${same}）keep ${g.keep.id} ts=${g.keep.ts} 摘要「${g.keep.brief ?? ''}」`);
    for (const m of g.moves) {
      console.log(`[rowseq]   move ${m.id} ts=${m.ts} → ${m.newSeq} 摘要「${m.brief ?? ''}」`);
    }
  }
  if (!report.plan.length) console.log('[rowseq] 没有重复 rowSeq，无需改号');
}

async function doApply(args) {
  fs.mkdirSync(path.dirname(args.backupPath), { recursive: true });
  const db = openDb(args.db, { allowExtension: true });
  db.exec('PRAGMA busy_timeout = 5000;');
  try {
    const plan = buildPlan(db);
    const before = logicalFingerprint(args.db);
    const digestBefore = contentDigest(db);
    const countsBefore = counts(db);
    const baseline = {
      at: new Date().toISOString(),
      db: args.db,
      total: plan.total,
      distinct: plan.distinct,
      maxSeq: plan.maxSeq,
      movedCount: plan.movedCount,
      groups: plan.plan.length,
      fingerprint: before,
      contentDigest: digestBefore,
      counts: countsBefore,
    };
    console.log(`[rowseq] 备份前：${JSON.stringify(baseline.fingerprint.canonical)}`);

    // 1) 在线备份（SQLite backup API，活库也能出一致快照）
    if (fs.existsSync(args.backupPath)) throw new Error(`备份目标已存在，不覆盖：${args.backupPath}`);
    await backup(db, args.backupPath);
    // 2) 校验备份本身
    const bk = openDb(args.backupPath, { readOnly: true });
    let bkCheck;
    try {
      bkCheck = {
        memories: Number(bk.prepare('SELECT count(*) AS n FROM memories').get()?.n ?? -1),
        vec_map: Number(bk.prepare('SELECT count(*) AS n FROM vec_map').get()?.n ?? -1),
        maxSeq: Number(bk.prepare('SELECT max(rowSeq) AS m FROM memories').get()?.m ?? -1),
        integrity: String(bk.prepare('PRAGMA integrity_check').get()?.integrity_check ?? ''),
        contentDigest: contentDigest(bk).sha256,
      };
    } finally {
      bk.close();
    }
    const backupOk = bkCheck.memories === baseline.total
      && bkCheck.vec_map === countsBefore.vec_map
      && bkCheck.maxSeq === baseline.maxSeq
      && bkCheck.integrity === 'ok'
      && bkCheck.contentDigest === digestBefore.sha256;
    console.log(`[rowseq] 备份 ${args.backupPath} 校验：memories=${bkCheck.memories} vec_map=${bkCheck.vec_map} max=${bkCheck.maxSeq} integrity=${bkCheck.integrity} 内容指纹一致=${bkCheck.contentDigest === digestBefore.sha256}`);
    if (!backupOk) throw new Error('备份校验没过，放弃 apply（一条也没写）');
    fs.writeFileSync(path.join(path.dirname(args.backupPath), BASELINE_NAME), `${JSON.stringify(baseline, null, 2)}\n`);

    if (!plan.plan.length) {
      console.log('[rowseq] 库里没有重复 rowSeq，幂等收工（备份已留）');
      return 0;
    }

    // 3) 单事务改号；任何一步对不上就整体回滚
    db.exec('BEGIN IMMEDIATE;');
    let changed = 0;
    try {
      // 事务里重读一遍计划，别拿事务外快照的号去写
      const txPlan = buildPlan(db);
      if (txPlan.plan.length !== plan.plan.length || txPlan.maxSeq !== plan.maxSeq) {
        throw new Error(`事务内计划与预检不一致（组数 ${txPlan.plan.length} vs ${plan.plan.length}）`);
      }
      const upd = db.prepare('UPDATE memories SET rowSeq = ? WHERE id = ? AND rowSeq = ?');
      for (const g of txPlan.plan) {
        for (const m of g.moves) {
          const res = upd.run(m.newSeq, m.id, g.seq);
          const n = Number(res.changes ?? 0);
          if (n !== 1) throw new Error(`改号命中 ${n} 行（期望 1）：${m.id} ${g.seq} → ${m.newSeq}`);
          changed += n;
        }
      }
      db.exec('COMMIT;');
    } catch (error) {
      try { db.exec('ROLLBACK;'); } catch { /* 连接坏了由 sqlite 自己回滚 */ }
      throw error;
    }
    console.log(`[rowseq] apply 完成：单事务改了 ${changed} 行的 rowSeq（${plan.plan.length} 组），其它列一条没动`);
    return 0;
  } finally {
    db.close();
  }
}

function doVerify(args) {
  const db = openDb(args.db, { readOnly: true });
  const problems = [];
  try {
    const total = Number(db.prepare('SELECT count(*) AS c FROM memories').get()?.c ?? 0);
    const distinct = Number(db.prepare('SELECT count(DISTINCT rowSeq) AS c FROM memories').get()?.c ?? 0);
    const dups = db.prepare('SELECT rowSeq, count(*) AS n FROM memories GROUP BY rowSeq HAVING count(*) > 1').all();
    const maxSeq = Number(db.prepare('SELECT max(rowSeq) AS m FROM memories').get()?.m ?? 0);
    const integrity = String(db.prepare('PRAGMA integrity_check').get()?.integrity_check ?? '');
    const now = counts(db);
    const digest = contentDigest(db);

    if (dups.length) problems.push(`仍有 ${dups.length} 组重复 rowSeq`);
    if (distinct !== total) problems.push(`distinct(rowSeq)=${distinct} ≠ 行数 ${total}`);
    if (integrity !== 'ok') problems.push(`integrity_check=${integrity}`);

    const baselinePath = path.join(path.dirname(args.backupPath), BASELINE_NAME);
    if (fs.existsSync(baselinePath)) {
      const base = JSON.parse(fs.readFileSync(baselinePath, 'utf8'));
      const expect = base.counts;
      for (const key of ['memories', 'vec_map', 'discards', 'pending_ops', 'ingest_files', 'orphanVec', 'noVec']) {
        if (expect[key] !== null && now[key] !== expect[key]) problems.push(`${key} 行数 ${expect[key]} → ${now[key]}`);
      }
      if (digest.sha256 !== base.contentDigest.sha256) problems.push('正文/状态内容指纹变了');
      if (baselinePath && fs.existsSync(args.backupPath)) {
        const bk = openDb(args.backupPath, { readOnly: true });
        try {
          const bkCounts = counts(bk);
          for (const key of ['memories', 'vec_map', 'discards', 'pending_ops', 'ingest_files']) {
            if (bkCounts[key] !== now[key]) problems.push(`与备份比 ${key}：${bkCounts[key]} → ${now[key]}`);
          }
        } finally {
          bk.close();
        }
      }
      if (maxSeq !== base.maxSeq + base.movedCount) {
        problems.push(`max(rowSeq) ${maxSeq} ≠ 备份 ${base.maxSeq} + 改号 ${base.movedCount}`);
      }
      // 关联仍成立：vec_map 每个 id 都能在 memories 找到（已知孤儿条数不变）
      const orphanNow = now.orphanVec;
      if (orphanNow !== expect.orphanVec) problems.push(`孤儿向量 ${expect.orphanVec} → ${orphanNow}`);
    } else {
      problems.push(`没有基线文件 ${baselinePath}，行数/内容只能自查`);
    }

    console.log(`[rowseq] verify：总行 ${total}，distinct rowSeq ${distinct}，重复 ${dups.length} 组，max(rowSeq)=${maxSeq}，integrity=${integrity}`);
    console.log(`[rowseq] verify：memories=${now.memories} vec_map=${now.vec_map} discards=${now.discards} pending_ops=${now.pending_ops} ingest_files=${now.ingest_files} 孤儿向量=${now.orphanVec} 缺向量=${now.noVec}`);
    console.log(`[rowseq] verify：内容指纹 ${digest.sha256.slice(0, 16)}（${digest.rows} 行）`);
    if (problems.length) {
      for (const p of problems) console.error(`[rowseq] ✗ ${p}`);
      console.error('[rowseq] verify 不通过');
      return 1;
    }
    console.log('[rowseq] verify 通过：重复已清、行数与内容指纹未变、vec_map 关联仍成立、integrity ok');
    return 0;
  } finally {
    db.close();
  }
}

async function main() {
  let args;
  try {
    args = parseArgs();
  } catch (error) {
    console.error(String(error.message || error));
    return 2;
  }
  if (args.help) {
    console.log('用法：node scripts/fix-rowseq-dupes.mjs [--dry-run|--apply|--verify] [--db <path>] [--backup <path>] [--json]');
    return 0;
  }
  try {
    if (args.mode === 'verify') return doVerify(args);
    if (args.mode === 'apply') return await doApply(args);
    const db = openDb(args.db, { readOnly: true });
    try {
      const plan = buildPlan(db, { withText: true });
      if (args.json) {
        console.log(JSON.stringify({ ...plan, db: args.db, mode: args.mode, rowSeqTables: rowSeqTables(db) }, null, 2));
        return plan.plan.length ? 1 : 0;
      }
      printPlan({ ...plan, db: args.db, mode: args.mode }, db);
      return plan.plan.length ? 1 : 0;
    } finally {
      db.close();
    }
  } catch (error) {
    console.error(`[rowseq] 失败：${String((error && error.message) || error)}`);
    return 2;
  }
}

if (import.meta.url === (process.argv[1] ? new URL(`file://${process.argv[1]}`).href : '')) {
  process.exit(await main());
}
