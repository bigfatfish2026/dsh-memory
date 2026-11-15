#!/usr/bin/env node
/**
 * 把 memories.jsonl 的正文一次性迁进 memory.db 的 memories 表。
 *
 * 这一轮只做 schema + 一次性迁移，不改 store.js 的运行逻辑；store.js 仍写 jsonl。
 * 向量表 vec_items / vec_map 原样保留，本脚本不新建第二套向量表，也不改向量数据。
 *
 * 护栏：默认输出就是默认真库，但没有 --force-live
 * 一律拒写（和 dispatcher 的迁移脚本一个规矩），先在 /tmp 出库演练。
 *
 * 用法：
 *   node scripts/migrate-jsonl-to-sqlite.mjs [--in <jsonl>] [--out <db>] [--dry-run] [--force-live]
 *
 * 退出码：0 成功且核对全过；1 数据/核对失败（整次回滚）；2 参数不对。
 */
import fs from 'node:fs';
import path from 'node:path';
import { openDb, DEFAULT_DB_PATH, resolveMemoryFile } from './lib/db.mjs';

const LIVE_DB = DEFAULT_DB_PATH;
const LIVE_DIR = path.dirname(LIVE_DB);
const DEFAULT_IN = resolveMemoryFile('memories.jsonl');

const KNOWN_KEYS = [
  'text', 'tags', 'source', 'ts', 'type', 'entities', 'validFrom',
  'supersedes', 'updatedAt', 'status', 'reason', 'supersededBy',
  'validTo', 'confidence',
];
const JSON_KEYS = new Set(['tags', 'entities', 'supersedes']);

const DDL = `
CREATE TABLE IF NOT EXISTS memories (
  id            TEXT PRIMARY KEY,
  text          TEXT NOT NULL,
  tags          TEXT,
  source        TEXT,
  ts            TEXT,
  type          TEXT,
  entities      TEXT,
  validFrom     TEXT,
  supersedes    TEXT,
  updatedAt     TEXT,
  status        TEXT,
  reason        TEXT,
  supersededBy  TEXT,
  validTo       TEXT,
  confidence    REAL,
  extra         TEXT,
  present       TEXT,
  rowSeq        INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_memories_status ON memories(status);
CREATE INDEX IF NOT EXISTS idx_memories_type   ON memories(type);
CREATE INDEX IF NOT EXISTS idx_memories_ts     ON memories(ts);
`;

const USAGE = `用法：node scripts/migrate-jsonl-to-sqlite.mjs [选项]

选项：
  --in <路径>    输入 jsonl；默认 ${DEFAULT_IN}
  --out <路径>   输出库；默认 ${LIVE_DB}
  --dry-run      只读：只解析输入、报计划，不建表不写盘
  --force-live   允许写真实库 ${LIVE_DB}（默认拒写）
  --json         以 JSON 打印结果
  --help, -h     打印这段

退出码：0 成功且核对全过；1 数据/核对失败；2 参数不对。`;

class UsageError extends Error {}

function parseArgs(argv) {
  const options = {
    in: DEFAULT_IN,
    out: LIVE_DB,
    dryRun: false,
    forceLive: false,
    json: false,
    help: false,
  };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === '--help' || arg === '-h') options.help = true;
    else if (arg === '--dry-run') options.dryRun = true;
    else if (arg === '--force-live') options.forceLive = true;
    else if (arg === '--json') options.json = true;
    else if (arg === '--in' || arg === '--out') {
      const value = argv[i + 1];
      if (!value) throw new UsageError(`${arg} 后面要跟路径`);
      options[arg.slice(2)] = value;
      i += 1;
    } else if (arg.startsWith('--in=')) options.in = arg.slice(5);
    else if (arg.startsWith('--out=')) options.out = arg.slice(6);
    else if (arg.startsWith('-')) throw new UsageError(`不认识的选项：${arg}`);
    else throw new UsageError(`多余的参数：${arg}`);
  }
  return options;
}

function assertSafeOut(outPath, forceLive) {
  const resolved = path.resolve(outPath);
  const parent = path.dirname(resolved);
  const realParent = fs.existsSync(parent) ? fs.realpathSync(parent) : parent;
  const real = path.join(realParent, path.basename(resolved));
  const realLiveDir = fs.existsSync(LIVE_DIR) ? fs.realpathSync(LIVE_DIR) : LIVE_DIR;
  if (!forceLive && (real === LIVE_DB || real === path.join(realLiveDir, path.basename(LIVE_DB))
    || realParent === realLiveDir)) {
    throw new Error(
      `拒绝写入真实库 ${real}：迁移脚本默认不碰真库。先 --out 指到 /tmp 下的副本演练；`
      + '确实要写真库再加 --force-live。',
    );
  }
  return real;
}

function readJsonl(jsonlPath) {
  const raw = fs.readFileSync(jsonlPath, 'utf8');
  const lines = raw.split('\n');
  const records = [];
  const badLines = [];
  lines.forEach((line, index) => {
    if (!line.trim()) return;
    const lineNo = index + 1;
    let obj;
    try {
      obj = JSON.parse(line);
    } catch (error) {
      badLines.push({ line: lineNo, reason: `JSON 解析失败：${error.message}` });
      return;
    }
    if (obj === null || typeof obj !== 'object' || Array.isArray(obj)) {
      badLines.push({ line: lineNo, reason: '不是 JSON 对象' });
      return;
    }
    if (typeof obj.id !== 'string' || !obj.id) {
      badLines.push({ line: lineNo, reason: '缺 id 或 id 不是字符串' });
      return;
    }
    if (typeof obj.text !== 'string') {
      badLines.push({ line: lineNo, reason: '缺 text 或 text 不是字符串' });
      return;
    }
    records.push({ lineNo, obj });
  });
  const seen = new Map();
  const duplicateIds = [];
  for (const { obj, lineNo } of records) {
    if (seen.has(obj.id)) duplicateIds.push({ id: obj.id, lines: [seen.get(obj.id), lineNo] });
    else seen.set(obj.id, lineNo);
  }
  return { records, badLines, duplicateIds, totalLines: lines.filter((l) => l.trim()).length };
}

function jsonText(value) {
  if (value === undefined) return null;
  if (value === null) return null;
  return JSON.stringify(value);
}

function toDbRow(obj, rowSeq) {
  const extra = {};
  for (const key of Object.keys(obj)) {
    if (key !== 'id' && !KNOWN_KEYS.includes(key)) extra[key] = obj[key];
  }
  return {
    id: obj.id,
    text: obj.text,
    tags: jsonText(obj.tags),
    source: obj.source ?? null,
    ts: obj.ts ?? null,
    type: obj.type ?? null,
    entities: jsonText(obj.entities),
    validFrom: obj.validFrom ?? null,
    supersedes: jsonText(obj.supersedes),
    updatedAt: obj.updatedAt ?? null,
    status: obj.status ?? null,
    reason: obj.reason ?? null,
    supersededBy: obj.supersededBy ?? null,
    validTo: obj.validTo ?? null,
    confidence: obj.confidence ?? null,
    extra: Object.keys(extra).length > 0 ? JSON.stringify(extra) : null,
    present: JSON.stringify(Object.keys(obj)),
    rowSeq,
  };
}

const DB_COLUMNS = [
  'id', 'text', 'tags', 'source', 'ts', 'type', 'entities', 'validFrom',
  'supersedes', 'updatedAt', 'status', 'reason', 'supersededBy', 'validTo',
  'confidence', 'extra', 'present', 'rowSeq',
];
const UPSERT_SQL = `INSERT INTO memories (${DB_COLUMNS.join(', ')})
VALUES (${DB_COLUMNS.map(() => '?').join(', ')})
ON CONFLICT(id) DO UPDATE SET ${DB_COLUMNS.filter((c) => c !== 'id').map((c) => `${c}=excluded.${c}`).join(', ')}`;
const SELECT_SQL = `SELECT ${DB_COLUMNS.join(', ')} FROM memories`;

function dbRowKey(row) {
  return JSON.stringify(DB_COLUMNS.map((c) => row[c] ?? null));
}

function tableExists(db, name) {
  return Boolean(db.prepare("SELECT 1 AS ok FROM sqlite_master WHERE type='table' AND name=?").get(name));
}

function findVecExtension() {
  const candidates = [
    path.join(import.meta.dirname, '../node_modules/sqlite-vec-linux-arm64/vec0.so'),
  ];
  for (const candidate of candidates) if (fs.existsSync(candidate)) return candidate;
  return null;
}

function loadVec(db) {
  const ext = findVecExtension();
  if (!ext) return false;
  try {
    db.loadExtension(ext);
    return true;
  } catch {
    return false;
  }
}

function vectorStats(db, vecLoaded) {
  const out = { vecLoaded, hasVecMap: tableExists(db, 'vec_map'), count: null, dim: null };
  if (out.hasVecMap) {
    out.count = db.prepare('SELECT count(*) AS c FROM vec_map').get().c;
  }
  if (vecLoaded) {
    try {
      const c = db.prepare('SELECT count(*) AS c FROM vec_items').get().c;
      out.vectorCount = c;
      const one = db.prepare('SELECT embedding FROM vec_items LIMIT 1').get();
      if (one && one.embedding) out.dim = one.embedding.length;
    } catch {
      out.vectorCount = null;
    }
  }
  if (out.vectorCount === null || out.vectorCount === undefined) out.vectorCount = out.count;
  return out;
}

function readMeta(db) {
  if (!tableExists(db, 'meta')) return {};
  const meta = {};
  for (const row of db.prepare('SELECT key, value FROM meta').all()) meta[row.key] = row.value;
  return meta;
}

function writeMeta(db, key, value) {
  db.prepare('INSERT INTO meta(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value')
    .run(key, String(value));
}

function normalizeStatus(value) {
  return value === null || value === undefined || value === '' ? 'active' : value;
}

function compareRow(obj, dbRow) {
  const problems = [];
  const expectedPresent = new Set(Object.keys(obj));
  const actualPresent = new Set(JSON.parse(dbRow.present ?? '[]'));
  for (const key of KNOWN_KEYS) {
    const expected = key in obj ? obj[key] : undefined;
    let actual = dbRow[key];
    if (JSON_KEYS.has(key) && typeof actual === 'string') {
      try {
        actual = JSON.parse(actual);
      } catch {
        problems.push(`${key}: 库里的 JSON 解析失败`);
        continue;
      }
    }
    if (actual === undefined) actual = null;
    const expectedNorm = expected === undefined ? null : expected;
    if (JSON.stringify(expectedNorm) !== JSON.stringify(actual)) {
      problems.push(`${key}: jsonl=${JSON.stringify(expectedNorm)} 库=${JSON.stringify(actual)}`);
    }
    if (expectedPresent.has(key) !== actualPresent.has(key)) {
      problems.push(`${key}: present 标记不符（jsonl=${expectedPresent.has(key)} 库=${actualPresent.has(key)}）`);
    }
  }
  for (const key of Object.keys(obj)) {
    if (key === 'id' || KNOWN_KEYS.includes(key)) continue;
    const extra = dbRow.extra ? JSON.parse(dbRow.extra) : {};
    if (JSON.stringify(extra[key]) !== JSON.stringify(obj[key])) {
      problems.push(`extra.${key}: jsonl=${JSON.stringify(obj[key])} 库=${JSON.stringify(extra[key])}`);
    }
  }
  return problems;
}

function dryRunPlan(options, scan, outPath) {
  const plan = {
    dryRun: true,
    input: options.in,
    out: outPath,
    lines: scan.totalLines,
    ids: scan.records.length,
    badLines: scan.badLines,
    duplicateIds: scan.duplicateIds,
    inserted: scan.records.length,
    updated: 0,
    skipped: 0,
    dbExists: fs.existsSync(outPath),
  };
  if (scan.badLines.length || scan.duplicateIds.length) return plan;
  if (fs.existsSync(outPath)) {
    let db;
    try {
      db = openDb(outPath, { readOnly: true });
      if (tableExists(db, 'memories')) {
        const existing = new Map();
        for (const row of db.prepare(SELECT_SQL).all()) existing.set(row.id, row);
        plan.inserted = 0;
        for (const { obj } of scan.records) {
          const prior = existing.get(obj.id);
          if (prior === undefined) plan.inserted += 1;
          else if (dbRowKey(prior) === dbRowKey(toDbRow(obj, prior.rowSeq))) plan.skipped += 1;
          else plan.updated += 1;
        }
      }
    } catch {
      // 读不了就按新库算
    } finally {
      try { db?.close(); } catch {}
    }
  }
  return plan;
}

function formatChecks(checks) {
  const lines = ['核对表：'];
  for (const check of checks) lines.push(`  ${check.ok ? 'OK  ' : 'FAIL'} ${check.item}: ${check.detail}`);
  return lines.join('\n');
}

function migrate(options) {
  const outPath = options.dryRun ? path.resolve(options.out) : assertSafeOut(options.out, options.forceLive);
  const scan = readJsonl(options.in);
  if (options.dryRun) return dryRunPlan(options, scan, outPath);
  if (scan.badLines.length > 0) {
    throw new Error(`输入有 ${scan.badLines.length} 行不合格：${scan.badLines.slice(0, 3).map((b) => `第 ${b.line} 行 ${b.reason}`).join('；')}`);
  }
  if (scan.duplicateIds.length > 0) {
    throw new Error(`输入有重复 id：${scan.duplicateIds.slice(0, 3).map((d) => `${d.id}（第 ${d.lines.join('/')} 行）`).join('，')}`);
  }

  const startedAt = Date.now();
  const db = openDb(outPath);
  let vecLoaded = false;
  const counts = { inserted: 0, updated: 0, skipped: 0 };
  try {
    db.exec('PRAGMA journal_mode = WAL;');
    db.exec('PRAGMA busy_timeout = 5000;');
    db.exec('PRAGMA synchronous = NORMAL;');
    vecLoaded = loadVec(db);

    db.exec('BEGIN IMMEDIATE;');
    try {
      db.exec(DDL);
      const existing = new Map();
      for (const row of db.prepare(SELECT_SQL).all()) existing.set(row.id, row);
      const upsert = db.prepare(UPSERT_SQL);
      scan.records.forEach(({ obj }, index) => {
        const row = toDbRow(obj, index + 1);
        const prior = existing.get(obj.id);
        if (prior === undefined) counts.inserted += 1;
        else if (dbRowKey(prior) === dbRowKey(row)) {
          // 完全一致就别 upsert：ON CONFLICT DO UPDATE 即使值相同也会重写页，
          // 幂等复跑会白白改库文件（sha 变、WAL 有页），这里直接跳过。
          counts.skipped += 1;
          return;
        } else counts.updated += 1;
        upsert.run(...DB_COLUMNS.map((c) => row[c]));
      });

      const memCount = db.prepare('SELECT count(*) AS c FROM memories').get().c;
      const checks = [];
      const fail = [];
      const push = (ok, item, detail, fatal = true) => {
        checks.push({ ok, item, detail });
        if (!ok && fatal) fail.push(item);
      };

      push(memCount === scan.records.length, '行数 jsonl vs memories',
        `jsonl ${scan.totalLines} 行 / 唯一 id ${scan.records.length}；memories ${memCount} 行`);

      const statusRows = db.prepare(
        "SELECT COALESCE(NULLIF(status, ''), 'active') AS s, count(*) AS c FROM memories GROUP BY s ORDER BY s",
      ).all();
      const statusMap = Object.fromEntries(statusRows.map((r) => [r.s, r.c]));
      push(true, 'status 分组', `active ${statusMap.active ?? 0} / merged ${statusMap.merged ?? 0} / archived ${statusMap.archived ?? 0}`,
        false);

      const typeRows = db.prepare('SELECT COALESCE(type, \'(missing)\') AS t, count(*) AS c FROM memories GROUP BY t ORDER BY t').all();
      push(true, 'type 分组', typeRows.map((r) => `${r.t} ${r.c}`).join(' / '), false);

      const stats = vectorStats(db, vecLoaded);
      let orphans = [];
      let noVector = null;
      if (stats.hasVecMap) {
        orphans = db.prepare('SELECT v.id FROM vec_map v LEFT JOIN memories m ON m.id = v.id WHERE m.id IS NULL ORDER BY v.id').all().map((r) => r.id);
        noVector = db.prepare('SELECT count(*) AS c FROM memories m LEFT JOIN vec_map v ON v.id = m.id WHERE v.id IS NULL').get().c;
        const knownOrphans = new Set(['m_mw07uksm_t07f', 'm_mw07ukwa_kp41', 'm_mw07ukzc_4c0e']);
        const unexpected = orphans.filter((id) => !knownOrphans.has(id));
        push(unexpected.length === 0, '孤儿向量（vec_map 有、memories 没有）',
          orphans.length === 0
            ? '无'
            : `${orphans.length} 条${unexpected.length === 0 ? '（均在已知 3 条名单）' : `，其中意外 ${unexpected.length} 条：${unexpected.join(' / ')}`}：${orphans.join(' / ')}`,
          unexpected.length > 0);
        push(true, 'memories 里没有向量的条数', `${noVector} 条`, false);
      } else {
        push(true, '向量核对', '库里没有 vec_map，跳过（不是接入向量后的库）', false);
      }

      const meta = readMeta(db);
      const trueVectorCount = stats.vectorCount;
      if (tableExists(db, 'meta') && trueVectorCount !== null) {
        if (Number(meta.rows) !== trueVectorCount) writeMeta(db, 'rows', trueVectorCount);
        if (stats.dim && Number(meta.dim) !== stats.dim) writeMeta(db, 'dim', stats.dim);
        if (!meta.builtAt || Number.isNaN(Date.parse(meta.builtAt))) writeMeta(db, 'builtAt', new Date().toISOString());
      }
      const metaAfter = readMeta(db);
      const metaOk = trueVectorCount === null
        || (Number(metaAfter.rows) === trueVectorCount
          && (!stats.dim || Number(metaAfter.dim) === stats.dim)
          && Number.isFinite(Date.parse(metaAfter.builtAt)));
      push(metaOk, 'meta dim/rows/builtAt',
        trueVectorCount === null
          ? '无向量表，跳过'
          : `dim ${metaAfter.dim ?? '缺失'} / rows ${meta.rows ?? '缺失'}→${metaAfter.rows ?? '缺失'}（真实向量 ${trueVectorCount}）/ builtAt ${metaAfter.builtAt ?? '缺失'}`);

      const orderRows = db.prepare('SELECT id FROM memories ORDER BY rowSeq ASC').all().map((r) => r.id);
      const expectedOrder = scan.records.map((r) => r.obj.id);
      const orderMismatch = orderRows.length !== expectedOrder.length
        ? `长度 ${orderRows.length} vs ${expectedOrder.length}`
        : (() => {
          const idx = orderRows.findIndex((id, i) => id !== expectedOrder[i]);
          return idx === -1 ? null : `第 ${idx + 1} 条 ${orderRows[idx]} vs ${expectedOrder[idx]}`;
        })();
      push(orderMismatch === null, '回放顺序（rowSeq 顺序 == jsonl 行序）',
        orderMismatch === null ? `${orderRows.length} 条逐条一致` : `不一致：${orderMismatch}`);

      const step = Math.max(1, Math.floor(scan.records.length / 20));
      const sampleIdx = [];
      for (let i = 0; i < scan.records.length && sampleIdx.length < 20; i += step) sampleIdx.push(i);
      if (sampleIdx.length < 20 && scan.records.length > 0 && sampleIdx[sampleIdx.length - 1] !== scan.records.length - 1) {
        sampleIdx[sampleIdx.length - 1] = scan.records.length - 1;
      }
      const sampleProblems = [];
      for (const idx of sampleIdx) {
        const obj = scan.records[idx].obj;
        const dbRow = db.prepare(`${SELECT_SQL} WHERE id = ?`).get(obj.id);
        if (!dbRow) sampleProblems.push(`${obj.id}: 库里没有`);
        else {
          const problems = compareRow(obj, dbRow);
          if (problems.length) sampleProblems.push(`${obj.id}: ${problems.join('；')}`);
        }
      }
      push(sampleProblems.length === 0, '抽样 20 条逐字段对比',
        sampleProblems.length === 0
          ? `${sampleIdx.length} 条全部一致（含 tags/entities/supersedes JSON、缺字段=NULL）`
          : sampleProblems.join(' | '));

      const result = {
        dryRun: false,
        input: options.in,
        out: outPath,
        lines: scan.totalLines,
        ids: scan.records.length,
        inserted: counts.inserted,
        updated: counts.updated,
        skipped: counts.skipped,
        memories: memCount,
        status: statusMap,
        type: Object.fromEntries(typeRows.map((r) => [r.t, r.c])),
        orphans,
        noVector,
        vectors: trueVectorCount,
        vectorDim: stats.dim,
        meta: metaAfter,
        orderOk: orderMismatch === null,
        sampleOk: sampleProblems.length === 0,
        sampleProblems,
        checks,
        ok: fail.length === 0,
        elapsedMs: 0,
      };

      if (!result.ok) {
        db.exec('ROLLBACK;');
        result.out = outPath;
        result.elapsedMs = Date.now() - startedAt;
        result.error = `核对未过，已回滚：${fail.join('、')}`;
        return result;
      }
      db.exec('COMMIT;');
      try { db.exec('PRAGMA wal_checkpoint(TRUNCATE);'); } catch {}
      result.elapsedMs = Date.now() - startedAt;
      result.sizeBytes = fs.existsSync(outPath) ? fs.statSync(outPath).size : null;
      return result;
    } catch (error) {
      try { db.exec('ROLLBACK;'); } catch {}
      throw error;
    }
  } finally {
    try { db.close(); } catch {}
  }
}

function main() {
  let options;
  try {
    options = parseArgs(process.argv.slice(2));
  } catch (error) {
    process.stderr.write(`${error.message}\n\n${USAGE}\n`);
    process.exit(2);
  }
  if (options.help) {
    process.stdout.write(`${USAGE}\n`);
    return;
  }
  let result;
  try {
    result = migrate(options);
  } catch (error) {
    process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`);
    process.exit(options.dryRun ? 2 : 1);
  }
  if (options.json) {
    process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
    process.exit(result.ok ? 0 : 1);
  }
  if (result.dryRun) {
    process.stdout.write('迁移计划（--dry-run，只读，不建库）\n');
    process.stdout.write(`  输入 ${result.input}\n  输出 ${result.out}\n`);
    process.stdout.write(`  行 ${result.lines} / 唯一 id ${result.ids}｜将插入 ${result.inserted}、更新 ${result.updated}、跳过 ${result.skipped}\n`);
    if (result.badLines.length) process.stdout.write(`  坏行 ${result.badLines.length}\n`);
    if (result.duplicateIds.length) process.stdout.write(`  重复 id ${result.duplicateIds.length}\n`);
    return;
  }
  if (!result.ok) {
    process.stdout.write(`迁移未提交（已回滚）：${result.out}\n`);
    for (const check of result.checks) {
      process.stdout.write(`  ${check.ok ? 'OK  ' : 'FAIL'} ${check.item}: ${check.detail}\n`);
    }
    process.stdout.write(`核对未过：${result.error}\n`);
    process.exit(1);
  }
  process.stdout.write(`已迁移到 ${result.out}\n`);
  process.stdout.write(`  新写入 ${result.inserted}，更新 ${result.updated}，跳过 ${result.skipped}；耗时 ${result.elapsedMs}ms；库大小 ${(result.sizeBytes / 1024 / 1024).toFixed(1)}MB\n`);
  for (const check of result.checks) {
    process.stdout.write(`  ${check.ok ? 'OK  ' : 'FAIL'} ${check.item}: ${check.detail}\n`);
  }
  process.stdout.write('核对全过\n');
}

main();
