#!/usr/bin/env node
/**
 * 合并票：把 dup-verdict-20261113.json 里 147 对 complementary + 10 对
 * merge_needed 合并成单条（新条取代 a、b 一并降 merged）。
 *
 * 两段：
 *   dry-run（默认）：读稿 JSON，跑三项硬校验——值 token 覆盖 / 长度 / 指代词——逐对报缺失。
 *   --apply        ：校验全过后在单事务里落库：插新条（extra.pair_id 幂等键）→ 给 a 写
 *                    supersedes 边（memory-link.js 统一入口，origin='curate'）→ b 写
 *                    merged_into 边（b 已非 active，统一入口的 target_is_active 会拒，
 *                    这里直插同一张表，rel 沿用既有 legacy 口径）→ a/b status=merged。
 *
 * 用法：
 *   node scripts/merge-review-20261114.mjs --draft <draft.json> [--report <out.json>]
 *   node scripts/merge-review-20261114.mjs --draft <draft.json> --apply [--backup-dir <数据根目录>]
 */
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { openDb, resolveDbPath, resolveMemoryFile, resolveMemoryRoot } from './lib/db.mjs';
import { writeSupersedesLink } from '../lib/memory-link.js';

const argv = process.argv.slice(2);
const argOf = (k, d) => (argv.includes(k) ? argv[argv.indexOf(k) + 1] : d);
const SRC = argOf('--src', resolveMemoryFile('dup-verdict-20261113.json'));
const DB = argOf('--db', resolveDbPath());
const DRAFT = argOf('--draft', resolveMemoryFile('merge-draft-20261114.json'));
const REPORT = argOf('--report', resolveMemoryFile('merge-review-20261114.json'));
const APPLY = argv.includes('--apply');
const ORIGIN = 'merge-review-20261114';

/* ── 值 token 抽取（规则冻结，改规则要改票）────────────────────────── */
const RE = {
  date: /\d{4}[-/.]\d{1,2}[-/.]\d{1,2}|\d{1,2}[-/月]\d{1,2}日?/g,
  version: /[vV]?\d+\.\d+(?:\.\d+)*/g,
  ip: /\d{1,3}(?:\.\d{1,3}){3}/g,
  path: /(?:~|\/|\.{1,2}\/)[\w@.\-/]*[\w\-]/g,
  file: /[\w\-.]+\.(?:js|mjs|cjs|json|jsonl|db|md|yml|yaml|py|sh|ts|txt|log|sql|toml|csv|gz|bin|patch|service)/g,
  cmd: /\b(?:npm|pnpm|node|npx|sqlite3|git|systemctl|journalctl|python3?|curl|jq|grep|sed|awk|rsync|ssh|tar|unzip|fc-list|vcgencmd|smartctl|df|free|lsblk|chmod|chown|kill)\b/g,
  ticket: /t-[a-z0-9]{4,}(?:-[a-z0-9]+)*/g,
  flag: /--[\w-]+/g,
  num: /\d[\d,]*(?:\.\d+)?/g,
};

function valueTokens(s) {
  const out = new Map(); // token -> 类别
  const add = (tok, kind) => {
    const t = String(tok || '').trim();
    if (!t) return;
    if (!out.has(t)) out.set(t, kind);
  };
  const text = String(s || '');
  for (const [kind, re] of Object.entries(RE)) {
    re.lastIndex = 0;
    for (const m of text.matchAll(re)) add(m[0], kind);
  }
  for (const m of text.matchAll(/`([^`]+)`/g)) add(m[1], 'cmd');
  return out;
}

const REFERENTIAL = /同上|见前条|见上文|见上条|同上条|参见前|见前述/;

function charLen(s) {
  return Array.from(String(s || '')).length;
}

function loadDraft(file) {
  const raw = JSON.parse(fs.readFileSync(file, 'utf8'));
  const list = Array.isArray(raw) ? raw : raw.drafts || [];
  const map = new Map();
  for (const item of list) {
    if (!item || !item.pair_id) continue;
    map.set(String(item.pair_id), item);
  }
  return map;
}

function loadPairs(db) {
  const verdict = JSON.parse(fs.readFileSync(SRC, 'utf8'));
  const get = db.prepare('SELECT id,text,type,tags,status,ts,validFrom,supersedes FROM memories WHERE id=?');
  const pairs = [];
  for (const p of verdict.verdicts) {
    if (p.verdict !== 'complementary' && p.verdict !== 'merge_needed') continue;
    const a = get.get(p.id_a);
    const b = get.get(p.id_b);
    if (!a || !b) {
      pairs.push({ pair_id: p.pair_id, verdict: p.verdict, error: 'source_missing' });
      continue;
    }
    pairs.push({ pair_id: p.pair_id, verdict: p.verdict, a, b });
  }
  return pairs;
}

function check(pair, draft) {
  const { a, b } = pair;
  if (!a || !b) return { pair_id: pair.pair_id, ok: false, error: 'source_missing' };
  if (!draft) return { pair_id: pair.pair_id, ok: false, error: 'no_draft' };
  const text = String(draft.text || '');
  const len = charLen(text);
  const sum = charLen(a.text) + charLen(b.text);
  const limit = Math.floor(sum * 1.2);
  const floor = Math.max(charLen(a.text), charLen(b.text));
  const tokens = valueTokens(`${a.text}\n${b.text}`);
  const missing = [];
  for (const [tok, kind] of tokens) {
    if (!text.includes(tok)) missing.push({ tok, kind });
  }
  const referential = REFERENTIAL.test(text);
  const lenOk = len <= limit && len >= floor;
  return {
    pair_id: pair.pair_id,
    ok: missing.length === 0 && lenOk && !referential,
    len,
    sum,
    limit,
    floor,
    lenOk,
    referential,
    tokenCount: tokens.size,
    missing,
    rewrites: Number(draft.rewrites || 0),
  };
}

const TYPE_RANK = { world: 3, observation: 2, experience: 1, opinion: 1 };

function pickType(a, b) {
  const ta = String(a.type || '').toLowerCase();
  const tb = String(b.type || '').toLowerCase();
  if (ta === tb) return ta || 'world';
  const [hi, lo] = TYPE_RANK[ta] >= TYPE_RANK[tb] ? [ta, tb] : [tb, ta];
  if (hi === 'world') return 'world';
  // 其余取字多的那条的 type
  return charLen(a.text) >= charLen(b.text) ? ta || hi : tb || hi;
}

function pickTags(a, b) {
  const set = new Set();
  for (const src of [a.tags, b.tags]) {
    if (!src) continue;
    let arr = src;
    if (typeof src === 'string') {
      try { arr = JSON.parse(src); } catch { arr = [src]; }
    }
    for (const t of arr || []) if (t) set.add(String(t));
  }
  return [...set];
}

function dateOnly(v) {
  const s = String(v || '');
  const m = s.match(/^\d{4}-\d{2}-\d{2}/);
  return m ? m[0] : '';
}

function newId() {
  const base = Date.now().toString(36);
  const rand = crypto.randomBytes(3).toString('base64url').replace(/[-_]/g, '').slice(0, 4);
  return `m_${base}_${rand}`;
}

function md5(file) {
  return crypto.createHash('md5').update(fs.readFileSync(file)).digest('hex');
}

function main() {
  if (argv.includes('--vectors')) {
    runVectors().catch((error) => {
      console.error('vector backfill failed:', error?.message || error);
      process.exitCode = 4;
    });
    return;
  }
  const db = openDb(DB, APPLY ? {} : { readOnly: true });
  db.exec('PRAGMA busy_timeout = 5000');
  const pairs = loadPairs(db);
  const draftMap = loadDraft(DRAFT);
  const results = pairs.map((p) => check(p, draftMap.get(p.pair_id)));
  const bad = results.filter((r) => !r.ok);
  const stats = {
    pairs: pairs.length,
    draftMissing: results.filter((r) => r.error === 'no_draft').length,
    pass: results.length - bad.length,
    fail: bad.length,
    tokenMissPairs: results.filter((r) => (r.missing || []).length).length,
    tokenMissTotal: results.reduce((n, r) => n + (r.missing || []).length, 0),
    lenFail: results.filter((r) => r.lenOk === false).length,
    referentialFail: results.filter((r) => r.referential).length,
    rewritesMax: Math.max(0, ...results.map((r) => r.rewrites || 0)),
    rewritesSum: results.reduce((n, r) => n + (r.rewrites || 0), 0),
  };

  const report = {
    ticket: 't-mwf6xolw-2xn5',
    mode: APPLY ? 'apply' : 'dry-run',
    at: new Date().toISOString(),
    src: SRC,
    db: DB,
    stats,
    failures: bad.map((r) => ({
      pair_id: r.pair_id, error: r.error || null, len: r.len, limit: r.limit, floor: r.floor,
      referential: r.referential, missing: (r.missing || []).slice(0, 20),
    })),
    results,
  };

  if (!APPLY) {
    fs.writeFileSync(REPORT, JSON.stringify(report, null, 1));
    console.log(JSON.stringify(stats, null, 1));
    for (const b of bad.slice(0, 40)) {
      console.log(`FAIL ${b.pair_id} ${b.error || ''} len=${b.len}/${b.limit}(floor ${b.floor}) ref=${b.referential} missing=${(b.missing || []).map((x) => x.tok).join(',')}`);
    }
    return;
  }

  // ── apply ──────────────────────────────────────────────────────────
  if (bad.length) {
    console.error(`refuse to apply: ${bad.length} pair(s) failed checks`);
    process.exit(2);
  }

  // 备份（WAL 安全：用 VACUUM INTO）
  const stamp = new Date().toISOString().replace(/[-:]/g, '').replace('T', '-').slice(0, 15);
  const backup = path.join(argOf('--backup-dir', resolveMemoryRoot()), `memory-before-merge-${stamp}.db`);
  db.exec(`VACUUM INTO '${backup.replace(/'/g, "''")}'`);

  const before = {
    active: db.prepare("SELECT count(*) c FROM memories WHERE status IS NULL OR status='active'").get().c,
    merged: db.prepare("SELECT count(*) c FROM memories WHERE status='merged'").get().c,
    archived: db.prepare("SELECT count(*) c FROM memories WHERE status='archived'").get().c,
    links: db.prepare('SELECT count(*) c FROM memory_link').get().c,
    rows: db.prepare('SELECT count(*) c FROM memories').get().c,
  };

  let maxRowSeq = db.prepare('SELECT coalesce(max(rowSeq),0) m FROM memories').get().m;
  const insertSql = `INSERT INTO memories (id,text,tags,source,ts,type,entities,validFrom,supersedes,updatedAt,status,reason,supersededBy,validTo,confidence,extra,present,rowSeq)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`;
  const insert = db.prepare(insertSql);
  const setStatus = db.prepare('UPDATE memories SET status=?, reason=?, updatedAt=? WHERE id=?');
  // 7 个占位符必须给 7 个参数：少给会让 created_ts 落 NULL，OR IGNORE 会把
  // NOT NULL 冲突静默吞掉，边一条都写不进去（2026-11-14 首跑实测踩过）。
  const linkEdge = db.prepare(
    'INSERT OR IGNORE INTO memory_link(src,dst,rel,origin,attr_key,ts,created_ts) VALUES (?,?,?,?,?,?,?)',
  );
  const findByPair = db.prepare("SELECT id FROM memories WHERE extra LIKE ? LIMIT 1");

  const nowIso = new Date().toISOString();
  const nowMs = Date.now();
  const applied = [];
  const skipped = [];
  let changes0 = 0;
  try {
    changes0 = db.prepare('SELECT total_changes() c').get().c;
  } catch { changes0 = 0; }

  // 同一条旧条可能同时出现在两对里（实测只有 1 条 archived 被 p347/p397 共用），
  // 边只能建一次，第二次遇到的只记 shared，不再 setStatus/建边。
  const usedOld = new Set();
  const shared = [];
  db.exec('BEGIN IMMEDIATE');
  try {
    for (const pair of pairs) {
      const draft = draftMap.get(pair.pair_id);
      const existing = findByPair.get(`%"pair_id":"${pair.pair_id}"%`);
      if (existing) {
        // 幂等重跑：补齐可能缺的边（首跑漏写时靠这里修）
        const { a, b } = pair;
        const ensure = (dst, rel) => {
          const hit = db.prepare('SELECT 1 AS ok FROM memory_link WHERE src=? AND dst=? AND rel=?').get(existing.id, dst, rel);
          if (hit) return 0;
          return linkEdge.run(existing.id, dst, rel, 'curate', '', nowIso, nowMs).changes;
        };
        const repaired = { supersedes: ensure(a.id, 'supersedes'), merged_into: ensure(b.id, 'merged_into') };
        skipped.push({ pair_id: pair.pair_id, reason: 'already_merged', new_id: existing.id, repaired });
        continue;
      }
      const { a, b } = pair;
      let id = newId();
      while (db.prepare('SELECT 1 AS ok FROM memories WHERE id=?').get(id)) id = newId();
      const type = pickType(a, b);
      const tags = pickTags(a, b);
      const validFrom = [dateOnly(a.validFrom) || dateOnly(a.ts), dateOnly(b.validFrom) || dateOnly(b.ts)]
        .filter(Boolean).sort()[0] || dateOnly(a.ts) || null;
      const confidence = Math.max(Number(a.confidence || 0) || 0, Number(b.confidence || 0) || 0) || null;
      const extra = { origin: ORIGIN, pair_id: pair.pair_id, merged_from: [a.id, b.id], verdict: pair.verdict };
      maxRowSeq += 1;
      insert.run(
        id, String(draft.text), JSON.stringify(tags), 'merge-review', nowIso, type, '[]',
        validFrom, JSON.stringify([a.id, b.id]), nowIso, null, null, null, null,
        confidence, JSON.stringify(extra), JSON.stringify(['id', 'text', 'tags', 'source', 'ts', 'type', 'entities', 'validFrom', 'supersedes', 'updatedAt', 'status', 'reason', 'confidence', 'extra']),
        maxRowSeq,
      );
      // a 一般是 active，走统一入口写 supersedes 边；若 a 已被前面的对合并过（usedOld），跳过
      const aReused = usedOld.has(a.id);
      const bReused = usedOld.has(b.id);
      const r1 = aReused
        ? { ok: false, reason: 'old_already_merged_in_earlier_pair' }
        : writeSupersedesLink(db, { src: id, dst: a.id, origin: 'curate', now: nowMs });
      // b 已非 active（archived/merged），统一入口 target 检查必拒，直插 merged_into
      const r2 = bReused ? { changes: 0 } : linkEdge.run(id, b.id, 'merged_into', 'curate', '', nowIso, nowMs);
      if (aReused) shared.push({ pair_id: pair.pair_id, old_id: a.id, role: 'a' });
      if (bReused) shared.push({ pair_id: pair.pair_id, old_id: b.id, role: 'b' });
      if (!aReused) setStatus.run('merged', `merged into ${id}`, nowIso, a.id);
      if (!bReused) setStatus.run('merged', `merged into ${id}`, nowIso, b.id);
      usedOld.add(a.id);
      usedOld.add(b.id);
      applied.push({
        pair_id: pair.pair_id, new_id: id, type, tags, verdict: pair.verdict,
        a: a.id, b: b.id, b_status_before: b.status, link_a: r1, link_b: r2.changes,
        shared_old: { a: aReused, b: bReused },
      });
    }
    db.exec('COMMIT');
  } catch (error) {
    try { db.exec('ROLLBACK'); } catch { /* ignore */ }
    console.error('apply failed:', error?.message || error);
    process.exit(3);
  }

  let changes1 = 0;
  try { changes1 = db.prepare('SELECT total_changes() c').get().c; } catch { changes1 = 0; }
  const integrity = db.prepare('PRAGMA integrity_check').all().map((r) => r.integrity_check).join(';');
  const after = {
    active: db.prepare("SELECT count(*) c FROM memories WHERE status IS NULL OR status='active'").get().c,
    merged: db.prepare("SELECT count(*) c FROM memories WHERE status='merged'").get().c,
    archived: db.prepare("SELECT count(*) c FROM memories WHERE status='archived'").get().c,
    links: db.prepare('SELECT count(*) c FROM memory_link').get().c,
    rows: db.prepare('SELECT count(*) c FROM memories').get().c,
  };
  const picked = [
    ...applied.slice(0, 4),
    ...applied.slice(Math.max(4, Math.floor(applied.length / 2) - 2), Math.floor(applied.length / 2) + 2),
    ...applied.slice(-4),
  ];
  const spot = picked.map((r) => {
    const row = db.prepare('SELECT id,text,status FROM memories WHERE id=?').get(r.new_id);
    const a = db.prepare('SELECT id,status,reason FROM memories WHERE id=?').get(r.a);
    const b = db.prepare('SELECT id,status,reason FROM memories WHERE id=?').get(r.b);
    const links = db.prepare("SELECT src,dst,rel,origin FROM memory_link WHERE src=? AND dst IN (?,?)").all(r.new_id, r.a, r.b);
    return { pair_id: r.pair_id, new: row, a, b, links };
  });

  let prevReport = {};
  try { prevReport = JSON.parse(fs.readFileSync(REPORT, 'utf8')); } catch { /* 首跑没有旧报告 */ }
  const prevList = prevReport.apply?.appliedList || [];
  const thisRun = applied.map((r) => ({ pair_id: r.pair_id, new_id: r.new_id, a: r.a, b: r.b, b_status_before: r.b_status_before, shared_old: r.shared_old }));
  report.history = [...(prevReport.history || []), {
    at: new Date().toISOString(), before, after, changes: changes1 - changes0, integrity,
    backup, backupMd5: md5(backup), applied: applied.length, skipped: skipped.length, shared: shared.length,
  }];
  report.apply = {
    before, after, changes: changes1 - changes0, integrity, backup, backupMd5: md5(backup), spot,
    applied: applied.length, appliedList: thisRun.length ? thisRun : prevList,
    skipped, shared,
  };
  fs.writeFileSync(REPORT, JSON.stringify(report, null, 1));
  console.log(JSON.stringify({ mode: 'apply', before, after, changes: changes1 - changes0, integrity, backup, backupMd5: report.apply.backupMd5, applied: applied.length, skipped: skipped.length, shared: shared.length }, null, 1));
}

/* ── 补向量：新条必须能在语义路上召回 ─────────────────────────────── */
async function embedBatch(texts, api) {
  const resp = await fetch(`${api}/embed`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ inputs: texts }),
  });
  if (!resp.ok) throw new Error(`embed failed: HTTP ${resp.status}`);
  const data = await resp.json();
  return data.embeddings;
}

async function runVectors() {
  const api = String(argOf('--api', process.env.DSH_EMBED_API || 'http://127.0.0.1:8080')).replace(/\/$/, '');
  const batch = Math.max(1, Number(argOf('--batch', 32)) || 32);
  const report = JSON.parse(fs.readFileSync(REPORT, 'utf8'));
  const ext = path.join(import.meta.dirname, '../node_modules/sqlite-vec-linux-arm64/vec0.so');
  const db = openDb(DB, { allowExtension: true });
  db.exec('PRAGMA busy_timeout = 15000');
  db.loadExtension(ext);
  // 名单以库为准：report 可能被后续幂等重跑覆盖，extra 里的 origin 才是落库事实
  const list = db
    .prepare("SELECT id AS new_id FROM memories WHERE extra LIKE ? AND source='merge-review'")
    .all(`%${ORIGIN}%`);
  if (!list.length) throw new Error('库里没有 origin=merge-review 的新条，先跑 --apply');
  const getText = db.prepare('SELECT text FROM memories WHERE id=?');
  const has = db.prepare('SELECT 1 AS ok FROM vec_map WHERE id=?');
  const nextRowid = db.prepare('SELECT coalesce(max(rowid),0)+1 AS n FROM vec_map');
  const insMap = db.prepare('INSERT INTO vec_map(rowid, id) VALUES (?, ?)');
  const insVec = db.prepare('INSERT INTO vec_items(rowid, embedding) VALUES (?, ?)');
  const todo = list.filter((r) => !has.get(r.new_id));
  let done = 0;
  const started = Date.now();
  for (let i = 0; i < todo.length; i += batch) {
    const slice = todo.slice(i, i + batch);
    const texts = slice.map((r) => String(getText.get(r.new_id)?.text || '').slice(0, 2000));
    const vecs = await embedBatch(texts, api);
    let rowid = Number(nextRowid.get().n);
    db.exec('BEGIN IMMEDIATE');
    try {
      for (let j = 0; j < slice.length; j += 1) {
        const arr = Float32Array.from(vecs[j]);
        insVec.run(BigInt(rowid), new Uint8Array(arr.buffer, arr.byteOffset, arr.byteLength));
        insMap.run(rowid, slice[j].new_id);
        rowid += 1;
      }
      db.exec('COMMIT');
    } catch (error) {
      try { db.exec('ROLLBACK'); } catch { /* ignore */ }
      throw error;
    }
    done += slice.length;
    process.stdout.write(`\r${done}/${todo.length}`);
  }
  const dim = db.prepare("SELECT value FROM meta WHERE key='dim'").get()?.value;
  report.vectors = { at: new Date().toISOString(), api, inserted: done, already: list.length - todo.length, dim, seconds: Math.round((Date.now() - started) / 1000) };
  fs.writeFileSync(REPORT, JSON.stringify(report, null, 1));
  console.log(`\n${JSON.stringify(report.vectors)}`);
}

main();
