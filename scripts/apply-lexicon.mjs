#!/usr/bin/env node
/**
 * 把 data/entity-lexicon.json 应用到 entities 表。
 *
 * 规则：
 *   - 库里的 norm 命中名录主名/别名即算命中；norm 没命中再看 display（老实体 display 可能带
 *     原始大小写/路径，但 foldForMatch 后与名录同一套比较）；
 *   - 默认只改 etype='unknown' 的行；已经是具体类型的跳过并记数，不覆盖（名录是补洞，不是洗库）；
 *   - --force 才覆盖已有具体类型（只改「名录 etype 与当前不同」的行，同类型不算改动），
 *     用于主会话裁定后纠正错类；UPDATE 带上原 etype 做条件，并发变了就整批回滚；
 *   - 名录条目带 platform 且 etype=account 时，顺手把 attrs.platform 写成该值；
 *   - 一个名录名在库里 norm/display 都找不到，说明名录写错或库里没这个对象，单独列出来；
 *   - 真跑一个事务包住全部 UPDATE，逐条校验 changes=1，总数对不上就回滚，跑完 integrity_check。
 *
 * 用法：
 *   node scripts/apply-lexicon.mjs --dry-run            # 只看命中/跳过/未匹配，不写库
 *   node scripts/apply-lexicon.mjs                      # 真跑（务必先做整库备份）
 *   node scripts/apply-lexicon.mjs --force              # 连已有具体类型一起纠正
 *   node scripts/apply-lexicon.mjs --db /path/memory.db --lexicon /path/entity-lexicon.json --json
 */
import fs from 'node:fs';
import { openDb, resolveDbPath } from './lib/db.mjs';
import { foldForMatch, loadLexicon, LEXICON_URL } from '../lib/shared/entity-normalize.js';

const argv = process.argv.slice(2);
const argOf = (k, d) => (argv.includes(k) ? argv[argv.indexOf(k) + 1] : d);
const DB = argOf('--db', resolveDbPath());
const LEXICON = argOf('--lexicon', LEXICON_URL);
const DRY = argv.includes('--dry-run');
const FORCE = argv.includes('--force');
const JSON_OUT = argv.includes('--json');

function readLexiconEntries(file) {
  let doc;
  try {
    doc = JSON.parse(fs.readFileSync(file, 'utf8'));
  } catch (err) {
    throw new Error(`读不了名录 ${String(file)}：${err.message}`);
  }
  const entries = [];
  for (const [key, items] of Object.entries(doc)) {
    if (!Array.isArray(items)) continue;
    for (const it of items) {
      const etype = String(it?.etype || key || '').toLowerCase();
      const keys = [it?.name, ...(Array.isArray(it?.aliases) ? it.aliases : [])]
        .map((n) => foldForMatch(n)).filter(Boolean);
      entries.push({
        name: String(it?.name ?? ''),
        etype,
        note: String(it?.note ?? ''),
        platform: String(it?.platform ?? ''),
        keys: [...new Set(keys)],
      });
    }
  }
  return entries;
}

function distribution(db) {
  const out = {};
  for (const r of db.prepare('SELECT etype, count(*) AS n FROM entities GROUP BY etype ORDER BY n DESC').all()) {
    out[String(r.etype)] = Number(r.n);
  }
  return out;
}

function bump(map, key) {
  map[key] = (map[key] || 0) + 1;
}

const entries = readLexiconEntries(LEXICON);
const index = loadLexicon(LEXICON);
/** norm → 名录条目（首条优先，与 loadLexicon 的索引口径一致）。 */
const byKey = new Map();
for (const e of entries) for (const k of e.keys) if (!byKey.has(k)) byKey.set(k, e);

const db = openDb(DB);
db.exec('PRAGMA busy_timeout = 15000;');

const before = distribution(db);
const rows = db.prepare('SELECT id, norm, display, etype, status FROM entities ORDER BY id').all();

const matchedKeys = new Set();
const toUpdate = [];
const skipped = [];
for (const row of rows) {
  const normKey = foldForMatch(row.norm);
  const displayKey = foldForMatch(row.display);
  const hit = byKey.get(normKey) || byKey.get(displayKey);
  if (!hit) continue;
  matchedKeys.add(normKey);
  matchedKeys.add(displayKey);
  const current = String(row.etype || 'unknown');
  if (current === 'unknown' || (FORCE && current !== hit.etype)) {
    toUpdate.push({
      id: Number(row.id),
      norm: String(row.norm),
      display: String(row.display),
      current,
      etype: hit.etype,
      forced: current !== 'unknown',
      platform: hit.etype === 'account' ? hit.platform : '',
    });
  } else {
    skipped.push({ id: Number(row.id), norm: String(row.norm), display: String(row.display), current, etype: hit.etype });
  }
}

const forcedList = toUpdate.filter((u) => u.forced);
const platformWrites = toUpdate.filter((u) => u.platform);
const hitByType = {};
for (const u of toUpdate) bump(hitByType, u.etype);
const skipByType = {};
for (const s of skipped) bump(skipByType, `${s.current}->${s.etype}`);
const lexiconByType = {};
for (const e of entries) bump(lexiconByType, e.etype);

const unmatchedList = [];
for (const e of entries) {
  const present = e.keys.filter((k) => matchedKeys.has(k));
  if (!present.length) unmatchedList.push({ name: e.name, etype: e.etype, keys: e.keys });
}

const result = {
  db: String(DB),
  lexicon: String(LEXICON),
  dryRun: DRY,
  lexiconEntries: entries.length,
  lexiconByType,
  force: FORCE,
  forced: forcedList.length,
  platformWrites: platformWrites.map((u) => ({ norm: u.norm, platform: u.platform })),
  entities: rows.length,
  hits: toUpdate.length + skipped.length,
  toUpdate: toUpdate.length,
  toUpdateByType: hitByType,
  skipped: skipped.length,
  skippedByType: skipByType,
  unmatched: unmatchedList.length,
  before,
  after: null,
  changed: 0,
  integrity: null,
};

if (DRY) {
  result.after = before;
  if (JSON_OUT) {
    console.log(JSON.stringify({ ...result, toUpdateList: toUpdate, skippedList: skipped, unmatchedList }, null, 2));
  } else {
    console.log(`[dry-run] 库 ${DB}`);
    console.log('名录 ' + entries.length + ' 条：' + Object.entries(lexiconByType).map(([k, v]) => `${k} ${v}`).join(' / '));
    console.log(`实体 ${rows.length} 条，命中 ${result.hits} 条：待改 ${toUpdate.length} 条，跳过 ${skipped.length} 条`);
    console.log('待改分布：' + (Object.entries(hitByType).map(([k, v]) => `${k} ${v}`).join(' / ') || '无'));
    if (forcedList.length) {
      console.log('强改（--force，覆盖已有具体类型）：');
      for (const u of forcedList) console.log(`  ${u.norm}\t${u.current} -> ${u.etype}${u.platform ? `（platform=${u.platform}）` : ''}\t${u.display}`);
    }
    if (skipped.length) {
      console.log('跳过（当前已有具体类型，不覆盖）：');
      for (const s of skipped) console.log(`  ${s.norm}\t${s.current} -> 名录 ${s.etype}\t${s.display}`);
    }
    if (unmatchedList.length) {
      console.log('名录名在库里找不到：');
      for (const m of unmatchedList) console.log(`  ${m.name}（${m.etype}）keys=${m.keys.join(',')}`);
    }
    console.log('未写库（dry-run）。');
  }
  process.exit(0);
}

let changed = 0;
try {
  db.exec('BEGIN IMMEDIATE');
  const stmt = db.prepare(`UPDATE entities
      SET etype = ?,
          attrs = CASE WHEN ? = '' THEN attrs
                       ELSE json_set(CASE WHEN json_valid(attrs) THEN attrs ELSE '{}' END, '$.platform', ?) END
    WHERE id = ? AND etype = ?`);
  for (const u of toUpdate) {
    const r = stmt.run(u.etype, u.platform || '', u.platform || '', u.id, u.current);
    const n = Number(r.changes || 0);
    if (n !== 1) throw new Error(`实体 ${u.norm}（id=${u.id}）changes=${n}，期望 1`);
    changed += n;
  }
  if (changed !== toUpdate.length) throw new Error(`改动数 ${changed} 与计划 ${toUpdate.length} 不一致`);
  db.exec('COMMIT');
} catch (err) {
  try { db.exec('ROLLBACK'); } catch { /* 已提交/未开事务 */ }
  console.error(`失败已回滚：${err.message}`);
  process.exit(1);
}

const after = distribution(db);
const integrity = db.prepare('PRAGMA integrity_check').get();
result.after = after;
result.changed = changed;
result.integrity = integrity ? String(Object.values(integrity)[0]) : '';
if (JSON_OUT) {
  console.log(JSON.stringify({ ...result, unmatchedList }, null, 2));
} else {
  console.log(`库 ${DB}`);
  console.log(`名录 ${entries.length} 条，实体 ${rows.length} 条，命中 ${result.hits} 条：改 ${changed} 条（含强改 ${forcedList.length} 条），跳过 ${skipped.length} 条`);
  if (platformWrites.length) console.log('attrs.platform：' + platformWrites.map((u) => `${u.norm}=${u.platform}`).join(', '));
  console.log('改前分布：' + JSON.stringify(before));
  console.log('改后分布：' + JSON.stringify(after));
  console.log(`unknown 剩余：${after.unknown || 0}`);
  if (unmatchedList.length) console.log("名录名在库里找不到：" + unmatchedList.map((m) => m.name).join(", "));
  console.log(`integrity_check：${result.integrity}`);
}
if (String(result.integrity).toLowerCase() !== 'ok') process.exit(1);
