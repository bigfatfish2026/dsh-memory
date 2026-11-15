#!/usr/bin/env node
/**
 * 划线：把 §12.1 D 类的 active 记忆标进 entity_excluded（只标不删，不回填）。
 *
 * 判据（docs/dsh-memory-entity-rework-2026.md §12.1 D 类）：
 *   - 范围：active（status IS NULL/''）且 source 属 split 家族（source='split' 或 sleep:split%）
 *   - 无具体值：正文抽不出任何值 token（IP / 版本 / 票号 / 路径 / 文件 / 含数字标识符 / 长数字）
 *   - 零独立佐证：有具体值，但每个值 token 在"独立来源"（source 不属 split 家族、也不是 sleep:merge
 *     的 4665 条）的正文里都找不到
 * 处置：无具体值 → scope='shadow'（降 shadow 候选），零佐证 → scope='drop'（作废候选）。
 * 两者都只进 entity_excluded，不动 memories 行、不动 status。
 *
 * 用法：node scripts/exclude-entities.mjs [--apply] [--db ...]
 */
import { openDb, resolveDbPath } from './lib/db.mjs';
import { foldForMatch } from '../lib/shared/entity-normalize.js';

const argv = process.argv.slice(2);
const argOf = (k, d) => (argv.includes(k) ? argv[argv.indexOf(k) + 1] : d);
const DB = argOf('--db', resolveDbPath());
const APPLY = argv.includes('--apply');

const db = openDb(DB);
db.exec('PRAGMA busy_timeout = 15000;');

const isSplitFamily = (src) => src === 'split' || String(src || '').startsWith('sleep:split');
const isIndependent = (src) => !isSplitFamily(src) && src !== 'sleep:merge';

const rows = db.prepare('SELECT id, text, source, status, ts FROM memories').all();

// 独立来源正文：先抽成 token 集合（比每 token 扫 1.4MB 正文快两个数量级）

const TOKEN_RES = [
  /\b(?:\d{1,3}\.){3}\d{1,3}\b/g,                                  // IPv4
  /\bt-[a-z0-9]{8}-[a-z0-9]{4}\b/gi,                               // 票号
  /(?:~|\/)[\w.\-\/]{2,}/g,                                        // 路径
  /\b[\w.-]+\.(?:sh|js|mjs|py|md|log|png|jpg|webp|db|yml|yaml|json|toml|ini|conf|env|service|timer|socket|txt|bin)\b/gi, // 文件
  /\b[a-z][a-z0-9_-]*\d[a-z0-9_.-]*\b/gi,                          // 含数字标识符（wd14/arm64/v4）
  /\b\d+[a-z][a-z0-9_.-]*\b/gi,                                    // 4k/512g
  /\bv?\d+\.\d+(?:\.\d+)*\b/g,                                     // 版本号
  /\b\d{4,}\b/g,                                                   // 长数字
];

function valueTokens(text) {
  const folded = foldForMatch(text);
  const out = new Set();
  for (const re of TOKEN_RES) {
    for (const m of folded.matchAll(re)) {
      const t = m[0].replace(/[.,;:!?、，。；：！？]+$/, '');
      if (t.length >= 2) out.add(t);
    }
  }
  return [...out];
}

const independentTokens = new Set();
{
  const collect = (text) => { for (const t of valueTokens(text)) independentTokens.add(t); };
  for (const r of rows) if (isIndependent(r.source)) collect(r.text || '');
}

const st = {
  db: DB, apply: APPLY,
  splitActive: 0, noValue: 0, zeroEvidence: 0, kept: 0, independentRows: 0,
  noValueSamples: [], zeroEvidenceSamples: [], keptSamples: [],
  bySource: {},
};
for (const r of rows) if (isIndependent(r.source)) st.independentRows += 1;

const ins = APPLY
  ? db.prepare(`INSERT INTO entity_excluded(memory_id, scope, reason, detail, ts, created_at)
      VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(memory_id) DO NOTHING`)
  : null;
const now = new Date().toISOString();

db.exec('BEGIN IMMEDIATE');
try {
  for (const r of rows) {
    if (r.status) continue;
    if (!isSplitFamily(r.source)) continue;
    st.splitActive += 1;
    const srcKey = String(r.source || '');
    st.bySource[srcKey] = (st.bySource[srcKey] || 0) + 1;
    const tokens = valueTokens(r.text || '');
    if (!tokens.length) {
      st.noValue += 1;
      if (st.noValueSamples.length < 3) st.noValueSamples.push(r.id);
      if (ins) ins.run(r.id, 'shadow', 'no_value', '正文抽不出任何值 token', r.ts || '', now);
      continue;
    }
    const found = tokens.filter((t) => independentTokens.has(t));
    if (!found.length) {
      st.zeroEvidence += 1;
      if (st.zeroEvidenceSamples.length < 3) st.zeroEvidenceSamples.push(r.id);
      if (ins) ins.run(r.id, 'drop', 'zero_evidence', `值 token 全无独立出处：${tokens.slice(0, 5).join(', ')}`, r.ts || '', now);
    } else {
      st.kept += 1;
      if (st.keptSamples.length < 3) st.keptSamples.push(r.id);
    }
  }
  db.exec('COMMIT');
} catch (e) {
  try { db.exec('ROLLBACK'); } catch { /* ignore */ }
  throw e;
}

st.excludedRows = db.prepare('SELECT count(*) c FROM entity_excluded').get().c;
st.excludedByReason = Object.fromEntries(
  db.prepare('SELECT reason, count(*) c FROM entity_excluded GROUP BY reason').all().map((x) => [x.reason, x.c]),
);
console.log(JSON.stringify(st, null, 2));
db.close();
