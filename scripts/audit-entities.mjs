#!/usr/bin/env node
/**
 * 审计：W4 违规 + 四类脏名（只记不删）。
 *
 * 依据 §4.4 A1（实体名必须在原文出现）与 §12.1 / 任务书"脏名四类"：
 *   单字 / 含版本号 / Pi4 类纯字母数字 / 大小写变体。
 * 处理原则：能靠归一化自动合并的直接合（大小写变体必然合；版本号剥出 base 也算合）；
 * 合不了的进 entity_issue，原行、旧 JSON 列一个字不删。
 *
 * 用法：node scripts/audit-entities.mjs [--db ...] [--apply]
 *   不带 --apply 只统计；带 --apply 才写 entity_issue（幂等 INSERT OR IGNORE）。
 */
import { openDb, resolveDbPath } from './lib/db.mjs';
import { normalizeEntity, foldForMatch, splitVersion } from '../lib/shared/entity-normalize.js';

const argv = process.argv.slice(2);
const argOf = (k, d) => (argv.includes(k) ? argv[argv.indexOf(k) + 1] : d);
const DB = argOf('--db', resolveDbPath());
const APPLY = argv.includes('--apply');

const db = openDb(DB);
db.exec('PRAGMA busy_timeout = 15000;');

const rows = db.prepare('SELECT id, text, entities, status FROM memories').all();
// 时间戳每行写入时现取：早先这里是脚本启动常量，一次 --apply 全库共用一个时刻。
const nowIso = () => new Date().toISOString();
const insIssue = APPLY
  ? db.prepare(`INSERT OR IGNORE INTO entity_issue(memory_id, entity_norm, raw_name, reason, detail, created_at)
      VALUES (?, ?, ?, ?, ?, ?)`)
  : null;

const variantsByNorm = new Map(); // norm -> Map(raw -> count)
const foldedById = new Map();     // memory id -> 折叠正文（只缓存挂了实体的行）
const docsByNorm = new Map();     // norm -> Set(memory id)，这个 norm 挂在哪几条记忆上
const wouldIssue = new Set();     // `${memory_id}|${norm}`，新判据下 not_in_source 会写的行数
const activeByNorm = new Map();   // norm -> active 元素数
const elemByNorm = new Map();     // norm -> 全库元素数
const normEtype = new Map();      // norm -> etype（规则判的）

const st = {
  scannedRows: 0, elemTotal: 0, emptySkipped: 0,
  global: { entities: 0, violElems: 0, violMems: 0 },
  active: { entities: 0, violElems: 0, violMems: 0 },
  dirty: {
    single_char: { all: 0, active: 0, autoFixed: 0, issue: 0, samples: [] },
    has_version: { all: 0, active: 0, autoFixed: 0, issue: 0, samples: [] },
    alpha_num: { all: 0, active: 0, autoFixed: 0, issue: 0, samples: [] },
    case_variant: { all: 0, active: 0, autoFixed: 0, issue: 0, samples: [], groups: 0 },
  },
  issueRows: 0,
  wouldIssue: { not_in_source: 0 },
  sourceFound: { all: 0, allTotal: 0, active: 0, activeTotal: 0, viaMounts: 0 },
};

function sample(bucket, id) {
  if (bucket.samples.length < 3 && !bucket.samples.includes(id)) bucket.samples.push(id);
}

// 第一遍：收集大小写变体组
for (const r of rows) {
  if (!r.entities || !String(r.entities).trim()) continue;
  let arr;
  try { arr = JSON.parse(r.entities); } catch { continue; }
  if (!Array.isArray(arr)) arr = [arr];
  foldedById.set(r.id, foldForMatch(r.text));
  for (const item of arr) {
    const raw = String(item ?? '').trim();
    if (!raw) continue;
    const n = normalizeEntity(raw);
    if (n.drop === 'empty') continue;
    let docs = docsByNorm.get(n.norm);
    if (!docs) { docs = new Set(); docsByNorm.set(n.norm, docs); }
    docs.add(r.id);
    let m = variantsByNorm.get(n.norm);
    if (!m) { m = new Map(); variantsByNorm.set(n.norm, m); }
    m.set(raw, (m.get(raw) || 0) + 1);
    elemByNorm.set(n.norm, (elemByNorm.get(n.norm) || 0) + 1);
    if (!r.status) activeByNorm.set(n.norm, (activeByNorm.get(n.norm) || 0) + 1);
    normEtype.set(n.norm, n.etype);
  }
}

const caseGroups = new Map(); // norm -> {count, variants:[...]}
for (const [norm, m] of variantsByNorm) {
  if (m.size < 2) continue;
  const variants = [...m.keys()];
  const lowerSet = new Set(variants.map((v) => v.toLowerCase()));
  const hasCase = lowerSet.size < variants.length;
  if (!hasCase) continue;
  const count = [...m.values()].reduce((a, b) => a + b, 0);
  caseGroups.set(norm, { count, variants });
  st.dirty.case_variant.all += count;
  st.dirty.case_variant.active += activeByNorm.get(norm) || 0;
  st.dirty.case_variant.autoFixed += count;
  st.dirty.case_variant.groups += 1;
}

// 第二遍：逐行判 A1 + 脏名，写 issue
db.exec('BEGIN IMMEDIATE');
try {
  for (const r of rows) {
    st.scannedRows += 1;
    const isActive = !r.status;
    const folded = foldedById.get(r.id) ?? foldForMatch(r.text);
    if (!r.entities || !String(r.entities).trim()) continue;
    let arr;
    try { arr = JSON.parse(r.entities); } catch { continue; }
    if (!Array.isArray(arr)) arr = [arr];
    const seenNorm = new Set();
    for (const item of arr) {
      const raw = String(item ?? '').trim();
      if (!raw) { st.emptySkipped += 1; continue; }
      const n = normalizeEntity(raw);
      if (n.drop === 'empty') { st.emptySkipped += 1; continue; }
      st.elemTotal += 1;

      // A1：norm 或原始写法在折叠后的正文里出现都算过（版本剥离后 base 在原文）。
      // 2026-11-14：实体名来自上下文别段是正常行为，只查本条正文实测 97% 误报。
      // 新判据——该实体所有挂载记忆的正文里都找不到这个名字才记 issue；
      // 名字在它任意一条挂载记忆里出现过就算有出处。
      const hitIn = (body) => body.includes(foldForMatch(n.norm)) || body.includes(foldForMatch(raw));
      let inSource = hitIn(folded);
      if (!inSource) {
        for (const mid of docsByNorm.get(n.norm) || []) {
          if (mid === r.id) continue;
          const other = foldedById.get(mid);
          if (other && hitIn(other)) { inSource = true; st.sourceFound.viaMounts += 1; break; }
        }
      }
      st.sourceFound.allTotal += 1;
      if (isActive) st.sourceFound.activeTotal += 1;
      if (inSource) {
        st.sourceFound.all += 1;
        if (isActive) st.sourceFound.active += 1;
      }

      st.global.entities += 1;
      if (isActive) st.active.entities += 1;
      if (!inSource) {
        st.global.violElems += 1;
        if (isActive) st.active.violElems += 1;
        if (!seenNorm.has(n.norm)) {
          seenNorm.add(n.norm);
        }
        wouldIssue.add(`${r.id}|${n.norm}`);
        if (APPLY) {
          const res = insIssue.run(r.id, n.norm, raw, 'not_in_source', 'A1: 该实体所有挂载记忆的正文里都找不到这个名字', nowIso());
          st.issueRows += Number(res.changes || 0);
        }
      }

      // 脏名四类
      const isDirty = (kind, extra) => {
        const b = st.dirty[kind];
        b.all += 1;
        if (isActive) b.active += 1;
        sample(b, r.id);
        if (APPLY) {
          const res = insIssue.run(r.id, n.norm, raw, kind, extra, nowIso());
          st.issueRows += Number(res.changes || 0);
        }
        b.issue += 1;
      };

      if ([...raw].length === 1) {
        isDirty('single_char', 'R10 单字');
        continue;
      }

      const ver = /\d+\.\d+/.test(raw) || /(?:^|[\s-])v?\d+$/.test(raw);
      if (ver) {
        const sv = splitVersion(foldForMatch(raw));
        if (sv && sv.base && (sv.version || sv.model)) {
          st.dirty.has_version.all += 1;
          if (isActive) st.dirty.has_version.active += 1;
          st.dirty.has_version.autoFixed += 1;
          sample(st.dirty.has_version, r.id);
        } else {
          isDirty('has_version', 'R20 剥不出 base，保留原名');
        }
        continue;
      }

      if (/^[A-Za-z]+\d+$/.test(raw)) {
        const sv = splitVersion(foldForMatch(raw));
        if (sv && sv.model) {
          st.dirty.alpha_num.all += 1;
          if (isActive) st.dirty.alpha_num.active += 1;
          st.dirty.alpha_num.autoFixed += 1;
          sample(st.dirty.alpha_num, r.id);
        } else {
          isDirty('alpha_num', '纯字母数字，无 base 可并');
        }
        continue;
      }
    }

    // 违规记忆数（去重）
    if (!seenNorm.size) continue;
    st.global.violMems += 1;
    if (isActive) st.active.violMems += 1;
  }
  db.exec('COMMIT');
} catch (e) {
  try { db.exec('ROLLBACK'); } catch { /* ignore */ }
  throw e;
}

st.dirty.case_variant.issue = 0;
st.caseGroupsTop = [...caseGroups.entries()].slice(0, 5).map(([n, v]) => ({ norm: n, variants: v.variants.slice(0, 6), count: v.count }));
st.wouldIssue.not_in_source = wouldIssue.size;
st.coverage = {
  global: +(st.sourceFound.all / Math.max(1, st.sourceFound.allTotal)).toFixed(4),
  active: +(st.sourceFound.active / Math.max(1, st.sourceFound.activeTotal)).toFixed(4),
};
console.log(JSON.stringify(st, null, 2));
db.close();
