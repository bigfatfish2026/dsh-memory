#!/usr/bin/env node
/**
 * 回填：给 keep 集的空实体记忆补实体。
 *
 * keep 集 = active（status IS NULL/''）且不在 entity_excluded（§12.1 D 类 839 条划线）。
 * 两段走：
 *   1) 规则段（零模型成本）：用 entities 表已有 norm 建词典，正文折叠后最长匹配，最多 4 个；
 *   2) agent 段：规则段零命中的才进模型，一次 8 条，温度 0，只许从原文取，型号/版本进 attrs；
 *      etype 落 unknown 的不写 memory_entity，只进 entity_issue。
 *
 * 幂等：memory_entity INSERT OR IGNORE；entities 冲突只更新 last_seen/mem_count。
 * 用法：
 *   node scripts/backfill-entities.mjs --rule-only
 *   node scripts/backfill-entities.mjs --agent-only [--agent-limit N]
 */
import { openDb, resolveDbPath } from './lib/db.mjs';
import { normalizeEntity, foldForMatch, E_TYPES } from '../lib/shared/entity-normalize.js';
import { ENTITY_SYSTEM } from '../lib/agents/entity.js';

const argv = process.argv.slice(2);
const argOf = (k, d) => (argv.includes(k) ? argv[argv.indexOf(k) + 1] : d);
const DB = argOf('--db', resolveDbPath());
const RULE_ONLY = argv.includes('--rule-only');
const AGENT_ONLY = argv.includes('--agent-only');
const AGENT_LIMIT = Number(argOf('--agent-limit', '100000'));
const BATCH_SIZE = Number(argOf('--batch-size', '8'));
const CALL_TIMEOUT_MS = Number(argOf('--call-timeout', String(Math.max(20000, 4000 * BATCH_SIZE))));
const GATEWAY = argOf('--gateway', 'http://127.0.0.1:3425/v1');
const MODEL = argOf('--model', 'group/ds');

const db = openDb(DB);
db.exec('PRAGMA busy_timeout = 15000;');

function gatewayKey() {
  const key = String(process.env.MAGPIE_GATEWAY_KEY || '').trim();
  if (!key) throw new Error('缺少 MAGPIE_GATEWAY_KEY：请在环境变量里配置，脚本不再读取用户家目录下的凭据文件');
  return key;
}

const nowIso = () => new Date().toISOString();
const st = {
  db: DB, ruleOnly: RULE_ONLY, agentOnly: AGENT_ONLY,
  keep: 0, keepEmpty: 0,
  rule: { matchedMems: 0, pairs: 0, noHit: 0, ms: 0 },
  agent: { calls: 0, items: 0, accepted: 0, unknown: 0, notInSource: 0, failed: 0, ms: 0, batches: 0 },
  activeHasEntityBefore: 0, activeTotal: 0,
};

const before = db.prepare(`SELECT count(*) c FROM memories m WHERE coalesce(m.status,'')=''
  AND exists(SELECT 1 FROM memory_entity me WHERE me.memory_id=m.id)`).get().c;
const activeTotal = db.prepare(`SELECT count(*) c FROM memories WHERE coalesce(status,'')=''`).get().c;
st.activeHasEntityBefore = before;
st.activeTotal = activeTotal;

const emp = db.prepare(`SELECT m.id, m.text, m.ts FROM memories m
  WHERE coalesce(m.status,'')=''
    AND NOT EXISTS (SELECT 1 FROM entity_excluded ex WHERE ex.memory_id=m.id)
    AND NOT EXISTS (SELECT 1 FROM memory_entity me WHERE me.memory_id=m.id)
  ORDER BY m.id`);
const empties = emp.all();
st.keepEmpty = empties.length;
st.keep = db.prepare(`SELECT count(*) c FROM memories m WHERE coalesce(m.status,'')=''
  AND NOT EXISTS (SELECT 1 FROM entity_excluded ex WHERE ex.memory_id=m.id)`).get().c;

const getEntity = db.prepare('SELECT id, etype FROM entities WHERE norm=?');
const insEntity = db.prepare(`INSERT INTO entities(norm, display, etype, attrs, first_seen, last_seen, mem_count, status)
  VALUES (?, ?, ?, ?, ?, ?, 0, 'active')
  ON CONFLICT(norm) DO UPDATE SET last_seen=CASE WHEN excluded.last_seen>entities.last_seen THEN excluded.last_seen ELSE entities.last_seen END`);
const insME = db.prepare(`INSERT OR IGNORE INTO memory_entity(memory_id, entity_id, role, ts, status, attrs, version, prev_memory_id)
  VALUES (?, ?, 'about', ?, ?, ?, 0, '')`);
const insIssue = db.prepare(`INSERT OR IGNORE INTO entity_issue(memory_id, entity_norm, raw_name, reason, detail, created_at)
  VALUES (?, ?, ?, ?, ?, ?)`);

function upsertEntity(norm, display, etype, attrs, ts) {
  let ent = getEntity.get(norm);
  if (!ent) {
    insEntity.run(norm, display, etype, JSON.stringify(attrs || {}), ts, ts);
    ent = getEntity.get(norm);
  } else if (ts) {
    insEntity.run(norm, display, etype, JSON.stringify(attrs || {}), ts, ts);
    ent = getEntity.get(norm);
  }
  return ent;
}

/* ── 规则段 ─────────────────────────────────────────────────────────── */
const WORD = /[a-z0-9_]/;
function buildDict() {
  const seen = new Set();
  const dict = [];
  for (const e of db.prepare('SELECT id, norm, etype FROM entities').all()) {
    const fl = foldForMatch(e.norm);
    if (!fl || fl.length < 2 || /^\d+$/.test(fl) || seen.has(fl)) continue;
    seen.add(fl);
    dict.push({ id: e.id, norm: e.norm, fl, etype: e.etype });
  }
  dict.sort((a, b) => b.fl.length - a.fl.length);
  return dict;
}

function matchEntities(text, dict) {
  const folded = foldForMatch(text);
  const used = [];
  const hits = [];
  for (const d of dict) {
    if (hits.length >= 4) break;
    let from = 0;
    while (from <= folded.length - d.fl.length) {
      const i = folded.indexOf(d.fl, from);
      if (i < 0) break;
      from = i + 1;
      const before = i > 0 ? folded[i - 1] : '';
      const after = folded[i + d.fl.length] || '';
      if (WORD.test(d.fl[0]) && before && WORD.test(before)) continue;
      if (WORD.test(d.fl[d.fl.length - 1]) && after && WORD.test(after)) continue;
      if (used.some(([s, e]) => i < e && i + d.fl.length > s)) continue;
      hits.push({ ...d, at: i });
      used.push([i, i + d.fl.length]);
      break;
    }
  }
  return hits;
}

async function runRule() {
  const t0 = Date.now();
  const dict = buildDict();
  db.exec('BEGIN IMMEDIATE');
  try {
    for (const m of empties) {
      const hits = matchEntities(m.text || '', dict);
      if (!hits.length) { st.rule.noHit += 1; continue; }
      st.rule.matchedMems += 1;
      for (const h of hits) {
        const ent = upsertEntity(h.norm, h.norm, h.etype, {}, m.ts || '');
        const r = insME.run(m.id, ent.id, m.ts || '', 'active', '{}');
        if (r.changes) st.rule.pairs += 1;
      }
    }
    db.exec('COMMIT');
  } catch (e) {
    try { db.exec('ROLLBACK'); } catch { /* ignore */ }
    throw e;
  }
  st.rule.ms = Date.now() - t0;
}

/* ── agent 段 ───────────────────────────────────────────────────────── */
// 提示词不再各写一份：直接复用 lib/agents/entity.js 的 ENTITY_SYSTEM，
// 类型表跟着 E_TYPES 走。这里只取 items；回填不传 candidates，links 回来是空的，忽略掉。
const SYSTEM = ENTITY_SYSTEM;

function extractJson(text) {
  if (!text) return null;
  let t = String(text).trim();
  const fence = t.match(/```(?:json)?\s*([\s\S]*?)```/);
  if (fence) t = fence[1].trim();
  try { return JSON.parse(t); } catch { /* continue */ }
  const start = t.indexOf('{');
  const end = t.lastIndexOf('}');
  if (start >= 0 && end > start) {
    try { return JSON.parse(t.slice(start, end + 1)); } catch { return null; }
  }
  return null;
}

async function callAgent(batch, key) {
  const payload = {
    model: MODEL,
    temperature: 0,
    messages: [
      { role: 'system', content: SYSTEM },
      { role: 'user', content: JSON.stringify({ items: batch.map((m, i) => ({ i: i + 1, text: String(m.text || '').slice(0, 400) })) }) },
    ],
  };
  const res = await fetch(`${GATEWAY}/chat/completions`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${key}` },
    body: JSON.stringify(payload),
    signal: AbortSignal.timeout(CALL_TIMEOUT_MS),
  });
  if (!res.ok) throw new Error(`gateway ${res.status}: ${(await res.text()).slice(0, 200)}`);
  const json = await res.json();
  return json?.choices?.[0]?.message?.content || '';
}

async function runAgent() {
  const key = gatewayKey();
  if (!key) throw new Error('MAGPIE_GATEWAY_KEY 读不到');
  const remain = db.prepare(`SELECT m.id, m.text, m.ts FROM memories m
    WHERE coalesce(m.status,'')=''
      AND NOT EXISTS (SELECT 1 FROM entity_excluded ex WHERE ex.memory_id=m.id)
      AND NOT EXISTS (SELECT 1 FROM memory_entity me WHERE me.memory_id=m.id)
    ORDER BY m.id LIMIT ?`).all(Math.min(AGENT_LIMIT, 100000));
  const t0 = Date.now();
  for (let off = 0; off < remain.length; off += BATCH_SIZE) {
    const batch = remain.slice(off, off + BATCH_SIZE);
    st.agent.batches += 1;
    st.agent.calls += 1;
    let content = '';
    try {
      content = await callAgent(batch, key);
    } catch (e) {
      st.agent.failed += 1;
      process.stderr.write(`[agent] batch ${off / BATCH_SIZE} failed: ${e.message}\n`);
      continue;
    }
    const parsed = extractJson(content);
    const items = Array.isArray(parsed?.items) ? parsed.items : [];
    db.exec('BEGIN IMMEDIATE');
    try {
      for (const it of items) {
        const idx = Number(it?.i) - 1;
        const m = batch[idx];
        if (!m) continue;
        const rawName = String(it?.name ?? '').trim();
        if (!rawName) continue;
        st.agent.items += 1;
        const folded = foldForMatch(m.text);
        if (!folded.includes(foldForMatch(rawName))) {
          st.agent.notInSource += 1;
          insIssue.run(m.id, foldForMatch(rawName), rawName, 'not_in_source', 'agent: A1 原文找不到', nowIso());
          continue;
        }
        const n = normalizeEntity(rawName);
        if (n.drop && n.drop !== 'single_char') continue;
        let etype = E_TYPES.includes(it?.etype) ? it.etype : 'unknown';
        if (etype === 'unknown' || n.drop === 'single_char') {
          st.agent.unknown += 1;
          insIssue.run(m.id, n.norm || foldForMatch(rawName), rawName, 'etype_unknown',
            `agent: etype=${String(it?.etype || '')}`, nowIso());
          continue;
        }
        if (n.etype !== 'unknown' && n.etype !== etype && !['concept'].includes(etype)) etype = n.etype;
        const attrs = (it?.attrs && typeof it.attrs === 'object') ? it.attrs : {};
        const ent = upsertEntity(n.norm, n.display || rawName, etype, attrs, m.ts || '');
        const r = insME.run(m.id, ent.id, m.ts || '', 'active', JSON.stringify(attrs));
        if (r.changes) st.agent.accepted += 1;
      }
      db.exec('COMMIT');
    } catch (e) {
      try { db.exec('ROLLBACK'); } catch { /* ignore */ }
      throw e;
    }
  }
  st.agent.ms = Date.now() - t0;
}

if (!AGENT_ONLY) await runRule();
if (!RULE_ONLY) {
  const left = db.prepare(`SELECT count(*) c FROM memories m WHERE coalesce(m.status,'')=''
    AND NOT EXISTS (SELECT 1 FROM entity_excluded ex WHERE ex.memory_id=m.id)
    AND NOT EXISTS (SELECT 1 FROM memory_entity me WHERE me.memory_id=m.id)`).get().c;
  st.agent.remainingBefore = left;
  if (left > 0) await runAgent();
}

// 重算 mem_count / first_seen / last_seen
db.exec('BEGIN IMMEDIATE');
try {
  const upd = db.prepare(`UPDATE entities SET mem_count=?, first_seen=?, last_seen=? WHERE id=?`);
  const agg = db.prepare(`SELECT count(*) c, coalesce(min(CASE WHEN ts<>'' THEN ts END),'') f,
    coalesce(max(CASE WHEN ts<>'' THEN ts END),'') l FROM memory_entity WHERE entity_id=?`);
  for (const row of db.prepare('SELECT id FROM entities').all()) {
    const a = agg.get(row.id);
    upd.run(a.c, a.f, a.l, row.id);
  }
  db.exec('COMMIT');
} catch (e) {
  try { db.exec('ROLLBACK'); } catch { /* ignore */ }
  throw e;
}

st.after = {
  activeTotal: db.prepare(`SELECT count(*) c FROM memories WHERE coalesce(status,'')=''`).get().c,
  keep: db.prepare(`SELECT count(*) c FROM memories m WHERE coalesce(m.status,'')=''
    AND NOT EXISTS (SELECT 1 FROM entity_excluded ex WHERE ex.memory_id=m.id)`).get().c,
  keepHasEntity: db.prepare(`SELECT count(*) c FROM memories m WHERE coalesce(m.status,'')=''
    AND NOT EXISTS (SELECT 1 FROM entity_excluded ex WHERE ex.memory_id=m.id)
    AND EXISTS (SELECT 1 FROM memory_entity me WHERE me.memory_id=m.id)`).get().c,
  activeHasEntity: db.prepare(`SELECT count(*) c FROM memories m WHERE coalesce(m.status,'')=''
    AND EXISTS (SELECT 1 FROM memory_entity me WHERE me.memory_id=m.id)`).get().c,
  keepEmpty: db.prepare(`SELECT count(*) c FROM memories m WHERE coalesce(m.status,'')=''
    AND NOT EXISTS (SELECT 1 FROM entity_excluded ex WHERE ex.memory_id=m.id)
    AND NOT EXISTS (SELECT 1 FROM memory_entity me WHERE me.memory_id=m.id)`).get().c,
  entities: db.prepare('SELECT count(*) c FROM entities').get().c,
  memory_entity: db.prepare('SELECT count(*) c FROM memory_entity').get().c,
};
console.log(JSON.stringify(st, null, 2));
db.close();
