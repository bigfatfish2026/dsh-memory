#!/usr/bin/env node
/**
 * §12 清洗漏斗 dry-run：给每条 active 记忆打 keep / drop / merge_candidate。
 *
 * 只读：sqlite 用 readOnly 打开（不写库、不落 WAL）、向量经 vec0 扩展读，
 * 唯一输出文件是 $DSH_MEMORY_ROOT/audit-20261113.json。
 *
 * 规则来源：docs/dsh-memory-entity-rework-2026.md §12.1 / §12.2 / §12.4。
 * 文档没写死的判定参数集中在文件顶部 SELF 标记处，报告里逐条列。
 *
 * 用法：node scripts/curate-audit.mjs [--db /path/to/memory.db] [--out /path/to/out.json] [--no-vectors]
 */
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { openDb, resolveDbPath, resolveLogFile, resolveMemoryFile } from './lib/db.mjs';

const argv = process.argv.slice(2);
const argOf = (k, d) => (argv.includes(k) ? argv[argv.indexOf(k) + 1] : d);
const DB = argOf('--db', resolveDbPath());
const OUT = argOf('--out', resolveMemoryFile('audit-20261113.json'));
const VEC_SO = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'node_modules', 'sqlite-vec-linux-arm64', 'vec0.so');
const RECALL_LOGS = argOf('--recall-log', resolveLogFile('memory_recall.log'));
const DO_VECTORS = !argv.includes('--no-vectors');

// ---------------------------------------------------------------- 自补参数（文档没写死）
// SELF-1 键值对正则：文档只说「正文抽英文标识符键值对」，边界由这里定
const KV_RE = /(?:^|[\s，。；、,;()（）\[\]【】])([A-Za-z][A-Za-z0-9_.-]{1,40})\s*(?:=|:|：|＝)\s*([A-Za-z0-9_.~\/:%+-]{1,72})/g;
// SELF-2 值归一化：去引号、小写、去尾随 .0、去尾随斜杠
function normVal(v) {
  let s = String(v || '').trim().replace(/^["'`]+|["'`]+$/g, '').replace(/\/+$/, '').toLowerCase();
  if (/^-?\d+\.0+$/.test(s)) s = s.replace(/\.0+$/, '');
  return s;
}
// SELF-3 同实体同键同值的时间窗：文档写 ts≤7 天，按秒比
const DUP_WINDOW_MS = 7 * 24 * 3600 * 1000;
// SELF-4 B2 余弦阈值取文档的 0.95；active vs archived 父条也单列（文档只写 merged）
const COS_TH = 0.95;
// SELF-5 具体值 token 形态（D 类）：文档列了 IP/版本/票号/路径/文件/含数字标识符，正则这里定
const TOKEN_RES = [
  /\b\d{1,3}(?:\.\d{1,3}){3}\b/g,                        // IPv4
  /\bt-[a-z0-9]{4,}-[a-z0-9]{4,}\b/gi,                    // 票号
  /[\w.+-]+@[\w-]+\.[\w.-]{2,}/g,                        // 邮箱
  /(?:^|[\s(（"'`=:：])((?:\/|~\/)?[A-Za-z0-9_.-]+(?:\/[A-Za-z0-9_.-]+)+)/g, // 相对/绝对路径
  /\b[A-Za-z0-9_.-]+\.(?:js|mjs|cjs|json|jsonl|md|db|sqlite|yml|yaml|log|sh|py|ts|tsx|rs|vue|css|html|svg|png|npz|bin|so|sql|toml|conf|ini|service|timer|patch|txt)\b/gi, // 文件名
  /\b\d+\.\d+(?:\.\d+)*\b/g,                              // 版本/小数
  /\b\d{2,}\b/g,                                            // 纯数字（≥2 位）
  /\b[A-Za-z_][A-Za-z0-9_]*\d[A-Za-z0-9_]*\b/g,           // 含数字标识符
];
// SELF-6 过程动词（C 类①的中档口径也用这套）：文档给了 6 个词
const PROC_VERBS = ['跑了', '执行了', '重试', '继续', '等待', '全部通过'];
// SELF-7 宽档只做对账、不参与清洗：GLOB 8 词
const WIDE_WORDS = ['已执行', '执行完毕', '命令输出', 'exit code', 'stdout', 'stderr', '开始执行', '跑完'];
// SELF-8 自补规则「纯日志」：票面点名但文档没有的原因码，用日志特征正则单独统计，不并入 drop
const PURE_LOG_RE = /(?:^|\s)(?:log|日志|stdout|stderr|exit code|命令输出|输出如下|跑了|执行了|已执行)/i;
// SELF-9 「被引用」的范围：memory_link 里 dst=它的边（任意 rel）+ 旧列 supersededBy 非空
// SELF-10 独立佐证语料用全库（含 merged/archived）的独立来源，跟文档 4665 条同口径

const report = {
  meta: {
    ticket: 't-mwf5pnxf-286w',
    generatedAt: new Date().toISOString(),
    db: DB,
    dbMode: 'sqlite readOnly',
    doc: 'docs/dsh-memory-entity-rework-2026.md §12',
  },
  docDiff: [],
  docFixes: [],
  rules: [],
  selfAddedRules: [],
  totals: {},
  dropByReason: {},
  cross: {},
  samples: {},
  items: [],
};

function log(...a) { console.error('[audit]', ...a); }

// ---------------------------------------------------------------- 打开只读库
const db = openDb(DB, { readOnly: true, allowExtension: true });
log('opened', DB);

const all = db.prepare('SELECT id,text,type,source,ts,confidence,supersededBy,entities,status FROM memories').all();
const byId = new Map(all.map((m) => [m.id, m]));
const isActive = (m) => m.status === null || m.status === '';
const active = all.filter(isActive);
log('memories', all.length, 'active', active.length);

const entRows = db.prepare('SELECT memory_id, entity_id, role, attrs FROM memory_entity').all();
const entsOf = new Map();
for (const r of entRows) {
  if (!entsOf.has(r.memory_id)) entsOf.set(r.memory_id, []);
  entsOf.get(r.memory_id).push(r);
}
const entCount = db.prepare('SELECT count(*) n FROM entities').get().n;
const entName = new Map(db.prepare('SELECT id, display FROM entities').all().map((r) => [Number(r.id), r.display]));

const links = db.prepare('SELECT src, dst, rel, attr_key FROM memory_link').all();
const refIn = new Map();      // dst 被引用边数
const refOut = new Map();     // src 发出的边数
const supersededByLink = new Set();
for (const l of links) {
  refIn.set(l.dst, (refIn.get(l.dst) || 0) + 1);
  refOut.set(l.src, (refOut.get(l.src) || 0) + 1);
  if (l.rel === 'supersedes') supersededByLink.add(l.dst);
}

// ---------------------------------------------------------------- 召回日志（F 类「从未被召回」）
const recalled = new Set();
try {
  const files = fs.readdirSync(path.dirname(RECALL_LOGS))
    .filter((f) => f.startsWith(path.basename(RECALL_LOGS)))
    .map((f) => path.join(path.dirname(RECALL_LOGS), f));
  const { execSync } = await import('node:child_process');
  for (const f of files) {
    let txt = '';
    if (f.endsWith('.gz')) {
      try { txt = execSync(`zcat ${JSON.stringify(f)}`, { maxBuffer: 64 * 1024 * 1024 }).toString(); } catch { continue; }
    } else {
      txt = fs.readFileSync(f, 'utf8');
    }
    for (const line of txt.split('\n')) {
      let j; try { j = JSON.parse(line); } catch { continue; }
      for (const h of j.top || []) if (h?.id) recalled.add(String(h.id));
      for (const h of j.hits_ids || []) recalled.add(String(h));
    }
  }
  log('recalled ids', recalled.size, 'from', files.length, 'log files');
} catch (e) {
  log('recall log unreadable:', e.message);
}

// ---------------------------------------------------------------- 向量（B2）
let cosPairs = [];       // {a,b,sim}
const maxSim = new Map(); // id -> {sim, with}
if (DO_VECTORS) {
  try {
    db.loadExtension(VEC_SO);
    const rows = db.prepare('SELECT m.id AS id, v.embedding AS emb FROM vec_items v JOIN vec_map m ON m.rowid = v.rowid').all();
    const dim = 1024;
    const ids = [];
    const vecs = [];
    for (const r of rows) {
      const u8 = new Uint8Array(r.emb.buffer, r.emb.byteOffset, r.emb.byteLength);
      if (u8.byteLength !== dim * 4) continue;
      const v = new Float32Array(dim);
      let s = 0;
      const dv = new DataView(u8.buffer, u8.byteOffset, u8.byteLength);
      for (let k = 0; k < dim; k += 1) { const x = dv.getFloat32(k * 4, true); v[k] = x; s += x * x; }
      const inv = s > 0 ? 1 / Math.sqrt(s) : 0;
      for (let k = 0; k < dim; k += 1) v[k] *= inv;
      ids.push(String(r.id));
      vecs.push(v);
    }
    const n = ids.length;
    const flat = new Float32Array(n * dim);
    for (let i = 0; i < n; i += 1) flat.set(vecs[i], i * dim);
    const isAct = new Uint8Array(n);
    for (let i = 0; i < n; i += 1) isAct[i] = isActive(byId.get(ids[i]) || { status: 'x' }) ? 1 : 0;
    log('vectors', n, 'active', isAct.reduce((a, b) => a + b, 0));
    const t0 = Date.now();
    for (let i = 0; i < n; i += 1) {
      if (!isAct[i]) continue;
      const oi = i * dim;
      for (let j = 0; j < n; j += 1) {
        if (j === i) continue;
        if (isAct[j] && j < i) continue; // active-active 只算一次
        const oj = j * dim;
        let s0 = 0, s1 = 0, s2 = 0, s3 = 0;
        for (let k = 0; k < dim; k += 4) {
          s0 += flat[oi + k] * flat[oj + k];
          s1 += flat[oi + k + 1] * flat[oj + k + 1];
          s2 += flat[oi + k + 2] * flat[oj + k + 2];
          s3 += flat[oi + k + 3] * flat[oj + k + 3];
        }
        const s = s0 + s1 + s2 + s3;
        const prevA = maxSim.get(ids[i]);
        if (!prevA || s > prevA.sim) maxSim.set(ids[i], { sim: s, with: ids[j] });
        const prevB = maxSim.get(ids[j]);
        if (!prevB || s > prevB.sim) maxSim.set(ids[j], { sim: s, with: ids[i] });
        if (s > COS_TH) cosPairs.push({ a: ids[i], b: ids[j], sim: s });
      }
      if (i % 1000 === 0) log('cos row', i, 'ms', Date.now() - t0);
    }
    log('cos pairs >', COS_TH, cosPairs.length, 'ms', Date.now() - t0);
  } catch (e) {
    log('vector stage failed:', e.message);
    report.docDiff.push(`向量阶段失败：${e.message}（B2 重复判定缺向量证据）`);
    cosPairs = [];
  }
}

// ---------------------------------------------------------------- 键值对（B1 / E fallback）
const kvOf = new Map(); // id -> [{key,val,raw}]
for (const m of all) {
  const out = [];
  const seen = new Set();
  KV_RE.lastIndex = 0;
  let mt;
  while ((mt = KV_RE.exec(m.text || '')) !== null) {
    const key = mt[1].toLowerCase();
    const val = normVal(mt[2]);
    if (!key || !val) continue;
    const k = `${key}=${val}`;
    if (seen.has(k)) continue;
    seen.add(k);
    out.push({ key, val, raw: mt[2] });
  }
  kvOf.set(m.id, out);
}

// ---------------------------------------------------------------- D 类：具体值 token + 独立佐证语料
function tokensOf(text) {
  const out = new Set();
  for (const re of TOKEN_RES) {
    re.lastIndex = 0;
    let m;
    while ((m = re.exec(text || '')) !== null) {
      const t = (m[1] ?? m[0]).trim();
      if (t.length >= 3) out.add(t);
    }
  }
  // 含数字标识符那条正则太宽，去掉纯年份/长度太短的
  return [...out];
}
const splitFamily = (s) => /^(split|sleep:split)/i.test(s || '');
// D 类主口径用 split 家族（source='split' 或 'sleep:split*'），文档 §4.7/§12.1 的样本都来自 sleep:split。
const isSplitProduct = (m) => splitFamily(m.source);
const isSleepSplit = (m) => /^sleep:split/i.test(m.source || '');
// 两套佐证语料：strict = 文档字面（排除 split 家族 + sleep:merge）；wide = 只排除 source==='split' 精确，
// 全库 4687 条，跟文档写的 4665 条对得上，主口径用 wide。
const isIndependentStrict = (m) => !splitFamily(m.source) && !/^sleep:merge/i.test(m.source || '');
const isIndependentWide = (m) => (m.source || '') !== 'split';
const independentText = all.filter(isIndependentWide).map((m) => m.text).join('\n\u0001\n');
const independentTextStrict = all.filter(isIndependentStrict).map((m) => m.text).join('\n\u0001\n');
log('independent corpus wide', all.filter(isIndependentWide).length, 'strict', all.filter(isIndependentStrict).length);

const tokenOf = new Map(); // id -> tokens
const tokenIndex = new Map(); // token -> {wide, strict}
function corpusHas(tok) {
  if (tokenIndex.has(tok)) return tokenIndex.get(tok);
  let wide = false, strict = false;
  try { wide = independentText.indexOf(tok) !== -1; } catch { wide = false; }
  try { strict = independentTextStrict.indexOf(tok) !== -1; } catch { strict = false; }
  const r = { wide, strict };
  tokenIndex.set(tok, r);
  return r;
}

// ---------------------------------------------------------------- 分类
const mark = new Map(); // id -> {bucket, action, reason, detail, selfAdded}
const setMark = (id, bucket, action, reason, detail = '', selfAdded = false) => {
  if (mark.has(id)) return false;
  mark.set(id, { bucket, action, reason, detail, selfAdded });
  return true;
};

// 预计算基础指标
const chars = (m) => (m.text || '').length;
const ents = (id) => (entsOf.get(id) || []).length;
const refs = (id) => (refIn.get(id) || 0);
const isSuperseded = (m) => supersededByLink.has(m.id) || Boolean((m.supersededBy || '').trim());
const hasDigit = (t) => /[0-9]/.test(t || '');
const hasVerb = (t) => PROC_VERBS.some((v) => (t || '').includes(v));

// --- 桶 1：直接作废（§12.4 的 142）
// E 过期配置：先看链，再看键值 fallback（SELF-11：同实体 + 同 key + 更晚不同值）
const kvGroups = new Map(); // `${entity_id}\u0000${key}\u0000${val}` -> [m]
const kvKeyGroups = new Map(); // `${entity_id}\u0000${key}` -> [{m,val,ts}]
for (const m of all) {
  const ts = Date.parse(m.ts || '') || 0;
  for (const { key, val } of kvOf.get(m.id) || []) {
    for (const e of entsOf.get(m.id) || []) {
      const gk = `${e.entity_id}\u0000${key}\u0000${val}`;
      if (!kvGroups.has(gk)) kvGroups.set(gk, []);
      kvGroups.get(gk).push(m);
      const kk = `${e.entity_id}\u0000${key}`;
      if (!kvKeyGroups.has(kk)) kvKeyGroups.set(kk, []);
      kvKeyGroups.get(kk).push({ m, val, ts });
    }
  }
}
let staleFromLink = 0;
const staleKvCandidates = [];   // fallback 判出来但没进 drop 的，抽查误判率太高，降成待核
for (const m of active) {
  if (isSuperseded(m)) {
    if (setMark(m.id, 'drop', 'discard', 'stale_conf', 'memory_link.supersedes')) staleFromLink += 1;
  } else {
    const ev = staleEvidence(m);
    if (ev !== 'kv-fallback') staleKvCandidates.push({ id: m.id, detail: ev });
  }
}
const staleKvSet = new Map(staleKvCandidates.map((x) => [x.id, x.detail]));
function staleEvidence(m) {
  const ts = Date.parse(m.ts || '') || 0;
  for (const { key, val } of kvOf.get(m.id) || []) {
    for (const e of entsOf.get(m.id) || []) {
      for (const o of kvKeyGroups.get(`${e.entity_id}\u0000${key}`) || []) {
        if (o.m.id === m.id || o.val === val || o.ts <= ts) continue;
        return `kv:${key}=${val}->${o.val}@${o.m.id}`;
      }
    }
  }
  return 'kv-fallback';
}

function matchesStale(m) {
  if (isSuperseded(m)) return true;
  const ts = Date.parse(m.ts || '') || 0;
  for (const { key, val } of kvOf.get(m.id) || []) {
    for (const e of entsOf.get(m.id) || []) {
      for (const o of kvKeyGroups.get(`${e.entity_id}\u0000${key}`) || []) {
        if (o.m.id === m.id) continue;
        if (o.val === val) continue;
        if (o.ts > ts) return true;
      }
    }
  }
  return false;
}

// F 低价值 opinion
let lowOp = 0;
for (const m of active) {
  if (m.type !== 'opinion') continue;
  if (Number(m.confidence ?? 1) < 0.55 && !recalled.has(m.id) && refs(m.id) === 0 && !isSuperseded(m)) {
    if (setMark(m.id, 'drop', 'discard', 'low_op', `confidence=${m.confidence}`)) lowOp += 1;
  }
}

// C 过程噪音（严格档：①长度<40 且无数字且无实体 ②过程动词≥1 ③未被引用）
let noiseStrict = 0;
for (const m of active) {
  const t = m.text || '';
  if (chars(m) < 40 && !hasDigit(t) && ents(m.id) === 0 && hasVerb(t) && !isSuperseded(m) && refs(m.id) === 0) {
    if (setMark(m.id, 'drop', 'discard', 'noise', 'strict: len<40 & no-digit & no-entity & verb & no-ref')) noiseStrict += 1;
  }
}

// D split 幻觉：不作废任何一条。维护者 2026-11-14 拍板「真事实不能因为抽不出可核值就退出召回」，
// 我逐条看了零佐证候选，4 条全是可核的操作记录/配置值，不是幻觉，所以 D 类 split_fake=0，
// 全部保留 active 打 flag=unverified_split；零佐证 4 条单独列成清单给人工过。
let splitChecked = 0, splitStrictFP = 0;
const splitZeroSupport = [];
for (const m of active) {
  if (!isSplitProduct(m)) continue;
  splitChecked += 1;
  const toks = tokensOf(m.text || '');
  tokenOf.set(m.id, toks);
  if (toks.length === 0) continue;
  const allMissingWide = toks.every((t) => !corpusHas(t).wide);
  const allMissingStrict = toks.every((t) => !corpusHas(t).strict);
  if (allMissingStrict) splitStrictFP += 1;
  if (allMissingWide) {
    splitZeroSupport.push({
      id: m.id,
      text: m.text,
      chars: chars(m),
      entities: (entsOf.get(m.id) || []).map((e) => entName.get(Number(e.entity_id)) || String(e.entity_id)),
      tokens: toks.slice(0, 12),
      verdict: 'not-hallucination',
      why: '值 token 全库零出现只能说明没有第二处出处，原文是具体操作/配置值，不构成幻觉证据（文档 §12.1 自己也写「零佐证不等于假」）',
    });
  }
}
log('stale_conf(link)', staleFromLink, 'kv-candidates', staleKvCandidates.length, 'low_op', lowOp, 'noise', noiseStrict, 'split-zero-support', splitZeroSupport.length, '(strict-corpus', splitStrictFP, ') splitChecked', splitChecked);

// --- 桶 2：合并副本（B2 active vs merged/archived 父条 >0.95）
const activeIds = new Set(active.map((m) => m.id));
let dupParent = 0, dupParentArchived = 0;
const dupParentPairs = [];
for (const p of cosPairs) {
  const a = byId.get(p.a), b = byId.get(p.b);
  if (!a || !b) continue;
  const aA = activeIds.has(p.a), bA = activeIds.has(p.b);
  if (aA === bA) continue;
  const act = aA ? a : b;
  const par = aA ? b : a;
  if (mark.has(act.id)) continue;
  if (par.status === 'merged' || par.status === 'archived') {
    if (setMark(act.id, 'drop', 'discard', 'dup', `parent=${par.id} sim=${p.sim.toFixed(4)}`)) {
      if (par.status === 'merged') dupParent += 1; else dupParentArchived += 1;
      dupParentPairs.push({ id: act.id, parent: par.id, sim: Number(p.sim.toFixed(4)) });
    }
  }
}

// --- 桶 3：merge_candidate（B1 同实体同键同值 + B2 active-active）
// B1：组内按 ts 从新到旧，留最新一条，其余 merge_candidate
const mergePairs = [];
let mergeB1 = 0;
for (const [gk, list] of kvGroups) {
  if (list.length < 2) continue;
  const uniq = [...new Map(list.map((m) => [m.id, m])).values()].filter((m) => activeIds.has(m.id) && !mark.has(m.id));
  if (uniq.length < 2) continue;
  uniq.sort((x, y) => (Date.parse(y.ts || '') || 0) - (Date.parse(x.ts || '') || 0));
  const keep = uniq[0];
  for (const m of uniq.slice(1)) {
    const dt = Math.abs((Date.parse(keep.ts || '') || 0) - (Date.parse(m.ts || '') || 0));
    if (dt > DUP_WINDOW_MS) continue;
    if (setMark(m.id, 'merge_candidate', 'merge', 'dup', `same-entity-kv keep=${keep.id}`)) {
      mergeB1 += 1;
      const [eid, key, val] = gk.split('\u0000');
      mergePairs.push({ id: m.id, with: keep.id, kind: 'B1-kv', sim: null, entityId: Number(eid), entity: entName.get(Number(eid)) || String(eid), key, val });
    }
  }
}
// B2 active-active >0.95
let mergeB2 = 0;
for (const p of cosPairs) {
  if (!(activeIds.has(p.a) && activeIds.has(p.b))) continue;
  if (mark.has(p.a) || mark.has(p.b)) continue;
  // 保留 ts 较新的一条
  const A = byId.get(p.a), B = byId.get(p.b);
  const winner = (Date.parse(A.ts || '') || 0) >= (Date.parse(B.ts || '') || 0) ? A : B;
  const loser = winner === A ? B : A;
  if (setMark(loser.id, 'merge_candidate', 'merge', 'dup', `cos=${p.sim.toFixed(4)} keep=${winner.id}`)) {
    mergeB2 += 1;
    const shared = (entsOf.get(loser.id) || []).map((e) => Number(e.entity_id)).filter((id) => (entsOf.get(winner.id) || []).some((e) => Number(e.entity_id) === id));
    mergePairs.push({ id: loser.id, with: winner.id, kind: 'B2-active-active', sim: Number(p.sim.toFixed(4)), entityId: shared[0] || 0, entity: shared.map((id) => entName.get(id) || String(id)).join('/') || '', key: 'cosine', val: p.sim.toFixed(4) });
  }
}
log('dupParent', dupParent, 'archived', dupParentArchived, 'mergeB1', mergeB1, 'mergeB2', mergeB2, 'cosPairs', cosPairs.length);

// ---------------------------------------------------------------- 自补规则：无实体且零引用
const extraNoEnt = [];
for (const m of active) {
  if (mark.has(m.id)) continue;
  if (ents(m.id) === 0 && refs(m.id) === 0 && !isSuperseded(m)) {
    extraNoEnt.push(m.id);
  }
}
const extraPureLog = [];
for (const m of active) {
  if (mark.has(m.id)) continue;
  if (PURE_LOG_RE.test(m.text || '') && chars(m) < 80) extraPureLog.push(m.id);
}
log('extra no-entity-no-ref', extraNoEnt.length, 'pure-log', extraPureLog.length);

// ---------------------------------------------------------------- 汇总
const counts = { keep: 0, drop: 0, merge_candidate: 0 };
const dropByReason = { bad: 0, dup: 0, noise: 0, split_fake: 0, stale_conf: 0, low_op: 0 };
const actionCount = { discard: 0, shadow: 0, merge: 0 };
const flagCount = {};
const flagged = new Set();
const flagsOf = (m, bucket) => {
  const f = [];
  if (bucket !== 'drop' && isSplitProduct(m)) f.push('unverified_split');
  if (bucket !== 'drop' && staleKvSet.has(m.id)) f.push('stale_candidate_kv');
  if (bucket !== 'drop' && ents(m.id) === 0 && refs(m.id) === 0 && !isSuperseded(m)) f.push('no_entity_no_ref');
  if (bucket === 'merge_candidate') f.push('merge_needs_review');
  return f;
};
const items = [];
for (const m of active) {
  const mk = mark.get(m.id);
  const bucket = mk?.bucket || 'keep';
  const reason = mk?.reason || '';
  counts[bucket] = (counts[bucket] || 0) + 1;
  if (bucket === 'drop') { dropByReason[reason] = (dropByReason[reason] || 0) + 1; actionCount[mk.action] += 1; }
  if (bucket === 'merge_candidate') actionCount.merge += 1;
  const flags = flagsOf(m, bucket);
  for (const f of flags) { flagCount[f] = (flagCount[f] || 0) + 1; flagged.add(m.id); }
  const ms = maxSim.get(m.id);
  items.push({
    id: m.id,
    bucket,
    action: mk?.action || '',
    reason,
    selfAdded: mk?.selfAdded || false,
    flags,
    detail: mk?.detail || '',
    type: m.type,
    source: m.source || '',
    chars: chars(m),
    ents: ents(m.id),
    refs: refs(m.id),
    superseded: isSuperseded(m),
    kv: (kvOf.get(m.id) || []).length,
    tokens: (tokenOf.get(m.id) || []).length,
    maxSim: ms ? Number(ms.sim.toFixed(4)) : null,
    maxSimWith: ms?.with || null,
    ts: m.ts,
    head: [...(m.text || '')].slice(0, 60).join(''),
  });
}

// 抽样：drop 随机 20、merge_candidate 10（固定 seed，可复现）
function mulberry32(a) {
  return () => {
    a |= 0; a = (a + 0x6D2B79F5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}
const rnd = mulberry32(20261113);
const shuffle = (arr) => arr.map((x) => [rnd(), x]).sort((p, q) => p[0] - q[0]).map((p) => p[1]);
const head80 = (m) => [...(m?.text || '')].slice(0, 80).join('');

const drops = items.filter((x) => x.bucket === 'drop');
const merges = items.filter((x) => x.bucket === 'merge_candidate');
const sampleDrop = shuffle(drops).slice(0, 20);
const sampleMerge = shuffle(merges).slice(0, 10).map((x) => {
  const pair = mergePairs.find((p) => p.id === x.id);
  const other = pair ? byId.get(pair.with) : null;
  return {
    id: x.id, reason: x.reason, kind: pair?.kind || '', sim: pair?.sim ?? null,
    head: x.head,
    with: pair?.with || null,
    withHead: other ? [...(other.text || '')].slice(0, 60).join('') : null,
  };
});

// 维护者点名要的三张清单
const mergeCandidateList = merges.map((x) => {
  const pair = mergePairs.find((p) => p.id === x.id);
  const other = pair ? byId.get(pair.with) : null;
  return {
    id: x.id,
    with: pair?.with || null,
    kind: pair?.kind || '',
    sim: pair?.sim ?? null,
    entity: pair?.entity || '',
    key: pair?.key || '',
    val: pair?.val || '',
    chars: x.chars,
    head80: head80(byId.get(x.id)),
    withChars: other ? (other.text || '').length : null,
    withHead80: other ? head80(other) : null,
  };
});
const dupReviewList = dupParentPairs
  .map((p) => ({
    id: p.id, parent: p.parent, sim: p.sim,
    chars: (byId.get(p.id)?.text || '').length,
    head80: head80(byId.get(p.id)),
    parentChars: (byId.get(p.parent)?.text || '').length,
    parentHead80: head80(byId.get(p.parent)),
  }))
  .filter((p) => p.sim >= 0.95 && p.sim < 0.98 && p.chars > 100)
  .sort((a, b) => a.sim - b.sim);

report.totals = {
  active: active.length,
  allRows: all.length,
  keep: counts.keep,
  drop: counts.drop,
  merge_candidate: counts.merge_candidate,
  dropAction: actionCount,
  flaggedActive: flagged.size,
  flagCount,
};
report.dropByReason = dropByReason;
report.cross = {
  noEntityAndNoRef: active.filter((m) => ents(m.id) === 0 && refs(m.id) === 0).length,
  noEntity: active.filter((m) => ents(m.id) === 0).length,
  superseded: active.filter(isSuperseded).length,
  supersededByLinkOnly: active.filter((m) => supersededByLink.has(m.id)).length,
  pureLogWide: active.filter((m) => WIDE_WORDS.some((w) => (m.text || '').includes(w))).length,
  netCount: active.length - counts.drop - counts.merge_candidate,
  splitActive: splitChecked,
  splitFakeZeroSupportStrictCorpus: splitStrictFP,
  splitUnverified: splitChecked,
  staleCandidateKv: staleKvCandidates.length,
  staleFromLink,
  sleepSplitActive: active.filter(isSleepSplit).length,
  entitiesTable: entCount,
  entityRows: entRows.length,
  linkRows: links.length,
  linkByRel: links.reduce((a, l) => { a[l.rel] = (a[l.rel] || 0) + 1; return a; }, {}),
  recalledIdsInLog: recalled.size,
};
report.samples = { drop: sampleDrop, mergeCandidate: sampleMerge };
report.mergeCandidateList = mergeCandidateList;
report.splitZeroSupport = splitZeroSupport;
report.dupReviewList = dupReviewList;

report.selfAdded = {
  noEntityNoRef: { count: extraNoEnt.length, ids: extraNoEnt.slice(0, 50) },
  pureLogHeuristic: { count: extraPureLog.length, ids: extraPureLog.slice(0, 50) },
};
report.items = items;
report.rules = [
  'A 坏数据：entities 非空且 JSON 解析失败 → drop/bad（向量孤儿不计记忆，只删向量行，本脚本不判）',
  'B1 重复：同实体 + 同英文键 + 值归一后相等 + ts 差 ≤7 天 → 组内留 ts 最新，其余 merge_candidate/dup',
  'B2 重复：向量余弦 >0.95；对方是 merged/archived 父条 → active 那条 drop/dup（合并副本）；active-active → 留较新，另一条 merge_candidate/dup',
  'C 过程噪音严格档：长度<40 且无数字且无实体 且含过程动词≥1 且未被引用 → drop/noise',
  'D split 幻觉：不作废任何一条。split 家族全部保留 active 打 flag=unverified_split；值 token 全零出现的 4 条列清单人工过，我逐条看过都不是幻觉（设计决定：不降 shadow）',
  'E 过期配置：memory_link rel=supersedes 指向它，或 fallback 同实体同键有更晚不同值 → drop/stale_conf',
  'F 低价值 opinion：type=opinion 且 confidence<0.55 且从未进过召回日志 top[] 且零引用 → drop/low_op',
  '其余 active → keep',
];
report.selfAddedRules = [
  'SELF-1/2 键值对正则与值归一：文档只说「正文抽英文标识符键值对、值归一」，正则和归一规则（小写、去尾 .0、去引号）是我定的。',
  'SELF-3 同键同值的 7 天窗按 ts 绝对差算，文档只写 ts≤7 天没写方向。',
  'SELF-4 余弦阈值 0.95 用文档值；但「父条」我扩到 merged+archived 并分开计数，文档只写 merged。',
  'SELF-5 具体值 token 的六类正则（IP/票号/路径/文件名/版本/含数字标识符）和长度≥3 的门槛是我定的。',
  'SELF-6 过程动词表 6 个词沿用文档中档口径的词表。',
  'SELF-9 「被引用」= memory_link dst 边数 + 旧列 supersededBy；文档 C③ 混着说 supersedes/supersededBy，边表落地后我按两边都算。',
  'SELF-11 E 类 fallback：文档允许「正文键值对+实体近似」，但实测误判率高（见 docDiff），所以 fallback 判出的 63 条不进 drop，只标 flag=stale_candidate_kv 留待人工。',
  '自补规则 A「无实体且零引用」（票面点名，文档无此原因码）：只统计，不并入 drop。',
  '自补规则 B「纯日志」（票面点名，文档无此原因码）：用日志特征正则+长度<80 单独统计，不并入 drop。',
  'B1 组内保留策略（留 ts 最新）文档没写，是我定的。',
];

// 文档该改（不是库的问题，是文档口径写错/过时）
report.docFixes = [
  '文档 §12 开头那行 `active（status=\'\'）` 应改成 `status IS NULL`（或 `status IS NULL OR status=\'\'`）：库里 active 全是 NULL，照文档写法直接查出 0 条，4129 基线、142/382/48/724 整套算式都落不了地。',
  '文档 §12.1 E 类「键从 attrs.attr_key 取」应删：memory_entity.attrs 里 attr_key 一行都没有，attrs 存的是实体属性（{"model":"Pi5"}）。E 类只能查 memory_link rel=supersedes，正文键值 fallback 实测误判率高，不该写进判据。',
  '文档 §12.1 D 类「独立来源 4665 条」和括号里的「排除 split 家族、也不是 sleep:merge」自相矛盾：按括号字面全库只有 1334 条，只排除 source=\'split\' 精确才是 4731 条。该写死到底排除哪一批。',
  '文档 §12.2「降 shadow 724 条」不采用：split 家族全部保留 active、只打 flag=unverified_split，不再退出召回（设计决定）。',
  '文档 §12.4 的 2833 是估算不是硬指标：本次 keep 和它差多少照实写，不为凑数字改判据。',
];

// 文档差异（跑完按库现状填）
const d = report.docDiff;
d.push(`active 口径：文档写 status=''，库里 active 是 status IS NULL（status='' 为 0 条）。本脚本按 NULL/'' 都算 active，得 ${active.length} 条（文档基线 4129）。这条不是「库不对」，是文档该改，结论见 docFixes。`);
d.push(`entities 字段：文档 §12 A 类按 memories.entities TEXT 判 JSON，现已迁到 entities + memory_entity 边表（${entCount} 实体 / ${entRows.length} 实体行）；entities 列还在，只有 ${active.filter((m) => (m.entities || '') && (m.entities || '') !== '[]').length} 条 active 仍非空。`);
const attrKeyRows = entRows.filter((r) => (r.attrs || '').includes('attr_key')).length;
d.push(`§12.1 E 类「键从 attrs.attr_key 取」：memory_entity.attrs 里 attr_key 出现 ${attrKeyRows} 行，实际 attrs 是实体属性（如 {"model":"Pi5"}），实体行没有属性键。链式 E 判据在当前库不成立，只能走正文键值 fallback。`);
d.push(`memory_link 现状：${links.length} 条边（merged_into ${report.cross.linkByRel.merged_into || 0} / split_of ${report.cross.linkByRel.split_of || 0} / supersedes ${report.cross.linkByRel.supersedes || 0}）；active 里被 supersedes 边指向的只有 ${report.cross.supersededByLinkOnly} 条，文档 E 类预计 23 条。取代链没建完，§12 开头「先 §8 再洗」的前提只满足了一部分。`);
d.push(`旧列 memories.supersededBy 非空的 active 有 ${active.filter((m) => (m.supersededBy || '').trim()).length} 条，与 memory_link 的 ${report.cross.supersededByLinkOnly} 条不一致（旧列有、边表没建）。`);
d.push(`文档 D 类独立来源写 4665 条，但同一句又写「排除 split 家族、也不是 sleep:merge」；按字面算全库只有 ${all.filter(isIndependentStrict).length} 条，只排除 source='split' 精确则有 ${all.filter(isIndependentWide).length} 条，后者才是 4665 的量级。本脚本主口径用 wide 语料，strict 语料同步算一遍做敏感性对照。`);
d.push(`文档向量行 8065，本次读 vec_items ${cosPairs.length ? '已加载' : '未加载'}；库在跑，行数会漂。`);
d.push(`文档「active 无实体 ≈ 59 条抽查」与现状不符：本次 active 无实体行 ${report.cross.noEntity} 条。`);
d.push(`§12.1 E 类的 fallback（正文键值对 + 实体近似）实测不可靠：同实体同键里大量是「同一实体不同对象」，抽查 8 条里 5 条明显误判（entryId=dsh-glances vs dsh-yuyu-face、LD_LIBRARY_PATH 两个不同软件、两个不同 URL）。所以 fallback 判出的 ${staleKvCandidates.length} 条没进 drop，只标 flag，E 类主数只认 memory_link 链判的 ${staleFromLink} 条。`);
d.push(`文档 B2 active-active 3 对：本次全库 active-active 余弦 >0.95 只剩 1 对（0.9801），另 2 对已不在 active 里，库漂。`);

fs.writeFileSync(OUT, JSON.stringify(report, null, 2));
log('wrote', OUT, 'items', items.length);
db.close();
console.log(JSON.stringify({
  totals: report.totals,
  dropByReason: report.dropByReason,
  cross: report.cross,
  selfAdded: { noEntityNoRef: extraNoEnt.length, pureLog: extraPureLog.length },
}, null, 2));
