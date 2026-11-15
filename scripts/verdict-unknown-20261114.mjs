#!/usr/bin/env node
/**
 * 票：给 unknown 实体判类型（dry-run，只出 JSON，不动库）。
 *
 * 输入：memory.db 里 status='active' 且 etype='unknown' 的实体（archived 跳过），
 *       每条反查 memory_entity 挂着的记忆，取最多 3 条（active 优先、字数多优先），正文前 200 字。
 * 输出：$DSH_MEMORY_ROOT/unknown-verdict-20261114.json（缺省 cwd/memory）
 *       [{entity_id,name,etype,conf,reason}]，etype 只取 14 类之一。
 * 分批：默认 40 条一批，走本机 magpie 网关 group/ds，温度 0；缺项重试一次，仍缺落 unknown/low。
 *
 * DB 只读打开（node:sqlite readOnly），本脚本没有任何 INSERT/UPDATE。
 *
 * 用法：node scripts/verdict-unknown-20261114.mjs [--batch-size 40] [--concurrency 3]
 *        [--limit N] [--offset N] [--model group/ds] [--out <file>]
 */
import { openDb, resolveDbPath, resolveMemoryFile } from './lib/db.mjs';

const argv = process.argv.slice(2);
const argOf = (k, d) => (argv.includes(k) ? argv[argv.indexOf(k) + 1] : d);
const DB = argOf('--db', resolveDbPath());
const OUT = argOf('--out', resolveMemoryFile('unknown-verdict-20261114.json'));
const BATCHLOG = argOf('--batch-log', '/tmp/unknown-verdict-20261114.batches.jsonl');
const GATEWAY = argOf('--gateway', 'http://127.0.0.1:3425/v1');
const MODEL = argOf('--model', 'group/ds');
const BATCH = Number(argOf('--batch-size', '40'));
const CONC = Number(argOf('--concurrency', '3'));
const LIMIT = Number(argOf('--limit', '0')) || Infinity;
const OFFSET = Number(argOf('--offset', '0'));
const CTX_PER = 3;
const CTX_CHARS = 200;

const ETYPES = new Set([
  'file', 'tool', 'config', 'service', 'concept', 'device', 'ticket',
  'repo', 'org', 'person', 'host', 'project', 'account', 'unknown',
]);
const CONFS = new Set(['high', 'medium', 'low']);

const SYSTEM = `你是记忆实体分类员，给记忆库里实体名判类型。
etype 只能取这 14 个之一，一个字母都不许多：file, tool, config, service, concept, device, ticket, repo, org, person, host, project, account, unknown。
不许自造类型（hardware_component、scheduled_task 这类一律不合法）。

判据：
- file：具体路径或文件名，如 /srv/app/x.sh、config.yml。
- tool：可执行程序、命令、脚本、库，如 ffmpeg、jq、node:sqlite、BGE-M3 这类模型/库也算。
- config：配置项、键、配置文件里的条目，如 maxTokens、entityQueue.reasoningEffort。
- service：跑着的服务、systemd 单元、API 服务；定时任务（如 bili-notify-scan）也算 service。
- host：IP、域名、主机名。
- device：硬件实物，如树莓派、小屏、UPS、传感器。
- repo：单个代码仓。
- project：跨多仓、多机器的长期工程线。
- concept：抽象概念、型号名、机制、术语，如 FiberState 枚举、高峰时段。
- org：公司、平台、组织，如 DeepSeek、B站。
- person：真实的人或角色（人设、NPC 也算）。
- account：B站/GitHub/邮箱这类平台账号；名字与真人同形的仍算 person。
- ticket：票号，如 t-mwf799w5-obuz。
- unknown：判不出，或压根不是对象（句子碎片、动词、泛词）。

conf：high = 上下文直接写明它是什么；medium = 上下文能推出来但没直说；low = 只能靠名字猜。
reason：一句话，不超过 30 个汉字，必须点出上下文里的依据（引原句片段）；没有上下文就写「无上下文，按名字判」。
拿不准就判 unknown，不许硬塞。unknown 是合法结论。

只输出严格 JSON，不要解释、不要围栏：
{"items":[{"i":1,"etype":"file","conf":"high","reason":"上下文写它存放在 /srv/app/x.sh"}]}`;

function gatewayKey() {
  const key = String(process.env.MAGPIE_GATEWAY_KEY || '').trim();
  if (!key) throw new Error('缺少 MAGPIE_GATEWAY_KEY：请在环境变量里配置，脚本不再读取用户家目录下的凭据文件');
  return key;
}
const KEY = gatewayKey();

const db = openDb(DB, { readOnly: true });
const memCtx = db.prepare(`SELECT m.text, m.status, m.ts
  FROM memory_entity me JOIN memories m ON m.id = me.memory_id
  WHERE me.entity_id = ?
  ORDER BY (coalesce(m.status,'')='') DESC, length(m.text) DESC
  LIMIT ${CTX_PER}`);

const all = db.prepare(`SELECT id, display, mem_count FROM entities
  WHERE status = 'active' AND etype = 'unknown' ORDER BY id`).all()
  .filter((r, i) => i >= OFFSET)
  .slice(0, LIMIT === Infinity ? undefined : LIMIT);

const items = all.map((r, idx) => ({
  i: idx + 1,
  id: Number(r.id),
  name: String(r.display),
  mem_count: Number(r.mem_count) || 0,
  ctx: memCtx.all(Number(r.id)).map((m) => String(m.text || '').replace(/\s+/g, ' ').trim().slice(0, CTX_CHARS)),
}));

function parseJson(raw) {
  const s = String(raw ?? '').trim();
  if (!s) return null;
  const cands = [s];
  const fenced = s.match(/```(?:json)?\s*([\s\S]*?)```/i);
  if (fenced) cands.push(fenced[1].trim());
  const a = s.indexOf('{');
  const b = s.lastIndexOf('}');
  if (a >= 0 && b > a) cands.push(s.slice(a, b + 1));
  for (const c of cands) {
    try {
      const v = JSON.parse(c);
      if (v && Array.isArray(v.items)) return v;
    } catch { /* 下一个 */ }
  }
  return null;
}

function cleanVerdict(v) {
  if (!v || typeof v !== 'object') return null;
  const etype = String(v.etype ?? '').trim().toLowerCase();
  const conf = String(v.conf ?? '').trim().toLowerCase();
  let reason = String(v.reason ?? '').replace(/\s+/g, ' ').trim();
  if (!ETYPES.has(etype) || !CONFS.has(conf)) return null;
  if (reason.length > 120) reason = reason.slice(0, 120);
  if (!reason) reason = '模型未给依据';
  if (etype === 'unknown' && conf === 'high') return { etype, conf: 'medium', reason };
  return { etype, conf, reason };
}

let calls = 0;
let netFails = 0;
let retries = 0;

async function callModel(batch, attempt) {
  const payload = {
    items: batch.map((it) => ({ i: it.i, name: it.name, mem_count: it.mem_count, ctx: it.ctx })),
  };
  const t0 = Date.now();
  calls++;
  const res = await fetch(`${GATEWAY}/chat/completions`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${KEY}` },
    body: JSON.stringify({
      model: MODEL,
      temperature: 0,
      messages: [
        { role: 'system', content: SYSTEM },
        { role: 'user', content: JSON.stringify(payload) },
      ],
    }),
    signal: AbortSignal.timeout(300000),
  });
  const ms = Date.now() - t0;
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`HTTP ${res.status} ${text.slice(0, 200)} (batch ${batch[0]?.i}, attempt ${attempt})`);
  }
  const data = await res.json();
  const content = data?.choices?.[0]?.message?.content ?? '';
  const parsed = parseJson(content);
  const got = new Map();
  for (const v of parsed?.items ?? []) {
    const i = Number(v?.i);
    const cv = cleanVerdict(v);
    if (Number.isInteger(i) && cv) got.set(i, cv);
  }
  return { got, ms, usage: data?.usage ?? null, raw: content };
}

async function judgeBatch(batch, idx) {
  const t0 = Date.now();
  let got = new Map();
  let rawSnippets = [];
  let err = null;
  try {
    const r1 = await callModel(batch, 1);
    got = r1.got;
    rawSnippets.push(r1.raw);
  } catch (e) {
    err = String(e.message || e);
    netFails++;
  }
  let missing = batch.filter((it) => !got.has(it.i));
  if (missing.length) {
    retries++;
    await new Promise((r) => setTimeout(r, 2000));
    try {
      const r2 = await callModel(missing, 2);
      for (const [k, v] of r2.got) got.set(k, v);
      rawSnippets.push(r2.raw);
    } catch (e) {
      netFails++;
      err = (err ? err + ' | ' : '') + String(e.message || e);
    }
  }
  missing = batch.filter((it) => !got.has(it.i));
  const out = batch.map((it) => {
    const v = got.get(it.i);
    return {
      entity_id: it.id,
      name: it.name,
      etype: v ? v.etype : 'unknown',
      conf: v ? v.conf : 'low',
      reason: v ? v.reason : '模型重试后仍未返回有效判定',
      _ok: Boolean(v),
    };
  });
  const ms = Date.now() - t0;
  fs.appendFileSync(BATCHLOG, JSON.stringify({
    batch: idx, size: batch.length, missing: missing.length, ms, err,
    ids: batch.map((b) => b.id), items: out,
  }) + '\n');
  console.log(`batch ${idx}: n=${batch.length} ok=${batch.length - missing.length} miss=${missing.length} ${ms}ms${err ? ' err=' + err.slice(0, 120) : ''}`);
  return out;
}

async function pool(tasks, n) {
  const results = new Array(tasks.length);
  let next = 0;
  const workers = Array.from({ length: Math.min(n, tasks.length) }, async () => {
    for (;;) {
      const k = next++;
      if (k >= tasks.length) return;
      results[k] = await tasks[k]();
    }
  });
  await Promise.all(workers);
  return results;
}

const started = Date.now();
fs.writeFileSync(BATCHLOG, '');
const batches = [];
for (let k = 0; k < items.length; k += BATCH) batches.push(items.slice(k, k + BATCH));

const parts = await pool(
  batches.map((b, idx) => () => judgeBatch(b, idx)),
  CONC,
);

const rows = parts.flat();
const durMs = Date.now() - started;

const conf = { high: 0, medium: 0, low: 0 };
const byType = {};
let failed = 0;
for (const r of rows) {
  conf[r.conf] = (conf[r.conf] || 0) + 1;
  byType[r.etype] = (byType[r.etype] || 0) + 1;
  if (!r._ok) failed++;
}
const outRows = rows.map(({ _ok, ...rest }) => rest);
fs.writeFileSync(OUT, JSON.stringify(outRows, null, 1) + '\n');

const summary = {
  out: OUT,
  total: outRows.length,
  conf,
  etype: Object.fromEntries(Object.entries(byType).sort((a, b) => b[1] - a[1])),
  unknown_left: byType.unknown || 0,
  unknown_left_with_high_medium: rows.filter((r) => r.etype === 'unknown' && (r.conf === 'medium' || r.conf === 'high')).length,
  will_become: outRows.length - (byType.unknown || 0),
  high_medium_rows: rows.filter((r) => r.etype !== 'unknown' && (r.conf === 'high' || r.conf === 'medium')).length,
  model_calls: calls,
  retry_rounds: retries,
  net_fails: netFails,
  failed_items: failed,
  batches: batches.length,
  batch_ms_avg: Math.round(parts.length ? durMs / parts.length : 0),
  wall_ms: durMs,
  wall_s: Math.round(durMs / 100) / 10,
};
console.log(JSON.stringify(summary, null, 2));
