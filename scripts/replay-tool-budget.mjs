#!/usr/bin/env node
/**
 * 工具上下文预算的估算 / 回放（2026-11-23）。
 *
 * 两个模式：
 *
 * ① 离线估算（默认，不调模型）：
 *    node scripts/replay-tool-budget.mjs --estimate --since 2026-11-23T04:20:00Z --until 2026-11-23T05:20:00Z
 *    读 memory_sleep.log，按 `[ingest] file=` 边界把 `[usage] site=sleep:askModel` 分行分组，
 *    把每组第 1 条请求的 in 当「300 条候选 listing」，后面每条请求的 in 当「新一截工具结果」，
 *    累计超过 --cap（默认 4000）的部分算「少送 token」。
 *
 * ② 真回放（A/B，调真模型，必须用副本库）：
 *    node scripts/replay-tool-budget.mjs --replay --sessions <目录> --db <副本/memory.db> \
 *      --limit 3 --since 2026-11-23T04:30:00Z
 *    同一批会话文件、同一份候选，跑两次 askModel：一次预算/定向查证全关（baseline），
 *    一次按默认配置开。逐文件比 未缓存输入 / 缓存读取 / 输出，以及解析出的
 *    add/update/merge/archive 动作集合（update 连正文比）。脚本只读副本库，不写任何记忆。
 *
 * 注意：真回放每轮温度固定 0.3，但模型本身仍有非确定性；结论差异要按「同一文件两次」
 * 配对看，不能跨文件比。缓存命中与实际费用必须上线后按 [usage] 行另算。
 */
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createStore } from '../lib/store.js';
import { createSleepEnv } from '../lib/sleep/env.js';
import { createModelCalls } from '../lib/sleep/model-calls.js';
import { readSessionEvents, eventsToTranscript, parseActions } from '../lib/transcript.js';
import { resolveDbPath, resolveLogFile, resolveSessionsRoot, DEFAULT_DB_PATH, openDb } from './lib/db.mjs';
import { createRehearseLlm } from './rehearse-pipeline.mjs';

const CAP_DEFAULT = 4000;
const DEFAULT_BASE_URL = process.env.DSH_REHEARSE_BASE_URL || 'http://127.0.0.1:3425/v1';
const DEFAULT_MODEL = process.env.DSH_REHEARSE_MODEL || 'group/ds';

function usage() {
  return `用法：
  node scripts/replay-tool-budget.mjs --estimate [--log <日志>] [--since <ISO>] [--until <ISO>] [--cap 4000] [--json]
  node scripts/replay-tool-budget.mjs --replay --sessions <目录> --db <副本/memory.db> [--limit 3] [--since <ISO>] [--out <json>] [--json]

  --estimate  离线估算（默认；不调模型）
  --replay    真回放 A/B（调真模型；--db 必须是副本库）
  --log       日志路径，默认 $DSH_MEMORY_LOG_DIR/memory_sleep.log
  --sessions  会话目录，默认 $DSH_SESSIONS_DIR 或 ~/.dsh/sessions
  --db        副本库（文件名一般就是 memory.db）
  --limit     回放几个文件（默认 3，按 mtime 从近到远）
  --since     只统计/回放这个时刻之后（ISO，日志是 UTC）
  --until     只统计到这个时刻之前（ISO，估算模式）
  --cap       累计工具片段上限（token，默认 4000）
  --out       回放结果写成 JSON 的路径
  --json      只输出机读 JSON

环境变量：DSH_REHEARSE_BASE_URL / DSH_REHEARSE_MODEL / DSH_REHEARSE_API_KEY（真回放用）。`;
}

function parseArgs(argv) {
  const out = {
    estimate: false, replay: false, log: '', sessions: '', db: '', limit: 3,
    since: '', until: '', cap: CAP_DEFAULT, out: '', json: false, help: false,
  };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    const take = () => {
      const v = argv[i + 1];
      if (v === undefined) throw new Error(`${arg} 缺值`);
      i += 1;
      return v;
    };
    if (arg === '--estimate') out.estimate = true;
    else if (arg === '--replay') out.replay = true;
    else if (arg === '--log') out.log = path.resolve(take());
    else if (arg.startsWith('--log=')) out.log = path.resolve(arg.slice(6));
    else if (arg === '--sessions') out.sessions = path.resolve(take());
    else if (arg.startsWith('--sessions=')) out.sessions = path.resolve(arg.slice(11));
    else if (arg === '--db') out.db = path.resolve(take());
    else if (arg.startsWith('--db=')) out.db = path.resolve(arg.slice(5));
    else if (arg === '--limit') out.limit = Math.max(1, Number(take()) || 3);
    else if (arg.startsWith('--limit=')) out.limit = Math.max(1, Number(arg.slice(8)) || 3);
    else if (arg === '--since') out.since = take();
    else if (arg.startsWith('--since=')) out.since = arg.slice(8);
    else if (arg === '--until') out.until = take();
    else if (arg.startsWith('--until=')) out.until = arg.slice(8);
    else if (arg === '--cap') out.cap = Math.max(0, Number(take()) || 0);
    else if (arg.startsWith('--cap=')) out.cap = Math.max(0, Number(arg.slice(6)) || 0);
    else if (arg === '--out') out.out = path.resolve(take());
    else if (arg.startsWith('--out=')) out.out = path.resolve(arg.slice(6));
    else if (arg === '--json') out.json = true;
    else if (arg === '--help' || arg === '-h') out.help = true;
    else throw new Error(`未知参数：${arg}`);
  }
  return out;
}

/** 一行日志的 ISO 时间（取行首 [..]），取不到返回 ''。 */
function tsOf(line) {
  const m = line.match(/^\[([^\]]+)\]/);
  return m ? m[1] : '';
}

const USAGE_RE = /^\[[^\]]+\] \[usage\] site=sleep:askModel .*\bin=(\S+) out=(\S+) cacheRead=(\S+)/;
const FILE_RE = /^\[[^\]]+\] \[ingest\] file=(\S+) (?:ok|FAIL)/;

function numOf(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
}

/**
 * 离线估算（只用日志，能算的算，算不了的标明）：
 *   - 请求数 / 未缓存输入 / 缓存读取 / 输出：日志里的真值；
 *   - 文件数：[ingest] file= 收口行数；
 *   - 「每条请求 4000 上限」的上界：cacheRead>0 的请求是同一文件的后续往返，
 *     其 in 至少包含「这一轮新送进去的工具结果」。若按每条请求把历史工具片段压到
 *     cap 以内，单条最多能省 max(0, in - cap)。加总得到**上界**；
 *   - 下界 0（工具结果本来就小）；票面给的 187 次配对回算 ≈ 50 万是中间值。
 * 并发 ingest 下日志里没有「这条请求属于哪个文件」的字段，所以这里不假装能精确配对。
 */
function estimateFromLog({ logPath, since, until, cap }) {
  const text = fs.readFileSync(logPath, 'utf8');
  let requests = 0;
  let files = 0;
  let input = 0;
  let output = 0;
  let cacheRead = 0;
  let followUps = 0;
  let upperBoundSaved = 0;
  for (const line of text.split('\n')) {
    if (!line) continue;
    const ts = tsOf(line);
    if (!ts) continue;
    if (since && ts < since) continue;
    if (until && ts > until) continue;
    const usageHit = line.match(USAGE_RE);
    if (usageHit) {
      const inTok = numOf(usageHit[1]);
      const outTok = numOf(usageHit[2]);
      const cached = numOf(usageHit[3]);
      requests += 1;
      input += inTok;
      output += outTok;
      cacheRead += cached;
      if (cached > 0) {
        followUps += 1;
        upperBoundSaved += Math.max(0, inTok - cap);
      }
      continue;
    }
    if (FILE_RE.test(line)) files += 1;
  }
  return {
    mode: 'estimate',
    log: logPath,
    window: { since: since || '', until: until || '' },
    cap,
    requests,
    files,
    followUps,
    input,
    output,
    cacheRead,
    upperBoundSaved,
    assumptions: [
      '请求数 / 未缓存输入 / 缓存读取 / 输出是日志 [usage] 行的真值。',
      'files 只数 [ingest] file= 收口行；并发 ingest 下组内文件会混，仅作规模参考。',
      '上界 = Σ max(0, 后续往返的 in − cap)：假设日志里每条后续请求的未缓存 in 就是可被压掉的历史工具片段。',
      '下界 0；票面给的 187 次配对回算（≈50 万）在两者之间。真实值需按路由记录配对逐文件算。',
      '压缩后历史工具片段不再重发，缓存前缀断点会前移；cacheRead 与费用不在这里扣，上线后按 [usage] 行另测。',
    ],
  };
}

/* ── 真回放：同一批会话文件、同一份候选，跑两次 askModel ───────────────── */

function pickSessionFiles(root, limit) {
  const out = [];
  const walk = (dir, depth) => {
    if (depth < 0 || out.length > 4000) return;
    let ents = [];
    try { ents = fs.readdirSync(dir, { withFileTypes: true }); } catch { return; }
    for (const e of ents) {
      const full = path.join(dir, e.name);
      if (e.isDirectory()) walk(full, depth - 1);
      else if (/^session.*\.jsonl(\.zst|\.zstd)?$/.test(e.name)) out.push(full);
    }
  };
  walk(root, 6);
  return out
    .map((file) => ({ file, mtime: fs.statSync(file).mtimeMs }))
    .sort((a, b) => b.mtime - a.mtime)
    .slice(0, limit)
    .map((x) => x.file);
}

/** 把一次解析结果归一成可比的动作集合。 */
function actionSignature(raw) {
  const a = parseActions(raw) || { add: [], update: [], merge: [], archive: [] };
  const norm = (v) => String(v ?? '').replace(/\s+/g, '');
  return {
    add: (a.add || []).map((x) => norm(x.text)).sort(),
    update: (a.update || []).map((x) => `${norm(x.id)}::${norm(x.text)}`).sort(),
    merge: (a.merge || []).map((x) => `${(x.ids || []).map(String).sort().join('+')}::${norm(x.text)}`).sort(),
    archive: (a.archive || []).map((x) => norm(x.id)).sort(),
  };
}

function sameSignature(a, b) {
  return JSON.stringify(a) === JSON.stringify(b);
}

function countOf(sig) {
  return sig.add.length + sig.update.length + sig.merge.length + sig.archive.length;
}

async function replayArm({ armName, files, cfg, ctxBase, budgetCfg, checkCfg }) {
  const llm = createRehearseLlm({
    baseUrl: process.env.DSH_REHEARSE_BASE_URL || DEFAULT_BASE_URL,
    model: process.env.DSH_REHEARSE_MODEL || DEFAULT_MODEL,
    apiKey: process.env.DSH_REHEARSE_API_KEY || '',
  });
  const ctx = { ...ctxBase, get: (name) => (name === 'llm' ? llm : undefined) };
  const envCore = createSleepEnv(ctx, ctx.store, cfg);
  const model = createModelCalls({
    ctx,
    store: ctx.store,
    cfg: { ...cfg, ...budgetCfg, ...checkCfg },
    log: () => {},
    lint: {},
  });
  const rows = [];
  for (const item of files) {
    const before = { calls: llm.usage.calls, input: llm.usage.promptTokens, output: llm.usage.completionTokens, cacheRead: llm.usage.cacheReadTokens };
    let raw = '';
    let error = '';
    try {
      raw = await model.askModel(item.transcript, item.existing, cfg.reasoningEffort);
    } catch (e) {
      error = String(e?.message || e);
    }
    rows.push({
      file: path.basename(item.file),
      arm: armName,
      calls: llm.usage.calls - before.calls,
      input: llm.usage.promptTokens - before.input,
      output: llm.usage.completionTokens - before.output,
      cacheRead: llm.usage.cacheReadTokens - before.cacheRead,
      error,
      signature: actionSignature(raw),
    });
  }
  return rows;
}

async function replay({ sessions, db, limit, since }) {
  if (!sessions) throw new Error('--replay 必须给 --sessions');
  if (!db) throw new Error('--replay 必须给 --db（副本库）');
  if (path.resolve(db) === path.resolve(DEFAULT_DB_PATH)) {
    throw new Error(`--db 指到真库了：${db}；回放必须用副本（用 cp 或 VACUUM INTO 拷一份）`);
  }
  if (!fs.existsSync(db)) throw new Error(`副本库不存在：${db}`);
  const files = pickSessionFiles(sessions, limit);
  if (!files.length) throw new Error(`--sessions 下没找到 session*.jsonl(.zst/.zstd)：${sessions}`);

  const runDir = path.join(path.dirname(db), '.replay-tool-budget');
  fs.mkdirSync(runDir, { recursive: true });
  const cfg = {
    storePath: path.dirname(db),
    sessionsRoot: sessions,
    stateFile: path.join(runDir, 'sleep.state.json'),
    backupDir: path.join(runDir, 'backup'),
    archiveDir: path.join(runDir, 'archive'),
    sleepLogFile: path.join(runDir, 'memory_sleep.log'),
    storeLogFile: path.join(runDir, 'memory_store.log'),
    notifyScript: '/bin/true',
    notify: false,
    sleepEnabled: false,
    sleepTools: true,
    provider: 'magpie-replay',
    model: process.env.DSH_REHEARSE_MODEL || DEFAULT_MODEL,
    reasoningEffort: 'low',
    maxMessageChars: 1500,
    maxTranscriptChars: 120000,
    logger: { info() {}, warn() {}, error() {} },
  };
  const store = createStore(cfg);
  const sinceMs = since ? Date.parse(since) : 0;
  const picked = [];
  for (const file of files) {
    const events = readSessionEvents(file);
    if (!events) continue;
    const { lines, messages } = eventsToTranscript(cfg, events, Number.isFinite(sinceMs) ? sinceMs : 0);
    const transcript = lines.join('\n');
    if (!messages) continue;
    const envCore = createSleepEnv({ ...cfg, store, get: () => undefined, logger: cfg.logger }, store, cfg);
    const existing = envCore.memoryTexts(transcript);
    picked.push({ file, transcript, existing });
  }
  if (!picked.length) throw new Error('选中的会话文件都没有可回放的消息');

  const ctxBase = { store, logger: cfg.logger, provide() {}, effect() {}, tools: { get: () => null, register() {} } };
  const baseline = await replayArm({
    armName: 'baseline', files: picked, cfg, ctxBase,
    budgetCfg: { sleepToolBudget: { enabled: false }, sleepToolCheck: { enabled: false } },
    checkCfg: {},
  });
  const budgeted = await replayArm({
    armName: 'budget', files: picked, cfg, ctxBase,
    budgetCfg: {}, // 默认配置：预算开、定向查证开
    checkCfg: {},
  });

  const rows = picked.map((item, index) => {
    const b = baseline[index];
    const n = budgeted[index];
    return {
      file: b.file,
      baseline: { calls: b.calls, input: b.input, output: b.output, cacheRead: b.cacheRead, actions: countOf(b.signature), signature: b.signature },
      budget: { calls: n.calls, input: n.input, output: n.output, cacheRead: n.cacheRead, actions: countOf(n.signature), signature: n.signature },
      sameConclusion: sameSignature(b.signature, n.signature),
      error: b.error || n.error,
    };
  });
  const sum = (arm, key) => rows.reduce((acc, r) => acc + r[arm][key], 0);
  return {
    mode: 'replay',
    sessions,
    db,
    files: rows.length,
    totals: {
      baseline: { calls: sum('baseline', 'calls'), input: sum('baseline', 'input'), output: sum('baseline', 'output'), cacheRead: sum('baseline', 'cacheRead'), actions: sum('baseline', 'actions') },
      budget: { calls: sum('budget', 'calls'), input: sum('budget', 'input'), output: sum('budget', 'output'), cacheRead: sum('budget', 'cacheRead'), actions: sum('budget', 'actions') },
    },
    sameConclusion: rows.filter((r) => r.sameConclusion).length,
    rows,
    notes: [
      '两个 arm 都是独立进程内的独立 llm 实例，输入/输出是按 [usage] 字段累计的真值。',
      '温度固定 0.3 仍有非确定性：结论差异按同一文件配对看，不能跨文件比。',
      '缓存读取取决于上游前缀缓存命中，回放数值与线上不同；费用要上线后另测。',
    ],
  };
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) { console.log(usage()); return 0; }
  if (!args.replay) {
    const logPath = args.log || resolveLogFile('memory_sleep.log');
    const report = estimateFromLog({ logPath, since: args.since, until: args.until, cap: args.cap });
    if (args.json) {
      console.log(JSON.stringify(report, null, 2));
      return 0;
    }
    console.log(`离线估算：${report.log}`);
    console.log(`窗口：${report.window.since || '-'} → ${report.window.until || '-'}；上限 ${report.cap} token`);
    console.log(`请求 ${report.requests} 条 / 文件收口 ${report.files} 行 / 其中带缓存读取的后续往返 ${report.followUps} 条`);
    console.log(`观测：未缓存输入 ${report.input} / 缓存读取 ${report.cacheRead} / 输出 ${report.output}`);
    console.log(`估算少送输入上界：${report.upperBoundSaved} token（下界 0）`);
    console.log('依据与不确定性：');
    for (const line of report.assumptions) console.log(`  - ${line}`);
    return 0;
  }
  const report = await replay({ sessions: args.sessions || resolveSessionsRoot(), db: args.db || resolveDbPath(), limit: args.limit, since: args.since });
  if (args.out) fs.writeFileSync(args.out, JSON.stringify(report, null, 2));
  if (args.json) {
    console.log(JSON.stringify(report, null, 2));
    return 0;
  }
  console.log(`真回放：${report.files} 个文件（${report.sessions}）`);
  console.log('arm        calls   未缓存输入   缓存读取   输出   动作数');
  for (const arm of ['baseline', 'budget']) {
    const t = report.totals[arm];
    console.log(`${arm.padEnd(10)} ${String(t.calls).padStart(5)} ${String(t.input).padStart(12)} ${String(t.cacheRead).padStart(10)} ${String(t.output).padStart(8)} ${String(t.actions).padStart(7)}`);
  }
  console.log(`结论一致：${report.sameConclusion}/${report.files}`);
  for (const line of report.notes) console.log(`  - ${line}`);
  return 0;
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main()
    .then((code) => { process.exitCode = code; })
    .catch((error) => {
      console.error(`回放失败：${error?.message || error}`);
      process.exitCode = 1;
    });
}
