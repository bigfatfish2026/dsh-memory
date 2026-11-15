#!/usr/bin/env node
/**
 * 在副本库上真跑一轮睡觉流水线（2026-11-05）。
 *
 *   node scripts/rehearse-pipeline.mjs --db <副本路径> --sessions <目录> --budget <N> \
 *     [--since <日期>] [--limit-op <N>] [--json]
 *
 * 存在意义：演练必须零碰真库。脚本开跑前后各取一次真库的逻辑指纹，变了就大声报错，
 * 哪怕副本那边跑得再漂亮也算失败。
 *
 * 2026-11-05 t-fix-first-run：别拿文件 sha/mtime 判真库有没有被写。真库是 WAL 模式、
 * qqbot 常驻读，新写全先落在 memory.db-wal 里，主文件 sha 可以一动不动；反过来
 * TRUNCATE checkpoint 又拿不到锁，拷出来的 sha 也不能代表"这一刻没人写"。
 * 所以守卫改成逻辑指纹：在 READONLY 连接上开一个读事务，一次取完
 * memories 行数 / vec_map 行数 / max(rowSeq) / ingest_files / pending_ops / discards 行数，
 * 再拼成 canonical 串比。文件 sha 只当参考，不参与判定。
 *
 * 行为：把 --db 当 storePath 打开（真 LLM，不 mock），按 agentPipeline/writeGate/ingestAck
 * 全开的配置跑 sleepPass：入库 → 合并 → 证据校验 → 拆分 → 逐条去重 → 入库。
 * --budget 是本轮最多读几个会话文件（从 --sessions 里挑最近的、且拷贝时校验能解压的）。
 * 所有状态/日志/备份都落在副本目录下的 .rehearse-<时间戳>/，不碰真库目录。
 */
import fs from 'node:fs';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { DEFAULT_DB_PATH, resolveMemoryFile } from './lib/db.mjs';
import { createStore } from '../lib/store.js';
import { setupSleep } from '../lib/sleep.js';
import { scanSessions } from '../lib/ingest-index.js';
import { readSessionEvents } from '../lib/transcript.js';
// 2026-11-05 t-fix-first-run：真库守卫改用逻辑指纹（WAL 里写的东西文件 sha 看不见）
import { logicalFingerprint, diffFingerprint, fileInfo } from '../lib/db-fingerprint.js';

const REAL_DB = DEFAULT_DB_PATH;
const REAL_STATE = resolveMemoryFile('sleep.state.json');
const DEFAULT_BASE_URL = process.env.DSH_REHEARSE_BASE_URL || 'http://127.0.0.1:3425/v1';
const DEFAULT_MODEL = process.env.DSH_REHEARSE_MODEL || 'group/ds';

function usage() {
  return `用法：node scripts/rehearse-pipeline.mjs --db <副本路径> --sessions <目录> --budget <N> [--since <日期>] [--limit-op <N>] [--json]

  --db        副本 memory.db 路径（必须不是真库）
  --sessions  会话目录，递归找 session*.jsonl(.zstd)
  --budget    本轮最多读几个会话文件（默认 5，按 mtime 从近到远）
  --since     lastSleepAt 水位，日期 YYYY-MM-DD 或 ISO 时间；默认读真库 sleep.state.json
  --state     指定现成的 sleep.state.json（默认在 runDir 新建空 state；要带线上缓存就显式给）
  --limit-op  重放/拆分 pending_ops 的条数上限（默认 20）
  --live      允许 --db 指真库：真库 sha 守卫只在副本模式生效（2026-11-05 t-first-live-run）
  --pipeline=pool|legacy  显式指定整理调度形态；不给就照旧不传（缺键按 legacy，脚本默认行为不变）
  --sessions-inplace      不拷会话文件，直接用 --sessions 目录里的原文件（真整理验证用；默认仍拷贝）
  --json      只输出机读 JSON

环境变量：DSH_REHEARSE_BASE_URL（默认 ${DEFAULT_BASE_URL}）、DSH_REHEARSE_MODEL（默认 ${DEFAULT_MODEL}）、DSH_REHEARSE_API_KEY。`;
}

function parseArgs(argv) {
  const out = { db: '', sessions: '', budget: 5, since: '', state: '', limitOp: 20, json: false, live: false, pipeline: '', sessionsInplace: false, help: false };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    const take = () => {
      const v = argv[i + 1];
      if (v === undefined) throw new Error(`${arg} 缺值`);
      i += 1;
      return v;
    };
    if (arg === '--db') out.db = path.resolve(take());
    else if (arg.startsWith('--db=')) out.db = path.resolve(arg.slice(5));
    else if (arg === '--sessions') out.sessions = path.resolve(take());
    else if (arg.startsWith('--sessions=')) out.sessions = path.resolve(arg.slice(11));
    else if (arg === '--budget') out.budget = Number(take()) || 5;
    else if (arg.startsWith('--budget=')) out.budget = Number(arg.slice(9)) || 5;
    else if (arg === '--since') out.since = take();
    else if (arg.startsWith('--since=')) out.since = arg.slice(8);
    else if (arg === '--state') out.state = path.resolve(take());
    else if (arg.startsWith('--state=')) out.state = path.resolve(arg.slice(8));
    else if (arg === '--limit-op') out.limitOp = Math.max(0, Number(take()) || 0);
    else if (arg.startsWith('--limit-op=')) out.limitOp = Math.max(0, Number(arg.slice(11)) || 0);
    else if (arg === '--pipeline') out.pipeline = take();
    else if (arg.startsWith('--pipeline=')) out.pipeline = arg.slice(11);
    else if (arg === '--sessions-inplace') out.sessionsInplace = true;
    else if (arg === '--json') out.json = true;
    else if (arg === '--live') out.live = true;
    else if (arg === '--help' || arg === '-h') out.help = true;
    else throw new Error(`未知参数：${arg}`);
  }
  return out;
}

function stampNow() {
  return new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
}

function parseSince(value) {
  if (!value) {
    try {
      const st = JSON.parse(fs.readFileSync(REAL_STATE, 'utf8'));
      if (Number(st.lastSleepAt) > 0) return { ms: Number(st.lastSleepAt), from: 'real sleep.state.json' };
    } catch {
      /* 没有就退回 24 小时 */
    }
    return { ms: Date.now() - 24 * 3600000, from: '默认最近 24 小时' };
  }
  const day = String(value).match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (day) {
    const ms = new Date(`${value}T00:00:00+08:00`).getTime();
    if (Number.isFinite(ms)) return { ms, from: `--since ${value}（北京时间当天 0 点）` };
  }
  const ms = Date.parse(value);
  if (Number.isFinite(ms)) return { ms, from: `--since ${value}` };
  throw new Error(`--since 解析不了：${value}`);
}

/* ── 真 LLM：OpenAI 兼容 /chat/completions 流式，翻成宿主 chunk 协议 ───────── */
function toOpenAiTools(tools) {
  return (tools || []).map((t) => ({
    type: 'function',
    function: { name: t.name, description: t.description || '', parameters: t.parameters || { type: 'object', properties: {} } },
  }));
}

function textOfContent(content) {
  if (typeof content === 'string') return content;
  if (!Array.isArray(content)) return '';
  return content.filter((p) => p && p.type === 'text').map((p) => String(p.text || '')).join('\n');
}

function toOpenAiMessages(system, messages) {
  const out = [];
  if (system) out.push({ role: 'system', content: String(system) });
  for (const msg of messages || []) {
    if (!msg) continue;
    if (msg.role === 'tool') {
      out.push({ role: 'tool', tool_call_id: String(msg.toolCallId || msg.tool_call_id || ''), content: textOfContent(msg.content) });
      continue;
    }
    if (msg.role === 'assistant') {
      const content = textOfContent(msg.content);
      const calls = Array.isArray(msg.content)
        ? msg.content.filter((p) => p && p.type === 'tool-call').map((p) => ({
          id: String(p.id || ''),
          type: 'function',
          function: { name: String(p.name || ''), arguments: typeof p.arguments === 'string' ? p.arguments : JSON.stringify(p.arguments ?? {}) },
        }))
        : [];
      const row = { role: 'assistant', content: content || (calls.length ? null : '') };
      if (calls.length) row.tool_calls = calls;
      out.push(row);
      continue;
    }
    out.push({ role: msg.role || 'user', content: textOfContent(msg.content) });
  }
  return out;
}

export function createRehearseLlm({ baseUrl, model, apiKey }) {
  const usage = { calls: 0, promptTokens: 0, completionTokens: 0, cost: 0, errors: 0 };
  return {
    usage,
    stream(request = {}) {
      usage.calls += 1;
      const body = {
        model,
        messages: toOpenAiMessages(request.system, request.messages),
        stream: true,
        stream_options: { include_usage: true },
        // 上游没传 maxTokens（默认）时按 provider 自己的上限走：本机 magpie 是 393216
        max_tokens: Number(request.maxTokens) || 393216,
        temperature: request.temperature === undefined ? 0.3 : request.temperature,
      };
      if (request.tools?.length) body.tools = toOpenAiTools(request.tools);
      if (request.reasoningEffort && request.reasoningEffort !== 'default') body.reasoning_effort = request.reasoningEffort;
      const headers = { 'content-type': 'application/json' };
      if (apiKey) headers.authorization = `Bearer ${apiKey}`;
      const self = {
        async *[Symbol.asyncIterator]() {
          let res;
          try {
            res = await fetch(`${baseUrl.replace(/\/$/, '')}/chat/completions`, {
              method: 'POST',
              headers,
              body: JSON.stringify(body),
              signal: AbortSignal.timeout(600000),
            });
          } catch (error) {
            usage.errors += 1;
            yield { type: 'finish', reason: { kind: 'error', failure: { transport: String(error?.message || error) } } };
            return;
          }
          if (!res.ok) {
            const detail = await res.text().catch(() => '');
            usage.errors += 1;
            yield { type: 'finish', reason: { kind: 'error', failure: { status: res.status, body: detail.slice(0, 500) } } };
            return;
          }
          const reader = res.body.getReader();
          const decoder = new TextDecoder();
          const tools = new Map();
          let buf = '';
          let finishReason = 'stop';
          const handle = (line) => {
            const raw = line.replace(/^data:\s*/, '').trim();
            if (!raw || raw === '[DONE]') return [];
            let obj;
            try { obj = JSON.parse(raw); } catch { return []; }
            if (obj.usage) {
              usage.promptTokens += Number(obj.usage.prompt_tokens) || 0;
              usage.completionTokens += Number(obj.usage.completion_tokens) || 0;
              usage.cost += Number(obj.usage.cost) || 0;
            }
            const choice = obj.choices?.[0];
            if (!choice) return [];
            if (choice.finish_reason) finishReason = choice.finish_reason;
            const delta = choice.delta || {};
            const chunks = [];
            if (typeof delta.content === 'string' && delta.content) chunks.push({ type: 'text-delta', text: delta.content });
            for (const tc of delta.tool_calls || []) {
              const idx = Number(tc.index) || 0;
              const acc = tools.get(idx) || { id: '', name: '', args: '' };
              if (tc.id) acc.id = tc.id;
              if (tc.function?.name) acc.name = tc.function.name;
              const arg = tc.function?.arguments;
              if (typeof arg === 'string' && arg) acc.args += arg;
              tools.set(idx, acc);
              chunks.push({
                type: 'tool-call-delta',
                index: idx,
                ...(tc.id ? { id: acc.id } : {}),
                ...(tc.function?.name ? { name: acc.name } : {}),
                argumentsDelta: typeof arg === 'string' ? arg : '',
              });
            }
            return chunks;
          };
          try {
            while (true) {
              const { done, value } = await reader.read();
              if (done) break;
              buf += decoder.decode(value, { stream: true }).replace(/\r\n/g, '\n');
              let cut;
              while ((cut = buf.indexOf('\n\n')) >= 0) {
                const frame = buf.slice(0, cut);
                buf = buf.slice(cut + 2);
                for (const line of frame.split('\n')) {
                  for (const chunk of handle(line)) yield chunk;
                }
              }
            }
            if (buf.trim()) for (const chunk of handle(buf)) yield chunk;
            for (const [idx, acc] of tools) {
              if (!acc.name) continue;
              yield { type: 'tool-call', index: idx, id: acc.id || `call_${idx}`, name: acc.name, arguments: acc.args || '{}' };
            }
            yield { type: 'finish', reason: { kind: finishReason === 'error' ? 'error' : finishReason } };
          } catch (error) {
            usage.errors += 1;
            yield { type: 'finish', reason: { kind: 'error', failure: { stream: String(error?.message || error) } } };
          }
        },
      };
      return self;
    },
  };
}

/* ── 复制会话文件：只拷最近的、能解压的，且拷的时候没在变 ───────────────── */
function pickSessions(sourceDir, runDir, budget, { inplace = false } = {}) {
  const all = scanSessions(sourceDir);
  const picked = [];
  const skipped = [];
  // inplace：直接用源目录当 sessionsRoot（不拷贝、也不伪造路径）
  const copyRoot = inplace ? sourceDir : path.join(runDir, 'sessions');
  if (!inplace) fs.mkdirSync(copyRoot, { recursive: true });
  // 先看一眼 mtime，1.2 秒后再看；区别说明还有人在写，别拷半截 zstd
  const first = new Map(all.map((f) => [f.path, `${f.mtime}:${f.size}`]));
  const wait = Date.now() + 1200;
  // 同步等待：脚本是离线的，这里偷个懒
  while (Date.now() < wait) {
    try { fs.statSync(sourceDir); } catch { /* 忽略 */ }
    Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 40);
  }
  for (let i = all.length - 1; i >= 0 && picked.length < budget; i -= 1) {
    const file = all[i];
    let st;
    try { st = fs.statSync(file.path); } catch { skipped.push({ path: file.path, reason: 'stat_fail' }); continue; }
    if (first.get(file.path) !== `${st.mtimeMs}:${st.size}`) {
      skipped.push({ path: file.path, reason: 'still_writing' });
      continue;
    }
    // 默认行为：拷一份到 runDir（演练零碰真库）；--sessions-inplace 才直接用原文件。
    let dst = file.path;
    if (!inplace) {
      const dir = path.join(copyRoot, `s${String(picked.length).padStart(2, '0')}`);
      fs.mkdirSync(dir, { recursive: true });
      dst = path.join(dir, path.basename(file.path));
      try {
        fs.copyFileSync(file.path, dst);
      } catch (error) {
        skipped.push({ path: file.path, reason: `copy_fail:${String(error?.message || error)}` });
        continue;
      }
    }
    const events = readSessionEvents(dst);
    if (!events) {
      skipped.push({ path: file.path, reason: inplace ? 'unreadable_source' : 'unreadable_copy' });
      if (!inplace) fs.rmSync(dst, { force: true });
      continue;
    }
    picked.push({ source: file.path, copy: dst, events: events.length, lastEventTs: events.reduce((n, e) => Math.max(n, Number(e.time) || 0), 0) });
  }
  picked.reverse();
  return { picked, skipped, scanned: all.length, copyRoot };
}

function readDedupeVerdicts(logFile) {
  const counts = { DUPLICATE: 0, SUPERSEDES: 0, DISTINCT: 0 };
  let total = 0;
  try {
    for (const line of fs.readFileSync(logFile, 'utf8').split('\n')) {
      const m = line.match(/\[dedupe\] verdict=(DUPLICATE|SUPERSEDES|DISTINCT)/);
      if (!m) continue;
      counts[m[1]] += 1;
      total += 1;
    }
  } catch {
    /* 没日志就是零 */
  }
  return { counts, total };
}

function pendingSummary(db) {
  const rows = db.prepare('SELECT kind, status, count(*) AS n FROM pending_ops GROUP BY kind, status ORDER BY kind, status').all();
  const out = { total: 0, byKindStatus: [] };
  for (const r of rows) {
    const n = Number(r.n) || 0;
    out.total += n;
    out.byKindStatus.push({ kind: r.kind, status: r.status, n });
  }
  return out;
}

function duplicateSample(db, sinceDiscardId) {
  const rows = db.prepare('SELECT id, text, matched_id, reason, created_ts FROM discards WHERE id > ? ORDER BY id LIMIT 20').all(sinceDiscardId);
  for (const row of rows) {
    if (!row.matched_id) continue;
    const matched = db.prepare('SELECT id, text, type, status FROM memories WHERE id = ?').get(row.matched_id);
    if (!matched) continue;
    return {
      candidate: { text: String(row.text || '').slice(0, 500), reason: row.reason || '' },
      matchedId: String(row.matched_id),
      matchedText: String(matched.text || '').slice(0, 500),
      matchedType: matched.type || '',
      matchedStatus: matched.status || '',
    };
  }
  return null;
}

/**
 * 2026-11-06 t-fix-tooldup：整理参数别在演练脚本里写死。
 *
 * 原来 :412 固定 organizeMaxCalls=60、:414 clusterMaxSize=30、:429
 * writeGateThreshold=0.9，跟本机部署的实际配置（organizeMaxCalls=200）对不上，
 * 演练跑出来的排队上限比线上小一截。现在按优先级读：
 *   插件自带 cordis.patch.yml → 用户 profile 覆盖 → DSH_MEMORY_CONFIG 指定文件
 * 后读的覆盖先读的；都没配就由调用处兜底（60 / 30 / 0.9）。
 * 只做「键: 数字」这一层的提取，引号与行尾注释会去掉，不引入 YAML 依赖。
 */
function readMemoryConfig() {
  const home = process.env.HOME || os.homedir();
  const files = [
    path.join(import.meta.dirname, '..', 'cordis.patch.yml'),
    path.join(home, '.dsh/profiles/qqbot/cordis.patch.yml'),
    process.env.DSH_MEMORY_CONFIG || '',
  ].filter((f) => f && fs.existsSync(f));
  const out = {};
  for (const file of files) {
    let inMemory = false;
    let entryIndent = -1;
    let section = '';
    let sectionIndent = -1;
    for (const raw of fs.readFileSync(file, 'utf8').split('\n')) {
      const line = raw.replace(/\s+#.*$/, '');
      const text = line.trim();
      if (!text || text.startsWith('#')) continue;
      const indent = line.length - line.trimStart().length;
      const bullet = text.match(/^-\s+id:\s*['"]?([\w-]+)['"]?$/);
      if (bullet) {
        inMemory = bullet[1] === 'memory';
        entryIndent = indent;
        section = '';
        sectionIndent = -1;
        continue;
      }
      if (entryIndent >= 0 && indent <= entryIndent) inMemory = false; // 换到下一个插件条目
      if (!inMemory) continue;
      const m = text.match(/^([A-Za-z_][\w]*):\s*(.*)$/);
      if (!m) continue;
      const [, key, value] = m;
      if (value === '') {
        section = key;
        sectionIndent = indent;
        continue;
      }
      const num = Number(String(value).replace(/^['"]|['"]$/g, '').trim());
      if (!Number.isFinite(num)) continue;
      out[section === 'pipeline' && indent > sectionIndent ? `pipeline.${key}` : key] = num;
    }
  }
  return out;
}

/** 取第一个有限正数；全给空就返回 undefined，由调用处的 ?? 兜底。 */
/**
 * 演练并发：DSH_REHEARSE_CONCURRENCY 显式给了（含 0 = 不覆盖、走负载自适应）就用它，
 * 没给就维持老默认 2。默认行为一个字不变。
 */
function rehearseConcurrency(memoryCfg) {
  const raw = process.env.DSH_REHEARSE_CONCURRENCY;
  if (raw !== undefined && raw !== '' && Number.isFinite(Number(raw))) return Number(raw);
  return pickNum(memoryCfg.organizeConcurrency) ?? 2;
}

function pickNum(...candidates) {
  for (const v of candidates) {
    if (v === undefined || v === null || v === '') continue;
    const n = Number(v);
    if (Number.isFinite(n) && n > 0) return n;
  }
  return undefined;
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help) { console.log(usage()); return 0; }
  if (!args.db) throw new Error('--db 必填');
  if (!args.sessions) throw new Error('--sessions 必填');
  if (!fs.existsSync(args.db)) throw new Error(`副本库不存在：${args.db}`);
  const liveMode = args.db === REAL_DB;
  if (liveMode && !args.live) throw new Error(`--db 指到真库了：${args.db}；演练必须用副本（真要写真库请显式加 --live）`);
  if (!fs.statSync(args.sessions).isDirectory()) throw new Error(`--sessions 不是目录：${args.sessions}`);
  const budget = Math.max(1, Math.floor(args.budget || 5));

  const realBefore = logicalFingerprint(REAL_DB);
  const realFilesBefore = fileInfo(REAL_DB);
  const started = Date.now();
  const since = parseSince(args.since);
  const runDir = path.join(path.dirname(args.db), `.rehearse-${stampNow()}`);
  fs.mkdirSync(runDir, { recursive: true });
  const llm = createRehearseLlm({ baseUrl: DEFAULT_BASE_URL, model: DEFAULT_MODEL, apiKey: process.env.DSH_REHEARSE_API_KEY || '' });
  const say = (line) => { if (!args.json) console.error(line); };

  const pickedInfo = pickSessions(args.sessions, runDir, budget, { inplace: args.sessionsInplace });
  if (!pickedInfo.picked.length) throw new Error('没有可读的会话文件，放弃演练');
  say(`会话：扫到 ${pickedInfo.scanned} 个，选 ${pickedInfo.picked.length}/${budget} 个（跳过 ${pickedInfo.skipped.length} 个）`);

  // 2026-11-05 t-state-swap-run：--state 给现成 state（线上那份带 organizeCache），
  // 已存在就原样用、绝不覆盖；没给就照旧在 runDir 新建空 state。
  const stateFile = args.state || path.join(runDir, 'sleep.state.json');
  if (!fs.existsSync(stateFile)) {
    fs.writeFileSync(stateFile, JSON.stringify({ lastSleepAt: since.ms, slots: {} }, null, 2), 'utf8');
  }
  const storeLogFile = path.join(runDir, 'memory_store.log');
  const sleepLogFile = path.join(runDir, 'memory_sleep.log');
  const splitLogFile = path.join(runDir, 'memory_split.log');

  // 2026-11-06 t-fix-tooldup：这三个值原来写死（60/30/0.9），演练跟线上配置脱节。
  const memoryCfg = readMemoryConfig();
  const organizeMaxCalls = pickNum(process.env.DSH_REHEARSE_MAX_MERGE_CALLS, memoryCfg.organizeMaxCalls) ?? 60;
  const clusterMaxSize = pickNum(memoryCfg.clusterMaxSize) ?? 30;
  const writeGateThreshold = pickNum(memoryCfg['pipeline.writeGateThreshold']) ?? 0.9;
  say(`整理参数（读配置）：organizeMaxCalls=${organizeMaxCalls} clusterMaxSize=${clusterMaxSize} writeGateThreshold=${writeGateThreshold}`);

  const cfg = {
    storePath: path.dirname(args.db),
    sessionsRoot: pickedInfo.copyRoot,
    stateFile,
    backupDir: path.join(runDir, 'backup'),
    archiveDir: path.join(runDir, 'archive'),
    storeLogFile,
    sleepLogFile,
    splitLogFile,
    splitStateFile: path.join(runDir, 'split.state.json'),
    splitPreviewFile: path.join(runDir, 'split-preview.md'),
    notifyScript: '/bin/true',
    notify: false,
    sleepSummaryNotify: false,
    sleepEnabled: false,
    sleepSplitEnabled: true,
    sleepTools: true,
    conflictCheckEnabled: true,
    provider: 'magpie-rehearse',
    model: DEFAULT_MODEL,
    apiUrl: process.env.DSH_REHEARSE_EMBED_URL || process.env.DSH_EMBED_API || 'http://127.0.0.1:8080',
    timeoutMs: 60000,
    maxChanges: 20,
    maxSessions: budget,
    maxMessageChars: 1500,
    maxTranscriptChars: 120000,
    addPassChars: 40000,
    sleepMemoryChars: 120000,
    sleepMaxTokens: 16000,
    mergeMaxTokens: 0,
    mergeBatchMax: 6,
    organizeMaxCalls,
    organizeConcurrency: rehearseConcurrency(memoryCfg),
    clusterMaxSize,
    clusterNearDup: 0.9,
    clusterQuestionMin: 0.78,
    clusterChanges: 6,
    splitMaxChars: 200,
    splitMaxItems: 20,
    sleepSplitLimit: 20,
    splitMaxTokens: 6000,
    reasoningEffort: 'low',
    // 只在显式 --pipeline 时写这一行：不给就保持「cfg 没这个键 = legacy」的老行为。
    ...(args.pipeline ? { organizePipelineMode: args.pipeline } : {}),
    pipeline: {
      ingestAck: true,
      ingestBudget: budget,
      replayBudget: args.limitOp,
      agentPipeline: true,
      writeGate: true,
      writeGateThreshold,
    },
    logger: { info() {}, warn() {}, error() {} },
  };

  let store;
  let db;
  let result = null;
  let failure = null;
  let discardIdBefore = 0;
  let tablesBefore = null;
  try {
    store = createStore(cfg);
    store.attachLlm?.(llm);
    db = store.db();
    discardIdBefore = Number(db.prepare('SELECT coalesce(max(id), 0) AS n FROM discards').get()?.n ?? 0);
    tablesBefore = {
      memories: Number(db.prepare('SELECT count(*) AS n FROM memories').get()?.n ?? 0),
      ingest: Number(db.prepare('SELECT count(*) AS n FROM ingest_files').get()?.n ?? 0),
      pending: Number(db.prepare('SELECT count(*) AS n FROM pending_ops').get()?.n ?? 0),
    };
    const ctx = {
      logger: { info() {}, warn() {}, error() {} },
      llm,
      tools: { get: () => null, register() {} },
      provide() {},
      effect() {},
    };
    const sleep = setupSleep(ctx, store, cfg);
    say(`开跑：db=${args.db} since=${new Date(since.ms).toISOString()}（${since.from}）model=${DEFAULT_MODEL} state=${stateFile}`);
    result = await sleep.sleepPass({ dryRun: false });
  } catch (error) {
    failure = { message: String(error?.message || error), stack: String(error?.stack || '').split('\n').slice(0, 6).join('\n') };
  }

  const realAfter = logicalFingerprint(REAL_DB);
  const realFilesAfter = fileInfo(REAL_DB);
  const realDiff = diffFingerprint(realBefore, realAfter);
  const realGuard = {
    // 逻辑指纹：READONLY 读事务里取的几个数，拼起来比。文件 sha 不进这里
    before: realBefore,
    after: realAfter,
    diff: realDiff,
    // 文件层面的参考信息（含 -wal），只给人看
    files: { before: realFilesBefore, after: realFilesAfter },
    enforced: !liveMode,
    unchanged: liveMode ? null : realDiff.length === 0,
  };

  let report = null;
  if (db) {
    const verdicts = readDedupeVerdicts(storeLogFile);
    const org = result?.organize || {};
    const splitAgent = org.splitAgent || {};
    const inflation = Number(splitAgent.charsBefore) > 0
      ? Number((Number(splitAgent.charsAfter) / Number(splitAgent.charsBefore)).toFixed(3))
      : null;
    report = {
      db: args.db,
      runDir,
      stateFile,
      since: { ms: since.ms, iso: new Date(since.ms).toISOString(), from: since.from },
      budget,
      limitOp: args.limitOp,
      sessions: {
        scanned: pickedInfo.scanned,
        picked: pickedInfo.picked.map((p) => ({ file: p.source, events: p.events })),
        skipped: pickedInfo.skipped,
      },
      ingest: {
        files: result?.ingest?.processed ?? null,
        events: result?.ingest?.events ?? null,
        candidates: result?.ingest?.cand ?? null,
        chars: result?.ingest?.chars ?? null,
        failed: result?.ingest?.failed ?? null,
        queued: result?.ingest?.queued ?? null,
      },
      merge: {
        batches: org.clusters ?? null,
        queued: org.queued ?? null,
        calls: org.calls ?? null,
        ops: org.ops ?? null,
        invalid: org.invalid ?? null,
        invalidSamples: org.invalidSamples || [],
        merged: org.merged?.length ?? null,
        updated: org.updated?.length ?? null,
        archived: org.archived?.length ?? null,
        added: org.added?.length ?? null,
        splitRequested: org.split?.length ?? null,
      },
      split: {
        parts: splitAgent.parts ?? null,
        inserted: splitAgent.inserted ?? null,
        deduped: splitAgent.deduped ?? null,
        rejected: splitAgent.rejected ?? null,
        unavailable: splitAgent.unavailable ?? null,
        charsBefore: splitAgent.charsBefore ?? null,
        charsAfter: splitAgent.charsAfter ?? null,
        inflation,
        splitPass: result?.split || null,
      },
      dedupe: {
        counts: verdicts.counts,
        total: verdicts.total,
        duplicateSample: duplicateSample(db, discardIdBefore),
      },
      written: {
        added: result?.added?.length ?? null,
        merged: result?.merged?.length ?? null,
        updated: result?.updated?.length ?? null,
        archived: result?.archived?.length ?? null,
      },
      pending: pendingSummary(db),
      tablesBefore,
      tablesAfter: {
        memories: Number(db.prepare('SELECT count(*) AS n FROM memories').get()?.n ?? 0),
        ingest: Number(db.prepare('SELECT count(*) AS n FROM ingest_files').get()?.n ?? 0),
        pending: Number(db.prepare('SELECT count(*) AS n FROM pending_ops').get()?.n ?? 0),
        discards: Number(db.prepare('SELECT count(*) AS n FROM discards').get()?.n ?? 0),
      },
      llm: { model: DEFAULT_MODEL, ...llm.usage },
      ms: Date.now() - started,
      realGuard,
      skipped: result?.skipped || null,
      failure,
    };
  }

  if (args.json) {
    console.log(JSON.stringify({ ok: !failure && (liveMode || realGuard.unchanged), report, realGuard, failure }, null, 2));
  } else {
    if (report) {
      console.log('— 演练账目 —');
      console.log(`库：${report.db}`);
      console.log(`水位：${report.since.iso}（${report.since.from}）`);
      console.log(`会话：选 ${report.sessions.picked.length}/${budget} 个，读 ${report.ingest.files ?? '-'} 个文件 / ${report.ingest.events ?? '-'} 个事件 / ${report.ingest.chars ?? '-'} 字符`);
      console.log(`候选：${report.ingest.candidates ?? '-'} 条；失败 ${report.ingest.failed ?? '-'}，入队 ${report.ingest.queued ?? '-'}`);
      console.log(`合并：${report.merge.batches ?? '-'} 批 / ${report.merge.calls ?? '-'} 次调用，出 ${report.merge.ops ?? '-'} 个 op；证据作废 ${report.merge.invalid ?? '-'} 条`);
      for (const s of report.merge.invalidSamples) console.log(`  作废示例 ${s.op} ${s.reason}：${String(s.text || s.quote || '').slice(0, 100)}`);
      console.log(`拆分：${report.split.parts ?? '-'} 段（入库 ${report.split.inserted ?? '-'}，判重 ${report.split.deduped ?? '-'}，拒 ${report.split.rejected ?? '-'}），膨胀率 ${report.split.inflation ?? '-'}`);
      console.log(`去重：DUPLICATE=${report.dedupe.counts.DUPLICATE} SUPERSEDES=${report.dedupe.counts.SUPERSEDES} DISTINCT=${report.dedupe.counts.DISTINCT}（共 ${report.dedupe.total} 次裁决）`);
      if (report.dedupe.duplicateSample) {
        const d = report.dedupe.duplicateSample;
        console.log(`  DUPLICATE 抽样：${d.candidate.text.slice(0, 80)} … ↔ ${d.matchedId}`);
      }
      console.log(`写入：新增 ${report.written.added ?? '-'} 合并 ${report.written.merged ?? '-'} 修订 ${report.written.updated ?? '-'} 归档 ${report.written.archived ?? '-'}`);
      console.log(`pending 剩：${report.pending.total} 条 ${JSON.stringify(report.pending.byKindStatus)}`);
      console.log(`表：memories ${report.tablesBefore.memories}→${report.tablesAfter.memories}，ingest_files ${report.tablesBefore.ingest}→${report.tablesAfter.ingest}，pending_ops ${report.tablesBefore.pending}→${report.tablesAfter.pending}`);
      console.log(`LLM：${report.llm.calls} 次调用 / prompt ${report.llm.promptTokens} tok / completion ${report.llm.completionTokens} tok / 错误 ${report.llm.errors}`);
      console.log(`耗时：${(report.ms / 1000).toFixed(1)}s`);
    } else {
      console.log(`失败：${failure?.message || '未知'}`);
      if (failure?.stack) console.log(failure.stack);
    }
    console.log(liveMode ? '— 真库守卫（live 模式：不比对指纹）—' : '— 真库守卫（逻辑指纹，不看文件 sha）—');
    if (realBefore.error || realAfter.error) {
      console.log(`指纹取不到：${realBefore.error || realAfter.error}`);
    } else {
      console.log(`前：${realBefore.canonical}`);
      console.log(`后：${realAfter.canonical}`);
      console.log(`-wal：${realFilesBefore.wal?.size ?? '-'} → ${realFilesAfter.wal?.size ?? '-'} 字节（仅供参考，新写都在里面）`);
      for (const line of realDiff) console.log(`  变了 ${line}`);
      console.log(liveMode
        ? 'live：本轮就是写真库，数字变化属预期'
        : realGuard.unchanged ? '真库逻辑上没动 ✅' : '!!! 真库被写了，这次演练作废 !!!');
    }
  }

  if (!liveMode && !realGuard.unchanged) {
    console.error('!!! 真库逻辑指纹变了（行数 / rowSeq / 新表行数任一不同）：演练不合格，别拿这份账目当真 !!!');
    for (const line of realDiff) console.error(`  ${line}`);
    console.error('  注意：常驻 qqbot 也在写同一个库，先看上面的 diff 是不是它的手笔，再决定要不要重跑。');
    return 2;
  }
  if (failure) return 1;
  return 0;
}

// 只有直接执行才跑 main()：sleep-now.mjs / 测试要 import createRehearseLlm 时不许顺手开跑。
if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main()
    .then((code) => { process.exitCode = code; })
    .catch((error) => {
      console.error(`演练启动失败：${error?.message || error}`);
      process.exitCode = 1;
    });
}
