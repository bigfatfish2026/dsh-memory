/**
 * 队列 + 池的端到端测试（2026-11-14 维护者拍板：sleepPass 从「顺序大函数」变成「起几个池 → 等它们都空 → 汇总」）。
 *
 * 钉七件事：
 *   1) pool 模式：6 个会话文件全部入库，ingest 池收口，逐文件日志有 ms；
 *   2) 领活粒度：每条 `[pool:ingest] 领 N 条` 的 N ≤ 本轮并发数，加起来正好 6；
 *   3) 一轮只取一次负载快照：自适应开着时 `[ingest] 并发自适应` 只有一行（不按批重算）；
 *   4) 自适应关掉：没有自适应行，各段回落到固定值；
 *   5) curate 通道：两条近似记忆聚成一批 → merge_batch 入队 → 池里问模型 → 空 ops 记 noop、op 终态 done；
 *   6) 灰度开关：organizePipelineMode='legacy' 或没带这个键 → 走老路径，日志里没有 [pool]；
 *   7) 结果一致：同一批文件，pool 与 legacy 的「新增」条数一样。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { applySchema } from '../scripts/apply-pipeline-schema.mjs';
import { setupSleep } from '../lib/sleep.js';

const T0 = Date.now();

function putSession(root, idx) {
  const dir = path.join(root, `agent-${idx % 2}`, `s${idx}`);
  fs.mkdirSync(dir, { recursive: true });
  const file = path.join(dir, `session-${idx}.jsonl`);
  const events = [
    {
      time: T0 - 60000 + idx * 1000,
      type: 'user/message',
      data: { source: { kind: 'user' }, content: [{ type: 'text', text: `文件${idx}的问题：机器几度？` }] },
    },
    {
      time: T0 - 59000 + idx * 1000,
      type: 'assistant/message',
      data: { message: { content: [{ type: 'text', text: `文件${idx}：${40 + idx}℃。` }] } },
    },
  ];
  fs.writeFileSync(file, `${events.map((e) => JSON.stringify(e)).join('\n')}\n`, 'utf8');
  return file;
}

const textChunks = (obj) => [
  { type: 'text-delta', text: JSON.stringify(obj) },
  { type: 'finish', reason: { kind: 'stop' } },
];
const toolChunks = (args) => [
  { type: 'tool-call-delta', index: 0, id: 'call_1', name: 'submit_ops', argumentsDelta: args },
  { type: 'finish', reason: { kind: 'stop' } },
];

/** 假模型：入库那条路回正文 JSON，merge 那条路回 submit_ops 工具调用。 */
function fakeLlm({ badIndex = -1 } = {}) {
  const prompts = [];
  return {
    prompts,
    stream(options) {
      const text = String(options?.messages?.map((m) => (
        Array.isArray(m.content) ? m.content.map((c) => c.text || '').join('') : m.content || ''
      )).join('\n') || '');
      prompts.push(text);
      const m = /文件(\d+)/.exec(text);
      if (m) {
        const idx = Number(m[1]);
        if (idx === badIndex) {
          // 坏输出：解析失败，只能记这一条失败，别的照跑
          return (async function* gen() {
            yield { type: 'text-delta', text: '不是 JSON' };
            yield { type: 'finish', reason: { kind: 'stop' } };
          })();
        }
        return (async function* gen() {
          yield* textChunks({
            add: [{ text: `文件${idx}的结论：树莓派温度 ${40 + idx} 度。`, type: 'world', tags: ['hardware'] }],
          });
        })();
      }
      // 拆分 agent（agents/split.js）：prompt 是「原条目 …：<正文>\n\n输出 …」。
      // 2026-11-15 切分点协议：只回切点偏移（80/160），正文由代码 slice，不许回正文。
      if (text.includes('原条目')) {
        return (async function* gen() {
          yield {
            type: 'text-delta',
            text: JSON.stringify({ offsets: [80, 160], unavoidable: false, reason: '' }),
          };
          yield { type: 'finish', reason: { kind: 'stop' } };
        })();
      }
      // merge agent：明确空 ops = 有效 noop
      return (async function* gen() {
        yield* toolChunks('{"ops":[]}');
      })();
    },
  };
}

/**
 * 写库计数桩：save/merge/update/archive 计入飞数量，峰值 >1 就是并发写库。
 * memories 给了就让 store.listAll() 吐它们（curate 通道聚类要用），否则恒空。
 */
function makeStoreStub(db, { memories = [], delayMs = 1 } = {}) {
  const writes = [];
  let inFlight = 0;
  let maxInFlight = 0;
  let seq = 0;
  const track = async (kind, text) => {
    inFlight += 1;
    if (inFlight > maxInFlight) maxInFlight = inFlight;
    try {
      if (delayMs) await new Promise((r) => setTimeout(r, delayMs));
      writes.push({ kind, text: String(text || '') });
      return { id: `new_${++seq}`, text, tags: [], type: 'world', status: 'active', entities: [] };
    } finally {
      inFlight -= 1;
    }
  };
  return {
    writes,
    get maxInFlight() { return maxInFlight; },
    db: () => db,
    listAll: () => memories,
    metas: () => memories,
    vectorIndex: () => new Map(memories.map((m) => [m.id, m.__vec])),
    typeOf: (m) => m?.type || 'world',
    save: (text) => track('save', text),
    merge: (_ids, text) => track('merge', text),
    update: (id, text) => track('update', text),
    archive: (id, reason) => track('archive', `${id} ${reason}`),
  };
}

function makeEnv({
  count = 0, mode = 'pool', ingestConcurrency = 3, adaptive = false, memories = [], badIndex = -1,
} = {}) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-pool-'));
  const dbPath = path.join(dir, 'memory.db');
  new DatabaseSync(dbPath).close();
  applySchema(dbPath, { logger: () => {} });
  const db = new DatabaseSync(dbPath);
  const sessionsRoot = path.join(dir, 'sessions');
  for (let i = 0; i < count; i += 1) putSession(sessionsRoot, i);

  const store = makeStoreStub(db, { memories });
  const llm = fakeLlm({ badIndex });
  const cfg = {
    storePath: dir,
    sessionsRoot,
    stateFile: path.join(dir, 'sleep.state.json'),
    backupDir: path.join(dir, 'backup'),
    sleepLogFile: path.join(dir, 'memory_sleep.log'),
    splitStateFile: path.join(dir, 'split.state.json'),
    splitLogFile: path.join(dir, 'memory_split.log'),
    splitPreviewFile: path.join(dir, 'split-preview.md'),
    notifyScript: '/bin/true',
    notify: false,
    sleepSummaryNotify: false,
    provider: 'fake',
    model: 'fake',
    reasoningEffort: 'low',
    maxTranscriptChars: 120000,
    maxMessageChars: 1500,
    conflictCheckEnabled: false,
    organizeIngestConcurrency: ingestConcurrency,
    organizeIngestAdaptive: adaptive,
    organizePipelineMode: mode,
    simulateSleep: false,
    sleepSplitEnabled: false,
    sleepTools: false,
    clusterMaxSize: 30,
    clusterNearDup: 0.9,
    clusterQuestionMin: 0.78,
    pipeline: { ingestAck: true, textLint: false, writeGate: false, agentPipeline: false },
  };
  const ctx = {
    logger: { info() {}, warn() {} },
    llm,
    tools: { get: () => null, register() {} },
    provide() {},
    effect: (fn) => fn(),
    get: () => null,
  };
  return { dir, db, dbPath, store, llm, cfg, sleep: setupSleep(ctx, store, cfg) };
}

const sleepLog = (cfg) => {
  try {
    return fs.readFileSync(cfg.sleepLogFile, 'utf8');
  } catch {
    return '';
  }
};

test('pool 模式：6 个文件全部入库，ingest 池收口，逐文件日志带 ms', async () => {
  const env = makeEnv({ count: 6, ingestConcurrency: 3 });
  const out = await env.sleep.sleepPass({});
  assert.equal(out.mode, 'pool');
  assert.equal(out.ingest.done, 6, `入库成功数 ${out.ingest.done}`);
  assert.equal(out.added.length, 6);
  assert.equal(env.store.maxInFlight, 1, '写库峰值必须是 1');
  assert.ok(out.pools.ingest.peakRunning <= 3, `在飞条数 ${out.pools.ingest.peakRunning} 超过并发 3`);

  const log = sleepLog(env.cfg);
  assert.match(log, /\[pool\] 轮开始 mode=pool ingest=3\/3/);
  assert.match(log, /\[pool:ingest\] 并发 3 路 条 6 成功 6 失败 0/);
  assert.equal((log.match(/\[ingest\] file=session-\d+\.jsonl ok=\d+ ms=\d+/g) || []).length, 6, '逐文件日志要 6 行');
  // 每次模型调用都要留一行 usage（成功路径也记），12:30 那轮 0 条 usage 就是这么漏的
  assert.match(log, /\[usage\] site=sleep:askModel provider=fake model=fake in=\S+ out=\S+ cacheRead=\S+ .* ok=1/);
});

test('领活粒度：每批领的条数不超过本轮并发数', async () => {
  const env = makeEnv({ count: 7, ingestConcurrency: 3 });
  await env.sleep.sleepPass({});
  const lines = sleepLog(env.cfg).match(/\[pool:ingest\] 领 (\d+) 条/g) || [];
  assert.ok(lines.length >= 3, `应当分批领活，实测 ${lines.length} 批`);
  let total = 0;
  for (const line of lines) {
    const n = Number(/领 (\d+) 条/.exec(line)[1]);
    assert.ok(n <= 3, `一批领了 ${n} 条，超过并发 3`);
    total += n;
  }
  assert.equal(total, 7, '领活总数要等于文件数');
});

test('一轮只取一次负载快照：自适应行只有一行，不按批重算', async () => {
  const env = makeEnv({ count: 6, ingestConcurrency: 3, adaptive: true });
  await env.sleep.sleepPass({});
  const log = sleepLog(env.cfg);
  const adaptiveLines = log.match(/\[ingest\] 并发自适应/g) || [];
  assert.equal(adaptiveLines.length, 1, `自适应行 ${adaptiveLines.length} 行，只许一轮一行`);
  assert.match(log, /\[pool:ingest\] 并发 \d+ 路/, '池收尾仍要报实开几路');
});

test('自适应关掉：没有自适应行，各段回落到固定上限', async () => {
  const env = makeEnv({ count: 4, ingestConcurrency: 2, adaptive: false });
  const out = await env.sleep.sleepPass({});
  const log = sleepLog(env.cfg);
  assert.doesNotMatch(log, /并发自适应/);
  assert.equal(out.pools.ingest.peakRunning <= 2, true, '关掉自适应就恒用配置值 2');
});

test('curate 通道：两条近似记忆聚成一批 → 池里问模型 → 空 ops 记 noop、op 终态 done', async () => {
  const vec = [1, 0, 0, 0];
  const memories = [
    { id: 'm_a', text: '树莓派温度 45 度，风扇正常。', ts: T0 - 2000, tags: ['hardware'], type: 'world', entities: ['树莓派'], status: 'active', __vec: vec },
    { id: 'm_b', text: '树莓派温度 46 度，风扇正常。', ts: T0 - 1000, tags: ['hardware'], type: 'world', entities: ['树莓派'], status: 'active', __vec: vec },
  ];
  const env = makeEnv({ count: 0, ingestConcurrency: 2, memories });
  const out = await env.sleep.sleepPass({});
  assert.equal(out.mode, 'pool');
  assert.ok(out.pools.curate.claimed >= 1, 'curate 池要领到 merge_batch');

  const rows = env.db.prepare("SELECT status, count(*) AS n FROM pending_ops WHERE kind='merge_batch' GROUP BY status").all();
  const byStatus = Object.fromEntries(rows.map((r) => [String(r.status), Number(r.n)]));
  assert.equal(byStatus.pending || 0, 0, '领过的批不许留在 pending（更不许整队标 running）');
  assert.equal(byStatus.done || 0, 1, '空 ops 是有效 noop，op 终态 done');

  const state = JSON.parse(fs.readFileSync(env.cfg.stateFile, 'utf8'));
  const cache = state.organizeCache || {};
  assert.equal(Object.values(cache).filter((v) => v.v === 'noop').length, 1, 'noop 必须写进 organizeCache');
  // noop 不写记忆，写库峰值只能是 0；一旦 >1 就说明池里并行写库了
  assert.ok(env.store.maxInFlight <= 1, `写库峰值 ${env.store.maxInFlight}`);
});

test('灰度开关：legacy 模式走老路径，日志里没有 [pool]', async () => {
  const env = makeEnv({ count: 4, mode: 'legacy', ingestConcurrency: 2 });
  const out = await env.sleep.sleepPass({});
  assert.notEqual(out.mode, 'pool');
  assert.equal(out.added.length, 4, '老路径照常入库');
  assert.doesNotMatch(sleepLog(env.cfg), /\[pool/);
});

test('缺省不带 organizePipelineMode：按 legacy 跑（手写 cfg 不因升级换路径）', async () => {
  const env = makeEnv({ count: 3, ingestConcurrency: 2 });
  delete env.cfg.organizePipelineMode;
  const out = await env.sleep.sleepPass({});
  assert.notEqual(out.mode, 'pool');
  assert.equal(out.added.length, 3);
});

test('结果一致：同一批文件，pool 与 legacy 的新增条数一样', async () => {
  const poolEnv = makeEnv({ count: 5, ingestConcurrency: 2 });
  const poolOut = await poolEnv.sleep.sleepPass({});
  const legacyEnv = makeEnv({ count: 5, ingestConcurrency: 2, mode: 'legacy' });
  const legacyOut = await legacyEnv.sleep.sleepPass({});
  assert.equal(poolOut.added.length, legacyOut.added.length, '两种调度的结果条数必须一致');
  assert.deepEqual(
    poolOut.added.map((a) => String(a.text)).sort(),
    legacyOut.added.map((a) => String(a.text)).sort(),
    '落地内容也要一致',
  );
});

test('单条失败只试一次：同一轮不许把失败的活又领回来（不收敛不算完）', async () => {
  const env = makeEnv({ count: 5, ingestConcurrency: 3, badIndex: 2 });
  const out = await env.sleep.sleepPass({});
  const log = sleepLog(env.cfg);
  const failLines = log.match(/\[ingest\] file=session-2\.jsonl FAIL/g) || [];
  assert.equal(failLines.length, 1, `同一轮只许失败一次，实测 ${failLines.length} 次`);
  assert.equal(out.ingest.done, 4, '别的文件照常落地');
  assert.equal(out.ingest.failed, 1, '失败的那条只记一次失败');
  assert.equal(out.ingest.queued, 1, '没判死就入队等下一轮');
  const row = env.db.prepare("SELECT status, attempts FROM pending_ops WHERE kind='ingest_file' AND dedupe_key LIKE '%session-2.jsonl'").get();
  assert.ok(row, '失败的活要留下账');
  assert.equal(Number(row.attempts), 1, 'attempts 只加一次，不许同轮原地加到 5');
});

test('curate 通道的拆分活：超长记忆走 split 消费者，拆开入库、原条归档、op 终态 done', async () => {
  const longText = '长条目：树莓派的温度在四十五度左右，风扇转速正常，磁盘剩余十五个点，网络延迟十二毫秒。'.repeat(8);
  const memories = [{
    id: 'm_long', text: longText, ts: T0 - 1000, tags: ['hardware'], type: 'world',
    entities: ['树莓派'], status: 'active', __vec: [1, 0, 0, 0],
  }];
  const env = makeEnv({ count: 0, ingestConcurrency: 4, memories });
  env.cfg.splitMaxChars = 200;
  env.cfg.pipeline = { ...env.cfg.pipeline, textLint: false, lintReview: false };
  const out = await env.sleep.sleepPass({});
  assert.ok(out.pools.split, '拆分消费者要跑起来');
  assert.ok(out.pools.split.inserted >= 2, `拆开入库 ${out.pools.split.inserted} 段`);
  assert.equal(out.splitInserted >= 2, true);
  const row = env.db.prepare("SELECT status FROM pending_ops WHERE kind='split_item'").get();
  assert.equal(row.status, 'done', '拆完的 split_item 必须终态 done');
  assert.equal(env.store.maxInFlight, 1, '写库峰值必须是 1');
});

test('拆分账不被 merge 池误收口：split_item 由拆分消费者处理，绝不许 done/stale_members', async () => {
  const env = makeEnv({ count: 0, ingestConcurrency: 2 });
  const source = `长条目：${'树莓派温度四十五度，风扇三千转。'.repeat(12)}`;
  env.db.prepare(
    `INSERT INTO pending_ops (kind, channel, dedupe_key, payload_json, attempts, next_try_at, last_error, status, created_ts, updated_ts)
     VALUES ('split_item', 'curate', 'm_split', ?, 0, ?, NULL, 'pending', ?, ?)`,
  ).run(JSON.stringify({ source_id: 'm_split', text: source, type: 'world' }), Date.now() - 1000, Date.now(), Date.now());

  await env.sleep.sleepPass({});

  const row = env.db.prepare("SELECT status, attempts, last_error FROM pending_ops WHERE kind='split_item'").get();
  assert.notEqual(row.last_error, 'stale_members', 'merge 池的成员失效结论不许盖在 split 账上');
  assert.equal(row.status, 'done', `拆分消费者应把它处理完，实测 status=${row.status} last_error=${row.last_error}`);
  assert.equal(Number(row.attempts), 0, '成功路径不耗 attempts');
  assert.match(sleepLog(env.cfg), /\[split\] item=m_split parts=\d+/, '必须有拆分消费者自己的落地日志');
  assert.doesNotMatch(sleepLog(env.cfg), /\[curate\] batch=split:m_split/, 'merge 池不该领到这条');
});

test('轮完成「拒绝」= discards 真实新增，「打回」才是提案口径', async () => {
  const env = makeEnv({ count: 0 });
  // 轮前就有一行 discards：按轮首/轮尾差值算，这一行不许算进本轮「拒绝」
  env.db.prepare(
    `INSERT INTO discards (candidate_hash, text, matched_id, verdict, reason, source_file, created_ts)
     VALUES ('h_old', '上轮留下的判重', NULL, 'DUPLICATE', '旧账', 'test', ?)`,
  ).run(Date.now() - 60000);

  const out = await env.sleep.sleepPass({});
  const line = sleepLog(env.cfg).split('\n').filter((l) => l.includes('[pool] 轮完成')).at(-1) || '';
  assert.match(line, /拒绝0\(discards\)/, `本轮没写 discards，拒绝必须是 0：${line}`);
  assert.match(line, new RegExp(`打回${out.rejected.length}\\(提案\\)`), '提案打回单列，别混进拒绝');
  assert.equal(out.discards, 0, '返回值里的 discards 也是本轮新增');
});
