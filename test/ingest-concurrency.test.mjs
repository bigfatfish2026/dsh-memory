/**
 * ingest 并发：
 * 入库是整轮第一段，原来一个文件问一次模型、全串行，29 个重放文件要 70 分钟。
 * 改成 N 路并发问模型、落地串行写库（和 merge 段同一套）。这里钉六件事：
 *   1) 10 个文件、并发 3 → 全部入库，计数对，和并发 1 的结果一致；
 *   2) 一个文件的模型输出坏掉 → 只记它失败，其他 9 个照常完成，日志有 1 失败；
 *   3) 并发 1 → 退回原来的 prepare→commit→下一个 串行节奏（回归保护）；
 *   4) 配置超过硬顶 64 → 夹到 64（2026-11-14 从 8 提到 64）；
 *   5) 落地串行：写库回调同时最多 1 个在跑（计数桩）；
 *   6) 池里一个 prepare 抛错不拖累别人；
 *   7) 文件数比配置少时只开文件数那么多 worker（8 个文件配 64 → 实测 8 路）。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { applySchema } from '../scripts/apply-pipeline-schema.mjs';
import { setupSleep, runIngestPool, ingestConcurrencyOf } from '../lib/sleep.js';
import { Config } from '../lib/index.js';

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

/** 假模型：从 prompt 里认出「文件N」，给一条 add；failFiles 里的返回坏 JSON（可在测试中途修好）。 */
function fakeLlm({ failFiles = new Set() } = {}) {
  const prompts = [];
  const failSet = new Set(failFiles);
  return {
    prompts,
    failFiles: failSet,
    stream(options) {
      const text = String(options?.messages?.[0]?.content?.[0]?.text || '');
      prompts.push(text);
      const m = /文件(\d+)/.exec(text);
      const idx = m ? Number(m[1]) : 0;
      const chunks = async function* gen() {
        if (failSet.has(idx)) {
          yield { type: 'text-delta', text: '不是 JSON，模型胡写了' };
          yield { type: 'finish', reason: { kind: 'stop' } };
          return;
        }
        yield {
          type: 'text-delta',
          text: JSON.stringify({
            add: [{ text: `文件${idx}的结论：树莓派温度 ${40 + idx} 度。`, type: 'world', tags: ['hardware'] }],
          }),
        };
        yield { type: 'finish', reason: { kind: 'stop' } };
      };
      return chunks();
    },
  };
}

/**
 * 写库计数桩：save/merge/update/archive 每个都记在飞数量，inFlight 峰值 >1 就是并发写库。
 * listAll 恒空，organize/作废清扫阶段没有记忆可整理，不会插进来调模型。
 */
function makeStoreStub(db, { delayMs = 1 } = {}) {
  const writes = [];
  let inFlight = 0;
  let maxInFlight = 0;
  let seq = 0;
  const track = async (kind, text) => {
    inFlight += 1;
    maxInFlight = Math.max(maxInFlight, inFlight);
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
    listAll: () => [],
    metas: () => [],
    vectorIndex: () => new Map(),
    typeOf: () => 'world',
    save: (text) => track('save', text),
    merge: (_ids, text) => track('merge', text),
    update: (id, text) => track('update', text),
    archive: (id, reason) => track('archive', `${id} ${reason}`),
  };
}

function makeEnv({ count = 10, failFiles = [], ingestConcurrency = 3, ingestAdaptive = false } = {}) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-ingest-conc-'));
  const dbPath = path.join(dir, 'memory.db');
  new DatabaseSync(dbPath).close();
  applySchema(dbPath, { logger: () => {} });
  const db = new DatabaseSync(dbPath);
  const sessionsRoot = path.join(dir, 'sessions');
  for (let i = 0; i < count; i += 1) putSession(sessionsRoot, i);

  const store = makeStoreStub(db);
  const llm = fakeLlm({ failFiles: new Set(failFiles) });
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
    // 2026-11-14：这组测试钉的是固定并发口径，显式关掉自适应；
    // 自适应本身在 ingest-adaptive.test.mjs 与下面那条日志测试里验。
    organizeIngestAdaptive: ingestAdaptive,
    organizeConcurrency: 1,
    sleepSplitEnabled: false,
    sleepTools: false,
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
  const sleep = setupSleep(ctx, store, cfg);
  return { dir, db, dbPath, store, llm, cfg, sleep };
}

function sleepLog(cfg) {
  try {
    return fs.readFileSync(cfg.sleepLogFile, 'utf8');
  } catch {
    return '';
  }
}

function pendingCount(db) {
  // 只数还欠着的（done/dead 是历史账，留在表里给人看）
  return Number(
    db.prepare("SELECT count(*) AS n FROM pending_ops WHERE status IN ('pending','running')").get()?.n ?? 0,
  );
}

test('10 个文件、并发 3：全部入库、计数正确、写库同时只有 1 个', async () => {
  const env = makeEnv({ count: 10, ingestConcurrency: 3 });
  const out = await env.sleep.sleepPass({ dryRun: false });

  assert.equal(out.ran, true);
  assert.equal(out.ingest.processed, 10, '10 个文件全处理');
  assert.equal(out.ingest.done, 10, '10 个文件全成功');
  assert.equal(out.ingest.failed, 0);
  assert.equal(out.ingest.cand, 20, '每个文件两条消息（cand 是老字段名，数的是消息）');
  assert.equal(out.ingest.events, 20, '每个文件两条事件');
  assert.equal(out.added.length, 10, '10 条记忆全入库');
  assert.equal(env.store.writes.filter((w) => w.kind === 'save').length, 10);
  for (let i = 0; i < 10; i += 1) {
    assert.ok(
      env.store.writes.some((w) => w.kind === 'save' && w.text.includes(`文件${i}的结论`)),
      `文件${i} 的结果必须落库`,
    );
  }
  assert.equal(env.store.maxInFlight, 1, '落地必须串行：写库回调同时最多 1 个');
  assert.match(sleepLog(env.cfg), /\[ingest\] 并发 3 路 文件 10 成功 10 失败 0/);
  assert.equal(pendingCount(env.db), 0, '全成功不该留 pending_ops');
  env.db.close();
});

test('并发 1 与并发 3 结果一致（回归保护）', async () => {
  const serial = makeEnv({ count: 10, ingestConcurrency: 1 });
  const outSerial = await serial.sleep.sleepPass({ dryRun: false });

  const parallel = makeEnv({ count: 10, ingestConcurrency: 3 });
  const outParallel = await parallel.sleep.sleepPass({ dryRun: false });

  for (const key of ['processed', 'done', 'failed', 'cand', 'events', 'chars', 'dead', 'queued']) {
    assert.equal(outParallel.ingest[key], outSerial.ingest[key], `stats.${key} 必须一致`);
  }
  assert.deepEqual(
    outParallel.added.map((a) => a.text).sort(),
    outSerial.added.map((a) => a.text).sort(),
    '入库正文集合一致',
  );
  assert.equal(serial.store.maxInFlight, 1);
  assert.equal(parallel.store.maxInFlight, 1);
  assert.match(sleepLog(serial.cfg), /\[ingest\] 并发 1 路 文件 10 成功 10 失败 0/);
  assert.match(sleepLog(parallel.cfg), /\[ingest\] 并发 3 路 文件 10 成功 10 失败 0/);
  serial.db.close();
  parallel.db.close();
});

test('一个文件抛错：其他 9 个照常完成，日志记 1 失败', async () => {
  const env = makeEnv({ count: 10, failFiles: [3], ingestConcurrency: 3 });
  const out = await env.sleep.sleepPass({ dryRun: false });

  assert.equal(out.ingest.processed, 10);
  assert.equal(out.ingest.done, 9, '坏文件不拖累别的');
  assert.equal(out.ingest.failed, 1);
  assert.equal(out.added.length, 9);
  assert.ok(
    env.store.writes.every((w) => !w.text.includes('文件3的结论')),
    '坏文件一条都不许落库',
  );
  const logText = sleepLog(env.cfg);
  assert.match(logText, /\[ingest\] 并发 3 路 文件 10 成功 9 失败 1/);
  assert.match(logText, /\[ingest\] file=session-3\.jsonl FAIL \S+: \S+/, '失败文件单独留一行，带原因');
  assert.equal(pendingCount(env.db), 1, '失败文件进 pending_ops 等重放');
  env.db.close();
});

test('重放段也走并发池：失败文件下一轮重放成功，不和新文件重复落库', async () => {
  const env = makeEnv({ count: 10, failFiles: [5], ingestConcurrency: 3 });
  const first = await env.sleep.sleepPass({ dryRun: false });
  assert.equal(first.ingest.failed, 1);
  assert.equal(pendingCount(env.db), 1, '坏文件进了 pending_ops');

  // 模型修好，下一轮应把 pending 的 file5 重放掉
  env.llm.failFiles.delete(5);
  const second = await env.sleep.sleepPass({ dryRun: false });
  assert.equal(second.ingest.replayed, 1, '重放段领到并完成 1 个');
  assert.equal(second.ingest.replayFailed, 0);
  assert.equal(pendingCount(env.db), 0, '重放成功 markOp done');
  const saves = env.store.writes.filter((w) => w.kind === 'save' && w.text.includes('文件5的结论'));
  assert.equal(saves.length, 1, '同一文件不许被重放和新文件池各落一遍');
  assert.match(sleepLog(env.cfg), /\[retry\] id=\d+ kind=ingest_file .*status=done file=.*session-5\.jsonl/);
  assert.match(
    sleepLog(env.cfg),
    /\[ingest\] file=session-5\.jsonl ok=\d+ ms=\d+ replay=1/,
    '重放落地也即时打一行（replay=1），汇总的「文件 N」含重放，行数才对得上',
  );
  env.db.close();
});

test('并发超过硬顶 64：配置夹到 64', async () => {
  assert.equal(ingestConcurrencyOf({ organizeIngestConcurrency: 999 }), 64, '999 夹到 64');
  assert.equal(ingestConcurrencyOf({ organizeIngestConcurrency: 65 }), 64);
  assert.equal(ingestConcurrencyOf({ organizeIngestConcurrency: 64 }), 64);
  assert.equal(ingestConcurrencyOf({ organizeIngestConcurrency: 8 }), 8, '8 仍合法，不往上凑');
  assert.equal(ingestConcurrencyOf({ organizeIngestConcurrency: 3 }), 3);
  assert.equal(ingestConcurrencyOf({ organizeIngestConcurrency: 0 }), 64, '0 按「没配」处理，函数兜底 64');
  assert.equal(ingestConcurrencyOf({}), 64, '连键都没有时函数兜底 64');

  // 池里再夹一次：给 999，实际同时在跑的 prepare 不许超过 min(64, 文件数)
  let live = 0;
  let peak = 0;
  const items = Array.from({ length: 100 }, (_, i) => i);
  const results = await runIngestPool({
    items,
    concurrency: 999,
    prepare: async (i) => {
      live += 1;
      peak = Math.max(peak, live);
      await new Promise((r) => setTimeout(r, 5));
      live -= 1;
      return i;
    },
    commit: async (v) => v,
  });
  assert.equal(results.length, 100);
  assert.ok(peak <= 64, `同时在跑 ${peak} 路，超过硬顶 64`);
  assert.ok(peak > 48, `确实开到大并发（实得 ${peak} 路）`);
});

test('配置默认 64：Config 解析出 64，夹取后仍是 64', () => {
  const cfg = Config({});
  assert.equal(cfg.organizeIngestConcurrency, 64, 'schema 默认值必须是 64');
  assert.equal(ingestConcurrencyOf(cfg), 64, '64 正好在上界，夹取后不缩');
  assert.equal(ingestConcurrencyOf({ organizeIngestConcurrency: 64 }), 64);
});

test('并发 8：8 个文件全成功、写库峰值仍 1', async () => {
  const env = makeEnv({ count: 8, ingestConcurrency: 8 });
  const out = await env.sleep.sleepPass({ dryRun: false });

  assert.equal(out.ingest.processed, 8);
  assert.equal(out.ingest.done, 8, '8 路并发下 8 个文件全成功');
  assert.equal(out.ingest.failed, 0);
  assert.equal(out.added.length, 8);
  assert.equal(env.store.writes.filter((w) => w.kind === 'save').length, 8);
  assert.equal(env.store.maxInFlight, 1, '并发 8 也只许串行写库');
  assert.match(sleepLog(env.cfg), /\[ingest\] 并发 8 路 文件 8 成功 8 失败 0/);
  env.db.close();
});

/**
 * 2026-11-14：默认提到 64 之后，文件数比配置少时不许开一堆空 worker。
 * 两级都盯：池的 worker 数 = min(并发, 文件数)；整轮日志的「N 路」打的是实际路数。
 */
test('8 个文件配并发 64：实际只开 8 路、全成功、写库峰值 1', async () => {
  const env = makeEnv({ count: 8, ingestConcurrency: 64 });
  const out = await env.sleep.sleepPass({ dryRun: false });

  assert.equal(out.ingest.processed, 8);
  assert.equal(out.ingest.done, 8, '8 个文件全成功');
  assert.equal(out.ingest.failed, 0);
  assert.equal(out.added.length, 8);
  assert.equal(env.store.maxInFlight, 1, '并发 64 也只许串行写库');
  assert.match(
    sleepLog(env.cfg),
    /\[ingest\] 并发 8 路 文件 8 成功 8 失败 0/,
    '日志要打实际路数（min(64, 8)=8），不是配置值 64',
  );
  env.db.close();
});

test('自适应打开：开池前打一行依据，取 1..64 路且整轮跑通', async () => {
  const env = makeEnv({ count: 2, ingestConcurrency: 64, ingestAdaptive: true });
  const out = await env.sleep.sleepPass({ dryRun: false });
  assert.equal(out.ingest.done, 2, '自适应算出的路数下两个文件都要落地');
  const text = sleepLog(env.cfg);
  const line = /\[ingest\] 并发自适应 取 (\d+) 路（上限 (\d+)，load1=\S+ cores=\d+ mem_avail=\S+GB temp=\S+C 依据=(cap|cpu|mem|temp75|temp80)）/.exec(text);
  assert.ok(line, `自适应日志缺了或格式不对：\n${text}`);
  const got = Number(line[1]);
  assert.ok(got >= 1 && got <= 64, `自适应路数 ${got} 越界`);
  assert.equal(Number(line[2]), 64, '上限要打理论硬顶 64');
  env.db.close();
});

test('自适应关掉：日志里没有自适应行，恒用配置值', async () => {
  const env = makeEnv({ count: 2, ingestConcurrency: 5, ingestAdaptive: false });
  const out = await env.sleep.sleepPass({ dryRun: false });
  assert.equal(out.ingest.done, 2);
  const text = sleepLog(env.cfg);
  assert.doesNotMatch(text, /并发自适应/);
  assert.match(text, /\[ingest\] 并发 2 路 文件 2 成功 2 失败 0/);
  env.db.close();
});

test('池按文件数收窄：8 个文件给 64 并发，同时最多 8 个 prepare 在跑', async () => {
  let live = 0;
  let peak = 0;
  const results = await runIngestPool({
    items: Array.from({ length: 8 }, (_, i) => i),
    concurrency: 64,
    prepare: async (i) => {
      live += 1;
      peak = Math.max(peak, live);
      await new Promise((r) => setTimeout(r, 10));
      live -= 1;
      return i;
    },
    commit: async (v) => v,
  });
  assert.equal(results.length, 8);
  assert.equal(peak, 8, `8 个文件必须正好开 8 路（实得 ${peak}），别开 56 个空 worker`);
});

test('并发 1：池按 prepare→commit 严格交替（原串行节奏）', async () => {
  const seq = [];
  await runIngestPool({
    items: [1, 2, 3],
    concurrency: 1,
    prepare: async (i) => {
      seq.push(`prepare:${i}`);
      return i;
    },
    commit: async (v) => {
      seq.push(`commit:${v}`);
      return v;
    },
  });
  assert.deepEqual(seq, ['prepare:1', 'commit:1', 'prepare:2', 'commit:2', 'prepare:3', 'commit:3']);
});

test('落地串行：commit 计数桩同时最多 1 个', async () => {
  let live = 0;
  let peak = 0;
  const landed = [];
  await runIngestPool({
    items: Array.from({ length: 20 }, (_, i) => i),
    concurrency: 5,
    prepare: async (i) => i,
    commit: async (i) => {
      live += 1;
      peak = Math.max(peak, live);
      await new Promise((r) => setTimeout(r, 2));
      landed.push(i);
      live -= 1;
      return i;
    },
  });
  assert.equal(peak, 1, '写库不许并发');
  assert.deepEqual([...landed].sort((a, b) => a - b), Array.from({ length: 20 }, (_, i) => i));
});

test('一个 prepare 抛错：只记它失败，其他照常落地', async () => {
  const results = await runIngestPool({
    items: [1, 2, 3, 4],
    concurrency: 2,
    prepare: async (i) => {
      if (i === 2) throw new Error('boom');
      return i;
    },
    commit: async (i) => `ok:${i}`,
  });
  assert.deepEqual(results.map((r) => r.ok), [true, false, true, true]);
  assert.equal(results[1].phase, 'prepare');
  assert.match(String(results[1].error.message), /boom/);
  assert.equal(results[0].value, 'ok:1');
  assert.equal(results[2].value, 'ok:3');
  assert.equal(results[3].value, 'ok:4');
});

/**
 * 2026-11-14：逐文件日志必须落地那一刻就打，不能攒到池收尾补打。
 * 原来 [ingest] file= 行在 for 循环里，池整个 await 完之后才一次性冒出来，
 * 中途十五分钟日志一条没有，只能去翻 memory.db 的 ingest_files.updated_ts。
 */
test('逐文件日志落地即打：3 个文件 → 3 行逐文件 + 1 行汇总，行数对得上、时间戳分散', async () => {
  const env = makeEnv({ count: 3, ingestConcurrency: 3 });
  const out = await env.sleep.sleepPass({ dryRun: false });
  assert.equal(out.ingest.done, 3);

  const text = sleepLog(env.cfg);
  const perFile = text.split('\n').filter((line) => line.includes('[ingest] file='));
  assert.equal(perFile.length, 3, '3 个文件必须正好 3 行逐文件日志，一行一个，不多不少');
  for (const line of perFile) {
    assert.match(line, /\[ingest\] file=session-\d+\.jsonl ok=\d+ ms=\d+/, `逐文件行格式：${line}`);
    assert.ok(!line.includes('/agent-'), '行里只放基名，别塞绝对路径');
  }
  const summary = text.split('\n').filter((line) => line.includes('[ingest] 并发 3 路 文件 3 成功 3 失败 0'));
  assert.equal(summary.length, 1, '汇总行保留且只有一行');
  assert.ok(
    text.lastIndexOf('[ingest] file=') < text.indexOf('[ingest] 并发 3 路'),
    '逐文件行必须都在汇总行之前（汇总仍是收尾那行）',
  );
  const stamps = perFile.map((line) => /^\[([^\]]+)\]/.exec(line)[1]);
  assert.equal(new Set(stamps).size, 3, `3 行时间戳应落在不同毫秒（同毫秒=补打），实得 ${stamps.join(' / ')}`);
  env.db.close();
});

test('池的 onLanded：commit 完立刻回调、ms 含 prepare+commit、时间戳各不同', async () => {
  const order = [];
  const stamps = [];
  const landedMs = [];
  const results = await runIngestPool({
    items: [0, 1, 2],
    concurrency: 3,
    prepare: async (i) => {
      await new Promise((r) => setTimeout(r, 10));
      return i;
    },
    commit: async (v) => {
      await new Promise((r) => setTimeout(r, 12));
      order.push(`commit:${v}`);
      return v;
    },
    onLanded: ({ item, result, ms }) => {
      order.push(`landed:${item}`);
      stamps.push(Date.now());
      landedMs.push(ms);
      assert.equal(result.ok, true);
    },
  });

  assert.deepEqual(results.map((r) => r.value), [0, 1, 2]);
  assert.deepEqual(
    order,
    ['commit:0', 'landed:0', 'commit:1', 'landed:1', 'commit:2', 'landed:2'],
    '每个文件 commit 完立刻回调，再落下一个',
  );
  assert.equal(landedMs.length, 3);
  for (const ms of landedMs) assert.ok(ms >= 10, `ms 要含 prepare+commit 的耗时，实得 ${ms}`);
  assert.equal(new Set(stamps).size, 3, '三次回调落在三个毫秒，不是攒完一起打');
});

test('池的 onLanded：失败也回调，带 phase 和原因', async () => {
  const got = [];
  await runIngestPool({
    items: [1, 2],
    concurrency: 2,
    prepare: async (i) => {
      if (i === 1) throw new Error('prepare 炸了');
      return i;
    },
    commit: async (i) => {
      if (i === 2) throw new Error('commit 炸了');
      return i;
    },
    onLanded: ({ item, result, ms }) => got.push({ item, result, ms }),
  });
  assert.equal(got.length, 2);
  assert.equal(got[0].result.ok, false);
  assert.equal(got[0].result.phase, 'prepare');
  assert.match(String(got[0].result.error.message), /prepare 炸了/);
  assert.equal(got[1].result.ok, false);
  assert.equal(got[1].result.phase, 'commit');
  assert.match(String(got[1].result.error.message), /commit 炸了/);
});
