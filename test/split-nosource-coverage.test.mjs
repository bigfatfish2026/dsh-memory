/**
 * 无原条拆分的覆盖闸、轮末补扫、分类计数（t-mwp0uk01-98eh）。
 *
 * 钉四件事：
 *   ① 无原条（payload.source_id 为空、dedupeKey 只是队列键）也过覆盖闸：缺内容 → dead，
 *      不通过就绝不许标 done（op 10992：原文 402 字、两段产物 272 字，缺 130 字全库无承接）；
 *      覆盖完整 → done。
 *   ② 领空就收工的补丁：consumeSplitOpsWithRecheck 在生产者收工后再扫一轮，
 *      新入队的活能被领到；扫不到就停，maxPasses 硬顶，不死等。
 *   ③ 汇总口径：落库 / 全判重 / 跳过 / 回队 / 版本不符 / 覆盖未过 各数各的，
 *      相加等于 scanned，且能和 pending_ops 的 done/dead/pending 对上。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { applySchema } from '../scripts/apply-pipeline-schema.mjs';
import {
  consumeSplitOps, consumeSplitOpsWithRecheck, verifySplitTextCoverage,
} from '../lib/pipeline-split.js';
import { enqueue } from '../lib/ingest-index.js';
import { splitQueueKey } from '../lib/split-queue.js';

function tmpDb() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-split-nosrc-'));
  const dbPath = path.join(dir, 'memory.db');
  new DatabaseSync(dbPath).close();
  applySchema(dbPath, { logger: () => {} });
  return { dir, dbPath, db: new DatabaseSync(dbPath) };
}

/** 假模型：从提示词里取回原文，按 planOf 给切分方案。 */
function fakeLlm(planOf = twoPartPlan) {
  const calls = [];
  return {
    calls,
    stream(options) {
      const prompt = String(options?.messages?.[0]?.content?.[0]?.text || '');
      const m = /）：([\s\S]*?)\n\n输出/.exec(prompt);
      const src = m ? m[1] : '';
      const plan = planOf(src, calls.length);
      calls.push(prompt);
      return (async function* chunks() {
        yield { type: 'text-delta', text: JSON.stringify(plan) };
        yield { type: 'finish', reason: { kind: 'stop' } };
      })();
    },
  };
}

/** 两段切分：第二段开头当锚。 */
function twoPartPlan(src) {
  const cut = src.indexOf('。') + 1;
  return { cuts: src && cut > 0 && cut < src.length ? [src.slice(cut, cut + 10)] : [] };
}

/** 假 store：metas 就是库视图，save 落一条 active 记录；records 可预置（承接用）。 */
function fakeStore({ metas = [], saveImpl = null } = {}) {
  const saves = [];
  const archives = [];
  return {
    metas: () => metas,
    typeOf: () => 'world',
    async save(text, tags, source, meta) {
      saves.push({ text: String(text), source, meta: meta || {} });
      if (saveImpl) {
        const injected = await saveImpl({ metas, text: String(text), source, meta: meta || {}, saves });
        if (injected !== undefined) return injected;
      }
      const rec = { id: `child_${saves.length}`, text: String(text).trim(), status: 'active', tags: [], entities: [], source };
      metas.push(rec);
      return { id: rec.id, text: rec.text };
    },
    async archive(id, reason, opts) {
      archives.push({ id, reason, opts });
      const rec = metas.find((m) => m.id === id);
      if (rec) {
        rec.status = 'archived';
        if (opts?.supersededBy) rec.supersededBy = opts.supersededBy;
      }
    },
    saves,
    archives,
  };
}

/** 无原条的拆分活：payload 里只有正文，source_id 留空，dedupe_key 用 split:<sha1 前16>。 */
function enqueueNoSource(db, text, now = 1) {
  const key = splitQueueKey({ text });
  const row = enqueue(db, 'split_item', key, { text, type: 'world', source_id: '' }, { now });
  return { key, row };
}

function opRow(db, key) {
  return db.prepare("SELECT status, last_error, attempts FROM pending_ops WHERE kind='split_item' AND dedupe_key=?").get(key);
}

/* ─────────────── ① 无原条：去空白覆盖比对 ─────────────── */

test('① op 10992 的形状：原文 402 字、产物 272 字、缺的 130 字没有承接 → 覆盖校验不通过', () => {
  const head = '甲'.repeat(272);
  const tail = '乙'.repeat(130);
  const parts = [{ text: head, start: 0, end: head.length }];
  const bad = verifySplitTextCoverage({ sourceText: head + tail, parts });
  assert.equal(bad.ok, false,'尾部 130 字无人承接，必须判不通过');
  assert.equal(bad.reason, 'text_tail_uncovered');
  assert.ok(String(bad.detail).length > 0);

  // 中间挖掉 100 字没交代：缝隙同样要抓
  const gap = verifySplitTextCoverage({
    sourceText: '丙'.repeat(300),
    parts: [{ text: '丙'.repeat(100), start: 0, end: 100 }, { text: '丙'.repeat(100), start: 200, end: 300 }],
  });
  assert.equal(gap.ok, false);
  assert.equal(gap.reason, 'text_gap_uncovered');

  // 缺的那截如果记在被剔段里（lint/dedupe 真丢了账），不算丢内容
  const explained = verifySplitTextCoverage({ sourceText: head + tail, parts, discards: [{ text: tail }] });
  assert.equal(explained.ok, true, '被剔正文有账就不误拦');
  // 覆盖完整（段界空白允许 trim）
  const ok = verifySplitTextCoverage({ sourceText: head, parts: [{ text: head, start: 0, end: head.length }] });
  assert.equal(ok.ok, true);
});

test('① 无原条 + lint 剔掉一段且无承接：op 标 dead，绝不 done', async () => {
  const { db } = tmpDb();
  const S1 = '2026-11-20 早上八点，小林把机房的温湿度抄进值班表，读数 23 度、41%。';
  // 以「但是」开头：命中 text-lint 的 leading_conjunction，必被代码 lint 打回
  const S2 = '但是机柜后面的灰没擦干净，风扇转速只有 1200 转，这个数字后来没人复核。';
  const S3 = '下午两点，小林把 dsh-memory 的拆分日志翻了一遍，第 42 行写着进度 80%。';
  const source = `${S1}${S2}${S3}`;
  const llm = fakeLlm((src) => ({ cuts: [S2.slice(0, 12), S3.slice(0, 12)] }));
  const store = fakeStore();
  const op = enqueueNoSource(db, source);

  const logs = [];
  const stats = await consumeSplitOps({ db, store, llm, limit: 5, maxChars: 200, lintReview: false, log: (line) => logs.push(line) });

  assert.equal(stats.inserted, 2, 'S1/S3 仍然入库');
  assert.equal(stats.succeeded, 0, '没归档、也没过覆盖闸，不算成功');
  assert.equal(stats.coverageBlocked, 1);
  assert.equal(stats.archived, 0);
  const row = opRow(db, op.key);
  assert.equal(row.status, 'dead', '无原条也要判死，交人工');
  assert.equal(row.last_error, 'coverage_unverified');
  assert.match(logs.join('\n'), /item=\S+ op=\d+ coverage_unverified lint_discard_uncovered/);
  db.close();
});

test('① 无原条 + 覆盖完整：两段落库、op 标 done', async () => {
  const { db } = tmpDb();
  const source = '早上八点小林抄了机房温湿度，读数 23 度、41%。下午两点他又核对了一遍，数字没变。';
  const llm = fakeLlm();
  const store = fakeStore();
  const op = enqueueNoSource(db, source);

  const stats = await consumeSplitOps({ db, store, llm, limit: 5, maxChars: 200, textLint: false, lintReview: false });

  assert.equal(stats.inserted, 2);
  assert.equal(stats.succeeded, 1);
  assert.equal(stats.coverageBlocked, 0);
  assert.equal(stats.archived, 0, '无原条可归档');
  assert.equal(opRow(db, op.key).status, 'done');
  assert.equal(store.saves[0].source.startsWith('sleep:split:queue:'), true, '无原条的来源标记不许冒充记忆 id');
  db.close();
});

/* ─────────────── ② 领空后补扫 ─────────────── */

test('② 第一轮领空后新入队的活，轮末补扫能领到（生产者收工信号）', async () => {
  const { db } = tmpDb();
  const textA = '第一段话讲早上八点抄温湿度。第二段话讲下午两点复核数字。';
  const textB = '第三条讲风扇转速一千二百转。第四条讲机柜后面的灰没擦。';
  enqueueNoSource(db, textA);
  const store = fakeStore();
  const llm = fakeLlm();
  let producerCalls = 0;
  // 生产者：split 领空之后，合并/入库池才把下一条活排进来
  const awaitProducers = async () => {
    producerCalls += 1;
    enqueueNoSource(db, textB, 2);
  };

  const logs = [];
  const stats = await consumeSplitOpsWithRecheck({
    db, store, llm, limit: Infinity, maxChars: 200, textLint: false, lintReview: false,
    awaitProducers, log: (line) => logs.push(line),
  });

  // 补扫真领到活之后会再等一次生产者（那时已经收工，promise 立刻 resolve），扫到 0 才停
  assert.equal(producerCalls, 2, '补扫前后各等一次生产者收工');
  assert.doesNotMatch(logs.join('\n'), /轮末补扫第 3 轮/, '扫不到新活就停，不空转');
  assert.equal(stats.scanned, 2, '补扫把后入队的那条也领到了');
  assert.equal(stats.succeeded, 2);
  assert.equal(stats.inserted, 4);
  assert.equal(db.prepare("SELECT count(*) AS n FROM pending_ops WHERE kind='split_item' AND status='done'").get().n, 2);
  assert.match(logs.join('\n'), /\[split\] 轮末补扫第 1 轮：条 1 落库 1 回队 0/);
  db.close();
});

test('② 补扫扫不到新活就停；maxPasses 硬顶，不死等', async () => {
  const { db } = tmpDb();
  enqueueNoSource(db, '第一条讲早上八点抄温湿度。第二条讲下午两点复核数字。');
  const store = fakeStore();
  const llm = fakeLlm();
  let produced = 0;
  // 生产者每次都想再排一条：没有 maxPasses 就会永动
  const awaitProducers = async () => {
    produced += 1;
    enqueueNoSource(db, `补扫第${produced}条正文讲别的事。再来一句收尾，凑够两段。`, produced + 10);
  };

  const logs = [];
  const stats = await consumeSplitOpsWithRecheck({
    db, store, llm, limit: Infinity, maxChars: 200, textLint: false, lintReview: false,
    awaitProducers, maxPasses: 3, log: (line) => logs.push(line),
  });

  assert.equal(produced, 2, '三轮封顶：最多等两次生产者（没有上限就是永动）');
  assert.equal(stats.scanned, 3, '第一轮 1 条 + 两次补扫各 1 条');
  assert.equal(logs.filter((line) => line.includes('轮末补扫第')).length, 2);

  // 没有 awaitProducers 时行为与 consumeSplitOps 一致（不多扫）
  const { db: db2 } = tmpDb();
  enqueueNoSource(db2, '第一条讲早上八点抄温湿度。第二条讲下午两点复核数字。');
  const only = await consumeSplitOpsWithRecheck({
    db: db2, store: fakeStore(), llm: fakeLlm(), limit: Infinity, maxChars: 200, textLint: false, lintReview: false,
  });
  assert.equal(only.scanned, 1);
  db.close();
  db2.close();
});

/* ─────────────── ③ 分类计数 ─────────────── */

test('③ 分类计数与构造数据一致，且能和 pending_ops 的 done/dead/pending 对上', async () => {
  const { db } = tmpDb();
  // 六条活：落库 2（有原条 1 + 无原条 1）、全判重 1、跳过 1、版本不符 1、覆盖未过 1
  // 正文都带数字坐标，免得被 text-lint 的 too_short 打回（那是另一条链的测试）
  const OK = '早上八点小林抄机房温湿度，读数 23 度、41%。下午两点复核数字没变，风扇 1200 转。';
  const VER_DB = '库里这版正文讲的是别的事，读数 18 度。第二句用来凑两段，转速 900 转。';
  const VER_PAY = 'payload 里这版正文已经过期，读数 25 度。第二句也对不上，转速 1500 转。';
  const DUP = '重复段一讲机房温度 23 度、湿度 41%。重复段二讲风扇转速 1200 转和机柜灰尘。';
  const GATE_S1 = '2026-11-20 早上八点，小林把机房的温湿度抄进值班表，读数 23 度、41%。';
  const GATE_S2 = '但是机柜后面的灰没擦干净，风扇转速只有 1200 转，这个数字后来没人复核。';
  const GATE_S3 = '下午两点，小林把 dsh-memory 的拆分日志翻了一遍，第 42 行写着进度 80%。';

  const metas = [
    { id: 'm_ok', text: OK, status: 'active', tags: [], entities: [], source: 'sleep', ts: '2026-11-01T00:00:00Z', validFrom: '' },
    { id: 'm_ver', text: VER_DB, status: 'active', tags: [], entities: [], source: 'sleep', ts: '2026-11-01T00:00:00Z', validFrom: '' },
    { id: 'm_dup', text: DUP, status: 'active', tags: [], entities: [], source: 'sleep', ts: '2026-11-01T00:00:00Z', validFrom: '' },
    { id: 'm_gate', text: `${GATE_S1}${GATE_S2}${GATE_S3}`, status: 'active', tags: [], entities: [], source: 'sleep', ts: '2026-11-01T00:00:00Z', validFrom: '' },
    // m_dup 的两段正文在库里已经有活跃条目承接 → 判重丢段不算丢内容
    { id: 'old_1', text: DUP.slice(0, DUP.indexOf('。') + 1), status: 'active' },
    { id: 'old_2', text: DUP.slice(DUP.indexOf('。') + 1), status: 'active' },
  ];
  const store = fakeStore({
    metas,
    saveImpl: ({ source }) => (String(source).includes('m_dup') ? { discarded: true } : undefined),
  });
  const llm = fakeLlm((src) => (src.startsWith('2026-11-20') ? { cuts: [GATE_S2.slice(0, 12), GATE_S3.slice(0, 12)] } : twoPartPlan(src)));

  enqueue(db, 'split_item', 'm_ok', { source_id: 'm_ok', text: OK, type: 'world' }, { now: 1 });
  enqueueNoSource(db, OK.replace('小林', '小周'));
  enqueue(db, 'split_item', 'm_ver', { source_id: 'm_ver', text: VER_PAY, type: 'world' }, { now: 1 });
  enqueue(db, 'split_item', 'm_dup', { source_id: 'm_dup', text: DUP, type: 'world' }, { now: 1 });
  // 没有 id、没有正文：终态 done，但算「跳过」，不算成功
  enqueue(db, 'split_item', 'split:emptyop', { text: '', type: 'world' }, { now: 1 });
  enqueue(db, 'split_item', 'm_gate', { source_id: 'm_gate', text: `${GATE_S1}${GATE_S2}${GATE_S3}`, type: 'world' }, { now: 1 });

  const logs = [];
  const stats = await consumeSplitOps({ db, store, llm, limit: Infinity, maxChars: 200, lintReview: false, log: (line) => logs.push(line) });
  const byState = {};
  for (const row of db.prepare("SELECT status, count(*) AS n FROM pending_ops WHERE kind='split_item' GROUP BY status").all()) {
    byState[row.status] = Number(row.n);
  }
  db.close();

  assert.equal(stats.scanned, 6);
  assert.equal(stats.succeeded, 2, `落库的：m_ok 归档 + 无原条那条，实测 ${stats.succeeded}\n${logs.join('\n')}`);
  assert.equal(stats.dedupedOnly, 1, '全判重且库里有承接');
  assert.equal(stats.skipped, 1, `没 id 没正文，实测 ${stats.skipped}\n${logs.join('\n')}`);
  assert.equal(stats.versionMismatch, 1);
  assert.equal(stats.coverageBlocked, 1);
  assert.equal(stats.pendingAfter, 0);
  assert.equal(stats.unavoidable, 0);
  assert.equal(stats.inflateDead, 0);
  assert.equal(stats.failed, 2, '失败只数真失败：版本不符 + 覆盖未过');

  // 库口径：done = 落库 + 全判重 + 跳过；dead = 版本不符 + 覆盖未过
  assert.equal(byState.done, 4);
  assert.equal(byState.dead, 2);
  assert.equal(byState.pending, undefined);

  const sum = stats.succeeded + stats.dedupedOnly + stats.skipped + stats.pendingAfter
    + stats.versionMismatch + stats.coverageBlocked + stats.unavoidable + stats.inflateDead;
  assert.equal(sum, stats.scanned, '每条 op 恰好一类，相加等于 scanned');
  const logLine = logs.find((line) => line.includes('[split] 并发')) || '';
  assert.match(logLine, /条 6 落库 2 全判重 1 跳过 1 回队 0（含不可拆 0） 版本不符 1 覆盖未过 1 判死 0 失败 2 分类合计 6/);
  assert.doesNotMatch(logs.join('\n'), /分类合计 \d+ 与条 \d+ 不符/);
});
