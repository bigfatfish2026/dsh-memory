/**
 * 轮末补扫的两道护栏（2026-11-20，t-mwp26z05-qfrz）。
 *
 * 钉两件事：
 *   1) 生产者（ingest/merge 两路 drain）永不 settle 时，等生产者有超时，超时后打 WARN 并收尾，
 *      不许把整轮 sleep 挂死；
 *   2) 本轮失败/回 pending 的 op 不许在补扫里被原地重领（next_try_at=now 会让它立刻又到期），
 *      跨 pass 传同一份 exclude 集合。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { applySchema } from '../scripts/apply-pipeline-schema.mjs';
import { consumeSplitOpsWithRecheck } from '../lib/pipeline-split.js';
import { enqueue } from '../lib/ingest-index.js';

const A = '小林去机房巡检时看了风扇的转速，顺手记下当天的温湿度读数。';
const B = '小林把机柜上的灰擦干净，又核对了一遍当天的处理经过，最后写进值班记录。';
const SOURCE = `${A}${B}`;
const SRC_ID = 'm_src';

const toolChunks = (args) => [
  { type: 'tool-call-delta', index: 0, id: 'call_1', name: 'submit_parts', argumentsDelta: args.slice(0, 20) },
  { type: 'tool-call-delta', index: 0, argumentsDelta: args.slice(20) },
  { type: 'finish', reason: { kind: 'stop' } },
];

/** 按提示词里的原文切两段，每次都返回同样两段。 */
function fakeLlm() {
  return {
    stream(options) {
      const prompt = String(options?.messages?.[0]?.content?.[0]?.text || '');
      const m = /）：([\s\S]*?)\n\n输出/.exec(prompt);
      const src = m ? m[1] : '';
      const cut = src.indexOf('。') + 1;
      const cuts = src && cut > 0 && cut < src.length ? [src.slice(cut, cut + 8)] : [];
      const args = JSON.stringify({ cuts, unavoidable: false, reason: '' });
      return (async function* streamChunks() {
        yield* toolChunks(args);
      })();
    },
  };
}

function tmpDb() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-split-recheck-'));
  const dbPath = path.join(dir, 'memory.db');
  new DatabaseSync(dbPath).close();
  applySchema(dbPath, { logger: () => {} });
  return { dbPath, db: new DatabaseSync(dbPath) };
}

/** 假 store：archiveFails 控制归档失败次数；其余跟 split-partial-retry 的同构。 */
function fakeStore({ archiveFails = 0 } = {}) {
  const metas = [{ id: SRC_ID, text: SOURCE, status: 'active', tags: [], entities: [], source: 'sleep' }];
  let archiveLeft = archiveFails;
  let seq = 0;
  return {
    metas: () => metas,
    typeOf: () => 'world',
    async save(text, tags, source) {
      seq += 1;
      const rec = { id: `new_${seq}`, text };
      metas.push({ id: rec.id, text, status: 'active', tags, entities: [], source });
      return rec;
    },
    async archive(id) {
      if (archiveLeft > 0) {
        archiveLeft -= 1;
        throw new Error('inject: 归档失败');
      }
      const m = metas.find((x) => x.id === id);
      if (m) m.status = 'archived';
    },
  };
}

test('① 生产者永不 settle：超时后打 WARN 收尾，整轮不挂死', async () => {
  const { db } = tmpDb();
  enqueue(db, 'split_item', SRC_ID, { source_id: SRC_ID, text: SOURCE, type: 'world' }, { now: 1 });
  const logs = [];
  const started = Date.now();
  // 生产者永不 resolve：模拟某路 drain 的模型调用不返回
  const stuck = () => new Promise(() => {});
  let timer = null;
  const guard = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error('consumeSplitOpsWithRecheck 挂住了：等生产者没超时')), 3000);
  });
  const stats = await Promise.race([
    consumeSplitOpsWithRecheck({
      db, store: fakeStore(), llm: fakeLlm(), limit: Infinity, maxChars: 200,
      textLint: false, lintReview: false,
      awaitProducers: stuck, producerTimeoutMs: 30,
      log: (line) => logs.push(line),
    }),
    guard,
  ]).finally(() => clearTimeout(timer));

  assert.equal(stats.scanned, 1, '第一轮照常扫完');
  assert.equal(stats.succeeded, 1);
  assert.match(logs.join('\n'), /\[split\] 轮末补扫等待生产者超时，跳过补扫（30ms）/);
  assert.doesNotMatch(logs.join('\n'), /轮末补扫第 1 轮/, '超时后不再补扫');
  assert.ok(Date.now() - started < 3000, '超时后立刻收尾，不等生产者');
  db.close();
});

test('② 本轮失败的 op 在补扫里不再被领：attempts 不涨，新活照常落库', async () => {
  const { db } = tmpDb();
  enqueue(db, 'split_item', SRC_ID, { source_id: SRC_ID, text: SOURCE, type: 'world' }, { now: 1 });
  const store = fakeStore({ archiveFails: 1 });
  const OTHER = '第三条讲风扇转速一千二百转。第四条讲机柜后面的灰没擦。';
  let produced = 0;
  // 生产者收工时再排一条无原条的活：拿到补扫轮里（只排一次，第二次收工信号是空转）
  const awaitProducers = () => {
    produced += 1;
    if (produced === 1) enqueue(db, 'split_item', 'other_key', { text: OTHER, type: 'world' }, { now: 2 });
  };

  const logs = [];
  const stats = await consumeSplitOpsWithRecheck({
    db, store, llm: fakeLlm(), limit: Infinity, maxChars: 200,
    textLint: false, lintReview: false,
    awaitProducers, producerTimeoutMs: 1000, log: (line) => logs.push(line),
  });

  assert.equal(stats.scanned, 2, '第一轮的失败条 + 补扫的新条，没有第三条（失败条没被重领）');
  assert.equal(produced, 2, '补扫领到新活后再等一次生产者收工，扫到 0 才停');
  const failed = db.prepare("SELECT status, attempts FROM pending_ops WHERE kind='split_item' AND dedupe_key = ?").get(SRC_ID);
  assert.equal(failed.status, 'pending', '归档失败仍回 pending 等下一轮');
  assert.equal(Number(failed.attempts), 1, '同一轮里没被重领，attempts 只涨一次');
  const other = db.prepare("SELECT status FROM pending_ops WHERE kind='split_item' AND dedupe_key = 'other_key'").get();
  assert.equal(other.status, 'done', '补扫的新活正常收尾');
  assert.match(logs.join('\n'), /轮末补扫第 1 轮：条 1/);
  db.close();
});
