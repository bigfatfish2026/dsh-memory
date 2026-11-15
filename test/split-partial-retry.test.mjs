/**
 * 拆分段落地的原子性（2026-11-15 存储高-3 / 中-5）。
 *
 * 钉三件事：
 *   1) 有子段没落地就不许归档原条、不许标 done（原条留着，下一轮重来）；
 *   2) 重试幂等：上一轮已入库的段按「来源 + 正文」认出来，不再重复入库；
 *   3) commit 自己抛错（比如 markOp / store.typeOf 炸）也只算这条失败，
 *      landed 一定 resolve，整批不许挂在 worker 的 await 上。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { applySchema } from '../scripts/apply-pipeline-schema.mjs';
import { consumeSplitOps } from '../lib/pipeline-split.js';
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

/** 按提示词里的原文切两段（跟 split-concurrency 的假模型同构），每次都返回同样两段。 */
function fakeLlm() {
  const calls = [];
  return {
    calls,
    stream(options) {
      calls.push(options);
      const prompt = String(options?.messages?.[0]?.content?.[0]?.text || '');
      const m = /）：([\s\S]*?)\n\n输出/.exec(prompt);
      const src = m ? m[1] : '';
      // 2026-11-15 切分点协议：只给第二段开头的定位串，正文由代码切
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
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-split-partial-'));
  const dbPath = path.join(dir, 'memory.db');
  new DatabaseSync(dbPath).close();
  applySchema(dbPath, { logger: () => {} });
  return { dbPath, db: new DatabaseSync(dbPath) };
}

/**
 * 假 store：metas 里带 source（幂等靠它认出已入库的段）。
 * failTexts 里的正文只失败一次；archiveFails 控制归档失败次数。
 */
function fakeStore({ failTexts = [], archiveFails = 0 } = {}) {
  const metas = [{ id: SRC_ID, text: SOURCE, status: 'active', tags: [], entities: [], source: 'sleep' }];
  const savedTexts = [];
  const archived = [];
  const failOnce = new Set(failTexts);
  let archiveLeft = archiveFails;
  return {
    metas: () => metas,
    typeOf: () => 'world',
    async save(text, tags, source) {
      if (failOnce.has(text)) {
        failOnce.delete(text);
        throw new Error('inject: 子段入库失败');
      }
      savedTexts.push(text);
      const rec = { id: `new_${savedTexts.length}`, text };
      metas.push({ id: rec.id, text, status: 'active', tags: [], entities: [], source });
      return rec;
    },
    async archive(id) {
      if (archiveLeft > 0) {
        archiveLeft -= 1;
        throw new Error('inject: 归档失败');
      }
      archived.push(id);
      const m = metas.find((x) => x.id === id);
      if (m) m.status = 'archived';
    },
    report: () => ({ savedTexts: [...savedTexts], archived: [...archived] }),
  };
}

function seed(db) {
  enqueue(db, 'split_item', SRC_ID, { source_id: SRC_ID, text: SOURCE, type: 'world' }, { now: 1 });
}

test('存储高-3：一段失败不归档原条，op 回 pending；重试跳过已入库的段，不重复入库', async () => {
  const { dbPath, db } = tmpDb();
  const llm = fakeLlm();
  const store = fakeStore({ failTexts: [B] });
  seed(db);

  const logs = [];
  const first = await consumeSplitOps({ db, store, llm, limit: 5, maxChars: 200, log: (line) => logs.push(line) });
  assert.equal(first.parts, 2);
  assert.equal(first.inserted, 1, '只有第一段落地');
  assert.equal(first.rejected, 1);
  assert.equal(first.archived, 0, '有段没落地，原条不许归档');
  let op = db.prepare("SELECT status, attempts, last_error FROM pending_ops WHERE kind='split_item'").get();
  assert.equal(op.status, 'pending', 'op 回 pending 等下一轮');
  assert.match(String(op.last_error), /^partial_write:1\/2/);
  assert.equal(Number(op.attempts), 1);
  assert.equal(store.metas().find((m) => m.id === SRC_ID).status, 'active', '原条保持 active');
  assert.deepEqual(store.report().savedTexts, [A]);
  assert.match(logs.join('\n'), /\[split\] item=m_src WARN 子段只落地 1\/2（partial_write:1\/2）/);

  // 下一轮：原条还在、op 立刻到期，已入库的 A 段靠来源+正文认出来
  const second = await consumeSplitOps({ db, store, llm, limit: 5, maxChars: 200, log: (line) => logs.push(line) });
  assert.equal(second.inserted, 2, '两段都算落地（A 复用上一轮的 id）');
  assert.equal(second.rejected, 0);
  assert.equal(second.archived, 1, '全部段落地后才归档原条');
  assert.deepEqual(store.report().savedTexts, [A, B], 'A 没有被重复入库');
  assert.equal(store.metas().filter((m) => m.text === A && m.status === 'active' && m.source === `sleep:split:${SRC_ID}`).length, 1);
  op = db.prepare("SELECT status FROM pending_ops WHERE kind='split_item'").get();
  assert.equal(op.status, 'done');
  assert.match(logs.join('\n'), /replayed=1/);
  db.close();
});

test('存储高-3：归档失败也不标 done，原条留 active；下一轮重试成功才收尾', async () => {
  const { db } = tmpDb();
  const llm = fakeLlm();
  const store = fakeStore({ archiveFails: 1 });
  seed(db);

  const first = await consumeSplitOps({ db, store, llm, limit: 5, maxChars: 200 });
  assert.equal(first.inserted, 2);
  assert.equal(first.archived, 0);
  let op = db.prepare("SELECT status, last_error FROM pending_ops WHERE kind='split_item'").get();
  assert.equal(op.status, 'pending', '归档没成，op 不许标 done');
  assert.match(String(op.last_error), /^archive_failed:/);
  assert.equal(store.metas().find((m) => m.id === SRC_ID).status, 'active');

  const second = await consumeSplitOps({ db, store, llm, limit: 5, maxChars: 200 });
  assert.equal(second.archived, 1);
  assert.deepEqual(store.report().savedTexts, [A, B], '重试不再入库第二遍');
  op = db.prepare("SELECT status FROM pending_ops WHERE kind='split_item'").get();
  assert.equal(op.status, 'done');
  db.close();
});

test('存储中-5：commit 抛错（typeOf 炸）也只算这条失败，consumeSplitOps 不挂死', async () => {
  const { db } = tmpDb();
  const llm = fakeLlm();
  const store = fakeStore();
  // payload.type 给了 world，prepare 不会调 typeOf；commit 里算 type 时才炸
  store.typeOf = () => { throw new Error('inject: typeOf 炸了'); };
  seed(db);

  const logs = [];
  let timer = null;
  const guard = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error('consumeSplitOps 挂住了：commit 抛错后 landed 没 resolve')), 3000);
  });
  const stats = await Promise.race([
    consumeSplitOps({ db, store, llm, limit: 5, maxChars: 200, log: (line) => logs.push(line) }),
    guard,
  ]).finally(() => clearTimeout(timer));

  assert.match(logs.join('\n'), /\[split\] commit 抛错/);
  assert.equal(stats.failed, 0, 'commit 抛错不再额外记失败（op 留给租约回收）');
  const op = db.prepare("SELECT status FROM pending_ops WHERE kind='split_item'").get();
  assert.equal(op.status, 'running', '没落地就留在 running，等 reap 计 attempts 捞回');
  assert.equal(store.report().savedTexts.length, 0);
  db.close();
});
