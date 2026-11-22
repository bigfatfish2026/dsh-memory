/**
 * 拆分子段的部分写入隔离（2026-11-22）。
 *
 * 背景：拆分子段一段一笔事务写库。写到第 2 段失败（fetch failed / 覆盖未过 / 写闸未决）时
 * 第 1 段已经落库；以前它是 active，能被默认召回，原条也还是 active，半截残句就进了记忆
 * （实锤：11-22 05:00 那轮 309 个活跃子段里 33 个来自部分写入或覆盖失败的任务）。
 *
 * 本文件用真实 store + 假模型钉住：
 *   1) 第 2 段抛错：已落段是 pending_split、默认召回搜不到、原条 active、op 回 pending；
 *   2) 重试补齐：整组转 active，两段都能召回；
 *   3) 重试耗尽（attempts 到 5）：半成品归档 reason='partial-split'，entityView 看不到；
 *   4) done 前复核：段齐了但父条没归档，不许标 done。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { applySchema } from '../scripts/apply-pipeline-schema.mjs';
import { createStore } from '../lib/store.js';
import { consumeSplitOps } from '../lib/pipeline-split.js';
import { enqueue } from '../lib/ingest-index.js';
import { isPendingSplit } from '../lib/store/split-group.js';
import { scanPartialSplits } from '../scripts/clean-partial-splits.mjs';

const VEC_SO = path.join(import.meta.dirname, '../node_modules/sqlite-vec-linux-arm64/vec0.so');

const A = '小林去机房巡检时看了风扇的转速，顺手记下当天的温湿度读数。';
const B = '小林把机柜上的灰擦干净，又核对了一遍当天的处理经过，最后写进值班记录。';
const SOURCE = `${A}${B}`;
const SRC_ID = 'm_src';

const MEMORY_DDL = `CREATE TABLE memories (
  id TEXT PRIMARY KEY, text TEXT NOT NULL, tags TEXT, source TEXT, ts TEXT, type TEXT,
  entities TEXT, validFrom TEXT, supersedes TEXT, updatedAt TEXT, status TEXT, reason TEXT,
  supersededBy TEXT, validTo TEXT, confidence REAL, extra TEXT, present TEXT, rowSeq INTEGER NOT NULL
);`;

const toolChunks = (args) => [
  { type: 'tool-call-delta', index: 0, id: 'call_1', name: 'submit_parts', argumentsDelta: args.slice(0, 20) },
  { type: 'tool-call-delta', index: 0, argumentsDelta: args.slice(20) },
  { type: 'finish', reason: { kind: 'stop' } },
];

/** 按提示词里的原文切两段：只给第二段开头的定位串，正文由 split() 自己切。 */
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

function makeStore({ seedRows = [] } = {}) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-split-partial-iso-'));
  const dbPath = path.join(dir, 'memory.db');
  const db = new DatabaseSync(dbPath, { allowExtension: true });
  db.loadExtension(VEC_SO);
  db.exec(MEMORY_DDL);
  db.exec('CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);');
  db.exec("INSERT INTO meta (key, value) VALUES ('dim', '4');");
  db.exec('CREATE VIRTUAL TABLE vec_items USING vec0(embedding float[4] distance_metric=cosine);');
  db.exec('CREATE TABLE vec_map (rowid INTEGER PRIMARY KEY, id TEXT);');
  let seq = 1;
  const insertMemory = db.prepare(
    "INSERT INTO memories (id, text, tags, type, status, ts, rowSeq) VALUES (?, ?, '[]', 'world', ?, '2026-11-20T00:00:00Z', ?)",
  );
  const insertVecMap = db.prepare('INSERT INTO vec_map (rowid, id) VALUES (?, ?)');
  const insertVec = db.prepare('INSERT INTO vec_items (rowid, embedding) VALUES (?, ?)');
  for (const row of seedRows) {
    insertMemory.run(row.id, row.text, row.status || 'active', seq);
    insertVecMap.run(seq, row.id);
    insertVec.run(BigInt(seq), new Uint8Array(Float32Array.from(row.vec).buffer));
    seq += 1;
  }
  db.close();
  applySchema(dbPath, { logger: () => {} });

  const cfg = {
    storePath: dir,
    storeLogFile: path.join(dir, 'store.log'),
    notify: false,
    notifyScript: '/bin/true',
    apiUrl: 'http://127.0.0.1:1',
    timeoutMs: 50,
    provider: 'fake',
    model: 'fake',
    topK: 10,
    minScore: 0.1,
    relativeGap: 1,
    // 写入闸关掉：本文件只钉拆分组状态，不测判重（假 embedding 全同会把两段判成近似）
    pipeline: { textLint: false, writeGate: false, lintReview: false },
    embed: async (texts) => texts.map(() => [1, 0, 0, 0]),
    logger: { info() {}, warn() {} },
  };
  return { dir, dbPath, store: createStore(cfg), cfg };
}

/** 真 store 外面套一层注入器：save 按正文失败一次 / archive 假装成功但不改库。 */
function wrapStore(store, { failTexts = [], failAlways = [], ghostTexts = [], swallowArchive = false } = {}) {
  const failOnce = new Set(failTexts);
  const alwaysFail = new Set(failAlways);
  const ghost = new Set(ghostTexts);
  return new Proxy(store, {
    get(target, prop) {
      if (prop === 'save') {
        return async (text, tags, source, meta) => {
          if (alwaysFail.has(text)) throw new Error('fetch failed');
          if (failOnce.has(text)) {
            failOnce.delete(text);
            throw new Error('fetch failed');
          }
          if (ghost.has(text)) {
            // 返回值说成功、库里没有这行：专门用来构造「done 前复核」的缺段场景
            return { id: `ghost_${String(text).length}`, text };
          }
          return target.save(text, tags, source, meta);
        };
      }
      if (prop === 'archive' && swallowArchive) {
        return async () => ({ id: 'swallowed' });
      }
      const value = target[prop];
      return typeof value === 'function' ? value.bind(target) : value;
    },
  });
}

function seed(db) {
  enqueue(db, 'split_item', SRC_ID, { source_id: SRC_ID, text: SOURCE, type: 'world' }, { now: 1 });
}

function rowOf(db, text) {
  return db.prepare('SELECT id, status, reason, source, extra FROM memories WHERE text = ?').get(text);
}

test('第 2 段 fetch failed：已落段 pending_split 不可召回，原条 active，op 回 pending', async () => {
  const { dbPath, store } = makeStore({ seedRows: [{ id: SRC_ID, text: SOURCE, vec: [1, 0, 0, 0] }] });
  const db = new DatabaseSync(dbPath);
  seed(db);
  const logs = [];
  const wrapped = wrapStore(store, { failTexts: [B] });

  const stats = await consumeSplitOps({ db, store: wrapped, llm: fakeLlm(), limit: 5, maxChars: 200, log: (line) => logs.push(line) });
  assert.equal(stats.inserted, 1, '只有第一段落地');
  assert.equal(stats.rejected, 1);
  assert.equal(stats.archived, 0, '有段没落地，原条不许归档');

  const op = db.prepare("SELECT status, attempts, last_error FROM pending_ops WHERE kind='split_item'").get();
  assert.equal(op.status, 'pending', 'op 回 pending 等下一轮');
  assert.match(String(op.last_error), /^partial_write:1\/2/);
  assert.equal(Number(op.attempts), 1);

  const landed = rowOf(db, A);
  assert.equal(landed.status, 'pending_split', '已落段必须是中间态，不许直接 active');
  const mark = JSON.parse(landed.extra).splitGroup;
  assert.deepEqual(mark, { source: `sleep:split:${SRC_ID}`, expected: 2, index: 0 }, '组键/expected/下标要落在行上');
  assert.equal(db.prepare('SELECT status FROM memories WHERE id = ?').get(SRC_ID).status, 'active', '原条保持 active');

  // 默认召回：中间态硬剔除，语义/关键词通道都搜不到它
  const hits = await store.recall(A.slice(0, 12), 10);
  assert.ok(!hits.some((h) => h.text === A), `默认召回不许出现 pending_split 残段，实际：${JSON.stringify(hits.map((h) => h.text))}`);
  assert.equal(isPendingSplit({ status: 'pending_split' }), true);
  assert.match(logs.join('\n'), /\[split\] item=m_src WARN 子段只落地 1\/2（partial_write:1\/2）/);
  db.close();
});

test('重试补齐：整组转 active，两段都能召回，父条归档、op done', async () => {
  const { dbPath, store } = makeStore({ seedRows: [{ id: SRC_ID, text: SOURCE, vec: [1, 0, 0, 0] }] });
  const db = new DatabaseSync(dbPath);
  seed(db);
  const logs = [];
  const wrapped = wrapStore(store, { failTexts: [B] });

  await consumeSplitOps({ db, store: wrapped, llm: fakeLlm(), limit: 5, maxChars: 200, log: (line) => logs.push(line) });
  const second = await consumeSplitOps({ db, store: wrapped, llm: fakeLlm(), limit: 5, maxChars: 200, log: (line) => logs.push(line) });

  assert.equal(second.inserted, 2, '两段都算落地（第一段复用上一轮的 id）');
  assert.equal(second.rejected, 0);
  assert.equal(second.archived, 1, '整组齐了才归档原条');
  assert.match(logs.join('\n'), /组 sleep:split:m_src 齐 2\/2，激活 2 段/);

  const a = rowOf(db, A);
  const b = rowOf(db, B);
  assert.equal(a.status, 'active');
  assert.equal(b.status, 'active');
  assert.equal(db.prepare('SELECT status FROM memories WHERE id = ?').get(SRC_ID).status, 'archived');
  assert.equal(db.prepare("SELECT status FROM pending_ops WHERE kind='split_item'").get().status, 'done');

  const hitsA = await store.recall(A.slice(0, 12), 10);
  const hitsB = await store.recall(B.slice(0, 12), 10);
  assert.ok(hitsA.some((h) => h.text === A), '激活后第一段能召回');
  assert.ok(hitsB.some((h) => h.text === B), '激活后第二段能召回');
  db.close();
});

test('重试耗尽：半成品归档 partial-split，entityView 看不到，op 判 dead', async () => {
  const { dbPath, store } = makeStore({ seedRows: [{ id: SRC_ID, text: SOURCE, vec: [1, 0, 0, 0] }] });
  const db = new DatabaseSync(dbPath);
  seed(db);
  // attempts 已经 3：再失败两次就撞 MAX_ATTEMPTS=5
  db.prepare("UPDATE pending_ops SET attempts = 3 WHERE kind = 'split_item'").run();
  const logs = [];
  const wrapped = wrapStore(store, { failAlways: [B] });

  const first = await consumeSplitOps({ db, store: wrapped, llm: fakeLlm(), limit: 5, maxChars: 200, log: (line) => logs.push(line) });
  assert.equal(first.archived, 0);
  let op = db.prepare("SELECT status, attempts FROM pending_ops WHERE kind='split_item'").get();
  assert.equal(op.status, 'pending');
  assert.equal(Number(op.attempts), 4);
  assert.equal(rowOf(db, A).status, 'pending_split');

  const second = await consumeSplitOps({ db, store: wrapped, llm: fakeLlm(), limit: 5, maxChars: 200, log: (line) => logs.push(line) });
  assert.equal(second.archived, 0, '半成品不算成功归档（父条没动）');
  op = db.prepare("SELECT status, attempts, last_error FROM pending_ops WHERE kind='split_item'").get();
  assert.equal(op.status, 'dead', 'attempts 到 5 由 ledger 判 dead');
  assert.equal(Number(op.attempts), 5);

  const landed = rowOf(db, A);
  assert.equal(landed.status, 'archived', '重试耗尽后半成品必须归档');
  assert.equal(landed.reason, 'partial-split');
  assert.equal(JSON.parse(landed.extra).splitGroup.source, `sleep:split:${SRC_ID}`, '归档后组键保留，清理脚本能二次识别');
  assert.equal(db.prepare('SELECT status FROM memories WHERE id = ?').get(SRC_ID).status, 'active', '原条没被半成品带着归档');
  assert.match(logs.join('\n'), /attempts 耗尽|半成品 1 段已归档/);

  const view = store.entityView('小林', 10);
  assert.ok(!view.items.some((item) => item.text === A), 'memory_entity 看不到半成品');

  // 清理脚本再扫：已归档的 partial-split 不再算残片
  const report = scanPartialSplits(db);
  assert.equal(report.groups.length, 0);
  db.close();
});

test('done 前复核：段齐了但父条没归档，不标 done 回 pending', async () => {
  const { dbPath, store } = makeStore({ seedRows: [{ id: SRC_ID, text: SOURCE, vec: [1, 0, 0, 0] }] });
  const db = new DatabaseSync(dbPath);
  seed(db);
  const logs = [];
  // archive 假装成功但不改库：段全落、组完整，唯独父条还 active
  const wrapped = wrapStore(store, { swallowArchive: true });

  const stats = await consumeSplitOps({ db, store: wrapped, llm: fakeLlm(), limit: 5, maxChars: 200, log: (line) => logs.push(line) });
  assert.equal(stats.inserted, 2);
  assert.equal(stats.succeeded, 0, '父条没归档，不许算成功');
  const op = db.prepare("SELECT status, last_error FROM pending_ops WHERE kind='split_item'").get();
  assert.equal(op.status, 'pending', 'done 前复核没过，打回 pending');
  assert.match(String(op.last_error), /^done_parent_not_archived:active/);
  assert.match(logs.join('\n'), /WARN done 前复核没过（done_parent_not_archived:active）/);
  assert.equal(rowOf(db, A).status, 'active', '组齐了，段已经激活');
  assert.equal(db.prepare('SELECT status FROM memories WHERE id = ?').get(SRC_ID).status, 'active');
  db.close();
});

test('清理脚本 dry-run：只列残片、不改库；--apply 才归档', async () => {
  const { dbPath } = makeStore({ seedRows: [{ id: 'm_p1', text: '旧残片一', vec: [1, 0, 0, 0] }] });
  const db = new DatabaseSync(dbPath);
  // 组 A：一条中间态 + 一条老 active，expected=3 只落了 2 位，op 还 pending → 残片
  db.prepare(
    "INSERT INTO memories (id, text, source, status, ts, extra, rowSeq) VALUES (?, ?, ?, ?, ?, ?, ?)",
  ).run('p1_a', '残片甲', 'sleep:split:m_p1', 'pending_split', '2026-11-20T00:00:00Z',
    JSON.stringify({ splitGroup: { source: 'sleep:split:m_p1', expected: 3, index: 0 } }), 90);
  db.prepare(
    "INSERT INTO memories (id, text, source, status, ts, extra, rowSeq) VALUES (?, ?, ?, ?, ?, ?, ?)",
  ).run('p1_b', '残片乙', 'sleep:split:m_p1', 'active', '2026-11-20T00:00:00Z',
    JSON.stringify({ splitGroup: { source: 'sleep:split:m_p1', expected: 3, index: 1 } }), 91);
  // 组 B：完整（2/2，全 active），不该出现在清单里
  db.prepare(
    "INSERT INTO memories (id, text, source, status, ts, extra, rowSeq) VALUES (?, ?, ?, ?, ?, ?, ?)",
  ).run('ok_a', '完整甲', 'sleep:split:m_ok', 'active', '2026-11-20T00:00:00Z',
    JSON.stringify({ splitGroup: { source: 'sleep:split:m_ok', expected: 2, index: 0 } }), 92);
  db.prepare(
    "INSERT INTO memories (id, text, source, status, ts, extra, rowSeq) VALUES (?, ?, ?, ?, ?, ?, ?)",
  ).run('ok_b', '完整乙', 'sleep:split:m_ok', 'active', '2026-11-20T00:00:00Z',
    JSON.stringify({ splitGroup: { source: 'sleep:split:m_ok', expected: 2, index: 1 } }), 93);
  enqueue(db, 'split_item', 'm_p1', { source_id: 'm_p1', text: '残片甲残片乙', type: 'world' }, { now: 1 });
  db.prepare("UPDATE pending_ops SET last_error = 'partial_write:2/3' WHERE kind = 'split_item'").run();

  const before = db.prepare('SELECT status, reason FROM memories ORDER BY rowSeq').all().map((r) => `${r.status}/${r.reason ?? ''}`);
  const report = scanPartialSplits(db, { sample: 5 });
  assert.equal(report.groups.length, 1, '只有不完整的那组进清单');
  assert.equal(report.groups[0].key, 'sleep:split:m_p1');
  assert.equal(report.groups[0].expected, 3);
  assert.deepEqual(report.groups[0].missing, [2], '缺第 3 段（index=2）');
  assert.deepEqual(report.rows.map((r) => r.id).sort(), ['p1_a', 'p1_b']);
  const after = db.prepare('SELECT status, reason FROM memories ORDER BY rowSeq').all().map((r) => `${r.status}/${r.reason ?? ''}`);
  assert.deepEqual(after, before, 'dry-run 一个字节都不许改');

  const { applyPartialSplitCleanup } = await import('../scripts/clean-partial-splits.mjs');
  const changed = applyPartialSplitCleanup(db, report.rows);
  assert.equal(changed, 2);
  const archived = db.prepare('SELECT id, status, reason FROM memories WHERE id IN (?, ?)').all('p1_a', 'p1_b');
  for (const row of archived) {
    assert.equal(row.status, 'archived');
    assert.equal(row.reason, 'partial-split');
  }
  assert.equal(db.prepare('SELECT status FROM memories WHERE id = ?').get('ok_a').status, 'active', '完整组不动');
  db.close();
});
