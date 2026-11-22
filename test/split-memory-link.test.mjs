/**
 * 拆分归档补 memory_link 的 curate 边（t-mwp0uk01-98eh）。
 *
 * 钉四件事：
 *   ① 拆分归档时每条子段补一条 child→原条 的 supersedes 边，origin='curate'；
 *   ② 写边必须在归档之前（写入口要求 dst 还活着），原条归档后 supersededBy 与边都在；
 *   ③ 重复写幂等（uq_memory_link_edge），同级不重复插；
 *   ④ 与 queue 边不冲突：同 dst 的 curate 边被 queue 升级成一行 queue 边，别的 dst 的 curate 边不受影响。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { applySchema } from '../scripts/apply-pipeline-schema.mjs';
import { applyCurateSchema } from '../scripts/apply-curate-schema.mjs';
import { createStore } from '../lib/store.js';
import { consumeSplitOps } from '../lib/pipeline-split.js';
import { hasMemoryLinkTable, writeSupersedesLink } from '../lib/memory-link.js';
import { enqueue } from '../lib/ingest-index.js';

const VEC_SO = path.join(import.meta.dirname, '../node_modules/sqlite-vec-linux-arm64/vec0.so');

const MEMORY_DDL = `CREATE TABLE memories (
  id TEXT PRIMARY KEY, text TEXT NOT NULL, tags TEXT, source TEXT, ts TEXT, type TEXT,
  entities TEXT, validFrom TEXT, supersedes TEXT, updatedAt TEXT, status TEXT, reason TEXT,
  supersededBy TEXT, validTo TEXT, confidence REAL, extra TEXT, present TEXT, rowSeq INTEGER NOT NULL
);`;

/** 真 store + 真 curate 三表（memory_link 一定要在，不然测的就是「表不在」那条路）。 */
function makeEnv({ withCurateSchema = true } = {}) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-split-link-'));
  const dbPath = path.join(dir, 'memory.db');
  const db = new DatabaseSync(dbPath, { allowExtension: true });
  db.loadExtension(VEC_SO);
  db.exec(MEMORY_DDL);
  db.exec('CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);');
  db.exec("INSERT INTO meta (key, value) VALUES ('dim', '4');");
  db.exec('CREATE TABLE entities (id INTEGER PRIMARY KEY, name TEXT);');
  db.exec('CREATE TABLE memory_entity (memory_id TEXT, entity_id INTEGER, role TEXT);');
  db.exec('CREATE VIRTUAL TABLE vec_items USING vec0(embedding float[4] distance_metric=cosine);');
  db.exec('CREATE TABLE vec_map (rowid INTEGER PRIMARY KEY, id TEXT);');
  db.close();
  applySchema(dbPath, { logger: () => {} });
  if (withCurateSchema) applyCurateSchema(dbPath, { logger: { info() {}, warn() {} } });
  const cfg = {
    storePath: dir,
    storeLogFile: path.join(dir, 'store.log'),
    apiUrl: 'http://127.0.0.1:1',
    timeoutMs: 50,
    provider: 'fake',
    model: 'fake',
    embed: async (texts) => texts.map(() => [1, 0, 0, 0]),
    pipeline: { textLint: false, writeGate: false },
    logger: { info() {}, warn() {} },
  };
  return { store: createStore(cfg), dbPath, dir };
}

function readRows(dbPath, sql, ...params) {
  const db = new DatabaseSync(dbPath, { readOnly: true });
  try {
    return db.prepare(sql).all(...params);
  } finally {
    db.close();
  }
}

function fakeLlm() {
  return {
    stream(options) {
      const prompt = String(options?.messages?.[0]?.content?.[0]?.text || '');
      const m = /）：([\s\S]*?)\n\n输出/.exec(prompt);
      const src = m ? m[1] : '';
      const cut = src.indexOf('。') + 1;
      const plan = { cuts: src && cut > 0 && cut < src.length ? [src.slice(cut, cut + 10)] : [] };
      return (async function* chunks() {
        yield { type: 'text-delta', text: JSON.stringify(plan) };
        yield { type: 'finish', reason: { kind: 'stop' } };
      })();
    },
  };
}

const SOURCE = '2026-11-20 早上八点，小林把机房的温湿度抄进值班表，读数 23 度、41%。下午两点复核数字没变，风扇 1200 转。';

/** 跑一次拆分：原条落库 → 入队 → 消费。返回原条 id、子段 id、日志。 */
async function splitOnce(env) {
  const db = env.store.db();
  const src = await env.store.save(SOURCE, [], 'test', { type: 'world', validFrom: '2026-11-01' });
  enqueue(db, 'split_item', String(src.id), { source_id: src.id, text: SOURCE, type: 'world' }, { now: 1 });
  const logs = [];
  const stats = await consumeSplitOps({ db, store: env.store, llm: fakeLlm(), limit: 5, maxChars: 200, lintReview: false, log: (line) => logs.push(line) });
  return { db, srcId: String(src.id), stats, logs };
}

test('① 拆分归档写 curate 边：每条子段一条 child→原条', async () => {
  const env = makeEnv();
  const { db, srcId, stats, logs } = await splitOnce(env);
  db.close();

  assert.equal(stats.archived, 1);
  assert.equal(stats.inserted, 2);
  const edges = readRows(env.dbPath, "SELECT src, dst, rel, origin FROM memory_link WHERE dst = ? ORDER BY src", srcId);
  assert.equal(edges.length, 2, '两段各一条边');
  assert.deepEqual(edges.map((e) => e.rel), ['supersedes', 'supersedes']);
  assert.deepEqual(edges.map((e) => e.origin), ['curate', 'curate'], '整理轮这条链写 curate');
  const children = readRows(env.dbPath, 'SELECT id, supersededBy FROM memories WHERE source = ? ORDER BY id', `sleep:split:${srcId}`)
    .map((r) => String(r.id));
  assert.deepEqual(edges.map((e) => String(e.src)).sort(), children.slice().sort(), '边只连真正落库的子段');
  const parent = readRows(env.dbPath, 'SELECT status, supersededBy FROM memories WHERE id = ?', srcId)[0];
  assert.equal(parent.status, 'archived');
  assert.ok(parent.supersededBy, 'supersededBy 照旧写');
  assert.match(logs.join('\n'), /\[split\] item=\S+ memory_link curate 边 2 条（子段→原条）/);
});

test('② 同一条边重复写幂等：edge_exists，唯一键只留一行', async () => {
  const env = makeEnv();
  const db = env.store.db();
  const { srcId } = await splitOnce(env);
  const children = readRows(env.dbPath, 'SELECT id FROM memories WHERE source = ? ORDER BY id', `sleep:split:${srcId}`).map((r) => String(r.id));

  // 原条已经归档：再拿它当 dst 会被 target_not_active 挡住（写入口的护栏，不是崩溃）
  const afterArchive = writeSupersedesLink(db, { src: 'x', dst: srcId, origin: 'curate' });
  assert.equal(afterArchive.ok, false);
  assert.equal(afterArchive.reason, 'target_not_active');

  // 活着的 dst 上重复写同一条 curate 边：第二次判 edge_exists，不插第二行
  const alive = await env.store.save('第五条讲机柜灰尘没擦，风扇 1200 转。', [], 'test', { type: 'world' });
  assert.equal(writeSupersedesLink(db, { src: children[0], dst: String(alive.id), origin: 'curate' }).ok, true);
  const dup = writeSupersedesLink(db, { src: children[0], dst: String(alive.id), origin: 'curate' });
  assert.equal(dup.ok, false);
  assert.equal(dup.reason, 'edge_exists');
  db.close();
  assert.equal(readRows(env.dbPath, "SELECT count(*) AS n FROM memory_link WHERE src = ? AND dst = ? AND rel='supersedes'", children[0], String(alive.id)).at(0).n, 1);
});

test('③ 与 queue 边不冲突：queue 能顶掉拆分写的 curate 边，反过来 curate 顶不动 queue', async () => {
  const env = makeEnv();
  const db = env.store.db();
  const { srcId } = await splitOnce(env);
  const children = readRows(env.dbPath, 'SELECT id FROM memories WHERE source = ? ORDER BY id', `sleep:split:${srcId}`).map((r) => String(r.id));
  assert.equal(children.length, 2);

  // 队列后来判出「子段 1 取代 T」：queue 优先级高于 curate，顶掉拆分写的那条 curate 边
  const t = await env.store.save('第三条讲风扇转速 1200 转和机柜灰尘。', [], 'test', { type: 'world' });
  const up = writeSupersedesLink(db, { src: children[0], dst: String(t.id), origin: 'queue' });
  assert.equal(up.ok, true);
  const child0Edges = readRows(env.dbPath, "SELECT dst, origin FROM memory_link WHERE src = ? AND rel='supersedes'", children[0]);
  assert.equal(child0Edges.length, 1, '同一 src 只留一行');
  assert.equal(child0Edges[0].origin, 'queue');
  assert.equal(String(child0Edges[0].dst), String(t.id));

  // 反方向：queue 边在时再写 curate 边，必须被挡住，queue 边不许被顶掉
  const another = await env.store.save('第四条讲机房巡检 23 度、41% 的记录。', [], 'test', { type: 'world' });
  const blocked = writeSupersedesLink(db, { src: children[0], dst: String(another.id), origin: 'curate' });
  assert.equal(blocked.ok, false);
  assert.equal(blocked.reason, 'queue_edge_kept');
  const still = readRows(env.dbPath, "SELECT dst, origin FROM memory_link WHERE src = ? AND rel='supersedes'", children[0]);
  assert.equal(still.length, 1);
  assert.equal(still[0].origin, 'queue');
  assert.equal(String(still[0].dst), String(t.id));

  // 另一段的 curate 边没被这一串动过
  const child1Edges = readRows(env.dbPath, "SELECT dst, origin FROM memory_link WHERE src = ? AND rel='supersedes'", children[1]);
  assert.equal(child1Edges.length, 1);
  assert.equal(child1Edges[0].origin, 'curate');
  assert.equal(String(child1Edges[0].dst), srcId);
  db.close();
});

test('④ 老库没有 memory_link 表：拆分照常落库，只提醒一次', async () => {
  const env = makeEnv({ withCurateSchema: false });
  const db = env.store.db();
  assert.equal(hasMemoryLinkTable(db), false);
  const { stats, logs } = await splitOnce(env);
  db.close();
  assert.equal(stats.succeeded, 1);
  assert.equal(stats.inserted, 2);
  assert.equal(logs.filter((line) => line.includes('memory_link 表不在')).length, 1, '整个一轮只提醒一次');
  assert.doesNotMatch(logs.join('\n'), /memory_link curate 边/);
});

test('⑤ 归档失败回滚本批 curate 边；重试成功后才留边', async () => {
  const env = makeEnv();
  const db = env.store.db();
  const orig = env.store.archive.bind(env.store);
  let archiveFails = 1;
  // 归档抛错：边已经写了（写边在 archive 之前），必须随失败撤掉，不能留「子段取代了还活着的原条」的脏边
  env.store.archive = async (id, reason, opts) => {
    if (archiveFails > 0) {
      archiveFails -= 1;
      throw new Error('inject: 归档失败');
    }
    return orig(id, reason, opts);
  };
  const src = await env.store.save(SOURCE, [], 'test', { type: 'world', validFrom: '2026-11-01' });
  const srcId = String(src.id);
  enqueue(db, 'split_item', srcId, { source_id: src.id, text: SOURCE, type: 'world' }, { now: 1 });

  const logs = [];
  const first = await consumeSplitOps({ db, store: env.store, llm: fakeLlm(), limit: 5, maxChars: 200, lintReview: false, log: (line) => logs.push(line) });
  assert.equal(first.inserted, 2);
  assert.equal(first.archived, 0);
  assert.equal(readRows(env.dbPath, 'SELECT count(*) AS n FROM memory_link WHERE dst = ?', srcId).at(0).n, 0, '归档失败后本批写的边全撤掉');
  assert.match(logs.join('\n'), /\[split\] item=\S+ 归档失败，回滚 memory_link curate 边 2 条/);
  const op = db.prepare("SELECT status, attempts FROM pending_ops WHERE kind='split_item'").get();
  assert.equal(op.status, 'pending', '归档没成，op 回 pending 等下一轮');
  assert.equal(Number(op.attempts), 1);
  assert.notEqual(readRows(env.dbPath, 'SELECT status FROM memories WHERE id = ?', srcId).at(0).status, 'archived', '原条没被归档（status 列为 NULL/active 都算 active）');

  const second = await consumeSplitOps({ db, store: env.store, llm: fakeLlm(), limit: 5, maxChars: 200, lintReview: false, log: (line) => logs.push(line) });
  assert.equal(second.archived, 1, '重试成功才归档');
  assert.equal(readRows(env.dbPath, "SELECT count(*) AS n FROM memory_link WHERE dst = ? AND origin='curate'", srcId).at(0).n, 2, '重试成功后边正常写');
  db.close();
});
