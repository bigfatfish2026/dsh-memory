/**
 * 活跃快照（2026-11-14，阶段 4）。
 *
 * 钉三件事：
 *   1) 同一版本内 get 只 load 一次，返回同一个对象（一轮一次）；
 *   2) invalidate / 版本变化一定重取（写后不许拿旧视图）；
 *   3) 接进 store 后：save/update/archive 之后的快照能读到新数据。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { createActiveSnapshot } from '../lib/active-snapshot.js';
import { createStore } from '../lib/store.js';

const VEC_SO = path.join(import.meta.dirname, '../node_modules/sqlite-vec-linux-arm64/vec0.so');
const MEMORY_DDL = `CREATE TABLE memories (
  id TEXT PRIMARY KEY, text TEXT NOT NULL, tags TEXT, source TEXT, ts TEXT, type TEXT,
  entities TEXT, validFrom TEXT, supersedes TEXT, updatedAt TEXT, status TEXT, reason TEXT,
  supersededBy TEXT, validTo TEXT, confidence REAL, extra TEXT, present TEXT, rowSeq INTEGER NOT NULL
);`;
const ENTITY_DDL = `
CREATE TABLE entities (id INTEGER PRIMARY KEY, norm TEXT NOT NULL, display TEXT NOT NULL,
  etype TEXT NOT NULL DEFAULT 'unknown', aliases_note TEXT NOT NULL DEFAULT '',
  attrs TEXT NOT NULL DEFAULT '{}', first_seen TEXT NOT NULL DEFAULT '',
  last_seen TEXT NOT NULL DEFAULT '', mem_count INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'active', UNIQUE (norm));
CREATE TABLE memory_entity (memory_id TEXT NOT NULL, entity_id INTEGER NOT NULL, role TEXT NOT NULL DEFAULT 'about',
  ts TEXT NOT NULL DEFAULT '', status TEXT NOT NULL DEFAULT 'active',
  attrs TEXT NOT NULL DEFAULT '{}', version INTEGER NOT NULL DEFAULT 0,
  prev_memory_id TEXT NOT NULL DEFAULT '', PRIMARY KEY (memory_id, entity_id, role)) WITHOUT ROWID;
CREATE TABLE entity_surface (surface TEXT PRIMARY KEY, norm TEXT NOT NULL, entity_id INTEGER NOT NULL,
  source TEXT NOT NULL CHECK (source IN ('migrate','gate','coarse','extract','model','manual','text')),
  count INTEGER NOT NULL DEFAULT 0, first_seen TEXT NOT NULL DEFAULT '',
  last_seen TEXT NOT NULL DEFAULT '', flag TEXT NOT NULL DEFAULT '', UNIQUE (norm, surface)) WITHOUT ROWID;
CREATE TABLE entity_issue (memory_id TEXT NOT NULL DEFAULT '', entity_norm TEXT NOT NULL, raw_name TEXT NOT NULL DEFAULT '',
  reason TEXT NOT NULL, detail TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL,
  PRIMARY KEY (memory_id, entity_norm, reason)) WITHOUT ROWID;
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);`;

function makeStore() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-snap-'));
  const dbPath = path.join(dir, 'memory.db');
  const db = new DatabaseSync(dbPath, { allowExtension: true });
  db.loadExtension(VEC_SO);
  db.exec(MEMORY_DDL);
  db.exec(ENTITY_DDL);
  db.exec('CREATE VIRTUAL TABLE vec_items USING vec0(embedding float[4] distance_metric=cosine);');
  db.exec('CREATE TABLE vec_map (rowid INTEGER PRIMARY KEY, id TEXT);');
  db.exec("INSERT INTO meta(key,value) VALUES ('dim','4');");
  db.close();
  const cfg = {
    storePath: dir,
    storeLogFile: path.join(dir, 'store.log'),
    entityQueue: { enqueue: false },
    splitMaxChars: 200,
    embed: async (texts) => texts.map(() => [1, 0, 0, 0]),
    logger: { info() {}, warn() {} },
    pipeline: { writeGate: false },
  };
  return { store: createStore(cfg), dir };
}

test('原语：同版本只 load 一次、invalidate 强制重取、版本变化自动重取', () => {
  let loads = 0;
  let v = 1;
  const snap = createActiveSnapshot({
    load: () => { loads += 1; return { list: [{ id: `m_${loads}` }], vecOf: new Map() }; },
    version: () => v,
  });
  const a = snap.get();
  const b = snap.get();
  assert.equal(loads, 1, '同版本只 load 一次');
  assert.equal(a, b, '同一轮拿到同一个对象');
  assert.equal(a.byId.get(a.list[0].id), a.list[0], 'byId 指向同一批对象');

  v += 1;
  const c = snap.get();
  assert.equal(loads, 2, '版本变了要重取');
  assert.notEqual(c, a);

  snap.invalidate();
  snap.get();
  assert.equal(loads, 3, '手动失效也要重取');
});

test('store 集成：save 之后快照能读到新条，版本号递增', async () => {
  const { store } = makeStore();
  const before = store.snapshot();
  assert.equal(before.list.length, 0);
  const v0 = store.snapshotVersion();

  const rec = await store.save('树莓派温度 45 度', ['infra'], 'agent', { type: 'world' });
  assert.ok(store.snapshotVersion() > v0, '写事务提交后版本 +1');
  const after = store.snapshot();
  assert.notEqual(after, before, '写后必须换新快照');
  assert.equal(after.list.length, 1);
  assert.equal(after.byId.get(rec.id).text, '树莓派温度 45 度');
});

test('store 集成：update 后 byId 是新状态，archive 后不在活跃快照里', async () => {
  const { store } = makeStore();
  const rec = await store.save('树莓派温度 45 度', ['infra'], 'agent', { type: 'world' });
  await store.update(rec.id, { text: '树莓派温度 46 度' });
  assert.equal(store.snapshot().byId.get(rec.id).text, '树莓派温度 46 度', 'update 后读到新正文');

  await store.archive(rec.id, '过期');
  const snap = store.snapshot();
  assert.ok(!snap.byId.has(rec.id), '归档后不再出现');
  assert.equal(snap.list.length, 0);
});

test('原语兜底：不传 version 只取一次；load 返回空/非 Map 不炸', () => {
  let loads = 0;
  const snap = createActiveSnapshot({ load: () => { loads += 1; return null; } });
  const a = snap.get();
  snap.get();
  assert.equal(loads, 1, '没有版本源时一直命中缓存');
  assert.deepEqual(a.list, []);
  assert.equal(a.vecOf instanceof Map, true, 'vecOf 非 Map 时兜底空 Map');

  const snap2 = createActiveSnapshot({ load: () => ({ list: [{ id: 'x' }], vecOf: [] }), version: () => 0 });
  assert.equal(snap2.get().vecOf instanceof Map, true, '传数组也给空 Map');
  assert.equal(snap2.version(), 0);
});
