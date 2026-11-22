/**
 * 实体待观察登记（src/store/observe-notify.ts + 各写路径的接线）。
 *
 * 覆盖票面十项：
 *   1) 新建记忆 → 该实体 pending、reason='new-memory'；
 *   2) 正文改写 A→B → A、B 都有行、reason='rewrite'；
 *   3) 合并 → 源条实体与新条实体都有行、reason='merge'；
 *   4) 归档 → 被归档条实体有行、reason='archive'；
 *   5) 挂载晚到（origin=agent）→ reason='mount-late'；origin=rule 的同步写入不重复登记；
 *   6) 拆分 → 父条实体与子段实体都有行、reason='split'；
 *   7) observe.enabled=false → 一行都不写，原写入照常；
 *   8) 外层事务回滚 → observe_task 不留行；
 *   9) 缺 observe_task 表 → 不报错、不影响原写入；
 *  10) 同一实体重复登记 → 只有一行、version 递增（大小写归一后算同一个键）。
 *
 * 全程离线：假库在 tmpdir，嵌入用假函数，不调模型。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { createStore } from '../lib/store.js';
import { entityGraphOf } from '../lib/entity-graph.js';
import { insertMemoryEntity } from '../lib/entity-gate.js';
import { hasObserveTaskTable } from '../lib/store/observe-queue.js';
import { notifyEntitiesChanged } from '../lib/store/observe-notify.js';
import { applyObserveSchema } from '../scripts/apply-observe-schema.mjs';

const VEC_SO = path.join(import.meta.dirname, '../node_modules/sqlite-vec-linux-arm64/vec0.so');

const MEMORY_DDL = `CREATE TABLE memories (
  id TEXT PRIMARY KEY, text TEXT NOT NULL, tags TEXT, source TEXT, ts TEXT, type TEXT,
  entities TEXT, validFrom TEXT, supersedes TEXT, updatedAt TEXT, status TEXT, reason TEXT,
  supersededBy TEXT, validTo TEXT, confidence REAL, extra TEXT, present TEXT, rowSeq INTEGER NOT NULL
);`;

const ENTITY_DDL = `
CREATE TABLE entities (
  id INTEGER PRIMARY KEY, norm TEXT NOT NULL, display TEXT NOT NULL,
  etype TEXT NOT NULL DEFAULT 'unknown', aliases_note TEXT NOT NULL DEFAULT '',
  attrs TEXT NOT NULL DEFAULT '{}', first_seen TEXT NOT NULL DEFAULT '',
  last_seen TEXT NOT NULL DEFAULT '', mem_count INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'active', UNIQUE (norm));
CREATE TABLE memory_entity (
  memory_id TEXT NOT NULL, entity_id INTEGER NOT NULL, role TEXT NOT NULL DEFAULT 'about',
  ts TEXT NOT NULL DEFAULT '', status TEXT NOT NULL DEFAULT 'active',
  attrs TEXT NOT NULL DEFAULT '{}', version INTEGER NOT NULL DEFAULT 0,
  prev_memory_id TEXT NOT NULL DEFAULT '', origin TEXT NOT NULL DEFAULT 'agent',
  PRIMARY KEY (memory_id, entity_id, role)) WITHOUT ROWID;
CREATE TABLE entity_surface (
  surface TEXT PRIMARY KEY, norm TEXT NOT NULL, entity_id INTEGER NOT NULL,
  source TEXT NOT NULL CHECK (source IN ('migrate','gate','coarse','extract','model','manual','text')),
  count INTEGER NOT NULL DEFAULT 0, first_seen TEXT NOT NULL DEFAULT '',
  last_seen TEXT NOT NULL DEFAULT '', flag TEXT NOT NULL DEFAULT '',
  UNIQUE (norm, surface)) WITHOUT ROWID;
CREATE INDEX idx_surface_norm ON entity_surface(norm);
CREATE TABLE entity_issue (
  memory_id TEXT NOT NULL DEFAULT '', entity_norm TEXT NOT NULL, raw_name TEXT NOT NULL DEFAULT '',
  reason TEXT NOT NULL, detail TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL,
  PRIMARY KEY (memory_id, entity_norm, reason)) WITHOUT ROWID;
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
INSERT INTO meta(key, value) VALUES ('dim', '4');
`;

/** 临时库 + store；withObserve=false 用来验「缺表」，observe=false 用来验开关。 */
function makeStore({ observe = true, withObserve = true } = {}) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-observe-enqueue-'));
  const dbPath = path.join(dir, 'memory.db');
  const db = new DatabaseSync(dbPath, { allowExtension: true });
  db.loadExtension(VEC_SO);
  db.exec(MEMORY_DDL);
  db.exec(ENTITY_DDL);
  db.exec('CREATE VIRTUAL TABLE vec_items USING vec0(embedding float[4] distance_metric=cosine);');
  db.exec('CREATE TABLE vec_map (rowid INTEGER PRIMARY KEY, id TEXT);');
  db.close();
  if (withObserve) applyObserveSchema(dbPath, { logger: () => {} });

  const cfg = {
    storePath: dir,
    storeLogFile: path.join(dir, 'store.log'),
    apiUrl: 'http://127.0.0.1:1',
    timeoutMs: 50,
    provider: 'fake',
    model: 'fake',
    splitMaxChars: 200,
    notifyScript: '/bin/true',
    embed: async (texts) => texts.map(() => [1, 0, 0, 0]),
    logger: { info() {}, warn() {} },
    pipeline: { textLint: false, writeGate: false },
    observe: { enabled: observe },
  };
  const store = createStore(cfg);
  // entity-graph 的开关平时由插件 apply() 设（index.ts）；测试绕过 apply，这里照同一口径设。
  entityGraphOf(store.db()).setObserveEnabled(observe);
  return { store, dir, dbPath, cfg };
}

function observeRows(store) {
  return store.db().prepare('SELECT * FROM observe_task ORDER BY entity_key').all();
}

function rowOf(store, key) {
  const row = store.db().prepare('SELECT * FROM observe_task WHERE entity_key = ?').get(key);
  assert.ok(row, `observe_task 里应有 ${key}`);
  return row;
}

test('新建记忆：实体登记为 pending、reason=new-memory', async () => {
  const { store } = makeStore();
  const rec = await store.save('2026-11-21 树莓派温度 45 度', ['infra'], 'agent', {
    type: 'world', entities: ['树莓派'],
  });
  assert.ok(!rec.discarded, '原写入必须成功');

  const row = rowOf(store, '树莓派');
  assert.equal(String(row.status), 'pending');
  assert.equal(String(row.reason), 'new-memory');
  assert.equal(Number(row.version), 1);
  assert.equal(Number(row.tries), 0);
  assert.equal(Number(row.lease_until), 0);
});

test('正文改写：旧实体 A 和新实体 B 都登记，reason=rewrite', async () => {
  const { store } = makeStore();
  const rec = await store.save('2026-11-20 Alpha 的情况', ['x'], 'agent', {
    type: 'world', entities: ['Alpha'],
  });
  const next = await store.update(rec.id, { text: '2026-11-21 Beta 的情况', entities: ['Beta'] });
  assert.ok(!next.discarded);

  const a = rowOf(store, 'alpha');
  const b = rowOf(store, 'beta');
  assert.equal(String(a.reason), 'rewrite', '改写前的实体要重算');
  assert.equal(String(b.reason), 'rewrite', '改写后的实体要重算');
});

test('合并：源条实体和新条实体都有行，reason=merge', async () => {
  const { store } = makeStore();
  const one = await store.save('2026-11-20 源甲的一条', ['x'], 'agent', { type: 'world', entities: ['源甲'] });
  const two = await store.save('2026-11-20 源乙的一条', ['x'], 'agent', { type: 'world', entities: ['源乙'] });
  const merged = await store.merge([one.id, two.id], '2026-11-21 源甲和源乙合成一条', ['x'], { type: 'world' });
  assert.ok(!merged.discarded);

  assert.equal(String(rowOf(store, '源甲').reason), 'merge');
  assert.equal(String(rowOf(store, '源乙').reason), 'merge');
  // 新条自身实体（源并集）也在同一笔事务里登记
  assert.equal(String(rowOf(store, '源乙').status), 'pending');
});

test('归档：被归档条实体有行，reason=archive', async () => {
  const { store } = makeStore();
  const rec = await store.save('2026-11-21 归档目标实体的一条', ['x'], 'agent', {
    type: 'world', entities: ['归档目标实体'],
  });
  await store.archive(rec.id, '过期了');
  assert.equal(String(rowOf(store, '归档目标实体').reason), 'archive');
});

test('挂载晚到：origin=agent 后补挂载登记 mount-late，origin=rule 不重复登记', async () => {
  const { store } = makeStore();
  const db = store.db();
  const ins = db.prepare('INSERT INTO entities(norm, display, etype) VALUES (?, ?, ?)');
  const late = ins.run('晚到实体', '晚到实体', 'concept').lastInsertRowid;
  const sync = ins.run('同步实体', '同步实体', 'concept').lastInsertRowid;

  assert.equal(insertMemoryEntity(db, { memoryId: 'm_late', entityId: Number(late), origin: 'agent' }), true);
  assert.equal(String(rowOf(store, '晚到实体').reason), 'mount-late');
  assert.equal(Number(rowOf(store, '晚到实体').entity_id), Number(late), 'entity_id 要一起写进队列行');

  assert.equal(insertMemoryEntity(db, { memoryId: 'm_rule', entityId: Number(sync), origin: 'rule' }), true);
  assert.equal(
    db.prepare('SELECT count(*) AS n FROM observe_task WHERE entity_key = ?').get('同步实体').n,
    0,
    '粗实体的同步写入由 records 按 new-memory/rewrite 登记，不在这里重复',
  );
});

test('拆分：父条实体与子段实体都有行，reason=split', async () => {
  const { store } = makeStore();
  const rec = await store.save('2026-11-21 父实体的一条长记忆', ['x'], 'agent', {
    type: 'world', entities: ['父实体'],
  });
  // 与 pipeline-split 收尾同一口径：归档这一笔事务里带上全部子段实体，reason 覆盖成 split
  await store.archive(rec.id, '拆成 2 条', {
    supersededBy: 'm_child',
    observeEntities: ['子一', '子二'],
    observeReason: 'split',
  });

  for (const key of ['父实体', '子一', '子二']) {
    assert.equal(String(rowOf(store, key).reason), 'split', `${key} 要登记`);
  }
});

test('observe.enabled=false：一行都不写，原写入照常', async () => {
  const { store } = makeStore({ observe: false });
  const rec = await store.save('2026-11-21 开关关着的一条', ['x'], 'agent', {
    type: 'world', entities: ['关着实体'],
  });
  assert.ok(!rec.discarded);
  assert.equal(observeRows(store).length, 0, '开关关着时 observe_task 一行都不许有');
  assert.equal(store.db().prepare('SELECT count(*) AS n FROM memories').get().n, 1);
});

test('外层事务回滚：observe_task 不留行', async () => {
  const { store } = makeStore();
  const db = store.db();
  const eid = db.prepare('INSERT INTO entities(norm, display, etype) VALUES (?, ?, ?)')
    .run('回滚实体', '回滚实体', 'concept').lastInsertRowid;

  db.exec('BEGIN IMMEDIATE');
  try {
    insertMemoryEntity(db, { memoryId: 'm_rollback', entityId: Number(eid), origin: 'agent' });
    assert.ok(rowOf(store, '回滚实体'), '回滚前登记确实写进去了');
    throw new Error('人为抛错');
  } catch {
    db.exec('ROLLBACK');
  }

  assert.equal(
    db.prepare('SELECT count(*) AS n FROM observe_task WHERE entity_key = ?').get('回滚实体').n,
    0,
    '事务回滚后登记行也要没',
  );
  assert.equal(db.prepare('SELECT count(*) AS n FROM memory_entity').get().n, 0, '挂载行一起回滚');
});

test('缺 observe_task 表：不报错，原写入不受影响', async () => {
  const { store } = makeStore({ withObserve: false });
  assert.equal(hasObserveTaskTable(store.db()), false);
  const rec = await store.save('2026-11-21 老库还没跑迁移的一条', ['x'], 'agent', {
    type: 'world', entities: ['老库实体'],
  });
  assert.ok(!rec.discarded, '缺表不许挡住写记忆');
  assert.equal(store.db().prepare('SELECT count(*) AS n FROM memories').get().n, 1);
});

test('同一实体重复登记：只有一行，version 递增（大小写归一后算同一个键）', async () => {
  const { store } = makeStore();
  await store.save('2026-11-20 Raspberry Pi 第一次', ['x'], 'agent', { type: 'world', entities: ['Raspberry Pi'] });
  await store.save('2026-11-21 raspberry pi 第二次', ['x'], 'agent', { type: 'world', entities: ['raspberry pi'] });

  const rows = store.db().prepare("SELECT * FROM observe_task WHERE entity_key = 'raspberry pi'").all();
  assert.equal(rows.length, 1, '同一个键只许一行');
  assert.equal(Number(rows[0].version), 2, '第二次登记只涨 version');
});

test('登记端按 etype 收口：file/config/tool/unknown 跳过，person 与查不到 etype 的照旧入队', () => {
  const { store } = makeStore();
  const db = store.db();
  const ins = db.prepare('INSERT INTO entities(norm, display, etype) VALUES (?, ?, ?)');
  for (const [norm, etype] of [
    ['readonly', 'file'],
    ['providers.json', 'config'],
    ['bash', 'tool'],
    ['某个未知', 'unknown'],
    ['anri', 'person'],
    ['树莓派', 'device'],
  ]) ins.run(norm, norm, etype);

  const n = notifyEntitiesChanged(
    db,
    ['readonly', 'providers.json', 'bash', '某个未知', 'Anri', '树莓派', '没有实体行'],
    'new-memory',
  );
  assert.equal(n, 3, '只登记 person/device/查不到 etype 的三条');
  const keys = observeRows(store).map((row) => String(row.entity_key));
  assert.deepEqual(new Set(keys), new Set(['anri', '树莓派', '没有实体行']), '四类脏键不许进队列');

  // 缺 entities 表 = 查不到 etype：照旧放行，别把未知当脏
  db.exec('DROP TABLE entities');
  assert.equal(notifyEntitiesChanged(db, ['another'], 'rewrite'), 1, '缺 entities 表照旧登记');
  assert.ok(
    observeRows(store).some((row) => String(row.entity_key) === 'another'),
    '缺表时新键也要有行',
  );
});
