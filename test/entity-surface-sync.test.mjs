/**
 * 同步写入直接维护 entity_surface。
 *
 * 起因：memory_save 的粗实体路径只插 memory_entity、不碰 surface，全靠异步 entity_extract
 * 事后补；进程挂/热重载/队列积压时 surface.count 就漏记。修法是把计数挂到
 * insertMemoryEntity 的「行真的新增了」这一个口径上，同步/异步共用。
 *
 * 钉住四件事：
 *   1. 同步插入一条 memory_entity → 已有 surface 行 count+1、last_seen 更新；
 *   2. 同一条记忆同一实体重跑（IGNORE）→ 不再 +1；
 *   3. 同步先插、异步 upsert(countSurface=false) 再补，整条链路只 +1；
 *   4. 实体没有 surface 行 → 同步就建一行，source='gate'，count=1。
 *
 * 全程 tmpdir 假库，不碰真库。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { insertMemoryEntity, upsertEntity } from '../lib/entity-gate.js';

const DDL = `
CREATE TABLE entities (
  id INTEGER PRIMARY KEY, norm TEXT NOT NULL, display TEXT NOT NULL,
  etype TEXT NOT NULL DEFAULT 'unknown', aliases_note TEXT NOT NULL DEFAULT '',
  attrs TEXT NOT NULL DEFAULT '{}', first_seen TEXT NOT NULL DEFAULT '',
  last_seen TEXT NOT NULL DEFAULT '', mem_count INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'active', UNIQUE (norm));
CREATE TABLE entity_issue (
  memory_id TEXT NOT NULL DEFAULT '', entity_norm TEXT NOT NULL, raw_name TEXT NOT NULL DEFAULT '',
  reason TEXT NOT NULL, detail TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL,
  PRIMARY KEY (memory_id, entity_norm, reason)) WITHOUT ROWID;
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
INSERT INTO meta(key, value) VALUES ('entity_schema_version', '2');
CREATE TABLE entity_surface (
  surface TEXT PRIMARY KEY, norm TEXT NOT NULL, entity_id INTEGER NOT NULL,
  source TEXT NOT NULL CHECK (source IN ('migrate','gate','coarse','extract','model','manual','text')),
  count INTEGER NOT NULL DEFAULT 0, first_seen TEXT NOT NULL DEFAULT '',
  last_seen TEXT NOT NULL DEFAULT '', flag TEXT NOT NULL DEFAULT '',
  UNIQUE (norm, surface)) WITHOUT ROWID;
CREATE TABLE memory_entity (
  memory_id TEXT NOT NULL, entity_id INTEGER NOT NULL, role TEXT NOT NULL DEFAULT 'about',
  ts TEXT NOT NULL DEFAULT '', status TEXT NOT NULL DEFAULT '', attrs TEXT NOT NULL DEFAULT '{}',
  version INTEGER NOT NULL DEFAULT 0, prev_memory_id TEXT NOT NULL DEFAULT '',
  origin TEXT NOT NULL DEFAULT '', PRIMARY KEY (memory_id, entity_id, role)) WITHOUT ROWID;
`;

const TS = '2026-11-14T02:59:29.387Z';

function makeDb() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-surface-sync-'));
  const db = new DatabaseSync(path.join(dir, 'memory.db'));
  db.exec(DDL);
  return db;
}

function seedEntity(db, { id, norm, display = norm }) {
  db.prepare('INSERT INTO entities(id, norm, display, etype) VALUES (?, ?, ?, ?)').run(Number(id), norm, display, 'device');
}

function seedSurface(db, { surface, norm = surface, entityId, count = 3 }) {
  db.prepare(
    `INSERT INTO entity_surface(surface, norm, entity_id, source, count, first_seen, last_seen)
     VALUES (?, ?, ?, 'migrate', ?, '2026-10-01T00:00:00.000Z', '2026-10-01T00:00:00.000Z')`,
  ).run(surface, norm, Number(entityId), Number(count));
}

const surface = (db, name) => db.prepare('SELECT * FROM entity_surface WHERE surface=?').get(name);
const countOf = (db, sql, ...p) => Number(db.prepare(sql).get(...p)?.n || 0);

test('同步插入一条 memory_entity：已有 surface count+1、last_seen 更新', () => {
  const db = makeDb();
  seedEntity(db, { id: 7, norm: '树莓派' });
  seedSurface(db, { surface: '树莓派', entityId: 7, count: 3 });

  const inserted = insertMemoryEntity(db, { memoryId: 'm1', entityId: 7, ts: TS, origin: 'rule' });
  assert.equal(inserted, true);
  const row = surface(db, '树莓派');
  assert.equal(Number(row.count), 4, '同步路径当场 +1');
  assert.equal(row.last_seen, TS, 'last_seen 用这条记忆的 ts');
  assert.equal(row.source, 'migrate', '老行只 touch，不改 source');
});

test('同一条记忆同一实体重跑：IGNORE 掉就不再 +1', () => {
  const db = makeDb();
  seedEntity(db, { id: 7, norm: '树莓派' });
  seedSurface(db, { surface: '树莓派', entityId: 7, count: 3 });

  assert.equal(insertMemoryEntity(db, { memoryId: 'm1', entityId: 7, ts: TS, origin: 'rule' }), true);
  assert.equal(insertMemoryEntity(db, { memoryId: 'm1', entityId: 7, ts: TS, origin: 'agent' }), false);
  assert.equal(Number(surface(db, '树莓派').count), 4, '一次挂载只记一次');
});

test('同步先插、异步再补：整条链路 count 只 +1', () => {
  const db = makeDb();
  seedEntity(db, { id: 7, norm: '树莓派' });
  seedSurface(db, { surface: '树莓派', entityId: 7, count: 3 });

  // memory_save 同步路径（origin='rule'）
  assert.equal(insertMemoryEntity(db, { memoryId: 'm1', entityId: 7, ts: TS, origin: 'rule' }), true);
  assert.equal(Number(surface(db, '树莓派').count), 4);

  // 异步 entity_extract 路径：upsertEntity 不再自己计数，insertMemoryEntity 被 IGNORE
  const ent = upsertEntity(db, { norm: '树莓派', display: '树莓派', etype: 'device', ts: TS, countSurface: false });
  assert.equal(ent.id, 7);
  assert.equal(Number(surface(db, '树莓派').count), 4, 'upsert 不许再 +1');
  assert.equal(insertMemoryEntity(db, { memoryId: 'm1', entityId: ent.id, ts: TS, origin: 'agent' }), false);
  assert.equal(Number(surface(db, '树莓派').count), 4, '整条链路合计只有同步那一次 +1');
});

test('实体没有 surface 行：同步就建一行，source=gate、count=1', () => {
  const db = makeDb();
  seedEntity(db, { id: 9, norm: 'ctx.logger' });

  assert.equal(insertMemoryEntity(db, { memoryId: 'm2', entityId: 9, ts: TS, origin: 'rule' }), true);
  const row = surface(db, 'ctx.logger');
  assert.ok(row, '同步路径必须把 surface 行建出来');
  assert.equal(row.source, 'gate');
  assert.equal(Number(row.count), 1);
  assert.equal(row.last_seen, TS);
  assert.equal(countOf(db, 'SELECT count(*) n FROM entities'), 1, '不许建第二个实体');
});

test('纯异步新实体：upsert(countSurface=false) 建行 count=0，insertMemoryEntity 记成 1', () => {
  const db = makeDb();
  const ent = upsertEntity(db, { norm: 'journalctl', display: 'journalctl', etype: 'tool', ts: TS, countSurface: false });
  assert.equal(Number(surface(db, 'journalctl').count), 0, '建行时不自带计数');
  assert.equal(insertMemoryEntity(db, { memoryId: 'm3', entityId: ent.id, ts: TS, origin: 'agent' }), true);
  assert.equal(Number(surface(db, 'journalctl').count), 1);
  assert.equal(surface(db, 'journalctl').source, 'gate');
});

test('原文已被别的实体占用：不抢计数，落 surface_conflict', () => {
  const db = makeDb();
  seedEntity(db, { id: 1, norm: 'memory_sleep.log' });
  seedEntity(db, { id: 2, norm: 'memory_sleep.log.1', display: 'memory_sleep.log' });
  seedSurface(db, { surface: 'memory_sleep.log', norm: 'memory_sleep.log', entityId: 1, count: 5 });

  assert.equal(insertMemoryEntity(db, { memoryId: 'm4', entityId: 2, ts: TS, origin: 'rule' }), true);
  assert.equal(Number(surface(db, 'memory_sleep.log').count), 5, '别人的行不许动');
  const issues = db.prepare("SELECT entity_norm, reason FROM entity_issue WHERE reason='surface_conflict'").all();
  assert.ok(issues.length >= 1, '冲突要落 entity_issue');
});
