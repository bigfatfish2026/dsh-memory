/**
 * mem_count 只跟着 memory_entity 的增删走。
 *
 * 早先 upsertEntity 建行写死 0、老行只更 last_seen，落 memory_entity 时从不累加，
 * 这列永远追不上（当天全表重算 151 条，3 分钟漂 17 条）。现在增删各挂一处。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { insertMemoryEntity, deleteMemoryEntity, upsertEntity } from '../lib/entity-gate.js';

const DDL = `
CREATE TABLE memories (id TEXT PRIMARY KEY, text TEXT NOT NULL, ts TEXT);
CREATE TABLE entities (
  id INTEGER PRIMARY KEY, norm TEXT NOT NULL, display TEXT NOT NULL,
  etype TEXT NOT NULL DEFAULT 'unknown', aliases_note TEXT NOT NULL DEFAULT '',
  attrs TEXT NOT NULL DEFAULT '{}', first_seen TEXT NOT NULL DEFAULT '',
  last_seen TEXT NOT NULL DEFAULT '', mem_count INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'active', UNIQUE (norm));
CREATE TABLE entity_surface (
  surface TEXT PRIMARY KEY, norm TEXT NOT NULL, entity_id INTEGER NOT NULL,
  source TEXT NOT NULL CHECK (source IN ('migrate','gate','coarse','extract','model','manual','text')),
  count INTEGER NOT NULL DEFAULT 0, first_seen TEXT NOT NULL DEFAULT '',
  last_seen TEXT NOT NULL DEFAULT '', flag TEXT NOT NULL DEFAULT '',
  UNIQUE (norm, surface)) WITHOUT ROWID;
CREATE INDEX idx_surface_norm ON entity_surface(norm);
CREATE TABLE memory_entity (
  memory_id TEXT NOT NULL, entity_id INTEGER NOT NULL, role TEXT NOT NULL DEFAULT 'about',
  ts TEXT NOT NULL DEFAULT '', status TEXT NOT NULL DEFAULT 'active',
  attrs TEXT NOT NULL DEFAULT '{}', version INTEGER NOT NULL DEFAULT 0,
  prev_memory_id TEXT NOT NULL DEFAULT '', origin TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (memory_id, entity_id, role)) WITHOUT ROWID;
`;

function makeDb() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-memcount-'));
  const db = new DatabaseSync(path.join(dir, 'memory.db'));
  db.exec(DDL);
  return db;
}

const memCount = (db, id) => Number(db.prepare('SELECT mem_count AS n FROM entities WHERE id=?').get(Number(id))?.n || 0);

test('插 memory_entity 才 +1：upsertEntity 建行是 0，重复插不加', () => {
  const db = makeDb();
  const ent = upsertEntity(db, { norm: 'jellyfin', display: 'jellyfin', etype: 'service' });
  assert.equal(memCount(db, ent.id), 0, '建实体不猜挂载数');

  assert.equal(insertMemoryEntity(db, { memoryId: 'm1', entityId: ent.id, origin: 'agent' }), true);
  assert.equal(memCount(db, ent.id), 1, '插一行 +1');

  assert.equal(insertMemoryEntity(db, { memoryId: 'm1', entityId: ent.id, origin: 'agent' }), false);
  assert.equal(memCount(db, ent.id), 1, 'IGNORE 掉的不加');

  // 老实体被 upsert 只更 last_seen，不许动计数
  upsertEntity(db, { norm: 'jellyfin', display: 'jellyfin', etype: 'service' });
  assert.equal(memCount(db, ent.id), 1);
});

test('删 memory_entity 行 -1，减到 0 就停不为负', () => {
  const db = makeDb();
  const ent = upsertEntity(db, { norm: 'navidrome', display: 'navidrome', etype: 'service' });
  insertMemoryEntity(db, { memoryId: 'm1', entityId: ent.id, ts: '', origin: 'rule' });
  insertMemoryEntity(db, { memoryId: 'm2', entityId: ent.id, ts: '', origin: 'rule' });
  assert.equal(memCount(db, ent.id), 2);

  assert.equal(deleteMemoryEntity(db, { memoryId: 'm1', origin: 'rule' }), 1);
  assert.equal(memCount(db, ent.id), 1, '删一行 -1');

  assert.equal(deleteMemoryEntity(db, { memoryId: 'm2', origin: 'rule' }), 1);
  assert.equal(memCount(db, ent.id), 0);

  assert.equal(deleteMemoryEntity(db, { memoryId: 'm2', origin: 'rule' }), 0);
  assert.equal(memCount(db, ent.id), 0, '没有行可删时也不许负数');
});
