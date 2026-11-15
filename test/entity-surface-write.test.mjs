/**
 * 实体写入走 entity_surface（2026-11-14，一期第二半）。
 *
 * 钉住五件事：
 *   1. 原文命中已有 surface → 用它的实体，不建新实体，只加 count/last_seen；
 *   2. norm 没认过 → 建 entities 行 + surface 行（source='gate'）；同 norm 新写法挂老实体；
 *   3. 同 norm 指过不同实体 → entity_issue(reason='surface_conflict')，用已有那条的 entity_id；
 *   4. 路径类 norm 保留全路径（R8 basename 折叠已废除）；
 *   5. 内存 Map 与库一致；meta.entity_schema_version 变了一整表重载；没 surface 表直接抛错。
 *
 * 全程离线：tmpdir 假库，不碰真库。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { loadSurfaceCache, upsertEntity } from '../lib/entity-gate.js';

const SURFACE_DDL = `
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
`;

const SURFACE_TABLE = `
CREATE TABLE entity_surface (
  surface TEXT PRIMARY KEY, norm TEXT NOT NULL, entity_id INTEGER NOT NULL,
  source TEXT NOT NULL CHECK (source IN ('migrate','gate','coarse','extract','model','manual','text')),
  count INTEGER NOT NULL DEFAULT 0, first_seen TEXT NOT NULL DEFAULT '',
  last_seen TEXT NOT NULL DEFAULT '', flag TEXT NOT NULL DEFAULT '',
  UNIQUE (norm, surface)) WITHOUT ROWID;
CREATE INDEX idx_surface_norm ON entity_surface(norm);
`;

function makeDb({ withSurface = true } = {}) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-entity-surface-'));
  const db = new DatabaseSync(path.join(dir, 'memory.db'));
  db.exec(SURFACE_DDL);
  if (withSurface) db.exec(SURFACE_TABLE);
  return db;
}

const countOf = (db, sql, ...p) => Number(db.prepare(sql).get(...p)?.n || 0);
const surfaceRow = (db, surface) => db.prepare('SELECT * FROM entity_surface WHERE surface=?').get(surface);

function seedEntity(db, { id, norm, display = norm, etype = 'device' }) {
  db.prepare('INSERT INTO entities(id, norm, display, etype) VALUES (?, ?, ?, ?)').run(Number(id), norm, display, etype);
}

function seedSurface(db, { surface, norm, entityId, count = 1, source = 'migrate' }) {
  db.prepare(
    `INSERT INTO entity_surface(surface, norm, entity_id, source, count, first_seen, last_seen, flag)
     VALUES (?, ?, ?, ?, ?, '', '', '')`,
  ).run(surface, norm, Number(entityId), source, Number(count));
}

test('命中已有 surface：不建新实体，只加 count 和 last_seen', () => {
  const db = makeDb();
  seedEntity(db, { id: 7, norm: '树莓派', etype: 'device' });
  seedSurface(db, { surface: '树莓派', norm: '树莓派', entityId: 7, count: 3 });

  const ent = upsertEntity(db, { norm: '树莓派', display: '树莓派', etype: 'device' });
  assert.equal(ent.id, 7);
  assert.equal(countOf(db, 'SELECT count(*) n FROM entities'), 1, '不许建新实体');
  const row = surfaceRow(db, '树莓派');
  assert.equal(Number(row.count), 4, '命中一次 +1');
  assert.ok(row.last_seen, 'last_seen 要有值');
});

test('未命中：建 entities 行 + surface 行（source=gate），norm 双写到 entities', () => {
  const db = makeDb();
  const ent = upsertEntity(db, { norm: 'mikan', display: 'mikan', etype: 'service' });
  assert.ok(ent.id > 0);
  const e = db.prepare('SELECT norm, display, etype, mem_count FROM entities WHERE id=?').get(ent.id);
  assert.equal(e.norm, 'mikan', 'entities.norm 继续双写');
  assert.equal(e.etype, 'service');
  assert.equal(Number(e.mem_count), 0, '挂载数仍由 memory_entity 维护，这里不猜');
  const s = surfaceRow(db, 'mikan');
  assert.equal(s.norm, 'mikan');
  assert.equal(Number(s.entity_id), ent.id);
  assert.equal(s.source, 'gate');
  assert.equal(Number(s.count), 1);
});

test('同 norm 新写法：挂到已有实体，不另建', () => {
  const db = makeDb();
  seedEntity(db, { id: 3, norm: 'home assistant', etype: 'device' });
  seedSurface(db, { surface: 'home assistant', norm: 'home assistant', entityId: 3 });

  const ent = upsertEntity(db, { norm: 'home assistant', display: 'Home-Assistant', etype: 'device' });
  assert.equal(ent.id, 3);
  assert.equal(countOf(db, 'SELECT count(*) n FROM entities'), 1);
  assert.equal(Number(surfaceRow(db, 'Home-Assistant').entity_id), 3);
});

test('同 norm 不同 entity：落 entity_issue(surface_conflict)，用已有那条的 entity_id', () => {
  const db = makeDb();
  seedEntity(db, { id: 1, norm: 'n', etype: 'concept' });
  seedEntity(db, { id: 2, norm: 'n-other', etype: 'concept' });
  seedSurface(db, { surface: 'aa', norm: 'n', entityId: 1 });
  seedSurface(db, { surface: 'bb', norm: 'n', entityId: 2 });

  // 原文命中：用这条 surface 自己指的实体（已有那条）
  const hit = upsertEntity(db, { norm: 'n', display: 'bb', etype: 'concept' });
  assert.equal(hit.id, 2, '用已有 surface 行的 entity_id');

  // 新写法：norm 已指过两个实体，用最小的已有 id，不静默合并
  const fresh = upsertEntity(db, { norm: 'n', display: 'cc', etype: 'concept' });
  assert.equal(fresh.id, 1, '无原文行时用已有最小 id');

  const issues = db.prepare("SELECT entity_norm, raw_name, detail FROM entity_issue WHERE reason='surface_conflict'").all();
  assert.ok(issues.length >= 1, '必须落 surface_conflict');
  assert.equal(issues[0].entity_norm, 'n');
  assert.deepEqual(JSON.parse(issues[0].detail).existing, [1, 2]);
  assert.equal(countOf(db, 'SELECT count(*) n FROM entities'), 2, '冲突不许建新实体');
});

test('路径 norm 保留全路径，不做 basename 折叠', () => {
  const db = makeDb();
  const raw = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'lib', 'entity-gate.js');
  const ent = upsertEntity(db, { norm: raw, display: raw, etype: 'file' });
  const s = surfaceRow(db, raw);
  assert.equal(s.norm, raw, 'norm 含 /');
  assert.ok(s.norm.includes('/'));
  assert.notEqual(s.norm, 'entity-gate.js');
  assert.equal(s.flag, 'path');
  assert.equal(Number(s.entity_id), ent.id);
});

test('内存 Map 与库一致；schema 版本变了一整表重载', () => {
  const db = makeDb();
  upsertEntity(db, { norm: 'a', display: 'a', etype: 'concept' });
  upsertEntity(db, { norm: 'b', display: 'b', etype: 'concept' });

  const cache = loadSurfaceCache(db);
  const dbNorms = countOf(db, 'SELECT count(DISTINCT norm) n FROM entity_surface');
  assert.equal(cache.byNorm.size, dbNorms, 'byNorm 与库的 norm 数一致');
  assert.equal(cache.bySurface.size, countOf(db, 'SELECT count(*) n FROM entity_surface'));
  assert.equal(cache.byNorm.get('a').entityId, db.prepare("SELECT entity_id e FROM entity_surface WHERE surface='a'").get().e);

  // 外部直插一行 + 抬版本号：缓存必须整表重载，不能拿着旧 Map
  seedSurface(db, { surface: 'outside', norm: 'outside', entityId: 99 });
  db.prepare("UPDATE meta SET value='3' WHERE key='entity_schema_version'").run();
  const reloaded = loadSurfaceCache(db);
  assert.ok(reloaded.byNorm.has('outside'), '版本变了要重读全表');
  assert.equal(reloaded.version, '3');
});

test('老库没有 entity_surface：直接抛，不回退硬编码', () => {
  const db = makeDb({ withSurface: false });
  assert.throws(
    () => upsertEntity(db, { norm: 'x', display: 'x', etype: 'concept' }),
    /entity_surface/,
  );
});
