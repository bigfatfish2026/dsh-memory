/**
 * P1 实体按关联迁移到路径级（v3 §4）的内核单测。
 *
 * 钉五件事：
 *   1) 唯一 /plugins/<repo>/<rel> 证据才迁：新实体 norm 写完整路径，surface 新增/改指都对；
 *   2) 一条记忆点到两个仓 → ambiguous，不迁，只在 memory_entity.attrs 留 needs_review；
 *   3) entity_surface.source 只能用 CHECK 里的合法值（本批固定 'migrate'），写 'path_split' 当场违例；
 *   4) undo 按 evidence 的 (memory_id, entity_id, role) 与精确 surface 键恢复，迁移后目标实体
 *      上新增的关联不跟着搬回（这是票面第 5 条，不能按实体整体搬）；
 *   5) 新增 surface 的 count 按 memories.entities 原文精确计数。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import {
  SURFACE_SOURCES, PATH_SURFACE_SOURCE, classifyByPathEvidence, surfaceOccurrences,
  applyBatch, undoBatch, markReviewOnly,
} from '../scripts/lib/path-split.mjs';

const DDL = `
CREATE TABLE memories (id TEXT PRIMARY KEY, text TEXT NOT NULL, entities TEXT, status TEXT);
CREATE TABLE entities (
  id INTEGER PRIMARY KEY, norm TEXT NOT NULL, display TEXT NOT NULL,
  etype TEXT NOT NULL DEFAULT 'unknown', aliases_note TEXT NOT NULL DEFAULT '',
  attrs TEXT NOT NULL DEFAULT '{}', first_seen TEXT NOT NULL DEFAULT '',
  last_seen TEXT NOT NULL DEFAULT '', mem_count INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'active', UNIQUE (norm));
CREATE TABLE memory_entity (memory_id TEXT NOT NULL, entity_id INTEGER NOT NULL, role TEXT NOT NULL DEFAULT 'about',
  ts TEXT NOT NULL DEFAULT '', status TEXT NOT NULL DEFAULT 'active', attrs TEXT NOT NULL DEFAULT '{}',
  version INTEGER NOT NULL DEFAULT 0, prev_memory_id TEXT NOT NULL DEFAULT '', origin TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (memory_id, entity_id, role)) WITHOUT ROWID;
CREATE UNIQUE INDEX uq_memory_entity_mem ON memory_entity(memory_id, entity_id);
CREATE TABLE entity_surface (surface TEXT PRIMARY KEY, norm TEXT NOT NULL, entity_id INTEGER NOT NULL,
  source TEXT NOT NULL CHECK (source IN ('migrate','gate','coarse','extract','model','manual','text')),
  count INTEGER NOT NULL DEFAULT 0, first_seen TEXT NOT NULL DEFAULT '', last_seen TEXT NOT NULL DEFAULT '',
  flag TEXT NOT NULL DEFAULT '', UNIQUE (norm, surface)) WITHOUT ROWID;
CREATE TABLE entity_merge (id INTEGER PRIMARY KEY AUTOINCREMENT, src_entity INTEGER NOT NULL,
  dst_entity INTEGER NOT NULL, reason TEXT NOT NULL, evidence TEXT NOT NULL DEFAULT '{}',
  run_id TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL, undone_at TEXT NOT NULL DEFAULT '');
CREATE UNIQUE INDEX uq_entity_merge_src ON entity_merge(src_entity) WHERE undone_at='';
`;

const OLD_ID = 465;
const HOME = os.homedir();
const P = (repo, rel) => `${HOME}/fatfish/plugins/${repo}/${rel}`;
const RUN = 't-test-path';

function freshDb() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-path-split-'));
  const db = new DatabaseSync(path.join(dir, 'memory.db'));
  db.exec(DDL);
  db.prepare(`INSERT INTO entities(id,norm,display,etype,mem_count) VALUES(?,?,?,?,0)`)
    .run(OLD_ID, 'index.js', 'lib/index.js', 'file');
  const surf = db.prepare(`INSERT INTO entity_surface(surface,norm,entity_id,source,count) VALUES(?,?,?,'migrate',?)`);
  surf.run('lib/index.js', 'lib/index.js', OLD_ID, 10);
  surf.run('index.js', 'index.js', OLD_ID, 5);
  surf.run(P('dsh-home', 'lib/index.js'), P('dsh-home', 'lib/index.js'), OLD_ID, 2);
  return db;
}

function seedMemory(db, { id, text, entities = null, entityId = OLD_ID, role = 'about' }) {
  db.prepare('INSERT INTO memories(id,text,entities,status) VALUES(?,?,?,?)').run(id, text, entities ? JSON.stringify(entities) : '[]', 'active');
  db.prepare('INSERT INTO memory_entity(memory_id,entity_id,role,ts,status,origin) VALUES(?,?,?,?,?,?)')
    .run(id, entityId, role, '2026-11-01', 'active', 'rule');
  db.prepare('UPDATE entities SET mem_count=(SELECT count(*) FROM memory_entity WHERE entity_id=?) WHERE id=?').run(OLD_ID, OLD_ID);
}

function planFor(db, entityId = OLD_ID, run = RUN) {
  const ent = db.prepare('SELECT * FROM entities WHERE id=?').get(entityId);
  const c = classifyByPathEvidence(db, { entityId, mustBasename: ent.norm });
  const occ = surfaceOccurrences(db, c.mounts.filter((m) => m.canonical).map((m) => m.canonical));
  const groups = new Map();
  const review = [];
  for (const m of c.mounts) {
    if (m.mode !== 'unique') { review.push({ memoryId: m.memoryId, role: m.role }); continue; }
    if (!groups.has(m.key)) {
      const exists = db.prepare('SELECT entity_id FROM entity_surface WHERE surface=?').get(m.canonical);
      groups.set(m.key, {
        key: m.key, canonical: m.canonical, etype: ent.etype, moved: [], review: [],
        surfaces: [exists
          ? { kind: 'update', surface: m.canonical }
          : { kind: 'insert', surface: m.canonical, norm: m.canonical, count: occ.get(m.canonical) || 0, flag: 'path' }],
      });
    }
    groups.get(m.key).moved.push({ memoryId: m.memoryId, role: m.role });
  }
  const gs = [...groups.values()];
  if (gs.length) gs[0].review = review;
  return { classify: c, review, groups: gs, plan: {
    reason: 'path_split', runId: run, source: PATH_SURFACE_SOURCE, oldEntityId: entityId,
    groups: gs.map((g) => ({
      ...g, entityAttrs: { path_split: run }, reviewMarker: `path_split:${run}`,
      evidence: { group: `${ent.norm}->${g.key}` },
    })),
  } };
}

test('唯一仓证据：迁到完整路径实体，surface 新增/改指、count 按原文计数', () => {
  const db = freshDb();
  seedMemory(db, { id: 'm1', text: `看 ${P('dsh-ego', 'lib/index.js')} 的挂载`, entities: ['lib/index.js', 'index.js'] });
  seedMemory(db, { id: 'm2', text: `home 那份在 ${P('dsh-home', 'lib/index.js')}`, entities: [P('dsh-home', 'lib/index.js')] });
  seedMemory(db, { id: 'm3', text: '只说 lib/index.js，没有路径' });

  const { classify, plan } = planFor(db);
  assert.equal(classify.bucket.unique, 2);
  assert.equal(classify.bucket.none, 1);
  const r = applyBatch(db, plan);
  assert.equal(r.created, 2);
  assert.equal(r.moved, 2);

  const ego = db.prepare('SELECT * FROM entities WHERE norm=?').get(P('dsh-ego', 'lib/index.js'));
  assert.ok(ego, '新建路径实体 norm 是完整路径');
  assert.equal(ego.display, P('dsh-ego', 'lib/index.js'));
  assert.equal(ego.etype, 'file');
  assert.deepEqual(JSON.parse(ego.attrs), { split_from: OLD_ID, canonical: P('dsh-ego', 'lib/index.js'), path_split: RUN });
  assert.equal(ego.mem_count, 1);

  const egoSurf = db.prepare('SELECT * FROM entity_surface WHERE surface=?').get(P('dsh-ego', 'lib/index.js'));
  assert.equal(egoSurf.entity_id, ego.id);
  assert.equal(egoSurf.source, PATH_SURFACE_SOURCE);
  assert.ok(SURFACE_SOURCES.includes(egoSurf.source), 'source 必须在 CHECK 合法值里');
  assert.equal(egoSurf.flag, 'path');
  assert.equal(egoSurf.count, 0, 'memories.entities 没写这条路径时 count=0');

  // dsh-home 这条本来就有 surface：只改指，不新增行。
  const homeSurfRows = db.prepare('SELECT * FROM entity_surface WHERE surface=?').all(P('dsh-home', 'lib/index.js'));
  assert.equal(homeSurfRows.length, 1);
  const home = db.prepare('SELECT * FROM entities WHERE norm=?').get(P('dsh-home', 'lib/index.js'));
  assert.equal(homeSurfRows[0].entity_id, home.id);
  assert.equal(home.mem_count, 1);

  const me = db.prepare('SELECT * FROM memory_entity WHERE memory_id=? AND entity_id=?').get('m2', home.id);
  assert.equal(me.role, 'about');
  assert.equal(db.prepare('SELECT count(*) c FROM memory_entity WHERE memory_id=? AND entity_id=?').get('m2', OLD_ID).c, 0);
  assert.equal(db.prepare('SELECT count(*) c FROM memory_entity WHERE entity_id=?').get(OLD_ID).c, 1, 'm3 留在 465');
  assert.equal(db.prepare('SELECT mem_count FROM entities WHERE id=?').get(OLD_ID).mem_count, 1);

  const ledger = db.prepare(`SELECT * FROM entity_merge WHERE reason='path_split'`).all();
  assert.equal(ledger.length, 2);
  const egoEv = JSON.parse(ledger.find((x) => x.src_entity === ego.id).evidence);
  assert.deepEqual(egoEv.memory_ids, ['m1']);
  assert.deepEqual(egoEv.mounts, [{ memory_id: 'm1', role: 'about' }]);
  assert.deepEqual(egoEv.surfaces.inserted, [P('dsh-ego', 'lib/index.js')]);
  assert.deepEqual(egoEv.surfaces.updated, []);
});

test('歧义：一条记忆点到两个仓 → ambiguous，不迁，只留 needs_review', () => {
  const db = freshDb();
  seedMemory(db, { id: 'm1', text: `两个仓都提到 ${P('dsh-ego', 'lib/index.js')} 和 ${P('dsh-display', 'lib/index.js')}` });
  // 同一实体上还有一条唯一证据，复核标记挂在它的账本上（实体一条 unique 都没有时整只跳过，见 CLI dry-run）。
  seedMemory(db, { id: 'm2', text: `只提 ${P('dsh-ego', 'lib/index.js')}` });
  const { classify, plan } = planFor(db);
  assert.equal(classify.bucket.ambiguous, 1);
  assert.equal(classify.bucket.unique, 1);
  const r = applyBatch(db, plan);
  assert.equal(r.created, 1);
  assert.equal(r.moved, 1);

  assert.equal(db.prepare('SELECT count(*) c FROM entity_merge WHERE reason=?').get('path_split').c, 1);
  assert.equal(db.prepare('SELECT count(*) c FROM memory_entity WHERE memory_id=? AND entity_id=?').get('m1', OLD_ID).c, 1, '歧义那条关联留在原实体');
  const row = db.prepare('SELECT attrs FROM memory_entity WHERE memory_id=? AND entity_id=?').get('m1', OLD_ID);
  assert.deepEqual(JSON.parse(row.attrs).needs_review, [`path_split:${RUN}`]);

  // undo 把标记摘干净，唯一那条也搬回原实体
  const u = undoBatch(db, { reason: 'path_split', runId: RUN });
  assert.equal(u.undone, 1);
  assert.equal(u.deletedEntities, 1);
  assert.deepEqual(JSON.parse(db.prepare('SELECT attrs FROM memory_entity WHERE memory_id=?').get('m1').attrs), {});
  assert.equal(db.prepare('SELECT count(*) c FROM memory_entity WHERE memory_id=? AND entity_id=?').get('m2', OLD_ID).c, 1);
});

test('source 只能用 CHECK 合法值：path_split 当场违例，migrate 能落', () => {
  const db = freshDb();
  seedMemory(db, { id: 'm1', text: P('dsh-ego', 'lib/index.js') });
  const { plan } = planFor(db);
  const bad = { ...plan, source: 'path_split' };
  assert.throws(() => applyBatch(db, bad), /不在 CHECK 允许值里/);
  assert.equal(db.prepare('SELECT count(*) c FROM entities').get().c, 1, '违例时一行没写');
  const r = applyBatch(db, plan);
  assert.equal(r.created, 1);
  const row = db.prepare('SELECT source FROM entity_surface WHERE surface=?').get(P('dsh-ego', 'lib/index.js'));
  assert.equal(row.source, 'migrate');
});

test('undo 精确恢复：只搬本批关联；目标实体后来新增的关联不跟着回搬', () => {
  const db = freshDb();
  seedMemory(db, { id: 'm1', text: P('dsh-ego', 'lib/index.js'), role: 'about' });
  seedMemory(db, { id: 'm2', text: P('dsh-display', 'lib/index.js'), role: 'mentioned' });
  seedMemory(db, { id: 'm3', text: '没有路径证据' });

  const before = {
    entities: db.prepare('SELECT count(*) c FROM entities').get().c,
    me: db.prepare('SELECT count(*) c FROM memory_entity').get().c,
    surface: db.prepare('SELECT count(*) c FROM entity_surface').get().c,
  };
  const { plan } = planFor(db);
  const r = applyBatch(db, plan);
  assert.equal(r.created, 2);
  assert.equal(r.moved, 2);

  const ego = db.prepare('SELECT id FROM entities WHERE norm=?').get(P('dsh-ego', 'lib/index.js'));
  // 迁移之后别的批次又往目标实体上挂了一条（不在本批 evidence 里）。
  db.prepare(`INSERT INTO memories(id,text,entities) VALUES('m9','后来新增','[]')`).run();
  db.prepare('INSERT INTO memory_entity(memory_id,entity_id,role,status) VALUES(?,?,?,?)').run('m9', ego.id, 'about', 'active');
  db.prepare('UPDATE entities SET mem_count=(SELECT count(*) FROM memory_entity WHERE entity_id=?) WHERE id=?').run(ego.id, ego.id);

  const u = undoBatch(db, { reason: 'path_split', runId: RUN });
  assert.equal(u.undone, 2);
  assert.equal(u.restoredMounts, 2);
  assert.equal(u.deletedEntities, 1, 'display 那个目标实体清空后删掉');

  // 后来新增的关联留在目标实体上，没有搬回 465。
  assert.equal(db.prepare('SELECT count(*) c FROM memory_entity WHERE memory_id=? AND entity_id=?').get('m9', ego.id).c, 1);
  assert.equal(db.prepare('SELECT count(*) c FROM memory_entity WHERE memory_id=? AND entity_id=?').get('m9', OLD_ID).c, 0);
  assert.ok(u.keptEntities.some((x) => x.id === ego.id && x.mounts === 1), '新实体还有残留就保留，不硬删');
  assert.equal(db.prepare('SELECT mem_count FROM entities WHERE id=?').get(ego.id).mem_count, 1);

  // 本批关联、role、surface、attrs 都回到迁移前。
  assert.equal(db.prepare('SELECT role FROM memory_entity WHERE memory_id=? AND entity_id=?').get('m1', OLD_ID).role, 'about');
  assert.equal(db.prepare('SELECT role FROM memory_entity WHERE memory_id=? AND entity_id=?').get('m2', OLD_ID).role, 'mentioned');
  assert.equal(db.prepare('SELECT count(*) c FROM entity_surface WHERE surface=?').get(P('dsh-display', 'lib/index.js')).c, 0, '本批新增的 surface 删掉');
  assert.equal(db.prepare('SELECT entity_id FROM entity_surface WHERE surface=?').get(P('dsh-home', 'lib/index.js')).entity_id, OLD_ID, '本批改指的 surface 改回');
  assert.deepEqual(JSON.parse(db.prepare('SELECT attrs FROM memory_entity WHERE memory_id=?').get('m3').attrs), {}, 'needs_review 摘干净');
  assert.equal(db.prepare('SELECT count(*) c FROM entity_merge WHERE reason=? AND undone_at=\'\'').get('path_split').c, 0);

  // 清掉后来那条再比一遍四张表口径（entity_merge 只多归档行）。
  db.prepare('DELETE FROM memory_entity WHERE memory_id=?').run('m9');
  db.prepare('DELETE FROM entities WHERE id=?').run(ego.id);
  assert.equal(db.prepare('SELECT count(*) c FROM entities').get().c, before.entities);
  assert.equal(db.prepare('SELECT count(*) c FROM memory_entity').get().c, before.me);
  assert.equal(db.prepare('SELECT count(*) c FROM entity_surface').get().c, before.surface);
});

test('目标实体已存在的组：不硬撞，走 review_only 账本，undo 能摘标记', () => {
  const db = freshDb();
  seedMemory(db, { id: 'm1', text: P('dsh-ego', 'lib/index.js') });
  // 预先建好同 norm 实体，模拟别的批次建过
  db.prepare('INSERT INTO entities(id,norm,display,etype,status) VALUES(9001,?,?,\'file\',\'active\')').run(P('dsh-ego', 'lib/index.js'), P('dsh-ego', 'lib/index.js'));
  const { plan } = planFor(db);
  const r = applyBatch(db, plan);
  assert.equal(r.created, 0);
  assert.equal(r.skipped, 1);
  const skip = r.groups.find((g) => g.skipped);
  assert.equal(skip.existingId, 9001);

  const m = markReviewOnly(db, { reason: 'path_split', runId: RUN, entityId: OLD_ID, mounts: skip.mounts, marker: `path_split:exists:${RUN}` });
  assert.equal(m.ok, true);
  assert.deepEqual(JSON.parse(db.prepare('SELECT attrs FROM memory_entity WHERE memory_id=?').get('m1').attrs).needs_review, [`path_split:exists:${RUN}`]);
  assert.equal(db.prepare('SELECT count(*) c FROM memory_entity WHERE entity_id=?').get(9001).c, 0, '不硬迁');

  const u = undoBatch(db, { reason: 'path_split', runId: RUN });
  assert.equal(u.undone, 1);
  assert.equal(u.deletedEntities, 0, 'review_only 不建实体，也就没有实体要删');
  assert.deepEqual(JSON.parse(db.prepare('SELECT attrs FROM memory_entity WHERE memory_id=?').get('m1').attrs), {});
  assert.equal(db.prepare('SELECT count(*) c FROM memory_entity WHERE entity_id=?').get(9001).c, 0, '关联始终没硬迁');
});
