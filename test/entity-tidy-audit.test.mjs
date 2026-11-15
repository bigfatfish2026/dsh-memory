/**
 * 实体整理审计修复。
 *
 * 一条审计项一组测试，全部用临时库（mkdtemp），不碰生产 memory.db：
 *   1) 高-1  跨批/链式撤销：dst 已被更晚的合并吃掉时先拒撤，撤完不许留重复挂载；
 *   2) 高-2  evidence 快照在 BEGIN IMMEDIATE 之后拍，实搬条数与快照不符整步回滚；
 *   3) 高-3  数字间的分隔符不许折叠抹掉（v1.2 ≠ v12），命中的只进 REPORT；
 *   4) 中-4  合并计划不校验 src/dst 当前状态：非 active 记 stale_plan 跳过；
 *   5) 中-5  撤销读写同一个事务：undone_at 条件更新失手就回滚报 already_undone；
 *   6) 中-6  撤销回执报真实 restored/skipped，不拿 evidence 条数充数；
 *   7) 中-11 surface_cross 必须双向成立，单链不传递。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import {
  scanEntityTidy, applyEntityTidy, undoEntityMerge, undoEntityTidyRun, foldName, cleanName,
} from '../lib/entity-tidy.js';

const DDL = `
CREATE TABLE entities (id INTEGER PRIMARY KEY, norm TEXT NOT NULL, display TEXT NOT NULL,
  etype TEXT NOT NULL DEFAULT 'unknown', aliases_note TEXT NOT NULL DEFAULT '',
  attrs TEXT NOT NULL DEFAULT '{}', first_seen TEXT NOT NULL DEFAULT '', last_seen TEXT NOT NULL DEFAULT '',
  mem_count INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'active', UNIQUE (norm));
CREATE TABLE memory_entity (memory_id TEXT NOT NULL, entity_id INTEGER NOT NULL, role TEXT NOT NULL DEFAULT 'about',
  ts TEXT NOT NULL DEFAULT '', status TEXT NOT NULL DEFAULT 'active', attrs TEXT NOT NULL DEFAULT '{}',
  version INTEGER NOT NULL DEFAULT 0, prev_memory_id TEXT NOT NULL DEFAULT '', origin TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (memory_id, entity_id, role)) WITHOUT ROWID;
CREATE TABLE entity_surface (surface TEXT PRIMARY KEY, norm TEXT NOT NULL, entity_id INTEGER NOT NULL,
  source TEXT NOT NULL CHECK (source IN ('migrate','gate','coarse','extract','model','manual','text')),
  count INTEGER NOT NULL DEFAULT 0, first_seen TEXT NOT NULL DEFAULT '', last_seen TEXT NOT NULL DEFAULT '',
  flag TEXT NOT NULL DEFAULT '', UNIQUE (norm, surface)) WITHOUT ROWID;
CREATE TABLE entity_merge (id INTEGER PRIMARY KEY AUTOINCREMENT, src_entity INTEGER NOT NULL,
  dst_entity INTEGER NOT NULL, reason TEXT NOT NULL, evidence TEXT NOT NULL DEFAULT '{}',
  run_id TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL, undone_at TEXT NOT NULL DEFAULT '');
CREATE UNIQUE INDEX uq_entity_merge_src ON entity_merge(src_entity) WHERE undone_at='';
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);`;

function freshDb() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-tidy-audit-'));
  const db = new DatabaseSync(path.join(dir, 'memory.db'));
  db.exec(DDL);
  return db;
}

function addEntity(db, display, { norm = '', mem = 0 } = {}) {
  const r = db.prepare('INSERT INTO entities(norm,display,etype,mem_count) VALUES (?,?,?,?)')
    .run(norm || display.toLowerCase(), display, 'concept', mem);
  return Number(r.lastInsertRowid);
}

function addSurface(db, surface, entityId, flag = '') {
  db.prepare(`INSERT INTO entity_surface(surface,norm,entity_id,source,count,flag)
    VALUES (?,?,?,'gate',1,?)`).run(surface, surface.toLowerCase(), entityId, flag);
}

function addMount(db, memoryId, entityId, role = 'about') {
  db.prepare(`INSERT INTO memory_entity(memory_id,entity_id,role,ts,status,origin)
    VALUES (?,?,?,'','active','rule')`).run(memoryId, entityId, role);
}

const surfacesOf = (db, entityId) => db.prepare('SELECT surface FROM entity_surface WHERE entity_id=? ORDER BY surface')
  .all(entityId).map((r) => String(r.surface));
const mountsOf = (db, entityId) => db.prepare('SELECT memory_id FROM memory_entity WHERE entity_id=? ORDER BY memory_id')
  .all(entityId).map((r) => String(r.memory_id));
const statusOf = (db, entityId) => String(db.prepare('SELECT status FROM entities WHERE id=?').get(entityId)?.status || '');
// mem_count 与 memory_entity 实数不一致的实体（链式撤销后必须一条都没有）
const countDrift = (db) => db.prepare(`SELECT id, mem_count, (SELECT count(*) FROM memory_entity WHERE entity_id=e.id) AS n
  FROM entities e WHERE e.mem_count <> n`).all().map((r) => ({ ...r }));

test('高-1 链式撤销：先撤更晚的那条，撤完不留重复挂载、计数不漂', () => {
  const db = freshDb();
  const a = addEntity(db, 'Alpha Watch');
  const b = addEntity(db, 'alpha-watch');
  addSurface(db, 'alpha_watch', b);
  addMount(db, 'm_1', a);
  addMount(db, 'm_2', b);

  const scanA = scanEntityTidy(db);
  assert.equal(scanA.auto.length, 1, '折叠同名先进自动合组');
  const first = applyEntityTidy(db, { plan: scanA.auto, runId: 'run-a' });
  assert.equal(first.merged, 1);
  const mergeA = first.pairs[0].mergeId;
  assert.equal(first.pairs[0].src, b);
  assert.equal(first.pairs[0].dst, a);

  // 第二批：新实体把 a 吃掉（链：b 的挂载 → a → c），dst 按真数选 c
  const c = addEntity(db, 'AlphaWatch');
  addMount(db, 'm_3', c);
  addMount(db, 'm_4', c);
  addMount(db, 'm_5', c);
  const scanB = scanEntityTidy(db);
  assert.equal(scanB.auto.length, 1);
  const second = applyEntityTidy(db, { plan: scanB.auto, runId: 'run-b' });
  assert.equal(second.merged, 1);
  const mergeB = second.pairs[0].mergeId;
  assert.equal(second.pairs[0].src, a);
  assert.equal(second.pairs[0].dst, c);

  // 先撤旧那条：拒绝，并指出挡路的是哪条
  const blocked = undoEntityMerge(db, { mergeId: mergeA });
  assert.equal(blocked.ok, false);
  assert.equal(blocked.reason, 'undo_newer_first');
  assert.equal(blocked.blockingMergeId, mergeB);
  assert.equal(statusOf(db, b), 'merged', '拒绝时不许留半截状态');
  assert.deepEqual(surfacesOf(db, a), [], '拒绝时 surface 不许被搬走一半');
  const runBlocked = undoEntityTidyRun(db, { runId: 'run-a' });
  assert.equal(runBlocked.ok, false, '按批撤也一样先拒');
  assert.equal(runBlocked.undone, 0);
  assert.equal(runBlocked.reason, 'undo_newer_first');

  // 按 id DESC 撤：先撤新的，再撤旧的
  assert.equal(undoEntityTidyRun(db, { runId: 'run-b' }).undone, 1);
  assert.equal(statusOf(db, a), 'active');
  const old = undoEntityMerge(db, { mergeId: mergeA });
  assert.equal(old.ok, true);
  assert.equal(statusOf(db, b), 'active');
  assert.equal(statusOf(db, a), 'active');
  assert.deepEqual(surfacesOf(db, b), ['alpha_watch'], 'surface 回到原来的 src');
  assert.deepEqual(mountsOf(db, b), ['m_2'], 'm_2 回 b');
  const dup = db.prepare("SELECT count(*) AS n FROM memory_entity WHERE memory_id='m_2'").get();
  assert.equal(Number(dup.n), 1, 'm_2 只能挂一个实体：链式撤销不许造重复挂载');
  assert.deepEqual(countDrift(db), [], '链上所有实体 mem_count 都要按实数重算');
  db.close();
});

test('高-2 evidence 快照进事务：实搬条数与快照不符时整步回滚', () => {
  const db = freshDb();
  const a = addEntity(db, 'Delta One');
  const b = addEntity(db, 'delta-one');
  addSurface(db, 'delta_one', b);
  addMount(db, 'm_1', a);
  addMount(db, 'm_2', b);
  const scan = scanEntityTidy(db);
  assert.equal(scan.auto.length, 1);

  // 让 surface 那一行被静默跳过：实搬 0 行 ≠ 快照 1 行，整步回滚
  db.exec(`CREATE TRIGGER skip_surface BEFORE UPDATE ON entity_surface WHEN OLD.surface='delta_one'
    BEGIN SELECT RAISE(IGNORE); END;`);
  const logs = [];
  const bad = applyEntityTidy(db, { plan: scan.auto, log: (l) => logs.push(String(l)) });
  assert.equal(bad.merged, 0);
  assert.equal(bad.skipped, 1);
  assert.ok(logs.some((l) => l.includes('surface 实搬')), '失败要留可 grep 的原因');
  assert.equal(statusOf(db, b), 'active', '回滚后 src 不能留在 merged');
  assert.equal(Number(db.prepare('SELECT count(*) AS n FROM entity_merge').get().n), 0, '账本不许留半截凭据');
  assert.deepEqual(mountsOf(db, b), ['m_2'], '回滚后挂载原样留在 src');
  db.exec('DROP TRIGGER skip_surface');

  const ok = applyEntityTidy(db, { plan: scan.auto, runId: 'snap' });
  assert.equal(ok.merged, 1);
  const evidence = JSON.parse(String(db.prepare('SELECT evidence FROM entity_merge').get().evidence));
  assert.deepEqual(evidence.surfaces.slice().sort(), surfacesOf(db, ok.pairs[0].dst).slice().sort(),
    'evidence 里的 surface 必须等于真实挂在 dst 上的那批');
  for (const m of evidence.mounts) {
    const hit = db.prepare('SELECT 1 AS ok FROM memory_entity WHERE entity_id=? AND memory_id=? AND role=?')
      .get(ok.pairs[0].dst, m.memory_id, m.role);
    assert.ok(hit, `evidence 记的挂载 ${m.memory_id} 必须真在 dst 上`);
  }
  db.close();
});

test('高-3 数字间分隔符保护：v1.2 不再被折成 v12 合掉，只报 version', () => {
  assert.equal(foldName('v1.2'), 'v1.2', '数字之间的点号要留着');
  assert.equal(foldName('majsoul-watch'), 'majsoulwatch', '字母之间的连字符照旧折叠');
  assert.equal(cleanName('v1.2'), false, '版本名不进自动合');
  assert.equal(cleanName('majsoul-watch'), true);

  const db = freshDb();
  addEntity(db, 'v1.2');
  addEntity(db, 'v12');
  const { auto, report } = scanEntityTidy(db);
  assert.equal(auto.length, 0, 'v1.2 和 v12 是两个版本，不许合成一个');
  const pair = report.find((r) => (r.a.display === 'v1.2' && r.b.display === 'v12')
    || (r.a.display === 'v12' && r.b.display === 'v1.2'));
  assert.equal(pair?.basis, 'version', '抹掉分隔符会撞名的对只进候选清单');
  // 点文件 vs 裸名（.dsh / dsh）折叠后同名，但这是「脏名字」，别混进版本候选里
  db.prepare('INSERT INTO entities(norm,display) VALUES (?,?)').run('.dsh', '.dsh');
  const again = scanEntityTidy(db);
  assert.ok(!again.report.some((r) => [r.a.display, r.b.display].includes('.dsh')),
    '点文件撞名不进版本候选');
  db.close();
});

test('中-4 stale plan：src/dst 已不是 active 时跳过并计数，不硬合', () => {
  const db = freshDb();
  const a = addEntity(db, 'Echo One');
  const b = addEntity(db, 'echo-one');
  addMount(db, 'm_1', a);
  addMount(db, 'm_2', b);
  const scan = scanEntityTidy(db);
  assert.equal(scan.auto.length, 1);
  const first = applyEntityTidy(db, { plan: scan.auto, runId: 'r1' });
  assert.equal(first.merged, 1);
  assert.equal(first.stale, 0);

  // 同一份计划再跑一遍：src 已经是 merged，必须记 stale 跳过
  const logs = [];
  const again = applyEntityTidy(db, { plan: scan.auto, runId: 'r1', log: (l) => logs.push(String(l)) });
  assert.equal(again.merged, 0);
  assert.equal(again.skipped, 1);
  assert.equal(again.stale, 1, 'stale_plan 要单独计数');
  assert.ok(logs.some((l) => l.includes('stale_plan')), '跳过原因要能 grep');
  assert.equal(Number(db.prepare('SELECT count(*) AS n FROM entity_merge').get().n), 1, '不许重复记账');

  // dst 自己也被合走了：整组跳过，不碰别的组
  const db2 = freshDb();
  const p = addEntity(db2, 'Foxtrot One');
  const q = addEntity(db2, 'foxtrot-one');
  addMount(db2, 'm_1', p);
  addMount(db2, 'm_2', q);
  const plan2 = scanEntityTidy(db2).auto;
  db2.prepare("UPDATE entities SET status='merged' WHERE id=?").run(p);
  const staleDst = applyEntityTidy(db2, { plan: plan2 });
  assert.equal(staleDst.merged, 0);
  assert.equal(staleDst.stale, 1);
  assert.equal(statusOf(db2, q), 'active', 'src 不许被半合');
  db.close();
  db2.close();
});

test('中-5 撤销读写在同一个事务：undone_at 条件更新失手就回滚报 already_undone', () => {
  const db = freshDb();
  const a = addEntity(db, 'Golf One');
  const b = addEntity(db, 'golf-one');
  addSurface(db, 'golf_one', b);
  addMount(db, 'm_1', a);
  addMount(db, 'm_2', b);
  const scan = scanEntityTidy(db);
  const applied = applyEntityTidy(db, { plan: scan.auto, runId: 'r' });
  const mergeId = applied.pairs[0].mergeId;

  // 模拟另一路在同一个事务窗口里先把这条撤了：触发器把 undone_at 抢着写掉
  db.exec(`CREATE TRIGGER rival BEFORE UPDATE ON entity_surface BEGIN
    UPDATE entity_merge SET undone_at='rival' WHERE undone_at=''; END;`);
  const out = undoEntityMerge(db, { mergeId });
  assert.equal(out.ok, false);
  assert.equal(out.reason, 'already_undone');
  assert.equal(statusOf(db, b), 'merged', '整步回滚：src 还是 merged');
  assert.deepEqual(surfacesOf(db, b), [], '整步回滚：surface 不许留在旧实体上');
  assert.equal(String(db.prepare('SELECT undone_at FROM entity_merge WHERE id=?').get(Number(mergeId)).undone_at), '',
    '回滚后凭据还是未撤销状态');
  db.exec('DROP TRIGGER rival');

  const ok = undoEntityMerge(db, { mergeId });
  assert.equal(ok.ok, true, '回滚之后还能正常撤');
  assert.equal(statusOf(db, b), 'active');
  db.close();
});

test('中-6 撤销回执报真实条数：已经不在 dst 上的行算 skipped', () => {
  const db = freshDb();
  const a = addEntity(db, 'Hotel One');
  const b = addEntity(db, 'hotel-one');
  addSurface(db, 'hotel_one', b);
  addMount(db, 'm_1', a);
  addMount(db, 'm_2', b);
  const scan = scanEntityTidy(db);
  const applied = applyEntityTidy(db, { plan: scan.auto, runId: 'r' });
  const { src, dst } = applied.pairs[0];

  // 手工先把 evidence 里那两行挪回 src：undo 时它们已经不在 dst 上了
  db.prepare('UPDATE entity_surface SET entity_id=? WHERE surface=?').run(src, 'hotel_one');
  addMount(db, 'm_2', src);
  db.prepare('DELETE FROM memory_entity WHERE entity_id=? AND memory_id=?').run(dst, 'm_2');

  const undo = undoEntityMerge(db);
  assert.equal(undo.ok, true);
  assert.equal(undo.restored, 0, '一行都没搬回来');
  assert.equal(undo.skipped, 2, '两行都在别处，记 skipped，不许拿 evidence 条数充数');
  assert.equal(undo.surfaces, 0);
  assert.equal(undo.mounts, 0);
  assert.deepEqual(mountsOf(db, src), ['m_2'], 'src 只拿回自己原来那条');
  assert.deepEqual(mountsOf(db, dst), ['m_1'], 'dst 自己的挂载不许被搬走');
  assert.deepEqual(countDrift(db), [], '重算之后计数不漂');
  db.close();
});

test('中-11 surface_cross 必须双向成立：单链不传递，双向才合', () => {
  const db = freshDb();
  const x = addEntity(db, 'Kilo');
  const y = addEntity(db, 'Lima');
  const z = addEntity(db, 'Mike');
  addSurface(db, 'Lima', x); // x 的写法行叫 y 的名字（单向）
  addSurface(db, 'Mike', y); // y 的写法行叫 z 的名字（单向）
  const oneWay = scanEntityTidy(db);
  assert.equal(oneWay.auto.length, 0, '顺着单链会把 Kilo/Lima/Mike 串成一组合掉');
  assert.ok(oneWay.report.some((r) => r.basis === 'surface_cross'), '单向交叉要进候选清单给人看');

  const db2 = freshDb();
  const p = addEntity(db2, 'Oscar');
  const q = addEntity(db2, 'Papa');
  addMount(db2, 'm_1', p);
  addMount(db2, 'm_2', p);
  addMount(db2, 'm_3', q);
  addSurface(db2, 'Papa', p); // p 的写法行是 q 的名字
  addSurface(db2, 'Oscar', q); // q 的写法行是 p 的名字
  const mutual = scanEntityTidy(db2);
  assert.equal(mutual.auto.length, 1, '双向交叉才允许自动合');
  assert.deepEqual(mutual.auto[0].basis, ['surface_cross']);
  assert.equal(mutual.auto[0].dst.id, p, 'dst 按挂载真数选');
  db.close();
  db2.close();
});
