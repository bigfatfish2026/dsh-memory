/**
 * memory_link 读现状与写入同事务（2026-11-15，审计实体中-10）。
 *
 * 钉四件事：
 *   1. 同 dst 低优先级边升级替换：curate 边被 queue 同 dst 顶成 queue，只留一行；
 *   2. 不同 dst：queue 顶掉 curate，优先级顶替生效；
 *   3. 写入中途失败整段回滚：被顶掉的 legacy 边不会先删掉再丢新边；
 *   4. 调用方已有事务时用 SAVEPOINT 参与外层事务，外层 ROLLBACK 能一起撤。
 *
 * 全程 :memory: 假库，不碰真库。
 */
import assert from 'node:assert/strict';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { writeSupersedesLink } from '../lib/memory-link.js';

const LINK_DDL = `
CREATE TABLE memory_link (
  src TEXT NOT NULL, dst TEXT NOT NULL, rel TEXT NOT NULL,
  origin TEXT NOT NULL DEFAULT '', attr_key TEXT NOT NULL DEFAULT '',
  ts TEXT NOT NULL DEFAULT '', created_ts INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX uq_memory_link_edge ON memory_link(src, dst, rel);
`;

function makeDb() {
  const db = new DatabaseSync(':memory:');
  db.exec("CREATE TABLE memories (id TEXT PRIMARY KEY, status TEXT NOT NULL DEFAULT 'active')");
  db.exec(LINK_DDL);
  const ins = db.prepare('INSERT INTO memories(id, status) VALUES(?, ?)');
  for (const id of ['new1', 'new2', 'oldA', 'oldB']) ins.run(id, 'active');
  return db;
}

const edges = (db, src) =>
  db.prepare("SELECT dst, origin FROM memory_link WHERE src=? AND rel='supersedes' ORDER BY dst").all(src).map((r) => ({ ...r }));

test('同 dst：queue 把 curate 边升级成 queue，不产生第二行', () => {
  const db = makeDb();
  assert.equal(writeSupersedesLink(db, { src: 'new1', dst: 'oldA', origin: 'curate', now: 1 }).ok, true);
  const r = writeSupersedesLink(db, { src: 'new1', dst: 'oldA', origin: 'queue', now: 2 });
  assert.equal(r.ok, true);
  assert.deepEqual(r.replaced, ['oldA']);
  assert.deepEqual(edges(db, 'new1'), [{ dst: 'oldA', origin: 'queue' }], '只留升级后的那一行');
  db.close();
});

test('不同 dst：queue 顶掉 curate，读到的现状与写在同一事务里', () => {
  const db = makeDb();
  writeSupersedesLink(db, { src: 'new1', dst: 'oldA', origin: 'curate', now: 1 });
  const r = writeSupersedesLink(db, { src: 'new1', dst: 'oldB', origin: 'queue', now: 2 });
  assert.equal(r.ok, true);
  assert.deepEqual(edges(db, 'new1'), [{ dst: 'oldB', origin: 'queue' }]);
  db.close();
});

test('写入失败整段回滚：先删掉的 legacy 边会回来，不留半截', () => {
  const db = makeDb();
  writeSupersedesLink(db, { src: 'new1', dst: 'oldA', origin: 'legacy', now: 1 });
  db.exec(`CREATE TRIGGER boom_link BEFORE INSERT ON memory_link
           BEGIN SELECT RAISE(ABORT, 'boom'); END;`);
  const r = writeSupersedesLink(db, { src: 'new1', dst: 'oldB', origin: 'queue', now: 2 });
  assert.equal(r.ok, false);
  assert.match(r.reason, /^constraint:.*boom/);
  assert.deepEqual(edges(db, 'new1'), [{ dst: 'oldA', origin: 'legacy' }], '被顶掉的旧边要回滚回来');
  assert.equal(db.isTransaction, false, '失败后不许把事务挂着');
  db.exec('DROP TRIGGER boom_link');
  db.close();
});

test('调用方已有事务：用 SAVEPOINT 参与外层事务，外层回滚能一起撤', () => {
  const db = makeDb();
  db.exec('BEGIN');
  const r = writeSupersedesLink(db, { src: 'new1', dst: 'oldA', origin: 'queue', now: 1 });
  assert.equal(r.ok, true, '外层事务里也能写');
  assert.equal(db.isTransaction, true, '不许自己 COMMIT 掉调用方的事务');
  db.exec('ROLLBACK');
  assert.deepEqual(edges(db, 'new1'), [], '外层回滚后边也不许留下');
  db.close();
});
