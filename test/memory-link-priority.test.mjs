/**
 * memory_link 来源优先级 + 回填判据。
 *
 * 钉五件事：
 *   1. schema 里不再有「一 src 一条 supersedes 出边」的 partial unique，DDL 会主动 drop 它；
 *   2. queue > curate > legacy：高优先级顶掉低优先级，低优先级被挡（queue_edge_kept / src_locked）；
 *   3. 同级不同 dst 允许多目标并存，同一条边重复写不产生第二行；
 *   4. 回填判据 A/B/C/D：拆分血缘 split_of、合并血缘 merged_into、白名单真取代 supersedes、其余跳过；
 *   5. 悬空 dst / 自环 / 时间倒挂不回填。
 */
import assert from 'node:assert/strict';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { writeSupersedesLink } from '../lib/memory-link.js';
import { classifyEdge, REAL_SUPERSEDES } from '../scripts/backfill-links-from-supersedes.mjs';
import { DDL } from '../scripts/apply-curate-schema.mjs';

const LINK_DDL = `
CREATE TABLE memory_link (
  src TEXT NOT NULL, dst TEXT NOT NULL, rel TEXT NOT NULL,
  origin TEXT NOT NULL DEFAULT '', attr_key TEXT NOT NULL DEFAULT '',
  ts TEXT NOT NULL DEFAULT '', created_ts INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX uq_memory_link_edge ON memory_link(src, dst, rel);
`;

function makeDb({ withLink = true } = {}) {
  const db = new DatabaseSync(':memory:');
  db.exec("CREATE TABLE memories (id TEXT PRIMARY KEY, status TEXT NOT NULL DEFAULT 'active')");
  if (withLink) db.exec(LINK_DDL);
  const ins = db.prepare('INSERT INTO memories(id, status) VALUES(?, ?)');
  for (const [id, st] of [
    ['new1', 'active'],
    ['new2', 'active'],
    ['oldA', 'active'],
    ['oldB', 'active'],
    ['archivedOld', 'archived'],
  ]) {
    ins.run(id, st);
  }
  return db;
}

const plain = (r) => ({ ...r });
const rows = (db) => db.prepare('SELECT src, dst, origin FROM memory_link ORDER BY dst').all().map(plain);
const edges = (db, src) =>
  db.prepare("SELECT dst, origin FROM memory_link WHERE src=? AND rel='supersedes' ORDER BY dst").all(src).map(plain);

test('schema：partial unique 已从 DDL 撤掉，DDL 会主动 drop 旧索引', () => {
  assert.ok(DDL.includes('DROP INDEX IF EXISTS uq_memory_link_supersedes_src'));
  assert.ok(!/CREATE UNIQUE INDEX[^;]*uq_memory_link_supersedes_src/.test(DDL));
  // 真建一遍：同一个 src 允许两条 supersedes
  const db = new DatabaseSync(':memory:');
  db.exec("CREATE TABLE IF NOT EXISTS memories (id TEXT PRIMARY KEY, status TEXT NOT NULL DEFAULT 'active')");
  db.exec(DDL);
  db.exec("INSERT INTO memories(id, status) VALUES('s','active'),('a','active'),('b','active')");
  db.exec("INSERT INTO memory_link(src,dst,rel,origin) VALUES('s','a','supersedes','queue'),('s','b','supersedes','queue')");
  assert.equal(db.prepare("SELECT count(*) AS n FROM memory_link WHERE src='s'").get().n, 2);
  db.close();
});

test('queue 写边成功，同 dst 重复写返回 edge_exists 且不增行', () => {
  const db = makeDb();
  const first = writeSupersedesLink(db, { src: 'new1', dst: 'oldA', origin: 'queue', now: 1 });
  assert.equal(first.ok, true);
  const again = writeSupersedesLink(db, { src: 'new1', dst: 'oldA', origin: 'queue', now: 2 });
  assert.equal(again.ok, false);
  assert.equal(again.reason, 'edge_exists');
  assert.deepEqual(rows(db), [{ src: 'new1', dst: 'oldA', origin: 'queue' }]);
  db.close();
});

test('curate 不覆盖 queue：不同 dst 写不进去，queue 那条留着', () => {
  const db = makeDb();
  writeSupersedesLink(db, { src: 'new1', dst: 'oldA', origin: 'queue', now: 1 });
  const r = writeSupersedesLink(db, { src: 'new1', dst: 'oldB', origin: 'curate', now: 2 });
  assert.equal(r.ok, false);
  assert.equal(r.reason, 'queue_edge_kept');
  assert.deepEqual(edges(db, 'new1'), [{ dst: 'oldA', origin: 'queue' }]);
  db.close();
});

test('queue 覆盖 legacy：不同 dst 时顶掉 legacy 边，只留 queue', () => {
  const db = makeDb();
  writeSupersedesLink(db, { src: 'new1', dst: 'oldA', origin: 'legacy', now: 1 });
  const r = writeSupersedesLink(db, { src: 'new1', dst: 'oldB', origin: 'queue', now: 2 });
  assert.equal(r.ok, true);
  assert.deepEqual(r.replaced, ['oldA']);
  assert.deepEqual(edges(db, 'new1'), [{ dst: 'oldB', origin: 'queue' }]);
  db.close();
});

test('queue 覆盖 legacy：同 dst 时升级 origin，不产生第二行', () => {
  const db = makeDb();
  writeSupersedesLink(db, { src: 'new1', dst: 'oldA', origin: 'legacy', now: 1 });
  const r = writeSupersedesLink(db, { src: 'new1', dst: 'oldA', origin: 'queue', now: 2 });
  assert.equal(r.ok, true);
  assert.deepEqual(edges(db, 'new1'), [{ dst: 'oldA', origin: 'queue' }]);
  db.close();
});

test('curate 覆盖 legacy，但 legacy 抢不动 curate（src_locked）', () => {
  const db = makeDb();
  writeSupersedesLink(db, { src: 'new1', dst: 'oldA', origin: 'legacy', now: 1 });
  const up = writeSupersedesLink(db, { src: 'new1', dst: 'oldB', origin: 'curate', now: 2 });
  assert.equal(up.ok, true);
  assert.deepEqual(edges(db, 'new1'), [{ dst: 'oldB', origin: 'curate' }]);

  const back = writeSupersedesLink(db, { src: 'new1', dst: 'oldA', origin: 'legacy', now: 3 });
  assert.equal(back.ok, false);
  assert.equal(back.reason, 'src_locked');
  db.close();
});

test('legacy 在 queue 边面前一律挡住（queue_edge_kept）', () => {
  const db = makeDb();
  writeSupersedesLink(db, { src: 'new1', dst: 'oldA', origin: 'queue', now: 1 });
  const r = writeSupersedesLink(db, { src: 'new1', dst: 'oldB', origin: 'legacy', now: 2 });
  assert.equal(r.ok, false);
  assert.equal(r.reason, 'queue_edge_kept');
  assert.deepEqual(edges(db, 'new1'), [{ dst: 'oldA', origin: 'queue' }]);
  db.close();
});

test('同级不同 dst 允许多目标并存（取代是一对多）', () => {
  const db = makeDb();
  assert.equal(writeSupersedesLink(db, { src: 'new1', dst: 'oldA', origin: 'queue', now: 1 }).ok, true);
  const r = writeSupersedesLink(db, { src: 'new1', dst: 'oldB', origin: 'queue', now: 2 });
  assert.equal(r.ok, true);
  assert.deepEqual(edges(db, 'new1'), [
    { dst: 'oldA', origin: 'queue' },
    { dst: 'oldB', origin: 'queue' },
  ]);
  db.close();
});

test('target 不是 active / 自环 / 表不存在都拒绝', () => {
  const db = makeDb();
  assert.equal(writeSupersedesLink(db, { src: 'new1', dst: 'archivedOld', origin: 'queue' }).reason, 'target_not_active');
  assert.equal(writeSupersedesLink(db, { src: 'new1', dst: 'new1', origin: 'queue' }).reason, 'self_link');
  const noTable = makeDb({ withLink: false });
  assert.equal(writeSupersedesLink(noTable, { src: 'new1', dst: 'oldA', origin: 'queue' }).reason, 'no_table');
  db.close();
  noTable.close();
});

test('回填判据：A 拆分血缘 → split_of，B 合并血缘 → merged_into', () => {
  const base = { src: 's1', src_status: 'active', src_ts: '2026-11-02', dst: 'd1', dst_id: 'd1', dst_ts: '2026-11-01' };
  assert.equal(classifyEdge({ ...base, source: 'split' }).rel, 'split_of');
  assert.equal(classifyEdge({ ...base, source: 'split:abc' }).rel, 'split_of');
  assert.equal(classifyEdge({ ...base, source: 'sleep:split:split:f43c' }).rel, 'split_of');
  assert.equal(classifyEdge({ ...base, source: 'sleep:merge' }).rel, 'merged_into');
  assert.equal(classifyEdge({ ...base, source: 'sleep', src_status: 'merged' }).rel, 'merged_into');
});

test('回填判据：C 只认白名单真取代，D 其余跳过', () => {
  const [pair] = [...REAL_SUPERSEDES];
  const [src, dst] = pair.split('>');
  assert.equal(REAL_SUPERSEDES.size, 41);
  const row = { src, src_status: 'archived', src_ts: '2026-11-02', dst, dst_id: dst, dst_ts: '2026-11-01' };
  assert.equal(classifyEdge({ ...row, source: 'sleep' }).rel, 'supersedes');
  // 同一 src 换成白名单外的 dst → 不回填
  const other = classifyEdge({ ...row, dst: 'm_nope', dst_id: 'm_nope', source: 'sleep' });
  assert.equal(other.rel, null);
  assert.equal(other.reason, 'not_audited_supersedes');
});

test('回填判据：悬空 dst / 自环 / 时间倒挂不建边', () => {
  const base = { src: 's1', src_status: 'archived', src_ts: '2026-11-02', source: 'split' };
  assert.equal(classifyEdge({ ...base, dst: 'd1', dst_id: null, dst_ts: '2026-11-01' }).reason, 'dangling_dst');
  assert.equal(classifyEdge({ ...base, dst: 's1', dst_id: 's1', dst_ts: '2026-11-01' }).reason, 'self_loop');
  const [pair] = [...REAL_SUPERSEDES];
  const [src, dst] = pair.split('>');
  const inverted = classifyEdge({ src, src_status: 'archived', src_ts: '2026-11-01', dst, dst_id: dst, dst_ts: '2026-11-02', source: 'sleep' });
  assert.equal(inverted.reason, 'time_inverted');
});
