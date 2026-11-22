/**
 * observation 专用写路径（src/store/observation-records.ts，t-mwqd22yq-65yg）。
 *
 * 钉的边界：
 *   ① 正常写入：type=observation、extra 自动标记、两条 derived_from 边、同事务落库；
 *   ② 证据不存在 / 已归档 / 实体不相交 → 整笔拒绝，零写入；
 *   ③ 同实体同正文重复写 → 返回已有 id，不新增；
 *   ④ 绕开通用写入闸：正文相近的旧证据不会被归档、不会被丢（writeGate 开着也照写）；
 *   ⑤ 后一步抛错 → 观察行与证据边一起回滚；
 *   ⑥ observe_task 缺表 → 写入照常成功；
 *   ⑦ 向量索引与粗实体挂载照常维护；
 *   ⑧ replacesObservationId → 旧观察在同一笔事务里归档。
 *
 * 全程离线：临时库 + 假 embedding，模型一次不调。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { createStore } from '../lib/store.js';
import { observationEvidence } from '../lib/store/observation-records.js';
import { applySchema } from '../scripts/apply-pipeline-schema.mjs';
import { applyCurateSchema } from '../scripts/apply-curate-schema.mjs';
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
INSERT INTO entities(id, norm, display) VALUES (1, '树莓派', '树莓派'), (2, 'alpha', 'Alpha'), (3, 'beta', 'Beta');
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
INSERT INTO meta(key, value) VALUES ('dim', '4');
`;

/** 临时库 + 真 store；withLink/withObserve 关掉用来验缺表兜底。 */
function makeStore({ withLink = true, withObserve = true } = {}) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-observation-records-'));
  const dbPath = path.join(dir, 'memory.db');
  const db = new DatabaseSync(dbPath, { allowExtension: true });
  db.loadExtension(VEC_SO);
  db.exec(MEMORY_DDL);
  db.exec(ENTITY_DDL);
  db.exec('CREATE VIRTUAL TABLE vec_items USING vec0(embedding float[4] distance_metric=cosine);');
  db.exec('CREATE TABLE vec_map (rowid INTEGER PRIMARY KEY, id TEXT);');
  db.close();
  applySchema(dbPath, { logger: () => {} });
  if (withLink) applyCurateSchema(dbPath, { logger: { info() {}, warn() {} } });
  if (withObserve) applyObserveSchema(dbPath, { logger: () => {} });

  const cfg = {
    storePath: dir,
    storeLogFile: path.join(dir, 'store.log'),
    apiUrl: 'http://127.0.0.1:1',
    timeoutMs: 50,
    provider: 'fake',
    model: 'fake',
    embed: async (texts) => texts.map(() => [1, 0, 0, 0]),
    logger: { info() {}, warn() {} },
    pipeline: { textLint: false, writeGate: false },
    observe: { enabled: true },
  };
  return { store: createStore(cfg), cfg, dir, dbPath };
}

function count(store, sql, ...params) {
  return Number(store.db().prepare(sql).get(...params)?.n || 0);
}

function rowOf(store, id) {
  return store.db().prepare('SELECT * FROM memories WHERE id = ?').get(id);
}

function edgesOf(store, id) {
  return store.db().prepare(
    'SELECT dst, rel, origin FROM memory_link WHERE src = ? ORDER BY dst',
  ).all(id);
}

/** 两条同一实体的 active 证据。 */
async function seedEvidence(store) {
  const a = await store.save('2026-11-21 树莓派温度 45 度，风扇转速正常', ['infra'], 'agent', {
    type: 'world', entities: ['树莓派'],
  });
  const b = await store.save('2026-11-21 树莓派连续跑了 12 小时，内存占用 3G', ['infra'], 'agent', {
    type: 'world', entities: ['树莓派'],
  });
  assert.ok(!a.discarded && !b.discarded, '证据必须落库成功');
  return [a, b];
}

test('正常写入：observation 行 + extra 自动标记 + 两条证据边 + 向量/挂载', async () => {
  const { store } = makeStore();
  const [a, b] = await seedEvidence(store);
  const res = await store.saveObservation({
    text: '树莓派 2026-11-21 运行稳定，温度和内存都在正常范围',
    entities: ['树莓派'],
    evidenceIds: [a.id, b.id],
    confidence: 0.8,
    reason: 'nightly-observe',
  });
  assert.equal(res.created, true);

  const row = rowOf(store, res.id);
  assert.ok(row, '观察行必须落库');
  assert.equal(String(row.type), 'observation');
  assert.equal(String(row.status), 'active', '初态 active：证据已在本笔事务里校验过');
  assert.equal(String(row.source), 'sleep:observe');
  assert.equal(String(row.reason), 'nightly-observe');
  assert.equal(Number(row.rowSeq) > 0, true, 'rowSeq 必须分配');
  assert.deepEqual(JSON.parse(row.extra), { autoObservation: true, version: 1 });
  assert.deepEqual(JSON.parse(row.entities), ['树莓派']);

  const edges = edgesOf(store, res.id);
  assert.equal(edges.length, 2, '每条证据一条边');
  assert.deepEqual(edges.map((e) => [String(e.dst), String(e.rel), String(e.origin)]), [
    [a.id, 'derived_from', 'sleep'],
    [b.id, 'derived_from', 'sleep'],
  ]);
  assert.deepEqual(observationEvidence(store.db(), res.id), [a.id, b.id].sort());

  // 同一条连接 / 同一笔事务之后：内存视图、向量索引、粗挂载都看得到
  assert.ok(store.metas().some((m) => m.id === res.id), 'state.metas 与库同事务更新');
  // 向量索引照写；vectorIndex() 只收 active，初态 active 的观察会进召回视图
  assert.equal(count(store, 'SELECT count(*) AS n FROM vec_map WHERE id = ?', res.id), 1, '观察行要进向量索引');
  assert.equal(
    count(store, `SELECT count(*) AS n FROM memory_entity me
                     JOIN entities e ON e.id = me.entity_id
                    WHERE me.memory_id = ? AND e.norm = '树莓派'`, res.id),
    1,
    '观察行要挂到实体上',
  );
});

test('初态：默认 active；显式传 pending_review 仍可覆盖；写入推进观察版本号', async () => {
  const { store } = makeStore();
  const [a, b] = await seedEvidence(store);
  const epochBefore = store.observationVersion();

  const plain = await store.saveObservation({
    text: '树莓派 2026-11-21 默认初态的观察', entities: ['树莓派'], evidenceIds: [a.id, b.id],
  });
  assert.equal(String(rowOf(store, plain.id).status), 'active', '默认初态必须是 active');

  const review = await store.saveObservation({
    text: '树莓派 2026-11-21 复核期间重开的观察', entities: ['树莓派'], evidenceIds: [a.id, b.id],
  }, { status: 'pending_review' });
  assert.equal(String(rowOf(store, review.id).status), 'pending_review', '显式传的初态不得被默认值盖掉');

  assert.equal(store.observationVersion(), epochBefore + 2, '每条观察写入都在写事务里把版本 +1');
});

test('证据不存在：整笔拒绝，零写入', async () => {
  const { store } = makeStore();
  const before = count(store, 'SELECT count(*) AS n FROM memories');
  await assert.rejects(
    store.saveObservation({
      text: '树莓派 2026-11-21 证据缺失的一条', entities: ['树莓派'], evidenceIds: ['m_not_exist'],
    }),
    (err) => err.code === 'EVIDENCE_MISSING' && err.evidenceId === 'm_not_exist',
  );
  assert.equal(count(store, 'SELECT count(*) AS n FROM memories'), before);
  assert.equal(count(store, 'SELECT count(*) AS n FROM memory_link'), 0);
  assert.equal(store.metas().length, before);
});

test('证据已归档：整笔拒绝，零写入', async () => {
  const { store } = makeStore();
  const [a] = await seedEvidence(store);
  await store.archive(a.id, '过期了');
  const before = count(store, 'SELECT count(*) AS n FROM memories');
  await assert.rejects(
    store.saveObservation({
      text: '树莓派 2026-11-21 拿归档证据写观察', entities: ['树莓派'], evidenceIds: [a.id],
    }),
    (err) => err.code === 'EVIDENCE_INACTIVE' && err.evidenceId === a.id,
  );
  assert.equal(count(store, 'SELECT count(*) AS n FROM memories'), before);
  assert.equal(count(store, 'SELECT count(*) AS n FROM memory_link'), 0);
});

test('证据与观察实体不相交：整笔拒绝，零写入', async () => {
  const { store } = makeStore();
  const other = await store.save('2026-11-21 Beta 的一条证据', ['x'], 'agent', {
    type: 'world', entities: ['Beta'],
  });
  const before = count(store, 'SELECT count(*) AS n FROM memories');
  await assert.rejects(
    store.saveObservation({
      text: '树莓派 2026-11-21 拿不相干证据写观察', entities: ['树莓派'], evidenceIds: [other.id],
    }),
    (err) => err.code === 'EVIDENCE_ENTITY_MISMATCH' && err.evidenceId === other.id,
  );
  assert.equal(count(store, 'SELECT count(*) AS n FROM memories'), before);
  assert.equal(count(store, 'SELECT count(*) AS n FROM memory_link'), 0);
});

test('正文重复：第二次返回已有 id，不新增行、不重复写边', async () => {
  const { store } = makeStore();
  const [a, b] = await seedEvidence(store);
  const input = {
    text: '树莓派 2026-11-21 重跑要去重的观察', entities: ['树莓派'], evidenceIds: [a.id, b.id],
  };
  const first = await store.saveObservation(input);
  const before = count(store, 'SELECT count(*) AS n FROM memories');
  const second = await store.saveObservation(input);

  assert.equal(second.id, first.id);
  assert.equal(second.created, false);
  assert.equal(count(store, 'SELECT count(*) AS n FROM memories'), before, '不得新增行');
  assert.equal(edgesOf(store, first.id).length, 2);
  assert.deepEqual(second.evidenceIds, [a.id, b.id].sort());
});

test('绕开通用注册闸：writeGate 打开也不归档、不丢弃相似旧条', async () => {
  const { store, cfg } = makeStore();
  const [a] = await seedEvidence(store);
  // 只在观察写入前打开闸：真走 saveMemory 会因为没有 llm 直接抛 WRITE_GATE_NO_LLM
  cfg.pipeline.writeGate = true;
  const text = '树莓派 2026-11-21 温度 45 度，风扇转速正常，运行稳定';
  const res = await store.saveObservation({
    text, entities: ['树莓派'], evidenceIds: [a.id],
  });
  assert.equal(res.created, true);

  const prior = rowOf(store, a.id);
  // 新写入的 status 是 NULL（读侧按 active 兜底），只要没被归档就行
  assert.equal(prior.status === null || prior.status === '' || String(prior.status) === 'active', true,
    '证据不能被写入闸归档');
  assert.equal(prior.supersededBy, null);
  assert.ok(rowOf(store, res.id), '观察行必须落库');
  assert.equal(String(rowOf(store, res.id).text), text, '正文不得被闸改写或丢弃');
});

test('事务回滚：后一步抛错，观察行与证据边都不留', async () => {
  const { store } = makeStore();
  const [a, b] = await seedEvidence(store);
  store.db().exec(
    "CREATE TRIGGER boom BEFORE INSERT ON memory_link BEGIN SELECT RAISE(ABORT, 'boom'); END;",
  );
  const beforeRows = count(store, 'SELECT count(*) AS n FROM memories');
  const beforeSeq = Number(store.db().prepare('SELECT COALESCE(MAX(rowSeq), 0) AS n FROM memories').get().n);
  await assert.rejects(
    store.saveObservation({
      text: '树莓派 2026-11-21 这条会在写边时炸掉', entities: ['树莓派'], evidenceIds: [a.id, b.id],
    }),
    /boom/,
  );
  assert.equal(count(store, 'SELECT count(*) AS n FROM memories'), beforeRows, '观察行必须回滚');
  assert.equal(count(store, 'SELECT count(*) AS n FROM memory_link'), 0, '证据边必须回滚');
  assert.equal(Number(store.db().prepare('SELECT COALESCE(MAX(rowSeq), 0) AS n FROM memories').get().n), beforeSeq);
  assert.equal(store.metas().some((m) => String(m.text).includes('会在写边时炸掉')), false);
});

test('observe_task 缺表：写入照常成功、不报错', async () => {
  const { store } = makeStore({ withObserve: false });
  const [a] = await seedEvidence(store);
  assert.equal(
    count(store, "SELECT count(*) AS n FROM sqlite_master WHERE type='table' AND name='observe_task'"),
    0,
  );
  // enqueueObserve=true 也要静默跳过，不把观察写入带崩
  const res = await store.saveObservation({
    text: '树莓派 2026-11-21 没有 observe_task 也要能落库', entities: ['树莓派'], evidenceIds: [a.id],
  }, { enqueueObserve: true });
  assert.equal(res.created, true);
  assert.ok(rowOf(store, res.id));
  assert.equal(edgesOf(store, res.id).length, 1);
});

test('replacesObservationId：旧观察在同一笔事务里归档', async () => {
  const { store } = makeStore();
  const [a, b] = await seedEvidence(store);
  const first = await store.saveObservation({
    text: '树莓派 2026-11-21 旧观察', entities: ['树莓派'], evidenceIds: [a.id, b.id],
  });
  const second = await store.saveObservation({
    text: '树莓派 2026-11-21 新观察',
    entities: ['树莓派'],
    evidenceIds: [a.id, b.id],
    replacesObservationId: first.id,
  });
  assert.equal(second.created, true);
  assert.equal(second.replaced, first.id);

  const old = rowOf(store, first.id);
  assert.equal(String(old.status), 'archived');
  assert.equal(String(old.supersededBy), second.id);
  assert.equal(String(old.validTo).length, 10, 'validTo 落日期');
  assert.equal(String(rowOf(store, second.id).status), 'active');
  // 内存视图同步，下一次写不会拿旧 active 视图
  const metas = new Map(store.metas().map((m) => [m.id, m]));
  assert.equal(String(metas.get(first.id).status), 'archived');
  assert.equal(String(metas.get(second.id).status), 'active');
});

test('memory_link 缺表：观察行照写，证据边跳过', async () => {
  const { store } = makeStore({ withLink: false });
  const [a] = await seedEvidence(store);
  const res = await store.saveObservation({
    text: '树莓派 2026-11-21 没有 memory_link 也要能落库', entities: ['树莓派'], evidenceIds: [a.id],
  });
  assert.equal(res.created, true);
  assert.ok(rowOf(store, res.id));
  assert.deepEqual(observationEvidence(store.db(), res.id), []);
});
