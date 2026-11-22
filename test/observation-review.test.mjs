/**
 * 证据变更时的 observation 就地复核（src/store/observation-review.ts，t-mwqermos-ybm1）。
 *
 * 钉的边界：
 *   ① 证据归档后独立来源不足 2 条 → archived + reason='evidence-archive'，epoch +1；
 *   ② 证据归档但还剩 2 个独立来源 → pending_review；
 *   ③ 合并：证据边改指后继（后继 active 且实体相交），后继不活则删边；
 *   ④ 正文改写 → pending_review，证据边不动，needsModelReview=true；
 *   ⑤ 来源根去重：同一原条切出的两片只算一个来源，归档一片后仍按 1 条算；
 *   ⑥ 手工观察（无 autoObservation）不被碰；
 *   ⑦ 防循环：观察自己被归档时不登记它的实体（observe_task 不新增行）；
 *   ⑧ 没有 derived_from 边时无操作、不报错；
 *   ⑨ 独立解挂（entity-graph.deleteMemoryEntity）→ pending_review，边保留。
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
import { reviewObservationsOnEvidenceChange } from '../lib/store/observation-review.js';
import { entityGraphOf } from '../lib/entity-gate.js';
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
INSERT INTO entities(id, norm, display, etype) VALUES (1, '树莓派', '树莓派', 'device'), (2, 'alpha', 'Alpha', 'person'), (3, 'beta', 'Beta', 'person');
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
INSERT INTO meta(key, value) VALUES ('dim', '4');
`;

/** 临时库 + 真 store；observe/pipeline 开关按测试需要打开。 */
function makeStore() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-observation-review-'));
  const dbPath = path.join(dir, 'memory.db');
  const db = new DatabaseSync(dbPath, { allowExtension: true });
  db.loadExtension(VEC_SO);
  db.exec(MEMORY_DDL);
  db.exec(ENTITY_DDL);
  db.exec('CREATE VIRTUAL TABLE vec_items USING vec0(embedding float[4] distance_metric=cosine);');
  db.exec('CREATE TABLE vec_map (rowid INTEGER PRIMARY KEY, id TEXT);');
  db.close();
  applySchema(dbPath, { logger: () => {} });
  applyCurateSchema(dbPath, { logger: { info() {}, warn() {} } });
  applyObserveSchema(dbPath, { logger: () => {} });

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

function extraOf(row) {
  return row?.extra ? JSON.parse(row.extra) : {};
}

function edgesOf(store, id) {
  return store.db().prepare(
    'SELECT dst, rel, origin FROM memory_link WHERE src = ? ORDER BY dst',
  ).all(id).map((row) => String(row.dst));
}

function queueRows(store) {
  return store.db().prepare(
    'SELECT entity_key, reason, version FROM observe_task ORDER BY entity_key',
  ).all();
}

/** 证据落库前先清空 observe_task：seed 的 new-memory 登记不参与本测试的断言。 */
function clearQueue(store) {
  store.db().exec('DELETE FROM observe_task');
}

/** n 条同一实体的 active 证据。 */
async function seedEvidence(store, n = 2) {
  const out = [];
  for (let i = 0; i < n; i += 1) {
    const res = await store.save(`2026-11-21 树莓派证据 ${i + 1} 号，温度 ${40 + i} 度`, ['infra'], 'agent', {
      type: 'world', entities: ['树莓派'],
    });
    assert.ok(!res.discarded, '证据必须落库成功');
    out.push(res);
  }
  return out;
}

async function seedObservation(store, evidenceIds, text = '树莓派 2026-11-21 运行稳定，温度和负载都正常') {
  const res = await store.saveObservation({ text, entities: ['树莓派'], evidenceIds });
  assert.equal(res.created, true, '观察必须新建');
  return res;
}

test('证据归档且只剩 1 个独立来源：观察 archived、reason 与 epoch 正确、队列有登记', async () => {
  const { store } = makeStore();
  const [a, b] = await seedEvidence(store, 2);
  const obs = await seedObservation(store, [a.id, b.id]);
  const epochBefore = store.observationVersion();
  clearQueue(store);

  await store.archive(a.id, '过期了');

  const row = rowOf(store, obs.id);
  assert.equal(String(row.status), 'archived', '独立来源不足 2 条必须归档');
  assert.equal(String(row.reason), 'evidence-archive');
  assert.deepEqual(edgesOf(store, obs.id), [b.id], '失效证据的边必须删掉');
  assert.equal(store.observationVersion(), epochBefore + 1, '任何状态变化都要 bump 观察版本');
  assert.equal(count(store, 'SELECT count(*) AS n FROM observe_task'), 1, '归档写路径仍要登记实体');
  assert.equal(String(queueRows(store)[0].entity_key), '树莓派');
});

test('证据归档但还有 2 个独立来源：转 pending_review，并登记 review 任务', async () => {
  const { store } = makeStore();
  const [a, b, c] = await seedEvidence(store, 3);
  const obs = await seedObservation(store, [a.id, b.id, c.id]);
  const epochBefore = store.observationVersion();
  clearQueue(store);

  await store.archive(a.id, '过期了');

  const row = rowOf(store, obs.id);
  assert.equal(String(row.status), 'pending_review');
  assert.equal(String(row.reason), 'evidence-archive');
  assert.deepEqual(edgesOf(store, obs.id), [b.id, c.id]);
  assert.equal(extraOf(row).needsModelReview, undefined, '归档不额外标 needsModelReview');
  assert.equal(store.observationVersion(), epochBefore + 1);
  assert.equal(queueRows(store).length, 1);
  assert.equal(String(queueRows(store)[0].reason), 'review');
  assert.equal(count(store, "SELECT count(*) AS n FROM observe_task WHERE status='pending'"), 1);
});

test('合并：证据边 dst 改成后继，后继 active；后继不活则删边', async () => {
  const { store } = makeStore();
  const [a, b] = await seedEvidence(store, 2);
  const obs = await seedObservation(store, [a.id, b.id]);
  clearQueue(store);

  const merged = await store.merge([a.id, b.id], '2026-11-21 树莓派两条证据合并成一条', ['infra'], {
    type: 'world', entities: ['树莓派'],
  });
  assert.ok(!merged.discarded, '合并必须落库');

  const mergedRow = rowOf(store, merged.id);
  assert.ok(mergedRow.status === null || mergedRow.status === '' || String(mergedRow.status) === 'active',
    '后继必须 active（NULL 按 active 读）');
  assert.deepEqual(edgesOf(store, obs.id), [merged.id], '两条边都指向同一个后继，只留一条');
  const row = rowOf(store, obs.id);
  assert.equal(String(row.status), 'pending_review');
  assert.equal(String(row.reason), 'evidence-merge');
  // 后继来源根是 a/b 两条 → 独立来源仍是 2
  assert.equal(queueRows(store).length, 1);
});

test('合并后继被归档：删边而不是改指死条', async () => {
  const { store } = makeStore();
  const [a, b] = await seedEvidence(store, 2);
  const dead = await seedEvidence(store, 1);
  await store.archive(dead[0].id, '先归档，等会儿当不合法后继');
  const obs = await seedObservation(store, [a.id, b.id]);
  clearQueue(store);

  const result = reviewObservationsOnEvidenceChange(store.db(), {
    kind: 'merge',
    evidenceIds: [a.id],
    successorOf: { [a.id]: dead[0].id },
  });

  assert.equal(result.reviewed, 1);
  assert.deepEqual(edgesOf(store, obs.id), [b.id], '后继不 active，旧边必须删掉');
  assert.equal(String(rowOf(store, obs.id).reason), 'evidence-merge');
});

test('正文改写：pending_review、证据边不动、needsModelReview=true', async () => {
  const { store } = makeStore();
  const [a, b] = await seedEvidence(store, 2);
  const obs = await seedObservation(store, [a.id, b.id]);
  const epochBefore = store.observationVersion();
  clearQueue(store);

  await store.update(a.id, { text: '2026-11-21 树莓派温度 46 度，风扇转速略有波动' });

  const row = rowOf(store, obs.id);
  assert.equal(String(row.status), 'pending_review');
  assert.equal(String(row.reason), 'evidence-rewrite');
  assert.deepEqual(edgesOf(store, obs.id), [a.id, b.id], '改写不改边');
  assert.equal(extraOf(row).needsModelReview, true);
  assert.equal(extraOf(row).autoObservation, true, '原有 extra 标记必须保留');
  assert.equal(store.observationVersion(), epochBefore + 1);
  assert.equal(String(queueRows(store)[0].reason), 'review');
});

test('来源根去重：同一原条两片只算 1 个来源，归档一片后仍归档', async () => {
  const { store } = makeStore();
  const [a, b] = await seedEvidence(store, 2);
  // 模拟拆分：两片来源都指向同一个父条 id
  store.db().prepare('UPDATE memories SET source = ? WHERE id IN (?, ?)')
    .run('sleep:split:m_parent_1', a.id, b.id);
  const obs = await seedObservation(store, [a.id, b.id]);
  clearQueue(store);

  await store.archive(a.id, '原条过期');

  const row = rowOf(store, obs.id);
  assert.equal(String(row.status), 'archived', '两片同根只能算 1 条独立来源');
  assert.equal(String(row.reason), 'evidence-archive');
  assert.deepEqual(edgesOf(store, obs.id), [b.id]);
});

test('手工观察（无 autoObservation）不被碰', async () => {
  const { store } = makeStore();
  const [a, b] = await seedEvidence(store, 2);
  const obs = await seedObservation(store, [a.id, b.id]);
  // 去掉自动标记：模拟手工写的 observation（仍保留证据边）
  store.db().prepare('UPDATE memories SET extra = NULL WHERE id = ?').run(obs.id);
  clearQueue(store);

  await store.archive(a.id, '过期了');

  const row = rowOf(store, obs.id);
  assert.equal(String(row.status), 'active', '手工观察状态不许被复核改');
  assert.equal(extraOf(row).needsModelReview, undefined);
  assert.deepEqual(edgesOf(store, obs.id), [a.id, b.id], '手工观察的边也不动');
});

test('防循环：观察自己被归档时不产生新的 review 登记', async () => {
  const { store } = makeStore();
  const [a, b] = await seedEvidence(store, 2);
  const obs = await seedObservation(store, [a.id, b.id]);
  clearQueue(store);

  const result = reviewObservationsOnEvidenceChange(store.db(), {
    kind: 'archive',
    evidenceIds: [a.id, b.id],
  });

  assert.equal(result.reviewed, 1);
  assert.equal(result.archived, 1);
  assert.equal(result.pending, 0);
  assert.equal(String(rowOf(store, obs.id).status), 'archived');
  assert.equal(count(store, 'SELECT count(*) AS n FROM observe_task'), 0, '观察自身归档时不许再登记实体');
});

test('没有 derived_from 边：无操作、不报错', async () => {
  const { store } = makeStore();
  const [a] = await seedEvidence(store, 1);
  clearQueue(store);

  const result = reviewObservationsOnEvidenceChange(store.db(), {
    kind: 'archive',
    evidenceIds: [a.id],
  });

  assert.deepEqual(
    { reviewed: result.reviewed, archived: result.archived, pending: result.pending },
    { reviewed: 0, archived: 0, pending: 0 },
  );
  assert.equal(count(store, 'SELECT count(*) AS n FROM observe_task'), 0);
  assert.equal(String(rowOf(store, a.id).status || 'active'), 'active');
});

test('独立解挂：边保留但转 pending_review，needsModelReview=true', async () => {
  const { store } = makeStore();
  const [a, b] = await seedEvidence(store, 2);
  const obs = await seedObservation(store, [a.id, b.id]);
  const epochBefore = store.observationVersion();
  clearQueue(store);
  // 显式 entities 的 save 不落 memory_entity（粗实体为空），这里直接补一行挂载
  store.db().prepare(
    `INSERT INTO memory_entity(memory_id, entity_id, role, ts, status, attrs, version, prev_memory_id, origin)
     VALUES (?, 1, 'about', '', 'active', '{}', 0, '', 'rule')`,
  ).run(a.id);

  const removed = entityGraphOf(store.db()).deleteMemoryEntity({ memoryId: a.id });
  assert.equal(removed > 0, true, 'a 必须先有挂载');

  const row = rowOf(store, obs.id);
  assert.equal(String(row.status), 'pending_review');
  assert.equal(String(row.reason), 'evidence-unmount');
  assert.deepEqual(edgesOf(store, obs.id), [a.id, b.id], '解挂不改证据边');
  assert.equal(extraOf(row).needsModelReview, true);
  assert.equal(store.observationVersion(), epochBefore + 1);
});
