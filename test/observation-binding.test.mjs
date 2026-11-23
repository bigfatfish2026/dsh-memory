/**
 * 证据绑定口径统一（t-mwsuyr33-2qmv）：取证据与事务复验共用 evidenceBindingOf。
 *
 * 钉的边界：
 *   ① 空实体列 + 正文整词明确命中 → 接受（旧行不再被 assertEvidence 误拒）；
 *   ② 空实体列 + 正文只泛泛提及（裸子串）→ 照旧拒绝；
 *   ③ 空实体列 + 命中歧义（片段过短 / 同一词命中多个候选实体）→ 不拒不放行，转 pending_review；
 *   ④ 有实体列的老路径不变：只认精确键交集，正文提到也不算；
 *   ⑤ 带进来的 basis 不能伪造通过：事务重新按行判定。
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
import { evidenceBindingOf } from '../lib/sleep/observation-sources.js';
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
INSERT INTO entities(id, norm, display) VALUES (1, '树莓派', '树莓派'), (2, 'vec_items', 'vec_items');
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
INSERT INTO meta(key, value) VALUES ('dim', '4');
`;

/** 临时库 + 真 store。 */
function makeStore() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-observation-binding-'));
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
    observe: { enabled: false },
  };
  return { store: createStore(cfg), dir };
}

/** 直接插一行证据：entities 传 null 就是空实体列的老记录（不走 store.save，免得粗实体回填）。 */
function insertEvidence(store, { id, text, type = 'world', entities = null }) {
  const db = store.db();
  const seq = Number(db.prepare('SELECT COALESCE(MAX(rowSeq), 0) + 1 AS n FROM memories').get().n);
  db.prepare(
    'INSERT INTO memories(id, text, type, status, entities, ts, rowSeq) VALUES (?, ?, ?, NULL, ?, ?, ?)',
  ).run(id, text, type, entities, '2026-11-01T00:00:00.000Z', seq);
}

function count(store, sql, ...params) {
  return Number(store.db().prepare(sql).get(...params)?.n || 0);
}

function edgesOf(store, id) {
  return store.db().prepare('SELECT dst, rel FROM memory_link WHERE src = ? ORDER BY dst').all(id);
}

test('空实体列 + 正文整词明确命中：接受，观察进 active，证据边照写', async () => {
  const { store } = makeStore();
  // 真库同形：vec_items 是实体，旧记录实体列为空但正文整词直指它
  insertEvidence(store, { id: 'm_old_vec', text: 'memory/memory.db 的 vec_items 共 5308 条 1024 维向量' });
  insertEvidence(store, { id: 'm_keyed_vec', text: 'vec_items 存 embedding，vec_map 存 id', entities: JSON.stringify(['vec_items']) });

  const res = await store.saveObservation({
    text: 'vec_items 是 sqlite-vec 的向量表，行数要按 vec_map 对',
    entities: ['vec_items'],
    evidenceIds: ['m_old_vec', 'm_keyed_vec'],
  });

  assert.equal(res.created, true);
  assert.equal(res.status, 'active', '明确命中不得因为实体列为空被拒');
  assert.deepEqual(res.pendingReviewEvidence, []);
  assert.deepEqual(res.evidenceIds, ['m_old_vec', 'm_keyed_vec']);
  assert.equal(edgesOf(store, res.id).length, 2, '两条证据边都要落');
});

test('空实体列 + 正文只泛泛提及（裸子串）：照旧拒绝，零写入', async () => {
  const { store } = makeStore();
  // 'read' 只出现在 'readonly' 里，不是整词；观察实体就是 read
  insertEvidence(store, { id: 'm_generic', text: 'readonly 打开数据库，没有写权限' });
  const before = count(store, 'SELECT count(*) AS n FROM memories');

  await assert.rejects(
    store.saveObservation({
      text: 'read 这个键被泛泛提到，不该算绑定',
      entities: ['read'],
      evidenceIds: ['m_generic'],
    }),
    (err) => err.code === 'EVIDENCE_ENTITY_MISMATCH' && err.evidenceId === 'm_generic',
  );
  assert.equal(count(store, 'SELECT count(*) AS n FROM memories'), before, '拒绝时一行都不留');
});

test('空实体列 + 片段过短（歧义）：不拒，转 pending_review + needsModelReview', async () => {
  const { store } = makeStore();
  // 正文只有实体词本身，定位不到上下文
  insertEvidence(store, { id: 'm_tiny', text: 'vec_items' });
  const res = await store.saveObservation({
    text: 'vec_items 的片段太短，先挂待复核',
    entities: ['vec_items'],
    evidenceIds: ['m_tiny'],
  });

  assert.equal(res.created, true);
  assert.equal(res.status, 'pending_review', '歧义不直接放行：初态进待复核');
  assert.deepEqual(res.pendingReviewEvidence, ['m_tiny']);
  const row = store.db().prepare('SELECT status, extra FROM memories WHERE id = ?').get(res.id);
  assert.equal(String(row.status), 'pending_review');
  assert.equal(JSON.parse(row.extra).needsModelReview, true, '复核链路靠这个标记决定要不要问模型');
  assert.equal(edgesOf(store, res.id).length, 1, '证据边照写，复核时还要用');
});

test('空实体列 + 同一词命中多个候选实体（歧义）：转 pending_review', async () => {
  const { store } = makeStore();
  insertEvidence(store, { id: 'm_two', text: 'alpha 和 beta 都在同一段正文里，分不清这条证据说的是谁' });
  const res = await store.saveObservation({
    text: 'alpha 与 beta 的关系待复核',
    entities: ['alpha', 'beta'],
    evidenceIds: ['m_two'],
  });

  assert.equal(res.status, 'pending_review');
  assert.deepEqual(res.pendingReviewEvidence, ['m_two']);
  const basis = evidenceBindingOf(
    { id: 'm_two', text: 'alpha 和 beta 都在同一段正文里，分不清这条证据说的是谁', entities: [] },
    'alpha',
  );
  assert.equal(basis.rule, 'text');
  assert.equal(basis.ambiguous, false, '单看一个实体键不歧义；歧义由事务侧对候选实体集合判定');
});

test('有实体列的老路径不变：正文提到也不算，照旧拒绝', async () => {
  const { store } = makeStore();
  insertEvidence(store, {
    id: 'm_keyed_other',
    text: '树莓派 第二天重启后温度回到 45 度',
    entities: JSON.stringify(['dsh-ego']),
  });
  await assert.rejects(
    store.saveObservation({
      text: '树莓派 的温度观察',
      entities: ['树莓派'],
      evidenceIds: ['m_keyed_other'],
    }),
    (err) => err.code === 'EVIDENCE_ENTITY_MISMATCH',
  );
});

test('带进来的 basis 不能伪造通过：事务按最新行重新判定', async () => {
  const { store } = makeStore();
  insertEvidence(store, { id: 'm_plain', text: 'readonly 打开数据库，没有写权限' });
  await assert.rejects(
    store.saveObservation({
      text: '伪造 basis 不该放行',
      entities: ['read'],
      evidenceIds: [{
        id: 'm_plain',
        basis: {
          rule: 'exact', entityNorm: 'read', word: 'read', snippet: '',
          entityKeys: ['read'], ambiguous: false, ambiguousReason: '',
        },
      }],
    }),
    (err) => err.code === 'EVIDENCE_ENTITY_MISMATCH',
  );

  // 真正带 text 依据的旧行照样通过，结果里能看到依据带来的状态
  insertEvidence(store, { id: 'm_real', text: 'memory/memory.db 的 vec_items 共 5308 条' });
  const basis = evidenceBindingOf(
    { id: 'm_real', text: 'memory/memory.db 的 vec_items 共 5308 条', entities: [] },
    'vec_items',
  );
  assert.equal(basis.rule, 'text');
  const res = await store.saveObservation({
    text: 'vec_items 共 5308 条，这是实测',
    entities: ['vec_items'],
    evidenceIds: [{ id: 'm_real', basis }],
  });
  assert.equal(res.status, 'active');
});
