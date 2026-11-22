/**
 * observation 可见性（2026-11-21，t-mwqeckwi-acqs）。
 *
 * 钉四件事：
 *   ① 默认召回剔除 isHiddenObservation 的自动观察（pending_review），active 的自动观察照常出现；
 *   ② 手工 observation（extra 无 autoObservation）行为不变：非 active 仍是 0.3 权重影子；
 *   ③ memory_entity 的计数与列表只算可见观察，observation 条目带 derived_from 证据 id，
 *      渲染出「证据：m_x, m_y（2 条独立来源）」；
 *   ④ prefill 注入缓存按观察版本号失效：观察状态变化后同一条消息不再注入旧结论。
 *
 * 全程离线：临时库 + 假 embedding，模型一次不调。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { createStore, registerStoreTools } from '../lib/store.js';
import { setupAutoPrefill } from '../lib/prefill.js';
import { applyCurateSchema } from '../scripts/apply-curate-schema.mjs';

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
INSERT INTO entities(id, norm, display) VALUES (1, '树莓派', '树莓派'), (2, '风扇', '风扇');
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
INSERT INTO meta(key, value) VALUES ('dim', '4');
`;

/** 4 维单位向量：第一维就是余弦。 */
const vecOf = (cos) => [cos, Math.sqrt(Math.max(0, 1 - cos * cos)), 0, 0];

/**
 * 临时库 + 真 store。
 * @param {object} opts.seeds 直接写进 memories + 向量索引的种子行
 * @param {boolean} opts.withLink 是否建 memory_link（观察证据边要它）
 */
function makeStore({ seeds = [], withLink = false } = {}) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-observation-visibility-'));
  const dbPath = path.join(dir, 'memory.db');
  const db = new DatabaseSync(dbPath, { allowExtension: true });
  db.loadExtension(VEC_SO);
  db.exec(MEMORY_DDL);
  db.exec(ENTITY_DDL);
  db.exec('CREATE VIRTUAL TABLE vec_items USING vec0(embedding float[4] distance_metric=cosine);');
  db.exec('CREATE TABLE vec_map (rowid INTEGER PRIMARY KEY, id TEXT);');
  const insertMemory = db.prepare(
    `INSERT INTO memories (id, text, tags, ts, type, entities, status, reason, extra, rowSeq)
     VALUES (?, ?, '[]', ?, ?, ?, ?, ?, ?, ?)`,
  );
  const insertVecMap = db.prepare('INSERT INTO vec_map (rowid, id) VALUES (?, ?)');
  const insertVec = db.prepare('INSERT INTO vec_items (rowid, embedding) VALUES (?, ?)');
  seeds.forEach((row, idx) => {
    insertMemory.run(
      row.id,
      row.text,
      row.ts || '2026-11-21T00:00:00Z',
      row.type || 'world',
      JSON.stringify(row.entities || []),
      row.status || 'active',
      row.reason || null,
      row.autoObservation ? JSON.stringify({ autoObservation: true, version: 1 }) : null,
      idx + 1,
    );
    insertVecMap.run(idx + 1, row.id);
    insertVec.run(BigInt(idx + 1), new Uint8Array(Float32Array.from(vecOf(row.cos)).buffer));
  });
  db.close();
  if (withLink) applyCurateSchema(dbPath, { logger: { info() {}, warn() {} } });

  const cfg = {
    storePath: dir,
    storeLogFile: path.join(dir, 'store.log'),
    apiUrl: 'http://127.0.0.1:1',
    timeoutMs: 50,
    provider: 'fake',
    model: 'fake',
    topK: 8,
    minScore: 0.45,
    relativeGap: 0.1,
    rrfK: 60,
    embed: async (texts) => texts.map(() => [1, 0, 0, 0]),
    logger: { info() {}, warn() {} },
    pipeline: { textLint: false, writeGate: false },
  };
  return { store: createStore(cfg), cfg, dir, dbPath };
}

test('默认召回：pending_review 的自动观察不出现，active 的自动观察出现；管理视图能看见', async () => {
  const activeText = '树莓派 2026-11-21 运行稳定，温度与内存都正常';
  const reviewText = '树莓派 2026-11-21 温度偏高，等待复核后再采信';
  const { store } = makeStore({
    seeds: [
      { id: 'm_ev1', text: '树莓派 2026-11-21 温度 45 度，风扇转速正常', entities: ['树莓派'], cos: 0.9 },
      { id: 'm_obs_ok', text: activeText, type: 'observation', entities: ['树莓派'], autoObservation: true, cos: 0.95 },
      { id: 'm_obs_review', text: reviewText, type: 'observation', entities: ['树莓派'], autoObservation: true, status: 'pending_review', cos: 0.95 },
    ],
  });

  const hits = await store.recall('树莓派温度和内存怎么样', 8);
  const texts = hits.map((it) => it.text);
  assert.ok(texts.includes(activeText), 'active 的自动观察必须在默认召回里');
  assert.ok(!texts.includes(reviewText), 'pending_review 的自动观察不得进默认召回');

  const admin = await store.recall('树莓派温度和内存怎么样', 8, { includeHidden: true });
  assert.ok(admin.map((it) => it.text).includes(reviewText), 'includeHidden 的管理视图要能看到待复核观察');
});

test('手工 observation 行为不变：archived 仍按非 active 0.3 权重留影子', async () => {
  const manualText = '风扇转速三千转，散热片是铜的';
  const { store } = makeStore({
    seeds: [
      { id: 'm_world', text: '磁盘空间还剩百分之十五，暂时不用清', cos: 0.7 },
      // 手工写的观察：没有 extra.autoObservation，archived 也只是影子
      { id: 'm_manual', text: manualText, type: 'observation', status: 'archived', cos: 0.95 },
    ],
  });

  const hits = await store.recall('风扇转速和散热片材质', 8);
  assert.equal(hits.length, 2, '手工 archived 观察不得被隐藏过滤剔除');
  assert.equal(hits[0].status, 'active', 'active 的普通记忆仍排在前');
  assert.equal(hits.at(-1).status, 'archived', '非 active 的排在最后当影子');
  assert.equal(hits.at(-1).score, 0.95, '影子余弦保留（0.3 只压融合分，不改 score）');
});

test('memory_entity：pending_review 不计数不列出；active 观察带证据 id 并渲染证据链', async () => {
  const { store } = makeStore({ withLink: true });
  const a = await store.save('2026-11-21 树莓派温度 45 度，风扇转速正常', ['infra'], 'agent', {
    type: 'world', entities: ['树莓派'],
  });
  const b = await store.save('2026-11-21 树莓派连续跑了 12 小时，内存占用 3G', ['infra'], 'agent', {
    type: 'world', entities: ['树莓派'],
  });
  assert.ok(!a.discarded && !b.discarded);

  const activeText = '树莓派 2026-11-21 温度与内存都在正常范围，连续运行稳定';
  const reviewText = '树莓派 2026-11-21 温度曲线异常，待复核';
  await store.saveObservation({
    text: activeText, entities: ['树莓派'], evidenceIds: [a.id, b.id], confidence: 0.85,
  });
  await store.saveObservation({
    text: reviewText, entities: ['树莓派'], evidenceIds: [a.id, b.id],
  }, { status: 'pending_review' });

  const view = store.entityView('树莓派');
  assert.equal(view.counts.observation, 1, 'observation 计数只算可见观察');
  assert.equal(view.total, 3, 'world 两条 + active 观察一条');
  assert.ok(!view.items.some((it) => it.text === reviewText), 'pending_review 不列出');
  assert.ok(view.items.some((it) => it.text === activeText), 'active 观察要列出');

  const tools = [];
  const ctx = { tools: { register: (t) => tools.push(t) }, logger: { info() {}, warn() {} } };
  registerStoreTools(ctx, store, { logger: { info() {}, warn() {} } });
  const tool = tools.find((t) => t.name === 'memory_entity');
  const out = await tool.execute({ name: '树莓派' }, {});
  assert.equal(out.observation, 1);
  const obsItem = out.items.find((it) => it.type === 'observation');
  assert.equal(obsItem.text, activeText);
  assert.deepEqual(obsItem.evidence.slice().sort(), [a.id, b.id].sort(), '观察条目要带 derived_from 证据 id');

  const rendered = tool.output.render({}, out)[0].text;
  assert.match(rendered, new RegExp(`证据：${obsItem.evidence.join(', ')}（2 条独立来源）`));
  assert.ok(!rendered.includes(reviewText), '待复核观察连渲染也不许出现');
});

test('prefill 缓存：观察版本不变时命中；版本一变同一条消息重新召回、不再注入旧结论', async () => {
  const firstText = '树莓派 2026-11-21 温度正常，风扇转速稳定';
  let epoch = 1;
  let items = [{ id: 'm_obs', text: firstText, type: 'observation', score: 0.9 }];
  let recalls = 0;
  const store = {
    async recall() {
      recalls += 1;
      return items;
    },
    listAll: () => [],
    observationVersion: () => epoch,
  };

  let handler;
  const ctx = { on(event, fn) { if (event === 'system-prompt/assemble') handler = fn; }, logger: { info() {} } };
  const logFile = path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-observation-prefill-')), 'prefill.log');
  const handle = setupAutoPrefill(ctx, store, { autoPrefillLogPath: logFile });
  assert.equal(typeof handler, 'function', 'assemble 钩子没挂上');

  const message = '树莓派今天的温度和风扇转速怎么样，需要处理吗？';
  const session = {
    id: 's-observation-cache',
    header: { origin: 'user' },
    deriveMessages: () => [{ role: 'user', content: [{ type: 'text', text: message }] }],
  };
  const run = async () => {
    const sections = [];
    await handler({ sections }, { agent: { session } }, async () => ({ sections }));
    return sections.find((s) => s.name === 'memory-auto-prefill')?.text || '';
  };

  const first = await run();
  assert.match(first, new RegExp(firstText));
  assert.equal(recalls, 1);
  assert.equal(handle.stats.injected, 1);

  // 版本没变：命中缓存，不重新查库（哪怕召回结果已经变了）
  items = [];
  const cached = await run();
  assert.equal(recalls, 1, '同一版本同一消息必须走缓存');
  assert.match(cached, new RegExp(firstText));
  assert.equal(handle.stats.cached, 1);

  // 观察状态变化（模拟证据失效 → pending_review）：版本 +1，旧缓存不再命中
  epoch = 2;
  const invalidated = await run();
  assert.equal(recalls, 2, '版本变化后必须重新召回');
  assert.equal(invalidated, '', '旧结论不许再注入');
  assert.equal(handle.cache.size, 2, '新版本写新 key，旧 key 不再被读到');
});
