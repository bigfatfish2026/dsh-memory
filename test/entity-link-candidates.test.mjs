/**
 * 关系候选时序 + links 解析。
 *
 * 钉住四件事：
 *   1. 第一次进队列的记忆，实体落库后能反查到非空候选（以前候选在抽实体前算，恒空）；
 *   2. 没有候选时只调一次模型，提示词层面根本不要求 links；
 *   3. links 解析认 target / dst 别名（以前只认 target_memory_id，其余静默丢）；
 *   4. 非法 rel、i 越界会打 WARN 带原始片段，不再无声消失。
 *
 * 全程离线：假模型、tmpdir 假库、不联网、不碰真库。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { createStore } from '../lib/store.js';
import { applyCurateSchema } from '../scripts/apply-curate-schema.mjs';
import { runEntityQueueOnce } from '../lib/entity-worker.js';
import { LINK_SYSTEM, normalizeLinks } from '../lib/agents/entity.js';

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
  prev_memory_id TEXT NOT NULL DEFAULT '', PRIMARY KEY (memory_id, entity_id, role)) WITHOUT ROWID;
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
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
INSERT INTO meta(key, value) VALUES ('dim', '4');
`;

function makeStore() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-entitylink-'));
  const dbPath = path.join(dir, 'memory.db');
  const db = new DatabaseSync(dbPath, { allowExtension: true });
  db.loadExtension(VEC_SO);
  db.exec(MEMORY_DDL);
  db.exec(ENTITY_DDL);
  db.exec('CREATE VIRTUAL TABLE vec_items USING vec0(embedding float[4] distance_metric=cosine);');
  db.exec('CREATE TABLE vec_map (rowid INTEGER PRIMARY KEY, id TEXT);');
  // 词典只放树莓派：mikan 这种实体只能靠队列 agent 抽，才能复现「第一次进队列时自己没有实体行」。
  db.prepare('INSERT INTO entities(norm, display, etype) VALUES (?, ?, ?)').run('树莓派', '树莓派', 'device');
  db.close();

  applyCurateSchema(dbPath, { logger: { info() {}, warn() {} } });
  const cfg = {
    storePath: dir,
    storeLogFile: path.join(dir, 'store.log'),
    entityQueue: {
      enqueue: true,
      batch: 8,
      leaseMs: 180000,
      genericDf: 0.05,
      genericDfMin: 20,
      coarseMaxWords: 4000,
      coarseBudgetMs: 3,
      logFile: path.join(dir, 'entity.log'),
    },
    apiUrl: 'http://127.0.0.1:1',
    timeoutMs: 50,
    provider: 'fake',
    model: 'fake',
    splitMaxChars: 200,
    consolidateTools: false,
    notifyScript: '/bin/true',
    embed: async (texts) => texts.map(() => [1, 0, 0, 0]),
    logger: { info() {}, warn() {} },
    pipeline: { writeGate: false },
  };
  return { store: createStore(cfg), dbPath, cfg };
}

function rows(dbPath, sql, ...params) {
  const db = new DatabaseSync(dbPath, { readOnly: true });
  try {
    return db.prepare(sql).all(...params);
  } finally {
    db.close();
  }
}

const count = (dbPath, sql, ...params) => Number(rows(dbPath, sql, ...params)[0]?.n ?? 0);

/** 假模型：按 system 分辨两个阶段，把 payload 原样记下来。 */
function fakeLlm({ onExtract, onLinks }) {
  const calls = [];
  return {
    calls,
    stream(opts) {
      const payload = JSON.parse(opts.messages[0].content[0].text);
      const phase = opts.system === LINK_SYSTEM ? 'links' : 'extract';
      calls.push({ phase, payload });
      const reply = phase === 'links' ? onLinks(payload) : onExtract(payload);
      return (async function* fake() {
        yield { type: 'text-delta', text: JSON.stringify(reply) };
      })();
    },
  };
}

test('第一次进队列：实体落库后送到模型的是非空候选，取代边能落库', async () => {
  const { store, dbPath, cfg } = makeStore();
  const old = await store.save('2026-11-01 mikan 番剧订阅是 720p', ['anime'], 'agent', { type: 'world' });
  await store.flushEntityQueue();
  // 旧条第一次进队列：库里没有任何共享实体，只有一次抽实体调用。
  const firstLlm = fakeLlm({
    onExtract: () => ({ items: [{ i: 1, name: 'mikan', etype: 'service', attrs: {} }] }),
    onLinks: () => ({ links: [] }),
  });
  const first = await runEntityQueueOnce({ store, cfg, llm: firstLlm, log: () => {} });
  assert.equal(first.claimed, 1);
  assert.equal(firstLlm.calls.length, 1, '没有候选就不发第二次调用');
  assert.equal(count(dbPath, "SELECT count(*) AS n FROM memory_entity WHERE memory_id=? AND origin='agent'", old.id), 1);

  // 新条第一次进队列：自己还没有实体行，但候选必须来自「实体落库之后」的反查。
  const fresh = await store.save('2026-11-10 mikan 番剧订阅改成 1080p', ['anime'], 'agent', { type: 'world' });
  await store.flushEntityQueue();
  const llm = fakeLlm({
    onExtract: () => ({ items: [{ i: 1, name: 'mikan', etype: 'service', attrs: {} }] }),
    onLinks: () => ({ links: [{ i: 1, rel: 'supersedes', dst: old.id }] }),
  });
  const out = await runEntityQueueOnce({ store, cfg, llm, log: () => {} });
  assert.equal(out.linked, 1, '判出取代就落一条边');
  assert.equal(llm.calls.length, 2, '抽实体一次 + 判关系一次');
  assert.equal(llm.calls[0].phase, 'extract');
  assert.equal('candidates' in llm.calls[0].payload.items[0], false, '抽实体阶段还不能带候选');

  const linkCall = llm.calls.find((c) => c.phase === 'links');
  assert.ok(linkCall, '第二阶段必须真的被调用');
  const cands = linkCall.payload.items[0].candidates;
  assert.ok(cands.length >= 1, '第一次进队列也要拿到非空候选');
  assert.equal(cands[0].id, old.id, '候选是共享实体 mikan 的旧条');
  assert.ok(String(cands[0].text).length <= 120);

  const edge = rows(dbPath, 'SELECT src, dst, rel, origin FROM memory_link').map((r) => ({ ...r }));
  assert.deepEqual(edge, [{ src: fresh.id, dst: old.id, rel: 'supersedes', origin: 'queue' }]);
});

test('候选为空：只调一次模型，提示词层面不要求 links', async () => {
  const { store, dbPath, cfg } = makeStore();
  const mem = await store.save('2026-11-10 mikan 番剧订阅改成 1080p', ['anime'], 'agent', { type: 'world' });
  await store.flushEntityQueue();
  const llm = fakeLlm({
    onExtract: () => ({ items: [{ i: 1, name: 'mikan', etype: 'service', attrs: {} }] }),
    onLinks: () => ({ links: [{ i: 1, rel: 'supersedes', target_memory_id: 'm_ghost' }] }),
  });
  const out = await runEntityQueueOnce({ store, cfg, llm, log: () => {} });
  assert.equal(out.accepted, 1, '实体照抽');
  assert.equal(out.linked, 0);
  assert.equal(llm.calls.length, 1, '没有候选就不发第二次调用');
  assert.equal(llm.calls.some((c) => c.phase === 'links'), false, '关系判定那次调用不发生');
  assert.equal('candidates' in llm.calls[0].payload.items[0], false, 'payload 里没有 candidates 键');
  assert.equal(count(dbPath, 'SELECT count(*) AS n FROM memory_link'), 0);
  assert.equal(count(dbPath, "SELECT count(*) AS n FROM memory_entity WHERE memory_id=? AND origin='agent'", mem.id), 1);
});

test('links 解析：target / dst 别名都认', () => {
  const out = normalizeLinks(
    [
      { i: 1, rel: 'supersedes', target: 'm_old1' },
      { i: 2, rel: 'supersedes', dst: 'm_old2' },
      { i: 3, rel: 'supersedes', target_memory_id: 'm_old3' },
    ],
    { itemCount: 3 },
  );
  assert.deepEqual(out.map((l) => l.target_memory_id), ['m_old1', 'm_old2', 'm_old3']);
  assert.deepEqual(out.map((l) => l.i), [1, 2, 3]);
});

test('links 解析：非法 rel / i 越界 / supersedes 缺 target 都 WARN 且不静默', () => {
  const lines = [];
  const out = normalizeLinks(
    [
      { i: 1, rel: 'replaces', target_memory_id: 'm_a' },
      { i: 9, rel: 'parallel', target_memory_id: '' },
      { i: 2, rel: 'supersedes', target_memory_id: '' },
      { i: 3, rel: 'parallel', target_memory_id: '' },
    ],
    { itemCount: 3, log: (line) => lines.push(line) },
  );
  assert.deepEqual(out, [{ i: 3, rel: 'parallel', target_memory_id: '' }], '只剩合法那一条');
  assert.equal(lines.length, 3, '三种丢弃各一行 WARN');
  assert.ok(lines.every((l) => l.includes('[WARN]')), '都是 WARN');
  assert.ok(lines[0].includes('replaces') && lines[0].includes('raw='), '非法 rel 带原始片段');
  assert.ok(lines[1].includes('link.i=9'), 'i 越界带原始值');
  assert.ok(lines[2].includes('supersedes') && lines[2].includes('target'), '缺 target 说清');
});
