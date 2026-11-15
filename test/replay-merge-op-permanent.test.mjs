/**
 * merge_op 重放的收敛口径。
 *
 * 背景：merge_op 账本里躺的都是**当初就被判 invalid** 的 op（merge.js 的 recordInvalidOps）。
 * 旧重放逻辑拿现库正文再验一遍，结果必然还是同一个 invalid，但每轮照走 attempts+1，
 * 5 轮 × 每条白推一遍，20:00 那轮就有 11 条这么空转（4 条推成 dead）。
 *
 * 三条新口径，各一条测试钉住：
 *   1. 拿入队时的 source 快照再验，原因和 payload.reason 一样 → 确定性 invalid，当场 dead，
 *      last_error=replay_invalid_permanent <reason>，不再耗 attempts；
 *   2. 快照能过、现库过不了（源条被改/并走）→ dead，last_error=replay_source_drift，不空转；
 *   3. 原因和当初不一样（上下文真变了）→ 维持老行为：pending + attempts+1，下一轮再试。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { applySchema } from '../scripts/apply-pipeline-schema.mjs';
import { applyCurateSchema } from '../scripts/apply-curate-schema.mjs';
import { createStore } from '../lib/store.js';
import { setupSleep } from '../lib/sleep.js';
import { enqueue } from '../lib/ingest-index.js';

const VEC_SO = path.join(import.meta.dirname, '../node_modules/sqlite-vec-linux-arm64/vec0.so');
const MEMORY_DDL = `CREATE TABLE memories (
  id TEXT PRIMARY KEY, text TEXT NOT NULL, tags TEXT, source TEXT, ts TEXT, type TEXT,
  entities TEXT, validFrom TEXT, supersedes TEXT, updatedAt TEXT, status TEXT, reason TEXT,
  supersededBy TEXT, validTo TEXT, confidence REAL, extra TEXT, present TEXT, rowSeq INTEGER NOT NULL
);`;

/** applyCurateSchema 要给 memory_entity 加 origin 列，实体表得先有。 */
const ENTITY_DDL = `
CREATE TABLE entities (id INTEGER PRIMARY KEY, norm TEXT NOT NULL, display TEXT NOT NULL,
  etype TEXT NOT NULL DEFAULT 'unknown', aliases_note TEXT NOT NULL DEFAULT '',
  attrs TEXT NOT NULL DEFAULT '{}', first_seen TEXT NOT NULL DEFAULT '', last_seen TEXT NOT NULL DEFAULT '',
  mem_count INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'active', UNIQUE (norm));
CREATE TABLE memory_entity (memory_id TEXT NOT NULL, entity_id INTEGER NOT NULL, role TEXT NOT NULL DEFAULT 'about',
  ts TEXT NOT NULL DEFAULT '', status TEXT NOT NULL DEFAULT 'active', attrs TEXT NOT NULL DEFAULT '{}',
  version INTEGER NOT NULL DEFAULT 0, prev_memory_id TEXT NOT NULL DEFAULT '', PRIMARY KEY (memory_id, entity_id, role)) WITHOUT ROWID;
CREATE TABLE entity_surface (surface TEXT PRIMARY KEY, norm TEXT NOT NULL, entity_id INTEGER NOT NULL,
  source TEXT NOT NULL CHECK (source IN ('migrate','gate','coarse','extract','model','manual','text')),
  count INTEGER NOT NULL DEFAULT 0, first_seen TEXT NOT NULL DEFAULT '', last_seen TEXT NOT NULL DEFAULT '',
  flag TEXT NOT NULL DEFAULT '', UNIQUE (norm, surface)) WITHOUT ROWID;
CREATE TABLE entity_issue (memory_id TEXT NOT NULL DEFAULT '', entity_norm TEXT NOT NULL, raw_name TEXT NOT NULL DEFAULT '',
  reason TEXT NOT NULL, detail TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL,
  PRIMARY KEY (memory_id, entity_norm, reason)) WITHOUT ROWID;`;

function makeDb({ memories }) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-mo-perm-'));
  const dbPath = path.join(dir, 'memory.db');
  const db = new DatabaseSync(dbPath, { allowExtension: true });
  db.loadExtension(VEC_SO);
  db.exec(MEMORY_DDL);
  db.exec(ENTITY_DDL);
  db.exec('CREATE VIRTUAL TABLE vec_items USING vec0(embedding float[4] distance_metric=cosine);');
  db.exec('CREATE TABLE vec_map (rowid INTEGER PRIMARY KEY, id TEXT);');
  db.exec("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);");
  db.exec("INSERT INTO meta(key,value) VALUES ('dim','4');");
  const ins = db.prepare(`INSERT INTO memories(id,text,tags,source,ts,type,entities,status,rowSeq)
    VALUES (?,?,?,?,?,?,?,?,?)`);
  let seq = 0;
  for (const m of memories) {
    seq += 1;
    ins.run(m.id, m.text, '[]', 'seed', '2026-11-10T00:00:00Z', 'world', '[]', m.status || 'active', seq);
  }
  db.close();
  applySchema(dbPath, { logger: () => {} });
  applyCurateSchema(dbPath, { logger: { info() {}, warn() {} } });
  return { dir, dbPath };
}

function makeSleep(dbPath, dir) {
  const cfg = {
    storePath: dir,
    storeLogFile: path.join(dir, 'store.log'),
    stateFile: path.join(dir, 'sleep.state.json'),
    sessionsRoot: path.join(dir, 'sessions'),
    backupDir: path.join(dir, 'backup'),
    archiveDir: path.join(dir, 'archive'),
    splitStateFile: path.join(dir, 'split.state.json'),
    splitLogFile: path.join(dir, 'split.log'),
    splitPreviewFile: path.join(dir, 'preview.md'),
    sleepLogFile: path.join(dir, 'sleep.log'),
    entityQueue: { enqueue: false },
    provider: 'fake', model: 'fake', splitMaxChars: 200,
    embed: async (texts) => texts.map(() => [1, 0, 0, 0]),
    logger: { info() {}, warn() {} },
    pipeline: { writeGate: false, textLint: false, lintReview: false, agentPipeline: true },
    organizePipelineMode: 'pool',
    sleepSplitEnabled: false,
    consolidateTools: false,
    notifyScript: '/bin/true',
  };
  const store = createStore(cfg);
  const ctx = {
    logger: { info() {}, warn() {} },
    tools: { register() {}, get: () => null },
    provide() {}, effect() {}, on() {},
    get: () => undefined,
  };
  const sleep = setupSleep(ctx, store, cfg);
  return { sleep, store, db: new DatabaseSync(dbPath) };
}

/** 取 pending_ops 里某条 merge_op 的当前账。 */
function opRow(db, key) {
  const row = db.prepare("SELECT * FROM pending_ops WHERE kind='merge_op' AND dedupe_key=?").get(key);
  return row ? { attempts: Number(row.attempts) || 0, status: row.status, lastError: String(row.last_error || '') } : null;
}

const SOURCE = [
  { id: 'm_1', text: '树莓派今天温度 45 度。', type: 'world' },
  { id: 'm_2', text: '风扇转速 3000 转。', type: 'world' },
];

test('source 快照再验仍是同一原因 → 当场 dead，不再耗 attempts', () => {
  const { dir, dbPath } = makeDb({
    memories: [
      { id: 'm_1', text: '树莓派今天温度 45 度。' },
      { id: 'm_2', text: '风扇转速 3000 转。' },
    ],
  });
  const { sleep, db } = makeSleep(dbPath, dir);
  const payload = {
    batch: 'b1',
    op: 'merge',
    ids: ['m_1', 'm_2'],
    // 当初被判 invalid 的原因就是这个：正文里的日期不在证据/原文里
    reason: 'fact_not_in_evidence',
    fact: '2026-11-03',
    factKind: 'date',
    text: '2026-11-03 树莓派温度 45 度，风扇 3000 转。',
    evidence: [
      { id: 'm_1', quote: '树莓派今天温度 45 度' },
      { id: 'm_2', quote: '风扇转速 3000 转' },
    ],
    source: SOURCE,
  };
  const id = enqueue(db, 'merge_op', 'perm-1', payload).id;
  const op = { id, kind: 'merge_op', dedupeKey: 'perm-1', payload, attempts: 0 };
  return sleep.__replay({ onlyOps: [op] }).then(() => {
    const row = opRow(db, 'perm-1');
    assert.equal(row.status, 'dead');
    assert.match(row.lastError, /^replay_invalid_permanent fact_not_in_evidence/);
    assert.equal(row.attempts, 0, '确定性 invalid 不许再推 attempts');
    db.close();
  });
});

test('快照能过、现库过不了 → dead replay_source_drift，不空转', () => {
  const { dir, dbPath } = makeDb({
    // 现库正文已经换掉：原文里的 quote 不再出现
    memories: [
      { id: 'm_1', text: '树莓派今天温度 47 度（改过）。' },
      { id: 'm_2', text: '风扇转速 3000 转。' },
    ],
  });
  const { sleep, db } = makeSleep(dbPath, dir);
  const payload = {
    batch: 'b2',
    op: 'merge',
    ids: ['m_1', 'm_2'],
    reason: 'fact_not_in_evidence',
    text: '树莓派温度 45 度，风扇 3000 转。',
    evidence: [
      { id: 'm_1', quote: '树莓派今天温度 45 度' },
      { id: 'm_2', quote: '风扇转速 3000 转' },
    ],
    source: SOURCE,
  };
  const id = enqueue(db, 'merge_op', 'drift-1', payload).id;
  const op = { id, kind: 'merge_op', dedupeKey: 'drift-1', payload, attempts: 1 };
  return sleep.__replay({ onlyOps: [op] }).then(() => {
    const row = opRow(db, 'drift-1');
    assert.equal(row.status, 'dead');
    assert.match(row.lastError, /replay_source_drift/);
    assert.equal(row.attempts, 0, '漂移判死也不推 attempts');
    db.close();
  });
});

test('原因和当初不一样 → 维持 pending + attempts+1', () => {
  const { dir, dbPath } = makeDb({
    memories: [
      { id: 'm_1', text: '树莓派今天温度 45 度。' },
      { id: 'm_2', text: '风扇转速 3000 转。' },
    ],
  });
  const { sleep, db } = makeSleep(dbPath, dir);
  const payload = {
    batch: 'b3',
    op: 'merge',
    ids: ['m_1', 'm_2'],
    // 当初记的是 id_not_in_batch，现在重新验是 fact_not_in_evidence → 上下文变了，值得下一轮再试
    reason: 'id_not_in_batch',
    text: '2026-11-03 树莓派温度 45 度，风扇 3000 转。',
    evidence: [
      { id: 'm_1', quote: '树莓派今天温度 45 度' },
      { id: 'm_2', quote: '风扇转速 3000 转' },
    ],
    source: SOURCE,
  };
  const id = enqueue(db, 'merge_op', 'retry-1', payload).id;
  const op = { id, kind: 'merge_op', dedupeKey: 'retry-1', payload, attempts: 0 };
  return sleep.__replay({ onlyOps: [op] }).then(() => {
    const row = opRow(db, 'retry-1');
    assert.equal(row.status, 'pending');
    assert.match(row.lastError, /replay_invalid fact_not_in_evidence/);
    assert.equal(row.attempts, 1, '真变化的原因才推 attempts');
    db.close();
  });
});
