/**
 * pool 与 legacy 的对拍（2026-11-14，阶段 5）。
 *
 * 同一个 pending_ops 队列（merge_op + dedupe_check 各若干），喂两种调度：
 *   A legacy 式：一次 replay({}) 领整队跑；
 *   B pool 式：compact 池逐条 claim → replay({onlyOps:[op]})。
 * 断言两边跑完后的账本状态序列与记忆正文集合逐条一致——这是「搬 handler 没改行为」的锁。
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
import { enqueue, claimChannelOps, channelStats } from '../lib/ingest-index.js';
import { kindsOfChannel } from '../lib/kind-handlers.js';

const VEC_SO = path.join(import.meta.dirname, '../node_modules/sqlite-vec-linux-arm64/vec0.so');
const MEMORY_DDL = `CREATE TABLE memories (
  id TEXT PRIMARY KEY, text TEXT NOT NULL, tags TEXT, source TEXT, ts TEXT, type TEXT,
  entities TEXT, validFrom TEXT, supersedes TEXT, updatedAt TEXT, status TEXT, reason TEXT,
  supersededBy TEXT, validTo TEXT, confidence REAL, extra TEXT, present TEXT, rowSeq INTEGER NOT NULL
);`;
const ENTITY_DDL = `
CREATE TABLE entities (id INTEGER PRIMARY KEY, norm TEXT NOT NULL, display TEXT NOT NULL,
  etype TEXT NOT NULL DEFAULT 'unknown', aliases_note TEXT NOT NULL DEFAULT '',
  attrs TEXT NOT NULL DEFAULT '{}', first_seen TEXT NOT NULL DEFAULT '',
  last_seen TEXT NOT NULL DEFAULT '', mem_count INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'active', UNIQUE (norm));
CREATE TABLE memory_entity (memory_id TEXT NOT NULL, entity_id INTEGER NOT NULL, role TEXT NOT NULL DEFAULT 'about',
  ts TEXT NOT NULL DEFAULT '', status TEXT NOT NULL DEFAULT 'active',
  attrs TEXT NOT NULL DEFAULT '{}', version INTEGER NOT NULL DEFAULT 0,
  prev_memory_id TEXT NOT NULL DEFAULT '', PRIMARY KEY (memory_id, entity_id, role)) WITHOUT ROWID;
CREATE TABLE entity_surface (surface TEXT PRIMARY KEY, norm TEXT NOT NULL, entity_id INTEGER NOT NULL,
  source TEXT NOT NULL CHECK (source IN ('migrate','gate','coarse','extract','model','manual','text')),
  count INTEGER NOT NULL DEFAULT 0, first_seen TEXT NOT NULL DEFAULT '',
  last_seen TEXT NOT NULL DEFAULT '', flag TEXT NOT NULL DEFAULT '', UNIQUE (norm, surface)) WITHOUT ROWID;
CREATE TABLE entity_issue (memory_id TEXT NOT NULL DEFAULT '', entity_norm TEXT NOT NULL, raw_name TEXT NOT NULL DEFAULT '',
  reason TEXT NOT NULL, detail TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL,
  PRIMARY KEY (memory_id, entity_norm, reason)) WITHOUT ROWID;
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
INSERT INTO meta(key,value) VALUES ('dim','4');`;

function fakeLlm() {
  return {
    stream() {
      return (async function* gen() {
        yield { type: 'text-delta', text: JSON.stringify({ verdict: 'DISTINCT', reason: '对拍测试：新事实' }) };
        yield { type: 'finish', reason: { kind: 'stop' } };
      })();
    },
  };
}

function fakeCtx() {
  return {
    logger: { info() {}, warn() {} },
    tools: { register() {}, get: () => null },
    provide() {},
    effect() {},
    get: () => undefined,
    on() {},
  };
}

function makeStore() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-parity-'));
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
    pipeline: { writeGate: true, writeGateThreshold: 0.9, textLint: false, lintReview: false },
    organizePipelineMode: 'pool',
  };
  const store = createStore(cfg);
  store.attachLlm(fakeLlm());
  return { store, cfg, dbPath, dir };
}

async function seed(store, cfg) {
  const a = await store.save('树莓派温度 45 度，风扇 3000 转', ['infra'], 'test', { type: 'world' });
  const b = await store.save('树莓派风扇转速 3200 转，温度 46 度', ['infra'], 'test', { type: 'world' });
  const db = store.db();
  const now = Date.now();
  enqueue(db, 'merge_op', 'mo-1', {
    batch: 'b1',
    op: 'update',
    ids: [String(a.id)],
    text: '树莓派温度 45 度，风扇 3000 转（已复核）',
    evidence: [{ id: String(a.id), quote: '树莓派温度 45 度' }],
    source: [{ id: String(a.id), text: String(a.text), type: 'world' }],
  }, { now });
  enqueue(db, 'dedupe_check', 'dc-1', {
    action: 'save',
    candidate: { text: '树莓派今天温度 47 度。', tags: ['infra'], type: 'world', source: 'replay:test' },
    ids: [],
  }, { now });
  return { a, b };
}

function snapshot(store) {
  const db = store.db();
  const ops = db.prepare('SELECT kind, status, attempts FROM pending_ops ORDER BY kind, dedupe_key').all()
    .map((r) => `${r.kind}:${r.status}:${r.attempts}`);
  const mem = db.prepare("SELECT text FROM memories WHERE COALESCE(NULLIF(status,''),'active')='active' ORDER BY text").all()
    .map((r) => String(r.text));
  return { ops, mem };
}

test('对拍：同一队列，legacy 整队 replay 与 pool 单条 replay 的账本+记忆逐条一致', async () => {
  // A：legacy 式
  const A = makeStore();
  await seed(A.store, A.cfg);
  const sleepA = setupSleep(fakeCtx(), A.store, A.cfg);
  const outA = await sleepA.__replay({});
  assert.ok(outA.merge_op?.done >= 1, `legacy 式 merge_op 要落账：${JSON.stringify(outA)}`);
  assert.ok(outA.dedupe_check?.done >= 1, `legacy 式 dedupe_check 要落账：${JSON.stringify(outA)}`);

  // B：pool 式（compact 通道逐条领）
  const B = makeStore();
  await seed(B.store, B.cfg);
  const sleepB = setupSleep(fakeCtx(), B.store, B.cfg);
  const dbB = B.store.db();
  let claimed = 0;
  for (;;) {
    const [op] = claimChannelOps(dbB, 'compact', 1, { now: Date.now(), kinds: kindsOfChannel('compact') });
    if (!op) break;
    claimed += 1;
    await sleepB.__replay({ onlyOps: [op] });
  }
  assert.ok(claimed >= 2, `compact 池至少领到 2 条，实测 ${claimed}`);
  assert.equal(channelStats(dbB, 'compact').done >= 2, true, 'compact 队列收口');

  assert.deepEqual(snapshot(B.store), snapshot(A.store));
});

test('对拍：queue 空时两边都不产事件（不空转）', async () => {
  const A = makeStore();
  const sleepA = setupSleep(fakeCtx(), A.store, A.cfg);
  assert.deepEqual(await sleepA.__replay({}), {});
  const before = snapshot(A.store);
  assert.deepEqual(before.ops, []);
  assert.deepEqual(before.mem, []);
});
