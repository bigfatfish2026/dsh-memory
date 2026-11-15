/**
 * memory_save 3 参契约 + 粗实体 + entity_extract 队列 + 队列判取代（2026-11-13，
 * 2026-11-13 把 supersedes 也删掉、取代关系挪到队列判）。
 *
 * 全程离线：不联网、不调模型、不碰真库。假库在 tmpdir 里造，嵌入用假函数。
 * 覆盖：
 *   1. 签名：memory_save 只剩 text / tags / type，删掉的五个参数被忽略；
 *   2. 粗实体：已知实体命中（origin='rule'）、未知实体空着、纯数字无命中；
 *   3. 入队：三条各排一行 curate_queue，dedupe_key = memory_id，重复入队不加行；
 *   4. 领取：两步 CAS 不重复领，租约过期回 ready，3 次失败转 dead；
 *   5. 闸：not_in_source / etype_unknown→entity_issue / 泛词 df 闸；
 *   6. 耗时：100 条正文的粗实体匹配均值必须 ≤3ms（票面预算）；
 *   7. 取代：队列判出 supersedes 落 memory_link、target 非 active 不写、无候选只抽实体、
 *      整理轮不覆盖 origin='queue' 的边。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { createStore, registerStoreTools } from '../lib/store.js';
import { applyCurateSchema } from '../scripts/apply-curate-schema.mjs';
import { buildCoarseDict, matchCoarse, createCoarseMatcher } from '../lib/entity-coarse.js';
import { enqueueEntityExtract, claimBatch, reapExpired, failOne, queueStats, hasCurateTables } from '../lib/entity-queue.js';
import { runEntityQueueOnce } from '../lib/entity-worker.js';
import { gateEntityName, insertMemoryEntity, upsertEntity } from '../lib/entity-gate.js';
import { writeSupersedesLink } from '../lib/memory-link.js';

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

/** 三条假记忆：已知实体 / 未知实体 / 纯数字无实体。 */
const MEM_KNOWN = '2026-11-10 树莓派温度 45 度，风扇 3000 转，dsh-memory 队列空着';
const MEM_UNKNOWN = '把 mikan 的番剧订阅改成 1080p';
const MEM_NUMBERS = '跑了 12345 次，耗时 987 ms，失败 0 次';

function makeStore({ entityQueue = {} } = {}) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-memsave-'));
  const dbPath = path.join(dir, 'memory.db');
  const db = new DatabaseSync(dbPath, { allowExtension: true });
  db.loadExtension(VEC_SO);
  db.exec(MEMORY_DDL);
  db.exec(ENTITY_DDL);
  db.exec('CREATE VIRTUAL TABLE vec_items USING vec0(embedding float[4] distance_metric=cosine);');
  db.exec('CREATE TABLE vec_map (rowid INTEGER PRIMARY KEY, id TEXT);');
  // 已知实体词典：树莓派 / dsh-memory（外加一个用来验最长匹配的短名 dsh）
  const insEnt = db.prepare('INSERT INTO entities(norm, display, etype) VALUES (?, ?, ?)');
  insEnt.run('树莓派', '树莓派', 'device');
  insEnt.run('dsh-memory', 'dsh-memory', 'repo');
  insEnt.run('dsh', 'dsh', 'concept');
  // mikan 故意不进词典：它是「正文里有名字、词典里没有」的那一类（新实体，留给队列）
  db.close();

  // 建 curate_queue / curate_run + memory_entity.origin（真脚本，不是测试里另写一份 DDL）
  const applied = applyCurateSchema(dbPath, { logger: { info() {}, warn() {} } });

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
      ...entityQueue,
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
  const store = createStore(cfg);
  return { store, dir, dbPath, cfg, applied };
}

function rows(dbPath, sql, ...params) {
  const db = new DatabaseSync(dbPath, { readOnly: true });
  try {
    return db.prepare(sql).all(...params);
  } finally {
    db.close();
  }
}

function count(dbPath, sql, ...params) {
  return Number(rows(dbPath, sql, ...params)[0]?.n ?? 0);
}

function toolsOf(store, cfg) {
  const tools = [];
  const ctx = { tools: { register: (t) => tools.push(t) }, logger: { info() {}, warn() {} } };
  registerStoreTools(ctx, store, cfg);
  return tools;
}

test('签名硬切：memory_save 只剩 text / tags / type', () => {
  const { store, cfg } = makeStore();
  const tool = toolsOf(store, cfg).find((t) => t.name === 'memory_save');
  const props = tool.parameters.properties || {};
  assert.deepEqual(Object.keys(props).sort(), ['tags', 'text', 'type']);
  assert.deepEqual(tool.parameters.required, ['text']);
  for (const gone of ['entities', 'validFrom', 'validTo', 'confidence', 'supersedes']) {
    assert.equal(props[gone], undefined, `${gone} 必须删掉`);
    assert.equal(tool.description.includes(`Comma-separated ${gone}`), false, `描述里不该再教调用方填 ${gone}`);
  }
  assert.match(tool.description, /server/, '描述要说清实体/日期是服务端的事');
  assert.match(tool.description, /background consolidation/, '描述要说清取代关系由后台整理判断');
});

test('删掉的参数被忽略，日期从正文头部反抽，实体由服务端补', async () => {
  const { store, dbPath, cfg } = makeStore();
  const tool = toolsOf(store, cfg).find((t) => t.name === 'memory_save');
  const out = await tool.execute({
    text: MEM_KNOWN,
    tags: 'infra',
    entities: '不存在的实体,手填的',
    validFrom: '2020-01-01',
    validTo: '2020-01-02',
    confidence: 0.1,
  }, {});
  assert.equal(out.saved, true);
  await store.flushEntityQueue();
  const row = rows(dbPath, 'SELECT entities, validFrom, validTo, confidence FROM memories WHERE id=?', out.id)[0];
  assert.equal(row.validFrom, '2026-11-10', '正文头部日期要当 validFrom');
  assert.equal(row.validTo, null);
  assert.equal(row.confidence, null, '手填 confidence 不许再生效');
  assert.equal(row.entities.includes('不存在的实体'), false, '手填 entities 不许生效');
  assert.equal(row.entities.includes('树莓派'), true, '粗实体要写进老列');
});

test('supersedes 传了也不认：不写取代关系、不归档旧条，只留一行 WARN', async () => {
  const { store, dbPath, cfg } = makeStore();
  const old = await store.save('2026-11-01 树莓派整理时间每天 04:30 一次', ['infra'], 'agent', { type: 'world' });
  const tool = toolsOf(store, cfg).find((t) => t.name === 'memory_save');
  const out = await tool.execute({
    text: '2026-11-10 树莓派整理时间改成 04:30 和 11:00 各一次',
    tags: 'infra',
    type: 'world',
    supersedes: old.id,
  }, {});
  assert.equal(out.saved, true);
  const row = rows(dbPath, 'SELECT supersedes, status FROM memories WHERE id=?', out.id)[0];
  assert.equal(row.supersedes, null, 'supersedes 参数不许生效');
  assert.equal(row.status ?? 'active', 'active');
  const before = rows(dbPath, 'SELECT status, supersededBy FROM memories WHERE id=?', old.id)[0];
  assert.equal(before.status ?? 'active', 'active', '旧条不许被归档');
  assert.equal(before.supersededBy ?? null, null, '旧条不许挂 supersededBy');
  const log = fs.readFileSync(cfg.storeLogFile, 'utf8');
  assert.match(log, /memory_save 收到已删参数（忽略）/);
  assert.match(log, /keys=[^\n]*supersedes/);
});

test('粗实体：已知命中 origin=rule，未知/纯数字空着，靠队列补', async () => {
  const { store, dbPath } = makeStore();
  const a = await store.save(MEM_KNOWN, ['infra'], 'agent', { type: 'world' });
  const b = await store.save(MEM_UNKNOWN, ['anime'], 'agent', { type: 'world' });
  const c = await store.save(MEM_NUMBERS, ['bench'], 'agent', { type: 'world' });
  await store.flushEntityQueue();

  const hits = rows(dbPath, `SELECT m.text, me.memory_id, e.norm, me.role, me.origin
    FROM memory_entity me JOIN entities e ON e.id = me.entity_id
    JOIN memories m ON m.id = me.memory_id ORDER BY m.id, e.norm`);
  const perMemory = new Map();
  for (const h of hits) perMemory.set(h.memory_id, [...(perMemory.get(h.memory_id) || []), h]);

  assert.ok((perMemory.get(a.id) || []).length >= 2, '已知实体的那条至少命中 树莓派 + dsh-memory');
  assert.deepEqual((perMemory.get(a.id) || []).map((h) => h.norm).sort(), ['dsh-memory', '树莓派']);
  assert.ok((perMemory.get(a.id) || []).every((h) => h.origin === 'rule' && h.role === 'about'));
  assert.equal(perMemory.get(b.id), undefined, '未知实体粗匹配不许瞎编（mikan 不在词典里）');
  assert.equal(perMemory.get(c.id), undefined, '纯数字不存在实体');

  const stats = store.coarseStats();
  assert.ok(stats.words >= 3, `词典应有词，实际 ${stats.words}`);
  assert.ok(stats.maxMs <= 50, `单条粗匹配不许爆表，实际 ${stats.maxMs}ms`);
  return { a, b, c, stats };
});

test('入队 + 幂等：三条三行，dedupe_key=memory_id，重复入队不加行', async () => {
  const { store, dbPath } = makeStore();
  const a = await store.save(MEM_KNOWN, ['infra'], 'agent', { type: 'world' });
  const b = await store.save(MEM_UNKNOWN, ['anime'], 'agent', { type: 'world' });
  const c = await store.save(MEM_NUMBERS, ['bench'], 'agent', { type: 'world' });
  await store.flushEntityQueue();

  const q = rows(dbPath, "SELECT id, channel, kind, dedupe_key, payload_json, state, attempts FROM curate_queue ORDER BY id");
  assert.equal(q.length, 3, '三条记忆各排一行');
  assert.deepEqual(q.map((r) => r.dedupe_key).sort(), [a.id, b.id, c.id].sort());
  assert.ok(q.every((r) => r.channel === 'curate' && r.kind === 'entity_extract' && r.state === 'ready'));
  assert.deepEqual(JSON.parse(q[0].payload_json), { memory_id: q[0].dedupe_key });

  // 再入队两条（同一批 memory_id）：只刷新，不加行
  enqueueEntityExtract(store.db(), { memoryId: a.id });
  enqueueEntityExtract(store.db(), { memoryId: a.id });
  assert.equal(count(dbPath, 'SELECT count(*) AS n FROM curate_queue'), 3, 'dedupe_key 挡住重复');

  // memory_entity 幂等：同 (memory_id, entity_id, role) 插两次只有一行
  const ent = upsertEntity(store.db(), { norm: '树莓派', display: '树莓派', etype: 'device' });
  const first = insertMemoryEntity(store.db(), { memoryId: b.id, entityId: ent.id, ts: '', origin: 'agent' });
  const second = insertMemoryEntity(store.db(), { memoryId: b.id, entityId: ent.id, ts: '', origin: 'agent' });
  assert.equal(first, true);
  assert.equal(second, false);
  assert.equal(count(dbPath, 'SELECT count(*) AS n FROM memory_entity WHERE memory_id=?', b.id), 1);
});

test('领取/租约/重试：两步 CAS 不重复领，过期回 ready，3 次失败转 dead', async () => {
  const { store, dbPath } = makeStore();
  const a = await store.save(MEM_KNOWN, [], 'agent', {});
  const b = await store.save(MEM_UNKNOWN, [], 'agent', {});
  const c = await store.save(MEM_NUMBERS, [], 'agent', {});
  await store.flushEntityQueue();
  const db = store.db();
  const t0 = 1_800_000_000_000;

  const first = claimBatch(db, { owner: 'test:1', batch: 8, now: t0, leaseMs: 1000 });
  assert.equal(first.length, 3, '一批领完三条');
  assert.ok(first.every((r) => r.attempts === 1));
  assert.equal(claimBatch(db, { owner: 'test:2', batch: 8, now: t0 + 10, leaseMs: 1000 }).length, 0, '租约内不许重复领');

  const reaped = reapExpired(db, { now: t0 + 5000 });
  assert.equal(reaped, 3, '租约过期三条回 ready');
  const again = claimBatch(db, { owner: 'test:3', batch: 8, now: t0 + 5001, leaseMs: 1000 });
  assert.equal(again.length, 3);
  assert.ok(again.every((r) => r.attempts === 2));

  const target = again[0];
  const r2 = failOne(db, { id: target.id, attempts: target.attempts, error: 'timeout', now: t0 + 6000 });
  assert.equal(r2.state, 'ready');
  assert.ok(r2.nextTryAt > t0 + 6000, '失败要退避');

  const third = claimBatch(db, { owner: 'test:4', batch: 8, now: t0 + 6000 + 10 * 60000, leaseMs: 1000 });
  const t3 = third.find((r) => r.id === target.id);
  assert.ok(t3, '退避到点后能再领');
  assert.equal(t3.attempts, 3);
  const r3 = failOne(db, { id: t3.id, attempts: t3.attempts, error: 'still bad', now: t0 + 900000 });
  assert.equal(r3.state, 'dead', '第 3 次失败转 dead');
  const st = queueStats(db);
  assert.equal(st.dead, 1);
  assert.equal(st.leased, 2);
  // 剩下两条收尾，别把库留成脏的
  for (const r of third.filter((x) => x.id !== t3.id)) failOne(db, { id: r.id, attempts: r.attempts, error: 'cleanup', now: t0 + 900001 });
  assert.ok(count(dbPath, "SELECT count(*) AS n FROM curate_queue WHERE state='dead'") >= 1);
});

test('实体闸：原文不包含/类别不可判/泛词 df 三档', async () => {
  const { store, dbPath } = makeStore();
  const text = '合并这两条记忆时注意去重，别把 op 当成实体，也别把 stop 当实体，实体只留原文里的名字';
  assert.ok(text.length >= 25);
  const mem = await store.save(text, [], 'agent', {});
  await store.flushEntityQueue();
  const db = store.db();

  const notInSource = gateEntityName({ db, memoryId: mem.id, text: '温度 45 度', rawName: '树莓派', etype: 'device' });
  assert.equal(notInSource.ok, false);
  assert.equal(notInSource.reason, 'not_in_source');

  const unknown = gateEntityName({ db, memoryId: mem.id, text: 'zqx-thing 是新东西', rawName: 'zqx-thing', etype: 'unknown' });
  assert.equal(unknown.ok, false);
  assert.equal(unknown.reason, 'etype_unknown');

  // 泛词：合并 在 1 条 active 记忆里出现 1 次，genericDfMin=1 时就是 100% > 5%
  const word = upsertEntity(db, { norm: '合并', display: '合并', etype: 'concept' });
  insertMemoryEntity(db, { memoryId: mem.id, entityId: word.id, origin: 'agent' });
  const generic = gateEntityName({
    db, memoryId: mem.id, text,
    rawName: '合并', etype: 'concept', genericDf: 0.05, genericDfMin: 1,
  });
  assert.equal(generic.ok, false);
  assert.equal(generic.reason, 'generic');

  const good = gateEntityName({ db, memoryId: mem.id, text, rawName: '原文', etype: 'concept', genericDf: 0.05, genericDfMin: 20 });
  assert.equal(good.ok, true, 'df 没到最小次数的不算泛词');
  // 黑名单先于 df：这些词库里 df 还低（1-2），比例闸拦不到，必须显式拒
  for (const w of ['合并', '去重', 'op', 'stop']) {
    const bad = gateEntityName({ db, memoryId: mem.id, text, rawName: w, etype: 'concept', genericDf: 0.05, genericDfMin: 20 });
    assert.equal(bad.ok, false, `${w} 必须被泛词黑名单拒`);
    assert.equal(bad.reason, 'generic');
  }
  assert.equal(count(dbPath, "SELECT count(*) AS n FROM entity_issue WHERE reason='etype_unknown'"), 0, '闸本身不落库，落库由消费端做');
});

test('粗实体耗时：100 条正文均值 ≤3ms（票面预算）', () => {
  const { dbPath } = makeStore();
  const db = new DatabaseSync(dbPath, { readOnly: true });
  let dict;
  try {
    dict = buildCoarseDict(db, { genericDf: 0.05, genericDfMin: 20, maxWords: 4000 });
  } finally {
    db.close();
  }
  const texts = [];
  for (let i = 0; i < 100; i += 1) texts.push(`${MEM_KNOWN} 第 ${i} 轮`);

  const t0 = process.hrtime.bigint();
  let hits = 0;
  for (const t of texts) hits += matchCoarse(t, dict.words, 4).length;
  const totalMs = Number(process.hrtime.bigint() - t0) / 1e6;
  const avg = totalMs / texts.length;
  assert.ok(hits >= 200, '每条至少命中 2 个实体');
  assert.ok(avg <= 3, `100 条均值 ${avg.toFixed(3)}ms 超预算`);

  // 匹配器本身的统计（含 TTL/预算保护那层）
  const matcher = createCoarseMatcher({ db: () => new DatabaseSync(dbPath, { readOnly: true }), genericDfMin: 20 });
  for (const t of texts.slice(0, 20)) matcher.match(t);
  const st = matcher.stats();
  assert.ok(st.avgMs <= 3, `匹配器均值 ${st.avgMs}ms`);
  console.log(JSON.stringify({
    case: 'coarse-timing',
    texts: texts.length,
    words: dict.words.length,
    totalMs: Number(totalMs.toFixed(3)),
    avgMs: Number(avg.toFixed(3)),
    matcherAvgMs: st.avgMs,
    hits,
  }));
});

test('消费端（假模型，不联网）：落库 origin=agent、队列 done、run 记录收尾', async () => {
  const { store, dbPath, cfg } = makeStore();
  const a = await store.save(MEM_KNOWN, [], 'agent', { type: 'world' });
  const b = await store.save(MEM_UNKNOWN, [], 'agent', { type: 'world' });
  await store.flushEntityQueue();
  assert.equal(count(dbPath, 'SELECT count(*) AS n FROM curate_queue'), 2);

  const calls = [];
  const llm = {
    stream: (opts) => {
      calls.push(opts);
      return (async function* fake() {
        yield {
          type: 'text-delta',
          text: JSON.stringify({
            items: [
              { i: 1, name: '树莓派', etype: 'device', attrs: {} },
              { i: 2, name: 'mikan', etype: 'service', attrs: { model: '1080p' } },
            ],
          }),
        };
      })();
    },
  };
  const out = await runEntityQueueOnce({ store, cfg, llm, log: () => {} });
  assert.equal(out.claimed, 2);
  assert.equal(out.accepted, 2);
  assert.equal(calls.length, 1, '一批只调一次模型');
  assert.equal(calls[0].temperature, 0, '温度必须是 0');
  assert.equal(calls[0].maxTokens, undefined, '不传 maxTokens（2026-11-06 硬规矩）');

  const st = queueStats(store.db());
  assert.equal(st.done, 2, '两条都置 done');
  assert.equal(st.ready, 0);

  const agentRows = rows(dbPath, `SELECT m.text, e.norm, me.origin, me.attrs
    FROM memory_entity me JOIN entities e ON e.id=me.entity_id JOIN memories m ON m.id=me.memory_id
    WHERE me.origin='agent' ORDER BY e.norm`);
  assert.deepEqual(agentRows.map((r) => r.norm), ['mikan'], '树莓派已有 rule 行，INSERT OR IGNORE 不重复写');
  assert.equal(JSON.parse(agentRows[0].attrs).model, '1080p', '型号进 attrs 不并进实体名');
  assert.ok(hasCurateTables(store.db()));

  const run = rows(dbPath, "SELECT state, budget_json FROM curate_run ORDER BY started_ts DESC LIMIT 1")[0];
  assert.equal(run.state, 'done');
  assert.equal(JSON.parse(run.budget_json).llm_calls, 1);
  // b 的两条链：粗实体没有 + agent 补上 mikan
  assert.equal(count(dbPath, 'SELECT count(*) AS n FROM memory_entity WHERE memory_id=?', b.id), 1);
  assert.equal(count(dbPath, 'SELECT count(*) AS n FROM memory_entity WHERE memory_id=? AND origin=?', a.id, 'rule'), 2);
});

/* ── 取代关系：队列判定 + memory_link 落边 ────────────── */

/** 只让指定这条记忆进下一轮：其余队列行直接标 done。 */
function finishQueueRows(store, ids) {
  const stmt = store.db().prepare("UPDATE curate_queue SET state='done' WHERE dedupe_key=?");
  for (const id of ids) stmt.run(id);
}

/** 假模型：yield 一段 JSON，同时把 payload 记下来。 */
function fakeLlm(reply, seen = []) {
  return {
    calls: seen,
    stream: (opts) => {
      seen.push(JSON.parse(opts.messages[0].content[0].text));
      return (async function* fake() {
        yield { type: 'text-delta', text: JSON.stringify(reply) };
      })();
    },
  };
}

test('队列判取代：判出 supersedes 且 target active 就落 memory_link（origin=queue）', async () => {
  const { store, dbPath, cfg } = makeStore();
  const old = await store.save('2026-11-01 树莓派整理时间每天 04:30 一次', ['infra'], 'agent', { type: 'world' });
  await store.flushEntityQueue();
  finishQueueRows(store, [old.id]);
  const fresh = await store.save('2026-11-10 树莓派整理时间改成 04:30 和 11:00 各一次', ['infra'], 'agent', { type: 'world' });
  await store.flushEntityQueue();

  const llm = fakeLlm({ items: [], links: [{ i: 1, rel: 'supersedes', target_memory_id: old.id }] });
  const out = await runEntityQueueOnce({ store, cfg, llm, log: () => {} });
  assert.equal(out.claimed, 1);
  assert.equal(out.linked, 1, '判出取代就记一条边');
  assert.equal(llm.calls.length, 2, '抽实体一次 + 判关系一次');
  assert.equal('candidates' in llm.calls[0].items[0], false, '抽实体那一次还没实体行，不能带候选');
  const payload = llm.calls.at(-1).items[0];
  assert.equal(payload.candidates.length, 1, '候选是共享实体的旧条');
  assert.equal(payload.candidates[0].id, old.id);
  assert.ok(String(payload.candidates[0].text).length <= 120, '候选正文只给前 120 字');

  const edge = rows(dbPath, 'SELECT src, dst, rel, origin, attr_key FROM memory_link').map((r) => ({ ...r }));
  assert.deepEqual(edge, [{ src: fresh.id, dst: old.id, rel: 'supersedes', origin: 'queue', attr_key: '' }]);
});

test('队列判取代：target 不是 active（已归档）就不写边', async () => {
  const { store, dbPath, cfg } = makeStore();
  const old = await store.save('2026-11-01 树莓派整理时间每天 04:30 一次', ['infra'], 'agent', { type: 'world' });
  await store.flushEntityQueue();
  finishQueueRows(store, [old.id]);
  await store.archive(old.id, '过期');
  const fresh = await store.save('2026-11-10 树莓派整理时间改成 04:30 和 11:00 各一次', ['infra'], 'agent', { type: 'world' });
  await store.flushEntityQueue();

  // 模型就算硬给了这条已归档的 id，队列也不许落边
  const llm = fakeLlm({ items: [], links: [{ i: 1, rel: 'supersedes', target_memory_id: old.id }] });
  const out = await runEntityQueueOnce({ store, cfg, llm, log: () => {} });
  assert.equal(out.linked, 0);
  assert.equal(count(dbPath, 'SELECT count(*) AS n FROM memory_link'), 0, 'target 不 active 不许落边');

  // 写边那道闸本身也要拒：target_not_active
  const w = writeSupersedesLink(store.db(), { src: fresh.id, dst: old.id, origin: 'queue' });
  assert.equal(w.ok, false);
  assert.equal(w.reason, 'target_not_active');
  assert.equal(count(dbPath, 'SELECT count(*) AS n FROM memory_link'), 0);
});

test('队列判取代：候选为空就只抽实体，不判取代、不落边', async () => {
  const { store, dbPath, cfg } = makeStore();
  const mem = await store.save(MEM_UNKNOWN, ['anime'], 'agent', { type: 'world' });
  await store.flushEntityQueue();
  assert.equal(count(dbPath, 'SELECT count(*) AS n FROM memory_entity WHERE memory_id=?', mem.id), 0, '没有共享实体');

  const llm = fakeLlm({
    items: [{ i: 1, name: 'mikan', etype: 'service', attrs: {} }],
    links: [{ i: 1, rel: 'supersedes', target_memory_id: 'm_ghost' }],
  });
  const out = await runEntityQueueOnce({ store, cfg, llm, log: () => {} });
  assert.equal(llm.calls.length, 1, '没有候选就不发第二次模型调用');
  const payload = llm.calls[0].items[0];
  assert.equal('candidates' in payload, false, '没有候选就不带这个键，模型不用判取代');
  assert.equal(out.linked, 0);
  assert.equal(count(dbPath, 'SELECT count(*) AS n FROM memory_link'), 0);
  assert.equal(out.accepted, 1, '实体照抽，mikan 照落');
  assert.equal(count(dbPath, "SELECT count(*) AS n FROM memory_entity WHERE memory_id=? AND origin='agent'", mem.id), 1);
});

test('整理轮不覆盖 origin=queue 的边：只补缺、不改已有', async () => {
  const { store, dbPath } = makeStore();
  const src = await store.save('2026-11-10 树莓派温度 51 度，风扇 3200 转', ['infra'], 'agent', { type: 'world' });
  const dst1 = await store.save('2026-11-01 树莓派温度 46 度，风扇 3000 转', ['infra'], 'agent', { type: 'world' });
  const dst2 = await store.save('2026-10-20 树莓派温度 44 度，风扇 2800 转', ['infra'], 'agent', { type: 'world' });
  const db = store.db();

  const q = writeSupersedesLink(db, { src: src.id, dst: dst1.id, origin: 'queue' });
  assert.equal(q.ok, true);
  const sleep = writeSupersedesLink(db, { src: src.id, dst: dst2.id, origin: 'sleep' });
  assert.equal(sleep.ok, false);
  assert.equal(sleep.reason, 'queue_edge_kept', '队列边在，整理轮不许换 dst');
  const same = writeSupersedesLink(db, { src: src.id, dst: dst1.id, origin: 'sleep' });
  assert.equal(same.ok, false);
  assert.equal(same.reason, 'edge_exists', '同一条边不重复写');

  const all = rows(dbPath, 'SELECT src, dst, origin FROM memory_link').map((r) => ({ ...r }));
  assert.deepEqual(all, [{ src: src.id, dst: dst1.id, origin: 'queue' }], '边保持队列写的那条，origin 不变');
});

test('同步写入当场就把 entity_surface 记上；异步再补也不重复计数', async () => {
  const { store, dbPath } = makeStore();
  const a = await store.save(MEM_KNOWN, ['infra'], 'agent', { type: 'world' });

  // 同步：memory_save 落完，粗实体命中的两个实体必须已有 surface 行，不用等队列
  const s1 = rows(dbPath, 'SELECT surface, source, count FROM entity_surface ORDER BY surface');
  assert.deepEqual(s1.map((r) => r.surface).sort(), ['dsh-memory', '树莓派'], '同步路径直接建 surface 行');
  assert.ok(s1.every((r) => Number(r.count) === 1), `同步一次就记 1，实际 ${JSON.stringify(s1)}`);
  assert.ok(s1.every((r) => r.source === 'gate'), '新建行 source=gate');

  // 异步：模拟 entity_extract 消费落同一批实体（origin='agent'）——memory_entity 被 IGNORE，计数不许再动
  const db = store.db();
  for (const name of ['树莓派', 'dsh-memory']) {
    const ent = upsertEntity(db, { norm: name, display: name, etype: 'device', countSurface: false });
    insertMemoryEntity(db, { memoryId: a.id, entityId: ent.id, ts: a.ts, origin: 'agent' });
  }
  const s2 = rows(dbPath, 'SELECT surface, count FROM entity_surface ORDER BY surface');
  assert.deepEqual(s2.map((r) => Number(r.count)), [1, 1], '同步 + 异步合计只 +1');
});

/* ── 阶段 1 行为契约：单条隔离 / 空返回记账 ───────── */

test('单条脏数据只回滚自己：同批另一条照常 done，坏条回 ready 等下一轮', async () => {
  const { store, cfg, dbPath } = makeStore();
  const good = await store.save('树莓派温度 45 度，风扇 3000 转', ['infra'], 'agent', { type: 'world' });
  const bad = await store.save('mikan 番剧订阅是 720p', ['anime'], 'agent', { type: 'world' });
  await store.flushEntityQueue();
  // 只让坏条的 memory_entity 插入失败：验证事务里一条坏不带走整批
  store.db().exec(
    `CREATE TRIGGER boom BEFORE INSERT ON memory_entity WHEN NEW.memory_id = '${bad.id}' BEGIN SELECT RAISE(ABORT, 'boom'); END;`,
  );
  const lines = [];
  const out = await runEntityQueueOnce({
    store,
    cfg,
    llm: fakeLlm({ items: [{ i: 1, name: '树莓派', etype: 'device' }, { i: 2, name: 'mikan', etype: 'service' }] }),
    log: (l) => lines.push(l),
  });
  assert.equal(out.done, 1, `好条要 done，实测 ${out.done}`);
  assert.equal(out.failed, 1, `坏条要 failed，实测 ${out.failed}`);

  const g = rows(dbPath, 'SELECT state FROM curate_queue WHERE dedupe_key=?', good.id)[0];
  assert.equal(g.state, 'done', '好条照常收尾');
  const b = rows(dbPath, 'SELECT state, attempts, last_error FROM curate_queue WHERE dedupe_key=?', bad.id)[0];
  assert.equal(b.state, 'ready', '坏条回 ready，下一轮还能领');
  assert.equal(b.attempts, 1, '坏条 attempts+1');
  assert.match(String(b.last_error), /boom/);

  assert.equal(
    count(dbPath, "SELECT count(*) AS n FROM memory_entity WHERE memory_id=? AND origin='agent'", bad.id),
    0,
    '坏条的实体行跟着 savepoint 一起回滚',
  );
  assert.ok(lines.some((l) => l.includes(`[entity-queue] done id=${good.id} attempts=1 ms=`)), '逐条 done 日志');
  assert.ok(lines.some((l) => l.includes(`[entity-queue] fail id=${bad.id} attempts=1 reason=`)), '逐条 fail 日志');
});

test('结构坏的空返回记一次 attempts：没给 items 键不许静默 markDone', async () => {
  const { store, cfg, dbPath } = makeStore();
  const one = await store.save('树莓派温度 45 度', ['infra'], 'agent', { type: 'world' });
  await store.flushEntityQueue();
  const out = await runEntityQueueOnce({ store, cfg, llm: fakeLlm({ links: [] }), log: () => {} });
  assert.equal(out.failed, 1, '结构坏算一次失败');
  const q = rows(dbPath, 'SELECT state, attempts, last_error FROM curate_queue WHERE dedupe_key=?', one.id)[0];
  assert.equal(q.state, 'ready');
  assert.equal(q.attempts, 1);
  assert.equal(q.last_error, 'empty_items');
});

test('模型明确给 {"items":[]} 是有效 noop：收尾 done、不额外耗 attempts', async () => {
  const { store, cfg, dbPath } = makeStore();
  const one = await store.save('树莓派温度 45 度', ['infra'], 'agent', { type: 'world' });
  await store.flushEntityQueue();
  // 模拟下一轮：把可能的退避拨到现在
  store.db().prepare('UPDATE curate_queue SET next_try_at=0 WHERE dedupe_key=?').run(one.id);
  const lines = [];
  const out = await runEntityQueueOnce({ store, cfg, llm: fakeLlm({ items: [], links: [] }), log: (l) => lines.push(l) });
  assert.equal(out.done, 1, '明确空也收尾');
  const q = rows(dbPath, 'SELECT state, attempts FROM curate_queue WHERE dedupe_key=?', one.id)[0];
  assert.equal(q.state, 'done');
  assert.equal(q.attempts, 1, '只算领取一次');
  assert.ok(lines.some((l) => l.includes(`[entity-queue] done id=${one.id} attempts=1 ms=0`)), '空批也留逐条 done 行');
});
