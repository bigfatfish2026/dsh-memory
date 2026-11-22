/**
 * 夜间 observation 提炼（src/sleep/observation-pass.ts，t-mwqdxc83-cqt8）。
 *
 * 钉的边界：
 *   ① enabled=false：不 claim、不调模型、队列不变；
 *   ② dryRun：照跑模型与计量，但任务仍 pending、无新观察、无证据边；
 *   ③ 正常 create：观察 active（证据已在写事务里校验）+ 2 条 derived_from 边 + 任务 done；
 *   ④ update：旧观察归档、新观察替代、任务 done；
 *   ⑤ 校验拒绝（证据不在本批）：任务 done、无观察、rejected 记日志；
 *   ⑥ 版本冲突：模型跑期间 enqueue → 不留下活观察、任务仍 pending、version 涨；
 *   ⑦ 模型抛错：failObserveTask（tries+1）、不影响其它任务；
 *   ⑧ 计量行能解析出 calls / ms / accepted。
 *
 * mock 方式照仓库现有测试（test/split-agent.test.mjs 等）：直接注入假 llm.stream，
 * 由 createModelCalls 产出真的 askObservation；全程不碰网络、不调真模型。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { createStore } from '../lib/store.js';
import { createModelCalls } from '../lib/sleep/model-calls.js';
import { runObservationPass } from '../lib/sleep/observation-pass.js';
import { enqueueObserveTask } from '../lib/store/observe-queue.js';
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
-- claim 侧按 entities.etype 收口（t-mwr4i2td-7n7n）：夹具直接插实体、绕过写侧登记，
-- 这里必须给真实 etype；默认 'unknown' 会被 claim 当非实体类跳过。
INSERT INTO entities(id, norm, display, etype) VALUES (1, '树莓派', '树莓派', 'device'), (2, 'alpha', 'Alpha', 'person'), (3, 'beta', 'Beta', 'person');
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
INSERT INTO meta(key, value) VALUES ('dim', '4');
`;

/** 临时库 + 真 store；observe 段直接给 pass 与写侧读。 */
function makeStore(observe) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-observation-pass-'));
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
    sleepLogFile: null,
    apiUrl: 'http://127.0.0.1:1',
    timeoutMs: 50,
    provider: 'fake',
    model: 'fake',
    embed: async (texts) => texts.map(() => [1, 0, 0, 0]),
    logger: { info() {}, warn() {} },
    pipeline: { textLint: false, writeGate: false },
    sleepTools: false,
    observe,
  };
  return { store: createStore(cfg), cfg, dir, dbPath };
}

/** 假 llm.stream：按需要返回一段 JSON 文本或抛错；onStream 可在模型跑期间动手脚。 */
function fakeModel(store, cfg, logs, { reply = '{"ops":[]}', throwFor = null, onStream = null } = {}) {
  const calls = [];
  const llm = {
    stream(options) {
      calls.push(options);
      const user = String(options?.messages?.[0]?.content?.[0]?.text || '');
      return (async function* fakeStream() {
        if (onStream) onStream(user);
        if (throwFor && user.includes(throwFor)) throw new Error('上游断开（测试注入）');
        yield { type: 'text-delta', text: reply };
      })();
    },
  };
  const ctx = { get: (name) => (name === 'llm' ? llm : undefined) };
  const model = createModelCalls({
    ctx,
    store,
    cfg,
    log: (line) => logs.push(line),
    lint: { lintRedoOnce: async () => ({}) },
  });
  return { model, llm, calls };
}

function count(store, sql, ...params) {
  return Number(store.db().prepare(sql).get(...params)?.n || 0);
}

function observationRows(store) {
  return store.db().prepare("SELECT id, text, status, supersededBy FROM memories WHERE type = 'observation' ORDER BY rowSeq").all();
}

function taskRow(store, key = '树莓派') {
  return store.db().prepare('SELECT * FROM observe_task WHERE entity_key = ?').get(key);
}

function edgesOf(store, id) {
  return store.db().prepare('SELECT dst, rel, origin FROM memory_link WHERE src = ? ORDER BY dst').all(id);
}

/** 两条同实体、不同来源根的 active world 证据（独立来源计数 = 2）。 */
async function seedEvidence(store, entity = '树莓派') {
  const a = await store.save(`2026-11-21 ${entity} 温度 45 度，风扇转速正常`, ['infra'], 'agent', {
    type: 'world', entities: [entity],
  });
  const b = await store.save(`2026-11-21 ${entity} 连续跑了 12 小时，内存占用 3G`, ['infra'], 'agent', {
    type: 'world', entities: [entity],
  });
  assert.ok(!a.discarded && !b.discarded, '证据必须落库成功');
  return [a, b];
}

function opsJson(ops) {
  return JSON.stringify({ ops });
}

function baseDeps({ store, cfg, logs, model, owner = 'test:1:1' }) {
  return {
    store,
    cfg,
    log: (line) => logs.push(String(line)),
    model,
    owner,
  };
}

test('enabled=false：不 claim、不调模型、队列一行不动', async () => {
  const { store, cfg } = makeStore({ enabled: false });
  const [a, b] = await seedEvidence(store);
  enqueueObserveTask(store.db(), '树莓派', { reason: 'test', now: 1000 });
  const logs = [];
  const { model, calls } = fakeModel(store, cfg, logs, {
    reply: opsJson([{ op: 'create', text: '不该落库的观察', evidence: [a.id, b.id], confidence: 0.8 }]),
  });

  const out = await runObservationPass(baseDeps({ store, cfg, logs, model }), {});
  assert.deepEqual(out, { skipped: true });
  assert.equal(calls.length, 0, '关着时一次模型都不调');
  const row = taskRow(store);
  assert.equal(String(row.status), 'pending', '任务仍是 pending');
  assert.equal(Number(row.version), 1, 'version 未变');
  assert.equal(String(row.lease_owner), '', '没被 claim（不写租约）');
  assert.equal(observationRows(store).length, 0);
});

test('dryRun：照跑模型、出计量；任务仍 pending、无新观察、无证据边', async () => {
  const { store, cfg } = makeStore({ enabled: false });
  const [a, b] = await seedEvidence(store);
  cfg.observe.enabled = true; // 播种时关着，免得写侧顺手登记把 version 抬高
  enqueueObserveTask(store.db(), '树莓派', { reason: 'test', now: 1000 });
  const logs = [];
  const { model, calls } = fakeModel(store, cfg, logs, {
    reply: opsJson([{ op: 'create', text: '树莓派 2026-11-21 运行稳定（演练）', evidence: [a.id, b.id], confidence: 0.8 }]),
  });

  const out = await runObservationPass(baseDeps({ store, cfg, logs, model }), { dryRun: true });
  assert.equal(out.dryRun, true);
  assert.equal(out.processed, 1);
  assert.equal(out.created, 1, 'dryRun 的 created 是本应落库的条数');
  assert.equal(calls.length, 1, 'dryRun 也要真调模型');

  const row = taskRow(store);
  assert.equal(String(row.status), 'pending', 'dryRun 不 claim、不改状态');
  assert.equal(Number(row.version), 1);
  assert.equal(String(row.lease_owner), '');
  assert.equal(observationRows(store).length, 0, '不落观察行');
  assert.equal(count(store, "SELECT count(*) AS n FROM memory_link WHERE rel = 'derived_from'"), 0, '不写证据边');

  const meter = logs.find((line) => line.startsWith('[observe][dry-run] entity=树莓派'));
  assert.ok(meter, 'dryRun 计量行必须带 [observe][dry-run]');
  assert.match(meter, /calls=1/);
  assert.match(meter, /accepted=1/);
  assert.match(meter, /ms=\d+/);
});

test('正常 create：观察 active + 2 条 derived_from 边 + 任务 done', async () => {
  const { store, cfg } = makeStore({ enabled: false });
  const [a, b] = await seedEvidence(store);
  cfg.observe.enabled = true; // 播种时关着，免得写侧顺手登记把 version 抬高
  enqueueObserveTask(store.db(), '树莓派', { reason: 'test', now: 1000 });
  const logs = [];
  const { model, calls } = fakeModel(store, cfg, logs, {
    reply: opsJson([
      { op: 'create', text: '树莓派 2026-11-21 温度和内存都在正常范围，连续运行稳定', evidence: [a.id, b.id], confidence: 0.85 },
    ]),
  });

  const out = await runObservationPass(baseDeps({ store, cfg, logs, model }), {});
  assert.equal(out.created, 1);
  assert.equal(out.updated, 0);
  assert.equal(out.failed, 0);
  assert.equal(calls.length, 1);

  const rows = observationRows(store);
  assert.equal(rows.length, 1, '落一条观察');
  assert.equal(String(rows[0].status), 'active', '初态 active：证据在 saveObservation 事务里已校验');
  const edges = edgesOf(store, rows[0].id);
  assert.equal(edges.length, 2, '两条证据各一条边');
  assert.deepEqual(edges.map((e) => [String(e.dst), String(e.rel), String(e.origin)]), [
    [a.id, 'derived_from', 'sleep'],
    [b.id, 'derived_from', 'sleep'],
  ]);

  const row = taskRow(store);
  assert.equal(String(row.status), 'done');
  assert.equal(Number(row.version), 1);
  assert.equal(String(row.last_error), '', 'done 时清 last_error');
});

test('update：旧观察归档、新观察替代、任务 done', async () => {
  const { store, cfg } = makeStore({ enabled: false });
  const [a, b] = await seedEvidence(store);
  const old = await store.saveObservation({
    text: '树莓派 2026-11-21 旧观察：温度正常', entities: ['树莓派'], evidenceIds: [a.id, b.id],
  });
  cfg.observe.enabled = true;
  enqueueObserveTask(store.db(), '树莓派', { reason: 'text_update', now: 1000 });
  const logs = [];
  const newText = '树莓派 2026-11-21 温度与内存都正常，连续运行 12 小时';
  const { model } = fakeModel(store, cfg, logs, {
    reply: opsJson([{ op: 'update', text: newText, evidence: [a.id, b.id], replaces: old.id, confidence: 0.9 }]),
  });

  const out = await runObservationPass(baseDeps({ store, cfg, logs, model }), {});
  assert.equal(out.updated, 1, 'update 计入 updated');
  assert.equal(out.created, 0);

  const prior = store.db().prepare('SELECT status, supersededBy FROM memories WHERE id = ?').get(old.id);
  assert.equal(String(prior.status), 'archived', '旧观察被归档');
  const rows = observationRows(store).filter((r) => String(r.status) !== 'archived');
  assert.equal(rows.length, 1, '只剩一条活观察');
  assert.equal(String(rows[0].text), newText);
  assert.equal(String(prior.supersededBy), String(rows[0].id));
  assert.equal(String(taskRow(store).status), 'done');
});

test('校验拒绝（证据不在本批）：任务 done、无观察、rejected 记日志', async () => {
  const { store, cfg } = makeStore({ enabled: false });
  await seedEvidence(store);
  cfg.observe.enabled = true;
  enqueueObserveTask(store.db(), '树莓派', { reason: 'test', now: 1000 });
  const logs = [];
  const { model } = fakeModel(store, cfg, logs, {
    reply: opsJson([{ op: 'create', text: '拿了输入外证据的观察', evidence: ['m_not_in_input'], confidence: 0.9 }]),
  });

  const out = await runObservationPass(baseDeps({ store, cfg, logs, model }), {});
  assert.equal(out.created, 0);
  assert.equal(out.failed, 0, '模型答了、只是被校验拒：算正常完成');
  assert.equal(observationRows(store).length, 0, '不落观察');
  assert.equal(String(taskRow(store).status), 'done', '任务正常收尾');

  const line = logs.find((l) => l.includes('entity=树莓派') && l.includes('rejected=1'));
  assert.ok(line, '计量/日志里要能看见 rejected 计数');
  assert.match(line, /rejectedDetail=create:EVIDENCE_NOT_IN_INPUT/);
});

test('版本冲突：模型跑期间 enqueue → 不留活观察、任务仍 pending、version 涨', async () => {
  const { store, cfg } = makeStore({ enabled: false });
  const [a, b] = await seedEvidence(store);
  cfg.observe.enabled = true; // 播种时关着，免得写侧顺手登记把 version 抬高
  enqueueObserveTask(store.db(), '树莓派', { reason: 'test', now: 1000 });
  const logs = [];
  let bumped = false;
  const { model } = fakeModel(store, cfg, logs, {
    reply: opsJson([{ op: 'create', text: '证据在模型跑期间变了，这条应作废', evidence: [a.id, b.id], confidence: 0.8 }]),
    onStream: () => {
      // 模型跑到一半，写侧又登记了一次（同一实体）：version 1 → 2、租约清掉、状态回 pending
      if (!bumped) {
        bumped = true;
        enqueueObserveTask(store.db(), '树莓派', { reason: 'archive', now: 2000 });
      }
    },
  });

  const out = await runObservationPass(baseDeps({ store, cfg, logs, model }), {});
  assert.equal(out.versionMismatch, 1);
  assert.equal(out.created, 0, '冲突的结果不计入 created');

  const row = taskRow(store);
  assert.equal(String(row.status), 'pending', '任务保持 pending 等下一轮');
  assert.ok(Number(row.version) >= 2, 'version 涨了（冲突 enqueue + 补偿归档各一次）');
  assert.equal(String(row.lease_owner), '', '租约已清');

  const live = observationRows(store).filter((r) => ['active', 'pending_review'].includes(String(r.status)));
  assert.equal(live.length, 0, '补偿后不留下活观察（归档行允许存在）');
  assert.ok(logs.some((l) => l.includes('版本冲突')), '日志要能看见版本冲突');
});

test('模型抛错：failObserveTask 记 tries+1，不影响其它任务', async () => {
  const { store, cfg } = makeStore({ enabled: false });
  const [a1, b1] = await seedEvidence(store, '树莓派');
  const [a2, b2] = await seedEvidence(store, 'Alpha');
  cfg.observe.enabled = true;
  enqueueObserveTask(store.db(), '树莓派', { reason: 'test', now: 1000 });
  enqueueObserveTask(store.db(), 'alpha', { reason: 'test', now: 1001 });
  const logs = [];
  // 只在问「树莓派」时抛错；alpha 正常返回
  const { model } = fakeModel(store, cfg, logs, {
    reply: opsJson([{ op: 'create', text: 'Alpha 2026-11-21 运行稳定，两条证据一致', evidence: [a2.id, b2.id], confidence: 0.8 }]),
    throwFor: '实体：树莓派',
  });

  const out = await runObservationPass(baseDeps({ store, cfg, logs, model }), {});
  assert.equal(out.failed, 1, '失败只算树莓派那条');
  assert.equal(out.processed, 2);

  const bad = taskRow(store, '树莓派');
  assert.equal(String(bad.status), 'pending', '失败回 pending 等重试');
  assert.equal(Number(bad.tries), 1, 'tries+1');
  assert.match(String(bad.last_error), /上游断开/);

  const good = taskRow(store, 'alpha');
  assert.equal(String(good.status), 'done', '另一条照常收尾');
  assert.equal(observationRows(store).filter((r) => String(r.status) === 'active').length, 1);
});

test('计量行能解析出 calls / ms / accepted；汇总行单独一行', async () => {
  const { store, cfg } = makeStore({ enabled: false });
  const [a, b] = await seedEvidence(store);
  cfg.observe.enabled = true; // 播种时关着，免得写侧顺手登记把 version 抬高
  enqueueObserveTask(store.db(), '树莓派', { reason: 'test', now: 1000 });
  const logs = [];
  const { model } = fakeModel(store, cfg, logs, {
    reply: opsJson([{ op: 'create', text: '树莓派 2026-11-21 计量用观察，两条独立证据支持', evidence: [a.id, b.id], confidence: 0.8 }]),
  });

  await runObservationPass(baseDeps({ store, cfg, logs, model }), {});
  const line = logs.find((l) => l.startsWith('[observe] entity=树莓派'));
  assert.ok(line, '每实体一行计量');
  const num = (name) => {
    const got = line.match(new RegExp(`${name}=(\\d+)`));
    return got ? Number(got[1]) : null;
  };
  assert.equal(num('calls'), 1);
  assert.ok(num('promptChars') > 0, 'promptChars > 0');
  assert.ok(num('completionChars') > 0, 'completionChars > 0');
  assert.equal(num('accepted'), 1);
  assert.equal(num('rejected'), 0);
  assert.ok(num('ms') >= 0, 'ms 可解析');

  const summary = logs.find((l) => l.startsWith('[observe] 汇总'));
  assert.ok(summary, '收尾必须有汇总行');
  assert.match(summary, /calls=1/);
  assert.match(summary, /created=1/);
});

/* ── 复核分支（t-mwqf4fjy-d4sg）：pending_review 的自动观察在下一轮被消费 ── */

/** 把一条刚落的观察改成写侧复核后的样子：pending_review + 可选 needsModelReview。 */
function markPendingReview(store, id, { needsModelReview = false, reason = 'evidence-rewrite' } = {}) {
  const row = store.db().prepare('SELECT extra FROM memories WHERE id = ?').get(id);
  const extra = row?.extra ? JSON.parse(String(row.extra)) : {};
  if (needsModelReview) extra.needsModelReview = true;
  else delete extra.needsModelReview;
  store.db().prepare('UPDATE memories SET status = ?, reason = ?, extra = ? WHERE id = ?')
    .run('pending_review', reason, Object.keys(extra).length ? JSON.stringify(extra) : null, id);
}

/** 读观察行的复核相关字段（extra 摊平后取 needsModelReview）。 */
function reviewRow(store, id) {
  const row = store.db().prepare('SELECT * FROM memories WHERE id = ?').get(id);
  const extra = row?.extra ? JSON.parse(String(row.extra)) : {};
  return {
    status: String(row?.status || ''),
    reason: String(row?.reason || ''),
    validTo: row?.validTo === null || row?.validTo === undefined ? '' : String(row.validTo),
    needsModelReview: extra.needsModelReview,
    supersededBy: row?.supersededBy === null || row?.supersededBy === undefined ? '' : String(row.supersededBy),
  };
}

test('复核：证据仍够、无 needsModelReview → 恢复 active，0 次模型调用，任务 done', async () => {
  const { store, cfg } = makeStore({ enabled: false });
  const [a, b] = await seedEvidence(store);
  const obs = await store.saveObservation({
    text: '树莓派 2026-11-21 温度和内存都正常', entities: ['树莓派'], evidenceIds: [a.id, b.id],
  });
  markPendingReview(store, obs.id);
  const epochBefore = store.observationVersion();

  cfg.observe.enabled = true;
  enqueueObserveTask(store.db(), '树莓派', { reason: 'review', now: 1000 });
  const logs = [];
  const { model, calls } = fakeModel(store, cfg, logs, { reply: '{"ops":[]}' });

  const out = await runObservationPass(baseDeps({ store, cfg, logs, model }), {});
  assert.equal(out.review, 1, '走复核分支');
  assert.equal(out.restored, 1);
  assert.equal(out.archived, 0);
  assert.equal(out.created, 0, '复核任务不产生新观察');
  assert.equal(calls.length, 0, '程序能判就不调模型');

  const row = reviewRow(store, obs.id);
  assert.equal(row.status, 'active', '恢复 active');
  assert.equal(row.reason, 'review-ok');
  assert.equal(row.needsModelReview, undefined, 'needsModelReview 清掉');
  assert.ok(store.observationVersion() > epochBefore, 'bumpObservationEpoch');
  assert.equal(String(taskRow(store).status), 'done');
  assert.ok(logs.some((l) => l.includes('entity=树莓派') && l.includes('review=1') && l.includes('calls=0')));
});

test('复核：独立来源不足 → 归档，reason=evidence-review，0 次模型调用', async () => {
  const { store, cfg } = makeStore({ enabled: false });
  const [a] = await seedEvidence(store);
  const obs = await store.saveObservation({
    text: '树莓派 2026-11-21 只有一条证据撑着的结论', entities: ['树莓派'], evidenceIds: [a.id],
  });
  markPendingReview(store, obs.id);

  cfg.observe.enabled = true;
  enqueueObserveTask(store.db(), '树莓派', { reason: 'review', now: 1000 });
  const logs = [];
  const { model, calls } = fakeModel(store, cfg, logs, { reply: '{"ops":[]}' });

  const out = await runObservationPass(baseDeps({ store, cfg, logs, model }), {});
  assert.equal(out.review, 1);
  assert.equal(out.archived, 1);
  assert.equal(out.restored, 0);
  assert.equal(calls.length, 0, '来源不足是程序判定，不调模型');

  const row = reviewRow(store, obs.id);
  assert.equal(row.status, 'archived');
  assert.equal(row.reason, 'evidence-review');
  assert.ok(row.validTo, '归档写 validTo');
  assert.equal(String(taskRow(store).status), 'done');
});

test('复核：来源不足且证据全失效 → 同样归档到 evidence-review', async () => {
  const { store, cfg } = makeStore({ enabled: false });
  const [a, b] = await seedEvidence(store);
  const obs = await store.saveObservation({
    text: '树莓派 2026-11-21 证据后来都失效的结论', entities: ['树莓派'], evidenceIds: [a.id, b.id],
  });
  markPendingReview(store, obs.id);
  // 两条证据都被归档：边还在，但独立来源已不可用
  store.db().prepare("UPDATE memories SET status = 'archived' WHERE id IN (?, ?)").run(a.id, b.id);

  cfg.observe.enabled = true;
  enqueueObserveTask(store.db(), '树莓派', { reason: 'review', now: 1000 });
  const logs = [];
  const { model, calls } = fakeModel(store, cfg, logs, { reply: '{"ops":[]}' });

  const out = await runObservationPass(baseDeps({ store, cfg, logs, model }), {});
  assert.equal(calls.length, 0);
  assert.equal(out.archived, 1);
  const row = reviewRow(store, obs.id);
  assert.equal(row.status, 'archived');
  assert.equal(row.reason, 'evidence-review');
});

test('复核：needsModelReview + 模型给 update → 新旧替换，新 active、旧 archived，1 次调用', async () => {
  const { store, cfg } = makeStore({ enabled: false });
  const [a, b] = await seedEvidence(store);
  const old = await store.saveObservation({
    text: '树莓派 2026-11-21 旧结论：温度正常', entities: ['树莓派'], evidenceIds: [a.id, b.id],
  });
  markPendingReview(store, old.id, { needsModelReview: true });

  cfg.observe.enabled = true;
  enqueueObserveTask(store.db(), '树莓派', { reason: 'review', now: 1000 });
  const logs = [];
  const newText = '树莓派 2026-11-21 修订结论：温度与内存都正常，连续运行 12 小时';
  const { model, calls } = fakeModel(store, cfg, logs, {
    reply: opsJson([{ op: 'update', text: newText, evidence: [a.id, b.id], replaces: old.id, confidence: 0.9 }]),
  });

  const out = await runObservationPass(baseDeps({ store, cfg, logs, model }), {});
  assert.equal(out.review, 1);
  assert.equal(out.updated, 1, 'update 计入 updated');
  assert.equal(calls.length, 1, '需要改写才叫模型');

  const prior = reviewRow(store, old.id);
  assert.equal(prior.status, 'archived', '旧观察归档');
  assert.ok(prior.supersededBy, '旧观察带 supersededBy');
  const fresh = store.db().prepare(
    "SELECT id, status, text, reason FROM memories WHERE type = 'observation' AND status = 'active'",
  ).all();
  assert.equal(fresh.length, 1);
  assert.equal(String(fresh[0].text), newText);
  assert.equal(String(fresh[0].reason), 'observe:review:review');
  assert.equal(String(taskRow(store).status), 'done');
});

test('复核：needsModelReview + 空 ops → 恢复 active，reason=review-keep', async () => {
  const { store, cfg } = makeStore({ enabled: false });
  const [a, b] = await seedEvidence(store);
  const obs = await store.saveObservation({
    text: '树莓派 2026-11-21 结论在改写后仍成立', entities: ['树莓派'], evidenceIds: [a.id, b.id],
  });
  markPendingReview(store, obs.id, { needsModelReview: true });

  cfg.observe.enabled = true;
  enqueueObserveTask(store.db(), '树莓派', { reason: 'review', now: 1000 });
  const logs = [];
  const { model, calls } = fakeModel(store, cfg, logs, { reply: '{"ops":[]}' });

  const out = await runObservationPass(baseDeps({ store, cfg, logs, model }), {});
  assert.equal(calls.length, 1, 'needsModelReview 必须问一次模型');
  assert.equal(out.restored, 1);
  assert.equal(out.updated, 0);

  const row = reviewRow(store, obs.id);
  assert.equal(row.status, 'active');
  assert.equal(row.reason, 'review-keep');
  assert.equal(row.needsModelReview, undefined);
  assert.equal(String(taskRow(store).status), 'done');
});

test('复核：模型返回 create → REVIEW_NO_CREATE 拒掉，不长出新观察', async () => {
  const { store, cfg } = makeStore({ enabled: false });
  const [a, b] = await seedEvidence(store);
  const obs = await store.saveObservation({
    text: '树莓派 2026-11-21 复核中不许长出 create 的结论', entities: ['树莓派'], evidenceIds: [a.id, b.id],
  });
  markPendingReview(store, obs.id, { needsModelReview: true });

  cfg.observe.enabled = true;
  enqueueObserveTask(store.db(), '树莓派', { reason: 'review', now: 1000 });
  const logs = [];
  const { model } = fakeModel(store, cfg, logs, {
    reply: opsJson([{ op: 'create', text: '复核任务顺手写的一条全新观察', evidence: [a.id, b.id], confidence: 0.8 }]),
  });

  const out = await runObservationPass(baseDeps({ store, cfg, logs, model }), {});
  assert.equal(out.created, 0, 'create 被拒');
  assert.equal(out.updated, 0);
  assert.equal(observationRows(store).length, 1, '没有新观察行');

  const row = reviewRow(store, obs.id);
  assert.equal(row.status, 'active', '模型没给 update，按空 ops 保留结论');
  assert.equal(row.reason, 'review-keep');
  const line = logs.find((l) => l.includes('entity=树莓派') && l.includes('review=1'));
  assert.ok(line && line.includes('REVIEW_NO_CREATE'), '日志要能看见 REVIEW_NO_CREATE');
});

test('复核版本冲突：complete 前被 enqueue → 不误恢复、任务 pending', async () => {
  const { store, cfg } = makeStore({ enabled: false });
  const [a, b] = await seedEvidence(store);
  const obs = await store.saveObservation({
    text: '树莓派 2026-11-21 复核期间证据又变了的结论', entities: ['树莓派'], evidenceIds: [a.id, b.id],
  });
  markPendingReview(store, obs.id);

  cfg.observe.enabled = true;
  enqueueObserveTask(store.db(), '树莓派', { reason: 'review', now: 1000 });
  const logs = [];
  const { model, calls } = fakeModel(store, cfg, logs, { reply: '{"ops":[]}' });

  // claim 之后（status='leased'）立刻再登记一次：version 1 → 2，complete 的 CAS 必然失败
  let bumped = false;
  const baseNow = Date.now();
  const nowFn = () => {
    if (!bumped) {
      const row = store.db().prepare('SELECT status FROM observe_task WHERE entity_key = ?').get('树莓派');
      if (String(row?.status) === 'leased') {
        bumped = true;
        enqueueObserveTask(store.db(), '树莓派', { reason: 'archive', now: baseNow + 10 });
      }
    }
    return baseNow;
  };

  const out = await runObservationPass({ ...baseDeps({ store, cfg, logs, model }), now: nowFn }, {});
  assert.equal(out.versionMismatch, 1);
  assert.equal(calls.length, 0, '纯程序判定的复核路径不调模型');

  const row = reviewRow(store, obs.id);
  assert.equal(row.status, 'pending_review', '版本冲突时不许误恢复');
  assert.equal(row.reason, 'evidence-rewrite', '状态没被复核改写');
  const task = taskRow(store);
  assert.equal(String(task.status), 'pending');
  assert.ok(Number(task.version) >= 2);
  assert.ok(logs.some((l) => l.includes('版本冲突') && l.includes('review=1')));
});
