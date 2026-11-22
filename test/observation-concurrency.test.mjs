/**
 * observation 并发与端到端（2026-11-21，t-mwqfj5xb-mims）。
 *
 * 钉五件事：
 *   ① 模型跑着的时候证据被 store.archive：模型仍返回引用该证据的 create →
 *      saveObservation 的证据校验拒（EVIDENCE_INACTIVE），无新观察、无证据边、任务 pending；
 *   ② 从写记忆触发登记的端到端版本冲突：模型跑期间同一实体再写一条 → 结果作废、
 *     任务 pending、version 增、不留下活观察；
 *   ③ 租约过期重领：version+1、owner 换掉；旧 owner 在新 owner 完成前后提交都返回 false；
 *   ④ 端到端正向：两条不同来源的世界记忆 → 写侧登记 → create → active 观察 + 2 条
 *      derived_from + 任务 done → memory_recall 召回、memory_entity 看到证据链；
 *   ⑤ 端到端负例：两条记忆同 sleep:split 父根 → create 被 INSUFFICIENT_INDEPENDENT 拒，
 *      无观察落库。
 *
 * 全程离线：临时库 + 假 embedding + 假 llm.stream（模板同 test/observation-pass.test.mjs），
 * 网络一次不连。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { createStore, registerStoreTools } from '../lib/store.js';
import { createModelCalls } from '../lib/sleep/model-calls.js';
import { OBSERVATION_REVIEW_NOTE } from '../lib/sleep/observation-prompt.js';
import { runObservationPass } from '../lib/sleep/observation-pass.js';
import {
  claimObserveTasks, completeObserveTask, enqueueObserveTask, failObserveTask,
} from '../lib/store/observe-queue.js';
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
INSERT INTO entities(id, norm, display, etype) VALUES (1, '树莓派', '树莓派', 'device'), (2, 'alpha', 'Alpha', 'person');
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
INSERT INTO meta(key, value) VALUES ('dim', '4');
`;

/** 临时库 + 真 store；observe 段给 pass 与写侧共用。 */
function makeStore(observe) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-observation-concurrency-'));
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
    topK: 8,
    minScore: 0.45,
    relativeGap: 0.1,
    rrfK: 60,
    embed: async (texts) => texts.map(() => [1, 0, 0, 0]),
    logger: { info() {}, warn() {} },
    pipeline: { textLint: false, writeGate: false },
    sleepTools: false,
    observe,
  };
  return { store: createStore(cfg), cfg, dir, dbPath };
}

/**
 * 假 llm.stream：onStream 在返回文本之前跑，允许 await（模型跑期间改库）。
 * 模板同 test/observation-pass.test.mjs 的 fakeModel。
 */
function fakeModel(store, cfg, logs, { reply = '{"ops":[]}', onStream = null } = {}) {
  const calls = [];
  const llm = {
    stream(options) {
      calls.push(options);
      const user = String(options?.messages?.[0]?.content?.[0]?.text || '');
      return (async function* fakeStream() {
        if (onStream) await onStream(user);
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
  return store.db().prepare("SELECT id, text, status FROM memories WHERE type = 'observation' ORDER BY rowSeq").all();
}

function liveObservations(store) {
  return observationRows(store).filter((r) => ['active', 'pending_review'].includes(String(r.status)));
}

function taskRow(store, key = '树莓派') {
  return store.db().prepare('SELECT * FROM observe_task WHERE entity_key = ?').get(key);
}

function edgesOf(store, id) {
  return store.db().prepare('SELECT dst, rel, origin FROM memory_link WHERE src = ? ORDER BY dst').all(id);
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

/* ── ① 模型跑着的时候证据被归档 ── */

test('模型跑期间证据被归档：落库按 EVIDENCE_INACTIVE 拒绝，无观察、无边、任务 pending', async () => {
  const { store, cfg } = makeStore({ enabled: false });
  const a = await store.save('2026-11-21 树莓派温度 45 度，风扇转速正常', ['infra'], 'agent', {
    type: 'world', entities: ['树莓派'],
  });
  const b = await store.save('2026-11-21 树莓派连续跑了 12 小时，内存占用 3G', ['infra'], 'agent', {
    type: 'world', entities: ['树莓派'],
  });
  assert.ok(!a.discarded && !b.discarded, '证据必须落库成功');
  cfg.observe.enabled = true; // 播种时关着，免得写侧登记把 version 抬高
  enqueueObserveTask(store.db(), '树莓派', { reason: 'test', now: 1000 });

  const logs = [];
  let archived = false;
  const { model } = fakeModel(store, cfg, logs, {
    reply: opsJson([
      { op: 'create', text: '证据在模型跑期间被撤销，这条不该落库', evidence: [a.id, b.id], confidence: 0.8 },
    ]),
    onStream: async () => {
      if (archived) return;
      archived = true;
      // 模型还没返回，证据就被别的路径归档；它仍被本批 allowed 允许，只能靠写事务再校验拦下
      await store.archive(a.id, '模型跑期间证据被撤销（测试注入）');
    },
  });

  const out = await runObservationPass(baseDeps({ store, cfg, logs, model }), {});
  assert.equal(out.created, 0, '被拒的 op 不计 created');
  assert.equal(out.updated, 0);
  assert.equal(out.failed, 1, '落库全失败按现有语义计一次失败');

  assert.equal(observationRows(store).length, 0, '不落观察行');
  assert.equal(
    count(store, "SELECT count(*) AS n FROM memory_link WHERE rel = 'derived_from'"),
    0,
    '不写证据边',
  );
  const row = taskRow(store);
  assert.equal(String(row.status), 'pending', '落库失败回 pending 等下一轮');
  assert.equal(Number(row.version), 2, '归档写侧登记把 version 1 → 2');
  assert.equal(Number(row.tries), 0, 'version 已被顶掉，旧 claim 的 fail CAS 不生效（现有语义）');
  assert.equal(String(row.lease_owner), '', '归档登记已清租约');

  const joined = logs.join('\n');
  assert.match(joined, /落库拒绝：证据记忆不是 active/, '日志要能看见 EVIDENCE_INACTIVE 的拒绝原因');
  assert.ok(joined.includes(a.id), '日志要指明是哪条证据失效');
  assert.match(joined, /FAIL 落库全失败/, '失败收尾也要留痕');

  const evidence = store.db().prepare('SELECT status FROM memories WHERE id = ?').get(a.id);
  assert.equal(String(evidence.status), 'archived', '证据确实已归档');
});

/* ── ①b 复核分支真的把复核约束送进模型输入 ── */

/** 把一条刚落的观察改成写侧复核后的样子：pending_review + needsModelReview。 */
function markPendingReview(store, id) {
  const row = store.db().prepare('SELECT extra FROM memories WHERE id = ?').get(id);
  const extra = row?.extra ? JSON.parse(String(row.extra)) : {};
  extra.needsModelReview = true;
  store.db().prepare('UPDATE memories SET status = ?, reason = ?, extra = ? WHERE id = ?')
    .run('pending_review', 'evidence-rewrite', JSON.stringify(extra), id);
}

test('复核分支问模型时 user message 末尾带 OBSERVATION_REVIEW_NOTE', async () => {
  const { store, cfg } = makeStore({ enabled: false });
  const a = await store.save('2026-11-21 树莓派温度 45 度，风扇转速正常', ['infra'], 'agent', {
    type: 'world', entities: ['树莓派'],
  });
  const b = await store.save('2026-11-21 树莓派连续跑了 12 小时，内存占用 3G', ['infra'], 'agent', {
    type: 'world', entities: ['树莓派'],
  });
  const obs = await store.saveObservation({
    text: '树莓派 2026-11-21 旧结论：温度正常', entities: ['树莓派'], evidenceIds: [a.id, b.id],
  });
  markPendingReview(store, obs.id);
  cfg.observe.enabled = true;
  enqueueObserveTask(store.db(), '树莓派', { reason: 'review', now: 1000 });

  const logs = [];
  const { model, calls } = fakeModel(store, cfg, logs, {
    reply: opsJson([{ op: 'update', text: '树莓派 2026-11-21 修订结论：温度与内存都正常', evidence: [a.id, b.id], replaces: obs.id, confidence: 0.9 }]),
  });
  const out = await runObservationPass(baseDeps({ store, cfg, logs, model }), {});
  assert.equal(out.review, 1);
  assert.equal(calls.length, 1, 'needsModelReview 要问一次模型');
  const user = String(calls[0]?.messages?.[0]?.content?.[0]?.text || '');
  assert.ok(user.endsWith(OBSERVATION_REVIEW_NOTE), '复核分支必须把复核约束拼在消息末尾');
});

/* ── ② 端到端版本冲突：从写记忆触发登记开始 ── */

test('写记忆触发的版本冲突：模型跑期间同实体再登记 → 结果作废、任务 pending、不留活观察', async () => {
  const { store, cfg } = makeStore({ enabled: true }); // 开着：写记忆就登记
  const a = await store.save('2026-11-21 树莓派温度 45 度，风扇转速正常', ['infra'], 'agent', {
    type: 'world', entities: ['树莓派'],
  });
  const b = await store.save('2026-11-21 树莓派连续跑了 12 小时，内存占用 3G', ['infra'], 'agent', {
    type: 'world', entities: ['树莓派'],
  });
  assert.ok(!a.discarded && !b.discarded);
  const seeded = taskRow(store);
  assert.equal(String(seeded.status), 'pending');
  assert.equal(Number(seeded.version), 2, '两条记忆各登记一次');
  assert.equal(String(seeded.reason), 'new-memory');

  const logs = [];
  let wrote = false;
  const { model } = fakeModel(store, cfg, logs, {
    reply: opsJson([
      { op: 'create', text: '树莓派 2026-11-21 温度和内存都在正常范围，连续运行稳定', evidence: [a.id, b.id], confidence: 0.85 },
    ]),
    onStream: async () => {
      if (wrote) return;
      wrote = true;
      // 模型跑到一半，同一实体又有新记忆落库 → 写侧登记把 version 2 → 3、任务回 pending
      const extra = await store.save('2026-11-21 树莓派刚换过风扇，转速记录已更新', ['infra'], 'agent', {
        type: 'world', entities: ['树莓派'],
      });
      assert.ok(!extra.discarded, '新增证据要落库');
    },
  });

  const out = await runObservationPass(baseDeps({ store, cfg, logs, model }), {});
  assert.equal(out.versionMismatch, 1, 'claim 的 version 在模型跑期间过期');
  assert.equal(out.created, 0, '冲突结果不计 created');
  assert.equal(liveObservations(store).length, 0, '补偿后不留活观察（archived 行允许存在）');

  const row = taskRow(store);
  assert.equal(String(row.status), 'pending', '任务保持 pending 等下一轮');
  assert.ok(Number(row.version) >= 3, 'version 至少涨到 3（新增证据登记）');
  assert.equal(String(row.lease_owner), '', '租约已清');
  assert.ok(logs.some((l) => l.includes('版本冲突')), '日志要能看见版本冲突');
});

/* ── ③ 租约过期重领 ── */

test('租约过期：重领涨 version、换 owner；旧 owner 在新 owner 完成前后提交都返回 false', async () => {
  const { store, cfg } = makeStore({ enabled: false });
  const db = store.db();
  enqueueObserveTask(db, '树莓派', { reason: 'lease', now: 1000 });

  const [first] = claimObserveTasks(db, { limit: 1, owner: 'owner-a', leaseMs: 60000, now: 2000 });
  assert.ok(first, '第一次 claim 要拿到任务');
  assert.equal(String(taskRow(store).lease_owner), 'owner-a');
  assert.equal(Number(taskRow(store).lease_until), 62000);

  // 模拟进程卡死：租约被改到过去
  db.prepare('UPDATE observe_task SET lease_until = ? WHERE entity_key = ?').run(1500, '树莓派');

  const [again] = claimObserveTasks(db, { limit: 1, owner: 'owner-b', leaseMs: 60000, now: 3000 });
  assert.ok(again, '租约过期后必须能重新领取');
  assert.equal(again.entityKey, '树莓派');
  assert.equal(again.version, first.version + 1, '重领涨 version，旧 claim 的版本立即失效');
  const reowned = taskRow(store);
  assert.equal(String(reowned.lease_owner), 'owner-b', 'owner 换掉');
  assert.equal(String(reowned.status), 'leased');
  assert.equal(Number(reowned.lease_until), 63000, '租约按新 claim 续期');

  // 旧 owner 赶在新 owner 完成之前提交：版本不符（owner 也不符），必须落不进去
  const beforeNewOwner = taskRow(store);
  assert.equal(
    completeObserveTask(db, '树莓派', first.version, { now: 3500, owner: 'owner-a' }),
    false,
    '旧 owner 在新 owner 完成前提交必须 false',
  );
  assert.equal(
    failObserveTask(db, '树莓派', first.version, '旧 owner 超时', { now: 3500, owner: 'owner-a' }),
    false,
    '旧 owner 的 fail 也不能清掉新租约',
  );
  assert.deepEqual(taskRow(store), beforeNewOwner, '状态 / owner / version 都不许动');
  assert.equal(String(taskRow(store).status), 'leased', '任务仍在新 owner 手里');

  // 新 owner 先跑完：任务置 done
  assert.equal(completeObserveTask(db, '树莓派', again.version, { now: 4000, owner: 'owner-b' }), true);
  assert.equal(String(taskRow(store).status), 'done');

  // 旧 owner 的迟到结果：任务已不是 leased，必须落不进去
  assert.equal(
    completeObserveTask(db, '树莓派', first.version, { now: 5000, owner: 'owner-a' }),
    false,
    '旧 owner 的迟到 complete 返回 false（任务已是 done）',
  );
  assert.equal(String(taskRow(store).status), 'done', '旧 owner 不得改回任何状态');
});

/* ── ④ 端到端正向：写记忆 → 登记 → 提炼 → 落库 → 召回 / 实体视图 ── */

test('端到端：两条独立来源记忆 → create 落 active + 2 条 derived_from + 任务 done → recall / memory_entity 可见证据链', async () => {
  const { store, cfg } = makeStore({ enabled: true });
  const a = await store.save('2026-11-21 树莓派温度 45 度，风扇转速正常', ['infra'], 'agent:chat-a', {
    type: 'world', entities: ['树莓派'],
  });
  const b = await store.save('2026-11-21 树莓派连续跑了 12 小时，内存占用 3G', ['infra'], 'agent:chat-b', {
    type: 'world', entities: ['树莓派'],
  });
  assert.ok(!a.discarded && !b.discarded);
  assert.equal(Number(taskRow(store).version), 2, '两条记忆各自登记一次');

  const obsText = '树莓派 2026-11-21 温度和内存都在正常范围，连续运行稳定';
  const logs = [];
  const { model, calls } = fakeModel(store, cfg, logs, {
    reply: opsJson([{ op: 'create', text: obsText, evidence: [a.id, b.id], confidence: 0.85 }]),
  });

  const out = await runObservationPass(baseDeps({ store, cfg, logs, model }), {});
  assert.equal(out.created, 1);
  assert.equal(out.failed, 0);
  assert.equal(calls.length, 1, '一个实体一次模型调用');

  const rows = observationRows(store);
  assert.equal(rows.length, 1, '落一条观察');
  assert.equal(String(rows[0].status), 'active', '初态 active');
  assert.equal(String(rows[0].text), obsText);

  const edges = edgesOf(store, rows[0].id);
  assert.equal(edges.length, 2, '两条证据各一条边');
  assert.deepEqual(edges.map((e) => [String(e.dst), String(e.rel), String(e.origin)]), [
    [a.id, 'derived_from', 'sleep'],
    [b.id, 'derived_from', 'sleep'],
  ]);
  assert.equal(String(taskRow(store).status), 'done', '任务提交为 done');
  assert.equal(String(taskRow(store).last_error), '');

  // memory_recall：默认召回能看见这条 active 自动观察
  const hits = await store.recall('树莓派温度和内存怎么样', 8);
  assert.ok(
    hits.some((hit) => hit.text === obsText && hit.type === 'observation'),
    'memory_recall 要能召回刚落库的观察',
  );

  // memory_entity：观察条目带 derived_from 证据 id，渲染出证据链
  const tools = [];
  const ctx = { tools: { register: (t) => tools.push(t) }, logger: { info() {}, warn() {} } };
  registerStoreTools(ctx, store, { logger: { info() {}, warn() {} } });
  const tool = tools.find((t) => t.name === 'memory_entity');
  assert.ok(tool, 'memory_entity 工具要注册出来');
  const view = await tool.execute({ name: '树莓派' }, {});
  assert.equal(view.observation, 1, '实体视图里 observation 计数为 1');
  const item = view.items.find((it) => it.type === 'observation');
  assert.ok(item, '实体视图要列出这条观察');
  assert.equal(item.text, obsText);
  assert.deepEqual(item.evidence.slice().sort(), [a.id, b.id].sort(), '观察条目要带两条证据 id');
  const rendered = tool.output.render({}, view)[0].text;
  assert.match(rendered, /证据：.+（2 条独立来源）/, '渲染要显示证据链与独立来源数');
});

/* ── ⑤ 端到端负例：同一来源根的两条记忆凑不出独立来源 ── */

test('端到端负例：两条记忆同 sleep:split 父根 → create 被 INSUFFICIENT_INDEPENDENT 拒，无观察落库', async () => {
  const { store, cfg } = makeStore({ enabled: true });
  const parent = 'm_parent_split_1';
  const a = await store.save('2026-11-21 树莓派温度 45 度，风扇转速正常', ['infra'], `sleep:split:${parent}`, {
    type: 'world', entities: ['树莓派'],
  });
  const b = await store.save('2026-11-21 树莓派连续跑了 12 小时，内存占用 3G', ['infra'], `sleep:split:${parent}`, {
    type: 'world', entities: ['树莓派'],
  });
  assert.ok(!a.discarded && !b.discarded);
  assert.equal(Number(taskRow(store).version), 2, '两条记忆各自登记一次');

  const logs = [];
  const { model, calls } = fakeModel(store, cfg, logs, {
    reply: opsJson([
      { op: 'create', text: '同一父条拆出来的两片不构成独立来源，这条该被拒', evidence: [a.id, b.id], confidence: 0.9 },
    ]),
  });

  const out = await runObservationPass(baseDeps({ store, cfg, logs, model }), {});
  assert.equal(calls.length, 1, '模型照问一次');
  assert.equal(out.created, 0, '校验拒绝，不落观察');
  assert.equal(out.failed, 0, '模型答了、被校验拒：算正常完成');
  assert.equal(observationRows(store).length, 0, '无观察行');
  assert.equal(
    count(store, "SELECT count(*) AS n FROM memory_link WHERE rel = 'derived_from'"),
    0,
    '无证据边',
  );
  assert.equal(String(taskRow(store).status), 'done', '证据不足也算正常收尾');
  const line = logs.find((l) => l.includes('entity=树莓派') && l.includes('rejected=1'));
  assert.ok(line, '日志要能看见 rejected 计数');
  assert.match(line, /create:INSUFFICIENT_INDEPENDENT/);
});
