/**
 * 2026-11-14：replay 的 merge_batch 段必须收敛。
 *
 * 三件事各测一条：
 *   ① 批里本来就没活（成员已被并掉 / 指纹已被整理段判过 noop|acted）→ 捡一次就 done，不算失败；
 *   ② 批里还有活、模型却给空 ops → 算一次失败，attempts+1，到 5 转 dead，不许原地不动；
 *   ③ 正常的批照旧落库，重放段不添乱。
 * 再加一条：同一批连续 6 轮重放，模型调用次数必须是有限的（<=5），回来一看仍是 dead，不再被捡起。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { applySchema } from '../scripts/apply-pipeline-schema.mjs';
import { createStore } from '../lib/store.js';
import { setupSleep, batchFingerprint } from '../lib/sleep.js';

const VEC_SO = path.join(import.meta.dirname, '../node_modules/sqlite-vec-linux-arm64/vec0.so');

const MEMORY_DDL = `CREATE TABLE memories (
  id TEXT PRIMARY KEY, text TEXT NOT NULL, tags TEXT, source TEXT, ts TEXT, type TEXT,
  entities TEXT, validFrom TEXT, supersedes TEXT, updatedAt TEXT, status TEXT, reason TEXT,
  supersededBy TEXT, validTo TEXT, confidence REAL, extra TEXT, present TEXT, rowSeq INTEGER NOT NULL
);`;

// 两两余弦 0.8（过 0.78 的默认阈值，凑成一批），整簇一批 = 三条同实体。
const SEEDS = [
  { id: 'm_1', text: '树莓派温度 45 度。', vec: [1, 0, 0, 0] },
  { id: 'm_2', text: '树莓派风扇转速 3000 转。', vec: [0.8, 0.6, 0, 0] },
  { id: 'm_3', text: '树莓派温度 45 度，风扇 3000 转，今晚重启过。', vec: [0.8, 0, 0.6, 0] },
];

const MERGE_OP = {
  op: 'merge',
  ids: ['m_1', 'm_2'],
  text: '树莓派温度 45 度，风扇转速 3000 转。',
  evidence: [
    { id: 'm_1', quote: '树莓派温度 45 度。' },
    { id: 'm_2', quote: '风扇转速 3000 转。' },
  ],
  needsSplit: false,
  reason: '同一台机器的两条状态',
};

// 过了证据校验、但落地必被规则挡下的 op：m_1 正文不是状态类、无标签，archive 一定被判
// 「非状态类条目，不归档」→ acted=0，正是 2026-11-14 这条要收口的场景。
const REJECT_OP = {
  op: 'archive',
  ids: ['m_1'],
  evidence: [{ id: 'm_1', quote: '树莓派温度 45 度。' }],
  reason: '这条状态过期了',
};

const FP = 'aaaabbbbccccdddd';

function makeStore({ agentPipeline = true, statuses = {}, questionMin = 0.78 } = {}) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-replay-converge-'));
  const dbPath = path.join(dir, 'memory.db');
  const db = new DatabaseSync(dbPath, { allowExtension: true });
  db.loadExtension(VEC_SO);
  db.exec(MEMORY_DDL);
  db.exec('CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);');
  db.exec("INSERT INTO meta (key, value) VALUES ('dim', '4');");
  db.exec('CREATE VIRTUAL TABLE vec_items USING vec0(embedding float[4] distance_metric=cosine);');
  db.exec('CREATE TABLE vec_map (rowid INTEGER PRIMARY KEY, id TEXT);');
  let seq = 1;
  const insertMemory = db.prepare(
    'INSERT INTO memories (id, text, tags, type, status, ts, entities, rowSeq) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
  );
  const insertVecMap = db.prepare('INSERT INTO vec_map (rowid, id) VALUES (?, ?)');
  const insertVec = db.prepare('INSERT INTO vec_items (rowid, embedding) VALUES (?, ?)');
  for (const row of SEEDS) {
    insertMemory.run(
      row.id,
      row.text,
      '[]',
      'world',
      statuses[row.id] || 'active',
      '2026-11-01T00:00:00Z',
      JSON.stringify(['树莓派']),
      seq,
    );
    insertVecMap.run(seq, row.id);
    insertVec.run(BigInt(seq), new Uint8Array(Float32Array.from(row.vec).buffer));
    seq += 1;
  }
  db.close();
  applySchema(dbPath, { logger: () => {} });

  const cfg = {
    storePath: dir,
    storeLogFile: path.join(dir, 'store.log'),
    sleepLogFile: path.join(dir, 'sleep.log'),
    stateFile: path.join(dir, 'sleep.state.json'),
    apiUrl: 'http://127.0.0.1:1',
    timeoutMs: 50,
    provider: 'fake',
    model: 'fake',
    rrfK: 60,
    opinionAlpha: 0.1,
    splitMaxChars: 200,
    embed: async (texts) => texts.map(() => [0.9, 0.2, 0.2, 0]),
    pipeline: { agentPipeline, writeGate: false, textLint: false },
    clusterMaxSize: 30,
    clusterNearDup: 0.9,
    clusterQuestionMin: questionMin,
    organizeConcurrency: 2,
    clusterChanges: 4,
    logger: { info() {}, warn() {} },
  };
  const store = createStore(cfg);
  return { store, cfg, dir, dbPath };
}

function fakeLlm(handlers) {
  const calls = [];
  return {
    calls,
    stream(options) {
      calls.push(options);
      const handler = handlers[Math.min(calls.length - 1, handlers.length - 1)];
      return (async function* streamChunks() {
        for (const chunk of handler(options)) yield chunk;
      })();
    },
  };
}

const toolChunks = (args) => [
  { type: 'tool-call-delta', index: 0, id: 'call_1', name: 'submit_ops', argumentsDelta: args },
  { type: 'finish', reason: { kind: 'stop' } },
];

function query(dbPath, sql, ...params) {
  const db = new DatabaseSync(dbPath, { readOnly: true });
  try {
    return db.prepare(sql).all(...params);
  } finally {
    db.close();
  }
}

/** 直接塞一条 merge_batch 账：模拟上一轮整理段失败留下的积压。 */
function seedPending(dbPath, {
  key = FP,
  ids = SEEDS.map((s) => s.id),
  attempts = 0,
  status = 'pending',
  nextTryAt = Date.now() - 1000,
  items = [],
  reason = null,
} = {}) {
  const db = new DatabaseSync(dbPath);
  try {
    db.prepare(
      `INSERT INTO pending_ops (kind, dedupe_key, payload_json, attempts, next_try_at, last_error, status, created_ts, updated_ts)
       VALUES ('merge_batch', ?, ?, ?, ?, 'call_fail', ?, ?, ?)`,
    ).run(
      key,
      JSON.stringify(reason === null ? { batch: key, ids, items } : { batch: key, ids, items, reason }),
      attempts,
      nextTryAt,
      status,
      Date.now(),
      Date.now(),
    );
  } finally {
    db.close();
  }
}

function readState(cfg) {
  try {
    return JSON.parse(fs.readFileSync(cfg.stateFile, 'utf8'));
  } catch {
    return {};
  }
}

const sleepLog = (cfg) => {
  try {
    return fs.readFileSync(cfg.sleepLogFile, 'utf8');
  } catch {
    return '';
  }
};

test('空批：成员已被并掉 → 捡一次标 done（stale_members），不算失败，下轮不再出现', async () => {
  const { store, cfg, dbPath } = makeStore({ statuses: { m_1: 'merged', m_2: 'merged' }, questionMin: 0.85 });
  seedPending(dbPath, { attempts: 2 });
  const llm = fakeLlm([() => toolChunks('{"ops":[]}')]);
  const sleep = setupSleep({ llm, provide() {}, effect: () => () => {}, tools: { register() {} } }, store, cfg);

  await sleep.organizePass({ dryRun: false });
  const rows = query(dbPath, "SELECT * FROM pending_ops WHERE kind='merge_batch'");
  assert.equal(rows.length, 1);
  assert.equal(rows[0].status, 'done', '批里没活就收口，别每轮重试');
  assert.equal(Number(rows[0].attempts), 2, '不算失败，attempts 一格都不加');
  assert.equal(rows[0].last_error, 'stale_members');
  assert.equal(llm.calls.length, 0, '空批连模型都不该问');
  assert.match(sleepLog(cfg), /kind=merge_batch id=\d+ attempts=2 status=done last_error=stale_members/);

  await sleep.organizePass({ dryRun: false });
  assert.equal(llm.calls.length, 0, '第二轮不再捡它');
  assert.equal(query(dbPath, "SELECT * FROM pending_ops WHERE status='pending'").length, 0);
});

test('空批：指纹已被整理段判过 noop / acted → 不调模型直接 done', async () => {
  const { store, cfg, dbPath } = makeStore({ questionMin: 0.85 });
  seedPending(dbPath, { key: 'aaaaaaaaaaaaaaa1' });
  seedPending(dbPath, { key: 'bbbbbbbbbbbbbbb2' });
  fs.writeFileSync(
    cfg.stateFile,
    JSON.stringify({
      organizeCache: {
        aaaaaaaaaaaaaaa1: { v: 'noop', t: Date.now() },
        bbbbbbbbbbbbbbb2: { v: 'acted', t: Date.now() },
      },
    }),
    'utf8',
  );
  const llm = fakeLlm([() => toolChunks('{"ops":[]}')]);
  const sleep = setupSleep({ llm, provide() {}, effect: () => () => {}, tools: { register() {} } }, store, cfg);

  await sleep.organizePass({ dryRun: false });
  const rows = query(dbPath, "SELECT dedupe_key, status, attempts, last_error FROM pending_ops ORDER BY dedupe_key");
  assert.equal(rows.length, 2);
  assert.deepEqual(rows.map((r) => r.status), ['done', 'done']);
  assert.deepEqual(rows.map((r) => r.last_error), ['cache_noop', 'cache_acted']);
  assert.deepEqual(rows.map((r) => Number(r.attempts)), [0, 0], '有结论的空批不算失败');
  assert.equal(llm.calls.length, 0, '整理段已经判过，不必再问模型');
});

test('非空批 + 模型给空 ops：算失败一次，attempts+1，第 5 次转 dead', async () => {
  const { store, cfg, dbPath } = makeStore({ questionMin: 0.85 });
  seedPending(dbPath, { attempts: 0 });
  const llm = fakeLlm([() => toolChunks('{"ops":[]}')]);
  const sleep = setupSleep({ llm, provide() {}, effect: () => () => {}, tools: { register() {} } }, store, cfg);

  for (let round = 1; round <= 5; round += 1) {
    await sleep.organizePass({ dryRun: false });
    const [row] = query(dbPath, "SELECT * FROM pending_ops WHERE kind='merge_batch'");
    assert.equal(Number(row.attempts), round, `第 ${round} 轮要把 attempts 推到 ${round}`);
    assert.equal(row.status, round >= 5 ? 'dead' : 'pending');
  }
  const [row] = query(dbPath, "SELECT * FROM pending_ops WHERE kind='merge_batch'");
  assert.equal(Number(row.attempts), 5, 'attempts 到 5 判死');
  assert.equal(row.status, 'dead');
  assert.equal(row.last_error, 'replay_empty');
  assert.equal(llm.calls.length, 5, '每轮只问一次，不原地重试');
  assert.match(sleepLog(cfg), /status=dead last_error=replay_empty/);
});

test('同一批连续 6 轮重放：最多问 5 次，之后不再被捡起（不无限重来）', async () => {
  const { store, cfg, dbPath } = makeStore({ questionMin: 0.85 });
  seedPending(dbPath, { attempts: 0 });
  const llm = fakeLlm([() => toolChunks('{"ops":[]}')]);
  const sleep = setupSleep({ llm, provide() {}, effect: () => () => {}, tools: { register() {} } }, store, cfg);

  for (let round = 1; round <= 6; round += 1) await sleep.organizePass({ dryRun: false });

  const [row] = query(dbPath, "SELECT * FROM pending_ops WHERE kind='merge_batch'");
  assert.equal(row.status, 'dead');
  assert.equal(Number(row.attempts), 5);
  assert.equal(llm.calls.length, 5, '6 轮里只出现有限次：dead 之后不再领');
  const replayLines = sleepLog(cfg).split('\n').filter((l) => /kind=merge_batch id=\d+ attempts=\d+ status=(pending|dead)/.test(l));
  assert.equal(replayLines.length, 5, '日志也只该有 5 行重放记录');
});

test('正常批：照旧落库合并，重放段不插一手', async () => {
  const { store, cfg, dbPath } = makeStore();
  const llm = fakeLlm([() => toolChunks(JSON.stringify({ ops: [MERGE_OP] }))]);
  const sleep = setupSleep({ llm, provide() {}, effect: () => () => {}, tools: { register() {} } }, store, cfg);

  const out = await sleep.organizePass({ dryRun: false });
  assert.equal(out.merged.length, 1);
  assert.deepEqual(out.merged[0].ids.sort(), ['m_1', 'm_2']);
  assert.equal(out.replay.merge_batch, undefined, '这一步没有账要重放');
  assert.equal(query(dbPath, 'SELECT * FROM pending_ops').length, 0, '成功批不进 pending_ops');
  assert.equal(query(dbPath, "SELECT id FROM memories WHERE status='merged'").length, 2);
  assert.equal(query(dbPath, "SELECT id FROM memories WHERE status='active'").length, 1);
});

test('判死的账：同一批再失败一次（只有摘要文字变了）不许被复活', async () => {
  const { store, cfg, dbPath } = makeStore();
  // 指纹按 sleep.js 的算法算，跟整理段写进来的键逐字节一致
  const cluster = SEEDS.map((s) => ({ id: s.id, text: s.text }));
  const fp = batchFingerprint(cluster);
  seedPending(dbPath, {
    key: fp,
    ids: cluster.map((m) => m.id).sort(),
    items: [...cluster].sort((a, b) => (a.id < b.id ? -1 : 1)),
    attempts: 5,
    status: 'dead',
    reason: 'call_fail finish=stop completionTokens=4000 tool=5字:"{oops"',
  });
  // 同样的批再来一次失败：模型吐的不是 JSON，reason 会变成 parse_fail，摘要也完全不同
  const broken = () => [
    { type: 'text-delta', text: '不是 JSON' },
    { type: 'tool-call-delta', index: 0, name: 'submit_ops', argumentsDelta: '{oops' },
    { type: 'finish', reason: { kind: 'stop' } },
  ];
  const llm = fakeLlm([broken]);
  const sleep = setupSleep({ llm, provide() {}, effect: () => () => {}, tools: { register() {} } }, store, cfg);

  await sleep.organizePass({ dryRun: false });
  const rows = query(dbPath, "SELECT * FROM pending_ops WHERE kind='merge_batch'");
  assert.equal(rows.length, 1, '不许另开一条新账');
  assert.equal(rows[0].status, 'dead', '摘要变了不算新证据，dead 得躺住');
  assert.ok(Number(rows[0].attempts) >= 5, 'attempts 绝不归零重来');
  assert.match(rows[0].last_error, /parse_fail/, '失败原因照旧落 last_error');
  assert.doesNotMatch(sleepLog(cfg), /\[retry\] revive/, '没有复活这回事');
  assert.doesNotMatch(sleepLog(cfg), /kind=merge_batch id=\d+ attempts=\d+ status=(pending|done)/, 'dead 的账不再被重放');
});

test('整理段 acted=0：写 noop 缓存 + 落一笔账，下一轮不再问模型（6 轮只问 1 次）', async () => {
  const { store, cfg, dbPath } = makeStore();
  const llm = fakeLlm([() => toolChunks(JSON.stringify({ ops: [REJECT_OP] }))]);
  const sleep = setupSleep({ llm, provide() {}, effect: () => () => {}, tools: { register() {} } }, store, cfg);

  const first = await sleep.organizePass({ dryRun: false });
  assert.equal(first.calls, 1, '第一轮按老样子问一次');
  assert.equal(first.rejected.length, 1, 'ops 过了校验，被写入规则挡下');
  assert.equal(first.merged.length + first.updated.length + first.archived.length, 0, '一条都没落地');

  const cacheRows = readState(cfg).organizeCache || {};
  assert.equal(Object.keys(cacheRows).length, 1, 'acted=0 的批要留缓存，别每轮重问');
  assert.equal(Object.values(cacheRows)[0].v, 'noop');
  assert.match(String(Object.values(cacheRows)[0].reason), /acted_none .*非状态类条目/, '缓存要带原因');

  let rows = query(dbPath, "SELECT * FROM pending_ops WHERE kind='merge_batch'");
  assert.equal(rows.length, 1, 'acted=0 也要有账，不能无账重来');
  assert.equal(Number(rows[0].attempts), 1, '落账即 attempts+1');

  for (let round = 2; round <= 6; round += 1) await sleep.organizePass({ dryRun: false });
  assert.equal(llm.calls.length, 1, '6 轮里只问过 1 次：第二轮起缓存命中，账由 replay 按缓存收口');

  rows = query(dbPath, "SELECT * FROM pending_ops WHERE kind='merge_batch'");
  assert.equal(rows.length, 1);
  assert.equal(rows[0].status, 'done', '缓存命中的账由 replay 收口成 done');
  assert.equal(rows[0].last_error, 'cache_noop');
  assert.equal(query(dbPath, "SELECT id FROM memories WHERE status='active'").length, 3, '打回的批一条记忆都不许动');
});

test('整理段 acted=0：缓存被轮转掉也不无账重来，attempts 爬到 5 转 dead 且不复活', async () => {
  const { store, cfg, dbPath } = makeStore();
  const llm = fakeLlm([() => toolChunks(JSON.stringify({ ops: [REJECT_OP] }))]);
  const sleep = setupSleep({ llm, provide() {}, effect: () => () => {}, tools: { register() {} } }, store, cfg);

  for (let round = 1; round <= 6; round += 1) {
    // 每轮把缓存清空，模拟 organizeCache 轮转/换机器丢掉结论
    fs.writeFileSync(cfg.stateFile, JSON.stringify({ organizeCache: {} }), 'utf8');
    await sleep.organizePass({ dryRun: false });
    const [row] = query(dbPath, "SELECT * FROM pending_ops WHERE kind='merge_batch'");
    assert.equal(Number(row.attempts), round, `第 ${round} 轮 attempts 要涨到 ${round}`);
  }
  const rows = query(dbPath, "SELECT * FROM pending_ops WHERE kind='merge_batch'");
  assert.equal(rows.length, 1, '只有一条账，没有另开新账');
  assert.equal(rows[0].status, 'dead', 'attempts 到 5 就该躺住');
  assert.ok(Number(rows[0].attempts) >= 5);
  assert.match(sleepLog(cfg), /\[merge\] WARN merge_batch dead/, '判死要留日志');
  assert.doesNotMatch(sleepLog(cfg), /\[retry\] revive/, '只有摘要/次数变化，不算新证据，不许复活');
  assert.equal(llm.calls.length, 6, '每轮各问一次：这是缓存被清掉后的兜底路径');
});
