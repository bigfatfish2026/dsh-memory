import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { applySchema } from '../scripts/apply-pipeline-schema.mjs';
import { createStore } from '../lib/store.js';

const VEC_SO = path.join(import.meta.dirname, '../node_modules/sqlite-vec-linux-arm64/vec0.so');

const MEMORY_DDL = `CREATE TABLE memories (
  id TEXT PRIMARY KEY, text TEXT NOT NULL, tags TEXT, source TEXT, ts TEXT, type TEXT,
  entities TEXT, validFrom TEXT, supersedes TEXT, updatedAt TEXT, status TEXT, reason TEXT,
  supersededBy TEXT, validTo TEXT, confidence REAL, extra TEXT, present TEXT, rowSeq INTEGER NOT NULL
);`;

function fakeLlm(handler) {
  const calls = [];
  return {
    calls,
    stream(options) {
      calls.push(options);
      return (async function* streamChunks() {
        for (const chunk of handler(options)) yield chunk;
      })();
    },
  };
}

const toolChunks = (args) => [
  { type: 'tool-call-delta', index: 0, id: 'call_1', name: 'submit_verdict', argumentsDelta: args },
  { type: 'finish', reason: { kind: 'stop' } },
];

function makeStore({ writeGate, llm = null, seedRows = [] } = {}) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-gate-store-'));
  const dbPath = path.join(dir, 'memory.db');
  const db = new DatabaseSync(dbPath, { allowExtension: true });
  db.loadExtension(VEC_SO);
  db.exec(MEMORY_DDL);
  db.exec('CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);');
  db.exec("INSERT INTO meta (key, value) VALUES ('dim', '4');");
  db.exec('CREATE VIRTUAL TABLE vec_items USING vec0(embedding float[4] distance_metric=cosine);');
  db.exec('CREATE TABLE vec_map (rowid INTEGER PRIMARY KEY, id TEXT);');
  let seq = 1;
  const insertMemory = db.prepare('INSERT INTO memories (id, text, tags, type, status, ts, rowSeq) VALUES (?, ?, ?, ?, ?, ?, ?)');
  const insertVecMap = db.prepare('INSERT INTO vec_map (rowid, id) VALUES (?, ?)');
  const insertVec = db.prepare('INSERT INTO vec_items (rowid, embedding) VALUES (?, ?)');
  for (const row of seedRows) {
    insertMemory.run(row.id, row.text, '[]', 'world', 'active', '2026-11-01T00:00:00Z', seq);
    insertVecMap.run(seq, row.id);
    insertVec.run(BigInt(seq), new Uint8Array(Float32Array.from([1, 0, 0, 0]).buffer));
    seq += 1;
  }
  db.close();
  applySchema(dbPath, { logger: () => {} });

  const cfg = {
    storePath: dir,
    storeLogFile: path.join(dir, 'store.log'),
    apiUrl: 'http://127.0.0.1:1',
    timeoutMs: 50,
    provider: 'fake',
    model: 'fake',
    rrfK: 60,
    opinionAlpha: 0.1,
    splitMaxChars: 200,
    embed: async (texts) => texts.map(() => [1, 0, 0, 0]),
    pipeline: { writeGate, writeGateThreshold: 0.9, textLint: false },
    logger: { info() {}, warn() {} },
  };
  const store = createStore(cfg);
  if (llm) store.attachLlm(llm);
  return { store, dir, dbPath };
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
  const db = new DatabaseSync(dbPath, { readOnly: true });
  try {
    return Number(db.prepare(sql).get(...params)?.n ?? 0);
  } finally {
    db.close();
  }
}

test('开关关：重复 save / update / merge 全放行，不调 llm、不写 discards', async () => {
  const bomb = { stream() { throw new Error('开关关时不该调模型'); } };
  const { store, dbPath } = makeStore({ writeGate: false, llm: bomb });

  const first = await store.save('树莓派温度 45 度', ['raspberrypi'], 'test', { type: 'world' });
  const second = await store.save('树莓派温度45度。', [], 'test', { type: 'world' });
  assert.ok(first.id);
  assert.ok(second.id);
  assert.equal(count(dbPath, 'SELECT count(*) AS n FROM memories'), 2);
  assert.equal(count(dbPath, 'SELECT count(*) AS n FROM discards'), 0);

  const merged = await store.merge([first.id, second.id], '树莓派温度 45 度，两份重复记录合并', [], { type: 'world' });
  assert.ok(merged.id);
  assert.equal(merged.text, '树莓派温度 45 度，两份重复记录合并');
  assert.equal(count(dbPath, "SELECT count(*) AS n FROM memories WHERE status='merged'"), 2);

  const updated = await store.update(first.id, { text: '树莓派温度 45 度（已复核）' });
  assert.equal(updated.text, '树莓派温度 45 度（已复核）');
  assert.equal(count(dbPath, 'SELECT count(*) AS n FROM discards'), 0);
});

test('开关开：完全重复被挡在库外，discards 记一笔', async () => {
  const llm = fakeLlm(() => toolChunks(JSON.stringify({ verdict: 'DISTINCT', matchedId: '', reason: '不该走到这里' })));
  const { store, dbPath } = makeStore({
    writeGate: true,
    llm,
    seedRows: [{ id: 'm_1', text: '树莓派温度 45 度' }],
  });
  const rec = await store.save('树莓派温度45度。', [], 'test', { type: 'world' });
  assert.equal(rec.discarded, true);
  assert.equal(rec.matchedId, 'm_1');
  assert.equal(llm.calls.length, 0);
  assert.equal(count(dbPath, 'SELECT count(*) AS n FROM memories'), 1);
  assert.equal(count(dbPath, "SELECT count(*) AS n FROM discards WHERE matched_id='m_1'"), 1);
  assert.equal(store.count(), 1, '内存里的 metas 也不能多');
});

test('开关开：近似候选交 judged，DISTINCT 才放行', async () => {
  const llm = fakeLlm(() => toolChunks(JSON.stringify({ verdict: 'DISTINCT', matchedId: '', reason: '主题相近但事实不同' })));
  const { store, dbPath } = makeStore({
    writeGate: true,
    llm,
    seedRows: [{ id: 'm_2', text: '树莓派今天有点热' }],
  });
  const rec = await store.save('树莓派风扇 3 号不转了', [], 'test', { type: 'world' });
  assert.equal(rec.discarded, undefined);
  assert.ok(rec.id);
  assert.equal(llm.calls.length, 1);
  assert.equal(count(dbPath, 'SELECT count(*) AS n FROM memories'), 2);
  assert.equal(count(dbPath, 'SELECT count(*) AS n FROM discards'), 0);
});

test('开关开：update 改成库里已有正文被挡，原条不动', async () => {
  const llm = fakeLlm(() => toolChunks(JSON.stringify({ verdict: 'DISTINCT', matchedId: '', reason: '备用' })));
  const { store, dbPath } = makeStore({
    writeGate: true,
    llm,
    seedRows: [
      { id: 'm_3', text: '树莓派温度 45 度' },
      { id: 'm_4', text: '树莓派风扇正常' },
    ],
  });
  const out = await store.update('m_4', { text: '树莓派温度45度。' });
  assert.equal(out.discarded, true);
  assert.equal(out.matchedId, 'm_3');
  const db = new DatabaseSync(dbPath, { readOnly: true });
  const row = db.prepare("SELECT text, status FROM memories WHERE id='m_4'").get();
  db.close();
  assert.equal(row.text, '树莓派风扇正常');
  assert.equal(row.status || 'active', 'active');
  assert.equal(count(dbPath, "SELECT count(*) AS n FROM discards WHERE matched_id='m_3'"), 1);
});

test('开关开：memory_merge 跟批外的旧条撞车时被挡，两个源条都不动', async () => {
  // 2026-11-05 t-fix-first-run：判重对象必须是「不在本批」的旧条。
  // 跟自己的成员撞不该再拦（那是本票第 1 处要修的 bug），所以这里加一条 m_8 当靶子。
  const llm = fakeLlm(() => toolChunks(JSON.stringify({ verdict: 'DUPLICATE', matchedId: 'm_8', reason: '已有同一件事' })));
  const { store, dbPath } = makeStore({
    writeGate: true,
    llm,
    seedRows: [
      { id: 'm_5', text: '树莓派温度 45 度' },
      { id: 'm_6', text: '树莓派今天有点热' },
      { id: 'm_8', text: '树莓派温度 45 度，今天有点热，风扇正常' },
    ],
  });
  const rec = await store.merge(['m_5', 'm_6'], '树莓派温度 45 度，今天有点热', [], { type: 'world' });
  assert.equal(rec.discarded, true);
  assert.equal(rec.matchedId, 'm_8');
  const prompt = llm.calls[0].messages[0].content[0].text;
  assert.ok(prompt.includes('"id":"m_8"'), '批外旧条 m_8 要在召回里');
  assert.ok(!prompt.includes('"id":"m_5"') && !prompt.includes('"id":"m_6"'), '本批成员不许出现在召回里');
  assert.equal(count(dbPath, 'SELECT count(*) AS n FROM memories'), 3);
  assert.equal(count(dbPath, "SELECT count(*) AS n FROM memories WHERE status='merged'"), 0);
  assert.equal(count(dbPath, "SELECT count(*) AS n FROM memories WHERE status='archived'"), 0);
  assert.equal(count(dbPath, "SELECT count(*) AS n FROM discards WHERE verdict='DUPLICATE'"), 1);
});

test('开关开：两次裁决都坏 → 抛 WRITE_GATE_PENDING、落 pending_ops、不写库', async () => {
  const llm = fakeLlm(() => [
    { type: 'text-delta', text: '不是 JSON' },
    { type: 'finish', reason: { kind: 'stop' } },
  ]);
  const { store, dbPath } = makeStore({
    writeGate: true,
    llm,
    seedRows: [{ id: 'm_7', text: '树莓派温度 45 度' }],
  });
  await assert.rejects(
    () => store.save('树莓派风扇换了', [], 'test', { type: 'world' }),
    (error) => error.code === 'WRITE_GATE_PENDING',
  );
  assert.equal(llm.calls.length, 2);
  assert.equal(count(dbPath, 'SELECT count(*) AS n FROM memories'), 1);
  assert.equal(count(dbPath, "SELECT count(*) AS n FROM pending_ops WHERE kind='dedupe_check'"), 1);
  // 2026-11-05 t-fix-first-run 第 4 处：未决必须能复查——payload 里带 source 和模型原文前 120 字
  const payload = JSON.parse(rows(dbPath, "SELECT payload_json FROM pending_ops WHERE kind='dedupe_check'")[0].payload_json);
  const reason = String(payload.reason || '');
  assert.match(reason, /source=text/, '通道要写出来');
  assert.match(reason, /raw=不是 JSON/, '模型原文前 120 字要写出来');
  assert.ok(reason.length < 400);
  // 超长原文只留 120 字
  assert.equal(reason.includes('不是 JSON不是 JSON'), false);
});

test('开关开：未决原文超长时只留前 120 字，且不带换行', async () => {
  const noisy = '这是一段不合法输出'.repeat(40);
  const llm = fakeLlm(() => [
    { type: 'text-delta', text: noisy },
    { type: 'finish', reason: { kind: 'stop' } },
  ]);
  const { store, dbPath } = makeStore({
    writeGate: true,
    llm,
    seedRows: [{ id: 'm_7', text: '树莓派温度 45 度' }],
  });
  await assert.rejects(() => store.save('树莓派风扇换了', [], 'test', { type: 'world' }));
  const payload = JSON.parse(rows(dbPath, "SELECT payload_json FROM pending_ops WHERE kind='dedupe_check'")[0].payload_json);
  const match = String(payload.reason).match(/raw=([\s\S]*?) err=/);
  assert.ok(match, 'raw 段要在');
  assert.ok(match[1].length <= 120, `raw 段 ${match[1].length} 字，超过 120`);
  assert.ok(match[1].startsWith('这是一段不合法输出'));
});

test('开关开：三条同主题合并成 1 条 → 能落库，不被自己的成员判重（首跑 22/22 全撞）', async () => {
  const llm = fakeLlm(() => {
    throw new Error('本批成员全被排除，模型根本不该被问');
  });
  const { store, dbPath } = makeStore({
    writeGate: true,
    llm,
    seedRows: [
      { id: 'm_5', text: '树莓派温度 45 度' },
      { id: 'm_6', text: '树莓派今天有点热' },
      { id: 'm_7', text: '树莓派风扇转速 3000 转' },
    ],
  });
  const text = '树莓派温度 45 度，今天有点热，风扇转速 3000 转';
  const rec = await store.merge(['m_5', 'm_6', 'm_7'], text, [], { type: 'world' });
  assert.equal(rec.discarded ?? false, false, '不许再被自己的成员判成 DUPLICATE');
  assert.equal(llm.calls.length, 0, '三个成员都该在 excludeIds 里，不该问模型');
  assert.equal(count(dbPath, 'SELECT count(*) AS n FROM memories'), 4, '三条旧条 + 一条合并结果');
  assert.equal(count(dbPath, "SELECT count(*) AS n FROM memories WHERE status='merged'"), 3);
  assert.equal(count(dbPath, "SELECT count(*) AS n FROM memories WHERE (status IS NULL OR status='active') AND text=?", text), 1);
  assert.equal(count(dbPath, "SELECT count(*) AS n FROM discards"), 0);
  const merged = rows(dbPath, 'SELECT supersedes FROM memories WHERE id=?', rec.id);
  assert.deepEqual(JSON.parse(merged[0].supersedes).sort(), ['m_5', 'm_6', 'm_7']);
});

test('开关开：update 时自己不进召回，别的旧条照样能挡', async () => {
  const llm = fakeLlm(() => toolChunks(JSON.stringify({ verdict: 'DUPLICATE', matchedId: 'm_2', reason: '已有同一件事' })));
  const { store, dbPath } = makeStore({
    writeGate: true,
    llm,
    seedRows: [
      { id: 'm_1', text: '树莓派温度 45 度' },
      { id: 'm_2', text: '树莓派风扇转速 3000 转' },
    ],
  });
  const r = await store.update('m_1', { text: '树莓派风扇转速 3000 转，今天正常' });
  assert.equal(r.discarded, true);
  assert.equal(r.matchedId, 'm_2');
  const prompt = llm.calls[0].messages[0].content[0].text;
  assert.ok(!prompt.includes('"id":"m_1"'), '被改的那条不许进召回（否则永远跟自己撞）');
  assert.ok(prompt.includes('"id":"m_2"'));
  assert.equal(count(dbPath, "SELECT count(*) AS n FROM memories WHERE id='m_1' AND text='树莓派温度 45 度'"), 1);
});

test('开关开：update 只跟自己同主题 → 直接放行改正文', async () => {
  const llm = fakeLlm(() => {
    throw new Error('只剩自己，不该问模型');
  });
  const { store, dbPath } = makeStore({
    writeGate: true,
    llm,
    seedRows: [{ id: 'm_1', text: '树莓派温度 45 度' }],
  });
  const r = await store.update('m_1', { text: '树莓派温度 46 度，风扇正常' });
  assert.equal(r.id, 'm_1');
  assert.equal(llm.calls.length, 0);
  assert.equal(count(dbPath, "SELECT count(*) AS n FROM memories WHERE id='m_1' AND text='树莓派温度 46 度，风扇正常'"), 1);
});

test('开关开：save 带 supersedes 时旧条不参与判重，直接取代', async () => {
  const llm = fakeLlm(() => {
    throw new Error('被取代的旧条该在 excludeIds 里，不该问模型');
  });
  const { store, dbPath } = makeStore({
    writeGate: true,
    llm,
    seedRows: [{ id: 'm_1', text: '树莓派温度 45 度' }],
  });
  // 第三个参数是来源，supersedes 在第四个小对象里
  const rec = await store.save('树莓派温度 46 度，空调开了', ['infra'], 'test', { type: 'world', supersedes: ['m_1'] });
  assert.equal(rec.discarded ?? false, false);
  assert.equal(llm.calls.length, 0);
  const old = rows(dbPath, 'SELECT status, supersededBy FROM memories WHERE id=?', 'm_1');
  assert.equal(old[0].supersededBy, rec.id);
  assert.deepEqual(JSON.parse(rows(dbPath, 'SELECT supersedes FROM memories WHERE id=?', rec.id)[0].supersedes), ['m_1']);
});
