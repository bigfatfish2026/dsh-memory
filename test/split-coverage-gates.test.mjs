/**
 * 拆分裂的「版本校验 → 覆盖校验」两道闸（t-mwogaubu-m1k8）。
 *
 * 钉五件事：
 *   ① 三段里中间一段被 lint 拒、且被剔内容没有任何落库承接 → 原条不归档，op 交人工；
 *      反过来，被剔内容只要还能在落库子段里找到，就不该误拦。
 *   ② 子段被判重（写入闸 DUPLICATE）且库里找不到活跃承接 → 不归档。
 *   ③ 拆分期间原条被改（含正文内部空格变化）→ 停，不归档；入口 payload.text 与库里不一致同样停。
 *   ④ 二次切分（超长段按句读再切）之后 offsets 与 parts 一一对应。
 *   ⑤ 子段 save 不再改原条的 supersededBy/validTo；归档那一笔事务才写。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { applySchema } from '../scripts/apply-pipeline-schema.mjs';
import { createStore } from '../lib/store.js';
import { consumeSplitOps, verifySplitCoverage } from '../lib/pipeline-split.js';
import { split } from '../lib/agents/split.js';
import { enqueue } from '../lib/ingest-index.js';

const VEC_SO = path.join(import.meta.dirname, '../node_modules/sqlite-vec-linux-arm64/vec0.so');
const SRC_ID = 'm_src';

const S1 = '2026-11-20 早上八点，小林把机房的温湿度抄进值班表，读数 23 度、41%。';
// 以「但是」开头：命中 text-lint 的 leading_conjunction，必被代码 lint 打回
const S2 = '但是机柜后面的灰没擦干净，风扇转速只有 1200 转，这个数字后来没人复核。';
const S3 = '下午两点，小林把 dsh-memory 的拆分日志翻了一遍，第 42 行写着进度 80%。';

function tmpDb() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-split-gates-'));
  const dbPath = path.join(dir, 'memory.db');
  new DatabaseSync(dbPath).close();
  applySchema(dbPath, { logger: () => {} });
  return { dir, dbPath, db: new DatabaseSync(dbPath) };
}

/** 假模型：从提示词里取回原文，按 planOf 给切分方案（切分点协议，只回 cuts/offsets）。 */
function fakeLlm(planOf) {
  const calls = [];
  return {
    calls,
    stream(options) {
      const prompt = String(options?.messages?.[0]?.content?.[0]?.text || '');
      const m = /）：([\s\S]*?)\n\n输出/.exec(prompt);
      const src = m ? m[1] : '';
      const plan = planOf(src, calls.length);
      calls.push(prompt);
      return (async function* chunks() {
        yield { type: 'text-delta', text: JSON.stringify(plan) };
        yield { type: 'finish', reason: { kind: 'stop' } };
      })();
    },
  };
}

/** 两段切分：第二段开头当锚。 */
function twoPartPlan(src) {
  const cut = src.indexOf('。') + 1;
  return { cuts: src && cut > 0 && cut < src.length ? [src.slice(cut, cut + 10)] : [] };
}

/**
 * 假 store：metas 就是库视图，save 默认落一条 active 记录（与真 store 一样立刻可查）。
 * saveImpl 可注入「判重丢弃 / 中途改原条」。
 */
function fakeStore({ sourceText = '', saveImpl = null, withSource = true } = {}) {
  const metas = withSource
    ? [{ id: SRC_ID, text: sourceText, status: 'active', tags: [], entities: [], source: 'sleep', ts: '2026-11-01T00:00:00Z', validFrom: '2026-11-01' }]
    : [];
  const saves = [];
  const archives = [];
  return {
    metas: () => metas,
    typeOf: () => 'world',
    async save(text, tags, source, meta) {
      saves.push({ text: String(text), meta: meta || {} });
      if (saveImpl) {
        const injected = await saveImpl({ metas, text: String(text), meta: meta || {}, saves });
        if (injected !== undefined) return injected;
      }
      const rec = { id: `child_${saves.length}`, text: String(text).trim(), status: 'active', tags: [], entities: [], source };
      metas.push(rec);
      return { id: rec.id, text: rec.text };
    },
    async archive(id, reason, opts) {
      archives.push({ id, reason, opts });
      const rec = metas.find((m) => m.id === id);
      if (rec) {
        rec.status = 'archived';
        if (opts?.supersededBy) rec.supersededBy = opts.supersededBy;
      }
    },
    saves,
    archives,
  };
}

function seed(db, payload) {
  enqueue(db, 'split_item', SRC_ID, payload, { now: 1 });
}

/* ─────────────── ① lint 剔段：没有承接就不归档 ─────────────── */

test('① 中间一段被 lint 拒且无落库承接：原条不归档，op 标 dead 交人工', async () => {
  const { db } = tmpDb();
  const source = `${S1}${S2}${S3}`;
  const llm = fakeLlm((src) => ({ cuts: [S2.slice(0, 12), S3.slice(0, 12)] }));
  const store = fakeStore({ sourceText: source });
  seed(db, { source_id: SRC_ID, text: source, type: 'world' });

  const logs = [];
  const stats = await consumeSplitOps({ db, store, llm, limit: 5, maxChars: 200, lintReview: false, log: (line) => logs.push(line) });

  assert.equal(stats.inserted, 2, 'S1/S3 入库');
  assert.equal(stats.archived, 0, '被剔的 S2 没有承接，不许归档');
  assert.equal(stats.coverageBlocked, 1);
  assert.equal(store.metas().find((m) => m.id === SRC_ID).status, 'active', '原条保持 active');
  const op = db.prepare("SELECT status, last_error FROM pending_ops WHERE kind='split_item'").get();
  assert.equal(op.status, 'dead');
  assert.equal(op.last_error, 'coverage_unverified');
  assert.match(logs.join('\n'), /coverage_unverified lint_discard_uncovered/);
  db.close();
});

test('① 反面：被剔内容能在别的落库子段里找到时，不误拦归档', async () => {
  // 把 S2 的正文并进 S1 那段之后：lint 还是拒 S2，但承接段里逐字包含它
  const merged = `${S1}${S2}`;
  const result = verifySplitCoverage({
    sourceText: `${merged}${S3}`,
    parts: [{ text: merged, start: 0, end: merged.length }, { text: S3, start: merged.length, end: merged.length + S3.length }],
    landedIds: ['c1', 'c2'],
    lintDiscards: [{ text: S2 }],
    dedupeDiscards: [],
    metas: [
      { id: 'c1', text: merged, status: 'active' },
      { id: 'c2', text: S3, status: 'active' },
    ],
  });
  assert.equal(result.ok, true, '被剔内容有承接，覆盖校验应放行');
});

/* ─────────────── ② 判重丢段：没有活跃承接就不归档 ─────────────── */

test('② 子段被判重且找不到活跃承接：不归档，op 标 dead 交人工', async () => {
  const { db } = tmpDb();
  const source = `${S1}${S2}`;
  const llm = fakeLlm((src) => twoPartPlan(src));
  // 写入闸一律判 DUPLICATE：不返回 id，也不往库里加记录
  const store = fakeStore({ sourceText: source, saveImpl: () => ({ discarded: true }) });
  seed(db, { source_id: SRC_ID, text: source, type: 'world' });

  const stats = await consumeSplitOps({ db, store, llm, limit: 5, maxChars: 200, textLint: false, lintReview: false });

  assert.equal(stats.inserted, 0);
  assert.equal(stats.deduped, 2);
  assert.equal(stats.archived, 0, '判重段没有活跃承接，原条不许归档');
  assert.equal(store.metas().find((m) => m.id === SRC_ID).status, 'active');
  const op = db.prepare("SELECT status, last_error FROM pending_ops WHERE kind='split_item'").get();
  assert.equal(op.status, 'dead');
  assert.equal(op.last_error, 'coverage_unverified');
  db.close();
});

/* ─────────────── ③ 拆中改正文 / 入口版本不一致 ─────────────── */

test('③ 拆分期间原条被改（正文内部多一个空格）：停，不归档', async () => {
  const { db } = tmpDb();
  const source = `${S1}${S2}`;
  const llm = fakeLlm((src) => twoPartPlan(src));
  let mutated = false;
  const store = fakeStore({
    sourceText: source,
    saveImpl: ({ metas }) => {
      if (!mutated) {
        mutated = true;
        // 内部空格变化（trim 两端抹不掉）也必须算版本变化
        metas[0].text = source.replace('，', '， ');
      }
      return undefined;
    },
  });
  seed(db, { source_id: SRC_ID, text: source, type: 'world' });

  const logs = [];
  const stats = await consumeSplitOps({ db, store, llm, limit: 5, maxChars: 200, textLint: false, lintReview: false, log: (line) => logs.push(line) });

  assert.equal(stats.archived, 0);
  assert.equal(stats.versionMismatch, 1);
  assert.equal(store.archives.length, 0, '版本变了就不许调 archive');
  const op = db.prepare("SELECT status, last_error FROM pending_ops WHERE kind='split_item'").get();
  assert.equal(op.status, 'dead');
  assert.equal(op.last_error, 'text_version_mismatch');
  assert.match(logs.join('\n'), /text_changed_during_split db_len=\d+ split_len=\d+ db_sha1=[0-9a-f]{40} split_sha1=[0-9a-f]{40}/);
  db.close();
});

test('③ 入口版本不一致：payload.text 与库里正文对不上，直接 dead，不叫模型', async () => {
  const { db } = tmpDb();
  const dbText = `${S1}${S2}`;
  const oldText = `${S1}${S2}旧版多出来的一段`;
  const llm = fakeLlm((src) => twoPartPlan(src));
  const store = fakeStore({ sourceText: dbText });
  seed(db, { source_id: SRC_ID, text: oldText, type: 'world' });

  const logs = [];
  const stats = await consumeSplitOps({ db, store, llm, limit: 5, maxChars: 200, log: (line) => logs.push(line) });

  assert.equal(llm.calls.length, 0, '版本对不上就不拆，不花模型调用');
  assert.equal(stats.versionMismatch, 1);
  assert.equal(stats.archived, 0);
  const op = db.prepare("SELECT status, last_error FROM pending_ops WHERE kind='split_item'").get();
  assert.equal(op.status, 'dead');
  assert.equal(op.last_error, 'text_version_mismatch');
  assert.match(logs.join('\n'), /op=1 text_version_mismatch payload_len=\d+ db_len=\d+ payload_sha1=[0-9a-f]{40} db_sha1=[0-9a-f]{40}/);
  db.close();
});

/* ─────────────── ④ 二次切分后 offsets 与 parts 一一对应 ─────────────── */

test('④ 超长段按句读再切后，offsets/parts 一一对应且每段区间逐字覆盖原文', async () => {
  const { db } = tmpDb();
  const head = '开头这句很短，用来说明上下文。';
  const sentence = `${'树莓派温度四十五度风扇三千转磁盘剩余十五个点网络延迟十二毫秒，'.repeat(3)}这是一段很长的正文。`;
  const longText = sentence.repeat(4);
  const tail = '结尾这句也很短，收个尾。';
  const source = `${head}${longText}${tail}`;
  // 只切一刀：head 与后面全部；后面按 200 字上限由代码二次切
  const llm = fakeLlm(() => ({ offsets: [head.length] }));

  const result = await split({ sourceId: SRC_ID, text: source, llm, maxChars: 200, textLint: false, lintReview: false });
  db.close();

  assert.ok(result);
  assert.ok(result.parts.length >= 4, `二次切分后应有多段，实测 ${result.parts.length}`);
  assert.equal(result.cuts.length, result.parts.length - 1, '切点数 = 段数 - 1');
  const trimmed = source.trim();
  let cursor = 0;
  for (let i = 0; i < result.parts.length; i += 1) {
    const part = result.parts[i];
    assert.equal(part.start, cursor, `第 ${i} 段起点接上一段`);
    assert.equal(part.end, part.start + part.text.length);
    assert.equal(trimmed.slice(part.start, part.end), part.text, `第 ${i} 段正文逐字来自原文`);
    if (i > 0) assert.equal(result.cuts[i - 1], part.start, `第 ${i} 个切点 = 该段起点`);
    cursor = part.end;
  }
});

/* ─────────────── ⑤ save 不再改原条 supersededBy/validTo ─────────────── */

const MEMORY_DDL = `CREATE TABLE memories (
  id TEXT PRIMARY KEY, text TEXT NOT NULL, tags TEXT, source TEXT, ts TEXT, type TEXT,
  entities TEXT, validFrom TEXT, supersedes TEXT, updatedAt TEXT, status TEXT, reason TEXT,
  supersededBy TEXT, validTo TEXT, confidence REAL, extra TEXT, present TEXT, rowSeq INTEGER NOT NULL
);`;

function makeRealStore() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-split-archive-'));
  const dbPath = path.join(dir, 'memory.db');
  const db = new DatabaseSync(dbPath, { allowExtension: true });
  db.exec(MEMORY_DDL);
  db.close();
  applySchema(dbPath, { logger: () => {} });
  const conn = new DatabaseSync(dbPath, { allowExtension: true });
  conn.loadExtension(VEC_SO);
  conn.exec("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);");
  if (!conn.prepare("SELECT value FROM meta WHERE key = 'dim'").get()) {
    conn.exec("INSERT INTO meta (key, value) VALUES ('dim', '4')");
  }
  conn.exec('CREATE VIRTUAL TABLE IF NOT EXISTS vec_items USING vec0(embedding float[4] distance_metric=cosine);');
  conn.exec('CREATE TABLE IF NOT EXISTS vec_map (rowid INTEGER PRIMARY KEY, id TEXT);');
  conn.close();
  const cfg = {
    storePath: dir,
    storeLogFile: path.join(dir, 'store.log'),
    apiUrl: 'http://127.0.0.1:1',
    timeoutMs: 50,
    provider: 'fake',
    model: 'fake',
    embed: async (texts) => texts.map(() => [1, 0, 0, 0]),
    pipeline: { textLint: false },
    logger: { info() {}, warn() {} },
  };
  return { store: createStore(cfg), dbPath };
}

function readAll(dbPath, sql, ...params) {
  const db = new DatabaseSync(dbPath, { readOnly: true });
  try {
    return db.prepare(sql).all(...params);
  } finally {
    db.close();
  }
}

test('⑤ 子段 save 不动原条 supersededBy/validTo；归档那一笔事务才写', async () => {
  const { store, dbPath } = makeRealStore();
  const source = `${S1}${S2}`;
  const src = await store.save(source, [], 'test', { type: 'world', validFrom: '2026-11-01' });
  const child1 = await store.save(S1, [], `sleep:split:${src.id}`, { type: 'world', __gateExclude: [src.id] });
  const child2 = await store.save(S2, [], `sleep:split:${src.id}`, { type: 'world', __gateExclude: [src.id] });

  let row = readAll(dbPath, 'SELECT status, supersededBy, validTo FROM memories WHERE id = ?', src.id)[0];
  assert.equal(row.status || 'active', 'active');
  assert.equal(row.supersededBy, null, '子段保存那一步不许改原条 supersededBy');
  assert.equal(row.validTo, null, '子段保存那一步不许改原条 validTo');
  for (const row2 of readAll(dbPath, 'SELECT supersedes FROM memories WHERE id IN (?, ?)', child1.id, child2.id)) {
    assert.equal(row2.supersedes, null, '子段本身也不写 supersedes');
  }

  await store.archive(src.id, '拆成 2 条', { supersededBy: child2.id, childValidFrom: '2026-11-01' });
  row = readAll(dbPath, 'SELECT status, supersededBy, validTo FROM memories WHERE id = ?', src.id)[0];
  assert.equal(row.status, 'archived');
  assert.equal(row.supersededBy, child2.id, '归档同一笔事务写 supersededBy');
  assert.equal(row.validTo, '2026-11-01', '归档同一笔事务写 validTo');
});
