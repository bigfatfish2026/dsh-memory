/**
 * 2026-11-06 t-docs-align：split-pass 超长子段不再丢弃。
 *
 * 旧行为：normalizeItems 遇到 >200 字的子段直接 continue，短段入库后原条照样 archive，
 * 超长那半截正文凭空消失。新行为：超长段原样入 pending_ops(kind='split_item') 继续拆，
 * 源条保持 active，等拆分消费端处理完再归档。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { applySchema } from '../scripts/apply-pipeline-schema.mjs';
import { createStore } from '../lib/store.js';
import { normalizeItems, runSplitPass } from '../lib/split-pass.js';

const VEC_SO = path.join(import.meta.dirname, '../node_modules/sqlite-vec-linux-arm64/vec0.so');

const MEMORY_DDL = `CREATE TABLE memories (
  id TEXT PRIMARY KEY, text TEXT NOT NULL, tags TEXT, source TEXT, ts TEXT, type TEXT,
  entities TEXT, validFrom TEXT, supersedes TEXT, updatedAt TEXT, status TEXT, reason TEXT,
  supersededBy TEXT, validTo TEXT, confidence REAL, extra TEXT, present TEXT, rowSeq INTEGER NOT NULL
);`;

const SENTENCE = '树莓派温度四十五度风扇转速三千转磁盘用了百分之十五内存还剩两吉字节负载一点二持续了六小时';
const longText = (n) => SENTENCE.repeat(Math.ceil(n / SENTENCE.length)).slice(0, n);
const SOURCE = longText(300);
const SHORT_PART = `${SENTENCE.slice(0, 80)}。`;
const LONG_PART = longText(260);

function makeStore() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-split-pass-'));
  const dbPath = path.join(dir, 'memory.db');
  const db = new DatabaseSync(dbPath, { allowExtension: true });
  db.loadExtension(VEC_SO);
  db.exec(MEMORY_DDL);
  db.exec('CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);');
  db.exec("INSERT INTO meta (key, value) VALUES ('dim', '4');");
  db.exec('CREATE VIRTUAL TABLE vec_items USING vec0(embedding float[4] distance_metric=cosine);');
  db.exec('CREATE TABLE vec_map (rowid INTEGER PRIMARY KEY, id TEXT);');
  db.prepare(`INSERT INTO memories (id, text, tags, type, status, ts, rowSeq) VALUES (?, ?, '[]', 'world', 'active', '2026-11-01T00:00:00Z', 1)`)
    .run('m_src', SOURCE);
  db.prepare('INSERT INTO vec_map (rowid, id) VALUES (1, ?)').run('m_src');
  db.prepare('INSERT INTO vec_items (rowid, embedding) VALUES (?, ?)')
    .run(1n, new Uint8Array(Float32Array.from([1, 0, 0, 0]).buffer));
  db.close();
  applySchema(dbPath, { logger: () => {} });

  const cfg = {
    storePath: dir,
    storeLogFile: path.join(dir, 'store.log'),
    splitLogFile: path.join(dir, 'split.log'),
    splitStateFile: path.join(dir, 'split.state.json'),
    splitPreviewFile: path.join(dir, 'split-preview.md'),
    apiUrl: 'http://127.0.0.1:1',
    timeoutMs: 50,
    provider: 'fake',
    model: 'fake',
    splitMaxChars: 200,
    splitItemMaxChars: 200,
    splitMaxItems: 32,
    embed: async (texts) => texts.map(() => [1, 0, 0, 0]),
    logger: { info() {}, warn() {} },
    pipeline: { writeGate: false },
  };
  const store = createStore(cfg);
  return { store, dir, dbPath, cfg };
}

function rows(dbPath, sql, ...params) {
  const db = new DatabaseSync(dbPath, { readOnly: true });
  try {
    return db.prepare(sql).all(...params);
  } finally {
    db.close();
  }
}

test('normalizeItems 不再丢弃超长子段，打 tooLong 标记带出去', () => {
  const source = { id: 'm_src', text: SOURCE, tags: [], entities: [], type: 'world', validFrom: '' };
  const items = normalizeItems(
    [
      { text: SHORT_PART, type: 'world' },
      { text: LONG_PART, type: 'world' },
    ],
    source,
    { splitItemMaxChars: 200, splitMaxItems: 32 },
  );
  assert.equal(items.length, 2, '260 字子段不许被丢掉');
  assert.equal(items[0].tooLong, false);
  assert.equal(items[1].tooLong, true);
  assert.equal(items[1].text, LONG_PART);
});

test('runSplitPass：260 字子段入 pending_ops 继续拆，源条保持 active', async () => {
  const { store, dbPath, cfg } = makeStore();
  const llm = {
    calls: [],
    stream(options) {
      this.calls.push(options);
      const body = JSON.stringify({
        items: [
          { text: SHORT_PART, type: 'world' },
          { text: LONG_PART, type: 'world' },
        ],
      });
      return (async function* chunks() {
        yield { type: 'text-delta', text: body };
        yield { type: 'finish', reason: { kind: 'stop' } };
      })();
    },
  };

  const out = await runSplitPass({
    store, llm, cfg, dryRun: false, limit: 1, maxChars: 200,
  });

  assert.equal(out.tried, 1);
  assert.equal(out.details[0].action, 'queued');
  assert.equal(out.details[0].enqueued, 1);

  const ops = rows(dbPath, "SELECT dedupe_key, payload_json FROM pending_ops WHERE kind='split_item'");
  assert.equal(ops.length, 1, '260 字子段必须入队，不许消失');
  const payload = JSON.parse(ops[0].payload_json);
  assert.equal(payload.source_id, 'm_src');
  assert.equal(payload.text, LONG_PART);
  assert.equal(payload.length, 260);
  assert.equal(payload.reason, 'part_too_long');

  const source = rows(dbPath, 'SELECT status FROM memories WHERE id = ?', 'm_src');
  assert.equal(source[0].status, 'active', '有超长段在队列时源条必须保持 active');

  const log = fs.readFileSync(cfg.splitLogFile, 'utf8');
  assert.match(log, /\[split\] part_too_long enqueued=1/);
});
