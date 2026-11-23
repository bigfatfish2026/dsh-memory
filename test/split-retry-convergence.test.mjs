/**
 * 2026-11-15：拆分失败必须可重试，且切出来不许比原文长一个字。
 *
 * 2026-11-15 换法（模型只给切分点，正文由代码 slice）之后，这个文件只钉两件事：
 *   ① 切分点协议下 charsAfter 恒等于 charsBefore，INFLATE_RATIO=1 不是「宽松红线」而是等值线；
 *   ② consumeSplitOps 连续失败：attempts 递增、状态始终 pending，到第 5 次才 dead，绝不写 done。
 *
 * 单个切分方案的重问（锚串找不到 / 拼回不等 → 重问一次）见 test/split-agent.test.mjs。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { applySchema } from '../scripts/apply-pipeline-schema.mjs';
import { split, validateParts, INFLATE_RATIO } from '../lib/agents/split.js';
import { consumeSplitOps } from '../lib/pipeline-split.js';
import { enqueue, MAX_ATTEMPTS } from '../lib/ingest-index.js';

function tmpDb() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-split-retry-'));
  const dbPath = path.join(dir, 'memory.db');
  new DatabaseSync(dbPath).close();
  applySchema(dbPath, { logger: () => {} });
  return { dir, dbPath, db: new DatabaseSync(dbPath) };
}

function rowsOf(dbPath) {
  const db = new DatabaseSync(dbPath, { readOnly: true });
  try {
    return db.prepare("SELECT * FROM pending_ops WHERE kind='split_item'").all();
  } finally {
    db.close();
  }
}

function fakeLlm(handlers) {
  const calls = [];
  return {
    calls,
    stream(options) {
      calls.push(options);
      const handler = handlers[Math.min(calls.length - 1, handlers.length - 1)];
      return (async function* gen() {
        for (const chunk of handler(options)) yield chunk;
      })();
    },
  };
}

const cutsChunks = (cuts) => [
  { type: 'tool-call-delta', index: 0, id: 'call_1', name: 'submit_parts', argumentsDelta: JSON.stringify({ cuts, unavoidable: false, reason: '' }) },
  { type: 'finish', reason: { kind: 'stop' } },
];

const brokenChunks = () => [
  { type: 'text-delta', text: '这不是 JSON' },
  { type: 'finish', reason: { kind: 'stop' } },
];

// 2026-11-23：正文中间必须有一个合法切点（句号），否则本地预检会直接拦下、根本走不到
// 模型失败重试这条路径——本文件钉的是「模型给了坏锚串/坏 JSON 之后怎么重试」，不是预检。
const SRC = `${'A'.repeat(50)}。${'B'.repeat(50)}`;
const CUTS = ['B'.repeat(8)];

test('切分点协议：正文全来自原文，charsAfter === charsBefore，INFLATE_RATIO 就是 1', () => {
  assert.equal(INFLATE_RATIO, 1, '不是红线收紧了，是等值线：多一个字就该失败');
  const ok = validateParts({ cuts: CUTS }, SRC);
  assert.equal(ok.ok, true);
  assert.equal(ok.parts.map((p) => p.text).join(''), SRC);
  assert.equal(ok.charsBefore, SRC.length);
  assert.equal(ok.charsAfter, SRC.length);
  assert.equal(ok.ratio, 1);

  // 同一个源上模型若「重写」出比原文多的字，切分点协议根本不接受：锚串带原文没有的字就找不到
  const bad = validateParts({ cuts: [`${'B'.repeat(8)}多出来的字`] }, SRC);
  assert.equal(bad.ok, false);
  assert.equal(bad.reason, 'anchor_not_found');
});

test('切不出来带 feedback 重问一次：第二次合格就正常返回，attempts 不动', async () => {
  const { dbPath, db } = tmpDb();
  const llm = fakeLlm([() => cutsChunks(['找不到的锚串']), () => cutsChunks(CUTS)]);
  const logs = [];
  const result = await split({ sourceId: 'm_src', text: SRC, db, llm, log: (line) => logs.push(line) });
  db.close();

  assert.ok(result, '重问一次合格后要返回结果');
  assert.equal(result.parts.length, 2);
  assert.equal(llm.calls.length, 2, '只重问一次');
  assert.match(String(llm.calls[1].messages[0].content[0].text), /找不到/);
  assert.equal(rowsOf(dbPath).length, 0, '合格就不该落 pending');
  assert.equal(logs.some((line) => line.includes('带 feedback 重问一次')), true);
});

test('两次都切不出来：落 pending，attempts=1，原文不动', async () => {
  const { dbPath, db } = tmpDb();
  const llm = fakeLlm([() => cutsChunks(['找不到的锚串'])]);
  const result = await split({ sourceId: 'm_src', text: SRC, db, llm });
  db.close();

  assert.equal(result, undefined);
  assert.equal(llm.calls.length, 2, '首问 + 重问一次');
  const rows = rowsOf(dbPath);
  assert.equal(rows.length, 1);
  assert.equal(rows[0].status, 'pending', '失败不许写 done');
  assert.equal(Number(rows[0].attempts), 1);
  assert.match(String(rows[0].last_error), /^anchor_not_found/);
  assert.equal(JSON.parse(rows[0].payload_json).text, SRC, '原文一个字不丢');
});

test('消费端连续失败：attempts 递增、始终 pending，到第 5 次才 dead', async () => {
  const { dbPath, db } = tmpDb();
  enqueue(db, 'split_item', 'm_src', { source_id: 'm_src', text: SRC, type: 'world' }, { now: 1 });
  const store = { metas: () => [] };
  const llm = fakeLlm([brokenChunks]);
  const logs = [];
  const base = Date.now();

  for (let round = 1; round <= MAX_ATTEMPTS; round += 1) {
    // 失败落账的 next_try_at 用真实时钟（recordSplitFailure），领活的 now 也得跟上
    await consumeSplitOps({
      db, store, llm, maxChars: 200, concurrency: 1, now: base + round * 1000, log: (line) => logs.push(line),
    });
    const [row] = rowsOf(dbPath);
    assert.equal(Number(row.attempts), round, `第 ${round} 轮 attempts 应该涨到 ${round}`);
    if (round < MAX_ATTEMPTS) {
      assert.equal(row.status, 'pending', `第 ${round} 轮失败要可重试`);
      assert.notEqual(row.last_error, 'stale_members');
    } else {
      assert.equal(row.status, 'dead', '到阈值才判死');
    }
  }
  db.close();
  assert.equal(logs.some((line) => line.includes('FAIL 拆不出合格分段')), true, '失败要逐条打日志');
});
