/**
 * 「不可拆」判定缓存 + 自动观察不进普通拆分（2026-11-22，t-mwriwazy-de37）。
 *
 * 钉两件事：
 *   ① 同一正文 + 同一字数上限 + 同一规则版本，第二轮不再问模型；命中后照旧回 pending、
 *      原文一个字不动，且缓存跨轮存活在 sleep.state.json 的 splitVerdictCache 里；
 *   ② 自动观察（extra.autoObservation === true）在消费端与 split-pass 入口都被挡住，
 *      手工 observation（无标记）照常拆，证明不是一刀切。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { applySchema } from '../scripts/apply-pipeline-schema.mjs';
import { consumeSplitOps } from '../lib/pipeline-split.js';
import { runSplitPass } from '../lib/split-pass.js';
import { createSleepState } from '../lib/sleep-state.js';
import {
  createSplitVerdictCache, splitVerdictKey, SPLIT_RULE_VERSION, SPLIT_VERDICT_FIELD,
} from '../lib/split-verdict-cache.js';
import { enqueue } from '../lib/ingest-index.js';

const T0 = Date.now();

/** 两句各 30+ 字：避开正文 lint 的 too_short，切点落在第二个句号后。 */
function twoSentenceText(tag) {
  const a = `小林去机房巡检时看了${tag}的运转情况，顺手记下当天的温湿度与磁盘读数。`;
  const b = `小林把${tag}上的灰擦干净，又核对了一遍当天的处理经过，最后写进值班记录里。`;
  return `${a}${b}`;
}

function tmpDb() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-split-verdict-'));
  const dbPath = path.join(dir, 'memory.db');
  new DatabaseSync(dbPath).close();
  applySchema(dbPath, { logger: () => {} });
  return { dir, dbPath, db: new DatabaseSync(dbPath) };
}

function fakeStore(rows) {
  const metas = rows.map((r) => ({ tags: [], entities: [], status: 'active', type: 'world', ...r }));
  const saved = [];
  const archived = [];
  let seq = 0;
  return {
    metas: () => metas,
    typeOf: () => 'world',
    async save(text) {
      seq += 1;
      const rec = { id: `new_${seq}`, text, status: 'active', tags: [], entities: [] };
      saved.push(rec);
      metas.push(rec);
      return rec;
    },
    async archive(id) {
      archived.push(id);
      const m = metas.find((x) => x.id === id);
      if (m) m.status = 'archived';
    },
    report: () => ({ saved: [...saved], archived: [...archived] }),
  };
}

/** 假模型：按提示词里的正文决定回「不可拆」还是给切点。 */
function fakeLlm({ unavoidableWords = [] } = {}) {
  const calls = [];
  return {
    calls,
    stream(options) {
      calls.push(options);
      const prompt = String(options?.messages?.[0]?.content?.[0]?.text || '');
      const m = /）：([\s\S]*?)\n\n输出/.exec(prompt);
      const src = m ? m[1] : '';
      const unavoidable = unavoidableWords.some((w) => src.includes(w));
      return (async function* gen() {
        if (unavoidable) {
          yield {
            type: 'tool-call-delta', index: 0, id: 'call_1', name: 'submit_parts',
            argumentsDelta: JSON.stringify({ cuts: [], unavoidable: true, reason: '条件与结论不可分' }),
          };
          yield { type: 'finish', reason: { kind: 'stop' } };
          return;
        }
        const cut = src.indexOf('。') + 1;
        const cuts = src && cut > 0 && cut < src.length ? [src.slice(cut, cut + 8)] : [];
        yield {
          type: 'tool-call-delta', index: 0, id: 'call_1', name: 'submit_parts',
          argumentsDelta: JSON.stringify({ cuts, unavoidable: false, reason: '' }),
        };
        yield { type: 'finish', reason: { kind: 'stop' } };
      })();
    },
  };
}

function opRows(db, keys) {
  const marks = keys.map(() => '?').join(',');
  return db.prepare(
    `SELECT dedupe_key, status, attempts, last_error, payload_json FROM pending_ops WHERE dedupe_key IN (${marks}) ORDER BY dedupe_key`,
  ).all(...keys);
}

test('判定缓存：同一正文第二轮不再调模型，命中仍回 pending、原文不丢、跨轮存活', async () => {
  const { dir, db } = tmpDb();
  const text = twoSentenceText('风扇');
  const rows = [
    { id: 'm_1', text },
    { id: 'm_2', text },
  ];
  const store = fakeStore(rows);
  const stateFile = path.join(dir, 'sleep.state.json');
  const stateStore = createSleepState({ file: stateFile });
  const logs = [];

  // 第一轮：m_1 问一次模型，判不可拆 → 回 pending，写缓存。
  enqueue(db, 'split_item', 'm_1', { source_id: 'm_1', text, type: 'world' });
  const llm = fakeLlm({ unavoidableWords: ['风扇'] });
  const cache1 = createSplitVerdictCache(stateStore);
  const first = await consumeSplitOps({
    db, store, llm, concurrency: 1, maxChars: 200,
    verdictCache: cache1, log: (line) => logs.push(line),
  });

  assert.equal(first.scanned, 1);
  assert.equal(llm.calls.length, 1, '第一轮问了一次模型');
  assert.equal(first.unavoidable, 1);
  assert.equal(first.cacheHits, 0, '冷缓存不命中');
  assert.equal(first.inserted, 0);
  assert.equal(first.archived, 0, '不可拆不归档原条');
  assert.equal(store.metas().find((m) => m.id === 'm_1').status, 'active', '原条仍 active');

  // 缓存落进 sleep.state.json 的 splitVerdictCache，键含正文哈希/上限/规则版本。
  const persisted = JSON.parse(fs.readFileSync(stateFile, 'utf8'));
  const key = splitVerdictKey(text, 200);
  assert.equal(persisted[SPLIT_VERDICT_FIELD][key].reason, '条件与结论不可分');
  assert.match(key, new RegExp(`\\|200\\|${SPLIT_RULE_VERSION.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`));
  // 键包含正文哈希 / 字数上限 / 规则版本三段：任一段变了一定不命中，必须重问模型。
  assert.equal(splitVerdictKey(text, 200), key);
  assert.notEqual(splitVerdictKey(`${text}补一句`, 200), key, '正文变了键就变');
  assert.notEqual(splitVerdictKey(text, 201), key, '字数上限变了键就变');
  assert.notEqual(splitVerdictKey(text, 200, 'split-rules-v2:00000000'), key, '规则版本变了键就变');
  assert.match(logs.join('\n'), /\[split\] verdict-cache hit=0 saved_llm=0 added=1 size=1 max=500 rules=split-rules-v1:/);

  // m_1 已回队并退避；把它推到很远的未来，隔离出「第二轮只领 m_2」。
  db.prepare("UPDATE pending_ops SET next_try_at = ? WHERE dedupe_key = 'm_1'").run(T0 + 999_999_999);
  enqueue(db, 'split_item', 'm_2', { source_id: 'm_2', text, type: 'world' });

  // 第二轮：新缓存实例从同一 state 文件加载（模拟下一轮/插件重载），命中不再调模型。
  const cache2 = createSplitVerdictCache(stateStore);
  assert.equal(cache2.size(), 1, '缓存跨轮存活');
  const logs2 = [];
  const second = await consumeSplitOps({
    db, store, llm, concurrency: 1, maxChars: 200,
    verdictCache: cache2, log: (line) => logs2.push(line),
  });

  assert.equal(llm.calls.length, 1, '第二轮一条都没问模型（省 1 次调用）');
  assert.equal(second.scanned, 1);
  assert.equal(second.tried, 0, '没调模型就不计 tried');
  assert.equal(second.unavoidable, 1);
  assert.equal(second.cacheHits, 1);
  assert.equal(second.pendingAfter, 1, '命中仍按不可拆回队');
  assert.equal(second.inserted, 0);
  assert.equal(store.report().archived.length, 0, '两条原条都没被归档');
  assert.match(logs2.join('\n'), /\[split\] item=m_2 unavoidable 命中判定缓存，未调模型 ms=0/);
  assert.match(logs2.join('\n'), /\[split\] verdict-cache hit=1 saved_llm=1 added=0 size=1 max=500 rules=split-rules-v1:/);

  // 条目没丢：两条 op 都在、回 pending、原文逐字还在 payload 里。
  const ops = opRows(db, ['m_1', 'm_2']);
  assert.equal(ops.length, 2);
  for (const op of ops) {
    assert.equal(op.status, 'pending', `${op.dedupe_key} 回 pending 等下一轮`);
    assert.match(String(op.last_error || ''), /unavoidable/);
    assert.equal(JSON.parse(op.payload_json).text, text, '原文一个字都没动');
  }
  db.close();
});

test('自动观察不进普通拆分（消费端）：不调模型、op 标 done、原条不动；手工观察照常拆', async () => {
  const { db } = tmpDb();
  const obsText = `${twoSentenceText('观测点甲')}${twoSentenceText('观测点乙')}`;
  const manualText = twoSentenceText('电源');
  const rows = [
    { id: 'm_obs', text: obsText, type: 'observation', autoObservation: true, version: 1 },
    { id: 'm_manual', text: manualText, type: 'observation' },
  ];
  const store = fakeStore(rows);
  const llm = fakeLlm();
  const logs = [];

  enqueue(db, 'split_item', 'm_obs', { source_id: 'm_obs', text: obsText, type: 'observation' });
  enqueue(db, 'split_item', 'm_manual', { source_id: 'm_manual', text: manualText, type: 'observation' });

  const stats = await consumeSplitOps({
    db, store, llm, concurrency: 2, maxChars: 200, log: (line) => logs.push(line),
  });

  assert.equal(llm.calls.length, 1, '只有手工观察那条问了模型');
  assert.equal(stats.autoObservationSkipped, 1, '自动观察被挡在普通拆分外');
  assert.equal(stats.skipped, 1);
  assert.equal(stats.scanned, 2);
  assert.equal(stats.inserted, 2, '手工观察照常拆成两段');
  assert.match(logs.join('\n'), /\[split\] item=m_obs 自动观察跳过普通拆分/);
  assert.match(logs.join('\n'), /自动观察跳过 1/);

  const ops = opRows(db, ['m_obs', 'm_manual']);
  const obsOp = ops.find((o) => o.dedupe_key === 'm_obs');
  assert.equal(obsOp.status, 'done', '自动观察的 op 直接终态，不再占队列');
  assert.equal(obsOp.last_error, 'auto_observation_skip');
  assert.equal(ops.find((o) => o.dedupe_key === 'm_manual').status, 'done');

  assert.deepEqual(store.report().archived, ['m_manual'], '只归档手工那条原条');
  assert.equal(store.metas().find((m) => m.id === 'm_obs').status, 'active', '自动观察原条一个字都没动');
  assert.equal(store.metas().find((m) => m.id === 'm_obs').text, obsText);
  // 拆出来的子段全部来自手工那条正文，不含自动观察的正文
  for (const rec of store.report().saved) {
    assert.ok(manualText.includes(rec.text), '子段来自手工观察原文');
    assert.ok(!obsText.includes(rec.text) || manualText.includes(rec.text));
  }
  db.close();
});

test('自动观察不进普通拆分（split-pass 入口）：不放进 targets，日志留 skip_auto_observation', async () => {
  const { dir } = tmpDb();
  const obsText = twoSentenceText('观测点丙');
  const manualText = twoSentenceText('网线');
  const store = {
    listAll: () => [
      { id: 'm_obs', text: obsText, type: 'observation', autoObservation: true, entities: [], tags: [], status: 'active' },
      { id: 'm_manual', text: manualText, type: 'observation', entities: [], tags: [], status: 'active' },
    ],
    async save() { throw new Error('dryRun 不该写库'); },
    async archive() { throw new Error('dryRun 不该归档'); },
  };
  const calls = [];
  const llm = {
    stream(options) {
      calls.push(options);
      const prompt = String(options?.messages?.[0]?.content?.[0]?.text || '');
      const src = prompt.includes(manualText) ? manualText : obsText;
      const cut = src.indexOf('。') + 1;
      const items = [
        { text: src.slice(0, cut), type: 'observation', entities: [], validFrom: '2026-11-22' },
        { text: src.slice(cut), type: 'observation', entities: [], validFrom: '2026-11-22' },
      ];
      return (async function* gen() {
        yield { type: 'text-delta', text: JSON.stringify({ items }) };
        yield { type: 'finish', reason: { kind: 'stop' } };
      })();
    },
  };
  const logFile = path.join(dir, 'memory_split.log');
  const previewFile = path.join(dir, 'split-preview.md');
  const out = await runSplitPass({
    store,
    llm,
    cfg: {
      splitStateFile: path.join(dir, 'split.state.json'),
      splitLogFile: logFile,
      splitPreviewFile: previewFile,
      pipeline: { textLint: false },
    },
    dryRun: true,
    maxChars: 10,
    concurrency: 1,
  });

  assert.equal(calls.length, 1, '只问了手工观察那条');
  assert.equal(out.tried, 1);
  assert.deepEqual(out.details.map((d) => d.id), ['m_manual']);
  const logText = fs.readFileSync(logFile, 'utf8');
  assert.match(logText, /\[split\] skip_auto_observation n=1/);
  const preview = fs.readFileSync(previewFile, 'utf8');
  assert.ok(!preview.includes(obsText), '预览里没有自动观察正文');
  assert.ok(preview.includes(twoSentenceText('网线')), '预览里有手工观察的正文');
});
