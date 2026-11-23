/**
 * 拆分本地预检（2026-11-23，t-mwsviczp-epd3）。
 *
 * 钉四件事：
 *   ① 正文中间没有任何合法切点：split() / consumeSplitOps 都不调模型，原文不动、落 pending，
 *      日志里有可统计的 `[split] 预检不可拆 跳过模型`；
 *   ② 有合法切点（哪怕边界模糊）：照常送审，模型调用次数不变；
 *   ③ 代码块围栏内的换行/句号不算切点；围栏外的才算；
 *   ④ 正文为空/超短不崩：split('') 仍按旧约定抛错，预检函数对空串返回 false。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { applySchema } from '../scripts/apply-pipeline-schema.mjs';
import { consumeSplitOps } from '../lib/pipeline-split.js';
import { split, hasLegalCutPoint, legalCutPoints, CUT_MARKS } from '../lib/agents/split.js';
import { enqueue } from '../lib/ingest-index.js';

/** 两句各 30+ 字：避开正文 lint 的 too_short。 */
function twoSentenceText(tag) {
  const a = `小林去机房巡检时看了${tag}的运转情况，顺手记下当天的温湿度与磁盘读数。`;
  const b = `小林把${tag}上的灰擦干净，又核对了一遍当天的处理经过，最后写进值班记录里。`;
  return `${a}${b}`;
}

/** 一句话没有任何句末标点或换行：这种正文模型只会答「不可拆」。 */
const NO_CUT_TEXT = '小李把机房三号机柜的进风口温度与磁盘占用逐项记进值班本又核对了三遍才走';

function tmpDb() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-split-precheck-'));
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

/** 假模型：从提示词里取原文，在第一个句号后给一个切点；没有句号就给空 cuts。 */
function fakeLlm() {
  const calls = [];
  return {
    calls,
    stream(options) {
      calls.push(options);
      const prompt = String(options?.messages?.[0]?.content?.[0]?.text || '');
      const m = /）：([\s\S]*?)\n\n输出/.exec(prompt);
      const src = m ? m[1] : '';
      const cut = src.indexOf('。') + 1;
      const cuts = src && cut > 0 && cut < src.length ? [src.slice(cut, cut + 8)] : [];
      return (async function* gen() {
        yield {
          type: 'tool-call-delta', index: 0, id: 'call_1', name: 'submit_parts',
          argumentsDelta: JSON.stringify({ cuts, unavoidable: false, reason: '' }),
        };
        yield { type: 'finish', reason: { kind: 'stop' } };
      })();
    },
  };
}

function opRow(db, key) {
  return db.prepare('SELECT status, attempts, last_error, payload_json FROM pending_ops WHERE dedupe_key = ?').get(key);
}

test('无切点：split() 直接返回不可拆，不调模型，原文不动', async () => {
  const { db } = tmpDb();
  const llm = fakeLlm();
  const logs = [];
  const result = await split({
    sourceId: 'm_nocut', text: NO_CUT_TEXT, type: 'world', llm, provider: 'fake', model: 'fake',
    db, log: (line) => logs.push(line),
  });

  assert.equal(llm.calls.length, 0, '预检拦下后一次模型都没调');
  assert.equal(result.unavoidable, true);
  assert.equal(result.precheck, true);
  assert.deepEqual(result.parts, []);
  assert.match(result.reason, /precheck_unavoidable/);
  assert.equal(hasLegalCutPoint(NO_CUT_TEXT), false, '判据本身也认为是无切点');
  assert.match(logs.join('\n'), /\[split\] 预检不可拆 跳过模型 item=m_nocut len=\d+/);

  // 原文与 attempts 照旧落 pending_ops：一处不丢，下一轮还能再处理。
  const row = opRow(db, 'm_nocut');
  assert.equal(row.status, 'pending');
  assert.equal(row.attempts, 1);
  assert.match(String(row.last_error), /precheck_unavoidable/);
  assert.equal(JSON.parse(row.payload_json).text, NO_CUT_TEXT, '原文一个字都没动');
  db.close();
});

test('无切点：consumeSplitOps 前置预检，tried=0、precheckSkipped=1、op 回 pending', async () => {
  const { db } = tmpDb();
  const text = NO_CUT_TEXT;
  const store = fakeStore([{ id: 'm_1', text }]);
  const llm = fakeLlm();
  const logs = [];
  enqueue(db, 'split_item', 'm_1', { source_id: 'm_1', text, type: 'world' });

  const stats = await consumeSplitOps({
    db, store, llm, concurrency: 2, maxChars: 200, log: (line) => logs.push(line),
  });

  assert.equal(llm.calls.length, 0, '整条链上一次模型调用都没有');
  assert.equal(stats.scanned, 1);
  assert.equal(stats.tried, 0, '没调模型就不计 tried');
  assert.equal(stats.precheckSkipped, 1);
  assert.equal(stats.unavoidable, 1, '口径照旧算不可拆');
  assert.equal(stats.pendingAfter, 1, '回队等下一轮');
  assert.equal(stats.inserted, 0);
  assert.equal(store.metas().find((m) => m.id === 'm_1').status, 'active', '原条仍 active');
  assert.match(logs.join('\n'), /\[split\] 预检不可拆 跳过模型 item=m_1 len=\d+/);
  assert.match(logs.join('\n'), /\[split\] 预检不可拆 合计 n=1 saved_llm=1/);
  assert.match(logs.join('\n'), /预检跳过 1/);

  const row = opRow(db, 'm_1');
  assert.equal(row.status, 'pending');
  assert.match(String(row.last_error), /precheck_unavoidable/);
  assert.equal(JSON.parse(row.payload_json).text, text);
  db.close();
});

test('有切点：照常送审，模型调用一次，precheckSkipped=0', async () => {
  const { db } = tmpDb();
  const text = twoSentenceText('风扇');
  const store = fakeStore([{ id: 'm_2', text }]);
  const llm = fakeLlm();
  const logs = [];
  enqueue(db, 'split_item', 'm_2', { source_id: 'm_2', text, type: 'world' });

  const stats = await consumeSplitOps({
    db, store, llm, concurrency: 1, maxChars: 200, log: (line) => logs.push(line),
  });

  assert.equal(llm.calls.length, 1, '有切点必须送模型，不许为省 token 漏拆');
  assert.equal(stats.tried, 1);
  assert.equal(stats.precheckSkipped, 0);
  assert.equal(stats.inserted, 2);
  assert.match(logs.join('\n'), /\[split\] item=m_2 parts=2/);
  assert.doesNotMatch(logs.join('\n'), /预检不可拆/);
  db.close();
});

test('代码围栏内的换行/句号不算切点，围栏外的才算', () => {
  const fencedOnly = [
    '把下面这段配置原样贴进配置文件再重启服务',
    '```',
    'key=one',
    'key2=two',
    '```',
    '然后观察日志有没有报错',
  ].join('\n');
  // 围栏外这一行正好是换行分隔：围栏后的「然后…」前有换行 → 其实有切点。
  // 所以构造一个「所有换行都在围栏内」的正文：围栏外的两块只能连成一行。
  const allInside = `把下面这段配置原样贴进配置文件再重启服务\`\`\`\nkey=one\nkey2=two\n\`\`\`然后观察日志有没有报错`;
  assert.equal(legalCutPoints(allInside).length, 0, '围栏内的三个换行都不算切点');
  assert.equal(hasLegalCutPoint(allInside), false);
  assert.equal(hasLegalCutPoint(fencedOnly), true, '围栏外那一行换行是合法切点');

  // 围栏外的句号仍然算切点
  const outsidePeriod = `\`\`\`\nkey=one\n\`\`\`重启完成。日志里没有报错，值班结束。`;
  assert.equal(hasLegalCutPoint(outsidePeriod), true);
  assert.ok(legalCutPoints(outsidePeriod).length >= 1);

  // 引号内的句号也不算切点（同一张定界符表）
  const quoted = '值班员在记录里写了一句「重启完成。日志正常」就交班了';
  assert.equal(hasLegalCutPoint(quoted), false, '句号在引号内部，不算切点');
  // 孤立的直双引号（正则字符类那种）不许把后面整段当引号内：
  // 回放抓到过一条 `.../remind-sender.js:24-33 ... [^\s'"] ... 抛出点在 remind-sender.js:99。`
  // 只有一个 "，旧实现从它保护到末尾，把合法句号一起吞了（误拦）。
  const loneQuote = "抠值用 /^\\s*appSecret:\\s*'?([^\\s'\"]+)'?\\s*$/m，抛出点在 remind-sender.js:99。这与另一条同类。";
  assert.equal(hasLegalCutPoint(loneQuote), true, '没配对的直双引号不产生保护');
  assert.ok(legalCutPoints(loneQuote).some((at) => loneQuote.slice(0, at).includes('remind-sender.js:99。')));
  // CUT_MARKS 与 SENTENCE_MARKS 同源：句末标点 + 换行，分号被排除
  assert.ok(CUT_MARKS.includes('\n'));
  assert.equal(CUT_MARKS.includes('；'), false, '分号后不许切');
});

test('空正文/超短：split() 仍按旧约定抛错，预检函数不崩', async () => {
  await assert.rejects(() => split({ sourceId: 'm_empty', text: '', llm: fakeLlm() }), /text 为空/);
  await assert.rejects(() => split({ sourceId: 'm_blank', text: '   ', llm: fakeLlm() }), /text 为空/);

  assert.equal(hasLegalCutPoint(''), false);
  assert.equal(hasLegalCutPoint('   '), false);
  assert.equal(hasLegalCutPoint('短'), false);
  assert.equal(hasLegalCutPoint('。'), false, '只有标点、切完一侧为空 → 不算切点');
  assert.deepEqual(legalCutPoints(''), []);
  assert.equal(hasLegalCutPoint(undefined), false);
  assert.equal(hasLegalCutPoint(null), false);
});
