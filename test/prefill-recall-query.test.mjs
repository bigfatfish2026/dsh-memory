/**
 * prefill 召回词剔除 harness 指令行（2026-11-08 t-prefill-fix）。
 *
 * 病灶：speak-up / dsh-ego 这类长指令消息整段是括号包裹的 harness 指令行，
 * 真意图排在最后又被 QUERY_MAX_CHARS 截掉，召回 top3 永远是插件规则条文。
 * 修法：送 recall 前把整行括号包裹的行剔掉；剔空回退原文；仍按 500 字截断。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { recallQuery, setupAutoPrefill } from '../lib/prefill.js';

/** 把一条用户消息灌进 autoPrefill，返回真正送进 store.recall 的 query 列表。 */
async function queriesFor(text, sessionId) {
  const sent = [];
  const store = {
    async recall(query) {
      sent.push(query);
      return [];
    },
  };
  let handler;
  const ctx = {
    on(event, fn) {
      if (event === 'system-prompt/assemble') handler = fn;
    },
    logger: { info() {} },
  };
  const logFile = path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-prefill-query-')), 'prefill.log');
  setupAutoPrefill(ctx, store, { autoPrefillLogPath: logFile });
  assert.equal(typeof handler, 'function', 'assemble 钩子没挂上');

  const session = {
    id: sessionId,
    header: { origin: 'user' },
    deriveMessages: () => [{ role: 'user', content: [{ type: 'text', text }] }],
  };
  await handler({ sections: [] }, { agent: { session } }, async () => ({ sections: [] }));
  return sent;
}

test('注入模板：三行括号指令 + 一句真意图 → query 只剩真意图', async () => {
  const text = [
    '（只许问、许查，不许改文件）',
    '（工具只用只读的那几个）',
    '（回答控制在三句以内）',
    '先 memory_recall 翻一下：手上有没有待办',
  ].join('\n');
  const sent = await queriesFor(text, 's-inject-template');
  assert.equal(sent.length, 1);
  assert.equal(sent[0], '先 memory_recall 翻一下：手上有没有待办');
  assert.ok(!sent[0].includes('只许问'), '括号指令残留在 query 里');
  assert.ok(!sent[0].includes('工具只用只读'), '括号指令残留在 query 里');
});

test('普通用户短句（无括号行）→ query 与原文逐字一致', async () => {
  const text = '帮小林查一下上次改的 prefill 是哪天';
  const sent = await queriesFor(text, 's-plain');
  assert.equal(sent.length, 1);
  assert.equal(sent[0], text);
});

test('全是括号行（剔除后为空）→ 回退原文，不报错、不空 query', async () => {
  const text = ['（查一下昨天的待办）', '（只读工具就好）'].join('\n');
  const sent = await queriesFor(text, 's-all-bracketed');
  assert.equal(sent.length, 1);
  assert.ok(sent[0].length > 0, 'query 被剔空了');
  assert.equal(sent[0], text);
});

test('半角括号 () 包裹的行同样剔除', async () => {
  const text = ['(only read-only tools)', '(no file edits)', '昨天那个内存的事后来怎么了'].join('\n');
  const sent = await queriesFor(text, 's-halfwidth');
  assert.equal(sent.length, 1);
  assert.equal(sent[0], '昨天那个内存的事后来怎么了');
});

test('剔除后仍超 500 字 → 正常截断到 500', async () => {
  const long = 'A'.repeat(700);
  const text = ['（' + 'x'.repeat(600) + '）', long].join('\n');
  const sent = await queriesFor(text, 's-long');
  assert.equal(sent.length, 1);
  assert.equal(sent[0].length, 500);
  assert.equal(sent[0], long.slice(0, 500));
});

test('recallQuery 本身：单字符括号行不算包裹，正文里的括号不误伤', () => {
  assert.equal(recallQuery('（'), '（');
  assert.equal(recallQuery('（看看这个）'), '（看看这个）');
  assert.equal(recallQuery('（看看这个）\n真的问题'), '真的问题');
  assert.equal(recallQuery('前面有字（不是整行）\n真的问题'), '前面有字（不是整行）\n真的问题');
  assert.equal(recallQuery(''), '');
});
