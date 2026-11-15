/**
 * prefill 取词跳过 runtime-context 快照（2026-11-08 t-prefill-v042）。
 *
 * 病灶：harness 每轮在 ego 念头（source.kind=dsh-ego）后面又插一条
 * source.kind=runtime-context 的 role=user 消息。lastUserText() 取「最后一条 user」，
 * 拿到的是环境快照，recallQuery 对它无括号可剥，召回词永远是那堆环境文字。
 *
 * 修法：倒序跳过快照，落回用户真发言或念头；全是快照时回退最后一条，不返回空。
 * 消息对象 shape 取自真实日志（deriveMessages 对 user/message 直接返回 event.data）。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { lastUserText, setupAutoPrefill } from '../lib/prefill.js';

/** 真日志里的快照原文（截取前两段，够判定了）。 */
const RUNTIME_SNAPSHOT = [
  'Current runtime context. This snapshot supersedes earlier runtime-context snapshots.',
  '',
  '现在 2026-11-08 周日 19:00（北京时间，半价），本机 45℃，根分区已用 15%。',
  '',
  'Current working directory: "/srv/app".',
].join('\n');

/** 真日志里的 ego 念头原文。 */
const EGO_THOUGHT = [
  '（下面这句是你自己心里冒出来的念头，不是对方发来的消息，不用回应它，顺着它去做就行。）',
  '（这是你自己开的一轮：跟维护者说一句，短，一两句就好；别说「我来汇报」这种，也别列清单。）',
  'dsh-memory 的 pending_ops 表还欠 337 条没处理，要清掉那 55 条吗？',
].join('\n');

/** 按日志 shape 造一条 user 消息。 */
function userMessage(text, kind) {
  const message = { id: `m-${Math.random().toString(36).slice(2)}`, role: 'user', content: [{ type: 'text', text }] };
  if (kind) message.source = { kind };
  return message;
}

/** 灌一串消息进 autoPrefill，返回真正送进 store.recall 的 query 列表。 */
async function queriesForMessages(messages, sessionId) {
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
  const logFile = path.join(fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-prefill-source-')), 'prefill.log');
  setupAutoPrefill(ctx, store, { autoPrefillLogPath: logFile });
  assert.equal(typeof handler, 'function', 'assemble 钩子没挂上');

  const session = {
    id: sessionId,
    header: { origin: 'user' },
    deriveMessages: () => messages,
  };
  await handler({ sections: [] }, { agent: { session } }, async () => ({ sections: [] }));
  return sent;
}

test('取词：念头(dsh-ego) → runtime-context 快照 → 拿到念头那句，query 里没有环境文字', async () => {
  const messages = [userMessage(EGO_THOUGHT, 'dsh-ego'), userMessage(RUNTIME_SNAPSHOT, 'runtime-context')];
  assert.equal(lastUserText({ deriveMessages: () => messages }), EGO_THOUGHT);

  const sent = await queriesForMessages(messages, 's-ego-then-snapshot');
  assert.equal(sent.length, 1);
  assert.ok(sent[0].includes('pending_ops 表还欠 337 条'), '召回词没够着念头');
  assert.ok(!sent[0].includes('Current runtime context'), '召回词还是快照');
  assert.ok(!sent[0].includes('根分区已用'), '召回词还是快照');
  assert.ok(!sent[0].includes('这是你自己开的一轮'), '括号指令没剥掉');
});

test('取词：用户短句 → runtime-context 快照 → 拿到用户那句', async () => {
  const line = '帮小林查一下上次改的 prefill 是哪天';
  const messages = [userMessage(line), userMessage(RUNTIME_SNAPSHOT, 'runtime-context')];
  assert.equal(lastUserText({ deriveMessages: () => messages }), line);

  const sent = await queriesForMessages(messages, 's-user-then-snapshot');
  assert.equal(sent.length, 1);
  assert.equal(sent[0], line);
});

test('取词：全是 runtime-context → 回退最后一条原文，不返回空', () => {
  const messages = [userMessage(RUNTIME_SNAPSHOT, 'runtime-context'), userMessage(RUNTIME_SNAPSHOT + '\n第二份', 'runtime-context')];
  const text = lastUserText({ deriveMessages: () => messages });
  assert.ok(text.length > 0, '回退成空串了');
  assert.equal(text, RUNTIME_SNAPSHOT + '\n第二份');
});

test('取词：隔了几轮的念头 → 快照 → 快照，仍落回最近那条非快照', () => {
  const messages = [
    userMessage('上上轮的话'),
    userMessage(RUNTIME_SNAPSHOT, 'runtime-context'),
    userMessage('上一轮的话'),
    userMessage(RUNTIME_SNAPSHOT, 'runtime-context'),
  ];
  assert.equal(lastUserText({ deriveMessages: () => messages }), '上一轮的话');
});

test('取词：普通单条用户消息 → 与改动前逐字一致（不回归）', async () => {
  const line = '帮小林查一下上次改的 prefill 是哪天';
  const messages = [userMessage(line)];
  assert.equal(lastUserText({ deriveMessages: () => messages }), line);

  const sent = await queriesForMessages(messages, 's-plain-single');
  assert.equal(sent.length, 1);
  assert.equal(sent[0], line);
});

test('取词：没有 source 的旧消息不受影响；取不到会话/消息返回空串', () => {
  assert.equal(lastUserText({ deriveMessages: () => [{ role: 'user', content: [{ type: 'text', text: '老消息' }] }] }), '老消息');
  assert.equal(lastUserText({}), '');
  assert.equal(lastUserText({ deriveMessages: () => [] }), '');
  assert.equal(lastUserText({ deriveMessages: () => [{ role: 'user', content: [{ type: 'image' }] }] }), '');
});
