import assert from 'node:assert/strict';
import test from 'node:test';
import { collectTurnChunks, mergeToolCalls, repairToolCallIds } from '../lib/sleep.js';

/**
 * 2026-11-06 t-fix-tooldup 的回归用例。
 *
 * 线上表现：ingest 每个文件第一跳调工具查记忆，流里同一个调用既来完整
 * tool-call 又来 tool-call-delta，旧代码两边都 push，calls 里同 id 出现两次，
 * 回填成 assistant.tool_calls + tool 消息后上游 400 `Duplicate value for
 * 'tool_call_id'`，7/7 文件全挂、入库 0 条。
 */

function fullCall(i) {
  return { type: 'tool-call', id: `call_${i}`, name: 'memory_recall', arguments: { query: `q${i}`, topK: 3 } };
}

function deltaOf(i, piece) {
  return { type: 'tool-call-delta', index: i, id: `call_${i}`, name: 'memory_recall', argumentsDelta: piece };
}

/** 按真实流水线的方式把一圈 calls 回填成消息：assistant(with tool-calls) + 每条一个 tool 消息。 */
function toMessages(calls) {
  return [
    {
      role: 'assistant',
      content: [
        { type: 'text', text: '我先查一下记忆。' },
        ...calls.map((c) => ({ type: 'tool-call', id: c.id, name: c.name, arguments: c.args })),
      ],
      source: { kind: 'model', provider: 'magpie', model: 'group/ds' },
    },
    ...calls.map((c) => ({
      role: 'tool',
      toolCallId: c.id,
      isError: false,
      content: [{ type: 'text', text: 'ok' }],
      source: { kind: 'tool', callId: c.id },
    })),
  ];
}

test('4 个完整 tool_call + 4 个同 id partial：calls 收成 4 条，id 唯一，tool 消息一一对应', async () => {
  const chunks = [];
  for (let i = 0; i < 4; i += 1) {
    chunks.push(deltaOf(i, '{"query":'));
    chunks.push(fullCall(i));
    chunks.push(deltaOf(i, `"q${i}"}`));
  }
  const { calls } = await collectTurnChunks(chunks);
  assert.equal(calls.length, 4, `calls 应为 4 条，实际 ${calls.length}：${JSON.stringify(calls.map((c) => c.id))}`);
  assert.equal(new Set(calls.map((c) => c.id)).size, 4, 'id 必须唯一');
  assert.deepEqual(calls.map((c) => c.id), ['call_0', 'call_1', 'call_2', 'call_3']);
  // 完整件为准：partial 的字符串 args 不能覆盖完整对象
  assert.deepEqual(calls[0].args, { query: 'q0', topK: 3 });

  const msgs = toMessages(calls);
  assert.equal(repairToolCallIds(msgs), 0, '合法体不该触发修复');
  const callIds = msgs[0].content.filter((p) => p.type === 'tool-call').map((p) => p.id);
  const toolIds = msgs.slice(1).filter((m) => m.role === 'tool').map((m) => m.toolCallId);
  assert.deepEqual(toolIds, callIds, 'tool 消息必须与 assistant 的 tool_call 按 id 一一对应');
});

test('partial 先到、完整后到（4 组全反过来）：仍只留完整件 4 条，id 唯一', async () => {
  const chunks = [];
  for (let i = 0; i < 4; i += 1) chunks.push(deltaOf(i, '{"query":'));
  for (let i = 0; i < 4; i += 1) chunks.push(fullCall(i));
  for (let i = 0; i < 4; i += 1) chunks.push(deltaOf(i, `"q${i}"}`));

  const { calls } = await collectTurnChunks(chunks);
  assert.equal(calls.length, 4, `calls 应为 4 条，实际 ${calls.length}：${JSON.stringify(calls.map((c) => c.id))}`);
  assert.equal(new Set(calls.map((c) => c.id)).size, 4, 'id 必须唯一');
  assert.deepEqual(calls[0].args, { query: 'q0', topK: 3 }, '完整件参数优先于 partial 拼接');

  const msgs = toMessages(calls);
  assert.equal(repairToolCallIds(msgs), 0);
  const callIds = msgs[0].content.filter((p) => p.type === 'tool-call').map((p) => p.id);
  assert.deepEqual(msgs.slice(1).map((m) => m.toolCallId), callIds);
});

test('只有 partial、没等到完整件：id 按 index 兜底生成，仍唯一', async () => {
  const chunks = [
    { type: 'tool-call-delta', index: 0, name: 'memory_recall', argumentsDelta: '{"query":"a"}' },
    { type: 'tool-call-delta', index: 1, name: 'memory_recall', argumentsDelta: '{"query":"b"}' },
  ];
  const { calls } = await collectTurnChunks(chunks);
  assert.equal(calls.length, 2);
  assert.equal(new Set(calls.map((c) => c.id)).size, 2, 'id 为空时也必须生成唯一 id');
  for (const c of calls) assert.ok(c.id, 'id 不能为空');
});

test('防御：assistant 里同 id 出现两次、tool 消息也重复，发请求前按 id 修掉', () => {
  const messages = [
    {
      role: 'assistant',
      content: [
        { type: 'tool-call', id: 'call_x', name: 'memory_recall', arguments: '{}' },
        { type: 'tool-call', id: 'call_x', name: 'memory_recall', arguments: '{}' },
        { type: 'tool-call', id: 'call_y', name: 'memory_recall', arguments: '{}' },
      ],
    },
    { role: 'tool', toolCallId: 'call_x', content: [{ type: 'text', text: 'a' }] },
    { role: 'tool', toolCallId: 'call_x', content: [{ type: 'text', text: 'b' }] },
    { role: 'tool', toolCallId: 'call_y', content: [{ type: 'text', text: 'c' }] },
    { role: 'user', content: [{ type: 'text', text: '继续' }] },
  ];
  const repaired = repairToolCallIds(messages);
  assert.equal(repaired, 2, '1 条重复 tool-call + 1 条重复 tool 消息');
  const ids = messages[0].content.filter((p) => p.type === 'tool-call').map((p) => p.id);
  assert.deepEqual(ids, ['call_x', 'call_y']);
  const toolIds = messages.slice(1, 3).map((m) => m.toolCallId);
  assert.deepEqual(toolIds, ['call_x', 'call_y'], '修完 tool 消息与 calls 一一对应');
  assert.equal(messages.at(-1).role, 'user', '别的消息不能被误删');
});

test('防御：assistant 有 call 但缺 tool 消息，补一条 error 结果而不是把非法体发出去', () => {
  const messages = [
    { role: 'assistant', content: [{ type: 'tool-call', id: 'call_z', name: 'memory_recall', arguments: '{}' }] },
  ];
  assert.equal(repairToolCallIds(messages), 1);
  assert.equal(messages.length, 2);
  assert.equal(messages[1].toolCallId, 'call_z');
  assert.equal(messages[1].isError, true);
});

test('mergeToolCalls 纯函数：重复完整件只留第一份', () => {
  const calls = mergeToolCalls(
    [
      { id: 'a', name: 't', args: '1' },
      { id: 'a', name: 't', args: '2' },
      { id: '', name: 't', args: '3' },
      { id: '', name: 't', args: '4' },
    ],
    new Map(),
  );
  assert.equal(calls.length, 3);
  assert.equal(new Set(calls.map((c) => c.id)).size, 3);
  assert.equal(calls[0].args, '1');
});
