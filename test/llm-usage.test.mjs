/**
 * 2026-11-15：每次 LLM 调用都要记一行 usage（成功失败都记）。
 *
 * 现场：12:30 那轮日志 0 条 usage，consolidate 只在失败摘要里拼 in/out/cacheRead。
 * 这里锁四件事：
 *   ① 成功：usage 字段（in/out/cacheRead/reason）、耗时、调用点都进固定行；
 *   ② 失败：流抛错也记一行，ok=0 带 error，不把调用次数吞掉；
 *   ③ 宿主字段名不同（completionTokens / input_tokens）也认得；
 *   ④ 没给 log/cfg 时原样返回同一个 llm（测试身份断言不破）。
 */
import assert from 'node:assert/strict';
import test from 'node:test';
import { formatUsageLine, tapLlmUsage, usageFrom } from '../lib/llm-usage.js';
import { llmOf } from '../lib/llm-service.js';

function fakeLlm(chunks, { fail = null } = {}) {
  const calls = [];
  return {
    calls,
    other: 'keep',
    stream(options) {
      calls.push(options);
      return (async function* gen() {
        for (const chunk of chunks) yield chunk;
        if (fail) throw fail;
      })();
    },
  };
}

async function drain(stream) {
  const out = [];
  for await (const chunk of stream) out.push(chunk);
  return out;
}

test('usageFrom：认 camel / snake / completionTokens，认不出返回 null', () => {
  assert.deepEqual(usageFrom({ type: 'usage', usage: { inputTokens: 10, outputTokens: 5, cacheReadTokens: 3, reasoningTokens: 2 } }), {
    input: 10, output: 5, cacheRead: 3, reasoning: 2,
  });
  assert.deepEqual(usageFrom({ usage: { input_tokens: 7, completion_tokens: 4 } }), {
    input: 7, output: 4, cacheRead: null, reasoning: null,
  });
  assert.equal(usageFrom({ type: 'text-delta', text: 'x' }), null);
});

test('formatUsageLine：固定字段顺序，拿不到的数写 ?', () => {
  const line = formatUsageLine({ site: 'merge', provider: 'magpie', model: 'group/ds', usage: { input: 1, output: 2, cacheRead: 0, reasoning: 3 }, ms: 12, ok: true });
  assert.equal(line, '[usage] site=merge provider=magpie model=group/ds in=1 out=2 cacheRead=0 reason=3 ms=12 ok=1');
  const unknown = formatUsageLine({ site: 'split', ms: 5, ok: false, error: new Error('断流') });
  assert.match(unknown, /^\[usage\] site=split provider=\? model=\? in=\? out=\? cacheRead=\? reason=\? ms=5 ok=0 error=断流$/);
});

test('成功调用：流消费完写一行 usage，调用点/模型/耗时都在', async () => {
  const lines = [];
  const llm = fakeLlm([
    { type: 'text-delta', text: 'hi' },
    { type: 'usage', usage: { inputTokens: 123, outputTokens: 45, cacheReadTokens: 67, reasoningTokens: 8 } },
    { type: 'finish', reason: { kind: 'stop' } },
  ]);
  const tapped = tapLlmUsage(llm, { site: 'split', log: (line) => lines.push(line) });
  assert.equal(tapped.other, 'keep', '非 stream 属性原样透传');
  const got = await drain(tapped.stream({ provider: 'magpie', model: 'group/ds' }));
  assert.equal(got.length, 3, 'chunk 一个不少地透传');

  assert.equal(lines.length, 1);
  assert.match(lines[0], /^\[usage\] site=split provider=magpie model=group\/ds in=123 out=45 cacheRead=67 reason=8 ms=\d+ ok=1$/);
});

test('失败调用：流抛错也记一行，ok=0 带 error', async () => {
  const lines = [];
  const llm = fakeLlm([{ type: 'text-delta', text: '半截' }], { fail: new Error('上游断开') });
  const tapped = tapLlmUsage(llm, { site: 'entity', log: (line) => lines.push(line) });
  await assert.rejects(() => drain(tapped.stream({ provider: 'p', model: 'm' })), /上游断开/);
  assert.equal(lines.length, 1);
  assert.match(lines[0], /site=entity provider=p model=m in=\? out=\? .* ok=0 error=上游断开$/);
});

test('llmOf 不传 site 时原样返回；传了才包 usage', async () => {
  const lines = [];
  const base = fakeLlm([{ type: 'usage', usage: { inputTokens: 1, outputTokens: 2 } }, { type: 'finish', reason: { kind: 'stop' } }]);
  const ctx = { get: () => base, fiber: { uid: 1, state: 1 } };
  assert.equal(llmOf(ctx), base, '不传 site 不做包装');
  const tapped = llmOf(ctx, { site: 'consolidate', log: (line) => lines.push(line) });
  await drain(tapped.stream({ provider: 'magpie', model: 'group/ds' }));
  assert.equal(lines.length, 1);
  assert.match(lines[0], /site=consolidate .* in=1 out=2 .* ok=1/);
});
