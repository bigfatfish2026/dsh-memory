/**
 * entity agent 的思考档位。
 *
 * 事故：lib/agents/entity.js 里写死 reasoningEffort:'off'，memory 的路由是 magpie + group/ds，
 * dsh-llm 查到该模型没有 reasoning 元数据 → 抛 UNSUPPORTED_REASONING_EFFORT，整批 8 条全败。
 * 这组用例钉住三件事：
 *   1. 配置没给档位时，请求体里根本没有 reasoningEffort 这个键；
 *   2. 配置给了，但模型不认，就退化成不传，不发一个必然被拒的请求；
 *   3. 拿不到能力列表时（老 llm / 适配器不报元数据），被拒一次后自动去掉档位重试，不赔掉整批。
 */
import assert from 'node:assert/strict';
import test from 'node:test';
import { extractEntities, pickReasoningEffort } from '../lib/agents/entity.js';

const GOOD_JSON = JSON.stringify({ items: [{ i: 1, name: '树莓派', etype: 'device', attrs: {} }], links: [] });

const textChunks = (text) => [{ type: 'text-delta', text }, { type: 'finish', reason: { kind: 'stop' } }];

const unsupportedChunks = (message) => [
  {
    type: 'finish',
    reason: {
      kind: 'error',
      failure: { code: 'UNSUPPORTED_REASONING_EFFORT', message },
    },
  },
];

/**
 * 假 llm：只有真传了 reasoningEffort 且模型没有 reasoning 元数据时才「拒」，
 * 跟 dsh-llm 的 resolveCallWithInfo 行为对齐（requested !== undefined 且 info.reasoning === undefined → 抛）。
 */
function fakeLlm({ modelInfo, supported = true } = {}) {
  const calls = [];
  const llm = {
    calls,
    stream(options) {
      calls.push(options);
      const rejected = supported === false || (options.reasoningEffort !== undefined && modelInfo?.reasoning === undefined);
      if (rejected) {
        return (async function* rejectedStream() {
          for (const chunk of unsupportedChunks(
            `provider "${options.provider}" model "${options.model}" does not support reasoning effort "${options.reasoningEffort}"`,
          )) {
            yield chunk;
          }
        })();
      }
      return (async function* okStream() {
        for (const chunk of textChunks(supported === false ? '' : GOOD_JSON)) yield chunk;
      })();
    },
  };
  // 能力查询：只有显式给了 modelInfo 才挂这个方法，模拟「llm 版本支持 / 不支持 resolveModelInfo」
  if (modelInfo !== null) llm.resolveModelInfo = async () => {
    llm.infoCalls = (llm.infoCalls || 0) + 1;
    return modelInfo;
  };
  return llm;
}

const ITEMS = [{ id: 'm1', text: '维护者把树莓派换成了 Pi5' }];

test('pickReasoningEffort：空 / default / 非字符串都算不传', () => {
  assert.equal(pickReasoningEffort(undefined), undefined);
  assert.equal(pickReasoningEffort(''), undefined);
  assert.equal(pickReasoningEffort('   '), undefined);
  assert.equal(pickReasoningEffort('default'), undefined);
  assert.equal(pickReasoningEffort(123), undefined);
  assert.equal(pickReasoningEffort('low'), 'low');
});

test('配置没给档位：请求体里没有 reasoningEffort，模型能过', async () => {
  const llm = fakeLlm({ modelInfo: { provider: 'magpie', id: 'group/ds' } });
  const out = await extractEntities({ llm, provider: 'magpie', model: 'group/ds', items: ITEMS });
  assert.equal(llm.calls.length, 1);
  const sent = llm.calls[0];
  assert.equal(Object.hasOwn(sent, 'reasoningEffort'), false);
  assert.equal(sent.model, 'group/ds');
  assert.equal(sent.temperature, 0);
  assert.equal(Object.hasOwn(sent, 'maxTokens'), false);
  assert.equal(out.items.length, 1);
  assert.equal(llm.infoCalls, undefined, '不传档位时连能力查询都不该发');
});

test("配置给了 'off' 但模型没有推理元数据：退化成不传，请求不再被拒", async () => {
  const logs = [];
  const llm = fakeLlm({ modelInfo: { provider: 'magpie', id: 'group/ds' } });
  const out = await extractEntities({
    llm,
    provider: 'magpie',
    model: 'group/ds',
    items: ITEMS,
    reasoningEffort: 'off',
    log: (line) => logs.push(line),
  });
  assert.equal(llm.calls.length, 1, '能力检查应该挡在调用前，不该先撞一次报错');
  assert.equal(Object.hasOwn(llm.calls[0], 'reasoningEffort'), false);
  assert.equal(out.items.length, 1);
  assert.ok(logs.some((l) => l.includes('不支持 reasoningEffort="off"') && l.includes('无推理档位')));
});

test('配置给了模型支持的档位：原样带上', async () => {
  const llm = fakeLlm({ modelInfo: { reasoning: { efforts: [{ id: 'low' }, { id: 'high' }], defaultEffort: 'low' } } });
  await extractEntities({ llm, provider: 'magpie', model: 'group/ds', items: ITEMS, reasoningEffort: 'high' });
  assert.equal(llm.calls[0].reasoningEffort, 'high');
});

test('配置给了不在支持列表里的档位：不传，不炸', async () => {
  const logs = [];
  const llm = fakeLlm({ modelInfo: { reasoning: { efforts: [{ id: 'low' }, { id: 'high' }] } } });
  const out = await extractEntities({
    llm,
    provider: 'magpie',
    model: 'group/ds',
    items: ITEMS,
    reasoningEffort: 'max',
    log: (line) => logs.push(line),
  });
  assert.equal(Object.hasOwn(llm.calls[0], 'reasoningEffort'), false);
  assert.equal(out.items.length, 1);
  assert.ok(logs.some((l) => l.includes('支持 low/high')));
});

test('能力查询抛错：照传（查不到不等于不支持），但被拒后去掉档位重试一次', async () => {
  const llm = fakeLlm({ modelInfo: null, supported: false });
  llm.resolveModelInfo = async () => {
    throw new Error('adapter 没实现 resolveModel');
  };
  const logs = [];
  // 第一次带档位被拒；第二次不传就放行 —— 用 supported=false 模拟「一律拒」，所以这里只验重试行为
  await assert.rejects(
    () => extractEntities({ llm, provider: 'magpie', model: 'group/ds', items: ITEMS, reasoningEffort: 'low', log: (l) => logs.push(l) }),
    /UNSUPPORTED_REASONING_EFFORT|模型调用失败/,
  );
  assert.equal(llm.calls.length, 2, '被拒一次后应该重试一次');
  assert.equal(llm.calls[0].reasoningEffort, 'low');
  assert.equal(Object.hasOwn(llm.calls[1], 'reasoningEffort'), false);
  assert.ok(logs.some((l) => l.includes('查询模型能力失败')));
  assert.ok(logs.some((l) => l.includes('重试一次')));
});

test('兜底重试成功：没有能力查询方法、第一次被拒，第二次不传就过', async () => {
  let n = 0;
  const calls = [];
  const llm = {
    stream(options) {
      calls.push(options);
      n += 1;
      const bad = n === 1;
      return (async function* s() {
        const chunks = bad ? unsupportedChunks('provider "magpie" model "group/ds" does not support reasoning effort "low"') : textChunks(GOOD_JSON);
        for (const chunk of chunks) yield chunk;
      })();
    },
  };
  const logs = [];
  const out = await extractEntities({
    llm,
    provider: 'magpie',
    model: 'group/ds',
    items: ITEMS,
    reasoningEffort: 'low',
    log: (line) => logs.push(line),
  });
  assert.equal(calls.length, 2);
  assert.equal(calls[0].reasoningEffort, 'low');
  assert.equal(Object.hasOwn(calls[1], 'reasoningEffort'), false);
  assert.equal(calls[1].temperature, 0);
  assert.equal(out.items.length, 1);
  assert.ok(logs.some((l) => l.includes('重试一次')));
});

test("reasoningEffort='default' 视同不传", async () => {
  const llm = fakeLlm({ modelInfo: { reasoning: { efforts: [{ id: 'low' }] } } });
  await extractEntities({ llm, provider: 'magpie', model: 'group/ds', items: ITEMS, reasoningEffort: 'default' });
  assert.equal(Object.hasOwn(llm.calls[0], 'reasoningEffort'), false);
  assert.equal(llm.infoCalls, undefined);
});
