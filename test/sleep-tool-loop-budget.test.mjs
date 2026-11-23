import assert from 'node:assert/strict';
import test from 'node:test';
import { createModelCalls } from '../lib/sleep/model-calls.js';

/**
 * askModel 工具循环的累计预算集成用例（2026-11-23）。
 *
 * 用假 llm 按脚本吐 4 轮：3 轮 memory_recall + 1 轮收尾 JSON，截获每次 stream 收到的
 * messages，验证：预算内原文、超预算旧结果变片段、候选 listing 不动、最终 JSON 照常返回、
 * 定向查证提示只注入规定的两次、两个开关都关时消息体与改前一致。
 */

/** 假 llm：每次 stream 先把当时的 messages 深拷一份存档，再按脚本吐 chunk。 */
function fakeLlm(script, seen) {
  return {
    stream(options = {}) {
      seen.push(JSON.parse(JSON.stringify(options.messages || [])));
      const round = seen.length - 1;
      const chunks = Array.isArray(script[round]) ? script[round] : script[script.length - 1];
      return (async function* gen() {
        for (const chunk of chunks) yield chunk;
      })();
    },
  };
}

/** 一条长工具结果：memory_recall 的返回会被 runReadTool 截到 2500 字。
 *  正文故意写成中文为主：estimateTokens 对中文按 1 字 1 token，两轮就能超过 4000。 */
function recallItems(prefix) {
  const items = [];
  for (let i = 0; i < 80; i += 1) {
    items.push({
      type: 'world',
      confidence: 0.8,
      date: '2026-11-23',
      text: `${prefix}第${i}条：树莓派温度记录，依赖装不上的那次排查，路径与会话原文都在这条长期记忆里，内容较长以便占用上下文预算。`,
    });
  }
  return items;
}

function toolCall(id, query) {
  return { type: 'tool-call', id, name: 'memory_recall', arguments: { query, k: 8 } };
}

function script() {
  return [
    [toolCall('c1', '树莓派')],
    [toolCall('c2', '温度')],
    [toolCall('c3', '路径')],
    [{ type: 'text-delta', text: '{"add":[]}' }],
  ];
}

function harness(cfgExtra = {}) {
  const seen = [];
  const logs = [];
  const llm = fakeLlm(script(), seen);
  const model = createModelCalls({
    ctx: { get: (name) => (name === 'llm' ? llm : undefined) },
    store: { listAll: () => [], recall: async () => recallItems('recall') },
    cfg: { provider: 'test', model: 'test', ...cfgExtra },
    log: (line) => logs.push(String(line)),
    lint: {},
  });
  return { model, seen, logs };
}

const existing = [{
  id: 'm-1',
  type: 'world',
  text: '树莓派温度 45 度',
  entities: ['树莓派'],
  tags: ['hardware'],
  ts: '2026-11-23T00:00:00Z',
}];

/** 取某一轮请求里的某条工具结果文本。 */
function toolText(messages, callId) {
  const hit = messages.find((m) => m.role === 'tool' && String(m.toolCallId) === callId);
  return hit ? hit.content[0].text : null;
}

test('累计预算：超阈值后旧工具结果只留片段，最近一轮与候选 listing 不动', async () => {
  // 阈值压到 1000，让两轮 2500 字的工具结果必然超；默认 4000 的边界由单元用例锁。
  const { model, seen, logs } = harness({ sleepToolBudget: { tokens: 1000 } });
  const out = await model.askModel('对话记录', existing, 'low');
  assert.equal(out, '{"add":[]}', '工具轮跑完后仍要能拿到最终 JSON');
  assert.equal(seen.length, 4, '3 轮工具 + 1 轮收尾');

  // 第 2 轮请求里第 1 轮结果仍是原文：最近一轮保留原文
  assert.equal(toolText(seen[1], 'c1').includes('〔压缩〕'), false, '最近一轮不许压缩');
  // 第 4 轮请求：c1/c2 已被压成片段，c3（最新一轮）仍是原文
  assert.ok(toolText(seen[3], 'c1').startsWith('〔压缩〕'), '最旧的 c1 必须压');
  assert.ok(toolText(seen[3], 'c2').startsWith('〔压缩〕'), '第二轮 c2 必须压');
  assert.equal(toolText(seen[3], 'c3').includes('〔压缩〕'), false, '最新一轮原文保留');
  assert.match(toolText(seen[3], 'c1'), /tool=memory_recall/);
  assert.match(toolText(seen[3], 'c1'), /原文 \d+ 字/);

  // 幂等：第 4 轮里的 c1 与第 3 轮里的 c1 完全一致（前缀缓存不被每轮重写）
  assert.equal(toolText(seen[3], 'c1'), toolText(seen[2], 'c1'));

  // 候选 listing 从第一轮到最后一轮逐字节不变
  assert.equal(seen[0][0].content[0].text, seen[3][0].content[0].text);
  assert.match(seen[3][0].content[0].text, /id=m-1 \| type=world/);

  // 预算日志可 grep，且带前后 token
  assert.ok(
    logs.some((line) => line.startsWith('[sleep] 工具上下文预算 ')
      && /新增压缩=\d+/.test(line) && /token=\d+→\d+/.test(line) && /上限=1000/.test(line)),
    `要有 [sleep] 工具上下文预算 行，实际日志：${logs.filter((l) => l.includes('预算')).join(' | ')}`,
  );
  assert.ok(logs.some((line) => line.startsWith('[sleep] 工具上下文预算汇总 ') && /压缩=\d+/.test(line)));
});

test('定向查证：第一轮后注入 start、第 3 轮后注入 wrap，各只一次', async () => {
  const { model, seen } = harness();
  await model.askModel('对话记录', existing, 'low');
  const allUser = seen.map((messages) => messages
    .filter((m) => m.role === 'user')
    .map((m) => m.content[0].text)
    .filter((t) => t.includes('定向查证')));
  assert.deepEqual(allUser.map((x) => x.length), [0, 1, 1, 2], 'start 一次、wrap 一次');
  assert.match(allUser[1][0], /定向查证：先在 3 轮里只查最可疑的 1-3 条/);
  assert.match(allUser[3][1], /定向查证轮次已到（3 轮）/);
});

test('两个开关都关：消息体与改前一致（无压缩、无定向查证提示）', async () => {
  const { model, seen, logs } = harness({
    sleepToolBudget: { enabled: false },
    sleepToolCheck: { enabled: false },
  });
  await model.askModel('对话记录', existing, 'low');
  for (const messages of seen) {
    for (const m of messages) {
      const text = JSON.stringify(m);
      assert.equal(text.includes('〔压缩〕'), false, '关掉预算后不许出现压缩标记');
      assert.equal(text.includes('定向查证'), false, '关掉定向查证后不许注入提示');
    }
  }
  assert.equal(logs.some((line) => line.includes('工具上下文预算')), false, '关着时不打预算日志');
});

test('预算开着、定向查证关掉：只压缩、不注入提示（两个开关互不牵连）', async () => {
  const { model, seen } = harness({ sleepToolCheck: { enabled: false }, sleepToolBudget: { tokens: 1000 } });
  await model.askModel('对话记录', existing, 'low');
  assert.ok(toolText(seen[3], 'c1').startsWith('〔压缩〕'));
  for (const messages of seen) {
    assert.equal(messages.some((m) => JSON.stringify(m).includes('定向查证')), false);
  }
});

test('tokens=0 回退口子：压缩整个关掉，工具轮照常跑完', async () => {
  const { model, seen } = harness({ sleepToolBudget: { enabled: true, tokens: 0 }, sleepToolCheck: { enabled: false } });
  const out = await model.askModel('对话记录', existing, 'low');
  assert.equal(out, '{"add":[]}');
  assert.equal(seen.length, 4);
  assert.equal(toolText(seen[3], 'c1').includes('〔压缩〕'), false);
});
