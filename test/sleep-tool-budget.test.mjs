import assert from 'node:assert/strict';
import test from 'node:test';
import {
  TOOL_BUDGET_MARK,
  TOOL_BUDGET_DEFAULT_TOKENS,
  TOOL_CHECK_DEFAULT_ROUNDS,
  buildToolCheckHint,
  compactToolHistory,
  focusTermsOf,
  pickRelevantLines,
  toolBudgetCfgOf,
  toolCheckCfgOf,
} from '../lib/sleep.js';

/**
 * 工具循环累计上下文预算（2026-11-23）的单元用例。
 *
 * 线上事实：12:30 那轮 27 个文件 202 次请求（7.5 次/文件），每轮把旧工具结果原文重发。
 * 这里锁住四件事：预算内不截断、超预算只留相关片段、已压缩的条目不再变（幂等）、
 * 空结果与非工具消息（候选 listing）一个字节都不动。
 */

function userMsg(text) {
  return { role: 'user', content: [{ type: 'text', text }], source: { kind: 'user' } };
}

function assistantCall(id, name, args) {
  return {
    role: 'assistant',
    content: [{ type: 'tool-call', id, name, arguments: args }],
    source: { kind: 'model', provider: 'test', model: 'test' },
  };
}

function toolMsg(id, text) {
  return { role: 'tool', toolCallId: id, isError: false, content: [{ type: 'text', text }], source: { kind: 'tool', callId: id } };
}

/** 一段像 chat_recall 结果的正文：噪音行里夹实体、路径、否定结论。 */
function recallText(tag, lines = 60) {
  const out = [`· ${tag} 第 1 条：和树莓派无关的日常记录`];
  for (let i = 2; i < lines; i += 1) out.push(`· ${tag} 第 ${i} 条：普通的会话碎片，没有关键信息`);
  out.push(`· ${tag} 关键：树莓派装不上 dsh-qqbot 的依赖，路径 /home/julia/fatfish/plugins/dsh-memory/package.json，2026-11-23 实测 45 度`);
  return out.join('\n');
}

function listing() {
  return '现有记忆：\nid=m-1 | type=world | ent=树莓派 | tags=hardware | 树莓派温度记录\n\n对话记录（上次睡觉之后）：\n…';
}

test('配置归一：缺省 4000 token / 保留 1 轮 / 前 3 轮定向查证，明确 false 才关', () => {
  const budget = toolBudgetCfgOf(undefined);
  assert.equal(budget.enabled, true);
  assert.equal(budget.tokens, TOOL_BUDGET_DEFAULT_TOKENS);
  assert.equal(budget.tokens, 4000);
  assert.equal(budget.keepRecentRounds, 1);
  assert.equal(toolBudgetCfgOf({ enabled: false }).enabled, false);
  assert.equal(toolBudgetCfgOf({ enabled: 'false' }).enabled, true, '字符串 false 不许误关');
  assert.equal(toolBudgetCfgOf({ tokens: 0 }).tokens, 0, '0 = 不压缩的回退口子');
  assert.equal(toolBudgetCfgOf({ tokens: -5 }).tokens, 0);

  const check = toolCheckCfgOf(undefined);
  assert.equal(check.enabled, true);
  assert.equal(check.rounds, TOOL_CHECK_DEFAULT_ROUNDS);
  assert.equal(check.rounds, 3);
  assert.equal(check.maxCallsPerRound, 3);
  assert.equal(toolCheckCfgOf({ enabled: false }).enabled, false);
});

test('预算内不截断：工具结果原文逐字节保留，统计里 compacted=0', () => {
  const messages = [
    userMsg(listing()),
    assistantCall('c1', 'memory_recall', { query: '树莓派' }),
    toolMsg('c1', recallText('前一轮')),
    assistantCall('c2', 'chat_recall', { query: '温度' }),
    toolMsg('c2', recallText('后一轮')),
  ];
  const before = JSON.stringify(messages);
  const stats = compactToolHistory(messages, { budget: toolBudgetCfgOf({ tokens: 100000 }) });
  assert.equal(stats.compacted, 0);
  assert.equal(stats.tools, 2);
  assert.equal(stats.afterTokens, stats.beforeTokens);
  assert.equal(JSON.stringify(messages), before, '预算内必须一个字节都不动');
});

test('超预算：旧结果只留「出处 + 相关片段」，最新一轮原文保留，候选 listing 不动', () => {
  const messages = [
    userMsg(listing()),
    assistantCall('c1', 'memory_recall', { query: '树莓派 依赖' }),
    toolMsg('c1', recallText('旧轮')),
    assistantCall('c2', 'memory_recall', { query: '路径' }),
    toolMsg('c2', recallText('新轮')),
  ];
  const listingText = messages[0].content[0].text;
  const newestText = messages[4].content[0].text;
  const stats = compactToolHistory(messages, {
    budget: toolBudgetCfgOf({ tokens: 200, keepRecentRounds: 1, excerptChars: 240 }),
    focus: focusTermsOf([{ id: 'm-1', entities: ['树莓派'] }]),
  });
  assert.equal(stats.compacted, 1, '只压最旧那一轮');
  assert.ok(stats.afterTokens < stats.beforeTokens);

  const oldText = messages[2].content[0].text;
  assert.ok(oldText.startsWith(TOOL_BUDGET_MARK), '压缩后必须带标记');
  assert.match(oldText, /tool=memory_recall/, '出处要带工具名');
  assert.match(oldText, /query/, '出处要带参数摘要');
  assert.match(oldText, /原文 \d+ 字/, '出处要带原文长度');
  assert.match(oldText, /树莓派装不上/, '相关片段要命中实体/否定结论那一行');
  assert.ok(oldText.length < 600, `片段应明显变短，实际 ${oldText.length} 字`);

  assert.equal(messages[4].content[0].text, newestText, '最新一轮工具结果必须保持原文');
  assert.equal(messages[0].content[0].text, listingText, '候选 listing 不许被改');
  assert.equal(messages[1].content[0].arguments.query, '树莓派 依赖', 'assistant 消息不动');
  assert.equal(messages.length, 5, '压缩不许增删消息，tool_call 配对不能断');
  assert.equal(messages[2].toolCallId, 'c1');
});

test('幂等：第二次压缩不再动已压过的条目，消息体保持稳定（前缀缓存不被反复改写）', () => {
  const messages = [
    userMsg(listing()),
    assistantCall('c1', 'memory_recall', { query: 'q1' }),
    toolMsg('c1', recallText('旧轮')),
    assistantCall('c2', 'memory_recall', { query: 'q2' }),
    toolMsg('c2', recallText('新轮')),
  ];
  const budget = toolBudgetCfgOf({ tokens: 150 });
  const first = compactToolHistory(messages, { budget });
  const afterFirst = JSON.stringify(messages);
  assert.equal(first.compacted, 1);
  const second = compactToolHistory(messages, { budget });
  assert.equal(second.compacted, 0, '已压过的不重算');
  assert.equal(JSON.stringify(messages), afterFirst);
});

test('空结果 / 没有工具消息：不崩，stats 归零', () => {
  const empty = [
    userMsg(listing()),
    assistantCall('c1', 'memory_recall', { query: 'q' }),
    toolMsg('c1', ''),
    toolMsg('c2', '没有相关记忆'),
  ];
  const stats = compactToolHistory(empty, { budget: toolBudgetCfgOf({ tokens: 10 }) });
  assert.equal(stats.tools, 2);
  assert.equal(stats.compacted, 0, '空结果没有可压的内容，不许写坏消息');
  assert.equal(empty[2].content[0].text, '');
  assert.equal(empty[3].content[0].text, '没有相关记忆');

  const none = compactToolHistory([userMsg(listing())], { budget: toolBudgetCfgOf({ tokens: 1 }) });
  assert.deepEqual(none, {
    rounds: 0, tools: 0, compacted: 0, kept: 0, beforeTokens: 0, afterTokens: 0, budget: 1,
  });
  assert.deepEqual(compactToolHistory([], { budget: toolBudgetCfgOf({}) }).tools, 0);
});

test('关闭开关：整段消息逐字节一致（回退口子）', () => {
  const messages = [
    userMsg(listing()),
    assistantCall('c1', 'memory_recall', { query: 'q1' }),
    toolMsg('c1', recallText('旧轮')),
  ];
  const before = JSON.stringify(messages);
  const stats = compactToolHistory(messages, { budget: toolBudgetCfgOf({ enabled: false }) });
  assert.equal(stats.compacted, 0);
  assert.equal(JSON.stringify(messages), before);
  // tokens=0 也是关：配置能整体退回旧行为
  const zero = compactToolHistory(messages, { budget: toolBudgetCfgOf({ tokens: 0 }) });
  assert.equal(zero.compacted, 0);
  assert.equal(JSON.stringify(messages), before);
});

test('pickRelevantLines：实体/路径/否定结论优先，保持原顺序', () => {
  const text = [
    '普通一行',
    '树莓派温度 45 度，风扇正常',
    '无关的闲聊',
    '/home/julia/fatfish/plugins/dsh-memory/src/sleep/model-calls.ts:88 工具循环',
    '装不上 npm 包 node-sqlite',
  ].join('\n');
  const picked = pickRelevantLines(text, ['树莓派'], 200, 3);
  assert.match(picked, /树莓派温度/);
  assert.match(picked, /model-calls\.ts:88|装不上/);
  assert.ok(picked.split('\n').length <= 3);
});

test('定向查证提示：start 写清轮数与每轮条数，wrap 收口，关掉开关时由调用方不注入', () => {
  const cfg = toolCheckCfgOf({ rounds: 2, maxCallsPerRound: 2 });
  const start = buildToolCheckHint('start', cfg);
  assert.match(start, /定向查证/);
  assert.match(start, /2 轮/);
  assert.match(start, /1-2 条/);
  const wrap = buildToolCheckHint('wrap', cfg);
  assert.match(wrap, /定向查证轮次已到/);
  assert.match(wrap, /归因或状态/);
});
