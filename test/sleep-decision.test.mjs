/**
 * 冲突预判（decision 桥）的五条路径 + 与 runConflictCheck 的接线。
 *
 * 钉六件事：
 *   1) 批成功：一批一次 POST，questions 是对象 map（每个值是 type/instructions/criteria 对象），
 *      answers 乱序也按 qid 取，置信度只认 probabilities[choice]；
 *   2) 部分 qid 缺失：整批降级（decided 空），不猜缺的那条；
 *   3) 429：整批降级，reason=http_429；
 *   4) 超时：整批降级，reason=timeout；
 *   5) 开关关闭：runConflictCheck 一次桥请求都不发，日志与加这层之前一致；
 *   6) 开关打开：桥判 new 的条不再调生成模型，kept 顺序按 adds 原顺序还原。
 *
 * 假桥用真的本地 http 桩（127.0.0.1:0），不 mock fetch：URL、body、超时都走真协议。
 */
import assert from 'node:assert/strict';
import http from 'node:http';
import test from 'node:test';
import { conflictDecideCfgOf, decideNewBatch, DECIDE_BATCH_CAP } from '../lib/sleep/decision.js';
import { createModelCalls } from '../lib/sleep/model-calls.js';

/** 起一个假桥：handler 拿到 { body, url, res }，自己决定状态码和响应体。 */
async function startBridge(handler) {
  const server = http.createServer(async (req, res) => {
    const chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    let body = null;
    try { body = JSON.parse(Buffer.concat(chunks).toString('utf8') || 'null'); } catch { body = undefined; }
    await handler({ body, url: req.url, res });
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address();
  return {
    baseUrl: `http://127.0.0.1:${port}`,
    close: () => new Promise((resolve) => {
      server.closeAllConnections?.();
      server.close(resolve);
    }),
  };
}

function send(res, status, obj) {
  res.writeHead(status, { 'content-type': 'application/json' });
  res.end(typeof obj === 'string' ? obj : JSON.stringify(obj));
}

function candidates(n) {
  return Array.from({ length: n }, (_, i) => ({
    text: `第${i}条新记忆：树莓派温度现在 45 度`,
    related: [{ id: `old-${i}`, text: `树莓派温度是 40 度`, validFrom: '2026-11-01' }],
  }));
}

const cfgWith = (patch = {}) => conflictDecideCfgOf({ enabled: true, ...patch });

/** 桥的 answer 形状：choice 是判出来的键，置信度在 probabilities[choice]。 */
function answerOf(choice, newProb) {
  return { choice, probabilities: { new: newProb, not_new: Number((1 - newProb).toFixed(2)) } };
}

test('批成功：questions 是对象 map，answers 乱序也按 qid 取', async () => {
  const asks = [];
  const bridge = await startBridge(({ body, url, res }) => {
    asks.push({ url, body });
    // 故意倒序回，验证按 qid 取而不是按顺序取
    const answers = {};
    for (let i = 7; i >= 0; i -= 1) {
      answers[`c${i}`] = i % 2 === 0 ? answerOf('new', 0.96) : { choice: 'not_new', probabilities: { new: 0.4, not_new: 0.6 } };
    }
    send(res, 200, { answers, usage: { totalTokens: 100 } });
  });
  try {
    const result = await decideNewBatch(candidates(8), cfgWith({ baseUrl: bridge.baseUrl }));
    assert.equal(result.reason, 'ok');
    assert.deepEqual([...result.decided].sort((a, b) => a - b), [0, 2, 4, 6]);
    assert.equal(result.degraded, 4);
    assert.equal(asks.length, 1, '一批只发一次请求');
    assert.equal(asks[0].url, '/systemone');
    assert.equal(asks[0].body.model, 'gemini-decision-1');
    assert.equal(typeof asks[0].body.state, 'string');
    assert.ok(!Array.isArray(asks[0].body.questions), 'questions 必须是对象 map，数组会 400');
    assert.deepEqual(Object.keys(asks[0].body.questions), ['c0', 'c1', 'c2', 'c3', 'c4', 'c5', 'c6', 'c7']);
    for (const qid of Object.keys(asks[0].body.questions)) {
      const q = asks[0].body.questions[qid];
      assert.equal(typeof q, 'object', `${qid} 必须是对象，字符串会 400`);
      assert.equal(q.type, 'choice', `${qid} 必须带 type=choice`);
      assert.equal(typeof q.instructions, 'string');
      assert.deepEqual(Object.keys(q.criteria).sort(), ['new', 'not_new'], `${qid} 的 criteria 候选键`);
      assert.ok(q.criteria.new.length > 0 && q.criteria.not_new.length > 0, `${qid} 两个选项都要有说明`);
    }
    const q0 = asks[0].body.questions.c0;
    assert.ok(q0.instructions.includes('第0条新记忆'), '题目里要有新记忆正文');
    assert.ok(q0.instructions.includes('树莓派温度是 40 度'), '题目里要有旧记忆摘要');
    assert.ok(!asks[0].body.state.includes('第0条新记忆'), 'state 不许夹带正文（防跨题污染）');
  } finally {
    await bridge.close();
  }
});

test('置信度只认 probabilities[choice]：顶层 confidence 不算数', async () => {
  const bridge = await startBridge(({ res }) => {
    // 顶层 confidence 故意给高值、probabilities 给低值：命中与否必须看后者
    send(res, 200, {
      answers: {
        c0: { choice: 'new', confidence: 0.99, probabilities: { new: 0.31, not_new: 0.69 } },
        c1: { choice: 'new', confidence: 0.1, probabilities: { new: 0.98, not_new: 0.02 } },
      },
    });
  });
  try {
    const result = await decideNewBatch(candidates(2), cfgWith({ baseUrl: bridge.baseUrl }));
    assert.equal(result.reason, 'ok');
    assert.deepEqual([...result.decided], [1], '只有 probabilities[new] 过阈值的 c1 命中');
    assert.equal(result.degraded, 1);
  } finally {
    await bridge.close();
  }
});

test('部分 qid 缺失：整批降级，不拿缺的条当命中', async () => {
  const bridge = await startBridge(({ res }) => {
    send(res, 200, {
      answers: {
        c0: answerOf('new', 0.99),
        c1: answerOf('new', 0.99),
        c2: answerOf('new', 0.99),
        c3: answerOf('new', 0.99),
      },
    });
  });
  try {
    const result = await decideNewBatch(candidates(6), cfgWith({ baseUrl: bridge.baseUrl }));
    assert.equal(result.reason, 'missing_answer');
    assert.equal(result.decided.size, 0);
    assert.equal(result.degraded, 6);
  } finally {
    await bridge.close();
  }
});

test('429：整批降级，reason=http_429', async () => {
  const bridge = await startBridge(({ res }) => {
    send(res, 429, { error: 'rate limited' });
  });
  try {
    const result = await decideNewBatch(candidates(8), cfgWith({ baseUrl: bridge.baseUrl }));
    assert.equal(result.reason, 'http_429');
    assert.equal(result.decided.size, 0);
    assert.equal(result.degraded, 8);
  } finally {
    await bridge.close();
  }
});

test('超时：整批降级，reason=timeout', async () => {
  const bridge = await startBridge(async ({ res }) => {
    await new Promise((resolve) => setTimeout(resolve, 500));
    send(res, 200, { answers: {} });
  });
  try {
    const result = await decideNewBatch(candidates(3), cfgWith({ baseUrl: bridge.baseUrl, timeoutMs: 200 }));
    assert.equal(result.reason, 'timeout');
    assert.equal(result.decided.size, 0);
    assert.equal(result.degraded, 3);
    assert.ok(result.ms < 450, `超时应在 200ms 左右收口，实得 ${result.ms}ms`);
  } finally {
    await bridge.close();
  }
});

test('概率非法、choice 认不出、旧字段 confidence、非 JSON：整批降级', async () => {
  const badConfidence = await startBridge(({ res }) => {
    send(res, 200, {
      answers: {
        c0: { choice: 'new', probabilities: { new: 1.7, not_new: 0.1 } },
        c1: answerOf('new', 0.99),
      },
    });
  });
  try {
    const result = await decideNewBatch(candidates(2), cfgWith({ baseUrl: badConfidence.baseUrl }));
    assert.equal(result.reason, 'bad_confidence');
    assert.equal(result.decided.size, 0);
  } finally {
    await badConfidence.close();
  }

  const legacyField = await startBridge(({ res }) => {
    // 只给顶层 confidence、没有 probabilities：不许拿旧字段凑数
    send(res, 200, {
      answers: {
        c0: { choice: 'new', confidence: 0.99 },
        c1: { choice: 'new', confidence: 0.99 },
      },
    });
  });
  try {
    const result = await decideNewBatch(candidates(2), cfgWith({ baseUrl: legacyField.baseUrl }));
    assert.equal(result.reason, 'bad_confidence');
    assert.equal(result.decided.size, 0);
  } finally {
    await legacyField.close();
  }

  const badChoice = await startBridge(({ res }) => {
    send(res, 200, { answers: { c0: answerOf('maybe', 0.99), c1: answerOf('new', 0.99) } });
  });
  try {
    const result = await decideNewBatch(candidates(2), cfgWith({ baseUrl: badChoice.baseUrl }));
    assert.equal(result.reason, 'bad_choice');
    assert.equal(result.decided.size, 0);
  } finally {
    await badChoice.close();
  }

  const notJson = await startBridge(({ res }) => send(res, 200, 'not json'));
  try {
    const result = await decideNewBatch(candidates(2), cfgWith({ baseUrl: notJson.baseUrl }));
    assert.equal(result.reason, 'bad_body');
    assert.equal(result.decided.size, 0);
  } finally {
    await notJson.close();
  }
});

test('串行闸：两批并发调用，桥侧同一时刻只有一个在途请求', async () => {
  let inFlight = 0;
  let maxInFlight = 0;
  const bridge = await startBridge(async ({ body, res }) => {
    inFlight += 1;
    maxInFlight = Math.max(maxInFlight, inFlight);
    await new Promise((resolve) => setTimeout(resolve, 60));
    inFlight -= 1;
    const answers = {};
    for (const qid of Object.keys(body.questions)) answers[qid] = answerOf('new', 0.99);
    send(res, 200, { answers });
  });
  try {
    const cfg = cfgWith({ baseUrl: bridge.baseUrl });
    const [a, b] = await Promise.all([
      decideNewBatch(candidates(2), cfg),
      decideNewBatch(candidates(2), cfg),
    ]);
    assert.equal(a.reason, 'ok');
    assert.equal(b.reason, 'ok');
    assert.equal(maxInFlight, 1, `桥侧应只见到 1 个在途请求，实得 ${maxInFlight}`);
  } finally {
    await bridge.close();
  }
});

test('配置归一：开关只认严格 true，batchSize 夹到硬上限，baseUrl 去尾斜杠', () => {
  assert.equal(conflictDecideCfgOf(undefined).enabled, false);
  assert.equal(conflictDecideCfgOf({ enabled: 'true' }).enabled, false, '字符串 true 不许误开');
  assert.equal(conflictDecideCfgOf({ enabled: true }).batchSize, 8);
  assert.equal(conflictDecideCfgOf({ enabled: true, batchSize: 50 }).batchSize, DECIDE_BATCH_CAP);
  assert.equal(conflictDecideCfgOf({ enabled: true, batchSize: 0 }).batchSize, 1);
  assert.equal(conflictDecideCfgOf({ enabled: true, minConfidence: 2 }).minConfidence, 1);
  assert.equal(conflictDecideCfgOf({ enabled: true, baseUrl: 'http://127.0.0.1:3427/' }).baseUrl, 'http://127.0.0.1:3427');
  assert.equal(conflictDecideCfgOf({ enabled: true }).timeoutMs, 2500);
  assert.equal(conflictDecideCfgOf({ enabled: true }).model, 'gemini-decision-1');
});

/** 集成用的旧记忆：实体重叠、正文 2-gram 有交集（relatedEntries 才判得出来）。 */
function oldRecords() {
  return [{
    id: 'old-1',
    text: '树莓派温度是四十度，风扇转速低',
    entities: ['树莓派'],
    status: 'active',
    ts: '2026-11-01T00:00:00Z',
  }];
}

function newAdd(tag) {
  return { tag, text: '树莓派当前温度四十五度，风扇正常', entities: ['树莓派'] };
}

/** 假生成模型：每次 stream 记一笔，回一个合法裁决 JSON。 */
function fakeLlm(counter) {
  return {
    stream() {
      counter.calls += 1;
      return (async function* gen() {
        yield { type: 'text-delta', text: '{"verdict":"new","supersedes":[],"text":""}' };
      })();
    },
  };
}

/** 按顺序回不同裁决的假模型（null = 回一段非 JSON，模拟解析失败）。 */
function fakeLlmSeq(counter, verdicts) {
  return {
    stream() {
      const verdict = verdicts[Math.min(counter.calls, verdicts.length - 1)];
      counter.calls += 1;
      return (async function* gen() {
        yield {
          type: 'text-delta',
          text: verdict === null ? '不是 JSON' : JSON.stringify(verdict),
        };
      })();
    },
  };
}

function harness(conflictDecide, llm = null) {
  const counter = { calls: 0 };
  const logs = [];
  const fake = llm || fakeLlm(counter);
  const cfg = { provider: 'test', model: 'test', conflictDecide };
  const model = createModelCalls({
    ctx: { get: (name) => (name === 'llm' ? fake : undefined) },
    store: { listAll: () => oldRecords() },
    cfg,
    log: (line) => logs.push(String(line)),
    lint: {},
  });
  return { model, counter, logs };
}

test('开关关闭：一次桥请求都不发，askConflict 照常逐条问，日志无 [decide]', async () => {
  let requests = 0;
  const bridge = await startBridge(({ res }) => {
    requests += 1;
    send(res, 200, { answers: {} });
  });
  try {
    const { model, counter, logs } = harness({ enabled: false, baseUrl: bridge.baseUrl });
    const adds = [newAdd('a'), newAdd('b')];
    const result = await model.runConflictCheck(adds);
    assert.equal(requests, 0, '开关没开不许打桥');
    assert.equal(counter.calls, 2, '两条都要走原 askConflict');
    assert.equal(result.adds.length, 2);
    assert.ok(!logs.some((l) => l.includes('[decide]')), '关着时不应出现 [decide] 日志');
  } finally {
    await bridge.close();
  }
});

test('开关打开：桥判 new 的条不再调生成模型，kept 顺序按 adds 原顺序', async () => {
  const bridge = await startBridge(({ body, res }) => {
    const answers = {};
    for (const qid of Object.keys(body.questions)) {
      // 第一题（adds[0]）判 new，第二题（adds[2]）退回
      answers[qid] = qid === 'c0'
        ? answerOf('new', 0.99)
        : { choice: 'not_new', probabilities: { new: 0.02, not_new: 0.98 } };
    }
    send(res, 200, { answers });
  });
  try {
    const { model, counter, logs } = harness({ enabled: true, baseUrl: bridge.baseUrl });
    // adds[1] 没实体 → 本地跳过、不参与预判，用来验证顺序还原
    const adds = [newAdd('a'), { tag: 'skip', text: '没有实体的条', entities: [] }, newAdd('b')];
    const result = await model.runConflictCheck(adds);
    assert.equal(counter.calls, 1, '只有退回的那条调 askConflict');
    assert.deepEqual(result.adds.map((x) => x.tag), ['a', 'skip', 'b'], 'kept 顺序必须与 adds 一致');
    assert.equal(result.checked, 2);
    assert.ok(logs.some((l) => l.startsWith('[decide] 批') && l.includes('命中=1')), '要有批次行');
    assert.ok(logs.some((l) => l.startsWith('[decide] 汇总') && l.includes('省askConflict=1')), '要有汇总行');
    assert.ok(logs.some((l) => l.includes('预判省调用1')), '收尾统计要带省下的调用数');
  } finally {
    await bridge.close();
  }
});

test('开关打开但桥挂了：整批退回 askConflict，结果与关闭时一致', async () => {
  const bridge = await startBridge(({ res }) => send(res, 429, { error: 'rate limited' }));
  try {
    const { model, counter, logs } = harness({ enabled: true, baseUrl: bridge.baseUrl });
    const adds = [newAdd('a'), newAdd('b')];
    const result = await model.runConflictCheck(adds);
    assert.equal(counter.calls, 2, '降级后两条都要走原 askConflict');
    assert.deepEqual(result.adds.map((x) => x.tag), ['a', 'b']);
    assert.equal(result.failed, 0);
    assert.ok(logs.some((l) => l.includes('原因=http_429')), '降级原因要落日志');
  } finally {
    await bridge.close();
  }
});

test('开关关闭：冗余丢弃、状态变更改写、解析失败原样保留，kept 顺序不变', async () => {
  const counter = { calls: 0 };
  const llm = fakeLlmSeq(counter, [
    { verdict: 'redundant', supersedes: ['old-1'], text: '' },
    { verdict: 'state_change', supersedes: ['old-1'], text: '原来是四十度，2026-11-20 测到四十五度，风扇正常' },
    null,
  ]);
  const { model, logs } = harness(undefined, llm);
  const adds = [newAdd('a'), newAdd('b'), newAdd('c')];
  const result = await model.runConflictCheck(adds);
  assert.equal(result.checked, 3);
  assert.equal(result.redundant.length, 1);
  assert.equal(result.stateChange.length, 1);
  assert.equal(result.failed, 1, '解析失败 fail-open 原样入库');
  assert.deepEqual(result.adds.map((x) => x.tag), ['b', 'c'], 'a 冗余丢弃，b 改写，c 解析失败原样，顺序不变');
  assert.ok(result.adds[0].text.startsWith('原来是四十度'));
  assert.deepEqual(result.adds[0].supersedes, ['old-1']);
  assert.equal(result.adds[1].text, adds[2].text);
  assert.ok(!logs.some((l) => l.includes('[decide]')), '关着时没有 [decide] 日志');
});
