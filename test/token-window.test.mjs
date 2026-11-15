/**
 * 拼 prompt 的容量边界按 token 动态算。
 *
 * 覆盖：
 *   1. token 估算（中文 1 字 ≈ 1 token，ASCII 4 字符 ≈ 1 token，代理对只算一个字）；
 *   2. 窗口来源：cfg.contextWindowTokens 优先，其次 tokenMeter、session.requestContext、
 *      宿主 compaction 的 modelPolicies/全局；
 *   3. 预算 = 窗口 − 输出预留（宿主声明优先，否则按比例）；
 *   4. packByTokens 装满、chunkByTokens 切批、窗口未知（budget=0）不切；
 *   5. 拿不到 session / tokenMeter 抛错时不炸，按下一来源降级。
 */
import assert from 'node:assert/strict';
import test from 'node:test';
import {
  estimateTokens,
  sessionOf,
  windowInfoOf,
  windowTokensOf,
  logWindowOnce,
  outputReserveOf,
  promptBudgetOf,
  itemTokensOf,
  packByTokens,
  chunkByTokens,
  DEFAULT_RESERVE_RATIO,
} from '../lib/token-window.js';

test('token 估算：中文按字 1:1、ASCII 4:1、emoji 只算一个字', () => {
  assert.equal(estimateTokens(''), 0);
  assert.equal(estimateTokens('树莓派'), 3);
  assert.equal(estimateTokens('树莓派 45 度'), 4 + 1, '中文 4 字 + ASCII 4 字符（4/4=1）');
  assert.equal(estimateTokens('abcd'), 1);
  assert.equal(estimateTokens('abcde'), 2);
  assert.equal(estimateTokens('🐟'), 1, '代理对只算一个字');
  assert.equal(estimateTokens('a🐟b'), 2, '1 个中文字 + 2 个 ASCII = 1 + 0.5 → 向上取整 2');
});

test('窗口来源：插件配置优先，其次宿主 compaction 的 modelPolicies，再全局，最后 0', () => {
  const compaction = {
    config: {
      contextWindow: 100000,
      maxTokens: 8000,
      modelPolicies: [{ provider: 'magpie', model: 'group/ds', contextWindow: 200000, maxTokens: 16000 }],
    },
  };
  const ctx = { get: (name) => (name === 'compaction' ? compaction : undefined) };
  assert.equal(windowTokensOf(ctx, { provider: 'magpie', model: 'group/ds' }), 200000, '精确匹配 policy');
  assert.equal(windowTokensOf(ctx, { provider: 'other', model: 'x' }), 100000, '匹配不到继承全局');
  assert.equal(
    windowTokensOf(ctx, { provider: 'magpie', model: 'group/ds', contextWindowTokens: 64000 }),
    64000,
    '插件配置里的窗口最优先',
  );
  assert.equal(windowTokensOf({ get: () => undefined }, {}), 0, '谁都没给 = 0，不猜');
  assert.equal(windowTokensOf(undefined, {}), 0, 'ctx 都没有也不炸');
});

test('窗口来源：tokenMeter 实测窗口优先于 requestContext 和 compaction', () => {
  const session = {
    id: 'session-abc',
    requestContext: () => ({ contextWindow: 32768 }),
  };
  const ctx = {
    get(name) {
      if (name === 'tokenMeter') {
        return { contextPressure: (s) => (s === session ? { contextWindow: 1048576 } : {}) };
      }
      if (name === 'sessions') return { get: (id) => (id === 'session-abc' ? session : undefined) };
      if (name === 'compaction') return { config: { contextWindow: 100000 } };
      return undefined;
    },
  };
  assert.deepEqual(windowInfoOf(ctx, {}, session), { tokens: 1048576, source: 'tokenMeter' }, 'tokenMeter 命中');
  assert.equal(windowTokensOf(ctx, {}, 'session-abc'), 1048576, 'sessionId 字符串也能解析成会话');
  assert.equal(
    windowTokensOf(ctx, { contextWindowTokens: 64000 }, session),
    64000,
    '插件配置写死的窗口仍然最优先',
  );
});

test('窗口来源：tokenMeter 没记到时退回 session.requestContext', () => {
  const session = { requestContext: () => ({ contextWindow: 65536 }) };
  const ctx = {
    get(name) {
      if (name === 'tokenMeter') return { contextPressure: () => ({}) };
      if (name === 'compaction') return { config: { contextWindow: 100000 } };
      return undefined;
    },
  };
  assert.deepEqual(windowInfoOf(ctx, {}, session), { tokens: 65536, source: 'session.requestContext' }, 'requestContext 命中');
});

test('窗口来源：三个都没有时返回 0（未知），拿不到 session 不抛', () => {
  const ctx = {
    get(name) {
      if (name === 'tokenMeter') {
        return {
          contextPressure(session) {
            if (!session) throw new Error('tokenMeter 要 session');
            return {};
          },
        };
      }
      return undefined;
    },
  };
  assert.equal(windowTokensOf(ctx, {}, null), 0, '没会话就降级，不抛');
  assert.equal(windowTokensOf(ctx, {}, '不存在的会话'), 0, 'sessionId 查不到也降级');
  assert.equal(sessionOf(ctx, '不存在的会话'), null);
  assert.equal(sessionOf(ctx, null), null);
  assert.doesNotThrow(() => windowTokensOf({ get: () => { throw new Error('sessions 服务炸了'); } }, {}), 'ctx.get 抛错也不炸');
  assert.deepEqual(windowInfoOf({ get: () => undefined }, {}), { tokens: 0, source: '' }, '谁都没给 = 未知');
});

test('窗口日志：warn 打一次、带来源，第二次不再打', () => {
  const lines = [];
  const session = { id: 'session-log' };
  const ctx = {
    get(name) {
      if (name === 'tokenMeter') return { contextPressure: () => ({ contextWindow: 1048576 }) };
      return undefined;
    },
    logger: { warn: (line) => lines.push(line) },
  };
  logWindowOnce(ctx, {}, session, { logUnknown: true });
  logWindowOnce(ctx, {}, session, { logUnknown: true });
  assert.deepEqual(lines, ['dsh-memory 拼 prompt 窗口：1048576 token（来源 tokenMeter）'], '进程内只打一次，带来源');
});

test('promptBudgetOf 认 session：窗口从会话路由取，预算按比例扣预留', () => {
  const session = { requestContext: () => ({ contextWindow: 200000 }) };
  const budget = promptBudgetOf({ get: () => undefined }, {}, { session });
  assert.equal(budget.window, 200000);
  assert.equal(budget.reserve, Math.floor(200000 * DEFAULT_RESERVE_RATIO));
  assert.equal(budget.budget, 200000 - Math.floor(200000 * DEFAULT_RESERVE_RATIO));
});

test('输出预留：宿主声明了 maxTokens 就用它，否则按窗口比例', () => {
  const ctx = {
    get: () => ({ config: { contextWindow: 100000, maxTokens: 8000, modelPolicies: [] } }),
  };
  assert.equal(outputReserveOf(ctx, {}, 100000), 8000);
  assert.equal(outputReserveOf({ get: () => undefined }, {}, 100000), Math.floor(100000 * DEFAULT_RESERVE_RATIO));
  const budget = promptBudgetOf({ get: () => undefined }, { contextWindowTokens: 100000 });
  assert.equal(budget.window, 100000);
  assert.equal(budget.reserve, 25000);
  assert.equal(budget.budget, 75000);
  assert.deepEqual(promptBudgetOf({ get: () => undefined }, {}), { window: 0, reserve: 0, budget: 0 }, '窗口未知时 budget=0');
});

test('packByTokens：装得下就收、装不下跳过，预算 0 全收', () => {
  const items = [{ text: '一'.repeat(10) }, { text: '二'.repeat(100) }, { text: '三'.repeat(10) }];
  const packed = packByTokens(items, { budget: 40, textOf: (x) => x.text });
  assert.deepEqual(packed.picked.map((x) => x.text[0]), ['一', '三'], '长条跳过，短条继续塞');
  assert.deepEqual(packed.omitted.map((x) => x.text[0]), ['二']);
  assert.equal(packed.used, 20);
  const whole = packByTokens(items, { budget: 0, textOf: (x) => x.text });
  assert.equal(whole.picked.length, 3, '窗口未知不设限');
});

test('chunkByTokens：按预算切满，单条超预算自己独占一批，预算 0 不切', () => {
  const items = Array.from({ length: 9 }, (_, i) => ({ text: '字'.repeat(10), i }));
  const chunks = chunkByTokens(items, { budget: 35, textOf: (x) => x.text, overhead: 5 });
  // 每条 = 10 字 + 5 开销 = 15 token；35 一批只装得下 2 条（30），第 3 条 45 超预算 → 下一批
  assert.deepEqual(chunks.map((c) => c.length), [2, 2, 2, 2, 1]);
  assert.equal(chunks.reduce((n, c) => n + c.length, 0), 9, '一条不丢');
  const huge = chunkByTokens([...items, { text: '长'.repeat(100), i: 99 }], { budget: 35, textOf: (x) => x.text, overhead: 5 });
  assert.equal(huge.at(-1).length, 1, '单条超预算自己独占一批，不拆正文');
  const whole = chunkByTokens(items, { budget: 0, textOf: (x) => x.text });
  assert.equal(whole.length, 1);
  assert.equal(whole[0].length, 9, '窗口未知不切');
});

test('itemTokensOf：正文 + 固定开销', () => {
  assert.equal(itemTokensOf({ text: '树莓派温度 45 度。' }, (x) => x.text, 24), estimateTokens('树莓派温度 45 度。') + 24);
});
