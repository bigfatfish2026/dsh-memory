import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import {
  FALLBACK_HEADROOM,
  NOTIFY_RATIO,
  PRESSURE_EVENT_DEF,
  PRESSURE_TOPIC,
  RESET_RATIO,
  compactThreshold,
  formatPressureMessage,
  lastCompactionAt,
  notifyThreshold,
  selectPolicy,
  setupContextWatch,
  shouldNotify,
  shouldSkipSession,
} from '../lib/context-watch.js';

/** 主会话的真实路由：magpie/group/ds，窗口 1048576，ratio 0.1，headroom 65536。 */
const COMPACTION = {
  thresholdRatio: 0.15,
  headroomTokens: 65536,
  retainTokens: 12000,
  maxTokens: 65536,
  modelPolicies: [
    { provider: 'ollama', model: 'qwen38-coder390:Q6_K', thresholdRatio: 0.99, headroomTokens: 1024, maxTokens: 8192 },
    { provider: 'magpie', model: 'group/ds', thresholdRatio: 0.1, headroomTokens: 65536, maxTokens: 65536 },
    { provider: 'magpie-grp', model: 'group/ds', thresholdRatio: 0.01953125, headroomTokens: 65536, maxTokens: 65536 },
  ],
};

/** 主会话压缩线 104857 → 提醒线 floor(104857×0.95) = 99614。 */
const LINE = 104857;
const NOTIFY_AT = 99614;

/** 造一个假宿主：sessions/tokenMeter/compaction/events 都由 params 决定。 */
function makeCtx({ session, pressure, compaction = COMPACTION, events } = {}) {
  const published = [];
  const service = events ?? {
    registry: new Map(),
    defineEvent(def) {
      service.registry.set(def.name, def);
    },
    publish(input) {
      published.push(input);
      return { seq: published.length };
    },
  };
  if (service.registry && typeof service.registry.set === 'function') {
    const seen = new Map();
    service.registry = {
      get: (name) => seen.get(name),
      set: (name, def) => seen.set(name, def),
    };
    const define = service.defineEvent.bind(service);
    service.defineEvent = (def) => {
      define(def);
      service.registry.set(def.name, def);
    };
  }
  const ctx = {
    get(name) {
      if (name === 'sessions') return { get: () => session };
      if (name === 'tokenMeter') return { contextPressure: () => pressure };
      if (name === 'compaction') return { config: compaction };
      if (name === 'events') return service;
      return undefined;
    },
  };
  return { ctx, published };
}

function tempCfg(extra = {}) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ctx-watch-'));
  return {
    contextWatchEnabled: true,
    contextWatchNotify: false,
    triggerTokens: 70000,
    contextWatchStateFile: path.join(dir, 'state.json'),
    contextWatchLogFile: path.join(dir, 'watch.log'),
    notifyScript: '/bin/true',
    ...extra,
  };
}

function liveSession(overrides = {}) {
  return {
    header: {},
    log: [],
    requestContext: () => ({ provider: 'magpie', model: 'group/ds', contextWindow: 1048576 }),
    requestHeader: () => ({ config: {} }),
    ...overrides,
  };
}

test('压缩线按路由 policy 复算：magpie/group/ds 是 104857', () => {
  const policy = selectPolicy(COMPACTION, { provider: 'magpie', model: 'group/ds' });
  assert.equal(policy.source, 'policy');
  assert.equal(
    compactThreshold({
      contextWindow: 1048576,
      thresholdRatio: policy.thresholdRatio,
      headroomTokens: policy.headroomTokens,
      reservedTokens: policy.maxTokens,
    }),
    104857,
  );
});

test('压缩线：容量项卡住时取容量项', () => {
  assert.equal(
    compactThreshold({ contextWindow: 100000, thresholdRatio: 0.99, headroomTokens: 1024, reservedTokens: 8192 }),
    90784,
  );
});

test('压缩线：容量项算崩退回窗口*ratio，不退化成 0', () => {
  assert.equal(
    compactThreshold({ contextWindow: 100000, thresholdRatio: 0.1, headroomTokens: 200000, reservedTokens: 65536 }),
    10000,
  );
});

test('压缩线：缺 ratio/headroom 时用兜底，窗口无效返回 0', () => {
  assert.equal(compactThreshold({ contextWindow: 1000000 }), 150000);
  assert.equal(compactThreshold({ contextWindow: 0 }), 0);
  assert.equal(compactThreshold({}), 0);
  assert.equal(FALLBACK_HEADROOM, 65536);
});

test('提醒线只有一档：压缩线 ×95%（含），没有 80%/98%/100%/130%', () => {
  assert.equal(NOTIFY_RATIO, 0.95);
  assert.equal(RESET_RATIO, 0.45);
  assert.equal(notifyThreshold(LINE), NOTIFY_AT);
  assert.equal(notifyThreshold(0), 0);
  // 0.94 线以下不发，0.95 线（含）就发
  assert.equal(shouldNotify({ pressureTokens: Math.floor(LINE * 0.94), compactTokens: LINE }).emit, false);
  assert.equal(shouldNotify({ pressureTokens: NOTIFY_AT, compactTokens: LINE }).emit, true);
  assert.equal(shouldNotify({ pressureTokens: Math.floor(LINE * 1.02), compactTokens: LINE }).emit, true);
  // 2026-11-15：98% 不是另一档，它的提醒线还是 99614（没有 0.98 这条线）
  const at98 = shouldNotify({ pressureTokens: Math.floor(LINE * 0.98), compactTokens: LINE });
  assert.equal(at98.threshold, NOTIFY_AT);
  assert.equal(shouldNotify({ pressureTokens: Math.floor(LINE * 0.98), compactTokens: LINE }).emit, true);
});

test('每会话只发一次：提醒过就不再发，压低过才重置', () => {
  const sent = { notified: true, atTokens: NOTIFY_AT, level: 1, at: 1 };
  // 过线以后继续涨：不再发（下一轮就压了，再发没意义）
  assert.equal(shouldNotify({ pressureTokens: LINE * 1.02, compactTokens: LINE, entry: sent }).emit, false);
  // 水位没掉：也不发
  assert.equal(shouldNotify({ pressureTokens: NOTIFY_AT, compactTokens: LINE, entry: sent }).emit, false);
  // 压过了（掉到 45% 以下）：清记录
  const reset = shouldNotify({ pressureTokens: Math.floor(LINE * 0.12), compactTokens: LINE, entry: sent });
  assert.deepEqual(reset, { emit: false, reset: true, notified: false, threshold: NOTIFY_AT });
  // 压完又涨回 96%：重发一次
  assert.equal(shouldNotify({ pressureTokens: Math.floor(LINE * 0.96), compactTokens: LINE }).emit, true);
});

test('旧 state（只有 level）当已提醒，重启不重弹；level 0 不算', () => {
  assert.equal(shouldNotify({ pressureTokens: LINE * 1.02, compactTokens: LINE, entry: { level: 4 } }).emit, false);
  assert.equal(shouldNotify({ pressureTokens: LINE * 1.02, compactTokens: LINE, entry: { level: 0 } }).emit, true);
  // 比上次提醒时水位低一半以上也算压过了，能重新提醒
  const dipped = shouldNotify({
    pressureTokens: Math.floor(NOTIFY_AT * 0.4),
    compactTokens: LINE,
    entry: { notified: true, atTokens: NOTIFY_AT },
  });
  assert.equal(dipped.reset, true);
});

test('提醒之后有压缩记录、且水位比上次提醒低，才允许再发一条', () => {
  // 上次是在 104000（约 99%）提醒的；压过一次之后掉到 96%
  const sent = { notified: true, atTokens: 104000, level: 1, at: 1790000000000 };
  // 压缩记录比提醒早：不算压过，继续压着不发
  assert.equal(
    shouldNotify({ pressureTokens: NOTIFY_AT, compactTokens: LINE, entry: sent, compactedAt: sent.at - 1 }).emit,
    false,
  );
  // 压过且水位回落（但仍高于 95% 线）：允许再发一条
  assert.equal(
    shouldNotify({ pressureTokens: Math.floor(LINE * 0.96), compactTokens: LINE, entry: sent, compactedAt: sent.at + 1 }).emit,
    true,
  );
  // 2026-11-15：压缩记录说压过、水位还在涨 → 不发。
  // 当天 03:11 发 199891、03:14 发 205232 两条就是这么误判出来的。
  assert.equal(
    shouldNotify({ pressureTokens: 105000, compactTokens: LINE, entry: sent, compactedAt: sent.at + 1 }).emit,
    false,
    '水位没降下来，不许只凭压缩记录重发',
  );
  // 压过但还没到 95%：只清记录，不发
  const early = shouldNotify({
    pressureTokens: NOTIFY_AT - 1,
    compactTokens: LINE,
    entry: sent,
    compactedAt: sent.at + 1,
  });
  assert.deepEqual(early, { emit: false, reset: true, notified: false, threshold: NOTIFY_AT });
});

test('lastCompactionAt：只认 since 之后的 compaction/end，往前扫到更早就停', () => {
  const session = {
    log: [
      { type: 'compaction/end', time: 1000 },
      { type: 'message', time: 1500 },
      { type: 'compaction/end', time: 5000 },
      { type: 'message', time: 6000 },
    ],
  };
  assert.equal(lastCompactionAt(session, 2000), 5000);
  assert.equal(lastCompactionAt(session, 5500), 0);
  assert.equal(lastCompactionAt(session, 0), 5000);
  assert.equal(lastCompactionAt({}, 0), 0);
});

test('policy 精确匹配 provider+model，匹配不到继承全局', () => {
  const grp = selectPolicy(COMPACTION, { provider: 'magpie-grp', model: 'group/ds' });
  assert.equal(grp.source, 'policy');
  assert.equal(grp.thresholdRatio, 0.01953125);

  const none = selectPolicy(COMPACTION, { provider: 'nope', model: 'nope' });
  assert.equal(none.source, 'global');
  assert.equal(none.thresholdRatio, 0.15);
  // magpie/group/ds 与 magpie-grp/group/ds 只有 model 同名，provider 必须分清
  assert.equal(selectPolicy(COMPACTION, { provider: 'magpie', model: 'group/ds' }).thresholdRatio, 0.1);
  assert.equal(selectPolicy(undefined, { provider: 'magpie', model: 'group/ds' }).thresholdRatio, 0.15);
});

test('文案：当前/压缩线/百分比 + 下一轮会自动压缩一次', () => {
  const main = formatPressureMessage({
    origin: 'main',
    pressureTokens: 101000,
    compactTokens: 104857,
  });
  assert.equal(
    main,
    '🐟 主会话 · 上下文 101.0k / 104.9k 压缩线（96%），超过后自动压缩\n下一轮会自动压缩一次',
  );

  const worker = formatPressureMessage({
    origin: 'worker',
    label: '票 t-abc「压缩线」',
    sessionId: '1753f90b-aaaa',
    pressureTokens: 1000000,
    compactTokens: 104857,
  });
  assert.match(worker, /^🐟 子会话（票 t-abc「压缩线」） · /);
  assert.match(worker, /（954%），超过后自动压缩/);
  assert.match(worker, /下一轮会自动压缩一次/);

  const noLabel = formatPressureMessage({
    origin: 'worker',
    sessionId: '1753f90b-aaaa',
    pressureTokens: 80000,
    compactTokens: 104857,
  });
  assert.match(noLabel, /子会话（1753f90b）/);
});

test('事件定义：topic 带点，payload 校验挡掉缺字段', () => {
  assert.equal(PRESSURE_EVENT_DEF.name, 'memory.context.pressure');
  assert.equal(PRESSURE_TOPIC, 'memory.context.pressure');
  const ok = {
    sessionId: 's1',
    title: 't',
    origin: 'main',
    level: 1,
    pressureTokens: 100000,
    thresholdTokens: NOTIFY_AT,
    compactTokens: LINE,
    ratio: 0.95,
    providerModel: 'magpie/group/ds',
    at: '2026-11-13T12:00:00.000Z',
  };
  assert.equal(PRESSURE_EVENT_DEF.payloadSchema(ok), true);
  assert.equal(PRESSURE_EVENT_DEF.payloadSchema({ ...ok, level: 0 }), false);
  assert.equal(PRESSURE_EVENT_DEF.payloadSchema({ ...ok, sessionId: '' }), false);
  assert.equal(PRESSURE_EVENT_DEF.payloadSchema({ ...ok, origin: 'other' }), false);
  assert.equal(PRESSURE_EVENT_DEF.payloadSchema({ ...ok, thresholdTokens: 0 }), false);
});

test('端到端：94% 不发、95% 发一次、1.02 不重发、压过后涨到 96% 再发一条', async () => {
  const cfg = tempCfg();
  const session = liveSession();
  const pressure = { pressureTokens: Math.floor(LINE * 0.5), contextWindow: 1048576 };
  const { ctx, published } = makeCtx({ session, pressure });
  const watch = setupContextWatch(ctx, cfg);

  // 0.5 线：不发
  await watch.check({ sessionId: 'sid-1', provider: 'magpie', model: 'group/ds' });
  assert.equal(published.length, 0, '50% 不该发');

  // 0.94 线：不发
  pressure.pressureTokens = Math.floor(LINE * 0.94);
  await watch.check({ sessionId: 'sid-1' });
  assert.equal(published.length, 0, '94% 不该发');

  // 0.95 线（含）：发一条
  pressure.pressureTokens = NOTIFY_AT;
  await watch.check({ sessionId: 'sid-1' });
  assert.equal(published.length, 1, '95% 要发一条');
  const payload = published[0].payload;
  assert.equal(published[0].name, 'memory.context.pressure');
  assert.equal(payload.level, 1);
  assert.equal(payload.origin, 'main');
  assert.equal(payload.thresholdTokens, NOTIFY_AT);
  assert.equal(payload.compactTokens, LINE);
  assert.equal(payload.providerModel, 'magpie/group/ds');
  assert.equal(PRESSURE_EVENT_DEF.payloadSchema(payload), true);
  const state = JSON.parse(fs.readFileSync(cfg.contextWatchStateFile, 'utf8'));
  assert.equal(state.sessions['sid-1'].notified, true, '已提醒要落盘，重启不重弹');
  assert.equal(state.sessions['sid-1'].atTokens, NOTIFY_AT);

  // 1.02 线：不重发
  pressure.pressureTokens = Math.floor(LINE * 1.02);
  await watch.check({ sessionId: 'sid-1' });
  assert.equal(published.length, 1, '过线后不许再发');

  // 模拟重启：还是不许重发
  const restarted = setupContextWatch(ctx, cfg);
  await restarted.check({ sessionId: 'sid-1' });
  assert.equal(published.length, 1, '重启后不能重弹');

  // 压过了（掉到 12%）：清记录，不发
  pressure.pressureTokens = Math.floor(LINE * 0.12);
  await restarted.check({ sessionId: 'sid-1' });
  assert.equal(published.length, 1, '压缩那一轮不发');
  const cleared = JSON.parse(fs.readFileSync(cfg.contextWatchStateFile, 'utf8'));
  assert.equal(cleared.sessions['sid-1'], undefined, '压过后记录要清掉');

  // 压完又涨到 0.96 线：再发一条（只一条）
  pressure.pressureTokens = Math.floor(LINE * 0.96);
  await restarted.check({ sessionId: 'sid-1' });
  assert.equal(published.length, 2, '压过之后涨回来可以再发一条');
  pressure.pressureTokens = Math.floor(LINE * 0.99);
  await restarted.check({ sessionId: 'sid-1' });
  assert.equal(published.length, 2, '再涨也不重复发');
});

test('端到端：压过一次、水位回落到 96% 也能再发一条（不必等跌破 45%）', async () => {
  const cfg = tempCfg();
  const session = liveSession();
  const pressure = { pressureTokens: LINE, contextWindow: 1048576 };
  const { ctx, published } = makeCtx({ session, pressure });
  const watch = setupContextWatch(ctx, cfg);
  await watch.check({ sessionId: 'sid-r' });
  assert.equal(published.length, 1, '100% 发第一条');
  const first = JSON.parse(fs.readFileSync(cfg.contextWatchStateFile, 'utf8')).sessions['sid-r'];
  assert.equal(typeof first.at, 'number');
  // 压缩发生在提醒之后，两轮检查之间水位只回落到 96%（没跌破 45%，也没低于上次提醒）
  session.log.push({ type: 'compaction/end', time: first.at + 1 });
  pressure.pressureTokens = Math.floor(LINE * 0.96);
  await watch.check({ sessionId: 'sid-r' });
  assert.equal(published.length, 2, '压过且水位比上次提醒低，允许再发一条');
  // 再涨：记录已更新，压缩记录不再晚于新提醒，不连发
  const second = JSON.parse(fs.readFileSync(cfg.contextWatchStateFile, 'utf8')).sessions['sid-r'];
  session.log[0].time = second.at;
  pressure.pressureTokens = Math.floor(LINE * 0.97);
  await watch.check({ sessionId: 'sid-r' });
  assert.equal(published.length, 2, '同一次压缩只换一条提醒');
});

test('端到端：压缩记录晚于提醒但水位继续涨，不重复发', async () => {
  const cfg = tempCfg();
  const session = liveSession();
  const pressure = { pressureTokens: NOTIFY_AT, contextWindow: 1048576 };
  const { ctx, published } = makeCtx({ session, pressure });
  const watch = setupContextWatch(ctx, cfg);
  await watch.check({ sessionId: 'sid-grow' });
  assert.equal(published.length, 1, '95% 发一条');
  const first = JSON.parse(fs.readFileSync(cfg.contextWatchStateFile, 'utf8')).sessions['sid-grow'];
  // 2026-11-15 03:11→03:14 的真身：日志里多了一条更晚的 compaction/end，水位却从
  // 199891 涨到 205232，第二条推送就是 98% 那条。
  session.log.push({ type: 'compaction/end', time: first.at + 1 });
  pressure.pressureTokens = 205232;
  await watch.check({ sessionId: 'sid-grow' });
  assert.equal(published.length, 1, '水位没降下来，只凭压缩记录不许重发');
  const kept = JSON.parse(fs.readFileSync(cfg.contextWatchStateFile, 'utf8')).sessions['sid-grow'];
  assert.equal(kept.atTokens, NOTIFY_AT, '记录保持不变');
});

test('端到端：旧内存副本落盘不会抹掉别的会话的已提醒记录', async () => {
  const cfg = tempCfg();
  const pressure = { pressureTokens: NOTIFY_AT, contextWindow: 1048576 };
  const { ctx, published } = makeCtx({ session: liveSession(), pressure });
  const stale = setupContextWatch(ctx, cfg);
  const fresh = setupContextWatch(ctx, cfg);
  await fresh.check({ sessionId: 'sid-a' });
  assert.equal(published.length, 1, 'sid-a 发一条');
  // 旧内存副本（建立时 state 是空的）对另一个会话发提醒并落盘：
  // 整份覆盖会抹掉 sid-a 的记录，读-合并-写不会。
  await stale.check({ sessionId: 'sid-b' });
  assert.equal(published.length, 2, 'sid-b 发一条');
  const reread = setupContextWatch(ctx, cfg);
  await reread.check({ sessionId: 'sid-a' });
  assert.equal(published.length, 2, 'sid-a 的已提醒记录不能被旧副本抹掉');
  const state = JSON.parse(fs.readFileSync(cfg.contextWatchStateFile, 'utf8'));
  assert.equal(state.sessions['sid-a']?.notified, true);
  assert.equal(state.sessions['sid-b']?.notified, true);
});

test('端到端：压过之后的删除墓碑挡住旧副本把记录加回来', async () => {
  const cfg = tempCfg();
  const pressure = { pressureTokens: NOTIFY_AT, contextWindow: 1048576 };
  const { ctx, published } = makeCtx({ session: liveSession(), pressure });
  const stale = setupContextWatch(ctx, cfg);
  await stale.check({ sessionId: 'sid-c' });
  assert.equal(published.length, 1, 'sid-c 发一条，旧副本内存里有记录');
  const fresh = setupContextWatch(ctx, cfg);
  pressure.pressureTokens = Math.floor(LINE * 0.12);
  await fresh.check({ sessionId: 'sid-c' });
  assert.equal(published.length, 1, '压缩那一轮不发');
  // 旧副本随后对别的会话落盘：墓碑要挡住它把 sid-c 的已提醒记录加回来
  pressure.pressureTokens = NOTIFY_AT;
  await stale.check({ sessionId: 'sid-d' });
  const state = JSON.parse(fs.readFileSync(cfg.contextWatchStateFile, 'utf8'));
  assert.equal(state.sessions['sid-c'], undefined, '墓碑生效，记录不许被旧副本复活');
  assert.equal(typeof state.tombstones['sid-c'], 'number');
});

test('端到端：旧 state 里 level=4 的会话，重启后不重弹', async () => {
  const cfg = tempCfg();
  fs.writeFileSync(
    cfg.contextWatchStateFile,
    JSON.stringify({ sessions: { 'sid-old': { level: 4, at: 1794566627421 } } }),
  );
  const session = liveSession();
  const { ctx, published } = makeCtx({
    session,
    pressure: { pressureTokens: Math.floor(LINE * 0.96), contextWindow: 1048576 },
  });
  const watch = setupContextWatch(ctx, cfg);
  await watch.check({ sessionId: 'sid-old' });
  assert.equal(published.length, 0, '旧 state 已提醒过的会话不许重弹');
});

test('端到端：拿不到会话对象/窗口时不发事件，也不炸', async () => {
  const cfg = tempCfg();
  const { ctx, published } = makeCtx({ session: undefined, pressure: {} });
  const watch = setupContextWatch(ctx, cfg);
  await watch.check({ sessionId: 'needle' });
  assert.equal(published.length, 0);
  assert.match(fs.readFileSync(cfg.contextWatchLogFile, 'utf8'), /拿不到会话对象/);
});

test('端到端：events 服务不在时只记日志，判档照走', async () => {
  const cfg = tempCfg();
  const session = liveSession();
  const { ctx } = makeCtx({ session, pressure: { pressureTokens: NOTIFY_AT, contextWindow: 1048576 } });
  const noEvents = { get: ctx.get };
  const watch = setupContextWatch(
    { get: (name) => (name === 'events' ? undefined : noEvents.get(name)) },
    cfg,
  );
  await watch.check({ sessionId: 'sid-x' });
  assert.match(fs.readFileSync(cfg.contextWatchLogFile, 'utf8'), /events 服务不在/);
});

test('事件类型重复注册（热重载）时继续用已注册的那份发事件，别把提醒搞哑', async () => {
  const cfg = tempCfg();
  const session = liveSession();
  const published = [];
  const def = { name: PRESSURE_TOPIC, version: 1 };
  const events = {
    registry: { get: (name) => (name === PRESSURE_TOPIC ? def : undefined) },
    defineEvent() {
      throw Object.assign(new Error('事件类型重复注册：memory.context.pressure v1'), { code: 'EV-001' });
    },
    publish(input) {
      published.push(input);
    },
  };
  const ctx = {
    get: (name) =>
      name === 'sessions' ? { get: () => session }
      : name === 'tokenMeter' ? { contextPressure: () => ({ pressureTokens: NOTIFY_AT, contextWindow: 1048576 }) }
      : name === 'compaction' ? { config: COMPACTION }
      : name === 'events' ? events
      : undefined,
  };
  const watch = setupContextWatch(ctx, cfg);
  await watch.check({ sessionId: 'sid-dup' });
  assert.equal(published.length, 1);
  assert.equal(published[0].payload.level, 1);
});

test('子会话判定：compactWorker 没显式打开就跳过，主会话永远不跳', () => {
  assert.equal(shouldSkipSession({ origin: 'worker', compactWorker: false }), true);
  assert.equal(shouldSkipSession({ origin: 'worker', compactWorker: undefined }), true, '配置缺这个键也当没开');
  assert.equal(shouldSkipSession({ origin: 'worker', compactWorker: true }), false, '显式开了才恢复提醒');
  assert.equal(shouldSkipSession({ origin: 'main', compactWorker: false }), false, '主会话不受影响');
  assert.equal(shouldSkipSession({ origin: 'main' }), false);
  assert.equal(shouldSkipSession(), false);
});

test('端到端：子会话（header.origin=subagent）在 99% 也不发提醒，主会话同水位照发', async () => {
  const cfg = tempCfg();
  const worker = liveSession({ header: { origin: 'subagent', delegationDepth: 1 } });
  const workerPressure = { pressureTokens: Math.floor(LINE * 0.99), contextWindow: 1048576 };
  const workerCtx = makeCtx({ session: worker, pressure: workerPressure });
  const workerWatch = setupContextWatch(workerCtx.ctx, cfg);
  await workerWatch.check({ sessionId: 'session-worker-1' });
  await workerWatch.check({ sessionId: 'session-worker-1' });
  assert.equal(workerCtx.published.length, 0, '子会话不许收到压缩线提醒');
  assert.match(
    fs.readFileSync(cfg.contextWatchLogFile, 'utf8'),
    /不限制上下文，跳过压线提醒/,
    '跳过要留一行日志，别静默',
  );
  const state = fs.existsSync(cfg.contextWatchStateFile)
    ? JSON.parse(fs.readFileSync(cfg.contextWatchStateFile, 'utf8'))
    : { sessions: {} };
  assert.equal(state.sessions['session-worker-1'], undefined, '子会话不落 state');

  // 同一个 99% 水位换主会话，照旧发一条
  const main = liveSession();
  const mainCtx = makeCtx({ session: main, pressure: { pressureTokens: Math.floor(LINE * 0.99), contextWindow: 1048576 } });
  const mainWatch = setupContextWatch(mainCtx.ctx, cfg);
  await mainWatch.check({ sessionId: 'sid-main-1' });
  assert.equal(mainCtx.published.length, 1, '主会话 95% 提醒保留');
  assert.equal(mainCtx.published[0].payload.origin, 'main');
  assert.equal(mainCtx.published[0].payload.compactTokens, LINE);
});

test('端到端：子会话以前发过的记录被清掉，不会因compactWorker以后打开而重弹旧水位', async () => {
  const cfg = tempCfg();
  fs.writeFileSync(
    cfg.contextWatchStateFile,
    JSON.stringify({ sessions: { 'session-worker-old': { level: 1, notified: true, atTokens: NOTIFY_AT, at: 1794566627421 } } }),
  );
  const worker = liveSession({ header: { origin: 'subagent' } });
  const { ctx, published } = makeCtx({
    session: worker,
    pressure: { pressureTokens: Math.floor(LINE * 0.99), contextWindow: 1048576 },
  });
  const watch = setupContextWatch(ctx, cfg);
  await watch.check({ sessionId: 'session-worker-old' });
  assert.equal(published.length, 0, '子会话不发');
  const state = JSON.parse(fs.readFileSync(cfg.contextWatchStateFile, 'utf8'));
  assert.equal(state.sessions['session-worker-old'], undefined, '旧提醒记录要清掉');
});
