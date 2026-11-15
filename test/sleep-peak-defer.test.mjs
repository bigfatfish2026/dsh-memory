// 2026-11-14：补跑只许在便宜时段发生。
// 贵时段重启，启动补判不许当场把过点的那一轮跑掉——一轮 30 分钟以上、烧真钱。
//
// 这批测试分两层：
// - 纯函数层：价档判据注入、补跑判定、待补槽分堆（含边界分钟、跨天、积压兜底）；
// - 集成层：用 setupSleep 真起一遍（sessions 空 → 整理秒退成 too-quiet），
//   看 sleep.state.json 的 owedSlots / slots 和日志行到底写成什么样。
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { setupSleep } from '../lib/sleep.js';
import {
  SLEEP_CATCHUP_GRACE_MS, SLEEP_OWED_MAX_AGE_MS,
  isCatchUpSlot, isCheapNow, planSlotRun, resetCheapNowProbe, setCheapNowProbe,
  slotTimeOf, splitOwedSlots,
} from '../lib/price-gate.js';

const DAY_MS = 24 * 60 * 60 * 1000;
const pad = (n) => String(n).padStart(2, '0');
const dayKey = (d) => `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
const slotKey = (d, h, m) => `${dayKey(d)} ${pad(h)}:${pad(m)}`;

function tmpDir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'dshm-peak-'));
}

/** 最小可跑的 sleep 配置：sessions 空目录 + 走老路径 → sleepPass 立刻 too-quiet 返回。 */
function makeCfg(dir, schedule = [{ hour: 5, minute: 0 }]) {
  const sessionsRoot = path.join(dir, 'sessions');
  fs.mkdirSync(sessionsRoot, { recursive: true });
  return {
    storePath: dir,
    sessionsRoot,
    stateFile: path.join(dir, 'sleep.state.json'),
    backupDir: path.join(dir, 'backup'),
    sleepLogFile: path.join(dir, 'memory_sleep.log'),
    splitStateFile: path.join(dir, 'split.state.json'),
    splitLogFile: path.join(dir, 'memory_split.log'),
    splitPreviewFile: path.join(dir, 'split-preview.md'),
    selfTestMarker: path.join(dir, 'no-self-test-marker'),
    notifyScript: '/bin/true',
    notify: false,
    sleepEnabled: true,
    // 这批测试自己用 setCheapNowProbe 摆判据，不要让 setupSleep 再注入 dsh-price-clock 盖掉
    // （2026-11-14 接了真注入；真注入的覆盖在 sleep-price-probe.test.mjs 里测）。
    sleepPriceProbe: false,
    provider: 'fake',
    model: 'fake',
    schedule,
    maxSessions: 5,
    maxMessageChars: 1500,
    sleepMaxTokens: 512,
    splitMaxTokens: 512,
    clusterMaxSize: 30,
    clusterNearDup: 0.9,
    clusterQuestionMin: 0.78,
    clusterChanges: 6,
    organizeMaxCalls: 1,
    organizeConcurrency: 1,
    sleepSplitEnabled: false,
    conflictCheckEnabled: false,
    sleepTools: false,
    pipeline: { ingestAck: false, ingestBudget: 5, replayBudget: 5, writeGate: false, agentPipeline: false },
  };
}

/** 起一遍 setupSleep：effect 立即执行（跟 dryrun-no-write 那套假 ctx 一致），并收集 disposer。 */
function bootSleep(cfg, dir) {
  const calls = { llm: 0 };
  const disposers = [];
  const ctx = {
    logger: { info() {} },
    llm: {
      async *stream() {
        calls.llm += 1;
        yield { type: 'text-delta', text: '{"add":[],"merge":[],"update":[],"archive":[]}' };
        yield { type: 'finish', reason: { kind: 'stop' } };
      },
    },
    tools: {
      get: () => null,
      register() {},
    },
    provide() {},
    effect(fn) {
      const off = fn();
      if (typeof off === 'function') disposers.push(off);
      return off;
    },
  };
  const store = {};
  setupSleep(ctx, store, cfg);
  return {
    calls,
    stop() {
      for (const off of disposers) {
        try {
          off();
        } catch {
          /* 收尾失败不影响断言 */
        }
      }
    },
  };
}

function readState(cfg) {
  try {
    return JSON.parse(fs.readFileSync(cfg.stateFile, 'utf8'));
  } catch {
    return {};
  }
}

function writeState(cfg, state) {
  fs.mkdirSync(path.dirname(cfg.stateFile), { recursive: true });
  fs.writeFileSync(cfg.stateFile, JSON.stringify(state, null, 2), 'utf8');
}

function readLog(cfg) {
  try {
    return fs.readFileSync(cfg.sleepLogFile, 'utf8');
  } catch {
    return '';
  }
}

/** 等异步 scheduledRun 把 state 写出来（整理是 void 起的，setupSleep 不 await 它）。 */
async function waitFor(fn, ms = 3000) {
  const started = Date.now();
  for (;;) {
    const v = await fn();
    if (v) return v;
    if (Date.now() - started > ms) return undefined;
    await new Promise((r) => setTimeout(r, 20));
  }
}

test.afterEach(() => {
  resetCheapNowProbe();
});

// ── 纯函数层：判据注入 ─────────────────────────────────────────────

test('没有接判据：isCheapNow 一律便宜（行为跟改动前一模一样）', () => {
  resetCheapNowProbe();
  assert.equal(isCheapNow(new Date('2026-11-11T10:00:00+08:00')), true, '工作日 10 点也放行');
  assert.equal(isCheapNow(new Date('2026-11-11T15:00:00+08:00')), true);
});

test('注入判据后按它说：false = 贵，true = 便宜；拔掉后回到默认', () => {
  setCheapNowProbe(() => false);
  assert.equal(isCheapNow(new Date()), false);
  setCheapNowProbe(() => true);
  assert.equal(isCheapNow(new Date()), true);
  setCheapNowProbe(undefined);
  assert.equal(isCheapNow(new Date('2026-11-11T10:00:00+08:00')), true);
});

test('判据抛错按便宜处理（fail-open：不许判据一坏就把整理停掉）', () => {
  setCheapNowProbe(() => {
    throw new Error('价档表炸了');
  });
  assert.equal(isCheapNow(new Date()), true);
});

// ── 纯函数层：补跑判定与边界分钟 ───────────────────────────────────

test('slotKey 解析：合法 key 给出那一档的本地时刻，坏 key 给 undefined', () => {
  assert.equal(slotTimeOf('2026-11-14 12:30').getTime(), new Date('2026-11-14T12:30:00+08:00').getTime());
  assert.equal(slotTimeOf('坏 key'), undefined);
  assert.equal(slotTimeOf(undefined), undefined);
});

test('补跑判定：正好 grace 不算，多 1ms 才算', () => {
  const at = new Date('2026-11-14T12:30:00+08:00');
  const key = '2026-11-14 12:30';
  assert.equal(isCatchUpSlot({ key, now: at }), false, '准点触发不是补跑');
  assert.equal(isCatchUpSlot({ key, now: new Date(at.getTime() + SLEEP_CATCHUP_GRACE_MS) }), false, '正好 5 分钟仍算准点抖动');
  assert.equal(
    isCatchUpSlot({ key, now: new Date(at.getTime() + SLEEP_CATCHUP_GRACE_MS + 1) }),
    true,
    '过了 grace 就是补跑',
  );
  assert.equal(isCatchUpSlot({ key: '坏 key', now: at }), false);
});

test('半个价档边界：便宜开始那一分钟放行，前一分钟仍推迟', () => {
  // 假表：14:00 从贵切到便宜（模拟「峰价结束 / 半价开始」那一分钟）
  const switchAt = new Date('2026-11-11T14:00:00+08:00').getTime();
  setCheapNowProbe((now) => now.getTime() >= switchAt);
  const key = '2026-11-11 05:00';

  assert.equal(isCheapNow(new Date(switchAt - 1000)), false, '13:59:59 还是贵');
  assert.equal(planSlotRun({ key, cheap: isCheapNow(new Date(switchAt - 1000)), catchUp: true }), 'defer');
  assert.equal(isCheapNow(new Date(switchAt)), true, '14:00:00 整点算便宜');
  assert.equal(planSlotRun({ key, cheap: isCheapNow(new Date(switchAt)), catchUp: true }), 'run');
});

test('价档边界：贵时段的最后一分钟仍推迟，下一秒放行', () => {
  // 假表：贵到 18:00 为止（18:00 起便宜）
  const endAt = new Date('2026-11-11T18:00:00+08:00').getTime();
  setCheapNowProbe((now) => now.getTime() >= endAt);
  const key = '2026-11-11 12:30';
  assert.equal(planSlotRun({ key, cheap: isCheapNow(new Date(endAt - 60 * 1000)), catchUp: true }), 'defer');
  assert.equal(planSlotRun({ key, cheap: isCheapNow(new Date(endAt - 1)), catchUp: true }), 'defer');
  assert.equal(planSlotRun({ key, cheap: isCheapNow(new Date(endAt)), catchUp: true }), 'run');
});

test('准点定时档撞贵时段：这轮先不改定时器行为，仍放行', () => {
  assert.equal(planSlotRun({ key: '2026-11-11 12:30', cheap: false, catchUp: false }), 'run');
  assert.equal(planSlotRun({ key: '2026-11-11 12:30', cheap: false, catchUp: true }), 'defer');
  assert.equal(planSlotRun({ key: '', cheap: false, catchUp: true }), 'none');
});

test('待补槽分堆：今天的留下、跨天的丢、积压超 2 天标 expired', () => {
  const now = new Date('2026-11-14T13:00:00+08:00');
  const { today, dropped } = splitOwedSlots(
    {
      '2026-11-14 05:00': now.getTime() - 3600 * 1000,
      '2026-11-14 12:30': now.getTime() - 60 * 1000,
      '2026-11-13 12:30': now.getTime() - DAY_MS,
      '2026-11-12 12:30': now.getTime() - (2 * DAY_MS - 3600 * 1000),
      '2026-11-09 12:30': now.getTime() - 5 * DAY_MS,
    },
    now,
  );
  assert.deepEqual(today.map((o) => o.key), ['2026-11-14 05:00', '2026-11-14 12:30'], '今天的两档按时刻升序');
  assert.equal(dropped.length, 3);
  assert.equal(dropped.find((o) => o.key === '2026-11-13 12:30').expired, false, '跨天一天：普通作废');
  assert.equal(dropped.find((o) => o.key === '2026-11-12 12:30').expired, false, '差一小时到 2 天：还算普通作废');
  const over = dropped.find((o) => o.key === '2026-11-09 12:30');
  assert.equal(over.expired, true, '积压 5 天：要打醒目日志');
  assert.ok(over.ageMs > SLEEP_OWED_MAX_AGE_MS);
});

// ── 集成层：真起 setupSleep，看 state 和日志 ───────────────────────

test('贵时段 + 有待补的档：不跑、记 owedSlots、日志写明推迟', () => {
  const dir = tmpDir();
  const now = new Date();
  const key = slotKey(now, now.getHours(), now.getMinutes()); // 档口就是现在，必然过点
  const cfg = makeCfg(dir, [{ hour: now.getHours(), minute: now.getMinutes() }]);
  const oldOwner = 'deadbeef:1234:5678';
  writeState(cfg, {
    slots: {},
    owner: oldOwner,
    runningAt: now.getTime() - 2000,
    heartbeatAt: now.getTime() - 2000,
  });
  setCheapNowProbe(() => false); // 现在是贵时段

  const boot = bootSleep(cfg, dir);
  try {
    const st = readState(cfg);
    assert.deepEqual(Object.keys(st.owedSlots || {}), [key], '这一档要记进 owedSlots');
    assert.equal(st.slots?.[key], undefined, '没跑，就不许落 slot 标记');
    assert.equal(boot.calls.llm, 0, '贵时段一次模型调用都不许发');
    const logText = readLog(cfg);
    assert.match(logText, new RegExp(`\\[sleep\\] 贵时段，槽 ${key.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')} 推迟到下一个便宜时段`));
    assert.doesNotMatch(logText, /接管上一实例的轮/, '推迟了就不算接管，别打接管的日志');
  } finally {
    boot.stop();
  }
});

test('owner 是空（老 state / 正常退出）：过点很久的补跑一样推迟', () => {
  // 00:00-00:05 之间「2 小时前」落不进今天，这个时段本身没法构造补跑场景，跳过。
  const now = new Date();
  if (now.getHours() === 0 && now.getMinutes() < 6) return;
  const dir = tmpDir();
  const slot = now.getHours() >= 2 ? { hour: now.getHours() - 2, minute: 0 } : { hour: 0, minute: 0 };
  const key = slotKey(now, slot.hour, slot.minute);
  const cfg = makeCfg(dir, [slot]);
  writeState(cfg, { slots: {} }); // 没有 owner，靠「过点超过 grace」判成补跑
  setCheapNowProbe(() => false);

  const boot = bootSleep(cfg, dir);
  try {
    const st = readState(cfg);
    assert.deepEqual(Object.keys(st.owedSlots || {}), [key], '过点很久的档在贵时段照样记待补');
    assert.equal(st.slots?.[key], undefined);
    assert.match(readLog(cfg), /\[sleep\] 贵时段，槽 /);
  } finally {
    boot.stop();
  }
});

test('贵时段同一档连判两拍：只记一次推迟日志，不刷屏', () => {
  const dir = tmpDir();
  const now = new Date();
  const cfg = makeCfg(dir, [{ hour: now.getHours(), minute: now.getMinutes() }]);
  writeState(cfg, { slots: {}, owner: 'deadbeef:1:2', runningAt: now.getTime() - 1000, heartbeatAt: now.getTime() - 1000 });
  setCheapNowProbe(() => false);

  const first = bootSleep(cfg, dir);
  first.stop();
  const second = bootSleep(cfg, dir); // 第二次启动 = 下一拍
  try {
    const logText = readLog(cfg);
    const hits = logText.match(/\[sleep\] 贵时段，槽 /g) || [];
    assert.equal(hits.length, 1, `推迟日志只许一行，实际 ${hits.length}`);
    assert.equal(Object.keys(readState(cfg).owedSlots || {}).length, 1);
  } finally {
    second.stop();
  }
});

test('进入便宜时段：待补的槽当场补跑一次、清 owed、照常落 slot 标记', async () => {
  const dir = tmpDir();
  const now = new Date();
  const key = slotKey(now, 5, 0); // 05:00 那档欠着
  const cfg = makeCfg(dir, [{ hour: 5, minute: 0 }]);
  writeState(cfg, {
    slots: {},
    owedSlots: { [key]: now.getTime() - 3600 * 1000 },
    owner: 'deadbeef:9:9',
    runningAt: now.getTime() - 3600 * 1000,
    heartbeatAt: now.getTime() - 3600 * 1000,
  });
  setCheapNowProbe(() => true); // 现在是便宜时段

  const boot = bootSleep(cfg, dir);
  try {
    const st = await waitFor(() => {
      const cur = readState(cfg);
      return cur.slots?.[key] ? cur : undefined;
    });
    assert.ok(st, '补跑完必须落 slot 标记');
    assert.deepEqual(readState(cfg).owedSlots || {}, {}, '补跑完 owedSlots 要清空');
    assert.match(readLog(cfg), new RegExp(`\\[sleep\\] 便宜时段，补跑待补的槽 ${key}`));
  } finally {
    boot.stop();
  }
});

test('便宜时段 + 没有待补：正常到点当场跑，不写 owedSlots', async () => {
  const dir = tmpDir();
  const now = new Date();
  const key = slotKey(now, now.getHours(), now.getMinutes());
  const cfg = makeCfg(dir, [{ hour: now.getHours(), minute: now.getMinutes() }]);
  writeState(cfg, { slots: {}, owner: 'deadbeef:3:4', runningAt: now.getTime() - 500, heartbeatAt: now.getTime() - 500 });
  setCheapNowProbe(() => true);

  const boot = bootSleep(cfg, dir);
  try {
    const st = await waitFor(() => {
      const cur = readState(cfg);
      return cur.slots?.[key] ? cur : undefined;
    });
    assert.ok(st, '便宜时段该跑');
    assert.deepEqual(readState(cfg).owedSlots || {}, {});
    assert.match(readLog(cfg), /接管上一实例的轮/);
    assert.doesNotMatch(readLog(cfg), /\[sleep\] 贵时段/);
  } finally {
    boot.stop();
  }
});

test('同一个槽补跑两次：第二次被 slots 标记挡住，不再跑', async () => {
  const dir = tmpDir();
  const now = new Date();
  const key = slotKey(now, 5, 0);
  const cfg = makeCfg(dir, [{ hour: 5, minute: 0 }]);
  writeState(cfg, { slots: {}, owedSlots: { [key]: now.getTime() - 3600 * 1000 } });
  setCheapNowProbe(() => true);

  const first = bootSleep(cfg, dir);
  const marked = await waitFor(() => readState(cfg).slots?.[key]);
  first.stop();
  assert.ok(marked, '第一遍要真跑完并落标记');

  // 第二遍：把同一档又塞回 owedSlots（模拟状态被手改 / 重复记账）
  const before = readState(cfg);
  writeState(cfg, { ...before, owedSlots: { [key]: now.getTime() - 1800 * 1000 } });
  const second = bootSleep(cfg, dir);
  try {
    await new Promise((r) => setTimeout(r, 200));
    const after = readState(cfg);
    assert.equal(after.slots[key], before.slots[key], 'slot 标记时间戳不许被第二遍改写');
    assert.deepEqual(after.owedSlots || {}, {}, '有 slot 标记的待补槽要被清掉');
    const hits = readLog(cfg).match(/\[sleep\] 便宜时段，补跑待补的槽 /g) || [];
    assert.equal(hits.length, 1, `补跑日志只许一行，实际 ${hits.length}`);
  } finally {
    second.stop();
  }
});

test('待补槽积压超过 2 天：醒目日志作废，不静默丢', () => {
  const dir = tmpDir();
  const now = new Date();
  const cfg = makeCfg(dir, [{ hour: 23, minute: 59 }]); // 深夜档，进来只看清理不看跑
  const stale = slotKey(new Date(now.getTime() - 5 * DAY_MS), 12, 30);
  writeState(cfg, { slots: {}, owedSlots: { [stale]: now.getTime() - 5 * DAY_MS } });
  setCheapNowProbe(() => true);

  const boot = bootSleep(cfg, dir);
  try {
    const logText = readLog(cfg);
    assert.match(logText, /\[sleep\] ⚠️ 待补槽 .* 积压 \d+ 小时还没补上，价档判断可能坏了，先作废/);
    assert.deepEqual(readState(cfg).owedSlots || {}, {}, '作废要真的从 state 里删掉');
  } finally {
    boot.stop();
  }
});

test('待补槽跨天（未超 2 天）：作废不补，别隔天白跑一遍', () => {
  const dir = tmpDir();
  const now = new Date();
  const cfg = makeCfg(dir, [{ hour: 23, minute: 59 }]);
  const yesterday = slotKey(new Date(now.getTime() - DAY_MS), 12, 30);
  writeState(cfg, { slots: {}, owedSlots: { [yesterday]: now.getTime() - DAY_MS } });
  setCheapNowProbe(() => true);

  const boot = bootSleep(cfg, dir);
  try {
    assert.match(readLog(cfg), /\[sleep\] 待补槽 .* 已跨天，作废不补/);
    assert.doesNotMatch(readLog(cfg), /补跑待补的槽/);
    assert.deepEqual(readState(cfg).owedSlots || {}, {});
  } finally {
    boot.stop();
  }
});

test('sleepDeferInPeak=false：退回旧行为，贵时段照样当场补跑', async () => {
  const dir = tmpDir();
  const now = new Date();
  const key = slotKey(now, 5, 0);
  const cfg = makeCfg(dir, [{ hour: 5, minute: 0 }]);
  cfg.sleepDeferInPeak = false;
  writeState(cfg, {
    slots: {},
    owner: 'deadbeef:5:6',
    runningAt: now.getTime() - 3600 * 1000,
    heartbeatAt: now.getTime() - 3600 * 1000,
  });
  setCheapNowProbe(() => false); // 贵时段，但开关关着

  const boot = bootSleep(cfg, dir);
  try {
    const st = await waitFor(() => {
      const cur = readState(cfg);
      return cur.slots?.[key] ? cur : undefined;
    });
    assert.ok(st, '开关关着就按老行为跑');
    assert.deepEqual(readState(cfg).owedSlots || {}, {});
  } finally {
    boot.stop();
  }
});
