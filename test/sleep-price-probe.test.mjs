// 2026-11-14：价档闸只接了一半——注入口在自己手里，判据在 dsh-price-clock。
// 这批测试盯三件事：
//   1. 真接上了没有：注入后 price-gate 的判据就是 dsh-price-clock 的 isCheapNow（固定时刻对账）；
//   2. 接不上会不会把整理停掉：路径断了、判据抛错，都按全时段便宜继续跑，只留一行告警；
//   3. 真跑一遍 setupSleep：贵时段推迟、同一槽不刷日志、翻便宜当拍补跑。
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { setupSleep } from '../lib/sleep.js';
import { attachPriceProbe, PRICE_CLOCK_CLOCK_FILE, PROBE_FAIL_LINE, PROBE_OK_LINE } from '../lib/price-probe.js';
import { isCheapNow, resetCheapNowProbe } from '../lib/price-gate.js';

const pad = (n) => String(n).padStart(2, '0');
const dayKey = (d) => `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
const slotKey = (d, h, m) => `${dayKey(d)} ${pad(h)}:${pad(m)}`;

function tmpDir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'dshm-probe-'));
}

/**
 * 假判据模块。后缀必须是 .mjs：临时目录没有 package.json，.js 会被当成 CJS 解析，require 直接炸。
 * 判据读 globalThis.__fakeCheap，测试中途翻档不用重写文件。
 */
function writeFakeClock(dir, { throws = false } = {}) {
  const file = path.join(dir, 'fake-clock.mjs');
  const body = throws
    ? 'export function isCheapNow() { throw new Error("fake clock boom"); }\n'
    : 'export function isCheapNow() { return globalThis.__fakeCheap !== false; }\n';
  fs.writeFileSync(file, body, 'utf8');
  return file;
}

/**
 * 找 dsh-price-clock 的判据文件。干净 clone 里没有兄弟仓，找不到就跳过真注入那条，
 * 不许因为环境缺件把整个测试挂掉。
 * 顺序：DSH_PRICE_CLOCK（也认 DSH_PRICE_CLOCK_FILE）→ node_modules → ../dsh-price-clock → 默认路径。
 */
function resolvePriceClockFile() {
  const candidates = [];
  const explicit = process.env.DSH_PRICE_CLOCK || process.env.DSH_PRICE_CLOCK_FILE;
  if (explicit) candidates.push(explicit);
  candidates.push(path.resolve(process.cwd(), 'node_modules/dsh-price-clock/lib/clock.js'));
  candidates.push(path.resolve(process.cwd(), '../dsh-price-clock/lib/clock.js'));
  candidates.push(PRICE_CLOCK_CLOCK_FILE);
  return candidates.find((file) => {
    try { return fs.existsSync(file); } catch { return false; }
  });
}

/** 最小可跑的 sleep 配置：sessions 空目录 → sleepPass 立刻 too-quiet 返回，但 slot 标记照样落。 */
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
function bootSleep(cfg) {
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
    tools: { get: () => null, register() {} },
    provide() {},
    effect(fn) {
      const off = fn();
      if (typeof off === 'function') disposers.push(off);
      return off;
    },
  };
  setupSleep(ctx, {}, cfg);
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

async function waitFor(fn, ms = 3000) {
  const started = Date.now();
  for (;;) {
    const v = await fn();
    if (v) return v;
    if (Date.now() - started > ms) return undefined;
    await new Promise((r) => setTimeout(r, 20));
  }
}

/** 造一个「上一实例留下的轮锁 + 档口就是现在」的状态：必然算补跑，走价档闸。 */
function dyingOwnerState(cfg) {
  const now = new Date();
  const key = slotKey(now, now.getHours(), now.getMinutes());
  writeState(cfg, {
    slots: {},
    owner: 'deadbeef:1234:5678',
    runningAt: now.getTime() - 2000,
    heartbeatAt: now.getTime() - 2000,
  });
  return key;
}

test.afterEach(() => {
  resetCheapNowProbe();
  delete globalThis.__fakeCheap;
});

test('真注入：判据就是 dsh-price-clock 的 isCheapNow（固定时刻对账）', async (t) => {
  const dir = tmpDir();
  const cfg = makeCfg(dir);
  const clockFile = resolvePriceClockFile();
  if (!clockFile) return t.skip('dsh-price-clock 未安装');
  // 解析出来的就是唯一那份，先确认它真在
  assert.ok(fs.existsSync(clockFile), `判据文件要在：${clockFile}`);
  assert.equal(attachPriceProbe(cfg, clockFile), true, '注入要成功');
  assert.match(readLog(cfg), new RegExp(PROBE_OK_LINE.replace(/[[\]]/g, '\\$&')));

  const clock = await import(clockFile);
  const samples = [
    new Date('2026-11-14T13:50:00+08:00'), // 周六下午：半价
    new Date('2026-11-11T10:00:00+08:00'), // 周三上午峰时：标准价
    new Date('2026-11-11T12:30:00+08:00'), // 周三午休：半价
    new Date('2026-11-11T03:00:00+08:00'), // 深夜：半价
  ];
  for (const t of samples) {
    assert.equal(isCheapNow(t), clock.isCheapNow(t), `${t.toISOString()} 两边判据要一致`);
  }
  assert.equal(isCheapNow(new Date('2026-11-11T10:00:00+08:00')), false, '周三上午确实判得贵');
});

test('注入成功 + 判据说贵：走推迟分支、记 owedSlots、日志写明推迟', () => {
  const dir = tmpDir();
  const cfg = makeCfg(dir, [{ hour: new Date().getHours(), minute: new Date().getMinutes() }]);
  cfg.priceClockFile = writeFakeClock(dir);
  globalThis.__fakeCheap = false; // 贵
  const key = dyingOwnerState(cfg);

  const boot = bootSleep(cfg);
  try {
    const st = readState(cfg);
    assert.deepEqual(Object.keys(st.owedSlots || {}), [key], '这一档要记进 owedSlots');
    assert.equal(st.slots?.[key], undefined, '没跑就不许落 slot 标记');
    assert.equal(boot.calls.llm, 0, '贵时段一次模型调用都不许发');
    const logText = readLog(cfg);
    assert.match(logText, /\[sleep\] 价档判据已接 dsh-price-clock/);
    assert.match(logText, new RegExp(`\\[sleep\\] 贵时段，槽 ${key} 推迟到下一个便宜时段`));
  } finally {
    boot.stop();
  }
});

test('同一个槽在 owedSlots 里连判两拍：只打一行推迟日志', () => {
  const dir = tmpDir();
  const cfg = makeCfg(dir, [{ hour: new Date().getHours(), minute: new Date().getMinutes() }]);
  cfg.priceClockFile = writeFakeClock(dir);
  globalThis.__fakeCheap = false;
  dyingOwnerState(cfg);

  const first = bootSleep(cfg);
  first.stop();
  const second = bootSleep(cfg); // 60 秒后的下一拍
  try {
    const hits = readLog(cfg).match(/\[sleep\] 贵时段，槽 /g) || [];
    assert.equal(hits.length, 1, `推迟日志只许一行，实际 ${hits.length}`);
    assert.equal(Object.keys(readState(cfg).owedSlots || {}).length, 1, '待补只记一条');
  } finally {
    second.stop();
  }
});

test('判据从贵翻成便宜：下一拍补跑 owed、清 owed、日志写明补跑', async () => {
  const dir = tmpDir();
  const now = new Date();
  const cfg = makeCfg(dir, [{ hour: now.getHours(), minute: now.getMinutes() }]);
  cfg.priceClockFile = writeFakeClock(dir);
  globalThis.__fakeCheap = false;
  const key = dyingOwnerState(cfg);

  const first = bootSleep(cfg);
  first.stop();
  assert.deepEqual(Object.keys(readState(cfg).owedSlots || {}), [key], '第一拍欠下这一档');

  globalThis.__fakeCheap = true; // 进入便宜时段
  const second = bootSleep(cfg);
  try {
    const st = await waitFor(() => {
      const cur = readState(cfg);
      return cur.slots?.[key] ? cur : undefined;
    });
    assert.ok(st, '补跑完必须落 slot 标记');
    assert.deepEqual(readState(cfg).owedSlots || {}, {}, '补跑完 owedSlots 要清空');
    assert.match(readLog(cfg), new RegExp(`\\[sleep\\] 便宜时段，补跑待补的槽 ${key}`));
  } finally {
    second.stop();
  }
});

test('注入失败（路径断了）：fail-open 照跑，留一行告警', async () => {
  const dir = tmpDir();
  const cfg = makeCfg(dir, [{ hour: new Date().getHours(), minute: new Date().getMinutes() }]);
  cfg.priceClockFile = path.join(dir, 'not-there', 'clock.mjs');
  const key = dyingOwnerState(cfg);

  const boot = bootSleep(cfg);
  try {
    const st = await waitFor(() => {
      const cur = readState(cfg);
      return cur.slots?.[key] ? cur : undefined;
    });
    assert.ok(st, '注入失败也必须照常整理');
    const logText = readLog(cfg);
    assert.match(logText, /\[sleep\] 价档判据注入失败，按全时段便宜处理/);
    assert.doesNotMatch(logText, /\[sleep\] 价档判据已接/, '失败就不许说接上了');
    assert.doesNotMatch(logText, /\[sleep\] 贵时段，槽 /, '没判据就是全时段便宜，不许推迟');
    assert.deepEqual(readState(cfg).owedSlots || {}, {});
  } finally {
    boot.stop();
  }
});

test('判据自己抛错：按便宜处理，照跑，不算注入失败', async () => {
  const dir = tmpDir();
  const cfg = makeCfg(dir, [{ hour: new Date().getHours(), minute: new Date().getMinutes() }]);
  cfg.priceClockFile = writeFakeClock(dir, { throws: true });
  const key = dyingOwnerState(cfg);

  const boot = bootSleep(cfg);
  try {
    const st = await waitFor(() => {
      const cur = readState(cfg);
      return cur.slots?.[key] ? cur : undefined;
    });
    assert.ok(st, '判据抛错也得跑（fail-open）');
    const logText = readLog(cfg);
    assert.match(logText, /\[sleep\] 价档判据已接 dsh-price-clock/, '加载成功就记成功');
    assert.doesNotMatch(logText, /判据注入失败/);
    assert.doesNotMatch(logText, /\[sleep\] 贵时段，槽 /);
  } finally {
    boot.stop();
  }
});

test('注入点没调（老路径）：行为与改动前逐字一致，一行注入日志都不许有', async () => {
  const dir = tmpDir();
  const cfg = makeCfg(dir, [{ hour: new Date().getHours(), minute: new Date().getMinutes() }]);
  cfg.sleepPriceProbe = false; // 排障口子：等于没接判据
  const key = dyingOwnerState(cfg);

  const boot = bootSleep(cfg);
  try {
    const st = await waitFor(() => {
      const cur = readState(cfg);
      return cur.slots?.[key] ? cur : undefined;
    });
    assert.ok(st, '老路径照常当场跑');
    const logText = readLog(cfg);
    assert.doesNotMatch(logText, /价档判据/, '没调注入就不许打注入日志');
    assert.doesNotMatch(logText, /\[sleep\] 贵时段，槽 /);
    assert.deepEqual(readState(cfg).owedSlots || {}, {}, '老路径不写 owedSlots');
    assert.match(logText, /接管上一实例的轮/, '接管日志跟改动前一致');
  } finally {
    boot.stop();
  }
});
