import assert from 'node:assert/strict';
import test from 'node:test';
import { pickDueSleepSlot } from '../lib/sleep.js';
import {
  SLEEP_EVENT_DEF,
  SLEEP_TOPIC,
  attachServiceWatch,
  createSleepDriver,
  cronOf,
  minGapMsOf,
  normalizeSlots,
  slotFromPeriodKey,
  slotIdOf,
} from '../lib/sleep-schedule.js';

const TOPIC = 'memory.sleep.tick';

/** 假 events/schedule 服务：注册、订阅、列表都记账，失败点由 failOn 决定。 */
function makeService({ failOn, subscribeFails = false } = {}) {
  const rules = new Map();
  const registry = new Map();
  const calls = { register: [], unregister: [], subscribe: [], stopped: 0, defined: [] };
  const service = {
    registry: { get: (name) => registry.get(name) },
    defineEvent(def) {
      calls.defined.push(def.name);
      registry.set(def.name, def);
    },
    subscribe(topic, cb, opts) {
      if (subscribeFails) throw new Error('订阅炸了');
      calls.subscribe.push({ topic, opts });
      const sub = {
        stop() {
          calls.stopped += 1;
        },
      };
      service._cb = cb;
      return sub;
    },
    register(def) {
      calls.register.push(def);
      if (failOn) failOn(def);
      rules.set(def.id, { ...def });
      return { id: def.id, pending: false };
    },
    unregister(id) {
      calls.unregister.push(id);
      return rules.delete(id);
    },
    list() {
      return [...rules.values()].map((rule) => ({ ...rule }));
    },
    emit(periodKey) {
      service._cb?.({ payload: { name: TOPIC, firedAt: new Date().toISOString(), periodKey } });
    },
  };
  return { service, rules, calls };
}

/** 假时钟：setInterval 只记账，由测试手动 fireTimers 推进。 */
function makeClock(start = '2026-11-09T04:31:00+08:00') {
  let ms = Date.parse(start);
  const timers = [];
  return {
    now: () => new Date(ms),
    setNow(value) {
      ms = typeof value === 'number' ? value : Date.parse(value);
    },
    advance(delta) {
      ms += delta;
    },
    setInterval(fn, everyMs) {
      const handle = {
        fn,
        everyMs,
        cleared: false,
        unref() {
          return this;
        },
      };
      timers.push(handle);
      return handle;
    },
    clearInterval(handle) {
      if (handle) handle.cleared = true;
    },
    timers,
    alive() {
      return timers.filter((t) => !t.cleared);
    },
    fireTimers() {
      for (const t of timers) if (!t.cleared) t.fn();
    },
  };
}

function makeCtx() {
  const services = new Map();
  const listeners = new Map();
  return {
    get(name) {
      return services.get(name);
    },
    set(name, value) {
      services.set(name, value);
    },
    on(event, fn) {
      if (!listeners.has(event)) listeners.set(event, []);
      listeners.get(event).push(fn);
      return () => {
        const list = listeners.get(event) || [];
        const at = list.indexOf(fn);
        if (at >= 0) list.splice(at, 1);
      };
    },
    emit(event, ...args) {
      for (const fn of listeners.get(event) || []) fn(...args);
    },
  };
}

function build({ slots = [{ hour: 5, minute: 0 }, { hour: 12, minute: 30 }], service, events, ctx, clock } = {}) {
  const logs = { info: [], warn: [] };
  const runs = [];
  const driver = createSleepDriver({
    slots: () => slots,
    events: () => events,
    schedule: () => service,
    run: (meta) => runs.push(meta),
    checkMs: 60 * 1000,
    clock: clock ?? makeClock(),
    info: (line) => logs.info.push(line),
    warn: (line) => logs.warn.push(line),
  });
  return { driver, runs, logs, ctx: ctx ?? makeCtx(), clock: clock ?? makeClock() };
}

test('多时段映射成多条 cron 规则：id/topic/catchUp 全对，且注册齐了才停自己的定时器', () => {
  const { service, rules, calls } = makeService();
  const clock = makeClock();
  const { driver, logs } = build({ service: service, events: service, clock });
  driver.start();

  assert.equal(rules.size, 2, '两个时段就是两条规则');
  assert.deepEqual(
    calls.register.map((r) => [r.id, r.cron]),
    [
      [slotIdOf({ hour: 5, minute: 0 }), '0 5 * * *'],
      [slotIdOf({ hour: 12, minute: 30 }), '30 12 * * *'],
    ],
  );
  for (const rule of calls.register) {
    assert.equal(rule.topic, TOPIC);
    assert.equal(rule.catchUp, 'once', 'dsh-timer 只认 once/skip；once 让过点启动当天补一次');
    assert.equal(rule.every, undefined, '用 cron 不是 every');
  }
  // 先 defineEvent 再 subscribe
  assert.deepEqual(calls.defined, [SLEEP_TOPIC]);
  assert.equal(calls.subscribe.length, 1);
  assert.equal(calls.subscribe[0].opts.subscriberId, 'dsh-memory.sleep');
  assert.deepEqual(calls.subscribe[0].opts.replay, { mode: 'none' });

  assert.equal(driver.mode(), 'tick');
  assert.equal(clock.alive().length, 1, '只剩 watchdog，原轮询定时器已停');
  assert.match(logs.info.join('\n'), /memory\.sleep\.0500、memory\.sleep\.1230/);
});

test('重复 reconcile 不重注册、不算 DUP：同一 id 已存在就复用', () => {
  const { service, calls } = makeService();
  const { driver } = build({ service: service, events: service });
  driver.start();
  driver.reconcile();
  driver.reconcile();
  assert.equal(calls.register.length, 2, '只注册了初始那两条');
  assert.equal(driver.mode(), 'tick');
});

test('单条失败整体回落：已注册的撤干净，回原生定时器，冷却期不重试', () => {
  const { service, rules, calls } = makeService({
    failOn(def) {
      if (def.id === 'memory.sleep.1230') {
        const error = new Error('EV-006 定时规则引用了未注册的 topic');
        error.code = 'EV-006';
        throw error;
      }
    },
  });
  const clock = makeClock();
  const { driver, runs, logs } = build({ service: service, events: service, clock });
  driver.start();

  assert.equal(driver.mode(), 'interval', '有一条失败就不许进 tick 模式');
  assert.equal(rules.size, 0, '半套规则也不许留');
  assert.ok(calls.unregister.includes('memory.sleep.0500'), '先注册成功的那条要回滚注销');
  assert.match(logs.warn.join('\n'), /EV-006/);
  assert.equal(clock.alive().length, 1, '原生定时器还活着');
  assert.equal(clock.alive()[0].everyMs, 60 * 1000);

  // 冷却期内再 reconcile 不撞同一堵墙
  const before = calls.register.length;
  driver.reconcile();
  assert.equal(calls.register.length, before, '冷却期内不再注册');

  // start() 启动补判已经叫过一次（source=interval），定时器这一拍再来一次
  assert.equal(runs.length, 1, 'start() 启动补判一次');
  assert.equal(runs[0].source, 'interval');
  clock.fireTimers();
  assert.equal(runs.length, 2);
});

test('EV-006 的 code 记进 warn，且不进 tick 模式', () => {
  const { service } = makeService({
    failOn() {
      const error = new Error('没有注册这个 topic');
      error.code = 'EV-006';
      throw error;
    },
  });
  const { driver, logs } = build({ service: service, events: service });
  driver.start();
  assert.equal(driver.mode(), 'interval');
  assert.match(logs.warn.join('\n'), /EV-006/);
});

test('schedule 服务缺失：保持原生定时器，服务出现后下一拍就切过去并整套重挂', () => {
  const ctx = makeCtx();
  const clock = makeClock();
  const { service, rules } = makeService();
  ctx.set('events', service);
  const runs = [];
  const logs = { info: [], warn: [] };
  const driver = createSleepDriver({
    slots: () => [{ hour: 5, minute: 0 }, { hour: 12, minute: 30 }],
    events: () => ctx.get('events'),
    schedule: () => ctx.get('schedule'),
    run: (meta) => runs.push(meta),
    checkMs: 60 * 1000,
    clock,
    info: (line) => logs.info.push(line),
    warn: (line) => logs.warn.push(line),
  });
  driver.start();
  assert.equal(driver.mode(), 'interval');
  assert.match(logs.warn.join('\n'), /schedule 服务尚未就绪/);
  assert.equal(rules.size, 0);

  // 服务中途出现：不用等 internal/service 事件，轮询每一拍都会探一遍
  ctx.set('schedule', service);
  clock.fireTimers();
  assert.equal(driver.mode(), 'tick');
  assert.equal(rules.size, 2);
  driver.dispose();
});

test('internal/service 销毁：清标记并当场回落原生定时器', () => {
  const ctx = makeCtx();
  const clock = makeClock();
  const { service } = makeService();
  ctx.set('events', service);
  ctx.set('schedule', service);
  const runs = [];
  const warns = [];
  const driver = createSleepDriver({
    slots: () => [{ hour: 5, minute: 0 }],
    events: () => ctx.get('events'),
    schedule: () => ctx.get('schedule'),
    run: (meta) => runs.push(meta),
    clock,
    warn: (line) => warns.push(line),
    info: () => {},
  });
  const off = attachServiceWatch(ctx, driver, { warn: (line) => warns.push(line) });
  driver.start();
  assert.equal(driver.mode(), 'tick');

  ctx.set('schedule', undefined);
  ctx.emit('internal/service', 'schedule', undefined);
  assert.equal(driver.mode(), 'interval');
  assert.match(warns.join('\n'), /schedule 服务被销毁/);
  assert.equal(clock.alive().length, 1, '原生定时器回来了');
  assert.equal(runs.length, 1, 'start() 启动补判一次');
  clock.fireTimers();
  assert.equal(runs.length, 2, '回落后的定时器拍也能叫醒');
  off();
});

test('监听器抛错不冒出去（只 warn）', () => {
  const ctx = makeCtx();
  const warns = [];
  const driver = {
    noteServiceEvent() {
      throw new Error('炸了');
    },
    reconcile() {},
  };
  const off = attachServiceWatch(ctx, driver, { warn: (line) => warns.push(line) });
  ctx.emit('internal/service', 'schedule', {});
  assert.match(warns.join('\n'), /internal\/service 监听处理失败/);
  off();
});

test('tick 断流：3 个最短周期没来就退订、撤规则、回落定时器并 warn 一次', () => {
  const { service, rules, calls } = makeService();
  const clock = makeClock('2026-11-09T05:00:00+08:00');
  const { driver, runs, logs } = build({ service: service, events: service, clock });
  driver.start();
  assert.equal(driver.mode(), 'tick');
  // 05:00 与 12:30 的最短间隔是 7.5 小时，阈值 22.5 小时
  assert.equal(driver.status().stallMs, 7.5 * 60 * 60 * 1000 * 3);

  clock.advance(7.5 * 60 * 60 * 1000 + 60 * 1000);
  clock.fireTimers();
  assert.equal(driver.mode(), 'tick', '7 小时 31 分还没到阈值');

  clock.advance(16 * 60 * 60 * 1000);
  clock.fireTimers();
  assert.equal(driver.mode(), 'interval');
  assert.match(logs.warn.join('\n'), /断流/);
  assert.equal(calls.stopped, 1, '退订');
  assert.equal(rules.size, 0, '规则撤干净');

  // 回落后的定时器叫醒一次
  clock.fireTimers();
  assert.ok(runs.some((r) => r.source === 'interval'));
});

test('同 periodKey 重复 tick 幂等：done 里有 key 就不再跑，业务判定只命中一次', () => {
  const now = new Date('2026-11-09T12:30:10+08:00');
  const slots = [{ hour: 5, minute: 0 }, { hour: 12, minute: 30 }];
  // 05:00 那拍今天已经跑过，所以 12:30 的 tick 只该命中 12:30
  const done = { '2026-11-09 05:00': 1 };
  const first = pickDueSleepSlot({ slots, done, now, lastRunAt: 0 });
  assert.equal(first, '2026-11-09 12:30');
  // 跑完 markSlot 之后，同一拍再来一次（补发、重复 tick）不许再跑
  done[first] = Date.now();
  const second = pickDueSleepSlot({ slots, done, now, lastRunAt: 0 });
  assert.equal(second, undefined);
});

test('中午档边界：12:29 不跑、12:30 跑、12:31 已跑过不再跑', () => {
  const slots = [{ hour: 5, minute: 0 }, { hour: 12, minute: 30 }];
  const done = { '2026-11-09 05:00': 1 };
  const at = (hms) => new Date(`2026-11-09T${hms}+08:00`);

  assert.equal(pickDueSleepSlot({ slots, done, now: at('12:29:59'), lastRunAt: 0 }), undefined, '12:29 不跑');
  const key = pickDueSleepSlot({ slots, done, now: at('12:30:00'), lastRunAt: 0 });
  assert.equal(key, '2026-11-09 12:30', '12:30 到点就跑');
  // 跑完 markSlot 之后，12:31 的补发 tick / 重挂规则不许再跑一次
  done[key] = Date.now();
  assert.equal(pickDueSleepSlot({ slots, done, now: at('12:31:00'), lastRunAt: 0 }), undefined, '12:31 已跑过不再跑');
});

test('时段从 13:00 提前到 12:30：当天老 key「… 13:00」要认，不许同一天跑两遍', () => {
  const slots = [{ hour: 5, minute: 0 }, { hour: 12, minute: 30 }];
  // 部署当天 13:00 那轮刚跑完，sleep.state.json 里只有老 key
  const done = { '2026-11-09 05:00': 1, '2026-11-09 13:00': 1 };
  assert.equal(
    pickDueSleepSlot({ slots, done, now: new Date('2026-11-09T15:10:00+08:00'), lastRunAt: 0 }),
    undefined,
    '老 key 认成 12:30 已跑',
  );
  // 前一天的老 key 不挡今天
  assert.equal(
    pickDueSleepSlot({
      slots,
      done: { '2026-11-09 05:00': 1, '2026-11-08 13:00': 1 },
      now: new Date('2026-11-09T12:30:30+08:00'),
      lastRunAt: 0,
    }),
    '2026-11-09 12:30',
  );
  // 比老时刻更晚的新时段不被老 key 挡住
  assert.equal(
    pickDueSleepSlot({
      slots: [{ hour: 22, minute: 0 }],
      done: { '2026-11-09 13:00': 1 },
      now: new Date('2026-11-09T22:00:30+08:00'),
      lastRunAt: 0,
    }),
    '2026-11-09 22:00',
  );
});

test('当天已跑过 / 没到点：不跑；minGap 删了，到点且有未跑档就直接判 due', () => {
  const slots = [{ hour: 5, minute: 0 }, { hour: 12, minute: 30 }];
  // 当天 05:00 已经跑过，12:30 还没到
  assert.equal(
    pickDueSleepSlot({
      slots,
      done: { '2026-11-09 05:00': 1 },
      now: new Date('2026-11-09T12:29:00+08:00'),
    }),
    undefined,
  );
  // 2026-11-14：删掉「距上轮 10 分钟」的去抖，到点且 12:30 没跑过就判 due。
  // 防叠跑靠 slots 幂等（下面第一段）+ inFlight（scheduledRun 里）+ 心跳锁。
  assert.equal(
    pickDueSleepSlot({
      slots,
      done: { '2026-11-09 05:00': 1 },
      now: new Date('2026-11-09T12:30:10+08:00'),
    }),
    '2026-11-09 12:30',
  );
  // 05:00 那拍到点、没跑过 → 跑
  assert.equal(
    pickDueSleepSlot({ slots, done: {}, now: new Date('2026-11-09T05:00:30+08:00') }),
    '2026-11-09 05:00',
  );
});

test('tick payload 的 periodKey 能反解回时段，handler 知道是哪一拍', () => {
  const slots = [{ hour: 5, minute: 0 }, { hour: 12, minute: 30 }];
  assert.deepEqual(slotFromPeriodKey('memory.sleep.1230.daily.2026-11-09', slots), { hour: 12, minute: 30 });
  assert.equal(slotFromPeriodKey('memory.sleep.9999.daily.2026-11-09', slots), undefined);
  assert.equal(slotFromPeriodKey('', slots), undefined);
});

test('真实 tick 只带 name/firedAt/periodKey，schema 卡住畸形 payload', () => {
  assert.equal(SLEEP_EVENT_DEF.name, TOPIC);
  assert.equal(SLEEP_EVENT_DEF.version, 1);
  assert.equal(SLEEP_EVENT_DEF.durability, 'volatile');
  assert.equal(
    SLEEP_EVENT_DEF.payloadSchema({ name: TOPIC, firedAt: '2026-11-09T05:00:00.000Z', periodKey: 'memory.sleep.0500.daily.2026-11-09' }),
    true,
  );
  assert.equal(SLEEP_EVENT_DEF.payloadSchema({ name: TOPIC, firedAt: 'x' }), false);
});

test('cron 映射与最短周期：单时段一天，多时段取环形最短间隔', () => {
  assert.equal(cronOf({ hour: 5, minute: 0 }), '0 5 * * *');
  assert.equal(cronOf({ hour: 12, minute: 30 }), '30 12 * * *');
  assert.equal(minGapMsOf([{ hour: 5, minute: 0 }]), 24 * 60 * 60 * 1000);
  assert.equal(minGapMsOf([{ hour: 5, minute: 0 }, { hour: 12, minute: 30 }]), 7.5 * 60 * 60 * 1000);
  assert.equal(
    minGapMsOf([{ hour: 23, minute: 0 }, { hour: 1, minute: 0 }, { hour: 12, minute: 0 }]),
    2 * 60 * 60 * 1000,
  );
  // 同 HH:MM 去重，非法项丢掉
  assert.deepEqual(normalizeSlots([{ hour: 5, minute: 0 }, { hour: 5, minute: 0 }, null]), [{ hour: 5, minute: 0 }]);
});

test('订阅失败就不装规则，仍走原生定时器', () => {
  const { service, rules } = makeService({ subscribeFails: true });
  const { driver, logs } = build({ service: service, events: service });
  driver.start();
  assert.equal(driver.mode(), 'interval');
  assert.equal(rules.size, 0);
  assert.match(logs.warn.join('\n'), /订阅/);
});

test('schedule 换实例：规则在新实例上重挂，旧实例的规则不重复算', () => {
  const first = makeService();
  const second = makeService();
  const clock = makeClock();
  const runs = [];
  const warns = [];
  let current = first.service;
  let events = first.service;
  const driver = createSleepDriver({
    slots: () => [{ hour: 5, minute: 0 }],
    events: () => events,
    schedule: () => current,
    run: (meta) => runs.push(meta),
    clock,
    info: () => {},
    warn: (line) => warns.push(line),
  });
  driver.start();
  assert.equal(first.rules.size, 1);
  assert.equal(driver.mode(), 'tick');

  current = second.service;
  events = second.service;
  driver.reconcile();
  assert.equal(second.rules.size, 1, '新实例上重挂');
  assert.equal(driver.mode(), 'tick');

  // start() 启动补判先叫了一次，新实例来的 tick 还能再叫醒业务
  assert.equal(runs.length, 1);
  assert.equal(runs[0].source, 'interval');
  second.service.emit('memory.sleep.0500.daily.2026-11-09');
  assert.equal(runs.length, 2);
  assert.equal(runs[1].source, 'tick');
  assert.ok(warns.length === 0);
});

test('driver 串真实判定：两个时段各跑一次，同日同一拍补发 tick 不再跑', () => {
  const slots = [{ hour: 5, minute: 0 }, { hour: 12, minute: 30 }];
  const done = {};
  const runs = [];
  const clock = makeClock('2026-11-09T05:00:05+08:00');
  const { service } = makeService();
  const driver = createSleepDriver({
    slots: () => slots,
    events: () => service,
    schedule: () => service,
    clock,
    info: () => {},
    warn: () => {},
    run: () => {
      const key = pickDueSleepSlot({ slots, done, now: clock.now(), lastRunAt: 0 });
      if (!key) return;
      done[key] = Date.now();
      runs.push(key);
    },
  });
  driver.start();
  service.emit('memory.sleep.0500.daily.2026-11-09');
  // dsh-timer 重试 / 规则重挂都可能把同一拍再发一次
  service.emit('memory.sleep.0500.daily.2026-11-09');
  clock.setNow('2026-11-09T12:30:05+08:00');
  service.emit('memory.sleep.1230.daily.2026-11-09');
  assert.deepEqual(runs, ['2026-11-09 05:00', '2026-11-09 12:30'], '一早一晚各一次，补发那拍被 done 挡住');
  driver.dispose();
});

test('过点启动立刻补判一次：不等 60 秒定时器，也不等 dsh-timer 补发 tick', () => {
  const clock = makeClock('2026-11-09T12:35:00+08:00');
  const { service } = makeService();
  const runs = [];
  const driver = createSleepDriver({
    slots: () => [{ hour: 5, minute: 0 }, { hour: 12, minute: 30 }],
    events: () => service,
    schedule: () => service,
    clock,
    run: (meta) => runs.push(meta),
    info: () => {},
    warn: () => {},
  });
  driver.start();
  assert.equal(runs.length, 1, 'start() 里当场 fire 一次');
  assert.equal(runs[0].source, 'interval');
  driver.dispose();
});

test('同一 slot 第二次 start 不补跑：done 里已标，静默不动', () => {
  const slots = [{ hour: 5, minute: 0 }, { hour: 12, minute: 30 }];
  const done = {};
  const runs = [];
  const clock = makeClock('2026-11-09T12:35:00+08:00');
  const { service } = makeService();
  const mk = () =>
    createSleepDriver({
      slots: () => slots,
      events: () => service,
      schedule: () => service,
      clock,
      info: () => {},
      warn: () => {},
      run: () => {
        const key = pickDueSleepSlot({ slots, done, now: clock.now(), lastRunAt: 0 });
        if (!key) return;
        done[key] = Date.now();
        runs.push(key);
      },
    });
  const first = mk();
  first.start();
  assert.deepEqual(runs, ['2026-11-09 05:00'], '过点启动补最早的未跑档');
  first.dispose();
  // 第二次 start 时 05:00 已标过：不许重复跑它，只补另一档 12:30
  const second = mk();
  second.start();
  assert.deepEqual(runs, ['2026-11-09 05:00', '2026-11-09 12:30'], '已跑过的 05:00 不重复，只补未跑的 12:30');
  second.dispose();
  // 两档都标了以后，再来一次 start 静默
  const third = mk();
  third.start();
  assert.equal(runs.length, 2, '两档都跑过之后 start 不补跑');
  third.dispose();
});
