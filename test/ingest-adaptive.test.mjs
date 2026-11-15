/**
 * ingest 并发负载自适应：
 * 64 只是理论上限（cap），实际并发必须按树莓派当时的负载决定，机器忙或热时不能无脑开 64。
 * 这里钉六件事：
 *   1) 低负载（4 核、load1=2.18、内存足、45℃）→ 接近理论上限 64；
 *   2) 负载越高系数越低：util=1.0 → 32、util=2.0 → 16、util=3.0 → 6；
 *   3) 内存紧张（MemAvailable=1.2GB）→ 按内存项压低；
 *   4) 78℃ 对折、82℃ → 1；
 *   5) organizeIngestAdaptive=false → 恒用 organizeIngestConcurrency；
 *   6) 上限 64 不被突破。
 * 算法是纯函数，喂假快照，不依赖本机当时忙不忙。
 */
import assert from 'node:assert/strict';
import test from 'node:test';
import {
  adaptIngestConcurrency,
  formatIngestAdaptiveLog,
  ingestConcurrencyOf,
  ingestLoadSnapshot,
  selectIngestConcurrency,
} from '../lib/sleep.js';
import { Config } from '../lib/index.js';

const GB = 1024 ** 3;

test('低负载：4 核、load1=2.18、内存足、45℃ → 接近理论上限 64', () => {
  const pick = adaptIngestConcurrency({
    cap: 64, load1: 2.18, cores: 4, memAvailableBytes: 8 * GB, tempC: 45,
  });
  assert.equal(pick.basis, 'cpu');
  assert.ok(pick.conc >= 58 && pick.conc <= 64, `实得 ${pick.conc} 路，该接近 64`);
  assert.equal(pick.conc, 61, 'util=0.545 → 系数 0.955 → 64×0.955=61.12 → 61');
});

test('CPU 系数曲线：util 0.5/1/2/3 → 64/32/16/6', () => {
  const mk = (load1) => adaptIngestConcurrency({
    cap: 64, load1, cores: 4, memAvailableBytes: 8 * GB, tempC: 45,
  }).conc;
  assert.equal(mk(2), 64, 'util=0.5，系数 1.0，满 cap');
  assert.equal(mk(4), 32, 'util=1.0，系数 0.5');
  assert.equal(mk(8), 16, 'util=2.0，系数 0.25');
  assert.equal(mk(12), 6, 'util=3.0，系数 0.1，64×0.1=6.4 → 6');
});

test('高负载：load1 = cores×3 → 压到 6，绝不为 0', () => {
  const pick = adaptIngestConcurrency({
    cap: 64, load1: 12, cores: 4, memAvailableBytes: 8 * GB, tempC: 45,
  });
  assert.equal(pick.conc, 6);
  assert.equal(pick.basis, 'cpu');
});

test('内存紧张：MemAvailable=1.2GB → 按内存项压低到 2', () => {
  const pick = adaptIngestConcurrency({
    cap: 64, load1: 1, cores: 4, memAvailableBytes: 1.2 * GB, tempC: 45,
  });
  assert.equal(pick.conc, 2, '(1.2GB − 1GB) / 100MB = 2');
  assert.equal(pick.basis, 'mem');
});

test('内存连 1GB 保留都不够 → 兜到 1，不出 0', () => {
  const pick = adaptIngestConcurrency({
    cap: 64, load1: 0, cores: 4, memAvailableBytes: 0.5 * GB, tempC: 45,
  });
  assert.equal(pick.conc, 1);
  assert.equal(pick.basis, 'mem');
});

test('温度 78℃ 对折、82℃ 压到 1', () => {
  const base = { cap: 64, load1: 0, cores: 4, memAvailableBytes: 8 * GB };
  assert.equal(adaptIngestConcurrency({ ...base, tempC: 45 }).conc, 64, '常温先满 cap');
  const warm = adaptIngestConcurrency({ ...base, tempC: 78 });
  assert.equal(warm.conc, 32, '78℃ 对折');
  assert.equal(warm.basis, 'temp75');
  const hot = adaptIngestConcurrency({ ...base, tempC: 82 });
  assert.equal(hot.conc, 1, '82℃ 直接 1 路');
  assert.equal(hot.basis, 'temp80');
});

test('organizeIngestAdaptive=false：机器再忙再热也恒用配置值', () => {
  const hot = { load1: 99, cores: 2, memAvailableBytes: 256 * 1024 * 1024, tempC: 90 };
  const off = selectIngestConcurrency({ organizeIngestConcurrency: 8, organizeIngestAdaptive: false }, hot);
  assert.equal(off.conc, 8);
  assert.equal(off.adaptive, false);
  const on = selectIngestConcurrency({ organizeIngestConcurrency: 8, organizeIngestAdaptive: true }, hot);
  assert.equal(on.conc, 1, '同一个快照下自适应会压到 1');
  assert.equal(on.adaptive, true);
});

test('上限 64 不被突破：配置 999、128 核也只到 64', () => {
  assert.equal(ingestConcurrencyOf({ organizeIngestConcurrency: 999 }), 64);
  const pick = selectIngestConcurrency({ organizeIngestConcurrency: 999 }, {
    load1: 0, cores: 128, memAvailableBytes: 256 * GB, tempC: 45,
  });
  assert.equal(pick.conc, 64);
  assert.equal(pick.cap, 64);
  assert.equal(
    adaptIngestConcurrency({ cap: 999, load1: 0, cores: 128, memAvailableBytes: 256 * GB, tempC: 45 }).conc,
    64,
  );
});

test('Config 默认：adaptive=true、上限 64', () => {
  const cfg = Config({});
  assert.equal(cfg.organizeIngestAdaptive, true, '默认开着浮动');
  assert.equal(cfg.organizeIngestConcurrency, 64, '默认的理论上限仍是 64');
});

test('本机快照可读，日志行按票面格式', () => {
  const snap = ingestLoadSnapshot();
  assert.equal(typeof snap, 'object');
  assert.equal(typeof snap.cores, 'number');
  const pick = selectIngestConcurrency({ organizeIngestConcurrency: 64 }, {
    load1: 2.18, cores: 4, memAvailableBytes: 8 * GB, tempC: 50.5,
  });
  assert.equal(pick.conc, 61, '4 核 load1=2.18 → 61 路');
  const line = formatIngestAdaptiveLog(pick);
  assert.match(
    line,
    /^\[ingest\] 并发自适应 取 61 路（上限 64，load1=2\.18 cores=4 mem_avail=8\.0GB temp=50\.5C 依据=cpu）$/,
  );
});
