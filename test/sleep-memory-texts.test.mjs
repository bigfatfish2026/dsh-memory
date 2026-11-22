/**
 * 睡觉链的「现有记忆」候选筛选（memoryTexts，2026-11-20）。
 *
 * 以前按 token 预算把整库等距铺满：sleep:askModel 单次输入 81.5 万 token，listing 占 74 万，
 * 一轮 47 个文件还各拼一份。现在先筛后装，本文件钉住四条口径：
 *   1. 只带实体字面命中的条目，不再整库铺满（窗口未知也一样）；
 *   2. 候选空/不足时，只补最近 7 天的条目兜底，不碰更老的；
 *   3. 向量邻域：命中条目质心的近邻进候选，正交的不进，命中的排前面；
 *   4. 上限：条数上限 300，token 上限仍走真实窗口（预算 0 就交空表）。
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import assert from 'node:assert/strict';
import test from 'node:test';
import { createSleepEnv } from '../lib/sleep/env.js';
import { estimateTokens, itemTokensOf } from '../lib/token-window.js';

const TMP = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-memtexts-'));
const OVERHEAD = 24;
const DAY = 86400000;

function daysAgo(n) {
  return new Date(Date.now() - n * DAY).toISOString();
}

function view(id, { text = `条目 ${id}`, entities = [], ts = daysAgo(30), status = 'active' } = {}) {
  return {
    id, text, tags: [], type: 'world', confidence: null, entities,
    validFrom: '', validTo: '', ts, status, reason: '',
  };
}

function makeStore(list, vecOf = new Map()) {
  return {
    snapshot: () => ({ list, byId: new Map(list.map((m) => [m.id, m])), vecOf, version: 1 }),
    listAll: () => list,
    vectorIndex: () => vecOf,
    db: () => { throw new Error('memoryTexts 不该碰 db'); },
    typeOf: () => 'world',
    save: async () => ({}), update: async () => ({}),
    archive: async () => ({}), merge: async () => ({}),
  };
}

function makeEnv(list, { vecOf = new Map(), contextWindowTokens = 0 } = {}) {
  const cfg = {
    storePath: '', notifyScript: '', sessionsRoot: '', archiveDir: '', backupDir: '',
    sleepLogFile: path.join(TMP, `sleep-${Math.random().toString(16).slice(2)}.log`),
    contextWindowTokens,
  };
  return createSleepEnv({ get: () => null }, makeStore(list, vecOf), cfg);
}

test('不再整库铺满：窗口未知时也只带实体字面命中的条目', () => {
  const old = daysAgo(30);
  const list = [
    view('m1', { entities: ['树莓派'], ts: old }),
    view('m2', { entities: ['dsh-memory'], ts: old }),
  ];
  for (let i = 0; i < 500; i += 1) list.push(view(`n${i}`, { entities: ['无关实体'], ts: old }));
  const env = makeEnv(list);
  const got = env.memoryTexts('今天在树莓派上改 dsh-memory 的 memoryTexts');
  assert.deepEqual(got.map((m) => m.id), ['m1', 'm2'], '500 条无关条目一条都不该带进来');
});

test('实体命中用折叠比较：全角/大小写不同的写法也算命中', () => {
  const old = daysAgo(30);
  const list = [view('m1', { entities: ['ＤＳＨ-Ｍｅｍｏｒｙ'], ts: old })];
  const env = makeEnv(list);
  const got = env.memoryTexts('dsh-memory 的 memoryTexts 要改');
  assert.deepEqual(got.map((m) => m.id), ['m1']);
});

test('候选为空时补最近几天的条目兜底，7 天以前的不碰', () => {
  const list = [
    view('recent', { ts: daysAgo(2) }),
    view('mid', { ts: daysAgo(5) }),
    view('edge', { ts: daysAgo(8) }),
    view('old', { ts: daysAgo(30) }),
  ];
  const env = makeEnv(list);
  const got = env.memoryTexts('跟库里完全没有关系的一句话');
  assert.deepEqual(got.map((m) => m.id), ['recent', 'mid'], '只补最近 7 天，按 ts 倒序');
});

test('向量邻域：命中条目的近邻进候选，正交条目不进，命中的排前面', () => {
  const old = daysAgo(30);
  const list = [
    view('a', { entities: ['树莓派'], ts: old }),
    view('b', { ts: old }),
    view('c', { ts: old }),
  ];
  const vecOf = new Map([
    ['a', Float32Array.from([1, 0, 0])],
    ['b', Float32Array.from([0.9, 0.1, 0])],
    ['c', Float32Array.from([0, 1, 0])],
  ]);
  const env = makeEnv(list, { vecOf });
  const ids = env.memoryTexts('树莓派').map((m) => m.id);
  assert.equal(ids[0], 'a', '实体命中的排最前');
  assert.ok(ids.includes('b'), '余弦 0.99 的近邻要进候选');
  assert.ok(!ids.includes('c'), '正交条目（余弦 0）不算邻域');
});

test('条数上限 300：窗口未知时命中的 400 条也只带 300 条', () => {
  const old = daysAgo(30);
  const list = [];
  for (let i = 0; i < 400; i += 1) list.push(view(`m${i}`, { entities: ['树莓派'], ts: old }));
  const env = makeEnv(list);
  const got = env.memoryTexts('树莓派');
  assert.equal(got.length, 300);
});

test('token 上限仍走真实窗口：装不下就少带', () => {
  const transcript = '树莓派'.repeat(100);
  const old = daysAgo(30);
  const list = [];
  for (let i = 0; i < 20; i += 1) {
    list.push(view(`m${i}`, { entities: ['树莓派'], ts: old, text: '记'.repeat(400) }));
  }
  const env = makeEnv(list, { contextWindowTokens: 4000 });
  const got = env.memoryTexts(transcript);
  // 窗口 4000 − 输出预留 1000 − 外壳（记录 400 + SLEEP_PROMPT_SHELL_TOKENS 800）= 1800
  const budget = 4000 - 1000 - (estimateTokens(transcript) + 800);
  const used = got.reduce((n, m) => n + itemTokensOf(m, (x) => x.text, OVERHEAD), 0);
  assert.ok(got.length > 0 && got.length < list.length, `应少于 ${list.length} 条，实际 ${got.length}`);
  assert.ok(used <= budget, `装进去的 ${used} token 不该超预算 ${budget}`);
});

test('窗口已知但一条都装不下时交空表（不无视预算硬塞）', () => {
  const old = daysAgo(30);
  const list = [view('m1', { entities: ['树莓派'], ts: old })];
  const env = makeEnv(list, { contextWindowTokens: 1000 });
  assert.equal(env.memoryTexts('树莓派'.repeat(133)).length, 0);
});
