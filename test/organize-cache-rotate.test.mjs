// 2026-11-09：organizeCache 轮转 + 按月归档。
// 纯模块级测试：不碰真库、不碰真 sleep.state.json，全部走临时目录。
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import zlib from 'node:zlib';
import {
  DEFAULT_CACHE_MAX, archiveDirOf, archiveFileOf, listArchiveMonths, readArchiveRows,
  trimOrganizeCache,
} from '../lib/organize-cache.js';
import { readState } from '../lib/sleep.js';

function tmpCfg(extra = {}) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-organize-cache-'));
  return {
    dir,
    cfg: {
      stateFile: path.join(dir, 'sleep.state.json'),
      organizeCacheMax: DEFAULT_CACHE_MAX,
      organizeArchiveDir: path.join(dir, 'organize-archive'),
      ...extra,
    },
  };
}

function keyOf(i) {
  return crypto.createHash('sha1').update(`k${i}`).digest('hex').slice(0, 16);
}

function makeCache(count, { start = Date.UTC(2026, 8, 1, 0, 0, 0), step = 60000, base = 0 } = {}) {
  const cache = {};
  for (let i = 1; i <= count; i += 1) {
    cache[keyOf(i + base)] = { v: i % 2 ? 'acted' : 'noop', t: start + (i - 1) * step };
  }
  return cache;
}

function archiveRows(cfg) {
  const rows = [];
  for (const f of listArchiveMonths(cfg.organizeArchiveDir)) {
    if (f.error) throw new Error(`${f.name} 解压失败：${f.error}`);
    rows.push(...readArchiveRows(f.file));
  }
  return rows;
}

test('超阈值：只留最新 200 条，最旧的溢出去归档', () => {
  const { cfg } = tmpCfg();
  const cache = makeCache(250);
  const v = trimOrganizeCache(cfg, cache);
  assert.equal(v.failed, false);
  assert.equal(v.before, 250);
  assert.equal(Object.keys(v.kept).length, 200);
  assert.equal(v.spilled, 50);
  assert.equal(v.written, 50);
  // 最新的 50 条中，第 1~50 条（最旧）被搬走，51~250 留下
  assert.equal(cache[keyOf(1)] !== undefined, true);
  assert.equal(v.kept[keyOf(1)], undefined);
  assert.notEqual(v.kept[keyOf(51)], undefined);
  assert.notEqual(v.kept[keyOf(250)], undefined);
});

test('未超阈值：原样返回，不建目录、不写文件', () => {
  const { cfg } = tmpCfg();
  const cache = makeCache(200);
  const v = trimOrganizeCache(cfg, cache);
  assert.equal(v.spilled, 0);
  assert.equal(v.kept, cache);
  assert.equal(fs.existsSync(cfg.organizeArchiveDir), false);
});

test('跨月分卷：按写入时间归属月份，各进各的 gzip', () => {
  const { cfg } = tmpCfg({ organizeCacheMax: 1 });
  const sep = Date.UTC(2026, 8, 10, 12, 0, 0);
  const oct = Date.UTC(2026, 9, 2, 3, 0, 0);
  const cache = {
    [keyOf(1)]: { v: 'acted', t: sep },
    [keyOf(2)]: { v: 'noop', t: sep + 1000 },
    [keyOf(3)]: { v: 'acted', t: oct },
    [keyOf(4)]: { v: 'noop', t: oct + 1000 },
  };
  const v = trimOrganizeCache(cfg, cache, { now: Date.UTC(2026, 10, 9, 14, 0, 0) });
  assert.equal(v.spilled, 3);
  assert.equal(Object.keys(v.kept).length, 1);
  assert.equal(v.kept[keyOf(4)]?.v, 'noop');
  const months = listArchiveMonths(cfg.organizeArchiveDir).map((f) => f.name).sort();
  // 2026-09 归 2 条，2026-10 归 1 条（本地时区：用的 UTC 时间戳，请按本地月份断言）
  const localMonth = (t) => {
    const d = new Date(t);
    return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}`;
  };
  assert.deepEqual(months, [
    `organize-${localMonth(sep)}.jsonl.gz`,
    `organize-${localMonth(oct)}.jsonl.gz`,
  ].sort());
});

test('gzip 内容可解可读：一行一条 {key, value, archivedAt}', () => {
  const { cfg } = tmpCfg({ organizeCacheMax: 1 });
  const cache = makeCache(3);
  const now = Date.UTC(2026, 10, 9, 14, 30, 0);
  trimOrganizeCache(cfg, cache, { now });
  const rows = archiveRows(cfg);
  assert.equal(rows.length, 2);
  for (const row of rows) {
    assert.equal(typeof row.key, 'string');
    assert.equal(row.key.length, 16);
    assert.equal(typeof row.value?.t, 'number');
    assert.ok(row.value.v === 'acted' || row.value.v === 'noop');
    assert.equal(row.archivedAt, new Date(now).toISOString());
  }
  // 文件确实是 gzip（魔数 1f 8b），且能一次 gunzip 出全部行
  const files = listArchiveMonths(cfg.organizeArchiveDir);
  const buf = fs.readFileSync(files[0].file);
  assert.equal(buf[0], 0x1f);
  assert.equal(buf[1], 0x8b);
  assert.equal(zlib.gunzipSync(buf).toString('utf8').trim().split('\n').length, files[0].count);
});

test('归档条数守恒：归档 + 主文件 = 迁移前总数', () => {
  const { cfg } = tmpCfg();
  const cache = makeCache(255);
  const before = Object.keys(cache).length;
  const v = trimOrganizeCache(cfg, cache);
  const rows = archiveRows(cfg);
  assert.equal(Object.keys(v.kept).length + rows.length, before);
  assert.equal(new Set(rows.map((r) => r.key)).size, rows.length);
});

test('目录不可写：不丢条目，只 warn，返回原 cache', () => {
  const { dir, cfg } = tmpCfg();
  const blocker = path.join(dir, 'not-a-dir');
  fs.writeFileSync(blocker, 'x');
  cfg.organizeArchiveDir = path.join(blocker, 'organize-archive');
  const cache = makeCache(220);
  const warns = [];
  const v = trimOrganizeCache(cfg, cache, { warn: (l) => warns.push(l) });
  assert.equal(v.failed, true);
  assert.match(String(v.error), /ENOTDIR|ENOENT|EACCES/);
  assert.equal(v.kept, cache);
  assert.equal(Object.keys(v.kept).length, 220);
  assert.equal(warns.length, 1);
  assert.match(warns[0], /主文件保持原样/);
});

test('重复迁移幂等：同一批 cache 跑两次，归档不重复写', () => {
  const { cfg } = tmpCfg();
  const cache = makeCache(230);
  const first = trimOrganizeCache(cfg, cache);
  const rowsAfterFirst = archiveRows(cfg).length;
  assert.equal(rowsAfterFirst, 30);
  // 主文件已只剩 200 条：再跑不再触发
  const second = trimOrganizeCache(cfg, first.kept);
  assert.equal(second.spilled, 0);
  assert.equal(archiveRows(cfg).length, rowsAfterFirst);
  // 最坏情况：有人把老条目又塞回主文件，重跑也不能重复归档
  const merged = { ...first.kept, ...cache };
  const third = trimOrganizeCache(cfg, merged);
  assert.equal(third.spilled, 30);
  assert.equal(third.written, 0, '已归档的 key 不重复写');
  assert.equal(archiveRows(cfg).length, rowsAfterFirst);
  assert.equal(Object.keys(third.kept).length, 200);
});

test('加载路径不读归档：归档里有坏文件也照常读主文件', () => {
  const { cfg } = tmpCfg();
  const cache = makeCache(3);
  fs.mkdirSync(cfg.organizeArchiveDir, { recursive: true });
  fs.writeFileSync(archiveFileOf(cfg.organizeArchiveDir, '2026-09'), 'this is not gzip');
  fs.writeFileSync(cfg.stateFile, JSON.stringify({ lastSleepAt: 1, organizeCache: cache }, null, 2));
  const state = readState(cfg);
  assert.equal(state.lastSleepAt, 1);
  assert.deepEqual(Object.keys(state.organizeCache).sort(), Object.keys(cache).sort());
  // 目录整个不可读也一样：readState 用不到它
  fs.rmSync(cfg.organizeArchiveDir, { recursive: true, force: true });
  assert.deepEqual(Object.keys(readState(cfg).organizeCache).sort(), Object.keys(cache).sort());
});

test('默认值：上限 200、归档目录跟 stateFile 同级', () => {
  assert.equal(DEFAULT_CACHE_MAX, 200);
  const cfg = { stateFile: '/x/y/sleep.state.json' };
  assert.equal(archiveDirOf(cfg), path.join('/x/y', 'organize-archive'));
  assert.equal(archiveDirOf({ ...cfg, organizeArchiveDir: '/z/arch' }), '/z/arch');
});

test('同月文件已存在：旧行原样保留，只追加新行（不覆盖、不删）', () => {
  const { cfg } = tmpCfg({ organizeCacheMax: 1 });
  const month = '2026-11';
  fs.mkdirSync(cfg.organizeArchiveDir, { recursive: true });
  const file = archiveFileOf(cfg.organizeArchiveDir, month);
  const oldRow = { key: keyOf(999), value: { v: 'acted', t: Date.UTC(2026, 10, 1, 0, 0, 0) }, archivedAt: '2026-11-01T00:00:00.000Z' };
  fs.writeFileSync(file, zlib.gzipSync(`${JSON.stringify(oldRow)}\n`));
  const cache = {
    [keyOf(1)]: { v: 'noop', t: Date.UTC(2026, 10, 5, 0, 0, 0) },
    [keyOf(2)]: { v: 'acted', t: Date.UTC(2026, 10, 6, 0, 0, 0) },
  };
  const v = trimOrganizeCache(cfg, cache);
  assert.equal(v.spilled, 1);
  const rows = readArchiveRows(file);
  assert.equal(rows.length, 2);
  assert.equal(rows[0].key, oldRow.key, '旧行必须原样在最前面');
  assert.equal(rows[1].key, keyOf(1));
  assert.equal(zlib.gunzipSync(fs.readFileSync(file)).toString('utf8').split('\n').filter(Boolean).length, 2);
});
