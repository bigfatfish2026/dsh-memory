#!/usr/bin/env node
/**
 * 检索日志冒烟测试（2026-11-01 加）。
 *
 * 这个仓库没有测试框架（package.json 里没有 test 脚本），所以用这个脚本代替：
 * 真跑一次 store.recall，检查
 *   1) 日志文件多了一行
 *   2) 那一行是合法 JSON
 *   3) query 字段跟传进去的对得上
 *   4) hits / k / caller / ms 都在，top 里的正文不超过 60 字 + 省略号
 * 日志写到临时目录，不碰真实日志目录。
 *
 * 用法：node scripts/smoke-recall-log.mjs
 */
import assert from 'node:assert';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { createStore } from '../lib/store.js';
import { resolveMemoryRoot } from './lib/db.mjs';

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-memory-recall-log-'));
const logFile = path.join(tmp, 'memory_recall.log');

const cfg = {
  storePath: resolveMemoryRoot(),
  apiUrl: process.env.DSH_EMBED_API || 'http://127.0.0.1:8080',
  timeoutMs: 15000,
  topK: 8,
  minScore: 0.45,
  relativeGap: 0.1,
  recallLogFile: logFile,
};

const query = '树莓派 温度';
const store = createStore(cfg);

assert.ok(!fs.existsSync(logFile), '测试开始前日志文件不该存在');

const items = await store.recall(query, 3, {
  caller: 'tool',
  session: 'smoke-session',
  origin: 'user',
  callId: 'call_smoke_1',
});

assert.ok(fs.existsSync(logFile), '调过 recall 之后日志文件应该被创建');
const lines = fs.readFileSync(logFile, 'utf8').split('\n').filter((l) => l.trim());
assert.strictEqual(lines.length, 1, `应该正好写一行，实际 ${lines.length} 行`);

const row = JSON.parse(lines[0]); // 不是合法 JSON 这里就抛
assert.strictEqual(row.query, query, 'query 字段要跟传入的一致');
assert.strictEqual(row.k, 3, 'k 记的是实际生效的值');
assert.strictEqual(row.caller, 'tool');
assert.strictEqual(row.session, 'smoke-session');
assert.strictEqual(row.origin, 'user');
assert.strictEqual(row.callId, 'call_smoke_1');
assert.strictEqual(row.type, null);
assert.strictEqual(row.hits, items.length, 'hits 要跟返回值条数一致');
assert.ok(typeof row.ms === 'number' && row.ms >= 0, 'ms 应该是非负数字');
assert.ok(typeof row.ts === 'string' && !Number.isNaN(Date.parse(row.ts)), 'ts 应该是 ISO 时间戳');
assert.ok(Array.isArray(row.top) && row.top.length <= 3, 'top 最多 3 条');
for (const t of row.top) {
  assert.ok('id' in t && 'type' in t && 'score' in t && 'text' in t, 'top 每项要有 id/type/score/text');
  assert.ok(t.text.length <= 61, `top.text 只写前 60 字（实际 ${t.text.length}）`);
}

// 返回值本身没被日志污染：字段还是 output schema 那一套，没有多出 id / trace
for (const it of items) {
  assert.deepStrictEqual(
    Object.keys(it).filter((k) => !['text', 'score', 'type', 'confidence', 'tags', 'entities', 'date', 'match', 'ts', 'status'].includes(k)),
    [],
    '返回值不能多出日志字段',
  );
}

// 滚动：把日志撑过 8MB，再调一次，应该滚成 .1，新文件只剩一行
fs.writeFileSync(logFile, 'x'.repeat(8 * 1024 * 1024 + 1024));
await store.recall('猪排', 1, { caller: 'prefill', session: null, origin: null });
assert.ok(fs.existsSync(`${logFile}.1`), '超过 8MB 应该滚成 .1');
const afterRotate = fs.readFileSync(logFile, 'utf8').split('\n').filter((l) => l.trim());
assert.strictEqual(afterRotate.length, 1, '滚动后新日志只有一行');
assert.strictEqual(JSON.parse(afterRotate[0]).caller, 'prefill');
console.log('OK 超过 8MB 滚成 .1，新文件重新开始');

console.log(`OK 日志一行：${lines[0].slice(0, 300)}`);
console.log(`OK 返回值 ${items.length} 条，字段未被污染`);
fs.rmSync(tmp, { recursive: true, force: true });
