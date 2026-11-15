/**
 * 推送脚本的 spawn 错误处理：脚本不存在时不许把宿主带走。
 *
 * 2026-11-16：宿主两次被 uncaught exception 带走，根因就是 notifyScript 指向
 * 一个不存在的路径（缺省 cwd 下没有 notify.sh），spawn 的 ENOENT 走异步
 * 'error' 事件，调用处的同步 try/catch 抓不到。runNotify 必须：
 *   ① 同步返回、不抛；
 *   ② 异步 error 记一条 warn（带 ENOENT）；
 *   ③ 主流程照常往下走。
 * 空路径那条也一并钉住：不 spawn、不记日志。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { runNotify } from '../lib/shared/notify.js';

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

test('runNotify：脚本路径不存在时记 warn、不抛，主流程继续', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'notify-missing-'));
  const missing = path.join(dir, 'notify.sh'); // 故意不创建，spawn 必然 ENOENT
  const warn = [];
  let continued = false;

  assert.doesNotThrow(() => {
    runNotify({ script: missing, args: ['hello'], label: 'test-missing', log: (line) => warn.push(line) });
    continued = true; // 这一行能跑到，就说明 spawn 没把异常同步扔回来
  });
  assert.equal(continued, true, 'runNotify 同步返回，主流程照常往下走');

  const started = Date.now();
  while (warn.length === 0 && Date.now() - started < 2000) await sleep(20);

  const text = warn.join('\n');
  assert.ok(warn.length > 0, 'ENOENT 必须留下一条 warn，不许静默吞掉');
  assert.match(text, /\[notify\] spawn 失败/);
  assert.match(text, /ENOENT/);
  assert.match(text, /label=test-missing/);
  assert.match(text, /notify\.sh/, '日志里要能看出是哪个脚本');
  assert.equal(warn.filter((line) => /spawn 失败/.test(line)).length, 1, 'ENOENT 只记一条，别让随后的 close 再记一次退出码');
});

test('runNotify：脚本路径为空直接 return，不 spawn 也不记日志', () => {
  const warn = [];
  assert.doesNotThrow(() => {
    runNotify({ script: '', args: ['x'], label: 'test-empty', log: (line) => warn.push(line) });
    runNotify({ script: '   ', args: ['x'], label: 'test-blank', log: (line) => warn.push(line) });
    runNotify({ script: undefined, args: ['x'], label: 'test-undefined', log: (line) => warn.push(line) });
  });
  assert.deepEqual(warn, [], '没配脚本就是不发，别刷日志');
});
