/**
 * SleepState 对象（2026-11-14，阶段 4）。
 *
 * 钉五件事：坏 JSON 不抛、save 原子、patch 对象/函数两种形态、嵌套更新抛错、
 * 内存版（没配 stateFile）行为一致。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { createSleepState, createMemorySleepState } from '../lib/sleep-state.js';

function tmpFile() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-sstate-'));
  return path.join(dir, 'sleep.state.json');
}

test('load：文件不存在/坏 JSON 都返回空态，不抛', () => {
  const file = tmpFile();
  const st = createSleepState({ file });
  assert.deepEqual(st.load(), { lastSleepAt: 0, lastSleepDay: '' });
  fs.writeFileSync(file, '{坏 json', 'utf8');
  assert.deepEqual(st.load(), { lastSleepAt: 0, lastSleepDay: '' });
});

test('save：写主文件、不留 .tmp，读回来是同一份', () => {
  const file = tmpFile();
  const st = createSleepState({ file });
  st.save({ lastSleepAt: 123, lastSleepDay: '2026-11-14', slots: { 'a b': 1 } });
  assert.deepEqual(JSON.parse(fs.readFileSync(file, 'utf8')), { lastSleepAt: 123, lastSleepDay: '2026-11-14', slots: { 'a b': 1 } });
  assert.deepEqual(fs.readdirSync(path.dirname(file)), ['sleep.state.json'], '没有残留 .tmp');
});

test('patch：对象合并基于最新 state；函数形态原地改', () => {
  const file = tmpFile();
  const st = createSleepState({ file });
  st.save({ lastSleepAt: 1, owner: 'old', slots: {} });
  const a = st.patch({ lastSleepAt: 2 });
  assert.equal(a.lastSleepAt, 2);
  assert.equal(a.owner, 'old', '没给的字段保留');

  const b = st.update((s) => {
    const out = { ...s };
    delete out.owner;
    out.owedSlots = { k: 1 };
    return out;
  });
  assert.equal('owner' in b, false, '函数形态可以删字段');
  assert.deepEqual(b.owedSlots, { k: 1 });
  assert.equal(JSON.parse(fs.readFileSync(file, 'utf8')).owedSlots.k, 1, '落盘了');
});

test('update：嵌套更新直接抛错（防半截状态）', () => {
  const file = tmpFile();
  const st = createSleepState({ file });
  assert.throws(() => st.update(() => st.patch({ lastSleepAt: 9 })), /嵌套更新/);
  assert.equal(st.load().lastSleepAt, 0, '失败那次没写进去');
});

test('内存版：接口一致，不碰文件', () => {
  const st = createMemorySleepState();
  assert.deepEqual(st.load(), { lastSleepAt: 0, lastSleepDay: '' });
  st.patch({ lastSleepAt: 5, owner: 'x' });
  assert.equal(st.load().lastSleepAt, 5);
  assert.equal(st.load().owner, 'x');
  st.update((s) => ({ ...s, owedSlots: { a: 1 } }));
  assert.deepEqual(st.load().owedSlots, { a: 1 });
});

test('边界：没给 file 直接抛；update 返回 null 时保留原 state', () => {
  assert.throws(() => createSleepState({}), /file 必填/);
  const file = tmpFile();
  const st = createSleepState({ file });
  st.save({ lastSleepAt: 7 });
  const out = st.update(() => null);
  assert.equal(out.lastSleepAt, 7, '回调返回空时用 load 出来的那份');
});

test('save：父目录不可建时抛错，不静默吞（由调用方决定要不要继续）', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-sstate-'));
  const blocker = path.join(dir, 'blocker');
  fs.writeFileSync(blocker, 'x', 'utf8');
  const st = createSleepState({ file: path.join(blocker, 'sleep.state.json') });
  assert.throws(() => st.save({ lastSleepAt: 1 }));
});

test('边界：patch() 不传参等于原样重写；自定义 pid 走自己的 tmp 名', () => {
  const file = tmpFile();
  const st = createSleepState({ file, pid: 4242 });
  st.save({ lastSleepAt: 3 });
  const out = st.patch();
  assert.equal(out.lastSleepAt, 3, '不传参就是读出来再写回');
  assert.deepEqual(fs.readdirSync(path.dirname(file)), ['sleep.state.json']);
});
