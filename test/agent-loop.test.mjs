/**
 * 工具循环的活性守卫。
 *
 * 工具调用次数上限全删了，防死循环只剩两条「判进展」的规则：
 *   ① 同一工具 + 同一组参数连续 3 次 → 中断；
 *   ② 连续 20 次工具调用的返回内容全是旧的（没有新信息）→ 中断。
 * 只要中间有一次新结果，②的计数就清零。
 */
import assert from 'node:assert/strict';
import test from 'node:test';
import {
  createToolLoopGuard, REPEAT_STREAK, NO_PROGRESS_STREAK,
} from '../lib/agent-loop.js';

test('同工具同参数连续 3 次 → 中断，日志写清原因', () => {
  const logs = [];
  const guard = createToolLoopGuard({ log: (line) => logs.push(line), label: 'test' });
  const call = { name: 'read_file', args: { path: '/tmp/a' }, result: '内容 A' };
  assert.equal(guard.record(call).stop, false);
  assert.equal(guard.record(call).stop, false);
  const third = guard.record(call);
  assert.equal(third.stop, true);
  assert.equal(third.reason, 'repeat');
  assert.match(third.detail, /read_file 连续 3 次同参数/);
  assert.ok(logs.some((line) => line.includes('[agent-loop] 中断 reason=repeat test')));
  assert.equal(REPEAT_STREAK, 3);

  // 换参数就重新计数，不会误杀正常的连续读
  const g2 = createToolLoopGuard({});
  assert.equal(g2.record({ name: 'read_file', args: { path: '/a' } }).stop, false);
  assert.equal(g2.record({ name: 'read_file', args: { path: '/b' } }).stop, false);
  assert.equal(g2.record({ name: 'read_file', args: { path: '/c' } }).stop, false);
});

test('同一工具不同参数不算重复；换工具也算新调用', () => {
  const guard = createToolLoopGuard({});
  assert.equal(guard.record({ name: 'grep', args: 'a' }).stop, false);
  assert.equal(guard.record({ name: 'grep', args: 'b' }).stop, false);
  assert.equal(guard.record({ name: 'stat_path', args: 'b' }).stop, false);
  assert.equal(guard.record({ name: 'stat_path', args: 'c' }).stop, false);
});

test('连续 20 次返回内容全是旧的 → 无进展中断；中间有新内容就清零', () => {
  const logs = [];
  const guard = createToolLoopGuard({ log: (line) => logs.push(line), label: 'test' });
  // 每一轮工具名/参数都不同（不触发重复规则），但返回内容永远是同一段
  let tripped = null;
  // 第 1 次把内容放进 context（算新信息），从第 2 次起连续重复；第 21 次时 streak 才到 20
  for (let i = 1; i <= NO_PROGRESS_STREAK + 1; i += 1) {
    const verdict = guard.record({ name: 'grep', args: `q${i}`, result: '同一段旧内容' });
    if (verdict.stop) {
      tripped = { at: i, verdict };
      break;
    }
  }
  assert.ok(tripped, '连续 20 次没有新信息必须中断');
  assert.equal(tripped.at, NO_PROGRESS_STREAK + 1);
  assert.equal(tripped.verdict.reason, 'no_progress');
  assert.match(tripped.verdict.detail, /连续 20 次返回内容全是旧的/);
  assert.ok(logs.some((line) => line.includes('[agent-loop] 中断 reason=no_progress')));

  // 中途来一条新内容，计数清零，之后还能再跑 19 次
  const g2 = createToolLoopGuard({});
  let stopped = false;
  for (let i = 1; i < NO_PROGRESS_STREAK; i += 1) {
    if (g2.record({ name: 'grep', args: `x${i}`, result: '旧内容' }).stop) stopped = true;
  }
  assert.equal(stopped, false);
  assert.equal(g2.record({ name: 'grep', args: 'fresh', result: '新内容' }).stop, false);
  for (let i = 1; i < NO_PROGRESS_STREAK; i += 1) {
    if (g2.record({ name: 'grep', args: `y${i}`, result: '又一段旧内容' }).stop) stopped = true;
  }
  assert.equal(stopped, false, '清零后重新计数，19 次不该中断');
});

test('没有返回内容（result 缺省）不算无进展，只按重复规则判', () => {
  const guard = createToolLoopGuard({});
  for (let i = 1; i <= NO_PROGRESS_STREAK + 5; i += 1) {
    assert.equal(guard.record({ name: 'list_dir', args: `p${i}` }).stop, false);
  }
});
