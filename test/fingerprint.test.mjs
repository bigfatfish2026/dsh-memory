/**
 * 指纹原语（2026-11-14，阶段 4）。
 *
 * 钉四件事：
 *   1) sha1Hex 与 crypto 直算一致，len 只截断不改内容；
 *   2) digestParts 有分隔符，防止 "ab"+"c" 与 "a"+"bc" 撞键；
 *   3) digestSorted 与属性顺序无关；
 *   4) 回归锁：batchFingerprint 与改造前的字节序一致（排序 id + \u0000 + 正文 + \u0001，截 16）；
 *      splitQueueKey 用原始正文，不归一化。
 */
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import test from 'node:test';
import { sha1Hex, digestParts, digestSorted } from '../lib/fingerprint.js';
import { batchFingerprint } from '../lib/sleep.js';
import { splitQueueKey } from '../lib/split-queue.js';

const direct = (s) => crypto.createHash('sha1').update(s, 'utf8').digest('hex');

test('sha1Hex：与 crypto 直算一致，len 只截前几位', () => {
  assert.equal(sha1Hex('树莓派'), direct('树莓派'));
  assert.equal(sha1Hex('abc', { len: 8 }), direct('abc').slice(0, 8));
  assert.equal(sha1Hex('abc', { len: 0 }), direct('abc'), 'len=0 不截断');
  assert.equal(sha1Hex(null), direct(''), 'null/undefined 当空串，不炸');
});

test('digestParts：分隔符防撞（ab+c ≠ a+bc）', () => {
  assert.notEqual(digestParts(['ab', 'c']), digestParts(['a', 'bc']));
  assert.equal(digestParts(['a', 'b'], { sep: '' }), sha1Hex('ab'));
  assert.equal(digestParts([1, 2], { len: 6 }).length, 6);
});

test('digestSorted：属性顺序不同也得到同一个键', () => {
  assert.equal(digestSorted({ a: 1, b: 2 }), digestSorted({ b: 2, a: 1 }));
  assert.notEqual(digestSorted({ a: 1 }), digestSorted({ a: 2 }));
  assert.equal(digestSorted({ z: 1, a: 2 }), sha1Hex(JSON.stringify([['a', 2], ['z', 1]])), '键按字典序排');
});

test('batchFingerprint 回归锁：与改造前的字节序逐位一致', () => {
  const cluster = [
    { id: 'm_b', text: '第二条' },
    { id: 'm_a', text: '第一条' },
  ];
  const h = crypto.createHash('sha1');
  for (const m of [...cluster].sort((x, y) => (x.id < y.id ? -1 : 1))) {
    h.update(m.id); h.update('\u0000'); h.update(m.text); h.update('\u0001');
  }
  assert.equal(batchFingerprint(cluster), h.digest('hex').slice(0, 16));
  assert.equal(batchFingerprint(cluster).length, 16);
  assert.notEqual(batchFingerprint(cluster), batchFingerprint([{ id: 'm_a', text: '第一条改过' }].concat(cluster[0])), '正文变了键就变');
});

test('splitQueueKey：sourceId 优先；无 id 时用原始正文（不归一化）', () => {
  assert.equal(splitQueueKey({ sourceId: 'm_1', text: 'x' }), 'm_1');
  const a = splitQueueKey({ text: '温度 45 度' });
  const b = splitQueueKey({ text: '温度45度' });
  assert.notEqual(a, b, '只差空格的正文必须是不同键（写入门才归一化）');
  assert.ok(a.startsWith('split:') && a.length === 'split:'.length + 16);
});

test('原语兜底：非数组/非对象输入不炸，按空处理', () => {
  assert.equal(digestParts(null), sha1Hex(''));
  assert.equal(digestSorted(null), sha1Hex('[]'));
  assert.equal(sha1Hex(undefined), sha1Hex(''));
});
