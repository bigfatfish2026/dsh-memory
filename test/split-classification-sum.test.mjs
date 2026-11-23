/**
 * 拆分对账口径（2026-11-22）。
 *
 * 11-22 05:00 那轮：split 493 条 → 落库 104、回队 294、版本不符 85、覆盖未过 10，
 * 不可拆 247、失败 142。旧汇总把 unavoidable 同时算进 pendingAfter 和分类合计，
 * 493 被算成 740，`scanned - sum` 变成负数，「差多少条未落账」的方向就反了。
 *
 * 新口径：493 = 104 + 294 + 85 + 10，不可拆包含在回队里，不再单列。
 */
import assert from 'node:assert/strict';
import test from 'node:test';
import { splitBreakdownOf, splitClassificationSum } from '../lib/pipeline-split.js';

test('对账：493 = 104 + 294 + 85 + 10，不可拆算在回队里不重复计', () => {
  // 固定夹具：直接按那轮日志的四个分类数构造 stats
  const stats = {
    scanned: 493,
    tried: 493,
    unavailable: 0,
    unavoidable: 247,
    archived: 104,
    inserted: 104,
    deduped: 0,
    parts: 300,
    rejected: 0,
    failed: 142,
    skipped: 0,
    charsBefore: 0,
    charsAfter: 0,
    concurrency: 8,
    versionMismatch: 85,
    coverageBlocked: 10,
    succeeded: 104,
    dedupedOnly: 0,
    pendingAfter: 294,
    inflateDead: 0,
  };
  const b = splitBreakdownOf(stats);
  assert.equal(b.pendingAfter, 294, '不可拆已经包含在回队里');
  assert.equal(b.unavoidable, 247, '不可拆仍单独留作附注');
  assert.equal(splitClassificationSum(b), 493, '分类合计必须等于条数');
  assert.equal(b.succeeded + b.pendingAfter + b.versionMismatch + b.coverageBlocked, 493);

  // 老算法（把 unavoidable 再加一遍）会得到 740：钉住，不许回退
  const doubleCounted = b.succeeded + b.dedupedOnly + b.skipped + b.pendingAfter
    + b.versionMismatch + b.coverageBlocked + b.unavoidable + b.inflateDead;
  assert.equal(doubleCounted, 740, '旧口径双计 247 条，差值方向必反');
  assert.notEqual(splitClassificationSum(b), doubleCounted);
});

test('对账：少了 inflateDead 就不等于条数（分类必须逐项加全）', () => {
  const b = {
    succeeded: 1, dedupedOnly: 0, skipped: 0, pendingAfter: 0,
    versionMismatch: 0, coverageBlocked: 0, unavoidable: 0, inflateDead: 1, precheckDead: 0,
  };
  assert.equal(splitClassificationSum(b), 2);
});

test('对账：预检跳过按实际 status 落类（pending 进回队，dead 单列 precheckDead）', () => {
  // 预检不计 attempts 之后正常落 pending：回队 +1，precheckDead 保持 0，合计仍等于条数。
  const ok = {
    succeeded: 0, dedupedOnly: 0, skipped: 0, pendingAfter: 1,
    versionMismatch: 0, coverageBlocked: 0, unavoidable: 1, inflateDead: 0, precheckDead: 0,
  };
  assert.equal(splitClassificationSum(ok), 1);

  // 表坏/行已经被别的路径判 dead：不许先加 pendingAfter 再改，dead 类要能对得上。
  const dead = {
    succeeded: 0, dedupedOnly: 0, skipped: 0, pendingAfter: 0,
    versionMismatch: 0, coverageBlocked: 0, unavoidable: 1, inflateDead: 0, precheckDead: 1,
  };
  assert.equal(splitClassificationSum(dead), 1, 'precheckDead 必须计进分类合计');
  assert.equal(splitBreakdownOf({ ...dead, precheckDead: 1 }).precheckDead, 1);
});
