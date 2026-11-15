import assert from 'node:assert/strict';
import test from 'node:test';
import {
  lintMemoryText, lintItems, formatLintFeedback, lintRejectReason,
  MEMORY_LINT_MIN_CHARS, LEADING_CONJUNCTIONS, BARE_REFERENCES,
} from '../lib/text-lint.js';

const codes = (text) => lintMemoryText(text).violations.map((v) => v.code);
const has = (text, code) => codes(text).includes(code);

/* ─────────────── 开头连接词 ─────────────── */

test('leading_conjunction：但是/所以/前导空格后的另外都命中', () => {
  assert.equal(has('但是小林把 store.js 的 lint 落到了 saveMemory 第 530 行。', 'leading_conjunction'), true);
  assert.equal(has('所以在 2026-11-06 这一天，小林把 141 条记忆重判了一遍。', 'leading_conjunction'), true);
  assert.equal(has('  另外根分区已用 15%，今天不用清理。', 'leading_conjunction'), true);
});

test('leading_conjunction：句中出现的连接词不算违规', () => {
  assert.equal(has('小林在 2026-11-06 把 lint 接进 store.js 的 saveMemory，闸在第 530 行。', 'leading_conjunction'), false);
  assert.equal(has('这条的正文里出现了但是两个字，不在开头位置。', 'leading_conjunction'), false);
  assert.equal(has('2026-11-06 的根分区已用 15%，小林没动 sleepEnabled 这个开关。', 'leading_conjunction'), false);
});

test('leading_conjunction：命中词取最长那个，index 指到开头', () => {
  const result = lintMemoryText('但是小林把 store.js 的 lint 落到了 saveMemory 第 530 行。');
  const violation = result.violations.find((v) => v.code === 'leading_conjunction');
  assert.equal(violation.match, '但是');
  assert.equal(violation.index, 0);
});

/* ─────────────── 指代词 ─────────────── */

test('bare_reference：句首的这一步/上述/后者都命中', () => {
  assert.equal(has('2026-11-06 小林改完 store.js。这一步把 lint 接进 saveMemory 第 530 行。', 'bare_reference'), true);
  assert.equal(has('树莓派温度 45 度；上述结论来自 sensors 的实测输出。', 'bare_reference'), true);
  assert.equal(has('根分区已用 15%。后者由 df -h 的输出确认。', 'bare_reference'), true);
});

test('bare_reference：其它/此前/句中指代都不算，不误伤', () => {
  assert.equal(has('其它三个插件都没动，仍停在 49347ce 那个提交上。', 'bare_reference'), false);
  assert.equal(has('此前小林在 2026-10-30 改过一次 llm-params.js 的默认值。', 'bare_reference'), false);
  assert.equal(has('小林把三条记忆逐条读了一遍，其它都合格。', 'bare_reference'), false);
});

test('bare_reference：它/此 单字只在整条正文第一个字算，分句首不算（2026-11-06 t-mw515yk8）', () => {
  // 分句首的「它」不再算：同一句里对象已经写清，读得懂
  assert.equal(has('2026-11-06 小林改完闸。它把 lint 接进 store.js 第 530 行。', 'bare_reference'), false);
  assert.equal(has('小林查了 store.js 的 lint 入口，其它两个写入函数也接上了。', 'bare_reference'), false);
  // 整条第一个字就是「它」/「此」→ 仍然命中
  assert.equal(has('它以 store.js 为入口，2026-11-06 接上了三个写入函数。', 'bare_reference'), true);
  assert.equal(has('此条来自 2026-11-06 的 store.js 写入链，闸在第 530 行。', 'bare_reference'), true);
  const ref = lintMemoryText('它以 store.js 为入口，2026-11-06 接上了三个写入函数。')
    .violations.find((v) => v.code === 'bare_reference');
  assert.equal(ref.match, '它');
  // 短语指代照旧按分句首判
  assert.equal(has('2026-11-06 小林改完闸。这一步把 lint 接进 store.js 第 530 行。', 'bare_reference'), true);
});

/* ─────────────── 字数 ─────────────── */

test('too_short：不足 25 字且不带坐标时命中', () => {
  assert.equal(has('树莓派温度正常。', 'too_short'), true);
  assert.equal(has('  风扇转得挺稳。 ', 'too_short'), true);
  assert.equal(has('根分区快满了', 'too_short'), true);
});

test('too_short：不足 25 字但带路径/文件名/行号/id/数字时放行（2026-11-06 t-mw515yk8）', () => {
  assert.equal(has('小林把 qq_img 目录改名为 img/qq。', 'too_short'), false, '24 字带路径');
  assert.equal(has('闸在 store.js 的 saveMemory 里。', 'too_short'), false, '带文件名');
  assert.equal(has('见 sleep.js:1097 那行。', 'too_short'), false, '带行号');
  assert.equal(has('记 m_mw4uae9q 这一条。', 'too_short'), false, '带 id');
  assert.equal(has('根分区 15%', 'too_short'), false, '带数字');
});

test('too_short：够 25 字不命中，空白不算字', () => {
  const ok = '小林在 2026-11-06 把正文 lint 接进 store.js 的三个写入函数。';
  assert.equal(ok.trim().length >= MEMORY_LINT_MIN_CHARS, true);
  assert.equal(has(ok, 'too_short'), false);
  assert.equal(has('树   莓   派   温   度', 'too_short'), true, '去空白后只剩 5 个字');
});

/* ─────────────── 元说明 ─────────────── */

test('meta_reference：此条被/正文见/同上/（略）都命中', () => {
  assert.equal(has('此条被 2026-11-06 的新条目取代，正文见上面那条。', 'meta_reference'), true);
  assert.equal(has('同上，根分区已用 15%，今天先不清理。', 'meta_reference'), true);
  assert.equal(has('旧结论（略），2026-11-06 的根分区占用只留 15% 这个数字。', 'meta_reference'), true);
});

test('meta_reference：正常正文不命中', () => {
  assert.equal(has('小林在 2026-11-06 把 lint 接进 store.js 的 saveMemory，闸在第 530 行。', 'meta_reference'), false);
  assert.equal(has('旧条目已归档，新条 m_9f2 记的是根分区 15% 这个现状。', 'meta_reference'), false);
  assert.equal(has('小林把 141 条记忆逐条重判，结论写进了 test/text-lint.test.mjs。', 'meta_reference'), false);
});

test('meta_reference：被…取代 需要成立才命中，普通「改成」不算', () => {
  assert.equal(has('此条被 2026-11-06 的新条目取代，正文见上面那条。', 'meta_reference'), true);
  assert.equal(has('2026-11-06 小林把 sleepEnabled 从 true 改成 false，晚上不再自动跑。', 'meta_reference'), false);
});

/* ─────────────── 2026-11-06 t-mw515yk8：三处误杀的放宽 ─────────────── */

test('meta_reference：被引号包住的规则字面串不算命中（t-mw515yk8）', () => {
  assert.equal(has('lint 规则第 5 条里写的「此条被」是举例，不是正文在写元说明。', 'meta_reference'), false);
  assert.equal(has('提示词里「见上一条」这四个字属于引用，不是正文内容。', 'meta_reference'), false);
  assert.equal(has('别写"此条被"这种字样，2026-11-06 小林已经说过了。', 'meta_reference'), false);
  // 不带引号的照旧命中
  assert.equal(has('此条被 2026-10-28 那条取代，别再照着旧说法走。', 'meta_reference'), true);
  assert.equal(has('小林在 2026-11-06 正文见第三节，旧条已经归档。', 'meta_reference'), true);
});

test('leading_conjunction：而且/而已/而后/而况 开头不算（t-mw515yk8）', () => {
  assert.equal(has('而且不对称——2026-11-06 这轮 cli 和 api 两个入口返回的字段不一样。', 'leading_conjunction'), false);
  assert.equal(has('而已这个说法只是语气词，2026-11-06 小林没写进正文。', 'leading_conjunction'), false);
  assert.equal(has('而后小林把 store.js 的闸挪到第 530 行，测试全绿。', 'leading_conjunction'), false);
  assert.equal(has('而况这条早就归档了，2026-11-06 不用再看。', 'leading_conjunction'), false);
  // 单个「而」开头照旧命中
  assert.equal(has('而小林在 2026-11-06 只改了 lint 一个文件。', 'leading_conjunction'), true);
});

test('四条存量误杀原文在修复后全部放行（t-mw515yk8）', () => {
  const samples = [
    'lint 规则第 5 条里写的「此条被」是引用举例，不是元说明。',
    '而且不对称——2026-11-06 这轮 cli 和 api 两个入口返回的字段不一样。',
    '小林安装了 dsh-github-mcp 插件，它是官方远程 MCP 的包装。',
    '小林把 qq_img 目录改名为 img/qq。',
  ];
  for (const text of samples) assert.equal(lintMemoryText(text).ok, true, text);
});

test('真毛病仍然被拦：但是开头、此条被…取代、这一步怎么走（t-mw515yk8）', () => {
  assert.equal(has('但是这段以连接词开头，单条读不懂发生了什么。', 'leading_conjunction'), true);
  assert.equal(has('此条被 2026-10-28 那条取代，正文见上面。', 'meta_reference'), true);
  assert.equal(has('这一步怎么走，小林在 2026-11-06 还没定。', 'bare_reference'), true);
  assert.equal(has('它能跑起来。', 'bare_reference'), true);
});

/* ─────────────── 无坐标判断词 ─────────────── */

test('unverified_claim：没坐标的已验证/已确认/实测 都命中', () => {
  assert.equal(has('小林已验证树莓派温度正常，风扇转速也没问题，可以放心。', 'unverified_claim'), true);
  assert.equal(has('这条结论已经确认过，备份也在原处放着，不用重新导一遍。', 'unverified_claim'), true);
  assert.equal(has('实测这批记忆都能单独读懂，没有一条需要人工补主语。', 'unverified_claim'), true);
});

test('unverified_claim：带坐标（数字/路径/file:line）不命中', () => {
  assert.equal(has('小林实测 2026-11-06 根分区已用 15%，数字来自 df -h 的输出。', 'unverified_claim'), false);
  assert.equal(has('已确认 store.js:530 那行的闸生效，130 条测试全绿。', 'unverified_claim'), false);
  assert.equal(has('实测树莓派温度 45 度、风扇转速 3000 转，都来自 sensors 输出。', 'unverified_claim'), false);
});

test('unverified_claim：hint 必须是「补坐标或删掉判断词」', () => {
  const violation = lintMemoryText('小林已验证树莓派温度正常，风扇转速也没问题，可以放心。')
    .violations.find((v) => v.code === 'unverified_claim');
  assert.equal(violation.hint, '补坐标或删掉判断词');
});

/* ─────────────── 批量 / 提示 / 纯函数 ─────────────── */

test('lintItems：一次出多条只打回不合格的，通过的照常放行', () => {
  const items = [
    { text: '小林在 2026-11-06 把 lint 接进 store.js 的 saveMemory，闸在第 530 行。' },
    { text: '但是这段以连接词开头，单条读不懂发生了什么。' },
    { text: '根分区还剩一些，今天不用清理。' },
  ];
  const { accepted, rejected } = lintItems(items);
  assert.equal(accepted.length, 1);
  assert.equal(rejected.length, 2);
  assert.equal(rejected[0].index, 1);
  assert.equal(rejected[1].index, 2);
  assert.equal(rejected[0].text, items[1].text, '原样带出，不改写');
  assert.equal(items[1].text, '但是这段以连接词开头，单条读不懂发生了什么。', '输入不被改');
});

test('formatLintFeedback：带命中词和 hint，不夹带改写后的正文', () => {
  const { rejected } = lintItems([{ text: '但是这段以连接词开头，单条读不懂发生了什么。' }]);
  const feedback = formatLintFeedback(rejected);
  assert.match(feedback, /leading_conjunction/);
  assert.match(feedback, /去掉这个连接词/);
  assert.match(feedback, /但是这段以连接词开头/);
});

test('lintRejectReason：code:hint 拼起来，最长 500 字', () => {
  const { rejected } = lintItems([{ text: '但是' }]);
  const reason = lintRejectReason(rejected[0].violations);
  assert.match(reason, /leading_conjunction:/);
  assert.match(reason, /too_short:/);
  assert.equal(reason.length <= 500, true);
});

test('纯函数：同一输入连跑两次结果一致，不抛错', () => {
  const text = '所以小林把这一步的闸挪了，实测都没问题（略）。';
  const a = lintMemoryText(text);
  const b = lintMemoryText(text);
  assert.deepEqual(a, b);
  assert.equal(typeof text, 'string');
  assert.equal(lintMemoryText('').ok, false);
  assert.equal(lintMemoryText(undefined).ok, false);
});

test('常量表与提示词口径一致：连接词 15 个、指代词 12 个', () => {
  assert.equal(LEADING_CONJUNCTIONS.length, 15);
  assert.equal(LEADING_CONJUNCTIONS.includes('但是'), true);
  assert.equal(LEADING_CONJUNCTIONS.includes('总之'), true);
  assert.equal(BARE_REFERENCES.length, 12);
  assert.equal(BARE_REFERENCES.includes('这一步'), true);
  assert.equal(BARE_REFERENCES.includes('此'), true);
});
