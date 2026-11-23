/**
 * observation 证据来源根（src/sleep/observation-sources.ts，t-mwqddlm2-ot3g）。
 *
 * 钉住的边界：
 *   ① 同一原条拆出的两片只算 1 个来源，不同原条算 2 个；
 *   ② 多级拆分沿 source 追到最顶层（孙段不另算来源）；
 *   ③ queue 键拆分按 key 归并，不同 key 分开；
 *   ④ 合并产物（extra.sources）+ 其中一个成员片段同时作证据 → 不重复计数；
 *   ⑤ 来历不明（source 空、无父边）→ uncertain，独立计数时单列；
 *   ⑥ 空证据 / 重复 id；
 *   ⑦ selectEntityEvidence：只取 active 的 world/experience、按 validFrom||ts 倒序、
 *      limit 生效、不同实体不混、条目带 roots；
 *   ⑧ 递归防环 + 历史 split_of 边也能追到父条。
 *
 * 纯函数：不建库、不调模型、不依赖 store 实例。
 */
import assert from 'node:assert/strict';
import test from 'node:test';
import {
  BINDING_MIN_LOCATE_CONTEXT,
  evidenceBindingOf,
  independentSourceCount,
  selectEntityEvidence,
  sourceRootsOf,
} from '../lib/sleep/observation-sources.js';

/** 造一条记忆行：默认 source='agent'（普通来源明确的条目）。 */
function memory(id, extra = {}) {
  return { id, text: `正文 ${id}`, source: 'agent', type: 'world', ...extra };
}

function rootsOf(records, id, links = []) {
  return sourceRootsOf(records, links).get(id);
}

test('同原条拆出的两片只算 1 个来源，不同原条算 2 个', () => {
  const records = [
    memory('p1'),
    memory('p2'),
    memory('a', { source: 'sleep:split:p1' }),
    memory('b', { source: 'sleep:split:p1' }),
    memory('c', { source: 'sleep:split:p2' }),
  ];
  const map = sourceRootsOf(records);

  assert.deepEqual(map.get('a').roots, ['p1']);
  assert.equal(map.get('a').uncertain, false);
  assert.deepEqual(map.get('b').roots, ['p1']);

  const sameParent = independentSourceCount(['a', 'b'], map);
  assert.equal(sameParent.count, 1, '同一原条的两片只算一条');
  assert.deepEqual(sameParent.roots, ['p1']);
  assert.equal(sameParent.uncertainCount, 0);

  const differentParents = independentSourceCount(['a', 'c'], map);
  assert.equal(differentParents.count, 2);
  assert.deepEqual(differentParents.roots, ['p1', 'p2']);
});

test('多级拆分：孙段沿 source 追到最顶层原条，仍算 1 个来源', () => {
  const records = [
    memory('root'),
    memory('mid', { source: 'sleep:split:root' }),
    memory('grand', { source: 'sleep:split:mid' }),
    memory('grand2', { source: 'sleep:split:mid' }),
  ];
  const map = sourceRootsOf(records);

  assert.deepEqual(map.get('mid').roots, ['root']);
  assert.deepEqual(map.get('grand').roots, ['root'], '孙段不能算成新来源');
  assert.deepEqual(map.get('grand2').roots, ['root']);

  const counted = independentSourceCount(['mid', 'grand', 'grand2'], map);
  assert.equal(counted.count, 1);
  assert.deepEqual(counted.roots, ['root']);
});

test('queue 键拆分：同 key 归并成 1，不同 key 分开算', () => {
  const records = [
    memory('q1a', { source: 'sleep:split:queue:split:aaaa1111' }),
    memory('q1b', { source: 'sleep:split:queue:split:aaaa1111' }),
    memory('q2a', { source: 'sleep:split:queue:split:bbbb2222' }),
  ];
  const map = sourceRootsOf(records);

  assert.deepEqual(map.get('q1a').roots, ['queue:split:aaaa1111']);
  assert.deepEqual(map.get('q1b').roots, ['queue:split:aaaa1111']);

  const sameKey = independentSourceCount(['q1a', 'q1b'], map);
  assert.equal(sameKey.count, 1);
  assert.deepEqual(sameKey.roots, ['queue:split:aaaa1111']);

  const differentKey = independentSourceCount(['q1a', 'q2a'], map);
  assert.equal(differentKey.count, 2);
});

test('合并产物 + 其中一个成员片段同时作证据：仍是 2 不是 3', () => {
  const records = [
    memory('r1'),
    memory('r2'),
    memory('prod', { source: 'sleep:merge', extra: { sources: ['r1', 'r2'] } }),
    memory('frag', { source: 'sleep:split:r1' }),
  ];
  const map = sourceRootsOf(records);

  assert.deepEqual(map.get('prod').roots, ['r1', 'r2'], '合并产物的根是 extra.sources 集合');
  assert.deepEqual(map.get('frag').roots, ['r1']);

  const counted = independentSourceCount(['prod', 'frag'], map);
  assert.equal(counted.count, 2, '成员片段的根已在产物根集合里，不许再 +1');
  assert.deepEqual(counted.roots, ['r1', 'r2']);

  const withMember = independentSourceCount(['prod', 'r1', 'frag'], map);
  assert.equal(withMember.count, 2, '产物 + 两个成员（一个还是片段）仍只有两个独立来源');
});

test('合并产物在真库的写法：source=sleep:merge，成员只在 supersedes 列，也能追到成员根', () => {
  const records = [
    memory('m1', { source: 'sleep:split:p1' }),
    memory('p1'),
    memory('m2'),
    memory('prod', { source: 'sleep:merge', supersedes: ['m1', 'm2'] }),
  ];
  const map = sourceRootsOf(records);

  assert.deepEqual(map.get('prod').roots, ['m2', 'p1'], '成员片段按其原条归并，不另算');
  const counted = independentSourceCount(['prod', 'm1'], map);
  assert.equal(counted.count, 2);
});

test('来历不明（source 空、无父边）→ uncertain，独立计数时单列', () => {
  const records = [
    memory('known'),
    { id: 'ghost', text: '没有 source 的老条目' },
  ];
  const map = sourceRootsOf(records);

  assert.deepEqual(map.get('ghost').roots, ['ghost']);
  assert.equal(map.get('ghost').uncertain, true);
  assert.equal(map.get('known').uncertain, false);

  const alone = independentSourceCount(['ghost'], map);
  assert.equal(alone.count, 0, 'uncertain 不参与并集计数');
  assert.deepEqual(alone.uncertainIds, ['ghost']);
  assert.equal(alone.uncertainCount, 1);

  const mixed = independentSourceCount(['known', 'ghost'], map);
  assert.equal(mixed.count, 1);
  assert.deepEqual(mixed.uncertainIds, ['ghost']);

  const missing = independentSourceCount(['not-in-map'], map);
  assert.equal(missing.count, 0, '拿不到来历的 id 也算 uncertain，不能冒充独立来源');
  assert.deepEqual(missing.uncertainIds, ['not-in-map']);
});

test('空证据返回 0，重复 id 去重', () => {
  const records = [
    memory('p'),
    memory('a', { source: 'sleep:split:p' }),
  ];
  const map = sourceRootsOf(records);

  const empty = independentSourceCount([], map);
  assert.deepEqual(empty, { count: 0, roots: [], uncertainIds: [], uncertainCount: 0 });
  assert.equal(independentSourceCount(['a', 'a', 'a'], map).count, 1);
  assert.equal(independentSourceCount(['a', '', '  ', null], map).count, 1, '空 id 忽略');
});

test('历史 live 边：supersedes/split_of 边指向父条时父条根并进来，环不递归死', () => {
  const records = [
    memory('parent'),
    // 子段 source 没写成 sleep:split（历史行），只有 live 边指向父条
    memory('child', { source: '' }),
    // 自指 + 互指：只要求不死循环，并给出可用的根
    memory('loopA', { source: 'sleep:split:loopB' }),
    memory('loopB', { source: 'sleep:split:loopA' }),
  ];
  const links = [
    { src: 'child', dst: 'parent', rel: 'supersedes' },
    { src: 'child', dst: 'parent2', rel: 'split_of' },
  ];
  const map = sourceRootsOf(records, links);

  assert.deepEqual(map.get('child').roots, ['parent', 'parent2']);
  assert.equal(map.get('child').uncertain, false, '有父边就算来历清楚');

  const loop = sourceRootsOf([records[2], records[3]]);
  assert.equal(loop.get('loopA').uncertain, true, '环上的条目拿不出干净来历');
  assert.ok(loop.get('loopA').roots.length >= 1);
});

test('selectEntityEvidence：实体键只认整词，裸子串不再命中（t-mwr37s99-7y32）', () => {
  const records = [
    // 'readonly' 里含 'read'，但 'read' 后面还粘着 'o'：不是完整词
    memory('r1', { entities: ['readonly'], text: 'readonly 打开数据库，没有写权限' }),
    // 路径里的 basename 不是独立实体：记录键不以 'notify.ts' 开头
    memory('n1', { entities: ['src/shared/notify.ts'], text: 'notify.ts 的告警来自 src/shared/notify.ts' }),
    memory('a1', { entities: ['Anri'], text: 'Anri 今天在' }),
    memory('a2', { entities: ['Anri 的'], text: 'Anri 的笔记本放在桌上' }),
    memory('t1', { entities: ['treehouse'], text: 'treehouse 是树屋' }),
    // 有别的实体键、只是正文提到 bash：纯正文命中过不了落库精确键交集，不许选
    memory('b1', { entities: ['dsh-ego'], text: 'bash 跑了一遍测试' }),
    // 老条目没有实体列：正文整词兜底仍生效
    memory('x1', { entities: [], text: '老条目提到 Anri 但没有实体列' }),
  ];
  const idsOf = (name) => selectEntityEvidence(records, { entityKey: name }).items.map((item) => item.id).sort();

  assert.deepEqual(idsOf('read'), [], "'read' 不许命中实体键 'readonly'");
  assert.deepEqual(idsOf('readonly'), ['r1'], '完整键相等照旧命中');
  assert.deepEqual(idsOf('notify.ts'), [], "'notify.ts' 不许命中 'src/shared/notify.ts'");
  assert.deepEqual(idsOf('src/shared/notify.ts'), ['n1'], '完整路径相等照旧命中');
  assert.deepEqual(idsOf('tree'), [], "'tree' 不许命中 'treehouse'");
  assert.deepEqual(idsOf('bash'), [], '有实体键的行不许走纯正文兜底');
  assert.deepEqual(idsOf('Anri'), ['a1', 'a2', 'x1'], "'Anri' 命中相等、'Anri 的'（完整词前缀）与老条目整词兜底");
  assert.deepEqual(idsOf('anri 的'), ['a2'], "'Anri 的' 归一化后相等照旧命中");
});

test('selectEntityEvidence：只取 active 的 world/experience，按时间倒序，limit 生效，不同实体不混', () => {
  const records = [
    memory('w1', { entities: ['树莓派'], type: 'world', validFrom: '2026-11-01', text: '第一条' }),
    memory('w2', { entities: ['树莓派'], type: 'world', ts: '2026-11-05T10:00:00.000Z', text: '第二条' }),
    memory('x1', { entities: ['树莓派'], type: 'experience', validFrom: '2026-11-03', text: '第三条' }),
    memory('op1', { entities: ['树莓派'], type: 'opinion', validFrom: '2026-11-06', text: '判断不算证据' }),
    memory('ob1', { entities: ['树莓派'], type: 'observation', validFrom: '2026-11-07', text: '观察不算证据' }),
    memory('old', { entities: ['树莓派'], type: 'world', validFrom: '2026-11-08', status: 'archived', text: '归档了' }),
    memory('merged', { entities: ['树莓派'], type: 'world', validFrom: '2026-11-09', status: 'merged', text: '合并走了' }),
    memory('other', { entities: ['小林'], type: 'world', validFrom: '2026-11-10', text: '另一个实体' }),
    // status 空 = active（rows.ts isActive 口径）
    memory('w0', { entities: ['树莓派'], type: 'world', validFrom: '2026-11-02', status: '', text: '空状态也算活' }),
    // 片段：根的标注要能带出来
    memory('frag', { entities: ['树莓派'], type: 'world', source: 'sleep:split:w2', validFrom: '2026-11-04', text: '片段' }),
  ];
  const all = selectEntityEvidence(records, { entityKey: '树莓派', limit: 30 });

  const ids = all.items.map((item) => item.id);
  assert.deepEqual(ids, ['w2', 'frag', 'x1', 'w0', 'w1'], 'active 的 world/experience，按 validFrom||ts 倒序');
  assert.equal(all.total, 5);
  assert.ok(!ids.includes('op1') && !ids.includes('ob1'), 'opinion/observation 不当证据');
  assert.ok(!ids.includes('old') && !ids.includes('merged'), '非 active 不取');
  assert.ok(!ids.includes('other'), '不同实体不混');

  const frag = all.items.find((item) => item.id === 'frag');
  assert.deepEqual(frag.roots, ['w2'], '返回条目带 roots 标注');
  assert.equal(frag.uncertain, false);

  const limited = selectEntityEvidence(records, { entityKey: '树莓派', limit: 2 });
  assert.deepEqual(limited.items.map((item) => item.id), ['w2', 'frag']);
  assert.equal(limited.total, 5, 'total 是截断前的命中数');

  const byNorm = selectEntityEvidence(records, { entityNorm: '树莓派' });
  assert.equal(byNorm.items.length, 5);
  assert.equal(byNorm.entityNorm, '树莓派');
});

test('evidenceBindingOf：命中依据带规则/命中词/片段/现有实体键，歧义单独标（t-mwsuyr33-2qmv）', () => {
  const exact = evidenceBindingOf(
    { id: 'e1', text: 'Anri 今天在', entities: ['Anri'] },
    'anri',
  );
  assert.equal(exact.rule, 'exact');
  assert.equal(exact.ambiguous, false);
  assert.deepEqual(exact.entityKeys, ['anri']);

  const prefix = evidenceBindingOf(
    { id: 'e2', text: 'Anri 的笔记本放在桌上', entities: ['Anri 的'] },
    'anri',
  );
  assert.equal(prefix.rule, 'prefix', "'anri的' 以 'anri' 为完整词前缀，且正文有整词");
  assert.match(prefix.snippet, /anri/);
  assert.equal(prefix.ambiguous, false);

  const older = evidenceBindingOf(
    { id: 'e3', text: 'memory/memory.db 的 vec_items 共 5308 条 1024 维向量', entities: [] },
    'vec_items',
  );
  assert.equal(older.rule, 'text', '空实体列走正文整词兜底');
  assert.deepEqual(older.entityKeys, []);
  assert.equal(older.ambiguous, false);
  assert.match(older.snippet, /vec_items/);

  // 裸子串 / 有实体键只提正文：都不给依据（照旧拒绝）
  assert.equal(evidenceBindingOf({ id: 'g1', text: 'readonly 打开数据库', entities: [] }, 'read'), null);
  assert.equal(evidenceBindingOf({ id: 'g2', text: 'bash 跑了一遍测试', entities: ['dsh-ego'] }, 'bash'), null);
  assert.equal(evidenceBindingOf({ id: 'g3', text: 'treehouse 是树屋', entities: [] }, 'tree'), null);

  // 歧义：片段太短（正文只剩实体词本身）
  const tiny = evidenceBindingOf({ id: 'a1', text: 'vec_items', entities: [] }, 'vec_items');
  assert.equal(tiny.rule, 'text');
  assert.equal(tiny.ambiguous, true);
  assert.match(tiny.ambiguousReason, /snippet_too_short/);
  assert.ok('vec_items'.length - 'vec_items'.length < BINDING_MIN_LOCATE_CONTEXT);

  // 歧义：2 个字符的整词命中太容易撞
  const short = evidenceBindingOf({ id: 'a2', text: '这里提到 pi 这台机器', entities: [] }, 'pi');
  assert.equal(short.ambiguous, true);
  assert.match(short.ambiguousReason, /word_too_short/);

  // 选证据时命中项要把依据一起带出来
  const hit = selectEntityEvidence(
    [{ id: 'e4', text: 'memory/memory.db 的 vec_items 共 5308 条', entities: [], type: 'world' }],
    { entityKey: 'vec_items' },
  ).items[0];
  assert.equal(hit.basis.rule, 'text');
  assert.match(hit.basis.snippet, /vec_items/);
});

test('句子级顺带提及：否定/排除/对比/只修 X 降级为待复核，正向命中不受影响（t-mwszo23a-iwxs）', () => {
  // t6 给的例子：Redis 只是被拿来对照，不是这句在讲的主题。
  const negated = evidenceBindingOf(
    { id: 'n1', text: '本轮只修水泵，未检查 Redis。', entities: [] },
    'redis',
  );
  assert.equal(negated.rule, 'text');
  assert.equal(negated.ambiguous, true, '否定 + 只修范围：不直接放行');
  assert.match(negated.ambiguousReason, /negated_mention/);
  assert.match(negated.ambiguousReason, /scope_limited_mention/);

  // 排除语气：「除 Redis 外都正常」也不是在讲 Redis 本身。
  const excluded = evidenceBindingOf(
    { id: 'n2', text: '除 Redis 外，其余服务都没重启。', entities: [] },
    'redis',
  );
  assert.equal(excluded.ambiguous, true);
  assert.match(excluded.ambiguousReason, /negated_mention/);

  // 对比语气：而非 / 而不是。
  const contrast = evidenceBindingOf(
    { id: 'n3', text: '队列用的是 Redis，而非 Memcached。', entities: [] },
    'redis',
  );
  assert.equal(contrast.ambiguous, true);
  assert.match(contrast.ambiguousReason, /negated_mention/);

  // 正向命中：同一句在讲 Redis，判据一个字都不放宽也不误伤。
  const positive = evidenceBindingOf(
    { id: 'p1', text: 'Redis 的内存占用涨到 2GB，值班员已扩容。', entities: [] },
    'redis',
  );
  assert.equal(positive.ambiguous, false, '正常主题命中照旧直接放行');
  assert.equal(positive.ambiguousReason, '');

  // 断句：否定在上一句，不影响下一句的整词命中。
  const nextSentence = evidenceBindingOf(
    { id: 'p2', text: '水泵没有异响。Redis 内存占用 2GB。', entities: [] },
    'redis',
  );
  assert.equal(nextSentence.ambiguous, false, '否定词不在命中的那句里，不该误伤');

  // 一句里多个候选实体词：候选集一起传时算歧义；不传候选集时行为与以前一致。
  const record = { id: 'm1', text: 'alpha 和 beta 都在同一段正文里，分不清这条证据说的是谁', entities: [] };
  const one = evidenceBindingOf(record, 'alpha');
  assert.equal(one.ambiguous, false, '单看一个实体键（不传候选集）不判多候选');
  const multi = evidenceBindingOf(record, 'alpha', { candidates: ['alpha', 'beta'] });
  assert.equal(multi.ambiguous, true);
  assert.match(multi.ambiguousReason, /multiple_entities_in_sentence/);

  // selectEntityEvidence 把候选集透传下去：命中项带出来的 basis 也得是歧义。
  const hit = selectEntityEvidence(
    [{ ...record, type: 'world', status: 'active' }],
    { entityKey: 'alpha', candidateNorms: ['alpha', 'beta'] },
  ).items[0];
  assert.equal(hit.basis.ambiguous, true);
  assert.match(hit.basis.ambiguousReason, /multiple_entities_in_sentence/);
});

test('句子判据精度：否定不跨分句、「除…外还有」不算排除（t6 六条历史证据）', () => {
  // m_mw68mbxl_7qy0 同形：vec_items 在讲统计，「没有向量」是另一个分句在讲 orphan 条。
  const stats = evidenceBindingOf(
    { id: 'h1', text: '实测定簇：memory/memory.db 的 vec_items 共 5308 条 1024 维向量，另有 3 条 orphan 没有向量。', entities: [] },
    'vec_items',
  );
  assert.equal(stats.rule, 'text');
  assert.equal(stats.ambiguous, false, '同句别的分句里的否定不许借到命中上');

  // m_mw6wqdv0_yr0e 同形：「除 memories 表外还有 vec_items」是补充列举，不是排除。
  const enumerated = evidenceBindingOf(
    { id: 'h2', text: 'memory.db 中除 memories 表外还有 vec_items 向量表与 vec_map（rowid 对应记忆 id）；库里没有 FOREIGN KEY。', entities: [] },
    'vec_items',
  );
  assert.equal(enumerated.ambiguous, false, '「除 X 外还有 Y」是在讲 Y，不许当排除');

  // m_mw3j1rsi_phwx 同形：否定在别的分句，duplicate 这句本身是主题。
  const dup = evidenceBindingOf(
    { id: 'h3', text: 'store.js 的 mergeMemories 调写闸时没传 excludeIds，合并结果必然和自己的成员撞 DUPLICATE（22/22 全灭）。', entities: [] },
    'duplicate',
  );
  assert.equal(dup.rule, 'text');
  assert.equal(dup.ambiguous, false, '否定在另一个分句，不误伤 duplicate 命中');

  // m_mwqwi7bj_cajm 同形：命中分句干净，「根本没有」在冒号后的另一分句。
  const dup2 = evidenceBindingOf(
    { id: 'h4', text: '报这个错是因为走 DUPLICATE 分支、库里已有等价条」的解释对不上：这四组库里根本没有等价新条。', entities: [] },
    'duplicate',
  );
  assert.equal(dup2.ambiguous, false);

  // m_mw3j1ry3_n3y5 阴性对照：正文里没有 duplicate 整词，照旧拒绝（basis=null）。
  assert.equal(evidenceBindingOf(
    { id: 'h5', text: '改成 merge 时把本批全部源 ids（含被合并的旧 id 和 supersedes 点名的 id）都放进 excludeIds。', entities: [] },
    'duplicate',
  ), null);
});
