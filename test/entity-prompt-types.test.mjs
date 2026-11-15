/**
 * 抽实体提示词的类型表必须跟 E_TYPES 一致。
 *
 * 旧提示词写死十类，模型永远给不出 host / project / account——E_TYPES 早扩到十四类，
 * 提示词没同步，这三类实体就特别少。这条测试盯住两边不脱节。
 */
import assert from 'node:assert/strict';
import test from 'node:test';
import { ENTITY_SYSTEM } from '../lib/agents/entity.js';
import { E_TYPES } from '../lib/shared/entity-normalize.js';

test('ENTITY_SYSTEM 覆盖 E_TYPES 十四类，host/project/account 都在', () => {
  assert.equal(E_TYPES.length, 14);
  for (const t of E_TYPES) {
    assert.ok(new RegExp(`^\\s*-\\s*${t}：`, 'm').test(ENTITY_SYSTEM), `提示词缺 ${t} 的判据行`);
  }
  for (const t of ['host', 'project', 'account']) {
    assert.ok(ENTITY_SYSTEM.includes(`- ${t}：`), `${t} 必须在提示词里`);
  }
  // 每类都带一句判据，不许只列名字
  assert.match(ENTITY_SYSTEM, /- host：网络地址/);
  assert.match(ENTITY_SYSTEM, /- project：跨多个仓/);
  assert.match(ENTITY_SYSTEM, /- unknown：/);
});
