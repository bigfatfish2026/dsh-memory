/**
 * 回填 / 迁移脚本写 memory_entity.status 的口径。
 *
 * 票 t-mwogaszr-3h6t：写侧一直把 status 写成空串，存量长到 3 万多行；读侧只要有人写
 * `status='active'` 就静默漏行。这里钉住 scripts/migrate-entities.mjs 的口径：
 *   - memories.status 是 NULL / ''（活跃）→ memory_entity.status 落 'active'；
 *   - archived / merged 原样保留，绝不许被改成 active（改错等于把死关联算活）。
 *
 * 脚本是 top-level 执行的（不是可 import 的模块），所以用子进程跑 `--db <临时库>`。
 * 全程 tmpdir，不碰真库。
 */
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';

const ROOT = path.join(import.meta.dirname, '..');
const SCRIPT = path.join(ROOT, 'scripts/migrate-entities.mjs');

const DDL = `
CREATE TABLE memories (
  id TEXT PRIMARY KEY, text TEXT NOT NULL DEFAULT '', tags TEXT, source TEXT, ts TEXT, type TEXT,
  entities TEXT, status TEXT, rowSeq INTEGER NOT NULL DEFAULT 0);
CREATE TABLE entities (
  id INTEGER PRIMARY KEY, norm TEXT NOT NULL UNIQUE, display TEXT NOT NULL,
  etype TEXT NOT NULL DEFAULT 'unknown', attrs TEXT NOT NULL DEFAULT '{}',
  first_seen TEXT NOT NULL DEFAULT '', last_seen TEXT NOT NULL DEFAULT '',
  mem_count INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'active');
CREATE TABLE memory_entity (
  memory_id TEXT NOT NULL, entity_id INTEGER NOT NULL, role TEXT NOT NULL DEFAULT 'about',
  ts TEXT NOT NULL DEFAULT '', status TEXT NOT NULL DEFAULT '', attrs TEXT NOT NULL DEFAULT '{}',
  version INTEGER NOT NULL DEFAULT 0, prev_memory_id TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (memory_id, entity_id, role)) WITHOUT ROWID;
`;

function makeDb(rows) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-status-migrate-'));
  const dbPath = path.join(dir, 'memory.db');
  const db = new DatabaseSync(dbPath);
  db.exec(DDL);
  const ins = db.prepare('INSERT INTO memories(id, text, ts, entities, status) VALUES (?, ?, ?, ?, ?)');
  for (const [id, ts, entities, status] of rows) {
    ins.run(id, `记忆 ${id} 的正文`, ts, JSON.stringify(entities), status);
  }
  db.close();
  return dbPath;
}

function runMigrate(dbPath) {
  return execFileSync(process.execPath, [SCRIPT, '--db', dbPath], { cwd: ROOT, encoding: 'utf8' });
}

test('migrate：NULL / 空串 status 落 active，archived / merged 原样保留', () => {
  const dbPath = makeDb([
    ['m_null', '2026-11-01T00:00:00.000Z', ['cordis'], null],
    ['m_empty', '2026-11-02T00:00:00.000Z', ['jellyfin'], ''],
    ['m_archived', '2026-11-03T00:00:00.000Z', ['navidrome'], 'archived'],
    ['m_merged', '2026-11-04T00:00:00.000Z', ['navidrome'], 'merged'],
  ]);

  const out = runMigrate(dbPath);
  const stats = JSON.parse(out);
  assert.equal(stats.inserted, 4, '四条记忆各落一条挂载');

  const db = new DatabaseSync(dbPath, { readOnly: true });
  const rows = db.prepare('SELECT memory_id, status FROM memory_entity ORDER BY memory_id').all()
    .map((r) => [String(r.memory_id), String(r.status)]);
  db.close();

  assert.deepEqual(rows, [
    ['m_archived', 'archived'],
    ['m_empty', 'active'],
    ['m_merged', 'merged'],
    ['m_null', 'active'],
  ], '空值转 active，archived / merged 一个都不许动');
});

test('migrate 重跑幂等：已存在的挂载不被改成 active', () => {
  const dbPath = makeDb([
    ['m_a', '2026-11-05T00:00:00.000Z', ['cordis'], ''],
    ['m_b', '2026-11-06T00:00:00.000Z', ['home assistant'], 'archived'],
  ]);
  runMigrate(dbPath);

  // 手工把空串那条改成 merged，再跑一轮：INSERT OR IGNORE 不许把已有行改回来
  const db = new DatabaseSync(dbPath);
  db.prepare("UPDATE memory_entity SET status='merged' WHERE memory_id='m_a'").run();
  db.close();
  runMigrate(dbPath);

  const again = new DatabaseSync(dbPath, { readOnly: true });
  const byId = Object.fromEntries(
    again.prepare('SELECT memory_id, status FROM memory_entity').all().map((r) => [String(r.memory_id), String(r.status)]),
  );
  again.close();
  assert.equal(byId.m_a, 'merged', '重跑只补新增，不许覆盖已有行的状态');
  assert.equal(byId.m_b, 'archived');
});
