/**
 * 关键日志文件出口。
 *
 * 钉住三件事：
 *   1. keyLog 往 cfg.sleepLogFile 写一行，行首是 [ISO 时间戳]，追加不覆盖；
 *   2. 目录不存在会自己建；
 *   3. 「拼 prompt 窗口」那行（进程内一次）确实同时落文件，不只进 ctx.logger。
 *
 * 全程 tmpdir，不碰真库、不碰真实日志目录。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { DEFAULT_KEY_LOG_FILE, keyLog, keyLogFile } from '../lib/file-log.js';
import { logWindowOnce } from '../lib/token-window.js';
import { upsertEntity } from '../lib/entity-gate.js';

function tmpCfg(name = 'memory.log') {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-keylog-'));
  return { dir, file: path.join(dir, name), cfg: { sleepLogFile: path.join(dir, name) } };
}

function linesOf(file) {
  return fs.readFileSync(file, 'utf8').split('\n').filter(Boolean);
}

test('keyLog 写一行到 cfg 指定的文件，带 ISO 时间戳', () => {
  const { file, cfg } = tmpCfg();
  assert.equal(keyLogFile(cfg), file, '出口就是 cfg.sleepLogFile');
  assert.equal(keyLog(cfg, '关键行 hello'), true);

  const lines = linesOf(file);
  assert.equal(lines.length, 1, '文件里就该有一行');
  assert.match(lines[0], /^\[\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z\] 关键行 hello$/);
});

test('keyLog 追加不覆盖，目录不存在会自己建', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-keylog-'));
  const file = path.join(dir, 'nested', 'deeper', 'memory.log');
  const cfg = { sleepLogFile: file };
  assert.equal(keyLog(cfg, '第一行'), true);
  assert.equal(keyLog(cfg, '第二行'), true);

  const lines = linesOf(file);
  assert.equal(lines.length, 2);
  assert.match(lines[0], /第一行$/);
  assert.match(lines[1], /第二行$/);
});

test('没配 sleepLogFile 时退回默认路径常量', () => {
  assert.equal(keyLogFile({}), DEFAULT_KEY_LOG_FILE);
  assert.equal(keyLogFile(null), DEFAULT_KEY_LOG_FILE);
});

test('「拼 prompt 窗口」那行进文件，不只进 ctx.logger', () => {
  const { file, cfg } = tmpCfg();
  const pluginCfg = { ...cfg, contextWindowTokens: 1000 };
  const logged = [];
  const fakeCtx = { logger: { warn: (line) => logged.push(line) } };

  const tokens = logWindowOnce(fakeCtx, pluginCfg, null);
  assert.equal(tokens, 1000);
  assert.equal(logged.length, 1, 'ctx.logger 照旧拿到这行');
  assert.match(logged[0], /拼 prompt 窗口：1000 token（来源 contextWindowTokens）/);

  const lines = linesOf(file);
  assert.equal(lines.length, 1, '文件里也有一行');
  assert.match(lines[0], /^\[\d{4}-\d{2}-\d{2}T[\d:.]+Z\] dsh-memory 拼 prompt 窗口：1000 token（来源 contextWindowTokens）$/);
});

const SURFACE_DDL = `
CREATE TABLE entities (
  id INTEGER PRIMARY KEY, norm TEXT NOT NULL, display TEXT NOT NULL,
  etype TEXT NOT NULL DEFAULT 'unknown', aliases_note TEXT NOT NULL DEFAULT '',
  attrs TEXT NOT NULL DEFAULT '{}', first_seen TEXT NOT NULL DEFAULT '',
  last_seen TEXT NOT NULL DEFAULT '', mem_count INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'active', UNIQUE (norm));
CREATE TABLE entity_issue (
  memory_id TEXT NOT NULL DEFAULT '', entity_norm TEXT NOT NULL, raw_name TEXT NOT NULL DEFAULT '',
  reason TEXT NOT NULL, detail TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL,
  PRIMARY KEY (memory_id, entity_norm, reason)) WITHOUT ROWID;
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
INSERT INTO meta(key, value) VALUES ('entity_schema_version', '2');
CREATE TABLE entity_surface (
  surface TEXT PRIMARY KEY, norm TEXT NOT NULL, entity_id INTEGER NOT NULL,
  source TEXT NOT NULL CHECK (source IN ('migrate','gate','coarse','extract','model','manual','text')),
  count INTEGER NOT NULL DEFAULT 0, first_seen TEXT NOT NULL DEFAULT '',
  last_seen TEXT NOT NULL DEFAULT '', flag TEXT NOT NULL DEFAULT '',
  UNIQUE (norm, surface)) WITHOUT ROWID;
`;

function surfaceDb() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-keylog-surface-'));
  const db = new DatabaseSync(path.join(dir, 'memory.db'));
  db.exec(SURFACE_DDL);
  return db;
}

test('surface_conflict 落到注入的日志出口', () => {
  const db = surfaceDb();
  db.prepare("INSERT INTO entities(id, norm, display, etype) VALUES (1, 'x', 'x', 'device')").run();
  db.prepare("INSERT INTO entities(id, norm, display, etype) VALUES (2, 'x-other', 'x', 'device')").run();
  db.prepare("INSERT INTO entity_surface(surface, norm, entity_id, source, count) VALUES ('x', 'x', 1, 'migrate', 1)").run();
  db.prepare("INSERT INTO entity_surface(surface, norm, entity_id, source, count) VALUES ('x-old', 'x', 2, 'migrate', 1)").run();

  const lines = [];
  upsertEntity(db, { norm: 'x', display: 'x-new', etype: 'device', log: (line) => lines.push(line) });
  assert.ok(lines.some((l) => l.includes('[surface] conflict norm=x')), `没打出冲突行：${lines.join(' | ')}`);
});

test('entity_surface 读不到时，加载失败也走注入的日志出口', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-keylog-nosurface-'));
  const db = new DatabaseSync(path.join(dir, 'memory.db'));
  db.exec(SURFACE_DDL.replace(/CREATE TABLE entity_surface[\s\S]*?WITHOUT ROWID;/, ''));

  const lines = [];
  assert.throws(() => upsertEntity(db, { norm: 'y', display: 'y', etype: 'device', log: (line) => lines.push(line) }));
  assert.ok(lines.some((l) => l.includes('[surface] 加载失败')), `没打出加载失败行：${lines.join(' | ')}`);
});
