import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { applySchema, verifySchema, TABLES } from '../scripts/apply-pipeline-schema.mjs';

function tmpDb() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-schema-'));
  const dbPath = path.join(dir, 'memory.db');
  const db = new DatabaseSync(dbPath);
  db.exec('CREATE TABLE memories (id TEXT PRIMARY KEY, text TEXT NOT NULL);');
  db.exec('CREATE TABLE vec_map (rowid INTEGER PRIMARY KEY, id TEXT);');
  db.exec("INSERT INTO memories (id, text) VALUES ('m_1', 'hello');");
  db.exec("INSERT INTO vec_map (rowid, id) VALUES (1, 'm_1');");
  db.close();
  return { dir, dbPath };
}

test('dry-run 只打印 DDL，不建表', () => {
  const { dbPath } = tmpDb();
  const lines = [];
  const result = applySchema(dbPath, { dryRun: true, logger: (line) => lines.push(line) });
  assert.equal(result.applied, false);
  const db = new DatabaseSync(dbPath, { readOnly: true });
  const names = db.prepare("SELECT name FROM sqlite_master WHERE type='table'").all().map((r) => r.name);
  db.close();
  for (const table of TABLES) assert.equal(names.includes(table), false);
  assert.ok(lines.some((line) => line.includes('CREATE TABLE IF NOT EXISTS ingest_files')));
});

test('真实执行建三张表与索引，二次执行幂等', () => {
  const { dbPath } = tmpDb();
  const first = applySchema(dbPath, { logger: () => {} });
  assert.equal(first.applied, true);

  let report = verifySchema(dbPath);
  assert.equal(report.integrity, 'ok');
  for (const table of TABLES) assert.equal(report.tables[table].exists, true);
  assert.ok(report.tables.ingest_files.columns.some((c) => c.startsWith('last_acked_ts')));
  assert.ok(report.tables.pending_ops.columns.some((c) => c.startsWith('next_try_at')));
  assert.ok(report.tables.discards.indexes.includes('idx_discards_hash'));
  assert.ok(report.tables.ingest_files.indexes.includes('idx_ingest_status'));
  assert.ok(report.tables.pending_ops.indexes.includes('idx_pending_due'));

  const db = new DatabaseSync(dbPath);
  db.exec("INSERT INTO ingest_files (path, mtime, size, updated_ts) VALUES ('/tmp/a', 1, 2, 3);");
  db.close();

  const second = applySchema(dbPath, { logger: () => {} });
  assert.equal(second.applied, true);
  report = verifySchema(dbPath);
  assert.equal(report.tables.ingest_files.rows, 1);
  assert.equal(report.tables.pending_ops.rows, 0);
  assert.equal(report.tables.discards.rows, 0);
  assert.equal(report.integrity, 'ok');
});

test('verify 能指出缺表', () => {
  const { dbPath } = tmpDb();
  const report = verifySchema(dbPath);
  for (const table of TABLES) assert.equal(report.tables[table].exists, false);
});
