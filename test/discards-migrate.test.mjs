import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import {
  missingVerdicts, migrateDiscardVerdicts, verifyDiscardVerdicts, VERDICTS,
} from '../scripts/migrate-discard-verdicts.mjs';

/** 老库的 discards：CHECK 只有 DUPLICATE / SUPERSEDES。 */
const OLD_DDL = `CREATE TABLE discards (
  id             INTEGER PRIMARY KEY AUTOINCREMENT,
  candidate_hash TEXT NOT NULL,
  text           TEXT NOT NULL,
  matched_id     TEXT,
  verdict        TEXT NOT NULL CHECK (verdict IN ('DUPLICATE','SUPERSEDES')),
  reason         TEXT,
  source_file    TEXT,
  created_ts     INTEGER NOT NULL
)`;

function makeOldDb() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-discard-mig-'));
  const dbPath = path.join(dir, 'memory.db');
  const db = new DatabaseSync(dbPath);
  db.exec(OLD_DDL);
  db.exec('CREATE INDEX idx_discards_hash ON discards(candidate_hash)');
  const insert = db.prepare(
    'INSERT INTO discards (candidate_hash, text, matched_id, verdict, reason, source_file, created_ts) VALUES (?, ?, ?, ?, ?, ?, ?)',
  );
  insert.run('h1', '树莓派温度 45 度，重复表述。', 'm_1', 'DUPLICATE', '同一事实', 'test', 1);
  insert.run('h2', '根分区 15%，旧条被新条取代。', 'm_2', 'SUPERSEDES', '新版本', 'test', 2);
  db.close();
  return { dir, dbPath };
}

test('老库 CHECK 缺 LINT_REJECT：迁移后能插进去，行原样保留', () => {
  const { dir, dbPath } = makeOldDb();
  assert.deepEqual(missingVerdicts(dbPath).missing, ['LINT_REJECT']);

  const result = migrateDiscardVerdicts(dbPath, { logger: () => {} });
  assert.equal(result.migrated, true);
  assert.equal(result.before.discards, 2);
  assert.equal(result.after.discards, 2);
  assert.equal(result.before.maxId, result.after.maxId);

  const db = new DatabaseSync(dbPath);
  db.prepare(
    'INSERT INTO discards (candidate_hash, text, verdict, reason, source_file, created_ts) VALUES (?, ?, ?, ?, ?, ?)',
  ).run('h3', '但是这段没通过 lint。', 'LINT_REJECT', 'leading_conjunction:x', 'lint_reject', 3);
  const rows = db.prepare('SELECT id, verdict, text FROM discards ORDER BY id').all();
  db.close();
  assert.equal(rows.length, 3);
  assert.deepEqual(rows.map((row) => row.verdict), ['DUPLICATE', 'SUPERSEDES', 'LINT_REJECT']);
  assert.equal(rows[2].text, '但是这段没通过 lint。');
  fs.rmSync(dir, { recursive: true, force: true });
});

test('迁移幂等：第二次跑 migrated=false，索引还在', () => {
  const { dir, dbPath } = makeOldDb();
  migrateDiscardVerdicts(dbPath, { logger: () => {} });
  const again = migrateDiscardVerdicts(dbPath, { logger: () => {} });
  assert.equal(again.migrated, false);
  const report = verifyDiscardVerdicts(dbPath);
  assert.deepEqual(report.missing, []);
  assert.equal(report.integrity, 'ok');
  assert.equal(report.rows, 2);
  assert.equal(report.indexes.includes('idx_discards_hash'), true);
  assert.equal(report.indexes.includes('idx_discards_matched'), true);
  fs.rmSync(dir, { recursive: true, force: true });
});

test('枚举与 DDL 口径一致：3 个 verdict，含 LINT_REJECT', () => {
  assert.deepEqual(VERDICTS, ['DUPLICATE', 'SUPERSEDES', 'LINT_REJECT']);
});
