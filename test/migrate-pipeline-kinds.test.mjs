import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { KINDS, migrateKinds, missingKinds, verifyKinds } from '../scripts/migrate-pipeline-kinds.mjs';

const OLD_PENDING_DDL = `CREATE TABLE pending_ops (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  kind         TEXT NOT NULL
               CHECK (kind IN ('ingest_file','merge_batch','split_item','dedupe_check')),
  dedupe_key   TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  attempts     INTEGER NOT NULL DEFAULT 0,
  next_try_at  INTEGER NOT NULL,
  last_error   TEXT,
  status       TEXT NOT NULL DEFAULT 'pending'
               CHECK (status IN ('pending','running','done','dead')),
  created_ts   INTEGER NOT NULL,
  updated_ts   INTEGER NOT NULL,
  UNIQUE(kind, dedupe_key)
);`;
const OLD_INDEX = 'CREATE INDEX idx_pending_due ON pending_ops(status, next_try_at, attempts);';

function oldDb() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-kindmig-'));
  const dbPath = path.join(dir, 'memory.db');
  const db = new DatabaseSync(dbPath);
  db.exec('CREATE TABLE memories (id TEXT PRIMARY KEY, text TEXT NOT NULL);');
  db.exec("INSERT INTO memories (id, text) VALUES ('m_1', '树莓派温度四十五度'), ('m_2', '风扇三千转');");
  db.exec(OLD_PENDING_DDL);
  db.exec(OLD_INDEX);
  const ins = db.prepare(`INSERT INTO pending_ops
    (kind, dedupe_key, payload_json, attempts, next_try_at, status, created_ts, updated_ts)
    VALUES (?, ?, ?, ?, ?, 'pending', 1, 1)`);
  ins.run('split_item', 'm_1', '{"source_id":"m_1"}', 2, 111);
  ins.run('dedupe_check', 'h1', '{"a":1}', 0, 222);
  db.close();
  return { dir, dbPath };
}

function snapshot(dbPath) {
  const db = new DatabaseSync(dbPath, { readOnly: true });
  try {
    return {
      ops: db.prepare('SELECT * FROM pending_ops ORDER BY id').all(),
      memories: db.prepare('SELECT * FROM memories ORDER BY id').all(),
    };
  } finally {
    db.close();
  }
}

test('迁移：老 CHECK 补上 merge_op，行原样保留，memories 一个字节不动', () => {
  const { dbPath } = oldDb();
  const before = snapshot(dbPath);
  assert.deepEqual(missingKinds(dbPath).missing, ['merge_op']);

  const result = migrateKinds(dbPath, { logger: () => {} });
  assert.equal(result.migrated, true);

  const after = snapshot(dbPath);
  assert.deepEqual(after.ops, before.ops, 'pending_ops 行（id/attempts/退避/payload）原样保留');
  assert.deepEqual(after.memories, before.memories, 'memories 未动');
  assert.equal(verifyKinds(dbPath).missing.length, 0);
  assert.equal(verifyKinds(dbPath).index, true);
  const db = new DatabaseSync(dbPath);
  assert.equal(db.prepare("SELECT sql FROM sqlite_master WHERE name='pending_ops'").get().sql.includes("'merge_op'"), true);
  db.close();
});

test('迁移幂等：第二次跑不重建，返回 migrated=false', () => {
  const { dbPath } = oldDb();
  migrateKinds(dbPath, { logger: () => {} });
  const second = migrateKinds(dbPath, { logger: () => {} });
  assert.equal(second.migrated, false);
  assert.equal(verifyKinds(dbPath).missing.length, 0);
});

test('KINDS 与 CHECK 完整覆盖 5 个 kind', () => {
  const { dbPath } = oldDb();
  migrateKinds(dbPath, { logger: () => {} });
  const report = verifyKinds(dbPath);
  assert.equal(report.missing.length, 0);
  assert.deepEqual(KINDS, ['ingest_file', 'merge_batch', 'split_item', 'dedupe_check', 'merge_op']);
  assert.equal(report.integrity, 'ok');
});
