/**
 * t-fix-first-run 第 5 处：真库守卫从「文件 sha + mtime」换成「逻辑指纹」。
 *
 * 关键证据用例：WAL 模式下写完不 checkpoint，主库文件大小和 mtime 一动不动，
 * 逻辑指纹却能看见新行——这正是文件 sha 守卫失效的那个洞。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { logicalFingerprint, diffFingerprint, fileInfo } from '../lib/db-fingerprint.js';

function tmpDb({ wal = false } = {}) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-fp-'));
  const dbPath = path.join(dir, 'memory.db');
  const db = new DatabaseSync(dbPath);
  if (wal) {
    db.exec('PRAGMA journal_mode=WAL');
    db.exec('PRAGMA wal_autocheckpoint=0');
  }
  db.exec(`CREATE TABLE memories (
    id TEXT PRIMARY KEY, text TEXT NOT NULL, rowSeq INTEGER NOT NULL,
    status TEXT, ts TEXT
  );`);
  db.exec('CREATE TABLE vec_map (rowid INTEGER PRIMARY KEY, id TEXT);');
  db.exec('CREATE TABLE ingest_files (dedupe_key TEXT UNIQUE, status TEXT);');
  db.exec('CREATE TABLE pending_ops (id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT);');
  db.exec('CREATE TABLE discards (id INTEGER PRIMARY KEY AUTOINCREMENT, verdict TEXT);');
  return { dir, dbPath, db };
}

const insert = (db, id, seq) => db
  .prepare('INSERT INTO memories (id, text, rowSeq, status, ts) VALUES (?, ?, ?, NULL, ?)')
  .run(id, `记忆 ${id}`, seq, '2026-11-01T00:00:00Z');

test('逻辑指纹：空库几个数都是 0，能取出 digest', () => {
  const { dbPath, db } = tmpDb();
  db.close();
  const fp = logicalFingerprint(dbPath);
  assert.equal(fp.error, undefined);
  assert.equal(fp.memories, 0);
  assert.equal(fp.vec_map, 0);
  assert.equal(fp.maxRowSeq, null);
  assert.equal(fp.ingest_files, 0);
  assert.equal(fp.pending_ops, 0);
  assert.equal(fp.discards, 0);
  assert.match(fp.digest, /^[0-9a-f]{16}$/);
});

test('逻辑指纹：插一行就变，diff 只说变了的字段', () => {
  const { dbPath, db } = tmpDb();
  db.close();
  const before = logicalFingerprint(dbPath);
  const w = new DatabaseSync(dbPath);
  insert(w, 'm_1', 2);
  w.close();
  const after = logicalFingerprint(dbPath);
  assert.equal(before.digest === after.digest, false);
  assert.deepEqual(diffFingerprint(before, after), ['memories: 0 → 1', 'maxRowSeq: null → 2']);
  assert.deepEqual(diffFingerprint(before, before), []);
});

test('逻辑指纹：vec_map / 三张新表任一变化都能抓到', () => {
  const { dbPath, db } = tmpDb();
  db.close();
  const before = logicalFingerprint(dbPath);
  const w = new DatabaseSync(dbPath);
  w.prepare('INSERT INTO vec_map (rowid, id) VALUES (1, ?)').run('m_1');
  w.prepare("INSERT INTO ingest_files (dedupe_key, status) VALUES ('k1', 'done')").run();
  w.prepare("INSERT INTO pending_ops (kind) VALUES ('split_item')").run();
  w.prepare("INSERT INTO discards (verdict) VALUES ('DUPLICATE')").run();
  w.close();
  const after = logicalFingerprint(dbPath);
  assert.deepEqual(diffFingerprint(before, after), [
    'vec_map: 0 → 1',
    'ingest_files: 0 → 1',
    'pending_ops: 0 → 1',
    'discards: 0 → 1',
  ]);
});

test('WAL 里写的新行：主库文件大小/mtime 不动，逻辑指纹却看得见（sha 守卫失效的那个洞）', () => {
  const { dbPath } = tmpDb({ wal: true });
  // 先落一个 checkpoint，让主库文件是"干净"的
  const setup = new DatabaseSync(dbPath);
  setup.exec('PRAGMA wal_checkpoint(TRUNCATE)');
  setup.close();
  const fileBefore = fileInfo(dbPath);
  const fpBefore = logicalFingerprint(dbPath);

  // 常驻连接写一行，不 checkpoint —— 模拟 qqbot 一边读一边被写
  const writer = new DatabaseSync(dbPath);
  writer.exec('PRAGMA wal_autocheckpoint=0');
  insert(writer, 'm_wal', 7);
  const fileAfter = fileInfo(dbPath);
  const fpAfter = logicalFingerprint(dbPath);
  writer.close();

  assert.equal(fileAfter.db.size, fileBefore.db.size, '主库文件大小一点没变');
  assert.equal(fileAfter.db.mtimeMs, fileBefore.db.mtimeMs, '主库 mtime 也一点没变');
  assert.ok(fileAfter.wal.size > 0, '-wal 里才是新写');
  assert.equal(fpBefore.memories, 0);
  assert.equal(fpAfter.memories, 1, 'READONLY 打开能看见 -wal 里的新行');
  assert.deepEqual(diffFingerprint(fpBefore, fpAfter), ['memories: 0 → 1', 'maxRowSeq: null → 7']);
});

test('逻辑指纹：打不开的库返回 error，不退回文件 sha 假装成功', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-fp-bad-'));
  const fp = logicalFingerprint(path.join(dir, 'nope.db'));
  assert.ok(fp.error);
  assert.equal(fp.digest, undefined);
  assert.deepEqual(diffFingerprint(fp, fp), [`无法比对：${fp.error}`]);
});

test('逻辑指纹：缺表记 null，不当成 0 行', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-fp-partial-'));
  const dbPath = path.join(dir, 'memory.db');
  const db = new DatabaseSync(dbPath);
  db.exec('CREATE TABLE memories (id TEXT PRIMARY KEY, text TEXT, rowSeq INTEGER);');
  db.exec('CREATE TABLE vec_map (rowid INTEGER PRIMARY KEY, id TEXT);');
  db.close();
  const fp = logicalFingerprint(dbPath);
  assert.equal(fp.memories, 0);
  assert.equal(fp.ingest_files, null);
  assert.equal(fp.pending_ops, null);
  assert.equal(fp.discards, null);
  assert.match(fp.canonical, /pending_ops=null/);
});
