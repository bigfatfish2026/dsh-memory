/**
 * 审计 not_in_source 新判据。
 *
 * 旧判据只查本条记忆正文，但实体名来自上下文别段是正常行为（实测 97% 误报）。
 * 新判据：该实体所有挂载记忆的正文里都找不到这个名字才记 issue；
 * 名字在它任意一条挂载记忆里出现过就算有出处。
 *
 * 走真实 CLI 的 --dry-run（只读），输出里的 wouldIssue.not_in_source 是唯一键
 * (memory_id, entity_norm) 计数，跟 --apply 会写的行数同口径。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { spawnSync } from 'node:child_process';
import { DatabaseSync } from 'node:sqlite';

const AUDIT = path.join(import.meta.dirname, '../scripts/audit-entities.mjs');

function makeDb(rows) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-audit-src-'));
  const dbPath = path.join(dir, 'memory.db');
  const db = new DatabaseSync(dbPath);
  db.exec('CREATE TABLE memories (id TEXT PRIMARY KEY, text TEXT NOT NULL, entities TEXT, status TEXT);');
  const ins = db.prepare('INSERT INTO memories(id, text, entities, status) VALUES (?, ?, ?, ?)');
  for (const r of rows) ins.run(r.id, r.text, JSON.stringify(r.entities), r.status ?? '');
  db.close();
  return dbPath;
}

function dryRun(dbPath) {
  const r = spawnSync(process.execPath, [AUDIT, '--db', dbPath, '--dry-run'], { encoding: 'utf8' });
  assert.equal(r.status, 0, r.stderr);
  return JSON.parse(r.stdout);
}

test('名字在别的挂载记忆正文里出现过就算有出处，不记 issue', () => {
  // m1 正文有 jellyfin；m2 也挂了 jellyfin 但正文没提它——旧判据会误报 m2 这一条
  const dbPath = makeDb([
    { id: 'm1', text: 'jellyfin 在 8096 端口跑着', entities: ['jellyfin'] },
    { id: 'm2', text: '这条只是上下文，没提那个服务', entities: ['jellyfin'] },
  ]);
  const out = dryRun(dbPath);
  assert.equal(out.wouldIssue.not_in_source, 0, '另一条挂载记忆有出处就不算违规');
  assert.equal(out.sourceFound.viaMounts, 1, 'm2 这一条是靠挂载记忆找到出处的');
});

test('该实体所有挂载记忆正文都找不到时才记 issue', () => {
  const dbPath = makeDb([
    { id: 'm1', text: 'jellyfin 在 8096 端口跑着', entities: ['jellyfin'] },
    { id: 'm2', text: '这条只是上下文，没提那个服务', entities: ['jellyfin', 'qqbot'] },
    { id: 'm3', text: '完全无关的一段话', entities: ['navidrome'] },
  ]);
  const out = dryRun(dbPath);
  // qqbot 两边正文都没有、navidrome 也一样；按 (memory_id, norm) 唯一键算 2 行
  assert.equal(out.wouldIssue.not_in_source, 2);
});
