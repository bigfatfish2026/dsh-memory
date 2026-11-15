/**
 * 存储事务/计数审计修复。
 *
 * 钉四件事：
 *   1. mergeMemories 整笔一笔事务：ids 先过滤，不存在的 id 不会中途抛；
 *      事务中途失败整笔回滚，不留「新条已落、源条还 active」的半截；
 *   2. 合并/删挂载时 entities.mem_count 和 entity_surface.count 跟着 -1，不虚高；
 *   3. 删挂载按 memory_id 全删（rule / agent 一起清），正文改写不留旧来源的挂载；
 *   4. 同一 (memory_id, entity_id) 多 role 只落一行，计数按记忆数不按行数。
 *
 * 全程 tmpdir 假库，不碰真库。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { createStore } from '../lib/store.js';
import { applyCurateSchema } from '../scripts/apply-curate-schema.mjs';
import { insertMemoryEntity, deleteMemoryEntity, upsertEntity } from '../lib/entity-gate.js';

const VEC_SO = path.join(import.meta.dirname, '../node_modules/sqlite-vec-linux-arm64/vec0.so');

const MEMORY_DDL = `CREATE TABLE memories (
  id TEXT PRIMARY KEY, text TEXT NOT NULL, tags TEXT, source TEXT, ts TEXT, type TEXT,
  entities TEXT, validFrom TEXT, supersedes TEXT, updatedAt TEXT, status TEXT, reason TEXT,
  supersededBy TEXT, validTo TEXT, confidence REAL, extra TEXT, present TEXT, rowSeq INTEGER NOT NULL
);`;

const ENTITY_DDL = `
CREATE TABLE entities (
  id INTEGER PRIMARY KEY, norm TEXT NOT NULL, display TEXT NOT NULL,
  etype TEXT NOT NULL DEFAULT 'unknown', aliases_note TEXT NOT NULL DEFAULT '',
  attrs TEXT NOT NULL DEFAULT '{}', first_seen TEXT NOT NULL DEFAULT '',
  last_seen TEXT NOT NULL DEFAULT '', mem_count INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'active', UNIQUE (norm));
CREATE TABLE memory_entity (
  memory_id TEXT NOT NULL, entity_id INTEGER NOT NULL, role TEXT NOT NULL DEFAULT 'about',
  ts TEXT NOT NULL DEFAULT '', status TEXT NOT NULL DEFAULT 'active',
  attrs TEXT NOT NULL DEFAULT '{}', version INTEGER NOT NULL DEFAULT 0,
  prev_memory_id TEXT NOT NULL DEFAULT '', PRIMARY KEY (memory_id, entity_id, role)) WITHOUT ROWID;
CREATE TABLE entity_surface (
  surface TEXT PRIMARY KEY, norm TEXT NOT NULL, entity_id INTEGER NOT NULL,
  source TEXT NOT NULL CHECK (source IN ('migrate','gate','coarse','extract','model','manual','text')),
  count INTEGER NOT NULL DEFAULT 0, first_seen TEXT NOT NULL DEFAULT '',
  last_seen TEXT NOT NULL DEFAULT '', flag TEXT NOT NULL DEFAULT '',
  UNIQUE (norm, surface)) WITHOUT ROWID;
CREATE INDEX idx_surface_norm ON entity_surface(norm);
CREATE TABLE entity_issue (
  memory_id TEXT NOT NULL DEFAULT '', entity_norm TEXT NOT NULL, raw_name TEXT NOT NULL DEFAULT '',
  reason TEXT NOT NULL, detail TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL,
  PRIMARY KEY (memory_id, entity_norm, reason)) WITHOUT ROWID;
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
INSERT INTO meta(key, value) VALUES ('dim', '4');
`;

function makeStore() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-audit-store-'));
  const dbPath = path.join(dir, 'memory.db');
  const db = new DatabaseSync(dbPath, { allowExtension: true });
  db.loadExtension(VEC_SO);
  db.exec(MEMORY_DDL);
  db.exec(ENTITY_DDL);
  db.exec('CREATE VIRTUAL TABLE vec_items USING vec0(embedding float[4] distance_metric=cosine);');
  db.exec('CREATE TABLE vec_map (rowid INTEGER PRIMARY KEY, id TEXT);');
  // 粗实体词典只放「树莓派」：save 时同步写 memory_entity + surface
  db.prepare('INSERT INTO entities(norm, display, etype) VALUES (?, ?, ?)').run('树莓派', '树莓派', 'device');
  db.close();
  applyCurateSchema(dbPath, { logger: { info() {}, warn() {} } });

  const cfg = {
    storePath: dir,
    storeLogFile: path.join(dir, 'store.log'),
    topK: 8,
    minScore: 0.45,
    relativeGap: 0.1,
    rrfK: 60,
    entityQueue: { enqueue: false, genericDf: 0.05, genericDfMin: 20, coarseMaxWords: 4000, coarseBudgetMs: 3 },
    embed: async (texts) => texts.map(() => [1, 0, 0, 0]),
    logger: { info() {}, warn() {} },
    pipeline: { writeGate: false, textLint: false },
  };
  return { store: createStore(cfg), cfg, dir, dbPath };
}

function rows(dbPath, sql, ...params) {
  const db = new DatabaseSync(dbPath, { readOnly: true });
  try {
    return db.prepare(sql).all(...params);
  } finally {
    db.close();
  }
}

const count = (dbPath, sql, ...params) => Number(rows(dbPath, sql, ...params)[0]?.n ?? 0);

test('merge：ids 里有不存在的先过滤，过滤后不足 2 条直接抛，一笔不落', async () => {
  const { store, dbPath } = makeStore();
  const a = await store.save('树莓派温度 45 度。', ['infra'], 'agent', { type: 'world' });
  const before = count(dbPath, 'SELECT count(*) AS n FROM memories');

  await assert.rejects(
    () => store.merge([a.id, 'm_not_exist'], '树莓派温度 45 度，风扇 3000 转。', ['infra'], {}),
    /merge needs at least 2 existing ids/,
  );
  assert.equal(count(dbPath, 'SELECT count(*) AS n FROM memories'), before, '过滤后不足 2 条不许写新条');
  assert.equal(
    count(dbPath, "SELECT count(*) AS n FROM memories WHERE status='merged'"),
    0,
    '源条一条都不许被标 merged',
  );

  // 2 条真 + 1 条不存在：不抛，只合并真的两条（不存在的那条不参与）
  const b = await store.save('树莓派风扇转速 3000 转。', ['infra'], 'agent', { type: 'world' });
  const rec = await store.merge([a.id, b.id, 'm_not_exist'], '树莓派温度 45 度，风扇 3000 转。', ['infra'], {});
  assert.ok(rec.id);
  assert.deepEqual(
    rows(dbPath, "SELECT id, status FROM memories WHERE status='merged' ORDER BY id").map((r) => r.id).sort(),
    [a.id, b.id].sort(),
  );
});

test('merge：中途抛错整笔回滚，新条不落、源条仍 active', async () => {
  const { store, dbPath } = makeStore();
  const a = await store.save('树莓派温度 45 度。', ['infra'], 'agent', { type: 'world' });
  const b = await store.save('树莓派风扇转速 3000 转。', ['infra'], 'agent', { type: 'world' });
  const before = count(dbPath, 'SELECT count(*) AS n FROM memories');
  const beforeVec = count(dbPath, 'SELECT count(*) AS n FROM vec_map');

  // 源条转 merged 的那一步炸：整笔事务必须回滚（新条、向量、状态全不落）
  const db = store.db();
  db.exec(`CREATE TRIGGER boom_merge BEFORE UPDATE ON memories
           WHEN NEW.status='merged' BEGIN SELECT RAISE(ABORT, 'boom'); END;`);
  await assert.rejects(() => store.merge([a.id, b.id], '树莓派温度 45 度，风扇 3000 转。', ['infra'], {}), /boom/);
  db.exec('DROP TRIGGER boom_merge');

  assert.equal(count(dbPath, 'SELECT count(*) AS n FROM memories'), before, '回滚后不加新条');
  assert.equal(count(dbPath, 'SELECT count(*) AS n FROM vec_map'), beforeVec, '回滚后向量表也不加行');
  assert.equal(
    count(dbPath, "SELECT count(*) AS n FROM memories WHERE id IN (?, ?) AND coalesce(status,'active')='active'", a.id, b.id),
    2,
    '源条还是 active',
  );
  assert.equal(count(dbPath, "SELECT count(*) AS n FROM memories WHERE status='merged'"), 0);
});

test('merge：源条挂载清掉，mem_count / entity_surface.count 同步 -1', async () => {
  const { store, dbPath } = makeStore();
  const a = await store.save('树莓派温度 45 度。', ['infra'], 'agent', { type: 'world' });
  const b = await store.save('树莓派风扇转速 3000 转。', ['infra'], 'agent', { type: 'world' });
  const ent = rows(dbPath, "SELECT id FROM entities WHERE norm='树莓派'")[0].id;
  assert.equal(count(dbPath, 'SELECT mem_count AS n FROM entities WHERE id=?', ent), 2, '两条记忆各挂一次');
  assert.equal(count(dbPath, 'SELECT count AS n FROM entity_surface WHERE surface=?', '树莓派'), 2);

  const rec = await store.merge([a.id, b.id], '树莓派温度 45 度，风扇 3000 转。', ['infra'], {});
  assert.ok(rec.id);
  assert.equal(
    count(dbPath, 'SELECT count(*) AS n FROM memory_entity WHERE memory_id IN (?, ?)', a.id, b.id),
    0,
    '合并掉的源条不再挂实体',
  );
  // 合并结果自带 entities 列，挂载等异步 entity_extract 补；这里只验源条那两次 -1 落干净
  assert.equal(count(dbPath, 'SELECT mem_count AS n FROM entities WHERE id=?', ent), 0, '两条源条的 +1 各减一次');
  assert.equal(count(dbPath, 'SELECT count AS n FROM entity_surface WHERE surface=?', '树莓派'), 0, 'surface 计数跟着减两次');
});

test('删挂载：默认按 memory_id 全删，rule / agent 一起清，计数各 -1', () => {
  const { store, dbPath } = makeStore();
  const db = store.db();
  const rule = upsertEntity(db, { norm: 'rule-ent', display: 'rule-ent', etype: 'tool', countSurface: false });
  const agent = upsertEntity(db, { norm: 'agent-ent', display: 'agent-ent', etype: 'tool', countSurface: false });
  insertMemoryEntity(db, { memoryId: 'm1', entityId: rule.id, origin: 'rule' });
  insertMemoryEntity(db, { memoryId: 'm1', entityId: agent.id, origin: 'agent' });

  assert.equal(deleteMemoryEntity(db, { memoryId: 'm1' }), 2, '不传 origin = 全删');
  assert.equal(count(dbPath, 'SELECT count(*) AS n FROM memory_entity WHERE memory_id=?', 'm1'), 0);
  for (const id of [rule.id, agent.id]) {
    assert.equal(count(dbPath, 'SELECT mem_count AS n FROM entities WHERE id=?', id), 0);
    assert.equal(count(dbPath, 'SELECT count AS n FROM entity_surface WHERE entity_id=?', id), 0);
  }
});

test('正文改写：旧 agent 挂载一起清，不留新正文上没有的实体', async () => {
  const { store, dbPath } = makeStore();
  const m = await store.save('树莓派温度 45 度。', ['infra'], 'agent', { type: 'world' });
  const db = store.db();
  const extra = upsertEntity(db, { norm: 'mikan', display: 'mikan', etype: 'service', countSurface: false });
  insertMemoryEntity(db, { memoryId: m.id, entityId: extra.id, origin: 'agent' });

  await store.update(m.id, { text: '磁盘还剩百分之十五的空间。' });
  assert.equal(
    count(dbPath, "SELECT count(*) AS n FROM memory_entity WHERE memory_id=? AND entity_id=?", m.id, extra.id),
    0,
    '正文改成没有 mikan 之后，agent 挂载也要清',
  );
  assert.equal(count(dbPath, 'SELECT mem_count AS n FROM entities WHERE id=?', extra.id), 0);
});

test('(memory_id, entity_id) 去重：多 role 只落一行，计数按记忆数', () => {
  const { store, dbPath } = makeStore();
  const db = store.db();
  const ent = upsertEntity(db, { norm: 'jellyfin', display: 'jellyfin', etype: 'service', countSurface: false });

  assert.equal(insertMemoryEntity(db, { memoryId: 'm1', entityId: ent.id, role: 'about', origin: 'agent' }), true);
  assert.equal(
    insertMemoryEntity(db, { memoryId: 'm1', entityId: ent.id, role: 'mentioned', origin: 'agent' }),
    false,
    '同记忆同实体换 role 不再插第二行',
  );
  assert.equal(count(dbPath, 'SELECT count(*) AS n FROM memory_entity WHERE memory_id=? AND entity_id=?', 'm1', ent.id), 1);
  assert.equal(count(dbPath, 'SELECT mem_count AS n FROM entities WHERE id=?', ent.id), 1);
  assert.equal(count(dbPath, 'SELECT count AS n FROM entity_surface WHERE entity_id=?', ent.id), 1);

  assert.equal(deleteMemoryEntity(db, { memoryId: 'm1' }), 1);
  assert.equal(count(dbPath, 'SELECT mem_count AS n FROM entities WHERE id=?', ent.id), 0, '删一条记忆只 -1，不按 role 行数减');
  assert.equal(count(dbPath, 'SELECT count AS n FROM entity_surface WHERE entity_id=?', ent.id), 0);
});
