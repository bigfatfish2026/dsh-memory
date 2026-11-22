/**
 * 三期实体整理。
 *
 * 钉五件事：
 *   1) 判定口径：折叠同名（连字符/下划线/空格/大小写）算「确定同义」自动合；
 *      前缀/版本剥壳这类看着像但不敢保证的，只报不合并；
 *   2) dry-run 一个字节都不写（实体状态、surface、挂载、entity_merge 全不变）；
 *   3) apply：迁 entity_surface 挂接 + 迁 memory_entity 挂载 + 清空 src 挂载 + src 置 merged +
 *      dst 的 mem_count 按实数重算 + 每次合并一行 entity_merge 凭据；
 *   4) undo：按 entity_merge 原样搬回去（surface 与挂载都回到 src，src 回 active）；
 *   5) 缓存失效：合并后 entity_surface_epoch 变了，entity-gate 的 surface 缓存必须重读。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import {
  scanEntityTidy, applyEntityTidy, undoEntityMerge, undoEntityTidyRun, writeEntityTidyReport, runEntityTidyPass,
} from '../lib/entity-tidy.js';
import { loadSurfaceCache } from '../lib/entity-gate.js';

const DDL = `
CREATE TABLE entities (id INTEGER PRIMARY KEY, norm TEXT NOT NULL, display TEXT NOT NULL,
  etype TEXT NOT NULL DEFAULT 'unknown', aliases_note TEXT NOT NULL DEFAULT '',
  attrs TEXT NOT NULL DEFAULT '{}', first_seen TEXT NOT NULL DEFAULT '', last_seen TEXT NOT NULL DEFAULT '',
  mem_count INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'active', UNIQUE (norm));
CREATE TABLE memory_entity (memory_id TEXT NOT NULL, entity_id INTEGER NOT NULL, role TEXT NOT NULL DEFAULT 'about',
  ts TEXT NOT NULL DEFAULT '', status TEXT NOT NULL DEFAULT 'active', attrs TEXT NOT NULL DEFAULT '{}',
  version INTEGER NOT NULL DEFAULT 0, prev_memory_id TEXT NOT NULL DEFAULT '', origin TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (memory_id, entity_id, role)) WITHOUT ROWID;
CREATE TABLE entity_surface (surface TEXT PRIMARY KEY, norm TEXT NOT NULL, entity_id INTEGER NOT NULL,
  source TEXT NOT NULL CHECK (source IN ('migrate','gate','coarse','extract','model','manual','text')),
  count INTEGER NOT NULL DEFAULT 0, first_seen TEXT NOT NULL DEFAULT '', last_seen TEXT NOT NULL DEFAULT '',
  flag TEXT NOT NULL DEFAULT '', UNIQUE (norm, surface)) WITHOUT ROWID;
CREATE TABLE entity_merge (id INTEGER PRIMARY KEY AUTOINCREMENT, src_entity INTEGER NOT NULL,
  dst_entity INTEGER NOT NULL, reason TEXT NOT NULL, evidence TEXT NOT NULL DEFAULT '{}',
  run_id TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL, undone_at TEXT NOT NULL DEFAULT '');
CREATE UNIQUE INDEX uq_entity_merge_src ON entity_merge(src_entity) WHERE undone_at='';
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);`;

function freshDb() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-tidy-'));
  const db = new DatabaseSync(path.join(dir, 'memory.db'));
  db.exec(DDL);
  db.exec("INSERT INTO meta(key,value) VALUES ('entity_schema_version','2')");
  return db;
}

function seed(db) {
  const ent = db.prepare('INSERT INTO entities(norm,display,etype,mem_count) VALUES (?,?,?,?)');
  ent.run('majsoul-watch', 'majsoul-watch', 'project', 2);
  ent.run('majsoul_watch', 'majsoul_watch', 'project', 1);
  ent.run('ha mcp', 'HA MCP', 'service', 1);
  ent.run('home assistant', 'Home Assistant', 'service', 4);
  ent.run('home-assistant-addon', 'home assistant addon', 'service', 1); // 只报（prefix）
  const surf = db.prepare(`INSERT INTO entity_surface(surface,norm,entity_id,source,count,first_seen,last_seen)
    VALUES (?,?,?,'gate',1,'2026-11-01','2026-11-01')`);
  surf.run('majsoul-watch', 'majsoul-watch', 1);
  surf.run('majsoul_watch', 'majsoul_watch', 2);
  surf.run('HA MCP', 'ha mcp', 3);
  const me = db.prepare('INSERT INTO memory_entity(memory_id,entity_id,role,ts,status,origin) VALUES (?,?,?,?,?,?)');
  me.run('m_1', 1, 'about', '', 'active', 'rule');
  me.run('m_2', 1, 'about', '', 'active', 'rule');
  me.run('m_3', 2, 'about', '', 'active', 'rule');
  me.run('m_4', 3, 'about', '', 'active', 'rule');
  me.run('m_5', 4, 'about', '', 'active', 'rule');
}

test('扫描：折叠同名进自动合，前缀/版本只报不合并', () => {
  const db = freshDb();
  seed(db);
  const { auto, report, stats } = scanEntityTidy(db);
  const folded = auto.find((g) => g.entities.some((e) => e.id === 1));
  assert.ok(folded, 'majsoul-watch / majsoul_watch 必须进自动合组');
  assert.deepEqual(folded.entities.map((e) => e.id).sort(), [1, 2]);
  assert.equal(folded.dst.id, 1, 'mem_count 大的当 dst');
  assert.deepEqual(folded.basis, ['fold_same']);
  assert.equal(folded.surfaceMoves, 1);
  assert.equal(folded.mountMoves, 1);
  assert.equal(stats.autoGroups, 1);
  // 收紧后 'home assistant addon' 连只报都不进（tail='addon' 不是分隔/数字收尾）；
  // 够格的前缀例在最后一条测试里钉。
  assert.ok(!auto.some((g) => g.entities.some((e) => e.id === 5)), '前缀候选不许进自动合');
  db.close();
});

test('dry-run 零写：实体状态、surface、挂载、entity_merge 一个字节不动', () => {
  const db = freshDb();
  seed(db);
  const { auto } = scanEntityTidy(db);
  const before = {
    status: db.prepare('SELECT id,status,mem_count FROM entities ORDER BY id').all().map((r) => ({ ...r })),
    surface: db.prepare('SELECT surface,entity_id FROM entity_surface ORDER BY surface').all().map((r) => ({ ...r })),
    mounts: db.prepare('SELECT memory_id,entity_id FROM memory_entity ORDER BY memory_id').all().map((r) => ({ ...r })),
    merges: db.prepare('SELECT count(*) AS n FROM entity_merge').get().n,
  };
  const out = applyEntityTidy(db, { plan: auto, dryRun: true });
  const after = {
    status: db.prepare('SELECT id,status,mem_count FROM entities ORDER BY id').all().map((r) => ({ ...r })),
    surface: db.prepare('SELECT surface,entity_id FROM entity_surface ORDER BY surface').all().map((r) => ({ ...r })),
    mounts: db.prepare('SELECT memory_id,entity_id FROM memory_entity ORDER BY memory_id').all().map((r) => ({ ...r })),
    merges: db.prepare('SELECT count(*) AS n FROM entity_merge').get().n,
  };
  assert.deepEqual(after, before);
  assert.equal(out.merged, 0);
  db.close();
});

test('apply：迁 surface 与挂载、src 退场、dst mem_count 重算、写 entity_merge 凭据', () => {
  const db = freshDb();
  seed(db);
  const { auto } = scanEntityTidy(db);
  const out = applyEntityTidy(db, { plan: auto, runId: 't1', now: 1794700000000 });
  assert.equal(out.merged, 1);
  assert.equal(out.surfaceMoved, 1);
  assert.equal(out.mountsMoved, 1);

  const src = db.prepare('SELECT id,status,mem_count FROM entities WHERE id=2').get();
  assert.equal(src.status, 'merged');
  assert.equal(Number(src.mem_count), 0, 'src 清空后 mem_count 归零');
  const dstSurfaces = db.prepare('SELECT surface,entity_id FROM entity_surface ORDER BY surface').all()
    .map((r) => `${r.surface}|${r.entity_id}`);
  assert.deepEqual(dstSurfaces, ['HA MCP|3', 'majsoul-watch|1', 'majsoul_watch|1'], 'surface 挂接全部指到 dst');
  const mounts = db.prepare('SELECT memory_id,entity_id FROM memory_entity ORDER BY memory_id').all()
    .map((r) => `${r.memory_id}|${r.entity_id}`);
  assert.deepEqual(mounts, ['m_1|1', 'm_2|1', 'm_3|1', 'm_4|3', 'm_5|4']);
  const dst = db.prepare('SELECT mem_count FROM entities WHERE id=1').get();
  assert.equal(Number(dst.mem_count), 3, 'dst mem_count 按 memory_entity 实数重算');
  const merges = db.prepare('SELECT src_entity,dst_entity,reason,run_id FROM entity_merge').all();
  assert.equal(merges.length, 1);
  assert.equal(Number(merges[0].src_entity), 2);
  assert.equal(Number(merges[0].dst_entity), 1);
  assert.equal(merges[0].reason, 'rule:fold_same');
  assert.equal(merges[0].run_id, 't1');
  db.close();
});

test('undo：按 entity_merge 把 surface 与挂载搬回 src，src 回 active', () => {
  const db = freshDb();
  seed(db);
  const { auto } = scanEntityTidy(db);
  applyEntityTidy(db, { plan: auto, runId: 't2' });
  const undo = undoEntityMerge(db);
  assert.equal(undo.ok, true);
  const src = db.prepare('SELECT status,mem_count FROM entities WHERE id=2').get();
  assert.equal(src.status, 'active');
  assert.equal(Number(src.mem_count), 1);
  const surfaces = db.prepare('SELECT surface,entity_id FROM entity_surface ORDER BY surface').all()
    .map((r) => `${r.surface}|${r.entity_id}`);
  assert.deepEqual(surfaces, ['HA MCP|3', 'majsoul-watch|1', 'majsoul_watch|2'], 'surface 回 src');
  const mounts = db.prepare('SELECT memory_id,entity_id FROM memory_entity ORDER BY memory_id').all()
    .map((r) => `${r.memory_id}|${r.entity_id}`);
  assert.deepEqual(mounts, ['m_1|1', 'm_2|1', 'm_3|2', 'm_4|3', 'm_5|4'], '挂载回 src');
  assert.equal(String(db.prepare("SELECT value FROM meta WHERE key='entity_surface_epoch'").get()?.value || ''), '2', 'undo 也要 bump epoch');
  const undone = db.prepare('SELECT count(*) AS n FROM entity_merge WHERE undone_at<>\'\'').get().n;
  assert.equal(Number(undone), 1);
  assert.equal(undoEntityMerge(db).ok, false, '同一个 src 撤完就没有可撤的了');
  db.close();
});

// 票 t-mwogaszr-3h6t：memory_entity.status 曾长期写空串（写侧字面量 ''）。
// 读侧口径统一成 COALESCE(NULLIF(status,''),'active')，搬挂载时不许把空串搬成新行。
const emptyStatusRows = (db) => Number(db.prepare("SELECT count(*) AS n FROM memory_entity WHERE status=''").get()?.n || 0);

test('老行 status 空串：合并搬挂载时按 active 搬，evidence 里也不留空串', () => {
  const db = freshDb();
  seed(db);
  db.prepare("UPDATE memory_entity SET status='' WHERE entity_id=2").run();
  assert.equal(emptyStatusRows(db), 1, '先确认样本确实是空串');

  const { auto } = scanEntityTidy(db);
  const out = applyEntityTidy(db, { plan: auto, runId: 't-empty' });
  assert.equal(out.merged, 1);
  assert.equal(emptyStatusRows(db), 0, "搬完不许还有 status=''");
  const moved = db.prepare("SELECT status FROM memory_entity WHERE entity_id=1 AND memory_id='m_3'").get();
  assert.equal(moved.status, 'active', '搬过去的行 status 读作 active');

  const evidence = JSON.parse(String(db.prepare('SELECT evidence FROM entity_merge').get().evidence));
  const statuses = evidence.mounts.map((m) => m.status);
  assert.deepEqual(statuses, ['active'], '快照里记 active，不记空串');
  db.close();
});

test('老 evidence 里 status 是空串：undo 写回时补成 active', () => {
  const db = freshDb();
  seed(db);
  const { auto } = scanEntityTidy(db);
  applyEntityTidy(db, { plan: auto, runId: 't-legacy' });

  // 模拟修复前落下的凭据：evidence.mounts 里的 status 是空串快照
  const row = db.prepare('SELECT id, evidence FROM entity_merge').get();
  const ev = JSON.parse(String(row.evidence));
  ev.mounts = ev.mounts.map((m) => ({ ...m, status: '' }));
  db.prepare('UPDATE entity_merge SET evidence=? WHERE id=?').run(JSON.stringify(ev), Number(row.id));

  const undo = undoEntityMerge(db);
  assert.equal(undo.ok, true);
  assert.equal(emptyStatusRows(db), 0, 'undo 不许把空串搬回来');
  const back = db.prepare("SELECT status FROM memory_entity WHERE entity_id=2 AND memory_id='m_3'").get();
  assert.equal(back.status, 'active', '搬回 src 的行 status 是 active');
  db.close();
});

test('缓存失效：entity_surface_epoch 变了，surface 缓存必须重读', () => {
  const db = freshDb();
  seed(db);
  const first = loadSurfaceCache(db, { force: true });
  assert.equal(first.byNorm.get('majsoul_watch').entityId, 2);
  const { auto } = scanEntityTidy(db);
  applyEntityTidy(db, { plan: auto });
  // apply 自己会把 epoch +1（不用测试手工写）
  assert.equal(String(db.prepare("SELECT value FROM meta WHERE key='entity_surface_epoch'").get()?.value || ''), '1');
  const second = loadSurfaceCache(db);
  assert.notEqual(second, first, '版本变了必须返回新缓存');
  assert.equal(second.byNorm.get('majsoul_watch').entityId, 1, '合并后 norm 指向保留实体');
  db.close();
});

test('脏名字不进自动合：点文件/命令行开关/路径都不算「同一个写法」', () => {
  const db = freshDb();
  const ent = db.prepare('INSERT INTO entities(norm,display,etype,mem_count) VALUES (?,?,?,?)');
  ent.run('dsh', 'dsh', 'tool', 10);
  ent.run('.dsh', '.dsh', 'file', 3);
  ent.run('apply', 'apply', 'concept', 2);
  ent.run('--apply', '--apply', 'config', 2);
  ent.run('github', 'GitHub', 'org', 9);
  ent.run('example-org/.github', 'Example-org/.github', 'repo', 4);
  const surf = db.prepare(`INSERT INTO entity_surface(surface,norm,entity_id,source,count,first_seen,last_seen,flag)
    VALUES (?,?,?,'gate',1,'','','')`);
  surf.run('GitHub', 'github', 5);
  surf.run('.github', '.github', 6); // Basename 折叠行：指向 repo，但不许当交叉证据
  const { auto } = scanEntityTidy(db);
  assert.equal(auto.length, 0, '.dsh/--apply/.github 都不能凑成自动合组');
  db.close();
});

test('只报规则收紧：单复数要精确，前缀要有分隔或数字收尾', () => {
  const db = freshDb();
  const ent = db.prepare('INSERT INTO entities(norm,display,etype,mem_count) VALUES (?,?,?,?)');
  ent.run('cordis', 'cordis', 'tool', 1);
  ent.run('cordis.patch.yml', 'cordis.patch.yml', 'file', 1);
  ent.run('timer', 'timer', 'service', 1);
  ent.run('timers', 'timers', 'service', 1);
  ent.run('chat', 'chat', 'tool', 1);
  ent.run('chatroom', 'chatroom', 'tool', 1);
  const { report } = scanEntityTidy(db);
  const pair = (a, b) => report.find((r) => (r.a.display === a && r.b.display === b) || (r.a.display === b && r.b.display === a));
  assert.equal(pair('cordis', 'cordis.patch.yml'), undefined, 'cordis vs cordis.patch.yml 不是单复数');
  assert.equal(pair('timer', 'timers')?.basis, 'plural', 'timer/timers 才是精确单复数');
  assert.equal(pair('chat', 'chatroom'), undefined, 'chat 是 chatroom 的前缀但不是同一族，不能报');
  db.close();
});

test('版本剥壳与相似度：version / similar 都只进候选清单', () => {
  const db = freshDb();
  const ent = db.prepare('INSERT INTO entities(norm,display,etype,mem_count) VALUES (?,?,?,?)');
  ent.run('python 3.11', 'python 3.11', 'tool', 1);
  ent.run('python 3.12', 'python 3.12', 'tool', 1);
  ent.run('abcdefghij', 'abcdefghij', 'concept', 1);
  ent.run('abcdefghik', 'abcdefghik', 'concept', 1);
  const { report, auto } = scanEntityTidy(db);
  assert.equal(auto.length, 0);
  assert.equal(report.find((r) => r.a.display === 'python 3.11' && r.b.display === 'python 3.12')?.basis, 'version');
  assert.equal(report.find((r) => r.a.display === 'abcdefghij' && r.b.display === 'abcdefghik')?.basis, 'similar');
  db.close();
});

test('大小写/分隔符变体：折叠同名合成一组，历史 norm 写法不同也只算一次', () => {
  const db = freshDb();
  const ent = db.prepare('INSERT INTO entities(norm,display,etype,mem_count) VALUES (?,?,?,?)');
  // 历史行 norm 写法不同（'hugging-face' vs 'hugging face'）：折叠同名先连上，
  // 后续任何规则再撞同一对都必须跳过（union.find 相等）。
  ent.run('hugging-face', 'Hugging Face', 'org', 5);
  ent.run('hugging face', 'hugging face', 'org', 1);
  const { auto } = scanEntityTidy(db);
  assert.equal(auto.length, 1);
  assert.deepEqual(auto[0].entities.map((e) => e.id).sort((a, b) => a - b), [1, 2]);
  assert.ok(auto[0].basis.includes('fold_same'));
  db.close();
});

test('库缺表也扫得动：没有 entity_surface / memory_entity 时按空处理，apply 只跳过不乱写', () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-tidy-broken-'));
  const db = new DatabaseSync(path.join(dir, 'memory.db'));
  db.exec(`CREATE TABLE entities (id INTEGER PRIMARY KEY, norm TEXT NOT NULL, display TEXT NOT NULL,
    etype TEXT NOT NULL DEFAULT 'unknown', aliases_note TEXT NOT NULL DEFAULT '', attrs TEXT NOT NULL DEFAULT '{}',
    first_seen TEXT NOT NULL DEFAULT '', last_seen TEXT NOT NULL DEFAULT '', mem_count INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'active', UNIQUE (norm));`);
  db.prepare('INSERT INTO entities(norm,display) VALUES (?,?)').run('a-b', 'a-b');
  db.prepare('INSERT INTO entities(norm,display) VALUES (?,?)').run('a_b', 'a_b');
  const scan = scanEntityTidy(db);
  assert.equal(scan.stats.surfaceRows, 0);
  assert.equal(scan.stats.autoGroups, 1, '实体表自己够判折叠同名');
  const applied = applyEntityTidy(db, { plan: scan.auto });
  assert.equal(applied.merged, 0);
  assert.equal(applied.skipped, 1, '缺 memory_entity 表时一条都落不了，记跳过不抛');
  const pass = runEntityTidyPass(db, { apply: true });
  assert.ok(pass.applied, 'apply=true 时也要返回 applied 账');
  assert.equal(pass.applied.merged, 0);
  const pass2 = runEntityTidyPass(db, { apply: false });
  assert.equal(pass2.applied, null, '默认只报不合');
  db.close();
});

test('apply 遇到不存在的 dst 只跳过；undo 支持指定 mergeId，库坏了返回 ok:false', () => {
  const db = freshDb();
  seed(db);
  const bad = applyEntityTidy(db, {
    plan: [{ dst: { id: 999 }, basis: ['fold_same'], planned: [{ src: 2, dst: 999 }] }],
  });
  assert.equal(bad.merged, 0);
  assert.equal(bad.skipped, 1);

  const { auto } = scanEntityTidy(db);
  applyEntityTidy(db, { plan: auto });
  const mergeId = db.prepare('SELECT id FROM entity_merge ORDER BY id DESC LIMIT 1').get().id;
  db.exec('DROP TABLE entity_surface');
  const failed = undoEntityMerge(db, { mergeId });
  assert.equal(failed.ok, false, 'surface 表没了，undo 要回滚并报失败');
  assert.equal(undoEntityMerge(db, { mergeId: 123456 }).reason, 'no_merge');
  db.close();
});

test('surface 自己就是实体本名时不算交叉证据（other.id === owner）', () => {
  const db = freshDb();
  db.prepare('INSERT INTO entities(norm,display,etype,mem_count) VALUES (?,?,?,?)').run('github', 'GitHub', 'org', 9);
  db.prepare(`INSERT INTO entity_surface(surface,norm,entity_id,source,count,first_seen,last_seen,flag)
    VALUES ('GitHub','github',1,'gate',5,'','','')`).run();
  const { auto } = scanEntityTidy(db);
  assert.equal(auto.length, 0, '自己的 surface 不能把自己和另一个自己连起来');
  db.close();
});

test('merge 落库中途失败：事务回滚、这一组记跳过，不抛给调度器', () => {
  const db = freshDb();
  seed(db);
  const { auto } = scanEntityTidy(db);
  // 用触发器让「写撤销凭据」这一步炸掉：模拟落库中途失败（比缺表更靠后，能走到 catch）
  db.exec("CREATE TRIGGER boom BEFORE INSERT ON entity_merge BEGIN SELECT RAISE(ABORT, 'boom'); END;");
  const out = applyEntityTidy(db, { plan: auto });
  assert.equal(out.merged, 0);
  assert.equal(out.skipped, 1);
  const src = db.prepare('SELECT status FROM entities WHERE id=2').get();
  assert.equal(src.status, 'active', '失败必须回滚，src 不能留在 merged');
  const surface = db.prepare("SELECT entity_id FROM entity_surface WHERE surface='majsoul_watch'").get();
  assert.equal(Number(surface.entity_id), 2, '回滚后 surface 挂接不能留在 dst');
  db.close();
});

test('entity_merge 表缺失：apply 直接按跳过返回，不抛', () => {
  const db = freshDb();
  seed(db);
  const { auto } = scanEntityTidy(db);
  db.exec('DROP TABLE entity_merge');
  const out = applyEntityTidy(db, { plan: auto });
  assert.equal(out.merged, 0);
  assert.equal(out.skipped, 1);
  const src = db.prepare('SELECT status FROM entities WHERE id=2').get();
  assert.equal(src.status, 'active', '失败必须回滚，src 不能留在 merged');
  db.close();
});

test('数字间分隔符保护：Python-3.15.1 不折成 python3151，只报 version 不自动合', () => {
  const db = freshDb();
  const ent = db.prepare('INSERT INTO entities(norm,display,etype,mem_count) VALUES (?,?,?,?)');
  ent.run('python', 'python', 'tool', 10);
  ent.run('python-3.15.1', 'Python-3.15.1', 'tool', 2);
  db.prepare(`INSERT INTO entity_surface(surface,norm,entity_id,source,count,first_seen,last_seen,flag)
    VALUES ('Python 3.15.1','python',1,'gate',1,'','','')`).run();
  const { auto, report } = scanEntityTidy(db);
  assert.equal(auto.length, 0, '3.15.1 是版本号，抹掉分隔符合进去就再也分不开');
  const pair = report.find((r) => ['python', 'Python-3.15.1'].includes(r.a.display)
    && ['python', 'Python-3.15.1'].includes(r.b.display));
  assert.equal(pair?.basis, 'version', '只报 version，给人看');
  db.close();
});

test('字段兜底：count/flag/display 为空或 NULL 也不炸', () => {
  const db = freshDb();
  const ent = db.prepare('INSERT INTO entities(norm,display,etype,mem_count) VALUES (?,?,?,?)');
  ent.run('a-b', 'a-b', 'tool', 1);
  ent.run('a_b', 'a_b', 'tool', 1);
  // 脏名（纯分隔符）走 cleanName 否分支：单列一条，别和 UNIQUE(norm) 抢同一把
  db.prepare("INSERT INTO entities(norm,display,etype,mem_count) VALUES ('---','---','unknown',0)").run();
  // count/flag/first_seen/last_seen 全走列默认值（只在 INSERT 里给必填列）
  db.prepare("INSERT INTO entity_surface(surface,norm,entity_id,source) VALUES ('a b','a b',1,'gate')").run();
  const scan = scanEntityTidy(db);
  assert.equal(scan.stats.autoGroups, 1);
  assert.equal(scan.stats.surfaceRows, 1);
  db.close();
});

// ── 2026-11-15：nightly 自动合并 + 可撤回 + 汇报 ──

test('按批撤回：undo-run 只撤指定 run_id，别批原地不动，重复撤报 no_merge', () => {
  const db = freshDb();
  seed(db);
  const a = scanEntityTidy(db);
  const outA = applyEntityTidy(db, { plan: a.auto, runId: 'batch-a' });
  assert.equal(outA.merged, 1);
  assert.equal(outA.pairs.length, 1, 'apply 要把合并对报给调用方');
  assert.ok(outA.pairs[0].mergeId > 0, '每对要带 entity_merge.id，撤回靠它');
  assert.equal(outA.pairs[0].src, 2);
  assert.equal(outA.pairs[0].dstName, 'majsoul-watch');

  // 第二批：把 3/4 的 display 折成同名，再合一次，证明撤一批不会伤到另一批
  db.prepare("UPDATE entities SET display='homeassistant' WHERE id=3").run();
  db.prepare("UPDATE entities SET display='Home-Assistant' WHERE id=4").run();
  const b = scanEntityTidy(db);
  const outB = applyEntityTidy(db, { plan: b.auto, runId: 'batch-b' });
  assert.equal(outB.merged, 1);
  // dst 按 memory_entity 真数选（两边都是 1 条时取 id 小的 3），所以这批是 4 → 3
  assert.equal(outB.pairs[0].src, 4);
  assert.equal(outB.pairs[0].dst, 3);

  const undoA = undoEntityTidyRun(db, { runId: 'batch-a' });
  assert.equal(undoA.ok, true);
  assert.equal(undoA.undone, 1);
  assert.equal(undoA.mergeIds.length, 1);
  assert.equal(db.prepare('SELECT status FROM entities WHERE id=2').get().status, 'active');
  assert.equal(db.prepare('SELECT status FROM entities WHERE id=4').get().status, 'merged', 'batch-b 的合并不许被动');
  assert.equal(undoEntityTidyRun(db, { runId: 'batch-a' }).reason, 'no_merge', '同一批撤完再撤要老实说没有');
  assert.equal(undoEntityTidyRun(db, { runId: '' }).reason, 'run_id_required');
  assert.equal(undoEntityTidyRun(db, { runId: 'batch-b' }).undone, 1);
  db.close();
});

test('报告：写清本轮合并对、跳过候选、按批撤回命令与「不动正文」口径', () => {
  const db = freshDb();
  seed(db);
  const scan = scanEntityTidy(db);
  const applied = applyEntityTidy(db, { plan: scan.auto, runId: 'sleep_2026-11-15', now: 1794700000000 });
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-tidy-report-'));
  const file = path.join(dir, 'entity-tidy-report.md');
  const written = writeEntityTidyReport(file, { runId: 'sleep_2026-11-15', scan, applied, now: 1794700000000 });
  assert.equal(written, file);
  const md = fs.readFileSync(file, 'utf8');
  assert.match(md, /run_id：sleep_2026-11-15/);
  assert.match(md, /本轮实际合并 1 次/);
  assert.match(md, /majsoul_watch/);
  assert.match(md, /--undo-run sleep_2026-11-15/);
  assert.match(md, /跳过（只报不合并）/);
  assert.match(md, /没有「合并正文 token 校验」/, '实体整理不动 memories 正文，口径要写实');
  db.close();
});

test('runEntityTidyPass：apply=true 但 dryRun=true 时零写库（睡觉 dry-run 不许真合）', () => {
  const db = freshDb();
  seed(db);
  const pass = runEntityTidyPass(db, { apply: true, dryRun: true, runId: 'dry' });
  assert.equal(pass.applied, null);
  assert.equal(db.prepare('SELECT status FROM entities WHERE id=2').get().status, 'active');
  assert.equal(Number(db.prepare('SELECT count(*) AS n FROM entity_merge').get().n), 0);
  db.close();
});
