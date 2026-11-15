/**
 * 实体名录 + apply-lexicon。
 *
 * 钉三件事：
 *   1. data/entity-lexicon.example.json 是五类结构，每条 name/aliases/etype/note 齐全，etype 与分区一致；
 *   2. classifyEntity 先查名录（norm 大小写不敏感、别名能命中主名），形态正则只作兜底；
 *   3. apply-lexicon 只改 unknown，已是具体类型的跳过，真跑后 integrity ok 且可重复跑（幂等）。
 *
 * 仓库里只带示例名录；私人名录 data/entity-lexicon.json 不进 git，测试只认示例。
 */
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';

const ROOT = path.resolve(import.meta.dirname, '..');
const LEXICON = path.join(ROOT, 'data', 'entity-lexicon.example.json');
// 固定用示例名录，免得本机私人名录把断言拖偏。
process.env.DSH_MEMORY_LEXICON = LEXICON;
const { classifyEntity, foldForMatch, loadLexicon, normalizeEntity } = await import('../lib/shared/entity-normalize.js');
const SCRIPT = path.join(ROOT, 'scripts', 'apply-lexicon.mjs');

const etypeOf = (raw) => normalizeEntity(raw).etype;

test('名录结构：六类分区齐全，每项字段齐全且 etype 与分区一致', () => {
  const doc = JSON.parse(fs.readFileSync(LEXICON, 'utf8'));
  const types = ['person', 'account', 'org', 'repo', 'service', 'project'];
  for (const t of types) {
    assert.ok(Array.isArray(doc[t]), `${t} 分区缺失`);
    for (const it of doc[t]) {
      assert.equal(typeof it.name, 'string');
      assert.ok(it.name.length > 0);
      assert.ok(Array.isArray(it.aliases));
      assert.equal(it.etype, t);
      assert.equal(typeof it.note, 'string');
      if (t === 'account') assert.ok(['github', 'bilibili', 'email'].includes(it.platform), `${it.name} 缺 platform`);
    }
  }
  const total = types.reduce((n, t) => n + doc[t].length, 0);
  assert.equal(total, 3, '示例名录只留三条虚构条目');
});

test('名录优先：命中直接拿名录 etype，别名与大小写都能命中', () => {
  assert.equal(etypeOf('张三'), 'person');
  assert.equal(etypeOf('示例人物'), 'person'); // 别名
  assert.equal(etypeOf('示例站点'), 'org');
  assert.equal(etypeOf('EXAMPLE.COM'), 'org'); // 别名 + 大小写
  assert.equal(etypeOf('example@example.com'), 'account'); // 别名
});

test('名录不抢兜底：没登记的仍走形态正则/unknown，型号名不进名录', () => {
  assert.equal(classifyEntity('dsh-memory'), 'tool');
  assert.equal(classifyEntity('cordis.patch.yml'), 'config');
  assert.equal(classifyEntity('t-abcdefgh-1234'), 'ticket');
  assert.equal(classifyEntity('deepseek-flash'), 'unknown');
  assert.equal(classifyEntity('bge-m3'), 'unknown');
  assert.equal(classifyEntity('qwen38-coder390:q6_k'), 'unknown');
  assert.equal(classifyEntity('qwen3.8'), 'repo'); // REPOS 兜底，不靠名录
});

test('名录读不到时安静退化成空表，不抛错', () => {
  const empty = loadLexicon(path.join(ROOT, 'data', 'no-such-lexicon.json'));
  assert.equal(empty.size, 0);
  const seeded = loadLexicon(LEXICON);
  assert.ok(seeded.get(foldForMatch('张三')) === 'person');
});

function tmpDb(rows) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-lexicon-'));
  const dbPath = path.join(dir, 'memory.db');
  const db = new DatabaseSync(dbPath);
  db.exec(`CREATE TABLE entities (
    id INTEGER PRIMARY KEY, norm TEXT NOT NULL, display TEXT NOT NULL,
    etype TEXT NOT NULL DEFAULT 'unknown'
      CHECK (etype IN ('person','account','device','repo','file','service','ticket','tool','config','org','project','host','concept','unknown')),
    attrs TEXT NOT NULL DEFAULT '{}', status TEXT NOT NULL DEFAULT 'active', UNIQUE (norm));`);
  const stmt = db.prepare('INSERT INTO entities (norm, display, etype, status) VALUES (?, ?, ?, ?)');
  for (const r of rows) stmt.run(r.norm, r.display ?? r.norm, r.etype ?? 'unknown', 'active');
  db.close();
  return dbPath;
}

const runScript = (dbPath, extra = []) => JSON.parse(execFileSync(process.execPath, [SCRIPT, '--db', dbPath, '--lexicon', LEXICON, '--json', ...extra], {
  cwd: ROOT, encoding: 'utf8',
}));

test('apply-lexicon：dry-run 不写库，真跑只改 unknown，跳过已定型，可重复跑', () => {
  const dbPath = tmpDb([
    { norm: '张三', display: '张三' },
    { norm: '示例站点', display: '示例站点' },
    { norm: 'example@example.com', display: 'example@example.com' },
    { norm: 'deepseek-flash', display: 'deepseek-flash' },
    { norm: 'example', display: 'example', etype: 'project' },
  ]);

  const dry = runScript(dbPath, ['--dry-run']);
  assert.equal(dry.dryRun, true);
  assert.equal(dry.changed, 0);
  assert.equal(dry.toUpdate, 3);
  assert.equal(dry.skipped, 1);
  assert.equal(dry.after.unknown, 4); // dry-run 不动库
  assert.deepEqual(dry.toUpdateByType, { person: 1, org: 1, account: 1 });

  const real = runScript(dbPath);
  assert.equal(real.changed, 3);
  assert.equal(real.after.person, 1);
  assert.equal(real.after.org, 1);
  assert.equal(real.after.account, 1);
  assert.equal(real.after.project, 1); // example 原本就是 project
  assert.equal(real.after.unknown, 1); // deepseek-flash 不在名录，保持 unknown
  assert.equal(String(real.integrity).toLowerCase(), 'ok');

  const again = runScript(dbPath);
  assert.equal(again.changed, 0);
  assert.equal(again.toUpdate, 0);
  assert.equal(again.skipped, 4);

  // --force 才覆盖已有具体类型：example 从 project 纠正成 account，其余同类型不动
  const forced = runScript(dbPath, ['--force']);
  assert.equal(forced.force, true);
  assert.equal(forced.changed, 1);
  assert.equal(forced.forced, 1);
  assert.equal(forced.after.account, 2); // example@example.com + example
  assert.equal(forced.after.person, 1); // 张三
  assert.equal(forced.after.project ?? 0, 0);
  assert.equal(forced.after.unknown, 1);
  assert.equal(forced.skipped, 3);
  assert.deepEqual(forced.platformWrites, [{ norm: 'example', platform: 'email' }]);
  assert.equal(runScript(dbPath, ['--force']).changed, 0); // 再跑无差异

  const db = new DatabaseSync(dbPath);
  const ex = db.prepare("SELECT etype, attrs FROM entities WHERE norm='example'").get();
  assert.equal(ex.etype, 'account');
  assert.equal(JSON.parse(ex.attrs).platform, 'email');
  assert.equal(db.prepare("SELECT etype FROM entities WHERE norm='张三'").get().etype, 'person');
  assert.equal(db.prepare("SELECT etype FROM entities WHERE norm='deepseek-flash'").get().etype, 'unknown');
  db.close();
  fs.rmSync(path.dirname(dbPath), { recursive: true, force: true });
});
