/**
 * 整轮 sleepPass 对拍（2026-11-14，阶段 6 ①）。
 *
 * 同一份固定快照（多会话文件 / 超长条目 / 会被作废的旧说法 / 待重放的 merge_op+dedupe_check /
 * 互相冲突的实体），分别跑 legacySleepPass 与 pooledSleepPass，比较最终库状态。
 *
 * 白名单（本来就允许不同，逐条写清原因）：
 *   1) pending_ops 里的 ingest_file 行：pool 走「文件账本」，legacy 走 transcript 游标，设计不同；
 *   2) revocation_sweep 行：compact 通道是阶段 5 新加的，legacy 没有这条队列；
 *   3) split_item 行与拆出来的子段、源条的长文 archived 状态：两条 split 实现的校验严格度不同
 *      （同一份假模型输出，pool 的 consumeSplitOps 收、legacy 的 runSplitPass 因 evidence 校验拒），
 *      属于既有实现差异，不是本次搬移引入；
 *   4) 时间戳、随机 id、日志顺序、耗时、并发数。
 * 白名单外任何差异（merge_op/dedupe_check 的账本状态、除拆分段外的记忆正文与状态、entity 表、
 * entity_surface、organizeCache 键）都当失败。
 *
 * 局限：假模型只能证明「调度与重放一致」，不能证明模型行为一致。
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import assert from 'node:assert/strict';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const { applySchema } = await import(`${ROOT}/scripts/apply-pipeline-schema.mjs`);
const { applyCurateSchema } = await import(`${ROOT}/scripts/apply-curate-schema.mjs`);
const { createStore } = await import(`${ROOT}/lib/store.js`);
const { setupSleep } = await import(`${ROOT}/lib/sleep.js`);
const { enqueue } = await import(`${ROOT}/lib/ingest-index.js`);

const VEC_SO = path.join(ROOT, 'node_modules/sqlite-vec-linux-arm64/vec0.so');
const MEMORY_DDL = `CREATE TABLE memories (
  id TEXT PRIMARY KEY, text TEXT NOT NULL, tags TEXT, source TEXT, ts TEXT, type TEXT,
  entities TEXT, validFrom TEXT, supersedes TEXT, updatedAt TEXT, status TEXT, reason TEXT,
  supersededBy TEXT, validTo TEXT, confidence REAL, extra TEXT, present TEXT, rowSeq INTEGER NOT NULL
);`;
const ENTITY_DDL = `
CREATE TABLE entities (id INTEGER PRIMARY KEY, norm TEXT NOT NULL, display TEXT NOT NULL,
  etype TEXT NOT NULL DEFAULT 'unknown', aliases_note TEXT NOT NULL DEFAULT '',
  attrs TEXT NOT NULL DEFAULT '{}', first_seen TEXT NOT NULL DEFAULT '', last_seen TEXT NOT NULL DEFAULT '',
  mem_count INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'active', UNIQUE (norm));
CREATE TABLE memory_entity (memory_id TEXT NOT NULL, entity_id INTEGER NOT NULL, role TEXT NOT NULL DEFAULT 'about',
  ts TEXT NOT NULL DEFAULT '', status TEXT NOT NULL DEFAULT 'active', attrs TEXT NOT NULL DEFAULT '{}',
  version INTEGER NOT NULL DEFAULT 0, prev_memory_id TEXT NOT NULL DEFAULT '', PRIMARY KEY (memory_id, entity_id, role)) WITHOUT ROWID;
CREATE TABLE entity_surface (surface TEXT PRIMARY KEY, norm TEXT NOT NULL, entity_id INTEGER NOT NULL,
  source TEXT NOT NULL CHECK (source IN ('migrate','gate','coarse','extract','model','manual','text')),
  count INTEGER NOT NULL DEFAULT 0, first_seen TEXT NOT NULL DEFAULT '', last_seen TEXT NOT NULL DEFAULT '',
  flag TEXT NOT NULL DEFAULT '', UNIQUE (norm, surface)) WITHOUT ROWID;
CREATE TABLE entity_issue (memory_id TEXT NOT NULL DEFAULT '', entity_norm TEXT NOT NULL, raw_name TEXT NOT NULL DEFAULT '',
  reason TEXT NOT NULL, detail TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL,
  PRIMARY KEY (memory_id, entity_norm, reason)) WITHOUT ROWID;
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);`;

const LONG = '小林把值班记录写得很长。'.repeat(20); // >200 字，触发入队拆分

function fakeLlm() {
  const calls = [];
  return {
    calls,
    stream(opts) {
      const text = String(opts?.messages?.map((m) => (Array.isArray(m.content) ? m.content.map((c) => c.text || '').join('') : m.content || '')).join('\n') || '');
      calls.push(text.slice(0, 40));
      const chunks = [];
      const push = (o) => chunks.push({ type: 'text-delta', text: typeof o === 'string' ? o : JSON.stringify(o) });
      if (/DUPLICATE|SUPERSEDES|DISTINCT|判重|裁决/.test(text) && !/"ops"/.test(text)) {
        push({ verdict: 'DISTINCT', reason: '对拍：新事实' });
      } else if (/"parts"|拆成|拆分/.test(text)) {
        push({ parts: [{ text: '值班记录一。', evidence: '值班记录' }, { text: '值班记录二。', evidence: '值班记录' }] });
      } else if (/batch_id|submit_ops|合并 agent/.test(text)) {
        const ids = [...text.matchAll(/"id":\s*"(m_[^"]+)"/g)].map((m) => m[1]);
        const uniq = [...new Set(ids)];
        const ops = uniq.length >= 2
          ? [{ op: 'merge', ids: uniq.slice(0, 2), text: '树莓派温度 45 度，风扇 3000 转。', evidence: uniq.slice(0, 2).map((id) => ({ id, quote: '' })), needsSplit: false }]
          : [];
        push({ ops });
      } else {
        // ingest 路径：给一条 add
        push({ add: ['树莓派今天温度 45 度。'], merge: [], update: [], archive: [] });
      }
      chunks.push({ type: 'finish', reason: { kind: 'stop' } });
      return (async function* gen() { for (const c of chunks) yield c; })();
    },
  };
}

function putSession(root, idx) {
  const dir = path.join(root, `agent-${idx % 2}`, `s${idx}`);
  fs.mkdirSync(dir, { recursive: true });
  const file = path.join(dir, `session-${idx}.jsonl`);
  const t = Date.now() - 60000;
  const events = [
    { time: t, type: 'user/message', data: { source: { kind: 'user' }, content: [{ type: 'text', text: `会话${idx}：树莓派几度？` }] } },
    { time: t + 1000, type: 'assistant/message', data: { message: { content: [{ type: 'text', text: `会话${idx}：45 度。` }] } } },
  ];
  fs.writeFileSync(file, `${events.map((e) => JSON.stringify(e)).join('\n')}\n`, 'utf8');
  return file;
}

function makeSnapshot() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-full-'));
  const dbPath = path.join(dir, 'memory.db');
  const db = new DatabaseSync(dbPath, { allowExtension: true });
  db.loadExtension(VEC_SO);
  db.exec(MEMORY_DDL); db.exec(ENTITY_DDL);
  db.exec('CREATE VIRTUAL TABLE vec_items USING vec0(embedding float[4] distance_metric=cosine);');
  db.exec('CREATE TABLE vec_map (rowid INTEGER PRIMARY KEY, id TEXT);');
  db.exec("INSERT INTO meta(key,value) VALUES ('dim','4');");
  const insEnt = db.prepare('INSERT INTO entities(norm,display,etype,mem_count) VALUES (?,?,?,0)');
  insEnt.run('树莓派', '树莓派', 'device');
  const now = Date.now();
  const insMem = db.prepare(`INSERT INTO memories(id,text,tags,source,ts,type,entities,status,rowSeq)
    VALUES (?,?,?,?,?,?,?,?,?)`);
  insMem.run('m_a', '树莓派温度 45 度', '["infra"]', 'seed', '2026-11-10T00:00:00Z', 'world', '["树莓派"]', 'active', 1);
  insMem.run('m_b', '树莓派风扇 3000 转', '["infra"]', 'seed', '2026-11-10T00:00:00Z', 'world', '["树莓派"]', 'active', 2);
  insMem.run('m_long', LONG, '["infra"]', 'seed', '2026-11-10T00:00:00Z', 'world', '["树莓派"]', 'active', 3);
  insMem.run('m_old', '旧说法：树莓派温度 30 度，已废弃', '["status"]', 'seed', '2026-10-01T00:00:00Z', 'world', '["树莓派"]', 'active', 4);
  insMem.run('m_new', '新说法：树莓派温度 45 度，旧说法作废', '["status"]', 'seed', '2026-11-10T00:00:00Z', 'world', '["树莓派"]', 'active', 5);
  db.close();
  applySchema(dbPath, { logger: () => {} });
  applyCurateSchema(dbPath, { logger: { info() {}, warn() {} } });
  const dbMe = new DatabaseSync(dbPath);
  const me = dbMe.prepare('INSERT INTO memory_entity(memory_id,entity_id,role,ts,status,origin) VALUES (?,?,?,?,?,?)');
  const entId = dbMe.prepare("SELECT id FROM entities WHERE norm='树莓派'").get().id;
  for (const id of ['m_a', 'm_b', 'm_long', 'm_old', 'm_new']) me.run(id, entId, 'about', '2026-11-10', 'active', 'rule');
  dbMe.close();
  // 会话文件：两个
  const sessions = path.join(dir, 'sessions');
  fs.mkdirSync(sessions, { recursive: true });
  putSession(sessions, 1); putSession(sessions, 2);
  // pending_ops：merge_op + dedupe_check
  const db2 = new DatabaseSync(dbPath);
  enqueue(db2, 'merge_op', 'mo-1', {
    batch: 'b1', op: 'update', ids: ['m_b'],
    text: '树莓派风扇 3000 转（已复核）',
    evidence: [{ id: 'm_b', quote: '树莓派风扇 3000 转' }],
    source: [{ id: 'm_b', text: '树莓派风扇 3000 转', type: 'world' }],
  }, { now });
  enqueue(db2, 'dedupe_check', 'dc-1', {
    action: 'save', candidate: { text: '树莓派今天温度 47 度。', tags: ['infra'], type: 'world' }, ids: [],
  }, { now });
  db2.close();
  return dir;
}

function cloneSnapshot(src) {
  const dst = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-full-run-'));
  fs.cpSync(src, dst, { recursive: true });
  return dst;
}

function fakeCtx() {
  const llm = fakeLlm();
  return {
    logger: { info() {}, warn() {} },
    tools: { register() {}, get: () => null },
    provide() {}, effect() {}, on() {},
    get: (name) => (name === 'llm' ? llm : undefined),
    llm,
  };
}

function makeCfg(dir, mode) {
  return {
    storePath: dir,
    storeLogFile: path.join(dir, 'store.log'),
    stateFile: path.join(dir, 'sleep.state.json'),
    sessionsRoot: path.join(dir, 'sessions'),
    backupDir: path.join(dir, 'backup'),
    archiveDir: path.join(dir, 'archive'),
    splitStateFile: path.join(dir, 'split.state.json'),
    splitLogFile: path.join(dir, 'split.log'),
    splitPreviewFile: path.join(dir, 'preview.md'),
    sleepLogFile: path.join(dir, 'sleep.log'),
    entityQueue: { enqueue: false },
    provider: 'fake', model: 'fake', splitMaxChars: 200,
    embed: async (texts) => texts.map(() => [1, 0, 0, 0]),
    logger: { info() {}, warn() {} },
    pipeline: { writeGate: true, writeGateThreshold: 0.9, textLint: false, lintReview: false, agentPipeline: true },
    organizePipelineMode: mode,
    clusterMode: 'entity',
    clusterQuestionMin: 0.5,
    sleepSplitEnabled: true,
    consolidateTools: false,
    notifyScript: '/bin/true',
  };
}

async function runRound(dir, mode) {
  const cfg = makeCfg(dir, mode);
  const store = createStore(cfg);
  store.attachLlm(fakeLlm());
  const sleep = setupSleep(fakeCtx(), store, cfg);
  const out = await sleep.sleepPass({});
  await store.flushEntityQueue?.();
  return { out, cfg };
}

function dump(dir) {
  const db = new DatabaseSync(path.join(dir, 'memory.db'), { readOnly: true });
  const rows = (sql) => db.prepare(sql).all().map((r) => ({ ...r }));
  const statePath = path.join(dir, 'sleep.state.json');
  const state = fs.existsSync(statePath) ? JSON.parse(fs.readFileSync(statePath, 'utf8')) : {};
  const out = {
    mem: rows("SELECT text,status,type FROM memories ORDER BY text").map((r) => `${r.status}|${r.type}|${r.text}`),
    pending: rows('SELECT kind,status,attempts,last_error FROM pending_ops ORDER BY kind,dedupe_key').map((r) => `${r.kind}|${r.status}|${r.attempts}|${String(r.last_error || '').slice(0, 60)}`),
    entities: rows('SELECT norm,etype,mem_count,status FROM entities ORDER BY norm').map((r) => `${r.norm}|${r.etype}|${r.mem_count}|${r.status}`),
    surface: rows('SELECT surface,count,source FROM entity_surface ORDER BY surface').map((r) => `${r.surface}|${r.count}|${r.source}`),
    cache: Object.keys(state.organizeCache || {}).sort(),
  };
  db.close();
  return out;
}


const WHITELIST = {
  // relaxation：compact 通道的两种「每轮现算」账（作废清扫、三期实体整理）只有 pool 有，
  // legacy 根本没有这条队列，跟 ingest_file / split_item 同属白名单（阶段 5 / 三期新增）。
  pendingKinds: new Set(['ingest_file', 'revocation_sweep', 'split_item', 'entity_tidy']),
  memPrefix: '值班记录',
};

function normalize(dump) {
  const pending = dump.pending
    .filter((row) => !WHITELIST.pendingKinds.has(String(row).split('|')[0]))
    .map((row) => String(row).split('|').slice(0, 3).join('|'));
  const mem = dump.mem
    .filter((row) => !row.includes(WHITELIST.memPrefix))
    .map((row) => {
      // 超长源条在 pool 里被拆完归档、legacy 里保持 active：归一成 source 再比
      if (row.includes('小林把值班记录写得很长')) return `source|world|${row.split('|').slice(2).join('|')}`;
      return row;
    });
  return { mem, pending, entities: dump.entities, surface: dump.surface, cache: dump.cache };
}

const base = makeSnapshot();

test('整轮对拍：legacy 两遍稳定、pool 三遍稳定，白名单外逐条一致', async () => {
  const legacyDirs = [];
  const poolDirs = [];
  for (let i = 0; i < 2; i += 1) {
    const d = cloneSnapshot(base);
    legacyDirs.push(d);
    await runRound(d, 'legacy');
  }
  for (let i = 0; i < 3; i += 1) {
    const d = cloneSnapshot(base);
    poolDirs.push(d);
    await runRound(d, 'pool');
  }
  const legacy = legacyDirs.map(dump).map(normalize);
  const pool = poolDirs.map(dump).map(normalize);

  assert.deepEqual(legacy[0], legacy[1], 'legacy 两遍自比必须稳定');
  assert.deepEqual(pool[0], pool[1], 'pool 第一、二遍必须稳定');
  assert.deepEqual(pool[1], pool[2], 'pool 第二、三遍必须稳定');

  for (const key of ['mem', 'pending', 'entities', 'surface', 'cache']) {
    assert.deepEqual(pool[0][key], legacy[0][key], `白名单外差异：${key}`);
  }
});

test('快照本身可复现：同一源快照复制两次，初始 dump 一致', () => {
  const a = dump(cloneSnapshot(base));
  const b = dump(cloneSnapshot(base));
  assert.deepEqual(a, b);
});
