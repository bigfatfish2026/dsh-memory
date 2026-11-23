/**
 * 观察证据绑定的只读回放（2026-11-23 t-mwszo23a-iwxs）。
 *
 * 目的：句子级「顺带提及」判据加严之后，核对历史证据该过的还得过——
 * 旧口径放行（ambiguous=false）的绑定，新口径不能变成 ambiguous。
 *
 * 只读：把生产库 memory.db（连同 -wal/-shm）拷到临时目录，全程只打开副本；
 * 生产库一个字节都不写，不调模型、不碰 pending_ops。
 *
 * 对每条 observation 的每条 derived_from 证据，按观察的候选实体键跑
 * evidenceBindingOf(record, key, { candidates })：
 *   - 新 ambiguous = basis.ambiguous；
 *   - 旧 ambiguous = 只看 word_too_short / snippet_too_short（加句子判据之前的两种）。
 * 输出逐条对照 + 汇总，并把「旧放行、新降级」的 id 单独列出来。
 *
 * 用法：
 *   node scripts/replay-observation-bindings.mjs [--db <memory.db>] [--ids id1,id2] [--keys vec_items,duplicate] [--json]
 *   --keys 只影响 --ids 里那些不在现役 observation 边上的历史 id：拿这几个实体键逐个判，
 *   用来核对「当时被拒、现在该不该过」的落库失败现场（t6 的 7 条）。
 *   库路径缺省取 DSH_MEMORY_DB，再缺省 /home/julia/fatfish/memory/memory.db。
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { evidenceBindingOf } from '../lib/sleep/observation-sources.js';
import { entityKeysOf } from '../lib/store/observe-notify.js';
import { entityList } from '../lib/store/types.js';
import { normalizeEntity } from '../lib/shared/entity-normalize.js';

function parseArgs(argv) {
  const out = { db: '', ids: [], keys: [], json: false };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === '--db') out.db = String(argv[++i] || '').trim();
    else if (arg === '--ids') out.ids = String(argv[++i] || '').split(',').map((s) => s.trim()).filter(Boolean);
    else if (arg === '--keys') out.keys = String(argv[++i] || '').split(',').map((s) => s.trim()).filter(Boolean);
    else if (arg === '--json') out.json = true;
  }
  return out;
}

/** 拷贝一份副本再打开：不碰生产库，也不受 WAL 里半截事务影响。 */
function openReplica(prod) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-observe-replay-'));
  const target = path.join(dir, 'memory.db');
  fs.copyFileSync(prod, target);
  for (const suffix of ['-wal', '-shm']) {
    if (fs.existsSync(`${prod}${suffix}`)) fs.copyFileSync(`${prod}${suffix}`, `${target}${suffix}`);
  }
  return { dir, db: new DatabaseSync(target, { readOnly: true }) };
}

/** 旧口径的歧义：只有词太短、片段太短两种。 */
function oldAmbiguousOf(basis) {
  return /word_too_short|snippet_too_short/.test(String(basis?.ambiguousReason || ''));
}

const args = parseArgs(process.argv.slice(2));
const prod = args.db || String(process.env.DSH_MEMORY_DB || '').trim()
  || '/home/julia/fatfish/memory/memory.db';
if (!fs.existsSync(prod)) {
  console.error(`库不存在：${prod}（用 --db 指定，或设 DSH_MEMORY_DB）`);
  process.exit(2);
}

const { dir, db } = openReplica(prod);
try {
  const wanted = new Set(args.ids);
  const observations = db.prepare(
    "SELECT id, entities, text FROM memories WHERE type = 'observation' ORDER BY id",
  ).all();
  const rows = [];
  const allNorms = [];
  for (const obs of observations) {
    for (const name of entityList(obs.entities)) {
      const norm = normalizeEntity(name).norm;
      if (norm && !allNorms.includes(norm)) allNorms.push(norm);
    }
  }
  for (const obs of observations) {
    const norms = [];
    for (const name of entityList(obs.entities)) {
      const norm = normalizeEntity(name).norm;
      if (norm && !norms.includes(norm)) norms.push(norm);
    }
    for (const key of entityKeysOf(entityList(obs.entities))) {
      if (key && !norms.includes(key)) norms.push(key);
    }
    if (!norms.length) continue;
    const evidenceIds = db.prepare(
      "SELECT dst FROM memory_link WHERE src = ? AND rel = 'derived_from' ORDER BY dst",
    ).all(String(obs.id)).map((r) => String(r.dst));
    for (const evidenceId of evidenceIds) {
      if (wanted.size && !wanted.has(evidenceId) && !wanted.has(String(obs.id))) continue;
      const rec = db.prepare('SELECT id, text, entities, type, status FROM memories WHERE id = ?').get(evidenceId);
      if (!rec) continue;
      for (const key of norms) {
        const basis = evidenceBindingOf(rec, key, { candidates: norms });
        if (!basis) continue;
        rows.push({
          observationId: String(obs.id),
          evidenceId: String(rec.id),
          key,
          rule: basis.rule,
          oldAmbiguous: oldAmbiguousOf(basis),
          ambiguous: basis.ambiguous,
          reason: basis.ambiguousReason || '',
          downgraded: !oldAmbiguousOf(basis) && basis.ambiguous,
        });
      }
    }
  }

  // --ids 里可能有不在现役 observation 边上的历史 id（当时走拒绝路径、库里没留边）：
  // 用 --keys 指定的实体键逐个判（没给就退成全库观察实体键），绑不上也记一行 rule=none，
  // 这样「该拒的仍拒」在输出里看得见。
  const covered = new Set(rows.map((row) => row.evidenceId));
  for (const id of args.ids) {
    if (covered.has(id)) continue;
    const rec = db.prepare('SELECT id, text, entities, type, status FROM memories WHERE id = ?').get(id);
    if (!rec) {
      rows.push({
        observationId: '-', evidenceId: id, key: '-', rule: 'missing',
        oldAmbiguous: false, ambiguous: false, reason: 'row_not_found',
        downgraded: false, requested: true, rejected: true,
      });
      continue;
    }
    const keys = args.keys.length
      ? [...new Set(args.keys.map((k) => normalizeEntity(k).norm).filter(Boolean))]
      : allNorms;
    for (const key of keys) {
      const basis = evidenceBindingOf(rec, key, { candidates: keys });
      rows.push({
        observationId: '-',
        evidenceId: id,
        key,
        rule: basis ? basis.rule : 'none',
        oldAmbiguous: basis ? oldAmbiguousOf(basis) : false,
        ambiguous: basis ? basis.ambiguous : false,
        reason: basis ? basis.ambiguousReason || '' : 'no_whole_word',
        downgraded: Boolean(basis) && !oldAmbiguousOf(basis) && basis.ambiguous,
        requested: true,
        rejected: !basis,
      });
    }
  }

  const requested = rows.filter((row) => row.requested);
  const requestedPass = requested.filter((row) => !row.rejected && !row.ambiguous).length;
  const requestedReview = requested.filter((row) => !row.rejected && row.ambiguous).length;
  const requestedReject = requested.filter((row) => row.rejected).length;

  if (args.json) {
    console.log(JSON.stringify({ db: prod, rows }, null, 2));
  } else {
    console.log(`[replay-observation-bindings] db=${prod} 观察=${observations.length} 绑定=${rows.length}`);
    for (const row of rows) {
      console.log(
        `${row.observationId} <- ${row.evidenceId} key=${row.key} rule=${row.rule} `
        + `old_ambiguous=${row.oldAmbiguous ? 1 : 0} new_ambiguous=${row.ambiguous ? 1 : 0} `
        + `verdict=${row.rejected ? 'reject' : (row.ambiguous ? 'review' : 'pass')} reason=${row.reason || '-'}`,
      );
    }
  }

  const downgraded = rows.filter((row) => row.downgraded);
  const bindable = rows.filter((row) => !row.rejected);
  const stillPass = bindable.filter((row) => !row.oldAmbiguous && !row.ambiguous);
  console.log(
    `[replay-observation-bindings] 旧放行 ${bindable.filter((r) => !r.oldAmbiguous).length} 条，`
    + `其中新口径仍放行 ${stillPass.length} 条、新降级 ${downgraded.length} 条`,
  );
  if (requested.length) {
    console.log(
      `[replay-observation-bindings] --ids 判定：通过 ${requestedPass} 条、待复核 ${requestedReview} 条、拒绝 ${requestedReject} 条`,
    );
  }
  if (downgraded.length) {
    console.log(`[replay-observation-bindings] 新降级 id：${downgraded.map((r) => r.evidenceId).join(',')}`);
    if (args.ids.length) process.exitCode = 1;
  }
} finally {
  db.close();
  fs.rmSync(dir, { recursive: true, force: true });
}
