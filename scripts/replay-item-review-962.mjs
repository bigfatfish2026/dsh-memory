#!/usr/bin/env node
/**
 * 962 条那笔的增量复审回放对照（2026-11-23，t-mwsvibwz-f9ve）。只读，不写库。
 *
 * 背景：2026-11-23 12:30 那轮一笔 merge_batch 带 962 条，吃 115697 未缓存输入 + 31134 输出
 * （logs/memory_sleep.log:64824）；其中 873 条与早上那批逐字相同，只因整批指纹随成员变化失效，
 * 被整批重审。本脚本用历史那笔的成员回放，验证「只送变化项 + 旧邻居」的结论与旧口径一致。
 *
 * 两种模式：
 *   默认（离线）：只读库 + 本地收缩，打印「旧口径送 N 条 / 新口径送 M 条 / 被复用 K 条」对照表，
 *                不调模型、不写任何东西。用来核对条数与 token 预估。
 *   --ask：额外真跑模型三次（模拟早上那批 / 旧口径整批 / 新口径增量），比较 op 集合，
 *          打印「结论差异 X 条」。密钥只从环境变量 DSH_REHEARSE_API_KEY 取，绝不读凭据文件。
 *
 * 用法：
 *   node scripts/replay-item-review-962.mjs [--db <memory.db>] [--json]
 *   node scripts/replay-item-review-962.mjs --ask [--base-url http://127.0.0.1:3425/v1] [--model group/ds]
 *
 * 只读保证：sqlite 连接 READONLY；askMergeOps 传 db:null（不落 pending_ops / discards）；
 * 不打开 store，不做任何写操作。
 */
import { DatabaseSync } from 'node:sqlite';
import { DEFAULT_DB_PATH } from './lib/db.mjs';
import { merge as askMergeOps } from '../lib/agents/merge.js';
import { createRehearseLlm } from './rehearse-pipeline.mjs';
import {
  shrinkClusterForReview, reviewAdditionsFromOps, itemReviewKey, itemReviewGroupKey,
  DEFAULT_ITEM_REVIEW_NEIGHBORS,
} from '../lib/item-review-cache.js';

const args = process.argv.slice(2);
function arg(name, fallback = '') {
  const i = args.indexOf(name);
  return i >= 0 ? String(args[i + 1] || '') : fallback;
}
const DB = arg('--db', DEFAULT_DB_PATH);
const ASK = args.includes('--ask');
const JSON_OUT = args.includes('--json');
const BASE_URL = arg('--base-url', process.env.DSH_REHEARSE_BASE_URL || 'http://127.0.0.1:3425/v1');
const MODEL = arg('--model', process.env.DSH_REHEARSE_MODEL || 'group/ds');
const TARGET_ID = arg('--target-id', '');
/**
 * 每个变化项带几个旧邻居。默认值只认生产同一个常量 DEFAULT_ITEM_REVIEW_NEIGHBORS
 * （= 配置 sleepItemReviewNeighbors 的默认值，8）；别在这里另写死一个数字（t6 复核 bug 2）。
 * --neighbors N 可显式覆盖，用来对照 2 / 8 两档的送审量。
 */
const NEIGHBORS = (() => {
  const raw = arg('--neighbors', '');
  if (raw === '') return DEFAULT_ITEM_REVIEW_NEIGHBORS;
  const n = Math.floor(Number(raw));
  return Number.isFinite(n) && n >= 0 ? n : DEFAULT_ITEM_REVIEW_NEIGHBORS;
})();
/** 每个口径重复跑几次（只在 --ask 下生效）。模型非确定：单跑一次归因不了差异，要拿重复臂量噪声。 */
const REPEAT = Math.max(1, Math.floor(Number(arg('--repeat', '1')) || 1));

/** 只读打开；找不到库就直说，别静默返回空表。 */
function openDb(file) {
  try {
    return new DatabaseSync(file, { readOnly: true });
  } catch (error) {
    throw new Error(`打不开库 ${file}（只读）：${String(error?.message || error)}`);
  }
}

function parsePayload(raw) {
  try {
    return JSON.parse(String(raw || '{}')) || {};
  } catch {
    return {};
  }
}

function idsOf(payload) {
  return Array.isArray(payload?.ids) ? payload.ids.map(String) : [];
}

/** 取所有 merge_batch 账里的候选（最多 5000 行，按 updated_ts 倒序）。 */
function loadBatches(db) {
  const rows = db.prepare(
    "SELECT id, dedupe_key, payload_json, updated_ts FROM pending_ops WHERE kind = 'merge_batch' ORDER BY updated_ts DESC LIMIT 5000",
  ).all();
  return rows.map((r) => {
    const payload = parsePayload(r.payload_json);
    return {
      rowId: r.id,
      key: String(r.dedupe_key || payload.batch || ''),
      ts: Number(r.updated_ts) || 0,
      ids: idsOf(payload),
      items: Array.isArray(payload.items) ? payload.items : [],
    };
  });
}

/** 交集最大的「更早那批」= 早上那批（只认成员有交集的，避免挑到不相干的批）。 */
function morningOf(batches, target) {
  const set = new Set(target.ids);
  let best = null;
  for (const b of batches) {
    if (b === target || b.ids.length < 2) continue;
    if (b.ts >= target.ts) continue;
    const inter = b.ids.filter((id) => set.has(id));
    if (!inter.length) continue;
    if (!best || inter.length > best.inter.length) best = { batch: b, inter };
  }
  return best;
}

/** 逐字相同：同一 id 在两批 payload.items 里的正文完全相同。 */
function sameTextIds(target, morning, interIds) {
  const textOf = new Map(target.items.map((it) => [String(it?.id ?? ''), String(it?.text ?? '')]));
  const oldText = new Map(morning.items.map((it) => [String(it?.id ?? ''), String(it?.text ?? '')]));
  const same = [];
  const rewritten = [];
  for (const id of interIds) {
    const now = textOf.get(id);
    const before = oldText.get(id);
    if (now !== undefined && before !== undefined && now === before) same.push(id);
    else rewritten.push(id);
  }
  return { same, rewritten };
}

/** 从库里取成员的完整面（只读）；取不到的按 payload 里的 id+text 兜底。 */
function loadMembers(db, ids, payloadItems) {
  const byId = new Map(payloadItems.map((it) => [String(it?.id ?? ''), it]));
  const out = [];
  const chunk = 500;
  const rows = new Map();
  for (let i = 0; i < ids.length; i += chunk) {
    const part = ids.slice(i, i + chunk);
    const ph = part.map(() => '?').join(',');
    const got = db.prepare(`SELECT id, text, type, ts, entities FROM memories WHERE id IN (${ph})`).all(...part);
    for (const r of got) rows.set(String(r.id), r);
  }
  for (const id of ids) {
    const r = rows.get(id);
    if (r) {
      let entities = [];
      try { entities = JSON.parse(String(r.entities || '[]')); } catch { entities = []; }
      out.push({
        id: String(r.id), type: String(r.type || 'world'), text: String(r.text || ''),
        ts: String(r.ts || ''), entities: Array.isArray(entities) ? entities.map(String) : [],
      });
    } else if (byId.has(id)) {
      out.push({ id, type: 'world', text: String(byId.get(id)?.text || ''), ts: '', entities: [] });
    }
  }
  return out;
}

/** op 集合签名：类型 + 排序后的 ids。正文不比（模型生成文本天然有漂移）。 */
function opSignatures(ops) {
  const set = new Set();
  for (const op of Array.isArray(ops) ? ops : []) {
    const ids = (Array.isArray(op?.ids) ? op.ids.map(String) : []).slice().sort().join('+');
    if (!ids) continue;
    set.add(`${String(op?.op || op?.kind || 'op')}:${ids}`);
  }
  return set;
}

async function ask(llm, members, label) {
  const t0 = Date.now();
  const answer = await askMergeOps({
    items: members,
    candidates: [],
    clusterId: label,
    llm,
    provider: 'rehearse',
    model: MODEL,
    log: () => {},
    db: null,
    textLint: false,
    lintReview: false,
  });
  return { ops: Array.isArray(answer?.ops) ? answer.ops : [], ms: Date.now() - t0 };
}

/** 用一次成立的口径给条目建缓存（valid ops 涉及的记 acted，其余记 noop）。 */
function buildCache(members, ops) {
  return reviewAdditionsFromOps(members, ops, { rejected: [] });
}

async function main() {
  const db = openDb(DB);
  try {
    const batches = loadBatches(db).filter((b) => b.ids.length >= 2);
    if (!batches.length) throw new Error('pending_ops 里没有 merge_batch 账');
    const target = TARGET_ID
      ? batches.find((b) => b.key === TARGET_ID || String(b.rowId) === TARGET_ID)
      : batches.slice().sort((a, b) => b.ids.length - a.ids.length)[0];
    if (!target) throw new Error(`找不到指定那批 merge_batch：${TARGET_ID}`);
    const morning = morningOf(batches, target);
    const interIds = morning ? morning.inter.filter((id) => target.ids.includes(id)) : [];
    const { same, rewritten } = morning
      ? sameTextIds(target, morning.batch, interIds)
      : { same: [], rewritten: target.ids };
    const oldMembers = loadMembers(db, target.ids, target.items);
    const sameSet = new Set(same);
    // 离线口径：把「逐字相同」的成员当作早上那批一起审过（noop + 同批组键 g）。
    // 关系级证据只认「同一批一起给过结论」（t6 复核 bug 1）：各自在不同批判过无关，
    // 不能证明它们这一对判过，所以离线缓存必须带上 morning 那批的组键。
    let itemCache = {};
    const morningMembersOffline = oldMembers.filter((m) => sameSet.has(String(m.id)));
    const morningGroup = morningMembersOffline.length >= 2 ? itemReviewGroupKey(morningMembersOffline) : '';
    for (const m of oldMembers) {
      if (sameSet.has(String(m.id))) {
        itemCache[itemReviewKey(m)] = morningGroup ? { v: 'noop', t: 0, g: morningGroup } : { v: 'noop', t: 0 };
      }
    }

    const oldRuns = [];
    const newRuns = [];
    let cachedOps = new Set();
    let llmUsage = null;
    let shrunk = { cluster: oldMembers, skipped: [], neighbors: [] };
    const llm = ASK
      ? createRehearseLlm({ baseUrl: BASE_URL, model: MODEL, apiKey: process.env.DSH_REHEARSE_API_KEY || '' })
      : null;
    if (ASK) {
      llmUsage = llm.usage;
      // ① 模拟早上那批：对「逐字相同」的 873 条跑一遍，把它当时的结论当已审结论
      const morningMembers = oldMembers.filter((m) => sameSet.has(String(m.id)));
      if (morningMembers.length >= 2) {
        const got = await ask(llm, morningMembers, `morning:${target.key}`);
        cachedOps = opSignatures(got.ops);
        itemCache = buildCache(morningMembers, got.ops);
      }
      // ② 旧口径：整批 962 条一次送，重复 REPEAT 次（第 2 次起就是噪声基线臂）
      for (let i = 0; i < REPEAT; i += 1) {
        oldRuns.push((await ask(llm, oldMembers, `old${i}:${target.key}`)).ops);
      }
    }

    shrunk = shrinkClusterForReview(oldMembers, itemCache, {
      relate: null, neighborLimit: NEIGHBORS,
    });
    if (ASK && shrunk.cluster.length >= 2) {
      // ③ 新口径：只送变化项 + 旧邻居，同样重复 REPEAT 次
      for (let i = 0; i < REPEAT; i += 1) {
        newRuns.push((await ask(llm, shrunk.cluster, `new${i}:${target.key}`)).ops);
      }
    }

    const idsOfSig = (sig) => sig.slice(sig.indexOf(':') + 1).split('+').filter(Boolean);
    // 整体口径用「并集」：模型每次给的合并组不同，取交集会把两次共有的结论抹成 0，反而看不出差异。
    const unionOfRuns = (runs) => new Set(runs.flatMap((r) => [...opSignatures(r)]));
    const oldSigs = unionOfRuns(oldRuns);
    const newSigs = unionOfRuns(newRuns);
    const changedIds = new Set(oldMembers.map((m) => String(m.id)).filter((id) => !sameSet.has(id)));
    const sentIds = new Set(shrunk.cluster.map((m) => String(m.id)));
    // 「变化项视角」：只看提到变化条目的 op——这是收缩唯一可能漏掉的东西，
    // 已审条目之间的关系本来就不该重判（复用早上那批的结论）。
    const oldAllSigs = oldSigs;
    const newAllSigs = newSigs;
    const changedScopeOld = [...oldAllSigs].filter((s) => idsOfSig(s).some((id) => changedIds.has(id)));
    const changedScopeNew = [...newAllSigs].filter((s) => idsOfSig(s).some((id) => changedIds.has(id)));
    const changedScopeMissing = changedScopeOld.filter((s) => !newAllSigs.has(s));
    const changedIdsCovered = new Set();
    for (const s of newAllSigs) for (const id of idsOfSig(s)) if (changedIds.has(id)) changedIdsCovered.add(id);
    const changedSent = [...changedIds].filter((id) => sentIds.has(id)).length;
    const union = new Set([...newSigs, ...cachedOps]);
    const missing = ASK ? [...oldSigs].filter((s) => !union.has(s)) : [];
    const extra = ASK ? [...union].filter((s) => !oldSigs.has(s)) : [];
    // 噪声臂：同一旧口径的第一次 vs 第二次（同输入重复，差异全部来自模型非确定性）。
    const baseSigs = oldRuns.length > 1 ? opSignatures(oldRuns[1]) : new Set();
    const baseMissing = ASK ? [...opSignatures(oldRuns[0] || [])].filter((s) => !baseSigs.has(s)) : [];
    const baseExtra = ASK ? [...baseSigs].filter((s) => !opSignatures(oldRuns[0] || []).has(s)) : [];
    // 缺失的 op 里只要提到一个「变化项」，就说明收缩漏审了该变化的条目；全落在已审集合里才是重复复审。
    const missingTouchingChanged = missing.filter((s) => idsOfSig(s).some((id) => changedIds.has(id)));

    const inTokens = 115697;
    const outTokens = 31134;
    const estIn = Math.round(inTokens * (shrunk.cluster.length / Math.max(1, oldMembers.length)));
    const estOut = Math.round(outTokens * (shrunk.cluster.length / Math.max(1, oldMembers.length)));
    const report = {
      db: DB,
      target: { key: target.key, rowId: target.rowId, ids: oldMembers.length },
      morning: morning ? { key: morning.batch.key, rowId: morning.batch.rowId, inter: interIds.length } : null,
      same: same.length,
      rewritten: rewritten.length,
      oldSent: oldMembers.length,
      newSent: shrunk.cluster.length,
      reused: shrunk.skipped.length,
      neighbors: shrunk.neighbors.length,
      neighborsLimit: NEIGHBORS,
      oldOps: oldSigs.size,
      newOps: newSigs.size,
      cachedOps: cachedOps.size,
      diffMissing: missing.length,
      diffExtra: extra.length,
      diffMissingTouchingChanged: missingTouchingChanged.length,
      diffMissingSamples: missing.slice(0, 5),
      diffExtraSamples: extra.slice(0, 5),
      runs: { old: oldRuns.map((r) => opSignatures(r).size), new: newRuns.map((r) => opSignatures(r).size) },
      changedScope: {
        changed: changedIds.size,
        sent: changedSent,
        opsOld: changedScopeOld.length,
        opsNew: changedScopeNew.length,
        opsMissing: changedScopeMissing.length,
        opsMissingSamples: changedScopeMissing.slice(0, 5),
        touchedByNew: changedIdsCovered.size,
      },
      noiseBaseline: oldRuns.length > 1
        ? {
          note: '同一旧口径再跑一遍：模型非确定性带来的差异',
          missing: baseMissing.length,
          extra: baseExtra.length,
        }
        : null,
      estInputTokens: estIn,
      estOutputTokens: estOut,
      history: { inputTokens: inTokens, outputTokens: outTokens, log: 'logs/memory_sleep.log:64824' },
      llm: llmUsage,
    };
    if (JSON_OUT) {
      console.log(JSON.stringify(report, null, 2));
      return;
    }
    console.log(`库：${DB}（只读）`);
    console.log(`目标批：${report.target.key.slice(0, 16)} 行=${report.target.rowId} 成员 ${report.target.ids} 条`);
    if (morning) {
      console.log(`早上那批：${report.morning.key.slice(0, 16)} 行=${report.morning.rowId} 交集 ${report.morning.inter} 条`);
    } else {
      console.log('早上那批：没找到（--ask 模式下无法回放早上结论，只能给离线条数）');
    }
    console.log('');
    console.log('口径            送审条数   复用已审   输入token   输出token   结论op数');
    console.log(`旧（整批）        ${String(report.oldSent).padEnd(10)} ${String(0).padEnd(10)} ${String(inTokens).padEnd(11)} ${String(outTokens).padEnd(11)} ${ASK ? oldSigs.size : '-'}`);
    console.log(`新（增量复审）    ${String(report.newSent).padEnd(10)} ${String(report.reused).padEnd(10)} ${String(estIn).padEnd(11)} ${String(estOut).padEnd(11)} ${ASK ? newSigs.size : '-'}`);
    console.log(`  其中邻居        ${report.neighbors} 条（每个变化项上限 ${NEIGHBORS}：DEFAULT_ITEM_REVIEW_NEIGHBORS / sleepItemReviewNeighbors）；逐字未变 ${report.same} 条、改写 ${report.rewritten} 条`);
    console.log('');
    if (!ASK) {
      console.log('结论差异：未跑模型（加 --ask 才做真模型对照）');
    } else {
      console.log(`全批口径差异：缺 ${missing.length} 条、多 ${extra.length} 条（旧 ${oldSigs.size} / 新 ${newSigs.size} / 复用 ${cachedOps.size}）`);
      console.log(`  其中涉及变化项的缺失 ${missingTouchingChanged.length} 条（应为 0：变化项一律送审）`);
      console.log(`变化项视角：变化 ${report.changedScope.changed} 条 / 送审 ${report.changedScope.sent} 条`
        + ` / 旧口径提到它们的 op ${report.changedScope.opsOld} 条 / 新口径 ${report.changedScope.opsNew} 条`
        + ` / 缺失 ${report.changedScope.opsMissing} 条（新口径 op 覆盖到的变化条目 ${report.changedScope.touchedByNew} 条）`);
      if (oldRuns.length > 1) {
        console.log(`每次 op 数：旧 ${report.runs.old.join('/')}，新 ${report.runs.new.join('/')}`);
        console.log(`噪声基线（旧 vs 旧）：缺 ${baseMissing.length}、多 ${baseExtra.length}`);
      } else {
        console.log('噪声基线：未跑（加 --repeat 2 才量模型自身的重复抖动）');
      }
    }
    if (missing.length) console.log(`  缺失样例：${missing.slice(0, 5).join(' | ')}`);
    if (extra.length) console.log(`  多出样例：${extra.slice(0, 5).join(' | ')}`);
    if (llmUsage) console.log(`模型调用 ${llmUsage.calls} 次 / 输入 ${llmUsage.promptTokens} tok（缓存读 ${llmUsage.cacheReadTokens}）/ 输出 ${llmUsage.completionTokens} tok / 错误 ${llmUsage.errors}`);
  } finally {
    db.close();
  }
}

main().catch((error) => {
  console.error(`回放失败：${String(error?.message || error)}`);
  process.exitCode = 1;
});
