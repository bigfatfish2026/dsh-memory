/**
 * 拆分两道闸的历史回放（t-mwogaubu-m1k8）。
 *
 * 只读导出：把生产库 memory.db（连同 -wal/-shm，如果有）**拷贝**到临时目录，
 * 全程只打开副本；生产库一个字节都不写，也不在它上面跑 dueOps。
 *
 * 回放四类样本（最近 done 的 split_item；最后一类用于覆盖闸回放）：
 *   - no_source：payload 里的 source_id 在 memories 里已经找不到；
 *   - all_parts_deduped：last_error = all_parts_deduped；
 *   - zero_parts：库里一条 sleep:split:<sid> 子段都没有；
 *   - with_children：有落库子段的 done 账（取最多 40 条），回放覆盖闸。
 * 对每条样本：
 *   1) 入口版本闸：payload.text vs memories.text（只 trim 两端）；
 *   2) 覆盖闸：按子段落库顺序在原文里 indexOf 还原区间，调 verifySplitCoverage；
 *   3) 统计「拦下」（覆盖闸不通过）与「误杀」（生产上原条已归档、新闸却拦下）。
 * 另外统计待拆 pending/running 的版本不一致条数，并列出「原条 active 但 supersededBy 指向子段」的 id。
 *
 * 用法：node scripts/replay-split-gates.mjs [生产库路径]
 * 默认库：/home/julia/fatfish/memory/memory.db
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { verifySplitCoverage } from '../lib/pipeline-split.js';

const PROD = process.argv[2] || '/home/julia/fatfish/memory/memory.db';
const LIMIT = { no_source: 25, all_parts_deduped: 12, zero_parts: 12 };
const SCAN_DONE = 2600;

function exportReplica() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-split-replay-'));
  const target = path.join(dir, 'memory.db');
  fs.copyFileSync(PROD, target);
  for (const suffix of ['-wal', '-shm']) {
    if (fs.existsSync(`${PROD}${suffix}`)) fs.copyFileSync(`${PROD}${suffix}`, `${target}${suffix}`);
  }
  return { dir, target };
}

const trim = (value) => String(value ?? '').trim();

function parsePayload(row) {
  try {
    return JSON.parse(String(row.payload_json || '{}'));
  } catch {
    return {};
  }
}

function main() {
  const { dir, target } = exportReplica();
  const db = new DatabaseSync(target);
  const out = { prod: PROD, replica: target };

  const memories = db.prepare('SELECT id, text, status, source, supersededBy FROM memories').all();
  const byId = new Map(memories.map((m) => [String(m.id), m]));

  // ① 待拆账本的版本不一致（票面依据：124 条里 34 条对不上）
  const pendingOps = db.prepare(
    "SELECT id, payload_json, status FROM pending_ops WHERE kind = 'split_item' AND status = 'pending'",
  ).all();
  let pendingChecked = 0;
  let pendingMismatch = 0;
  let pendingOldHasExtra = 0;
  const pendingSamples = [];
  for (const op of pendingOps) {
    const payload = parsePayload(op);
    const sid = String(payload.source_id || '');
    const src = sid ? byId.get(sid) : null;
    const payloadText = String(payload.text ?? '');
    if (!src || !payloadText.trim()) continue;
    pendingChecked += 1;
    const dbText = String(src.text ?? '');
    if (trim(payloadText) !== trim(dbText)) {
      pendingMismatch += 1;
      // 旧正文里有、新正文里没有的内容：按 60 字窗口抽样判断
      if (!trim(dbText).includes(trim(payloadText))) pendingOldHasExtra += 1;
      if (pendingSamples.length < 5) pendingSamples.push({ op: op.id, sid, payloadLen: payloadText.length, dbLen: dbText.length });
    }
  }
  out.pendingVersion = { checked: pendingChecked, mismatch: pendingMismatch, oldHasExtra: pendingOldHasExtra, samples: pendingSamples };

  // ② 分层样本
  const doneOps = db.prepare(
    "SELECT id, payload_json, last_error, updated_ts FROM pending_ops WHERE kind = 'split_item' AND status = 'done' ORDER BY updated_ts DESC LIMIT ?",
  ).all(SCAN_DONE);
  const childStmt = db.prepare("SELECT id, text, status, rowSeq FROM memories WHERE source = ? ORDER BY rowSeq");
  const samples = { no_source: [], all_parts_deduped: [], zero_parts: [] };
  const seen = new Set();
  for (const op of doneOps) {
    const payload = parsePayload(op);
    const sid = String(payload.source_id || '');
    const src = sid ? byId.get(sid) : null;
    const children = sid ? childStmt.all(`sleep:split:${sid}`) : [];
    const entry = { op: op.id, sid, payload, src, children, lastError: op.last_error };
    if (sid && !src && samples.no_source.length < LIMIT.no_source && !seen.has(op.id)) {
      seen.add(op.id);
      samples.no_source.push(entry);
    }
    if (String(op.last_error || '') === 'all_parts_deduped' && samples.all_parts_deduped.length < LIMIT.all_parts_deduped && !seen.has(op.id)) {
      seen.add(op.id);
      samples.all_parts_deduped.push(entry);
    }
    if (sid && !children.length && samples.zero_parts.length < LIMIT.zero_parts && !seen.has(op.id)) {
      seen.add(op.id);
      samples.zero_parts.push(entry);
    }
    if (samples.no_source.length >= LIMIT.no_source
      && samples.all_parts_deduped.length >= LIMIT.all_parts_deduped
      && samples.zero_parts.length >= LIMIT.zero_parts) break;
  }

  // ③ 逐条回放覆盖闸
  const replay = (entry) => {
    const notes = [];
    const payloadText = String(entry.payload?.text ?? '');
    const sid = entry.sid;
    const src = entry.src;
    if (!src) return { verdict: 'skip_no_source', reason: 'source 不在 memories', notes };
    const sourceText = trim(src.text);
    if (payloadText.trim() && trim(payloadText) !== sourceText) {
      notes.push('entry_version_mismatch');
    }
    const children = (entry.children || []).filter((c) => (c.status || 'active') !== 'archived');
    if (!children.length) return { verdict: 'skip_no_parts', reason: '没有活跃子段可核对', notes };
    const parts = [];
    const landedIds = [];
    let cursor = 0;
    for (const child of children) {
      const text = trim(child.text);
      const at = sourceText.indexOf(text, cursor);
      if (at < 0) return { verdict: 'skip_reconstruct', reason: `子段在原文里定位不到 id=${child.id}`, notes };
      parts.push({ text, start: at, end: at + text.length });
      landedIds.push(String(child.id));
      cursor = at + text.length;
    }
    const coverage = verifySplitCoverage({
      sourceText,
      parts,
      landedIds,
      metas: memories,
      excludeIds: [sid],
    });
    return { verdict: coverage.ok ? 'pass' : 'block', reason: coverage.reason || '', notes };
  };

  const emptyBucket = () => ({ n: 0, pass: 0, block: 0, skip: 0, falseKill: 0, reasons: {} });
  const report = { no_source: emptyBucket(), all_parts_deduped: emptyBucket(), zero_parts: emptyBucket() };
  const falseKillIds = [];
  for (const [kind, list] of Object.entries(samples)) {
    for (const entry of list) {
      const got = replay(entry);
      const bucket = report[kind];
      bucket.n += 1;
      if (got.verdict === 'pass') bucket.pass += 1;
      else if (got.verdict === 'block') {
        bucket.block += 1;
        bucket.reasons[got.reason || '?'] = (bucket.reasons[got.reason || '?'] || 0) + 1;
        const prodArchived = (entry.src?.status || 'active') === 'archived';
        if (prodArchived) {
          bucket.falseKill += 1;
          if (falseKillIds.length < 10) falseKillIds.push({ op: String(entry.op), sid: entry.sid, reason: got.reason });
        }
      } else {
        bucket.skip += 1;
        bucket.reasons[got.verdict] = (bucket.reasons[got.verdict] || 0) + 1;
      }
    }
  }
  out.samples = report;

  // 覆盖闸回放：拿最近 done、且子段能在原文里逐字定位的账（切分点协议之后的那批）
  const coverageReplay = emptyBucket();
  let reconstructFailed = 0;
  let scanned = 0;
  for (const op of doneOps) {
    const payload = parsePayload(op);
    const sid = String(payload.source_id || '');
    const src = sid ? byId.get(sid) : null;
    if (!sid || !src) continue;
    scanned += 1;
    const entry = { op: op.id, sid, payload, src, children: childStmt.all(`sleep:split:${sid}`), lastError: op.last_error };
    const got = replay(entry);
    if (got.verdict === 'skip_no_parts') continue;
    if (got.verdict === 'skip_reconstruct') {
      reconstructFailed += 1;
      continue;
    }
    coverageReplay.n += 1;
    if (got.notes.includes('entry_version_mismatch')) coverageReplay.entryVersionMismatch = (coverageReplay.entryVersionMismatch || 0) + 1;
    if (got.verdict === 'pass') coverageReplay.pass += 1;
    else {
      coverageReplay.block += 1;
      coverageReplay.reasons[got.reason || '?'] = (coverageReplay.reasons[got.reason || '?'] || 0) + 1;
      if ((entry.src?.status || 'active') === 'archived') {
        coverageReplay.falseKill += 1;
        if (falseKillIds.length < 10) falseKillIds.push({ op: String(entry.op), sid, reason: got.reason });
      }
    }
    if (coverageReplay.n >= 80) break;
  }
  out.coverageReplay = { scanned, reconstructFailed, entryVersionMismatch: coverageReplay.entryVersionMismatch || 0, ...coverageReplay };
  out.falseKillIds = falseKillIds;

  // ④ 原条 active 但 supersededBy 指向子段（票面 10 条）
  // 口径：原条自己 active，supersededBy 指向的正是它自己的子段（c.source = 'sleep:split:' || m.id）。
  // updatedAt 为空的那批是 save 带 supersedes 那条路径写的（子段保存那一步顺手改的），票面说的 10 条。
  const activeSuperseded = db.prepare(`
    SELECT m.id, m.supersededBy, m.updatedAt FROM memories m
    WHERE (m.status IS NULL OR m.status <> 'archived')
      AND m.supersededBy IS NOT NULL AND m.supersededBy <> ''
      AND EXISTS (SELECT 1 FROM memories c WHERE c.id = m.supersededBy AND c.source = 'sleep:split:' || m.id)
    ORDER BY m.id
  `).all();
  out.activeSupersededByChild = activeSuperseded.map((row) => ({
    id: String(row.id),
    supersededBy: String(row.supersededBy),
    updatedAt: row.updatedAt ? String(row.updatedAt) : null,
  }));
  out.activeSupersededByChildSavePath = out.activeSupersededByChild.filter((row) => row.updatedAt === null);
  out.activeSupersededByChildOtherUpdated = out.activeSupersededByChild.filter((row) => row.updatedAt !== null);

  db.close();
  fs.rmSync(dir, { recursive: true, force: true });
  console.log(JSON.stringify(out, null, 2));
}

main();
