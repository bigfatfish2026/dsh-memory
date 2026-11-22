/**
 * 拆分自足性段级信号的回放（2026-11-20 P0 阶段一，票 t-mwogzpga-ga10）。
 *
 * 只读：把生产库 memory.db（连同 -wal/-shm）**拷贝**到临时目录，全程只打开副本；
 * 生产库一个字节都不写，也不在它上面跑 dueOps。
 *
 * 样本：最近 done 的 split_item 里「payload.source_id 在 memories 里找得到」的原条，
 * 默认凑满 100 条（任务书要求 100 个原条样本）。
 * 切分：优先用真实落库子段（source='sleep:split:<sid>'）按原文 indexOf 还原区间；
 *       没有子段的，按句末（。！？换行）切一遍兜底，只为跑判据，不改正文。
 * 判据：lib/agents/split.js 的 splitSignalsOf（阶段一四条信号，纯函数，不写库）。
 *
 * 用法：node scripts/replay-split-signals.mjs <生产库路径> [样本数] [报告文件]
 * 库路径取 argv[2]，没给就读 DSH_MEMORY_DB；样本默认 100，报告写到 stdout。
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { splitSignalsOf } from '../lib/agents/split.js';

const PROD = (process.argv[2] || process.env.DSH_MEMORY_DB || '').trim();
if (!PROD) {
  console.error('缺少库路径：node scripts/replay-split-signals.mjs <生产库路径>（或设 DSH_MEMORY_DB）');
  process.exit(2);
}
const WANT = Number(process.argv[3] || 100);
const OUT = process.argv[4] || '';
const SCAN = Math.max(WANT * 8, 800);

const CODES = ['split_delimiter', 'split_deictic', 'split_premise', 'split_process'];

function exportReplica() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-split-signals-'));
  const target = path.join(dir, 'memory.db');
  fs.copyFileSync(PROD, target);
  for (const suffix of ['-wal', '-shm']) {
    if (fs.existsSync(`${PROD}${suffix}`)) fs.copyFileSync(`${PROD}${suffix}`, `${target}${suffix}`);
  }
  return { dir, target };
}

function parsePayload(value) {
  try {
    return JSON.parse(String(value || '{}'));
  } catch {
    return {};
  }
}

/**
 * 真实布局：直接拿这一条已落库的子段当 parts（它们就是上一轮生产的最终段）。
 * 不再按原文 indexOf 还原——存量里父条正文被改过的不少，子段拼不回原文是常态，
 * 回放判据只看段正文，不需要区间。
 */
function partsFromChildren(children) {
  const parts = [];
  for (const child of children) {
    const body = String(child.text || '');
    if (body) parts.push({ text: body, from: 'children' });
  }
  return parts.length >= 2 ? parts : null;
}

/** 兜底切分：只按句末断句，纯为了跑判据，不代表生产布局。 */
function partsBySentence(text) {
  const parts = [];
  let buf = '';
  for (const ch of String(text || '')) {
    buf += ch;
    if ('。！？\n'.includes(ch)) {
      if (buf.trim()) parts.push({ text: buf.trim(), from: 'sentence' });
      buf = '';
    }
  }
  if (buf.trim()) parts.push({ text: buf.trim(), from: 'sentence' });
  return parts;
}

function main() {
  const replica = exportReplica();
  const db = new DatabaseSync(replica.target, { readOnly: true });
  const lines = [];
  const say = (line) => {
    lines.push(line);
    console.log(line);
  };

  try {
    const ops = db.prepare(
      `SELECT id, dedupe_key, payload_json, last_error FROM pending_ops
       WHERE kind = 'split_item' AND status = 'done'
       ORDER BY id DESC LIMIT ?`,
    ).all(SCAN);

    const samples = [];
    let noSource = 0;
    let fromChildren = 0;
    let fromSentence = 0;
    const getSource = db.prepare('SELECT id, text, status FROM memories WHERE id = ?');
    // 子段不分状态：archived/merged 也是一轮生产出来的真实段，判据只看正文
    const getChildren = db.prepare('SELECT text FROM memories WHERE source = ? ORDER BY rowSeq');

    for (const op of ops) {
      if (samples.length >= WANT) break;
      const payload = parsePayload(op.payload_json);
      const sid = String(payload.source_id || '');
      if (!sid) { noSource += 1; continue; }
      const source = getSource.get(sid);
      if (!source) { noSource += 1; continue; }
      const text = String(source.text || payload.text || '');
      const children = getChildren.all(`sleep:split:${sid}`);
      let parts = partsFromChildren(children);
      if (parts) fromChildren += 1;
      else {
        parts = partsBySentence(text);
        fromSentence += 1;
      }
      samples.push({
        opId: op.id,
        sourceId: sid,
        lastError: String(op.last_error || ''),
        text,
        parts,
      });
    }

    const perCode = Object.fromEntries(CODES.map((code) => [code, { samples: 0, parts: 0 }]));
    const hits = [];
    for (const sample of samples) {
      const signals = splitSignalsOf(sample.parts);
      if (!signals.length) continue;
      const codes = new Set();
      for (const signal of signals) {
        perCode[signal.code].parts += 1;
        codes.add(signal.code);
        hits.push({
          op: sample.opId,
          sourceId: sample.sourceId,
          code: signal.code,
          index: signal.index,
          head: String(sample.parts[signal.index]?.text || '').trim().slice(0, 30),
          part: String(sample.parts[signal.index]?.text || ''),
        });
      }
      for (const code of codes) perCode[code].samples += 1;
    }

    say(`副本库=${replica.target}`);
    say(`生产库=${PROD}（只读拷贝，未写入；未在它上面跑 dueOps）`);
    say(`done 样本扫描=${ops.length}；原条存在=${samples.length}；无原条跳过=${noSource}`);
    say(`parts 来源：落库子段还原=${fromChildren}，按句末兜底=${fromSentence}`);
    const multi = samples.filter((s) => splitSignalsOf(s.parts).length > 0).length;
    say(`命中至少一条信号的样本=${multi}/${samples.length}`);
    for (const code of CODES) {
      const stat = perCode[code];
      say(`${code}: 命中样本 ${stat.samples} 条，命中段 ${stat.parts} 段`);
    }
    say('');
    say('命中明细（op / code / 段下标 / 段首 30 字）：');
    hits.forEach((hit, i) => {
      say(`[${i + 1}] op=${hit.op} src=${hit.sourceId} code=${hit.code} at=${hit.index} ${hit.head}`);
    });

    if (OUT) fs.writeFileSync(OUT, `${lines.join('\n')}\n`, 'utf8');
  } finally {
    db.close();
    fs.rmSync(replica.dir, { recursive: true, force: true });
  }
}

main();
