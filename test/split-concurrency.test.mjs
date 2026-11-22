/**
 * 拆分消费段的并发。
 *
 * 票面：split 原来是「一条一条问模型再落库」，单条 19-24 秒，599 条要跑五小时。
 * 改成 ingest 那套「小批领活 → 批内 N 路并发问模型 → 串行落库」，并且：
 *   - 一轮内并发固定；
 *   - 预领粒度 = 并发数（不许再一次把几百条全标 running）；
 *   - 单条失败只影响它自己，attempts+1，同轮不原地重试；
 *   - 自适应关闭时用固定值。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { DatabaseSync } from 'node:sqlite';
import { applySchema } from '../scripts/apply-pipeline-schema.mjs';
import { consumeSplitOps } from '../lib/pipeline-split.js';
import { enqueue } from '../lib/ingest-index.js';
import { selectIngestConcurrency } from '../lib/sleep.js';

const GB = 1024 ** 3;

const THINGS = [
  '风扇', '网线', '电源', '硬盘', '内存条', '机柜', '交换机', '路由', '散热片', '排线',
  '充电器', '键盘', '显示器', '音箱', '传感器', '摄像头', '读卡器', '扩展坞', '集线器', '接线板',
];

function tmpDb() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'dsh-split-conc-'));
  const dbPath = path.join(dir, 'memory.db');
  new DatabaseSync(dbPath).close();
  applySchema(dbPath, { logger: () => {} });
  return { dir, dbPath, db: new DatabaseSync(dbPath) };
}

/** 每条两份正文：两句话各 30+ 字（避开正文 lint 的 too_short），切点按句号落在第二句开头。 */
function sourceRows(count) {
  return Array.from({ length: count }, (_, i) => {
    const thing = THINGS[i % THINGS.length];
    const a = `小林去机房巡检时看了${thing}的运转情况，顺手记下当天的温湿度与磁盘读数。`;
    const b = `小林把${thing}上的灰擦干净，又核对了一遍当天的处理经过，最后写进值班记录里。`;
    return { id: `m_${i + 1}`, text: `${a}${b}` };
  });
}

function fakeStore(rows) {
  const metas = rows.map((r) => ({ ...r, status: 'active', tags: [], entities: [] }));
  let saved = 0;
  let live = 0;
  let peak = 0;
  const texts = [];
  return {
    metas: () => metas,
    typeOf: () => 'world',
    async save(text) {
      live += 1;
      peak = Math.max(peak, live);
      // 给写库留一点真实 await 窗口：commit 若并行，peak 会 >1
      await new Promise((r) => setTimeout(r, 3));
      saved += 1;
      const rec = { id: `new_${saved}`, text };
      metas.push({ id: rec.id, text, status: 'active', tags: [], entities: [] });
      texts.push(text);
      live -= 1;
      return rec;
    },
    async archive(id) {
      const m = metas.find((x) => x.id === id);
      if (m) m.status = 'archived';
    },
    report: () => ({ saved, peak, texts: [...texts] }),
  };
}

/**
 * 假 llm：按提示词里的原文分派。坏条返回两条通道都坏的流，split() 会落 pending。
 * peak 记同时在飞的模型调用数；probe 在每次调用开始（同步）时探一下库里的 running 数。
 */
function fakeLlm({ badWords = [] } = {}) {
  const calls = [];
  let live = 0;
  let peak = 0;
  let probe = null;
  const llm = {
    calls,
    get peak() {
      return peak;
    },
    setProbe(fn) {
      probe = fn;
    },
    stream(options) {
      calls.push(options);
      if (probe) probe();
      const prompt = String(options?.messages?.[0]?.content?.[0]?.text || '');
      const m = /）：([\s\S]*?)\n\n输出/.exec(prompt);
      const src = m ? m[1] : '';
      const bad = badWords.some((w) => src.includes(w));
      return (async function* gen() {
        live += 1;
        peak = Math.max(peak, live);
        try {
          await new Promise((r) => setTimeout(r, 8));
          if (bad) {
            yield { type: 'text-delta', text: '这不是 JSON' };
            yield { type: 'tool-call-delta', index: 0, name: 'submit_parts', argumentsDelta: '{oops' };
            yield { type: 'finish', reason: { kind: 'stop' } };
            return;
          }
          // 2026-11-15 切分点协议：只给第二段开头的定位串，正文由代码切
          const cut = src.indexOf('。') + 1;
          const cuts = src && cut > 0 && cut < src.length ? [src.slice(cut, cut + 8)] : [];
          const args = JSON.stringify({ cuts, unavoidable: false, reason: '' });
          yield {
            type: 'tool-call-delta', index: 0, id: 'call_1', name: 'submit_parts', argumentsDelta: args.slice(0, 20),
          };
          yield { type: 'tool-call-delta', index: 0, argumentsDelta: args.slice(20) };
          yield { type: 'finish', reason: { kind: 'stop' } };
        } finally {
          live -= 1;
        }
      })();
    },
  };
  return llm;
}

function seedOps(db, rows, order = 'asc') {
  const list = order === 'desc' ? [...rows].reverse() : rows;
  for (const row of list) {
    enqueue(db, 'split_item', row.id, { source_id: row.id, text: row.text, type: 'world' });
  }
}

function opRows(dbPath) {
  const db = new DatabaseSync(dbPath, { readOnly: true });
  try {
    return db.prepare('SELECT dedupe_key, status, attempts FROM pending_ops ORDER BY dedupe_key').all();
  } finally {
    db.close();
  }
}

test('20 条 + 并发 4：全部处理、写库峰值 1、顺序无关结果一致', async () => {
  const run = async (order) => {
    const { db, dbPath } = tmpDb();
    const rows = sourceRows(20);
    const store = fakeStore(rows);
    const llm = fakeLlm();
    seedOps(db, rows, order);
    const logs = [];
    const stats = await consumeSplitOps({
      db, store, llm, limit: Infinity, concurrency: 4, log: (line) => logs.push(line),
    });
    const ops = opRows(dbPath);
    db.close();
    return { stats, logs, ops, saved: store.report(), llm };
  };

  const first = await run('asc');
  assert.equal(first.stats.scanned, 20, '20 条全领到');
  assert.equal(first.stats.tried, 20, '20 条都问过模型');
  assert.equal(first.stats.inserted, 40, '每条拆两段，全入库');
  assert.equal(first.stats.failed, 0);
  assert.equal(first.stats.archived, 20, '原条都归档');
  assert.equal(first.saved.saved, 40);
  assert.equal(first.saved.peak, 1, '并发 4 也只许串行写库');
  assert.equal(first.llm.peak, 4, '模型调用确实并发到 4 路');
  assert.ok(first.ops.every((r) => r.status === 'done'), '20 条 op 全 done');

  const again = await run('desc');
  assert.deepEqual(again.saved.texts.slice().sort(), first.saved.texts.slice().sort(), '领活顺序变了，落库结果一致');
  assert.equal(again.saved.peak, 1);
  assert.equal(again.stats.inserted, 40);

  // 逐条成功行带 ms（跟 ingest 同风格），汇总行按票面文案
  assert.match(first.logs.join('\n'), /\[split\] item=m_\d+ parts=2 retry=0 rejected=0 inserted=2 deduped=0 replayed=0 ms=\d+/);
  // 2026-11-20 汇总口径：只数真正落库的，其余分类单列（以前写「成功 = scanned - failed」，
  // 把 skipped/versionMismatch/no_source_id 都算成成功）
  assert.match(first.logs.join('\n'), /\[split\] 并发 4 路 条 20 落库 20 全判重 0 跳过 0 回队 0（含不可拆 0） 版本不符 0 覆盖未过 0 判死 0 失败 0 分类合计 20/);
});

test('单条失败：其他照常、只 attempts+1、同轮不原地重试', async () => {
  const { db, dbPath } = tmpDb();
  const rows = sourceRows(20);
  // 第 2 条正文里塞坏词，让它的模型流两条通道都坏
  rows[1].text = `${rows[1].text}坏条`;
  const store = fakeStore(rows);
  const llm = fakeLlm({ badWords: ['坏条'] });
  seedOps(db, rows);

  const logs = [];
  const stats = await consumeSplitOps({
    db, store, llm, limit: Infinity, concurrency: 4, log: (line) => logs.push(line),
  });
  const ops = opRows(dbPath);
  db.close();

  assert.equal(stats.failed, 1, '只有坏条算失败');
  assert.equal(stats.inserted, 38, '其余 19 条各拆两段全入库');
  const bad = ops.find((r) => r.dedupe_key === 'm_2');
  assert.equal(bad.status, 'pending', '坏条回 pending 等下一轮');
  assert.equal(bad.attempts, 1, '同轮不许原地重试，attempts 只能 +1');
  assert.equal(ops.filter((r) => r.status === 'done').length, 19);
  assert.equal(store.report().peak, 1);
  assert.match(logs.join('\n'), /\[split\] 并发 4 路 条 20 落库 19 全判重 0 跳过 0 回队 1（含不可拆 0） 版本不符 0 覆盖未过 0 判死 0 失败 1 分类合计 20/);
});

test('自适应关闭 → 用固定值；固定值就是实际路数', async () => {
  // 关掉自适应 + 固定 4：哪怕负载高、温度爆表也只认 4
  const pick = selectIngestConcurrency(
    { organizeIngestAdaptive: false, organizeIngestConcurrency: 4 },
    { load1: 64, cores: 4, memAvailableBytes: 0, tempC: 90 },
  );
  assert.equal(pick.adaptive, false);
  assert.equal(pick.basis, 'fixed');
  assert.equal(pick.conc, 4, '关闭自适应后不再被负载压到 1');

  const { db } = tmpDb();
  const rows = sourceRows(8);
  const store = fakeStore(rows);
  const llm = fakeLlm();
  seedOps(db, rows);
  const stats = await consumeSplitOps({
    db, store, llm, limit: Infinity, concurrency: pick.conc,
  });
  db.close();

  assert.equal(stats.concurrency, 4);
  assert.equal(llm.peak, 4, '实际就是 4 路');
  assert.equal(stats.inserted, 16);
});

test('预领粒度：同时在 running 的条数不超过领活批次（不重现 599 条全 running）', async () => {
  const { db } = tmpDb();
  const rows = sourceRows(20);
  const store = fakeStore(rows);
  const llm = fakeLlm();
  let runningPeak = 0;
  llm.setProbe(() => {
    const n = Number(db.prepare("SELECT count(*) AS n FROM pending_ops WHERE status = 'running'").get()?.n ?? 0);
    runningPeak = Math.max(runningPeak, n);
  });
  seedOps(db, rows);

  const stats = await consumeSplitOps({ db, store, llm, limit: Infinity, concurrency: 4 });
  db.close();

  assert.equal(stats.inserted, 40);
  assert.ok(runningPeak <= 4, `同时 running ${runningPeak} 条，超过领活批次 4`);
  assert.equal(runningPeak, 4, '确实按批领满 4 条再干活（不是一条条领）');
  assert.ok(runningPeak < 20, '20 条不许一次全标 running');
});
