#!/usr/bin/env node
/**
 * 把旧列 memories.supersedes 回填进 memory_link。
 *
 * 口径（判据写死，跟票面一致）：
 *   A 拆分血缘：source='split' / 'split:*' / 'sleep:split*'        → rel='split_of'
 *   B 合并血缘：source='sleep:merge*'，或 src.status='merged'       → rel='merged_into'
 *   C 真取代  ：人工逐条判过的 41 条（src 非 merged、dst 早于 src） → rel='supersedes'
 *   D 其余    ：不算，计 skipped（错配 / 同义重复 / 旧轮合并残留之外的杂项）
 *
 * 为什么 C 用白名单而不是规则：84 条非血缘边里，光靠「正文含演进词」只能捞到 31 条，
 * 剩下 10 条是纯数据更新（如 29579 块→29799 块、133.8s→266.9s），规则判不出来。
 * 41 条是逐条看过的结果，写死在 REAL_SUPERSEDES 里，改之前先改票。
 *
 * 落库规则：
 *   - 全部 origin='legacy'，attr_key 存来源标记（memories.source 原值），不新增列；
 *   - 去重靠 uq_memory_link_edge(src,dst,rel)：INSERT OR IGNORE，重复跑 inserted=0；
 *   - 悬空 dst（旧列有、memories 里没有）、自环、时间倒挂一律跳过并计数，不建悬空边；
 *   - 整段 BEGIN IMMEDIATE ... COMMIT；--dry-run 走只读连接，一条不写。
 *
 * 用法：node scripts/backfill-links-from-supersedes.mjs [--db /path/to/memory.db] [--dry-run]
 */
import { openDb, resolveDbPath } from './lib/db.mjs';

const argv = process.argv.slice(2);
const argOf = (k, d) => (argv.includes(k) ? argv[argv.indexOf(k) + 1] : d);
const DB = argOf('--db', resolveDbPath());
const DRY = argv.includes('--dry-run');

/** 票 逐条判过的真取代：src>dst。36 个 src、41 条边。 */
export const REAL_SUPERSEDES = new Set([
  // agent 来源 9 条
  'm_mvrkz99j_280u>m_mvrkru7z_aodc',
  'm_mvvw2x9k_udxy>m_mvuebzou_id4a',
  'm_mvvw2x9k_udxy>m_mvuebztv_5ap8',
  'm_mvvw2x9k_udxy>m_mvuialh4_30nd',
  'm_mvvyon5p_hlg7>m_mvtikjy8_ug4q',
  'm_mvx43p1d_zjta>m_mvtle3sb_q3va',
  'm_mvyve32n_dg65>m_mvytjb3p_aids',
  'm_mvyve6dn_uyro>m_mvyoeum7_kg5z',
  'm_mw07md99_wv6k>m_mvybhy0b_q0c8',
  // sleep 来源 30 条
  'm_mvtl7pm6_ppt0>m_mvtk6qvx_uvve',
  'm_mvtldy6j_hf49>m_mvtk6qz5_jgf8',
  'm_mvtldyhv_hzu0>m_mvtl7pm6_ppt0',
  'm_mvu01uh3_ehlr>m_mvtle1kp_temh',
  'm_mvu9dmdo_ugvu>m_mvu8ij48_n35d',
  'm_mvue9ai7_kz1z>m_mvtlf5cn_x5he',
  'm_mvue9ap0_avx4>m_mvu01xfx_37oe',
  'm_mvue9ap0_avx4>m_mvtlf8ff_rx4u',
  'm_mvvgkk1a_ghoi>m_mvul2f1s_go31',
  'm_mvvgkk5n_8i7y>m_mvum4p14_vv8g',
  'm_mvvgkke7_r32x>m_mvuzoenr_ead8',
  'm_mvvgkkof_jxc0>m_mvtic6bc_pinu',
  'm_mvwvzui2_svky>m_mvw2ef8x_9ekb',
  'm_mvwvzvbg_oj9p>m_mvvxtozj_it6u',
  'm_mvybeo18_1yr1>m_mvtidmut_fy0v',
  'm_mvybeod9_ojdx>m_mvxtcrmx_j3nj',
  'm_mvytgq44_00vv>m_mvyrveke_cyj5',
  'm_mvytgq44_00vv>m_mvybivrh_q9cy',
  'm_mvytgq7n_nmss>m_mvyrvd3o_3fs3',
  'm_mvytgqb5_llpa>m_mvyopynz_6yc4',
  'm_mvzquug6_cyhf>m_mvzfh9ad_4adi',
  'm_mvzquug6_cyhf>m_mvzgeuuw_b4fl',
  'm_mw0jrcai_s691>m_mw0b4ros_zwa7',
  'm_mw330vf2_uy9w>m_mvtl9txv_gc5v',
  'm_mw3iq9td_olnm>m_mw2lv8l9_wgeg',
  'm_mw3k0uy6_8cu1>m_mw26o27y_fyz1',
  'm_mw3pxpz9_qilr>m_mw21hh0j_32xb',
  'm_mw4xxi1s_vf1s>m_mw25yuly_307z',
  'm_mw66y58w_t52i>m_mw3jctb0_8yeq',
  'm_mw8blkwt_dela>m_mw7fgp0p_a0c3',
  // consolidator 来源 2 条
  'm_mweeyk1u_2gn1>m_mweesn6f_405g',
  'm_mwej04ed_h8qn>m_mwegcvtr_ute7',
]);

const EDGE_SQL = `
SELECT m.id            AS src,
       m.source        AS source,
       m.ts            AS src_ts,
       COALESCE(NULLIF(m.status, ''), 'active') AS src_status,
       j.value         AS dst,
       d.id            AS dst_id,
       d.ts            AS dst_ts
  FROM memories m, json_each(m.supersedes) j
  LEFT JOIN memories d ON d.id = j.value
 WHERE m.supersedes IS NOT NULL
   AND m.supersedes <> ''
   AND m.supersedes <> '[]'
   AND json_valid(m.supersedes)
`;

/** A/B/C/D 四类判据。返回 {rel, reason}，rel=null 表示不回填。 */
export function classifyEdge(row) {
  const src = String(row.src || '');
  const dst = String(row.dst || '').trim();
  const source = String(row.source || '');
  const srcStatus = String(row.src_status || 'active').toLowerCase();
  if (!dst) return { rel: null, reason: 'empty_dst' };
  if (src === dst) return { rel: null, reason: 'self_loop' };
  // 悬空目标不建边：读取方不该拿到指向不存在记忆的边。
  if (!row.dst_id) return { rel: null, reason: 'dangling_dst' };
  // A 拆分血缘
  if (source === 'split' || source.startsWith('split:') || source.startsWith('sleep:split')) {
    return { rel: 'split_of', reason: 'lineage_split' };
  }
  // B 合并血缘（含旧整理轮没打 sleep:merge 标签、但 src 已经被合并走的残留）
  if (source.startsWith('sleep:merge') || srcStatus === 'merged') {
    return { rel: 'merged_into', reason: 'lineage_merge' };
  }
  // C 真取代：必须过人工白名单，且目标确实更早。
  if (!REAL_SUPERSEDES.has(`${src}>${dst}`)) return { rel: null, reason: 'not_audited_supersedes' };
  if (row.dst_ts && row.src_ts && String(row.dst_ts) >= String(row.src_ts)) {
    return { rel: null, reason: 'time_inverted' };
  }
  return { rel: 'supersedes', reason: 'audited_supersedes' };
}

function bump(obj, key) {
  obj[key] = (obj[key] || 0) + 1;
}

export function planBackfill(db) {
  const rows = db.prepare(EDGE_SQL).all();
  const plan = [];
  const skipped = {};
  const planned = {};
  for (const row of rows) {
    const { rel, reason } = classifyEdge(row);
    if (!rel) {
      bump(skipped, reason);
      continue;
    }
    bump(planned, rel);
    plan.push({
      src: String(row.src),
      dst: String(row.dst),
      rel,
      origin: 'legacy',
      attr_key: String(row.source || ''),
    });
  }
  return { scanned: rows.length, plan, planned, skipped };
}

export function backfillLinks(dbPath, { dryRun = false, logger = console } = {}) {
  const db = openDb(dbPath, dryRun ? { readOnly: true } : {});
  try {
    db.exec('PRAGMA busy_timeout = 15000;');
    const hasTable = db
      .prepare("SELECT 1 AS ok FROM sqlite_master WHERE type='table' AND name='memory_link'")
      .get();
    if (!hasTable) throw new Error('memory_link 不存在，先跑 scripts/apply-curate-schema.mjs');

    const { scanned, plan, planned, skipped } = planBackfill(db);
    const inserted = {};
    if (!dryRun) {
      const stmt = db.prepare(
        `INSERT OR IGNORE INTO memory_link(src, dst, rel, origin, attr_key, ts, created_ts)
         VALUES (?, ?, ?, 'legacy', ?, ?, ?)`,
      );
      const now = Date.now();
      const iso = new Date(now).toISOString();
      db.exec('BEGIN IMMEDIATE');
      try {
        for (const e of plan) {
          const r = stmt.run(e.src, e.dst, e.rel, e.attr_key, iso, now);
          if (r.changes) bump(inserted, e.rel);
          else bump(inserted, 'ignored');
        }
        db.exec('COMMIT');
      } catch (error) {
        try {
          db.exec('ROLLBACK');
        } catch {
          /* 连接坏了由 sqlite 回滚 */
        }
        throw error;
      }
    }

    const tableStats = {
      total: db.prepare('SELECT count(*) AS n FROM memory_link').get().n,
      byRel: db
        .prepare('SELECT rel, count(*) AS n FROM memory_link GROUP BY rel ORDER BY n DESC')
        .all(),
      byOrigin: db
        .prepare('SELECT origin, count(*) AS n FROM memory_link GROUP BY origin ORDER BY n DESC')
        .all(),
    };
    const out = { db: dbPath, dryRun, scanned, planned, skipped, tableStats };
    if (!dryRun) out.inserted = inserted;
    if (logger) {
      logger.log(
        `[${dryRun ? 'dry-run' : 'backfill'}] 扫 ${scanned} 条旧边：` +
          `计划 split_of=${planned.split_of || 0} merged_into=${planned.merged_into || 0} supersedes=${planned.supersedes || 0}` +
          `，跳过 ${JSON.stringify(skipped)}`,
      );
      if (!dryRun) logger.log(`写入 ${JSON.stringify(inserted)}；表内 ${tableStats.total} 行`);
    }
    return out;
  } finally {
    db.close();
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  const out = backfillLinks(DB, { dryRun: DRY });
  console.log(JSON.stringify(out, null, 2));
}
