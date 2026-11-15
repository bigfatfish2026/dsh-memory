/**
 * memory_link：记忆之间取代边的唯一写入口（2026-11-13， 改优先级）。
 *
 * 为什么只留一个入口：取代边有三个产出方——
 *   ① entity_extract 队列（memory_save 落库后判，origin='queue'，有正文 + 同实体候选上下文）；
 *   ② 整理轮（sleep / consolidate，origin='curate'，只看一批聚类，信息更少）；
 *   ③ 旧列回填（scripts/backfill-links-from-supersedes.mjs，origin='legacy'）。
 *
 * 2026-11-13 改口：
 *   - 一条 src 允许多条 supersedes 出边（去重只按 (src,dst,rel)）；取代本来是一对多，
 *     一条总述被两条细分各自推翻很正常，旧 partial unique 已从 schema 里删掉。
 *   - 来源优先级 queue > curate > legacy：高优先级可以顶掉低优先级的边，低优先级只补缺。
 *     这样回填出来的 legacy 边不会把 src 锁死，队列以后判出新取代还能改。
 *   - 同级之间允许多目标并存，同一条边（src,dst,rel）已存在就不重复写。
 *
 * 仍然保留的规矩：
 *   - target 必须存在、是 active、且不是自己；
 *   - 被 queue 边挡住的写入返回 queue_edge_kept，被更高优先级非 queue 边挡住返回 src_locked；
 *   - 写失败返回 {ok:false, reason}，不抛错——取代判错不该把整批实体抽取带崩。
 */

import type { DatabaseSync } from 'node:sqlite';
import { withNestedTransaction } from './shared/tx.js';

/** 写取代边的结果：ok=false 时 reason 说明为什么没写。 */
export interface SupersedesLinkResult {
  ok: boolean;
  reason?: string;
  inserted?: boolean;
  replaced?: string[];
  existingOrigin?: string;
  blockedBy?: string[];
}

/** writeSupersedesLink 的入参。 */
export interface SupersedesLinkOptions {
  src?: string;
  dst?: string;
  origin?: string;
  attrKey?: string;
  now?: number;
}

/** memory_link 的一行（本文件只读 dst 与 origin）。 */
interface MemoryLinkRow {
  dst: string;
  origin: string;
}

/** 来源优先级：数字大的可以覆盖数字小的。curate 是整理轮的统称，sleep/consolidate 同档。 */
const ORIGIN_RANK: Record<string, number> = {
  queue: 3,
  curate: 2,
  consolidate: 2,
  sleep: 2,
  legacy: 1,
  manual: 1,
};

function rankOf(origin: unknown): number {
  return ORIGIN_RANK[String(origin || '').toLowerCase()] ?? 1;
}

/** 表在不在：老库还没跑 apply-curate-schema 时，调用方要能优雅跳过而不是整批回滚。 */
export function hasMemoryLinkTable(db: DatabaseSync | null | undefined): boolean {
  if (!db) return false;
  try {
    return Boolean(db.prepare("SELECT 1 AS ok FROM sqlite_master WHERE type='table' AND name='memory_link'").get());
  } catch {
    return false;
  }
}

/** 目标记忆还活着吗：缺行、archived、merged、deleted 都算不活。 */
function targetIsActive(db: DatabaseSync, id: string): boolean {
  try {
    const row = db.prepare('SELECT id, status FROM memories WHERE id=?').get(id) as { status?: unknown } | undefined;
    if (!row) return false;
    const status = String(row.status || 'active').toLowerCase();
    return status === 'active';
  } catch {
    return false;
  }
}

/**
 * 写一条取代边（src 取代 dst）。
 *
 * 优先级 queue(3) > curate(2) > legacy(1)：
 *   - 同 dst 已有同级或更高优先级的边 → 不重复写，reason=edge_exists；
 *   - 同 dst 已有更低优先级的边 → 升级 origin（删低再插高，不产生第二行）；
 *   - 不同 dst 已有更高优先级的边 → 被挡，reason=queue_edge_kept / src_locked；
 *   - 不同 dst 只有更低优先级的边 → 顶掉它们（高优先级覆盖），再插新边；
 *   - 同级不同 dst → 允许并存（一对多）。
 *
 * 2026-11-15（审计实体中-10）：读现状原来在 SAVEPOINT 外面，
 * 两个进程交错时 curate 可能读完没看到 queue 刚写的边，于是两种 dst 并存、顶替失效。
 * 现在读+写同一个事务：调用方已在事务里就开 SAVEPOINT（失败只回滚这一小段），
 * 没有事务就自己 BEGIN IMMEDIATE 拿写锁（读到的就是写之前的最新状态）。
 *
 * @param {object} db sqlite 连接（store.db()）
 * @param {object} opts
 * @param {string} opts.src 新记忆 id
 * @param {string} opts.dst 被取代的旧记忆 id
 * @param {string} [opts.origin='queue'] queue | curate | sleep | consolidate | legacy | manual
 * @param {string} [opts.attrKey=''] 同一属性的键；判不出就留空
 * @param {number} [opts.now=Date.now()]
 * @returns {{ok:boolean, reason?:string, inserted?:boolean, replaced?:string[]}}
 */
export function writeSupersedesLink(db: DatabaseSync | null | undefined, { src, dst, origin = 'queue', attrKey = '', now = Date.now() }: SupersedesLinkOptions = {}): SupersedesLinkResult {
  const from = String(src || '').trim();
  const to = String(dst || '').trim();
  const who = String(origin || 'queue');
  if (!db) return { ok: false, reason: 'no_db' };
  if (!from || !to) return { ok: false, reason: 'bad_ids' };
  if (from === to) return { ok: false, reason: 'self_link' };
  if (!hasMemoryLinkTable(db)) return { ok: false, reason: 'no_table' };

  // 调用方已在事务里就开 SAVEPOINT（失败只回滚这一小段），没有事务就自己 BEGIN IMMEDIATE 拿写锁
  // （读到的就是写之前的最新状态）。两种形态在 shared/tx.js 的 withNestedTransaction 里。
  const abort = (reason: string): never => {
    const error = new Error(reason) as Error & { linkReason?: string };
    error.linkReason = reason;
    throw error;
  };

  try {
    return withNestedTransaction(db!, 'memory_link_write', () => {
      // 目标记忆活不活、src 现有哪些边，都要在写锁里读：读和写之间不许别人插一脚
      let active: boolean;
      try {
        active = targetIsActive(db!, to);
      } catch {
        return abort('read_failed');
      }
      if (!active) return abort('target_not_active');
      let existing: MemoryLinkRow[] = [];
      try {
        existing = db!
          .prepare("SELECT dst, origin FROM memory_link WHERE src=? AND rel='supersedes'")
          .all(from) as unknown as MemoryLinkRow[];
      } catch {
        return abort('read_failed');
      }

      const mineRank = rankOf(who);
      const same = existing.find((e) => e.dst === to);
      // 同一条边已经在了：同级或更高优先级都算 edge_exists（幂等，不重复写）。
      if (same && rankOf(same.origin) >= mineRank) {
        return { ok: false, reason: 'edge_exists', existingOrigin: same.origin };
      }

      const blockers = existing.filter((e) => e.dst !== to && rankOf(e.origin) > mineRank);
      if (blockers.length) {
        return {
          ok: false,
          reason: blockers.some((b) => b.origin === 'queue') ? 'queue_edge_kept' : 'src_locked',
          blockedBy: blockers.map((b) => `${b.dst}(${b.origin})`),
        };
      }

      // 只顶掉严格更低优先级的边（同 dst 的也算：升级 origin 用）；同级不同 dst 保留（一对多）。
      const replaced = existing.filter((e) => rankOf(e.origin) < mineRank).map((e) => e.dst) || [];

      try {
        if (replaced.length) {
          const del = db!.prepare("DELETE FROM memory_link WHERE src=? AND rel='supersedes' AND dst=?");
          for (const d of replaced) del.run(from, d);
        }
        db!.prepare(
          `INSERT INTO memory_link(src, dst, rel, origin, attr_key, ts, created_ts)
           VALUES (?, ?, 'supersedes', ?, ?, ?, ?)`,
        ).run(from, to, who, String(attrKey || ''), new Date(now).toISOString(), now);
      } catch (error) {
        return abort(`constraint:${String((error as { message?: unknown })?.message || error).slice(0, 80)}`);
      }
      return { ok: true, inserted: true, replaced };
    });
  } catch (error) {
    const reason = (error as { linkReason?: string })?.linkReason || `constraint:${String((error as { message?: unknown })?.message || error).slice(0, 80)}`;
    return { ok: false, reason };
  }
}
