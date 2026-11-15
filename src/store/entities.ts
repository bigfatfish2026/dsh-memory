/**
 * 实体挂载的写与读。
 *
 * 写：事务内落粗实体行（origin='rule'），失败只记日志、不回滚整条记忆。
 * 读：entityMatches / entityView，给 memory_entity 工具用。
 * 详细路径（agent 抽取、队列消费）在 src/entity-*.ts，这里只管 store 这一侧。
 */
import { insertMemoryEntity } from '../entity-gate.js';
import { keyLog } from '../file-log.js';
import type { KeyLogConfig } from '../file-log.js';
import { clamp01, dateOnly, normalizeType } from './types.js';
import { isActive } from './rows.js';
import { logStore } from './log.js';
import type { CoarseHit, EntityView, MemoryRecord, StoreState } from './types.js';
import type { DatabaseSync } from 'node:sqlite';

/** 事务内写粗实体行：命中就 role='about', origin='rule'，幂等。 */
export function writeCoarseEntities(state: StoreState, d: DatabaseSync, memoryId: string, hits: CoarseHit[] | null | undefined, ts?: string): number {
  if (!hits?.length) return 0;
  const { cfg } = state;
  let n = 0;
  for (const h of hits) {
    try {
      // 同步写入就在这行记账：memory_entity 真插进去时由 insertMemoryEntity 同事务维护
      // entity_surface（count+1 / 建 surface 行），不再等异步 entity_extract 补
      if (insertMemoryEntity(d, {
        memoryId,
        entityId: h.id,
        ts,
        attrs: {},
        origin: 'rule',
        log: (line: string) => keyLog(cfg as KeyLogConfig, line),
      })) n += 1;
    } catch (error) {
      // 实体表坏了不该回滚整条记忆：粗实体是加分项，丢了下轮队列会补
      logStore(cfg, 'WARN', '粗实体落库失败（记忆照写）', `${memoryId}/${h.norm}：${String((error && (error as { message?: unknown }).message) || error)}`);
    }
  }
  return n;
}

export function entityMatches(m: MemoryRecord, name: unknown): boolean {
  const target = String(name || '').toLowerCase().trim();
  if (!target) return false;
  if ((m.entities || []).some((e) => e.toLowerCase().includes(target) || target.includes(e.toLowerCase()))) {
    return true;
  }
  // 老条目没有 entities 字段：名字够长就按文本包含兜底
  return target.length >= 2 && String(m.text || '').toLowerCase().includes(target);
}

/** 按实体聚合：观察网络（S）的本机简化版，不生成摘要，只把相关的 W/B/O 摆齐 */
export function entityView(state: StoreState, name: unknown, limit: number = 8): EntityView {
  const pool = state.metas.filter((m) => isActive(m) && entityMatches(m, name));
  const byType: Record<string, MemoryRecord[]> = { world: [], experience: [], opinion: [], observation: [] };
  for (const m of pool) {
    const type = normalizeType(m.type, m.tags, m.text);
    byType[type].push(m);
  }
  const opinions = byType.opinion;
  const avgConfidence = opinions.length
    ? Number((opinions.reduce((s, m) => s + clamp01(m.confidence, 0.7), 0) / opinions.length).toFixed(3))
    : null;
  const flat = pool
    .slice()
    .sort((a, b) => String(b.validFrom || b.ts).localeCompare(String(a.validFrom || a.ts)))
    .slice(0, Math.max(1, limit));
  return {
    name: String(name),
    total: pool.length,
    counts: {
      world: byType.world.length,
      experience: byType.experience.length,
      opinion: byType.opinion.length,
      observation: byType.observation.length,
    },
    avgConfidence,
    items: flat.map((m) => ({
      id: m.id,
      type: normalizeType(m.type, m.tags, m.text),
      text: m.text,
      ...(m.confidence === undefined ? {} : { confidence: Number(m.confidence) }),
      date: m.validFrom || dateOnly(m.ts),
    })),
  };
}
