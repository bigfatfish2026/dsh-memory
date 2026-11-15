/**
 * 同一实体下跟这条新记忆最像的旧条目（纯本地算，不花模型）。
 *
 * 2026-11-05 从 sleep.js 抽出来公用：睡眠流程的入口冲突检查和新去重闸
 * 都走这一份，避免两处各算一套 2-gram。行为与原实现保持一致。
 */
import type { MemoryRecord } from './store/types.js';

/** 相关条目打分结果：条目 + 相似度。 */
export interface RelatedScored {
  m: MemoryRecord;
  score: number;
}

export function relatedEntriesScored(text: unknown, entities: string[], metas: MemoryRecord[], limit = 8): RelatedScored[] {
  const set = new Set(entities.map((e) => String(e).trim().toLowerCase()).filter(Boolean));
  if (!set.size) return [];
  const grams = (src: unknown): Set<string> => {
    const w = String(src || '').replace(/[^\p{Script=Han}a-zA-Z0-9]+/gu, '');
    const out = new Set<string>();
    for (let i = 0; i < w.length - 1; i += 1) out.add(w.slice(i, i + 2));
    return out;
  };
  const mine = grams(text);
  return metas
    .filter((m) => (m.entities || []).some((e) => set.has(String(e).toLowerCase())))
    .map((m) => {
      const other = grams(m.text);
      let hit = 0;
      for (const g of mine) if (other.has(g)) hit += 1;
      return { m, score: mine.size ? hit / mine.size : 0 };
    })
    .filter((x) => x.score > 0.05)
    .sort((a, b) => b.score - a.score || String(b.m.ts || '').localeCompare(String(a.m.ts || '')))
    .slice(0, limit);
}

/** 原 sleep.js 的返回形状：只给条目，不带分数。 */
export function relatedEntries(text: unknown, entities: string[], metas: MemoryRecord[], limit = 8): MemoryRecord[] {
  return relatedEntriesScored(text, entities, metas, limit).map((x) => x.m);
}
