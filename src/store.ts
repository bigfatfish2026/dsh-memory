/**
 * 记忆存储层入口：正文在 memory.db 的 memories 表，向量在 vec_items / vec_map（sqlite-vec），
 * 远端 BGE-M3 做嵌入。2026-11-05 起 jsonl 退出运行时：读只读 memories 表，写只走 SQL。
 *
 * 借鉴 Hindsight（arXiv:2512.12818）的四网络划分，给每条记忆加结构化信封：
 *   type       world（外部客观事实）/ experience（我自己做过的事）/ opinion（带置信度的判断）/ observation（由事实派生的实体观察）
 *   confidence 0～1，只有 opinion 真的有意义
 *   entities   涉及的人/设备/项目，用于按实体聚合
 *   validFrom / validTo  这条事实成立的时间区间（不是录入时间；ts 才是录入时间）
 *   supersedes 被这条取代的旧条目 id
 *
 * 检索也照它的做法：语义、关键词、实体、时间四路各自排序，再用 RRF(k) 融合，
 * 而不是只靠一条余弦；opinion 的置信度按 reinforce / weaken / contradict 更新。
 *
 * 2026-11-15：实现按职责拆到 src/store/ 下。
 * 2026-11-15：状态收进 MemoryStore 类（src/store/memory-store.ts），
 * 本文件只保留原来的两个工厂导出，签名/返回对象一个字不变：
 *   state.js / schema.js / log.js / types.js / rows.js / gate.js / entities.js /
 *   records.js / search.js / snapshot.js / tools.js 仍是实现函数，state 由 MemoryStore 私有持有。
 */
import { MemoryStore } from './store/memory-store.js';
import { registerStoreTools as registerTools } from './store/tools.js';
import type { StoreConfig } from './store/types.js';
import type { ToolContextLike } from './store/tools.js';

export { MemoryStore };

/** 对外 API：方法名、参数、返回结构跟拆模块前逐字一致，测试与其它插件直接吃这层。 */
export function createStore(cfg: StoreConfig): MemoryStore {
  return new MemoryStore(cfg);
}

/** 注册 memory_save / memory_recall / memory_list / memory_archive / memory_merge / memory_reflect / memory_entity */
export function registerStoreTools(ctx: ToolContextLike, store: MemoryStore, cfg: StoreConfig): void {
  return registerTools(ctx, store, cfg);
}
