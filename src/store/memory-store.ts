/**
 * MemoryStore：记忆存储层的唯一状态持有者（2026-11-15，重构第四步）。
 *
 * 拆分前是 createStore 里的一大段闭包：db、metas、snapshotVersion、rowSeq 映射、向量句柄、
 * 粗实体匹配器、llm 句柄、分词缓存全关在里面。拆模块后这些挂在一个 state 对象上按引用共享；
 * 本类只做一件事：把那份 state 收成私有字段，谁也不再从外面拿 state。
 *
 * 边界（故意不包成方法的）：
 *   - src/store/types.ts / rows.ts / search.ts 里的纯函数（类型归一、行映射、分词、日期解析）
 *     不搬进来：它们不持有状态，传参就够；
 *   - 日志（src/store/log.ts）保持函数：写文件是副作用，不是对象状态。
 *
 * 对外 API 与拆分前逐字一致：方法名、参数、返回结构都不变（src/store.ts 只是转发）。
 */
import {
  archiveMemory, listAllItems, mergeMemories, reflect, saveMemory, updateMemory,
} from './records.js';
import { recall } from './search.js';
import { entityView } from './entities.js';
import { createSnapshot, vecIndexOf } from './snapshot.js';
import { createStoreState } from './state.js';
import { ensureSchema, logVectorGaps } from './schema.js';
import { loadMemories, isActive } from './rows.js';
import { logStore } from './log.js';
import { normalizeType } from './types.js';
import { openMemVectors } from '../memvec.js';
import { createCoarseMatcher } from '../entity-coarse.js';
import { hasCurateTables } from '../entity-queue.js';
import type { DatabaseSync } from 'node:sqlite';
import type {
  CoarseMatcherLike, EntityView, MemVectorsLike, MemoryPatch, MemoryRecord, MemoryView, RecordMeta,
  SavedRecord, SnapshotSource, SnapshotView, StoreConfig, StoreLlm, StoreState,
} from './types.js';
import type { DiscardedRecord } from './types.js';
import type { HitView, RecallOptions } from './search.js';

/** 粗实体统计（entity-coarse 的 stats 产物）。 */
type CoarseStats = ReturnType<CoarseMatcherLike['stats']>;

export class MemoryStore {
  /** 共享 state 只在这里存一份；子模块拿到的是显式参数，不另存引用。 */
  #state: StoreState;

  #snapshot: SnapshotSource;

  constructor(cfg: StoreConfig) {
    this.#state = createStoreState(cfg);
    this.#initStorage();
    this.#snapshot = createSnapshot(this.#state);
  }

  /** 打开库、向量、粗实体匹配器并加载内存记录；顺序固定：建表 → 向量 → 匹配器 → load。 */
  #initStorage(): void {
    const state = this.#state;
    const { cfg } = state;
    ensureSchema(state);
    // 向量在同一连接上，save/update 才能把正文和向量放在一个事务里
    state.vecDb = openMemVectors({
      dbPath: state.dbPath, dim: 0, logger: cfg.logger ?? console, connection: state.connect(),
    } as Parameters<typeof openMemVectors>[0]) as MemVectorsLike;
    state.dim = state.vecDb!.dim || 0;

    /**
     * 写入时的粗实体匹配器（2026-11-13，src/entity-coarse.ts）。
     * 词典懒建、常驻内存：单条正文只做内存字符串扫描，不查全表。
     * 任何异常都退回「没有实体」，绝不因为实体匹配挡住写记忆。
     */
    state.coarseMatcher = createCoarseMatcher({
      db: () => state.connect(),
      genericDf: Number(cfg.entityQueue?.genericDf ?? 0.05),
      genericDfMin: Number(cfg.entityQueue?.genericDfMin ?? 20),
      maxWords: Number(cfg.entityQueue?.coarseMaxWords ?? 4000),
      budgetMs: Number(cfg.entityQueue?.coarseBudgetMs ?? 3),
      log: (line: string) => logStore(cfg, 'INFO', '粗实体', line),
    });

    try {
      state.metas = loadMemories(state);
      const gaps = logVectorGaps(state);
      const gapText = gaps && (gaps.orphans || gaps.missing)
        ? `，向量缺口：孤儿 ${gaps.orphans} / 缺向量 ${gaps.missing}`
        : '';
      logStore(cfg, 'INFO', 'store 已从 memories 表加载', `${state.metas.length} 条，最大 rowSeq ${state.maxRowSeq}${gapText}`);
    } catch (error) {
      logStore(cfg, 'ERROR', 'load 失败', String((error && (error as { message?: unknown }).message) || error));
      throw error;
    }
  }

  get dir(): string {
    return this.#state.dir;
  }

  /** 给 WriteGate 用：正文连接与向量连接是同一条，别另开库。 */
  db(): DatabaseSync {
    return this.#state.connect();
  }

  get vectors(): MemVectorsLike | null {
    return this.#state.vecDb;
  }

  /**
   * 注入模型客户端。传对象 = 测试/CLI 的固定假 llm；传函数 = 插件运行时每次现取
   * （` => llmOf(ctx)`，见 AGENTS「长任务：不持 setup ctx」）——setup 期绝不存 ctx.llm 的值。
   */
  attachLlm(llm: StoreLlm | (() => StoreLlm | null) | null | undefined): boolean {
    return this.#state.attachLlm(llm);
  }

  /** 测试/外部 CLI 查开关当前状态用，只读。 */
  writeGateEnabled(): boolean {
    return Boolean(this.#state.cfg.pipeline?.writeGate);
  }

  metas(): MemoryRecord[] {
    return this.#state.metas;
  }

  all(): MemoryRecord[] {
    return this.#state.metas;
  }

  isActive(m: MemoryRecord): boolean {
    return isActive(m);
  }

  count(): number {
    return this.#state.metas.filter(isActive).length;
  }

  listTexts(): string[] {
    return this.#state.metas.filter(isActive).map((m) => m.text);
  }

  typeOf(m: MemoryRecord): string {
    return normalizeType(m.type, m.tags, m.text);
  }

  /**
   * 聚类用：活跃条目 id -> 向量视图（subarray，只读；别在外面改它）。
   * 2026-10-29 睡觉改成「先聚类再分批喂模型」时加的。
   */
  checkpoint(): void {
    this.#state.vecDb!.checkpoint();
    logVectorGaps(this.#state);
  }

  /** sleep.js 转档用：透传 memvec 的在线备份，别在外面裸拷 WAL 活库 */
  async backupTo(dstPath: string): Promise<void> {
    const { cfg } = this.#state;
    try {
      await this.#state.vecDb!.backupTo(dstPath);
      logStore(cfg, 'INFO', 'memory.db 备份完成', dstPath);
    } catch (error) {
      logStore(cfg, 'ERROR', 'memory.db 备份失败', `${dstPath}：${String((error && (error as { message?: unknown }).message) || error)}`);
      throw error;
    }
  }

  vectorIndex(): Map<string, Float32Array> {
    return vecIndexOf(this.#state);
  }

  listAll(): MemoryView[] {
    return listAllItems(this.#state);
  }

  /**
   * 一轮一次的活跃快照：{ list, byId, vecOf, version }。
   * 写库（writeTx 提交）后版本变，下一次取自动重读，不会拿旧视图判「成员还在不在」。
   */
  snapshot(): SnapshotView {
    return this.#snapshot.get();
  }

  snapshotVersion(): number {
    return this.#state.snapshotVersion;
  }

  /** compact 通道量闸成本用：{calls,llm}，调用方取前后差值。 */
  gateStats(): { calls: number; llm: number } {
    return { calls: this.#state.gateCalls, llm: this.#state.gateLlm };
  }

  /** 检索缓存计数（测试/排障用）：tokenBuilds 分词次数，dfBuilds 全表 df 重建次数。 */
  recallCacheStats(): { tokenBuilds: number; dfBuilds: number } {
    return { tokenBuilds: this.#state.tokenBuilds, dfBuilds: this.#state.dfBuilds };
  }

  /** 等异步入队落地（测试、关机收尾用；写路径不 await）。 */
  flushEntityQueue(): Promise<void[]> {
    return this.#state.flushEntityQueue();
  }

  /** 粗实体匹配器只读统计：词表大小、平均耗时、超预算次数。 */
  coarseStats(): CoarseStats {
    return this.#state.coarseMatcher!.stats();
  }

  /** curate_queue / curate_run 建了没。 */
  hasCurateTables(): boolean {
    return hasCurateTables(this.#state.connect());
  }

  save(text: unknown, tags: unknown, source: string | undefined, meta?: RecordMeta): Promise<SavedRecord | DiscardedRecord> {
    return saveMemory(this.#state, text, tags, source, meta);
  }

  update(id: string, patch?: MemoryPatch): Promise<SavedRecord | DiscardedRecord> {
    return updateMemory(this.#state, id, patch || {});
  }

  archive(id: string, reason?: string): Promise<SavedRecord | DiscardedRecord> {
    return archiveMemory(this.#state, id, reason);
  }

  merge(ids: unknown, text: unknown, tags: unknown, meta?: RecordMeta): Promise<SavedRecord | DiscardedRecord> {
    return mergeMemories(this.#state, ids, text, tags, meta || {});
  }

  reflect(id: string, signal: unknown): Promise<SavedRecord | DiscardedRecord> {
    return reflect(this.#state, id, signal);
  }

  entityView(name: unknown, limit?: number): EntityView {
    return entityView(this.#state, name, limit);
  }

  recall(query: unknown, k: unknown, opts?: RecallOptions): Promise<HitView[]> {
    return recall(this.#state, query, k, opts);
  }
}
