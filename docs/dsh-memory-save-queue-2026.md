# memory_save 精简 + 保存队列（指路）

**正文不在本仓**：`<工作区>/docs/dsh-memory-save-queue-2026.md`（工作区 `fatfish/docs/`，2026-11-13 方案稿，300 行）。插件代码注释里写的 `docs/dsh-memory-save-queue-2026.md §x.y` 指的就是那份。

本文件只当路标，避免插件仓里出现「引用了不存在的文档」。要点与落点对照：

- §0 一页结论 / §1 A 只读核实：`memory_save` 删 `entities` 参数前后的事实基线（含 §1.3 的多天回溯调用次数）。
- §2.4 实体队列：落到 `lib/entity-queue.js`（`claimBatch` / `reapExpired` / `failOne`）、`lib/entity-worker.js`（10 分钟 tick 消费端）、`lib/index.js` 的 `entityQueue` 配置组。
- §2.4.1 写入时零成本粗实体：落到 `lib/entity-coarse.js`（事务内落 `origin='rule'`）。
- §2.4.2 EntityAgent 契约：落到 `lib/agents/entity.js`（抽实体 + 判关系两类调用）。
- §2.4.3 实体准入四道闸：落到 `lib/entity-gate.js`（含 `entityQueue.genericDf` 泛词闸）。

表结构在 `scripts/apply-entity-schema.mjs` / `scripts/apply-curate-schema.mjs`；队列当前状态用 `SELECT state, count(*) FROM curate_queue GROUP BY state` 看。
