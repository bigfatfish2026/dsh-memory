# dsh-memory 实体去重设计稿（指路）

**正文不在本仓**：`<工作区>/docs/dsh-memory-entity-dedup-2026-11-14.md`（工作区 `fatfish/docs/`，2026-11-14 surface 两层版，521 行）。`scripts/apply-entity-surface.mjs`、`scripts/migrate-entity-surface.mjs` 注释里的 `docs/dsh-memory-entity-dedup-2026-11-14.md §x.y` 指的就是那份。

本文件只当路标。代码引用到的章节与现存落点：

- §1.3 事实基线：真库实体 / surface 条数，迁移脚本按它核对输入。
- §3.1 DDL：`entity_surface`（写法层，`surface` 主键 → `norm` → `entity_id`）、`entity_merge`（合并撤销凭据）、`entity_dedup_candidate`（候选与 bucket），落在 `scripts/apply-entity-surface.mjs`。
- §3.2 三条不变式：一个 surface 只挂一个实体；`entity_merge` 必须先写后改（撤销才搬得回去）；`mem_count` 一律按 `memory_entity` 实数重算。实现见 `lib/entity-tidy.js` 的 `mergeEntities` / `undoEntityMerge`。
- §11.1 一期清单：`scripts/migrate-entity-surface.mjs` 回填历史写法，`lib/entity-tidy.js` 每轮扫候选。

每晚实体整理的开关是 `entityTidy.apply`（默认 false 只报不合），报告落 `memory/entity-tidy-report.md`，撤回走 `node scripts/entity-tidy.mjs --undo-run <run_id>` 或 `--undo <mergeId>`。
