# dsh-memory 实体层重做设计稿（指路）

**正文不在本仓**：`<工作区>/docs/dsh-memory-entity-rework-2026.md`（工作区 `fatfish/docs/`，2026-11-13 第四版，2150 行）。插件 `lib/`、`scripts/` 注释里的 `docs/dsh-memory-entity-rework-2026.md §x.y` 指的就是那份。

本文件只当路标。代码引用到的章节与现存落点：

- §3.1 / §3.2 / §3.5 数据模型 DDL：`scripts/apply-entity-schema.mjs`（`entities` / `memory_entity` / `entity_issue` / `entity_excluded`）。
- §3.6 整理队列表：`scripts/apply-curate-schema.mjs`（`curate_queue` / `curate_run` / `memory_link`）。
- §4.2 / §5.1 / §5.2 / §5.3 归一化与分解：`scripts/entity-normalize.mjs`（`normalizeEntity` / `splitVersion`），实体整理侧由 `lib/entity-tidy.js` 引用。
- §12.1 / §12.2 / §12.4 激进清洗规则：`scripts/exclude-entities.mjs`（D 类排除）、`scripts/curate-audit.mjs`（审计报告；该脚本里写的是工作区绝对路径）。

与 2026-11-14 的 surface 两层稿冲突时以 surface 稿为准（见 `docs/dsh-memory-entity-dedup-2026-11-14.md` 的「与 rework 稿的冲突」一节）。当前实现只做到「实体挂载 + 写法层 + 每晚整理」，稿里 §7~§10 的取代链骨架、时间线折叠渲染尚未接线。
