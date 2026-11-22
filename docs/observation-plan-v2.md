# observation 生成方案 v2

2026-11-21。方案已审核定稿；本票只落第一块地基：`observe_task` 表 + 队列 API + 迁移脚本 + 单测，
不改任何现有写路径。写侧接线、worker、提示词、复核流程在后续票。

现状：`src/sleep/prompts.ts:28` 写着「observation 不用你写，系统会按实体自己聚合」，
实际没有生成路径。库里现有 3 条 observation 是历史遗留，**本方案不动它们**：
不重写、不归档、不重算，也不拿它们当生成结果；以后要迁移另开票。

## 1 目标

- observation 有真正的生成路径：夜间整理按实体聚合 active 证据，生成/刷新一条 observation。
- 每条 observation 可追溯：`memory_link.rel='derived_from'` 指向每一条证据。
- 证据变了能重算；模型在跑的时候证据被撤销/改写，旧结果作废，任务留到下一轮。
- 输入没变不重复调模型：同实体同证据版本只有一次有效生成。

## 2 数据模型

### 2.1 observe_task

表由 `scripts/apply-observe-schema.mjs` 手建（幂等，不挂启动）：

| 列 | 类型 | 说明 |
| --- | --- | --- |
| `entity_key` | TEXT PK | 规范化实体键（口径同 `src/shared/entity-normalize.ts`） |
| `entity_id` | TEXT | `entities.id`；还没解析到实体行时为空串 |
| `status` | TEXT | `pending` / `leased` / `done` |
| `version` | INTEGER | 每次 enqueue（新证据/证据变更）+1，租约过期被重领也 +1，防旧结果覆盖 |
| `lease_owner` | TEXT | 领取者（host:pid 或 worker id） |
| `lease_until` | INTEGER | 租约到期 epoch ms；`< now` 即可被重领 |
| `enqueued_ts` | INTEGER | 第一次登记时间，用于排队顺序；重复 enqueue 不改 |
| `updated_ts` | INTEGER | 最近一次状态/版本变更时间 |
| `tries` | INTEGER | 失败次数，`failObserveTask` +1 |
| `last_error` | TEXT | 最后一次失败原因（前 500 字） |
| `reason` | TEXT | 最近一次为什么排队（save/archive/merge/split/text_update/unmount…） |

索引：`idx_observe_task_pick(status, enqueued_ts)`，领活只走这一条。

队列 API 在 `src/store/observe-queue.ts`：

```ts
enqueueObserveTask(db, entityKey, { entityId?, reason?, now?, log? }) -> { entityKey, version, created } | null
claimObserveTasks(db, { limit?, owner?, leaseMs?, now? }) -> [{ entityKey, entityId, version, reason, tries }]
completeObserveTask(db, entityKey, version, { now?, owner? }) -> boolean
failObserveTask(db, entityKey, version, error, { now?, owner? }) -> boolean
```

四个函数都只吃现有 `DatabaseSync` 句柄，内部不开事务，外层写事务里可直接调用。

### 2.2 observation 记忆行

- `type='observation'`，正文是按实体聚合后的观察，`entities` 至少含该实体键。
- 初态 `status='pending_review'`：生成完先不进入正常召回，等复核（人工或后续自动规则）通过才置
  `status='active'`。`memories.status` 的 active 口径沿用现有代码（新写入统一落字面量 `active`，
  空串/NULL 的历史行按现有读侧兜底）。
- `extra` 记生成元数据：`{ evidence_version, source_roots: [...], window_tokens, model, generated_at }`。
  这些只是排障/复核用，不参与召回打分。
- 同一实体同一时刻最多一条 active observation；重算时旧条归档（`status='archived'`，`reason` 写
  「被新 observation 取代」），不做原地改正文，保证证据边和正文对得上。

### 2.3 证据边

复用现有 `memory_link` 表，`rel='derived_from'`：

- `src` = observation 的记忆 id，`dst` = 证据记忆 id，`origin='observe'`。
- 现有唯一索引 `uq_memory_link_edge(src, dst, rel)` 天然去重，同一对边只留一份。
- 每轮重算在同一个写事务里先删 `src` 的旧 `derived_from` 边、再写新边；
  正文与边同成同败，不出现「新正文旧证据」。

### 2.4 来源根

证据按 `memories.source` 归组，一个 source 算一个「来源根」：

- 同一原条拆出来的多个分片（`sleep:split:<原条id>`）、同一批 merge 产物，共享来源根。
- 版本摘要 = 该来源根下所有 active 证据的 `(id, text hash)` 排序后的哈希；
  摘要不变就算「证据没变」，不重新排队。
- 这样同原条拆片不会因为行数变了就各算一条新证据（见 §7 场景 2）。

## 3 状态机

```
pending ──claim──> leased ──complete(version/owner 一致)──> done
   ^                  │
   │                  ├──fail(version/owner 一致)──> pending（tries+1、last_error）
   │                  └──租约过期（lease_until < now）──> 可被 claim 重领（version+1）
   └──enqueue（任何状态）──> pending（version+1、清租约、enqueued_ts 不动）
```

- `claim` 只选 `pending` 或租约已过期的 `leased`，按 `enqueued_ts` 升序，最多 `limit` 条；
  `UPDATE` 带 `version` 与状态条件，改到的行才返回。
- 过期重领同样让 `version+1`（pending 首次领取不涨）：旧 owner 手里那一版立即失效，
  它的迟到 `complete` / `fail` 版本条件不再命中，既不能把任务置 `done`，也不能清掉新租约。
- `complete` / `fail` 都带回 claim 时的 `version`；传了 `owner` 时再校验 `lease_owner`（双保险，
  不传则只校验 version，旧调用行为不变）。任一不符返回 `false` 且一行不改——
  模型跑期间证据变了或租约已被别人重领，这份结果作废，新任务留在 `pending` 下一轮再跑。
- `tries` 只由 `fail` 累加；租约过期重领不加。上限由 worker 决定（建议 3 次交人工），
  队列层只记数，不自动清零。

## 4 复核触发

以下写路径在同一事务里调用 `enqueueObserveTask`（本票只备好 API，调用点在后续票接）：

| 触发 | reason | 说明 |
| --- | --- | --- |
| 证据归档 | `archive` | 被 observation 引用的行失效 |
| 合并 | `merge` | 源条并进目标条，来源根变了 |
| 拆分 | `split` | 原条归档、分片新增；按来源根摘要判断是否真变 |
| 正文改写 | `text_update` | 证据正文/entities 变了 |
| 解除挂载 | `unmount` | 实体与该行关系消失，证据集合变了 |

- 挂载晚到也算：先有证据、后补 entity 挂载时，挂载点同样 enqueue（场景 6）。
- 同一实体一次事务里被多次触发只涨 `version`、不产生多行；`enqueued_ts` 保持首次登记时间。
- 正在跑的任务被 enqueue 会拉回 `pending`、清租约；旧 claim 的结果落不进去。

## 5 召回过滤

`pending_review` 的 observation 不能进默认召回，也不进 `memory_entity` 的聚合：

- 召回/快照：默认只取 active，`pending_review` 显式排除；复核工具可显式按状态查。
  实现点：`src/store/rows.ts` 的 active 判定、`src/store/search.ts` 的候选过滤。
- `memory_entity`：计数与列表排除 `pending_review`。实现点：`src/entity-worker.ts` 的挂载视图查询
  与 `src/entity-graph.ts` 的计数 SQL。
- 缓存失效：observation 状态迁移（pending_review → active / archived）后必须 bump 快照版本，
  让召回缓存与 `memory_entity` 缓存重算，不能把旧状态继续端出去。
- 本票不动这些读路径；落地过滤的票必须同时补测试（缓存失效单列一条）。

## 6 dryRun 语义

- **照跑模型**：真的组装证据、真的调模型，用来和正式轮对账 prompt/输出。
- **不消费任务**：不 claim；或 claim 后在回滚里丢掉。`observe_task` 的 status/version/tries/lease
  一个字节不改。
- **不写副作用**：不写 `memories`、不写 `memory_link`、不动缓存。
- 日志单独打 `[observe][dry-run]` 行，带 `entity_key`、`version`、`calls`、`tokens`、`ms`，
  方便和正式轮逐项比。
- dryRun 耗的调用数/token 照常计入 §8 的单独计量，不混进正式预算。

## 7 验收场景

1. **两条独立证据生成**：同一实体有来自两个 source 的 active 证据 → 生成 1 条 observation，
   `memory_link` 里 2 条 `derived_from` 边分别指向两条证据。
2. **同原条拆片不触发**：一条记忆拆成 3 片、内容未变 → 来源根摘要相同，不重新排队；
   即使被排队，生成结果与上一轮一致，零新增 observation、零模型调用。
3. **输入不变重跑零新增零调用**：连续两轮 tick，证据/挂载都没变 → 第二轮不 claim 该实体，
   模型调用 0 次，新行 0 条。
4. **模型跑时证据被撤销**：claim 后证据被归档 → 写侧 enqueue 使 `version+1`；
   模型返回后 `completeObserveTask` 返回 `false`，行保持 `pending`，下一轮按新证据重跑。
5. **>20 实体尾项续跑**：25 个 pending → 本轮只领 20（按 `enqueued_ts`），剩 5 个仍在 `pending`，
   下一轮继续，不丢任务。
6. **挂载晚到能触发**：证据先入库、实体后挂载 → 挂载点 enqueue，下一轮生成，不缺证据。
7. **旧证据超出输入窗口仍能复核**：证据总长超过模型窗口时按来源根摘要裁剪进 prompt，
   `derived_from` 仍指向原始行 id；复核能列出全部证据 id 和版本摘要，不因裁剪丢追溯。

## 8 灰度预算

- 每轮实体上限 20（`OBSERVE_BATCH`）；`claimObserveTasks` 的 `limit` 默认就是它。
- 调用数 / token / 耗时**单独计量**：日志固定字段 `calls=`、`tokens=`、`ms=`，
  不与 sleep / entity_extract / split 的预算合并，也不互相挤占。
- 灰度顺序：dryRun 一轮对账（比对 prompt 与候选结果）→ 小批 5 个实体 → 放到 20；
  每档看「新增 observation 数 / 复核驳回率 / 调用与耗时」三项，异常就关开关停止 claim。

## 9 本票范围

已交付：

- `scripts/apply-observe-schema.mjs`：幂等建 `observe_task` + `idx_observe_task_pick`，
  导出 `OBSERVE_DDL` / `applyObserveSchema`，支持 `--db` / `--dry-run`。
- `src/store/observe-queue.ts`：四个队列函数，事务安全、版本校验。
- `test/observe-queue.test.mjs`：去重版本、领取排序限额、租约过期、版本作废、失败回 pending、
  enqueue 打断在跑任务、缺表兜底、外层事务。
- 本文档。

未做（后续票）：写侧 enqueue 调用点、worker 与模型提示词、复核流程、召回/`memory_entity` 过滤、
缓存 bump、现有 3 条 observation 的任何处理。
