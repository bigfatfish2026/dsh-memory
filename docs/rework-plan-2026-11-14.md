# dsh-memory 结构改造大纲（2026-11-14）

依据：结构审计（票）。本文只出设计与顺序，不改运行时代码。

## 1 现状诊断

### 1.1 导致慢/导致错（先修）

- 三级漏斗一条没做：`planPoolQueues` 只做「图聚类 → 整批最大余弦 ≥ `clusterQuestionMin`(0.78) → 按 token 切子批」（`lib/sleep.js:3183-3230`），子批用 `batchMaxCos` 冒充批内相似度（`sleep.js:123`），批内两两最小余弦、实体/标签/时间相容、字面指纹三层全空。候选生成还有四份实现：`cluster.js:63`、`sleep.js:1661`、`similar-graph.js:131`、`split-pass.js:237-242`。
- 队列裂两套账本：`curate_queue` 只装 `entity_extract`（`entity-queue.js:15-16`，state `ready/leased/done/skipped/dead`，attempts 领取时 +1，`MAX_ATTEMPTS=3`）；`pending_ops` 装 `ingest_file/merge_batch/merge_op/dedupe_check/split_item`（`ingest-index.js:45-49`，status `pending/running/done/dead`，attempts 失败时 +1，`MAX_ATTEMPTS=5`）。claim 两套（`entity-queue.js:109` vs `ingest-index.js:104`），收尾两套（`markDone/markSkipped/failOne` vs `markOp`），阈值两套，日志两套。
- compact 通道是空壳：`KINDS_OF_CHANNEL.compact=[]`（`ingest-index.js:48`），`channelOfKind` 永不返回 compact（`:51-57`），`channelStats('compact')` 直接早退 `total:0`（`:145`）；但 `pooledSleepPass` 仍拿它算 `hasWork`（`sleep.js:3376-3378`）；`runCompactChannel` 是手写 await、不是池（`sleep.js:3139-3157`），注释说含「organizeCache 轮转」（`3137`）而函数体没有。
- legacy 约 1500 行并存：`legacySleepPass`(`sleep.js:3594-3739`) + `organizePass`(`1706`) + `organizePassAgent`(`2443-2775`) + `ingestByTranscript`(`2777`) + `ingestByFilePass`(`2921-3128`) + `applyActions`(`1173-1385`) 只有 legacy 入口可达。默认值还两处不一致：`index.js:295` schema default `'pool'`，`sleep.js:3132` 缺键按 legacy。
- AGENTS.md 四处违规集中在 entity_extract 链：实体事务一个 try 包整批、一条脏数据整批回滚（`entity-worker.js:192-251`，收尾事务 `:293-319` 同）；`items=[]` 静默继续、不记 attempts（`:175`），links 空返回同样（`:283`）；只有批级日志（`:179/:249/:317`），没有 `[entity-queue] done id=… attempts=…` 逐条行；setup 期存 `ctx.llm`（`index.js:418` `store.attachLlm(ctx.llm)`，持有方 `store.js:1362-1365`），重载后是 inactive context。

### 1.2 只是难看（但拖慢后续改造）

- 26 个超百行函数，代表：`setupSleep`(`sleep.js:768` 起)、`organizePassAgent`(`2443-2775`)、`consumeEntityBatch`(`137-333`)、`applyActions`(`1173-1385`)、`ingestByFilePass`(`2921-3128`)。
- 常量散落（grep 实测）：64 六处（`concurrency.js:25/28`、`pool.js:19`、`split-pass.js:256`、`pipeline-split.js:32`、`entity-worker.js:375`、`index.js:281`）；180000 三处（`entity-queue.js:18`、`index.js:68/95`，`index.js:134` 的 `triggerTokens` 同数字不同义）；600000 四处（`index.js:59/92/149/439`）；`BATCH=8` 两处（`entity-queue.js:24`、`index.js:439` 手写 8）；200 十一处（`text-limit.js:20`、`split-pass.js:233`、`sleep.js:2114/2634/2735/3216/3301/3481`、`store.js:1444/1467/1493`）。
- ActiveSnapshot 缺失：一轮内 `listAll().filter(active)` 被重复全表映射（`sleep.js:3184/3242/3298/3533`、`split-pass.js:237`），4020 条 × N 次。

## 2 阶段划分（每阶段可独立上线、独立回滚）

阶段 1（先做，小到一次能审完）：只修 AGENTS 四处，不拆 setupSleep。
文件：`lib/entity-worker.js`、`lib/index.js`、`lib/store.js`。改动量约 80 行。
内容：① 实体事务改逐条隔离（每条一个 savepoint，单条失败只失败这一条；模型整批报错仍整批 failOne）；② `items=[]` 记 attempts+1、`last_error='empty_items'`，写逐条日志（`links=[]` 是模型明确回答「没有关系」，按有效结果处理，不在此列）；③ 落地/失败各打一行 `[entity-queue] done id=<id> attempts=<n> ms=<ms>`；④ 去掉 `store.attachLlm(ctx.llm)` 存值，改成注入 ` => llmOf(ctx)` 的现取 getter。
依赖：无。风险：空返回改记账后 dead 数会上升，上线前先记一次 `queueStats` 基线。回滚：单文件 revert，无配置开关。验收：memory-save-contract 新增 3 条（脏条目只坏自己、空返回记 attempts、逐条日志行存在）。

阶段 2：队列统一接口层（不动表）。新增 `lib/ledger.js`，两表各一个 adapter，五个方法 `claim/reap/done/fail/stats` + kind handler 注册表。文件：`lib/ledger.js`(新)、`entity-queue.js`、`entity-worker.js`、`ingest-index.js`、`pool.js`。依赖阶段 1。回滚：新模块 revert，旧导出签名不变。风险：中，池的 claim/release 改由 adapter 提供，要保 `pool.test` 全绿。

阶段 3：L0-L3 漏斗 + BatchPlanner。新增 `lib/batch-planner.js`，`planPoolQueues` 改为调它。依赖阶段 2（handler 表按批产出）。风险：中高（改变送模型的批集合），回滚用配置 `organizeFunnelMode=off`（新增，默认 off，先在 dry-run 对照）。

阶段 4：抽象落地（SleepState/CooldownState/fingerprint.js/ActiveSnapshot）。纯搬移 + 转发，逐个文件上线。回滚：单文件 revert。

阶段 5：compact 通道真正入池：把作废清扫、本地重放、cache 轮转注册成派生 handler，`channelStats` 能报数，`hasWork` 不再读一个恒 0 的通道。回滚：配置 `organizePipelineMode=legacy`。

阶段 6：legacy 退场（见 §6）。

## 3 漏斗落地设计（并入 BatchPlanner）

- L0 候选生成（已有，保留）：向量近邻图连通分量 + 实体桶兜底，单点丢。4020 active → 约 285 组（审计口径：2988 条时 285 个多条目组）。
- L1 批内向量闸（阶段 3）：要求子批内至少一对 cos ≥ `clusterThreshold`(0.79)，且批内两两最小 cos ≥ 0.55；不再用整批最大值代替。285 → 约 200（-25%~-30%），砍掉大簇切片后内部不相干的子批。
- L2 事实相容闸（阶段 3，0 次模型）：实体交集为空 + 标签交集为空 + 正文无共享数字/路径/专名 → 不送 merge，记 cache `v=skip`，不耗 attempts。200 → 约 150。
- L3 字面指纹（阶段 3，0 次模型）：normalize（去空白标点、全半角）后 hash 相同或 Jaccard ≥0.8 → 直接判重复写 `discards`；0.55~0.8 只调 dedupe 不调 merge。150 → 约 110。
- 效果：merge 段模型调用 285 → 约 110（降 55%~60%，保守报 45%~60%）；端到端保守估计降 25%~40%，因为聚类（约 25s）与串行落地不变。**不承诺设计稿的 12 分钟**：先由阶段 0 用同一段链路测出「调用次数 + 各段耗时」基线，再按实测比例更新。
- 三处重复切批一起解决：`splitAgentBatches`(`sleep.js:110`)、`runSplitPass` 的 chunk 循环(`split-pass.js:264`)、`pipeline-split` 的 claim 批量(`pipeline-split.js:140`) 统一调 `planner.batching()`。

## 4 队列统一设计

结论：**保持两表，统一消费接口**，不物理合表。理由：合表要改两套 schema 脚本、五个消费方、二十多个测试，收益只是一个 adapter 的差别；而接口统一就能拿到一致语义与可观测性。

- Ledger 五方法：`claim(channel, limit, {exclude, leaseMs})`、`reap(channel)`、`done(id)`、`fail(id,{attempts,error})`、`stats(channel)`；`kindHandlers: Map<kind,{channel,prepare,commit,release}>`。PendingOpsLedger 保 `status/next_try_at` 语义；CurateQueueLedger 保 `priority/lease_owner` 语义，对外都映射成 ready/leased/dead。
- channel 语义：`ingest`（文件 ack，重、读多写少）、`curate`（merge_batch/merge_op/dedupe_check/split_item，问模型）、`entity`（entity_extract，单独 10 分钟 tick 池）、`compact`（**派生通道**：每轮现算的规则活，不占队列表；这样 `channelStats` 不再假装有 pending 行）。
- 生产端从「每轮扫表」改「事件入队」：① 写记忆的 writeTx 内同步 INSERT `curate_queue`（幂等键 memory_id）；② 事务提交成功后才 notify 池（进程内信号，不落盘）；③ 启动补账：memories 左连接 curate_queue，补最近 N 分钟缺失行；④ 删掉 `store.js:507-528` 的 setImmediate 路径。禁止在事务内发通知或调模型——现在的坑正是 setImmediate 在事务外：写库失败也会入队（幻影抽取），反过来先入队后写库失败会留空条目。迁移按 双写比对 → 补账扫描 → 删 setImmediate → 池通知化 四步，每步可回滚。

## 5 抽象落地

- BatchPlanner（`lib/batch-planner.js`）：持有 cfg、ActiveSnapshot、vecOf、organizeCache；方法 `plan(round)`、`batching(items,budget)`、`funnelStats()`；`pooledSleepPass` 每轮创建一个，用完即弃；迁移期与 legacy `organizePass` 的旧函数并存，靠 `organizePipelineMode` 分叉。
- SleepState（`lib/sleep-state.js`）：持有 stateFile、内存 state、owner、slots、organizeCache、mergeBatchCache；方法 `load/save(patch)/heartbeat/takeOver(owner)/markSlot(key)/cacheGet/cacheSet`；`setupSleep` 创建，`readState/writeState` 保留导出给测试。
- CooldownState（`lib/cooldown-state.js`）：冷却已删（`consolidate.js:515`），收的是节流与按会话 token 基线：`lastAttemptAt/lastSuccessAt/lastTokens{}/lastTokensAt/pendingTokens/estimateTriggerAt/estimateDone` + `inFlight`；方法 `normalize/shouldTrigger/begin/finish/persist`；`setupConsolidate` 创建。
- fingerprint.js（`lib/fingerprint.js`）：持有 hash 原语与 normalize；方法 `batch(members)`、`splitKey({sourceId,text})`、`dedupeKey(kind,payload)`、`cacheKey`。替换 `sleep.js:74`、`split-queue.js:23`、`merge.js:896`、`ingest-index.js:509` 四处各写。
- ActiveSnapshot：一轮只取一次 active 列表 + byId + vecOf + version，传给 planner 与 handler；替换 §1.2 列的五个调用点。
- kind handler 表（`lib/kind-handlers.js`）：`Map<kind,handler>`，`setupSleep` 注册 merge_batch/merge_op/dedupe_check/split_item/ingest_file，entity-worker 注册 entity_extract；迁移期查不到就回退旧 switch。

## 6 legacy 退场

删除条件四条全满足：① 同输入逐条一致：同一 tmp db + 同一假 llm，pool 与 legacy 各跑一轮，比较 memories 每行 status/text、discards 行、pending_ops 结果集（不是只比 added/merged 计数）；② 真库影子两周：每轮 pool 跑完用 dry-run legacy 跑同批，diff 为空；③ 回滚开关无人用：grep `memory_sleep.log` 近 30 天 `mode=legacy` 为 0、配置 dump 里没有 `organizePipelineMode=legacy`；④ `scripts/` 与 `test/` 没有 import legacy 内部函数。
满足后删 `legacySleepPass/organizePass/organizePassAgent/ingestByTranscript` 及 `applyActions` 的 legacy 分支，删 `index.js` 的 `organizePipelineMode`；打 tag `pre-rework-2026-11` 作回滚点，CHANGELOG 记。

## 7 测试策略

### 7.1 量化现状

- 492 条（审计口径），工具只有 `node --test test/`（`package.json:38`），**没有任何覆盖率数字**。补法：`node --test --experimental-test-coverage test/`（Node 自带，本机 v26 支持），只统计 lib/ 排除 scripts/。
- 已核对的重复覆盖（只换名字/入口，条件相同）：
  1) `queue-channel.test.mjs:165/177` 与 `split-concurrency.test.mjs` 的「并发≤N / 并发1=串行 / 结果条数一致」——都是 `consumeSplitOps` + 假 llm + `splitMaxChars=20`。
  2) `merge-batch-pending.test.mjs:159` 与 `replay-merge-batch-converge.test.mjs:236`——同一 attempts 到 5 → dead 状态机，一个走 parse fail、一个走空 ops。
  3) `write-gate.test.mjs:93` 与 `write-gate-store.test.mjs:117/133`——同一 DUPLICATE/SUPERSEDES/DISTINCT 裁决，函数级与集成级各写一遍。
  待逐条核对候选：`sleep-due-slot` vs `sleep-schedule`、`consolidate-async` vs `consolidate-estimate-throttle`、`memory-save-contract` 的取代边 vs `entity-link-candidates`。

### 7.2 目标与门槛

- 改动/新增的每个分支至少一条测试；断言行为（返回值、库行、日志行、模型调用次数），不断言内部调用顺序；同一分支的多条测试只在输入类别确实不同时保留。
- 门槛：改动文件行覆盖 ≥85%、分支 ≥80%；新增抽象模块（ledger/planner/fingerprint）行 ≥90%、分支 ≥85%；阶段 1 不许拉低全仓行覆盖。
- 明确反对凑数：不接受「只差测试名/只差造数」的第二条；评审时按「这条钉的是哪个分支」写进测试文件头。

### 7.3 池架构分支覆盖

已测（`pool.test.mjs`）：领活粒度(:31)、并发上限(:51)、串行落地+写闸 peak=1(:74)、空队列收摊(:98)、空队列等 notify(:116)、reap 调用次数(:145)、prepare/commit 抛错→release(:162)、硬顶 clamp(:191)、list 池串行节奏(:213)。
漏测：release 自己抛错(`pool.js:161-168`)、onLanded 抛错(177-181)、claim 抛错(197-203)、reap 抛错(189-193)、`stop()` 在批中途(237-241)、`triedThisRound` 使失败行同轮不再被领(100-102/215)、commit 返回 dead 的计数(157)、concurrency 传函数(104-107)、idle 超时分支(116-126)、writeLock 排队不计入 peak(30-46)；entity 队列侧：并发上限、空队列收敛、整批模型失败 attempts+1、单条脏数据隔离（阶段 1 后）、空 items 记账（阶段 1 后）、3 次 dead 后不再被 claim。每条用假 claim/prepare/commit 直接钉，不连真库。
