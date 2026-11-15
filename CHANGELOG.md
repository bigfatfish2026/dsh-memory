# Changelog

本插件的历史改动。日期用绝对日期，一条改动一行说明「改了什么、为什么」。

## 1.0.0（2026-11-15）

全量 TypeScript 重写后的第一个大版本：`src/**/*.ts` 是唯一源码，`lib/` 从手写源码目录变成构建产物，并随包发布 `.d.ts` 声明；运行行为与 0.6.0 逐行对拍，没有变化。v0.6.0 之后到本版发布前有 42 条未打 tag 的提交（`git log --oneline v0.6.0..v1.0.0 | wc -l` = 42），按「历史漏打不补」的规矩不补中间 tag，一并归入本版。全量 `node --test test/` 638 条全绿。

### 重构

- 抽 `lib/shared/`：`json.js` 收 merge/split/dedupe 三份 `parseJsonLoose`（取并集，候选顺序不变）；`fs.js` 收五份 `ensureDir`（收目录 / 收文件两个签名分开）；`tx.js` 收 `withTransaction` / `withSavepoint` / `withNestedTransaction`，store、ingest-index、entity-worker、entity-tidy、memory-link 八处事务样板改成调用它；`read-tools.js` 收 sleep/consolidate 两份 `safePath` / `collectFiles` / `clip` + 两个常量表。
- `tokensOf` 三份语义不同（store 检索打分含单字、prefill 关键词重排去停用、batch-planner 相似度走 NFKC 归一化），**不合并**：合成一份必然改口径，保留各自实现，名字不再互相冒充。
- `scripts/lib/db.mjs`：30 个脚本 43 处 `new DatabaseSync(...)` 与写死的库路径收成 `openDb` / `resolveDbPath` / `DEFAULT_DB_PATH` / `withTx`，路径按 `--db` → `DSH_MEMORY_DB` → 默认真库解析；真库护栏常量仍取 `DEFAULT_DB_PATH`。
- attempts 记账收成 `ledger.recordAttempt`（+1、到阈值判 dead、原因截 500 字）：ingest `failFile`、`merge_batch` 入队、dedupe 撞门三处接入；租约回收与 claim CAS 仍是单条原子 UPDATE，阈值由 ledger 传，不逐行改 JS。
- 测试：619 条全绿（改前 619）；43 files changed，+420/−566（净减 146 行）。这个 619 是「抽共享小件」那一刀的中间态，接在 0.6.0 的 590 后面（0.6.0 之后四个修复提交把用例从 590 推到 593 → 606 → 619），TS 迁移期的 633 再往后接，最终 638（633 + 5 条未绑定标识符回归，见下）。

### 类型与构建

- **全量 TS 迁移（刀 0 ~ 刀 9，2026-11-15 收口）**：95 个源文件从 `lib/*.js` 迁进 `src/**/*.ts`，逐文件镜像回 `lib/`（`unbundle`，不打包）；`@ts-nocheck` 清零，全仓 `npx tsc --noEmit` 0 error。迁移按层分批做，每批都拿 emit 产物与迁移前逐行对拍，运行行为一字未改。
- **`lib/` 转成构建产物目录**：`lib/` 进 `.gitignore`，`npm run build`（tsdown）从 `src` 重建；`scripts/entity-normalize.mjs` 归位成 `src/shared/entity-normalize.ts`，`entity-graph` / `entity-tidy` / `entity-coarse` 三处 import 改指构建产物，仓库里只剩一份实体文件，产物仍是 96 个。
- **声明产物（刀 9）**：`tsdown` 的 `dts` 打开，随包发布 96 个 `.d.ts`；`package.json` 补 `"types": "lib/index.d.ts"`；`Config` 显式标注 `Schema<MemoryConfig>`，消掉推断类型带 pnpm 私有路径的 TS2742。
- README 补「源码与构建」，AGENTS 补「改 src、先 build、别碰 lib」三条硬规矩。

### 修复

- **5 处历史遗留的未绑定标识符**（`@ts-nocheck` 遮住的既有缺陷，命中就抛或被 catch 静默吞）：`sleep/lint.ts` 的 `llmOf` / `ctx`、`sleep/scheduler.ts` 的 `this.#cfg`、`sleep/pool.ts` 缺 import 的 `CLUSTER_PROMPT_SHELL_TOKENS`、`sleep/pool-pass.ts` 的 `sleepOwner`、`sleep/setup.ts` 的 `windowSessionOf`。全是补绑定、无重构，回归用例在 `test/unbound-identifiers.test.mjs`（5 条）。
- **测试**：638 条全绿（633 条原有 + 5 条未绑定标识符回归）。

## 0.6.0（2026-11-15）

这版跨了 2026-11-09 到 11-15：整理链从「一条顺序大函数」改成「队列 + 池」，切批从固定条数改成按模型窗口算 token，补上实体层与每晚实体整理，价档闸接上 dsh-price-clock，调度在重启补判/贵时段上做了降级。全量 `node --test test/` 590 条全绿。

### 新增

- **整理链改「队列 + 池」（2026-11-14，维护者拍板）**
  - `organizePipelineMode=pool|legacy`（schema 默认 `pool`；手写 cfg 缺键按 `legacy` 跑）。pool 模式一轮分四步：规划只入队 → 有活才备份 → 起池 → 收尾轮转与汇总；`legacy` 保留老顺序路径当回滚口子，两条路整轮对拍过（`f196da0`）。
  - 四个通道：`ingest`（`ingest_file`）、`curate`（`merge_batch` / `split_item`）、`compact`（`revocation_sweep` / `merge_op` / `dedupe_check` / `entity_tidy`）、`entity`（`entity_extract`，10 分钟 tick）。kind 归哪个通道、哪个池、attempts 阈值只写在 `lib/kind-handlers.js` 一处。
  - 池语义钉在 `lib/pool.js` 文件头：小批领活（批大小 = 当前并发数，绝不整队标 running）、批内并发问模型、串行落地（三个池共用一把 `createWriteLock`，收尾打 `[pool] 写库闸 并发峰值`）、空了就等、不收敛不算完。`lib/ledger.js` 收状态机（claim → running → done / pending / dead + 租约 reap），`attemptsAt` 由各表适配器决定（curate_queue 领取即计、pending_ops 失败才计）。
  - 并发口径收成一处 `lib/concurrency.js`：cap（`organizeIngestConcurrency`，默认 64、硬顶 64）、负载快照（`/proc/loadavg`、`/proc/meminfo`、核数、温度，一轮只取一次）、本轮实际路数（`organizeIngestAdaptive`，关掉回落到 cap）。手工上限只剩 `organizeConcurrency` 一个口子，且只许往下压。
  - 拆分段同步改造：`lib/pipeline-split.js` 从「一段一条问模型」改成小批领活 + 批内 N 路并发 + 串行落地（单条 19-24 秒的链路原来是整轮最慢的一段）。
  - `lib/active-snapshot.js`（一轮一次活跃视图）、`lib/sleep-state.js`（state 文件唯一入口）、`lib/cooldown-state.js`（整理节流状态）、`lib/fingerprint.js`（五处 sha1 收成一个原语，语义各自保留）四个状态/原语模块落地。

- **切批按 token，不按条数**
  - 新模块 `lib/token-window.js`：窗口取值顺序 `contextWindowTokens` → `tokenMeter.contextPressure` → `session.requestContext()` → 宿主 compaction 的 `modelPolicies` → 未知；中文 1 字 1 token、ASCII 4 字符 1 token 保守估，默认预留窗口 1/4 给 system/输出。
  - 删掉固定预算：`sleepMemoryChars`、`addPassChars`、`clusterMaxSize`、`mergeBatchMax`、`clusterChanges`、`organizeMaxCalls`、`maxChanges`、`archiveMaxChanges`、`sleepSplitLimit`、`conflictCheckMaxCalls`、`cooldownMinutes`，以及五个 `*MaxTokens`。簇不再按条数截，批大小由 token 预算算。
  - 候选漏斗进 `lib/batch-planner.js`：L0 精确指纹去重 / L1 廉价字面相似度 + 成对余弦闸 / L2 结构约束 / L3 按分降序送模型，`organizeFunnelMode` 默认 `off`（与改造前逐条一致）；另有跨天 2 条批合批 `organizeBatchPackSize`（默认 0=关，真库实测跨天批一致率 96.1%）。

- **实体层与每晚实体整理（2026-11-13 ~ 11-15）**
  - `memory_save` 不再收 `entities`：写入时零成本粗实体（`lib/entity-coarse.js`，事务内落 `origin='rule'`），写入后进 `entity_extract` 队列（`lib/entity-queue.js` + `lib/entity-worker.js`，10 分钟一 tick、只挂主会话进程），由模型补 `origin='agent'`。四道准入闸 + 泛词 df 闸在 `lib/entity-gate.js`。
  - 实体/写法分两层：`entities` / `memory_entity` / `entity_surface` / `memory_link` / `entity_issue` / `entity_excluded`，DDL 分在 `scripts/apply-entity-schema.mjs`、`scripts/apply-curate-schema.mjs`、`scripts/apply-entity-surface.mjs`。
  - 三期实体整理 `lib/entity-tidy.js` 进 compact 通道：`fold_same` / `surface_cross` 两类「名字层面确定同义」可自动合（脏名字、路径、版本号一律不进），单复数/版本/前缀/相似只报；每次合并写一行 `entity_merge` 当撤销凭据（`scripts/entity-tidy.mjs --undo-run|--undo`），每轮报告落 `entity-tidy-report.md`。`entityTidy.apply` 默认 false（只报不合，与改前逐字节一致），2026-11-15 加夜间自动合并开关。

- **上下文占压缩线提醒搬进插件**
  - `lib/context-watch.js` 挂已有的 `llm/stream` 钩子，占用量/窗口走 `tokenMeter.contextPressure`，压缩线按 compaction 的 `modelPolicies` 复算；到压缩线 95% 发一条 `memory.context.pressure` 并推 QQ，同会话只发一次，压缩过一次才清记录。`fatfish/ctx-watch.sh` 那套读 projcache 猜 provider 的老路退役。

- **价档闸（2026-11-14 /）**
  - `lib/price-gate.js`：重启补判过点的那一档，便宜时段才当场补跑，贵时段记进 `sleep.state.json` 的 `owedSlots` 等进便宜时段再补；待补槽只补当天，攒过 2 天作废并醒目记一行。
  - 判据只有一份，由 `lib/price-probe.js` 从 `priceClockFile`（默认 dsh-price-clock 的 `clock.js`）接 `isCheapNow`；没判据或判据抛错一律按便宜处理（fail-open），`sleepPriceProbe=false` 能整个退回老行为。

### 修复

- **睡觉改由 dsh-timer 的 `memory.sleep.tick` 驱动**（`a33e424`）：拿不到才回落原生定时器；`schedule` 缺席的日志分情况打，不再一律说「dsh-timer 未挂」。
- **`/new` 存档与睡醒汇总**：`archiveNotify` / `sleepSummaryNotify` 按配置走，子会话（subagent）会话一律不整理、不推通知。
- **prefill 超时默认 800 → 1200ms**（`c6f71de`）：票面实测 p50 595ms / p90 761ms，800 贴着线，慢一下这次注入就白丢。
- **整理活性判据改「超时」替掉「总时长」**（`c6f71de`）：模型慢慢吐字不再被总时长误杀；`schedule` 过点补判不再丢档。
- **compact 通道 kind 改道**（`7554b97`）：channel 回填按 kind 注册表走，`merge_op` / `dedupe_check` 不再被误标成 curate。
- **`merge_op` 重放空转**（`b81afa9`）：确定性无效当场判死，不再反复 attempts+1。
- **中午整理档 13:00 → 12:30**（2026-11-11），给 60 分钟那轮留余量；顶层 `hour`/`minute` 被 `schedule` 覆盖的重复配置删掉。

### 文档与配置

- README 按现状重写：模块表分「存储/检索、整理链、状态、实体」四组，补 12 张表的清单、四个通道与池语义、文件级 ack / 重放 / 租约，配置表按 Config schema 85 个键逐项核对（删掉已删键，补 `entityQueue` 整组、`contextWatch` 整组、`entityTidy.apply`、`sleepDeferInPeak`、`priceClockFile` 等）。
- `docs/sleep-pipeline-redesign.md`、`docs/agent-prompts.md` 按现状对齐（maxSessions / ingestBudget / replayBudget / clusterMaxSize / mergeBatchMax / 「串行调用 TODO」/ 空 ops 语义 / attempts 阈值 3→5）。
- 代码注释引用的三份工作区设计稿（save-queue / entity-rework / entity-dedup）在 `docs/` 补了指路文件，指向 `fatfish/docs/` 的正文。
- `cordis.patch.yml` 清掉 schema 里已不存在的 12 个键（`maxChanges` / `archiveMaxChanges` / `sleepMemoryChars` / `cooldownMinutes` / `addPassChars` / `clusterMaxSize` / `clusterChanges` / `organizeMaxCalls` / `sleepSplitLimit` / `conflictCheckMaxCalls` / `pipeline.ingestBudget` / `pipeline.replayBudget`）。

## 0.5.1（2026-11-09）

### 修复

- **organizeCache 改「主文件轮转 + 按月归档」（，2026-11-09）**
  - 病灶：线上 `sleep.state.json` 的 `organizeCache` 655 条 / 2635 行 / 51296 字节，只增不减，启动全量读；老逻辑虽写了 3000 条上限，线上从没到过线，文件只涨不缩。
  - 改法：新模块 `lib/organize-cache.js`，写入处（`organizePass` / `organizePassAgent` 收尾）统一走 `persistOrganizeCache()`：按 `t` 从新到旧只留最近 `organizeCacheMax`（默认 200）条，溢出的按写入月份（本地时区）搬进 `<state 同级>/organize-archive/organize-YYYY-MM.jsonl.gz`，一行一条 `{key, value, archivedAt}`。
  - gzip 追加用「解压旧文件 → 拼新行 → gzipSync → .tmp + rename」：单 member、失败旧文件一个字节不动；同 key 已归档跳过，归档只追加不删不覆盖。
  - 加载路径不变：`readState` 只读主文件，**不读归档**；回看走 `scripts/organize-archive.mjs`（`--month` / `--key-prefix` / `--json`）。
  - 降级：归档任何一步失败（目录不可写、gzip 失败、旧文件损坏）→ 主文件一条不删、只 warn，最坏退化成「不轮转」。
  - 迁移：`scripts/migrate-organize-cache.mjs --state <文件> [--yes]`，先备份 `.bak-organize-rotate-<时间戳>`，幂等可重跑，迁完自核「归档 + 主文件 = 迁移前总数」。
  - 真数据副本演练：655 条 → 主文件 200 条 / 15855 字节，归档 2026-11 共 455 条 / 7182 字节，守恒 655 = 200 + 455，零丢失；真文件未动。
  - 测试：新增 `test/organize-cache-rotate.test.mjs`（10 条：超阈值 / 未超阈值 / 跨月分卷 / gzip 可解 / 守恒 / 目录不可写 / 幂等 / 加载不读归档 / 默认值 / 同月追加保留旧行）。全量 `node --test test/` 221 → 231 条全绿。

## 0.4.2（2026-11-08）

### 修复

- **prefill 取词跳过 runtime-context 快照（t-prefill-v042）**
  - 病灶：0.4.1 的 `recallQuery` 已经在跑，但 `lastUserText()` 取的是「最后一条 role=user」，而 harness 每轮在 ego 念头（`source.kind=dsh-ego`）后面又插一条 `source.kind=runtime-context` 的 user 消息（整段环境快照）。召回词于是对快照无括号可剥，top3 依旧是基建条文，够不着念头。
  - 判定依据：`deriveMessages()` 对 `user/message` 直接返回 `event.data`（`@deepseek-ai/dsh-session/lib/index.js:222`），所以消息对象就是 data，kind 挂在 `message.source.kind`。
  - 改法：`lib/prefill.js` 的 `lastUserText()` 倒序跳过 `source.kind === 'runtime-context'` 的消息，落回用户真发言 / ego 念头 / `agent-message` 回灌；全是快照时回退成原来的「最后一条 user 消息」，不返回空串。只动 prefill 取词，`store.recall` 语义未动。
  - 真日志回放：父会话 seq 2904（dsh-ego 念头）→ seq 2905（runtime-context 快照），改后取到念头，query 不含 `Current runtime context`。
  - 测试：新增 `test/prefill-last-user-text.test.mjs`（6 条：念头→快照 / 用户短句→快照 / 全快照回退 / 隔轮快照 / 普通单条逐字一致 / 旧消息无 source 与空会话）。全量 `node --test test/` 199 → 205 条全绿。
  - 生效：需下次重启 dsh 才吃到新代码（今晚 05:00 记忆整轮不打扰）。

## 0.4.1（2026-11-08）

### 修复

- **prefill 召回词剔除 harness 指令行（t-prefill-fix）**
  - 病灶：speak-up / dsh-ego 这类长指令消息整段是括号包裹的 harness 指令行（占前 70% 字符），真意图排在最后、又被 `QUERY_MAX_CHARS`（500）截掉，召回 top3 永远是插件规则条文自己。
  - 改法：`lib/prefill.js` 新增导出函数 `recallQuery(raw)` —— 一行 trim 后整体以 `（`/`(` 开头、以 `）`/`)` 结尾就整行丢掉；剔完为空回退原文（绝不把 query 变空）；仍按 500 字截断。只作用于 prefill 路径，`store.recall` 的通用语义没动。
  - 关键词重排（`rankByKeywords`）也改用清理后的 query，避免被指令行 token 带偏。
  - 可观测：`memory_recall.log` 里 `caller=prefill` 的 query 现在只剩真意图。
  - 测试：新增 `test/prefill-recall-query.test.mjs`（6 条：注入模板 / 无括号短句逐字一致 / 全括号回退原文 / 半角括号 / 超 500 截断 / 单字符与正文括号不误伤）。全量 `node --test test/` 199 条绿。

## 0.4.0（2026-11-08）

### 新增

- **候选生成改向量近邻图（t-graph-cluster，2026-11-06）**
  - 新模块 `lib/similar-graph.js`：分块算余弦 → top-k 近邻（默认 8）→ 相似度 ≥ `clusterThreshold`（0.79）建边 → 并查集连通分量 → 按度中心性种子封顶 `clusterMaxSize`；单点分量丢弃；缺向量的条目回落实体分桶（旧 `clusterForSleep`）并记 `[cluster] no_vector fallback=entity count=N`；并查集前按 id 排序，同输入同阈值输出稳定。
  - 为什么换：老实体分桶在真库 2988 条 active 上出 896 簇，701 批（2248 条）批内最大余弦 <0.78 是白批，467 条空实体落 `_misc` 永远没候选。新办法同数据出 285 个多条目组 / 914 条，最大分量 36（切一刀）。
  - 接线：`lib/sleep.js` 的 `organizePass` 与 `organizePassAgent` 都走 `buildSleepClusters()`；老 `clusterForSleep` 保留，`clusterMode: 'entity'` 可切回。
  - 新配置：`clusterMode`（graph）、`clusterThreshold`（0.79）、`clusterNeighbors`（8）；`clusterMaxSize` 语义扩到图分量封顶，默认仍 30。
  - 成本：2988 条建图 14.0s（纯 JS 标量约 0.4 GMAC/s，numpy/BLAS 原型 0.42s）；老办法 1.65s。一天两次睡觉，可接受。

- **每轮自动回忆注入（autoPrefill，分级触发）**（2026-11-01）
  - `lib/prefill.js` 挂 `system-prompt/assemble` 钩子：用户消息进来先做纯本地触发判定（跳过寒暄/表情/过短消息，命中疑问词、动作词、时间指代、路径/实体/数字单位/参数等信号才查），只对触发的消息跑一次 `store.recall(query, 3)`，把命中条目压成 `[type] 摘要（id）` 摘要拼进 system prompt，节名 `memory-auto-prefill`、order 9500。
  - 目的：把「动手前先翻记忆」从模型自觉变成机制，同时不为此每条消息都交一次远端嵌入的网络税。
  - 召回质量：按「本轮消息的关键词命中数」给语义分加 0.65～1.0 的权重再排（英文词/数字 + 中文 bigram，剔停用），候选取 2×topK 只为重排留位置，不多花检索调用。
  - 省成本/防拖慢：单次检索 `autoPrefillTimeoutMs`（默认 800ms）超时即放弃；连续 3 次失败本会话闭嘴 5 分钟；同一条消息按会话 id + sha256 缓存（上限 50 条）；同一会话 10 分钟内**同一条记忆（按 id）**不重复注入（检索照跑，全被过滤时记 `all-repeated`）；subagent 会话一律跳过。
  - 触发判定里的路径信号只认 `/x`、`~/x`，且斜杠前必须是行首/空白/括号引号，`true/false`、`和/或` 不再误判。
  - 降级：任何异常（嵌入服务不可用、超时、assembly 结构异常）都 try/catch 静默跳过，`next()` 照常返回，绝不让 prompt 构建失败。
  - 可观测：每次判定写一行 `$DSH_MEMORY_LOG_DIR/memory-prefill.log`（时间 | 会话 | 判定 | 原因 | 命中数 | 耗时）。
  - 新配置：`autoPrefill`、`autoPrefillMinChars`、`autoPrefillTopK`、`autoPrefillMaxChars`、`autoPrefillTimeoutMs`、`autoPrefillEntityWords`。

### 修复

- **正文 lint 改两级复核：代码打标记，复核 agent 定生死（，2026-11-06）**
  - 新模块 `lib/lint-review.js`：`reviewLintItems` 拿「原文 + violations + 五条规则说明」问复核 agent，一个批次只发**一次**调用，返回 `{"decisions":[{index,verdict,reason}]}`；`judgeSuspected` 把裁决分成 keep（误杀，原样放行）/ redo（退回原 agent 重做）；`buildLintRedoFeedback` 拼重做提示（带 violations + 复核原话）；`mergeReviewRejects` 把两轮 violations 与复核理由合并成一条 discards 账。
  - 三类接线都改成三段：`lib/sleep.js`（ingest / organize 共用内部 `lintRedoOnce`）、`lib/agents/merge.js`、`lib/agents/split.js`、`lib/split-pass.js`。**重做只有一次，永不循环**（代码注释与测试都在钉）。
  - `lib/store.js` 的 `assertTextLint` 仍是最后一道闸：复核判 keep 的条目带 `__lintReview: {verdict:'keep', reason}` 过闸并记一行 INFO，绕过复核的路径照旧拒绝；`lib/lint-reject.js` 落账时把复核理由拼进 reason（截 500 字）。
  - 降级：复核抛错 / 超时 / 非 JSON / 漏判一律按 redo 走旧的「重写一次」，日志 `[lint-review] fallback`，条目不丢。开关 `pipeline.lintReview`（默认 true）、`pipeline.lintReviewTimeoutMs`（默认 90000）。
  - `buildSplitPrompt` 的重做提示上限从 200 字放到 900 字：按 200 字会把复核理由切掉一半。
  - 测试：新增 `test/lint-review.test.mjs`（11 条，含「3 条可疑合并 1 次调用」计数断言、keep/redo/重做仍不过/复核挂了/开关关掉），`test/merge-agent.test.mjs` 补 keep 与 redo 两条，`test/text-lint-wiring.test.mjs` 的旧用例显式 `lintReview:false`。全量 `node --test test/` 176 条绿。
  - 文档：README 新增「lint 两级复核」一节，阶段一/二两张图改成 sequenceDiagram（原 agent → 代码 lint → 复核 agent → store 四角色），阶段三补复核分支；`docs/sleep-pipeline-redesign.mmd` 与 `docs/sleep-pipeline-redesign.md` §2.4 同步。

- **真库 rowSeq 撞号受控重编号（t-rowseq-fix，2026-11-06）**
  - 新增 `scripts/fix-rowseq-dupes.mjs`：`--dry-run`（默认，只读）列重复组与改号计划，`--apply` 先在线备份到 `memory/backup/pre-rowseq-fix-20261106/memory.db` 并校验，再在**单事务**里只改 `memories.rowSeq`，`--verify` 对照基线断言行数 / 内容指纹 / vec_map 关联 / integrity。
  - 规则：每组保 id 前缀（base36 毫秒时间戳）更早那行的原号，后写入者改到 `max(rowSeq)+1` 起连续发号；无重复时不动（幂等）。
  - 真库实测：82 组 / 82 行，rowSeq 4795..5204 区间；改号后 1..5305 连续、无重复，正文 / 状态 / 向量一行没动（内容指纹一致），vec_map 行数 5308 不变（3 条已知孤儿照旧）。
  - 引用面确认：只有 `memories` 带 rowSeq 列，`vec_map` 按 `id TEXT` 关联、不按 rowSeq，所以撞号不会让 knn 串正文（真按 rowid=rowSeq 硬对齐会串 2453 行）。

- **账本重放与 rowSeq 取号（t-fix-quality 第二批，2026-11-06）**
  - `dedupe_check` payload 现在必带 `ids` + 完整 `candidate`（store 的 save/update/merge 三个过闸点都传）；入队改走 `enqueue(..., reviveDead)`，同 key 新 payload 能把 dead 拉回 pending。老账只有 action + candidate 时，重放按正文 + 实体召回现库条目补位；`action=merge` 但只召回一条就按 update 落地；召回也找不到才判死，`last_error=payload_missing_ids_no_candidate`。
  - `merge_op` payload 存 `source`（源条目 id + 原文）：源条后来被改/归档也能照原文再验。验得过但源已不在 active 就记 `replay_source_gone` 收口；老账只有 quote、没有 ids/source 的记 `no_source_for_replay`，不再无限 pending。
  - **rowSeq 撞号**：`saveMemory` / `updateMemory` 的取号从「进程内存 maxRowSeq+1」改成写事务里的 `SELECT COALESCE(MAX(rowSeq),0)+1`（BEGIN IMMEDIATE 已持写锁）。真库 82 对重复（164 行，内容各不相同）只报不修，新增只读检查脚本 `scripts/check-rowseq.mjs`（`readOnly:true` 打开）。回归测试：两个连接并发写 200 行，rowSeq 1..200 无重复；把取号改回旧实现该测试立刻挂。
  - 重放升级：`attempts>=2` 时换策略——`merge_batch` 成员超批就按 `mergeBatchMax` 切小批重新入队、本来不超批就逐条转 `split_item`；`merge_op` 转 `split_item`；日志固定 `[replay] escalate kind= from= to=`，原账标 done，不原地撞墙。
  - 新增 `scripts/revive-dedupe-check.mjs`：一次性把 dead 的 `dedupe_check` / `merge_op` 账补上 ids/source 并拉回 pending（默认预演，`--apply` 才写）。

- **合并批量与输出上限（t-fix-quality，2026-11-06）**
  - 合并调用默认**不传 maxTokens**：新增 `lib/llm-params.js` 的 `maxTokensOption()`，`mergeMaxTokens` 默认 0 = 省略字段，由 provider 封顶；拆分 / 去重 / 睡觉 / 压缩前整理同步去掉写死的 1500 / 300 / 16000 / 6000。要短靠提示词，配置键只在排障时显式收口。
  - `mergeBatchMax` 默认 12 → 6（配置 + 代码默认 + 文档 + 演练脚本）：单批最多 6 条，思考与 ops JSON 都放得下。切批影响 `organizeCache` 键：>6 条的簇按 ceil(n/6) 分键，30 条簇 1 键 → 5 键，首轮全 miss；≤6 条的小簇键不变。
  - 撞墙单独记因：`finishReason=length` 且正文/工具通道都空时记 `reasoning_exhausted`（与 `empty_response`、`parse_fail` 分开），日志与 `last_error` 带 `maxTokens=unset / completionTokens / reasoningTokens / reasoningChars`，撞的就是 provider 上限时 completionTokens 即真实上限值。
  - 模型明确给 `{"ops":[]}` 的批按 noop 写 `organizeCache`（有效结论），不再原地重试、不再落 `pending_ops`；下一轮同指纹直接命中缓存。
  - 文档：`docs/agent-prompts.md`、`docs/sleep-pipeline-redesign.md` 加「硬规矩」三条，参数表统一改成「输出上限：不设（不传 maxTokens）」。

- **整理提示词不说工具额度，模型一路查到底、空手收尾**（2026-11-03）
  - `lib/consolidate.js` 的 `SYSTEM` 拆成 `SYSTEM_BASE` + `systemPrompt({useTools, rounds, maxCalls})`：工具开着时明确写「这一轮你最多可以查证 N 轮、总共最多 M 次工具调用」和「M 次以内够用就够了，别为了穷尽而查；用完额度必须立刻输出 JSON 结果，不许再无结果收尾」，N/M 取 `consolidateToolRounds` / `consolidateMaxToolCalls` 的实时值，不写死。
  - `consolidateTools=false` 或轮数为 0（tools 参数根本不发下去）时，工具那几句整段不出现，user 提示词里的「可以用只读工具查一眼」和「不要再调工具」也一起去掉，不留「你有工具」的暗示。
  - 兜底措辞跟提示词对同一组数：超预算的工具结果改成「额度 M 次已用完」，撤工具后的催收消息带上「N 轮查证 / M 次调用已经用完」。
  - 保留 6a45e39 那版：`<<<资料开始/资料结束>>>` 资料块、`［引用 #n｜label］` 引文块、「不要补写不要续写」、末行 JSON schema 硬要求、「拿不准就查一眼」「只读，不许写」。

- **宣告式作废条目清不掉**（2026-11-02）
  - 症结：整理全由模型出 JSON 决定时，模型会写「某说法已作废」的宣告条，但被作废的旧条还 active；宣告条自己又过不了第四道否决「非状态类条目，不归档」，两头都动不了，旧说法永远留着。
  - `lib/sleep.js` 新增 `REVOCATION_PATTERN`（只认「作废/废止/不再适用/已废弃/已失效/已被取代/不再成立/已被推翻/口子过时」这类明确宣告废除的字样，不收「过时」这类宽词），第四道否决放行带宣告字样的条目。
  - 新增纯规则、不调模型的 `revocationPass`，挂在 sleepPass 阶段三之后：按宣告条的 `supersedes` 找旧条——旧条全归档则把宣告条当空账归档；旧条还 active 就先归档旧条再归档宣告条；找不到旧条只记日志交给人。每轮独立预算 20 条（不占 applyActions 的共享 budget），跟随外部 dryRun，opinion 与 PROTECTED_TAGS 一律跳过，单条出错不影响整轮睡觉。
  - 取数走 `store.metas()` 而非 `listAll`：`listAll` 不吐 `supersedes`，往里加字段会顺着 `memoryTexts` 把喂模型的 prompt 撑大。
  - 两处提示词（`SYSTEM` / `SYSTEM_CLUSTER`）各补一条：宣告条的 archive 目标应是它指名的旧条，新事实写成不带宣告语气的独立条目并用 `supersedes` 指向旧条。
  - `setupSleep` 末尾返回 `{ sleepPass, organizePass, archivePass, revocationPass }`，只为 `node -e` 做只读演练，`lib/index.js` 不接这个返回值。
