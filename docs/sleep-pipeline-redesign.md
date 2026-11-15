# 睡觉流水线重设计：唯一去重闸、文件级 ack 与可重放落盘

## 硬规矩（2026-11-06 维护者定，改代码前先看）

1. 不许给模型调用设 maxTokens 上限（除 provider 自带上限），要短就在提示词里说。
2. 记忆正文不许截断、不许丢弃，超长一律交拆分 agent。
3. 参数变更必须同步改文档参数表，代码默认值与文档不一致视为 bug。

**输出上限：不设。不传 maxTokens 字段，由 provider 上限约束；篇幅由提示词管，代码里禁止写死数值上限。**

2026-11-05 设计稿。本轮只出设计和时序图，不改运行时代码、不改库；文档和时序图提交到 `feature/memory-sqlite`，不推远端。

**2026-11-15 按现状对齐**：这是设计稿，落地后口径有变。下面标〔11-15〕的段落是当前真实行为，没标的是历史记录（留着看当初为什么这么定）。最大的一条变化：整理链已经从「一条顺序大函数」改成「队列 + 池」，调度形态看 `organizePipelineMode`（`pool` / `legacy`），四个通道 ingest / curate / compact / entity 各领各的活，见 `README.md` 的「睡觉流程」一节。

现状基线都是核实过的：`lib/sleep.js` 的 `sleepPass` 在 1124-1294，`organizePass` 在 1012-1122，`split-pass.js` 的 `runSplitPass` 从 184 起，`revocationPass` 在 709-760；200 字闸门实现是 `lib/text-limit.js:21-56`，调用点在 `sleep.js:562/596/631`、`consolidate.js:680`、`store.js:1209`；`store.merge` 在 `store.js:614-645`，正文保存和旧条更新是两个独立事务；`organizePass` 在 `sleep.js:1097` 把模型返回空动作的批缓存成 `noop`；`memvec.knn` 在 `memvec.js:113-142`，一次 top10 查询实测约 20ms；`relatedEntries` 在 `sleep.js:868-893`。会话侧现在没有索引，唯一游标是 `fatfish/memory/sleep.state.json` 的 `lastSleepAt`，`collectTranscript` 按 mtime 只取最后 20 个文件（`maxSessions` 这个键 2026-11-14 已删，现按 `ingest_files` 逐文件对账，见 §3）。本机现有 764 个 `session*.jsonl(.zstd)`、4805 条 active 记忆、`organizeCache` 476 条（370 acted / 106 noop）、`split.state.json` 527 条。764 个文件按 20 个一轮取，多出来的部分游标照推，等于永久丢，这是这次改造的主要动因。

## 1. 目标与不变量

这轮改造只认三条不变量，任何实现细节跟它们冲突都以它们为准。

### (a) 写记忆前必须过唯一去重闸

所有会落 `memories` 的路径——阶段一 add/update/merge、阶段二 organize、阶段三 split 拆出来的每一段、阶段四以及 `consolidate` 和 `memory_merge` 工具——都只走同一个 `WriteGate`。闸门内部顺序固定：200 字口径检查 → 召回（knn top10 + 字面 hash + `relatedEntries`） → `DedupeAgent` 判 `DUPLICATE / SUPERSEDES / DISTINCT` → 通过才进 Store 事务。任何调用点都不许自己直接 `store.save`，也不许用「文本完全相等」代替整道闸。`DUPLICATE` 写 `discards` 不入库；`SUPERSEDES` 旧条归档、新条带 `supersedes`，两笔在同一事务。

### (b) 任何一步失败都要落盘可重试，不许静默跳过

模型调用失败、JSON 解析失败、embedding 失败、SQLite busy、单文件读一半崩掉，都必须留一条 `pending_ops` 或 `ingest_files` 状态，带 `attempts`、`last_error`、`next_try_at`。日志只允许把「确定性无效」标成终态，比如 merge 的 ids 少于 2、update 的 id 不存在；这类重试一百次也不会变，直接记 `rejected` 并标 done，不算静默跳过。重试耗尽标 `dead` 也不是静默跳过：表行保留、写 WARN 日志、带指纹和成员 id，人工可用 `--retry-dead` 一次性捞回。现在的 `sleepPass` 在 parseActions 失败时打一行「跳过入库、继续分簇整理」然后照常推进 `lastSleepAt`，这是要废掉的行为。

### (c) 写记忆一律不截断正文，超长只走拆分

旧实现里 `clampMemoryText` 有一个 `truncate` 分支（已移除），它曾在 sleep 的三个调用点和 consolidate 把正文砍到 200 字以内，砍掉的半句连同因果一起消失。现口径：闸门发现正文超过 200 字，不改一个字，置 `needsSplit=true` 交给 `SplitAgent`；拆出来的每段必须 ≤200 字且带原文 evidence。拆分失败时原条保持原样、状态退 pending，绝不允许「截一段先写进去」。`clampMemoryText` 只保留 `ok` 判定和日志能力，`truncate` / `drop` 在写路径上不再产生落库结果（旧实现，已移除）。

## 2. 三个 agent 的职责与契约

三个 agent 都是「一次输入一份 JSON、一次输出一份 JSON」，不共享内存状态。handler 负责校验结构，结构不合法按失败语义处理，不让模型输出直接进 Store。

三个 agent 的调用参数统一为：输出上限：不设。不传 maxTokens 字段，由 provider 上限约束；篇幅由提示词管，代码里禁止写死数值上限。〔11-15〕批大小不再按条数写死：切批由 `lib/token-window.js` 按模型窗口算 token 预算决定（`sleepMemoryChars` / `addPassChars` / `clusterMaxSize` / `mergeBatchMax` 四个固定条数与固定字符预算的键全删了）。

### 2.1 MergeAgent

职责：看一小批（〔11-15〕批大小由 token 预算决定，簇也不按条数截）同簇记忆和它们的候选关联条，决定合并、修订、归档还是新增。它只出提案，不落库。

输入：

```json
{
  "batch_id": "fp_8f3a...",
  "budget": 6,
  "items": [
    {"id": "m_abc123", "text": "原文", "type": "world", "tags": ["机器"],
     "entities": ["树莓派"], "ts": "2026-11-01T...", "status": "active"}
  ],
  "relatedEntries": [
    {"id": "m_def456", "text": "同实体的旧条", "score": 0.83, "source": "related"}
  ]
}
```

输出：

```json
{
  "ops": [
    {"op": "merge",  "ids": ["m_abc123", "m_def456"], "text": "合并后的完整叙事",
     "evidence": ["原文片段一", "原文片段二"], "needsSplit": false},
    {"op": "update", "ids": ["m_abc123"], "text": "修订后的完整正文",
     "evidence": ["原文片段"], "needsSplit": false},
    {"op": "archive","ids": ["m_def456"], "text": "", "evidence": ["过期依据"], "needsSplit": false},
    {"op": "add",    "ids": [], "text": "新记忆正文", "evidence": ["会话原文片段"], "needsSplit": false}
  ]
}
```

约束与失败语义：

- `op=merge` 必须 `ids.length>=2`，且 id 都在输入 `items` 或 `relatedEntries` 里；`op=update/archive` 必须恰好 1 个 id；`op=add` 的 `ids` 必须为空。
- `text` 超过 200 字**不许截断**，原样返回并置 `needsSplit=true`，由流水线转 `SplitAgent`；`needsSplit=true` 时 `op` 只能是 merge/add/update，archive 不允许带长文。
- `evidence` 必须逐条能在输入 `items[].text` 或 `relatedEntries[].text` 里原样找到（去空白后子串匹配）。找不到的 op 整条拒绝，日志记 `evidence_miss`，不落库；这是防「合并时顺手编事实」的硬闸。
- 可重试失败：模型超时、返回非 JSON、`ops` 不是数组、整体结构坏。落 `pending_ops(kind='merge_batch', payload_json=输入, attempts+1)`，`next_try_at` 指数退避 1m/5m/25m。
- 终态失败：单条 op 语义无效（ids 数量不对、id 不存在、evidence 缺失）。记 `rejected`，同批其它合法 op 照常执行，批次标 done；不因一条坏 op 重放整批。
- 同一批连续 5 次仍拿不到可解析 JSON（`pending_ops` 各 kind 的 `maxAttempts` 是 5），标 `dead`，写日志交人看，不再重试。

#### 2.1.1 候选生成：向量近邻图（2026-11-06 t-graph-cluster）

**候选生成 = 向量近邻图连通分量，阈值 0.79；实体分桶降级为缺向量兜底。**

老 `clusterForSleep`（`lib/cluster.js`）按「主实体」凑批，真库 2988 条 active 上出 896 簇，其中 701 批（2248 条）批内最大余弦 < 0.78，送模型也是白跑；467 条空实体条目落 `_misc`，永远拿不到候选。现在改走 `lib/similar-graph.js`：

1. 取向量：条目自带 `vector`，否则 `opts.vectors`（运行时是 `store.vectorIndex()` → `memvec.viewAll()`，直接扫 vec0 的 `vec_items ⋈ vec_map`）。
2. 分块算余弦、每条取 top-k 近邻（默认 8，排除自身）。
3. 近邻余弦 ≥ 阈值建边，并查集求连通分量（并查集前按 id 排序，同一输入同一阈值输出稳定）。
4. 单点分量丢弃——自己一组不算候选。
5. 〔11-15〕分量不再按条数封顶：`clusterMaxSize` 已删。装不装得下只由 token 预算决定——先截簇会把「同簇同批」这层关系提前砍掉，切批是 `batch-planner.js` 在送模型前按窗口算的事。
6. 缺向量的条目回落老的分实体桶，日志 `[cluster] no_vector fallback=entity count=N`，条目一条不丢。

实参（`lib/index.js` schema，代码默认值即下表；`cordis.patch.yml` 不动）：

| 配置 | 默认 | 说明 |
| --- | --- | --- |
| `clusterMode` | `graph` | 候选生成方式。`graph` = 向量近邻图连通分量；`entity` = 切回老实体分桶（排障用） |
| `clusterThreshold` | 0.79 | 建图余弦阈值，近邻 ≥ 此值才连边（原型推荐 0.82，维护者 2026-11-06 定 0.79） |
| `clusterNeighbors` | 8 | 每条取几个近邻建图（top-k，排除自身） |

〔11-15〕`clusterMaxSize`（单簇 30 条封顶）已删，表里不再有这一行；批大小改由 `lib/token-window.js` 按模型窗口算，`lib/batch-planner.js` 负责切批与漏斗。

真库 2988 条实测（只读，`mode=ro` + `query_only`）：分量 2358、单点 2074、最大分量 36（切一刀）、多条目候选组 285、进组 914 条、建图 14.0s。同数据老办法 896 簇 / 786 个多条目批 / 2878 条进批 / 701 个白批、耗时 1.65s。差异只有「组数 × 条数 × 耗时」，快慢换在候选质量上：新组每组自带 ≥0.79 的边。

### 2.2 SplitAgent

职责：把一条超长记忆拆成多条原子记忆。只拆不编，原文没说的不许补。

输入：

```json
{
  "source_id": "m_abc123",
  "text": "完整原文，可能 800 字",
  "type": "experience",
  "tags": ["部署"],
  "entities": ["树莓派"],
  "attempt": 0,
  "feedback": "上一轮第 2 段没有 evidence，重拆"
}
```

输出：

```json
{
  "parts": [
    {"text": "第一段，不超过 200 字", "evidence": "原文里对应的一整句或连续片段"},
    {"text": "第二段，不超过 200 字", "evidence": "原文里对应的另一段"}
  ],
  "lostFacts": ["原文提到但拆完没落下的点"]
}
```

硬约束：

- 每段 `text.length<=200`，按 JS 字符串长度算，跟 `MEMORY_TEXT_MAX` 同口径；超一段整份输出作废。
- `parts.length>=2`；只有一段说明没拆，按失败处理。
- 每段 `evidence` 必须是原文的连续子串，且不能所有段共用同一句；evidence 对不上就作废。
- 只许删连接词、改标点、把「他/她」补成明确主语；不许新增原文没有的因果、数字、路径、结论。
- `lostFacts` 非空不直接判失败，但要把缺的点拼进下一次 `feedback` 重拆；两轮后仍有非空 `lostFacts`，把这些点作为独立 add 候选走 WriteGate，原文条目标 `split_partial` 并记日志。

失败语义：

- 可重试：模型超时、非 JSON、段超长、evidence 对不上、只有一段。落 `pending_ops(kind='split_item', payload_json={source_id,attempt})`，`attempt+1`。
- 单条最多退两轮（`attempt` 0→1→2）；第三轮仍失败标 `dead`，原条保持 active 不动，日志写 `split_dead source_id=... reason=...`，交人看。
- 拆分成功的落库不是 SplitAgent 干：它只返回 parts，每一段都要再过一次 WriteGate 和 DedupeAgent，拆出来的段之间也要互相判重。

### 2.3 DedupeAgent

职责：对一条待写候选做最后裁决。它是唯一有权说「这条不用写」的角色。

输入：

```json
{
  "candidate": {
    "text": "待写正文", "type": "world", "tags": ["机器"],
    "entities": ["树莓派"], "source": "sleep:split", "evidence": ["原文片段"]
  },
  "recall": {
    "knn": [{"id": "m_def456", "similarity": 0.91}],
    "hash": [{"id": "m_aaa111", "hash": "sha1..."}],
    "related": [{"id": "m_bbb222", "text": "同实体旧条", "score": 0.78}]
  }
}
```

输出：

```json
{"verdict": "DUPLICATE", "matchedId": "m_def456", "reason": "同一件事的同一结论，只差措辞"}
```

`verdict` 只有三个值：

- `DUPLICATE`：同一事实的重复表述。候选写 `discards`，不入 `memories`；`matchedId` 必须指向 recall 里的条目。
- `SUPERSEDES`：候选是新版本，旧条该退。旧条 `status=archived` 或 `merged`，`supersededBy=新 id`，新条 `supersedes=[旧 id]`，新旧同事务。
- `DISTINCT`：确实是新事实，正常入库。

失败语义：

- 模型超时、非 JSON、`verdict` 不在枚举里、`DUPLICATE/SUPERSEDES` 却没给 `matchedId`、`matchedId` 不在 recall 集合里——一律按失败处理，落 `pending_ops(kind='dedupe_check')` 重试，**不许降级成 DISTINCT 直接写**。
- 连续 5 次失败标 `dead`，候选停在 pending，不写库，日志交人。
- 判重是写前最后一步，`store.save/merge/update` 内部不再做第二套文本相等判断；只保留事务内的候选 hash 唯一索引兜底。

### 2.4 正文 lint 的两级复核（2026-11-06 补）

正文 lint 从「代码一票否决」改成两级：`lib/text-lint.js` 只打标记并收集可疑集（一批可疑只发一次调用）；`lib/lint-review.js` 的复核 agent 拿原文 + violations + 五条规则说明，回答 `keep`（误杀，原样放行，写库带 `__lintReview`，store 记 INFO）或 `redo`（退回原 agent 重做，只重做一次，重做版再过代码 lint，仍不过落 `discards(verdict=LINT_REJECT)`，保留两轮 violations 与复核理由）。

- 开关 `pipeline.lintReview`（默认 true）；`false` 完全回到旧的「重写一次」逻辑。
- 复核超时 `pipeline.lintReviewTimeoutMs`（默认 90 秒）；抛错、超时、非 JSON、漏判一律按 redo 降级，日志 `[lint-review] fallback`，条目不丢。
- `lib/store.js` 的 `assertTextLint` 仍是最后一道闸：绕过复核直接写库的路径（consolidate、脚本、重放）照旧被它拦住。
- 完整链路图和 mermaid 时序见 `README.md` 的「lint 两级复核」与阶段一/二两张图。

## 3. 会话入库段改造

### 3.1 废除单游标 + 末 20 个文件

现在的 `collectTranscript` 用 `since=lastSleepAt` 过滤 mtime，再 `logs.slice(-cfg.maxSessions)`。两个问题叠在一起：20 个之外的文件永远读不到；游标在轮末照推，推过去就再也回不来。764 个文件里绝大多数从没进过模型。

〔11-15〕`maxSessions` 已删。待处理文件全部入 `pending_ops(kind='ingest_file')`，`planPoolQueues` 一轮只负责入队，处理不完的留在队列里等下一轮，不再有「单轮只看末 N 个」这层预算。

新做法是给每个会话文件建一行账，处理到哪、失败几次、下次什么时候试，都写在账上。游标从「一个全局时间戳」降级成「每个文件各自的 `last_acked_ts`」。

新表：

```sql
CREATE TABLE ingest_files (
  path          TEXT PRIMARY KEY,   -- 绝对路径，session*.jsonl 或 .zstd
  mtime         REAL NOT NULL,      -- 上次见到的文件 mtime，毫秒
  size          INTEGER NOT NULL,   -- 上次见到的字节数
  last_acked_ts INTEGER NOT NULL DEFAULT 0, -- 已确认消费到的最后事件时间，毫秒
  status        TEXT NOT NULL DEFAULT 'pending', -- pending|done|legacy_acked|failed|dead
  attempts      INTEGER NOT NULL DEFAULT 0,
  last_error    TEXT,
  updated_ts    INTEGER NOT NULL
);
CREATE INDEX idx_ingest_status ON ingest_files(status, updated_ts);
```

`next_try_at` 不在这张表里，统一放 `pending_ops(kind='ingest_file')`，避免两处退避时间打架；`ingest_files` 只记「这个文件当前是什么状态」。

### 3.2 枚举与待处理判定

每轮扫 `DSH_SESSIONS_DIR`，默认 `$DSH_HOME/sessions`，递归匹配 `session*.jsonl` 和 `session*.jsonl.zstd`，沿用现在「根/子目录/会话目录/文件」的四层结构，但不依赖层数，递归找就行。

待处理判定满足任一：

1. `ingest_files` 里没有这个 path；
2. 文件 `mtime` 或 `size` 跟表里记的不一样；
3. `last_acked_ts < 文件最后事件时间`（文件被追加过，但上次只读到旧位置）。

第 3 条要求读文件时顺手取最后一个合法事件的 `time`，只解析行不算贵；`.zstd` 要整包解压，一轮只解一次，解压结果在本次处理内复用。

`status='legacy_acked'` 是 6.3 的 bootstrap 造出来的历史状态：它表示「按 `lastSleepAt` 判定这批旧事件不重挖」，不参与常规 pending，也不按第 3 条重新捞。只有文件后来被追加、`mtime`/`size` 变了（第 2 条），或人工跑了 `--backfill`，它才会重新进 pending。

〔11-15〕**没有单轮文件预算。** `maxSessions` 这个键已经删了：`planPoolQueues` 把对不上账的文件一次全部入队，ingest 池按「批大小 = 当前并发数」一批批领，处理不完的留在 `pending_ops` 里等下一轮，永不丢弃。文件被标 `dead`（attempts 到 5）的不再自动捞，只出现在日志和人工清单里。

### 3.3 单文件流程与 ack

每个文件严格按这个顺序：

1. 读文件、解压、逐行解析事件，取 `time > last_acked_ts` 的事件（新文件 `last_acked_ts=0` 全读；`legacy_acked` 不读；被追加的文件只读新增段）。
2. 抽候选：沿用现有 transcript 抽取规则，只留真正的人机对话，跳过运行时上下文、后台回填、系统注入。
3. 候选逐条进 WriteGate：200 字检查 → 超长转 SplitAgent → 每段过 DedupeAgent → Store。
4. 整文件所有候选都处理完（入库、判重丢弃、终态 rejected 都算处理完），开一个事务：写 `ingest_files.last_acked_ts=读到的最后事件时间`、`mtime`、`size`、`status='done'`、`attempts=0`、`updated_ts=now`。
5. 事务提交成功，这个文件才算 ack。中途崩了没提交，下轮 `last_acked_ts` 还是旧的，整个文件重读；重读产生的重复候选会被 DedupeAgent 判 `DUPLICATE` 丢掉，所以重放安全。

### 3.4 失败与退避

- 单文件读失败、解压失败、embedding 连续失败、单文件事务回滚：`status='failed'`、`attempts=attempts+1`、`last_error` 存最后一条错误、**不推进 `last_acked_ts`**；同时 upsert 一条 `pending_ops(kind='ingest_file', payload_json={path}, next_try_at=now+退避)`。
- 退避按 `attempts`：1 分钟、5 分钟、25 分钟、125 分钟；上限 6 小时。
- `attempts` 到 5（`MAX_ATTEMPTS`）标 `status='dead'`，写一行 `[ingest] dead path=... attempts=5 last_error=...`，从自动队列移除，等人处理。dead 的文件不推进 ack，不算处理成功。
- 半成功：文件里 10 条候选，3 条入库、2 条重复、1 条终态 rejected、4 条因模型失败 pending——整文件事务仍可提交 ack 吗？不行。只要还有可重试的候选没落定，文件保持 `failed`/`pending`，`last_acked_ts` 不推进；下轮重读，已入库的 3 条会被判重，成本可接受，正确性优先。

### 3.5 `lastSleepAt` 的定位

`sleep.state.json.lastSleepAt` 保留，但只表示「上一轮完整跑完的时刻」，给调度、日报、兼容旧脚本看。它**不再是会话入库的唯一真相**，不再参与「从哪开始读」的判断。新代码里任何 `since=lastSleepAt` 的过滤都必须删掉；判断待处理只看 `ingest_files`。首次 bootstrap 会用它，见第 6 节。

## 4. 落盘重试：pending_ops

### 4.1 表

```sql
CREATE TABLE pending_ops (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  kind        TEXT NOT NULL,        -- ingest_file|merge_batch|split_item|dedupe_check
  dedupe_key  TEXT NOT NULL,        -- kind + 稳定 payload 摘要，防重复入队
  payload_json TEXT NOT NULL,
  attempts    INTEGER NOT NULL DEFAULT 0,
  next_try_at INTEGER NOT NULL,
  last_error  TEXT,
  status      TEXT NOT NULL DEFAULT 'pending', -- pending|running|done|dead
  created_ts  INTEGER NOT NULL,
  updated_ts  INTEGER NOT NULL,
  UNIQUE(kind, dedupe_key)
);
CREATE INDEX idx_pending_due ON pending_ops(status, next_try_at, attempts);
```

`status='running'` 带租约：领取时写 `updated_ts=now`，超过 10 分钟没更新视为租约过期，下轮重新捞回 pending，防进程崩溃后卡死。

### 4.2 每轮开头重放

`sleepPass` 一进来，先于会话扫描做重放：

```sql
SELECT * FROM pending_ops
WHERE status='pending' AND next_try_at<=:now
ORDER BY attempts ASC, next_try_at ASC;   -- 〔11-15〕没有 LIMIT :replayBudget，该键已删
```

按 `kind` 分派：`ingest_file` 走单文件流程；`merge_batch` 重问 MergeAgent；`split_item` 重问 SplitAgent；`dedupe_check` 重跑召回和裁决。重放成功标 `done`，失败 `attempts+1`、`next_try_at` 退避、`last_error` 更新；`attempts` 到 5 标 `dead`。当期到期的活全部领出来，没有条数预算。

### 4.3 重放必须幂等

- 写库前一律再查一次 dedupe，不能信「上次没写成」这种假设。候选文本的 sha1 在 `discards` 和 `memories` 里查得到，就判 `DUPLICATE`，记 `discards` 并标 `done`。
- `ingest_file` 的 ack 只在整文件事务里推进，重放时如果 `last_acked_ts` 已经 >= 目标事件时间，直接标 done，不重复读。
- merge 重放用 `batch_id` 做幂等键：新条 `source='sleep:merge:<batch_id>'` 已存在说明上次提交成功，只补旧条状态并标 done。
- split 重放先查 `split_done.source_id`，已有拆分明细就跳过，避免同一原条拆两遍。

### 4.4 `{}` 空动作与 parseActions 全失败的区别

〔11-15〕这两类已经分开处理了，别再按老口径写账：

- **模型明确给 `{"ops":[]}`（空动作）**：这是有效结论 —— 记 `organizeCache`（`v:'noop'`），不入 `pending_ops`、不耗 attempts，下一轮同指纹直接命中缓存。
- **空响应 / 非 JSON / 解析失败**：落 `pending_ops(kind='merge_batch', payload_json=原批, attempts+1)`，`last_error` 存 `empty_response` 或 `parse_fail`（raw 尾部 260 字），按退避重问；`attempts` 到 5 转 `dead` 并写 WARN（带 batch 指纹和成员 id），不再自动重问。

两者的区别只在终态字段：空响应记 `empty_response`，解析失败记 `parse_fail`；落盘、退避、转 dead、人工捞回走同一条路。阶段一发生这种事时，除了 merge_batch，还要为该轮抽出的候选落一条 `ingest_file` 重试，**不许再推进 `lastSleepAt`**。
- `--retry-dead` 语义：把指定 kind 的 `dead` 行改回 `pending`、`next_try_at=now`、`attempts=0`，给一轮新预算，只捞这一次；再连续失败 3 次还会回 dead。不会自动定时跑。

## 5. 一致性与事务

### 5.1 `store.merge` 改单事务

现状 `store.js:614-645`：`mergeMemories` 先 `await saveMemory(...)`（自己一个 `writeTx`），再对每个旧 id `await updateMemory(...)`（各一个 `writeTx`）。中间崩了会留下「新条已写、旧条还 active」，或者「一部分旧条 merged、一部分没动」。

改成 `mergeMemoriesTx(ops)`：embedding 仍在事务外先算好（网络调用不能占着 SQLite 写锁），拿到向量后开一次 `BEGIN IMMEDIATE`，在同一事务里：

1. `upsertRow(新条, nextSeq)`；
2. `vecDb.insert(新条 id, vec)`（`vecDb` 与正文同一个连接，见 `store.js` 的 `writeTx` 注释）；
3. 对每个旧 id `upsertRow(旧条 status='merged', supersededBy=新 id, reason='merged into ...')`；
4. `COMMIT`。

任一步抛错整笔 `ROLLBACK`，`metas` 内存数组不更新。`SUPERSEDES` 路径同理：新条 insert、旧条 archive、`supersededBy/validTo` 一次提交。`archive` 单独调用也走一个事务，但不再和 merge 拆开。

### 5.2 会话文件级事务

一个文件的候选可能十几条，逐条事务会让「文件 ack」和「正文落库」脱节。改成：先跑完模型和 dedupe，把决定攒成 `writes[]`（insert/update/archive），再开一个事务，把 `writes[]` 全部执行 + 更新 `ingest_files` 一行 + 写 `discards[]`，一起提交。文件大、候选多时事务会稍长，但 `busy_timeout=5000` 和 WAL 够用；实在太大按 200 条候选切块，最后一块带 ack。

### 5.3 写库与向量

`memvec` 与 `store` 共用同一个 SQLite 连接，这是现成的优势，别破坏。写正文和写向量必须在同一事务；如果先提交正文再异步补向量，进程崩了会留下「有记忆、搜不到」的孤儿。`saveMemory` 现在的做法是对的，`merge` 要补齐。事务提交后如果向量索引校验发现缺失（`logVectorGaps` 那类），记 `pending_ops(kind='dedupe_check')` 不适用，应该单独 `vector_repair` 队列；这轮先记录日志，不扩 kind。

### 5.4 DDL 草案

```sql
CREATE TABLE IF NOT EXISTS ingest_files (
  path          TEXT PRIMARY KEY,
  mtime         REAL NOT NULL,
  size          INTEGER NOT NULL,
  last_acked_ts INTEGER NOT NULL DEFAULT 0,
  status        TEXT NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending','done','legacy_acked','failed','dead')),
  attempts      INTEGER NOT NULL DEFAULT 0,
  last_error    TEXT,
  updated_ts    INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS pending_ops (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  kind         TEXT NOT NULL
               CHECK (kind IN ('ingest_file','merge_batch','split_item','dedupe_check')),
  dedupe_key   TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  attempts     INTEGER NOT NULL DEFAULT 0,
  next_try_at  INTEGER NOT NULL,
  last_error   TEXT,
  status       TEXT NOT NULL DEFAULT 'pending'
               CHECK (status IN ('pending','running','done','dead')),
  created_ts   INTEGER NOT NULL,
  updated_ts   INTEGER NOT NULL,
  UNIQUE(kind, dedupe_key)
);

CREATE TABLE IF NOT EXISTS discards (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  candidate_hash TEXT NOT NULL,     -- sha1(去空白正文)
  text         TEXT NOT NULL,       -- 候选原文，方便人看
  matched_id   TEXT,                -- 被判重/被取代的旧条
  verdict      TEXT NOT NULL CHECK (verdict IN ('DUPLICATE','SUPERSEDES')),
  reason       TEXT,
  source_file  TEXT,                -- 来自哪个会话文件，空表示来自 merge/split
  created_ts   INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_discards_hash ON discards(candidate_hash);
CREATE INDEX IF NOT EXISTS idx_discards_matched ON discards(matched_id);

CREATE TABLE IF NOT EXISTS split_done (
  source_id  TEXT PRIMARY KEY,
  into_json  TEXT NOT NULL,         -- 拆出来的新 id 数组
  n          INTEGER NOT NULL,
  ts         INTEGER NOT NULL
);
```

`split_done` 是第四张表，专门接 `split.state.json` 的 527 条历史，避免把已完成记录塞进 `pending_ops` 把队列表撑成日志表。

## 6. 迁移与兼容

### 6.1 `organizeCache` 476 条

现状 370 条 `acted`、106 条 `noop`。`acted` 是有价值的「这批已经处理过」证据，保留；`noop` 是这次要废掉的错误缓存，全部忽略但**不删**，留一版观察。

新代码读缓存时只看 `v==='acted'`，且给缓存加版本前缀 `v2:`；`noop` 条目不参与命中。迁移脚本把 370 条 `acted` 原样搬进新 key `v2:<fp>`，106 条 `noop` 移到 `legacyNoop` 下只读保存。等新流水线跑满两周、确认没有因为「跳过 noop 批」漏掉合并，再决定删不删。

### 6.2 `split.state.json` 527 条

结构是 `{ids: {source_id: {ts, into: [...], n}}}`。一条一条映射进 `split_done(source_id, into_json, n, ts)`，`into_json` 存新 id 数组。迁移后 `split.state.json` 改名 `split.state.json.migrated` 保留，不再读写；旧代码回滚时还能改回来。

### 6.3 首次 bootstrap `ingest_files`

首个版本**不重读历史**：旧会话重挖成本高、内容陈旧，而且旧代码只看了末 20 个，历史覆盖面本来就不全。首次运行按「文件最后事件时间」跟现有 `lastSleepAt` 比，分两类：

- `文件最后事件时间 <= lastSleepAt`：视为历史已覆盖，`last_acked_ts = 文件最后事件时间`、`status='legacy_acked'`。不读、不抽候选、不花模型调用。这批文件以后被追加时，`mtime`/`size` 变化会按 3.2 第 2 条重新进 pending，只读新增事件。
- `文件最后事件时间 > lastSleepAt`：`last_acked_ts = lastSleepAt`、`status='pending'`，正常入库；`lastSleepAt` 之前的旧事件不回头补。

`lastSleepAt` 同时写进 `meta.legacy_watermark` 留审计。**首个版本不保证历史上限前的会话被挖过**（`lastSleepAt` 之前的部分），这是明确取舍，不是漏做；需要回捞时按下面的手动子命令来。

手动回捞，默认不触发：

```
dsh-memory sleep --backfill <path|since>
```

- `--backfill /abs/path/to/session.jsonl`：只把该文件置 `pending`、`last_acked_ts=0`，下一轮从头重读。
- `--backfill 2026-10-01`：把最后事件时间晚于该日期的文件置 `pending`，`last_acked_ts = min(当前值, 该日期 00:00)`，只补该日期之后的事件。

两条命令都只改 `ingest_files` 状态，不直接跑模型；真正执行仍走正常轮次和单轮预算，重读产生的重复由 DedupeAgent 丢掉。

### 6.4 回滚

- cutover 前用 `VACUUM INTO '$DSH_MEMORY_ROOT/memory.db.bak-2026-11-05'` 备份，连同 `sleep.state.json`、`split.state.json` 一起进 `memory/backup/<日期>/`；备份文件校验 `PRAGMA integrity_check`。
- 新表全部 `CREATE TABLE IF NOT EXISTS`，旧代码不认识也不会碰；`organizeCache` 的 `v2:` 前缀旧代码不命中，等于回到旧缓存，不会崩。
- 代码保留旧分支 tag `pre-sleep-v2`，配置加 `pipelineV2: false` 开关；回滚就是停服务、`cp` 回备份库、切 tag、起服务。旧代码继续用 `lastSleepAt`，能跑，但会重新开始丢 20 个以外的文件——所以回滚只做止血，不做长期方案。
- 本次提交只含设计文档与配套渲染图，不动库；上面这些备份和切分支步骤都是 cutover 时才执行的。

### 6.5 已拍板的取舍

1. `organizeCache` 的 106 条 `noop` 不删，移到 `legacyNoop` 命名空间只读保存，不参与查询。留一版可对照，等新流水线跑满两周再决定清不清；`acted` 的 370 条照常迁移。
2. 历史会话**不默认重挖**：`lastSleepAt` 之前的文件统一 `legacy_acked`，不花模型调用；需要时用 `--backfill <path|since>` 手动回捞。代价是首个版本不保证历史上限前的会话被挖过，这一点写进发布说明，不藏。
3. 空动作批**不设 `noop_verified` 放行**：重试 3 次仍空就 `dead` + WARN，行保留、人工 `--retry-dead` 才捞。代价是会留下一些 dead 行需要人看，好处是「模型没吐东西」永远不会被固化成「这批没问题」。

## 7. 验收口径与日志样板

一轮结束必须能从日志回答五个问题：读了哪些会话文件、哪些失败要重试、多少条被判重复丢弃、拆分膨胀率多少、合并有没有改错事实。日志按下面的行样板出，字段名固定，别各写一套。

```
[sleep-v2] round=2026-11-05T03:00:00+08:00 budget=20 replay_budget=50 bootstrap=0
[ingest] file=$DSH_HOME/sessions/.../session-xxx.jsonl.zstd events=142 cand=11 ack_ts=1793826823423 status=done
[ingest] file=$DSH_HOME/sessions/.../session-yyy.jsonl events=8 cand=1 ack_ts=0 status=failed attempts=2 next_try_at=2026-11-05T03:05:00+08:00 error=zstd_decompress
[ingest] dead path=$DSH_HOME/sessions/.../session-zzz.jsonl attempts=4 last_error=...
[ingest] bootstrap 扫描764 legacy_acked=744 pending=20 读失败=0
[retry] id=41 kind=ingest_file attempts=1 next_try_at=2026-11-05T03:01:00+08:00 last_error=parse_fail file=$DSH_HOME/sessions/.../session-xxx.jsonl
[retry] id=42 kind=split_item attempts=1 next_try_at=2026-11-05T03:01:00+08:00 last_error=evidence_miss
[dedupe] verdict=DUPLICATE matched=m_def456 hash=9f2c... text40=树莓派温度...
[dedupe] verdict=SUPERSEDES matched=m_aaa111 new=m_ccc333 reason=新结论取代旧结论
[split] source=m_abc123 parts=5 chars_before=812 chars_after=690 ratio=0.85 lostFacts=0 attempt=0
[merge] batch=fp_8f3a ops=6 merged=2 updated=1 archived=1 add=2 rejected=0 evidence_hit=6/6
[merge] bad_fact batch=fp_8f3a op=merge ids=m_a,m_b evidence_miss=合并后多了原文没有的结论
[WARN] merge batch=fp_8f3a empty_response attempts=3 status=dead members=m_a,m_b 可 --retry-dead
[sleep-v2] end files_scanned=764 legacy_acked=744 pending=20 processed=20 done=18 failed=2 dead=0 over_budget=5 queued=2 replay_done=1 replay_failed=0
[sleep-v2] dedupe dup=9 supersede=4 distinct=44 discarded=9 split_items=5 split_parts=17
[sleep-v2] merge batches=6 ops=11 rejected_ops=1 tx_commit=18 tx_rollback=0 dead_empty=1 dead_parse=0
[sleep-v2] chars_in=18234 chars_out=17901 split_ratio=0.98 ms=84211
```

验收判定：

- `files_scanned - legacy_acked - pending - over_budget - dead` 应该等于 `done`；`pending = processed`，`processed = done + failed`，对不上就是账错了。
- 空动作或解析失败转 dead 的，必须有 `[WARN]` 行且带 batch 指纹与成员 id；`dead_empty` / `dead_parse` 与 WARN 行数对得上。表里 `dead` 行不许被自动删除，只能 `--retry-dead` 捞回。
- 任何 `status=failed` 必须同时有 `[retry]` 行和 `next_try_at`；只有 `[ingest] failed` 没有 `[retry]` 视为 bug。
- 入库账要闭合：`files_scanned = legacy_acked + pending + over_budget + done + dead`，`pending = processed`，`processed = done + failed`；`queued` 是本轮进 `pending_ops` 的条数（含重放失败回队），`replay_done + replay_failed` 等于本轮领到的重放条数。对不上就是账错了。
- `dedupe dup` 必须等于 `discards` 表本轮新增行数；`discards` 里 `verdict=DUPLICATE` 的候选正文不许出现在 `memories`。
- `split ratio = chars_after / chars_before`，正常应 ≤1.05；超过 1.2 说明 SplitAgent 在编内容，人工抽查该 source。
- `merge evidence_hit` 必须等于 ops 数；出现 `bad_fact` 行就停自动合并，人工复核该批。每周再抽 10 条 merge 结果做人工比对，日志记 `audit sampled=10 wrong=0`。
- `tx_rollback` 大于 0 要能对应到 `[retry]`，不能只回滚不重试。

### 7.1 从日志确认开关状态（运维）

插件每次启动打一行快照，开关状态直接看这行，不要再去 dump 配置反推：

```
[memory] pipeline ingestAck=true agentPipeline=true writeGate=true threshold=0.9 store=sqlite v=0.6.0
```

`ingestAck` / `agentPipeline` / `writeGate` 三个布尔值就是生效值；跟配置里的 `pipeline.*` 对不上，先怀疑配置没被插件读到（宿主改完配置要重启才生效）。`store=jsonl` 说明 `storePath` 下找不到 `memory.db`，这次启动的写路径不是 sqlite，得先查库路径。

#### 7.1.1 别拿文件 sha 判真库有没有被写

演练脚本早期用 `memory.db` 的 sha256 + mtime 当「真库没被写」的守卫，这条路在现在的真库上不成立，2026-11-05 换成逻辑指纹。

- 真库是 WAL 模式，新写先落 `memory.db-wal`，主文件可以一动不动。实测：往 WAL 连接里插一行、不 checkpoint，`memory.db` 的 size 和 mtime 完全没变，`-wal` 才有字节；反过来 `PRAGMA wal_checkpoint(TRUNCATE)` 因为常驻 qqbot 在读，直接 `disk I/O error`，也拿不到锁。
- 所以 sha 守卫会出现两种错判：真有人写了却「未变 ✅」；或者 checkpoint 恰好合进主文件就「变了 ❌」，都不是我们干的。
- 现在 `scripts/rehearse-pipeline.mjs` 前后各取一次**逻辑指纹**（`lib/db-fingerprint.js`）：READONLY 连接里开一个读事务，一次取完 `memories` 行数、`vec_map` 行数、`max(rowSeq)`、`ingest_files` / `pending_ops` / `discards` 行数，拼成 canonical 串再比。任一字段不同就 exit 2，并把变了的字段逐个打出来（`memories: 4855 → 4856` 这种）。
- 逻辑指纹变了也不一定是本轮写的：常驻 qqbot 也在写同一个库。看到 diff 先按字段判断是谁的手笔（`discards`/`pending_ops` 动通常是我们，`memories` 行数动且 `rowSeq` 连号要看时间戳），再决定要不要重跑。
- 顺带记一条：READONLY 打开是能看见 `-wal` 里还没 checkpoint 的行的。2026-11-05 实测真库 `memories` 4855 行（`vec_map` 4858、`max(rowSeq)` 4854），最新几条都在 `-wal` 里，照样读得到。
- 文件层面的大小 / mtime / `-wal` 大小仍然打印，但那只是给人看的参考，不参与判定。

## 8. 实施顺序（供下一轮拆票）

1. 建三张新表 + `split_done`，迁移 `split.state.json`；不动旧代码路径。
2. 写 `ingest_files` 扫描器，先 dry-run 只打印待处理清单，核对 764 个文件的账。
3. 接 WriteGate：把 sleep 三个调用点、consolidate、store 工具统一到一个入口，先保留旧 clamp 行为做对照。
4. 接 DedupeAgent / SplitAgent / MergeAgent，先影子模式：只记 `discards` 和日志，不真归档。
5. `store.merge` 单事务改造 + 回滚测试（人为在第二步抛错）。
6. bootstrap 只做 `legacy_acked` 标记和 `lastSleepAt` 之后事件入库，核对 `pending` 数量与 `legacy_acked` 数量；`--backfill` 先拿单个文件试跑，不放进默认轮次。
