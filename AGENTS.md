# dsh-memory 维护说明

通用工程规矩见工作区根 `../../AGENTS.md`，这里只写本插件整理链的落点和检查命令。
README 讲功能，这文件讲「动手改的时候照哪里做」。

## 三条硬规矩：改 src、先 build、别碰 lib

- 改代码只改 `src/**/*.ts`；`lib/` 是构建产物，手改会被下一次 build 盖掉，也不在 git 里。
- 切分支、回滚、从远端拉过代码之后，第一件事 `npm run build`（`prepare` 只在 install 时自动跑一次）。
- `pnpm run build` 自带类型检查（`tsc --noEmit && tsdown`），类型不过就构建不出来；`prepare` 仍是纯 `tsdown`，install 时不跑类型检查。
- 产物不进 git：`git status` 里出现 `lib/` 改动就是搞错地方了，先查是不是手改了产物。
- 本文件里凡是提到 `lib/*.js` 的地方都是历史表述，一律以 `src/*.ts` 为准。

## 整理链是队列 + 池，不是一条顺序大函数

- 池的语义只有五条，写在 `src/pool.ts` 文件头：小批领活、批内并发问模型、串行落地、
  空了就等、不收敛不算完。改调度先读那五条，别另起一套。
- 并发口径只有一处：`src/concurrency.ts`。cap / 负载快照 / 本轮实际路数都在那儿算，
  唯一允许手工写死的是 `organizeConcurrency`，而且只许往下压。别在某一段自己写死 3 或 8。
- 改性能之前，先把链上每一处「逐条问模型」的循环列全，一起看：
  - `src/consolidate.ts` 的 `askModel`：一段整理问一次，失败不自动重试，只记账等下一轮；
  - `src/sleep.ts` 的 `askModel` / `askCluster`：按簇切片，每片一次；
  - `src/split-pass.ts` 的 `askSplit`：`for (const source of targets)` 里逐条问，最长的一条链；
  - `src/entity-worker.ts` 的 `extractEntities`：一批问一次。
  只补其中一处，整条链不会变快。改完贴同一段链路的调用次数和耗时对比。

## 领活：一批 = 当前并发数

- 实体队列走 `claimBatch`（`src/entity-queue.ts`）：`SELECT ... LIMIT ?` 选、带 `state='ready'`
  条件 CAS 改 `leased`，领不到就换下一行。`batch` 不许超过当前并发数，也不许整表标 running。
- 租约过期由 `reapExpired` 捞回 ready，attempts 不重复加；租约时长看 `leaseMs`，
  别用 TTL 当锁——心跳和 owner 才是判活的依据。
- 池消费入口是 `consumeEntityBatch`；定时一轮用 `runEntityQueue`，并发上限 4，
  配置项 `entityQueue.concurrency`。长驻调度看 `src/entity-worker.ts` 末尾的 tick。

## 重试与判死：空结果也要记账

- 整理批的键是 `batchFingerprint()`（`src/sleep/batching.ts`，成员 id + 正文）。正文没变才允许命中缓存；
  条目被改、被并、被拆，指纹跟着变，自动重问。
- 模型空返回、解析 0 条、撞墙、ops 全被挡，都算一次尝试：落 `pending_ops`（或队列的 attempts 账），
  推进 attempts，写清 `last_error`。不许把空返回当 noop 静默吞掉。
- 例外只有一个：模型明确给 `{"ops":[]}` 是有效 noop，记 `organizeCache`、不入队、不耗 attempts；
  空响应和解析失败不许走这条。
- 两套阈值别混：`pending_ops`（整理批）attempts 到 5 由 `markOp` 转 dead；
  实体队列 `src/entity-queue.ts` 的 `MAX_ATTEMPTS` 是 3。都留给人看，别自动清零重来。
- 判死复活只看稳定字段（`reviveDead` 的 `reviveKeys`：batch / ids / items），
  不许把每轮变化的错误摘要拼进键。
- ingest 队列同一口径：`src/ingest-index.ts` 的 attempts 到 5 标 dead，
  `last_error` 只给人看，不参与判死。

## 单条失败隔离

- 写库循环里逐条 try/catch：一条坏了只标这一条 `failOne`，别让一条脏数据把整批回滚。
- 只有模型整批报错才整批 `failOne`（现有 `consumeEntityBatch` 就是这个口径）。
- 新增循环照这个写；改老循环时顺手补上，别等下一次事故。

## 进度日志：落地就打

- 每批开始/结束、每条落地各打一行，固定前缀：`[entity-queue]`、`[merge]`、`[sleep]`、`[ingest]`。
- 新增逐条日志统一写成 `[entity-queue] done id=<id> attempts=<n>` 这种形状，
  键值固定，方便 grep；别把整段模型输出当日志标识。
- 收尾汇总单独一行，不许拿汇总代替过程。中途排查时，没日志要当成「没跑」去查，
  不要假设「跑完了只是没打」。

## 统计口径：数事件，不数词

- 压缩次数只认结构化事件（宿主 compaction 事件 / 日志里的固定事件字段）。
  `logs/memory_context_watch.log` 里的「compaction 服务不在」是告警文本，不是一次压缩。
- 报「整理了几批 / 压缩了几次 / 合并了几条」时，把 grep 条件和原始行数一起贴出来；
  拿不准就抽 5 条原始日志人工核对。

## 长任务：不持 setup ctx，跨重载能接管

- 模型句柄每次现取：`src/llm-service.ts` 的 `llmOf(ctx)`，别在 setup 期存 `ctx.llm`。
  重载后旧 ctx 会抛 inactive context；取不到就第一批 fail-loud 中止并写日志，不许静默写「完成」。
- `sleep.state.json` 的 `owner` 用 `sleepOwnerOf()`：`bootId:pid:starttime`。
  启动补判发现 owner 不是自己（`sameSleepOwner` 为假）就当场接管，打
  `[sleep] 接管上一实例的轮 slot=... 旧 owner=...`。
- 只靠 `runningAt` / `heartbeatAt` 判活是给没有 owner 的老 state 留的退路，
  新代码必须写 owner；心跳 60s 一次，连续 3 次没心跳才判死。

## 验收

```bash
cd <repo> && npm test
grep -n "接管上一实例" "$DSH_MEMORY_LOG_DIR/memory_sleep.log" | tail -3
grep -n "attempts+1\|attempts=5\|dead" "$DSH_MEMORY_LOG_DIR/memory_sleep.log" | tail -5
```

改完在报告里写清：改了哪个文件、留了哪条可 grep 的文案、期望看到什么。

## state 文件跟 cwd 走，不跟 storePath 走

`sleep.state.json` / `backup/` / `logs/` 的默认路径来自 `paths.memoryRoot()` / `paths.logDir()`，
也就是 `process.cwd()`（这台机器 unit 的 WorkingDirectory 是 /home/julia，所以落在 /home/julia/memory 和 /home/julia/logs）；
`memory.db` 来自 `storePath`（这台机器上是绝对路径 /home/julia/fatfish/memory）。
核对「这轮到底写没写 state」先 grep 日志里 `[sleep] state 文件=` 那行拿准路径，再看
`[sleep] slot 已标记` 和 `[sleep] state 落地` 两行；别去库目录里同名的那份旧 state 上翻——
2026-11-16 05:00 那轮跑完被误判成「没写 state」，就是翻到了 /home/julia/fatfish/memory 里的旧文件。
