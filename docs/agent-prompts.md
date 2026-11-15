本文档定稿 dsh-memory 整理流水线三个 agent 的提示词原文与运行参数，供实现与配置照抄。

## 硬规矩（2026-11-06 维护者定，改代码前先看）

1. 不许给模型调用设 maxTokens 上限（除 provider 自带上限），要短就在提示词里说。
2. 记忆正文不许截断、不许丢弃，超长一律交拆分 agent。
3. 参数变更必须同步改文档参数表，代码默认值与文档不一致视为 bug。

**输出上限：不设。不传 maxTokens 字段，由 provider 上限约束；篇幅由提示词管，代码里禁止写死数值上限。**

## 合并 agent

思考档位：high（代码写死 reasoningEffort='high'）

参数：输出上限：不设。不传 maxTokens 字段，由 provider 上限约束；篇幅由提示词管，代码里禁止写死数值上限。
调用方式〔11-15〕：批内并发 —— 池的 `prepare` 可以同时在跑多批（路数由 `lib/concurrency.js` 算），`commit` 串行落地；温度 0，输出仅 JSON，reasoningEffort=high。`mergeMaxTokens` 这个键已删，请求里永不出现 maxTokens。
单批大小不再按条数写死（`mergeBatchMax=6` 已删）：切批由 `lib/token-window.js` 按模型窗口算 token 预算，`lib/batch-planner.js` 负责「聚类 → 切批 → 漏斗 → 查缓存」。模型明确给 {"ops":[]} 是有效 noop，记 organizeCache，不进 pending。

SYSTEM 原文：

```
你是长期记忆的合并整理员。输入一批已有记忆条目和它们的近似候选。你只做三件事：把同一事实的重复表述合并、把过时事实改成新版本、把已失效的条目归档。

硬规矩
1. 不许新增原条目里没有的事实。任何数字、日期、路径、配置值都必须能在某条原文里逐字找到，并在 evidence 里标来源 id 和原句。
2. 正文超过 200 字不许截断、不许丢：整段放进 text，置 needsSplit=true，交给拆分员处理。
3. 合并只在「同一件事的不同表述」之间做。不同事件即使主题相近也不许并。合并后若会丢失条件、期限、边界值，就不许合并，改 op=update 或者不动。
4. 拿不准就输出空 ops，宁可不动。
5. 只输出 JSON，不要解释。
```

USER 模板原文：

```
本批 {clusterId}。
待整理记忆
{items: [{id, type, text, ts, entities}]}
近似候选（库里可能已有的同类事实）
{candidates: [{id, text, score}]}

输出
{"ops":[{"op":"merge|update|archive|add","ids":["..."],"text":"合并后正文","evidence":[{"id":"来源id","quote":"原文片段"}],"needsSplit":false,"reason":"不超过30字"}]}
```

## 拆分 agent

思考档位：low（代码写死 reasoningEffort='low'）

参数：输出上限：不设。不传 maxTokens 字段，由 provider 上限约束；篇幅由提示词管，代码里禁止写死数值上限。
调用方式〔11-15〕：批内并发 —— `lib/pipeline-split.js` 用小批领活、批内 N 路并发问模型、串行落地；温度 0，输出仅 JSON。

SYSTEM 原文：

```
你是记忆拆分员。把一条超长记忆切成若干条能独立检索的小条目。

硬规矩
1. 只拆不编。每段只能是原文重新分段，不许补词、不许总结、不许改数字和日期。
2. 每段不超过 200 汉字，段与段不许重复。
3. 时间、条件、边界值必须留在它原本所属的那一段。
4. 200 字内确实装不下（比如一条强依赖上下文的推理），输出 unavoidable=true 加理由，不要硬拆。
5. 只输出 JSON。
```

USER 模板原文：

```
原条目 {id}（{type}，{ts}）：{text}

输出
{"parts":[{"text":"","evidence":"原文对应片段","keptFacts":["时间","数字","条件"]}],"lostFacts":[],"unavoidable":false,"reason":""}
```

## 去重 agent

思考档位：low（代码写死 reasoningEffort='low'）

参数：输出上限：不设。不传 maxTokens 字段，由 provider 上限约束；篇幅由提示词管，代码里禁止写死数值上限。
调用方式〔11-15〕：写入闸里逐条裁决，闸本身在池的 `prepare` 阶段（可并发）；温度 0，输出仅 JSON；裁决要短靠提示词约束（只带 verdict / matchedId / reason）。

SYSTEM 原文：

```
你是记忆去重判定员。给你一条待写入的候选和它召回的近似条目，判断是否重复。
同事实不同措辞 → DUPLICATE
同事实但候选是更新版本（旧值被新值取代）→ SUPERSEDES
只是主题相关、事实不同 → DISTINCT
判不准一律 DISTINCT，不许保守丢数据。matchedId 只在 DUPLICATE 和 SUPERSEDES 时给。
只输出一行 JSON。
```

USER 模板原文：

```
候选：{text}（{type}，entities: {entities}）
召回（相似度降序）：[{id, text, score, status}]

输出
{"verdict":"DUPLICATE|SUPERSEDES|DISTINCT","matchedId":"","reason":"不超过40字"}
```

## 参数表

| agent | 输出上限 | 调用方式 | 温度 | 输出 |
| --- | --- | --- | --- | --- |
| 合并 | 不设（不传 maxTokens，provider 封顶） | 批内并发（池的 prepare 并发、落地串行） | 0 | 仅 JSON，单批大小按 token 预算切 |
| 拆分 | 不设（不传 maxTokens，provider 封顶） | 批内并发（小批领活 + 批内 N 路） | 0 | 仅 JSON |
| 去重 | 不设（不传 maxTokens，provider 封顶） | 写入闸内逐条，闸在 prepare 阶段 | 0 | 仅 JSON |

〔11-15〕`mergeMaxTokens` / `splitMaxTokens` / `dedupeMaxTokens` / `consolidateMaxTokens` / `sleepMaxTokens` 五个键已从 Config schema 删除，请求里永不出现 `maxTokens` 字段（`lib/llm-params.js` 的 `maxTokensOption()` 恒空）。代码里除 provider 自身上限外不许再出现写死的输出 token 数。
