/**
 * EntityAgent（2026-11-13，契约见 docs/dsh-memory-save-queue-2026.md §2.4.2；
 * 2026-11-13 追加取代判定）。
 *
 * 输入一批记忆（默认 8 条，每条带 id、正文，可能带 candidates：同实体的最近 3 条旧记忆），
 * 输出实体名 + etype + attrs，外加每条记忆跟候选的关系 links。
 * 铁律跟回填脚本 scripts/backfill-entities.mjs 的 agent 段一致：
 *   1. 只许从原文取名字——A1 闸会再核一遍，模型编的当场丢；
 *   2. 型号/版本进 attrs.model / attrs.version，实体名只留基础名；
 *   3. etype 只能取十四类（2026-11-14 起与 E_TYPES 对齐），判不出给 unknown，
 *      由消费端落到 entity_issue，不进 memory_entity。
 * 取代判定只信 candidates 里的 id：模型给别的 id，消费端一律丢掉。
 *
 * 温度 0；不传 maxTokens（2026-11-14 起配置口子也删了），由 provider 封顶。
 * 思考档位默认不传：要么配置显式给 entityQueue.reasoningEffort，
 * 要么整字段不出现。这里不认任何写死的档位——magpie/group/ds 连 'off' 都是不支持的。
 */
import { maxTokensOption } from '../llm-params.js';
import type { LlmServiceLike } from '../llm-service.js';

/** 流式 chunk 的最小面：这里只读 type / text / reason.failure。 */
export interface EntityStreamChunk {
  type?: string;
  text?: string;
  reason?: { kind?: string; failure?: { code?: string } & Record<string, unknown> };
  [key: string]: unknown;
}

/** 模型能力查询的返回面（dsh-llm 的 resolveModelInfo）；只读 reasoning.efforts。 */
export interface EntityModelInfo {
  reasoning?: { efforts?: Array<string | { id?: string }>; defaultEffort?: string };
  [key: string]: unknown;
}

/** 一批记忆里的单条：正文 + 可选的同实体旧记忆（候选）。 */
export interface EntityInput {
  id?: string;
  text?: string;
  candidates?: EntityCandidate[];
}

/** 关系候选：只带 id / 正文 / 时间戳。 */
export interface EntityCandidate {
  id?: string;
  text?: string;
  ts?: string;
}

/** 抽出来的实体：i 是本批下标（1 起），name/etype 已 trim。 */
export interface ExtractedEntity {
  i: number;
  name: string;
  etype: string;
  attrs: Record<string, unknown>;
}

/** 关系判定结果：只认 supersedes / parallel / unrelated。 */
export interface EntityLink {
  i: number;
  rel: string;
  target_memory_id: string;
}

/** extractEntities 的产物：items 是实体，links 是关系；emptyAnswer 区分「明确空」与「结构坏」。 */
export interface ExtractEntitiesResult {
  items: ExtractedEntity[];
  links: EntityLink[];
  /** 只有「模型明确给了空 items」才是 true；0 条入参的早退没有这个键。 */
  emptyAnswer?: boolean;
}

/** 模型输出的宽松解析结果：items / links 都可能是坏结构，交给下游按数组判。 */
export interface ParsedEntityJson {
  items?: unknown;
  links?: unknown;
  [key: string]: unknown;
}

export const ENTITY_SYSTEM = `你是记忆实体抽取员。给你若干条记忆，逐条取出原文里已经出现的实体名，不许编造、不许改写、不许翻译、不许补全。
只输出记忆正文里出现过的字符串；原文没有实体就返回空 items。
实体类型 etype 只能取这十四类，按判据挑最贴近的一个：
- person：有名字的真人或宠物（张三、旺财）；平台上的账号不写这里，写 account。
- account：平台账号（B 站 / GitHub 账号、QQ 号、邮箱、带 @ 的用户名），平台写进 attrs.platform。
- device：本机或手里的硬件设备（树莓派、手机、路由器），型号进 attrs.model。
- host：网络地址，IP、域名、主机名（192.0.2.10、example.com、host.example）。
- repo：单个代码仓库（example-repo、demo-lib），一个仓一个名。
- project：跨多个仓、多台机器的长期工程线（example-platform、demo-suite），不是单仓也不是单机。
- file：具体文件或目录路径。
- service：常驻服务或 systemd 单元（nginx、postgresql）。
- ticket：票号（t-xxxxxxxx-xxxx 这种）。
- tool：软件工具、命令行、库（git、sqlite、node）。
- config：配置项、键名、环境变量名、配置文件里的开关。
- org：公司、团队、社区这类组织。
- concept：抽象概念、方法、指标（去重、温度、余弦）。
- unknown：上面都判不出时才给 unknown；unknown 不算有实体，消费端只记 entity_issue、不落库，别拿它兜底。
型号和版本号放进 attrs.model / attrs.version，实体名只留基础名（例：原文「树莓派5」→ name 用「树莓派」，attrs.model 用「Pi5」）。
一条记忆里最多取 4 个实体，泛词（合并、拆分、去重、操作、agent、ask、stop 这类）不要。

另外，有的记忆还带了 candidates：跟这条记忆共享实体、时间最近的几条 active 旧记忆。带 candidates 的那条，顺手判断它跟候选的关系，放进 links：
- supersedes：新记忆是候选里某一条的新版本/新值，旧结论被这条替代（改了配置、换了路径、时间表变了、状态变了）。
- parallel：同实体，但讲的是另一件事，可以并存。
- unrelated：跟候选没有实质关系。
rel 只能取 supersedes / parallel / unrelated 这三个值。
rel=supersedes 时必须给 target_memory_id，而且只能从该条的 candidates 里选，选时间最新、确实被替代的那一条；只有一条能选。
rel 是 parallel 或 unrelated 时 target_memory_id 留空字符串。
没有 candidates 的记忆不参与关系判定，也不要出现在 links 里。
只输出严格 JSON，不要解释：{"items":[{"i":1,"name":"","etype":"","attrs":{}}],"links":[{"i":1,"rel":"supersedes","target_memory_id":""}]}`;

/**
 * 第二阶段：只判关系，不抽实体。
 *
 * 为什么单独一次调用：候选依赖本条记忆自己的 memory_entity 行，而这行只有实体抽取落库后
 * 才存在。把候选塞进抽取那一次调用，就只能在行还没落库时算候选——永远为空。所以关系判定
 * 必须排在实体落库之后，多付一次 round-trip。
 */
export const LINK_SYSTEM = `你是记忆关系判定员。给你若干条记忆，每条带 candidates：跟它共享实体、时间最近的几条 active 旧记忆。只判关系，不抽实体、不改写正文。
- supersedes：这条记忆是候选里某一条的新版本/新值，旧结论被替代（改了配置、换了路径、时间表变了、状态变了）。
- parallel：同实体，但讲的是另一件事，可以并存。
- unrelated：跟候选没有实质关系。
rel 只能取 supersedes / parallel / unrelated 这三个值。
rel=supersedes 时必须给 target_memory_id，而且只能从该条的 candidates 里选，选时间最新、确实被替代的那一条；只有一条能选。
rel 是 parallel 或 unrelated 时 target_memory_id 留空字符串。
没有 candidates 的记忆不参与关系判定，也不要出现在 links 里。
只输出严格 JSON，不要解释：{"links":[{"i":1,"rel":"supersedes","target_memory_id":""}]}`;

/** 宽松 JSON 解析：裸 JSON / ```json 围栏 / 前后混了解释文字。 */
export function parseEntityJson(raw: unknown): ParsedEntityJson | null {
  const source = String(raw ?? '').trim();
  if (!source) return null;
  const candidates = [source];
  const fenced = source.match(/```(?:json)?\s*([\s\S]*?)```/i);
  if (fenced) candidates.push(fenced[1].trim());
  const first = source.indexOf('{');
  const last = source.lastIndexOf('}');
  if (first >= 0 && last > first) candidates.push(source.slice(first, last + 1));
  for (const c of candidates) {
    try {
      const v = JSON.parse(c);
      // 第二阶段的输出只有 links、没有 items，两边都认。
      if (v && typeof v === 'object' && (Array.isArray(v.items) || Array.isArray(v.links))) return v as ParsedEntityJson;
    } catch {
      /* 换下一个候选 */
    }
  }
  return null;
}

const RELS = new Set(['supersedes', 'parallel', 'unrelated']);

/** 可选档位归一：空 / 非字符串 / 'default' 都表示「不传」，不写死任何取值。 */
export function pickReasoningEffort(value: unknown): string | undefined {
  const v = typeof value === 'string' ? value.trim() : '';
  return v && v !== 'default' ? v : undefined;
}

/**
 * 问一次「这个 provider/model 认不认这个档位」。
 * dsh-llm 的 llm.resolveModelInfo(provider, model) 返回 { reasoning?: { efforts:[{id}], defaultEffort } }：
 *   - reasoning === undefined：模型压根没有推理档位，任何显式取值都会被 UNSUPPORTED_REASONING_EFFORT 拒（magpie/group/ds 就是这种）；
 *   - 有 efforts 列表：只在列表里才传；
 *   - 方法不存在 / 查询抛错：拿不到能力，返回 undefined = 「查不到，别拦」，交给 stream 自己判。
 * 拒绝在调用前完成，比让整批失败便宜。
 */
async function resolveSupportedEffort({ llm, provider, model, wanted, log }: {
  llm: LlmServiceLike | null | undefined;
  provider?: string;
  model?: string;
  wanted: string;
  log?: ((line: string) => void) | null;
}): Promise<string | undefined> {
  if (typeof llm?.resolveModelInfo !== 'function') return wanted;
  try {
    const info = await (llm.resolveModelInfo as (p?: string, m?: string) => Promise<EntityModelInfo | null | undefined>)(provider, model);
    const efforts = info?.reasoning?.efforts;
    const ids = Array.isArray(efforts) ? efforts.map((e) => (typeof e === 'string' ? e : e?.id)) : [];
    if (ids.includes(wanted)) return wanted;
    log?.(
      `entity agent: provider "${provider}" model "${model}" 不支持 reasoningEffort="${wanted}"` +
        (ids.length ? `（支持 ${ids.join('/')}）` : '（无推理档位）') +
        '，本次不传',
    );
    return undefined;
  } catch (error) {
    log?.(`entity agent: 查询模型能力失败（${String((error && (error as { message?: unknown }).message) || error)}），不传 reasoningEffort`);
    return wanted;
  }
}

/** 一次请求：effort 为 undefined 时整个 reasoning 字段不出现在请求体里。 */
async function streamEntityText({ llm, provider, model, payload, effort, maxTokens, system = ENTITY_SYSTEM }: {
  llm: LlmServiceLike | null | undefined;
  provider?: string;
  model?: string;
  payload: unknown;
  effort?: string;
  maxTokens?: number;
  system?: string;
}): Promise<string> {
  const stream = llm!.stream({
    provider,
    model,
    system,
    messages: [{ role: 'user', content: [{ type: 'text', text: JSON.stringify(payload) }] }],
    temperature: 0,
    ...(effort === undefined ? {} : { reasoningEffort: effort }),
    ...(maxTokensOption as (maxTokens?: number) => Record<string, never>)(maxTokens),
  }) as AsyncIterable<EntityStreamChunk>;
  let text = '';
  for await (const chunk of stream) {
    if (chunk?.type === 'text-delta') text += chunk.text ?? '';
    else if (chunk?.type === 'finish' && chunk.reason?.kind === 'error') {
      const failure = chunk.reason.failure || {};
      const error = new Error(`entity agent: 模型调用失败：${JSON.stringify(failure)}`) as Error & { code?: string };
      if (failure.code) error.code = failure.code;
      throw error;
    }
  }
  return text;
}

/**
 * 一次模型调用，返回 { items: [{i, name, etype, attrs}], links: [{i, rel, target_memory_id}] }。
 * links 原样收，不判「target 是不是真 active」——那是消费端落库前的事。
 *
 * @param {object} opts
 * @param {object} opts.llm ctx.llm（要有 stream）
 * @param {string} [opts.provider]
 * @param {string} [opts.model]
 * @param {Array<{id:string,text:string,candidates?:Array<{id:string,text:string,ts:string}>}>} opts.items 一批记忆
 * @param {(line:string)=>void} [opts.log]
 * @param {number} [opts.maxTokens=0] 0 = 不传，由 provider 封顶
 * @param {string} [opts.reasoningEffort] 可选思考档位；空 / 'default' = 不传
 */

/** 一次 round-trip：按需传思考档位，被拒就去掉重试一次，再宽松解析 JSON。 */
async function askJson({ system, payload, llm, provider, model, log, maxTokens = 0, reasoningEffort }: {
  system: string;
  payload: unknown;
  llm: LlmServiceLike | null | undefined;
  provider?: string;
  model?: string;
  log?: ((line: string) => void) | null;
  maxTokens?: number;
  reasoningEffort?: string;
}): Promise<ParsedEntityJson> {
  // 这里原来写死 reasoningEffort:'off'，2026-11-13 把整批 8 条打成 attempts+1 / dead。
  // 原因：memory 的路由是 magpie + group/ds，dsh-llm 在 resolveCallWithInfo 里查到该模型没有
  // reasoning 元数据，就抛 UNSUPPORTED_REASONING_EFFORT（provider "magpie" model "group/ds"
  // does not support reasoning effort "off"）。所以代码里不再出现任何写死的档位取值：
  // 配置给了才传，还得先问模型认不认；没给就整个字段不出现，让 provider 用自己的默认。
  const wanted = pickReasoningEffort(reasoningEffort);
  const effort = wanted === undefined ? undefined : await resolveSupportedEffort({ llm, provider, model, wanted, log });
  let text;
  try {
    text = await streamEntityText({ llm, provider, model, payload, effort, maxTokens, system });
  } catch (error) {
    // 兜底：能力查询拿不到（老 llm / 适配器不报元数据）时，第一次仍可能被拒。
    // 这属于配置问题而不是模型问题，去掉档位重试一次，别把一批记忆赔进 dead。
    if (effort === undefined || (error as { code?: string } | null | undefined)?.code !== 'UNSUPPORTED_REASONING_EFFORT') throw error;
    log?.(`entity agent: provider "${provider}" model "${model}" 不认 reasoningEffort="${effort}"，去掉后重试一次`);
    text = await streamEntityText({ llm, provider, model, payload, effort: undefined, maxTokens, system });
  }
  const parsed = parseEntityJson(text);
  if (!parsed) {
    log?.(`entity agent: 解析失败 raw=${JSON.stringify(String(text).slice(0, 120))}`);
    throw new Error('entity agent: 输出不是合法 JSON');
  }
  return parsed;
}

/** target 字段的三种写法：提示词要 target_memory_id，模型常常给 target / dst。 */
const TARGET_KEYS = ['target_memory_id', 'target', 'dst'];

/**
 * links 解析：别名兼容 + 不再静默丢。
 *
 * 原来是一条 filter 了事：字段名写成 dst、rel 写成 replaces、i 对不上批大小，全都无声消失，
 * 日志里只看到「取代边 0」。现在每种丢掉都打一行 WARN 带原始片段，排障时能直接看见模型给了什么。
 *
 * @param {Array<object>} rawLinks 模型输出的 links 原数组
 * @param {object} [opts]
 * @param {number} [opts.itemCount] 本批条数；i 必须在 1..itemCount 内
 * @param {(line:string)=>void} [opts.log]
 * @returns {Array<{i:number, rel:string, target_memory_id:string}>}
 */
export function normalizeLinks(rawLinks: unknown, { itemCount = 0, log = null }: { itemCount?: number; log?: ((line: string) => void) | null } = {}): EntityLink[] {
  const list = Array.isArray(rawLinks) ? rawLinks as Array<Record<string, unknown> | null | undefined> : [];
  const out: EntityLink[] = [];
  for (const it of list) {
    const raw = JSON.stringify(it ?? null).slice(0, 160);
    const i = Number(it?.i);
    const rel = String(it?.rel ?? '').trim().toLowerCase();
    let target = '';
    for (const key of TARGET_KEYS) {
      const v = String(it?.[key] ?? '').trim();
      if (v) {
        target = v;
        break;
      }
    }
    if (!Number.isInteger(i) || i < 1 || (itemCount > 0 && i > itemCount)) {
      log?.(`[WARN] entity agent: link.i=${JSON.stringify(it?.i ?? null)} 不在 1..${itemCount} 内，丢弃 raw=${raw}`);
      continue;
    }
    if (!RELS.has(rel)) {
      log?.(
        `[WARN] entity agent: link.rel=${JSON.stringify(it?.rel ?? null)} 不是 supersedes/parallel/unrelated，丢弃 raw=${raw}`,
      );
      continue;
    }
    if (rel === 'supersedes' && !target) {
      log?.(`[WARN] entity agent: link.rel=supersedes 缺 target（target_memory_id/target/dst 都空），丢弃 raw=${raw}`);
      continue;
    }
    out.push({ i, rel, target_memory_id: target });
  }
  return out;
}

/**
 * 一次模型调用，返回 { items: [{i, name, etype, attrs}], links: [{i, rel, target_memory_id}] }。
 * links 原样收，不判「target 是不是真 active」——那是消费端落库前的事。
 *
 * @param {object} opts
 * @param {object} opts.llm ctx.llm（要有 stream）
 * @param {string} [opts.provider]
 * @param {string} [opts.model]
 * @param {Array<{id:string,text:string,candidates?:Array<{id:string,text:string,ts:string}>}>} opts.items 一批记忆
 * @param {(line:string)=>void} [opts.log]
 * @param {number} [opts.maxTokens=0] 0 = 不传，由 provider 封顶
 * @param {string} [opts.reasoningEffort] 可选思考档位；空 / 'default' = 不传
 */
export async function extractEntities({
  llm,
  provider,
  model,
  items = [],
  log,
  maxTokens = 0,
  reasoningEffort,
}: {
  llm: LlmServiceLike | null | undefined;
  provider?: string;
  model?: string;
  items?: EntityInput[];
  log?: ((line: string) => void) | null;
  maxTokens?: number;
  reasoningEffort?: string;
}): Promise<ExtractEntitiesResult> {
  if (!llm || typeof llm.stream !== 'function') throw new Error('entity agent: llm 不可用');
  if (!items.length) return { items: [], links: [] };
  const payload = {
    items: items.map((m, idx) => {
      const row: { i: number; text: string; candidates?: Array<{ id: string; text: string; ts: string }> } = { i: idx + 1, text: String(m.text || '').slice(0, 400) };
      const cands = Array.isArray(m.candidates) ? m.candidates : [];
      // 没有候选就不带这个键：提示词里写清楚了「没有 candidates 的记忆不参与关系判定」。
      if (cands.length) {
        row.candidates = cands.slice(0, 3).map((c) => ({
          id: String(c.id || ''),
          text: String(c.text || '').slice(0, 120),
          ts: String(c.ts || ''),
        }));
      }
      return row;
    }),
  };
  const parsed = await askJson({ system: ENTITY_SYSTEM, payload, llm, provider, model, log, maxTokens, reasoningEffort });
  const rawItems = parsed.items;
  const rows: ExtractedEntity[] = (Array.isArray(rawItems) ? rawItems : [])
    .map((it: Record<string, unknown> | null | undefined) => ({
      i: Number(it?.i) || 0,
      name: String(it?.name ?? '').trim(),
      etype: String(it?.etype ?? '').trim().toLowerCase(),
      attrs: it?.attrs && typeof it.attrs === 'object' && !Array.isArray(it.attrs) ? it.attrs as Record<string, unknown> : {},
    }))
    .filter((it) => it.i > 0 && it.name);
  const links = normalizeLinks(parsed.links, { itemCount: items.length, log });
  // 空结果的两种：明确给 {"items":[]}（有效回答「这批没有实体」，消费端按 noop 收）
  // 与根本没给 items 键/不是数组（结构坏，消费端按一次失败记账）。别把两者混成一个 []。
  return { items: rows, links, emptyAnswer: Array.isArray(rawItems) && rawItems.length === 0 };
}

/**
 * 第二阶段：只问关系。
 *
 * 调用时机必须晚于本批 memory_entity 落库：候选靠本条自己的实体行反查，提前算就是空。
 * 只把有 candidates 的条目发出去；一条都没有就整个跳过（不产生模型调用）。
 *
 * @param {object} opts 同上，items 里每条带 candidates
 * @returns {Promise<{links:Array<{i:number, rel:string, target_memory_id:string}>}>}
 */
export async function judgeLinks({ llm, provider, model, items = [], log, maxTokens = 0, reasoningEffort }: {
  llm: LlmServiceLike | null | undefined;
  provider?: string;
  model?: string;
  items?: EntityInput[];
  log?: ((line: string) => void) | null;
  maxTokens?: number;
  reasoningEffort?: string;
}): Promise<{ links: EntityLink[] }> {
  if (!llm || typeof llm.stream !== 'function') throw new Error('entity agent: llm 不可用');
  if (!items.length) return { links: [] };
  const payload = {
    items: items
      .map((m, idx) => {
        const cands = Array.isArray(m.candidates) ? m.candidates : [];
        return {
          i: idx + 1,
          text: String(m.text || '').slice(0, 400),
          candidates: cands.slice(0, 3).map((c) => ({
            id: String(c.id || ''),
            text: String(c.text || '').slice(0, 120),
            ts: String(c.ts || ''),
          })),
        };
      })
      .filter((row) => row.candidates.length),
  };
  if (!payload.items.length) return { links: [] };
  const parsed = await askJson({ system: LINK_SYSTEM, payload, llm, provider, model, log, maxTokens, reasoningEffort });
  return { links: normalizeLinks(parsed.links, { itemCount: items.length, log }) };
}
