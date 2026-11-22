/**
 * observation 提炼的提示词与输入构造（2026-11-21，docs/observation-plan-v2.md）。
 *
 * 纯逻辑：不调模型、不读库、不改任何状态。调用方负责选证据（observation-sources.ts
 * 的 selectEntityEvidence）、按 limit 截总条数、把返回的文本当 user message 发出去。
 *
 * 输入里的来源根是硬约束：同一根下的多条证据只算一票，prompt 里必须让人一眼看出来，
 * 否则模型会把同一条记忆拆出来的几片当成多个独立来源。
 */

import type { EntityEvidenceHit } from './observation-sources.js';

/** 单条证据/观察正文进 prompt 的最大字数；超出截断并注明原文长度。 */
export const OBSERVATION_TEXT_LIMIT = 400;

/** observation 提炼的 system prompt。 */
export const OBSERVATION_SYSTEM = `你在给一个实体提炼长期观察：从证据里下结论，不是写日记。

一条观察必须能被至少 2 个互相独立的来源支持。同一来源根下的多条证据（例如同一条记忆拆出来的几段）只算一票；标着「来源不明」的证据不计票。证据不够就什么都不写。

输入给了：该实体的候选证据（每条带 id、时间、正文、来源根），以及该实体已有的观察（带 id 和证据）。只能根据输入里的事实说话，不许补细节、不许猜。

只允许两种动作：
- create：写一条新观察。
- update：改已有观察的正文或证据，用 replaces 填被改的观察 id。

每条 op 必须带 evidence 数组。evidence 只复制本轮「候选证据」栏方括号里的 ID；来源根和已有观察 ID 不填进 evidence。只提炼多份独立证据共同支持、以后还能用来判断的结论。单次事故、临时队列数量、同一原条的多个切片不足以单独生成观察。没有合适结论就输出 {"ops":[]}。

不许编 id，不许输出 archive / delete / merge。

输出严格 JSON，不要解释、不要加代码块：
{"ops":[{"op":"create","text":"一条结论","evidence":["id1","id2"],"confidence":0.8},{"op":"update","text":"修订后的完整结论","evidence":["id3","id4"],"replaces":"观察id"}]}

写法：
- 一条观察讲一个结论，带前提和边界（对谁、在什么条件下成立）；不写过程流水账。
- 不写「今天」「刚才」，时间用输入里的绝对日期。
- 已有观察重复或只是换个说法，不要 create。
- 证据不足 2 个独立来源就输出 {"ops":[]}。`;

/**
 * 复核模式的追加约束（2026-11-21，docs/observation-plan-v2.md §4）。
 *
 * 复核任务只消费已有的 pending_review 观察：模型改写得动就 update，改不动就空 ops 交回
 * 系统按来源重新判定；不许在复核里顺手 create，否则一次证据变更会凭空长出新结论。
 * 拼在 user message 末尾，主提示词 OBSERVATION_SYSTEM 不动。
 */
export const OBSERVATION_REVIEW_NOTE = '本次是复核已有观察：只允许 update 或空 ops；create 一律不收。证据不够就输出 {"ops":[]}，由系统归档。';

/** 已有观察的最小面：正文用来判重，evidenceIds 只用于展示。 */
export interface ObservationExistingRecord {
  id: string;
  text?: string;
  status?: string;
  confidence?: number | null;
  evidenceIds?: string[];
}

/** 调用方已经算好来源根的证据；也可以直接传 EntityEvidenceHit。 */
export interface ObservationEvidenceInput {
  id: string;
  text?: string;
  /** 展示用时间，缺省从 record.validFrom || record.ts 取。 */
  time?: string;
  roots?: string[];
  uncertain?: boolean;
  record?: { text?: unknown; validFrom?: unknown; ts?: unknown };
}

/** 允许两种入参形状：扁平的输入项，或 selectEntityEvidence 的命中项。 */
export type ObservationEvidenceLike = ObservationEvidenceInput | EntityEvidenceHit;

/** buildObservationUserMessage 的入参。 */
export interface BuildObservationUserMessageInput {
  entity: string;
  evidences: Iterable<ObservationEvidenceLike>;
  existingObservations?: Iterable<ObservationExistingRecord>;
  /**
   * 提炼场景：'create'（默认，普通提炼）或 'review'（复核已有观察）。
   * 只影响消息末尾是否追加 OBSERVATION_REVIEW_NOTE，主提示词与证据分组不变。
   */
  mode?: 'create' | 'review';
}

interface NormalizedEvidence {
  id: string;
  text: string;
  time: string;
  roots: string[];
  uncertain: boolean;
}

function stringList(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return [...new Set(value.map((item) => String(item ?? '').trim()).filter(Boolean))];
}

/** 按码点截断，避免把 emoji / 生僻字切成半个。 */
function clipText(text: string, limit: number): string {
  const chars = Array.from(text);
  if (chars.length <= limit) return text;
  return `${chars.slice(0, limit).join('')}…（原文 ${chars.length} 字，已截断）`;
}

function normalizeEvidence(item: ObservationEvidenceLike): NormalizedEvidence {
  const id = String(item?.id ?? '').trim();
  const record = 'record' in item ? item.record : undefined;
  const flat = item as ObservationEvidenceInput;
  const text = typeof flat.text === 'string' ? flat.text : String(record?.text ?? '');
  const time = String(flat.time ?? record?.validFrom ?? record?.ts ?? '');
  const roots = stringList(item?.roots);
  return {
    id,
    text,
    time,
    roots: roots.length ? roots : (id ? [id] : []),
    uncertain: item?.uncertain === true,
  };
}

/** 共享任一来源根的证据并成一组；来历不明的各自单独一组、不参与合并。 */
function groupEvidence(items: NormalizedEvidence[]): NormalizedEvidence[][] {
  const parent = items.map((_item, index) => index);
  const find = (index: number): number => {
    let cur = index;
    while (parent[cur] !== cur) {
      parent[cur] = parent[parent[cur]];
      cur = parent[cur];
    }
    return cur;
  };
  const union = (a: number, b: number): void => {
    const ra = find(a);
    const rb = find(b);
    if (ra !== rb) parent[rb] = ra;
  };

  const rootOwner = new Map<string, number>();
  for (let index = 0; index < items.length; index += 1) {
    const item = items[index];
    if (item.uncertain || !item.roots.length) continue;
    for (const root of item.roots) {
      const owner = rootOwner.get(root);
      if (owner === undefined) rootOwner.set(root, index);
      else union(owner, index);
    }
  }

  const groups = new Map<number, NormalizedEvidence[]>();
  for (let index = 0; index < items.length; index += 1) {
    const item = items[index];
    const key = item.uncertain || !item.roots.length ? -(index + 1) : find(index);
    const list = groups.get(key);
    if (list) list.push(item);
    else groups.set(key, [item]);
  }
  return [...groups.values()];
}

function groupHeader(group: NormalizedEvidence[]): string {
  const roots = [...new Set(group.flatMap((item) => (item.uncertain ? [] : item.roots)))].sort();
  if (!roots.length) return '【来源不明（不计票）】';
  if (group.length > 1) {
    return `【来源根 ${roots.join(' + ')}｜本组 ${group.length} 条，只算一票】`;
  }
  return `【来源根 ${roots.join(' + ')}】`;
}

function existingLine(observation: ObservationExistingRecord): string {
  const parts = [`observation id=${String(observation.id ?? '').trim()}`];
  const status = String(observation.status ?? '').trim();
  if (status) parts.push(`status=${status}`);
  const confidence = observation.confidence;
  if (typeof confidence === 'number' && Number.isFinite(confidence)) parts.push(`conf=${confidence}`);
  const evidence = stringList(observation.evidenceIds);
  parts.push(`证据=${evidence.length ? evidence.join(',') : '未知'}`);
  const text = String(observation.text ?? '').trim();
  return `- ${parts.join(' | ')} | ${text ? clipText(text, OBSERVATION_TEXT_LIMIT) : '（无正文）'}`;
}

/**
 * 拼 observation 提炼的 user message。
 *
 * 证据按来源根分组，同组标「本组 N 条，只算一票」；来历不明的单独列出并注明不计票。
 * 单条正文截 OBSERVATION_TEXT_LIMIT 字并写明原文长度；总条数不在这里截，由调用方 limit 控制。
 * mode='review' 时在末尾追加 OBSERVATION_REVIEW_NOTE（只许 update / 空 ops）。
 */
export function buildObservationUserMessage(input: BuildObservationUserMessageInput): string {
  const entity = String(input.entity ?? '').trim();
  const evidences = [...input.evidences]
    .map(normalizeEvidence)
    .filter((item) => item.id);
  const existing = [...(input.existingObservations ?? [])]
    .filter((item) => String(item?.id ?? '').trim());

  const lines: string[] = [];
  lines.push(`实体：${entity || '（未命名）'}`);
  lines.push('');
  lines.push('候选证据（按来源根分组；同一来源根下只算一票，不同来源根才算独立证据）：');
  if (!evidences.length) {
    lines.push('（无）');
  } else {
    for (const group of groupEvidence(evidences)) {
      lines.push(groupHeader(group));
      for (const item of group) {
        const time = item.time || '-';
        const text = item.text ? clipText(item.text, OBSERVATION_TEXT_LIMIT) : '（无正文）';
        lines.push(`- [${item.id}] 时间=${time} | ${text}`);
      }
    }
  }

  lines.push('');
  lines.push('该实体已有观察：');
  if (!existing.length) lines.push('（无）');
  else for (const observation of existing) lines.push(existingLine(observation));

  lines.push('');
  lines.push('任务：按证据给这个实体输出提炼动作。证据不足 2 个独立来源就输出 {"ops":[]}；只输出 JSON。');
  if (input.mode === 'review') lines.push(OBSERVATION_REVIEW_NOTE);
  return lines.join('\n');
}
