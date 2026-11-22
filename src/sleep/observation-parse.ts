/**
 * observation 提炼输出的解析与硬校验（2026-11-21，docs/observation-plan-v2.md）。
 *
 * 纯逻辑：不调模型、不写库、不改状态。模型输出先过 parseObservationOps（宽松解析、
 * 坏条目单丢），再过 validateObservationOps（逐条硬校验，拒绝给稳定短码）。
 *
 * JSON 容错复用 src/shared/json.ts 的 parseJsonLoose，不另造一份；独立来源计数复用
 * observation-sources.ts 的 independentSourceCount，uncertain 天然不计票。
 */

import { parseJsonLoose } from '../shared/json.js';
import { independentSourceCount } from './observation-sources.js';
import type { SourceRoots } from './observation-sources.js';

/** 允许落地的动作。archive / delete / merge 一律在校验层拒掉。 */
export type ObservationOpKind = 'create' | 'update';

/** 解析后的一条 op；op 保留原字符串，好让校验层给出 OP_NOT_ALLOWED。 */
export interface ParsedObservationOp {
  op: string;
  text: string;
  evidence: string[];
  confidence?: number;
  /** update 的目标观察 id；也接受模型写成 id 字段，两者 parser 都收。 */
  replaces?: string;
  id?: string;
}

/** parseObservationOps 的结果；parseError 是稳定短码，调用方按前缀判。 */
export interface ParseObservationOpsResult {
  ops: ParsedObservationOp[];
  parseError?: string;
}

/** 校验拒绝的稳定短码。 */
export type ObservationRejectReason =
  | 'OP_NOT_ALLOWED'
  | 'TEXT_EMPTY'
  | 'TEXT_TOO_LONG'
  | 'EVIDENCE_EMPTY'
  | 'EVIDENCE_NOT_IN_INPUT'
  | 'INSUFFICIENT_INDEPENDENT'
  | 'DUPLICATE_OBSERVATION'
  | 'DUPLICATE_IN_BATCH'
  | 'UNKNOWN_OBSERVATION';

/** 已有观察的最小面：正文用于判重，evidenceIds 只用于展示。 */
export interface ExistingObservationForValidation {
  id: string;
  text?: string;
  /** active / pending_review 算活观察；空串按 active 口径。 */
  status?: string;
  evidenceIds?: string[];
}

/** validateObservationOps 的上下文。 */
export interface ObservationValidateContext {
  /** 本次实际给模型的证据 id 集合；模型填了集合外的 id 就整条拒。 */
  allowedEvidenceIds: Iterable<string>;
  entityKey?: string;
  entityNorm?: string;
  /** 证据 id → 来源根；缺 id 按 uncertain 处理（independentSourceCount 口径）。 */
  rootsOf: ReadonlyMap<string, SourceRoots>;
  existingObservationIds: Iterable<string>;
  /** 查重复观察用；不给就只查 id 集合、不判正文重复。 */
  existingObservations?: Iterable<ExistingObservationForValidation>;
  /** 观察正文上限，超长直接拒、不截。默认 400。 */
  maxTextLen?: number;
}

/** 通过校验的一条 op。 */
export interface AcceptedObservationOp {
  index: number;
  op: ObservationOpKind;
  text: string;
  evidence: string[];
  confidence?: number;
  replaces?: string;
}

/** 被拒的一条 op。 */
export interface RejectedObservationOp {
  index: number;
  op: string;
  reason: ObservationRejectReason;
  /** 正文前 80 字，只给日志/排障看。 */
  text: string;
}

/** 一批 op 的校验结果。 */
export interface ValidateObservationOpsResult {
  accepted: AcceptedObservationOp[];
  rejected: RejectedObservationOp[];
}

const DEFAULT_MAX_TEXT_LEN = 400;

function codePointLength(text: string): number {
  return Array.from(text).length;
}

function stringField(value: unknown): string {
  return typeof value === 'string' ? value.trim() : '';
}

function evidenceIdsOf(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  const out: string[] = [];
  for (const item of value) {
    if (typeof item !== 'string') continue;
    const id = item.trim();
    if (id) out.push(id);
  }
  return [...new Set(out)];
}

/**
 * 解析模型输出。
 *
 * 容错顺序：parseJsonLoose 剥围栏/配平/取首尾花括号 → 顶层必须是对象且 ops 是数组 →
 * 逐条只收「对象 + op 非空字符串 + text 是字符串」，其余单条丢弃，不拖垮整批。
 * 坏 JSON / 空输出 / ops 不是数组返回 parseError，不抛。
 */
export function parseObservationOps(rawText: unknown): ParseObservationOpsResult {
  const raw = String(rawText ?? '').trim();
  if (!raw) return { ops: [], parseError: 'EMPTY_OUTPUT' };

  let parsed: unknown;
  try {
    parsed = parseJsonLoose(raw);
  } catch {
    return { ops: [], parseError: 'JSON_PARSE_FAILED' };
  }

  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
    return { ops: [], parseError: 'OPS_NOT_OBJECT' };
  }
  const root = parsed as Record<string, unknown>;
  if (!Array.isArray(root.ops)) return { ops: [], parseError: 'OPS_NOT_ARRAY' };

  const ops: ParsedObservationOp[] = [];
  for (const item of root.ops) {
    if (!item || typeof item !== 'object' || Array.isArray(item)) continue;
    const record = item as Record<string, unknown>;
    const op = stringField(record.op);
    if (!op || typeof record.text !== 'string') continue;

    const out: ParsedObservationOp = {
      op,
      text: record.text,
      evidence: evidenceIdsOf(record.evidence),
    };
    if (typeof record.confidence === 'number' && Number.isFinite(record.confidence)) {
      out.confidence = record.confidence;
    }
    const replaces = stringField(record.replaces);
    if (replaces) out.replaces = replaces;
    const id = stringField(record.id);
    if (id) out.id = id;
    ops.push(out);
  }
  return { ops };
}

/** status 的「还活着」口径同 observation-records.ts：active / pending_review，空串按 active。 */
function isLiveObservation(observation: ExistingObservationForValidation): boolean {
  const status = String(observation.status || 'active').toLowerCase();
  return status === 'active' || status === 'pending_review';
}

function reject(
  rejected: RejectedObservationOp[],
  index: number,
  op: string,
  reason: ObservationRejectReason,
  text: string,
): void {
  rejected.push({ index, op, reason, text: text.slice(0, 80) });
}

/**
 * 逐条硬校验。规则与短码：
 *   - op 不是 create/update → OP_NOT_ALLOWED
 *   - text 空 → TEXT_EMPTY；超 maxTextLen → TEXT_TOO_LONG（不截）
 *   - evidence 非数组/空 → EVIDENCE_EMPTY
 *   - evidence 有输入外的 id → EVIDENCE_NOT_IN_INPUT
 *   - 独立来源（uncertain 不计）< 2 → INSUFFICIENT_INDEPENDENT
 *   - update 目标不在 existingObservationIds → UNKNOWN_OBSERVATION
 *   - create 与已有活观察正文完全重复 → DUPLICATE_OBSERVATION
 *   - 同批 create 正文重复 → 第一条通过，后面的 DUPLICATE_IN_BATCH
 *
 * update 的独立来源按本次 op.evidence 计：落库侧新观察只会写本次 evidence 的
 * derived_from 边，不自动继承旧观察的证据；要沿用旧证据，编排层须先并进 evidence 再校验。
 */
export function validateObservationOps(
  ops: Iterable<unknown>,
  ctx: ObservationValidateContext,
): ValidateObservationOpsResult {
  const accepted: AcceptedObservationOp[] = [];
  const rejected: RejectedObservationOp[] = [];
  const allowed = new Set([...ctx.allowedEvidenceIds].map((id) => String(id).trim()).filter(Boolean));
  const existingIds = new Set([...ctx.existingObservationIds].map((id) => String(id).trim()).filter(Boolean));
  const existing = [...(ctx.existingObservations ?? [])].filter((item) => String(item?.id ?? '').trim());
  const duplicateTexts = new Set(
    existing.filter(isLiveObservation).map((item) => String(item.text ?? '').trim()).filter(Boolean),
  );
  const createdTexts = new Set<string>();
  const maxTextLen = Number.isFinite(ctx.maxTextLen) && Number(ctx.maxTextLen) > 0
    ? Math.floor(Number(ctx.maxTextLen))
    : DEFAULT_MAX_TEXT_LEN;

  let index = -1;
  for (const raw of ops) {
    index += 1;
    if (!raw || typeof raw !== 'object' || Array.isArray(raw)) {
      reject(rejected, index, '', 'OP_NOT_ALLOWED', '');
      continue;
    }
    const record = raw as Record<string, unknown>;
    const op = stringField(record.op);
    const text = typeof record.text === 'string' ? record.text.trim() : '';

    if (op !== 'create' && op !== 'update') {
      reject(rejected, index, op, 'OP_NOT_ALLOWED', text);
      continue;
    }
    if (!text) {
      reject(rejected, index, op, 'TEXT_EMPTY', text);
      continue;
    }
    if (codePointLength(text) > maxTextLen) {
      reject(rejected, index, op, 'TEXT_TOO_LONG', text);
      continue;
    }

    const evidence = evidenceIdsOf(record.evidence);
    if (!evidence.length) {
      reject(rejected, index, op, 'EVIDENCE_EMPTY', text);
      continue;
    }
    if (evidence.some((id) => !allowed.has(id))) {
      reject(rejected, index, op, 'EVIDENCE_NOT_IN_INPUT', text);
      continue;
    }

    let target = '';
    if (op === 'update') {
      target = stringField(record.replaces) || stringField(record.id);
      if (!target || !existingIds.has(target)) {
        reject(rejected, index, op, 'UNKNOWN_OBSERVATION', text);
        continue;
      }
    }

    const independence = independentSourceCount(evidence, ctx.rootsOf);
    if (independence.count < 2) {
      reject(rejected, index, op, 'INSUFFICIENT_INDEPENDENT', text);
      continue;
    }

    if (op === 'create') {
      if (duplicateTexts.has(text)) {
        reject(rejected, index, op, 'DUPLICATE_OBSERVATION', text);
        continue;
      }
      if (createdTexts.has(text)) {
        reject(rejected, index, op, 'DUPLICATE_IN_BATCH', text);
        continue;
      }
      createdTexts.add(text);
    }

    const out: AcceptedObservationOp = { index, op, text, evidence };
    if (typeof record.confidence === 'number' && Number.isFinite(record.confidence)) {
      out.confidence = record.confidence;
    }
    if (target) out.replaces = target;
    accepted.push(out);
  }

  return { accepted, rejected };
}
