/**
 * lint_reject 记账。
 *
 * 正文 lint 打回两次仍不合格的条目：不入 memories，原文 + violations 落 discards，
 * verdict='LINT_REJECT'、source_file='lint_reject'，以后能按哈希捞回来重写。
 * 只记账，不改写原文、不截断。
 *
 * 2026-11-06：lint 改两级后，进这里的条目是「复核判 redo + 重做一次仍不合格」，
 * 所以 reason 里还要带上**复核 agent 给的理由**（entry.reviewerReason），
 * violations 由调用方把第一轮和重做那轮的合并传进来。
 */
import type { DatabaseSync } from 'node:sqlite';
import { recordDiscard } from './write-gate.js';
import { lintRejectReason, type LintViolation } from './text-lint.js';

/** 落账目标：裸 sqlite 连接、带 db() 的 store，或带 db() 属性的 store 壳；null 时只记日志。 */
type LintRejectTarget =
  | DatabaseSync
  | { db: (() => DatabaseSync) | DatabaseSync }
  | null
  | undefined;

/** 一条被打回条目：violations 用 text-lint 的同一套结构，reviewerReason 是复核 agent 给的理由。 */
interface LintRejectEntry {
  text?: string;
  violations?: LintViolation[];
  reviewerReason?: string;
  sourceId?: string;
}

/** 落账选项；action 只做日志前缀。 */
interface LintRejectOpts {
  action?: string;
  sourceId?: string;
  reviewerReason?: string;
  log?: (line: string) => void;
}

/** 两轮 violations 合并后 code+hint 会重复，去重再拼，reason 只 500 字额度。 */
function dedupeViolations(violations: unknown): LintViolation[] {
  const seen = new Set<string>();
  const out: LintViolation[] = [];
  for (const v of Array.isArray(violations) ? violations as LintViolation[] : []) {
    const key = `${v?.code}\u0000${v?.hint}\u0000${v?.match}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(v);
  }
  return out;
}

/**
 * @param {object|import('node:sqlite').DatabaseSync|null} target store 或 sqlite 连接；null 时只记日志
 * @param {{text:string, violations:Array}} entry
 * @param {{action?:string, sourceId?:string, log?:(line:string)=>void}} [opts]
 * @returns {boolean} 是否落了账
 */
export function recordLintReject(target: LintRejectTarget, entry: LintRejectEntry | null | undefined, opts: LintRejectOpts = {}): boolean {
  const text = String(entry?.text ?? '');
  const violations = dedupeViolations(entry?.violations);
  const reviewer = String(entry?.reviewerReason ?? opts.reviewerReason ?? '').trim();
  const base = `${opts.action ? `[${opts.action}] ` : ''}${lintRejectReason(violations)}`;
  const reason = `${base}${reviewer ? ` | 复核：${reviewer}` : ''}`.slice(0, 500);
  const label = `lint_reject${opts.sourceId ? ` id=${opts.sourceId}` : ''} n=${
    Array.isArray(entry?.violations) ? entry.violations.length : 0} ${reason}`;
  if (!target) {
    opts.log?.(`[lint] WARN 没有 db，只记日志：${label} text40=${text.slice(0, 40)}`);
    return false;
  }
  try {
    recordDiscard(target, { text, sourceFile: 'lint_reject' }, { verdict: 'LINT_REJECT', reason });
    opts.log?.(`[lint] reject 落账：${label} text40=${text.slice(0, 40)}`);
    return true;
  } catch (error) {
    opts.log?.(`[lint] WARN lint_reject 落账失败（${String((error as Error && (error as Error).message) || error as string)}）：${label}`);
    return false;
  }
}

/** 一次记一批打回的条目。 */
export function recordLintRejects(target: LintRejectTarget, rejected: unknown, opts: LintRejectOpts = {}): { total: number; recorded: number } {
  const list = Array.isArray(rejected) ? rejected as LintRejectEntry[] : [];
  let ok = 0;
  for (const entry of list) {
    if (recordLintReject(target, entry, { ...opts, sourceId: entry?.sourceId || opts.sourceId })) ok += 1;
  }
  return { total: list.length, recorded: ok };
}

export default recordLintReject;
