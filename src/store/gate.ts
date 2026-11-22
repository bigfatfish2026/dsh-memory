/**
 * 写库前的两道闸：正文 lint 与唯一写入闸。
 *
 * 这一层只做裁决，不落正文；落库动作在 records.js。裁决所需的一切（模型、向量、
 * 候选池）都从 state 现取，别在这里存句柄。
 */
import { sha1Hex } from '../fingerprint.js';
import { recordDiscard } from '../write-gate.js';
import { lintMemoryText, lintRejectReason, lintWarnings } from '../text-lint.js';
import { dedupe } from '../agents/dedupe.js';
import { enqueue, MAX_ATTEMPTS } from '../ingest-index.js';
import { recordAttempt } from '../ledger.js';
import { dateOnly, entityList, normalizeType } from './types.js';
import { logStore } from './log.js';
import type {
  CandidateSource, DedupeDiagnostics, DiscardedRecord, LintReview, MemoryRecord, StoreState,
  WriteGateResult,
} from './types.js';

/** 写入闸的候选（assertWriteAllowed 内部整形后交给 dedupe 的形状）。 */
export interface GateCandidate {
  text: string;
  type: string;
  tags: string[];
  entities: string[];
  source?: string | CandidateSource | null;
  sourceFile?: string;
}

/** assertWriteAllowed 的 item：只认这些键，其余一概不读。 */
export interface WriteGateItem {
  text?: unknown;
  type?: unknown;
  tags?: unknown;
  entities?: unknown;
  source?: unknown;
  sourceFile?: string;
  ids?: unknown;
}

/** assertWriteAllowed 的可选项：预计算向量 / 排除 id / 复核裁决。 */
export interface WriteGateOptions {
  vector?: Float32Array | ArrayLike<number> | null;
  excludeIds?: unknown;
  ids?: unknown;
  lintReview?: LintReview | null;
}

/**
 * 正文 lint 兜底（2026-11-06；2026-11-06 接两级复核）。
 *
 * 这一层永远是**最后一道闸**：任何绕过复核直接写库的路径（consolidate、外部 CLI、replay）
 * 都还在这里过代码 lint，不合格照样整条不入库、落 discards(source_file='lint_reject')。
 *
 * 两级复核的接法（这里只认结果，不自己问模型——store 是同步写路径，不引 LLM）：
 *   ① 代码 lint 照旧标记（lintMemoryText）；
 *   ② 上层 agent 把可疑条目交给复核 agent 判过：判 keep 的条目写库时带
 *      meta.__lintReview = {verdict:'keep', reason}，这里放行并打一条 INFO 日志，
 *      记下「这条是复核放行的误杀」，不静默放水；
 *   ③ 没有 __lintReview 的（复核判 redo 重做后仍不过、或压根没走复核）→ 拒绝 + 留证。
 * `meta.__lintChecked === true` 是插件内部的老口令（mergeMemories → saveMemory），不是复核通道。
 *
 * 开关：cfg.pipeline.textLint === false 时整段跳过（给库外脚本和老测试留口子）。
 *
 * @param {string} action save | update | merge
 * @param {string} text 待入库正文
 * @param {string|object} source 来源（写日志用）
 * @param {{verdict?:string, reason?:string}|null} [review] 复核 agent 的裁决
 * @returns {null} 通过
 * @throws {Error} code=LINT_REJECT，带 violations / text
 */
export function assertTextLint(
  state: StoreState,
  action: string,
  text: string,
  source: string | CandidateSource | null | undefined,
  review: LintReview | null = null,
): null {
  const { cfg } = state;
  if (cfg.pipeline?.textLint === false) return null;
  const result = lintMemoryText(text);
  // 2026-11-20 P0 阶段一：warn 级违规（分句首单字指代等）只记不拒，留一行 INFO 可 grep；
  // 拒绝口径不变——只有 reject 级才会让 result.ok 为 false。
  const warns = lintWarnings(result);
  if (warns.length) {
    logStore(
      cfg, 'INFO', `[lint] warn ${action}`,
      `source=${typeof source === 'string' ? source : (source?.session || source?.id || '-')} ` +
        `n=${warns.length} ${lintRejectReason(warns)} text40=${String(text).slice(0, 40)}`,
    );
  }
  if (result.ok) return null;
  // 复核判 keep（误杀）：代码标记留着，但这一条按复核裁决放行，留一条日志可追
  if (review && review.verdict === 'keep') {
    logStore(
      cfg, 'INFO', `[lint] 复核放行 ${action}`,
      `source=${typeof source === 'string' ? source : (source?.session || source?.id || '-')} ` +
        `n=${result.violations.length} ${lintRejectReason(result.violations)} ` +
        `复核理由=${String(review.reason || '-').slice(0, 120)} ` +
        `text40=${String(text).slice(0, 40)}`,
    );
    return null;
  }
  const reason = lintRejectReason(result.violations);
  logStore(
    cfg, 'WARN', `[lint] reject ${action}`,
    `source=${typeof source === 'string' ? source : (source?.session || source?.id || '-')} ` +
      `n=${result.violations.length} ${reason} text40=${String(text).slice(0, 40)}`,
  );
  try {
    recordDiscard(state.connect(), { text, sourceFile: 'lint_reject' }, { verdict: 'LINT_REJECT', reason });
  } catch (error) {
    logStore(cfg, 'WARN', '[lint] lint_reject 落 discards 失败', String((error && (error as { message?: unknown }).message) || error));
  }
  const error = new Error(`正文 lint 不通过（${action}，${reason}）`) as Error & {
    code?: string;
    violations?: unknown;
    text?: string;
  };
  error.code = 'LINT_REJECT';
  error.violations = result.violations;
  error.text = text;
  throw error;
}

/**
 * 唯一写入闸（2026-11-05）。开关 pipeline.writeGate 打开时，所有落 memories 的
 * 路径（save / update / merge → memory_save、memory_merge、sleep 的 add/update/merge、
 * consolidate、split、外部 CLI 都汇到这三个函数）先过这里：
 *   findCandidates 召回 → DedupeAgent 裁决 → DUPLICATE 丢 / SUPERSEDES 归档旧条 / DISTINCT 放行。
 * 裁决拿不到（模型两次都坏）不降级 DISTINCT：落 pending_ops(kind='dedupe_check') 后抛错，
 * 调用方一个字节都不写。开关关时直接返回 null，行为与旧版逐字节一致。
 *
 * @param {string} action save | update | merge
 * @param {{text:string, type?:string, tags?:string[], entities?:string[]|string, source?:string, sourceFile?:string}} item
 * @param {{vector?:Float32Array|number[]}} [opts] 预计算向量，省一次 embedding
 * @returns {Promise<null|{verdict:string, matchedId:string|null, reason:string, source:string}>}
 */
export async function assertWriteAllowed(
  state: StoreState,
  action: string,
  item: WriteGateItem,
  opts: WriteGateOptions = {},
): Promise<WriteGateResult | null> {
  const { cfg } = state;
  if (!cfg.pipeline?.writeGate) return null;
  state.gateCalls += 1;
  const text = String(item?.text ?? '').trim();
  if (!text) throw new Error('write-gate: 待写正文为空');
  // 现取：getter 抛 LlmUnavailableError 就原样抛（fail-loud），别把「取不到模型」当「没开闸」。
  const llm = state.currentLlm();
  if (!llm || typeof llm.stream !== 'function') {
    const error = new Error(`writeGate 已打开，但 store 没有注入 llm（${action}）`) as Error & { code?: string };
    error.code = 'WRITE_GATE_NO_LLM';
    throw error;
  }
  const candidate: GateCandidate = {
    text,
    type: normalizeType(item.type, item.tags || [], text),
    tags: Array.isArray(item.tags) ? item.tags.map(String) : [],
    entities: entityList(item.entities),
    source: item.source as string | CandidateSource | null | undefined,
    sourceFile: item.sourceFile,
  };
  // 2026-11-06 t-fix-quality：账本要把「这条账本该动的 id」一起带走，
  // 老账只有 action + candidate，重放时 ids 永远缺，10 条 dedupe_check 全躺成 dead。
  const ids = Array.isArray(item?.ids) ? item.ids.map(String).filter(Boolean) : [];
  // 2026-11-06：复核 keep 的结论要跟着未决一起落账，
  // 否则重放时正文一字不差地再撞一次代码 lint，没有 review 就只能落 discards。
  const lintReview = opts.lintReview && typeof opts.lintReview === 'object' ? opts.lintReview : null;
  const diagnostics: DedupeDiagnostics = {};
  const result = await dedupe({
    target: { db: state.connect, vectors: state.vecDb, all: () => state.metas },
    candidate,
    diagnostics,
    llm,
    provider: cfg.provider,
    model: cfg.model,
    // 2026-11-14：不再传 maxTokens（dedupeMaxTokens 配置口子已删）
    threshold: Number(cfg.pipeline?.writeGateThreshold ?? 0.9),
    vector: opts.vector instanceof Float32Array ? opts.vector : (opts.vector ? Float32Array.from(opts.vector) : undefined),
    excludeIds: Array.isArray(opts.excludeIds) ? opts.excludeIds.map(String) : [],
    embed: async (text: string) => (await state.embedTexts([text]))[0],
    log: (line: string) => logStore(cfg, 'INFO', `dedupe ${action}`, line),
  }) as WriteGateResult | null;

  state.gateLlm += Array.isArray(diagnostics.attempts) ? diagnostics.attempts.length : 0;
  if (!result) {
    // 2026-11-05 t-fix-first-run：未决要能复查——带上 payload 来源、哪个通道坏、模型原文前 120 字符
    const last = diagnostics.attempts?.[diagnostics.attempts.length - 1] || {} as { source?: unknown; rawHead?: unknown; error?: unknown };
    const detail =
      `未决原因：source=${last.source || '-'} raw=${String(last.rawHead || '').slice(0, 120)}` +
      ` err=${last.error || diagnostics.lastError || '未知'}`;
    enqueueDedupePending(state, action, candidate, ids, `dedupe_no_verdict ${detail}`.slice(0, 400), lintReview);
    const sourceLabel = typeof candidate.source === 'string'
      ? candidate.source
      : (candidate.source?.session || candidate.source?.id || candidate.sourceFile || '-');
    const error = new Error(
      `写入闸未决（${action}，来源 ${sourceLabel}）：` +
        `模型两次都没给出可解析裁决 [${detail}]，候选已落 pending_ops，未写库`,
    ) as Error & { code?: string };
    error.code = 'WRITE_GATE_PENDING';
    logStore(cfg, 'WARN', `写入闸未决 ${action}`, `${error.message} text60=${text.slice(0, 60)}`);
    throw error;
  }

  if (result.verdict === 'DUPLICATE') {
    recordDiscard(state.connect(), { text, sourceFile: candidate.sourceFile }, result);
  }
  logStore(
    cfg,
    'INFO',
    `[dedupe] verdict=${result.verdict}`,
    `action=${action} matched=${result.matchedId || '-'} source=${result.source} reason=${result.reason || '-'} text40=${text.slice(0, 40)}`,
  );
  return result;
}

/** 裁决未决时落一条 pending_ops，交给 sleep 的重放段（§4.2）。没有表就只记日志，不静默丢。 */
export function enqueueDedupePending(
  state: StoreState,
  action: string,
  candidate: GateCandidate,
  ids: string[],
  lastError: string,
  lintReview: LintReview | null = null,
): boolean {
  const { cfg } = state;
  try {
    if (!state.tableExists('pending_ops')) {
      logStore(cfg, 'WARN', '写入闸未决但没有 pending_ops 表', '先跑 scripts/apply-pipeline-schema.mjs');
      return false;
    }
    // payload 必须自带 ids + 完整 candidate：重放只靠 action 没法知道该动哪条
    const payload = JSON.stringify({
      action,
      ids: ids || [],
      candidate,
      reason: lastError,
      // 复核裁决跟着账走：重放写库时原样回传，别让 keep 过的正文在账里二次判死
      ...(lintReview ? { review: { verdict: lintReview.verdict, reason: lintReview.reason } } : {}),
    });
    const dedupeKey = sha1Hex(payload);
    const now = Date.now();
    const conn = state.connect();
    const prior = conn.prepare("SELECT status, attempts FROM pending_ops WHERE kind = 'dedupe_check' AND dedupe_key = ?")
      .get(dedupeKey);
    enqueue(conn, 'dedupe_check', dedupeKey, payload, {
      now,
      nextTryAt: now + 60000,
      reviveDead: true,
      log: (line: string) => logStore(cfg, 'INFO', 'dedupe 未决入队', line),
    });
    if (prior) {
      // 同一笔未决再撞一次门：只有已入过账的才 attempts+1（首次落账保持 0，
      // 重放段判死看的是「重放又失败了几次」，和 2026-11-06 之前的老行为一致）
      // 记账口径统一走 ledger.recordAttempt（+1、原因截 500）；这里不判死，状态留给重放段。
      const rec = recordAttempt({ attempts: prior.attempts as number, maxAttempts: MAX_ATTEMPTS, error: lastError as unknown as null, now });
      conn.prepare(`UPDATE pending_ops SET attempts = ?, next_try_at = ?, last_error = ?, updated_ts = ?
          WHERE kind = 'dedupe_check' AND dedupe_key = ?`).run(rec.attempts, now + 60000, rec.message, now, dedupeKey);
    }
    return true;
  } catch (error) {
    logStore(cfg, 'WARN', 'pending_ops 入队失败', String((error && (error as { message?: unknown }).message) || error));
    return false;
  }
}

/** 被写入闸判 SUPERSEDES 的旧条：归档进同一笔事务（design §2.3）。 */
export function supersededRow(state: StoreState, id: string, newId: string, ts?: string): MemoryRecord | null {
  const prior = state.metas.find((m) => m.id === id);
  if (!prior) return null;
  return {
    ...prior,
    status: 'archived',
    supersededBy: newId,
    validTo: prior.validTo || dateOnly(ts),
    reason: prior.reason || `superseded by ${newId}（写入闸）`,
    updatedAt: new Date().toISOString(),
  };
}

export function discardedRecord(text: string, tagList: string[], type: string, result: WriteGateResult): DiscardedRecord {
  return {
    id: null,
    text,
    tags: tagList,
    type,
    discarded: true,
    verdict: result.verdict,
    matchedId: result.matchedId || null,
    reason: result.reason || '',
  };
}
