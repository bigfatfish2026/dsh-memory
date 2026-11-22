/**
 * split-pass —— 把长记忆拆成「一条只说一件事」的原子条目（2026-10-29）
 *
 * 背景：之前那套「把多条合成一条长叙事」被 维护者 否了。单条承载力太大，
 * recall 命中一条等于抱回一堆噪声。改成反向：长条拆短，一条一件事。
 *
 * 流程：
 *   1. 取活跃条目里正文超过 splitMaxChars 的；
 *   2. 逐条调 deepseek-flash，要求按 Hindsight 四类（world/experience/opinion）重判类型后拆开；
 *   3. 校验（条数、单条长度、类型合法）→ 新条目 save（supersedes 指向原条目）→ 原条目 archive；
 *   4. 状态落 split.state.json，同一条不会被拆两次。
 *
 * 硬规矩：
 *   - 原文里的路径、命令、数字、报错、阈值必须落到某一条里，丢掉就算失败；
 *   - 单条失败只记日志，不影响后面的条目；
 *   - 默认 dryRun，出稿给人看；
 *   - 每次落库前 store 自己有快照（sleep 那条链路），这里再每 20 条拍一次完整备份。
 */
import fs from 'node:fs';
import path from 'node:path';
import type { DatabaseSync } from 'node:sqlite';
import { MEMORY_TEXT_MAX } from './text-limit.js';
import * as paths from './shared/paths.js';
import type { LintBatchResult } from './text-lint.js';
import type { MemoryRecord } from './store/types.js';
import { enqueueSplitItem } from './split-queue.js';
import { isAutoObservation } from './memory-link.js';
import { lintItems } from './text-lint.js';
import {
  judgeSuspected, buildLintRedoFeedback, mergeReviewRejects, lintKeptMarker, lintReviewMeta,
} from './lint-review.js';
import { SELF_CONTAINED_RULES, SPLIT_RULES } from './lint-prompt.js';
import { recordLintRejects } from './lint-reject.js';
import { ensureDir, ensureDirFor } from './shared/fs.js';
import {
  PARTIAL_SPLIT_REASON, PENDING_SPLIT_STATUS, pendingSplitIds, splitGroupFromRecords, splitGroupGap,
} from './store/split-group.js';
import type { SplitGroupView } from './store/split-group.js';

const TYPES = ['world', 'experience', 'opinion', 'observation'];

/** 拆出来的一段：normalizeItems 的产物。 */
export interface SplitPiece {
  text: string;
  type: string;
  entities: string[];
  validFrom: string;
  confidence?: number;
  tooLong: boolean;
  [key: string]: unknown;
}

/** 模型流的一块：只认 type/text 与 finish 里的 reason。 */
export interface SplitChunk {
  type?: string;
  text?: string;
  reason?: { kind?: string; failure?: unknown };
}

/** 已拆过的源条：split.state.json 的 value，同一条不会被拆两次。 */
export interface SplitStateEntry {
  ts: number;
  into: Array<string | number>;
  n: number;
}

/** split.state.json 的形状。 */
export interface SplitState {
  ids: Record<string, SplitStateEntry>;
}

/** split-pass 用到的 store 面：快照、写库、归档、备份、db。 */
export interface SplitStore {
  listAll: () => MemoryRecord[];
  metas?: () => MemoryRecord[];
  db?: () => DatabaseSync;
  save: (text: string, tags: string[] | undefined, source: string, extra: Record<string, unknown>) => Promise<{ id?: string | number }>;
  archive: (id: string, reason: string, opts?: { supersededBy?: string; childValidFrom?: string; childTs?: string }) => Promise<unknown>;
  /** 整组转 active（一笔事务）；MemoryStore 有，假 store 可省。 */
  activateSplitGroup?: (ids: string[]) => number | Promise<number>;
  /** 半成品整组归档（一笔事务），reason 固定 partial-split。 */
  archiveSplitGroup?: (ids: string[], reason?: string) => number | Promise<number>;
  /** 假 store 没有批量组接口时的逐条退路。 */
  update?: (id: string, patch?: Record<string, unknown>) => Promise<unknown>;
  backupTo?: (file: string) => Promise<unknown>;
}

/** 阶段三的组键：源列固定写 'split'（历史口径与查询都靠它），组键只能放 extra.splitGroup。 */
export function splitPassGroupKey(sourceId: string | number): string {
  return `split:${String(sourceId || '').trim()}`;
}

async function activateGroupOf(store: SplitStore, ids: string[]): Promise<number> {
  if (!ids.length) return 0;
  if (typeof store.activateSplitGroup === 'function') return Number(await store.activateSplitGroup(ids)) || 0;
  if (typeof store.update === 'function') {
    let n = 0;
    for (const id of ids) {
      await store.update(id, { status: 'active' });
      n += 1;
    }
    return n;
  }
  return 0;
}

async function archiveGroupOf(store: SplitStore, ids: string[], reason: string = PARTIAL_SPLIT_REASON): Promise<number> {
  if (!ids.length) return 0;
  if (typeof store.archiveSplitGroup === 'function') return Number(await store.archiveSplitGroup(ids, reason)) || 0;
  if (typeof store.update === 'function') {
    let n = 0;
    for (const id of ids) {
      await store.update(id, { status: 'archived', reason });
      n += 1;
    }
    return n;
  }
  return 0;
}

/** split-pass 读到的配置面。 */
export interface SplitCfg {
  provider?: string;
  model?: string;
  reasoningEffort?: string;
  splitStateFile?: string;
  splitLogFile?: string;
  splitPreviewFile?: string;
  splitMaxChars?: number;
  splitItemMaxChars?: number;
  splitMaxItems?: number;
  backupDir?: string;
  storePath?: string;
  pipeline?: { textLint?: boolean };
  [key: string]: unknown;
}

/** 调模型只要一个 stream。 */
export interface SplitLlm {
  stream: (req: unknown) => AsyncIterable<unknown>;
}

/** 一条源条拆分的结果：raw 是模型原文，error 是调用失败。 */
export interface AskedResult {
  raw?: string;
  error?: unknown;
}

/** 一轮拆分的入参。 */
export interface SplitPassOpts {
  store: SplitStore;
  llm: SplitLlm;
  cfg?: SplitCfg;
  dryRun?: boolean;
  ids?: Array<string | number>;
  limit?: number;
  maxChars?: number;
  force?: boolean;
  concurrency?: number;
}

/** 一轮拆分的结果。 */
export interface SplitPassResult {
  scanned: number;
  tried: number;
  split: number;
  inserted: number;
  deduped: number;
  archived: number;
  failed: number;
  dryRun: boolean;
  details: Array<Record<string, unknown>>;
}

const SYSTEM = `你在整理长期记忆。给你一条记忆，把它拆成若干条「一条只说一件事」的原子条目。

硬规则：
- 每条 30~200 字，最长不超过 200 字。主谓宾完整，单独拿出来就能看懂，不要以「这/该/它/此/那/上述/上面/这份/本文件/该目录/这个仓库/这个插件」开头，要写具体对象。
- 一条只讲一件事：一个事实、一个决定、一次故障、一条偏好、一个数字。
- 类型从严判（Hindsight 四类，每条只能选一个）：
  world = 外部客观事实：机器状态、配置、文件路径、别人说过的话、某个结论。
  experience = 记录者自己做过的事，第一人称：我装了 X、我把 Y 改成了 Z、我踩到了 W。
  opinion = 判断/口味/押的结论，必须带 confidence（0~1）；使用者的偏好也算 opinion。
  observation 不要用。
- entities 填 2~4 个，用原文里的名字，别造别名。
- validFrom 用原文里的绝对日期（YYYY-MM-DD）；原文没有这个字段就留空字符串。
- 原文里的路径、命令、数字、报错原文、阈值、人名，一个都不能丢，必须落到某一条里。
- 原文本来就是一件事，就只返回一条，不要硬拆。
- 不许新增原文没有的事实，不许把推测写成结论。
- 自称一律写具体的执行者名字或「维护者」，不要写「我」。原文是谁做的事，条目主语就写谁。
- 拆完自查一遍：原文里的每个路径、命令、数字、报错原文、阈值，都必须在某一条里出现过；漏了就补一条，不要为了条数好看丢东西。
- 整条只是过程性状态（本轮、本轮工具不可用、这次失败），返回空 items，不要硬造条目。
- 条目数不设上限，原文塞了多少件事就拆多少条。

输出一个 JSON 对象，不要解释、不要代码块：
{"items":[{"text":"...","type":"world","entities":["..."],"validFrom":"2026-10-29","confidence":0.8}]}

${SELF_CONTAINED_RULES}

${SPLIT_RULES}`;

function readState(file: string): SplitState {
  try {
    return JSON.parse(fs.readFileSync(file, 'utf8'));
  } catch {
    return { ids: {} };
  }
}

function writeState(file: string, state: SplitState): void {
  ensureDirFor(file);
  fs.writeFileSync(file, JSON.stringify(state, null, 2), 'utf8');
}

function appendLog(file: string, line: string): void {
  try {
    ensureDirFor(file);
    fs.appendFileSync(file, `[${new Date().toISOString()}] ${line}\n`, 'utf8');
  } catch {
    /* 日志失败不影响主流程 */
  }
}

function parseItems(raw: unknown): unknown[] | null {
  const text = String(raw || '').trim();
  const start = text.indexOf('{');
  const end = text.lastIndexOf('}');
  if (start < 0 || end <= start) return null;
  try {
    const obj = JSON.parse(text.slice(start, end + 1));
    return Array.isArray(obj?.items) ? obj.items : null;
  } catch {
    return null;
  }
}

/**
 * 抽原文里的「硬信息」：路径、命令、数字、大写标识符。
 * 拆完拿这些反过来查，漏了就是真丢了，不是风格问题。
 */
function keyTokens(text: unknown): string[] {
  const out = new Set<string>();
  for (const m of String(text).matchAll(/[\w./~-]{4,}/g)) {
    const t = m[0].replace(/[.,;:]+$/, '');
    if (!t) continue;
    if (/[/.]/.test(t) && !/^\d+\.\d+$/.test(t)) out.add(t);
    else if (/^[A-Z_]{3,}$/.test(t)) out.add(t);
    else if (/\d{3,}/.test(t)) out.add(t);
  }
  return [...out];
}

const SEP = /[\s、，,；;|/]+/g;
const squash = (t: unknown): string => String(t).replace(SEP, '').toLowerCase();

function coverageGap(sourceText: string, items: SplitPiece[]): { total: number; missing: string[] } {
  const joined = squash(items.map((i) => i.text).join('\n'));
  const toks = keyTokens(sourceText);
  // 不区分分隔符：原文 test/tmp/out 与条目「test、tmp、out」算命中
  const missing = toks.filter((t) => !joined.includes(squash(t)));
  return { total: toks.length, missing };
}

/** 校验 + 清洗模型给的条目（导出只为边界自测，正常走 runSplitPass） */
export function normalizeItems(items: unknown, source: MemoryRecord, cfg: SplitCfg): SplitPiece[] {
  // 2026-11-03：单条上限跟落库闸门对齐到 200（原来是 220）。配置里还写着 220 也不认，
  // 一律以 HARD_MAX 封顶——否则 201~220 字的子条会绕过闸门直接 store.save。
  // 2026-11-06：超长子段不再丢弃（不截断也不消失），打 tooLong 标记带出去，由 runSplitPass
  // 入 pending_ops(kind='split_item') 继续拆。只有源条已不存在时才允许整份作废。
  const maxLen = Math.min(Number(cfg.splitItemMaxChars) || MEMORY_TEXT_MAX, MEMORY_TEXT_MAX);
  const maxItems = Number(cfg.splitMaxItems) || 20;
  const out: SplitPiece[] = [];
  const seen = new Set<string>();
  for (const item of (items as Array<{ text?: unknown; type?: unknown; entities?: unknown; validFrom?: unknown; confidence?: unknown }>).slice(0, maxItems)) {
    const text = String(item?.text || '').replace(/\s+/g, ' ').trim();
    if (text.length < 12) continue;
    if (seen.has(text)) continue;
    seen.add(text);
    const type = TYPES.includes(String(item?.type || '').toLowerCase()) ? String(item.type).toLowerCase() : source.type || 'world';
    const entities = Array.isArray(item?.entities)
      ? (item.entities as unknown[]).map((e) => String(e).trim()).filter(Boolean).slice(0, 6)
      : source.entities;
    const validFrom = String(item?.validFrom || '').match(/20\d{2}-\d{2}-\d{2}/)?.[0] || source.validFrom || '';
    const confidence =
      type === 'opinion'
        ? Math.min(1, Math.max(0, Number(item?.confidence ?? 0.7)))
        : undefined;
    out.push({
      text,
      type,
      entities: entities!.length ? entities as string[] : source.entities as string[],
      validFrom,
      confidence,
      tooLong: text.length > maxLen,
    });
  }
  return out;
}

/** 归一化正文去重（重做那版和第一版可能给出同一段）：先出现的说话。 */
function dedupeNormalized(items: unknown): SplitPiece[] {
  const seen = new Set<string>();
  const out: SplitPiece[] = [];
  for (const item of Array.isArray(items) ? items as SplitPiece[] : []) {
    const key = String(item?.text ?? '').replace(/\s+/g, '');
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push(item);
  }
  return out;
}

async function askSplit(llm: SplitLlm, cfg: SplitCfg, source: MemoryRecord, missing: string[] = [], lintHints = ''): Promise<string> {
  const meta = [
    `原条目：${source.text.length} 字`,
    `类型：${source.type || '未知'}`,
    `日期：${source.validFrom || '未知'}`,
    `实体：${(source.entities || []).join(',') || '无'}`,
  ].join('　');
  const stream = llm.stream({
    provider: cfg.provider,
    model: cfg.model,
    ...(cfg.reasoningEffort && cfg.reasoningEffort !== 'default' ? { reasoningEffort: cfg.reasoningEffort } : {}),
    system: SYSTEM,
    messages: [
      {
        role: 'user',
        content: [
          {
            type: 'text',
            text: `${meta}\n\n${source.text}` +
              (missing.length
                ? `\n\n注意：上一版拆分漏掉了原文里这些硬信息，这一版必须让它们在某个条目里原样出现：${missing.join('　')}`
                : '') +
              (lintHints ? `\n\n上一版这些条目读不懂，按【自足性硬规则】重写它们（不许截断原文）：\n${lintHints}` : ''),
          },
        ],
      },
    ],
    // 2026-11-14：不传 maxTokens（配置口子 splitMaxTokens 已删），由 provider 封顶
  });
  let out = '';
  for await (const chunk of stream as AsyncIterable<SplitChunk>) {
    if (chunk?.type === 'text-delta') out += chunk.text as string;
    else if (chunk?.type === 'finish' && chunk.reason?.kind === 'error') {
      throw new Error(`拆分时模型调用失败：${JSON.stringify(chunk.reason.failure || {})}`);
    }
  }
  return out;
}

/**
 * 跑一轮拆分。
 * @returns {scanned, tried, split, inserted, deduped, archived, saved, failed, dryRun, details}
 *   inserted = 真正入库的子段数；deduped = 被写入闸判重丢掉的子段数（各落一行 discards）；
 *   archived = 归档掉的源条数。三个数给 sleep.js 的汇总行用。
 */
export async function runSplitPass({
  store,
  llm,
  cfg = {},
  dryRun = true,
  ids = [],
  maxChars = 0,
  force = false,
  // 2026-11-14 维护者拍板：拆分段跟全链同一套口径，问模型并发、落地串行。
  concurrency = 1,
}: SplitPassOpts = {} as SplitPassOpts): Promise<SplitPassResult> {
  const stateFile = cfg.splitStateFile || paths.memoryFile('split.state.json');
  const logFile = cfg.splitLogFile || paths.logFile('memory_split.log');
  const previewFile = cfg.splitPreviewFile || paths.memoryFile('split-preview.md');
  const threshold = Number(maxChars) || Number(cfg.splitMaxChars) || 200;
  // 2026-11-14：单轮拆分条数上限（sleepSplitLimit）删了，本轮全部处理。

  const state = readState(stateFile);
  const all = store.listAll().filter((m) => m.status === 'active');
  const wanted = new Set(ids.map(String));
  // 自动观察不进普通拆分（2026-11-22）：跟 sleep 池的入队侧同一口径。
  // 手动 memory_split 也要挡，否则它照样能把自动观察拆出没有证据边的子条。
  const autoObservationSkipped = all.filter((m) => m.text.length > threshold && isAutoObservation(m)).length;
  let targets = all.filter((m) => m.text.length > threshold && !isAutoObservation(m));
  if (autoObservationSkipped) {
    appendLog(logFile, `[split] skip_auto_observation n=${autoObservationSkipped}（自动观察走复核链路）`);
  }
  if (wanted.size) targets = targets.filter((m) => wanted.has(m.id));
  if (!force) targets = targets.filter((m) => !state.ids?.[m.id]);
  targets.sort((a, b) => b.text.length - a.text.length);

  const details: Array<Record<string, unknown>> = [];
  const previews: Array<{ source: MemoryRecord; items: SplitPiece[] }> = [];
  let tried = 0;
  let split = 0;
  let failed = 0;
  let inserted = 0;
  let deduped = 0;
  let archived = 0;

  // 一次问 N 条（N = 调用方给的并发数），然后按原顺序一条条落地：
  // 问模型是慢的那段（单条 19-24 秒），写库只能有一条线。批与批之间先收口再领下一批，
  // 所以同时在途的模型调用最多 concurrency 个，库里不会出现半截结果。
  const poolConc = Math.min(Math.max(Math.floor(Number(concurrency) || 1), 1), 64);
  const askOne = async (source: MemoryRecord): Promise<AskedResult> => {
    try {
      return { raw: await askSplit(llm, cfg, source, []) };
    } catch (error) {
      return { error };
    }
  };
  for (let ci = 0; ci < targets.length; ci += poolConc) {
    const chunk = targets.slice(ci, ci + poolConc);
    const chunkRaw = await Promise.all(chunk.map((source) => askOne(source)));
    for (let j = 0; j < chunk.length; j += 1) {
    const source = chunk[j];
    tried += 1;
    try {
      let normalized: SplitPiece[] = [];
      let gap: { total: number; missing: string[] } = { total: 0, missing: [] };
      let raw = '';
      // 2026-11-14：删掉「最多试 3 次补漏」的固定重试——一次拿不到合法条目就记失败，
      // 下一轮拆分再处理（split.state.json 不推进这一条）。
      {
        if (chunkRaw[j]?.error) throw chunkRaw[j].error;
        raw = String(chunkRaw[j]?.raw ?? '');
        const items = parseItems(raw);
        if (items) {
          const candidate = normalizeItems(items, source, cfg);
          if (candidate.length) {
            normalized = candidate;
            gap = coverageGap(source.text, candidate);
          }
        }
      }
      // 正文 lint 三段：
      // ① 代码 lint 只打标记；② 复核 agent 一次裁决整批（keep = 误杀放行 / redo = 真有问题）；
      // ③ 判 redo 的退回本 agent 重做**一次**，重做后再过代码 lint，仍不过才落 discards。
      if (normalized.length && cfg?.pipeline?.textLint !== false) {
        const firstLint = lintItems(normalized);
        if (firstLint.rejected.length) {
          const logLine = (line: string): void => appendLog(logFile, line);
          const judged = await judgeSuspected({
            cfg,
            llm,
            agent: 'split-pass',
            suspects: firstLint.rejected.map((entry) => ({ ...entry, sourceId: source.id })),
            logger: logLine,
          });
          // keep = 误杀：打上 __lintKept 原样放行，落库时带 __lintChecked 过 store 最后一道闸
          const keepItems = [
            ...firstLint.accepted.map((entry) => entry.item),
            ...judged.keep.map((entry) => ({ ...entry.item, ...lintKeptMarker(entry) })),
          ];
          let retryLint: LintBatchResult<SplitPiece> = { accepted: [], rejected: [] };
          if (judged.redo.length) {
            try {
              const retryRaw = await askSplit(
                llm, cfg, source, gap.missing, buildLintRedoFeedback(judged.redo),
              );
              retryLint = lintItems(normalizeItems(parseItems(retryRaw) || [], source, cfg));
            } catch (error) {
              logLine(`[lint] WARN id=${source.id} 重做调用失败（${String((error as Error && (error as Error).message) || error as string)}）`);
            }
          }
          const retryPassed = retryLint.accepted.map((entry) => entry.item);
          let queueDb = null;
          try { queueDb = typeof store.db === 'function' ? store.db() : null; } catch { queueDb = null; }
          if (judged.redo.length) {
            const stillRejected = retryPassed.length ? retryLint.rejected : judged.redo;
            recordLintRejects(
              queueDb,
              mergeReviewRejects(stillRejected, judged.redo).map((entry) => ({ ...entry, sourceId: source.id })),
              {
                action: 'split-pass',
                sourceId: source.id,
                log: logLine,
              },
            );
            normalized = dedupeNormalized([...retryPassed, ...keepItems]);
            logLine(
              `[lint] id=${source.id} 第一版 ${firstLint.rejected.length} 条可疑（复核放行 ${
                judged.keep.length} 条），重做后通过 ${retryLint.accepted.length} 条、仍拒 ${stillRejected.length} 条`,
            );
          } else {
            normalized = dedupeNormalized(keepItems);
            logLine(`[lint] id=${source.id} 复核放行 ${judged.keep.length} 条误杀，不用重写`);
          }
        }
      }

      if (!normalized.length) {
        failed += 1;
        appendLog(logFile, `拆分失败 id=${source.id} 原因=本次调用没有合法条目 原始=${String(raw).replace(/\s+/g, ' ').slice(0, 160)}`);
        details.push({ id: source.id, action: 'failed', reason: '模型没给出可用条目' });
        continue;
      }

      if (dryRun) {
        previews.push({ source, items: normalized });
        appendLog(
          logFile,
          `演练 id=${source.id} 原${source.text.length}字 → ${normalized.length}条（${normalized.map((x) => x.text.length).join('/')}字）覆盖率 ${gap.total - gap.missing.length}/${gap.total}` +
            (gap.missing.length ? ` 漏：${gap.missing.slice(0, 8).join(' ')}` : ''),
        );
        details.push({
          id: source.id,
          action: 'would-split',
          items: normalized.length,
          preview: normalized[0].text.slice(0, 60),
          gap: gap.missing.length,
          missing: gap.missing.slice(0, 10).join(' '),
        });
        continue;
      }

      const savedIds: Array<string | number> = [];
      /** 本次直写落库的段 id（中间态）：失败/不完整时整组归档成 partial-split。 */
      const groupIds: string[] = [];
      /** 判重承接的段下标：判重没有新行，组完整性靠它补位。 */
      const dedupedIndexes: number[] = [];
      // 阶段三的组键：源列保持 'split'（历史查询都靠它），组键放 extra.splitGroup.source。
      const groupKey = splitPassGroupKey(source.id);
      // 本组应有几段：只在这次直写里落库的短段；超长段进 pending_ops，归 pipeline-split 那一组。
      const localItems = normalized.filter((item) => !(item.tooLong || String(item.text || '').length > MEMORY_TEXT_MAX));
      let enqueued = 0;
      for (const item of normalized) {
        // 超长子段不丢：原样入 pending_ops(kind='split_item') 继续拆，源条保持 active。
        // normalizeItems 已按 splitItemMaxChars 打 tooLong；这里再按 MEMORY_TEXT_MAX 兜一次。
        if (item.tooLong || String(item.text || '').length > MEMORY_TEXT_MAX) {
          let queueDb = null;
          try { queueDb = typeof store.db === 'function' ? store.db() : null; } catch { queueDb = null; }
          // 同一条源可能有多个超长子段：第一条用 source.id（消费端拆完会归档源条），
          // 后面几条带 #partN 后缀单独占键，避免 dedupe_key 相同互相覆盖丢正文。
          const queueSourceId = enqueued === 0 ? source.id : `${source.id}#part${enqueued + 1}`;
          const queued = enqueueSplitItem(queueDb, {
            sourceId: queueSourceId,
            text: item.text,
            tags: source.tags,
            type: item.type,
            op: 'split_part',
            batch: 'split-pass',
            reason: 'part_too_long',
            log: (line) => appendLog(logFile, line),
          });
          if (queued) {
            enqueued += 1;
            appendLog(logFile, `[split] part_too_long enqueued=1 id=${source.id} len=${item.text.length}`);
          } else {
            appendLog(logFile, `[split] WARN part_too_long enqueue_failed id=${source.id} len=${item.text.length}`);
          }
          continue;
        }
        const partIndex = localItems.indexOf(item);
        try {
          const rec = await store.save(item.text, source.tags, 'split', {
            // 复核判 keep（误杀）的条目带 __lintChecked 过 store 最后一道闸，原样入库
            ...lintReviewMeta(item),
            type: item.type,
            entities: item.entities,
            validFrom: item.validFrom,
            confidence: item.confidence,
            // 写入即中间态：同一笔 save 事务落 pending_split，整组齐了才激活（2026-11-22）。
            // 阶段三与 pipeline-split 走同一套口径，别让两条直写路径行为不一致。
            status: PENDING_SPLIT_STATUS,
            __splitGroup: { source: groupKey, expected: localItems.length, index: partIndex },
            // 原条只排除出写入闸召回，不建立取代关系；supersededBy/validTo 由下面的 archive 同一笔事务写
            __gateExclude: [source.id],
          });
          // 2026-11-05：写入闸判 DUPLICATE 时 rec.id 为空，别把 null 塞进状态，也别白归档源条
          if (!rec.id) {
            deduped += 1; // 判重丢掉的子段，写入闸已在 discards 落了一行账
            dedupedIndexes.push(partIndex);
            continue;
          }
          inserted += 1;
          savedIds.push(rec.id);
          groupIds.push(String(rec.id));
        } catch (error) {
          // 单段落库失败：本条源不归档，已落的 pending 段整组归档，别留半截残句在库里
          const cleaned = await archiveGroupOf(store, groupIds);
          appendLog(
            logFile,
            `[split] WARN 子段入库失败 id=${source.id} part=${partIndex}：${(error as Error)?.message || error}；` +
              `已落 ${cleaned} 段归档 reason=${PARTIAL_SPLIT_REASON}`,
          );
          throw error;
        }
      }
      // 组完整性：短段这一组要盖满 localItems；超长段在另一个组里由消费端收尾，不影响这里激活。
      // 组视图里一行都认不出来（假 store 不写 source / 老注入面）时退回本次落库账，不能凭空判不完整。
      const groupView: SplitGroupView | null = typeof store.metas === 'function'
        ? splitGroupFromRecords(store.metas(), groupKey)
        : null;
      const groupGap = groupView && groupView.rows.length
        ? splitGroupGap(groupView, localItems.length, dedupedIndexes)
        : {
          complete: savedIds.length + dedupedIndexes.length >= localItems.length,
          expected: localItems.length,
          have: savedIds.length + dedupedIndexes.length,
          missing: [] as number[],
        };
      const closeIds = groupView && groupView.rows.length ? pendingSplitIds(groupView) : groupIds;
      if (!groupGap.complete) {
        const cleaned = await archiveGroupOf(store, closeIds);
        appendLog(
          logFile,
          `[split] WARN 组不完整 id=${source.id} ${groupGap.have}/${localItems.length}` +
            (groupGap.missing.length ? ` missing=${groupGap.missing.join(',')}` : '') +
            `；半成品 ${cleaned} 段归档 reason=${PARTIAL_SPLIT_REASON}，源条保持 active`,
        );
        details.push({ id: source.id, action: 'error', reason: `split_group_incomplete:${groupGap.have}/${localItems.length}` });
        continue;
      }
      const activated = await activateGroupOf(store, closeIds);
      if (activated) appendLog(logFile, `[split] 组 ${groupKey} 齐 ${localItems.length}/${localItems.length}，激活 ${activated} 段`);
      // 只要还有超长子段在队列里，就不归档源条：等拆分消费端处理完再归档，
      // 避免短段已入库、长段还在队列时原条被提前作废。源条保持 active。
      if (enqueued > 0) {
        appendLog(logFile, `[split] part_too_long source_kept_active id=${source.id} enqueued=${enqueued}`);
        details.push({ id: source.id, action: 'queued', items: savedIds.length, enqueued });
        continue;
      }
      if (!savedIds.length) {
        appendLog(logFile, `写入闸全判重，源条保留 id=${source.id}`);
        continue;
      }
      await store.archive(source.id, `拆成 ${savedIds.length} 条：${savedIds.join(',')}`, {
        supersededBy: String(savedIds[savedIds.length - 1]),
        childValidFrom: String(source.validFrom || ''),
        childTs: String(source.ts || ''),
      });
      archived += 1;
      state.ids[source.id] = { ts: Date.now(), into: savedIds, n: savedIds.length };
      writeState(stateFile, state);
      split += 1;
      appendLog(
        logFile,
        `拆开 id=${source.id} ${source.text.length}字 → ${savedIds.length}条 ${savedIds.join(',')} 覆盖率 ${gap.total - gap.missing.length}/${gap.total}` +
          (gap.missing.length ? ` 漏：${gap.missing.slice(0, 8).join(' ')}` : ''),
      );
      details.push({ id: source.id, action: 'split', items: savedIds.length, into: savedIds.join(',') });

      // 每 20 条拍一次快照
      if (split % 20 === 0) {
        try {
          const stamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
          const dir = cfg.backupDir || paths.memoryFile('backup');
          ensureDir(dir);
          const jsonl = path.join(cfg.storePath!, 'memories.jsonl');
          if (fs.existsSync(jsonl)) fs.copyFileSync(jsonl, path.join(dir, `${stamp}-split-memories.jsonl`));
          if (typeof store?.backupTo === 'function') {
            await store.backupTo(path.join(dir, `${stamp}-split-memory.db`));
          }
          appendLog(logFile, `已拍快照 ${stamp}（拆到第 ${split} 条）`);
        } catch {
          /* 快照失败不挡主流程 */
        }
      }
    } catch (error) {
      failed += 1;
      appendLog(logFile, `拆出错 id=${source.id}：${(error as Error)?.message || error}`);
      details.push({ id: source.id, action: 'error', reason: String((error as Error)?.message || error) });
    }
    }
  }

  // 汇总一行：grep `[split] 并发` 看这轮开几路、条/成功/失败各几个（跟 ingest/consumeSplitOps 同口径）。
  if (targets.length) {
    appendLog(logFile, `[split] 并发 ${Math.min(poolConc, targets.length)} 路 条 ${targets.length} 成功 ${Math.max(0, targets.length - failed)} 失败 ${failed} dryRun=${dryRun}`);
  }

  if (dryRun && previews.length) {
    const md = [
      `# 拆分演练 ${new Date().toLocaleString('zh-CN', { timeZone: 'Asia/Shanghai' })}`,
      '',
      ...previews.map((p) => {
        const lines = [
          `## ${p.source.id}　${p.source.text.length} 字 → ${p.items.length} 条`,
          '',
          `**原文**：${p.source.text}`,
          '',
          ...p.items.map((it, i) => `${i + 1}. [${it.type}${it.confidence !== undefined ? ` ${it.confidence}` : ''}]（${it.text.length}字）${it.text}`),
          '',
        ];
        return lines.join('\n');
      }),
    ].join('\n');
    try {
      ensureDirFor(previewFile);
      fs.writeFileSync(previewFile, md, 'utf8');
    } catch {
      /* 写不出来不影响 */
    }
  }

  appendLog(logFile, `本轮完成 待拆=${targets.length} 实拆=${tried} 成功=${split} 失败=${failed} dryRun=${dryRun}`);
  // scanned 的旧语义是「全库超长待拆条数」，保持不受 ids 过滤影响；只把自动观察剔出去。
  return { scanned: all.filter((m) => m.text.length > threshold && !isAutoObservation(m)).length, tried, split, inserted, deduped, archived, failed, dryRun, details };
}
