/**
 * 记忆的纯类型/日期/实体辅助函数：不碰库、不碰状态，任何模块都能引。
 *
 * store 层的共享形状也收在这里：SQL 行（Row）、内存记录（MemoryRecord）、
 * state（StoreState）、写入闸结果与粗实体命中，别在各自文件里另造一套。
 */
import type { DatabaseSync } from 'node:sqlite';

/** SQL 行：列名 → 值。store 层只有这一份行类型，读库先落到这里再收窄。 */
export type Row = Record<string, unknown>;

/** memories 表的一行（recordToRow 的产物，写库参数只认这三种标量）。 */
export interface MemoryRow {
  [column: string]: string | number | null;
}

/** 内存里的记忆条目：jsonl 那 15 个 key + extra 展开的动态键。 */
export interface MemoryRecord {
  id: string;
  text: string;
  tags?: string[];
  source?: string;
  ts?: string;
  type?: string;
  entities?: string[];
  validFrom?: string;
  supersedes?: string[];
  updatedAt?: string;
  status?: string;
  reason?: string;
  supersededBy?: string | null;
  validTo?: string;
  confidence?: number;
  [key: string]: unknown;
}

/** 正常落库的记录；写入闸判 DUPLICATE 时额外带 discarded/verdict/matchedId 三个标记。 */
export interface SavedRecord extends MemoryRecord {
  discarded?: boolean;
  verdict?: string;
  matchedId?: string | null;
}

/** 被写入闸丢掉的那条：没落库，只给调用方回执用。 */
export interface DiscardedRecord {
  id: string | null;
  text: string;
  tags: string[];
  type: string;
  discarded: true;
  verdict: string;
  matchedId: string | null;
  reason: string;
  [key: string]: unknown;
}

/** 单条记忆的对外视图（listAll / 快照共用一份）。 */
export interface MemoryView {
  id: string;
  text: string;
  tags: string[];
  type: string;
  confidence: number | null;
  entities: string[];
  validFrom: string;
  validTo: string;
  ts?: string;
  status: string;
  reason: string;
}

/** entityView 的一条：某实体的挂载视图。 */
export interface EntityViewItem {
  id: string;
  type: string;
  text: string;
  confidence?: number;
  date: string;
}

/** memory_entity 工具的返回：按实体聚合的计数与条目。 */
export interface EntityView {
  name: string;
  total: number;
  counts: { world: number; experience: number; opinion: number; observation: number };
  avgConfidence: number | null;
  items: EntityViewItem[];
}

/** 写入前的字段补丁（updateMemory 的第三个参数）。 */
export interface MemoryPatch {
  text?: unknown;
  tags?: unknown;
  type?: string;
  confidence?: unknown;
  entities?: unknown;
  validFrom?: unknown;
  validTo?: unknown;
  supersedes?: unknown;
  supersededBy?: string | null;
  status?: string;
  reason?: string;
  sourceFile?: string;
  __lintChecked?: boolean;
  __lintReview?: LintReview | null;
  [key: string]: unknown;
}

/** 写库时携带的 meta（save / merge 的入参，工具与内部路径共用）。 */
export interface RecordMeta {
  type?: string;
  tags?: string[];
  confidence?: number;
  validFrom?: unknown;
  validTo?: unknown;
  entities?: unknown;
  supersedes?: unknown;
  sourceFile?: string;
  __lintChecked?: boolean;
  __lintReview?: LintReview | null;
  __vector?: ArrayLike<number> | null;
  __gateChecked?: boolean;
  __archive?: unknown;
  /** 只排除出写入闸召回、不建立取代关系（拆分子段用：原条不许把子段判成 DUPLICATE）。 */
  __gateExclude?: unknown;
  /** 新条落库时的初始状态；不给就是 active。拆分子段用它写中间态 pending_split。 */
  status?: string;
  /** 拆分子段的组标记（source/expected/index），落进 extra，整组收尾时按它核对。 */
  __splitGroup?: { source: string; expected: number; index: number };
  [key: string]: unknown;
}

/** 复核 agent 的裁决：keep 是误杀放行，其余照旧拒绝。 */
export interface LintReview {
  verdict?: string;
  reason?: string;
}

/** 候选来源：字符串标签，或带 session/id 的来源对象。 */
export interface CandidateSource {
  session?: string;
  id?: string;
}

/** 粗实体命中（entity-coarse 的 match 返回项；id 是 entities.id，运行时就是数字）。 */
export interface CoarseHit {
  id: number;
  norm: string;
  display?: string;
  etype?: string;
  at?: number;
}

/** 向量索引的最小面：store 只用到这几件事，句柄由 memvec 给。 */
export interface VectorHit {
  id: string;
  similarity: number;
}

export interface MemVectorsLike {
  dim?: number;
  insert(id: string, vec: Float32Array): unknown;
  replace(id: string, vec: Float32Array): unknown;
  knn(query: Float32Array | number[], k: number, opts?: { allow?: string[] }): VectorHit[];
  viewAll(): Map<string, Float32Array>;
  checkpoint(): void;
  backupTo(dstPath: string): Promise<unknown>;
}

/** 粗实体匹配器的最小面（entity-coarse 的 createCoarseMatcher 产物）。 */
export interface CoarseMatcherLike {
  match(text: string): CoarseHit[];
  stats(): {
    words: number;
    matches?: number;
    avgMs: number;
    activeCount?: number;
    builtAt?: number;
    maxWords?: number;
    [key: string]: unknown;
  };
}

/** llm 句柄：只保证可能有 stream，其余属性原样透传。 */
export interface StoreLlm {
  stream?: unknown;
  [key: string]: unknown;
}

/** 检索用 df 缓存：跟着 snapshotVersion 走。 */
export interface DfCache {
  version: number;
  totalActive: number;
  df: Map<string, number>;
}

/** 写入闸的裁决结果。 */
export interface WriteGateResult {
  verdict: string;
  matchedId?: string | null;
  reason?: string;
  source?: string;
}

/** dedupe agent 的失败诊断（只用于日志与落账文案）。 */
export interface DedupeDiagnostics {
  attempts?: Array<{ source?: unknown; rawHead?: unknown; error?: unknown }>;
  lastError?: unknown;
}

/** 活跃快照（active-snapshot 的产物）。 */
export interface SnapshotView {
  list: MemoryView[];
  byId: Map<string, MemoryView>;
  vecOf: Map<string, Float32Array>;
  version: number;
}

export interface SnapshotSource {
  get(): SnapshotView;
}

/** 插件配置：已知字段给类型，其余照旧透传（上层配置项还会加）。 */
export interface StoreConfig {
  storePath?: string;
  rrfK?: number;
  apiUrl?: string;
  timeoutMs?: number;
  embed?: (texts: string[]) => Promise<number[][]>;
  provider?: string;
  model?: string;
  splitMaxChars?: number;
  storeLogFile?: string;
  recallLogFile?: string;
  topK?: number;
  minScore?: number;
  relativeGap?: number;
  opinionAlpha?: number;
  rankWeightEntity?: number;
  logger?: unknown;
  entityQueue?: {
    enqueue?: boolean;
    genericDf?: number;
    genericDfMin?: number;
    coarseMaxWords?: number;
    coarseBudgetMs?: number;
    concurrency?: number;
  };
  pipeline?: {
    textLint?: boolean;
    writeGate?: boolean;
    writeGateThreshold?: number;
  };
  [key: string]: unknown;
}

/** 构造期还没装方法的那部分 state（installDb / installRuntime 之后才是完整 StoreState）。 */
export type StoreStateBase = Omit<StoreState, 'connect' | 'tableExists' | 'writeTx' | 'currentLlm' | 'embedTexts' | 'enqueueEntityLater' | 'attachLlm' | 'flushEntityQueue'>;

/** store 的共享状态：各模块只拿这个对象，不另开库、不另存副本。 */
export interface StoreState {
  cfg: StoreConfig;
  dir: string;
  dbPath: string;
  jsonlSource: string;
  rrfK: number;
  db: DatabaseSync | null;
  metas: MemoryRecord[];
  snapshotVersion: number;
  gateCalls: number;
  gateLlm: number;
  maxRowSeq: number;
  rowSeqById: Map<string, number>;
  dim: number;
  vecDb: MemVectorsLike | null;
  coarseMatcher: CoarseMatcherLike | null;
  pendingEnqueues: Promise<void>[];
  tokenCache: WeakMap<MemoryRecord, string[]>;
  tokenBuilds: number;
  dfBuilds: number;
  dfCache: DfCache | null;
  llmClient: StoreLlm | null;
  llmGetter: (() => StoreLlm | null) | null;
  connect(): DatabaseSync;
  tableExists(name: string): boolean;
  writeTx<T>(label: string, fn: (d: DatabaseSync) => T): T;
  currentLlm(): StoreLlm | null;
  embedTexts(texts: string[]): Promise<number[][]>;
  enqueueEntityLater(memoryId: string): void;
  attachLlm(llm: StoreLlm | (() => StoreLlm | null) | null | undefined): boolean;
  flushEntityQueue(): Promise<void[]>;
}

export const TYPES = ['world', 'experience', 'opinion', 'observation'];
export const ALIAS: Record<string, string> = {
  fact: 'world',
  facts: 'world',
  env: 'world',
  environment: 'world',
  exp: 'experience',
  action: 'experience',
  belief: 'opinion',
  preference: 'opinion',
  obs: 'observation',
  entity: 'observation',
};

export function inferType(tags: unknown, text: unknown): string {
  for (const t of (tags as unknown[]) || []) {
    const v = ALIAS[String(t).toLowerCase()] || String(t).toLowerCase();
    if (TYPES.includes(v)) return v;
  }
  const s = String(text || '');
  if (/^我[^，。]{0,6}(做|装|跑|查|改|修|写|试|删|重|部署|看过|读了)/.test(s)) return 'experience';
  if (/^(我认为|我觉得|我猜|我倾向|对方偏好|对方\s*(喜欢|讨厌|更愿意|偏好))/.test(s)) return 'opinion';
  return 'world';
}

export function normalizeType(value: unknown, tags: unknown, text: unknown): string {
  const v = String(value || '').toLowerCase().trim();
  if (TYPES.includes(v)) return v;
  if (ALIAS[v]) return ALIAS[v];
  return inferType(tags, text);
}

export function clamp01(value: unknown, fallback: number): number {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.min(1, Math.max(0, n));
}

export function dateOnly(value: unknown): string {
  const s = String(value || '').trim();
  const m = s.match(/(20\d{2})[-/.](\d{1,2})(?:[-/.](\d{1,2}))?/);
  if (!m) return '';
  return `${m[1]}-${String(m[2]).padStart(2, '0')}-${String(m[3] || '1').padStart(2, '0')}`;
}

/**
 * 正文头部的日期。
 *
 * memory_save 的 validFrom 参数删掉之后，「这条事实从哪天开始成立」只能从正文头反抽：
 * 以前 939 次调用里有 36 次靠这个参数做多天回溯（docs/dsh-memory-save-queue-2026.md §1.3），
 * 删参数不能把这能力一起删掉。认 20xx-xx-xx / 20xx年x月x日 / 20xx/x/x 三种写法，
 * 只扫前 40 个字（头部日期是「正文开头带绝对日期」这条规矩的写法，不在开头的不算）。
 */
export function dateFromHead(text: unknown): string {
  const head = String(text || '').slice(0, 40);
  const m = head.match(/(20\d{2})\s*[-/.年]\s*(\d{1,2})(?:\s*[-/.月]\s*(\d{1,2}))?/);
  if (!m) return '';
  return dateOnly(m[0]);
}

export function entityList(value: unknown): string[] {
  if (Array.isArray(value)) return value.map((s) => String(s).trim()).filter(Boolean);
  return String(value || '')
    .split(/[,，、;；]/)
    .map((s) => s.trim())
    .filter(Boolean);
}
