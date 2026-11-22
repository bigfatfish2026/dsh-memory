/**
 * 「不可拆」判定缓存（2026-11-22，t-mwriwazy-de37）。
 *
 * 背景：一轮 422 次拆分请求里 267 个判不可拆，其中至少 197 个同一天早上也判过，
 * 两轮备份的正文逐字相同——同一句话问了两遍模型，纯烧 token。
 *
 * 键 = 正文 sha1 + 字数上限 + 拆分规则版本（正文原始字节，不归一化：两条不同长文
 * 不许被归一化成同一个键，跟 split-queue.ts 的 splitQueueKey 一个口径）。
 * 命中就不再问模型；正文变了（哈希变）或规则版本变了（键里的版本段变）自然不命中。
 *
 * 存放：复用 sleep.state.json 的顶层字段 splitVerdictCache（跟 organizeCache 同一套
 * stateStore.load()/patch() 机制），不新造表、不新造文件。每轮由 pipeline-split
 * 在问模型前查、模型判 unavoidable 后记、轮末 flush 一次写回：
 *   - 按 t 保留最近 max 条（默认 500），防止 state 文件无限长；
 *   - 写回时只留当前规则版本的条目，旧版本自动清掉（换规则后旧结论不可信）；
 *   - 写回失败只 warn 一行，不抛：命中效果本轮仍有效，下轮冷启动而已。
 *
 * 不许静默：pipeline-split 每轮都打一行 `[split] verdict-cache hit=… size=… rules=…`，
 * 插件升级把字段读丢时 size=0、规则文本变了时 rules 串变，肉眼能看出来。
 */
import { sha1Hex } from './fingerprint.js';
import { SPLIT_RULES, SPLIT_RULES_VERSION } from './lint-prompt.js';
import type { SleepStateStore } from './sleep-state.js';

/** state 里的字段名（顶层，跟 organizeCache 平级）。 */
export const SPLIT_VERDICT_FIELD = 'splitVerdictCache';
/** 主文件里保留条数上限：一条约 90 字节，500 条约 45KB，state 文件扛得住。 */
export const DEFAULT_SPLIT_VERDICT_MAX = 500;

/**
 * 当前规则版本：人工语义版本 + SPLIT_RULES 文本的 sha1 前 8 位。
 * 文本一改自动变，不需要人记得 +1；SPLIT_RULES_VERSION 留作给人看的语义号。
 */
export const SPLIT_RULE_VERSION = `${SPLIT_RULES_VERSION}:${sha1Hex(SPLIT_RULES, { len: 8 })}`;

/** 一条缓存值：不可拆理由 + 写入时间（t 只用于上限淘汰）。 */
export interface SplitVerdictEntry {
  reason: string;
  t: number;
}

/** 缓存对象对外的面。 */
export interface SplitVerdictCache {
  /** 当前规则版本串（日志用）。 */
  readonly ruleVersion: string;
  /** 保留上限（日志用）。 */
  readonly max: number;
  /** 命中返回条目，不命中返回 null。 */
  hit(text: unknown, maxChars: unknown): SplitVerdictEntry | null;
  /** 记一条不可拆结论（同键覆盖，t 刷新）。 */
  remember(text: unknown, maxChars: unknown, reason: unknown): void;
  /** 本轮新增条数。 */
  added(): number;
  /** 内存里现有条数。 */
  size(): number;
  /** 写回 state，返回写回后条数；失败只记日志，不抛。 */
  flush(): number;
}

/** 建缓存只用 stateStore 这两个方法，测试可以传同形状的桩。 */
export type SplitVerdictStateStore = Pick<SleepStateStore, 'load' | 'patch'>;

/** 缓存键：正文 sha1（全 40 位）| 字数上限 | 规则版本。导出是为了测试能钉住形状。 */
export function splitVerdictKey(text: unknown, maxChars: unknown, ruleVersion = SPLIT_RULE_VERSION): string {
  const len = Math.max(0, Math.floor(Number(maxChars) || 0));
  return `${sha1Hex(String(text ?? ''))}|${len}|${ruleVersion}`;
}

/** 键的第三段是不是当前规则版本：旧版本条目在加载/写回时清掉。 */
function sameRuleVersion(key: string, ruleVersion: string): boolean {
  const at = key.lastIndexOf('|');
  return at > 0 && key.slice(at + 1) === ruleVersion;
}

/** 读 state 里的缓存字段，坏数据（不是对象、值形状不对）一律当空。 */
function readEntries(raw: unknown): Map<string, SplitVerdictEntry> {
  const out = new Map<string, SplitVerdictEntry>();
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return out;
  for (const [key, value] of Object.entries(raw as Record<string, unknown>)) {
    if (!value || typeof value !== 'object' || Array.isArray(value)) continue;
    const v = value as Record<string, unknown>;
    const t = Number(v.t);
    out.set(key, { reason: String(v.reason ?? '').slice(0, 120), t: Number.isFinite(t) ? t : 0 });
  }
  return out;
}

/**
 * 建一个判定缓存句柄。
 *
 * @param stateStore sleep.state.json 的唯一入口（src/sleep/state-access.ts 的 stateOf(cfg)）
 * @param opts.max 保留上限，默认 500
 * @param opts.now 测试注入的时钟
 * @param opts.log 写回失败的告警出口
 */
export function createSplitVerdictCache(
  stateStore: SplitVerdictStateStore,
  { max = DEFAULT_SPLIT_VERDICT_MAX, now = Date.now, log = null }: {
    max?: unknown;
    now?: () => number;
    log?: ((line: string) => void) | null;
  } = {},
): SplitVerdictCache {
  const cap = Math.max(1, Math.floor(Number(max) || DEFAULT_SPLIT_VERDICT_MAX));
  const clock = typeof now === 'function' ? now : Date.now;
  // 加载就丢掉旧规则版本的条目：换规则后旧结论不可信，也顺手防止 state 越积越厚。
  const entries = readEntries(stateStore.load()?.[SPLIT_VERDICT_FIELD]);
  for (const key of [...entries.keys()]) {
    if (!sameRuleVersion(key, SPLIT_RULE_VERSION)) entries.delete(key);
  }
  // 本轮新增的键（只用于日志里的 added=；同键覆盖不重复计数）。
  const addedBy = new Set<string>();

  return {
    ruleVersion: SPLIT_RULE_VERSION,
    max: cap,
    hit(text, maxChars) {
      const entry = entries.get(splitVerdictKey(text, maxChars));
      return entry || null;
    },
    remember(text, maxChars, reason) {
      const key = splitVerdictKey(text, maxChars);
      const prior = entries.get(key);
      const next: SplitVerdictEntry = { reason: String(reason ?? 'unavoidable').slice(0, 120), t: clock() };
      entries.set(key, next);
      if (prior) addedBy.delete(key);
      else addedBy.add(key);
    },
    added: () => addedBy.size,
    size: () => entries.size,
    flush() {
      const kept = [...entries.entries()]
        .sort((a, b) => (Number(b[1].t) || 0) - (Number(a[1].t) || 0))
        .slice(0, cap);
      const next: Record<string, SplitVerdictEntry> = {};
      for (const [key, value] of kept) next[key] = value;
      try {
        stateStore.patch({ [SPLIT_VERDICT_FIELD]: next });
      } catch (error) {
        log?.(
          `[split] WARN 判定缓存写回失败（本轮命中仍有效，下一轮重新积累）：` +
            `${String((error as Error && (error as Error).message) || (error as Error | null))}`,
        );
      }
      return kept.length;
    },
  };
}

export default createSplitVerdictCache;
