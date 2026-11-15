/**
 * store 的两种日志。
 *
 * 存储层自己的失败路径（迁移、load、写事务、备份）落 memory_store.log，
 * 格式跟 logs/memory_sleep.log / memory_consolidate.log 一致：`[ISO] 级别 内容`。
 * recall 每调一次记一行 JSONL 到 memory_recall.log，只记正文前 60 字。
 * 两处都把自身失败吞掉，日志坏了不许影响主流程。
 */
import fs from 'node:fs';
import { ensureDirFor } from '../shared/fs.js';
import * as paths from '../shared/paths.js';
import type { StoreConfig } from './types.js';

/** 检索日志里要落的那几项；其余字段原样带过去。 */
export interface RecallLogEntry {
  [key: string]: unknown;
}

/** 会话服务的最小面：只用到 sessions.get(id).meta.origin。 */
export interface SessionsLookup {
  get?: (id: string | null) => { meta?: { origin?: unknown } } | undefined;
}

/** tool exec / ctx 里真正读到的字段，其余一概不碰。 */
export interface RecallExecLike {
  agent?: {
    id?: unknown;
    session?: { id?: unknown; header?: { origin?: unknown } };
  } | null;
}

export interface RecallCtxLike {
  get?: (name: string) => SessionsLookup | undefined;
}

const STORE_LOG_DEFAULT = paths.logFile('memory_store.log');

export function logStore(cfg: StoreConfig | null | undefined, level: string, message: string, detail?: unknown): void {
  try {
    const file = (cfg && cfg.storeLogFile) || STORE_LOG_DEFAULT;
    ensureDirFor(file);
    const tail = detail === undefined || detail === null || detail === '' ? '' : ` | ${detail}`;
    fs.appendFileSync(file, `[${new Date().toISOString()}] ${level} ${message}${tail}\n`, 'utf8');
  } catch {
    /* 日志失败不影响主流程 */
  }
}

/* ── 检索日志（2026-11-01）──────────────────────────────────────────────
 * 目的：以后分析「该主动查记忆的那一轮查没查」。每调一次 recall 记一行 JSONL，
 * 带 session，能跟 ~/.dsh/sessions/*.jsonl 按会话对齐。
 * 只记正文前 60 字，绝不写整条记忆。
 */
const RECALL_LOG_MAX_BYTES = 8 * 1024 * 1024;
const RECALL_LOG_DEFAULT = paths.logFile('memory_recall.log');

/** 前 60 字摘要：压掉换行，长的截断 */
export function brief(text: unknown, n: number = 60): string {
  const s = String(text ?? '').replace(/\s+/g, ' ').trim();
  return s.length > n ? `${s.slice(0, n)}…` : s;
}

/**
 * 写一行检索日志。整段包 try/catch：日志坏了也不许影响召回
 * （跟 sleep / split 的日志同一套「失败吞掉」做法）。
 * 超过 8MB 先滚成 .1（只留一份，覆盖旧档），文件不会无限长。
 */
export function logRecall(cfg: StoreConfig | null | undefined, entry: RecallLogEntry): void {
  try {
    const file = (cfg && cfg.recallLogFile) || RECALL_LOG_DEFAULT;
    ensureDirFor(file);
    try {
      if (fs.statSync(file).size > RECALL_LOG_MAX_BYTES) fs.renameSync(file, `${file}.1`);
    } catch {
      /* 文件还不存在，或者滚动失败——直接往下写 */
    }
    fs.appendFileSync(file, `${JSON.stringify(entry)}\n`, 'utf8');
  } catch {
    /* 日志失败不影响主流程 */
  }
}

/**
 * 会话身份：只给检索日志用，拿不到就 null，绝不瞎填。
 * 首选 tool exec 上 agent 的运行时 face（`agent.session`，跟 prefill 用的一样），
 * 退回 agent.id + sessions 服务里的 session header。
 * 2026-11-01：第一版只走 ctx.get('sessions') 实测 origin 拿到 null，故加前面这条路。
 */
export function recallSessionInfo(
  ctx: RecallCtxLike | null | undefined,
  exec: RecallExecLike | null | undefined,
): { session: string | null; origin: string | null } {
  const agent = exec?.agent;
  try {
    const session = agent?.session;
    if (session) {
      return {
        session: String(session.id || '') || null,
        origin: session.header?.origin === 'subagent' ? 'subagent' : 'user',
      };
    }
  } catch {
    /* 往下走回退路径 */
  }
  const id = agent?.id ? String(agent.id) : null;
  try {
    const meta = (typeof ctx?.get === 'function' ? ctx.get('sessions') : undefined)?.get?.(id)?.meta;
    if (meta) return { session: id, origin: meta.origin === 'subagent' ? 'subagent' : 'user' };
  } catch {
    /* 服务没挂上也可能是正常的 */
  }
  return { session: id, origin: null };
}
