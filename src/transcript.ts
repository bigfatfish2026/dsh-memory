/** 会话记录抽取：把 DSH 会话日志（zstd jsonl）变成可读的对话文本。无外部依赖，方便单独测试。 */
import fs from 'node:fs';
import path from 'node:path';
import zlib from 'node:zlib';

/** 会话事件行：只保证 time 是数字，data 里的字段按需读。 */
export interface SessionEvent {
  time?: number;
  type?: string;
  data?: {
    source?: { kind?: unknown } | null;
    message?: { content?: unknown } | null;
    content?: unknown;
    [key: string]: unknown;
  } | null;
  [key: string]: unknown;
}

/** 会话抽取配置（sessionsRoot / 两个字数是必填项，其余字段不读）。 */
export interface TranscriptConfig {
  sessionsRoot: string;
  maxMessageChars: number;
  maxTranscriptChars: number;
  [key: string]: unknown;
}

/** 一段事件的抽取结果；lastEventTs 覆盖全部事件（ack 靠它）。 */
export interface TranscriptSlice {
  lines: string[];
  messages: number;
  lastEventTs: number;
}

/** 整个会话目录的抽取结果。 */
export interface CollectedTranscript {
  transcript: string;
  messages: number;
  files: number;
}

/** parseActions 的返回面：四类 ops 原文，元素未校验。 */
export interface ParsedActions {
  add: unknown[];
  merge: unknown[];
  update: unknown[];
  archive: unknown[];
}

export function textOf(content: unknown): string {
  if (!Array.isArray(content)) return '';
  return content
    .filter((p) => p && p.type === 'text' && typeof p.text === 'string')
    .map((p) => p.text)
    .join('\n')
    .trim();
}

/** 会话文件名口径（session*.jsonl / session*.jsonl.zstd），扫描和 collectTranscript 共用一份。 */
export const SESSION_FILE_RE = /^session.*\.jsonl(\.zstd)?$/;

export function isSessionFile(name: unknown): boolean {
  return SESSION_FILE_RE.test(String(name || ''));
}

/**
 * 读一个会话文件并按行返回文本；读不出（文件不在、zstd 坏包）返回 null。
 * zstd 整包解压，调用方一轮只读一次，别重复解。
 */
export function readSessionLines(file: string): string[] | null {
  try {
    const buf = fs.readFileSync(file);
    const raw = file.endsWith('.zstd') ? zlib.zstdDecompressSync(buf) : buf;
    return raw.toString('utf8').split('\n');
  } catch {
    return null;
  }
}

/**
 * 把一个会话文件解析成事件数组（只留带数字 time 的合法 JSON 行，坏行跳过）。
 * 读不出返回 null；空文件返回 []。
 */
export function readSessionEvents(file: string): SessionEvent[] | null {
  const lines = readSessionLines(file);
  if (!lines) return null;
  const out: SessionEvent[] = [];
  for (const line of lines) {
    if (!line.trim()) continue;
    let ev: SessionEvent | null;
    try {
      ev = JSON.parse(line);
    } catch {
      continue;
    }
    if (!ev || typeof ev !== 'object' || typeof ev.time !== 'number') continue;
    out.push(ev);
  }
  return out;
}

/** 文件最后事件时间（毫秒）；没有事件返回 0。ingest_files 的判定和 ack 都用它。 */
export function lastEventTsOf(events: SessionEvent[] | null | undefined): number {
  let last = 0;
  if (!Array.isArray(events)) return last;
  for (const ev of events) {
    if (ev && typeof ev.time === 'number' && ev.time > last) last = ev.time;
  }
  return last;
}

/**
 * 事件 → 对话行（只留真正的人机对话，跳过运行时上下文、后台回填、系统注入）。
 * 2026-11-05 从 collectTranscript 里抽出来：按文件入库的新路径也要同一套抽取规则，
 * 抽取逻辑只许有一份。lastEventTs 覆盖全部事件（不只 since 之后的），ack 靠它。
 */
export function eventsToTranscript(cfg: TranscriptConfig, events: SessionEvent[], since: number): TranscriptSlice {
  const lines: string[] = [];
  let lastEventTs = 0;
  for (const ev of events || []) {
    if (!ev || typeof ev.time !== 'number') continue;
    if (ev.time > lastEventTs) lastEventTs = ev.time;
    if (ev.time <= since) continue;
    let who = '';
    let text = '';
    if (ev.type === 'user/message') {
      // 只留真正由人发来的消息：运行时上下文、后台任务回填、系统注入都不是对话
      const kind = ev.data?.source?.kind;
      if (kind && kind !== 'user') continue;
      who = '用户';
      text = textOf(ev.data?.content);
      if (/^Current runtime context/.test(text)) continue;
    } else if (ev.type === 'assistant/message') {
      who = '我';
      text = textOf(ev.data?.message?.content);
    } else {
      continue;
    }
    if (!text) continue;
    if (text.length > cfg.maxMessageChars) text = `${text.slice(0, cfg.maxMessageChars)}…`;
    const t = new Date(ev.time).toLocaleString('zh-CN', { hour12: false });
    lines.push(`[${t}] ${who}: ${text}`);
  }
  return { lines, messages: lines.length, lastEventTs };
}

/**
 * 会话根目录由调用方传进来（cfg.sessionsRoot，默认见 src/index.ts），本函数自己拼的
 * 只有它下面的子目录名，不碰 ~/.dsh/sessions 这个绝对路径。
 * 宿主暂无会话接口，等 session/reader，见 docs；那时这里换成 ctx.get('sessions') 取内容。
 */
export function collectTranscript(cfg: TranscriptConfig, since: number): CollectedTranscript {
  const out: string[] = [];
  let files: string[] = [];
  try {
    files = fs.readdirSync(cfg.sessionsRoot);
  } catch {
    return { transcript: '', messages: 0, files: 0 };
  }
  const logs: Array<{ file: string; mtime: number }> = [];
  for (const dirName of files) {
    const dir = path.join(cfg.sessionsRoot, dirName);
    let entries: string[] = [];
    try {
      entries = fs.readdirSync(dir);
    } catch {
      continue;
    }
    for (const entry of entries) {
      const sessionDir = path.join(dir, entry);
      let inner: string[] = [];
      try {
        inner = fs.readdirSync(sessionDir);
      } catch {
        continue;
      }
      for (const f of inner) {
        if (!isSessionFile(f)) continue;
        const full = path.join(sessionDir, f);
        let st: fs.Stats;
        try {
          st = fs.statSync(full);
        } catch {
          continue;
        }
        if (st.mtimeMs < since - 60000) continue;
        logs.push({ file: full, mtime: st.mtimeMs });
      }
    }
  }
  logs.sort((a, b) => a.mtime - b.mtime);
  // 2026-11-14：删掉「只看末 maxSessions 个文件」的单轮文件上限，since 之后的全读。
  for (const item of logs) {
    const events = readSessionEvents(item.file);
    if (!events) continue;
    out.push(...eventsToTranscript(cfg, events, since).lines);
  }
  let transcript = out.join('\n');
  if (transcript.length > cfg.maxTranscriptChars) {
    transcript = `……（更早的省略）\n${transcript.slice(-cfg.maxTranscriptChars)}`;
  }
  return { transcript, messages: out.length, files: logs.length };
}

export function parseActions(text: unknown): ParsedActions | null {
  const raw = String(text || '').trim();
  const candidates: string[] = [];
  const fenced = raw.match(/```(?:json)?\s*([\s\S]*?)```/);
  if (fenced) candidates.push(fenced[1]);
  candidates.push(raw);
  const braced = raw.match(/\{[\s\S]*\}/);
  if (braced) candidates.push(braced[0]);
  for (const c of candidates) {
    try {
      const obj = JSON.parse(c);
      if (obj && typeof obj === 'object') {
        return {
          add: Array.isArray(obj.add) ? obj.add : [],
          merge: Array.isArray(obj.merge) ? obj.merge : [],
          update: Array.isArray(obj.update) ? obj.update : [],
          archive: Array.isArray(obj.archive) ? obj.archive : [],
        };
      }
    } catch {
      /* 换下一个候选 */
    }
  }
  return null;
}
