/**
 * 睡觉链的只读工具执行器 + 流式工具调用收口。
 */
import fs from 'node:fs';
import path from 'node:path';
import { READ_ROOT, TEXT_EXT, safePath, collectFiles, clip } from '../shared/read-tools.js';
import type { SleepCtx, SleepStore } from './env.js';

// ---------------------------------------------------------------- 只读工具
// 2026-10-30 维护者 的要求：整理记忆时给模型一套「只读但调用自由」的工具。
// 起因是 browser_test 早就改名成 browser_auto，整理时模型没法核对，只能照抄旧路径。
// 这里执行器全部走 fs 的读 API，没有任何写入口；路径限定在 READ_ROOT 之内。

/** 一条工具定义（原样交给宿主）。 */
export interface ReadToolDef {
  name: string;
  description: string;
  parameters: Record<string, unknown>;
}

/** 记忆召回结果的一条（memory_recall 用）。 */

/** chat_recall 返回的一条。 */

/** runReadTool 的依赖：store.recall、session、宿主工具表。 */
interface ReadToolDeps {
  store?: SleepStore;
  session?: unknown;
  ctx?: SleepCtx;
}

/** 一条已收口的工具调用。 */
export interface ToolCall {
  id: string;
  name?: unknown;
  args?: unknown;
}

/** 流里的 partial（按 index 累加）。 */
interface PartialCall {
  id?: unknown;
  name: string;
  args: string;
}

/** 消费一轮 stream 的 chunk 面。 */
export interface StreamChunk {
  type: string;
  text: string;
  index?: number;
  id?: unknown;
  name?: unknown;
  argumentsDelta: string;
  arguments?: unknown;
  reason?: { kind?: string; failure?: unknown };
}

/** 一条消息内容块。 */
export interface ChatPart {
  type?: string;
  text?: string;
  id?: unknown;
  [key: string]: unknown;
}

/** 对话消息（repairToolCallIds 会就地改 content）。 */
export interface ChatMessage {
  role?: string;
  content?: ChatPart[];
  toolCallId?: unknown;
  isError?: boolean;
  source?: unknown;
  [key: string]: unknown;
}

export const READ_TOOLS = [
  {
    name: 'list_dir',
    description: '列出一个目录里的条目（名字、类型、大小）。用来确认目录是否存在、里面有什么。',
    parameters: { type: 'object', properties: { path: { type: 'string', description: '绝对路径' } }, required: ['path'] },
  },
  {
    name: 'stat_path',
    description: '看某个路径存不存在、是文件还是目录、多大、最后修改时间。',
    parameters: { type: 'object', properties: { path: { type: 'string', description: '绝对路径' } }, required: ['path'] },
  },
  {
    name: 'read_file',
    description: '读一个文本文件的开头若干行（默认 80 行，最多 200 行）。',
    parameters: { type: 'object', properties: { path: { type: 'string' }, limit: { type: 'number' } }, required: ['path'] },
  },
  {
    name: 'glob',
    description: '按通配符找文件，例如 "src/*/README.md" 或 "src/plugins/*/lib/index.js"，最多返回 50 条。',
    parameters: { type: 'object', properties: { pattern: { type: 'string' }, dir: { type: 'string', description: '从哪个目录开始，默认当前工作目录' } }, required: ['pattern'] },
  },
  {
    name: 'grep',
    description: '在文本文件里搜正则，返回命中的文件与行（最多 30 条）。用来确认某条配置或路径还在不在。',
    parameters: { type: 'object', properties: { pattern: { type: 'string' }, dir: { type: 'string' }, include: { type: 'string', description: '文件名后缀过滤，例如 .js,.md，逗号分隔' } }, required: ['pattern'] },
  },
  {
    name: 'memory_recall',
    description: '在长期记忆里按语义搜（融合语义/关键词/实体/时间），返回最相关的几条。用来核对某件事是不是记过、当时怎么说的，避免重复记。',
    parameters: { type: 'object', properties: { query: { type: 'string' }, k: { type: 'number' } }, required: ['query'] },
  },
  {
    name: 'chat_recall',
    description: '在过去会话日志里按语义搜（chatdb 索引），返回原文片段和时间。用来确认之前到底说过什么。',
    parameters: { type: 'object', properties: { query: { type: 'string' }, k: { type: 'number' } }, required: ['query'] },
  },
];

export async function runReadTool(name: string, args: unknown, deps: ReadToolDeps = {}): Promise<string> {
  const a: Record<string, unknown> = args && typeof args === 'object' ? args as Record<string, unknown> : typeof args === 'string' ? JSON.parse(args) as Record<string, unknown> : {};
  if (name === 'memory_recall') {
    // caller='sleep'：睡眠流程里模型自己去翻记忆，跟聊天里直接调要分得开（2026-11-01）
    const items = await deps.store!.recall!(
      String(a.query || ''),
      Math.min(Math.max(Number(a.k) || 5, 1), 8),
      { caller: 'sleep', session: deps.session ?? null, origin: null },
    );
    if (!items?.length) return '没有相关记忆';
    return clip(items.map((it) => `· [${it.type}${it.confidence === undefined ? '' : ` ${it.confidence}`}] ${it.text}（${it.date || it.ts || ''}）`).join('\n'), 2500);
  }
  if (name === 'chat_recall') {
    const tool = deps.ctx?.tools?.get?.('chat_recall');
    if (!tool) return '（chat_recall 工具没挂上，跳过）';
    const res = await tool.execute({ query: String(a.query || ''), k: Math.min(Math.max(Number(a.k) || 5, 1), 8) });
    const hits = res?.hits ?? [];
    if (!hits.length) return '没有命中的会话片段';
    return clip(hits.map((h) => `· ${h.when || ''}｜${h.text || ''}`).join('\n'), 2500);
  }
  if (name === 'list_dir') {
    const dir = safePath(a.path);
    return clip(fs.readdirSync(dir, { withFileTypes: true })
      .map((e) => {
        if (e.isDirectory()) return `${e.name}/`;
        try { return `${e.name} (${fs.statSync(path.join(dir, e.name)).size}B)`; } catch { return e.name; }
      })
      .join('\n'));
  }
  if (name === 'stat_path') {
    const p = safePath(a.path);
    if (!fs.existsSync(p)) return `不存在：${p}`;
    const st = fs.statSync(p);
    return `${st.isDirectory() ? '目录' : '文件'}｜${p}｜${st.size} 字节｜改于 ${st.mtime.toISOString()}`;
  }
  if (name === 'read_file') {
    const p = safePath(a.path);
    const limit = Math.min(Math.max(Number(a.limit) || 80, 1), 200);
    return clip(fs.readFileSync(p, 'utf8').split('\n').slice(0, limit).join('\n'));
  }
  if (name === 'glob') {
    const base = safePath(a.dir || READ_ROOT);
    const hits = fs.globSync(String(a.pattern || '*'), { cwd: base }).slice(0, 50);
    return hits.length ? hits.map((h) => path.join(base, h)).join('\n') : '没有匹配的文件';
  }
  if (name === 'grep') {
    const base = safePath(a.dir || READ_ROOT);
    const re = new RegExp(String(a.pattern), 'i');
    const exts = String(a.include || '').split(',').map((x) => x.trim()).filter(Boolean);
    const files: string[] = [];
    collectFiles(base, 4, files);
    const lines: string[] = [];
    for (const f of files) {
      if (lines.length >= 30) break;
      if (exts.length && !exts.some((e) => f.endsWith(e))) continue;
      if (!exts.length && !TEXT_EXT.some((e) => f.endsWith(e))) continue;
      let txt;
      try { txt = fs.readFileSync(f, 'utf8'); } catch { continue; }
      for (const line of txt.split('\n')) {
        if (re.test(line)) { lines.push(`${f}: ${line.trim().slice(0, 120)}`); if (lines.length >= 30) break; }
      }
    }
    return lines.length ? lines.join('\n') : '没有命中';
  }
  throw new Error(`没有这个工具：${name}`);
}


/**
 * 一轮流式响应里工具调用的收口（2026-11-06 t-fix-tooldup）。
 *
 * 同一个调用在流里可能以两种形态出现：完整的 tool-call，和按 index 累加的
 * tool-call-delta。两者顺序不定，旧代码在收 chunk 时 push 一次、循环结束又把
 * partials 全补一遍，同 id 就进了 calls 两次；回填进 assistant.tool_calls 与
 * tool 消息后上游直接 400 `Duplicate value for 'tool_call_id'`。
 *
 * 规则：以完整 tool_call 为准，partial 只补 id 还没出现过的；id 为空时按 index
 * 或序号兜底生成，绝不产生重复 id。导出给 test/tool-call-dedupe.test.mjs。
 */
export function mergeToolCalls(complete: Array<{ id?: unknown; name?: unknown; args?: unknown }> = [], partials: Map<number, PartialCall> = new Map()): ToolCall[] {
  const out: ToolCall[] = [];
  const taken = new Set<string>();
  let seq = 0;
  const makeId = (want: unknown, scope: string): string => {
    const base = want ? String(want) : `tool_call_${scope}`;
    let id = base;
    let n = 1;
    while (taken.has(id)) {
      n += 1;
      id = `${base}_${n}`;
    }
    return id;
  };
  for (const c of complete) {
    if (!c) continue;
    if (c.id && taken.has(String(c.id))) continue; // 完整件重复，留第一份
    seq += 1;
    const id = makeId(c.id, `call_${seq}`);
    taken.add(id);
    out.push({ id, name: c.name, args: c.args });
  }
  for (const [index, p] of partials) {
    if (!p || !p.name) continue;
    if (p.id && taken.has(String(p.id))) continue; // 完整件已经收过，以完整件为准
    seq += 1;
    const id = makeId(p.id, `partial_${index ?? seq}`);
    taken.add(id);
    out.push({ id, name: p.name, args: p.args });
  }
  return out;
}

/**
 * 消费一轮 stream（数组或异步迭代都行），返回 { text, seen, calls }。
 * 两处调用方（askModel / askCluster）共用，别再各写一份收口逻辑。
 */
export async function collectTurnChunks(chunks: AsyncIterable<StreamChunk> | Iterable<StreamChunk>, { errorLabel = '模型调用失败' }: { errorLabel?: string } = {}): Promise<{ text: string; seen: string[]; calls: ToolCall[] }> {
  let text = '';
  const seen: string[] = [];
  const complete: Array<{ id?: unknown; name?: unknown; args?: unknown }> = [];
  const partials = new Map<number, PartialCall>();
  for await (const chunk of chunks) {
    seen.push(chunk?.type);
    if (chunk?.type === 'text-delta') text += chunk.text;
    else if (chunk?.type === 'tool-call-delta') {
      const k = chunk.index ?? 0;
      const p = partials.get(k) ?? { id: chunk.id, name: '', args: '' };
      if (chunk.id) p.id = chunk.id;
      if (chunk.name) p.name = chunk.name as string;
      p.args += chunk.argumentsDelta ?? '';
      partials.set(k, p);
    } else if (chunk?.type === 'tool-call') {
      complete.push({ id: chunk.id, name: chunk.name, args: chunk.arguments });
    } else if (chunk?.type === 'finish') {
      if (chunk.reason?.kind === 'error') {
        throw new Error(`${errorLabel}：${JSON.stringify(chunk.reason.failure || {})}`);
      }
      seen.push(`finish:${chunk.reason?.kind || '?'}`);
    }
  }
  return { text, seen, calls: mergeToolCalls(complete, partials) };
}

/**
 * 发请求前的断言式防御（2026-11-06 t-fix-tooldup）：assistant 里的 tool-call id
 * 必须唯一，紧随其后的 tool 消息必须与之一一对应。违例就地修——多出来的
 * tool-call 按 id 留第一份，对不上/重复的 tool 消息丢掉，缺的补一条 error 结果；
 * 返回修复条数，调用方记 `[llm] tool_call_id duplicated repaired n=N`。
 * 绝不把非法体发出去。
 */
export function repairToolCallIds(messages: ChatMessage[] = []): number {
  let repaired = 0;
  for (let i = 0; i < messages.length; i += 1) {
    const msg = messages[i];
    if (msg?.role !== 'assistant' || !Array.isArray(msg.content)) continue;
    const ids = new Set<string>();
    const kept: ChatPart[] = [];
    for (const part of msg.content) {
      if (part?.type !== 'tool-call') {
        kept.push(part);
        continue;
      }
      const id = part.id ? String(part.id) : '';
      if (id && ids.has(id)) {
        repaired += 1;
        continue;
      }
      if (id) ids.add(id);
      kept.push(part);
    }
    if (kept.length !== msg.content.length) msg.content = kept;

    const tail: ChatMessage[] = [];
    let j = i + 1;
    while (j < messages.length && messages[j]?.role === 'tool') {
      tail.push(messages[j]);
      j += 1;
    }
    const byId = new Map<string, ChatMessage>();
    for (const t of tail) {
      const id = t.toolCallId ? String(t.toolCallId) : '';
      if (!id || !ids.has(id) || byId.has(id)) {
        repaired += 1;
        continue;
      }
      byId.set(id, t);
    }
    const rebuilt: ChatMessage[] = [];
    for (const id of ids) {
      const t = byId.get(id);
      if (t) {
        rebuilt.push(t);
        continue;
      }
      repaired += 1;
      rebuilt.push({
        role: 'tool',
        toolCallId: id,
        isError: true,
        content: [{ type: 'text', text: '（工具结果缺失）' }],
        source: { kind: 'tool', callId: id },
      });
    }
    if (rebuilt.length !== tail.length || rebuilt.some((t, k) => t !== tail[k])) {
      messages.splice(i + 1, tail.length, ...rebuilt);
    }
  }
  return repaired;
}
