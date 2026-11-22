/**
 * memory_* 工具的注册：一个工具一段，参数 schema 与回执文案逐字保留。
 *
 * 工具只做参数整形与回执，写库/检索一律走 store 的公开方法；
 * 超长正文的「先拆再落」也在这里入队，不在 store 内部悄悄截断。
 */
import { defineTool } from '@deepseek-ai/dsh-tools';
import { assertMayWrite } from '../write-guard.js';
import { needsSplitText, clampLogLine } from '../text-limit.js';
import { enqueueSplitItem } from '../split-queue.js';
import { observationEvidence } from './observation-records.js';
import { normalizeType } from './types.js';
import { brief, logStore, recallSessionInfo } from './log.js';
import type { ToolDefinition, ToolRunContext } from '@deepseek-ai/dsh-tools';
import type { MemoryStore } from './memory-store.js';
import type { EntityViewItem, StoreConfig } from './types.js';
import type { RecallCtxLike, RecallExecLike } from './log.js';

/** render 的返回块：与宿主 ContentBlock 的 text 变体同形，不额外依赖 dsh-llm 类型。 */
interface RenderBlock {
  type: 'text';
  text: string;
}

/** 插件上下文里 registerStoreTools 真正用到的两处：tools.register 与 sessions 服务。 */
export interface ToolContextLike extends RecallCtxLike {
  tools: { register(definition: ToolDefinition): unknown };
}

/** memory_save 的参数（工具 schema 推断后的形状）。 */
interface SaveArgs {
  text: string;
  type?: string;
  tags?: string;
  [key: string]: unknown;
}

/** 各工具 output.schema 推断出来的回执形状；execute 返回与 render 共用。 */
interface SaveOutputValue {
  id: string;
  type: string;
  saved: boolean;
  total: number;
  queued?: boolean;
  length?: number;
}

interface RecallOutputItem {
  text: string;
  score: number;
  type: string;
  confidence?: number;
  tags: string;
  entities: string;
  date: string;
  match: string;
  ts: string;
  status: string;
}

interface RecallOutputValue {
  items: RecallOutputItem[];
}

interface ListOutputValue {
  total: number;
  items: Array<{ text: string; type: string; confidence?: number; tags: string; ts: string }>;
}

interface ArchiveOutputValue {
  id: string;
  status: string;
  total: number;
}

interface MergeOutputValue {
  id: string;
  type: string;
  merged: number;
  total: number;
  queued?: boolean;
  length?: number;
}

interface ReflectOutputValue {
  id: string;
  confidence: number;
  text: string;
}

/** memory_entity 的一条：observation 额外带证据 id（渲染层拼「证据：…」用）。 */
interface EntityOutputItem extends EntityViewItem {
  evidence?: string[];
}

interface EntityOutputValue {
  name: string;
  total: number;
  world: number;
  experience: number;
  opinion: number;
  observation: number;
  avgConfidence?: number;
  items: EntityOutputItem[];
}

function saveParameters(cfg: StoreConfig) {
  return {
    text: {
      type: 'string',
      required: true,
      description:
        `The memory content: ONE self-contained fact, at most ${Number(cfg.splitMaxChars) || 200} characters.` +
        ' Put an absolute date at the head when the fact has one (the server uses it as validFrom).' +
        ' Longer entries are not dropped: they go to the split queue as-is, but pre-splitting one fact per entry is better.',
    },
    type: { type: 'string', description: 'world | experience | opinion | observation. Defaults to inference from tags/text.' },
    tags: { type: 'string', description: 'Optional comma-separated tags, e.g. "preference,raspberrypi".' },
  } as const;
}

function saveOutput(cfg: StoreConfig) {
  return {
    schema: {
      type: 'object',
      additionalProperties: false,
      properties: {
        id: { type: 'string', required: true },
        type: { type: 'string', required: true },
        saved: { type: 'boolean', required: true },
        total: { type: 'number', required: true },
        queued: { type: 'boolean' },
        length: { type: 'number' },
      },
    },
    render: (_args: unknown, value: SaveOutputValue): RenderBlock[] => [{
      type: 'text',
      text: value.queued
        ? `正文 ${value.length} 字超过 ${Number(cfg.splitMaxChars) || 200}，已入队拆分（pending_ops kind=split_item），原文没丢也没截`
        : `已记住（${value.type}，共 ${value.total} 条）`,
    }],
  } as const;
}

async function executeSave(store: MemoryStore, cfg: StoreConfig, args: SaveArgs, exec: ToolRunContext): Promise<SaveOutputValue> {
  assertMayWrite(exec, 'memory_save');
  const text = String(args.text ?? '').trim();
  // 2026-11-05 t-fix-first-run：超长不再当场退回（退回等于丢），原样入队交给拆分 agent。
  // 写库路径一律不丢不截，这里只是「先拆再落」。
  if (needsSplitText(text)) {
    let pendingDbHandle = null;
    try { pendingDbHandle = store.db(); } catch { pendingDbHandle = null; }
    const queued = enqueueSplitItem(pendingDbHandle, {
      sourceId: '',
      text,
      tags: args.tags ? String(args.tags).split(',').map((s) => s.trim()).filter(Boolean) : [],
      type: String(args.type || ''),
      op: 'add',
      batch: 'memory_save',
      reason: 'save_over_max',
      log: (line: string) => logStore(cfg, 'WARN', 'memory_save 超长入队', line),
    });
    logStore(cfg, 'WARN', 'memory_save 正文超长', clampLogLine('入队拆分', '', text.length, text.length, text));
    if (!queued) {
      // 没 pending_ops 就真没处放了：明说，别装成成功
      throw new Error(
        `这条记忆 ${text.length} 字超过 ${Number(cfg.splitMaxChars) || 200}，且没有 pending_ops 可入队` +
          '（先跑 scripts/apply-pipeline-schema.mjs）；这条一个字都没存',
      );
    }
    return {
      id: `queued:${queued.key}`,
      type: normalizeType(args.type, [], text),
      saved: false,
      total: store.metas().length,
      queued: true,
      length: text.length,
    };
  }
  const tags = args.tags ? String(args.tags).split(',').map((s) => s.trim()).filter(Boolean) : [];
  // 2026-11-13：参数硬切到 3 个（text / tags / type）。
  // supersedes 已删：取代关系由后台整理（entity_extract 队列 + 整理轮）自己判，调用方不该知道它取代了谁。
  // entities / validFrom / validTo / confidence 也已删：实体由写入时粗匹配 + 后台队列补，
  // 日期服务端从正文头部反抽。老调用方还传就忽略，不报错——但记一行 WARN 抓没改的提示词。
  const legacy = ['entities', 'validFrom', 'validTo', 'confidence', 'supersedes'].filter((k) => args[k] !== undefined);
  if (legacy.length) {
    logStore(cfg, 'WARN', 'memory_save 收到已删参数（忽略）', `keys=${legacy.join(',')} text=${brief(text, 40)}`);
  }
  const rec = await store.save(text, tags, 'agent', {
    type: args.type,
  });
  // 写入闸判 DUPLICATE：不写库，但工具要给出可读的回执，别让上层以为存进去了
  if (rec.discarded) {
    return { id: rec.matchedId || 'duplicate', type: rec.type as string, saved: false, total: store.metas().length };
  }
  return { id: rec.id, type: rec.type as string, saved: true, total: store.metas().length };
}

function registerSave(ctx: ToolContextLike, store: MemoryStore, cfg: StoreConfig): void {
  ctx.tools.register(
    defineTool({
      name: 'memory_save',
      description:
        'Save a durable fact, preference, or decision about the user or an ongoing task into long-term memory. Use it when the user states something worth remembering across sessions. Only write the text (plus type/tags when they help): entities and dates are the server\'s job, and supersession is judged by background consolidation, so do not try to fill any of them in.',
      parameters: saveParameters(cfg),
      output: saveOutput(cfg),
      async execute(args, exec) {
        return executeSave(store, cfg, args, exec);
      },
    }),
  );
}

function registerRecall(ctx: ToolContextLike, store: MemoryStore): void {
  ctx.tools.register(
    defineTool({
      name: 'memory_recall',
      description:
        'Search long-term memory by meaning. Fuses semantic, keyword, entity and time channels. Returns the most relevant memories with type, confidence and why each matched. Use it before answering questions about past preferences, decisions, or earlier work.',
      parameters: {
        query: { type: 'string', required: true, description: 'What to look for, in natural language.' },
        k: { type: 'number', description: 'How many results. Defaults to the configured topK.' },
        type: { type: 'string', description: 'Only return this type: world | experience | opinion | observation.' },
      },
      output: {
        schema: {
          type: 'object',
          additionalProperties: false,
          properties: {
            items: {
              type: 'array',
              required: true,
              items: {
                type: 'object',
                additionalProperties: false,
                properties: {
                  text: { type: 'string', required: true },
                  score: { type: 'number', required: true },
                  type: { type: 'string', required: true },
                  confidence: { type: 'number' },
                  tags: { type: 'string', required: true },
                  entities: { type: 'string', required: true },
                  date: { type: 'string', required: true },
                  match: { type: 'string', required: true },
                  ts: { type: 'string', required: true },
                  status: { type: 'string', required: true },
                },
              },
            },
          },
        },
        render: (_args: unknown, value: RecallOutputValue): RenderBlock[] => [
          {
            type: 'text',
            text: value.items.length === 0
              ? '没有相关记忆'
              : value.items
                  .map((it) => {
                    const conf = it.confidence === null || it.confidence === undefined ? '' : ` ${it.confidence}`;
                    const head = `· [${it.type}${conf}] ${it.text}`;
                    const tail = `（${it.score}${it.match ? ` · ${it.match}` : ''}${it.date ? ` · ${it.date}` : ''}）`;
                    return head + tail;
                  })
                  .join('\n'),
          },
        ],
      },
      async execute(args, exec) {
        // session / origin 只喂日志；拿不到就 null，不瞎填
        const who = recallSessionInfo(ctx, exec as unknown as RecallExecLike);
        const items = await store.recall(String(args.query), args.k, {
          type: args.type,
          caller: 'tool',
          session: who.session,
          origin: who.origin,
          callId: exec?.callId ?? null,
        });
        return { items } as unknown as RecallOutputValue;
      },
    }),
  );
}

function registerList(ctx: ToolContextLike, store: MemoryStore): void {
  ctx.tools.register(
    defineTool({
      name: 'memory_list',
      description: 'List the most recently saved long-term memories, newest first.',
      parameters: {
        limit: { type: 'number', description: 'How many to return. Defaults to 10.' },
        all: { type: 'boolean', description: 'Include archived/merged entries.' },
        type: { type: 'string', description: 'Only list this type: world | experience | opinion | observation.' },
      },
      output: {
        schema: {
          type: 'object',
          additionalProperties: false,
          properties: {
            total: { type: 'number', required: true },
            items: {
              type: 'array',
              required: true,
              items: {
                type: 'object',
                additionalProperties: false,
                properties: {
                  text: { type: 'string', required: true },
                  type: { type: 'string', required: true },
                  confidence: { type: 'number' },
                  tags: { type: 'string', required: true },
                  ts: { type: 'string', required: true },
                },
              },
            },
          },
        },
        render: (_args: unknown, value: ListOutputValue): RenderBlock[] => [
          {
            type: 'text',
            text: value.items.length === 0
              ? '还没有任何记忆'
              : `共 ${value.total} 条，最近：\n` +
                value.items.map((it) => `· [${it.type}] ${it.text}`).join('\n'),
          },
        ],
      },
      async execute(args) {
        const limit = Math.max(1, Math.min(Number(args.limit) || 10, 100));
        const wantType = String(args.type || '').toLowerCase();
        const all = store.metas();
        const pool = (args.all ? all : all.filter(store.isActive)).filter(
          (m) => !wantType || store.typeOf(m) === wantType,
        );
        const items = pool
          .slice(-limit)
          .reverse()
          .map((m) => ({
            text: (store.isActive(m) ? '' : `[${m.status}] `) + m.text,
            type: store.typeOf(m),
            ...(m.confidence === undefined ? {} : { confidence: Number(m.confidence) }),
            tags: (m.tags || []).join(','),
            ts: m.ts,
          }));
        return { total: pool.length, items } as unknown as ListOutputValue;
      },
    }),
  );
}

function registerArchive(ctx: ToolContextLike, store: MemoryStore): void {
  ctx.tools.register(
    defineTool({
      name: 'memory_archive',
      description:
        'Mark one long-term memory as archived (作废或过期). It stays on disk for history but drops behind active memories in recall. Use when an entry is wrong, superseded, or no longer true.',
      parameters: {
        id: { type: 'string', required: true, description: 'Memory id, e.g. m_mvo1zf0u_pnd7' },
        reason: { type: 'string', description: 'Why it is archived' },
      },
      output: {
        schema: {
          type: 'object',
          additionalProperties: false,
          properties: {
            id: { type: 'string', required: true },
            status: { type: 'string', required: true },
            total: { type: 'number', required: true },
          },
        },
        render: (_args: unknown, value: ArchiveOutputValue): RenderBlock[] => [{ type: 'text', text: `已归档 ${value.id}（${value.status}），共 ${value.total} 条` }],
      },
      async execute(args, exec) {
        assertMayWrite(exec, 'memory_archive');
        const id = String(args.id || '').trim();
        if (!id) throw new Error('memory_archive needs an id');
        const rec = await store.archive(id, String(args.reason || 'archived manually'));
        return { id, status: (rec && rec.status) as string || 'archived', total: store.metas().length };
      },
    }),
  );
}

function registerMerge(ctx: ToolContextLike, store: MemoryStore, cfg: StoreConfig): void {
  ctx.tools.register(
    defineTool({
      name: 'memory_merge',
      description:
        'Merge several long-term memories into one rewritten entry; the originals are marked merged and kept for history. Use for near-duplicates, or when an entry was superseded by a later correction.',
      parameters: {
        ids: { type: 'string', required: true, description: 'Comma-separated memory ids, at least 2' },
        text: { type: 'string', required: true, description: 'The merged, self-contained text' },
        type: { type: 'string', description: 'world | experience | opinion | observation. Defaults to the first source type.' },
        tags: { type: 'string', description: 'Optional comma-separated tags' },
      },
      output: {
        schema: {
          type: 'object',
          additionalProperties: false,
          properties: {
            id: { type: 'string', required: true },
            type: { type: 'string', required: true },
            merged: { type: 'number', required: true },
            total: { type: 'number', required: true },
            queued: { type: 'boolean', description: '正文超长、已入队拆分（pending_ops kind=split_item）' },
            length: { type: 'number', description: '被入队拆分的正文长度' },
          },
        },
        render: (_args: unknown, value: MergeOutputValue): RenderBlock[] => [{
          type: 'text',
          text: value.queued
            ? `合并后正文 ${value.length} 字超过 200，已入队拆分（pending_ops kind=split_item），原条一条都没动`
            : `已把 ${value.merged} 条合成 ${value.id}（${value.type}），共 ${value.total} 条`,
        }],
      },
      async execute(args, exec) {
        assertMayWrite(exec, 'memory_merge');
        const ids = String(args.ids || '').split(',').map((s) => s.trim()).filter(Boolean);
        const tags = args.tags ? String(args.tags).split(',').map((s) => s.trim()).filter(Boolean) : [];
        const text = String(args.text || '');
        // 2026-11-05 t-fix-first-run：超长不再抛错、也不截断，原样入队交给拆分 agent。
        // 写库路径一律不丢不截，这条只不过是「先拆再合」而不是「拒绝合并」。
        if (needsSplitText(text)) {
          // registerStoreTools 这层拿不到 createStore 里的 tableExists/connect，走 store.db()；
          // 没有 pending_ops 时 enqueueSplitItem 自己会 catch 并返回 null
          let pendingDbHandle = null;
          try { pendingDbHandle = store.db(); } catch { pendingDbHandle = null; }
          const queued = enqueueSplitItem(pendingDbHandle, {
            sourceId: '',
            text,
            ids,
            tags,
            type: String(args.type || ''),
            op: 'merge',
            batch: 'memory_merge',
            reason: 'merge_over_max',
            log: (line: string) => logStore(cfg, 'WARN', 'memory_merge 超长入队', line),
          });
          logStore(cfg, 'WARN', 'memory_merge 正文超长', clampLogLine('入队拆分', ids.join('+'), text.length, text.length, text));
          if (!queued) {
            // 没 pending_ops 就真没处放了：明说，别装成成功
            throw new Error(
              `合并后正文 ${text.length} 字超过 200，且没有 pending_ops 可入队（先跑 scripts/apply-pipeline-schema.mjs）；原条一条都没动`,
            );
          }
          return {
            id: `queued:${queued.key}`,
            type: String(args.type || 'world'),
            merged: 0,
            total: store.metas().length,
            queued: true,
            length: text.length,
          } as unknown as MergeOutputValue;
        }
        const rec = await store.merge(ids, text, tags, { type: args.type });
        if (rec.discarded) {
          return { id: rec.matchedId || 'duplicate', type: rec.type as string, merged: 0, saved: false, total: store.metas().length } as unknown as MergeOutputValue;
        }
        return { id: rec.id, type: rec.type as string, merged: ids.length, total: store.metas().length };
      },
    }),
  );
}

function registerReflect(ctx: ToolContextLike, store: MemoryStore): void {
  ctx.tools.register(
    defineTool({
      name: 'memory_reflect',
      description:
        'Adjust the confidence of an opinion memory: reinforce (a later fact supports it), weaken (new evidence softens it), contradict (new evidence contradicts it; drops twice as fast). Opinions are never deleted, only re-weighted.',
      parameters: {
        id: { type: 'string', required: true, description: 'Memory id of an opinion entry' },
        signal: { type: 'string', required: true, description: 'reinforce | weaken | contradict' },
      },
      output: {
        schema: {
          type: 'object',
          additionalProperties: false,
          properties: {
            id: { type: 'string', required: true },
            confidence: { type: 'number', required: true },
            text: { type: 'string', required: true },
          },
        },
        render: (_args: unknown, value: ReflectOutputValue): RenderBlock[] => [
          { type: 'text', text: `已调整 ${value.id} 的置信度到 ${value.confidence}：${value.text.slice(0, 50)}` },
        ],
      },
      async execute(args, exec) {
        assertMayWrite(exec, 'memory_reflect');
        const rec = await store.reflect(String(args.id || '').trim(), String(args.signal || ''));
        return { id: rec.id as string, confidence: Number(rec.confidence), text: rec.text };
      },
    }),
  );
}

function registerEntityTool(ctx: ToolContextLike, store: MemoryStore): void {
  ctx.tools.register(
    defineTool({
      name: 'memory_entity',
      description:
        'Gather everything remembered about one entity (a person, machine, project): counts per type, average opinion confidence, and the most recent entries. Use before answering "what do you know about X".',
      parameters: {
        name: { type: 'string', required: true, description: 'Entity name, e.g. "Alice" or "example-host" or "dsh-memory"' },
        limit: { type: 'number', description: 'How many entries to list. Defaults to 8.' },
      },
      output: {
        schema: {
          type: 'object',
          additionalProperties: false,
          properties: {
            name: { type: 'string', required: true },
            total: { type: 'number', required: true },
            world: { type: 'number', required: true },
            experience: { type: 'number', required: true },
            opinion: { type: 'number', required: true },
            observation: { type: 'number', required: true },
            avgConfidence: { type: 'number' },
            items: {
              type: 'array',
              required: true,
              items: {
                type: 'object',
                additionalProperties: false,
                properties: {
                  id: { type: 'string', required: true },
                  type: { type: 'string', required: true },
                  text: { type: 'string', required: true },
                  confidence: { type: 'number' },
                  date: { type: 'string', required: true },
                  evidence: { type: 'array', items: { type: 'string' } },
                },
              },
            },
          },
        },
        render: (_args: unknown, value: EntityOutputValue): RenderBlock[] => {
          if (value.total === 0) return [{ type: 'text', text: `没有关于「${value.name}」的记忆` }];
          const head =
            `「${value.name}」共 ${value.total} 条：world ${value.world} / experience ${value.experience}` +
            ` / opinion ${value.opinion} / observation ${value.observation}` +
            (value.avgConfidence === undefined ? '' : `，opinion 平均置信度 ${value.avgConfidence}`);
          const body = value.items.map((it) => {
            const line = `· [${it.type}] ${it.text}（${it.date}）`;
            // 观察要能看出结论从哪来：证据 id 只对 observation 条拼，其它类型保持原样
            return it.type === 'observation' && it.evidence?.length
              ? `${line}\n  证据：${it.evidence.join(', ')}（${it.evidence.length} 条独立来源）`
              : line;
          });
          return [{ type: 'text', text: [head, ...body].join('\n') }];
        },
      },
      async execute(args) {
        const view = store.entityView(String(args.name || ''), Number(args.limit) || 8);
        // 证据链在这里补：observationEvidence 读 memory_link 的 derived_from 边，
        // 库里没有边（老观察 / 缺表）就返回空数组，条目不因此消失。
        const items: EntityOutputItem[] = view.items.map((it) => (
          it.type === 'observation'
            ? { ...it, evidence: observationEvidence(store.db(), it.id) }
            : it
        ));
        const out: EntityOutputValue = {
          name: view.name,
          total: view.total,
          world: view.counts.world,
          experience: view.counts.experience,
          opinion: view.counts.opinion,
          observation: view.counts.observation,
          items,
        };
        if (view.avgConfidence !== null) out.avgConfidence = view.avgConfidence;
        return out;
      },
    }),
  );
}

export function registerStoreTools(ctx: ToolContextLike, store: MemoryStore, cfg: StoreConfig): void {
  registerSave(ctx, store, cfg);
  registerRecall(ctx, store);
  registerList(ctx, store);
  registerArchive(ctx, store);
  registerMerge(ctx, store, cfg);
  registerReflect(ctx, store);
  registerEntityTool(ctx, store);
}
