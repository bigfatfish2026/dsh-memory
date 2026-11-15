/**
 * 问模型的三条路：整段睡觉（askModel）、按簇整理（askCluster）、入库前冲突检查（askConflict）。
 */
import { llmOf } from '../llm-service.js';
import { createToolLoopGuard } from '../agent-loop.js';
import { relatedEntries } from '../related.js';
import { collectTurnChunks, repairToolCallIds, READ_TOOLS, runReadTool } from './tool-calls.js';
import { SYSTEM, SYSTEM_CONFLICT, SYSTEM_CLUSTER } from './prompts.js';
import type { ChatMessage } from './tool-calls.js';
import type { SleepCfg, SleepCtx, SleepStore } from './env.js';
import type { MemoryRecord, MemoryView } from '../store/types.js';
import type { LintRedoOpts, LintRedone } from './lint.js';

/** 问模型用到的 env 面。 */
interface ModelCallsEnv {
  ctx: SleepCtx;
  store: SleepStore;
  cfg: SleepCfg;
  log: (line: string) => void;
  lint: { lintRedoOnce: (opts: LintRedoOpts) => Promise<LintRedone> };
}

/** askConflict 的裁决结果。 */
interface ConflictVerdict {
  verdict: string;
  supersedes: string[];
  text: string;
}

/** 入口冲突检查的一条新增。 */
interface ConflictAdd {
  text: string;
  entities?: unknown[];
  supersedes?: unknown;
  [key: string]: unknown;
}

/** 入口冲突检查的汇总。 */
interface ConflictCheckResult {
  adds: unknown[];
  checked: number;
  redundant: unknown[];
  stateChange: unknown[];
  conflict: unknown[];
  failed: number;
}

export function createModelCalls(env: ModelCallsEnv) {
  const { ctx, store, cfg } = env;
  const log = (_c: SleepCfg, line: string) => env.log(line);

async function askModel(transcript: string, existing: MemoryView[], effort: unknown, feedback = ''): Promise<string> {
  const listing = existing
    .map((m) => {
      const conf = m.confidence === null || m.confidence === undefined ? '' : ` | conf=${m.confidence}`;
      const date = m.validFrom ? ` | from=${m.validFrom}` : '';
      const ents = m.entities && m.entities.length ? ` | ent=${m.entities.join(',')}` : '';
      return `id=${m.id} | type=${m.type}${conf}${date}${ents} | tags=${(m.tags || []).join(',')} | ${m.text}`;
    })
    .join('\n');
  const prompt = [
    '现有记忆：',
    listing || '（空）',
    '',
    '对话记录（上次睡觉之后）：',
    transcript || '（空）',
    ...(feedback ? ['', feedback] : []),
  ].join('\n');

  const useTools = cfg.sleepTools !== false;
  const msgs: ChatMessage[] = [{ role: 'user', content: [{ type: 'text', text: prompt }] }];
  let out = '';
  const seen: string[] = [];
  // 批次越大越需要核对，给的轮数也越多；用完之后还有两轮纯收尾（不带工具，只逼 JSON）
  // 2026-11-14：工具调用不设次数上限（toolRounds=3 删了）。
  // 循环只在模型不再调工具时结束；原地转圈/无新信息由活性守卫中断（src/agent-loop.ts）。
  const guard = createToolLoopGuard({ log: (line: string) => log(cfg, line), label: 'sleep:askModel' } as unknown as { label?: string });
  let toolsOpen = useTools;
  for (;;) {
    // 2026-11-06 t-fix-tooldup：发请求前把上几轮回填的 tool-call 再校一遍，
    // id 重复或 tool 消息对不上就地修，绝不把非法体发给上游。
    const repaired = repairToolCallIds(msgs);
    if (repaired) log(cfg, `[llm] tool_call_id duplicated repaired n=${repaired}`);
    const stream = llmOf(ctx, { site: 'sleep:askModel', log: (line) => log(cfg, line), cfg }).stream({
      provider: cfg.provider,
      model: cfg.model,
      ...(effort && effort !== 'default' ? { reasoningEffort: effort } : {}),
      system: SYSTEM,
      messages: msgs,
      ...(toolsOpen ? { tools: READ_TOOLS } : {}),
      // 2026-11-14：不传 maxTokens（配置口子也删了），由 provider 封顶
    });
    const turn = await collectTurnChunks(stream as AsyncIterable<import('./tool-calls.js').StreamChunk>, { errorLabel: '睡觉时模型调用失败' });
    for (const t of turn.seen) seen.push(t);
    const text = turn.text;
    const calls = turn.calls;
    out += text;
    if (!calls.length) break;
    if (!toolsOpen) {
      // 已叫停还在按 DSML 写法吐工具调用：不再执行，直接交上层解析
      log(cfg, `睡觉：工具已关闭仍输出 ${calls.length} 个调用，结束本次调用`);
      break;
    }
    msgs.push({
      role: 'assistant',
      content: [
        ...(text ? [{ type: 'text', text }] : []),
        ...calls.map((c) => ({ type: 'tool-call', id: c.id, name: c.name, arguments: c.args })),
      ],
      source: { kind: 'model', provider: cfg.provider, model: cfg.model },
    });
    let tripped: { stop?: unknown; reason?: unknown } | null = null;
    for (const c of calls) {
      let detail;
      if (tripped) detail = '（检测到工具在重复或没有新信息，本次调用未执行；按已有信息下结论）';
      else {
        try { detail = await runReadTool(c.name as string, c.args, { store, ctx }); } catch (e) { detail = `工具出错：${(e as Error).message}`; }
        log(cfg, `整理时核对 ${c.name}(${String(JSON.stringify(c.args)).slice(0, 70)}) → ${String(detail).split('\n')[0].slice(0, 90)}`);
        const verdict = guard.record({ name: c.name, args: c.args, result: String(detail) });
        if (verdict.stop) tripped = verdict;
      }
      msgs.push({ role: 'tool', toolCallId: c.id, isError: false, content: [{ type: 'text', text: String(detail) }], source: { kind: 'tool', callId: c.id } });
    }
    if (tripped) {
      toolsOpen = false;
      msgs.push({
        role: 'user',
        content: [{
          type: 'text',
          text: `停止查证（${tripped.reason === 'repeat' ? '同一工具同参数连续重复' : '连续多次没有新信息'}）。`
            + '工具已经关闭。不要再写任何 XML / DSML 标签，也不要再调用工具。现在只输出一个 JSON 对象，形如 {"merge":[],"update":[],"archive":[]}，按你已经查到的情况直接下结论。',
        }],
        source: { kind: 'user' },
      });
    }
  }
  if (!out.trim()) log(cfg, `模型返回空文本，chunk 类型：${[...new Set(seen)].join(',')}`);
  return out;
}

/** 问模型：只针对这一批条目做整理（不带会话记录，不 add） */
async function askCluster(cluster: MemoryView[], feedback = ''): Promise<string> {
  const listing = cluster
    .map((m) => {
      const date = m.validFrom ? ` | from=${m.validFrom}` : '';
      return `id=${m.id} | type=${m.type}${date} | ${m.text}`;
    })
    .join('\n');
  const useTools = cfg.sleepTools !== false;
  // 每个簇从头到尾用同一份 msgs：查过的东西留在上下文里，后面几轮和收尾都能看见
  const msgs: ChatMessage[] = [{
    role: 'user',
    content: [{ type: 'text', text: `这一批记忆：\n${listing}${feedback ? `\n\n${feedback}` : ''}` }],
  }];
  let out = '';
  // 2026-11-14：工具调用次数不设上限（toolRounds 2..5 删了）。
  // 收敛靠模型自己停手 + 活性守卫（同工具同参连续 3 次 / 连续 20 次无新信息）。
  const guard = createToolLoopGuard({ log: (line: string) => log(cfg, line), label: 'sleep:askCluster' } as unknown as { label?: string });
  let toolsOpen = useTools;
  for (;;) {
    // 2026-11-06 t-fix-tooldup：同 askModel，发请求前再校一遍 tool-call id
    const repaired = repairToolCallIds(msgs);
    if (repaired) log(cfg, `[llm] tool_call_id duplicated repaired n=${repaired}`);
    const stream = llmOf(ctx, { site: 'sleep:askCluster', log: (line) => log(cfg, line), cfg }).stream({
      provider: cfg.provider,
      model: cfg.model,
      reasoningEffort: 'low',
      system: SYSTEM_CLUSTER,
      messages: msgs,
      ...(toolsOpen ? { tools: READ_TOOLS } : {}),
      // 2026-11-14：不传 maxTokens，由 provider 封顶
    });
    const turn = await collectTurnChunks(stream as AsyncIterable<import('./tool-calls.js').StreamChunk>, { errorLabel: '整理批次时模型调用失败' });
    const text = turn.text;
    const calls = turn.calls;
    out += text;
    if (!calls.length) break;
    if (!toolsOpen) {
      log(cfg, `分簇整理：工具已关闭仍输出 ${calls.length} 个调用，结束本批调用`);
      break;
    }
    msgs.push({
      role: 'assistant',
      content: [
        ...(text ? [{ type: 'text', text }] : []),
        ...calls.map((c) => ({ type: 'tool-call', id: c.id, name: c.name, arguments: c.args })),
      ],
      source: { kind: 'model', provider: cfg.provider, model: cfg.model },
    });
    let tripped: { stop?: unknown; reason?: unknown } | null = null;
    for (const c of calls) {
      let detail;
      if (tripped) detail = '（检测到工具在重复或没有新信息，本次调用未执行；按已有信息下结论）';
      else {
        try { detail = await runReadTool(c.name as string, c.args, { store, ctx }); } catch (e) { detail = `工具出错：${(e as Error).message}`; }
        log(cfg, `整理时核对 ${c.name}(${String(JSON.stringify(c.args)).slice(0, 70)}) → ${String(detail).split('\n')[0].slice(0, 90)}`);
        const verdict = guard.record({ name: c.name, args: c.args, result: String(detail) });
        if (verdict.stop) tripped = verdict;
      }
      msgs.push({ role: 'tool', toolCallId: c.id, isError: false, content: [{ type: 'text', text: String(detail) }], source: { kind: 'tool', callId: c.id } });
    }
    if (tripped) {
      toolsOpen = false;
      msgs.push({
        role: 'user',
        content: [{
          type: 'text',
          text: `停止查证（${tripped.reason === 'repeat' ? '同一工具同参数连续重复' : '连续多次没有新信息'}）。`
            + '工具已经关闭，不要再调用工具；直接输出这一批的 JSON 对象，形如 {"merge":[],"update":[],"archive":[]}，不要再写解释文字。',
        }],
        source: { kind: 'user' },
      });
    }
  }
  return out;
}

/**
 * 阶段二：分簇整理。
 * 先用向量+实体把记忆聚成小批（cluster.js），再一批一次模型调用——
 * 每批只看这二三十条，重复才看得见。单条成批的直接跳过，不问模型。
 */
async function askConflict(text: string, related: MemoryRecord[]): Promise<ConflictVerdict | null> {
  const listing = related
    .map((m) => `id=${m.id} | ${m.validFrom ? `from=${m.validFrom} | ` : ''}${m.text}`)
    .join('\n');
  const stream = llmOf(ctx, { site: 'sleep:askConflict', log: (line) => log(cfg, line), cfg }).stream({
    provider: cfg.provider,
    model: cfg.model,
    reasoningEffort: 'low',
    system: SYSTEM_CONFLICT,
    messages: [
      { role: 'user', content: [{ type: 'text', text: `新记忆：${text}\n\n同实体的旧记忆：\n${listing}` }] },
    ],
    // 2026-11-06 维护者硬规矩：不传 maxTokens，裁决短靠提示词约束
  });
  let raw = '';
  for await (const chunk of stream as AsyncIterable<{ type?: string; text?: string }>) {
    if (chunk?.type === 'text-delta' && chunk.text) raw += chunk.text;
  }
  const start = raw.indexOf('{');
  const end = raw.lastIndexOf('}');
  if (start < 0 || end <= start) return null;
  try {
    const obj = JSON.parse(raw.slice(start, end + 1)) as Record<string, unknown> | null;
    const verdict = String(obj?.verdict || '').trim();
    if (!['new', 'redundant', 'state_change', 'conflict'].includes(verdict)) return null;
    return {
      verdict,
      supersedes: Array.isArray(obj?.supersedes) ? obj.supersedes.map(String) : [],
      text: typeof obj?.text === 'string' ? obj.text.trim() : '',
    };
  } catch {
    return null;
  }
}

/**
 * 入口检查（2026-10-29，照 Hindsight 的冲突处理做的）：
 * 每出一条新记忆，先按实体找同实体的旧条，问模型这是冗余、矛盾还是状态变更。
 * 状态变更就改写成演进叙事并把旧条挂进 supersedes——相似度抓不到这种，余弦太低了。
 */
async function runConflictCheck(adds: unknown[] | null): Promise<ConflictCheckResult> {
  const out = { checked: 0, redundant: [] as unknown[], stateChange: [] as unknown[], conflict: [] as unknown[], failed: 0 };
  // 2026-11-14：一次最多检查 N 条（conflictCheckMaxCalls=20）删了，全部检查
  if (!Array.isArray(adds) || !adds.length) return { adds: adds || [], ...out };
  const active = store.listAll().filter((m) => (m.status || 'active') === 'active');
  const kept: unknown[] = [];
  for (const item of adds as ConflictAdd[]) {
    const text = String(item?.text || '').trim();
    const ents = (item?.entities || []).map((e) => String(e).trim()).filter(Boolean);
    if (!text || !ents.length) {
      kept.push(item);
      continue;
    }
    const related = relatedEntries(text, ents, active as unknown as MemoryRecord[]);
    if (!related.length) {
      kept.push(item);
      continue;
    }
    out.checked += 1;
    const verdict = await askConflict(text, related).catch((error) => {
      log(cfg, `入口冲突检查失败：${(error as Error | null)?.message || error}`);
      return null;
    });
    if (!verdict) {
      out.failed += 1;
      kept.push(item);
      continue;
    }
    if (verdict.verdict === 'redundant') {
      out.redundant.push({ text: text.slice(0, 60), supersedes: verdict.supersedes });
      continue; // 换个说法，不入库
    }
    if (verdict.verdict === 'state_change') {
      const next = { ...item };
      const rewritten = verdict.text;
      if (rewritten.length >= 10 && rewritten.length <= 220) next.text = rewritten;
      const sup = verdict.supersedes.filter((id) => related.some((m) => m.id === id));
      if (sup.length) {
        next.supersedes = [...new Set([...(Array.isArray(item.supersedes) ? item.supersedes : []), ...sup])];
      }
      out.stateChange.push({ supersedes: sup, text: next.text.slice(0, 60) });
      kept.push(next);
      continue;
    }
    if (verdict.verdict === 'conflict') {
      out.conflict.push({ text: text.slice(0, 60), against: verdict.supersedes });
    }
    kept.push(item);
  }
  if (out.checked) {
    log(
      cfg,
      `入口检查 ${out.checked} 条：状态变更${out.stateChange.length} 冗余丢弃${out.redundant.length} 矛盾${out.conflict.length} 失败${out.failed}`,
    );
  }
  return { adds: kept, ...out };
}

  return { askModel, askCluster, askConflict, runConflictCheck };
}
