/**
 * sleep.state.json 的唯一入口（2026-11-14，阶段 4）。
 *
 * 以前 sleep.js 里 18 处 readState(cfg)、13 处 writeState(cfg, …) 直接摸文件，
 * 读-改-写之间没有统一口径（谁都能拿一份旧快照覆盖别人刚写的字段）。
 * 这里把文件操作收成一个对象：
 *   load()       读主文件，坏 JSON 返回空态（绝不抛，启动不能因为状态文件挂掉）；
 *   save(state)  原子写（.tmp + rename），写一半断电只会留 .tmp；
 *   patch(next)  读-合并-写，next 是对象（{...state, ...next}）或函数（原地改）；
 *   update(fn)   同上，函数形态，返回新 state。
 *
 * 同步写、同步读：并发语义跟原来一样（Node 单线程里一次调用一个写），
 * 唯一的额外保护是禁止嵌套 update（防「patch 里又 patch」写出半截状态）。
 * 字段一个都不改：lastSleepAt/slots/owedSlots/owner/runningAt/heartbeatAt/organizeCache
 * 还是调用方说了算，这个对象只负责搬运。
 */
import fs from 'node:fs';
import path from 'node:path';

/**
 * 状态文件的顶层形状：字段由调用方维护，这里只保证能当对象读写。
 * 全部可空——坏 JSON 退回空态时只有 lastSleepAt / lastSleepDay。
 */
export interface SleepState {
  lastSleepAt?: number;
  lastSleepDay?: string;
  owner?: string;
  runningAt?: number;
  heartbeatAt?: number;
  slots?: Record<string, unknown>;
  owedSlots?: Record<string, unknown>;
  organizeCache?: Record<string, unknown>;
  [key: string]: unknown;
}

/** 读-改-写句柄：同步语义与老实现一致，只补类型。 */
export interface SleepStateStore {
  file: string;
  load(): SleepState;
  save(state: SleepState): SleepState;
  patch(next: SleepState | ((state: SleepState) => SleepState | void)): SleepState;
  update(fn: (state: SleepState) => SleepState | void): SleepState;
}

const emptyState = (): SleepState => ({ lastSleepAt: 0, lastSleepDay: '' });

/** 已知字段的形状检查：只查类型，不查业务语义（字段本身全可空）。 */
function asSleepState(raw: unknown): SleepState | null {
  if (raw === null || typeof raw !== 'object' || Array.isArray(raw)) return null;
  const s = raw as Record<string, unknown>;
  const numOrMissing = (v: unknown) => v === undefined || typeof v === 'number';
  const strOrMissing = (v: unknown) => v === undefined || typeof v === 'string';
  const objOrMissing = (v: unknown) => v === undefined
    || (v !== null && typeof v === 'object' && !Array.isArray(v));
  if (!numOrMissing(s.lastSleepAt) || !strOrMissing(s.lastSleepDay)) return null;
  if (!strOrMissing(s.owner) || !numOrMissing(s.runningAt) || !numOrMissing(s.heartbeatAt)) return null;
  for (const key of ['slots', 'owedSlots', 'organizeCache']) {
    if (!objOrMissing(s[key])) return null;
  }
  return s as SleepState;
}

/**
 * @param spec.file state 文件绝对路径
 * @param spec.pid tmp 文件名后缀（默认 process.pid，跟原来一致）
 */
export function createSleepState({ file, pid = process.pid }: { file?: string; pid?: number } = {}): SleepStateStore {
  if (!file) throw new Error('sleep-state: file 必填');
  let mutating = false;

  function load(): SleepState {
    let raw: unknown;
    try {
      raw = JSON.parse(fs.readFileSync(file as string, 'utf8'));
    } catch {
      return emptyState();
    }
    // 最小形状守卫：JSON.parse 出来的东西不直接当 SleepState 用。顶层必须是普通对象，
    // 已知字段出现时类型要对得上；不合格按坏文件处理，退回空态（跟坏 JSON 同一档）。
    const state = asSleepState(raw);
    return state || emptyState();
  }

  function save(state: SleepState): SleepState {
    try {
      fs.mkdirSync(path.dirname(file as string), { recursive: true });
    } catch {
      /* 目录建不了就让 writeFile 自己抛，别在这里吞掉 */
    }
    // 先写 .tmp 再 rename：写一半断电只会留 .tmp，主文件要么旧要么新，不会半截 JSON
    const tmp = `${file}.tmp-${pid}`;
    fs.writeFileSync(tmp, JSON.stringify(state, null, 2), 'utf8');
    fs.renameSync(tmp, file as string);
    return state;
  }

  function update(fn: (state: SleepState) => SleepState | void): SleepState {
    if (mutating) throw new Error('sleep-state: 嵌套更新（同步写，不许重入）');
    mutating = true;
    try {
      const state = load();
      const out = fn(state) || state;
      return save(out);
    } finally {
      mutating = false;
    }
  }

  const patch = (next: SleepState | ((state: SleepState) => SleepState | void)): SleepState =>
    (typeof next === 'function' ? update(next) : update((state) => ({ ...state, ...(next || {}) })));

  return { file, load, save, patch, update };
}

/**
 * 内存版（没配 stateFile 的测试桩/临时 cfg 用）：接口一样，只是不落盘。
 * 老实现里 cfg.stateFile 缺失时 readState 会抛错被 catch 成空态，writeState 会炸；
 * 这里给一个稳定的内存态，行为更可预期，也不改变任何生产路径。
 */
export function createMemorySleepState(): SleepStateStore {
  let state: SleepState = emptyState();
  const load = (): SleepState => ({ ...state });
  const save = (next: SleepState): SleepState => { state = next; return state; };
  const update = (fn: (state: SleepState) => SleepState | void): SleepState => save(fn(load()) || load());
  const patch = (next: SleepState | ((state: SleepState) => SleepState | void)): SleepState =>
    (typeof next === 'function' ? update(next) : update((cur) => ({ ...cur, ...(next || {}) })));
  return { file: '', load, save, patch, update };
}
