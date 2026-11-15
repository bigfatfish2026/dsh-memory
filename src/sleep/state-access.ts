/**
 * sleep.state.json 的唯一入口：读、写、读-改-写都走这里（别再直接摸文件）。
 * 同一个 state 文件只建一个对象（同步读写，靠它统一 load/patch 口径）。
 */
import { createSleepState, createMemorySleepState } from '../sleep-state.js';
import type { SleepState, SleepStateStore } from '../sleep-state.js';

/** 本文件只读 stateFile；其余配置字段一概不看。 */
interface StateCfg {
  stateFile?: unknown;
  [key: string]: unknown;
}

const STATE_STORES = new Map<string, SleepStateStore>();
const MEMORY_STATES = new WeakMap<StateCfg, SleepStateStore>();

export function stateOf(cfg: StateCfg): SleepStateStore {
  const file = String(cfg?.stateFile || '');
  if (!file) {
    // 没配 stateFile（老测试桩/临时 cfg）：给内存态，别把整轮整理卡在状态文件上
    const existing = MEMORY_STATES.get(cfg);
    if (existing) return existing;
    const store = createMemorySleepState();
    MEMORY_STATES.set(cfg, store);
    return store;
  }
  const existing = STATE_STORES.get(file);
  if (existing) return existing;
  const store = createSleepState({ file });
  STATE_STORES.set(file, store);
  return store;
}

/** 加载路径：只读主文件。organizeCache 归档目录是冷数据，这里绝不读。 */
export function readState(cfg: StateCfg): SleepState {
  return stateOf(cfg).load();
}

export function writeState(cfg: StateCfg, state: SleepState): SleepState {
  return stateOf(cfg).save(state);
}
