/**
 * store 的共享状态与基础设施。
 *
 * createStore 的原实现是一整段闭包：db 连接、metas、snapshotVersion、rowSeq 映射、
 * 向量句柄、粗实体匹配器、llm 句柄、分词缓存全都关在里面。拆模块时这些必须是**同一份**
 * 按引用共享——多一条连接、多一份 metas 就等于把行为和事务边界改了。所以这里统一成
 * 一个 state 对象，各模块只拿 state，不另开库、不另存副本。
 *
 * state 上挂的东西分两类：
 *   - 数据：db / metas / snapshotVersion / maxRowSeq / rowSeqById / dim / vecDb /
 *     coarseMatcher / pendingEnqueues / tokenCache / dfCache / 计数；
 *   - 基础设施：connect / tableExists / embedTexts / currentLlm / writeTx /
 *     enqueueEntityLater / attachLlm / flushEntityQueue。
 *
 * vecDb 与 coarseMatcher 的创建留在 store.js：它们必须在 ensureSchema 之后开，
 * 顺序写在一处比藏在 state 构造函数里好查。
 */
import fs from 'node:fs';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { ensureDir } from '../shared/fs.js';
import { withTransaction } from '../shared/tx.js';
import { enqueueEntityExtract } from '../entity-queue.js';
import { logStore } from './log.js';
import type { MemoryRecord, StoreConfig, StoreLlm, StoreState, StoreStateBase } from './types.js';

/** 远端 BGE-M3 的 /embed。超时控制在中止器上，别在调用方另加计时。 */
async function embedRemote(apiUrl: string, texts: string[], timeoutMs: number): Promise<number[][]> {
  if (!apiUrl) throw new Error('dsh-memory: apiUrl 未配置，无法调用嵌入服务');
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const resp = await fetch(`${apiUrl.replace(/\/$/, '')}/embed`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ inputs: texts }),
      signal: ctrl.signal,
    });
    if (!resp.ok) throw new Error(`embed failed: HTTP ${resp.status}`);
    const data = await resp.json() as { embeddings: number[][] };
    return data.embeddings;
  } finally {
    clearTimeout(timer);
  }
}

function createInitialState(cfg: StoreConfig): StoreStateBase {
  const dir: string = cfg.storePath as string;
  const dbPath = path.join(dir, 'memory.db');
  // 只作为「库里还没迁移时」的导入源存在，运行时不读不写
  const jsonlSource = path.join(dir, 'memories.jsonl');
  const rrfK = Math.max(1, Number(cfg.rrfK) || 60);
  ensureDir(dir);

  return {
    cfg,
    dir,
    dbPath,
    jsonlSource,
    rrfK,
    db: null,
    metas: [],
    /** 活跃视图版本：每次写事务提交 +1，快照据此失效（写后一定读到新数据）。 */
    snapshotVersion: 0,
    /** 写入闸计数：calls = 过闸次数，llm = 闸里真正发出的模型调用次数（compact 用它单独量闸成本）。 */
    gateCalls: 0,
    gateLlm: 0,
    maxRowSeq: 0,
    rowSeqById: new Map<string, number>(),
    dim: 0,
    vecDb: null,
    coarseMatcher: null,
    /** 等异步入队落地（测试与关机前收尾用；正常写路径不 await 它）。 */
    pendingEnqueues: [],
    // 检索缓存（2026-11-15，审计存储中-4）：
    // 分词按记录对象缓存，df 跟着 snapshotVersion 走。
    tokenCache: new WeakMap<MemoryRecord, string[]>(),
    tokenBuilds: 0,
    dfBuilds: 0,
    dfCache: null,
    /** DedupeAgent 用的模型客户端：测试里可直接给假 llm；插件运行时只存 getter，每次现取。 */
    llmClient: null,
    llmGetter: null,
  };
}

/** 连接、取表、写事务：全部落在 state 上，任何模块拿到的都是同一条连接。 */
function installDb(state: StoreState): void {
  const { cfg, dbPath } = state;

  function connect(): DatabaseSync {
    if (state.db) return state.db;
    if (!fs.existsSync(dbPath)) {
      const message = `记忆库不存在：${dbPath}（先跑 scripts/migrate-jsonl-to-sqlite.mjs）`;
      logStore(cfg, 'ERROR', '打不开记忆库', message);
      throw new Error(message);
    }
    state.db = new DatabaseSync(dbPath, { allowExtension: true });
    state.db.exec('PRAGMA journal_mode = WAL;');
    state.db.exec('PRAGMA busy_timeout = 5000;');
    state.db.exec('PRAGMA synchronous = NORMAL;');
    return state.db;
  }

  function tableExists(name: string): boolean {
    return Boolean(
      connect().prepare("SELECT 1 AS ok FROM sqlite_master WHERE type='table' AND name=?").get(name),
    );
  }

  /** 写事务：正文与向量同一条连接，同一事务；失败整笔回滚 */
  function writeTx<T>(label: string, fn: (d: DatabaseSync) => T): T {
    const d = connect();
    try {
      const out = withTransaction(d, fn);
      state.snapshotVersion += 1;
      return out;
    } catch (error) {
      logStore(cfg, 'ERROR', `${label} 写入失败，已回滚`, String((error && (error as { message?: unknown }).message) || error));
      throw error;
    }
  }

  Object.assign(state, { connect, tableExists, writeTx });
}

/** 模型/向量/异步入队：句柄一律现取（getter），绝不在 setup 期存 ctx 里的值。 */
function installRuntime(state: StoreState): void {
  const { cfg } = state;

  /** 每次用 llm 时现取：注入的是 ` => llmOf(ctx)`，重载后仍能拿到在线的 llm。 */
  function currentLlm(): StoreLlm | null {
    return typeof state.llmGetter === 'function' ? state.llmGetter() : state.llmClient;
  }

  /** 向量化入口：默认走远端 BGE-M3；测试/离线可传 cfg.embed 覆盖，别在写路径里另开一条。 */
  async function embedTexts(texts: string[]): Promise<number[][]> {
    if (typeof cfg.embed === 'function') return cfg.embed(texts);
    return embedRemote(cfg.apiUrl as string, texts, cfg.timeoutMs as number);
  }

  function enqueueEntityLater(memoryId: string): void {
    const p: Promise<void> = new Promise((resolve) => {
      // 不用 unref：这一拍只是把 INSERT 挪出写事务，unref 掉会在进程空闲时把入队直接丢给 GC
      setImmediate(() => {
        try {
          if (cfg.entityQueue?.enqueue !== false) {
            enqueueEntityExtract(state.connect(), {
              memoryId,
              log: (line: string) => logStore(cfg, 'WARN', '实体队列', line),
            });
          }
        } catch (error) {
          logStore(cfg, 'WARN', '实体队列入队异常', String((error && (error as { message?: unknown }).message) || error));
        } finally {
          const i = state.pendingEnqueues.indexOf(p);
          if (i >= 0) state.pendingEnqueues.splice(i, 1);
          resolve();
        }
      });
    });
    state.pendingEnqueues.push(p);
  }

  /**
   * 注入模型客户端。传对象 = 测试/CLI 的固定假 llm；传函数 = 插件运行时每次现取
   * （` => llmOf(ctx)`，见 AGENTS「长任务：不持 setup ctx」）——setup 期绝不存 ctx.llm 的值。
   */
  function attachLlm(llm: StoreLlm | (() => StoreLlm | null) | null | undefined): boolean {
    if (typeof llm === 'function') {
      state.llmGetter = llm;
      state.llmClient = null;
      return true;
    }
    state.llmGetter = null;
    state.llmClient = llm || null;
    return Boolean(state.llmClient);
  }

  Object.assign(state, {
    currentLlm,
    embedTexts,
    enqueueEntityLater,
    attachLlm,
    /** 等异步入队落地（测试、关机收尾用；写路径不 await）。 */
    flushEntityQueue: () => Promise.all(state.pendingEnqueues.slice()),
  });
}

export function createStoreState(cfg: StoreConfig): StoreState {
  const state = createInitialState(cfg) as StoreState;
  installDb(state);
  installRuntime(state);
  return state;
}
