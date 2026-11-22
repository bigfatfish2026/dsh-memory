/**
 * 实体待观察登记：把「这条记忆/这个实体的证据变了」翻成 observe_task 的一次 enqueue。
 *
 * 设计见 docs/observation-plan-v2.md §4。写路径（records / entity-graph / pipeline-split）
 * 只在**已经开着的事务里**调这里，函数内部不开事务、不 setImmediate：证据与登记同成同败。
 *
 * 两条硬规矩：
 *   1. 没有 observe_task 表时整段静默跳过（hasObserveTaskTable），老库跑写路径行为不变；
 *   2. observe.enabled 默认 false —— 关着时一行都不写，开了才登记。
 * 第三条（2026-11-22，t-mwr37s99-7y32）：登记前按 entities.etype 收口，file / config / tool /
 * unknown 四类不排队（它们本来就不是 observation 的主体，占着队列 2/3）；缺 entities 表、
 * 查不到行、etype 空一律照旧放行，不把「不知道类型」当脏键。
 * 这份常量与查询自 t-mwr4i2td-7n7n 起收口在 observe-queue.ts（claim 侧用同一份），
 * 本文件只 import 并按原路径 re-export，写侧判断不变。
 *
 * entity_key 口径直接用 shared/entity-normalize 的 normalizeEntity().norm：
 * NFKC 全半角 + 小写 + 空白折叠 + 中文旁空格删除 + 去尾标点 + 去包裹引号/书名号，
 * 版本后缀归到 base（树莓派 5 / 树莓派Pi5 → 树莓派）。别名不在这里展开（R9 已废除，
 * 别名由 entity_surface 表达）；同一批 key 按 norm 去重。
 */
import type { DatabaseSync } from 'node:sqlite';
import { normalizeEntity } from '../shared/entity-normalize.js';
import {
  entityEtypeOf, enqueueObserveTask, hasObserveTaskTable, SKIP_OBSERVE_ETYPES,
} from './observe-queue.js';

// 这两个符号的实现在 observe-queue.ts（写侧与 claim 侧共用一份）；按原路径继续对外导出。
export { entityEtypeOf, SKIP_OBSERVE_ETYPES };

/** 只读这几个键的配置面：插件 cfg 与 store cfg 都按结构兼容。 */
export interface ObserveSwitchConfig {
  observe?: { enabled?: boolean; [key: string]: unknown } | null;
  [key: string]: unknown;
}

/** 排障用的登记原因；字符串开放，后续票加新触发点不用改类型。 */
export type ObserveReason = 'new-memory' | 'rewrite' | 'merge' | 'archive' | 'mount-late' | 'split' | (string & {});

export interface NotifyOptions {
  /** 覆盖登记时间（测试用）。 */
  now?: number;
  /** 出错时的日志出口；不传就静默。 */
  log?: (line: string) => void;
  /** key → entities.id；挂载晚到路径用它把 entity_id 一起写进队列行。 */
  entityIdByKey?: Map<string, string>;
}

/** 开关：只有显式 true 才登记；缺配置、缺 observe 段一律当 false。 */
export function observeEnabled(cfg: ObserveSwitchConfig | null | undefined): boolean {
  return cfg?.observe?.enabled === true;
}

/** 名字 → entity_key（规范化后去空、去重、保持首次出现顺序）。 */
export function entityKeysOf(names: Iterable<unknown>): string[] {
  const out: string[] = [];
  const seen = new Set<string>();
  for (const raw of names) {
    const key = normalizeEntity(String(raw ?? '')).norm;
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push(key);
  }
  return out;
}

/**
 * 一条记忆当前挂着的实体键（memory_entity ⋈ entities）。
 *
 * 正文改写要拿「改写前」的实体，光看 memories.entities 老列会漏掉 agent 后补的挂载；
 * 表/列不在就返回空，绝不因为登记挡写路径。
 */
export function entityKeysMountedOn(db: DatabaseSync | null | undefined, memoryId: string): string[] {
  if (!db) return [];
  try {
    const rows = db.prepare(
      `SELECT DISTINCT e.norm AS norm
         FROM memory_entity me
         JOIN entities e ON e.id = me.entity_id
        WHERE me.memory_id = ?`,
    ).all(String(memoryId)) as Array<{ norm?: unknown }>;
    return entityKeysOf(rows.map((r) => r.norm));
  } catch {
    return [];
  }
}

/** 一个 entities.id 对应的 norm；查不到返回空串。 */
export function entityKeyOfId(db: DatabaseSync | null | undefined, entityId: unknown): string {
  const id = Number(entityId);
  if (!db || !Number.isFinite(id)) return '';
  try {
    const row = db.prepare('SELECT norm FROM entities WHERE id = ?').get(id) as { norm?: unknown } | undefined;
    return normalizeEntity(String(row?.norm ?? '')).norm;
  } catch {
    return '';
  }
}

/**
 * 登记一批实体键：缺表直接返回 0，同批去重，file/config/tool/unknown 跳过。
 *
 * 外层事务里调用：enqueueObserveTask 只发单条带 ON CONFLICT 的语句，
 * 事务回滚时这些行跟着没，不会留下「记忆没写成、任务却排上」的假活。
 */
export function notifyEntitiesChanged(
  db: DatabaseSync | null | undefined,
  entityKeys: Iterable<unknown>,
  reason: ObserveReason,
  opts: NotifyOptions = {},
): number {
  if (!db || !hasObserveTaskTable(db)) return 0;
  const log = typeof opts.log === 'function' ? opts.log : null;
  let n = 0;
  for (const key of entityKeysOf(entityKeys)) {
    const etype = entityEtypeOf(db, key);
    if (SKIP_OBSERVE_ETYPES.has(etype)) {
      log?.(`[observe] 跳过非实体类 key=${key} etype=${etype}`);
      continue;
    }
    const entityId = opts.entityIdByKey?.get(key) ?? '';
    if (enqueueObserveTask(db, key, { entityId, reason, now: opts.now, log: opts.log })) n += 1;
  }
  return n;
}

/** 带开关的名字登记：observe.enabled 不是 true 就一行不写。 */
export function notifyEntityNamesChanged(
  db: DatabaseSync | null | undefined,
  cfg: ObserveSwitchConfig | null | undefined,
  names: Iterable<unknown>,
  reason: ObserveReason,
  opts: NotifyOptions = {},
): number {
  if (!observeEnabled(cfg)) return 0;
  return notifyEntitiesChanged(db, names, reason, opts);
}

/** 带开关的 id 登记：先按 id 查 norm，再走同一套去重/缺表判定（挂载晚到用）。 */
export function notifyEntityIdsChanged(
  db: DatabaseSync | null | undefined,
  cfg: ObserveSwitchConfig | null | undefined,
  entityIds: Iterable<unknown>,
  reason: ObserveReason,
  opts: NotifyOptions = {},
): number {
  if (!observeEnabled(cfg)) return 0;
  if (!db || !hasObserveTaskTable(db)) return 0;
  const byKey = new Map<string, string>();
  for (const id of entityIds) {
    const key = entityKeyOfId(db, id);
    if (!key) continue;
    if (!byKey.has(key)) byKey.set(key, String(Number(id)));
  }
  return notifyEntitiesChanged(db, byKey.keys(), reason, { ...opts, entityIdByKey: byKey });
}
