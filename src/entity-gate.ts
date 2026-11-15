/**
 * 实体准入闸的对外面。
 *
 * 实现在 src/entity-graph.ts 的 EntityGraph 类：entity_surface/origin 缓存、实体 upsert、
 * 闸判定、挂载计数都归实例私有。本文件只保留旧导出名和旧函数签名（测试、脚本照用），
 * 每个函数按 db 取同一个实例再转调。
 */
import type { DatabaseSync } from 'node:sqlite';
import { EntityGraph, entityGraphOf, ROLES, KNOWN_ETYPES } from './entity-graph.js';
import type {
  DeleteMemoryEntityOptions,
  EntityIssueOptions,
  GateEntityOptions,
  GateVerdict,
  InsertMemoryEntityOptions,
  MaintainSurfaceOptions,
  MaintainSurfaceResult,
  SurfaceCache,
  UpsertEntityOptions,
  UpsertEntityResult,
} from './entity-graph.js';

export { EntityGraph, entityGraphOf, ROLES, KNOWN_ETYPES };

/** gateEntityName 的入参：闸判定要 db 才取得到实体图实例。 */
export type GateEntityNameOptions = GateEntityOptions & { db: DatabaseSync };

/** memory_entity 有没有 origin 列（老库可能还没跑 apply-curate-schema.mjs）。 */
export function hasOriginColumn(db: DatabaseSync): boolean {
  return entityGraphOf(db).hasOriginColumn();
}

export function activeMemoryCount(db: DatabaseSync): number {
  return entityGraphOf(db).activeMemoryCount();
}

/** 某实体的 df：在 memory_entity 里出现多少次。 */
export function entityDf(db: DatabaseSync, entityId: number): number {
  return entityGraphOf(db).entityDf(entityId);
}

/** 泛词判定：df / active > genericDf，且 df 至少到 genericDfMin（默认 20）。 */
export function isGenericEntity(db: DatabaseSync, entityId: number, genericDf = 0.05, genericDfMin = 20): boolean {
  return entityGraphOf(db).isGenericEntity(entityId, genericDf, genericDfMin);
}

export function entityIssue(db: DatabaseSync, opts: EntityIssueOptions): void {
  return entityGraphOf(db).entityIssue(opts);
}

/** entities.mem_count 的唯一维护入口。 */
export function bumpEntityMemCount(db: DatabaseSync, entityId: number, delta = 1): number {
  return entityGraphOf(db).bumpEntityMemCount(entityId, delta);
}

/** 读整张 entity_surface 进内存缓存；版本没变就复用，force=true 强制重读。 */
export function loadSurfaceCache(db: DatabaseSync, opts: { force?: boolean } = {}): SurfaceCache {
  return entityGraphOf(db).loadSurfaceCache(opts);
}

/** 按 norm/surface 查 entity_surface 取或建实体；返回 {id, etype}。 */
export function upsertEntity(db: DatabaseSync, opts: UpsertEntityOptions = {} as UpsertEntityOptions): UpsertEntityResult {
  return entityGraphOf(db).upsertEntity(opts);
}

/** 一条记忆真正挂上某实体时，同步维护 entity_surface。 */
export function maintainSurfaceForEntity(db: DatabaseSync, opts: MaintainSurfaceOptions = {}): MaintainSurfaceResult {
  return entityGraphOf(db).maintainSurfaceForEntity(opts);
}

/** 过四道闸 + df 闸，返回能不能落库。 */
export function gateEntityName(opts: GateEntityNameOptions = {} as GateEntityNameOptions): GateVerdict {
  return entityGraphOf(opts.db).gateEntityName(opts);
}

/** 落一条 memory_entity（幂等）。 */
export function insertMemoryEntity(db: DatabaseSync, opts: InsertMemoryEntityOptions = {} as InsertMemoryEntityOptions): boolean {
  return entityGraphOf(db).insertMemoryEntity(opts);
}

/** 删一批 memory_entity（按 memory_id，可选 origin）。 */
export function deleteMemoryEntity(db: DatabaseSync, opts: DeleteMemoryEntityOptions = {}): number {
  return entityGraphOf(db).deleteMemoryEntity(opts);
}

/** entity_surface.count -1。 */
export function bumpSurfaceCount(db: DatabaseSync, entityId: number, delta = -1): number {
  return entityGraphOf(db).bumpSurfaceCount(entityId, delta);
}
