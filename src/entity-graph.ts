/**
 * 实体图：实体准入闸 + entity_surface 内存缓存 + 挂载计数。
 *
 * 迁移前是 src/entity-gate.ts 的一组函数，两张缓存（ORIGIN_CACHE / SURFACE_CACHE）挂在
 * 模块级 WeakMap 上。现在收进 EntityGraph 类：
 *   - #surface  entity_surface 整表缓存（version + byNorm/bySurface/conflicts）；
 *   - #origin   memory_entity 有没有 origin 列的探测结果；
 *   - #db       每个实例认一个连接，不另开库。
 * 每库一个实例（entityGraphOf 的 WeakMap 注册表），跟原来的 WeakMap 缓存同一生命周期。
 * 纯函数（entity-normalize 的 fold/normalize、bump 的增减语义）不包成方法。
 *
 * 四道闸（docs/dsh-memory-save-queue-2026.md §2.4.3）：
 *   A1 原文包含 —— norm 折叠后在正文里必须找得到，模型编的当场丢；
 *   A2 形态合法 —— 走 entity-normalize 的 R10-R14，剥完型号/版本为空也丢；
 *   A3 可追溯   —— memory_id 必须真在 memories 表里；
 *   A4 类别可判 —— etype 十四类之外/unknown 只进 entity_issue，不进 memory_entity。
 * 四道之外还有一道 df 闸（票面「四、泛词过滤」）：某 norm 在 memory_entity 里的出现次数
 * 除以 active 记忆数 > genericDf（默认 0.05）就不落库——回填那轮 agent 爱抽的
 * 「合并/拆分/去重/op/agent/ask/stop/t1」全在这道闸后面。
 *
 * 落库一律 INSERT OR IGNORE；去重口径是 (memory_id, entity_id)——表上 PK 还带 role
 * （历史 schema 不动），代码层先查后插，同一记忆同一实体只留一行，重跑不加行。
 */
import type { DatabaseSync } from 'node:sqlite';
import { E_TYPES, foldForMatch, normalizeEntity, isGenericName } from './shared/entity-normalize.js';

/** surface 缓存里一条 norm：主 entityId + 同 norm 的其它 id + 原文写法。 */
interface SurfaceEntry {
  entityId: number;
  ids: Set<number>;
  surfaces: Set<string>;
}

/** entity_surface 整表缓存：版本没变就复用。 */
export interface SurfaceCache {
  version: string;
  byNorm: Map<string, SurfaceEntry>;
  bySurface: Map<string, number>;
  conflicts: Map<string, number[]>;
}

/** 四道闸 + df 闸的判定结果。 */
export interface GateVerdict {
  ok: boolean;
  reason: string;
  norm: string;
  display: string;
  etype: string;
  attrs: Record<string, string>;
}

/** entityIssue 的入参。 */
export interface EntityIssueOptions {
  memoryId: string;
  norm?: string;
  raw?: string;
  reason?: string;
  detail?: string;
  now?: number;
}

/** 按 norm/surface 取或建实体的入参。 */
export interface UpsertEntityOptions {
  norm: string | null | undefined;
  display?: string;
  etype: string;
  attrs?: Record<string, string>;
  ts?: string;
  now?: number;
  log?: ((line: string) => void) | null;
  countSurface?: boolean;
}

/** 取或建实体的产物。 */
export interface UpsertEntityResult {
  id: number;
  etype: string;
}

/** 同步维护 surface 的入参。 */
export interface MaintainSurfaceOptions {
  entityId?: number;
  stamp?: string;
  log?: ((line: string) => void) | null;
}

/** 维护 surface 的结果。 */
export type MaintainSurfaceResult = 'touch' | 'insert' | 'conflict' | 'no_entity';

/** 实体准入闸的入参。 */
export interface GateEntityOptions {
  memoryId: string;
  text: string;
  rawName: string;
  etype?: string;
  attrs?: Record<string, string>;
  genericDf?: number;
  genericDfMin?: number;
}

/** 落 memory_entity 的入参。 */
export interface InsertMemoryEntityOptions {
  memoryId: string;
  entityId: number;
  ts?: string;
  attrs?: Record<string, string>;
  origin?: string;
  role?: string;
  log?: ((line: string) => void) | null;
}

/** 删 memory_entity 的入参。 */
export interface DeleteMemoryEntityOptions {
  memoryId?: string;
  origin?: string | null;
}

export const ROLES = ['about', 'subject', 'object', 'mentioned'];
/** 十四类，unknown 不在里面（unknown 只表示「规则没抽出来」）。 */
export const KNOWN_ETYPES = E_TYPES.filter((t) => t !== 'unknown');

export class EntityGraph {
  #db: DatabaseSync;

  /** memory_entity 有没有 origin 列（老库可能还没跑 apply-curate-schema.mjs）。 */
  #origin: boolean | null = null;

  /** entity_surface 整表缓存；版本没变就复用。 */
  #surface: SurfaceCache | null = null;

  constructor(db: DatabaseSync) {
    this.#db = db;
  }

  get db(): DatabaseSync {
    return this.#db;
  }

  /** memory_entity 有没有 origin 列（老库可能还没跑 apply-curate-schema.mjs）。 */
  hasOriginColumn(): boolean {
    const db = this.#db;
    if (!db) return false;
    if (this.#origin !== null) return this.#origin;
    let ok = false;
    try {
      ok = db.prepare("PRAGMA table_info('memory_entity')").all().some((r) => r.name === 'origin');
    } catch {
      ok = false;
    }
    this.#origin = ok;
    return ok;
  }

  activeMemoryCount(): number {
    try {
      return Number((this.#db.prepare("SELECT count(*) AS n FROM memories WHERE coalesce(status,'') IN ('','active')").get() as { n?: unknown } | undefined)?.n || 0);
    } catch {
      return 0;
    }
  }

  /** 某实体的 df：在 memory_entity 里出现多少次。 */
  entityDf(entityId: number): number {
    try {
      return Number((this.#db.prepare('SELECT count(*) AS n FROM memory_entity WHERE entity_id=?').get(entityId) as { n?: unknown } | undefined)?.n || 0);
    } catch {
      return 0;
    }
  }

  /**
   * 泛词判定：df / active > genericDf，且 df 至少到 genericDfMin（默认 20）。
   * 加最小次数是因为小库（或刚建的新库）里 1 条 active 记忆配 1 条 memory_entity 就是 100%，
   * 只按比例判会把所有实体一刀切成泛词。
   */
  isGenericEntity(entityId: number, genericDf = 0.05, genericDfMin = 20): boolean {
    const active = this.activeMemoryCount();
    if (!active) return false;
    const df = this.entityDf(entityId);
    if (df < genericDfMin) return false;
    return df / active > genericDf;
  }

  entityIssue({ memoryId, norm, raw, reason, detail, now = Date.now() }: EntityIssueOptions): void {
    try {
      this.#db.prepare(
        `INSERT OR IGNORE INTO entity_issue(memory_id, entity_norm, raw_name, reason, detail, created_at)
         VALUES (?, ?, ?, ?, ?, ?)`,
      ).run(String(memoryId), String(norm || ''), String(raw || ''), String(reason || ''), String(detail || ''), new Date(now).toISOString());
    } catch {
      /* 没有 entity_issue 表就算了，别把队列搞死 */
    }
  }

  /**
   * entities.mem_count 的唯一维护入口。
   *
   * 只跟着 memory_entity 的真实增删走：插入一行 +1、删一行 -1，减到 0 就停。
   * 不在 upsertEntity 里算——实体是 get-or-create，跟「挂了几条记忆」没有因果关系，
   * 早先写死 0 又不累加，这列就永远追不上（当天全表重算 151 条，3 分钟漂 17 条）。
   * 表/列不在就静默跳过：计数不许挡住记忆写入。
   */
  bumpEntityMemCount(entityId: number, delta = 1): number {
    const d = Number(delta) || 0;
    if (!d) return 0;
    try {
      const r = this.#db
        .prepare('UPDATE entities SET mem_count = max(0, coalesce(mem_count, 0) + ?) WHERE id = ?')
        .run(d, Number(entityId));
      return Number(r.changes) || 0;
    } catch {
      return 0;
    }
  }

  #surfaceSchemaVersion(): string {
    try {
      // entity_surface_epoch：实体整理（合并/撤销）后由 src/entity-tidy.ts 自增。
      // 光靠 schema 版本不够——数据变了缓存也得重读，否则合并完的旧映射会一直指到已退场的实体。
      // 旧库没有这个键时取 ''，拼出来的版本串和以前一样，行为不变。
      const rows = this.#db.prepare(
        "SELECT key, value FROM meta WHERE key IN ('entity_schema_version','entity_surface_epoch')",
      ).all();
      const get = (k: string): string => {
        const row = rows.find((r) => String(r.key) === k);
        return row ? String(row.value ?? '') : '';
      };
      const schema = get('entity_schema_version');
      const epoch = get('entity_surface_epoch');
      // 没有 epoch 键（老库/没合过实体）时返回纯 schema 版本，跟以前逐字一致。
      return epoch ? `${schema}:${epoch}` : schema;
    } catch {
      // meta 表都没有的更老库当空版本；surface 表读不到会在下面抛。
      return '';
    }
  }

  /**
   * 读整张 entity_surface 进内存缓存。版本没变就复用；force=true 强制重读。
   * @returns {{version:string, byNorm:Map, bySurface:Map, conflicts:Map}}
   */
  loadSurfaceCache({ force = false }: { force?: boolean } = {}): SurfaceCache {
    const version = this.#surfaceSchemaVersion();
    const cached = this.#surface;
    if (!force && cached && cached.version === version) return cached;
    // 故意不 catch：表不在/库坏了必须炸，不许拿空表顶上。
    const rows = this.#db.prepare('SELECT surface, norm, entity_id FROM entity_surface').all();
    const cache: SurfaceCache = { version, byNorm: new Map(), bySurface: new Map(), conflicts: new Map() };
    for (const r of rows) {
      const norm = String(r.norm);
      const surface = String(r.surface);
      const entityId = Number(r.entity_id);
      let entry = cache.byNorm.get(norm);
      if (!entry) {
        entry = { entityId, ids: new Set([entityId]), surfaces: new Set<string>() };
        cache.byNorm.set(norm, entry);
      } else {
        entry.ids.add(entityId);
        if (entityId < entry.entityId) entry.entityId = entityId;
      }
      entry.surfaces.add(surface);
      cache.bySurface.set(surface, entityId);
    }
    for (const [norm, entry] of cache.byNorm) {
      if (entry.ids.size > 1) cache.conflicts.set(norm, [...entry.ids].sort((a, b) => a - b));
    }
    this.#surface = cache;
    return cache;
  }

  #rememberSurface(cache: SurfaceCache, { surface, norm, entityId }: { surface: string; norm: string; entityId: number }): void {
    let entry = cache.byNorm.get(norm);
    if (!entry) {
      entry = { entityId, ids: new Set([entityId]), surfaces: new Set<string>() };
      cache.byNorm.set(norm, entry);
    } else {
      entry.ids.add(entityId);
      if (entityId < entry.entityId) entry.entityId = entityId;
    }
    entry.surfaces.add(surface);
    cache.bySurface.set(surface, entityId);
    if (entry.ids.size > 1) cache.conflicts.set(norm, [...entry.ids].sort((a, b) => a - b));
  }

  /** 命中已有 surface 行：只累加计数，绝不动既有行的 norm（2841 行不迁移）。 */
  #touchSurface({ surface, stamp }: { surface: string; stamp: string }): void {
    this.#db.prepare('UPDATE entity_surface SET count = count + 1, last_seen = ? WHERE surface = ?').run(stamp, surface);
  }

  /**
   * 新写法落 surface 行；主键撞了就改成累加，幂等。
   * seedCount 是这一行落库时自带的出现次数：正常建行=1；异步补写且随后 memory_entity
   * 会统一计数时给 0（否则一次出现会被记两遍）。
   */
  #insertSurface({ surface, norm, entityId, stamp, seedCount = 1 }: { surface: string; norm: string; entityId: number; stamp: string; seedCount?: number }): void {
    const flag = norm.includes('/') && !norm.startsWith('@') ? 'path' : '';
    const r = this.#db.prepare(
      `INSERT OR IGNORE INTO entity_surface(surface, norm, entity_id, source, count, first_seen, last_seen, flag)
       VALUES (?, ?, ?, 'gate', ?, ?, ?, ?)`,
    ).run(surface, norm, Number(entityId), Number(seedCount) || 0, stamp, stamp, flag);
    if (Number(r.changes) === 0) this.#touchSurface({ surface, stamp });
  }

  /**
   * 按 norm/surface 查 entity_surface 取或建实体；返回 {id, etype}。etype 冲突时按「已有为准」。
   *
   * 三条分支：命中已有 surface → 用它的 entity_id；未命中但 norm 有主 → 建 surface 行挂上去；
   * 两样都没有 → 建 entities 行（norm 列照旧双写）+ surface 行（source='gate'）。
   * 同 norm 指过多个实体 → entity_issue(reason='surface_conflict')，用已有那条的 entity_id。
   * 不在函数里开事务：调用方（entity-worker）的 BEGIN IMMEDIATE 里写库，Map 同事务更新。
   *
   * countSurface：默认 true，保持「调一次 upsertEntity 就记一次出现」
   * 的老语义（backfill / 测试都靠它）。entity-worker 传 false——它紧跟着一定会调
   * insertMemoryEntity，计数统一由那边在「memory_entity 真插进一行」时做，避免同一次出现
   * 被 upsert 和 insert 各记一遍。这里仍负责把 surface 行建出来（seedCount=0，等 insert 记账）。
   */
  upsertEntity({ norm, display, etype, attrs = {}, ts = '', now = Date.now(), log = null, countSurface = true }: UpsertEntityOptions): UpsertEntityResult {
    const db = this.#db;
    const stamp = ts || new Date(now).toISOString();
    const key = String(norm);
    const surface = String(display || key).trim() || key;
    // 出口由调用方注入（线上唯一热路径是 entity-worker，传的是 keyLog(cfg)）。
    // 默认不写文件：单元测试/一次性脚本不该往生产 memory_sleep.log 里写行。
    // 加载失败若没人接，错误本身会往上抛，由 worker 的整批失败日志落地。
    const emit = typeof log === 'function' ? log : () => {};
    let cache: SurfaceCache;
    try {
      cache = this.loadSurfaceCache();
    } catch (error) {
      emit(`[surface] 加载失败，entity_surface 读不到、实体写入中止：${String((error && (error as { message?: unknown }).message) || error)}`);
      throw error;
    }

    const knownId = cache.bySurface.get(surface);
    const normEntry = cache.byNorm.get(key);
    const conflictIds = cache.conflicts.get(key);

    if (conflictIds && conflictIds.length > 1) {
      const resolved = knownId !== undefined ? knownId : normEntry ? normEntry.entityId : conflictIds[0];
      this.entityIssue({
        memoryId: '',
        norm: key,
        raw: surface,
        reason: 'surface_conflict',
        detail: JSON.stringify({ norm: key, existing: conflictIds, surface, resolved }),
        now,
      });
      // 同一个 norm 指过多个实体：进 entity_issue 之外再落一行日志，排障不用去查表
      emit(`[surface] conflict norm=${key} surface=${surface} existing=${conflictIds.join(',')} resolved=${resolved}`);
    }

    let entityId: number;
    if (knownId !== undefined) {
      // ① 原文写过：沿用它指的实体，只加计数（countSurface=false 时留给 insertMemoryEntity 记）。
      entityId = knownId;
      if (countSurface) this.#touchSurface({ surface, stamp });
      this.#rememberSurface(cache, { surface, norm: key, entityId });
    } else if (normEntry) {
      // ② norm 认过：新写法挂到同一个实体上，不另建。
      entityId = normEntry.entityId;
      this.#insertSurface({ surface, norm: key, entityId, stamp, seedCount: countSurface ? 1 : 0 });
      this.#rememberSurface(cache, { surface, norm: key, entityId });
    } else {
      // ③ 都没认过：建实体（norm 双写），再建 surface 行。老实体没 surface 行时复用不重造。
      let row = db.prepare('SELECT id, etype FROM entities WHERE norm=?').get(key) as { id?: unknown; etype?: unknown } | undefined;
      if (!row) {
        // mem_count 建行给 0，真正的计数由 insertMemoryEntity 落行时 +1（不在这里猜）。
        db.prepare(
          `INSERT OR IGNORE INTO entities(norm, display, etype, attrs, first_seen, last_seen, mem_count, status)
           VALUES (?, ?, ?, ?, ?, ?, 0, 'active')`,
        ).run(key, surface, etype, JSON.stringify(attrs || {}), stamp, stamp);
        row = db.prepare('SELECT id, etype FROM entities WHERE norm=?').get(key) as { id?: unknown; etype?: unknown } | undefined;
      }
      entityId = Number(row!.id);
      this.#insertSurface({ surface, norm: key, entityId, stamp, seedCount: countSurface ? 1 : 0 });
      this.#rememberSurface(cache, { surface, norm: key, entityId });
    }

    const row = db.prepare('SELECT id, etype FROM entities WHERE id=?').get(Number(entityId)) as { etype?: unknown } | undefined;
    if (row) db.prepare('UPDATE entities SET last_seen=? WHERE id=?').run(stamp, Number(entityId));
    return { id: Number(entityId), etype: String(row?.etype || etype) };
  }

  /**
   * 一条记忆真正挂上某实体时，同步维护 entity_surface。
   *
   * 为什么必须同步：memory_save 的粗实体路径直接拿 entities 里现成的 id 插 memory_entity，
   * 以前完全不碰 surface，全靠 entity_extract 异步补。进程挂掉/热重载/队列积压时
   * surface.count 就漏记——这列以后要拿来判同义权重，不能是脏的。
   *
   * 防重复计数只有一条：调用点放在 insertMemoryEntity 里、且只在 INSERT 真落了一行
   * （changes=1）时调用。同一 (memory_id, entity_id, role) 重跑/异步补写会被 INSERT OR IGNORE
   * 挡掉，changes=0 就不进这里，所以一次挂载永远只 +1。
   *
   * 不查全表：按 id 取 entities 一行拿 norm/display，然后复用 upsertEntity 那份 surface 缓存。
   * @returns {'touch'|'insert'|'conflict'|'no_entity'}
   */
  maintainSurfaceForEntity({ entityId, stamp = '', log = null }: MaintainSurfaceOptions = {}): MaintainSurfaceResult {
    const db = this.#db;
    const emit = typeof log === 'function' ? log : () => {};
    let row: { norm?: unknown; display?: unknown } | undefined;
    try {
      row = db.prepare('SELECT norm, display FROM entities WHERE id=?').get(Number(entityId)) as { norm?: unknown; display?: unknown } | undefined;
    } catch (error) {
      emit(`[surface] 维护失败，entities 读不到 id=${entityId}：${String((error && (error as { message?: unknown }).message) || error)}`);
      return 'no_entity';
    }
    if (!row) {
      emit(`[surface] 维护跳过，实体不存在 id=${entityId}`);
      return 'no_entity';
    }
    const key = String(row.norm);
    const surface = String(row.display || key).trim() || key;
    const at = stamp || new Date().toISOString();
    const cache = this.loadSurfaceCache();
    const known = cache.bySurface.get(surface);
    if (known === Number(entityId)) {
      this.#touchSurface({ surface, stamp: at });
      this.#rememberSurface(cache, { surface, norm: key, entityId: Number(entityId) });
      return 'touch';
    }
    if (known !== undefined) {
      // 这个原文已经指向别的实体：不抢它的计数，冲突照旧落 entity_issue + 一行日志
      this.entityIssue({
        memoryId: '',
        norm: key,
        raw: surface,
        reason: 'surface_conflict',
        detail: JSON.stringify({ norm: key, existing: [known, Number(entityId)], surface, resolved: known }),
        now: Date.parse(at) || Date.now(),
      });
      emit(`[surface] conflict norm=${key} surface=${surface} existing=${known},${entityId} resolved=${known}`);
      return 'conflict';
    }
    // 这个实体还没有自己的 surface 行：建一行并记 1 次（INSERT OR IGNORE 撞键时内部会转成 touch）
    this.#insertSurface({ surface, norm: key, entityId: Number(entityId), stamp: at, seedCount: 1 });
    this.#rememberSurface(cache, { surface, norm: key, entityId: Number(entityId) });
    return 'insert';
  }

  /**
   * 过四道闸 + df 闸，返回能不能落库。
   * @returns {{ok:boolean, reason:string, norm:string, display:string, etype:string, attrs:object}}
   */
  gateEntityName({ memoryId, text, rawName, etype, attrs = {}, genericDf = 0.05, genericDfMin = 20 }: GateEntityOptions): GateVerdict {
    const db = this.#db;
    const raw = String(rawName || '').trim();
    const failed = (reason: string, extra: Partial<GateVerdict> = {}): GateVerdict => ({ ok: false, reason, norm: '', display: raw, etype: 'unknown', attrs: {}, ...extra });
    if (!raw) return failed('empty');
    const foldedText = foldForMatch(text);
    // A1：原文包含
    if (!foldedText.includes(foldForMatch(raw))) return failed('not_in_source');
    const n = normalizeEntity(raw);
    // A2：形态合法（单字/纯数字/纯符号/超长/成句一律不收）
    if (n.drop) return failed(n.drop);
    if (!n.norm) return failed('empty_norm');
    // 泛词黑名单先于 df 闸：这几类词 df 还低，比例闸拦不到（票面四）
    if (isGenericName(n.norm)) return failed('generic', { norm: n.norm, display: raw, etype: 'concept', attrs: {} });
    // A3：可追溯
    let exists: { ok?: unknown } | null | undefined = null;
    try {
      exists = db.prepare('SELECT 1 AS ok FROM memories WHERE id=?').get(String(memoryId)) as { ok?: unknown } | undefined;
    } catch {
      exists = null;
    }
    if (!exists) return failed('memory_missing');
    // A4：类别可判
    const type = KNOWN_ETYPES.includes(String(etype || '').toLowerCase()) ? String(etype).toLowerCase() : n.etype;
    const merged = { ...(n.attrs || {}), ...(attrs || {}) };
    if (!KNOWN_ETYPES.includes(type)) {
      return failed('etype_unknown', { norm: n.norm, display: n.display || raw, attrs: merged });
    }
    // df 闸：太泛的 norm 不落库。只拦 concept —— agent 抽到「树莓派」(df 642/4139=15.5%)
    // 或「dsh-memory」(1131/4139=27%) 是正确结果，不能因为库里这类专名本来就高频就拒掉。
    // 票面四要拦的是「合并/拆分/去重/op/agent/ask」这一类概念词，它们都归 concept/unknown。
    const existing = db.prepare('SELECT id FROM entities WHERE norm=?').get(n.norm) as { id?: unknown } | undefined;
    if (existing && type === 'concept' && this.isGenericEntity(Number(existing.id), genericDf, genericDfMin)) {
      return failed('generic', { norm: n.norm, display: n.display || raw, etype: type, attrs: merged });
    }
    return { ok: true, reason: '', norm: n.norm, display: n.display || raw, etype: type, attrs: merged };
  }

  /**
   * 落一条 memory_entity（幂等）。
   *
   * 去重口径是 (memory_id, entity_id) 而不是表上的 PK (memory_id, entity_id, role)
   * （2026-11-15，审计实体中-9）：同一记忆对同一实体只留一行，
   * 否则同一条记忆换个 role 重跑就多一行，mem_count 数行不数记忆。
   * 已有行时不再插、也不再 +1；role 保持先落的那一行（agent 后补 about 不覆盖 rule）。
   * 真插进去一行才给 entities.mem_count +1（IGNORE 掉的不算），并在同一笔里同步维护
   * entity_surface——「行真的新增了」就是唯一计数口径，
   * 同步路径和异步路径都走这里，重复计数没有第二个入口。
   * @returns {boolean} 新插入了返回 true（已有行/IGNORE 掉就是 false）
   */
  insertMemoryEntity({ memoryId, entityId, ts = '', attrs = {}, origin = 'agent', role = 'about', log = null }: InsertMemoryEntityOptions): boolean {
    const db = this.#db;
    // (memory_id, entity_id) 已有一行就不动：多 role 只是同一次挂载的不同说法
    try {
      const exists = db.prepare('SELECT 1 AS ok FROM memory_entity WHERE memory_id=? AND entity_id=?')
        .get(String(memoryId), Number(entityId));
      if (exists) return false;
    } catch {
      /* 表不在下面 INSERT 会自己炸，交给调用方 */
    }
    const withOrigin = this.hasOriginColumn();
    const sql = withOrigin
      ? `INSERT OR IGNORE INTO memory_entity(memory_id, entity_id, role, ts, status, attrs, version, prev_memory_id, origin)
         VALUES (?, ?, ?, ?, '', ?, 0, '', ?)`
      : `INSERT OR IGNORE INTO memory_entity(memory_id, entity_id, role, ts, status, attrs, version, prev_memory_id)
         VALUES (?, ?, ?, ?, '', ?, 0, '')`;
    const args = withOrigin
      ? [String(memoryId), Number(entityId), role, String(ts || ''), JSON.stringify(attrs || {}), String(origin || '')]
      : [String(memoryId), Number(entityId), role, String(ts || ''), JSON.stringify(attrs || {})];
    const r = db.prepare(sql).run(...args);
    const inserted = Number(r.changes) === 1;
    if (inserted) {
      this.bumpEntityMemCount(entityId, 1);
      try {
        this.maintainSurfaceForEntity({ entityId, stamp: String(ts || ''), log });
      } catch (error) {
        // surface 是派生数据，坏了不许回滚记忆本身；留一行给排障
        const emit = typeof log === 'function' ? log : () => {};
        emit(`[surface] 维护异常（memory_entity 已落 ${memoryId}/${entityId}，surface 可能漏记）：${String((error && (error as { message?: unknown }).message) || error)}`);
      }
    }
    return inserted;
  }

  /**
   * 删一批 memory_entity（按 memory_id，可选 origin）。
   *
   * origin 不传（默认 null）= 按 memory_id 全删，rule / agent / backfill / manual 一起清；
   * 传 origin 只删那一批。正文重写、记忆合并都要走默认全删，否则 agent 补的旧挂载会留在
   * 新正文上（2026-11-15，审计实体中-8）。
   *
   * 计数口径：mem_count 和 entity_surface.count 都是「挂了几条记忆」，不是「几行」——
   * 同一实体在一段过渡期可能有多 role 行（PK 含 role），删的时候按 distinct entity_id 各 -1
   * 一次，别把一条记忆记成两次。surface 没有对应行（老库/冲突行）就静默跳过。
   * @returns {number} 实际删掉的行数
   */
  deleteMemoryEntity({ memoryId, origin = null }: DeleteMemoryEntityOptions = {}): number {
    const db = this.#db;
    const byOrigin = origin !== null && origin !== undefined;
    let rows: Array<{ entity_id: number }> = [];
    try {
      rows = byOrigin
        ? db.prepare('SELECT DISTINCT entity_id FROM memory_entity WHERE memory_id=? AND origin=?')
          .all(String(memoryId), String(origin)) as unknown as Array<{ entity_id: number }>
        : db.prepare('SELECT DISTINCT entity_id FROM memory_entity WHERE memory_id=?')
          .all(String(memoryId)) as unknown as Array<{ entity_id: number }>;
    } catch {
      return 0;
    }
    if (!rows.length) return 0;
    let removed = 0;
    try {
      const r = byOrigin
        ? db.prepare('DELETE FROM memory_entity WHERE memory_id=? AND origin=?').run(String(memoryId), String(origin))
        : db.prepare('DELETE FROM memory_entity WHERE memory_id=?').run(String(memoryId));
      removed = Number(r.changes) || 0;
    } catch {
      return 0;
    }
    for (const row of rows) {
      this.bumpEntityMemCount(row.entity_id, -1);
      this.bumpSurfaceCount(row.entity_id, -1);
    }
    return removed;
  }

  /**
   * entity_surface.count -1（2026-11-15，审计实体中-7）。
   * 只动「属于这个实体」的 surface 行（surface 主键可能被 norm 冲突的另一个实体占着，
   * 那种行不动）。表/列不在就静默跳过：计数不许挡住删挂载。
   */
  bumpSurfaceCount(entityId: number, delta = -1): number {
    const d = Number(delta) || 0;
    if (!d) return 0;
    try {
      const row = this.#db.prepare('SELECT norm, display FROM entities WHERE id=?').get(Number(entityId)) as { norm?: unknown; display?: unknown } | undefined;
      if (!row) return 0;
      const key = String(row.norm || '');
      const surface = String(row.display || key).trim() || key;
      const r = this.#db.prepare(
        'UPDATE entity_surface SET count = max(0, coalesce(count, 0) + ?) WHERE surface = ? AND entity_id = ?',
      ).run(d, surface, Number(entityId));
      return Number(r.changes) || 0;
    } catch {
      return 0;
    }
  }
}

/** 每库一张实体图：surface/origin 缓存跟实例走，和原来的模块级 WeakMap 同一生命周期。 */
const GRAPHS = new WeakMap<DatabaseSync, EntityGraph>();

export function entityGraphOf(db: DatabaseSync): EntityGraph {
  let graph = GRAPHS.get(db);
  if (!graph) {
    graph = new EntityGraph(db);
    GRAPHS.set(db, graph);
  }
  return graph;
}
