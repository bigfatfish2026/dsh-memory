/**
 * 记忆向量层：向量存 sqlite-vec，进程里不再有那份 11MB 的 Float32Array。
 *
 * 表结构
 *   vec_items(rowid, embedding float[1024] cosine)   ← vec0 虚拟表
 *   vec_map(rowid, id)                                ← rowid ↔ 记忆 id 的映射
 *
 * 重建走 scripts/rebuild_vectors.mjs（从 memories 表重新嵌入；build_vec_db.mjs 已改成转发它）。
 * 这里的写入是增量的：新记忆 insert，删掉的 remove。
 *
 * 2026-11-05 起只吃 sqlite：不再读 memories.jsonl、也不再读 vectors.bin。
 * 传入 connection 时复用调用方的连接（store 用它把正文和向量放进同一个事务）。
 */
import fs from 'node:fs';
import path from 'node:path';
import { DatabaseSync, backup } from 'node:sqlite';
import type { MemVectorsLike, VectorHit } from './store/types.js';

/** 向量层句柄：store 只认 MemVectorsLike 那几件事，这里给全量面。 */
export interface MemVectors extends MemVectorsLike {
  readonly dim: number;
  readonly rows: number;
  insert(id: string, vec: Float32Array): number;
  replace(id: string, vec: Float32Array): number;
  remove(ids: string[] | null | undefined): number;
  close(): void;
}

/** openMemVectors 的注入面。 */
export interface OpenMemVectorsOptions {
  dbPath?: string;
  dim?: number;
  logger?: { info?: (line: string) => void };
  connection?: DatabaseSync | null;
}

function findExtension(): string {
  const here = import.meta.dirname;
  const candidates = [
    path.join(here, '../node_modules/sqlite-vec-linux-arm64/vec0.so'),
  ];
  for (const c of candidates) if (fs.existsSync(c)) return c;
  throw new Error(`找不到 vec0 扩展：${candidates.join(', ')}`);
}

export function openMemVectors({ dbPath, dim = 1024, logger, connection }: OpenMemVectorsOptions = {}): MemVectors {
  if (!dbPath) throw new Error('dbPath 必填');
  let db: DatabaseSync | null = connection ?? null;
  let ownsDb = !connection;
  let cachedDim = dim;
  let ready = false;

  function ensure(): DatabaseSync {
    if (db) {
      if (!ready) {
        db.loadExtension(findExtension());
        readDim();
        ready = true;
      }
      return db;
    }
    if (!fs.existsSync(dbPath!)) {
      throw new Error(`记忆向量库还没有：${dbPath}（跑 node scripts/rebuild_vectors.mjs 重建）`);
    }
    db = new DatabaseSync(dbPath!, { allowExtension: true });
    ownsDb = true;
    db.loadExtension(findExtension());
    db.exec('PRAGMA journal_mode = WAL;');
    readDim();
    ready = true;
    logger?.info?.(`memory vectors opened: dim=${cachedDim}`);
    return db;
  }

  function readDim(): void {
    try {
      const row = db!.prepare("SELECT value FROM meta WHERE key = 'dim'").get() as { value?: unknown } | undefined;
      if (row?.value) cachedDim = Number(row.value) || dim;
    } catch {
      /* 老库没有 meta 表也无所谓，用配置里的 dim */
    }
  }

  return {
    get dim() { return cachedDim; },
    get rows() {
      try { return Number((ensure().prepare('SELECT count(*) AS n FROM vec_map').get() as { n?: unknown } | undefined)?.n ?? 0); } catch { return 0; }
    },

    /** 追加一条：rowid 自增，id 是记忆的字符串 id */
    insert(id: string, vec: Float32Array): number {
      const d = ensure();
      const next = Number((d.prepare('SELECT coalesce(max(rowid), 0) + 1 AS n FROM vec_map').get() as { n?: unknown } | undefined)?.n ?? 1);
      const u8 = new Uint8Array(vec.buffer, vec.byteOffset, vec.byteLength);
      d.prepare('INSERT INTO vec_items(rowid, embedding) VALUES (?, ?)').run(BigInt(next), u8);
      d.prepare('INSERT INTO vec_map(rowid, id) VALUES (?, ?)').run(next, String(id));
      return next;
    },

    /** 删掉若干条（按记忆 id） */
    remove(ids: string[] | null | undefined): number {
      const list = (ids || []).map(String);
      if (!list.length) return 0;
      const d = ensure();
      let n = 0;
      const sel = d.prepare('SELECT rowid FROM vec_map WHERE id = ?');
      for (const id of list) {
        const row = sel.get(id) as { rowid: number } | null | undefined;
        if (!row) continue;
        d.prepare('DELETE FROM vec_items WHERE rowid = ?').run(row.rowid);
        d.prepare('DELETE FROM vec_map WHERE rowid = ?').run(row.rowid);
        n += 1;
      }
      return n;
    },

    /** 换掉某条记忆的向量（rowid 会变，映射按 id 走） */
    replace(id: string, vec: Float32Array): number {
      this.remove([id]);
      return this.insert(id, vec);
    },

    /**
     * KNN：返回 [{ id, similarity }]，similarity = 1 - cosine 距离。
     *
     * 2026-11-05 去掉 4096 硬封：vec0 的 k 上限是编译进 .so 的 4096（实测传 4672 直接报
     * "k value in knn query too large"），所以按「距离阈值翻页」补齐：
     * 第一页 k=4096 拿最近的一批，记下最远距离 d，第二页加 `distance >= d` 再拿一批，
     * 直到凑够 vec_map 全表。4675 条实测两页 64～75ms（top10 只要一页 18ms，单页 4096 约 29ms）。
     * 全量取 embedding 走 SQL 逐行要 3 秒（vec0 读 blob 慢），这条路更快，也不需要常驻向量。
     *
     * allow（2026-11-15，审计中-4）：白名单，只把 allow 里的 id 收进结果，
     * 翻页/凑数都按白名单内已收到的条数算。recall 拿它按 active 过滤，省得把
     * archived/merged 和孤儿向量全塞进 Map；不传 allow 时行为跟以前一样。
     * @param {Float32Array} query
     * @param {number} k 想要几条
     * @param {{allow?: Iterable<string>|Set<string>}} [opts]
     */
    knn(query: Float32Array, k: number, { allow = null }: { allow?: Iterable<string> | Set<string> | null } = {}): VectorHit[] {
      const d = ensure();
      const allowSet = allow === null || allow === undefined ? null : new Set([...allow].map(String));
      if (allowSet && allowSet.size === 0) return [];
      const total = Number((d.prepare('SELECT count(*) AS c FROM vec_map').get() as { c?: unknown } | undefined)?.c ?? 0);
      const ceiling = allowSet ? allowSet.size : (total || 1);
      const want = Math.max(1, Math.min(Math.trunc(k) || 1, ceiling));
      const pageSize = 4096; // vec0 的硬上限
      const u8 = new Uint8Array(query.buffer, query.byteOffset, query.byteLength);
      const base = 'SELECT v.distance AS distance, m.id AS id FROM vec_items v JOIN vec_map m ON m.rowid = v.rowid '
        + 'WHERE v.embedding MATCH ? AND k = ?';
      const first = d.prepare(`${base} ORDER BY v.distance`);
      const next = d.prepare(`${base} AND v.distance >= ? ORDER BY v.distance`);
      const out = new Map<string, number>();
      const seen = new Set<string>();
      let threshold: unknown = null;
      for (let page = 0; page < 16 && out.size < want; page += 1) {
        // 白名单会滤掉一部分候选：第一页按「白名单占比」估需要的行数（再留一倍余量），
        // 尽量一次取够；估不准时下一轮按 4096 翻。没白名单时就是老行为 want 行。
        const ratio = allowSet ? Math.max(allowSet.size / Math.max(total, 1), 0.02) : 1;
        const pageK = threshold === null
          ? Math.min(pageSize, Math.max(want, Math.ceil(want / ratio) + want))
          : pageSize;
        const rows = threshold === null
          ? first.all(u8, BigInt(pageK)) as Array<{ id: unknown; distance: unknown }>
          : next.all(u8, BigInt(pageK), threshold as number) as Array<{ id: unknown; distance: unknown }>;
        if (rows.length === 0) break;
        let added = 0;
        let fresh = 0;
        for (const r of rows) {
          const id = String(r.id);
          if (seen.has(id)) continue;
          seen.add(id);
          fresh += 1;
          if (allowSet && !allowSet.has(id)) continue;
          out.set(id, 1 - Number(r.distance));
          added += 1;
          if (out.size >= want) break;
        }
        threshold = rows[rows.length - 1].distance;
        // 一整页全是老面孔（距离并列太多）就别再翻，免得死循环
        if (fresh === 0) break;
        if (out.size >= want) break;
        // 没有白名单时老行为：这一页一条新的都没加就停
        if (!allowSet && added === 0) break;
      }
      return [...out.entries()].map(([id, similarity]) => ({ id, similarity }));
    },

    /** 全量取回（睡觉聚类用，只用一次，别长期攥着）：Map<id, Float32Array> */
    viewAll(): Map<string, Float32Array> {
      const out = new Map<string, Float32Array>();
      const rows = ensure().prepare(
        'SELECT m.id AS id, v.embedding AS emb FROM vec_items v JOIN vec_map m ON m.rowid = v.rowid',
      ).all() as Array<{ id: unknown; emb: Uint8Array }>;
      for (const r of rows) {
        const u8 = new Uint8Array(r.emb.buffer, r.emb.byteOffset, r.emb.byteLength);
        if (u8.byteLength === cachedDim * 4) out.set(String(r.id), new Float32Array(u8.buffer, u8.byteOffset, cachedDim));
      }
      return out;
    },

    /** 把 WAL 落进主库，备份前调一下拷 .db 才是完整的 */
    checkpoint(): void {
      try { ensure().exec('PRAGMA wal_checkpoint(TRUNCATE);'); } catch { /* 忙就算了 */ }
    },

    /**
     * 真备份：走 SQLite 在线备份 API，写出一份一致的快照到 dstPath。
     * 别再 copyFileSync 裸拷这个库——活着的 WAL 库会拷出撕裂页集。
     * 目标库默认带 WAL 头，这里顺手转成单文件 delete 模式，不留 -wal/-shm。
     */
    async backupTo(dstPath: string): Promise<void> {
      const d = ensure();
      await backup(d, dstPath);
      try {
        const t = new DatabaseSync(dstPath);
        t.exec('PRAGMA journal_mode = delete;');
        t.close();
      } catch { /* 转不动也无所谓，能读就行 */ }
      for (const f of [`${dstPath}-wal`, `${dstPath}-shm`]) {
        try { if (fs.existsSync(f)) fs.rmSync(f); } catch { /* 忽略 */ }
      }
    },

    close(): void {
      // 共用连接归 store 管，这里不许替别人关
      if (!ownsDb) return;
      try { db?.close(); } catch { /* 忽略 */ }
      db = null;
      ready = false;
    },
  };
}
