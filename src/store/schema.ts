/**
 * memories 表的建表/迁移与健康检查。
 *
 * 这三件事都要在 store 启动时按顺序跑（ensureSchema → 开向量 → load），
 * 所以单独成模块，逻辑与拆模块前逐字一致。
 */
import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { DatabaseSync } from 'node:sqlite';
import { logStore } from './log.js';
import type { StoreState } from './types.js';

/**
 * 迁移前的整组副本：memory.db + -wal + -shm，名字带分钟戳。
 * 迁移失败时库不动（迁移脚本单事务回滚），这份副本是第二道保险。
 */
export function premigrateBackup(state: StoreState): string {
  const { dbPath, cfg } = state;
  const now = new Date();
  const pad = (n: number) => String(n).padStart(2, '0');
  const stamp = `${now.getFullYear()}${pad(now.getMonth() + 1)}${pad(now.getDate())}-${pad(now.getHours())}${pad(now.getMinutes())}`;
  let dst = `${dbPath}.premigrate-${stamp}`;
  for (let n = 1; fs.existsSync(dst) || fs.existsSync(`${dst}-wal`); n += 1) {
    dst = `${dbPath}.premigrate-${stamp}-${n}`;
  }
  try {
    state.connect().exec('PRAGMA wal_checkpoint(TRUNCATE);');
  } catch {
    /* 有别人在写就照拷，整组一起拷 */
  }
  fs.copyFileSync(dbPath, dst);
  for (const suffix of ['-wal', '-shm']) {
    if (fs.existsSync(`${dbPath}${suffix}`)) fs.copyFileSync(`${dbPath}${suffix}`, `${dst}${suffix}`);
  }
  let integrity = 'skipped';
  try {
    const copy = new DatabaseSync(dst, { readOnly: true });
    integrity = String(Object.values(copy.prepare('PRAGMA integrity_check').get() || {})[0] || 'unknown');
    copy.close();
  } catch (error) {
    integrity = `check failed: ${(error as { message?: unknown }).message}`;
  }
  logStore(cfg, 'INFO', '迁移前副本已建', `${dst}（integrity=${integrity}）`);
  return dst;
}

/**
 * memories 表缺了就做一次受控迁移：先整组备份，再调迁移脚本导 jsonl。
 * 任何一步失败都抛错，旧库和旧 jsonl 原样保留，绝不以半截状态启动。
 */
export function ensureSchema(state: StoreState): boolean {
  const { dbPath, jsonlSource, cfg } = state;
  if (state.tableExists('memories')) return false;
  if (!fs.existsSync(jsonlSource)) {
    const message = `memory.db 里没有 memories 表，也找不到导入源 ${jsonlSource}`;
    logStore(cfg, 'ERROR', '无法启动 store：库未迁移且无 jsonl 源', message);
    throw new Error(message);
  }
  const backup = premigrateBackup(state);
  // 本文件在 src/store/ 下，迁移脚本在插件根 scripts/，比原件多退一级
  const script = path.join(import.meta.dirname, '../../scripts/migrate-jsonl-to-sqlite.mjs');
  logStore(cfg, 'WARN', 'memories 表不存在，开始受控迁移', `${jsonlSource} → ${dbPath}`);
  const result = spawnSync(
    process.execPath,
    [script, '--in', jsonlSource, '--out', dbPath, '--force-live'],
    { encoding: 'utf8', timeout: 300000, maxBuffer: 32 * 1024 * 1024 },
  );
  if (result.error || result.status !== 0) {
    const tail = String(result.stderr || '').trim().split('\n').slice(-3).join(' / ');
    const message = `迁移失败 rc=${result.status}：${tail || result.error?.message || '未知错误'}`;
    logStore(cfg, 'ERROR', '迁移失败，旧库保持可用', `${message}；备份 ${backup}`);
    throw new Error(`${message}；旧库没动，迁移前副本在 ${backup}`);
  }
  if (!state.tableExists('memories')) {
    logStore(cfg, 'ERROR', '迁移脚本返回 0 但表仍不存在', backup);
    throw new Error(`迁移脚本返回 0 但 memories 表仍不存在（副本 ${backup}）`);
  }
  const last = String(result.stdout || '').trim().split('\n').filter(Boolean).slice(-1)[0] || '';
  logStore(cfg, 'INFO', '受控迁移完成', last);
  return true;
}

/** 向量表与正文表对不上的地方：只报不删（迁移脚本留下的 3 条已知孤儿保持原样） */
export function logVectorGaps(state: StoreState): { orphans: number; missing: number } | null {
  const { cfg } = state;
  try {
    const d = state.connect();
    const orphans = Number(
      d.prepare('SELECT count(*) AS c FROM vec_map v LEFT JOIN memories m ON m.id = v.id WHERE m.id IS NULL').get()?.c ?? 0,
    );
    const missing = Number(
      d.prepare('SELECT count(*) AS c FROM memories m LEFT JOIN vec_map v ON v.id = m.id WHERE v.id IS NULL').get()?.c ?? 0,
    );
    if (orphans || missing) {
      logStore(cfg, 'WARN', '向量与正文条数对不上', `孤儿向量 ${orphans} 条 / 没有向量的记忆 ${missing} 条（只报不删）`);
    }
    return { orphans, missing };
  } catch (error) {
    logStore(cfg, 'WARN', '向量核对失败', String((error && (error as { message?: unknown }).message) || error));
    return null;
  }
}
