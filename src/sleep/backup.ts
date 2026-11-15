/**
 * 备份 memory.db（含正文 + 向量）到 cfg.backupDir；memories.jsonl 冻结副本一起拷。
 * 走 SQLite 在线备份 API（store.backupTo），不裸拷活库。
 */
import fs from 'node:fs';
import path from 'node:path';
import { ensureDir } from '../shared/fs.js';
import { log } from './log.js';

/** 本文件只读备份目录与库目录（以及日志文件）。 */
interface BackupCfg {
  backupDir: string;
  storePath: string;
  sleepLogFile?: string | null;
  [key: string]: unknown;
}

/** store 只用到一个 backupTo；没有它就不备份 memory.db（与原来一致）。 */
interface BackupStore {
  backupTo?: (dst: string) => Promise<unknown> | unknown;
  [key: string]: unknown;
}

export async function snapshot(cfg: BackupCfg, store: BackupStore | null | undefined): Promise<string> {
  try {
    const stamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
    ensureDir(cfg.backupDir);
    const targets = ['memories.jsonl', 'memory.db'];
    for (const f of targets) {
      const src = path.join(cfg.storePath, f);
      if (!fs.existsSync(src)) continue;
      // 先写 .tmp 再 rename：拷到一半断电只会留下 .tmp，不会有半截的快照冒充完整备份
      const dst = path.join(cfg.backupDir, `${stamp}-${f}`);
      const tmp = `${dst}.tmp`;
      if (f === 'memory.db') {
        if (typeof store?.backupTo !== 'function') {
          log(cfg, '备份跳过 memory.db：store 没有 backupTo（不想再裸拷活库）');
          continue;
        }
        try { if (fs.existsSync(tmp)) fs.rmSync(tmp); } catch { /* 忽略 */ }
        await (store as BackupStore).backupTo!(tmp);
      } else {
        fs.copyFileSync(src, tmp);
      }
      fs.renameSync(tmp, dst);
    }
    // 上次拷到一半就断掉留下的 .tmp，顺手清掉
    const files = fs.readdirSync(cfg.backupDir).sort();
    for (const f of files) {
      if (f.endsWith('.tmp')) {
        try {
          fs.unlinkSync(path.join(cfg.backupDir, f));
        } catch {}
      }
    }
    // 只留最近 30 份快照（成对算，.tmp 不计入）
    const kept = fs.readdirSync(cfg.backupDir).filter((f) => !f.endsWith('.tmp')).sort();
    const cutoff = kept.length - 60;
    if (cutoff > 0) for (const f of kept.slice(0, cutoff)) fs.unlinkSync(path.join(cfg.backupDir, f));
    return stamp;
  } catch (err) {
    log(cfg, `[sleep] 备份失败：${(err as Error)?.message || String(err)}`);
    return '';
  }
}
