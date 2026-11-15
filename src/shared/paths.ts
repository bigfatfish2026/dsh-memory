/**
 * 路径解析的唯一入口：环境变量优先，其次相对 cwd 的默认。
 *
 * 开源版不出现任何本机绝对路径。约定：
 * DSH_MEMORY_ROOT 记忆数据根目录（库、state、backup 都在这儿）
 * DSH_MEMORY_LOG_DIR 日志目录
 * DSH_MEMORY_DB 库文件路径，优先级最高
 * DSH_HOME DSH 主目录（会话记录默认在 $DSH_HOME/sessions）
 * DSH_READ_ROOT 只读工具允许访问的根
 */
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';

const trimmed = (value: unknown): string => String(value ?? '').trim();

/** 记忆数据根目录：DSH_MEMORY_ROOT，缺省 cwd/memory。 */
export function memoryRoot(): string {
  return trimmed(process.env.DSH_MEMORY_ROOT) || path.resolve(process.cwd(), 'memory');
}

/** 日志目录：DSH_MEMORY_LOG_DIR，缺省 cwd/logs。 */
export function logDir(): string {
  return trimmed(process.env.DSH_MEMORY_LOG_DIR) || path.resolve(process.cwd(), 'logs');
}

/** 数据根目录下的一个文件。 */
export function memoryFile(name: string): string {
  return path.join(memoryRoot(), name);
}

/** 日志目录下的一个文件。 */
export function logFile(name: string): string {
  return path.join(logDir(), name);
}

/** 默认库文件：DSH_MEMORY_DB 优先，其次 $DSH_MEMORY_ROOT/memory.db。 */
export function dbPath(): string {
  return trimmed(process.env.DSH_MEMORY_DB) || memoryFile('memory.db');
}

/** DSH 主目录：DSH_HOME，缺省用户主目录下的 .dsh。 */
export function dshHome(): string {
  return trimmed(process.env.DSH_HOME) || path.join(os.homedir(), '.dsh');
}

/** 会话记录根目录：DSH_SESSIONS_DIR，缺省 $DSH_HOME/sessions。 */
export function sessionsRoot(): string {
  return trimmed(process.env.DSH_SESSIONS_DIR) || path.join(dshHome(), 'sessions');
}

/** 只读工具允许访问的根：DSH_READ_ROOT，缺省当前工作目录。 */
export function readRoot(): string {
  return trimmed(process.env.DSH_READ_ROOT) || process.cwd();
}

/** 推送脚本：DSH_NOTIFY_SCRIPT，缺省 cwd/notify.sh。 */
export function notifyScript(): string {
  return trimmed(process.env.DSH_NOTIFY_SCRIPT) || path.resolve(process.cwd(), 'notify.sh');
}

/** 价档判据模块：DSH_PRICE_CLOCK_FILE，其次 cwd 下的 plugins/ 或同级 dsh-price-clock。 */
export function priceClockFile(): string {
  const explicit = trimmed(process.env.DSH_PRICE_CLOCK_FILE);
  if (explicit) return explicit;
  const candidates = [
    path.resolve(process.cwd(), 'plugins/dsh-price-clock/lib/clock.js'),
    path.resolve(process.cwd(), '../dsh-price-clock/lib/clock.js'),
  ];
  return candidates.find((file) => {
    try { return fs.existsSync(file); } catch { return false; }
  }) || candidates[0];
}
