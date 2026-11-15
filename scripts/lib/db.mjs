/**
 * scripts/ 的 sqlite 与路径收口。
 *
 * 原来 30 个脚本各写一份 `new DatabaseSync(...)`，默认库路径也各自写死本机绝对路径。
 * 统一到这里：
 *   openDb(path?, opts?)      开库；path 省略时按环境变量/默认解析
 *   resolveDbPath(p?)         显式参数 → DSH_MEMORY_DB → 默认真库
 *   resolveMemoryRoot()       DSH_MEMORY_ROOT → cwd/memory
 *   resolveMemoryFile(name)   数据根目录下的文件
 *   resolveLogDir()           DSH_MEMORY_LOG_DIR → cwd/logs
 *   resolveLogFile(name)      日志目录下的文件
 *   resolveSessionsRoot()     DSH_SESSIONS_DIR → $DSH_HOME/sessions
 *   withTx(db, fn)            写事务（实现与 lib/shared/tx.js 同一份，不复制）
 *
 * 读脚本的护栏（rehearse / migrate-jsonl / split-path）要拿「默认真库」做比较时，
 * 用 DEFAULT_DB_PATH，不要用 resolveDbPath()——环境变量可以把它指到别处。
 */
import { DatabaseSync } from 'node:sqlite';
import os from 'node:os';
import path from 'node:path';

export { withTransaction as withTx } from '../../lib/shared/tx.js';

const trimmed = (value) => String(value ?? '').trim();

/** 数据根目录：DSH_MEMORY_ROOT，缺省 cwd/memory。 */
export function resolveMemoryRoot() {
  return trimmed(process.env.DSH_MEMORY_ROOT) || path.resolve(process.cwd(), 'memory');
}

/** 数据根目录下的一个文件。 */
export function resolveMemoryFile(name) {
  return path.join(resolveMemoryRoot(), name);
}

/** 日志目录：DSH_MEMORY_LOG_DIR，缺省 cwd/logs。 */
export function resolveLogDir() {
  return trimmed(process.env.DSH_MEMORY_LOG_DIR) || path.resolve(process.cwd(), 'logs');
}

/** 日志目录下的一个文件。 */
export function resolveLogFile(name) {
  return path.join(resolveLogDir(), name);
}

/** 会话记录根目录：DSH_SESSIONS_DIR，缺省 $DSH_HOME/sessions。 */
export function resolveSessionsRoot() {
  const home = trimmed(process.env.DSH_HOME) || path.join(os.homedir(), '.dsh');
  return trimmed(process.env.DSH_SESSIONS_DIR) || path.join(home, 'sessions');
}

/**
 * 护栏用的「默认真库」：只认 DSH_MEMORY_ROOT，不认 DSH_MEMORY_DB，
 * 免得环境变量把护栏比较的两边指成同一个路径。
 */
export const DEFAULT_DB_PATH = path.join(
  trimmed(process.env.DSH_MEMORY_ROOT) || path.resolve(process.cwd(), 'memory'),
  'memory.db',
);

/** 库路径：显式参数优先，其次 DSH_MEMORY_DB，最后默认真库。 */
export function resolveDbPath(p = null) {
  const explicit = p === undefined || p === null ? '' : String(p).trim();
  if (explicit) return explicit;
  const fromEnv = String(process.env.DSH_MEMORY_DB || '').trim();
  return fromEnv || DEFAULT_DB_PATH;
}

/** 开库：opts 原样透传（readOnly / allowExtension / timeout）。 */
export function openDb(p = null, opts = {}) {
  return new DatabaseSync(resolveDbPath(p), opts);
}
