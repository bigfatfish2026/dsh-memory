/**
 * 睡觉链的日志出口：统一走 file-log.js 的 keyLog，落 cfg.sleepLogFile。
 */
import { keyLog } from '../file-log.js';
import type { KeyLogConfig } from '../file-log.js';

export function log(cfg: KeyLogConfig | null | undefined, line: unknown): void {
  keyLog(cfg, line);
}
