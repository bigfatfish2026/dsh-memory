/**
 * 关键日志的文件出口。
 *
 * 为什么要有这个：本机 cordis LoggerService 只注册了内存环形缓冲 exporter，
 * ctx.logger.info / warn 都不进 journald（app-boot 的 exporter 只在启动失败时兜底）。
 * 所以「拼 prompt 窗口」「surface 加载失败」「整理轮起止」这类必须有据可查的行，
 * 光调 ctx.logger 等于没写。这里保证它们同时落进文件。
 *
 * 写哪里：cfg.sleepLogFile，默认 $DSH_MEMORY_LOG_DIR/memory_sleep.log——
 * 跟整理日志同一个文件、同一套行格式 `[ISO] 内容`，不再另起一套命名。
 *
 * 轮转：不在这里做。该文件交给使用者自己的 logrotate 策略（daily / rotate 7 / compress /
 * delaycompress / copytruncate）——跟 memory_consolidate.log 等同一策略。下面只留一道按大小的兜底：
 * 单文件超过 32MB（logrotate 没跑成时）就只保留后一半，绝不无限长。
 * 兜底走「原地截断保尾部」，不 rename，避免跟 copytruncate 的 .1 抢名字。
 */
import fs from 'node:fs';
import path from 'node:path';
import * as paths from './shared/paths.js';

/** 默认出口；跟 index.js 的 sleepLogFile 默认值保持一致。 */
export const DEFAULT_KEY_LOG_FILE = paths.logFile('memory_sleep.log');

/** 兜底轮转阈值：32MB。正常一天几万行也不会到。 */
export const KEY_LOG_MAX_BYTES = 32 * 1024 * 1024;

/** 本文件只认这一个配置项；其余配置字段一概不看。 */
export interface KeyLogConfig {
  sleepLogFile?: string | null;
}

/** 拿这次要写的文件：配置给了就用配置，没给用默认。 */
export function keyLogFile(cfg: KeyLogConfig | null | undefined): string {
  return (cfg && cfg.sleepLogFile) || DEFAULT_KEY_LOG_FILE;
}

/**
 * 超过上限就保留尾部一半。读尾部时可能切在行中间，把第一段残行丢掉再写回。
 * 任何失败都吞掉：日志轮转不许影响主流程。
 */
function trimIfHuge(file: string, maxBytes: number): void {
  try {
    const st = fs.statSync(file);
    if (!st.isFile() || st.size <= maxBytes) return;
    const keep = Math.max(4096, Math.floor(st.size / 2));
    const buf = Buffer.alloc(keep);
    const fd = fs.openSync(file, 'r');
    try {
      fs.readSync(fd, buf, 0, keep, st.size - keep);
    } finally {
      fs.closeSync(fd);
    }
    const nl = buf.indexOf(0x0a);
    const body = nl >= 0 && nl + 1 < buf.length ? buf.subarray(nl + 1) : buf;
    fs.writeFileSync(file, body);
  } catch {
    /* 轮转失败只影响体积，不影响日志本身 */
  }
}

/**
 * 写一行关键日志。行首永远是 `[ISO 时间]`，跟 memory_sleep.log 现有格式一致。
 * @param cfg 插件配置（只用 sleepLogFile）
 * @param line 正文，别带换行
 * @returns 写进去了返回 true；写失败返回 false（不抛）
 */
export function keyLog(cfg: KeyLogConfig | null | undefined, line: unknown): boolean {
  try {
    const file = keyLogFile(cfg);
    fs.mkdirSync(path.dirname(file), { recursive: true });
    trimIfHuge(file, KEY_LOG_MAX_BYTES);
    const text = String(line).replace(/\r?\n/g, ' ⏎ ');
    fs.appendFileSync(file, `[${new Date().toISOString()}] ${text}\n`, 'utf8');
    return true;
  } catch {
    return false;
  }
}
