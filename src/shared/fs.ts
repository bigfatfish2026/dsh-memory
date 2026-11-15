/**
 * 目录/文件落盘的公共件。
 *
 * 原来 store / split-pass / entity-worker / sleep / consolidate 各写一份 ensureDir，
 * 五份里有两个签名：收目录的、收文件自己算 dirname 的。都收在这里。
 */
import fs from 'node:fs';
import path from 'node:path';

/** 确保目录存在（递归）。建不了照原样抛错，由调用方决定吞不吞。 */
export function ensureDir(dir: string): void {
  fs.mkdirSync(dir, { recursive: true });
}

/** 确保某个文件所在目录存在（日志/状态文件用）。 */
export function ensureDirFor(file: string): void {
  ensureDir(path.dirname(file));
}
