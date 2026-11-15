/**
 * 整理/睡眠两条流程里「只读工具」的公共件。
 *
 * sleep.js 与 consolidate.js 原来各抄一份 safePath / collectFiles / clip + 两个常量表，
 * 逐字一样。执行器（runReadTool）还留在各自文件里，只把路径护栏和截断收在这里。
 */
import fs from 'node:fs';
import path from 'node:path';
import type { Dirent } from 'node:fs';
import * as paths from './paths.js';

/** 只读工具的根：任何路径解析后必须落在这棵树下。 */
export const READ_ROOT = paths.readRoot();

export const SKIP_DIRS = new Set(['node_modules', '.git', 'manim-venv', 'video', 'site-packages', '__pycache__', '.cache']);
export const TEXT_EXT = ['.js', '.mjs', '.json', '.jsonl', '.yml', '.yaml', '.md', '.txt', '.py', '.sh', '.ts', '.toml', '.service', '.timer', '.cfg'];

/** 白名单：只有 READ_ROOT 之内的路径可读，越界直接抛错，不做部分放行。 */
export function safePath(p: unknown): string {
  const abs = path.resolve(READ_ROOT, String(p || '.'));
  if (abs !== READ_ROOT && !abs.startsWith(READ_ROOT + path.sep)) throw new Error(`路径不在 ${READ_ROOT} 内：${p}`);
  return abs;
}

/** 递归收文件，跳过缓存/依赖目录，最多 cap 条；读不了的目录静默跳过。 */
export function collectFiles(dir: string, depth: number, out: string[], cap = 800): void {
  if (out.length >= cap) return;
  let ents: Dirent[];
  try {
    ents = fs.readdirSync(dir, { withFileTypes: true });
  } catch {
    return;
  }
  for (const e of ents) {
    if (out.length >= cap) return;
    if (SKIP_DIRS.has(e.name)) continue;
    const full = path.join(dir, e.name);
    if (e.isDirectory()) {
      if (depth > 0) collectFiles(full, depth - 1, out, cap);
    } else out.push(full);
  }
}

/** 截断长文本，尾巴留「截断，共 N 字」，模型和人都能看出截在哪。 */
export function clip(text: unknown, n = 3500): string {
  const t = String(text ?? '');
  return t.length > n ? `${t.slice(0, n)}…（截断，共 ${t.length} 字）` : t;
}
