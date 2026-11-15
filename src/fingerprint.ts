/**
 * 指纹原语（2026-11-14，阶段 4）。
 *
 * 全仓原来有五处各写一遍 sha1：批次指纹（sleep.js）、拆分键（split-queue.js）、
 * 未决 payload 键（store.js）、写入门正文 hash（write-gate.js）、作废 op 键（merge.js）。
 * 它们**语义不同、不能合并**：
 *   - 写入门 hash 先归一去空白标点（同一条不同排版要能撞上）；
 *   - 拆分键用原始正文（保留字节差异，避免两条不同长文被归一化成同一键）；
 *   - 批次指纹是「排序后的成员 id + 正文」；
 *   - 未决键与作废键是 JSON 结构；
 *   - reviveKeys（batch/ids/items）是字段选择，根本不是哈希。
 * 这里只收「怎么算 sha1」这一个原语，构造函数留在各自文件，语义一个字不改。
 */
import crypto from 'node:crypto';

/** sha1 十六进制；len>0 时截前 len 位（批次/拆分键用 16，未决键用全 40）。 */
export function sha1Hex(value: unknown, { len = 0 }: { len?: number } = {}): string {
  const hex = crypto.createHash('sha1').update(String(value ?? ''), 'utf8').digest('hex');
  return len > 0 ? hex.slice(0, len) : hex;
}

/**
 * 多段拼接后取 sha1。分隔符固定，防止 "ab"+"c" 和 "a"+"bc" 撞成同一个键。
 * @param {Array<string|number>} parts
 */
export function digestParts(parts: unknown, { sep = '\u0000', len = 0 }: { sep?: string; len?: number } = {}): string {
  return sha1Hex((Array.isArray(parts) ? parts : []).join(sep), { len });
}

/**
 * 对象按固定键序序列化再取 sha1：同一内容不同属性顺序必须得到同一个键
 * （作废 op 的 key 靠它；直接 JSON.stringify 会随调用方构造顺序变）。
 */
export function digestSorted(obj: unknown, { len = 0 }: { len?: number } = {}): string {
  const o: Record<string, unknown> = obj && typeof obj === 'object' ? (obj as Record<string, unknown>) : {};
  const keys = Object.keys(o).sort();
  return sha1Hex(JSON.stringify(keys.map((k) => [k, o[k]])), { len });
}
