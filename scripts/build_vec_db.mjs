#!/usr/bin/env node
/**
 * 已弃用，只做转发：转给同目录的 rebuild_vectors.mjs。
 *
 * 为什么改（2026-11-05）：老实现拿「vectors.bin 字节数 / memories.jsonl 行数」反推维度。
 * vectors.bin 从 2026-10-31 迁移后就不再写（停在 2695 条 × 1024 维），而 memories.jsonl
 * 一直涨（现在 4670 条），于是 dim 会被算成 11038720 / 4 / 4670 = 590，把 1024 维向量
 * 按 590 维切片灌进去，生成一个不可用的库，还会把 memory.db 覆盖掉。
 *
 * 重建只留一条路：scripts/rebuild_vectors.mjs（从 memories 表逐批重新嵌入，正文一条不动）。
 * 这个文件留着只为兼容旧调用和旧文档，参数照旧透传（--dir / --api / --batch）。
 */
import './rebuild_vectors.mjs';
