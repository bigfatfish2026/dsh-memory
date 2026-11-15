/**
 * 整理链的统一并发口径（2026-11-14 维护者拍板：全段同一套自适应，不许 A 段自适应、B 段写死 3）。
 *
 * 三个概念分清楚：
 *   cap   理论上限：organizeIngestConcurrency（默认 64，硬顶也是 64）。
 *         再叠一层 organizeConcurrency 当「手工上限覆盖」（0/不填 = 不覆盖），
 *         这是唯一允许写死的口子，且只许往下压，不许变成某一段自己的独立硬顶。
 *   snap  负载快照：/proc/loadavg + /proc/meminfo + 核数 + 温度，**一轮只取一次**，
 *         各段都拿这一份算，下一轮重算（免得每批重算导致路数抖动）。
 *   conc  本轮该段实际路数：adaptIngestConcurrency 按 cap × CPU 余量、内存、温度算出来。
 *
 * organizeIngestAdaptive=false 时全部段回落到 cap（固定值，行为可预期，回滚口子）。
 */
import fs from 'node:fs';
import os from 'node:os';

/** 并发相关配置项：只读这几项，其余一概不看。 */
export interface ConcurrencyConfig {
  organizeIngestConcurrency?: number;
  organizeConcurrency?: number;
  organizeIngestAdaptive?: boolean;
  [key: string]: unknown;
}

/** 一轮负载快照（/proc + node:os），读不到的项给 NaN / null。 */
export interface LoadSnapshot {
  load1: number;
  cores: number;
  memAvailableBytes: number;
  tempC: number | null;
}

/** 本轮路数与依据（自适应那一步的产物）。 */
export interface ConcurrencyBasis {
  conc: number;
  basis: string;
}

/** 一次 selectConcurrency 的结果（日志与池都用这一份）。 */
export interface ConcurrencyPick {
  conc: number;
  cap: number;
  adaptive: boolean;
  basis: string;
  snapshot: LoadSnapshot | null;
}

/**
 * 入库并发：原来是默认 8、硬顶 8。
 * 2026-11-14：默认与硬顶一起提到 64——上游两家都扛得住（DeepSeek 官方账号级
 * deepseek-flash 2500 / v4-pro 500，opencode-go 网关侧无并发数字，本机 magpie 不排队），
 * 本机只多 64 个 socket + 几十 MB 上下文，ulimit -n 524288 远远够。
 * 配置在 Config 里正规定义（src/index.ts），这里再夹一次，老配置/手写 cfg 配大了也不超。
 */
export function ingestConcurrencyOf(cfg: ConcurrencyConfig | null | undefined): number {
  return Math.min(Math.max(Number(cfg?.organizeIngestConcurrency) || 64, 1), 64);
}

export const INGEST_HARD_CAP = 64;
const INGEST_SLOT_MEM_BYTES = 100 * 1024 * 1024;
const INGEST_MEM_RESERVE_BYTES = 1024 * 1024 * 1024;

/**
 * 读本机负载快照：全走 /proc + node:os，不装依赖。
 * 读不到就给 NaN / null，由算法把这一项当「不设限」处理——单个探针失败不许把并发压到 0。
 * tempC 拿不到热区文件（非 Linux、容器里没挂）就是 null，跳过温度项。
 */
export function ingestLoadSnapshot(): LoadSnapshot {
  let load1 = NaN;
  try {
    const first = fs.readFileSync('/proc/loadavg', 'utf8').trim().split(/\s+/)[0];
    load1 = Number(first);
  } catch { /* 读不到就当没负载，只按内存和上限算 */ }
  let memAvailableBytes = NaN;
  try {
    const m = /^MemAvailable:\s+(\d+)\s+kB/m.exec(fs.readFileSync('/proc/meminfo', 'utf8'));
    if (m) memAvailableBytes = Number(m[1]) * 1024;
  } catch { /* 读不到内存就只按 CPU 算 */ }
  let cores = 0;
  try {
    cores = os.cpus().length;
  } catch { /* 拿不到核数按 1 核保守估 */ }
  let tempC: number | null = null;
  try {
    const milli = Number(fs.readFileSync('/sys/class/thermal/thermal_zone0/temp', 'utf8').trim());
    if (Number.isFinite(milli)) tempC = milli / 1000;
  } catch { /* 没有热区文件就跳过温度这一项 */ }
  return { load1, cores, memAvailableBytes, tempC };
}

/**
 * 负载自适应并发：整理以等模型回包为主，CPU 几乎不动，
 * 所以 CPU 项不直接给路数，只做「按余量缩放 cap」的系数；无脑开 64 会抢内存、热天更糟。
 *   - CPU 系数按 util = load1 / cores 分四段：
 *       util ≤ 0.5        → 1.0   （空闲，接近满 cap）
 *       0.5 < util ≤ 1.0  → 1.5 − util（0.5→1.0，1.0→0.5）
 *       1.0 < util ≤ 2.0  → 0.5 / util（1.0→0.5，2.0→0.25）
 *       util > 2.0        → 0.1
 *     cpu 路数 = floor(cap × 系数)；
 *   - 内存余量：MemAvailable 扣掉 1GB 保留后，每路按 100MB 估能塞几路；
 *   - 取两者较小值，夹到 [1, cap]；
 *   - 温度 ≥75℃ 对折，≥80℃ 直接压到 1；
 * 结果至少 1，绝不为 0。cap 是理论上限（默认 64，硬顶也是 64）。
 */
export function adaptIngestConcurrency({
  cap = INGEST_HARD_CAP, load1, cores, memAvailableBytes, tempC,
}: { cap?: number; load1?: number; cores?: number; memAvailableBytes?: number; tempC?: number | null } = {}): ConcurrencyBasis {
  const upper = Math.min(Math.max(Number(cap) || INGEST_HARD_CAP, 1), INGEST_HARD_CAP);
  const nCores = Math.max(Number(cores) || 1, 1);
  const load = Number.isFinite(Number(load1)) ? Math.max(Number(load1), 0) : 0;
  const util = load / nCores;
  let factor;
  if (util <= 0.5) factor = 1;
  else if (util <= 1) factor = 1.5 - util;
  else if (util <= 2) factor = 0.5 / util;
  else factor = 0.1;
  const cpuSlots = Math.floor(upper * factor);
  const memAvail = Number(memAvailableBytes);
  const memSlots = Number.isFinite(memAvail)
    ? Math.floor((memAvail - INGEST_MEM_RESERVE_BYTES) / INGEST_SLOT_MEM_BYTES)
    : Number.POSITIVE_INFINITY;

  let basis = 'cap';
  let conc = Math.min(upper, cpuSlots);
  if (cpuSlots < upper) basis = 'cpu';
  if (memSlots < conc) {
    conc = memSlots;
    basis = 'mem';
  }

  const temp = Number(tempC);
  if (tempC !== null && tempC !== undefined && Number.isFinite(temp)) {
    if (temp >= 80) {
      conc = 1;
      basis = 'temp80';
    } else if (temp >= 75) {
      conc = Math.floor(conc / 2);
      basis = 'temp75';
    }
  }

  return { conc: Math.min(Math.max(conc, 1), upper), basis };
}

/**
 * curate 通道的理论上限：organizeIngestConcurrency 打底，organizeConcurrency>0 时当手工上限覆盖。
 * 2026-11-14 收敛：organizeConcurrency 从「merge 自己的写死值」降级成「只压 curate 通道的上限覆盖」，
 * 不再有任何一段拿它当独立并发数；ingest 的 cap 仍只认 organizeIngestConcurrency。
 */
export function curateCapOf(cfg: ConcurrencyConfig | null | undefined): number {
  const base = ingestConcurrencyOf(cfg);
  const override = Number(cfg?.organizeConcurrency);
  if (Number.isFinite(override) && override > 0) return Math.min(base, Math.max(1, Math.floor(override)));
  return base;
}

/** 老名字保留：历史上叫 stageCapOf，语义是「curate 这一段的上限」。 */
export function stageCapOf(cfg: ConcurrencyConfig | null | undefined): number {
  return curateCapOf(cfg);
}

/** 给一段上限算本轮路数：自适应关掉就恒用上限，开着就按快照算（快照由调度器一轮给一次）。 */
export function selectConcurrency(cfg: ConcurrencyConfig | null | undefined, cap: number, snapshot: LoadSnapshot | null = null): ConcurrencyPick {
  const upper = Math.min(Math.max(Number(cap) || INGEST_HARD_CAP, 1), INGEST_HARD_CAP);
  if (cfg?.organizeIngestAdaptive === false) {
    return { conc: upper, cap: upper, adaptive: false, basis: 'fixed', snapshot: null };
  }
  const snap = snapshot || ingestLoadSnapshot();
  const { conc, basis } = adaptIngestConcurrency({ cap: upper, ...snap });
  return { conc, cap: upper, adaptive: true, basis, snapshot: snap };
}

/** ingest 通道：上限只认 organizeIngestConcurrency（默认/硬顶都是 64）。 */
export function selectIngestConcurrency(cfg: ConcurrencyConfig | null | undefined, snapshot: LoadSnapshot | null = null): ConcurrencyPick {
  return selectConcurrency(cfg, ingestConcurrencyOf(cfg), snapshot);
}

/** curate 通道：上限走 curateCapOf（organizeConcurrency 只当上限覆盖）。 */
export function selectCurateConcurrency(cfg: ConcurrencyConfig | null | undefined, snapshot: LoadSnapshot | null = null): ConcurrencyPick {
  return selectConcurrency(cfg, curateCapOf(cfg), snapshot);
}

function fmtPick(pick: ConcurrencyPick): string {
  const s: Partial<LoadSnapshot> = pick?.snapshot || {};
  const load = Number.isFinite(Number(s.load1)) ? Number(s.load1).toFixed(2) : 'n/a';
  const cores = Number(s.cores) > 0 ? String(Number(s.cores)) : 'n/a';
  const mem = Number.isFinite(Number(s.memAvailableBytes))
    ? (Number(s.memAvailableBytes) / (1024 ** 3)).toFixed(1)
    : 'n/a';
  const temp = s.tempC !== null && s.tempC !== undefined && Number.isFinite(Number(s.tempC))
    ? Number(s.tempC).toFixed(1)
    : 'n/a';
  return `取 ${pick.conc} 路（上限 ${pick.cap}，load1=${load} cores=${cores} `
    + `mem_avail=${mem}GB temp=${temp}C 依据=${pick.basis}）`;
}

/** 自适应那一行日志，复盘时 grep `[ingest] 并发自适应` 即可。 */
export function formatIngestAdaptiveLog(pick: ConcurrencyPick, stage = 'ingest'): string {
  return `[${stage}] 并发自适应 ${fmtPick(pick)}`;
}

/** 统一阶段日志：`[阶段] 并发自适应 取 N 路（…）`，各段一个口径。 */
export function formatStageAdaptiveLog(stage: string, pick: ConcurrencyPick): string {
  return `[${stage}] 并发自适应 ${fmtPick(pick)}`;
}

/** 池收尾的固定格式：`[pool:阶段] 并发 N 路 条 X 成功 Y 失败 Z 耗时 Mms`。 */
export function formatPoolSummary(channel: string, stats: Record<string, unknown> = {}): string {
  const conc = Math.min(
    Number(stats.peakRunning) || Number(stats.concurrency) || 1,
    Math.max(1, Number(stats.peakRunning) || 1),
  );
  return `[pool:${channel}] 并发 ${conc} 路 条 ${Number(stats.claimed) || 0} `
    + `成功 ${Number(stats.landed) || 0} 失败 ${Number(stats.failed) || 0} 耗时 ${Number(stats.ms) || 0}ms`;
}
