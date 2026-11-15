/**
 * 价档判据注入点。
 *
 * price-gate.js 只留了一个注入口 `setCheapNowProbe()`，判据本身只许有一份：
 * dsh-price-clock 的 `isCheapNow`。这里就是那个「接上」的地方。
 *
 * 路径写绝对路径，跟 dsh-ego/lib/window.js:14 一个写法——插件目录不在 node_modules 里，
 * 包名解析在这台机器上靠不住。
 *
 * 为什么用 createRequire 同步 require，而不是静态 `import ... from '/abs/path'`：
 * 静态 import 一旦路径断了（插件被挪走、改名），整个 dsh-memory 加载失败，整理全停，
 * 正好撞上「注入失败不许停整理」这条红线。require 能当场 try/catch，失败就退回去。
 * Node 22.12+ 支持 require() 直接加载 ESM，本机 v26 实测通过。
 * 同步还有第二个理由：setupSleep 里的 sleepDriver.start() 会**立即**补判一次，
 * 注入晚一拍，贵时段那轮就会当场跑掉，正好是这层闸要防的事。
 */
import { createRequire } from 'node:module';
import { keyLog } from './file-log.js';
import type { KeyLogConfig } from './file-log.js';
import { setCheapNowProbe } from './price-gate.js';
import * as paths from './shared/paths.js';

/** 价档判据的唯一来源；跟 dsh-ego/lib/window.js:14 一样写绝对路径。 */
export const PRICE_CLOCK_CLOCK_FILE = paths.priceClockFile();

/** 启动日志文案：两条都整行固定，方便 grep。 */
export const PROBE_OK_LINE = '[sleep] 价档判据已接 dsh-price-clock';
export const PROBE_FAIL_LINE = '[sleep] 价档判据注入失败，按全时段便宜处理';

const requireFrom = createRequire(import.meta.url);

/**
 * 把 dsh-price-clock 的 isCheapNow 接到 price-gate 的注入点。
 *
 * 任何失败（文件不在、导出没有、加载抛错）都只打一行告警，然后保持 price-gate 的默认
 * 语义「全时段便宜」——整理照跑，不因为价档判据毛病全停。返回是否接上了，给日志对账用。
 *
 * @param {object} cfg 插件配置（只用 sleepLogFile 当 keyLog 落点）
 * @param {string} [clockFile] 判据文件路径，默认就是唯一那份；测试塞假模块用
 * @returns {boolean}
 */
export function attachPriceProbe(cfg: KeyLogConfig | null | undefined, clockFile: string = PRICE_CLOCK_CLOCK_FILE): boolean {
  try {
    const mod: { isCheapNow?: unknown } = requireFrom(String(clockFile || PRICE_CLOCK_CLOCK_FILE));
    const isCheap = mod?.isCheapNow;
    if (typeof isCheap !== 'function') throw new TypeError('isCheapNow 不是函数');
    setCheapNowProbe(isCheap);
    keyLog(cfg, PROBE_OK_LINE);
    return true;
  } catch {
    // 失败时把注入点清回「没接」：热重载换路径时不许沿用上一份判据。
    setCheapNowProbe(undefined);
    keyLog(cfg, PROBE_FAIL_LINE);
    return false;
  }
}
