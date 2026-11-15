/**
 * 推送脚本（notify.sh）的唯一出口。
 *
 * 2026-11-16：宿主 00:31 / 00:45 各崩一次。根因是 notifyScript 缺省
 * `path.resolve(cwd, 'notify.sh')` 在 WorkingDirectory=/home/julia 下解析成
 * /home/julia/notify.sh（不存在），spawn 的 ENOENT 走异步 'error' 事件，
 * 调用处的同步 try/catch 抓不到，也没人挂 .on('error')，uncaught exception 把进程带走。
 *
 * 所以三处调用（consolidate / context-watch / sleep）统一走这里：
 * 空路径直接 return；spawn 后立刻挂 error 监听；stderr 留尾；非零退出记 warn。
 * 全程不抛、不 await、不阻塞主流程，spawn 完就 unref。
 */
import { spawn } from 'node:child_process';
import type { ChildProcess } from 'node:child_process';

/** 日志出口：调用方传自己的日志函数，不传就退回 console.warn。 */
export type NotifyLog = (line: string) => void;

export interface RunNotifyOptions {
  /** 脚本路径；空/空白一律当没配，直接不发。 */
  script?: string | null;
  /** 传给脚本的参数，和以前 spawn(script, [msg]) 一致。 */
  args?: string[];
  /** 日志里认调用点：consolidate / context-watch / sleep。 */
  label: string;
  /** 日志出口；缺省 console.warn。 */
  log?: NotifyLog;
}

/** stderr 只留尾部这么多字符，够看清报错就行。 */
const STDERR_LIMIT = 2000;

const tail = (text: string): string => text.trim().slice(-600).replace(/\s+/g, ' ');

export function runNotify(options: RunNotifyOptions): void {
  const { script, args = [], label } = options;
  const log: NotifyLog = typeof options.log === 'function' ? options.log : (line) => console.warn(line);
  const target = String(script ?? '').trim();
  if (!target) return;

  let child: ChildProcess;
  try {
    child = spawn(target, args, { detached: true, stdio: ['ignore', 'pipe', 'pipe'] });
  } catch (error) {
    // 参数非法这类同步抛错的，记一行就算，别往上扔。
    const err = error as NodeJS.ErrnoException;
    log(`[notify] 起不来 label=${label} script=${target} code=${err?.code || ''} msg=${err?.message || error}`);
    return;
  }

  let stderr = '';
  const collect = (chunk: unknown): void => {
    if (stderr.length >= STDERR_LIMIT) return;
    stderr += String(chunk);
  };
  // 两个 pipe 都得有人读，否则子进程写满缓冲区会卡住。
  child.stdout?.on('data', collect);
  child.stdout?.on('error', () => {});
  child.stderr?.on('data', collect);
  child.stderr?.on('error', () => {});

  // spawn 失败后 close 还会来一次（code=-2），别重复记成两条。
  let failed = false;
  // 这一行是这次事故的正解：ENOENT / EACCES 都在这里，不挂就是 uncaught。
  child.on('error', (error) => {
    failed = true;
    const err = error as NodeJS.ErrnoException;
    log(`[notify] spawn 失败 label=${label} script=${target} code=${err?.code || ''} msg=${err?.message || error}`);
  });

  child.on('close', (code, signal) => {
    const detail = stderr ? ` stderr=${tail(stderr)}` : '';
    if (!failed && typeof code === 'number' && code !== 0) {
      log(`[notify] 非零退出 label=${label} script=${target} code=${code}${detail}`);
    } else if (!failed && code === null && signal) {
      log(`[notify] 被信号结束 label=${label} script=${target} signal=${signal}${detail}`);
    }
    child.stdout?.destroy();
    child.stderr?.destroy();
  });

  // 不 await：推送只是旁路，绝不让主流程等它。
  child.unref();
}
