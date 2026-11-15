/**
 * 记忆写权限的代码级硬拦截。
 *
 * 设计决定：记忆的写权限只归主会话（主 agent）。subagent（子会话）
 * 只许用 memory_recall / memory_list / memory_entity 读，要更新就在 dispatch_report
 * 里提一句交回主会话落笔。光靠提示词不算数，所以在每个写类工具的 execute 第一行
 * 拦一道。
 *
 * 判定只认 exec.agent.session.header.origin（跟 store.js 里 recallSessionInfo 的
 * 首选路径一样）。插件自身定时器（memory_sleep 的夜间任务等）调底层函数时根本不
 * 经工具 exec，exec 是 undefined；旧调用路径上也未必拿得到 agent。这两种情况一律
 * 当主会话放行，宁可漏拦也不能把插件自己的定时任务打死。
 */

const DENY_MESSAGE =
  '记忆写权限只归主会话：子会话（subagent）只能用 memory_recall / memory_list / memory_entity 读。' +
  '要记的东西写进 dispatch_report 交回主会话落笔。';

/** 只取判定要用的那一串身份字段；工具 exec 上下文比这宽得多。 */
interface WriteGuardExec {
  agent?: { session?: { header?: { origin?: string } } };
}

/** 某次工具调用是不是 subagent 发起的；拿不到身份就当主会话。 */
function isSubagentCall(exec: WriteGuardExec | null | undefined): boolean {
  try {
    const origin = exec?.agent?.session?.header?.origin;
    return origin === 'subagent';
  } catch {
    // 拿不到 agent（旧调用路径 / 已销毁的会话对象）——不拦
    return false;
  }
}

/**
 * 写类工具的统一入口守卫：subagent 调用直接抛错，主会话正常返回。
 *
 * @param {unknown} exec 工具 execute 的第二个参数（tool exec 上下文）
 * @param {string} toolName 工具名，只用于错误信息定位
 */
export function assertMayWrite(exec: unknown, toolName: string): void {
  let subagent = false;
  try {
    subagent = isSubagentCall(exec as WriteGuardExec | null | undefined);
  } catch {
    subagent = false;
  }
  if (!subagent) return;
  const err: Error & { code?: string } = new Error(`${toolName}：${DENY_MESSAGE}`);
  err.code = 'MEMORY_WRITE_DENIED';
  throw err;
}

export default assertMayWrite;
