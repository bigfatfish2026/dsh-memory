/**
 * 模型调用参数（2026-11-06 维护者硬规矩，2026-11-14 收紧）。
 *
 * **不给模型调用设 maxTokens 上限**：请求里永远不出现这个字段，由 provider / 模型路由
 * 按自己的上限走。2026-11-06 那次只是把默认值改成 0（0 = 不传），配置里仍留了
 * mergeMaxTokens / splitMaxTokens / sleepMaxTokens / consolidateMaxTokens / dedupeMaxTokens
 * 这些口子；2026-11-14 维护者要求「如果代码里传了就去掉」，于是配置口子一起删了，
 * 这个函数只保留名字和调用点，恒返回 {}——任何人再想从配置打开都没门。
 *
 * 用法保持不变：llm.stream({ provider, model, ...maxTokensOption })
 */
// 调用点目前仍按老写法传一个 maxTokens 实参（JS 里多传无影响）；保持零参声明，
// 这样产物与加类型前逐字节一致。等上层摘掉类型豁免时，把实参一起删掉。
export function maxTokensOption(): Record<string, never> {
  return {};
}

export default { maxTokensOption };
