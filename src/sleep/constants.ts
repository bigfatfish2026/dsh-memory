/** 睡觉链的归档红线：稳定条目永不自动归档。 */
export const PROTECTED_TAGS = ['identity', 'preference', 'hardware', 'account', 'style', 'interest'];

export const ARCHIVABLE_TAGS = ['status', 'task', 'tmp', 'temp', 'progress', 'state'];
// 没有标签但明显是「某时点的进度快照」的，也算可归档
export const ARCHIVABLE_PATTERN = /^(20\d{2}[-年]\d{1,2}|截至|目前|当前状态)|还剩\s*\d|进度[:：]|队列|截至/;
// 正文在宣告某旧说法作废/失效的，也算可归档
export const REVOCATION_PATTERN = /作废|废止|不再适用|已废弃|已失效|已被取代|不再成立|已被推翻/;
