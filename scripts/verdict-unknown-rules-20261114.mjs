#!/usr/bin/env node
/**
 * 票 第二步（第 2 轮）：纯规则重判，覆盖模型给的类型。
 *
 * 只读 $DSH_MEMORY_ROOT/unknown-verdict-20261114.json（模型原始判定），
 * 按规则把「值、1-2 字名、泛词、concept 无指代对象」打成 unknown，
 * 写 $DSH_MEMORY_ROOT/unknown-verdict-20261114-filtered.json。
 * 不连模型、不碰 memory.db。
 *
 * 用法：node scripts/verdict-unknown-rules-20261114.mjs [--in ...] [--out ...] [--json]
 */
import fs from 'node:fs';
import { resolveMemoryFile } from './lib/db.mjs';

const argv = process.argv.slice(2);
const argOf = (k, d) => (argv.includes(k) ? argv[argv.indexOf(k) + 1] : d);
const IN = argOf('--in', resolveMemoryFile('unknown-verdict-20261114.json'));
const OUT = argOf('--out', resolveMemoryFile('unknown-verdict-20261114-filtered.json'));

// 泛词表：泛指名词，脱离上下文没有具体所指。单双字的那部分会被 R7 先拦，这里一并列出来便于对账。
const GENERIC = new Set([
  '群', '群聊', '主群', '票', '票号', '票台', '参数', '内存', '温度', '服务', '模型', '重启', '备份',
  '评论', '私聊', '窝', '草案', '界面', '统计', '其他', '产物', '实测', '升级', '编译', '表情', '插件',
  '气泡', '宿舍', '自检', '显存', '路由', '代理', '凭据', '许可', '沙箱', '宿主', '分区', '博客', '留言',
  '别名', '收藏', '已看', '番剧', '节目', '头像', '立绘', '配色', '睫毛', '白边', '抠图', '叠影', '眼区',
  '眼层', '嘴层', '嘴线', '嘴型', '相位', '补帧', '峰区', '漫画', '蓝牙', '闭嘴', '周末', '成本', '睡觉',
  '探针', '探头', '猫耳', '脏行', '图集', '硬币', '抽签', '风控', '去踪', '腐蚀', '已发送', '仓位', '现场',
]);

// 规则表：顺序即优先级，命中即停；enforce=false 的是「扩大解释」，单独统计给维护者看。
const RULES = [
  { id: 'R1', label: '错误码（4-6 位数字）', test: (n) => /^\d{4,6}$/.test(n), enforce: true },
  { id: 'R2', label: '纯数字/小数/科学计数', test: (n) => /^\d+(\.\d+)?([eE][+-]?\d+)?$/.test(n), enforce: true },
  { id: 'R3', label: '版本号（x.y.z / -rc.N / 内核串）', test: (n) => /^v?\d+(\.\d+)+([._-][0-9A-Za-z]+)*$/.test(n) || /^rc\.\d+$/.test(n), enforce: true },
  { id: 'R4', label: 'commit hash（7-40 位十六进制）', test: (n) => /^(?=[0-9a-f]*[a-f])[0-9a-f]{7,40}$/.test(n), enforce: true },
  { id: 'R5', label: 'URL scheme 片段', test: (n) => /^[a-zA-Z][a-zA-Z0-9+.-]*:$/.test(n) || n.includes('://'), enforce: true },
  { id: 'R6', label: '端口号', test: (n) => /^:\d+$/.test(n) || /^\d{2,5}$/.test(n), enforce: true },
  { id: 'R7', label: 'name 1-2 个字符', test: (n) => [...n].length <= 2, enforce: true },
  { id: 'R8', label: '泛词表', test: (n) => GENERIC.has(n), enforce: true },
  { id: 'R9', label: '时间点/时间段', test: (n) => /^\d{1,2}:\d{2}([-–~]\d{1,2}:\d{2}|[-–~]次日\s*\d{1,2}:\d{2})?$/.test(n), enforce: true },
  { id: 'R10', label: '尺寸/分辨率', test: (n) => /^\d+\s*[×xX*]\s*\d+$/.test(n) || /^\d+p\d+$/.test(n), enforce: true },
  { id: 'R11', label: '指标数值（fps/ms/G/步/天/行每帧/画布）', test: (n) => /^\d+(\.\d+)?\s*(fps|ms|s|G|GB|MB|%|步|天|岁|行\/帧|画布)$/.test(n) || /^\d+岁=\d+岁$/.test(n), enforce: true },
  { id: 'R12', label: '权限码（644 这类）', test: (n) => /^[0-7]{3,4}$/.test(n), enforce: true },
  { id: 'R13', label: '端口/协议组合（143/STARTTLS 这类）', test: (n) => /^\d{2,5}\/(STARTTLS|SSL|TLS)$/.test(n), enforce: true },
  { id: 'R14', label: '错误码字符串（全大写+下划线）', test: (n) => /^[A-Z][A-Z0-9]*(_[A-Z0-9]+){2,}$/.test(n), enforce: true },
  { id: 'R15', label: 'concept 无指代对象（reason 自述泛称/无上下文/描述片段）', test: (n, row) => row.etype === 'concept' && /泛称|泛指|描述片段|无直接上下文|只不过|一个说法|同类.*数字形态|不指向具体/.test(row.reason), enforce: true },
];

const rows = JSON.parse(fs.readFileSync(IN, 'utf8'));
const hits = new Map(RULES.map((r) => [r.id, []]));
const out = [];

for (const row of rows) {
  const name = String(row.name);
  if (row.etype === 'unknown') { out.push({ ...row }); continue; }
  let hit = null;
  for (const r of RULES) {
    if (r.test(name, row)) { hit = r; break; }
  }
  if (!hit) { out.push({ ...row }); continue; }
  hits.get(hit.id).push(row);
  out.push({
    ...row,
    etype: 'unknown',
    conf: row.conf === 'high' ? 'medium' : row.conf,
    reason: `规则拦（${hit.id} ${hit.label}）｜原 ${row.etype}/${row.conf}：${row.reason}`,
  });
}

fs.writeFileSync(OUT, JSON.stringify(out.map(({ _rule, ...r }) => r), null, 1) + '\n');

const byType = {};
const byConf = { high: 0, medium: 0, low: 0 };
for (const r of out) {
  byType[r.etype] = (byType[r.etype] || 0) + 1;
  byConf[r.conf]++;
}

const blocked = [...hits.entries()].map(([id, list]) => {
  const rule = RULES.find((r) => r.id === id);
  return { id, label: rule.label, n: list.length, high: list.filter((x) => x.conf === 'high').length };
});
const highBlocked = RULES.flatMap((rule) =>
  (hits.get(rule.id) || []).filter((x) => x.conf === 'high').map((x) => ({ rule: rule.id, name: x.name, was: `${x.etype}/${x.conf}`, reason: x.reason })),
);

const conceptRows = out.filter((r) => r.etype === 'concept').sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));

const originalType = {};
for (const r of rows) originalType[r.etype] = (originalType[r.etype] || 0) + 1;

const summary = {
  in: IN, out: OUT,
  total: out.length,
  blocked_from_typed: out.filter((r, i) => rows[i].etype !== 'unknown' && r.etype === 'unknown').length,
  kept_unknown: rows.filter((r) => r.etype === 'unknown').length,
  by_rule: blocked.filter((b) => b.n > 0).sort((a, b) => b.n - a.n),
  conf: byConf,
  etype: Object.fromEntries(Object.entries(byType).sort((a, b) => b[1] - a[1])),
  etype_before: Object.fromEntries(Object.entries(originalType).sort((a, b) => b[1] - a[1])),
  unknown_left: byType.unknown || 0,
  high_blocked: highBlocked,
  concept_left: conceptRows.length,
};
console.log(JSON.stringify(summary, null, 2));
if (argv.includes('--concept')) {
  fs.writeFileSync('/tmp/concept-left.txt', conceptRows.map((r) => `${r.name}\t${r.etype}/${r.conf}\t${r.reason}`).join('\n') + '\n');
  console.log('concept list -> /tmp/concept-left.txt');
}
