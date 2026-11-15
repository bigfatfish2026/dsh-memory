#!/usr/bin/env node
/**
 * 按三处修正重算候选，可选落库。
 *
 * 修 1：1-2 字名不再一刀切，逐条按上下文重判（26 条）。
 * 修 2：4 条平台账号显式白名单（纯数字不打回 unknown）。
 * 修 3：concept 里的接口路径/标识符按形态转 file / config（少数转 unknown / ticket / service，保留真术语）。
 *
 * 输出 $DSH_MEMORY_ROOT/unknown-verdict-20261114-final.json。
 * 带 --apply 才写 entities：单事务 UPDATE etype，只动 status='active' 且 etype='unknown' 的行。
 *
 * 用法：node scripts/apply-unknown-verdict-20261114.mjs [--apply] [--db ...]
 */
import fs from 'node:fs';
import { openDb, resolveDbPath, resolveMemoryFile } from './lib/db.mjs';

const argv = process.argv.slice(2);
const argOf = (k, d) => (argv.includes(k) ? argv[argv.indexOf(k) + 1] : d);
const DB = argOf('--db', resolveDbPath());
const FILTERED = resolveMemoryFile('unknown-verdict-20261114-filtered.json');
const OUT = resolveMemoryFile('unknown-verdict-20261114-final.json');
const APPLY = argv.includes('--apply');

// 修 1：26 条 1-2 字名按上下文重判。理由都引原文。
const SHORTFIX = {
  雀魂: { etype: 'org', conf: 'high', reason: '上下文「雀魂礼品码巡检」「雀魂官方账号」，是游戏平台' },
  豆包: { etype: 'tool', conf: 'high', reason: '上下文「dsh-pet 的动画素材由豆包生成绿幕 mp4」，是生成服务' },
  示例站点: { etype: 'org', conf: 'medium', reason: '上下文「示例站点的账号存在配置目录里」，是站点' },
  梵想: { etype: 'org', conf: 'medium', reason: '上下文「系统盘梵想 S542Pro 512G」，是硬盘厂商' },
  淘宝: { etype: 'org', conf: 'high', reason: '上下文「开放平台接口要企业资质」，是电商平台' },
  雪飘: { etype: 'org', conf: 'high', reason: '上下文「光之美少女订雪飘工作室（gid=6）」，是字幕组/工作室' },
  CF: { etype: 'org', conf: 'medium', reason: '上下文「CF 的 /cdn-cgi/trace」「ping CF 边缘」，指 Cloudflare' },
  张三: { etype: 'person', conf: 'medium', reason: '上下文「张三就是示例人物的昵称」，是角色' },
  李四: { etype: 'person', conf: 'medium', reason: '上下文「李四是示例账号的昵称」，是账号身份' },
  王五: { etype: 'person', conf: 'medium', reason: '上下文「王五决定：迁移时不搬旧取代链」' },
  wo: { etype: 'config', conf: 'medium', reason: '上下文「窗台对应 HA 区域 wo」，是 Home Assistant 区域标识' },
  分区: { etype: 'config', conf: 'high', reason: '上下文「回填 meta.json 的标题、BV 号、标签、分区」，是字段名' },
  t2: { etype: 'config', conf: 'medium', reason: '上下文「t2 只读档」「t2 仍无 bash」，是 subagent 档位配置' },
  t5: { etype: 'config', conf: 'medium', reason: '上下文「T5 媒体+HA 不拆成两档」，是 subagent 档位配置' },
  u: { etype: 'config', conf: 'high', reason: '上下文「认证参数用 u 加 t=md5(密码+salt)」，是 Subsonic 参数名' },
  gh: { etype: 'tool', conf: 'high', reason: '上下文「树莓派没装 gh，用 GitHub MCP 建仓」，是 GitHub CLI' },
  df: { etype: 'tool', conf: 'high', reason: '上下文「df 显示实际释放 2.67G」，是命令' },
  PR: { etype: 'unknown', conf: 'medium', reason: '上下文一处指 pull request、一处指 Premiere 抠像，指代不清' },
  桌宠: { etype: 'tool', conf: 'medium', reason: '上下文「桌宠别名 ego_status 已定为念头状态」「dsh-ego 的只读自省工具」，是桌宠程序' },
  仓库: { etype: 'repo', conf: 'low', reason: '上下文「有人在示例用户的仓库开 issue」，是代码仓（置信度低，落库不落）' },
  台灯: { etype: 'device', conf: 'high', reason: '上下文「关闭台灯、灯座和插座」，是可开关的实物灯' },
  灯座: { etype: 'device', conf: 'high', reason: '上下文「关闭台灯、灯座和插座」，是可开关的实物灯座' },
  小爱: { etype: 'device', conf: 'medium', reason: '上下文「本机 8901 小爱代理」「小爱拉一次就顺着放完」，是音箱' },
  票台: { etype: 'service', conf: 'medium', reason: '上下文「票台夜里从 jsonl 迁到 sqlite，TicketBackend 四个原语」，是票系统服务' },
  晓伊: { etype: 'concept', conf: 'medium', reason: '上下文「edge-tts 默认音色晓伊」，是音色型号名' },
};

// 修 2：平台账号白名单，纯数字规则不适用。
const ACCOUNTFIX = {
  4010964270: { etype: 'account', conf: 'high', reason: '上下文「机器人 QQ 号 4010964270」，账号本身即身份' },
  3441132598: { etype: 'account', conf: 'high', reason: '上下文「QQ 号为 3441132598」，账号本身即身份' },
  642684704: { etype: 'account', conf: 'high', reason: '上下文「bilibili 机器人号 mid/UID 642684704」，平台账号' },
  2589: { etype: 'account', conf: 'high', reason: '上下文「DMXAPI 用户 ID 2589」，平台账号' },
};

// 修 3：concept 里的路径/标识符按形态转类。
const PATHS = [
  '/api/*', '/Genres', '/health', '/Items', '/jobs', '/jobs/<id>/layers', '/Persons',
  '/Search/Hints', '/Shows', '/Users', '/v2/users/{openid}/files', '/v2/users/{openid}/messages',
];
const TO_FILE = Object.fromEntries([
  ...PATHS.map((n) => [n, '接口路径，归 file']),
  ['.clip', '文件格式后缀'],
  ['-alpha', '文件名后缀约定'],
  ['session_projcache', 'DSH 输出的 JSON 产物名'],
]);
const TO_UNKNOWN = {
  'content.some': 'content.some is not a function 的报错片段，不是对象',
  'V3.1.24': '规则版本号，是纯值',
  '503/402': 'HTTP 状态码，是纯值',
  'session-': '截断的会话前缀，句子碎片',
};
const TO_TICKET = { 'PR #97': '针对 dsh-qqbot 的改动提案，按票算' };
const TO_SERVICE = { 'systemd-journal': 'systemd 的日志单元，按服务算' };
// 保留 concept 的：真正的术语/型号/发行版/机制。
const KEEP_CONCEPT = new Set([
  'Ubuntu 26.04.1 LTS', 'Alpine Linux 3.24.2 64bit', 'PCIe 2.0 x1', 'USB3.0', 'Pascal sm_61',
  'try/catch', 'GPS/zone', 'zh-CN-XiaoyiNeural', 'Message-ID', 'RP1-C0', 'micro-HDMI',
  'npm dist-tags', 'VP9-alpha', 'bili-daily 评论生成', '个人资料-查看', 'J-Pop',
  'streamable-http', 'split-pass', 'memory_entity',
]);

const rows = JSON.parse(fs.readFileSync(FILTERED, 'utf8'));
const stats = { shortfix: 0, accountfix: 0, reclass: {}, kept: 0 };

for (const r of rows) {
  const name = String(r.name);
  if (SHORTFIX[name]) {
    const f = SHORTFIX[name];
    Object.assign(r, f);
    stats.shortfix++;
    continue;
  }
  if (ACCOUNTFIX[name]) {
    Object.assign(r, ACCOUNTFIX[name]);
    stats.accountfix++;
    continue;
  }
  if (r.etype !== 'concept') continue;
  let to = null;
  if (TO_FILE[name]) to = { etype: 'file', reason: `${TO_FILE[name]}（原 concept）` };
  else if (TO_UNKNOWN[name]) to = { etype: 'unknown', reason: `${TO_UNKNOWN[name]}（原 concept）` };
  else if (TO_TICKET[name]) to = { etype: 'ticket', reason: `${TO_TICKET[name]}（原 concept）` };
  else if (TO_SERVICE[name]) to = { etype: 'service', reason: `${TO_SERVICE[name]}（原 concept）` };
  else if (KEEP_CONCEPT.has(name)) { stats.kept++; continue; }
  else if (/[/._()=#<>]/.test(name) && !/[\u4e00-\u9fa5]/.test(name)) to = { etype: 'config', reason: `键/字段/事件名形态（原 concept）：${r.reason.slice(0, 40)}` };
  else if (/[/.]/.test(name) && /[\u4e00-\u9fa5]/.test(name)) { stats.kept++; continue; }
  if (to) {
    stats.reclass[to.etype] = (stats.reclass[to.etype] || 0) + 1;
    r.etype = to.etype;
    r.reason = to.reason;
  }
}

fs.writeFileSync(OUT, JSON.stringify(rows, null, 1) + '\n');

const byType = {};
const byConf = { high: 0, medium: 0, low: 0 };
for (const r of rows) {
  byType[r.etype] = (byType[r.etype] || 0) + 1;
  byConf[r.conf]++;
}
const cand = rows.filter((r) => r.etype !== 'unknown' && r.conf !== 'low');
console.log(JSON.stringify({ out: OUT, stats, etype: byType, conf: byConf, candidates: cand.length, unknown_after_if_apply: 1481 - cand.length }, null, 2));

if (!APPLY) process.exit(0);

const db = openDb(DB);
db.exec('PRAGMA busy_timeout = 30000;');
const upd = db.prepare("UPDATE entities SET etype = ? WHERE id = ? AND status = 'active' AND etype = 'unknown'");
let changed = 0;
let skipped = 0;
const fail = [];
db.exec('BEGIN IMMEDIATE');
try {
  for (const r of cand) {
    const res = upd.run(r.etype, r.entity_id);
    if (res.changes === 1) changed++;
    else { skipped++; fail.push({ id: r.entity_id, name: r.name, etype: r.etype }); }
  }
  db.exec('COMMIT');
} catch (e) {
  db.exec('ROLLBACK');
  throw e;
}
const integrity = db.prepare('PRAGMA integrity_check').get();
const after = db.prepare("SELECT etype, count(*) c FROM entities WHERE status != 'archived' GROUP BY etype").all();
const left = db.prepare("SELECT count(*) c FROM entities WHERE status = 'active' AND etype = 'unknown'").get().c;
console.log(JSON.stringify({ changed, skipped, fail: fail.slice(0, 10), integrity, unknown_left: left, after }, null, 2));
