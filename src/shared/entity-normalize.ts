/**
 * 实体归一化与粗分类（一期影子用，独立脚本可 import，不碰插件代码）。
 *
 * 依据 docs/dsh-memory-entity-rework-2026.md §4.2 / §5.1 / §5.2 / §5.3：
 *   R1 NFKC 全半角、R2 小写、R3 空白折叠、R4 中文旁空格删除、R5 去尾标点（配对括号保留）、
 *   R6 去包裹书名号/引号、R7 票号白名单、R10-R14 脏形态标记（迁移阶段只标不丢）、
 *   R19 规范名靠 mem_count（由迁移脚本重算）、R20 版本/型号三分法。
 *
 * 历史（2026-11-14 退场，别再往回加）：
 *   R8 路径取 basename 已废除 —— 路径类 norm 保留全路径（§3.2 不变式 3）；
 *   R9/R15/R16/R17 的硬编码别名表（原 ALIASES 20 条）与 ≤8 跳解析已删除 ——
 *   别名改由 entity_surface 表表达，实体身份查 surface 得到。
 *
 * 没落地的：R18 复数剥离（裸词存在性要按 etype + 词频判，独立脚本里没有可信词表，先不做）。
 */

import fs from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

/**
 * 实体类型白名单（2026-11-13 加 account；同时补齐 schema 里早就有的 host）。
 *
 * account = 平台账号，必须带平台特征才收：B 站/GitHub 账号、邮箱、@ 后缀、纯数字 ID、
 * 带站点后缀的名字。跟人名同形的（Anri、子会话这种）仍算 person，不另立账号；
 * 账号的平台写 attrs.platform（github / bilibili / email…），不塞进 person。
 */
export const E_TYPES: string[] = [
  'person', 'account', 'device', 'repo', 'file', 'service', 'ticket', 'tool', 'config',
  'org', 'project', 'host', 'concept', 'unknown',
];

const TICKET_RE = /^t-[a-z0-9]{8}-[a-z0-9]{4}$/;
const CN = /[\u4e00-\u9fff]/;

/** A1 闸与最长匹配共用的折叠：NFKC + 小写 + 空白折叠 + trim。 */
export function foldForMatch(s: string | null | undefined): string {
  return String(s ?? '').normalize('NFKC').toLowerCase().replace(/[\s\u3000]+/g, ' ').trim();
}

const CLI = new Set(['node', 'npm', 'pnpm', 'python', 'python3', 'clang', 'clang++', 'pandoc', 'sqlite3', 'ffmpeg', 'git', 'curl']);
const REPOS = new Set(['example-org', 'sbi', 'dsh-ego', 'dsh-dispatcher', 'dsh-memory', 'qwen3.8', 'llama.cpp']);
const DEVICES = new Set(['树莓派', '小屏', '手机', '显示器', 'mhs-3.5', 'nas', 'nvme', '硬盘盒', 'home assistant']);
const PEOPLE = new Set(['alice', 'bob', '张三']);
const ORGS = new Set(['deepseek', 'github', 'bilibili', 'anthropic', '腾讯', 'tencent', 'openai']);

function stripTrailingPunct(s: string): string {
  let cur = s;
  for (;;) {
    const last = cur.slice(-1);
    if (!last) break;
    if (last === ')' || last === '）') {
      const open = (cur.match(/[（(]/g) || []).length;
      const close = (cur.match(/[）)]/g) || []).length;
      if (close > open) { cur = cur.slice(0, -1); continue; }
      break;
    }
    if (/[.,;:!?、，。；：！？"'”’…·]/.test(last)) { cur = cur.slice(0, -1); continue; }
    break;
  }
  return cur;
}

function stripWrapping(s: string): string {
  const m = s.match(/^[《〈【[（""'']+([\s\S]*?)[》〉】\])）""'']+$/);
  if (m && m[1]) return m[1].trim();
  return s;
}

export interface SplitVersion {
  base: string;
  version: string;
  model: string;
}

/** R20 版本/型号三分法。返回 {base, version, model} 或 null。 */
export function splitVersion(s: string): SplitVersion | null {
  // 树莓派 5 / 树莓派Pi5 / Raspberry Pi 5 → 树莓派（R20b）；Pi4 不并（R20c）
  let m = s.match(/^(树莓派|raspberry pi)\s*(?:pi)?\s*(\d+)$/);
  if (m) {
    if (m[2] === '5') return { base: '树莓派', version: '', model: 'Pi5' };
    return { base: `树莓派pi${m[2]}`, version: '', model: `Pi${m[2]}` };
  }
  // 软件版本后缀：llvm 23.1.0 / python 3.15.1 / v0.5.1
  m = s.match(/^(.+?)\s+v?(\d+(?:\.\d+)+)$/);
  if (m && m[1] && !/^\d/.test(m[1])) return { base: m[1].trim(), version: m[2], model: '' };
  // 纯数字版本：node 22 / python 3
  m = s.match(/^(.+?)\s+(\d+)$/);
  if (m && m[1] && !/^\d/.test(m[1])) return { base: m[1].trim(), version: m[2], model: '' };
  return null;
}

/** 脏形态（R10-R14）。返回 drop 理由或空串。迁移阶段只标记，不删。 */
export function dropReason(norm: string): string {
  if (!norm) return 'empty';
  if ([...norm].length === 1) return 'single_char';
  if (/^\d+$/.test(norm)) return 'pure_number';
  if (/^[\p{P}\p{S}\s]+$/u.test(norm)) return 'pure_symbol';
  if ([...norm].length > 40) return 'too_long';
  if (/[。！？；]/.test(norm) || (norm.includes('，') && [...norm].length > 20)) return 'sentence';
  return '';
}

/** 仓库自带的示例名录；没有用户名录时用它兜底。 */
const LEXICON_EXAMPLE: URL = new URL('../../data/entity-lexicon.example.json', import.meta.url);
/** 用户自己的名录（已 gitignore，不进发布包）。 */
const LEXICON_LOCAL: URL = new URL('../../data/entity-lexicon.json', import.meta.url);

/**
 * 名录文件位置：DSH_MEMORY_LEXICON 优先，其次 data/entity-lexicon.json，
 * 都不在就用仓库自带的示例。发布包里只带示例。
 */
export function lexiconUrl(): URL {
  const explicit = String(process.env.DSH_MEMORY_LEXICON || '').trim();
  if (explicit) return pathToFileURL(path.resolve(explicit));
  try {
    fs.accessSync(LEXICON_LOCAL);
    return LEXICON_LOCAL;
  } catch {
    return LEXICON_EXAMPLE;
  }
}

export const LEXICON_URL: URL = lexiconUrl();

let lexiconCache: Map<string, string> | null = null;

/** 名录条目：只用到 name / aliases / etype 三个字段，其余字段不读。 */
interface LexiconItem {
  name?: string | null;
  aliases?: string[] | null;
  etype?: string | null;
}

/**
 * 读名录建索引：foldForMatch(主名/别名) → etype。
 *
 * 名录缺失或坏掉一律当空表（返回空 Map），形态正则照旧兜底；不在这里抛错，
 * 免得一个数据文件把实体闸整条打挂。同名重复时先到先得，后出现的忽略。
 * 进程内缓存一份，改名录后要重启进程才生效（名录不是热更资产）。
 */
export function loadLexicon(file: string | URL = LEXICON_URL): Map<string, string> {
  if (file === LEXICON_URL && lexiconCache) return lexiconCache;
  const index = new Map<string, string>();
  try {
    const doc = JSON.parse(fs.readFileSync(file, 'utf8')) as Record<string, unknown>;
    for (const [key, items] of Object.entries(doc)) {
      if (!Array.isArray(items)) continue;
      for (const it of items as LexiconItem[]) {
        const etype = String(it?.etype || key || '').toLowerCase();
        if (!E_TYPES.includes(etype) || etype === 'unknown') continue;
        for (const n of [it?.name, ...(Array.isArray(it?.aliases) ? it.aliases : [])]) {
          const norm = foldForMatch(n);
          if (norm && !index.has(norm)) index.set(norm, etype);
        }
      }
    }
  } catch {
    return file === LEXICON_URL ? (lexiconCache = index) : index;
  }
  return file === LEXICON_URL ? (lexiconCache = index) : index;
}

/** 名录查询：命中返回 etype，没命中返回空串（调用方继续走形态正则）。 */
export function lexiconEtype(norm: string): string {
  if (!norm) return '';
  return loadLexicon().get(foldForMatch(norm)) || '';
}

/** §4.2 规则优先（命中即停）。名录在最前，形态正则作兜底。 */
export function classifyEntity(norm: string): string {
  if (!norm) return 'unknown';
  const fromLexicon = lexiconEtype(norm);
  if (fromLexicon) return fromLexicon;
  if (TICKET_RE.test(norm)) return 'ticket';
  if (/\.(service|timer|socket|mount)$/.test(norm)) return 'service';
  if (/\.(ya?ml|toml|ini|conf|env|json)$/.test(norm)) return 'config';
  if (/^(~|\/)/.test(norm)) return 'file';
  if (/\.(sh|js|mjs|py|md|log|png|db|sql|txt|bin|jpg|webp)$/.test(norm)) return 'file';
  if (/^dsh(-|$)/.test(norm) || /^@[a-z0-9_.-]+\//.test(norm) || CLI.has(norm)) return 'tool';
  if (REPOS.has(norm)) return 'repo';
  if (/^树莓派/.test(norm) || /^(pi|raspberry)/.test(norm) || DEVICES.has(norm) || /^dh[-_]?\d+$/.test(norm)) return 'device';
  if (PEOPLE.has(norm)) return 'person';
  if (ORGS.has(norm)) return 'org';
  return 'unknown';
}

/** normalizeEntity 的返回形状。 */
export interface NormalizedEntity {
  norm: string;
  display: string;
  etype: string;
  attrs: Record<string, string>;
  drop: string;
  version: string;
}

/** normalizeEntity 的选项：drop=true 时照 R10-R14 返回 drop 标记（调用方决定丢不丢）。 */
/**
 * 归一化主函数。
 * @param raw 原始实体名
 */
export function normalizeEntity(raw: string | null | undefined): NormalizedEntity {
  const original = String(raw ?? '').trim();
  const out: NormalizedEntity = { norm: '', display: original, etype: 'unknown', attrs: {}, drop: '', version: '' };
  if (!original) { out.drop = 'empty'; return out; }

  let s = original.normalize('NFKC');
  s = s.toLowerCase();
  s = s.replace(/[\s\u3000]+/g, ' ').trim();
  s = s.replace(new RegExp(`(?<=${CN.source})\\s+|\\s+(?=${CN.source})`, 'g'), '');
  s = stripTrailingPunct(s);
  s = stripWrapping(s);

  if (TICKET_RE.test(s)) {
    out.norm = s;
    out.display = original;
    out.etype = 'ticket';
    return out;
  }

  // 历史：R8 取 basename 已废除（2026-11-14），路径保留全路径，见文件头。

  const drop = dropReason(s);
  if (drop) {
    out.norm = s;
    out.drop = drop;
    out.etype = classifyEntity(s);
    return out;
  }

  const ver = splitVersion(s);
  if (ver) {
    s = ver.base || s;
    out.version = ver.version;
    if (ver.version) out.attrs.version = ver.version;
    if (ver.model) out.attrs.model = ver.model;
  }
  out.norm = s;
  out.etype = classifyEntity(s);
  return out;
}

/**
 * 泛词黑名单（2026-11-13 补）。
 *
 * 光靠 df 闸拦不住：回填那轮 agent 爱抽的「合并/拆分/去重/op/agent/ask/stop/t1」在
 * memory_entity 里 df 只有 1-2（大多没落库或已被清），比例永远过不了 0.05 的线。
 * 所以粗实体词典和队列落库都先过这道显式名单，再谈 df。只收回填实测点名的词，不扩。
 */
export const GENERIC_NAMES: Set<string> = new Set(['合并', '拆分', '去重', 'op', 'agent', 'ask', 'stop', 't1']);

/** norm 是否泛词（黑名单，按 A1 同一套折叠比较）。 */
export function isGenericName(norm: string): boolean {
  return GENERIC_NAMES.has(foldForMatch(norm));
}

/** 脏名分类（给审计脚本统计用）。返回数组，可多个。目前未接线，保留给后续审计。 */
export function dirtyKinds(raw: string | null | undefined): string[] {
  const out: string[] = [];
  const s = String(raw ?? '').trim();
  if (!s) return ['empty'];
  if ([...s].length === 1) out.push('single_char');
  if (/\d+\.\d+/.test(s) || /(?:^|[\s-])v?\d+$/.test(s)) out.push('has_version');
  if (/^[A-Za-z]+\d+$/.test(s) || /^[A-Za-z]+\d+[A-Za-z]*$/.test(s)) out.push('alpha_num');
  return out;
}
