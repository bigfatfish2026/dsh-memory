#!/usr/bin/env node
/**
 * 只读查记忆的 CLI，走 lib/public.js，不直接读 jsonl / vectors.bin。
 *
 * 用法：
 *   node scripts/memory-query.mjs --list [--status active] [--type world] [--limit 20] [--json]
 *   node scripts/memory-query.mjs --get <id> [--json]
 *   node scripts/memory-query.mjs --vectors [--include-missing] [--limit n] [--json]
 *   node scripts/memory-query.mjs --migrated
 *
 * --db <路径> 可指定副本库；默认读真库。只读打开，不改库。
 * 退出码：0 成功；1 库/表有问题；2 参数不对。
 */
import { createMemoryReader, MemoryReaderError, MemoryNotMigratedError } from '../lib/public.js';

const USAGE = `用法：node scripts/memory-query.mjs <模式> [选项]

模式（选一个）：
  --list            列正文；可配 --status / --type / --limit
  --get <id>        取一条
  --vectors         列正文+向量（embedding 以数组输出）
  --migrated        只报库在不在、memories 表有没有

选项：
  --db <路径>       指定库（默认 DSH_MEMORY_DB，缺省 cwd/memory/memory.db；测试指向副本）
  --status <值>     active / merged / archived
  --type <值>       world / experience / opinion / observation
  --limit <n>       条数上限
  --include-missing --vectors 时补上没向量的记忆和孤儿向量
  --json            JSON 输出
  --help, -h        打印这段

退出码：0 成功；1 库/表问题；2 参数不对。`;

class UsageError extends Error {}

function parseArgs(argv) {
  const options = {
    mode: null, db: null, status: null, type: null, limit: null,
    includeMissing: false, json: false, help: false, id: null,
  };
  const setMode = (mode) => {
    if (options.mode) throw new UsageError(`模式只能选一个：已经有 --${options.mode}`);
    options.mode = mode;
  };
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === '--help' || arg === '-h') options.help = true;
    else if (arg === '--list') setMode('list');
    else if (arg === '--vectors') setMode('vectors');
    else if (arg === '--migrated') setMode('migrated');
    else if (arg === '--get') {
      setMode('get');
      options.id = argv[i + 1];
      if (!options.id) throw new UsageError('--get 后面要跟 id');
      i += 1;
    } else if (arg === '--db' || arg === '--status' || arg === '--type' || arg === '--limit') {
      const value = argv[i + 1];
      if (!value) throw new UsageError(`${arg} 后面要跟值`);
      if (arg === '--db') options.db = value;
      if (arg === '--status') options.status = value;
      if (arg === '--type') options.type = value;
      if (arg === '--limit') {
        options.limit = Number(value);
        if (!Number.isInteger(options.limit) || options.limit < 0) throw new UsageError('--limit 要是非负整数');
      }
      i += 1;
    } else if (arg === '--include-missing') options.includeMissing = true;
    else if (arg === '--json') options.json = true;
    else if (arg.startsWith('-')) throw new UsageError(`不认识的选项：${arg}`);
    else throw new UsageError(`多余的参数：${arg}`);
  }
  return options;
}

function printHuman(result, options) {
  if (options.mode === 'migrated') {
    process.stdout.write(result.migrated
      ? `已迁移：${result.dbPath}（memories 表在，${result.rows} 行）\n`
      : `未迁移：${result.dbPath} 里没有 memories 表\n`);
    return;
  }
  if (options.mode === 'get') {
    if (!result) {
      process.stdout.write('没找到这条\n');
      return;
    }
    process.stdout.write(`${result.id} [${result.status}] ${result.type ?? '-'} ${result.ts ?? '-'}\n`);
    process.stdout.write(`${result.text}\n`);
    process.stdout.write(`tags=${JSON.stringify(result.tags)} entities=${JSON.stringify(result.entities)}\n`);
    return;
  }
  if (options.mode === 'vectors') {
    const withVec = result.filter((r) => r.embedding && !r.skipped);
    const dim = withVec[0]?.embedding?.length ?? 0;
    process.stdout.write(`条目 ${result.length} 条｜带向量 ${withVec.length} 条｜维度 ${dim}\n`);
    const skipped = result.filter((r) => r.skipped);
    for (const row of result.slice(0, options.limit ?? 20)) {
      process.stdout.write(`  ${row.id} ${row.type ?? '-'} ${row.status ?? '-'}${row.skipped ? `（${row.skipped}）` : ''}\n`);
    }
    if (skipped.length) process.stdout.write(`  跳过原因：${skipped.map((r) => `${r.id}=${r.skipped}`).join('，')}\n`);
    return;
  }
  process.stdout.write(`共 ${result.length} 条\n`);
  for (const row of result) {
    process.stdout.write(`${row.id} [${row.status}] ${row.type ?? '-'} ${row.ts ?? '-'} ${row.text.slice(0, 40)}\n`);
  }
}

function main() {
  let options;
  try {
    options = parseArgs(process.argv.slice(2));
  } catch (error) {
    process.stderr.write(`${error.message}\n\n${USAGE}\n`);
    process.exit(2);
  }
  if (options.help || !options.mode) {
    process.stdout.write(`${USAGE}\n`);
    process.exit(options.help ? 0 : 2);
  }

  const reader = createMemoryReader({ dbPath: options.db ?? undefined });
  try {
    let result;
    if (options.mode === 'migrated') {
      const migrated = reader.migrated();
      result = { migrated, dbPath: reader.dbPath, rows: migrated ? reader.count() : 0 };
    } else if (options.mode === 'list') {
      result = reader.list({ status: options.status, type: options.type, limit: options.limit });
    } else if (options.mode === 'get') {
      result = reader.get(options.id);
    } else {
      result = reader.allWithVectors({ includeMissing: options.includeMissing, limit: options.limit });
      if (options.json) result = result.map((r) => ({ ...r, embedding: r.embedding ? Array.from(r.embedding) : null }));
    }
    if (options.json) {
      process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
    } else {
      printHuman(result, options);
    }
  } catch (error) {
    if (error instanceof MemoryNotMigratedError || error instanceof MemoryReaderError) {
      process.stderr.write(`${error.message}\n`);
      process.exit(1);
    }
    throw error;
  } finally {
    reader.close();
  }
}

main();
