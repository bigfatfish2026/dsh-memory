# 记忆正文落 SQLite：schema 与迁移说明

2026-11-05 起运行时只碰 SQLite：`lib/store.js` 从 `memories` 表 load / 写，`memories.jsonl`
只在「库还没迁移」时当一次性导入源（`scripts/migrate-jsonl-to-sqlite.mjs`）。jsonl 不再被运行时代码读写。

数据目录：`$DSH_MEMORY_ROOT/`（缺省 `./memory`）
- 真库：`memory.db`（`memories` 正文 + `vec_items` / `vec_map` / `meta`）
- 冻结原件：`memories.jsonl`（4672 行，切换后不再更新，只在迁移/回滚时用）

## 建表 DDL

脚本 `scripts/migrate-jsonl-to-sqlite.mjs` 里的 DDL 原文：

```sql
CREATE TABLE IF NOT EXISTS memories (
  id            TEXT PRIMARY KEY,
  text          TEXT NOT NULL,
  tags          TEXT,
  source        TEXT,
  ts            TEXT,
  type          TEXT,
  entities      TEXT,
  validFrom     TEXT,
  supersedes    TEXT,
  updatedAt     TEXT,
  status        TEXT,
  reason        TEXT,
  supersededBy  TEXT,
  validTo       TEXT,
  confidence    REAL,
  extra         TEXT,
  present       TEXT,
  rowSeq        INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_memories_status ON memories(status);
CREATE INDEX IF NOT EXISTS idx_memories_type   ON memories(type);
CREATE INDEX IF NOT EXISTS idx_memories_ts     ON memories(ts);
```

连接级设置：

```sql
PRAGMA journal_mode = WAL;
PRAGMA busy_timeout = 5000;
PRAGMA synchronous = NORMAL;   -- 只在迁移连接上用，迁移完关连接
```

## 字段来源（jsonl 15 个 key 全覆盖）

实际数据里共出现 15 个 key（题面写的 16 个按现存数据核不到第 16 个；`extra` 兜底保证以后多 key 也不丢）。

| jsonl key | 列 | 类型 | 说明 |
| --- | --- | --- | --- |
| `id` | `id` | TEXT PK | 记忆 id，如 `m_mvlwhnzg_0ise` |
| `text` | `text` | TEXT NOT NULL | 正文；缺 text 的行迁移时直接判坏行 |
| `tags` | `tags` | TEXT | JSON 数组文本，如 `["preference","example"]` |
| `source` | `source` | TEXT | agent / split / consolidator / sleep / sleep:merge |
| `ts` | `ts` | TEXT | ISO 时间 |
| `type` | `type` | TEXT | world / experience / opinion / observation |
| `entities` | `entities` | TEXT | JSON 数组文本 |
| `validFrom` | `validFrom` | TEXT | 日期或 ISO 时间 |
| `supersedes` | `supersedes` | TEXT | JSON 数组文本，可多值（实测最多 16 个） |
| `updatedAt` | `updatedAt` | TEXT | ISO 时间 |
| `status` | `status` | TEXT | active 判据是 `status IS NULL`；空串不算 active，只有 NULL 才是 |
| `reason` | `reason` | TEXT | merged/archived 的原因 |
| `supersededBy` | `supersededBy` | TEXT | 指向取代它的 id |
| `validTo` | `validTo` | TEXT | 失效时间 |
| `confidence` | `confidence` | REAL | 0..1 |
| 未知 key | `extra` | TEXT | JSON 对象，键值原样存；没有未知 key 时为 NULL |
| — | `present` | TEXT | JSON 数组，记录这一行 jsonl 里实际出现了哪些 key；用来区分「显式 null」和「整个 key 缺失」 |
| — | `rowSeq` | INTEGER | 1 起，等于 jsonl 行序；回放按它排序 |

`status` 的 active 判据只有一条：`status IS NULL`。空串不算 active，只有 NULL 才是；库里的 `status` 保留原始 NULL，
`get()/list()` 返回值里同时给 `status` 和 `statusRaw`（原值）。

JSON 列统一存 `JSON.stringify` 文本；数组顺序保持 jsonl 原样。
`null` / 缺字段都落成 SQL NULL，靠 `present` 区分：`present` 里有这个 key 说明是显式 null，没有就是缺字段。
实测当前 4672 行没有显式 null，全是缺字段。

## 向量表不动

`vec_items`（vec0 虚拟表，float[1024] cosine）和 `vec_map`（rowid ↔ id）保持现有结构，迁移脚本：
- 不 `DROP`、不 `CREATE` 第二套向量表；
- 不写 `vec_items` / `vec_map` 的任何一行；
- 只读它们做核对（孤儿向量、没向量的记忆、真实向量条数）。

`meta` 表只修过期值：`rows` 从 2695 改成真实向量条数（当前 4675），`dim` 与向量实际维度对齐（1024），
`builtAt` 缺失/不可解析时才补当前时间，否则保留原值。

## 迁移脚本行为

`scripts/migrate-jsonl-to-sqlite.mjs`

```bash
# 在副本上演练（推荐）
cp -p memory.db memory.db-wal memory.db-shm /tmp/memtest/
node scripts/migrate-jsonl-to-sqlite.mjs --out /tmp/memtest/memory.db

# 只读看计划
node scripts/migrate-jsonl-to-sqlite.mjs --out /tmp/memtest/memory.db --dry-run

# 真库（需要显式 --force-live）
node scripts/migrate-jsonl-to-sqlite.mjs --force-live
```

- 输入默认 `$DSH_MEMORY_ROOT/memories.jsonl`，输出默认 `$DSH_MEMORY_ROOT/memory.db`。
- 输出落在真实记忆目录、且没带 `--force-live`：直接拒绝，rc=1，库文件一个字节都不碰。
- `--dry-run` 只读输入；输出库存在就只读比对，报将插入/更新/跳过多少，不建表不写盘。
- 幂等：按 `id` 做 `INSERT ... ON CONFLICT(id) DO UPDATE`；内容与 `rowSeq` 都相同算 skipped，
  内容变了算 updated（目前没有重复 id，重复跑第二轮是 0 写 4672 跳过）。
- 单事务：建表、upsert、meta 修正、全部核对都在 `BEGIN IMMEDIATE` 里；行数/顺序/抽样/meta 任一不符整次 `ROLLBACK`，rc=1。
- 核对表：jsonl 行数 vs memories 行数、status 分组、type 分组、孤儿向量、没有向量的记忆、meta、回放顺序、抽样 20 条逐字段对比。
- 已知孤儿向量 3 条：`m_mw07uksm_t07f` / `m_mw07ukwa_kp41` / `m_mw07ukzc_4c0e`。
  它们只在 `vec_map`/`vec_items` 里，正文不在 jsonl；迁移不删它们，核对时单独列出，出现名单外的孤儿才判失败。

## 只读公共接口

别的插件和脚本读记忆只准走这两个入口，内部只碰 SQLite，不再出现 jsonl / vectors.bin 路径。

### ESM：`lib/public.js`

```js
import { createMemoryReader } from '<repo>/lib/public.js';

const reader = createMemoryReader();                 // 默认真库；测试可 { dbPath: '/tmp/memtest/memory.db' }
reader.migrated();                                   // true / false（库文件不存在会抛 ENOENT）
reader.list({ status: 'active', type: 'world', limit: 20 });
reader.get('m_mvlwhnzg_0ise');                       // 没有返回 null
reader.count({ status: 'active' });                  // 只要数字，不拉整表
reader.allWithVectors({ includeMissing: false });    // [{ id, text, tags, type, ts, status, embedding }]
reader.close();
```

- `allWithVectors()` 默认只有「正文 + 向量都在」的条目，`embedding` 是 `Float32Array(1024)`。
- `includeMissing: true` 时补上：没向量的记忆 `skipped: 'no-vector'`（embedding=null）；
  找不到正文的孤儿向量 `skipped: 'missing-memory'`（embedding 仍有值，text/tags 为 null）。
- 缺 `memories` 表：`migrated()` 返回 false；`list/get/count/allWithVectors` 抛 `MemoryNotMigratedError`（code `ENOTMIGRATED`），
  提示先跑迁移脚本，不会静默返回空数组。
- 库文件不存在：抛 `MemoryReaderError`，code `ENOENT`。
- 只读连接（`readOnly: true`），不写库；向量查询时才懒加载 `vec0.so`。

### CLI：`scripts/memory-query.mjs`

```bash
node scripts/memory-query.mjs --list --status active --json
node scripts/memory-query.mjs --get m_mvlwhnzg_0ise
node scripts/memory-query.mjs --vectors --json --limit 1
node scripts/memory-query.mjs --migrated
node scripts/memory-query.mjs --db /tmp/memtest/memory.db --list --type world --json
```

`--db` 指向副本；默认真库。只读打开。缺表/缺库：stderr 给明确原因，rc=1（`--migrated` 报「未迁移」rc=0）。

## 外部调用方迁移

规矩：外部脚本不许再直接读 `memories.jsonl` / `vectors.bin`，一律走上面的接口。
2026-11-05 第二轮：`bili-daily/daily.mjs`、`scripts/probe-cluster.mjs`、`timer-alias-sync.py`
已改成走接口，`memory_fix.py` 退役（只留拒绝执行的闸门）。

| 调用方 | 现状（直读点） | 改成 | 字段映射 |
| --- | --- | --- | --- |
| `<外部项目>/bili-daily/daily.mjs` | 原 `memoryRecall()`：读 `MEM_STORE/memories.jsonl` + `vectors.bin`，按行序切片算 cosine | 已改：`createMemoryReader().allWithVectors()` 按 `id` 关联正文与 `embedding`（Float32Array，1024 维），只留 `status === 'active'`；顺手修掉 `fetch(\`${EMBED_URL}/embed\`)` 拼成 `/embed/embed` 的老 404（这个工具以前一直静默返回空） | `text→text`、`embedding→向量`、`status` 已归一；cosine ≥0.42 取 4 条的口径没动 |
| `<外部项目>/scripts/probe-cluster.mjs` | 原 6/7 行读 jsonl + vectors.bin，按行号下标对向量 | 已改：`reader.list({ status: 'active' })` + `new Map(allWithVectors().map(r => [r.id, r.embedding]))` | `entities/type` 直接用接口字段 |
| `<外部项目>/timer-alias-sync.py` | 原来逐行扫 `memories.jsonl`，跳过 `archived` 字段 | 已改：`subprocess` 调 `scripts/memory-query.mjs --list --status active --json`（只取 active） | `text` 用接口字段；命中规则（任务名/脚本名 + ≥2 个语义词）不变 |
| `<外部项目>/memory_fix.py` | 整表读 jsonl、重算向量整表写回 `vectors.bin` | 已退役：切换后跑一次就会把库打回迁移前，现在只打印替代做法并 rc=1 | — |
| `<外部项目>/tmp/consolidate_real_run.mjs` | 163/174 行读 jsonl 数条数 | 一次性脚本，不再改；切换后读到的是冻结快照，别拿它的数字当现状 | `lines.length→reader.count()` |
| `<外部项目>/tmp/verify_new_entries.mjs` | 2 行读当前 jsonl、3 行读旧备份 jsonl | 一次性脚本，同上；旧备份对照可留着 | `lines→reader.list()` |
| `<外部项目>/scripts/sqlite-snapshot.sh` | 23–33 行注释仍把 jsonl 写成「唯一原件」 | 注释过时（脚本本身不读文件内容），下次动这个脚本时一起改 | 无 |
| `plugins/dsh-memory/scripts/rebuild_vectors.mjs` | 2026-11-05 已改：从 `memories` 表取 text 重算向量，先在线备份整个库到 `.tmp`、只重建两张向量表再原子换回，正文不动 | 已改完；`--limit N` 只给冒烟用 | `rows[].text←memories.text` |
| 插件内部 `lib/store.js` / `lib/memvec.js` | 2026-11-05 已切：读写只走 `memories` 表 + 向量表，不再碰 jsonl / vectors.bin；向量 KNN 用距离阈值翻页绕开 vec0 的 k=4096 上限 | 已改完 | — |
| `lib/sleep.js` / `lib/split-pass.js` 的快照逻辑 | 备份时仍会把 `memories.jsonl`（和 split 的 `vectors.bin`）拷进 `backup/` | 只是备份文件，不读内容、不影响运行时；留着当切换前的历史副本 | 无 |

迁移期间的建议顺序：先迁库并核对 → 外部脚本逐个改接口并自测 → 最后改 store.js 写路径并停写 jsonl。
2026-11-05 三步都完了：外部脚本这一轮改完并验证，`memories.jsonl` / `vectors.bin` 只剩冻结副本的作用。

## 运行时（2026-11-05 切换后）

- `load()`：`SELECT ... FROM memories ORDER BY rowSeq`，按 rowSeq 重建内存数组，下游结构不变。
  实测 4672 条 118ms（对比旧 jsonl 路径 54ms，多出来的是列解析）。
- `save()`：单事务 `INSERT ... ON CONFLICT(id) DO UPDATE` + 向量 `vecDb.insert`，同一 sqlite 连接；
  失败整笔 `ROLLBACK` 并抛错，同时写 `logs/memory_store.log`。jsonl 一个字节都不写。
- `update()/archive()/merge()/reflect()`：全部落到 `UPDATE`（走同一 upsert），status 语义
  （active / merged / archived）与返回值不变。
- 表不存在：先 checkpoint，把 `memory.db` + `-wal` + `-shm` 整组复制成
  `memory.db.premigrate-YYYYmmdd-HHMM`（带 integrity_check），再 spawn 迁移脚本；
  失败抛错、旧库原样可用，不会半截启动。
- `recall`：候选集是 `vec_map` 全表，不再有 4096 硬封（vec0 编译死了 k≤4096，
  靠「第一页 k=4096 + 第二页 `distance >= 上一页最远距离`」翻页，4675 条两页 64～75ms）。
  RRF 权重、statusWeight、opinion 置信系数、minScore 0.45 / gap 0.1 一点没改。
- `checkpoint()` / `backupTo()` 仍然可用；孤儿向量只报不删。
