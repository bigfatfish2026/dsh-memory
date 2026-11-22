/**
 * 路径拆解 / 路径级迁移的共用内核。
 *
 * 从 scripts/split-path-entities.mjs 抽出来给两个 CLI 用，口径只留一份：
 *   scripts/split-path-entities.mjs   reason='fake_split'（老 10 组假合并拆解，行为不变）
 *   scripts/migrate-entity-paths.mjs  reason='path_split'（P1：按 /plugins/<repo>/ 证据迁到路径级）
 *
 * 四条硬口径：
 *   1) 只有「唯一」证据才迁；多义（一条记忆点到两个仓/两种路径）与无证据一律不迁，
 *      在 memory_entity.attrs.needs_review 追加 <reason>:<run_id>，undo 时按记录摘掉。
 *   2) entity_surface.source 有 CHECK，合法值只有 SURFACE_SOURCES 这七个；迁移行固定用
 *      'migrate'（与 scripts/migrate-entity-surface.mjs 同属数据迁移，语义最近），迁移身份
 *      写到 entity_merge.reason / evidence，不给 CHECK 加值。
 *   3) 实体 norm 一律保留完整路径（src/shared/entity-normalize.ts:222，R21 不取 basename）。
 *      「<repo>/<相对路径>」只是 classify 的目标键（分组口径），不是实体名。
 *   4) undo 按 evidence 记下的精确键恢复：surface 用 (surface, src_entity) 定位，
 *      关联用 (memory_id, entity_id, role) 定位。只搬本批记录过的关联，
 *      目标实体后来新增的关联原样留在目标实体上。
 */
import os from 'node:os';

/** entity_surface.source 的合法值（表上有 CHECK，见 scripts/apply-entity-surface.mjs:38-39）。 */
export const SURFACE_SOURCES = ['migrate', 'gate', 'coarse', 'extract', 'model', 'manual', 'text'];

/** 路径级迁移创建的 surface 行用这个来源，理由见文件头 2)。 */
export const PATH_SURFACE_SOURCE = 'migrate';

const HOME = os.homedir();
const s = (x) => String(x ?? '');
const TOKEN_SPLIT = /[\s，。、；：！？（）()【】\[\]{}"'`<>|—…·]+/u;
const TOKEN_TRAIL = /[.,:;!?，。；：！？、）】」》*]+$/u;

export const nowIso = () => new Date().toISOString();

/** ~ 展开、去 ENV= 前缀、去尾斜杠；大小写不动（精确匹配优先）。 */
export function expandPath(p) {
  let t = s(p).trim().replace(/^[A-Z][A-Z0-9_]*=/, '');
  if (t === '~') t = HOME;
  else if (t.startsWith('~/')) t = HOME + t.slice(1);
  return t.replace(/\/+$/, '');
}

export function baseOf(p) {
  const x = s(p).replace(/\/+$/, '');
  return x.slice(x.lastIndexOf('/') + 1);
}

/**
 * 正文只取带 / 的 token（全路径才算证据）。raw 和去尾标点后的形式都留：
 * raw 用于和 surface 原文精确相等，trim 后用于展开比较（/api/* 这种尾 * 不被误砍）。
 */
export function textTokens(text) {
  const out = [];
  for (const raw of s(text).split(TOKEN_SPLIT)) {
    if (!raw.includes('/')) continue;
    out.push(raw);
    const t = raw.replace(TOKEN_TRAIL, '');
    if (t && t !== raw && t.includes('/')) out.push(t);
  }
  return out;
}

/** 老口径：surface 行 -> 归属目标键。路径写法按展开后的路径归族；裸写法单独一桶。 */
export function targetKeyOfSurface(surface, norm) {
  return s(norm).includes('/') ? expandPath(norm) : `<bare:${s(norm)}>`;
}

/**
 * 新口径：从一条 token 抽 /plugins/<repo>/<相对路径> 证据。
 * 只认展开后是绝对路径的 token；行号后缀 :12 / :12-14 先砍掉。
 * 返回 { repo, rel, key, canonical, basename } 或 null。
 *   key       = <repo>/<相对路径>，classify 的分组键
 *   canonical = 展开后的完整路径，实体 norm / surface 用它（R21）
 */
export function pathEvidenceOf(token) {
  let t = s(token).trim();
  if (!t) return null;
  t = t.replace(/:\d+(?:-\d+)?$/, '');
  t = expandPath(t);
  if (!t.startsWith('/')) return null;
  const idx = t.indexOf('/plugins/');
  if (idx < 0) return null;
  const tail = t.slice(idx + '/plugins/'.length);
  const slash = tail.indexOf('/');
  if (slash <= 0 || slash === tail.length - 1) return null;
  const repo = tail.slice(0, slash);
  const rel = tail.slice(slash + 1).replace(/\/+$/, '');
  if (!repo || !rel || rel.includes('/plugins/')) return null;
  return {
    repo,
    rel,
    key: `${repo}/${rel}`,
    canonical: `${t.slice(0, idx)}/plugins/${repo}/${rel}`,
    basename: baseOf(rel),
  };
}

export function tableExists(db, name) {
  return !!db.prepare(`SELECT 1 FROM sqlite_master WHERE type='table' AND name=?`).get(name);
}

export function requireTables(db, names = ['entities', 'entity_surface', 'memory_entity', 'memories', 'entity_merge']) {
  for (const t of names) if (!tableExists(db, t)) throw new Error(`缺表 ${t}`);
}

/** 某个实体的挂载行（含正文与 entities 原文数组，classify 两个口径都用）。 */
export function mountsOf(db, entityId) {
  return db.prepare(`
    SELECT me.memory_id, me.role, me.status AS me_status, m.status AS mem_status, m.text, m.entities
    FROM memory_entity me JOIN memories m ON m.id = me.memory_id
    WHERE me.entity_id = ? ORDER BY me.memory_id`).all(entityId);
}

/** 一条挂载的 token 集合：正文里带 / 的 token + memories.entities 原文。 */
export function tokensOf(mount) {
  const tokens = new Set(textTokens(mount.text));
  let raws = [];
  try { raws = JSON.parse(mount.entities || '[]'); } catch { raws = []; }
  if (Array.isArray(raws)) for (const t of raws) if (typeof t === 'string' && t) tokens.add(t);
  return tokens;
}

/**
 * 老口径 classify（split-path-entities 原 135-180）：拿旧实体的 surface 行做比对。
 *   a) 原文精确相等优先；b) 展开后相等；c) 都不中但 basename 撞上算 bare。
 * 返回逐条挂载 {memoryId, role, meStatus, memStatus, mode, target} + bucket 计数。
 */
export function classifyBySurfaces(db, { entityId, rows, keyOf = targetKeyOfSurface }) {
  const out = [];
  for (const m of mountsOf(db, entityId)) {
    const tokens = tokensOf(m);
    const hits = new Set();
    for (const t of tokens) {
      const exact = rows.filter((r) => t === r.surface);
      if (exact.length) {
        for (const r of exact) hits.add(keyOf(r.surface, r.norm));
        continue;
      }
      const et = expandPath(t);
      for (const r of rows) if (expandPath(r.norm) === et) hits.add(keyOf(r.surface, r.norm));
    }
    let mode;
    let target = null;
    if (hits.size === 1) {
      mode = 'unique';
      target = [...hits][0];
    } else if (hits.size > 1) {
      mode = 'ambiguous';
    } else {
      const bareHit = [...tokens].some((t) =>
        rows.some((r) => baseOf(t).toLowerCase() === baseOf(r.norm).toLowerCase()));
      mode = bareHit ? 'bare' : 'none';
    }
    out.push({ memoryId: m.memory_id, role: m.role, meStatus: m.me_status, memStatus: m.mem_status, mode, target });
  }
  const bucket = out.reduce((a, d) => ((a[d.mode] = (a[d.mode] || 0) + 1), a), {});
  return { mounts: out, bucket, mountTotal: out.length };
}

/**
 * 新口径 classify：从正文抽 /plugins/<repo>/<rel>。
 * mustBasename 给了就只认 basename 与实体裸名相等的证据（防止把「plugins 目录」这类
 * 目录实体的关联误迁到目录下某个具体文件）。
 * mode: unique（唯一 key 且 canonical 一致）/ ambiguous（多 key 或同 key 多 canonical）/ none。
 */
export function classifyByPathEvidence(db, { entityId, mustBasename = '', keyOf = pathEvidenceOf }) {
  const out = [];
  for (const m of mountsOf(db, entityId)) {
    const tokens = tokensOf(m);
    const hits = new Map();
    let canonicalConflict = false;
    for (const t of tokens) {
      const ev = keyOf(t);
      if (!ev) continue;
      if (mustBasename && ev.basename.toLowerCase() !== mustBasename.toLowerCase()) continue;
      const prev = hits.get(ev.key);
      if (prev && prev !== ev.canonical) canonicalConflict = true;
      if (!prev) hits.set(ev.key, ev.canonical);
    }
    let mode;
    if (hits.size === 1 && !canonicalConflict) mode = 'unique';
    else if (hits.size > 1 || canonicalConflict) mode = 'ambiguous';
    else mode = 'none';
    out.push({
      memoryId: m.memory_id,
      role: m.role,
      meStatus: m.me_status,
      memStatus: m.mem_status,
      mode,
      key: hits.size === 1 ? [...hits.keys()][0] : null,
      canonical: hits.size === 1 ? [...hits.values()][0] : null,
      keys: [...hits.keys()],
    });
  }
  const bucket = out.reduce((a, d) => ((a[d.mode] = (a[d.mode] || 0) + 1), a), {});
  return { mounts: out, bucket, mountTotal: out.length };
}

/** memories.entities 原文里各写法的出现次数（A6 口径：surface.count 要等于它）。 */
export function surfaceOccurrences(db, keys) {
  const want = new Set(keys);
  const occ = new Map();
  for (const m of db.prepare('SELECT entities FROM memories').iterate()) {
    let arr = [];
    try { arr = JSON.parse(m.entities || '[]'); } catch { arr = []; }
    if (!Array.isArray(arr)) continue;
    const seen = new Set();
    for (const t of arr) {
      if (typeof t !== 'string' || !t || !want.has(t) || seen.has(t)) continue;
      seen.add(t);
      occ.set(t, (occ.get(t) || 0) + 1);
    }
  }
  return occ;
}

function markReview(db, entityId, mounts, marker) {
  const ids = [];
  for (const m of mounts) {
    const role = m.role || '';
    const row = role
      ? db.prepare('SELECT attrs FROM memory_entity WHERE entity_id=? AND memory_id=? AND role=?').get(entityId, m.memoryId, role)
      : db.prepare('SELECT attrs FROM memory_entity WHERE entity_id=? AND memory_id=?').get(entityId, m.memoryId);
    if (!row) throw new Error(`复核挂载 ${m.memoryId} 在实体 ${entityId} 上不见了`);
    let attrs = {};
    try { attrs = JSON.parse(row.attrs || '{}') || {}; } catch { attrs = {}; }
    const cur = attrs.needs_review;
    const arr = Array.isArray(cur) ? cur.slice() : (typeof cur === 'string' && cur ? [cur] : []);
    if (!arr.includes(marker)) arr.push(marker);
    attrs.needs_review = arr;
    if (role) db.prepare('UPDATE memory_entity SET attrs=? WHERE entity_id=? AND memory_id=? AND role=?')
      .run(JSON.stringify(attrs), entityId, m.memoryId, role);
    else db.prepare('UPDATE memory_entity SET attrs=? WHERE entity_id=? AND memory_id=?')
      .run(JSON.stringify(attrs), entityId, m.memoryId);
    ids.push(m.memoryId);
  }
  return ids;
}

/**
 * 真迁移。plan 形状：
 * {
 *   reason, runId, source, oldEntityId, now?,
 *   groups: [{
 *     key, canonical, etype, entityAttrs?, evidence?, reviewMarker?,
 *     surfaces: [{kind:'update'|'insert', surface, norm?, count?, flag?}],
 *     moved:  [{memoryId, role}],
 *     review: [{memoryId, role}],
 *   }]
 * }
 * 一个 group = 一个目标实体 = 一条 entity_merge 账本（src=新, dst=旧）。
 * 目标 norm 已存在时不硬撞，整组不迁，关联标 needs_review（path_split:exists:<run>）。
 * 返回 stats，打印留给调用方。
 */
export function applyBatch(db, plan) {
  const { reason, runId, source, oldEntityId } = plan;
  if (!reason) throw new Error('applyBatch 缺 reason');
  if (!runId) throw new Error('applyBatch 缺 runId');
  if (!SURFACE_SOURCES.includes(source)) {
    throw new Error(`surface source=${source} 不在 CHECK 允许值里：${SURFACE_SOURCES.join('/')}`);
  }
  const ts = plan.now || nowIso();
  const stats = { created: 0, skipped: 0, moved: 0, review: 0, groups: [] };
  const recalc = db.prepare('UPDATE entities SET mem_count=(SELECT count(*) FROM memory_entity WHERE entity_id=?) WHERE id=?');

  for (const g of plan.groups) {
    const dup = db.prepare(`
      SELECT id FROM entity_merge
      WHERE reason=? AND undone_at='' AND run_id=? AND json_extract(evidence,'$.key')=?`).get(reason, runId, g.key);
    if (dup) {
      stats.skipped++;
      stats.groups.push({ key: g.key, skipped: true, why: `本 run 已有未 undo 记录 merge#${dup.id}` });
      continue;
    }

    const existing = db.prepare('SELECT id,status FROM entities WHERE norm=?').get(g.canonical);
    if (existing) {
      // 不硬撞、不写第二条账本（uq_entity_merge_src）；关联交给调用方用 reviewOnly 记账。
      stats.skipped++;
      stats.groups.push({
        key: g.key, canonical: g.canonical, skipped: true,
        why: `目标实体已存在 id=${existing.id}，不硬撞`, existingId: Number(existing.id),
        mounts: (g.moved || []).map((m) => ({ memoryId: m.memoryId, role: m.role || 'about' })),
      });
      continue;
    }

    const attrs = { split_from: oldEntityId, canonical: g.canonical, ...(g.entityAttrs || {}) };
    const ins = db.prepare(`
      INSERT INTO entities(norm,display,etype,attrs,first_seen,last_seen,mem_count,status)
      VALUES(?,?,?,?,?,?,0,'active')`).run(
      g.canonical, g.display || g.canonical, g.etype || 'file',
      JSON.stringify(attrs), ts, ts,
    );
    const newId = Number(ins.lastInsertRowid);

    const updated = [];
    const inserted = [];
    for (const sp of g.surfaces || []) {
      if (sp.kind === 'update') {
        const u = db.prepare('UPDATE entity_surface SET entity_id=? WHERE surface=?').run(newId, sp.surface);
        if (u.changes !== 1) throw new Error(`${relationName(reason)} 改指 surface=${sp.surface} 影响 ${u.changes} 行`);
        updated.push(sp.surface);
      } else {
        db.prepare(`
          INSERT INTO entity_surface(surface,norm,entity_id,source,count,first_seen,last_seen,flag)
          VALUES(?,?,?,?,?,?,?,?)`).run(
          sp.surface, sp.norm || sp.surface, newId, source, sp.count || 0, ts, ts, sp.flag ?? 'path',
        );
        inserted.push(sp.surface);
      }
    }

    for (const m of g.moved || []) {
      const role = m.role || 'about';
      const insMe = db.prepare(`
        INSERT OR IGNORE INTO memory_entity(memory_id,entity_id,role,ts,status,attrs,version,prev_memory_id,origin)
        SELECT memory_id, ?, role, ts, coalesce(nullif(status,''),'active'), attrs, version, prev_memory_id, origin
        FROM memory_entity WHERE entity_id=? AND memory_id=? AND role=?`).run(newId, oldEntityId, m.memoryId, role);
      const landed = db.prepare('SELECT 1 FROM memory_entity WHERE entity_id=? AND memory_id=? AND role=?').get(newId, m.memoryId, role);
      if (insMe.changes !== 1 && !landed) {
        throw new Error(`${relationName(reason)} 搬挂载 ${m.memoryId}/${role} 失败（insert changes=${insMe.changes}）`);
      }
      const del = db.prepare('DELETE FROM memory_entity WHERE entity_id=? AND memory_id=? AND role=?').run(oldEntityId, m.memoryId, role);
      if (del.changes < 1) throw new Error(`${relationName(reason)} 删旧挂载 ${m.memoryId}/${role} 影响 0 行`);
    }

    const marker = g.reviewMarker || `${reason}:${runId}`;
    const reviewIds = markReview(db, oldEntityId, g.review || [], marker);

    recalc.run(oldEntityId, oldEntityId);
    recalc.run(newId, newId);

    const evidence = {
      ...(g.evidence || {}),
      key: g.key,
      old_id: oldEntityId,
      new_id: newId,
      canonical: g.canonical,
      memory_ids: (g.moved || []).map((m) => m.memoryId),
      mounts: (g.moved || []).map((m) => ({ memory_id: m.memoryId, role: m.role || 'about' })),
      review_memory_ids: reviewIds,
      review_marker: marker,
      surfaces: { updated, inserted },
    };
    db.prepare(`
      INSERT INTO entity_merge(src_entity,dst_entity,reason,evidence,run_id,created_at)
      VALUES(?,?,?,?,?,?)`).run(newId, oldEntityId, reason, JSON.stringify(evidence), runId, ts);

    stats.created++;
    stats.moved += (g.moved || []).length;
    stats.review += reviewIds.length;
    stats.groups.push({
      key: g.key, canonical: g.canonical, oldId: oldEntityId, newId,
      moved: (g.moved || []).length, review: reviewIds.length,
      surfaces: { updated: updated.length, inserted: inserted.length },
    });
  }
  return stats;
}

const relationName = (reason) => (reason === 'fake_split' ? `fake_split/${reason}` : reason);

/**
 * 只标复核、不迁关联的账本（目标实体已存在时用）：写一条 review_only 的
 * entity_merge（src=dst=原实体），把 marker 与 memory_ids 记进去，undo 能精确摘掉。
 * uq_entity_merge_src 撞车（原实体已是别的未 undo 记录 src）时不硬写，返回 ok:false 交调用方报数。
 */
export function markReviewOnly(db, { reason, runId, entityId, mounts, marker, evidence = {}, now = null }) {
  const ts = now || nowIso();
  if (!mounts.length) return { ok: true, reviewIds: [], marker, skipped: true };
  const ids = markReview(db, entityId, mounts, marker);
  try {
    db.prepare(`
      INSERT INTO entity_merge(src_entity,dst_entity,reason,evidence,run_id,created_at)
      VALUES(?,?,?,?,?,?)`).run(entityId, entityId, reason, JSON.stringify({
      ...evidence, review_only: true, old_id: entityId, memory_ids: [], mounts: [],
      review_memory_ids: ids, review_marker: marker,
    }), runId, ts);
  } catch (e) {
    return { ok: false, reviewIds: ids, marker, error: String(e && e.message || e) };
  }
  return { ok: true, reviewIds: ids, marker };
}

/**
 * 逆做。按 evidence 记下的精确键恢复：
 *   - 关联：(memory_id, entity_id, role) 从 src 搬回 dst，只动本批记过的；
 *   - surface：updated 按 (surface, src) 改回 dst；inserted 按 (surface, src) 删；
 *   - needs_review：按 review_marker 从 dst 的对应挂载上摘掉；
 *   - 新实体：挂载与 surface 都清空才删，还有残留就留着并报出来（不硬删）。
 * 老 fake_split 记录没有 surfaces/mounts 字段时回落到 keys / memory_ids。
 */
export function undoBatch(db, { reason, runId = null }) {
  if (!reason) throw new Error('undoBatch 缺 reason');
  const rows = runId
    ? db.prepare(`SELECT * FROM entity_merge WHERE reason=? AND undone_at='' AND run_id=? ORDER BY id DESC`).all(reason, runId)
    : db.prepare(`SELECT * FROM entity_merge WHERE reason=? AND undone_at='' ORDER BY id DESC`).all(reason);
  const stats = {
    undone: 0, restoredMounts: 0, surfacesBack: 0, surfacesDeleted: 0,
    deletedEntities: 0, keptEntities: [], records: [],
  };
  const recalc = db.prepare('UPDATE entities SET mem_count=(SELECT count(*) FROM memory_entity WHERE entity_id=?) WHERE id=?');
  const ts = nowIso();

  for (const r of rows) {
    const ev = JSON.parse(r.evidence || '{}');
    const src = Number(r.src_entity);
    const dst = Number(r.dst_entity);
    if (!db.prepare('SELECT 1 FROM entities WHERE id=?').get(dst)) {
      throw new Error(`undo 目标原实体 ${dst} 不存在，停（merge#${r.id}）`);
    }

    const reviewOnly = ev.review_only === true;
    const mounts = !reviewOnly && Array.isArray(ev.mounts) && ev.mounts.length
      ? ev.mounts.map((x) => ({ memoryId: s(x.memory_id ?? x.memoryId), role: s(x.role) }))
      : (!reviewOnly && Array.isArray(ev.memory_ids) ? ev.memory_ids.map((mid) => ({ memoryId: s(mid), role: '' })) : []);

    for (const m of mounts) {
      const roleCond = m.role ? ' AND role=?' : '';
      const back = m.role ? [dst, src, m.memoryId, m.role] : [dst, src, m.memoryId];
      const ins = db.prepare(`
        INSERT OR IGNORE INTO memory_entity(memory_id,entity_id,role,ts,status,attrs,version,prev_memory_id,origin)
        SELECT memory_id, ?, role, ts, coalesce(nullif(status,''),'active'), attrs, version, prev_memory_id, origin
        FROM memory_entity WHERE entity_id=? AND memory_id=?${roleCond}`).run(...back);
      stats.restoredMounts += ins.changes;
      const gone = m.role ? [src, m.memoryId, m.role] : [src, m.memoryId];
      db.prepare(`DELETE FROM memory_entity WHERE entity_id=? AND memory_id=?${roleCond}`).run(...gone);
    }

    const touched = ev.surfaces && typeof ev.surfaces === 'object' ? ev.surfaces : null;
    const updatedSurfaces = Array.isArray(touched?.updated) ? touched.updated : (Array.isArray(ev.keys) ? ev.keys : []);
    for (const surface of updatedSurfaces) {
      const u = db.prepare('UPDATE entity_surface SET entity_id=? WHERE surface=? AND entity_id=?').run(dst, surface, src);
      if (u.changes !== 1) throw new Error(`undo 改回 surface=${surface} 影响 ${u.changes} 行（merge#${r.id}）`);
      stats.surfacesBack += u.changes;
    }
    const insertedSurfaces = Array.isArray(touched?.inserted) ? touched.inserted : [];
    for (const surface of insertedSurfaces) {
      const d = db.prepare('DELETE FROM entity_surface WHERE surface=? AND entity_id=?').run(surface, src);
      if (d.changes !== 1) throw new Error(`undo 删本批 surface=${surface} 影响 ${d.changes} 行（merge#${r.id}）`);
      stats.surfacesDeleted += d.changes;
    }

    const marker = ev.review_marker || `${reason}:${ev.group || ''}`;
    for (const mid of Array.isArray(ev.review_memory_ids) ? ev.review_memory_ids : []) {
      const row = db.prepare('SELECT attrs FROM memory_entity WHERE entity_id=? AND memory_id=?').get(dst, mid);
      if (!row) continue;
      let attrs = {};
      try { attrs = JSON.parse(row.attrs || '{}') || {}; } catch { attrs = {}; }
      const cur = attrs.needs_review;
      const arr = (Array.isArray(cur) ? cur : (typeof cur === 'string' && cur ? [cur] : [])).filter((x) => x !== marker);
      if (arr.length) attrs.needs_review = arr;
      else delete attrs.needs_review;
      db.prepare('UPDATE memory_entity SET attrs=? WHERE entity_id=? AND memory_id=?')
        .run(JSON.stringify(attrs), dst, mid);
    }

    recalc.run(dst, dst);

    if (!reviewOnly) {
      const leftMounts = db.prepare('SELECT count(*) c FROM memory_entity WHERE entity_id=?').get(src).c;
      const leftSurfaces = db.prepare('SELECT count(*) c FROM entity_surface WHERE entity_id=?').get(src).c;
      if (!leftMounts && !leftSurfaces) {
        db.prepare('DELETE FROM entities WHERE id=?').run(src);
        stats.deletedEntities++;
      } else {
        recalc.run(src, src);
        stats.keptEntities.push({ id: src, mounts: leftMounts, surfaces: leftSurfaces });
      }
    }

    db.prepare('UPDATE entity_merge SET undone_at=? WHERE id=?').run(ts, r.id);
    stats.undone++;
    stats.records.push({ mergeId: r.id, src, dst, group: ev.group || ev.key || '', reason });
  }
  return stats;
}
