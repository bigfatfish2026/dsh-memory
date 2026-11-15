/**
 * SQLite 写事务的公共件。
 *
 * 六处 BEGIN IMMEDIATE / COMMIT / ROLLBACK 复制收成三个入口，按形态分开、不混用：
 *   withTransaction       自己开写事务（store 写入、ingest 落账、entity 落地）
 *   withSavepoint         调用方已在事务里，只回滚这一小段（逐条失败隔离）
 *   withNestedTransaction 两者都要：有外层就 SAVEPOINT，没有就自己 BEGIN（memory_link）
 *
 * 纯读事务（BEGIN / COMMIT，不拿写锁，见 db-fingerprint.js）不在这里。
 */

/** 事务句柄：只要求 exec；`isTransaction` 存在即视为调用方已在事务里。 */
export interface TransactionalDb {
  exec(sql: string): unknown;
  isTransaction?: boolean;
}

/** 自己开一笔写事务；fn 抛错整笔回滚并把错误原样抛回去。 */
export function withTransaction<D extends TransactionalDb, T>(db: D, fn: (db: D) => T): T {
  db.exec('BEGIN IMMEDIATE');
  try {
    const out = fn(db);
    db.exec('COMMIT');
    return out;
  } catch (error) {
    try {
      db.exec('ROLLBACK');
    } catch {
      /* 连接坏了由 sqlite 自己回滚 */
    }
    throw error;
  }
}

/** 在已有事务里开一个保存点；fn 抛错只回滚到保存点，然后把错误抛回去。 */
export function withSavepoint<D extends TransactionalDb, T>(db: D, name: unknown, fn: (db: D) => T): T {
  const sp = String(name);
  db.exec(`SAVEPOINT ${sp}`);
  try {
    const out = fn(db);
    db.exec(`RELEASE ${sp}`);
    return out;
  } catch (error) {
    try {
      db.exec(`ROLLBACK TO ${sp}`);
    } catch {
      /* savepoint 自己没了就由外层事务兜底 */
    }
    try {
      db.exec(`RELEASE ${sp}`);
    } catch {
      /* 同上 */
    }
    throw error;
  }
}

/** 调用方已在事务里就开 SAVEPOINT（只回滚这一小段），否则自己 BEGIN IMMEDIATE 拿写锁。 */
export function withNestedTransaction<D extends TransactionalDb, T>(db: D, name: unknown, fn: (db: D) => T): T {
  if (!db.isTransaction) return withTransaction(db, fn);
  return withSavepoint(db, name, fn);
}
