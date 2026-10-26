
import type {Queryable} from "./types.js";

export const createSchema = async (db: Queryable, schema: string) => {
    await db.query(`CREATE SCHEMA IF NOT EXISTS ${schema}`)
}

export const lockMigration = async (db: Queryable, lockKey: string) => {
    await db.query("SELECT pg_advisory_lock(hashtext($1))", [lockKey]);
}

export const unlockMigration = async (db: Queryable, lockKey: string) => {
    await db.query("SELECT pg_advisory_unlock(hashtext($1))", [lockKey]);
}

export const ensureMigrationsTable = async (db: Queryable, schema: string) => {
    await db.query(
    `CREATE TABLE IF NOT EXISTS ${schema}.migrations (
            version    int PRIMARY KEY,
            applied_at timestamptz NOT NULL DEFAULT now()
        );`);
}

export const appliedVersions = async (db: Queryable, schema: string): Promise<Set<number>> => {
    const res =  await db.query<{version: number}>(`SELECT version FROM ${schema}.migrations`);
    return new Set(res.rows.map(r => r.version))
}