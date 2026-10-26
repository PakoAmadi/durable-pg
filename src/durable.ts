import type {Durable, DurableOptions, Migration} from "./types.js";
import {DurableError} from "./errors.js";
import {Pool, type PoolClient} from "pg";
import {migrations} from "./migrations.js";
import {appliedVersions, createSchema, ensureMigrationsTable, lockMigration, unlockMigration} from "./store.js";

export const createDurable = (options: DurableOptions): Durable => {

    if (!options.pool && !options.connectionString) {
        throw new DurableError("Еither pool or connectionString is required");
    }

    const schema = options.schema || 'durable';

    if (!/^[a-z_][a-z0-9_]*$/.test(schema)) {
        throw new DurableError(`invalid schema name: ${schema}`);
    }

    const ownsPool = options.pool === undefined;

    const pool = options.pool ?? new Pool({ connectionString: options.connectionString })


    const lockKey = `durable-pg:migrate:${schema}`;
    const doMigration = async(client: PoolClient, toApply: Migration[], applied: Set<number>) => {
        for (const migration of toApply) {
            if (applied.has(migration.version)) continue;

            await client.query("BEGIN");
            try {
                await client.query(migration.sql(schema));
                await client.query(`INSERT INTO ${schema}.migrations(version) VALUES ($1)`, [migration.version]);
                await client.query("COMMIT");
            } catch (e) {
                await client.query("ROLLBACK");
                throw new DurableError(`migration ${migration.version} failed`, { cause: e });
            }
        }
    }

    return {
        async migrate(): Promise<void> {
            const codeVersions = migrations.map((m) => m.version)
            if (new Set(codeVersions).size !== codeVersions.length) {
                throw new DurableError('Duplicate migration versions')
            }
            const client = await pool.connect()
            try {
                await lockMigration(client, lockKey);
                await createSchema(client, schema);
                await ensureMigrationsTable(client, schema);
                const applied = await appliedVersions(client, schema);
                const sorted = [...migrations].sort((a, b) => a.version - b.version);
                await doMigration(client, sorted, applied)


            } finally {
                await unlockMigration(client, lockKey);
                client.release();
            }

        },
        async shutdown() {
            if (ownsPool) await pool.end();
        }
    }
}