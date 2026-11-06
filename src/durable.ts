import type {
    Durable,
    DurableOptions,
    Migration,
    StartOptions,
    WorkflowFn,
    WorkflowHandle
} from "./types.js";
import {DuplicateWorkflowError, DurableError} from "./errors.js";
import {Pool, type PoolClient} from "pg";
import {migrations} from "./migrations.js";
import {
    appliedVersions,
    createSchema,
    ensureMigrationsTable, getWorkflow,
    insertWorkflow,
    lockMigration,
    unlockMigration,
} from "./store.js";
import {randomUUID} from "node:crypto";
import { setTimeout as sleep } from "node:timers/promises";
import { hostname } from "node:os";
import { createExecutor, type Registered } from "./executor.js";

export const createDurable = (options: DurableOptions): Durable => {

    if (!options.pool && !options.connectionString) {
        throw new DurableError("Either pool or connectionString is required");
    }

    const schema = options.schema || 'durable';

    if (!/^[a-z_][a-z0-9_]*$/.test(schema)) {
        throw new DurableError(`invalid schema name: ${schema}`);
    }

    const ownsPool = options.pool === undefined;
    const leaseMs = options.leaseMs ?? 30_000;

    const pool = options.pool ?? new Pool({ connectionString: options.connectionString })
    const registry = new Map<string, Registered>()

    const executor = createExecutor({
        pool,
        schema,
        registry,
        executorId: options.executorId ?? `${hostname()}:${process.pid}:${randomUUID().slice(0, 8)}`,
        leaseMs,
        heartbeatMs: options.heartbeatMs ?? Math.floor(leaseMs / 3),
        pollMs: options.pollMs ?? 1_000,
        maxConcurrent: options.maxConcurrent ?? 50,
        maxRecoveryAttempts: options.maxRecoveryAttempts ?? 10,
        hooks: options.testHooks ?? {},
    });

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

    const waitForResult = async<O>(id: string) => {
        while(true) {
            const row = await getWorkflow(pool, schema, id);
            if (!row) {
                throw new DurableError(`Workflow ${id} not found`)
            }
            if (row.status === 'completed') return row.output as O;
            if (row.status === 'failed') throw new DurableError(`workflow ${id} failed: ${row.error?.message ?? "unknown error"}`, { cause: row.error });
            await sleep(100)
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
        async start(): Promise<void> {
            await executor.start();
        },
        async shutdown() {
            await executor.stop();
            if (ownsPool) await pool.end();
        },

        workflow<I,O>(name: string, fn: WorkflowFn<I, O>, opts?: {version?: string}) {
            if (registry.has(name)) throw new DuplicateWorkflowError(`Workflow ${name} is already registered`)
            const version = opts?.version ?? "1"
            registry.set(name, { fn, version })

            return {
                name,
                async start(input: I, opts?: StartOptions): Promise<WorkflowHandle<O>> {
                    const id = opts?.id ?? randomUUID();
                    const inserted = await insertWorkflow(pool, schema, {id, name, input}, version)
                    if (inserted) await executor.claimAndLaunch(1, [id]);
                    return { id, result: () => waitForResult<O>(id) };
                },
            };
        },
        async resume(id: string): Promise<void> {
            await executor.runWorkflow(id, new AbortController());
        }
    }
}
