import type {
    Durable,
    DurableOptions,
    Migration,  SerializedError,
    StartOptions,
    WorkflowContext,
    WorkflowFn,
    WorkflowHandle
} from "./types.js";
import {DuplicateWorkflowError, DurableError} from "./errors.js";
import {Pool, type PoolClient} from "pg";
import {migrations} from "./migrations.js";
import {
    appliedVersions, completeWorkflow,
    createSchema,
    ensureMigrationsTable, failWorkflow, getWorkflow,
    insertWorkflow,
    lockMigration, markRunning,
    unlockMigration,
    loadSteps, saveStep
} from "./store.js";
import {randomUUID} from "node:crypto";
import { setTimeout as sleep } from "node:timers/promises";
import {toJson} from "./json.js";

export const createDurable = (options: DurableOptions): Durable => {

    if (!options.pool && !options.connectionString) {
        throw new DurableError("Either pool or connectionString is required");
    }

    const schema = options.schema || 'durable';

    if (!/^[a-z_][a-z0-9_]*$/.test(schema)) {
        throw new DurableError(`invalid schema name: ${schema}`);
    }

    const ownsPool = options.pool === undefined;

    const pool = options.pool ?? new Pool({ connectionString: options.connectionString })
    const registry = new Map<string, WorkflowFn<any, any>>()
    const running = new Set<Promise<void>>();


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

    const serializeError = (e: unknown): SerializedError => {
        if (e instanceof Error) {
            return { name: e.name, message: e.message, stack: e.stack }
        }
        return { name: 'Error', message: String(e)}
    }



    const runWorkflow = async (id: string): Promise<void> => {
        const row = await getWorkflow(pool, schema, id);
        if (!row || row.status === 'completed' || row.status === 'failed') return;
        const fn = registry.get(row.name);
        if (!fn) {
            console.warn(`Function doesnt exists! Id: ${id}, WorkflowName: ${row.name}`);
            return;
        }
        await markRunning(pool, schema, id);
        const steps = await loadSteps(pool, schema, id);
        const history = new Map(steps.map((s) => [s.seq, s] as const));
        let nextSeq = 0;

        const ctx: WorkflowContext = {
            workflowId: id,
            step: async <T>(name: string, stepFn: () => Promise<T>): Promise<T> => {
                const seq = nextSeq++;

                const saved = history.get(seq);
                if (saved) {
                    if (saved.error) throw new DurableError(saved.error.message, { cause: saved.error });
                    return saved.output as T;
                }

                //const json = JSON.stringify(await stepFn()) ?? "null";
                const json = toJson(await stepFn(), `result of step ${name} (seq ${seq})`);
                await saveStep(pool, schema, { workflow_id: id, seq, name, kind: "step", output: json, error: null });
                return JSON.parse(json) as T;
            },
        };
        try {
            const output = await fn(ctx, row.input)
            await completeWorkflow(pool, schema, id, output);
        } catch (e) {
           const error: SerializedError =  serializeError(e);
           await failWorkflow(pool, schema, id, error)
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
        async shutdown() {
            await Promise.allSettled([...running])
            if (ownsPool) await pool.end();
        },
        workflow<I,O>(name: string, fn: WorkflowFn<I, O>, opts?: {version?: string}) {
            if (registry.has(name)) throw new DuplicateWorkflowError(`Workflow ${name} is already registered`)
            registry.set(name, fn)
            const version = opts?.version ?? "1"
            return {
                name,
                async start(input: I, opts?: StartOptions): Promise<WorkflowHandle<O>> {
                    const id = opts?.id ?? randomUUID();

                    const inserted = await insertWorkflow(pool, schema, {id, name, input}, version)
                    if (inserted) {
                        const run = runWorkflow(id).catch((err) => console.error("durable-pg: runWorkflow failed", id, err));
                        running.add(run);
                        run.finally(() => running.delete(run))
                    }
                    return { id, result: () => waitForResult<O>(id) };
                },
            };
        },
        async resume(id: string): Promise<void> {
            await  runWorkflow(id);
        }
    }
}