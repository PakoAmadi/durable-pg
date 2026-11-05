
import type {Queryable, NewWorkflow, WorkflowRow, SerializedError, StepRow} from "./types.js";
import { LeaseLostError } from "./errors.js";
import {toJson} from "./json.js";


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

export const insertWorkflow = async (db: Queryable, schema: string, newWorkflow: NewWorkflow, version: string) => {
    const  res = await db.query(`INSERT INTO ${schema}.workflows (id, name, version, status, input)
                        VALUES ($1, $2, $3, 'pending', $4::jsonb)
                        ON CONFLICT (id) DO NOTHING
                        RETURNING id`, [newWorkflow.id, newWorkflow.name,  version, toJson(newWorkflow.input, `input of workflow ${newWorkflow.id}`)]);
    return res.rowCount === 1;
}


export const getWorkflow = async (db: Queryable, schema: string, id: string): Promise<WorkflowRow | undefined> => {
    const res = await db.query<WorkflowRow>(`SELECT id, name, status, input, output, error
                                                            FROM ${schema}.workflows WHERE id = $1`, [id]);
    return res.rows[0]
}

export const completeWorkflow = async (db: Queryable, schema: string, id: string, executorId: string, output: unknown) => {
     const res = await db.query(`UPDATE ${schema}.workflows SET status='completed', 
                output = $2::jsonb, updated_at=now() WHERE id=$1 AND executor_id = $3`, [id,  toJson(output, `output of workflow ${id}`), executorId]);
     if(res.rowCount === 0) throw new LeaseLostError(`lease lost on workflow ${id}`);
}

export const failWorkflow =  async(db: Queryable, schema: string, id: string, executorId: string, error: SerializedError) => {
    const res = await db.query(`UPDATE ${schema}.workflows SET status='failed', 
                error = $2::jsonb, updated_at=now() WHERE id=$1 AND executor_id=$3`, [id, JSON.stringify(error ?? null), executorId]);
    if (res.rowCount === 0) throw new LeaseLostError(`lease lost on workflow ${id}`);
}

export const loadSteps = async (db:Queryable, schema: string, workflowId: string) => {
    const res = await db.query(`SELECT seq, name, output, kind, error FROM ${schema}.steps WHERE workflow_id = $1 ORDER BY seq`, [workflowId]);
    return res.rows;
}

export const saveStep = async (db: Queryable, schema: string, executorId: string, stepRow: StepRow) => {
    const res = await db.query(
        `INSERT INTO ${schema}.steps (workflow_id, seq, name, kind, output, error)
         SELECT $1, $2, $3, $4, $5, $6
          WHERE EXISTS (SELECT 1 FROM ${schema}.workflows WHERE id = $1 AND executor_id = $7)`,
        [stepRow.workflow_id, stepRow.seq, stepRow.name, stepRow.kind, stepRow.output, stepRow.error, executorId]);
    if (res.rowCount === 0) throw new LeaseLostError(`lease lost on workflow ${stepRow.workflow_id}`);
};


export const claimWorkflows = async (
    db: Queryable,
    schema: string,
    opts: {
        executorId: string;
        leaseMs: number;
        limit: number;
        workflows: { name: string; version: string }[];
        ids?: string[];
    },
): Promise<{ id: string; recoveryAttempts: number }[]> => {
    if (opts.limit <= 0 || opts.workflows.length === 0) return [];

    const claimed = await db.query<{ id: string; recovery_attempts: number }>(
        `UPDATE ${schema}.workflows
            SET status            = 'running',
                executor_id       = $1,
                lease_until       = now() + $2 * interval '1 millisecond',
                recovery_attempts = recovery_attempts + 1,
                updated_at        = now()
          WHERE id IN (
                SELECT id
                  FROM ${schema}.workflows
                 WHERE status IN ('pending', 'running')
                   AND (lease_until IS NULL OR lease_until < now())
                   AND (name, version) IN (SELECT * FROM unnest($3::text[], $4::text[]))
                   AND ($6::text[] IS NULL OR id = ANY($6::text[]))
                 LIMIT $5
                   FOR UPDATE SKIP LOCKED
          )
          RETURNING id, recovery_attempts`,
        [
            opts.executorId,                         // $1
            opts.leaseMs,                            // $2
            opts.workflows.map((w) => w.name),       // $3
            opts.workflows.map((w) => w.version),    // $4
            opts.limit,                              // $5
            opts.ids ?? null,                        // $6: null = без фильтра по id
        ],
    );

    return claimed.rows.map((r) => ({ id: r.id, recoveryAttempts: r.recovery_attempts }));
};