
import type {Queryable, NewWorkflow, WorkflowRow, SerializedError, StepRow} from "./types.js";
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

export const markRunning = async(db: Queryable, schema: string, id: string) => {
    await db.query(`UPDATE ${schema}.workflows SET status = 'running', updated_at=now() WHERE id = $1`, [id])
}

export const completeWorkflow = async (db: Queryable, schema: string, id: string, output: unknown) => {
     await db.query(`UPDATE ${schema}.workflows SET status='completed', 
                output = $2::jsonb, updated_at=now() WHERE id=$1`, [id,  toJson(output, `output of workflow ${id}`)])
}

export const failWorkflow =  async(db: Queryable, schema: string, id: string, error: SerializedError) => {
    await db.query(`UPDATE ${schema}.workflows SET status='failed', 
                error = $2::jsonb, updated_at=now() WHERE id=$1`, [id, JSON.stringify(error ?? null)])
}

export const loadSteps = async (db:Queryable, schema: string, workflowId: string) => {
    const res = await db.query(`SELECT seq, name, output, kind, error FROM ${schema}.steps WHERE workflow_id = $1 ORDER BY seq`, [workflowId]);
    return res.rows;
}

export const saveStep = async (db:Queryable, schema: string, stepRow: StepRow) => {
    await db.query(`INSERT INTO ${schema}.steps (workflow_id, seq, name, kind, output, error)
                    VALUES($1, $2, $3, $4, $5, $6)`, [stepRow.workflow_id, stepRow.seq, stepRow.name, stepRow.kind, stepRow.output, stepRow.error])
}


