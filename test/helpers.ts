import { randomBytes } from "node:crypto";
import { Client, type QueryResultRow } from "pg";

export function databaseUrl(): string {
  const url = process.env.DATABASE_URL; //DATABASE_URL=postgresql://postgres:postgres@localhost:5432/durable
  if (!url) throw new Error("DATABASE_URL is not set: copy .env.example to .env");
  return url;
}

// Every test gets its own schema, so tests never see each other's tables
// and never touch the playground data.
export function uniqueSchema(): string {
  return `test_${randomBytes(4).toString("hex")}`;
}

export async function queryRows<R extends QueryResultRow>(
  sql: string,
  params: unknown[] = [],
): Promise<R[]> {
  const client = new Client({ connectionString: databaseUrl() });
  await client.connect();
  try {
    const result = await client.query<R>(sql, params);
    return result.rows;
  } finally {
    await client.end();
  }
}

export async function dropSchema(schema: string): Promise<void> {
  await queryRows(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
}

export interface SeedWorkflow {
  id: string;
  name: string;
  version?: string;
  status?: string;
  input?: unknown;
  executorId?: string | null;
  // Lease relative to now(): negative means already expired, null means no lease.
  leaseInMs?: number | null;
  recoveryAttempts?: number;
}

// Writes a workflow row directly, bypassing the library: this is how tests
// fake "another process started it" or "an executor died holding it".
export async function seedWorkflow(schema: string, w: SeedWorkflow): Promise<void> {
  await queryRows(
    `INSERT INTO "${schema}".workflows
       (id, name, version, status, input, executor_id, lease_until, recovery_attempts)
     VALUES ($1, $2, $3, $4, $5::jsonb, $6,
             now() + $7::float8 * interval '1 millisecond', $8)`,
    [
      w.id,
      w.name,
      w.version ?? "1",
      w.status ?? "pending",
      JSON.stringify(w.input ?? null),
      w.executorId ?? null,
      w.leaseInMs ?? null,
      w.recoveryAttempts ?? 0,
    ],
  );
}

export async function seedStep(
  schema: string,
  workflowId: string,
  seq: number,
  name: string,
  output: unknown,
): Promise<void> {
  await queryRows(
    `INSERT INTO "${schema}".steps (workflow_id, seq, name, kind, output)
     VALUES ($1, $2, $3, 'step', $4::jsonb)`,
    [workflowId, seq, name, JSON.stringify(output)],
  );
}

export interface OwnershipRow {
  status: string;
  output: unknown;
  error: { name: string; message: string } | null;
  executor_id: string | null;
  recovery_attempts: number;
  lease_left_ms: number | null;
}

export async function ownership(schema: string, id: string): Promise<OwnershipRow | undefined> {
  const [row] = await queryRows<OwnershipRow>(
    `SELECT status, output, error, executor_id, recovery_attempts,
            (extract(epoch FROM lease_until - now()) * 1000)::float8 AS lease_left_ms
     FROM "${schema}".workflows WHERE id = $1`,
    [id],
  );
  return row;
}

export async function waitFor(
  check: () => Promise<boolean> | boolean,
  what: string,
  timeoutMs = 5_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!(await check())) {
    if (Date.now() > deadline) throw new Error(`timed out after ${timeoutMs} ms waiting for ${what}`);
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
}

export function deferred<T = void>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((r) => (resolve = r));
  return { promise, resolve };
}
