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
