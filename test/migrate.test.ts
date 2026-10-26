import { Pool } from "pg";
import { afterEach, describe, expect, test } from "vitest";
import {createDurable, DurableError} from "../src/index.js";
import { databaseUrl, dropSchema, queryRows, uniqueSchema } from "./helpers.js";
import {migrations} from "../src/migrations.js";

// Acceptance tests for M1 (SPEC.md, section 11).

const created: string[] = [];

function newSchema(): string {
  const schema = uniqueSchema();
  created.push(schema);
  return schema;
}

async function appliedVersions(schema: string): Promise<number[]> {
  const rows = await queryRows<{ version: number }>(
    `SELECT version FROM "${schema}".migrations ORDER BY version`,
  );
  return rows.map((row) => row.version);
}

afterEach(async () => {
  for (const schema of created.splice(0)) await dropSchema(schema);
});

describe("migrate()", () => {
  test("creates the tables from the spec", async () => {
    const schema = newSchema();
    const durable = createDurable({ connectionString: databaseUrl(), schema });

    await durable.migrate();
    await durable.shutdown();

    const tables = await queryRows<{ table_name: string }>(
      `SELECT table_name FROM information_schema.tables
       WHERE table_schema = $1 ORDER BY table_name`,
      [schema],
    );
    expect(tables.map((t) => t.table_name)).toEqual([
      "messages",
      "migrations",
      "steps",
      "workflows",
    ]);
  });

  test("records every version exactly once, in order", async () => {
    const schema = newSchema();
    const durable = createDurable({ connectionString: databaseUrl(), schema });

    await durable.migrate();
    await durable.shutdown();

    const versions = await appliedVersions(schema);
    expect(versions.length).toBeGreaterThan(0);
    expect(versions).toEqual(versions.map((_, i) => i + 1));
  });

  test("running twice changes nothing", async () => {
    const schema = newSchema();
    const durable = createDurable({ connectionString: databaseUrl(), schema });

    await durable.migrate();
    const first = await appliedVersions(schema);
    await durable.migrate();
    const second = await appliedVersions(schema);
    await durable.shutdown();

    expect(second).toEqual(first);
  });

  test("two instances migrating in parallel do not collide", async () => {
    const schema = newSchema();
    const a = createDurable({ connectionString: databaseUrl(), schema });
    const b = createDurable({ connectionString: databaseUrl(), schema });

    await Promise.all([a.migrate(), b.migrate()]);
    await Promise.all([a.shutdown(), b.shutdown()]);

    const versions = await appliedVersions(schema);
    expect(versions).toEqual(versions.map((_, i) => i + 1));
  });


  test("a broken migration rolls back and reports its version", async () => {
    const schema = newSchema();
    const durable = createDurable({ connectionString: databaseUrl(), schema });

    migrations.push({ version: 99, sql: (s) => `CREATE TABLE ${s}.oops (id int); SELEC 1;` });
    try {
      const error = await durable.migrate().catch((e) => e);

      expect(error).toBeInstanceOf(DurableError);
      expect(error.message).toBe("migration 99 failed");
      expect(error.cause.message).toMatch(/syntax error/);

      const [oops] = await queryRows<{ t: string | null }>(
          "SELECT to_regclass($1) AS t", [`${schema}.oops`],
      );
      expect(oops.t).toBeNull();

      expect(await appliedVersions(schema)).not.toContain(99);

      await expect(durable.migrate()).rejects.toThrow("migration 99 failed");
    } finally {
      migrations.pop();
      await durable.shutdown();
    }
  });

});

describe("shutdown()", () => {
  test("does not close a pool passed in from outside", async () => {
    const schema = newSchema();
    const pool = new Pool({ connectionString: databaseUrl() });
    const durable = createDurable({ pool, schema });

    await durable.migrate();
    await durable.shutdown();

    const result = await pool.query("SELECT 1 AS ok");
    expect(result.rows[0].ok).toBe(1);
    await pool.end();
  });
});


