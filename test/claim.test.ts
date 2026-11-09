import { afterEach, beforeEach, describe, expect, test } from "vitest";
import { Pool } from "pg";
import { createDurable } from "../src/durable.js";
import { claimWorkflows } from "../src/store.js";
import { databaseUrl, dropSchema, ownership, seedWorkflow, uniqueSchema } from "./helpers.js";

// Unit tests for the claim query (SPEC.md FR-05), straight against store.ts.

let schema: string;
let pool: Pool;

beforeEach(async () => {
  schema = uniqueSchema();
  const durable = createDurable({ connectionString: databaseUrl(), schema });
  await durable.migrate();
  await durable.shutdown();
  pool = new Pool({ connectionString: databaseUrl() });
});

afterEach(async () => {
  await pool.end();
  await dropSchema(schema);
});

const HELLO = [{ name: "hello", version: "1" }];

function claim(executorId: string, limit = 10, workflows = HELLO, leaseMs = 30_000) {
  return claimWorkflows(pool, schema, { executorId, leaseMs, limit, workflows });
}

describe("claimWorkflows()", () => {
  test("claims a pending workflow: running, owned, leased, first attempt", async () => {
    await seedWorkflow(schema, { id: "wf-1", name: "hello" });

    expect(await claim("B")).toEqual([{ id: "wf-1", recoveryAttempts: 1 }]);

    const row = await ownership(schema, "wf-1");
    expect(row).toMatchObject({ status: "running", executor_id: "B", recovery_attempts: 1 });
    expect(row?.lease_left_ms).toBeGreaterThan(29_000);
    expect(row?.lease_left_ms).toBeLessThanOrEqual(30_000);
  });

  test("takes over a running workflow whose lease has expired", async () => {
    await seedWorkflow(schema, {
      id: "wf-1", name: "hello", status: "running",
      executorId: "dead", leaseInMs: -1_000, recoveryAttempts: 1,
    });

    expect(await claim("B")).toEqual([{ id: "wf-1", recoveryAttempts: 2 }]);
    expect(await ownership(schema, "wf-1")).toMatchObject({ executor_id: "B", recovery_attempts: 2 });
  });

  test("takes a running workflow that has no lease at all", async () => {
    await seedWorkflow(schema, { id: "wf-1", name: "hello", status: "running", leaseInMs: null });

    expect(await claim("B")).toEqual([{ id: "wf-1", recoveryAttempts: 1 }]);
  });

  test("leaves a workflow with a live lease alone", async () => {
    await seedWorkflow(schema, {
      id: "wf-1", name: "hello", status: "running", executorId: "A", leaseInMs: 60_000,
    });

    expect(await claim("B")).toEqual([]);
    expect(await ownership(schema, "wf-1")).toMatchObject({ executor_id: "A", recovery_attempts: 0 });
  });

  test("never takes completed, failed or cancelled workflows", async () => {
    for (const status of ["completed", "failed", "cancelled"]) {
      await seedWorkflow(schema, { id: `wf-${status}`, name: "hello", status, leaseInMs: null });
    }

    expect(await claim("B")).toEqual([]);
  });

  test("takes only the names and versions it was given", async () => {
    await seedWorkflow(schema, { id: "hello-v1", name: "hello", version: "1" });
    await seedWorkflow(schema, { id: "hello-v2", name: "hello", version: "2" });
    await seedWorkflow(schema, { id: "other-v1", name: "other", version: "1" });

    expect(await claim("B")).toEqual([{ id: "hello-v1", recoveryAttempts: 1 }]);
    expect(await claim("B", 10, [{ name: "hello", version: "2" }, { name: "other", version: "1" }]))
      .toHaveLength(2);
  });

  test("returns at most limit rows, and limit 0 changes nothing", async () => {
    for (let i = 0; i < 5; i++) await seedWorkflow(schema, { id: `wf-${i}`, name: "hello" });

    expect(await claim("B", 0)).toEqual([]);
    expect(await ownership(schema, "wf-0")).toMatchObject({ status: "pending", executor_id: null });

    expect(await claim("B", 3)).toHaveLength(3);
    expect(await claim("B", 3)).toHaveLength(2);
  });

  test("skips the ids it is told to exclude", async () => {
    for (const id of ["wf-1", "wf-2"]) {
      await seedWorkflow(schema, { id, name: "hello", status: "running", executorId: "B", leaseInMs: -1_000 });
    }

    const claimed = await claimWorkflows(pool, schema, {
      executorId: "B", leaseMs: 30_000, limit: 10, workflows: HELLO, exclude: ["wf-1"],
    });

    expect(claimed).toEqual([{ id: "wf-2", recoveryAttempts: 1 }]);
    expect(await ownership(schema, "wf-1")).toMatchObject({ recovery_attempts: 0 });
  });

  test("concurrent claims never hand out the same workflow twice", async () => {
    for (let i = 0; i < 100; i++) await seedWorkflow(schema, { id: `wf-${i}`, name: "hello" });

    const executors = ["E0", "E1", "E2", "E3", "E4", "E5", "E6", "E7"];
    const results = await Promise.all(executors.map((e) => claim(e, 20)));

    const ids = results.flat().map((r) => r.id);
    expect(new Set(ids).size).toBe(ids.length);
    for (const [i, claimed] of results.entries()) {
      for (const { id } of claimed) {
        expect((await ownership(schema, id))?.executor_id).toBe(executors[i]);
      }
    }
  });
});
