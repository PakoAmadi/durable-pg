import { afterEach, describe, expect, test } from "vitest";
import { Pool } from "pg";
import { setTimeout as sleep } from "node:timers/promises";
import { createDurable } from "../src/durable.js";
import { LeaseLostError } from "../src/errors.js";
import type { WorkflowContext } from "../src/types.js";
import {
  databaseUrl, deferred, dropSchema, ownership, queryRows,
  seedStep, seedWorkflow, uniqueSchema, waitFor,
} from "./helpers.js";

// Acceptance tests for M3 (SPEC.md: FR-04, FR-05, FR-06, FR-16).
// Several executors share one schema inside one process; each has its own
// registry, so the same workflow name can have a different body per executor.

type Instance = ReturnType<typeof createDurable>;
type Options = Parameters<typeof createDurable>[0];

// Small timings so that a lease expires in a fraction of a second.
const FAST = { leaseMs: 300, heartbeatMs: 100, pollMs: 50 };

const schemas: string[] = [];
const instances = new Set<Instance>();

async function newSchema(): Promise<string> {
  const schema = uniqueSchema();
  schemas.push(schema);
  const durable = createDurable({ connectionString: databaseUrl(), schema });
  await durable.migrate();
  await durable.shutdown();
  return schema;
}

function executor(schema: string, executorId: string, opts: Partial<Options> = {}): Instance {
  const durable = createDurable({ connectionString: databaseUrl(), schema, executorId, ...FAST, ...opts });
  instances.add(durable);
  return durable;
}

// For tests that shut an executor down themselves: afterEach must not do it again.
async function stop(durable: Instance, opts?: { timeoutMs?: number }): Promise<void> {
  instances.delete(durable);
  await durable.shutdown(opts);
}

afterEach(async () => {
  for (const durable of instances) await durable.shutdown({ timeoutMs: 1_000 });
  instances.clear();
  for (const schema of schemas.splice(0)) await dropSchema(schema);
});

async function statusIs(schema: string, id: string, status: string): Promise<boolean> {
  return (await ownership(schema, id))?.status === status;
}

describe("FR-05: recovery and background claiming", () => {
  test("start() runs a pending workflow that nobody has claimed", async () => {
    const schema = await newSchema();
    await seedWorkflow(schema, { id: "wf-1", name: "double", input: 21 });
    const b = executor(schema, "B");
    b.workflow("double", async (_ctx, n: number) => n * 2);

    await b.start();

    await waitFor(() => statusIs(schema, "wf-1", "completed"), "wf-1 to complete");
    expect(await ownership(schema, "wf-1")).toMatchObject({
      output: 42, executor_id: "B", recovery_attempts: 1,
    });
  });

  test("finishes a workflow left by a dead executor without repeating its checkpoints", async () => {
    const schema = await newSchema();
    await seedWorkflow(schema, {
      id: "wf-1", name: "two-steps", status: "running",
      executorId: "dead", leaseInMs: -1_000, recoveryAttempts: 1,
    });
    await seedStep(schema, "wf-1", 0, "a", "a from dead");
    const calls = { a: 0, b: 0 };
    const b = executor(schema, "B");
    b.workflow("two-steps", async (ctx, _input: null) => {
      const a = await ctx.step("a", async () => `a#${++calls.a}`);
      const bb = await ctx.step("b", async () => `b#${++calls.b}`);
      return [a, bb];
    });

    await b.start();

    await waitFor(() => statusIs(schema, "wf-1", "completed"), "wf-1 to complete");
    expect(calls).toEqual({ a: 0, b: 1 });
    expect(await ownership(schema, "wf-1")).toMatchObject({
      output: ["a from dead", "b#1"], executor_id: "B", recovery_attempts: 2,
    });
  });

  test("does not touch a workflow whose lease is still live", async () => {
    const schema = await newSchema();
    await seedWorkflow(schema, {
      id: "wf-1", name: "busy", status: "running", executorId: "A", leaseInMs: 60_000,
    });
    let calls = 0;
    const b = executor(schema, "B");
    b.workflow("busy", async () => ++calls);

    await b.start();
    await sleep(10 * FAST.pollMs);

    expect(calls).toBe(0);
    expect(await ownership(schema, "wf-1")).toMatchObject({
      status: "running", executor_id: "A", recovery_attempts: 0,
    });
  });

  test("leaves a workflow alone when its name is not registered", async () => {
    const schema = await newSchema();
    await seedWorkflow(schema, { id: "wf-1", name: "ghost" });
    const b = executor(schema, "B");
    b.workflow("other", async () => 1);

    await b.start();
    await sleep(10 * FAST.pollMs);

    expect(await ownership(schema, "wf-1")).toMatchObject({
      status: "pending", executor_id: null, recovery_attempts: 0,
    });
  });

  test("fails a poisoned workflow once maxRecoveryAttempts is exceeded, without running it", async () => {
    const schema = await newSchema();
    await seedWorkflow(schema, {
      id: "wf-1", name: "poison", status: "running",
      executorId: "dead", leaseInMs: -1_000, recoveryAttempts: 3,
    });
    let calls = 0;
    const b = executor(schema, "B", { maxRecoveryAttempts: 3 });
    b.workflow("poison", async () => ++calls);

    await b.start();

    await waitFor(() => statusIs(schema, "wf-1", "failed"), "wf-1 to fail");
    expect(calls).toBe(0);
    const row = await ownership(schema, "wf-1");
    expect(row?.recovery_attempts).toBe(4);
    expect(row?.error).not.toBeNull();
  });

  test("still runs the workflow on the last allowed attempt", async () => {
    const schema = await newSchema();
    await seedWorkflow(schema, {
      id: "wf-1", name: "poison", status: "running",
      executorId: "dead", leaseInMs: -1_000, recoveryAttempts: 2,
    });
    const b = executor(schema, "B", { maxRecoveryAttempts: 3 });
    b.workflow("poison", async () => "survived");

    await b.start();

    await waitFor(() => statusIs(schema, "wf-1", "completed"), "wf-1 to complete");
    expect(await ownership(schema, "wf-1")).toMatchObject({ output: "survived", recovery_attempts: 3 });
  });

  test("two executors finish 200 workflows and run every step exactly once", { timeout: 30_000 }, async () => {
    const schema = await newSchema();
    const maxConcurrent = 10;
    const runs = new Map<string, number>();
    const peak: Record<string, number> = {};

    const register = (durable: Instance, label: string) => {
      let inFlight = 0;
      peak[label] = 0;
      return durable.workflow("three-steps", async (ctx, _input: null) => {
        peak[label] = Math.max(peak[label]!, ++inFlight);
        try {
          for (let i = 0; i < 3; i++) {
            await ctx.step(`s${i}`, async () => {
              const key = `${ctx.workflowId}:${i}`;
              runs.set(key, (runs.get(key) ?? 0) + 1);
            });
          }
          return label;
        } finally {
          inFlight--;
        }
      });
    };
    const a = executor(schema, "A", { maxConcurrent });
    const b = executor(schema, "B", { maxConcurrent });
    const wf = register(a, "A");
    register(b, "B");

    await Promise.all(Array.from({ length: 200 }, (_, i) => wf.start(null, { id: `wf-${i}` })));
    await Promise.all([a.start(), b.start()]);

    await waitFor(async () => {
      const [row] = await queryRows<{ n: number }>(
        `SELECT count(*)::int AS n FROM "${schema}".workflows WHERE status = 'completed'`,
      );
      return row?.n === 200;
    }, "200 workflows to complete", 25_000);

    expect(runs.size).toBe(600);
    expect([...runs.values()].every((n) => n === 1)).toBe(true);
    const [steps] = await queryRows<{ n: number }>(`SELECT count(*)::int AS n FROM "${schema}".steps`);
    expect(steps?.n).toBe(600);
    const owners = await queryRows<{ executor_id: string }>(
      `SELECT DISTINCT executor_id FROM "${schema}".workflows ORDER BY executor_id`,
    );
    expect(owners.map((o) => o.executor_id)).toEqual(["A", "B"]);
    expect(peak.A).toBeLessThanOrEqual(maxConcurrent);
    expect(peak.B).toBeLessThanOrEqual(maxConcurrent);
  });
});

describe("FR-04: fencing", () => {
  test("an executor that lost its lease cannot write a step, and the new owner finishes", async () => {
    const schema = await newSchema();
    const entered = deferred();
    const release = deferred();
    // A is frozen: no heartbeats and a stuck checkpoint, like a long GC pause.
    const a = executor(schema, "A", {
      testHooks: {
        beforeHeartbeat: () => release.promise,
        beforeCheckpoint: () => release.promise,
      },
    });
    const b = executor(schema, "B");
    let lostOnA: unknown;
    let signalOnA: AbortSignal | undefined;
    const wfA = a.workflow("fenced", async (ctx, _input: null) => {
      signalOnA = ctx.signal;
      try {
        return await ctx.step("work", async () => {
          entered.resolve();
          return "A";
        });
      } catch (e) {
        lostOnA = e;
        throw e;
      }
    });
    b.workflow("fenced", async (ctx, _input: null) => ctx.step("work", async () => "B"));

    await wfA.start(null, { id: "wf-1" });
    await entered.promise;
    await b.start();
    await waitFor(() => statusIs(schema, "wf-1", "completed"), "B to finish wf-1");
    release.resolve();
    await waitFor(() => lostOnA !== undefined, "A to notice the lost lease");

    expect(lostOnA).toBeInstanceOf(LeaseLostError);
    expect(signalOnA?.aborted).toBe(true);
    expect(await ownership(schema, "wf-1")).toMatchObject({
      status: "completed", output: "B", executor_id: "B",
    });
    const steps = await queryRows(`SELECT seq, output FROM "${schema}".steps WHERE workflow_id = $1`, ["wf-1"]);
    expect(steps).toEqual([{ seq: 0, output: "B" }]);
  });
});

describe("FR-06: heartbeat", () => {
  test("a step that lasts 3 × leaseMs is not taken over", async () => {
    const schema = await newSchema();
    const entered = deferred();
    let calls = 0;
    const body = (label: string) => async (ctx: WorkflowContext, _input: null) =>
      ctx.step("slow", async () => {
        calls++;
        entered.resolve();
        await sleep(3 * FAST.leaseMs);
        return label;
      });
    const a = executor(schema, "A");
    const b = executor(schema, "B");
    const wf = a.workflow("slow", body("A"));
    b.workflow("slow", body("B"));

    await wf.start(null, { id: "wf-1" });
    await entered.promise;
    await b.start();

    await waitFor(() => statusIs(schema, "wf-1", "completed"), "wf-1 to complete");
    expect(calls).toBe(1);
    expect(await ownership(schema, "wf-1")).toMatchObject({
      output: "A", executor_id: "A", recovery_attempts: 1,
    });
  });
});

describe("FR-16: graceful shutdown", () => {
  test("waits for a run that finishes within timeoutMs", async () => {
    const schema = await newSchema();
    const entered = deferred();
    const a = executor(schema, "A");
    const wf = a.workflow("short", async (ctx, _input: null) =>
      ctx.step("work", async () => {
        entered.resolve();
        await sleep(200);
        return "done";
      }),
    );

    await wf.start(null, { id: "wf-1" });
    await entered.promise;
    await stop(a, { timeoutMs: 5_000 });

    expect(await ownership(schema, "wf-1")).toMatchObject({
      status: "completed", output: "done", executor_id: "A",
    });
  });

  test("after timeoutMs releases the lease, so another executor takes over at once", async () => {
    const schema = await newSchema();
    // A long lease: only the release in shutdown() can explain a quick takeover.
    const slow = { leaseMs: 60_000, heartbeatMs: 20_000 };
    const entered = deferred();
    const a = executor(schema, "A", slow);
    const b = executor(schema, "B", slow);
    const wfA = a.workflow("stuck", async (ctx, _input: null) =>
      ctx.step("work", async () => {
        entered.resolve();
        return new Promise<string>(() => {});
      }),
    );
    b.workflow("stuck", async (ctx, _input: null) => ctx.step("work", async () => "B"));

    await wfA.start(null, { id: "wf-1" });
    await entered.promise;
    await b.start();
    const startedAt = Date.now();
    await stop(a, { timeoutMs: 200 });

    expect(Date.now() - startedAt).toBeLessThan(2_000);
    await waitFor(() => statusIs(schema, "wf-1", "completed"), "B to take over wf-1", 3_000);
    expect(await ownership(schema, "wf-1")).toMatchObject({ output: "B", executor_id: "B" });
  });

  test("claims nothing after shutdown", async () => {
    const schema = await newSchema();
    // A pool the executor does not own stays open, so a forgotten poll loop
    // would still be able to claim.
    const pool = new Pool({ connectionString: databaseUrl() });
    try {
      const a = executor(schema, "A", { pool, connectionString: undefined });
      a.workflow("late", async () => 1);
      await a.start();
      await stop(a);

      await seedWorkflow(schema, { id: "wf-1", name: "late" });
      await sleep(10 * FAST.pollMs);

      expect(await ownership(schema, "wf-1")).toMatchObject({ status: "pending", executor_id: null });
    } finally {
      await pool.end();
    }
  });
});
