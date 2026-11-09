import { afterEach, describe, expect, test } from "vitest";
import { createDurable } from "../src/durable.js";
import { LeaseLostError, StepFailedError, WorkflowFailedError } from "../src/errors.js";
import type { WorkflowContext } from "../src/types.js";
import { databaseUrl, dropSchema, ownership, queryRows, uniqueSchema, waitFor } from "./helpers.js";

// Acceptance tests for M4 (SPEC.md: FR-07, section 10 "Ошибки").
//
// Contract these tests pin down:
// - ctx.step(name, fn, { retries, backoffMs, maxBackoffMs }): retries = extra
//   attempts, so retries: 3 means at most 4 calls of fn. Default retries: 0.
// - fn receives StepInfo { workflowId, seq, attempt, idempotencyKey }, where
//   attempt counts from 0 and idempotencyKey = `${workflowId}:${seq}`.
// - Delay before attempt n+1: min(maxBackoffMs, backoffMs * 2^n), multiplied by
//   a random factor in [0.5, 1] (jitter).
// - When retries run out, the step's checkpoint stores the error and the number
//   of attempts, and ctx.step throws StepFailedError. Its cause is the error in
//   its JSON form ({ name, message, stack }), on the first run and on replay alike.
// - On replay a recorded error is thrown again without calling fn.
// - LeaseLostError is never retried and never recorded.
// - handle.result() of a failed workflow throws WorkflowFailedError.

type Instance = ReturnType<typeof createDurable>;
type Options = Parameters<typeof createDurable>[0];

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

async function stop(durable: Instance): Promise<void> {
  instances.delete(durable);
  await durable.shutdown({ timeoutMs: 1_000 });
}

afterEach(async () => {
  for (const durable of instances) await durable.shutdown({ timeoutMs: 1_000 });
  instances.clear();
  for (const schema of schemas.splice(0)) await dropSchema(schema);
});

interface StepRow {
  seq: number;
  name: string;
  output: unknown;
  error: { name: string; message: string; stack?: string } | null;
  attempts: number;
}

function stepRows(schema: string, id: string): Promise<StepRow[]> {
  return queryRows<StepRow>(
    `SELECT seq, name, output, error, attempts FROM "${schema}".steps WHERE workflow_id = $1 ORDER BY seq`,
    [id],
  );
}

// A crash after the last step ran but before its checkpoint: the last
// checkpoint is gone and nobody holds the lease, so start() recovers it.
async function forgetLastStep(schema: string, id: string): Promise<void> {
  await queryRows(
    `DELETE FROM "${schema}".steps
     WHERE workflow_id = $1
       AND seq = (SELECT max(seq) FROM "${schema}".steps WHERE workflow_id = $1)`,
    [id],
  );
  await queryRows(
    `UPDATE "${schema}".workflows SET status = 'running', output = NULL, lease_until = NULL WHERE id = $1`,
    [id],
  );
}

async function caught(promise: Promise<unknown>): Promise<unknown> {
  return promise.then(
    () => { throw new Error("expected a rejection"); },
    (e: unknown) => e,
  );
}

describe("FR-07: step retries", () => {
  test("a step that fails 2 times out of 3 succeeds with retries: 3, attempts = 3", async () => {
    const schema = await newSchema();
    const a = executor(schema, "A");
    let calls = 0;
    const wf = a.workflow("flaky", async (ctx, _input: null) =>
      ctx.step("call", async () => {
        if (++calls < 3) throw new Error(`fail ${calls}`);
        return "ok";
      }, { retries: 3, backoffMs: 10 }),
    );

    const handle = await wf.start(null, { id: "wf-1" });

    expect(await handle.result()).toBe("ok");
    expect(calls).toBe(3);
    expect(await stepRows(schema, "wf-1")).toEqual([
      { seq: 0, name: "call", output: "ok", error: null, attempts: 3 },
    ]);
  });

  test("fn gets attempt from 0 up and the same idempotencyKey every time", async () => {
    const schema = await newSchema();
    const a = executor(schema, "A");
    const infos: unknown[] = [];
    const wf = a.workflow("info", async (ctx, _input: null) => {
      await ctx.step("first", async () => 1);
      return ctx.step("second", async (info) => {
        infos.push({ ...info });
        if (info.attempt < 2) throw new Error("again");
        return "ok";
      }, { retries: 5, backoffMs: 10 });
    });

    const handle = await wf.start(null, { id: "wf-1" });

    expect(await handle.result()).toBe("ok");
    expect(infos).toEqual([0, 1, 2].map((attempt) => ({
      workflowId: "wf-1", seq: 1, attempt, idempotencyKey: "wf-1:1",
    })));
  });

  test("without retries a failing step runs once", async () => {
    const schema = await newSchema();
    const a = executor(schema, "A");
    let calls = 0;
    const wf = a.workflow("once", async (ctx, _input: null) =>
      ctx.step("call", async () => {
        calls++;
        throw new Error("nope");
      }),
    );

    const handle = await wf.start(null, { id: "wf-1" });

    await caught(handle.result());
    expect(calls).toBe(1);
    expect(await stepRows(schema, "wf-1")).toMatchObject([{ seq: 0, attempts: 1 }]);
  });

  test("waits between attempts: backoffMs 200 gives at least 100 ms before attempt 1", async () => {
    const schema = await newSchema();
    const a = executor(schema, "A");
    const at: number[] = [];
    const wf = a.workflow("slow-retry", async (ctx, _input: null) =>
      ctx.step("call", async () => {
        at.push(Date.now());
        if (at.length < 2) throw new Error("again");
        return "ok";
      }, { retries: 1, backoffMs: 200 }),
    );

    const handle = await wf.start(null, { id: "wf-1" });

    expect(await handle.result()).toBe("ok");
    // 200 · 2^0 with jitter in [0.5, 1] is 100..200 ms.
    expect(at[1]! - at[0]!).toBeGreaterThanOrEqual(95);
  });

  test("maxBackoffMs caps the delay", async () => {
    const schema = await newSchema();
    const a = executor(schema, "A");
    let calls = 0;
    const wf = a.workflow("capped", async (ctx, _input: null) =>
      ctx.step("call", async () => {
        if (++calls < 4) throw new Error("again");
        return "ok";
      }, { retries: 3, backoffMs: 60_000, maxBackoffMs: 50 }),
    );

    const startedAt = Date.now();
    const handle = await wf.start(null, { id: "wf-1" });

    expect(await handle.result()).toBe("ok");
    expect(Date.now() - startedAt).toBeLessThan(2_000);
  });
});

describe("a step that runs out of retries", () => {
  test("is recorded with its error and attempts, and ctx.step throws StepFailedError", async () => {
    const schema = await newSchema();
    const a = executor(schema, "A");
    let error: unknown;
    const wf = a.workflow("exhausted", async (ctx, _input: null) => {
      try {
        await ctx.step("call", async () => {
          throw new TypeError("api down");
        }, { retries: 2, backoffMs: 10 });
      } catch (e) {
        error = e;
      }
      return "handled";
    });

    const handle = await wf.start(null, { id: "wf-1" });

    expect(await handle.result()).toBe("handled");
    expect(error).toBeInstanceOf(StepFailedError);
    expect((error as Error).message).toContain("api down");
    expect((error as Error).cause).toMatchObject({ name: "TypeError", message: "api down" });
    // The cause is the stored JSON form, not the original TypeError: that is
    // what replay can give back, so the first run must give the same.
    expect((error as Error).cause).not.toBeInstanceOf(TypeError);

    const [row] = await stepRows(schema, "wf-1");
    expect(row).toMatchObject({
      seq: 0, name: "call", output: null, attempts: 3,
      error: { name: "TypeError", message: "api down" },
    });
    expect(row?.error?.stack).toEqual(expect.any(String));
  });

  test("a thrown non-Error is stored as { name: 'Error', message }", async () => {
    const schema = await newSchema();
    const a = executor(schema, "A");
    const wf = a.workflow("string-throw", async (ctx, _input: null) => {
      await ctx.step("call", async () => {
        throw "plain string";
      });
    });

    const handle = await wf.start(null, { id: "wf-1" });

    await caught(handle.result());
    expect(await stepRows(schema, "wf-1")).toMatchObject([
      { error: { name: "Error", message: "plain string" } },
    ]);
  });

  test("uncaught, it fails the workflow and result() throws WorkflowFailedError", async () => {
    const schema = await newSchema();
    const a = executor(schema, "A");
    const wf = a.workflow("uncaught", async (ctx, _input: null) =>
      ctx.step("call", async () => {
        throw new TypeError("api down");
      }),
    );

    const handle = await wf.start(null, { id: "wf-1" });

    const error = await caught(handle.result());
    expect(error).toBeInstanceOf(WorkflowFailedError);
    expect((error as Error).message).toContain("api down");
    expect((error as Error).cause).toMatchObject({ name: "StepFailedError" });
    expect(await ownership(schema, "wf-1")).toMatchObject({
      status: "failed",
      error: { name: "StepFailedError" },
    });
  });
});

describe("replay of a recorded error", () => {
  test("an error recorded before a crash is thrown the same way after recovery", async () => {
    const schema = await newSchema();
    const seen: { stepFailed: boolean; message: string; cause: unknown }[] = [];
    let fetchCalls = 0;
    // Run 1: the API is down, the workflow falls back to "free".
    // Run 2: the API is back. Replay must still take the "free" branch.
    const body = (apiDown: boolean) => async (ctx: WorkflowContext, _input: null) => {
      let plan: string;
      try {
        plan = await ctx.step("fetchPlan", async () => {
          fetchCalls++;
          if (apiDown) throw new TypeError("api down");
          return "pro";
        });
      } catch (e) {
        const err = e as Error;
        seen.push({ stepFailed: e instanceof StepFailedError, message: err.message, cause: err.cause });
        plan = "free";
      }
      return ctx.step("bill", async () => plan);
    };

    const a = executor(schema, "A");
    const wfA = a.workflow("plan", body(true));
    const handle = await wfA.start(null, { id: "wf-1" });
    expect(await handle.result()).toBe("free");
    await stop(a);

    await forgetLastStep(schema, "wf-1");

    const b = executor(schema, "B");
    const wfB = b.workflow("plan", body(false));
    await b.start();

    // A's pool is closed: ask through B. For an existing id start() only returns a handle.
    expect(await (await wfB.start(null, { id: "wf-1" })).result()).toBe("free");
    expect(fetchCalls).toBe(1);
    expect(seen).toHaveLength(2);
    expect(seen[0]?.stepFailed).toBe(true);
    expect(seen[1]).toEqual(seen[0]);
  });
});

describe("lease loss during retries", () => {
  test("is not retried and not recorded: the backoff sleep is cut short", async () => {
    const schema = await newSchema();
    const a = executor(schema, "A");
    let calls = 0;
    let thrown: unknown;
    const wf = a.workflow("stolen", async (ctx, _input: null) => {
      try {
        return await ctx.step("call", async () => {
          calls++;
          throw new Error("try later");
        }, { retries: 5, backoffMs: 5_000 });
      } catch (e) {
        thrown = e;
        throw e;
      }
    });

    await wf.start(null, { id: "wf-1" });
    await waitFor(() => calls === 1, "the first attempt");
    // Another executor takes the workflow over while A sleeps 2.5..5 s before attempt 1.
    await queryRows(`UPDATE "${schema}".workflows SET executor_id = 'thief' WHERE id = 'wf-1'`);

    // A's heartbeat notices within heartbeatMs and aborts the sleep.
    await waitFor(() => thrown !== undefined, "ctx.step to give up", 2_000);
    expect(thrown).toBeInstanceOf(LeaseLostError);
    expect(calls).toBe(1);
    expect(await stepRows(schema, "wf-1")).toEqual([]);
    expect(await ownership(schema, "wf-1")).toMatchObject({ status: "running", executor_id: "thief" });
  });
});
