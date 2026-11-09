import { afterEach, describe, expect, test } from "vitest";
import { createDurable } from "../src/durable.js";
import { DuplicateWorkflowError, DurableError, SerializationError } from "../src/errors.js";
import { databaseUrl, dropSchema, queryRows, uniqueSchema } from "./helpers.js";

// Acceptance tests for M2 (SPEC.md: FR-01, FR-02, FR-03 without recovery).

type Instance = ReturnType<typeof createDurable>;

const schemas: string[] = [];
const instances: Instance[] = [];

async function newDurable(): Promise<{ durable: Instance; schema: string }> {
  const schema = uniqueSchema();
  schemas.push(schema);
  const durable = createDurable({ connectionString: databaseUrl(), schema });
  instances.push(durable);
  await durable.migrate();
  return { durable, schema };
}

// shutdown() first: it waits for background runs, then the schema can go.
afterEach(async () => {
  for (const durable of instances.splice(0)) await durable.shutdown();
  for (const schema of schemas.splice(0)) await dropSchema(schema);
});

interface WorkflowRow {
  id: string;
  name: string;
  status: string;
  input: unknown;
  output: unknown;
  error: { name: string; message: string; stack?: string } | null;
}

interface StepRow {
  seq: number;
  name: string;
  kind: string;
  output: unknown;
}

function workflowRows(schema: string, id: string): Promise<WorkflowRow[]> {
  return queryRows<WorkflowRow>(`SELECT * FROM "${schema}".workflows WHERE id = $1`, [id]);
}

function stepRows(schema: string, id: string): Promise<StepRow[]> {
  return queryRows<StepRow>(
    `SELECT seq, name, kind, output FROM "${schema}".steps WHERE workflow_id = $1 ORDER BY seq`,
    [id],
  );
}

// Simulates a crash after the last step ran but before its checkpoint and the
// final status were written: the next run must replay every other step.
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

describe("start() and result()", () => {
  test("returns the workflow output and marks it completed", async () => {
    const { durable, schema } = await newDurable();
    const double = durable.workflow("double", async (_ctx, n: number) => n * 2);

    const handle = await double.start(21, { id: "double-1" });

    expect(handle.id).toBe("double-1");
    expect(await handle.result()).toBe(42);
    const [row] = await workflowRows(schema, "double-1");
    expect(row).toMatchObject({ name: "double", status: "completed", input: 21, output: 42 });
  });

  test("generates an id when none is given", async () => {
    const { durable } = await newDurable();
    const echo = durable.workflow("echo", async (_ctx, s: string) => s);

    const a = await echo.start("a");
    const b = await echo.start("b");

    expect(a.id).not.toBe(b.id);
    expect(await a.result()).toBe("a");
    expect(await b.result()).toBe("b");
  });
});

describe("ctx.step()", () => {
  test("checkpoints steps with seq 0, 1, 2 and their names", async () => {
    const { durable, schema } = await newDurable();
    const wf = durable.workflow("three", async (ctx, _input: null) => {
      const a = await ctx.step("a", async () => 1);
      const b = await ctx.step("b", async () => a + 1);
      return ctx.step("c", async () => ({ sum: a + b }));
    });

    const handle = await wf.start(null, { id: "three-1" });

    expect(await handle.result()).toEqual({ sum: 3 });
    expect(await stepRows(schema, "three-1")).toEqual([
      { seq: 0, name: "a", kind: "step", output: 1 },
      { seq: 1, name: "b", kind: "step", output: 2 },
      { seq: 2, name: "c", kind: "step", output: { sum: 3 } },
    ]);
  });

  test("replay runs only the step without a checkpoint", async () => {
    const { durable, schema } = await newDurable();
    const calls = { a: 0, b: 0, c: 0 };
    const wf = durable.workflow("replay", async (ctx, _input: null) => {
      await ctx.step("a", async () => ++calls.a);
      await ctx.step("b", async () => ++calls.b);
      await ctx.step("c", async () => ++calls.c);
      return "done";
    });

    const handle = await wf.start(null, { id: "replay-1" });
    await handle.result();
    await forgetLastStep(schema, "replay-1");

    await durable.start();

    expect(await handle.result()).toBe("done");
    expect(calls).toEqual({ a: 1, b: 1, c: 2 });
    expect(await stepRows(schema, "replay-1")).toHaveLength(3);
  });

  test("a step returns the same JSON value on first run and on replay", async () => {
    const { durable, schema } = await newDurable();
    const seen: unknown[] = [];
    const wf = durable.workflow("clock", async (ctx, _input: null) => {
      const now: unknown = await ctx.step("now", async () => new Date());
      seen.push(now);
      await ctx.step("last", async () => "x");
      return now;
    });

    const handle = await wf.start(null, { id: "clock-1" });
    const first = await handle.result();
    await forgetLastStep(schema, "clock-1");
    await durable.start();
    expect(await handle.result()).toBe(first);

    // new Date() goes through JSON, so both runs see the same ISO string, not a Date.
    expect(seen).toHaveLength(2);
    expect(typeof seen[0]).toBe("string");
    expect(seen[1]).toBe(seen[0]);
    expect(await handle.result()).toBe(first);
  });

  test("a step that returns undefined is stored and returned as null", async () => {
    const { durable, schema } = await newDurable();
    const wf = durable.workflow("void", async (ctx, _input: null) => {
      const value: unknown = await ctx.step("nothing", async () => undefined);
      return { value };
    });

    const handle = await wf.start(null, { id: "void-1" });

    expect(await handle.result()).toEqual({ value: null });
    const [step] = await stepRows(schema, "void-1");
    expect(step?.output).toBeNull();
  });
});

describe("idempotency and errors", () => {
  test("100 parallel starts with one id give one row and one run", async () => {
    const { durable, schema } = await newDurable();
    let runs = 0;
    const wf = durable.workflow("once", async (ctx, _input: null) => {
      return ctx.step("side-effect", async () => ++runs);
    });

    const handles = await Promise.all(
      Array.from({ length: 100 }, () => wf.start(null, { id: "once-1" })),
    );

    expect(new Set(handles.map((h) => h.id))).toEqual(new Set(["once-1"]));
    expect(await handles[0]!.result()).toBe(1);
    expect(runs).toBe(1);
    expect(await workflowRows(schema, "once-1")).toHaveLength(1);
  });

  test("an exception marks the workflow failed and result() throws", async () => {
    const { durable, schema } = await newDurable();
    const wf = durable.workflow("boom", async (_ctx, _input: null) => {
      throw new TypeError("boom");
    });

    const handle = await wf.start(null, { id: "boom-1" });

    const error = await handle.result().then(
      () => null,
      (e: unknown) => e,
    );
    expect(error).toBeInstanceOf(DurableError);
    expect((error as Error).message).toContain("boom");
    const [row] = await workflowRows(schema, "boom-1");
    expect(row?.status).toBe("failed");
    expect(row?.error).toMatchObject({ name: "TypeError", message: "boom" });
    expect(row?.error?.stack).toEqual(expect.any(String));
  });

  test("registering the same name twice throws DuplicateWorkflowError", async () => {
    const { durable } = await newDurable();
    durable.workflow("dup", async () => 1);

    expect(() => durable.workflow("dup", async () => 2)).toThrow(DuplicateWorkflowError);
    expect(() => durable.workflow("dup", async () => 3)).toThrow(DurableError);
  });
});

describe("workflow versions", () => {
  test("stores the version given at registration", async () => {
    const { durable, schema } = await newDurable();
    const wf = durable.workflow("versioned", async (_ctx, _input: null) => "ok", { version: "2" });

    const handle = await wf.start(null, { id: "versioned-1" });

    expect(await handle.result()).toBe("ok");
    const [row] = await workflowRows(schema, "versioned-1");
    expect(row).toMatchObject({ name: "versioned", version: "2" });
  });

  test("defaults the version to 1", async () => {
    const { durable, schema } = await newDurable();
    const wf = durable.workflow("unversioned", async (_ctx, _input: null) => "ok");

    const handle = await wf.start(null, { id: "unversioned-1" });

    expect(await handle.result()).toBe("ok");
    const [row] = await workflowRows(schema, "unversioned-1");
    expect(row).toMatchObject({ name: "unversioned", version: "1" });
  });
});

describe("SerializationError", () => {
  test("start() rejects a BigInt input and creates no row", async () => {
    const { durable, schema } = await newDurable();
    const wf = durable.workflow("big-input", async (_ctx, _input: bigint) => 1);

    await expect(wf.start(10n, { id: "big-input-1" })).rejects.toBeInstanceOf(SerializationError);

    expect(await workflowRows(schema, "big-input-1")).toHaveLength(0);
  });

  test("start() rejects a circular input and creates no row", async () => {
    const { durable, schema } = await newDurable();
    const wf = durable.workflow("loop-input", async (_ctx, _input: object) => 1);
    const loop: Record<string, unknown> = {};
    loop.self = loop;

    await expect(wf.start(loop, { id: "loop-input-1" })).rejects.toBeInstanceOf(SerializationError);

    expect(await workflowRows(schema, "loop-input-1")).toHaveLength(0);
  });

  test("a step result that cannot be serialized fails the workflow without a checkpoint", async () => {
    const { durable, schema } = await newDurable();
    const wf = durable.workflow("big-step", async (ctx, _input: null) => {
      return ctx.step("load", async () => 10n);
    });

    const handle = await wf.start(null, { id: "big-step-1" });

    await expect(handle.result()).rejects.toThrow("result of step load (seq 0)");
    const [row] = await workflowRows(schema, "big-step-1");
    expect(row?.status).toBe("failed");
    expect(row?.error?.name).toBe("SerializationError");
    expect(await stepRows(schema, "big-step-1")).toHaveLength(0);
  });

  test("an error thrown by the step itself is not wrapped", async () => {
    const { durable, schema } = await newDurable();
    const wf = durable.workflow("bad-step", async (ctx, _input: null) => {
      return ctx.step("boom", async () => {
        throw new TypeError("boom");
      });
    });

    const handle = await wf.start(null, { id: "bad-step-1" });

    await expect(handle.result()).rejects.toThrow("boom");
    const [row] = await workflowRows(schema, "bad-step-1");
    expect(row?.error).toMatchObject({ name: "TypeError", message: "boom" });
  });

  test("a workflow output that cannot be serialized fails the workflow", async () => {
    const { durable, schema } = await newDurable();
    const wf = durable.workflow("big-output", async (_ctx, _input: null) => 10n);

    const handle = await wf.start(null, { id: "big-output-1" });

    await expect(handle.result()).rejects.toThrow("output of workflow big-output-1");
    const [row] = await workflowRows(schema, "big-output-1");
    expect(row?.status).toBe("failed");
    expect(row?.error?.name).toBe("SerializationError");
  });

  test("a workflow output with no JSON form is stored as JSON null, not SQL NULL", async () => {
    const { durable, schema } = await newDurable();
    const wf = durable.workflow("fn-output", async (_ctx, _input: null) => () => 1);

    const handle = await wf.start(null, { id: "fn-output-1" });

    expect(await handle.result()).toBeNull();
    const [row] = await queryRows<{ sql_null: boolean }>(
      `SELECT output IS NULL AS sql_null FROM "${schema}".workflows WHERE id = $1`,
      ["fn-output-1"],
    );
    expect(row?.sql_null).toBe(false);
  });
});
