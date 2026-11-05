import { afterEach, beforeEach, describe, expect, test } from "vitest";
import { fork } from "node:child_process";
import { fileURLToPath } from "node:url";
import { createDurable } from "../src/durable.js";
import { databaseUrl, dropSchema, ownership, queryRows, uniqueSchema } from "./helpers.js";

// Crash tests for M3 (SPEC.md section 12): a real process is killed with
// SIGKILL in the middle of a workflow, and a second process recovers it.

const worker = fileURLToPath(new URL("./fixtures/crash-worker.ts", import.meta.url));

let schema: string;

beforeEach(async () => {
  schema = uniqueSchema();
  const durable = createDurable({ connectionString: databaseUrl(), schema });
  await durable.migrate();
  await durable.shutdown();
  await queryRows(`CREATE TABLE "${schema}".side_effects (
    id bigserial PRIMARY KEY, run_id text NOT NULL, seq int NOT NULL)`);
});

afterEach(async () => {
  await dropSchema(schema);
});

function runWorker(env: Record<string, string>): Promise<{ code: number | null; signal: string | null }> {
  const child = fork(worker, [], {
    execArgv: ["--import", "tsx"],
    env: { ...process.env, DATABASE_URL: databaseUrl(), SCHEMA: schema, WF_ID: "crash-1", ...env },
  });
  return new Promise((resolve, reject) => {
    child.on("error", reject);
    child.on("exit", (code, signal) => resolve({ code, signal }));
  });
}

async function sideEffects(): Promise<number[]> {
  const rows = await queryRows<{ seq: number; n: number }>(
    `SELECT seq, count(*)::int AS n FROM "${schema}".side_effects GROUP BY seq ORDER BY seq`,
  );
  return rows.map((r) => r.n);
}

describe("crash and recovery in another process", { timeout: 30_000 }, () => {
  test("SIGKILL before the checkpoint of step 3: only step 3 runs twice", async () => {
    const first = await runWorker({ EXECUTOR_ID: "first", CRASH_AT_STEP: "3", CRASH_WHEN: "before" });
    expect(first.signal).toBe("SIGKILL");

    const second = await runWorker({ EXECUTOR_ID: "second" });
    expect(second.code).toBe(0);

    expect(await sideEffects()).toEqual([1, 1, 1, 2, 1]);
    expect(await ownership(schema, "crash-1")).toMatchObject({
      status: "completed", output: "done", executor_id: "second", recovery_attempts: 2,
    });
  });

  test("SIGKILL after the checkpoint of step 3: no step runs twice", async () => {
    const first = await runWorker({ EXECUTOR_ID: "first", CRASH_AT_STEP: "3", CRASH_WHEN: "after" });
    expect(first.signal).toBe("SIGKILL");

    const second = await runWorker({ EXECUTOR_ID: "second" });
    expect(second.code).toBe(0);

    expect(await sideEffects()).toEqual([1, 1, 1, 1, 1]);
    expect(await ownership(schema, "crash-1")).toMatchObject({
      status: "completed", executor_id: "second", recovery_attempts: 2,
    });
  });
});
