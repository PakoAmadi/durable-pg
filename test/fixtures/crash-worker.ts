import { Pool } from "pg";
import { createDurable } from "../../src/durable.js";

// Child process for test/crash.test.ts. Runs a five-step workflow and, if
// CRASH_AT_STEP is set, kills itself with SIGKILL right before or right after
// the checkpoint of that step (CRASH_WHEN = "before" | "after").

const env = process.env;
const schema = env.SCHEMA!;
const id = env.WF_ID!;
const crashAt = env.CRASH_AT_STEP === undefined ? -1 : Number(env.CRASH_AT_STEP);
const crashWhen = env.CRASH_WHEN ?? "before";

const crash = (when: string, seq: number) => {
    if (when === crashWhen && seq === crashAt) process.kill(process.pid, "SIGKILL");
};

const db = new Pool({ connectionString: env.DATABASE_URL });
const durable = createDurable({
    connectionString: env.DATABASE_URL,
    schema,
    executorId: env.EXECUTOR_ID,
    leaseMs: 500,
    heartbeatMs: 150,
    pollMs: 50,
    testHooks: {
        beforeCheckpoint: (_wfId, seq) => crash("before", seq),
        afterCheckpoint: (_wfId, seq) => crash("after", seq),
    },
});

const wf = durable.workflow("crash", async (ctx, _input: null) => {
    for (let seq = 0; seq < 5; seq++) {
        await ctx.step(`s${seq}`, async () => {
            await db.query(`INSERT INTO ${schema}.side_effects (run_id, seq) VALUES ($1, $2)`, [id, seq]);
            return seq;
        });
    }
    return "done";
});

await durable.start();
// For an existing id start() inserts nothing and only returns a handle.
const handle = await wf.start(null, { id });
await handle.result();
await durable.shutdown();
await db.end();
