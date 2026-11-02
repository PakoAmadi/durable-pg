import { Pool } from "pg";
import { createDurable } from "../src/durable.js";

const [mode, id = "crash-1"] = process.argv.slice(2);
const schema = "crash_demo";
const connectionString = process.env.DATABASE_URL!;
const db = new Pool({ connectionString });

const durable = createDurable({ connectionString, schema });
await durable.migrate();
await db.query(`CREATE TABLE IF NOT EXISTS ${schema}.side_effects (
    id bigserial PRIMARY KEY, run_id text NOT NULL, step text NOT NULL)`);

const effect = (step: string) => {
    console.log(`execute ${step}`);
    return db.query(`INSERT INTO ${schema}.side_effects(run_id, step) VALUES ($1, $2)`, [id, step]);
};

const wf = durable.workflow("crash", async (ctx, _input: null) => {
    await ctx.step("a", async () => { await effect("a"); });
    await ctx.step("b", async () => { await effect("b"); });
    await ctx.step("c", async () => {
        await effect("c");
        if (mode === "start") process.kill(process.pid, "SIGKILL");
    });
    return "done";
});

if (mode === "start") {
    const handle = await wf.start(null, { id });
    console.log(await handle.result());
} else {
    await durable.resume(id);
}

const { rows } = await db.query(
    `SELECT step, count(*)::int AS n FROM ${schema}.side_effects WHERE run_id = $1 GROUP BY step ORDER BY step`, [id]);
console.table(rows);
await db.end();
await durable.shutdown();