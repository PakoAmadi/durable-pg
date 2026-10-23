import {Pool} from "pg";


const db = new Pool({
    host: 'localhost',
    port: 5432,
    user: 'postgres',
    database: 'durable',
    password: 'postgres',
    max: 10,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 2000
})

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const runId = process.argv[2] ?? "run-1";

await db.query(`
    CREATE TABLE IF NOT EXISTS steps (
        run_id     text        NOT NULL,
        step       text        NOT NULL,
        output     jsonb,
        created_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (run_id, step)
    );
`);

await db.query(`
  CREATE TABLE IF NOT EXISTS side_effects (
    id         bigserial PRIMARY KEY,
    run_id     text NOT NULL,
    step       text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
  )`);

async function step<T>(name: string, fn: () => Promise<T> ): Promise<T> {
    const result = await db.query('SELECT * FROM steps WHERE steps.run_id = $1 AND steps.step = $2', [runId, name]);
    if (result.rowCount && result.rowCount > 0) return result.rows[0].output as T;
    const output = await fn();
    await db.query("INSERT INTO steps(run_id, step, output) VALUES ($1, $2, $3)", [runId, name, JSON.stringify(output)])
    return  output;
}

async function sideEffect(name: string) {
    console.log(`[${runId}] execute ${name}`);
    await db.query("INSERT INTO side_effects(run_id, step) VALUES ($1, $2)", [runId, name]);
    await sleep(2000);
}

async function processOrder() {
    const charge = await step("charge-card", async () => {
        await sideEffect("charge-card");
        return { chargeId: "ch_" + Date.now() };
    })

    await step("send-email", async () => {
        await sideEffect("send-email");
        console.log(`[${runId}] письмо про ${charge.chargeId}`);
    })

    await step("ship-order", async () => {
        await sideEffect("ship-order");
    })

    console.log(`[${runId}] готово`);
}

await processOrder();
await db.end();