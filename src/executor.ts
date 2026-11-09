import type { Pool } from "pg";
import type {SerializedError, StepInfo, StepOptions, StepRow, TestHooks, WorkflowContext, WorkflowFn} from "./types.js";
import {LeaseLostError, StepFailedError} from "./errors.js";
import {
    claimWorkflows, completeWorkflow, extendLeases, failWorkflow, getWorkflow, loadSteps, releaseLeases, saveStep,
} from "./store.js";
import { toJson } from "./json.js";
import { every } from "./loop.js";
import { setTimeout as sleep } from "node:timers/promises";
// Everything about running workflows in this process: what is running,
// how many more fit, claiming, heartbeat and the replay of one workflow.

export interface Registered { fn: WorkflowFn<any, any>; version: string }

export interface ExecutorConfig {
    pool: Pool;
    schema: string;
    executorId: string;
    leaseMs: number;
    heartbeatMs: number;
    pollMs: number;
    maxConcurrent: number;
    maxRecoveryAttempts: number;
    hooks: TestHooks;
    registry: Map<string, Registered>;
}

interface Run { promise: Promise<void>; controller: AbortController }

const serializeError = (e: unknown): SerializedError => {
    if (e instanceof Error) {
        return { name: e.name, message: e.message, stack: e.stack }
    }
    return { name: 'Error', message: String(e)}
}

const stepFailed = (name: string, seq: number, error: SerializedError) =>
    new StepFailedError(`step ${name} (seq ${seq}) failed: ${error.message}`, { cause: error });

// backoffMs · 2^attempt, capped at maxBackoffMs, times a random factor in [0.5, 1].
const backoff = (attempt: number, backoffMs: number, maxBackoffMs: number) =>
    Math.min(maxBackoffMs, backoffMs * 2 ** attempt) * (0.5 + Math.random() / 2);

export const createExecutor = (cfg: ExecutorConfig) => {
    const { pool, schema, executorId, leaseMs, maxConcurrent, maxRecoveryAttempts, hooks, registry } = cfg;

    const running = new Map<string, Run>()
    let reserved = 0;

    const stopPolling = new AbortController();
    let pollLoop: Promise<void> | undefined;
    const stopHeartbeat = new AbortController();
    let heartbeatLoop: Promise<void> | undefined;

    const heartbeat = async (): Promise<void> => {
        if (running.size === 0) return;
        await hooks.beforeHeartbeat?.();
        const ids = [...running.keys()];
        const alive = new Set(await extendLeases(pool, schema, executorId, leaseMs, ids));
        for (const id of ids) {
            if (!alive.has(id)) {
                running.get(id)?.controller.abort(new LeaseLostError(`lease lost on workflow ${id}`));
            }
        }
    };

    const launch = (id: string): void => {
        if (running.has(id)) return;
        const controller = new AbortController();
        const promise = runWorkflow(id, controller)
            .catch((err) => console.error("durable-pg: runWorkflow failed", id, err))
            .finally(() => running.delete(id));
        running.set(id, { promise, controller });
        heartbeatLoop ??= every(cfg.heartbeatMs, stopHeartbeat.signal, "heartbeat", heartbeat);
    };

    const claimAndLaunch = async (limit: number, ids?: string[]): Promise<void> => {
        const n = Math.min(limit, maxConcurrent - running.size - reserved);
        if (n <= 0) return;

        reserved += n;
        try {
            const claimed = await claimWorkflows(pool, schema, {
                executorId,
                leaseMs,
                limit: n,
                workflows: [...registry].map(([name, w]) => ({ name, version: w.version })),
                ids,
            });
            for (const { id, recoveryAttempts } of claimed) {
                if (recoveryAttempts > maxRecoveryAttempts) {
                    await failWorkflow(pool, schema, id, executorId, {
                        name: "DurableError",
                        message: `exceeded maxRecoveryAttempts (${maxRecoveryAttempts})`,
                    });
                } else {
                    launch(id);
                }
            }
        } finally {
            reserved -= n;
        }
    };

    const tick = () => claimAndLaunch(Infinity);



    const runWorkflow = async (id: string, controller: AbortController): Promise<void> => {
        const row = await getWorkflow(pool, schema, id);
        if (!row || row.status === 'completed' || row.status === 'failed') return;
        const fn = registry.get(row.name)?.fn;
        if (!fn) {
            console.warn(`Function doesnt exists! Id: ${id}, WorkflowName: ${row.name}`);
            return;
        }

        const checkpoint = async (step: StepRow): Promise<void> => {
            if (controller.signal.aborted) throw controller.signal.reason;
            await hooks.beforeCheckpoint?.(id, step.seq);
            await saveStep(pool, schema, executorId, step);
            await hooks.afterCheckpoint?.(id, step.seq);
        };

        const steps = await loadSteps(pool, schema, id);
        const history = new Map(steps.map((s) => [s.seq, s] as const));
        let nextSeq = 0;

        const ctx: WorkflowContext = {
            workflowId: id,
            signal: controller.signal,
            step: async <T>(name: string, stepFn: (info: StepInfo) => Promise<T>, opts: StepOptions = {}): Promise<T> => {
                if (controller.signal.aborted) throw controller.signal.reason;
                const seq = nextSeq++;

                const saved = history.get(seq);
                if (saved) {
                    if (saved.error) throw stepFailed(name, seq, saved.error);
                    return saved.output as T;
                }

                const { retries = 0, backoffMs = 1_000, maxBackoffMs = 60_000 } = opts;
                for (let attempt = 0; ; attempt++) {
                    let value: T;
                    try {
                        value = await stepFn({ workflowId: id, seq, attempt, idempotencyKey: `${id}:${seq}` });
                    } catch (e) {
                        // Lease lost while fn ran: the workflow is not ours, record nothing.
                        if (controller.signal.aborted) throw controller.signal.reason;
                        if (attempt < retries) {
                            // A lost lease aborts the signal and cuts the sleep short.
                            await sleep(backoff(attempt, backoffMs, maxBackoffMs), undefined, { signal: controller.signal })
                                .catch(() => { throw controller.signal.reason; });
                            continue;
                        }
                        // The JSON form, exactly what replay will read back.
                        const error = JSON.parse(JSON.stringify(serializeError(e))) as SerializedError;
                        await checkpoint({ workflow_id: id, seq, name, kind: "step", output: null, error, attempts: attempt + 1 });
                        throw stepFailed(name, seq, error);
                    }

                    const json = toJson(value, `result of step ${name} (seq ${seq})`);
                    await checkpoint({ workflow_id: id, seq, name, kind: "step", output: json, error: null, attempts: attempt + 1 });
                    return JSON.parse(json) as T;
                }
            },
        };
        try {
            const output = await fn(ctx, row.input)
            await completeWorkflow(pool, schema, id, executorId, output);
        } catch (e) {
            if (e instanceof LeaseLostError) {
                controller.abort(e);
                return;
            }
            await failWorkflow(pool, schema, id, executorId, serializeError(e))
        }
    }

    return {
        claimAndLaunch,
        async start(): Promise<void> {
            if (pollLoop) return;
            pollLoop = every(cfg.pollMs, stopPolling.signal, "poll", tick);
            await tick();
        },

        // Stop claiming, let the running workflows finish, then stop the heartbeat.
        async stop(timeoutMs: number): Promise<void> {
            // 1. Claim nothing new.
            stopPolling.abort();
            await pollLoop;

            // 2. Let the running workflows finish, but wait at most timeoutMs.
            //    ref: false — this timer alone must not keep the process alive.
            await Promise.race([
                Promise.allSettled([...running.values()].map((r) => r.promise)),
                sleep(timeoutMs, undefined, { ref: false }),
            ]);

            // 3. Stop the heartbeat, otherwise it would extend the leases we give back.
            stopHeartbeat.abort();
            await heartbeatLoop;

            // 4. Whatever is still running: stop it and give its lease back.
            const left = [...running.keys()];
            if (left.length === 0) return;
            for (const id of left) {
                running.get(id)?.controller.abort(new LeaseLostError(`executor ${executorId} shut down`));
            }
            await releaseLeases(pool, schema, executorId, left);
        },
    };
};