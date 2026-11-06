import { setTimeout as sleep } from "node:timers/promises";

export const every = (ms: number, signal: AbortSignal, name: string, fn: () => Promise<void>): Promise<void> =>
    (async () => {
        while (!signal.aborted) {
            try {
                await sleep(ms, undefined, { signal });
            } catch {
                return; // aborted while sleeping
            }
            try {
                await fn();
            } catch (e) {
                console.error(`durable-pg: ${name} failed`, e);
            }
        }
    })();