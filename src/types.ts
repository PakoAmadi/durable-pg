import type {Pool, QueryResult, QueryResultRow} from "pg";

export interface DurableOptions {
    connectionString?: string;
    pool?: Pool;
    schema?: string;
    executorId?: string;
    leaseMs?: number;              // 30_000
    heartbeatMs?: number;          // leaseMs / 3
    pollMs?: number;               // 1_000
    maxConcurrent?: number;        // 50
    maxRecoveryAttempts?: number;  // 10
    testHooks?: TestHooks;
}

export interface TestHooks {
    beforeCheckpoint?: (workflowId: string, seq: number) => void | Promise<void>;
    afterCheckpoint?: (workflowId: string, seq: number) => void | Promise<void>;
    beforeHeartbeat?: () => void | Promise<void>;
}

export interface Durable {
    migrate(): Promise<void>;
    shutdown(): Promise<void>;
    workflow<I, O>(name: string, fn: WorkflowFn<I, O>, opts?: { version?: string }): Workflow<I, O>;
    resume(name: string): Promise<void>;
    start(): Promise<void>;
}

export interface Migration {
    version: number;
    sql: (schema: string) => string;
}

export interface Queryable {
    query<R extends QueryResultRow = QueryResultRow>(
        text: string,
        values?: unknown[],
    ): Promise<QueryResult<R>>;
}

export interface WorkflowContext {
    readonly workflowId: string;
    readonly signal: AbortSignal;
    step<T>(name: string, fn: () => Promise<T>): Promise<T>;
}

export type WorkflowFn<I, O> = (ctx: WorkflowContext, input: I) => Promise<O>;

export interface StartOptions {
    id?: string;
}

export interface WorkflowHandle<O> {
    readonly id: string;
    result(): Promise<O>
}

export interface Workflow<I, O> {
    readonly name: string;
    start(input: I, opts?: StartOptions): Promise<WorkflowHandle<O>>
}

export interface NewWorkflow {
    id: string;
    name: string;
    input: unknown;
}

export type WorkflowStatus = "pending" | "running" | "completed" | "failed" | "cancelled";

export interface WorkflowRow {
    id: string;
    name: string;
    status: WorkflowStatus;
    input: unknown;
    output: unknown;
    error: SerializedError | null;
}

export interface SerializedError {
    name: string;
    message: string;
    stack?: string;
}

export type StepStatus = 'step'| 'sleep' | 'recv' | 'now' | 'uuid' | 'child';

export interface StepRow {
    workflow_id: string,
    seq: number,
    name:  string,
    kind: StepStatus,
    output: unknown,
    error:  SerializedError | null,
    attempts?: number,
    completed_at?: Date,
}

export interface Run { promise: Promise<void>; controller: AbortController }