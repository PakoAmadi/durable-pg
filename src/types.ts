import type {Pool, QueryResult, QueryResultRow} from "pg";

export interface DurableOptions {
    connectionString?: string;
    pool?: Pool;
    schema?: string;          // по умолчанию "durable"
}

export interface Durable {
    migrate(): Promise<void>;
    shutdown(): Promise<void>;
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