import type {Migration} from "./types.js";

export const migrations: Migration[] = [
    {
        version: 1,
        sql: (s) => `
            CREATE TABLE ${s}.workflows (
              id                text PRIMARY KEY,
              name              text NOT NULL,
              version           text NOT NULL,
              status            text NOT NULL
                                CHECK (status IN ('pending','running','completed','failed','cancelled')),
              input             jsonb NOT NULL,
              output            jsonb,
              error             jsonb,
              executor_id       text,
              lease_until       timestamptz,
              recovery_attempts int  NOT NULL DEFAULT 0,
              parent_id         text REFERENCES ${s}.workflows(id),
              created_at        timestamptz NOT NULL DEFAULT now(),
              updated_at        timestamptz NOT NULL DEFAULT now()
            );
            CREATE INDEX ON ${s}.workflows (status, lease_until);
            
            CREATE TABLE ${s}.steps (
              workflow_id  text NOT NULL REFERENCES ${s}.workflows(id) ON DELETE CASCADE,
              seq          int  NOT NULL,
              name         text NOT NULL,
              kind         text NOT NULL CHECK (kind IN ('step','sleep','recv','now','uuid','child')),
              output       jsonb,
              error        jsonb,
              attempts     int  NOT NULL DEFAULT 1,
              completed_at timestamptz NOT NULL DEFAULT now(),
              PRIMARY KEY (workflow_id, seq));`
    },
    {
        version: 2,
        sql: (s) => `
            CREATE TABLE ${s}.messages (
              id              bigserial PRIMARY KEY,
              workflow_id     text NOT NULL REFERENCES ${s}.workflows(id) ON DELETE CASCADE,
              topic           text NOT NULL,
              payload         jsonb,
              idempotency_key text UNIQUE,
              created_at      timestamptz NOT NULL DEFAULT now(),
              consumed_at     timestamptz
            );
            CREATE INDEX ON ${s}.messages (workflow_id, topic) WHERE consumed_at IS NULL;
        `
    }
]