import { existsSync, readFileSync } from "node:fs";
import { parseEnv } from "node:util";
import { defineConfig } from "vitest/config";

// DATABASE_URL lives in .env (see .env.example).
const env = existsSync(".env") ? parseEnv(readFileSync(".env", "utf8")) : {};

export default defineConfig({
  test: {
    env,
    include: ["test/**/*.test.ts"],
    testTimeout: 15_000,
  },
});
