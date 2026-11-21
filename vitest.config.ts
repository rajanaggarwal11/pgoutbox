import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["test/**/*.test.ts"],
    environment: "node",
    // The suites share one database and each owns a schema; run files one at a time.
    fileParallelism: false,
    testTimeout: 60_000,
    hookTimeout: 30_000,
  },
});
