import { defineConfig } from "tsup";

export default defineConfig({
  entry: ["src/index.ts", "src/drizzle.ts", "src/postgres-js.ts", "src/cli.ts"],
  format: ["esm"],
  target: "node22",
  dts: true,
  clean: true,
  sourcemap: true,
  splitting: false,
  // Drivers are optional: importing pgoutbox must not pull any of them in.
  external: ["drizzle-orm", "postgres", "pg"],
});
