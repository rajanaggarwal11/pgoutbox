import { execFile } from "node:child_process";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import type pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { enqueue } from "../src/enqueue.js";
import { DATABASE_URL, freshOutbox, pool, transaction } from "./helpers.js";

const run = promisify(execFile);
const BIN = fileURLToPath(new URL("../dist/cli.js", import.meta.url));

async function cli(...args: string[]) {
  try {
    const { stdout, stderr } = await run(process.execPath, [BIN, ...args], {
      env: { ...process.env, NO_COLOR: "1" },
    });
    return { code: 0, stdout, stderr };
  } catch (error) {
    const e = error as { code: number; stdout: string; stderr: string };
    return { code: e.code, stdout: e.stdout, stderr: e.stderr };
  }
}

let db: pg.Pool;

beforeAll(async () => {
  db = pool();
  await freshOutbox(db, "pgrelay_cli");
});

afterAll(async () => {
  await db.end();
});

describe("the CLI (built)", () => {
  it("prints the DDL, which Postgres accepts", async () => {
    const { code, stdout } = await cli("sql", "--schema", "pgrelay_cli", "--table", "from_cli");
    expect(code).toBe(0);
    expect(stdout).toContain(`CREATE TABLE IF NOT EXISTS "pgrelay_cli"."from_cli"`);
    await db.query(stdout);
    await db.query(stdout); // idempotent
  });

  it("status exits 0 when healthy and 1 when stuck", async () => {
    const ok = await cli("status", DATABASE_URL, "--schema", "pgrelay_cli", "--table", "outbox");
    expect(ok.code).toBe(0);
    expect(ok.stdout).toContain("✓ The outbox is healthy.");

    await transaction(db, (tx) =>
      enqueue(
        tx,
        { topic: "order.created", payload: 1 },
        { schema: "pgrelay_cli", table: "outbox" },
      ),
    );
    await db.query(`UPDATE pgrelay_cli.outbox SET due_at = now() - interval '10 minutes'`);
    const stuck = await cli("status", DATABASE_URL, "--schema", "pgrelay_cli", "--table", "outbox");
    expect(stuck.code).toBe(1);
    expect(stuck.stdout).toMatch(/pending\s+1/);
    expect(stuck.stdout).toContain("Is a relay running against this table?");

    const json = await cli(
      "status",
      DATABASE_URL,
      "--schema",
      "pgrelay_cli",
      "--table",
      "outbox",
      "--json",
      "--max-lag",
      "3600",
    );
    expect(json.code).toBe(0);
    expect(JSON.parse(json.stdout)).toMatchObject({ version: 1, healthy: true, pending: 1 });
  });

  it("exits 2 on usage errors and unreachable databases", async () => {
    expect((await cli("frobnicate")).code).toBe(2);
    expect((await cli("status", "--max-lag", "soon", DATABASE_URL)).code).toBe(2);
    const env = { ...process.env, DATABASE_URL: "" };
    const noUrl = await run(process.execPath, [BIN, "status"], { env }).catch(
      (e: { code: number; stderr: string }) => e,
    );
    expect((noUrl as { code: number }).code).toBe(2);
    const down = await cli("status", "postgres://nobody@127.0.0.1:1/none");
    expect(down.code).toBe(2);
    expect(down.stderr).toContain("could not run");
  });

  it("prints its version and help", async () => {
    expect((await cli("--version")).stdout.trim()).toMatch(/^\d+\.\d+\.\d+$/);
    const help = await cli("--help");
    expect(help.code).toBe(0);
    expect(help.stdout).toContain("pgrelay status");
  });
});
