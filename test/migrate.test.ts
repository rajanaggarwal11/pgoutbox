import pg from "pg";
import postgres from "postgres";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { migrate } from "../src/health.js";
import { withPostgresJs } from "../src/postgres-js.js";
import { DATABASE_URL } from "./helpers.js";

const schema = "pgrelay_migrate";
let db: pg.Pool;

beforeAll(() => {
  // One connection per concurrent migrate, so they really do overlap.
  db = new pg.Pool({ connectionString: DATABASE_URL, max: 10 });
});

afterAll(async () => {
  await db.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
  await db.end();
});

async function indexes(): Promise<string[]> {
  const { rows } = await db.query<{ indexname: string }>(
    `SELECT indexname FROM pg_indexes WHERE schemaname = $1 ORDER BY indexname`,
    [schema],
  );
  return rows.map((r) => r.indexname);
}

describe("migrate", () => {
  // Without the advisory lock, 10 boots racing on a fresh schema failed 138 times out of
  // 200 with 23505 on pg_namespace_nspname_index (and on pg_type / pg_class once the
  // schema existed).
  it("survives 10 processes running it at once on a fresh schema", async () => {
    for (let round = 0; round < 5; round++) {
      await db.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
      const results = await Promise.allSettled(
        Array.from({ length: 10 }, () => migrate(db, { schema, table: "outbox" })),
      );
      expect(results.filter((r) => r.status === "rejected")).toEqual([]);
    }
    expect(await indexes()).toEqual([
      "outbox_dead",
      "outbox_delivered",
      "outbox_event_id_key",
      "outbox_key_holder",
      "outbox_leased",
      "outbox_pending",
      "outbox_pending_key",
      "outbox_pkey",
    ]);
  });

  it("serialises two tables racing into one new schema, and works through postgres.js", async () => {
    const sql = postgres(DATABASE_URL, { max: 10, onnotice: () => {} });
    try {
      await db.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
      const results = await Promise.allSettled(
        Array.from({ length: 10 }, (_, i) =>
          migrate(withPostgresJs(sql), { schema, table: i % 2 ? "outbox_a" : "outbox_b" }),
        ),
      );
      expect(results.filter((r) => r.status === "rejected")).toEqual([]);
    } finally {
      await sql.end();
    }
  });

  it("holds the lock only until the DDL commits", async () => {
    await migrate(db, { schema, table: "outbox" });
    const { rows } = await db.query(`SELECT 1 FROM pg_locks WHERE locktype = 'advisory'`);
    expect(rows).toEqual([]);
  });
});
