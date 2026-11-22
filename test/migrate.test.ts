import pg from "pg";
import postgres from "postgres";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { enqueue } from "../src/enqueue.js";
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

  it("survives it when the connections have already cached that the schema is missing", async () => {
    // Postgres caches "no such schema" per connection. A migrate that waited on the lock
    // while another created the schema must not trust that cache: without the catch-up
    // after the lock, CREATE SCHEMA IF NOT EXISTS failed on pg_namespace here.
    for (let round = 0; round < 5; round++) {
      await db.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
      const clients = await Promise.all(Array.from({ length: 10 }, () => db.connect()));
      for (const c of clients) await c.query(`SELECT to_regclass('"${schema}".outbox')`);
      clients.forEach((c) => c.release());
      const results = await Promise.allSettled(
        Array.from({ length: 10 }, () => migrate(db, { schema, table: "outbox" })),
      );
      expect(results.filter((r) => r.status === "rejected")).toEqual([]);
    }
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

  it("on an existing outbox, takes no lock that waits for transactions writing to it", async () => {
    const table = { schema, table: "outbox" };
    await migrate(db, table);
    const writer = await db.connect();
    // A second process booting, with a lock_timeout so that waiting fails instead of hangs.
    const booting = new pg.Client({
      connectionString: DATABASE_URL,
      options: "-c lock_timeout=500",
    });
    await booting.connect();
    try {
      await writer.query("BEGIN");
      await enqueue(writer, { topic: "t", payload: 1 }, table); // holds ROW EXCLUSIVE until COMMIT
      // Before: CREATE INDEX IF NOT EXISTS asked for SHARE before seeing the index was
      // there, so this failed with 55P03 (and without a timeout, every later enqueue and
      // claim would have queued behind it).
      await expect(migrate(booting, table)).resolves.toBeUndefined();
    } finally {
      await writer.query("ROLLBACK");
      writer.release();
      await booting.end();
    }
  });

  it("still creates whatever is missing", async () => {
    const table = { schema, table: "outbox" };
    await migrate(db, table);
    await db.query(`DROP INDEX "${schema}".outbox_leased`);
    await migrate(db, table);
    expect(await indexes()).toContain("outbox_leased");
  });

  it("holds the lock only until the DDL commits", async () => {
    await migrate(db, { schema, table: "outbox" });
    const { rows } = await db.query(`SELECT 1 FROM pg_locks WHERE locktype = 'advisory'`);
    expect(rows).toEqual([]);
  });
});
