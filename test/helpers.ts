import pg from "pg";
import { migrate } from "../src/health.js";
import type { TableOptions } from "../src/types.js";

// Named DATABASE_URL, not URL: the latter shadows the global URL constructor.
export const DATABASE_URL =
  process.env.TEST_DATABASE_URL ?? "postgres://postgres:postgres@localhost:5432/pgoutbox_test";

export function pool(max = 20): pg.Pool {
  return new pg.Pool({ connectionString: DATABASE_URL, max });
}

/** A fresh schema with an empty outbox in it. Each test file owns one. */
export async function freshOutbox(db: pg.Pool, schema: string): Promise<TableOptions> {
  await db.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
  const table = { schema, table: "outbox" };
  await migrate(db, table);
  await db.query(`CREATE TABLE "${schema}".orders (id text PRIMARY KEY, total int NOT NULL)`);
  return table;
}

/** Runs `fn` in BEGIN … COMMIT on one pooled connection; ROLLBACK if it throws. */
export async function transaction<T>(
  db: pg.Pool,
  fn: (tx: pg.PoolClient) => Promise<T>,
): Promise<T> {
  const client = await db.connect();
  try {
    await client.query("BEGIN");
    const result = await fn(client);
    await client.query("COMMIT");
    return result;
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}

export async function count(db: pg.Pool, table: TableOptions, where = "true"): Promise<number> {
  const { rows } = await db.query<{ n: number }>(
    `SELECT count(*)::int AS n FROM "${table.schema}"."${table.table}" WHERE ${where}`,
  );
  return rows[0]!.n;
}

export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
