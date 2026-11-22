import { drizzle } from "drizzle-orm/node-postgres";
import { drizzle as drizzlePostgresJs } from "drizzle-orm/postgres-js";
import { sql as dsql } from "drizzle-orm";
import type pg from "pg";
import postgres from "postgres";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { withDrizzle } from "../src/drizzle.js";
import { enqueue, OutsideTransactionError } from "../src/enqueue.js";
import { withPostgresJs } from "../src/postgres-js.js";
import { Relay } from "../src/relay.js";
import type { TableOptions } from "../src/types.js";
import { count, DATABASE_URL, freshOutbox, pool } from "./helpers.js";

let db: pg.Pool;
let sql: postgres.Sql;
let table: TableOptions;

beforeAll(() => {
  db = pool();
  sql = postgres(DATABASE_URL, { max: 5, onnotice: () => {} });
});

beforeEach(async () => {
  table = await freshOutbox(db, "pgoutbox_adapters");
});

afterAll(async () => {
  await sql.end();
  await db.end();
});

describe("postgres.js", () => {
  it("enqueues inside sql.begin and rolls back with it", async () => {
    await sql.begin(async (tx) => {
      await tx`INSERT INTO pgoutbox_adapters.orders VALUES ('o1', 1)`;
      await enqueue(withPostgresJs(tx), { topic: "ok", key: "o1", payload: { n: 1 } }, table);
    });
    await sql
      .begin(async (tx) => {
        await enqueue(withPostgresJs(tx), { topic: "nope", payload: 1 }, table);
        throw new Error("abort");
      })
      .catch(() => {});
    expect(await count(db, table)).toBe(1);
  });

  it("refuses the top-level sql", async () => {
    await expect(
      enqueue(withPostgresJs(sql), { topic: "t", payload: 1 }, table),
    ).rejects.toBeInstanceOf(OutsideTransactionError);
    expect(await count(db, table)).toBe(0);
  });

  it("runs a relay", async () => {
    await sql.begin((tx) =>
      enqueue(
        withPostgresJs(tx),
        [
          { topic: "a", payload: { x: 1 } },
          { topic: "b", payload: null },
        ],
        table,
      ),
    );
    const seen: unknown[] = [];
    const delivered = await new Relay(withPostgresJs(sql), {
      ...table,
      handler: async (e) => void seen.push(e.payload),
    }).drain();
    expect(delivered).toBe(2);
    expect(seen).toEqual(expect.arrayContaining([{ x: 1 }, null]));
  });
});

describe("Drizzle", () => {
  it("enqueues inside db.transaction over node-postgres, and refuses db itself", async () => {
    const d = drizzle(db);
    await d.transaction(async (tx) => {
      await tx.execute(dsql`INSERT INTO pgoutbox_adapters.orders VALUES ('o1', 1)`);
      await enqueue(
        withDrizzle(tx),
        { topic: "ok", payload: { n: 1 }, headers: { v: "2" } },
        table,
      );
    });
    await expect(enqueue(withDrizzle(d), { topic: "t", payload: 1 }, table)).rejects.toBeInstanceOf(
      OutsideTransactionError,
    );
    const { rows } = await db.query(`SELECT topic, payload, headers FROM pgoutbox_adapters.outbox`);
    expect(rows).toEqual([{ topic: "ok", payload: { n: 1 }, headers: { v: "2" } }]);
  });

  it("enqueues inside db.transaction over postgres.js, and a relay delivers through Drizzle", async () => {
    const d = drizzlePostgresJs(sql);
    await d.transaction((tx) => enqueue(withDrizzle(tx), { topic: "pj", payload: 1 }, table));
    const delivered = await new Relay(withDrizzle(d), {
      ...table,
      handler: async () => {},
    }).drain();
    expect(delivered).toBe(1);
  });

  it("keeps parameters as parameters", async () => {
    const d = drizzle(db);
    const nasty = "'); DROP TABLE pgoutbox_adapters.orders; --";
    await d.transaction((tx) => enqueue(withDrizzle(tx), { topic: nasty, payload: nasty }, table));
    const { rows } = await db.query(`SELECT topic FROM pgoutbox_adapters.outbox`);
    expect(rows).toEqual([{ topic: nasty }]);
    expect(await count(db, { schema: "pgoutbox_adapters", table: "orders" })).toBe(0);
  });
});

describe("adapter names", () => {
  // `drizzle` would shadow drizzle-orm's own `drizzle` in every file that imports both,
  // which is every file that sets up a Drizzle database and enqueues. Clash-free names,
  // matching pgrls (`withPostgresJs`).
  it("exports withDrizzle and withPostgresJs, and nothing that clashes with the drivers", async () => {
    expect(Object.keys(await import("../src/drizzle.js"))).toEqual(["withDrizzle"]);
    expect(Object.keys(await import("../src/postgres-js.js"))).toEqual(["withPostgresJs"]);
  });
});
