import type pg from "pg";
import { afterAll, beforeEach, describe, expect, it } from "vitest";
import { enqueue, OutsideTransactionError } from "../src/enqueue.js";
import type { TableOptions } from "../src/types.js";
import { count, freshOutbox, pool, transaction } from "./helpers.js";

let db: pg.Pool;
let table: TableOptions;

beforeEach(async () => {
  db ??= pool();
  table = await freshOutbox(db, "pgrelay_enqueue");
});

afterAll(async () => {
  await db.end();
});

describe("enqueue", () => {
  it("writes the event inside the caller's transaction", async () => {
    const id = await transaction(db, async (tx) => {
      await tx.query(`INSERT INTO pgrelay_enqueue.orders VALUES ('o1', 100)`);
      return enqueue(tx, { topic: "order.created", key: "o1", payload: { total: 100 } }, table);
    });
    expect(id).toMatch(/^[0-9a-f-]{36}$/);
    const { rows } = await db.query(
      `SELECT event_id::text, topic, key, payload, headers FROM pgrelay_enqueue.outbox`,
    );
    expect(rows).toEqual([
      { event_id: id, topic: "order.created", key: "o1", payload: { total: 100 }, headers: {} },
    ]);
  });

  it("writes nothing when the transaction rolls back", async () => {
    await expect(
      transaction(db, async (tx) => {
        await tx.query(`INSERT INTO pgrelay_enqueue.orders VALUES ('o1', 100)`);
        await enqueue(tx, { topic: "order.created", payload: {} }, table);
        throw new Error("payment declined");
      }),
    ).rejects.toThrow("payment declined");
    expect(await count(db, table)).toBe(0);
  });

  it("refuses to run outside a transaction, and writes nothing", async () => {
    // A pool: each statement autocommits on whichever connection is free. This is the
    // mistake that silently turns an outbox back into a dual write.
    await expect(
      enqueue(db, { topic: "order.created", payload: {} }, table),
    ).rejects.toBeInstanceOf(OutsideTransactionError);
    const client = await db.connect();
    try {
      await expect(enqueue(client, { topic: "order.created", payload: {} }, table)).rejects.toThrow(
        /outside a transaction.*Nothing was written/s,
      );
    } finally {
      client.release();
    }
    expect(await count(db, table)).toBe(0);
  });

  it("works as the first statement of a transaction, before anything else is written", async () => {
    // No transaction id yet, so the in-statement guard can't tell this from autocommit;
    // the SAVEPOINT fallback has to recognise the transaction block.
    const id = await transaction(db, async (tx) => {
      const eventId = await enqueue(tx, { topic: "t", payload: 1 }, table);
      await tx.query(`INSERT INTO pgrelay_enqueue.orders VALUES ('o1', 1)`);
      return eventId;
    });
    expect(await count(db, table, `event_id = '${id}'`)).toBe(1);
  });

  it("refuses autocommit over the simple query protocol as well as the extended one", async () => {
    // pg sends a query with no parameters as a simple Query message. The guard must
    // hold for both protocols; route every call through a parameterless wrapper.
    const client = await db.connect();
    try {
      const simple = {
        query: (text: string, params?: readonly unknown[]) =>
          params && params.length > 0
            ? client.query(
                text
                  .replaceAll("$1", `'${String(params[0]).replaceAll("'", "''")}'`)
                  .replaceAll("$2", String(params[1])),
              )
            : client.query(text),
      };
      await expect(enqueue(simple, { topic: "t", payload: 1 }, table)).rejects.toBeInstanceOf(
        OutsideTransactionError,
      );
    } finally {
      client.release();
    }
    expect(await count(db, table)).toBe(0);
  });

  it("writes a batch in one statement, in order, and returns the ids in order", async () => {
    const ids = await transaction(db, (tx) =>
      enqueue(
        tx,
        Array.from({ length: 100 }, (_, i) => ({ topic: "n", key: "k", payload: { i } })),
        table,
      ),
    );
    expect(ids).toHaveLength(100);
    const { rows } = await db.query<{ event_id: string; i: number }>(
      `SELECT event_id::text, (payload->>'i')::int AS i FROM pgrelay_enqueue.outbox ORDER BY id`,
    );
    expect(rows.map((r) => r.i)).toEqual(Array.from({ length: 100 }, (_, i) => i));
    expect(rows.map((r) => r.event_id)).toEqual(ids);
  });

  it("treats a supplied id as idempotent", async () => {
    const id = "6f1c7d0e-2b8a-4a51-9a4e-0d2f7b3c9e11";
    await transaction(db, (tx) => enqueue(tx, { id, topic: "t", payload: 1 }, table));
    await transaction(db, (tx) => enqueue(tx, { id, topic: "t", payload: 2 }, table));
    const { rows } = await db.query(`SELECT payload FROM pgrelay_enqueue.outbox`);
    expect(rows).toEqual([{ payload: 1 }]);
  });

  it("stores headers and holds back delayed events", async () => {
    await transaction(db, (tx) =>
      enqueue(tx, { topic: "t", payload: null, headers: { trace: "abc" }, delayMs: 60_000 }, table),
    );
    const { rows } = await db.query<{ headers: unknown; delayed: boolean; payload: unknown }>(
      `SELECT headers, payload, available_at > now() + interval '50 seconds' AS delayed FROM pgrelay_enqueue.outbox`,
    );
    expect(rows).toEqual([{ headers: { trace: "abc" }, payload: null, delayed: true }]);
  });

  it("rejects malformed events before touching the database", async () => {
    const bad = [
      { topic: "", payload: 1 },
      { topic: "t" },
      { topic: "t", payload: 1, id: "not-a-uuid" },
      { topic: "t", payload: 1, delayMs: -1 },
      { topic: "t", payload: 1, headers: { n: 1 } },
      { topic: "t", payload: 1, key: 42 },
    ];
    for (const event of bad) {
      await expect(
        transaction(db, (tx) => enqueue(tx, event as never, table)),
      ).rejects.toBeInstanceOf(TypeError);
    }
    expect(await count(db, table)).toBe(0);
  });

  it("quotes the configured table name", async () => {
    const odd = { schema: "pgrelay_enqueue", table: 'out"box; DROP TABLE x' };
    const { migrate } = await import("../src/health.js");
    await migrate(db, odd);
    await transaction(db, (tx) => enqueue(tx, { topic: "t", payload: 1 }, odd));
    const { rows } = await db.query(
      `SELECT count(*)::int AS n FROM pgrelay_enqueue."out""box; DROP TABLE x"`,
    );
    expect(rows[0]).toEqual({ n: 1 });
  });
});
