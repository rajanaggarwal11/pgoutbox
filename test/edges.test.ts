import { drizzle as drizzlePg } from "drizzle-orm/node-postgres";
import type pg from "pg";
import { afterAll, beforeEach, describe, expect, it } from "vitest";
import { drizzle } from "../src/drizzle.js";
import { enqueue } from "../src/enqueue.js";
import { assertOutboxHealthy, migrate, outboxStatus } from "../src/health.js";
import { Relay, type RelayOptions } from "../src/relay.js";
import { outboxSql } from "../src/sql.js";
import type { TableOptions } from "../src/types.js";
import { count, freshOutbox, pool, sleep, transaction } from "./helpers.js";

/**
 * The cases an independent review found, each reproduced before it was fixed. They are
 * kept here as regressions: every one of them failed silently.
 */
let db: pg.Pool;
let table: TableOptions;

beforeEach(async () => {
  db ??= pool();
  table = await freshOutbox(db, "pgrelay_edges");
});

afterAll(async () => {
  await db.end();
});

function relay(options: Partial<RelayOptions> & Pick<RelayOptions, "handler">): Relay {
  return new Relay(db, { ...table, pollIntervalMs: 10, onError: () => {}, ...options });
}

/** Opens a transaction that has enqueued an event for `key` and not committed yet. */
async function openWriter(key: string, topic: string) {
  const client = await db.connect();
  await client.query("BEGIN");
  await client.query(`INSERT INTO pgrelay_edges.orders VALUES ($1, 1)`, [topic]);
  await enqueue(client, { topic, key, payload: null }, table);
  return {
    commit: async () => {
      await client.query("COMMIT");
      client.release();
    },
  };
}

describe("per-key exclusivity with late commits", () => {
  it("never runs a late-committing lower id beside its key's event already in flight", async () => {
    const early = await openWriter("K", "first-insert"); // lower id, commits late
    await transaction(db, (tx) =>
      enqueue(tx, { topic: "second-insert", key: "K", payload: null }, table),
    );

    const active = new Set<string>();
    const order: string[] = [];
    let overlap = false;
    let releaseA!: () => void;
    const gateA = new Promise<void>((r) => (releaseA = r));
    const handler = async (e: { topic: string }) => {
      if (active.size > 0) overlap = true;
      active.add(e.topic);
      order.push(e.topic);
      if (e.topic === "second-insert") await gateA;
      active.delete(e.topic);
    };

    const a = relay({ handler });
    const b = relay({ handler });
    a.start();
    for (let i = 0; i < 100 && order.length === 0; i++) await sleep(5);
    expect(order).toEqual(["second-insert"]); // the only visible event

    await early.commit(); // the lower id becomes visible while its key is in flight
    b.start();
    await sleep(150);
    expect(order).toEqual(["second-insert"]); // it waits its turn
    releaseA();
    for (let i = 0; i < 100 && order.length < 2; i++) await sleep(10);
    await Promise.all([a.stop(), b.stop()]);

    expect(overlap).toBe(false);
    expect(order).toEqual(["second-insert", "first-insert"]); // commit order
  });

  it("an event waiting to retry keeps its key from a late-committing lower id", async () => {
    const early = await openWriter("K", "late");
    await transaction(db, (tx) =>
      enqueue(tx, { topic: "retrying", key: "K", payload: null }, table),
    );
    let fail = true;
    const seen: string[] = [];
    const r = relay({
      backoff: () => 100,
      handler: async (e) => {
        seen.push(e.topic);
        if (e.topic === "retrying" && fail) {
          fail = false;
          throw new Error("try again");
        }
      },
    });
    await r.drain(); // "retrying" fails once and now waits out its backoff
    await early.commit();
    expect(await r.drain()).toBe(0); // "late" must not jump ahead
    await sleep(150);
    await r.drain();
    await r.drain();
    expect(seen).toEqual(["retrying", "retrying", "late"]);
  });

  it("Postgres itself refuses two started events for one key", async () => {
    await transaction(db, (tx) =>
      enqueue(
        tx,
        [
          { topic: "a", key: "K", payload: null },
          { topic: "b", key: "K", payload: null },
        ],
        table,
      ),
    );
    await expect(db.query(`UPDATE pgrelay_edges.outbox SET attempts = 1`)).rejects.toMatchObject({
      code: "23505",
    });
  });
});

describe("failures that must not go unrecorded", () => {
  it("an onError that throws neither crashes the process nor loses the failure", async () => {
    await transaction(db, (tx) => enqueue(tx, { topic: "t", payload: null }, table));
    const unhandled: unknown[] = [];
    const listener = (e: unknown) => unhandled.push(e);
    process.on("unhandledRejection", listener);
    try {
      await relay({
        onError: () => {
          throw new Error("logger is down too");
        },
        handler: async () => {
          throw new Error("broker down");
        },
      }).drain();
      await sleep(20);
    } finally {
      process.off("unhandledRejection", listener);
    }
    expect(unhandled).toEqual([]);
    const { rows } = await db.query(`SELECT last_error, locked_until FROM pgrelay_edges.outbox`);
    expect(rows[0]).toEqual({ last_error: "Error: broker down", locked_until: null });
  });

  it("a backoff that throws still records the failure, with the default delay", async () => {
    await transaction(db, (tx) => enqueue(tx, { topic: "t", payload: null }, table));
    await relay({
      backoff: () => {
        throw new Error("bad backoff");
      },
      handler: async () => {
        throw new Error("broker down");
      },
    }).drain();
    expect(
      await count(db, table, "last_error = 'Error: broker down' AND available_at > now()"),
    ).toBe(1);
  });

  it("an event that keeps killing its relay is set aside after maxAttempts", async () => {
    await transaction(db, (tx) => enqueue(tx, { topic: "oom", key: "K", payload: null }, table));
    // Simulate three relays that claimed it and died: attempts used up, lease expired.
    await db.query(
      `UPDATE pgrelay_edges.outbox SET attempts = 3, locked_until = now() - interval '1 second', locked_by = 'dead'`,
    );
    const seen: string[] = [];
    expect(
      await relay({ maxAttempts: 3, handler: async (e) => void seen.push(e.id) }).drain(),
    ).toBe(0);
    expect(seen).toEqual([]);
    const { rows } = await db.query<{ dead: boolean; last_error: string }>(
      `SELECT dead_at IS NOT NULL AS dead, last_error FROM pgrelay_edges.outbox`,
    );
    expect(rows[0]!.dead).toBe(true);
    expect(rows[0]!.last_error).toMatch(/abandoned 3 times/);
  });

  it("a relay whose lease was taken over cannot mark the event delivered under the new owner", async () => {
    await transaction(db, (tx) =>
      enqueue(
        tx,
        [
          { topic: "n", key: "K", payload: null },
          { topic: "n+1", key: "K", payload: null },
        ],
        table,
      ),
    );
    // A shared log of what each relay started and finished, in real order.
    const log: string[] = [];
    let releaseA!: () => void;
    const gateA = new Promise<void>((r) => (releaseA = r));
    const a = relay({
      leaseMs: 200,
      handler: async (e) => {
        log.push(`a start ${e.topic}`);
        if (e.topic === "n") await gateA; // hangs past its lease, ignoring the signal
        log.push(`a done ${e.topic}`);
      },
    });
    a.start();
    await sleep(300); // a's lease has expired; it is still "sending"
    // b takes over and is mid-send when a finishes.
    let releaseB!: () => void;
    const gateB = new Promise<void>((r) => (releaseB = r));
    const b = relay({
      handler: async (e) => {
        log.push(`b start ${e.topic}`);
        if (e.topic === "n") await gateB;
        log.push(`b done ${e.topic}`);
      },
    });
    b.start();
    // Wait for the takeover itself (b holds "n" on attempt 2), not a fixed time: on a
    // loaded machine a's lease may run out later than the sleep above suggests.
    for (let i = 0; i < 600 && !log.includes("b start n"); i++) await sleep(10);
    expect(await count(db, table, "topic = 'n' AND attempts = 2")).toBe(1);
    releaseA();
    await sleep(100);
    // a's success must not have released the key while b is re-sending "n".
    expect(await count(db, table, "topic = 'n+1' AND attempts > 0")).toBe(0);
    expect(log.some((l) => l.endsWith("start n+1"))).toBe(false);
    releaseB();
    for (let i = 0; i < 300 && !log.some((l) => l.endsWith("done n+1")); i++) await sleep(10);
    await Promise.all([a.stop(), b.stop()]);
    // Either relay may deliver n+1, but only after b finished re-sending n.
    const nextStart = log.findIndex((l) => l.endsWith("start n+1"));
    expect(nextStart).toBeGreaterThan(log.indexOf("b done n"));
    expect(log.indexOf("b done n")).toBeGreaterThan(-1);
  });
});

describe("health without false alarms", () => {
  it("a delayed event is scheduled, not stuck", async () => {
    await transaction(db, (tx) =>
      enqueue(tx, { topic: "later", payload: null, delayMs: 3_600_000 }, table),
    );
    await db.query(`UPDATE pgrelay_edges.outbox SET created_at = now() - interval '2 minutes'`);
    const status = await assertOutboxHealthy(db, table);
    expect(status).toMatchObject({ pending: 1, scheduled: 1, oldestPending: null });
  });

  it("an event in flight on its first attempt is not 'never attempted'", async () => {
    await transaction(db, (tx) => enqueue(tx, { topic: "slow", payload: null }, table));
    await db.query(
      `UPDATE pgrelay_edges.outbox SET attempts = 1, locked_until = now() + interval '1 minute', due_at = now() - interval '5 minutes'`,
    );
    const error = await assertOutboxHealthy(db, table).catch((e: Error) => e);
    expect((error as Error).message).toContain(
      "A relay claimed it (attempt 1) and has not finished",
    );
    expect((await outboxStatus(db, table)).inFlight).toBe(1);
  });

  it("lag counts from when the event was written, not from when its transaction began", async () => {
    const client = await db.connect();
    await client.query("BEGIN");
    await client.query(`INSERT INTO pgrelay_edges.orders VALUES ('o', 1)`);
    await sleep(1200); // a slow transaction
    await enqueue(client, { topic: "t", payload: null }, table);
    await client.query("COMMIT");
    client.release();
    const status = await outboxStatus(db, table);
    expect(status.oldestPending!.ageSeconds).toBeLessThan(1);
  });
});

describe("lifecycle and adapters", () => {
  it("start() while stop() is finishing is refused rather than leaking a loop", async () => {
    await transaction(db, (tx) => enqueue(tx, { topic: "t", payload: null }, table));
    let release!: () => void;
    const gate = new Promise<void>((r) => (release = r));
    const r = relay({ handler: async () => gate });
    r.start();
    await sleep(50);
    const stopping = r.stop();
    expect(() => r.start()).toThrow(/stop\(\) is still finishing/);
    release();
    await stopping;
    r.start(); // fine once stopped
    await r.stop();
  });

  it("drain() waits for running handlers even when a later claim fails", async () => {
    await transaction(db, (tx) => enqueue(tx, { topic: "t", payload: null }, table));
    let claims = 0;
    let finished = false;
    const flaky = {
      query: (text: string, params?: readonly unknown[]) => {
        if (text.includes("WITH candidates") && ++claims === 2) {
          return Promise.reject(new Error("connection lost"));
        }
        return db.query(text, params as unknown[]);
      },
    };
    const r = new Relay(flaky, {
      ...table,
      concurrency: 2,
      onError: () => {},
      handler: async () => {
        await sleep(100);
        finished = true;
      },
    });
    await expect(r.drain()).rejects.toThrow("connection lost");
    expect(finished).toBe(true);
  });

  it("the Drizzle adapter leaves $n inside quoted names alone", async () => {
    const odd = { schema: "pgrelay_edges", table: "events$2" };
    await migrate(db, odd);
    const d = drizzlePg(db);
    await d.transaction((tx) => enqueue(drizzle(tx), { topic: "t", payload: { a: "$1" } }, odd));
    expect(await new Relay(drizzle(d), { ...odd, handler: async () => {} }).drain()).toBe(1);
  });

  it("refuses a table name whose index names Postgres would truncate", () => {
    expect(() => outboxSql({ table: "x".repeat(60) })).toThrow(/exceed 63 bytes/);
  });

  it("refuses a lease longer than a timer can hold", () => {
    expect(() => relay({ handler: async () => {}, leaseMs: 2 ** 31 })).toThrow(/leaseMs/);
  });
});
