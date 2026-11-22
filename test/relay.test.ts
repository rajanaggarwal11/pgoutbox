import type pg from "pg";
import { afterAll, beforeEach, describe, expect, it } from "vitest";
import { enqueue } from "../src/enqueue.js";
import { Relay, type RelayOptions } from "../src/relay.js";
import type { OutboxEvent, TableOptions } from "../src/types.js";
import { count, freshOutbox, pool, sleep, transaction } from "./helpers.js";

let db: pg.Pool;
let table: TableOptions;

beforeEach(async () => {
  db ??= pool();
  table = await freshOutbox(db, "pgrelay_relay");
});

afterAll(async () => {
  await db.end();
});

const quiet = { onError: () => {} };

function relay(options: Partial<RelayOptions> & Pick<RelayOptions, "handler">): Relay {
  return new Relay(db, { ...table, pollIntervalMs: 20, ...quiet, ...options });
}

async function put(n: number, key?: (i: number) => string | null) {
  return transaction(db, (tx) =>
    enqueue(
      tx,
      Array.from({ length: n }, (_, i) => ({ topic: "t", key: key?.(i) ?? null, payload: { i } })),
      table,
    ),
  );
}

describe("Relay", () => {
  it("delivers committed events and records them as delivered", async () => {
    const ids = await put(5);
    const seen: OutboxEvent[] = [];
    const delivered = await relay({ handler: async (e) => void seen.push(e) }).drain();
    expect(delivered).toBe(5);
    expect(seen.map((e) => e.id).sort()).toEqual([...ids].sort());
    expect(seen[0]).toMatchObject({ topic: "t", attempt: 1, headers: {} });
    expect(seen[0]!.createdAt).toBeInstanceOf(Date);
    expect(await count(db, table, "delivered_at IS NOT NULL")).toBe(5);
  });

  it("never delivers an event whose transaction rolled back", async () => {
    await transaction(db, (tx) => enqueue(tx, { topic: "kept", payload: 1 }, table));
    await transaction(db, async (tx) => {
      await enqueue(tx, { topic: "rolled-back", payload: 1 }, table);
      throw new Error("abort");
    }).catch(() => {});
    const topics: string[] = [];
    await relay({ handler: async (e) => void topics.push(e.topic) }).drain();
    expect(topics).toEqual(["kept"]);
  });

  it("delivers an event whose transaction commits after a later one's — no cursor, no gap", async () => {
    // Transaction A takes the lower id and holds it; B takes a higher id and commits
    // first. A poller that remembers "last id seen" moves past A's id while it is
    // still invisible and never comes back for it. That is the classic silent loss.
    const slow = await db.connect();
    await slow.query("BEGIN");
    await slow.query(`INSERT INTO pgrelay_relay.orders VALUES ('a', 1)`);
    const a = await enqueue(slow, { topic: "a", payload: 1 }, table);

    const b = await transaction(db, (tx) => enqueue(tx, { topic: "b", payload: 1 }, table));
    const { rows } = await db.query<{ ok: boolean }>(
      `SELECT (SELECT id FROM pgrelay_relay.outbox WHERE event_id = $1) > 0 AS ok`,
      [b],
    );
    expect(rows[0]!.ok).toBe(true);

    const seen: string[] = [];
    const r = relay({ handler: async (e) => void seen.push(e.id) });
    expect(await r.drain()).toBe(1); // only B is visible
    await slow.query("COMMIT");
    slow.release();
    expect(await r.drain()).toBe(1); // A, late but not lost
    expect(seen).toEqual([b, a]);
  });

  it("retries a failing handler with backoff, then succeeds", async () => {
    await put(1);
    let calls = 0;
    const attempts: number[] = [];
    const r = relay({
      backoff: () => 50,
      handler: async (e) => {
        attempts.push(e.attempt);
        if (++calls < 3) throw new Error(`broker down (${calls})`);
      },
    });
    expect(await r.drain()).toBe(0); // failed once; now waiting out the backoff
    const { rows } = await db.query(`SELECT attempts, last_error FROM pgrelay_relay.outbox`);
    expect(rows[0]).toEqual({ attempts: 1, last_error: "Error: broker down (1)" });
    await sleep(80);
    await r.drain();
    await sleep(80);
    expect(await r.drain()).toBe(1);
    expect(attempts).toEqual([1, 2, 3]);
    expect(await count(db, table, "delivered_at IS NOT NULL AND last_error IS NULL")).toBe(1);
  });

  it("marks an event dead after maxAttempts, and the rest keep flowing", async () => {
    await transaction(db, (tx) =>
      enqueue(
        tx,
        [
          { topic: "poison", key: "k", payload: 1 },
          { topic: "fine", key: "other", payload: 1 },
        ],
        table,
      ),
    );
    const errors: unknown[] = [];
    const r = relay({
      maxAttempts: 2,
      backoff: () => 0,
      onError: (e) => errors.push(e),
      handler: async (e) => {
        if (e.topic === "poison") throw new Error("cannot parse");
      },
    });
    await r.drain();
    await r.drain();
    expect(
      await count(db, table, "dead_at IS NOT NULL AND topic = 'poison' AND attempts = 2"),
    ).toBe(1);
    expect(await count(db, table, "delivered_at IS NOT NULL AND topic = 'fine'")).toBe(1);
    expect(errors).toHaveLength(2);
  });

  it("lets another relay take over an event once the lease runs out", async () => {
    await put(1);
    let releaseHang!: () => void;
    const hung = new Promise<void>((r) => (releaseHang = r));
    let aborted = false;
    const first = relay({
      leaseMs: 300,
      handler: async (_e, { signal }) => {
        signal.addEventListener("abort", () => (aborted = true));
        await hung; // a handler that hangs past its lease
      },
    });
    first.start();
    try {
      await sleep(100);
      const second = relay({ handler: async () => {} });
      expect(await second.drain()).toBe(0); // still leased
      await sleep(350);
      expect(aborted).toBe(true); // the first handler was told to stop
      // The first relay is alive and polling, but does not re-run its own hung
      // handler; the second takes over the expired lease.
      expect(await second.drain()).toBe(1);
    } finally {
      releaseHang();
      await first.stop();
    }
    const { rows } = await db.query(
      `SELECT attempts, delivered_at IS NOT NULL AS done FROM pgrelay_relay.outbox`,
    );
    expect(rows[0]).toEqual({ attempts: 2, done: true });
  });

  it("stop() waits for in-flight handlers and claims nothing new", async () => {
    await put(3);
    const finished: number[] = [];
    const r = relay({
      concurrency: 1,
      handler: async (e) => {
        await sleep(100);
        finished.push((e.payload as { i: number }).i);
      },
    });
    r.start();
    await sleep(30);
    await r.stop();
    expect(finished).toEqual([0]); // the one in flight completed; nothing else started
    expect(await count(db, table, "delivered_at IS NOT NULL")).toBe(1);
    expect(await count(db, table, "locked_until IS NOT NULL AND delivered_at IS NULL")).toBe(0);
  });

  it("runs in the background, picks up new events, and wake() skips the idle wait", async () => {
    const seen: string[] = [];
    const r = relay({ pollIntervalMs: 60_000, handler: async (e) => void seen.push(e.topic) });
    r.start();
    await sleep(50);
    await transaction(db, (tx) => enqueue(tx, { topic: "late", payload: 1 }, table));
    r.wake();
    for (let i = 0; i < 50 && seen.length === 0; i++) await sleep(10);
    await r.stop();
    expect(seen).toEqual(["late"]);
  });

  it("keeps going when the database is briefly unreachable", async () => {
    await put(1);
    let failNext = true;
    const flaky = {
      query: (text: string, params?: readonly unknown[]) => {
        if (failNext) {
          failNext = false;
          return Promise.reject(new Error("connection terminated"));
        }
        return db.query(text, params as unknown[]);
      },
    };
    const errors: unknown[] = [];
    const seen: string[] = [];
    const r = new Relay(flaky, {
      ...table,
      pollIntervalMs: 10,
      onError: (e) => errors.push(e),
      handler: async (e) => void seen.push(e.id),
    });
    r.start();
    for (let i = 0; i < 300 && seen.length === 0; i++) await sleep(10);
    await r.stop();
    expect(errors).toHaveLength(1);
    expect(seen).toHaveLength(1);
  });

  it("deletes delivered rows immediately with keepDeliveredMs: 0, and on schedule otherwise", async () => {
    await put(3);
    await relay({ keepDeliveredMs: 0, handler: async () => {} }).drain();
    expect(await count(db, table)).toBe(0);

    await put(2);
    await relay({ handler: async () => {} }).drain();
    await db.query(`UPDATE pgrelay_relay.outbox SET delivered_at = now() - interval '2 days'`);
    const r = relay({ cleanupIntervalMs: 1, handler: async () => {} });
    r.start();
    for (let i = 0; i < 100 && (await count(db, table)) > 0; i++) await sleep(10);
    await r.stop();
    expect(await count(db, table)).toBe(0);
  });

  it("validates its options", () => {
    expect(() => new Relay(db, {} as RelayOptions)).toThrow(/handler/);
    expect(() => relay({ handler: async () => {}, concurrency: 0 })).toThrow(/concurrency/);
    expect(() => relay({ handler: async () => {}, leaseMs: 1.5 })).toThrow(/leaseMs/);
  });

  it("refuses a poll interval that would make an idle relay a busy loop", () => {
    // 0 re-ran the claim query back to back; over 2^31-1 ms, setTimeout fires after 1 ms.
    for (const pollIntervalMs of [0, 5, -1, Number.NaN, Number.POSITIVE_INFINITY, 2 ** 31]) {
      expect(() => relay({ handler: async () => {}, pollIntervalMs })).toThrow(
        /pollIntervalMs must be between 10 and 2147483647 ms/,
      );
    }
    expect(() => relay({ handler: async () => {}, pollIntervalMs: 10 })).not.toThrow();
    expect(() => relay({ handler: async () => {}, pollIntervalMs: undefined })).not.toThrow();
  });
});
