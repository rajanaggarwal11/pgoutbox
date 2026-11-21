import type pg from "pg";
import { afterAll, beforeEach, describe, expect, it } from "vitest";
import { enqueue } from "../src/enqueue.js";
import {
  assertOutboxHealthy,
  formatDuration,
  OutboxUnhealthyError,
  outboxStatus,
  retryDead,
} from "../src/health.js";
import { Relay } from "../src/relay.js";
import type { TableOptions } from "../src/types.js";
import { count, freshOutbox, pool, transaction } from "./helpers.js";

let db: pg.Pool;
let table: TableOptions;

beforeEach(async () => {
  db ??= pool();
  table = await freshOutbox(db, "pgrelay_health");
});

afterAll(async () => {
  await db.end();
});

const age = (seconds: number) =>
  db.query(`UPDATE pgrelay_health.outbox SET due_at = now() - make_interval(secs => $1)`, [
    seconds,
  ]);

describe("health", () => {
  it("is healthy when empty", async () => {
    const status = await assertOutboxHealthy(db, table);
    expect(status).toEqual({
      pending: 0,
      inFlight: 0,
      retrying: 0,
      dead: 0,
      scheduled: 0,
      oldestPending: null,
    });
  });

  it("names a stuck outbox that nothing has attempted — the relay is not running", async () => {
    await transaction(db, (tx) =>
      enqueue(
        tx,
        [
          { topic: "a", payload: 1 },
          { topic: "b", payload: 1 },
        ],
        table,
      ),
    );
    await age(15 * 60);
    const error = await assertOutboxHealthy(db, table).catch((e: unknown) => e);
    expect(error).toBeInstanceOf(OutboxUnhealthyError);
    expect((error as Error).message).toContain(
      "2 events pending; the oldest (a) has waited 15m 0s, over the 1m 0s limit and has never been attempted. Is a relay running against this table?",
    );
  });

  it("names the failing event, its error, and the key it is blocking", async () => {
    await transaction(db, (tx) =>
      enqueue(
        tx,
        [
          { topic: "invoice.paid", key: "acct_9", payload: 1 },
          { topic: "x", key: "acct_9", payload: 2 },
        ],
        table,
      ),
    );
    await new Relay(db, {
      ...table,
      onError: () => {},
      backoff: () => 60_000,
      handler: async () => {
        throw new Error("ECONNREFUSED 10.0.0.7:9092");
      },
    }).drain();
    await age(300);
    const status = await outboxStatus(db, table);
    expect(status).toMatchObject({ pending: 2, retrying: 1, dead: 0 });
    await expect(assertOutboxHealthy(db, { ...table, maxLagSeconds: 120 })).rejects.toThrow(
      "It has failed 1 time, most recently with: Error: ECONNREFUSED 10.0.0.7:9092 — and every later event for key acct_9 is waiting behind it",
    );
  });

  it("reports dead events, and retryDead puts them back in line", async () => {
    await transaction(db, (tx) => enqueue(tx, { topic: "t", payload: 1 }, table));
    await new Relay(db, {
      ...table,
      maxAttempts: 1,
      onError: () => {},
      handler: async () => {
        throw new Error("no");
      },
    }).drain();
    await expect(assertOutboxHealthy(db, table)).rejects.toThrow("1 event is dead");
    await expect(assertOutboxHealthy(db, { ...table, allowDead: true })).resolves.toMatchObject({
      dead: 1,
    });

    expect(await retryDead(db, table)).toBe(1);
    expect(await count(db, table, "dead_at IS NULL AND attempts = 0")).toBe(1);
    expect(await new Relay(db, { ...table, handler: async () => {} }).drain()).toBe(1);
  });

  it("retryDead can target specific events", async () => {
    const ids = await transaction(db, (tx) =>
      enqueue(
        tx,
        [
          { topic: "a", payload: 1 },
          { topic: "b", payload: 1 },
        ],
        table,
      ),
    );
    await db.query(`UPDATE pgrelay_health.outbox SET dead_at = now()`);
    expect(await retryDead(db, { ...table, ids: [ids[1]!] })).toBe(1);
    expect(await count(db, table, "dead_at IS NOT NULL")).toBe(1);
  });

  it("formats durations for humans", () => {
    expect([5, 65, 3700, 200_000].map(formatDuration)).toEqual(["5s", "1m 5s", "1h 1m", "2d 7h"]);
  });
});
