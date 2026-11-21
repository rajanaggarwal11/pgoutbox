import type pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { enqueue } from "../src/enqueue.js";
import { Relay } from "../src/relay.js";
import type { TableOptions } from "../src/types.js";
import { freshOutbox, pool, sleep, transaction } from "./helpers.js";

let db: pg.Pool;
let table: TableOptions;

beforeAll(async () => {
  db = pool(40);
  table = await freshOutbox(db, "pgrelay_ordering");
});

afterAll(async () => {
  await db.end();
});

describe("ordering across concurrent relays", () => {
  it("delivers every event exactly once and each key in commit order, with four relays racing", async () => {
    const KEYS = 25;
    const PER_KEY = 40;

    // Writers: each key's events are committed one transaction at a time, so commit
    // order per key is the sequence number. Keys are interleaved across writers.
    await Promise.all(
      Array.from({ length: 5 }, async (_, w) => {
        for (let seq = 0; seq < PER_KEY; seq++) {
          await transaction(db, (tx) =>
            enqueue(
              tx,
              Array.from({ length: KEYS / 5 }, (_, j) => ({
                topic: "seq",
                key: `k${w * (KEYS / 5) + j}`,
                payload: { seq },
              })),
              table,
            ),
          );
        }
      }),
    );

    // Plus unkeyed events, which may go in any order and in parallel.
    await transaction(db, (tx) =>
      enqueue(
        tx,
        Array.from({ length: 200 }, (_, i) => ({ topic: "free", payload: { i } })),
        table,
      ),
    );

    const log: { key: string | null; seq: number; relay: number }[] = [];
    const active = new Map<string, number>();
    let overlaps = 0;
    const errors: unknown[] = [];

    const relays = Array.from(
      { length: 4 },
      (_, r) =>
        new Relay(db, {
          ...table,
          concurrency: 16,
          pollIntervalMs: 5,
          onError: (e) => void errors.push(e),
          handler: async (event) => {
            const key = event.key;
            if (key) {
              // Two handlers for the same key at the same time is an ordering bug even
              // if the log happens to come out sorted.
              if (active.get(key)) overlaps++;
              active.set(key, 1);
            }
            await sleep(Math.random() * 3);
            log.push({ key, seq: (event.payload as { seq?: number }).seq ?? -1, relay: r });
            if (key) active.set(key, 0);
          },
        }),
    );
    relays.forEach((r) => r.start());

    const total = KEYS * PER_KEY + 200;
    for (let i = 0; i < 2000 && log.length < total; i++) await sleep(10);
    await Promise.all(relays.map((r) => r.stop()));

    expect(log).toHaveLength(total);
    expect(overlaps).toBe(0);
    expect(errors).toEqual([]);
    for (let k = 0; k < KEYS; k++) {
      const seqs = log.filter((e) => e.key === `k${k}`).map((e) => e.seq);
      expect(seqs, `key k${k}`).toEqual(Array.from({ length: PER_KEY }, (_, i) => i));
    }
    // The work really was shared.
    expect(new Set(log.map((e) => e.relay)).size).toBe(4);
  });
});
