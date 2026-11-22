import type pg from "pg";
import { afterAll, beforeEach, describe, expect, it } from "vitest";
import { outboxStatus } from "../src/health.js";
import { Relay } from "../src/relay.js";
import type { SqlExecutor, TableOptions } from "../src/types.js";
import { freshOutbox, pool } from "./helpers.js";

// How much work the queries pgoutbox runs on every poll do, measured as the buffers
// EXPLAIN (ANALYZE, BUFFERS) reports, against tables shaped like the bad days they have
// to survive. Buffer counts don't depend on machine speed, so these don't flake on a
// slow runner the way a timing would.

let db: pg.Pool;
let table: TableOptions;
const T = `"pgoutbox_plans"."outbox"`;

beforeEach(async () => {
  db ??= pool();
  table = await freshOutbox(db, "pgoutbox_plans");
});

afterAll(async () => {
  await db.end();
});

interface Captured {
  text: string;
  params: readonly unknown[];
}

/** An executor that runs everything on the pool and remembers what it ran. */
function recording(): { executor: SqlExecutor; ran: Captured[] } {
  const ran: Captured[] = [];
  return {
    ran,
    executor: {
      query: (text, params = []) => {
        ran.push({ text, params });
        return db.query(text, params as unknown[]) as never;
      },
    },
  };
}

/** Shared buffers (hit + read) the statement touches, run and rolled back. */
async function buffers({ text, params }: Captured): Promise<number> {
  const client = await db.connect();
  try {
    await client.query("BEGIN");
    const { rows } = await client.query<{ "QUERY PLAN": [{ Plan: Record<string, number> }] }>(
      `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ${text}`,
      params as unknown[],
    );
    const plan = rows[0]!["QUERY PLAN"][0].Plan;
    return plan["Shared Hit Blocks"]! + plan["Shared Read Blocks"]!;
  } finally {
    await client.query("ROLLBACK");
    client.release();
  }
}

describe("query cost", () => {
  it("a key stuck behind a retry costs each claim a linear pass, not a quadratic one", async () => {
    // 10,000 events for one key whose first event failed and waits an hour to retry,
    // then 20 events for other keys that are ready to go.
    const stuck = 10_000;
    await db.query(
      `INSERT INTO ${T} (event_id, topic, key, payload)
       SELECT gen_random_uuid(), 't', 'acct_stuck', '{}' FROM generate_series(1, $1)`,
      [stuck],
    );
    await db.query(
      `UPDATE ${T} SET attempts = 1, available_at = now() + interval '1 hour'
       WHERE id = (SELECT min(id) FROM ${T})`,
    );
    await db.query(
      `INSERT INTO ${T} (event_id, topic, key, payload)
       SELECT gen_random_uuid(), 't', 'k' || g, '{}' FROM generate_series(1, 20) g`,
    );
    await db.query(`ANALYZE ${T}`);

    const { executor, ran } = recording();
    const delivered = await new Relay(executor, {
      ...table,
      onError: () => {},
      handler: async () => {},
    }).drain();
    expect(delivered).toBe(20); // the other keys are not held up

    const claim = ran.find((q) => q.text.startsWith("WITH candidates"))!;
    const reap = ran.find((q) => q.text.includes("SET dead_at = now()"))!;
    await db.query(`UPDATE ${T} SET delivered_at = NULL, attempts = 0 WHERE key <> 'acct_stuck'`);

    // Before: the claim's key check was a BitmapOr over every lower row of the key, for
    // every row of the key — 292,000 buffers and 2.5 s here, 1.08 million and 11 s at
    // 20,000. Now it is about one heap visit and one index probe per row it skips.
    expect(await buffers(claim)).toBeLessThan(3 * stuck);
    // Before: a sequential scan of the whole table, every second, in every relay.
    expect(await buffers(reap)).toBeLessThan(20);
  });

  it("outboxStatus does not read the delivered rows kept for inspection", async () => {
    await db.query(
      `INSERT INTO ${T} (event_id, topic, payload, attempts, delivered_at)
       SELECT gen_random_uuid(), 't', '{}', 1, now() FROM generate_series(1, 20000)`,
    );
    await db.query(
      `INSERT INTO ${T} (event_id, topic, payload, attempts, dead_at)
       SELECT gen_random_uuid(), 't', '{}', 10, now() FROM generate_series(1, 3)`,
    );
    await db.query(
      `INSERT INTO ${T} (event_id, topic, payload)
       SELECT gen_random_uuid(), 't', '{}' FROM generate_series(1, 5)`,
    );
    await db.query(`ANALYZE ${T}`);

    const { executor, ran } = recording();
    const status = await outboxStatus(executor, table);
    expect(status).toMatchObject({ pending: 5, dead: 3 });

    // Before: `WHERE delivered_at IS NULL` matched no index, so every call read all
    // 20,000 delivered rows (about 300 buffers here; 7,648 at 500,000 rows).
    for (const query of ran) expect(await buffers(query)).toBeLessThan(30);
  });
});
