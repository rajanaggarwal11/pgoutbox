# pgrelay

**A transactional outbox for Postgres.** Events commit with your data or not at all, are never skipped by a transaction that commits late, stay in order per key across any number of relays — and the test suite kills relays with `kill -9` mid-delivery to show nothing is lost.

[![CI](https://github.com/rajanaggarwal11/pgrelay/actions/workflows/ci.yml/badge.svg)](https://github.com/rajanaggarwal11/pgrelay/actions/workflows/ci.yml)
[![CodeQL](https://github.com/rajanaggarwal11/pgrelay/actions/workflows/codeql.yml/badge.svg)](https://github.com/rajanaggarwal11/pgrelay/actions/workflows/codeql.yml)
[![license](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)

```ts
import { enqueue, Relay } from "pgrelay";

// In the transaction that changes your data
await client.query("BEGIN");
await client.query("INSERT INTO orders (id, total) VALUES ($1, $2)", [order.id, order.total]);
await enqueue(client, { topic: "order.created", key: order.id, payload: order });
await client.query("COMMIT");

// Anywhere — one process or twenty
new Relay(pool, {
  handler: async (event, { signal }) => {
    await fetch(WEBHOOK_URL, { method: "POST", body: JSON.stringify(event), signal });
  },
}).start();
```

```
kill -9 × 24: 2200 events, 149 taken over from a killed relay, 4 delivered twice, 0 lost, 40 keys in order
```

## Why an outbox

You save an order, then publish `order.created`. Between those two steps the process can die, the broker can be down, or the transaction can roll back after the publish went out. Every one of those fails silently: a customer with no confirmation email, a warehouse shipping an order that doesn't exist. An outbox writes the event **into the same transaction** as the order, and a relay delivers it after commit, so the event exists if and only if the order does.

The idea is simple. The ways to get it subtly wrong are not, and each one is silent:

| What goes wrong                                                                                                                                                                                                                     | What pgrelay does                                                                                                          | Proved by                                                              |
| ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| `enqueue` is called with a pool instead of the transaction's client, so the event commits on its own — the dual write is back, and every test that doesn't roll back passes                                                         | Refuses, with `OutsideTransactionError`, **having written nothing**. Checked by Postgres, not by trusting the driver       | `test/enqueue.test.ts`, `test/adapters.test.ts`                        |
| A poller remembers the last id it saw. Ids are handed out at insert, not at commit, so a transaction that commits late has an id the poller has already moved past. That event is never delivered                                   | No cursor. The relay asks which rows are still undelivered, so a late commit is just undelivered until it is delivered     | `test/relay.test.ts` — "no cursor, no gap"                             |
| Several relays share the work and two of them deliver `order.paid` and `order.shipped` for the same order at the same time, in the wrong order — or a transaction that commits late slips its event in beside one already in flight | One event per key at a time, across every relay, enforced by a unique index in Postgres; a late commit waits its turn      | `test/ordering.test.ts` — 4 relays, 1,200 events; `test/edges.test.ts` |
| A relay dies between publishing and recording it                                                                                                                                                                                    | Leases, not locks: the event is retried once the lease runs out. At-least-once, with a stable `event.id` to deduplicate on | `test/crash.test.ts` — real processes, `kill -9`                       |
| The relay stops, or a handler fails on every attempt. Nothing errors; the table just grows                                                                                                                                          | `assertOutboxHealthy` and `npx pgrelay status` name the stuck event, its last error, and the key it is blocking            | `test/health.test.ts`, `test/cli.test.ts`                              |

## Install

```bash
npm install pgrelay
```

Works with [node-postgres](https://node-postgres.com), [postgres.js](https://github.com/porsager/postgres) and [Drizzle](https://orm.drizzle.team). Postgres 14 or later (CI runs 14–18), Node 22.13 or later.

Create the table once — on boot, or in a migration:

```ts
import { migrate } from "pgrelay";

await migrate(pool); // CREATE TABLE IF NOT EXISTS public.pgrelay_outbox …
```

`migrate` is safe to run from every process at once: it takes a transaction-scoped advisory lock, so ten processes booting together create the table once instead of failing on a duplicate catalog entry.

```bash
npx pgrelay sql > migrations/0007_outbox.sql
```

## Writing events

```ts
import { enqueue } from "pgrelay";

const client = await pool.connect();
try {
  await client.query("BEGIN");
  await client.query("UPDATE accounts SET plan = 'pro' WHERE id = $1", [accountId]);
  const eventId = await enqueue(client, {
    topic: "account.upgraded",
    key: accountId, // events for one account are delivered in order
    payload: { accountId, plan: "pro" },
    headers: { traceId }, // optional string metadata
  });
  await client.query("COMMIT");
} finally {
  client.release();
}
```

`enqueue` takes one event or an array (one statement either way) and returns the event ids. Pass `id` to make a write idempotent (for as long as the row is kept — see `keepDeliveredMs`), and `delayMs` to hold an event back after it is written.

**postgres.js**

```ts
import postgres from "postgres";
import { enqueue, migrate, Relay } from "pgrelay";
import { withPostgresJs } from "pgrelay/postgres-js";

const sql = postgres(process.env.DATABASE_URL!);
await migrate(withPostgresJs(sql));

await sql.begin(async (tx) => {
  await tx`INSERT INTO orders ${tx(order)}`;
  await enqueue(withPostgresJs(tx), { topic: "order.created", key: order.id, payload: order });
});

// The relay gets the top-level sql: it runs each statement on its own.
new Relay(withPostgresJs(sql), { handler: async (event) => publish(event) }).start();
```

**Drizzle** (over node-postgres or postgres.js)

```ts
import { drizzle } from "drizzle-orm/node-postgres";
import { enqueue, migrate, Relay } from "pgrelay";
import { withDrizzle } from "pgrelay/drizzle";

const db = drizzle(process.env.DATABASE_URL!);
await migrate(withDrizzle(db));

await db.transaction(async (tx) => {
  await tx.insert(orders).values(order);
  await enqueue(withDrizzle(tx), { topic: "order.created", key: order.id, payload: order });
});

// The relay gets db, not a transaction.
new Relay(withDrizzle(db), { handler: async (event) => publish(event) }).start();
```

Pass the transaction, never the pool or `db`. If you get it wrong:

```
OutsideTransactionError: pgrelay: enqueue() was called outside a transaction, so the event would
commit on its own whether or not your other writes do. Nothing was written. Call it with the client
or transaction object your BEGIN … COMMIT runs on (pool.connect() + BEGIN, sql.begin(),
db.transaction()), not with a pool.
```

## Delivering events

```ts
import { Relay } from "pgrelay";

const relay = new Relay(pool, {
  handler: async (event, { signal }) => {
    // event: { id, topic, key, payload, headers, createdAt, attempt }
    await producer.send({
      topic: event.topic,
      key: event.key,
      value: JSON.stringify(event.payload),
    });
  },
});
relay.start();
process.on("SIGTERM", () => relay.stop()); // finishes what is in flight, claims nothing new
```

Resolve to mark an event delivered; throw to retry it with exponential backoff. Run as many relays as you like, in as many processes as you like; they coordinate through the table and nothing else.

| Option              | Default                            | What it does                                                                                                                                  |
| ------------------- | ---------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `concurrency`       | `10`                               | Events in flight at once, per relay                                                                                                           |
| `leaseMs`           | `30000`                            | How long a claimed event belongs to this relay. If the process dies, another relay takes over after this                                      |
| `maxAttempts`       | `10`                               | Attempts before an event is marked dead and set aside. A claim whose relay died counts, so an event that crashes every relay is set aside too |
| `backoff`           | 0.5–1 s, doubling, capped at 5 min | `(attempt) => ms` before the next attempt                                                                                                     |
| `pollIntervalMs`    | `1000`                             | Idle wait between polls, at least 10 ms. `relay.wake()` cuts it short                                                                         |
| `keepDeliveredMs`   | 24 h                               | Delivered rows are kept this long for inspection, then deleted. `0` deletes on delivery                                                       |
| `cleanupIntervalMs` | `60000`                            | How often delivered rows past `keepDeliveredMs` are deleted                                                                                   |
| `schema`, `table`   | `public`, `pgrelay_outbox`         | Where the outbox lives                                                                                                                        |
| `onError`           | `console.error`                    | Handler failures and database errors. The relay keeps running                                                                                 |

For tests, scripts and serverless functions, `await relay.drain()` delivers everything due and returns the count.

### What you can rely on

- **Only committed events are delivered.** A rolled-back transaction's events never existed.
- **At least once.** A relay that dies after your handler succeeded but before the row was marked means the event is delivered again. Deduplicate on `event.id`, which is the same on every attempt.
- **One event per key at a time.** Once an event for a key has been started, no other event for that key is handed to any relay until it is delivered or dead — an event waiting to retry holds its key; a dead one releases it. A unique index enforces this inside Postgres, so no interleaving of relays can break it. Unkeyed events go in parallel.
- **Per-key order.** Events for a key go in the order they were written. A transaction that commits after a later event for the same key has started waits for that event to finish, so delivery follows commit order. If two writers for one key really do overlap (most don't: they update the same aggregate row, which serialises them), they are delivered one at a time in the order Postgres numbered them.
- **No long transactions.** Claims are one short statement with a lease. No transaction stays open while your handler talks to the network, so a slow broker cannot hold back vacuum, and it works behind PgBouncer in transaction mode.
- **`signal`** aborts shortly before the lease runs out. Pass it to `fetch` or your client, so a hung call stops before another relay picks the event up.

## Knowing when it's stuck

An outbox doesn't fail loudly. It fills up. So pgrelay will tell you:

```ts
await assertOutboxHealthy(pool, { maxLagSeconds: 120 }); // in a health check or a cron job
```

```bash
DATABASE_URL=postgres://app@db/prod npx pgrelay status
```

```
pgrelay status · prod on db.internal

  pending   172  (oldest waited 14m 3s)
  in flight 0
  retrying  1
  dead      0
  scheduled 0  (delayed, not yet due)

✗ 172 events pending; the oldest (invoice.paid, key acct_42) has waited 14m 3s, over the 1m 0s limit.
  It has failed 1 time, most recently with: Error: POST https://hooks.example.com/billing: 503 Service
  Unavailable — and every later event for key acct_42 is waiting behind it
```

Exit codes: `0` healthy, `1` stuck or dead events, `2` could not run. `--json` for machines. When the cause is fixed, `retryDead(pool)` puts dead events back in line.

## Performance

Measured with [`bench/`](./bench) (`BENCH_DATABASE_URL=… pnpm bench`) and a handler that does nothing, so this measures pgrelay and Postgres, not a broker.

Machine: 13-inch MacBook Pro (Mac14,7), Apple M2, 8 cores, 8 GB, macOS 26.6, Node 24.4. Postgres 18.4 on the same machine, from the EDB installer, which is an x86_64 build running under Rosetta 2. Settings are `initdb` defaults: `fsync` and `synchronous_commit` on, `shared_buffers` 128 MB, `wal_sync_method` `open_datasync` (on macOS this does not flush the drive's own write cache; `fsync_writethrough` does). The machine was not idle while measuring (load average 5–15 from other work) and runs varied widely, so each row shows the median and the range of five runs:

|                                                                    | Median                | Range of 5 runs |
| ------------------------------------------------------------------ | --------------------- | --------------- |
| `enqueue`, one event, after the transaction's other writes, 1 conn | ~2,000 transactions/s | 1,600–2,600     |
| One relay, `concurrency: 50`, no keys                              | ~7,800 events/s       | 4,900–10,400    |
| One relay, `concurrency: 50`, 1,000 keys                           | ~7,800 events/s       | 5,200–9,000     |
| Four relays, `concurrency: 50` each, 1,000 keys                    | ~8,800 events/s       | 6,500–11,100    |

**A key stuck behind a retry.** While a key's first event waits to retry, every later event for that key waits behind it, and each claim passes over them once. Cost per claim (`concurrency: 50`) with 1,000 ready events for other keys queued after the stuck ones, from `EXPLAIN (ANALYZE, BUFFERS)`:

| Events waiting behind the stuck key | Claim   | Reaping abandoned leases |
| ----------------------------------- | ------- | ------------------------ |
| 2,000                               | 1.9 ms  | 0.01 ms                  |
| 20,000                              | 6.5 ms  | 0.01 ms                  |
| 200,000                             | 56.8 ms | 0.01 ms                  |

The cost grows linearly with that backlog, about 0.3 µs per waiting event. `assertOutboxHealthy` flags the key as soon as its oldest event has waited longer than `maxLagSeconds`, with the key and its last error.

## Why not pg-boss, graphile-worker, Debezium or @nestjs/outbox?

If you already run one of them, you may not need pgrelay.

- **[pg-boss](https://github.com/timgit/pg-boss)** and **[graphile-worker](https://github.com/graphile/worker)** are job queues: a row is work for a worker to do, and they come with what job queues need, such as scheduling, cron and priorities. Both can add a job inside your transaction, which gives you most of an outbox. Per-key order is opt-in: pg-boss has a `key_strict_fifo` queue policy, and graphile-worker runs jobs that share a named queue one at a time, but advises against high-cardinality queue names such as ids. pgrelay records events, facts that already happened, and orders every keyed event by default, with keys meant to be aggregate ids: one per order, account or user.
- **[Debezium](https://debezium.io)** reads the write-ahead log through logical decoding and a replication slot, and its outbox event router turns outbox rows into Kafka messages keyed by aggregate id. Nothing polls, and if you run Kafka Connect already it is the established choice. It needs that infrastructure, and the replication slot keeps WAL on the server while the connector is down. pgrelay is a library in your Node process and delivers to whatever your handler calls.
- **[@nestjs/outbox](https://github.com/nestjs/outbox)** is a transactional outbox built as a Nest module, with Postgres and MySQL stores and an inbox for deduplication. On NestJS it fits right in. pgrelay doesn't need a framework; it takes a `pg` client, postgres.js or Drizzle.

What pgrelay adds is that its guarantees are tests against a real Postgres: no event skipped by a late commit, one event per key at a time enforced by a unique index, and nothing lost when relays are killed with `kill -9` mid-delivery. The table at the top lists them.

## Design notes

- **No cursor.** Identity values are assigned when a row is inserted, not when its transaction commits, so `WHERE id > $last` is a race that loses events quietly and forever. pgrelay keeps state per row (`delivered_at`) and asks for undelivered rows, using a partial index that only contains them.
- **Leases, not held locks.** `FOR UPDATE SKIP LOCKED` picks the rows, a lease records the claim, and the statement commits. A dead relay's events become claimable when the lease runs out; a live one's never get double-claimed.
- **Keys serialise without coordination.** A started event holds its key; an unstarted one goes only when nothing for its key is started and nothing earlier is undelivered. The claim query applies that rule, and a partial unique index over "the started, unfinished event for this key" makes it a constraint: if two claims race on snapshots, one gets a unique violation and looks again. No locks held between statements, no coordinator, nothing but the table.
- **The transaction check costs nothing in the common case.** The insert checks `pg_current_xact_id_if_assigned()` in the same statement: if your transaction has already written something, it is a transaction. Only if `enqueue` is the first write does pgrelay spend two more round trips: it gives the transaction an id and asks again. In a transaction block the id sticks; in autocommit it belonged to a statement that is already over. The same check decides both times, and nothing is ever forced through. (`statement_timestamp() <> transaction_timestamp()` looks like a cheaper test and is wrong over the extended query protocol, which every driver uses for parameterised queries.)
- **No `LISTEN/NOTIFY` by default.** `NOTIFY` serialises the commits of every transaction that sends one, which is a poor trade on your busiest write path. Polling an index of only undelivered rows is cheap; call `relay.wake()` when you know there is work.

## Reviewed adversarially

Before the first release, an independent review tried to break each guarantee against a real Postgres. It found a late-commit ordering hole, false "stuck" alarms for delayed events, and an `onError` that could crash the process. All are fixed, and each reproduction lives on as a regression test in [`test/edges.test.ts`](./test/edges.test.ts).

A second audit, also before the first release, measured the queries every relay runs on every poll. The claim query was quadratic in a stuck key's backlog: 20,000 events behind one retrying event made each claim take 11 s. Reaping abandoned leases and `outboxStatus` scanned the whole table, including the delivered rows kept for inspection. And `migrate`, run from ten processes at once on a fresh schema, failed 138 times in 200 with a unique violation on a catalog table. All are fixed; [`test/plans.test.ts`](./test/plans.test.ts) checks the query costs and [`test/migrate.test.ts`](./test/migrate.test.ts) the concurrent boot.

## Development

```bash
pnpm install
TEST_DATABASE_URL=postgres://postgres@localhost:5432/pgrelay_test pnpm check
```

The tests need a real Postgres; there are no mocks. See [CONTRIBUTING.md](./CONTRIBUTING.md).

## License

MIT © Rajan Aggarwal
