// Measures the numbers in the README's Performance section.
//
//   pnpm build
//   BENCH_DATABASE_URL=postgres://postgres@localhost:5432/pgoutbox_bench node bench/throughput.mjs
//
// The handler does nothing, so this measures pgoutbox and Postgres, not a broker. It
// creates and drops a schema named pgoutbox_bench; point it at a database you don't mind
// that happening in.
import pg from "pg";
import { enqueue, migrate, Relay } from "../dist/index.js";

const url = process.env.BENCH_DATABASE_URL ?? process.env.TEST_DATABASE_URL;
if (!url) {
  console.error("Set BENCH_DATABASE_URL to a Postgres you can create a schema in.");
  process.exit(2);
}
const table = { schema: "pgoutbox_bench", table: "outbox" };
const EVENTS = Number(process.env.BENCH_EVENTS ?? 50_000);
const admin = new pg.Pool({ connectionString: url, max: 4 });

async function reset() {
  await admin.query(`DROP SCHEMA IF EXISTS pgoutbox_bench CASCADE`);
  await migrate(admin, table);
  await admin.query(`CREATE TABLE pgoutbox_bench.orders (id bigint PRIMARY KEY, total int)`);
}

async function fill(n, key) {
  const client = await admin.connect();
  try {
    for (let i = 0; i < n; i += 1000) {
      await client.query("BEGIN");
      const batch = Array.from({ length: Math.min(1000, n - i) }, (_, j) => ({
        topic: "bench",
        key: key(i + j),
        payload: { i: i + j },
      }));
      await enqueue(client, batch, table);
      await client.query("COMMIT");
    }
  } finally {
    client.release();
  }
  await admin.query(`VACUUM ANALYZE pgoutbox_bench.outbox`);
}

async function enqueueRate(seconds = 5) {
  await reset();
  const client = await admin.connect();
  let n = 0;
  const until = performance.now() + seconds * 1000;
  try {
    while (performance.now() < until) {
      await client.query("BEGIN");
      await client.query("INSERT INTO pgoutbox_bench.orders VALUES ($1, 1)", [n]);
      await enqueue(client, { topic: "order.created", key: String(n), payload: { n } }, table);
      await client.query("COMMIT");
      n++;
    }
  } finally {
    client.release();
  }
  return n / seconds;
}

async function relayRate(relays, key) {
  await reset();
  await fill(EVENTS, key);
  const pools = Array.from(
    { length: relays },
    () => new pg.Pool({ connectionString: url, max: 20 }),
  );
  let handled = 0;
  let done;
  const finished = new Promise((resolve) => (done = resolve));
  const workers = pools.map(
    (pool) =>
      new Relay(pool, {
        ...table,
        concurrency: 50,
        onError: (error) => console.error(error),
        handler: async () => {
          if (++handled === EVENTS) done();
        },
      }),
  );
  const start = performance.now();
  for (const w of workers) w.start();
  await finished;
  await Promise.all(workers.map((w) => w.stop()));
  const elapsed = (performance.now() - start) / 1000;
  await Promise.all(pools.map((p) => p.end()));
  const { rows } = await admin.query(
    `SELECT count(*)::int AS n FROM pgoutbox_bench.outbox WHERE delivered_at IS NULL`,
  );
  if (rows[0].n !== 0) throw new Error(`${rows[0].n} events not marked delivered`);
  return EVENTS / elapsed;
}

const fmt = (n) => `~${(Math.round(n / 100) * 100).toLocaleString("en-US")}`;
const { rows } = await admin.query(`SELECT version() AS v`);
console.log(rows[0].v);
console.log(`enqueue, one connection:              ${fmt(await enqueueRate())} transactions/s`);
console.log(
  `1 relay, concurrency 50, no keys:     ${fmt(await relayRate(1, () => null))} events/s`,
);
console.log(
  `1 relay, concurrency 50, 1,000 keys:  ${fmt(await relayRate(1, (i) => `k${i % 1000}`))} events/s`,
);
console.log(
  `4 relays, concurrency 50, 1,000 keys: ${fmt(await relayRate(4, (i) => `k${i % 1000}`))} events/s`,
);
await admin.query(`DROP SCHEMA IF EXISTS pgoutbox_bench CASCADE`);
await admin.end();
