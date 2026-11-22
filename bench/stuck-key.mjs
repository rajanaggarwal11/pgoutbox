// What one stuck key costs every poll: the README's "A key stuck behind a retry" table.
//
//   pnpm build
//   BENCH_DATABASE_URL=postgres://postgres@localhost:5432/pgoutbox_bench node bench/stuck-key.mjs
//
// For each size, writes that many events for one key whose first event has failed and
// waits an hour to retry, then 1,000 ready events for other keys, and reports what the
// relay's claim and reap statements cost, from EXPLAIN (ANALYZE, BUFFERS). Creates and
// drops a schema named pgoutbox_bench.
import pg from "pg";
import { migrate, Relay } from "../dist/index.js";

const url = process.env.BENCH_DATABASE_URL ?? process.env.TEST_DATABASE_URL;
if (!url) {
  console.error("Set BENCH_DATABASE_URL to a Postgres you can create a schema in.");
  process.exit(2);
}
const table = { schema: "pgoutbox_bench", table: "outbox" };
const T = `"pgoutbox_bench"."outbox"`;
const db = new pg.Pool({ connectionString: url, max: 4 });

async function setUp(stuck) {
  await db.query(`DROP SCHEMA IF EXISTS pgoutbox_bench CASCADE`);
  await migrate(db, table);
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
     SELECT gen_random_uuid(), 't', 'k' || g, '{}' FROM generate_series(1, 1000) g`,
  );
  await db.query(`VACUUM ANALYZE ${T}`);
}

/** The relay's own claim and reap statements, captured from a real claim. */
async function capture() {
  const ran = [];
  const recording = {
    query: (text, params = []) => {
      ran.push({ text, params });
      // Run nothing that changes the table: answer the claim with no rows.
      return Promise.resolve({ rows: [], rowCount: 0 });
    },
  };
  await new Relay(recording, { ...table, concurrency: 50, handler: async () => {} }).drain();
  return {
    claim: ran.find((q) => q.text.startsWith("WITH candidates")),
    reap: ran.find((q) => q.text.includes("SET dead_at = now()")),
  };
}

async function explain({ text, params }) {
  const client = await db.connect();
  try {
    await client.query("BEGIN");
    // Warm the cache first, so the number is the steady state of a relay polling.
    await client.query("SAVEPOINT warm");
    await client.query(`EXPLAIN (ANALYZE) ${text}`, params);
    await client.query("ROLLBACK TO SAVEPOINT warm");
    const { rows } = await client.query(`EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ${text}`, params);
    const [result] = rows[0]["QUERY PLAN"];
    return {
      ms: result["Execution Time"],
      buffers: result.Plan["Shared Hit Blocks"] + result.Plan["Shared Read Blocks"],
    };
  } finally {
    await client.query("ROLLBACK");
    client.release();
  }
}

const { rows } = await db.query(`SELECT version() AS v`);
console.log(rows[0].v);
const statements = await capture();
for (const stuck of [2_000, 20_000, 200_000]) {
  await setUp(stuck);
  const claim = await explain(statements.claim);
  const reap = await explain(statements.reap);
  console.log(
    `${stuck.toLocaleString("en-US").padStart(8)} waiting: ` +
      `claim ${claim.ms.toFixed(1)} ms (${claim.buffers} buffers), ` +
      `reap ${reap.ms.toFixed(2)} ms (${reap.buffers} buffers)`,
  );
}
await db.query(`DROP SCHEMA IF EXISTS pgoutbox_bench CASCADE`);
await db.end();
