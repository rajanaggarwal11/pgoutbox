// A relay process for test/crash.test.ts. It appends every delivery to a log file with
// a synchronous write, so the log is exactly what was handed to "the broker" before
// the process was killed — including deliveries that were never recorded in Postgres.
import { appendFileSync } from "node:fs";
import pg from "pg";
import { Relay } from "../../dist/index.js";

const [url, schema, logFile] = process.argv.slice(2);
const db = new pg.Pool({ connectionString: url, max: 10 });

const relay = new Relay(db, {
  schema,
  table: "outbox",
  concurrency: 8,
  pollIntervalMs: 10,
  leaseMs: 400,
  onError: () => {},
  handler: async (event) => {
    await new Promise((r) => setTimeout(r, 3 + Math.random() * 9));
    appendFileSync(logFile, `${event.id} ${event.key ?? "-"} ${event.payload.seq ?? -1}\n`);
  },
});
relay.start();
process.on("SIGTERM", async () => {
  await relay.stop();
  await db.end();
  process.exit(0);
});
