#!/usr/bin/env node
import { readFileSync, realpathSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import type * as PgModule from "pg";
import { formatDuration, healthProblems, outboxStatus, type OutboxStatus } from "./health.js";
import { outboxSql } from "./sql.js";
import type { TableOptions } from "./types.js";

/** 0 healthy · 1 the outbox is stuck or has dead events · 2 could not run. */
const EXIT_OK = 0;
const EXIT_FINDINGS = 1;
const EXIT_ERROR = 2;

const useColor = () =>
  !process.env.NO_COLOR && process.env.FORCE_COLOR !== "0" && process.stdout.isTTY;
const wrap = (open: number, close: number) => (s: string) =>
  useColor() ? `\u001b[${open}m${s}\u001b[${close}m` : s;
const bold = wrap(1, 22);
const red = wrap(31, 39);
const green = wrap(32, 39);
const dim = wrap(2, 22);

function version(): string {
  try {
    const raw = readFileSync(new URL("../package.json", import.meta.url), "utf8");
    return (JSON.parse(raw) as { version?: string }).version ?? "0.0.0";
  } catch {
    return "0.0.0";
  }
}

const HELP = `
pgrelay — a transactional outbox for Postgres

Usage
  pgrelay sql [options]                      Print the outbox DDL, for a migration file
  pgrelay status [<connection-url>] [options]  Is anything stuck? Exit 1 if so

  The URL may also come from DATABASE_URL, which keeps it out of your shell history.

Options
  --schema <name>       Schema of the outbox table. Default: public
  --table <name>        Name of the outbox table. Default: pgrelay_outbox
  --max-lag <seconds>   status: how long an event may wait before it counts as stuck. Default: 60
  --allow-dead          status: don't fail on dead events
  --json                status: machine-readable output
  -v, --version         Print the version
  -h, --help            Print this

Exit codes
  0  healthy (or: printed the SQL)
  1  an event has waited longer than --max-lag, or an event is dead
  2  could not run — bad option, no URL, no driver, connection failed

Examples
  npx pgrelay sql > migrations/0007_outbox.sql
  DATABASE_URL=postgres://app@db/prod npx pgrelay status --max-lag 300
`;

class UsageError extends Error {}

/** "db on host" for a header, without credentials, and without failing on a key=value string. */
function describeTarget(connection: string): string {
  try {
    const u = new URL(connection);
    return `${u.pathname.slice(1) || "postgres"} on ${u.hostname || "localhost"}`;
  } catch {
    const db = /dbname=(\S+)/.exec(connection)?.[1];
    const host = /host=(\S+)/.exec(connection)?.[1];
    return `${db ?? "postgres"} on ${host ?? "localhost"}`;
  }
}

async function connect(url: string) {
  let pg: typeof PgModule & { default?: typeof PgModule };
  try {
    pg = (await import("pg")) as typeof pg;
  } catch {
    throw new UsageError("pgrelay status needs the pg driver: npm install pg");
  }
  // pg is CommonJS; under ESM its exports arrive on `default`.
  const Client = pg.Client ?? pg.default!.Client;
  const client = new Client({ connectionString: url });
  await client.connect();
  return client;
}

function render(status: OutboxStatus, problems: string[], where: string): string {
  const lines = [`${bold("pgrelay status")} ${dim(`· ${where}`)}`, ""];
  const o = status.oldestPending;
  lines.push(
    `  pending   ${status.pending.toLocaleString("en-US")}${o ? dim(`  (oldest waited ${formatDuration(o.ageSeconds)})`) : ""}`,
    `  in flight ${status.inFlight.toLocaleString("en-US")}`,
    `  retrying  ${status.retrying.toLocaleString("en-US")}`,
    `  dead      ${status.dead.toLocaleString("en-US")}`,
    `  scheduled ${status.scheduled.toLocaleString("en-US")}${dim("  (delayed, not yet due)")}`,
    "",
  );
  if (problems.length === 0) lines.push(green("✓ The outbox is healthy."));
  else for (const p of problems) lines.push(red(`✗ ${p}`));
  return lines.join("\n") + "\n";
}

export async function main(argv: string[]): Promise<number> {
  let parsed;
  try {
    parsed = parseArgs({
      args: argv,
      allowPositionals: true,
      options: {
        schema: { type: "string" },
        table: { type: "string" },
        "max-lag": { type: "string" },
        "allow-dead": { type: "boolean" },
        json: { type: "boolean" },
        version: { type: "boolean", short: "v" },
        help: { type: "boolean", short: "h" },
      },
    });
  } catch (error) {
    process.stderr.write(`pgrelay: ${(error as Error).message}\n${HELP}`);
    return EXIT_ERROR;
  }
  const { values, positionals } = parsed;
  if (values.version) {
    process.stdout.write(`${version()}\n`);
    return EXIT_OK;
  }
  const [command, url] = positionals;
  if (values.help || !command) {
    process.stdout.write(HELP);
    return values.help ? EXIT_OK : EXIT_ERROR;
  }
  const table: TableOptions = {};
  if (values.schema) table.schema = values.schema;
  if (values.table) table.table = values.table;

  try {
    if (command === "sql") {
      process.stdout.write(outboxSql(table));
      return EXIT_OK;
    }
    if (command !== "status")
      throw new UsageError(`unknown command "${command}". Try: pgrelay --help`);

    const rawLag = values["max-lag"];
    const maxLag = rawLag === undefined ? 60 : rawLag.trim() === "" ? NaN : Number(rawLag);
    if (!Number.isFinite(maxLag) || maxLag < 0)
      throw new UsageError("--max-lag takes a number of seconds");
    const connectionUrl = url ?? process.env.DATABASE_URL;
    if (!connectionUrl) throw new UsageError("pass a connection URL or set DATABASE_URL");

    const client = await connect(connectionUrl);
    try {
      const status = await outboxStatus(client, table);
      const problems = healthProblems(status, {
        maxLagSeconds: maxLag,
        allowDead: values["allow-dead"] ?? false,
      });
      if (values.json) {
        process.stdout.write(
          JSON.stringify(
            { version: 1, healthy: problems.length === 0, problems, ...status },
            null,
            2,
          ) + "\n",
        );
      } else {
        process.stdout.write(render(status, problems, describeTarget(connectionUrl)));
      }
      return problems.length === 0 ? EXIT_OK : EXIT_FINDINGS;
    } finally {
      await client.end();
    }
  } catch (error) {
    const message =
      error instanceof UsageError ? error.message : `could not run: ${(error as Error).message}`;
    process.stderr.write(`pgrelay: ${message}\n`);
    return EXIT_ERROR;
  }
}

// Run only when executed directly (including through npm's bin symlink), not when imported.
const self = fileURLToPath(import.meta.url);
const invoked = process.argv[1] ? realpathSync(process.argv[1]) : "";
if (invoked === realpathSync(self)) {
  main(process.argv.slice(2)).then((code) => {
    process.exitCode = code;
  });
}
