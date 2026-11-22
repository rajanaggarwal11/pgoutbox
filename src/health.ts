import { migrateLockKey, outboxIndexes, outboxSql, resolveTable } from "./sql.js";
import type { SqlExecutor, TableOptions } from "./types.js";

/**
 * Creates the outbox table and its indexes if they don't exist. Safe to run on every
 * boot, from any number of processes at once.
 *
 * When everything already exists — every boot after the first — it only looks, with
 * one catalog query that takes no lock on the outbox, so a boot never waits behind (or
 * holds up) transactions writing to it. `CREATE INDEX IF NOT EXISTS` would take
 * a SHARE lock on the table before finding the index there, queueing behind any open
 * transaction that has enqueued and making every later enqueue and claim queue behind it.
 *
 * When something is missing, it creates it. `IF NOT EXISTS` alone is not safe for that:
 * two sessions that both find the schema, table or index missing both try to create it,
 * and the slower one fails with a unique violation on `pg_namespace`, `pg_type` or
 * `pg_class`. So the DDL runs behind a transaction-scoped advisory lock: one process
 * creates, the rest wait for it and find everything there. The script is sent as one
 * simple-protocol query, which Postgres runs as one implicit transaction, so the lock is
 * released when the DDL commits — or, if `db` is already inside your transaction, when
 * that commits. Building an index on a large existing table blocks writes to it while it
 * builds; see the README for creating them `CONCURRENTLY` instead.
 */
export async function migrate(db: SqlExecutor, options: TableOptions = {}): Promise<void> {
  const { schema, table } = resolveTable(options);
  const relnames = [table, ...outboxIndexes(options).map((index) => index.relname)];
  // A plain catalog query rather than to_regclass(): a lookup of a schema that does not
  // exist yet would leave a negative entry in this connection's catalog cache, and the
  // CREATE SCHEMA IF NOT EXISTS below could then trust it.
  const { rows } = await db.query<{ found: number }>(
    `SELECT count(*)::int AS found
     FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = $1 AND c.relname IN (SELECT jsonb_array_elements_text($2::text::jsonb))`,
    [schema, JSON.stringify(relnames)],
  );
  if (Number(rows[0]?.found) === relnames.length) return;
  // The advisory lock is not a relation lock, so taking it does not make this transaction
  // process the catalog changes other sessions committed while it waited. Opening
  // pg_namespace does — Postgres catches up on invalidations whenever a transaction first
  // locks a relation — so the IF NOT EXISTS checks below see what the process before this
  // one created, instead of a cached "not there".
  await db.query(
    `SELECT pg_advisory_xact_lock(${migrateLockKey(options)});
SELECT FROM pg_catalog.pg_namespace LIMIT 0;
${outboxSql(options)}`,
  );
}

export interface OldestPending {
  id: string;
  topic: string;
  key: string | null;
  attempts: number;
  lastError: string | null;
  ageSeconds: number;
}

export interface OutboxStatus {
  /** Committed and not yet delivered (and not dead). */
  pending: number;
  /** Pending and currently leased by a relay. */
  inFlight: number;
  /** Pending, failed at least once, waiting for the next attempt. */
  retrying: number;
  /** Gave up after `maxAttempts`. Set aside until you `retryDead` them. */
  dead: number;
  /** Pending but not yet due: written with a delay. Not counted as lag. */
  scheduled: number;
  /**
   * The due event that has waited longest since it became due — usually the reason
   * everything behind it is waiting.
   */
  oldestPending: OldestPending | null;
}

/**
 * What the outbox looks like right now. The interesting number is
 * `oldestPending.ageSeconds`: an outbox fails silently, by filling up, and that is
 * the first place it shows.
 */
export async function outboxStatus(
  db: SqlExecutor,
  options: TableOptions = {},
): Promise<OutboxStatus> {
  const { qualified } = resolveTable(options);
  const { rows } = await db.query<{
    pending: number;
    in_flight: number;
    retrying: number;
    dead: number;
    scheduled: number;
  }>(
    // Two scans, each over a partial index that holds only the rows it counts: the
    // pending rows, then the dead ones. Delivered rows, kept for keepDeliveredMs and often
    // most of the table, are never read.
    `SELECT p.pending, p.in_flight, p.retrying, d.dead, p.scheduled
     FROM (
       SELECT count(*)::int AS pending,
              count(*) FILTER (WHERE locked_until > now())::int AS in_flight,
              count(*) FILTER (WHERE attempts > 0
                                 AND (locked_until IS NULL OR locked_until <= now()))::int AS retrying,
              count(*) FILTER (WHERE due_at > now())::int AS scheduled
       FROM ${qualified}
       WHERE delivered_at IS NULL AND dead_at IS NULL
     ) p,
     (SELECT count(*)::int AS dead FROM ${qualified} WHERE dead_at IS NOT NULL) d`,
  );
  const oldest = await db.query<{
    event_id: string;
    topic: string;
    key: string | null;
    attempts: number;
    last_error: string | null;
    age_seconds: number;
  }>(
    `SELECT event_id::text, topic, key, attempts, last_error,
            extract(epoch FROM now() - due_at)::float8 AS age_seconds
     FROM ${qualified}
     WHERE delivered_at IS NULL AND dead_at IS NULL AND due_at <= now()
     ORDER BY due_at, id
     LIMIT 1`,
  );
  const counts = rows[0] ?? { pending: 0, in_flight: 0, retrying: 0, dead: 0, scheduled: 0 };
  const o = oldest.rows[0];
  return {
    pending: Number(counts.pending),
    inFlight: Number(counts.in_flight),
    retrying: Number(counts.retrying),
    dead: Number(counts.dead),
    scheduled: Number(counts.scheduled),
    oldestPending: o
      ? {
          id: o.event_id,
          topic: o.topic,
          key: o.key,
          attempts: Number(o.attempts),
          lastError: o.last_error,
          ageSeconds: Number(o.age_seconds),
        }
      : null,
  };
}

export interface HealthOptions extends TableOptions {
  /** The longest an event may wait before the outbox counts as stuck. Defaults to 60 s. */
  maxLagSeconds?: number;
  /** Don't fail on dead events. Defaults to false. */
  allowDead?: boolean;
}

export class OutboxUnhealthyError extends Error {
  override name = "OutboxUnhealthyError";
  constructor(
    readonly problems: string[],
    readonly status: OutboxStatus,
  ) {
    super(`pgoutbox: the outbox is not healthy.\n\n${problems.map((p) => `  ✗ ${p}`).join("\n")}`);
  }
}

export function formatDuration(seconds: number): string {
  const s = Math.max(0, Math.round(seconds));
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ${s % 60}s`;
  const h = Math.floor(m / 60);
  if (h < 48) return `${h}h ${m % 60}m`;
  return `${Math.floor(h / 24)}d ${h % 24}h`;
}

/** The problems with a status, one line each, worst first. Empty when healthy. */
export function healthProblems(status: OutboxStatus, options: HealthOptions = {}): string[] {
  const maxLag = options.maxLagSeconds ?? 60;
  const problems: string[] = [];
  const o = status.oldestPending;
  if (o && o.ageSeconds > maxLag) {
    const what = `${status.pending.toLocaleString("en-US")} event${status.pending === 1 ? "" : "s"} pending; the oldest (${o.topic}${o.key ? `, key ${o.key}` : ""}) has waited ${formatDuration(o.ageSeconds)}, over the ${formatDuration(maxLag)} limit`;
    const behind = o.key ? ` — and every later event for key ${o.key} is waiting behind it` : "";
    if (o.attempts > 0 && o.lastError) {
      problems.push(
        `${what}. It has failed ${o.attempts} time${o.attempts === 1 ? "" : "s"}, most recently with: ${o.lastError}${behind}`,
      );
    } else if (o.attempts > 0) {
      problems.push(
        `${what}. A relay claimed it (attempt ${o.attempts}) and has not finished: a handler slower than leaseMs, or a relay that died holding it${behind}`,
      );
    } else {
      problems.push(`${what} and has never been attempted. Is a relay running against this table?`);
    }
  }
  if (status.dead > 0 && !options.allowDead) {
    problems.push(
      `${status.dead.toLocaleString("en-US")} event${status.dead === 1 ? " is" : "s are"} dead — retried until maxAttempts and set aside. Inspect them (dead_at IS NOT NULL), fix the cause, then retryDead().`,
    );
  }
  return problems;
}

/**
 * Throws `OutboxUnhealthyError` if an event has waited longer than `maxLagSeconds`, or
 * if any event is dead. Put it in a health check or a scheduled job: a relay that has
 * stopped, or a handler failing on every attempt, raises nothing anywhere else.
 */
export async function assertOutboxHealthy(
  db: SqlExecutor,
  options: HealthOptions = {},
): Promise<OutboxStatus> {
  const status = await outboxStatus(db, options);
  const problems = healthProblems(status, options);
  if (problems.length > 0) throw new OutboxUnhealthyError(problems, status);
  return status;
}

/**
 * Puts dead events back in line: attempts reset, due now. Pass ids to revive only those.
 * Returns how many were revived.
 */
export async function retryDead(
  db: SqlExecutor,
  options: TableOptions & { ids?: readonly string[] } = {},
): Promise<number> {
  const { qualified } = resolveTable(options);
  const filter = options.ids ? "AND event_id = ANY(string_to_array($1::text, ',')::uuid[])" : "";
  const params = options.ids ? [options.ids.join(",")] : [];
  const result = await db.query(
    `UPDATE ${qualified}
     SET dead_at = NULL, attempts = 0, available_at = now(), last_error = NULL, locked_until = NULL, locked_by = NULL
     WHERE dead_at IS NOT NULL AND delivered_at IS NULL ${filter}`,
    params,
  );
  return result.rowCount ?? 0;
}
