import { outboxSql, resolveTable } from "./sql.js";
import type { SqlExecutor, TableOptions } from "./types.js";

/** Creates the outbox table and its indexes if they don't exist. Safe to run on every boot. */
export async function migrate(db: SqlExecutor, options: TableOptions = {}): Promise<void> {
  await db.query(outboxSql(options));
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
    `SELECT
       count(*) FILTER (WHERE delivered_at IS NULL AND dead_at IS NULL)::int AS pending,
       count(*) FILTER (WHERE delivered_at IS NULL AND dead_at IS NULL AND locked_until > now())::int AS in_flight,
       count(*) FILTER (WHERE delivered_at IS NULL AND dead_at IS NULL AND attempts > 0
                          AND (locked_until IS NULL OR locked_until <= now()))::int AS retrying,
       count(*) FILTER (WHERE dead_at IS NOT NULL)::int AS dead,
       count(*) FILTER (WHERE delivered_at IS NULL AND dead_at IS NULL AND due_at > now())::int AS scheduled
     FROM ${qualified}
     WHERE delivered_at IS NULL`,
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
    super(`pgrelay: the outbox is not healthy.\n\n${problems.map((p) => `  ✗ ${p}`).join("\n")}`);
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
