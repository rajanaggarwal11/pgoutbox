import { hostname } from "node:os";
import { randomBytes } from "node:crypto";
import { resolveTable } from "./sql.js";
import type { Handler, OutboxEvent, SqlExecutor, TableOptions } from "./types.js";

export interface RelayOptions<P = unknown> extends TableOptions {
  /**
   * Delivers one event: publish it to your broker, call the webhook, send the email.
   * Resolve to mark it delivered; throw to retry it later. Delivery is at-least-once —
   * a crash after the handler succeeds and before the relay records it means the
   * event is delivered again — so consumers should deduplicate on `event.id`.
   */
  handler: Handler<P>;
  /** Most events in flight at once, per relay. Defaults to 10. */
  concurrency?: number;
  /**
   * How long to wait between polls when there is nothing to do. Defaults to 1000 ms;
   * at least 10 ms. An idle relay runs one claim query per interval, so a very short
   * one is a steady load on the database for nothing.
   */
  pollIntervalMs?: number;
  /**
   * How long a claimed event belongs to this relay. If the process dies, another
   * relay can claim the event once the lease runs out. Set it well above your
   * slowest handler. Defaults to 30 s.
   */
  leaseMs?: number;
  /**
   * Attempts before an event is marked dead and set aside. Defaults to 10. An event
   * whose relay died mid-delivery counts as an attempt too, so an event that crashes
   * every relay that touches it is set aside instead of taking them down one by one.
   */
  maxAttempts?: number;
  /**
   * Milliseconds to wait before attempt `attempt + 1`. Defaults to exponential with full
   * jitter: 0.5–1 s, doubling each attempt, capped at 5 minutes.
   */
  backoff?: (attempt: number) => number;
  /**
   * How long to keep delivered rows, for inspection, before deleting them.
   * `0` deletes a row as soon as it is delivered. Defaults to 24 hours.
   */
  keepDeliveredMs?: number;
  /** How often to delete delivered rows older than `keepDeliveredMs`. Defaults to 60 s. */
  cleanupIntervalMs?: number;
  /** Identifies this relay in `locked_by`. Defaults to `hostname:pid:random`. */
  workerId?: string;
  /**
   * Called when a handler throws, or when the relay cannot reach the database.
   * Defaults to `console.error`. The relay keeps running either way — including when
   * `onError` itself throws.
   */
  onError?: (error: unknown, event?: OutboxEvent<P>) => void;
}

interface ClaimedRow {
  id: string;
  event_id: string;
  topic: string;
  key: string | null;
  payload: unknown;
  headers: Record<string, string>;
  created_at: Date | string;
  attempts: number;
  lease_left_ms: number;
}

/** The longest delay `setTimeout` honours; anything larger fires after 1 ms. */
const MAX_TIMER_MS = 2 ** 31 - 1;

/** The shortest idle wait. Below this an idle relay is a busy loop against the database. */
const MIN_POLL_INTERVAL_MS = 10;

export function defaultBackoff(attempt: number): number {
  const base = Math.min(1000 * 2 ** Math.max(0, attempt - 1), 5 * 60_000);
  // Full jitter keeps a fleet of relays from retrying a failing broker in lockstep.
  return Math.round(base / 2 + Math.random() * (base / 2));
}

function describe(error: unknown): string {
  const text =
    error instanceof Error
      ? `${error.name}: ${error.message}`
      : typeof error === "string"
        ? error
        : JSON.stringify(error);
  return (text ?? String(error)).slice(0, 2000);
}

/**
 * The SQL a relay runs, built once per relay. Every statement is a single,
 * autocommitted statement: no transaction is held open while a handler runs.
 */
function statements(qualified: string) {
  return {
    // A row is claimable when it is undelivered, not dead, due, not leased by a live
    // relay, under maxAttempts ($5), not already running in this relay ($4 — an
    // expired lease lets another process take over from a dead one, but this process
    // re-running its own hung handler would only double the work), and, if it has a
    // key, it is that key's turn:
    //
    //  - a row that has been started (attempts > 0) holds its key until it is
    //    delivered or dead. It waits for nobody, and nobody else for the key goes
    //    while it holds — whether it is in flight, waiting to retry, or abandoned by a
    //    relay that died.
    //  - a row not yet started goes when no row for its key is started and no
    //    undelivered row for its key has a lower id.
    //
    // Starting from "is anything started for this key" rather than only "is there a
    // lower id" is what makes a late commit safe: a lower id that commits after a
    // higher one has been started waits its turn instead of running beside it. The
    // `key_holder` unique index enforces the same rule inside Postgres, so a race
    // between two claims' snapshots ends in a unique violation (retried by the relay),
    // never in two handlers for one key.
    //
    // The two rules are two separate checks, each answered by one index lookup that
    // stops at the first match: "is a row for this key started" from `key_holder`
    // (`h.key IS NOT NULL` lets the planner use it, or hash it once per claim), and "is a
    // lower row for this key undelivered" from `pending_key`. Don't fold them into one
    // `EXISTS (… attempts > 0 OR id < o.id)`: the planner answers that with a BitmapOr
    // that collects every lower row of the key for every row it looks at, which is
    // quadratic in a key's backlog (20,000 events behind one retrying event: 11 s per
    // claim; test/plans.test.ts guards this). A started row skips both checks:
    // `key_holder` is unique, so nothing else for its key can be started.
    //
    // There is no cursor anywhere, so a row whose transaction commits late is simply
    // undelivered until it is delivered.
    //
    // MATERIALIZED: the candidate set must be computed exactly once. A planner that
    // re-evaluated a LIMIT … SKIP LOCKED subquery per join row could lock more rows
    // than asked for.
    claim: `WITH candidates AS MATERIALIZED (
  SELECT o.id FROM ${qualified} o
  WHERE o.delivered_at IS NULL AND o.dead_at IS NULL
    AND o.available_at <= now()
    AND (o.locked_until IS NULL OR o.locked_until <= now())
    AND o.attempts < $5
    AND NOT (o.id = ANY(string_to_array($4::text, ',')::bigint[]))
    AND (o.key IS NULL OR o.attempts > 0 OR (
      NOT EXISTS (
        SELECT 1 FROM ${qualified} h
        WHERE h.key = o.key AND h.key IS NOT NULL
          AND h.attempts > 0 AND h.delivered_at IS NULL AND h.dead_at IS NULL
      )
      AND NOT EXISTS (
        SELECT 1 FROM ${qualified} p
        WHERE p.key = o.key AND p.id < o.id AND p.delivered_at IS NULL AND p.dead_at IS NULL
      )
    ))
  ORDER BY o.id
  LIMIT $1
  FOR UPDATE SKIP LOCKED
)
UPDATE ${qualified} o
SET attempts = o.attempts + 1,
    locked_until = now() + make_interval(secs => $2 / 1000.0),
    locked_by = $3
FROM candidates c
WHERE o.id = c.id
RETURNING o.id::text, o.event_id::text, o.topic, o.key, o.payload, o.headers, o.created_at, o.attempts,
          (extract(epoch FROM o.locked_until - clock_timestamp()) * 1000)::float8 AS lease_left_ms`,

    // An event whose lease ran out with no outcome recorded was abandoned: its relay
    // crashed, was killed, or ran past leaseMs. Once that has happened maxAttempts
    // times, stop handing it out — it may be the thing killing the relays. Only leased
    // rows can qualify, and the `leased` index holds nothing else, so this reads a
    // handful of index entries however large the backlog is.
    reap: `UPDATE ${qualified}
SET dead_at = now(), locked_until = NULL, locked_by = NULL,
    last_error = coalesce(last_error || ' — then: ', '') ||
      'abandoned ' || attempts || ' times: claimed by a relay that never recorded an outcome (crashed, killed, or exceeded leaseMs)'
WHERE delivered_at IS NULL AND dead_at IS NULL AND attempts >= $1
  AND locked_until IS NOT NULL AND locked_until <= now()`,

    // Both outcomes are guarded on attempts: if this relay's lease ran out and another
    // relay has since claimed the event, this relay must not overwrite that claim. For
    // a success that means the other relay delivers again (at-least-once) and the key's
    // next event waits for it, instead of starting while the re-send is in flight.
    delivered: `UPDATE ${qualified}
SET delivered_at = now(), locked_until = NULL, locked_by = NULL, last_error = NULL
WHERE id = $1 AND delivered_at IS NULL AND attempts = $2`,

    deliveredDelete: `DELETE FROM ${qualified} WHERE id = $1 AND attempts = $2`,

    failed: `UPDATE ${qualified}
SET locked_until = NULL, locked_by = NULL, last_error = $2,
    available_at = now() + make_interval(secs => $3 / 1000.0),
    dead_at = CASE WHEN attempts >= $4 THEN now() END
WHERE id = $1 AND delivered_at IS NULL AND attempts = $5`,

    cleanup: `DELETE FROM ${qualified} WHERE id IN (
  SELECT id FROM ${qualified}
  WHERE delivered_at IS NOT NULL AND delivered_at < now() - make_interval(secs => $1 / 1000.0)
  ORDER BY delivered_at
  LIMIT 1000
  FOR UPDATE SKIP LOCKED
)`,
  };
}

function sqlState(error: unknown): string | undefined {
  // Drivers put the SQLSTATE on `code`; Drizzle wraps the driver's error in `cause`.
  const e = error as { code?: unknown; cause?: { code?: unknown } } | null;
  const code = e?.code ?? e?.cause?.code;
  return typeof code === "string" ? code : undefined;
}

function positiveInt(name: string, value: number | undefined, fallback: number): number {
  const v = value ?? fallback;
  if (!Number.isInteger(v) || v < 1)
    throw new TypeError(`pgrelay: ${name} must be a positive integer, got ${v}`);
  return v;
}

function nonNegative(name: string, value: number | undefined, fallback: number): number {
  const v = value ?? fallback;
  if (!Number.isFinite(v) || v < 0)
    throw new TypeError(`pgrelay: ${name} must be a non-negative number, got ${v}`);
  return v;
}

/**
 * Moves committed events from the outbox to your handler. Run one or many — in
 * separate processes, on separate machines — against the same table; they share the
 * work without coordinating beyond the table itself.
 *
 * ```ts
 * const relay = new Relay(pool, {
 *   handler: async (event, { signal }) => {
 *     await fetch(webhookUrl, { method: "POST", body: JSON.stringify(event), signal });
 *   },
 * });
 * relay.start();
 * process.on("SIGTERM", () => relay.stop());
 * ```
 *
 * `db` must be a pool (or anything whose `query` runs each statement on its own):
 * the relay never opens a transaction.
 */
export class Relay<P = unknown> {
  readonly workerId: string;
  private readonly sql: ReturnType<typeof statements>;
  private readonly handler: Handler<P>;
  private readonly concurrency: number;
  private readonly pollIntervalMs: number;
  private readonly leaseMs: number;
  private readonly maxAttempts: number;
  private readonly backoff: (attempt: number) => number;
  private readonly keepDeliveredMs: number;
  private readonly cleanupIntervalMs: number;
  private readonly onError: (error: unknown, event?: OutboxEvent<P>) => void;

  private running = false;
  private loop: Promise<void> | undefined;
  private wakeSleeper: (() => void) | undefined;
  private wakePending = false;
  private maxAttemptsReaped = 0;
  private readonly inFlight = new Set<Promise<void>>();
  /** Outbox row ids whose handler is running in this relay right now. */
  private readonly runningIds = new Set<string>();
  private lastCleanup = 0;

  constructor(
    private readonly db: SqlExecutor,
    options: RelayOptions<P>,
  ) {
    if (typeof options?.handler !== "function")
      throw new TypeError("pgrelay: a relay needs a handler function");
    this.sql = statements(resolveTable(options).qualified);
    this.handler = options.handler;
    this.concurrency = positiveInt("concurrency", options.concurrency, 10);
    this.pollIntervalMs = options.pollIntervalMs ?? 1000;
    if (
      !Number.isFinite(this.pollIntervalMs) ||
      this.pollIntervalMs < MIN_POLL_INTERVAL_MS ||
      this.pollIntervalMs > MAX_TIMER_MS
    ) {
      // 0 would make an idle relay re-run its claim query back to back, and anything over
      // MAX_TIMER_MS fires after 1 ms: both are a busy loop against the database.
      throw new TypeError(
        `pgrelay: pollIntervalMs must be between ${MIN_POLL_INTERVAL_MS} and ${MAX_TIMER_MS} ms, got ${this.pollIntervalMs}`,
      );
    }
    this.leaseMs = positiveInt("leaseMs", options.leaseMs, 30_000);
    if (this.leaseMs > MAX_TIMER_MS) {
      throw new TypeError(`pgrelay: leaseMs must be at most ${MAX_TIMER_MS} (about 24.8 days)`);
    }
    this.maxAttempts = positiveInt("maxAttempts", options.maxAttempts, 10);
    this.backoff = options.backoff ?? defaultBackoff;
    this.keepDeliveredMs = nonNegative(
      "keepDeliveredMs",
      options.keepDeliveredMs,
      24 * 60 * 60_000,
    );
    this.cleanupIntervalMs = nonNegative("cleanupIntervalMs", options.cleanupIntervalMs, 60_000);
    const onError = options.onError ?? ((error: unknown) => console.error(error));
    // An onError that throws must not take the relay — or the process, through an
    // unhandled rejection — down with it.
    this.onError = (error, event) => {
      try {
        onError(error, event);
      } catch {
        /* nothing sensible left to report to */
      }
    };
    this.workerId =
      options.workerId ?? `${hostname()}:${process.pid}:${randomBytes(3).toString("hex")}`;
  }

  /** Starts polling in the background. Returns immediately. */
  start(): void {
    if (this.running) return;
    if (this.loop) throw new Error("pgrelay: start() called while stop() is still finishing");
    this.running = true;
    this.loop = this.run();
  }

  /**
   * Stops claiming new events and resolves once every event already handed to a
   * handler has finished. Call it on SIGTERM; nothing claimed is abandoned mid-flight.
   */
  async stop(): Promise<void> {
    this.running = false;
    this.wake();
    await this.loop;
    this.loop = undefined;
  }

  /** Cuts the current idle wait short, e.g. right after your own code enqueued something. */
  wake(): void {
    if (this.wakeSleeper) this.wakeSleeper();
    else this.wakePending = true; // arrived mid-claim: skip the next wait instead
  }

  /**
   * Delivers everything that is due right now, then resolves with how many events were
   * delivered. Events waiting out a retry delay are left for later. Meant for tests,
   * scripts and serverless functions — not for use alongside `start()` on the same relay.
   */
  async drain(): Promise<number> {
    if (this.running || this.loop) {
      throw new Error("pgrelay: drain() cannot run while the relay is started");
    }
    let delivered = 0;
    try {
      for (;;) {
        const claimed = await this.claim(this.concurrency - this.inFlight.size);
        for (const row of claimed) this.launch(row, () => delivered++);
        if (claimed.length === 0 && this.inFlight.size === 0) break;
        if (this.inFlight.size > 0) await Promise.race(this.inFlight);
      }
    } finally {
      // If a claim failed, don't return while handlers it already started are running.
      await Promise.allSettled(this.inFlight);
    }
    return delivered;
  }

  private async run(): Promise<void> {
    while (this.running) {
      try {
        await this.maybeCleanup();
        const free = this.concurrency - this.inFlight.size;
        const claimed = free > 0 ? await this.claim(free) : [];
        for (const row of claimed) this.launch(row);
        if (claimed.length > 0 && this.inFlight.size < this.concurrency) continue;
        // Full, or nothing due: wait for a slot to free up, the poll interval, or wake().
        await this.waitFor(this.pollIntervalMs);
      } catch (error) {
        // Usually the database is unreachable. Say so, back off, and keep going.
        this.onError(error);
        await this.waitFor(Math.max(this.pollIntervalMs, 1000));
      }
    }
    await Promise.allSettled(this.inFlight);
  }

  /** Waits for `ms`, a free slot, or wake() — whichever is first — and leaves no timer behind. */
  private async waitFor(ms: number): Promise<void> {
    if (this.wakePending) {
      this.wakePending = false;
      return;
    }
    let done!: () => void;
    const slept = new Promise<void>((resolve) => {
      const timer = setTimeout(() => done(), ms);
      done = () => {
        clearTimeout(timer);
        if (this.wakeSleeper === done) this.wakeSleeper = undefined;
        resolve();
      };
      this.wakeSleeper = done;
    });
    try {
      await Promise.race([slept, ...this.inFlight]);
    } finally {
      done();
    }
  }

  private async claim(limit: number): Promise<ClaimedRow[]> {
    if (limit <= 0) return [];
    if (this.maxAttemptsReaped < Date.now() - 1000) {
      this.maxAttemptsReaped = Date.now();
      await this.db.query(this.sql.reap, [this.maxAttempts]);
    }
    const params = [
      limit,
      this.leaseMs,
      this.workerId,
      [...this.runningIds].join(","),
      this.maxAttempts,
    ];
    let rows: ClaimedRow[];
    try {
      ({ rows } = await this.db.query<ClaimedRow>(this.sql.claim, params));
    } catch (error) {
      // 23505 on key_holder: another relay started this key's event between this
      // claim's snapshot and its update. Nothing was claimed; look again.
      if (sqlState(error) !== "23505") throw error;
      ({ rows } = await this.db.query<ClaimedRow>(this.sql.claim, params));
    }
    return rows.sort((a, b) => (BigInt(a.id) < BigInt(b.id) ? -1 : 1));
  }

  private launch(row: ClaimedRow, onDelivered?: () => void): void {
    this.runningIds.add(row.id);
    const task = this.deliver(row).then((ok) => {
      if (ok) onDelivered?.();
    });
    const tracked = task.finally(() => {
      this.inFlight.delete(tracked);
      this.runningIds.delete(row.id);
    });
    this.inFlight.add(tracked);
  }

  /** Runs the handler for one claimed row and records the outcome. Never rejects. */
  private async deliver(row: ClaimedRow): Promise<boolean> {
    const event: OutboxEvent<P> = {
      id: row.event_id,
      topic: row.topic,
      key: row.key,
      payload: row.payload as P,
      headers: row.headers ?? {},
      createdAt: row.created_at instanceof Date ? row.created_at : new Date(row.created_at),
      attempt: Number(row.attempts),
    };

    // Abort a little before the lease ends, so a well-behaved handler has stopped by
    // the time another relay may claim the same event. The time left is measured by
    // Postgres at the end of the claim, so a slow round trip eats into the handler's
    // time rather than into the margin.
    const controller = new AbortController();
    const margin = Math.min(this.leaseMs * 0.1, 5000);
    const left = Number.isFinite(Number(row.lease_left_ms))
      ? Number(row.lease_left_ms)
      : this.leaseMs;
    const timer = setTimeout(
      () => controller.abort(new Error(`pgrelay: lease on event ${event.id} is about to expire`)),
      Math.min(MAX_TIMER_MS, Math.max(0, left - margin)),
    );

    let failure: { error: unknown } | undefined;
    try {
      await this.handler(event, { signal: controller.signal });
    } catch (error) {
      failure = { error };
    } finally {
      clearTimeout(timer);
    }

    try {
      if (failure) {
        this.onError(failure.error, event);
        const dead = event.attempt >= this.maxAttempts;
        let delay = 0;
        if (!dead) {
          try {
            delay = Math.max(0, Number(this.backoff(event.attempt)) || 0);
          } catch (backoffError) {
            this.onError(backoffError, event);
            delay = defaultBackoff(event.attempt);
          }
        }
        await this.db.query(this.sql.failed, [
          row.id,
          describe(failure.error),
          delay,
          this.maxAttempts,
          event.attempt,
        ]);
        return false;
      }
      await this.db.query(
        this.keepDeliveredMs === 0 ? this.sql.deliveredDelete : this.sql.delivered,
        [row.id, event.attempt],
      );
    } catch (recordError) {
      // The outcome could not be recorded. The lease will run out and the event will be
      // delivered again — the at-least-once half of the contract, and why consumers
      // deduplicate on event.id. Nothing is lost.
      this.onError(recordError, event);
    }
    return !failure;
  }

  private async maybeCleanup(): Promise<void> {
    if (this.keepDeliveredMs === 0 || this.cleanupIntervalMs === 0) return;
    const now = Date.now();
    if (now - this.lastCleanup < this.cleanupIntervalMs) return;
    this.lastCleanup = now;
    await this.db.query(this.sql.cleanup, [this.keepDeliveredMs]);
  }
}
