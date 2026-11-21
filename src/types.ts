/**
 * The minimum a database client has to expose: `pg`'s `Client` / `PoolClient` /
 * `Pool` shape. postgres.js and Drizzle are wrapped to match it by the adapters in
 * `pgrelay/postgres-js` and `pgrelay/drizzle`, so the core never depends on a driver.
 */
export interface SqlExecutor {
  query<T = Record<string, unknown>>(
    text: string,
    params?: readonly unknown[],
  ): Promise<{ rows: T[]; rowCount?: number | null }>;
}

/** Where the outbox lives. */
export interface TableOptions {
  /** Defaults to `public`. */
  schema?: string;
  /** Defaults to `pgrelay_outbox`. */
  table?: string;
}

/** An event to write into the outbox, inside the caller's transaction. */
export interface EventInput<P = unknown> {
  /** What happened, e.g. `order.created`. Consumers route on it. */
  topic: string;
  /** Any JSON value. Stored as `jsonb`. */
  payload: P;
  /**
   * Events with the same key are delivered one at a time, in commit order — the
   * next one is not handed to a handler until the previous one is delivered or
   * dead. Use the aggregate id (`order_123`). Events without a key are unordered
   * and delivered in parallel.
   */
  key?: string | null;
  /** String metadata carried alongside the payload: a trace id, a tenant, a schema version. */
  headers?: Record<string, string>;
  /**
   * The event's id. Generated (a random UUID) when omitted. Supplying one makes the
   * write idempotent for as long as the row is kept (see the relay's `keepDeliveredMs`):
   * enqueueing the same id again keeps the first and ignores the second.
   */
  id?: string;
  /** Hold the event back for this many milliseconds after it is written. */
  delayMs?: number;
}

/** An event as a handler receives it. */
export interface OutboxEvent<P = unknown> {
  /** Stable across redeliveries. Consumers deduplicate on it. */
  id: string;
  topic: string;
  key: string | null;
  payload: P;
  headers: Record<string, string>;
  /** When the transaction that wrote it ran. */
  createdAt: Date;
  /** 1 on the first delivery attempt, 2 on the first retry, and so on. */
  attempt: number;
}

export interface DeliveryContext {
  /**
   * Aborted shortly before this worker's lease on the event runs out. After that,
   * another worker may claim the event and deliver it again, so a handler that is
   * still running should stop. Pass it to `fetch`, to your broker client, to anything
   * that accepts one.
   */
  signal: AbortSignal;
}

export type Handler<P = unknown> = (
  event: OutboxEvent<P>,
  context: DeliveryContext,
) => Promise<void>;
