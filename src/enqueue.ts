import { randomUUID } from "node:crypto";
import { resolveTable } from "./sql.js";
import type { EventInput, SqlExecutor, TableOptions } from "./types.js";

/**
 * Thrown when `enqueue` is called outside a transaction.
 *
 * That is the one mistake that turns an outbox back into the dual write it exists to
 * prevent: the event commits on its own, whether or not the business write beside it
 * does. It raises no error anywhere else and passes every test that doesn't roll back,
 * so pgoutbox refuses it here — and inserts nothing.
 */
export class OutsideTransactionError extends Error {
  override name = "OutsideTransactionError";
  constructor() {
    super(
      "pgoutbox: enqueue() was called outside a transaction, so the event would commit on its own " +
        "whether or not your other writes do. Nothing was written. Call it with the client or " +
        "transaction object your BEGIN … COMMIT runs on (pool.connect() + BEGIN, sql.begin(), " +
        "db.transaction()), not with a pool.",
    );
  }
}

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

interface Row {
  id: string;
  topic: string;
  key: string | null;
  payload: unknown;
  headers: Record<string, string>;
  delay_ms: number;
}

function toRow(event: EventInput, index: number): Row {
  const where = `pgoutbox: event ${index}`;
  if (typeof event !== "object" || event === null) throw new TypeError(`${where} is not an object`);
  if (typeof event.topic !== "string" || event.topic.length === 0) {
    throw new TypeError(`${where} needs a non-empty string topic`);
  }
  if (event.payload === undefined) {
    throw new TypeError(`${where} (${event.topic}) has no payload; use null for an empty one`);
  }
  if (event.key != null && typeof event.key !== "string") {
    throw new TypeError(`${where} (${event.topic}) has a key that is not a string`);
  }
  if (event.id !== undefined && !UUID.test(event.id)) {
    throw new TypeError(`${where} (${event.topic}) has an id that is not a UUID: ${event.id}`);
  }
  const delay = event.delayMs ?? 0;
  if (!Number.isFinite(delay) || delay < 0) {
    throw new TypeError(
      `${where} (${event.topic}) has a delayMs that is not a finite, non-negative number`,
    );
  }
  const headers = event.headers ?? {};
  for (const [name, value] of Object.entries(headers)) {
    if (typeof value !== "string") {
      throw new TypeError(`${where} (${event.topic}) has a header "${name}" that is not a string`);
    }
  }
  return {
    id: event.id ?? randomUUID(),
    topic: event.topic,
    key: event.key ?? null,
    payload: event.payload,
    headers,
    delay_ms: delay,
  };
}

function insertSql(qualified: string): string {
  // One statement, one round trip, for one event or a thousand.
  //
  // The guard is the first CTE, and it is evaluated before the INSERT writes anything.
  // If this transaction already has an id, an earlier statement in it wrote something
  // — the order row, usually — so we are inside a transaction block and can insert.
  // An autocommitted statement never has one at this point, and the INSERT selects
  // nothing. A transaction that has not written yet (enqueue called first) looks the
  // same; enqueue() settles that case below.
  //
  // (statement_timestamp() <> transaction_timestamp() looks like a cheaper test and
  // is wrong: over the extended query protocol, which every driver uses for
  // parameterised queries, Parse and Execute get different timestamps even in
  // autocommit.)
  //
  // due_at and available_at use clock_timestamp(), not now(): a delay counts from when
  // the event was written, not from when a long transaction began.
  return `WITH guard AS (
  SELECT pg_current_xact_id_if_assigned() IS NOT NULL AS in_tx
), ins AS (
  INSERT INTO ${qualified} (event_id, topic, key, payload, headers, available_at, due_at)
  SELECT (e.value->>'id')::uuid, e.value->>'topic', e.value->>'key', e.value->'payload',
         coalesce(e.value->'headers', '{}'::jsonb), d.due, d.due
  FROM guard,
       jsonb_array_elements($1::text::jsonb) WITH ORDINALITY AS e(value, ord),
       LATERAL (SELECT clock_timestamp()
                       + make_interval(secs => coalesce((e.value->>'delay_ms')::float8, 0) / 1000.0) AS due) d
  WHERE guard.in_tx
  ORDER BY e.ord
  ON CONFLICT (event_id) DO NOTHING
  RETURNING 1
)
SELECT (SELECT in_tx FROM guard) AS in_tx, (SELECT count(*) FROM ins)::int AS inserted`;
}

/**
 * Writes events into the outbox as part of the caller's transaction. They become
 * visible to the relay when — and only if — that transaction commits.
 *
 * ```ts
 * await client.query("BEGIN");
 * await client.query("INSERT INTO orders …");
 * await enqueue(client, { topic: "order.created", key: order.id, payload: order });
 * await client.query("COMMIT");
 * ```
 *
 * Returns the event id (or ids, for an array), which handlers will see as `event.id`.
 * Throws `OutsideTransactionError`, having written nothing, if `tx` is not inside a
 * transaction.
 */
export async function enqueue(
  tx: SqlExecutor,
  event: EventInput,
  options?: TableOptions,
): Promise<string>;
export async function enqueue(
  tx: SqlExecutor,
  events: readonly EventInput[],
  options?: TableOptions,
): Promise<string[]>;
export async function enqueue(
  tx: SqlExecutor,
  input: EventInput | readonly EventInput[],
  options: TableOptions = {},
): Promise<string | string[]> {
  const many = Array.isArray(input);
  const rows = (many ? (input as readonly EventInput[]) : [input as EventInput]).map(toRow);
  if (rows.length === 0) return [];

  const { qualified } = resolveTable(options);
  const text = insertSql(qualified);
  const json = JSON.stringify(rows);

  const first = await tx.query<{ in_tx: boolean }>(text, [json]);
  if (!first.rows[0]?.in_tx) {
    // Autocommit — or a transaction that has not written anything yet. Give the
    // transaction an id and ask again. Inside a transaction block the id sticks and the
    // second attempt inserts. In autocommit, that SELECT was a transaction of its own,
    // already over, and the second attempt sees no id and inserts nothing. The same
    // guard decides both times; nothing is ever forced through.
    await tx.query("SELECT pg_current_xact_id()");
    const second = await tx.query<{ in_tx: boolean }>(text, [json]);
    if (!second.rows[0]?.in_tx) throw new OutsideTransactionError();
  }

  const ids = rows.map((row) => row.id);
  return many ? ids : ids[0]!;
}
