import type { Sql, TransactionSql } from "postgres";
import type { SqlExecutor } from "./types.js";

/**
 * Wraps a postgres.js `sql` (for a relay) or the `sql` that `sql.begin()` hands your
 * callback (for `enqueue`) as a pgoutbox executor.
 *
 * ```ts
 * await sql.begin(async (tx) => {
 *   await tx`INSERT INTO orders ${tx(order)}`;
 *   await enqueue(withPostgresJs(tx), { topic: "order.created", key: order.id, payload: order });
 * });
 *
 * new Relay(withPostgresJs(sql), { handler }).start();
 * ```
 *
 * Passing the top-level `sql` to `enqueue` is the dual-write mistake — each statement
 * would run on whatever pooled connection is free, outside your transaction — and
 * `enqueue` refuses it with `OutsideTransactionError`.
 */
export function withPostgresJs(sql: Sql | TransactionSql): SqlExecutor {
  return {
    async query<T = Record<string, unknown>>(text: string, params: readonly unknown[] = []) {
      const result = await sql.unsafe(text, params as Parameters<Sql["unsafe"]>[1]);
      // postgres.js returns an array-like RowList with the affected-row count on `count`.
      return { rows: Array.from(result) as unknown as T[], rowCount: result.count ?? null };
    },
  };
}
