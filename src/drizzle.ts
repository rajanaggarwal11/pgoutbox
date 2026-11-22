import { sql, type SQL } from "drizzle-orm";
import type { SqlExecutor } from "./types.js";

/** Anything with Drizzle's `execute`: a database, or the `tx` that `db.transaction()` hands you. */
export interface DrizzleExecutable {
  execute(query: SQL): Promise<unknown>;
}

/**
 * Rebuilds `text` with `$1`, `$2`… placeholders as a Drizzle `SQL`, parameters kept as
 * parameters. Placeholders are only recognised outside quoted identifiers and string
 * literals, so a table named `events$2` stays a name.
 */
function toSql(text: string, params: readonly unknown[]): SQL {
  const chunks: SQL[] = [];
  let raw = "";
  let quote: string | null = null;
  for (let i = 0; i < text.length; i++) {
    const ch = text[i]!;
    if (quote) {
      raw += ch;
      if (ch === quote) {
        if (text[i + 1] === quote)
          raw += text[++i]!; // a doubled quote stays inside
        else quote = null;
      }
      continue;
    }
    if (ch === '"' || ch === "'") {
      quote = ch;
      raw += ch;
      continue;
    }
    const m = ch === "$" ? /^\$(\d+)/.exec(text.slice(i)) : null;
    if (!m) {
      raw += ch;
      continue;
    }
    const index = Number(m[1]) - 1;
    if (index < 0 || index >= params.length) {
      throw new RangeError(
        `pgoutbox: query references $${m[1]} but has ${params.length} parameters`,
      );
    }
    if (raw) chunks.push(sql.raw(raw));
    raw = "";
    chunks.push(sql`${params[index]}`);
    i += m[0].length - 1;
  }
  if (raw) chunks.push(sql.raw(raw));
  return sql.join(chunks);
}

/**
 * Wraps a Drizzle database or transaction as a pgoutbox executor. Works with Drizzle
 * over node-postgres and over postgres.js.
 *
 * ```ts
 * await db.transaction(async (tx) => {
 *   await tx.insert(orders).values(order);
 *   await enqueue(withDrizzle(tx), { topic: "order.created", key: order.id, payload: order });
 * });
 *
 * new Relay(withDrizzle(db), { handler }).start();
 * ```
 *
 * Pass the `tx`, not `db`: `enqueue(withDrizzle(db), …)` runs outside your transaction
 * and is refused with `OutsideTransactionError`.
 */
export function withDrizzle(db: DrizzleExecutable): SqlExecutor {
  return {
    async query<T = Record<string, unknown>>(text: string, params: readonly unknown[] = []) {
      const result = (await db.execute(toSql(text, params))) as
        { rows: T[]; rowCount?: number | null } | (T[] & { count?: number });
      // node-postgres returns { rows, rowCount }; postgres.js returns an array with `count`.
      if (Array.isArray(result))
        return { rows: Array.from(result), rowCount: result.count ?? null };
      return { rows: result.rows, rowCount: result.rowCount ?? null };
    },
  };
}
