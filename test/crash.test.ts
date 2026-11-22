import { spawn, type ChildProcess } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import type pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { enqueue } from "../src/enqueue.js";
import type { TableOptions } from "../src/types.js";
import { count, DATABASE_URL, freshOutbox, pool, sleep, transaction } from "./helpers.js";

/**
 * The claim an outbox has to make is about crashes, so this test crashes it: relay
 * processes are SIGKILLed — no shutdown hook, no finally, no chance to record what
 * they were doing — at random moments, over and over, while they deliver. Then one
 * is left to finish. Every event must have reached the broker, and every key in order.
 */
const WORKER = fileURLToPath(new URL("./fixtures/worker.mjs", import.meta.url));
const DIST = fileURLToPath(new URL("../dist/index.js", import.meta.url));

let db: pg.Pool;
let table: TableOptions;
let dir: string;

beforeAll(async () => {
  if (!existsSync(DIST))
    throw new Error("crash.test.ts runs the built package: run `pnpm build` first");
  db = pool();
  table = await freshOutbox(db, "pgoutbox_crash");
  dir = mkdtempSync(join(tmpdir(), "pgoutbox-crash-"));
});

afterAll(async () => {
  await db.end();
  rmSync(dir, { recursive: true, force: true });
});

function worker(log: string): ChildProcess {
  return spawn(process.execPath, [WORKER, DATABASE_URL, "pgoutbox_crash", log], {
    stdio: "ignore",
  });
}

function exited(child: ChildProcess): Promise<void> {
  return new Promise((resolve) =>
    child.exitCode !== null || child.signalCode !== null
      ? resolve()
      : child.once("exit", () => resolve()),
  );
}

describe("kill -9", () => {
  it("loses nothing and keeps every key in order when relays are killed mid-delivery", async () => {
    const KEYS = 40;
    const PER_KEY = 50;
    const ids = new Set<string>();
    for (let seq = 0; seq < PER_KEY; seq++) {
      const batch = await transaction(db, (tx) =>
        enqueue(
          tx,
          Array.from({ length: KEYS }, (_, k) => ({
            topic: "seq",
            key: `k${k}`,
            payload: { seq },
          })),
          table,
        ),
      );
      batch.forEach((id) => ids.add(id));
    }
    const free = await transaction(db, (tx) =>
      enqueue(
        tx,
        Array.from({ length: 200 }, () => ({ topic: "free", payload: {} })),
        table,
      ),
    );
    free.forEach((id) => ids.add(id));

    const log = join(dir, "deliveries.log");
    const KILLS = 12;
    for (let i = 0; i < KILLS; i++) {
      // Two relays at a time, so a kill also leaves a live relay to take over leases.
      const a = worker(log);
      const b = worker(log);
      await sleep(80 + Math.random() * 200);
      a.kill("SIGKILL");
      await sleep(Math.random() * 50);
      b.kill("SIGKILL");
      await Promise.all([exited(a), exited(b)]);
    }

    const survivor = worker(log);
    for (let i = 0; i < 600 && (await count(db, table, "delivered_at IS NULL")) > 0; i++)
      await sleep(50);
    survivor.kill("SIGTERM");
    await exited(survivor);

    const lines = readFileSync(log, "utf8")
      .trim()
      .split("\n")
      .map((l) => l.split(" "));
    const delivered = new Set(lines.map(([id]) => id));

    // Nothing lost.
    expect([...ids].filter((id) => !delivered.has(id))).toEqual([]);
    expect(await count(db, table, "delivered_at IS NULL")).toBe(0);

    // At-least-once: a kill between "handed to the broker" and "recorded" means that
    // event goes out again — at most once per kill per in-flight slot.
    const duplicates = lines.length - delivered.size;
    expect(duplicates).toBeLessThanOrEqual(KILLS * 2 * 8);

    // And the kills really did land mid-delivery: events were claimed, abandoned by a
    // dead process, and taken over after their lease ran out.
    const takenOver = await count(db, table, "attempts > 1");
    expect(takenOver).toBeGreaterThan(0);

    // In order: the first delivery of each event, per key, follows commit order.
    const firstSeen = new Set<string>();
    const perKey = new Map<string, number[]>();
    for (const [id, key, seq] of lines) {
      if (firstSeen.has(id!)) continue;
      firstSeen.add(id!);
      if (key === "-") continue;
      perKey.set(key!, [...(perKey.get(key!) ?? []), Number(seq)]);
    }
    for (let k = 0; k < KEYS; k++) {
      expect(perKey.get(`k${k}`), `key k${k}`).toEqual(
        Array.from({ length: PER_KEY }, (_, i) => i),
      );
    }

    console.log(
      `kill -9 × ${KILLS * 2}: ${ids.size} events, ${takenOver} taken over from a killed relay, ${duplicates} delivered twice, 0 lost, ${KEYS} keys in order`,
    );
  });
});
