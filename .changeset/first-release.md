---
"pgoutbox": minor
---

First release. `enqueue` writes events in the caller's transaction and refuses — having written nothing — to run outside one. `Relay` delivers committed events with leases instead of held locks, no cursor (so a late commit is never skipped), per-key ordering across any number of relays, retries with backoff and a dead state. `migrate` creates the table and can run from every process on boot at once. `assertOutboxHealthy`, `outboxStatus`, `retryDead` and `npx pgoutbox status` say when the outbox is stuck and why. Adapters for postgres.js (`withPostgresJs` from `pgoutbox/postgres-js`) and Drizzle (`withDrizzle` from `pgoutbox/drizzle`). Tested against Postgres 14–18, including relays killed with `kill -9` mid-delivery.
