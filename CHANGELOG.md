# pgrelay

## 0.1.0

### Minor Changes

- [`41e497c`](https://github.com/rajanaggarwal11/pgrelay/commit/41e497cd673fdcd2d50b6db9720e9be0201d48ea) Thanks [@rajanaggarwal11](https://github.com/rajanaggarwal11)! - First release. `enqueue` writes events in the caller's transaction and refuses — having written nothing — to run outside one. `Relay` delivers committed events with leases instead of held locks, no cursor (so a late commit is never skipped), per-key ordering across any number of relays, retries with backoff and a dead state. `assertOutboxHealthy`, `outboxStatus`, `retryDead` and `npx pgrelay status` say when the outbox is stuck and why. Adapters for postgres.js and Drizzle. Tested against Postgres 14–18, including relays killed with `kill -9` mid-delivery.
