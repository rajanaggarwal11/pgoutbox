# Contributing

Thanks for looking. pgoutbox is small on purpose, and every guarantee in the README has a test against a real Postgres behind it. Contributions that keep it that way are very welcome.

## Setup

You need Node 22.13+, pnpm, and a Postgres 14+ you can create schemas in. The dev container (`.devcontainer/`) has all three.

```bash
pnpm install
export TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/pgoutbox_test
pnpm build   # the crash and CLI tests run the built package
pnpm test
```

`pnpm check` runs lint, typecheck, build and tests — the same as CI.

## Ground rules

- **No mocks for the database.** If a change affects what Postgres sees, test it against Postgres. The crash test spawns real processes and kills them; keep it that way.
- **A guarantee needs a test that would fail without it.** The README table maps each guarantee to its test; add a row if you add one.
- **Silent failures are bugs.** If something can go wrong without an error, either make it impossible or make `assertOutboxHealthy` say so.
- **Add a changeset** (`pnpm changeset`) for anything a user would notice.

## Good first issues

Look for the [`good first issue`](https://github.com/rajanaggarwal11/pgoutbox/labels/good%20first%20issue) label. If you want to take something bigger, open an issue first so we can agree on the shape.
