# Security policy

## Supported versions

The latest published minor version receives fixes. This project is pre-1.0, so older minors are not backported.

## Reporting a vulnerability

Please report privately through GitHub's [security advisory form](https://github.com/rajanaggarwal11/pgoutbox/security/advisories/new) rather than opening a public issue.

You should get an acknowledgement within 72 hours and an assessment within a week.

## Scope

`pgoutbox` runs SQL against the database you give it and calls the handler you give it. It makes no network requests of its own and spawns no processes. Event payloads and headers are always sent as bind parameters; the schema and table names you configure are quoted as identifiers.

The things most worth reporting:

- Any way for event content, or a configured schema or table name, to change the SQL that runs.
- Any way for an event to be delivered when its transaction rolled back, to be skipped permanently, or to be delivered out of order for its key.
- Any way for `enqueue` to write an event outside the caller's transaction without throwing.
- Credentials from a connection URL appearing in `pgoutbox status` output or errors.
