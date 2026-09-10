---
"@durable-streams-internal/caddy-plugin": patch
---

Add a Postgres-backed store (`PostgresStore`) to the Caddy plugin, matching the `Store` interface contract of the file store. Enable it with the `postgres_url` Caddyfile subdirective (plus optional `postgres_max_conns` and `postgres_schema`). Stream metadata lives in a `durable_streams` table and message payloads in a `durable_stream_messages` table; appends, closes, creates and deletes serialize per stream via `SELECT ... FOR UPDATE` row locks. Includes Go unit tests (gated on `POSTGRES_URL`) and a Postgres conformance test (`test/postgres.conformance.test.ts`, gated on `POSTGRES_URL`).
