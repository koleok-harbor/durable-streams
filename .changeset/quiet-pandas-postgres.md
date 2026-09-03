---
"@durable-streams/server": patch
---

Add a Postgres-backed stream store (`PostgresStreamStore`) alongside the existing in-memory and file-backed stores. Enable it by passing a `postgresUrl` connection string to `DurableStreamTestServer`. The store interface is now async, so the server awaits all store calls; the in-memory and file-backed stores are unchanged in behavior.
