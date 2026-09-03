/**
 * Postgres-backed server conformance tests.
 *
 * These run only when a `POSTGRES_URL` environment variable is set, e.g.:
 *
 *   POSTGRES_URL=postgres://postgres:test@localhost:5433/ds \
 *     pnpm vitest run --project server test/postgres.test.ts
 */

import { afterAll, beforeAll, describe } from "vitest"
import { runConformanceTests } from "@durable-streams/server-conformance-tests"
import { DurableStreamTestServer } from "../src/server"

const POSTGRES_URL = process.env.POSTGRES_URL

describe.runIf(POSTGRES_URL)(`Postgres-Backed Server Implementation`, () => {
  let server: DurableStreamTestServer

  const config = { baseUrl: ``, subscriptions: true }

  beforeAll(async () => {
    server = new DurableStreamTestServer({
      postgresUrl: POSTGRES_URL!,
      port: 0,
      longPollTimeout: 500,
      webhooks: true,
    })
    await server.start()
    config.baseUrl = server.url
  })

  afterAll(async () => {
    await server.stop()
  })

  runConformanceTests(config)
})
