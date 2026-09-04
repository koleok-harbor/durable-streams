/**
 * Postgres-backed Caddy conformance tests.
 *
 * These run only when a `POSTGRES_URL` environment variable is set, e.g.:
 *
 *   POSTGRES_URL=postgres://postgres:test@localhost:5433/ds \
 *     pnpm vitest run packages/caddy-plugin/test/postgres.conformance.test.ts
 */

import { spawn } from "node:child_process"
import * as path from "node:path"
import { afterAll, beforeAll, describe } from "vitest"
import { runConformanceTests } from "@durable-streams/server-conformance-tests"
import type { ChildProcess } from "node:child_process"

const POSTGRES_URL = process.env.POSTGRES_URL

describe.runIf(POSTGRES_URL)(`Caddy Postgres-Backed Implementation`, () => {
  let caddy: ChildProcess | null = null
  const port = 4437
  const config = { baseUrl: `http://localhost:${port}` }

  beforeAll(async () => {
    const caddyBinary = path.join(__dirname, `..`, `caddy`)
    const caddyfile = path.join(__dirname, `Caddyfile.postgres`)

    caddy = spawn(caddyBinary, [`run`, `--config`, caddyfile], {
      stdio: [`ignore`, `pipe`, `pipe`],
      env: { ...process.env, POSTGRES_URL },
    })

    caddy.stderr?.on(`data`, (data: Buffer) => {
      process.stderr.write(`[caddy] ${data.toString()}`)
    })

    await waitForServer(config.baseUrl, 10000)
  }, 15000)

  afterAll(async () => {
    if (caddy) {
      caddy.kill(`SIGTERM`)
      await new Promise((resolve) => setTimeout(resolve, 500))
    }
  })

  runConformanceTests(config)
})

async function waitForServer(
  baseUrl: string,
  timeoutMs: number
): Promise<void> {
  const start = Date.now()

  while (Date.now() - start < timeoutMs) {
    try {
      const response = await fetch(`${baseUrl}/v1/stream/__health__`, {
        method: `PUT`,
        headers: { "Content-Type": `text/plain` },
      })

      if (response.ok || response.status === 201) {
        await fetch(`${baseUrl}/v1/stream/__health__`, { method: `DELETE` })
        return
      }
    } catch {
      // Server not ready yet
    }

    await new Promise((resolve) => setTimeout(resolve, 100))
  }

  throw new Error(`Server did not become ready within ${timeoutMs}ms`)
}
