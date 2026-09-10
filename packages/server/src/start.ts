#!/usr/bin/env node
/**
 * Standalone server entrypoint for running the dev server.
 */

import { DurableStreamTestServer } from "./server"

const postgresUrl = process.env.POSTGRES_URL
const dataDir = process.env.DATA_DIR
const port = process.env.PORT ? Number(process.env.PORT) : 3000
const host = process.env.HOST ?? `0.0.0.0`
const longPollTimeout = process.env.LONG_POLL_TIMEOUT
  ? Number(process.env.LONG_POLL_TIMEOUT)
  : 30_000
const webhooks = process.env.WEBHOOKS === `true`

const server = new DurableStreamTestServer({
  postgresUrl,
  dataDir,
  port,
  host,
  longPollTimeout,
  webhooks,
})

async function start(): Promise<void> {
  await server.start()

  const backend = postgresUrl
    ? `postgres`
    : dataDir
      ? `file-backed (${dataDir})`
      : `in-memory`

  console.log(`Durable Streams dev server running at ${server.url}`)
  console.log(`Storage backend: ${backend}`)
  console.log(`Webhooks: ${webhooks ? `enabled` : `disabled`}`)
  console.log(`\nPress Ctrl+C to stop`)
}

async function shutdown(): Promise<void> {
  console.log(`\nShutting down...`)
  await server.stop()
  process.exit(0)
}

process.on(`SIGINT`, shutdown)
process.on(`SIGTERM`, shutdown)

start().catch((err) => {
  console.error(`Failed to start server:`, err)
  process.exit(1)
})
