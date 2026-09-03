/**
 * Postgres-backed stream storage implementation.
 *
 * Mirrors the interface of the in-memory `StreamStore` and the
 * `FileBackedStreamStore` so it can be dropped into the server as a
 * drop-in replacement. Stream metadata lives in a `durable_streams` table
 * and message payloads in a `durable_stream_messages` table. Offsets are
 * byte-based (`0000000000000000_<byteOffset>`), matching the file store's
 * framing so byte-exact resumption holds.
 *
 * Concurrency: appends, closes, creates and deletes serialize per stream
 * using `SELECT ... FOR UPDATE` row locks inside a transaction, so the
 * read-modify-write of `current_offset` (and producer state) is atomic.
 */

import { Pool } from "pg"
import { serverLog } from "./log"
import {
  formatJsonMessages,
  normalizeContentType,
  processJsonAppend,
} from "./store"
import type { PoolClient } from "pg"
import type { AppendOptions, AppendResult } from "./store"
import type {
  PendingLongPoll,
  ProducerState,
  ProducerValidationResult,
  Stream,
  StreamMessage,
} from "./types"

const ZERO_OFFSET = `0000000000000000_0000000000000000`

/**
 * Frame overhead per message: 4-byte length prefix + 1-byte newline.
 * Matches the file store's framing so offsets are byte-exact.
 */
const FRAME_OVERHEAD = 5

/**
 * Serializable producer state stored in the `producers` jsonb column.
 */
interface SerializableProducerState {
  epoch: number
  lastSeq: number
  lastUpdated: number
}

/**
 * A row from the `durable_streams` table.
 */
interface StreamRow {
  path: string
  content_type: string | null
  current_offset: string
  last_seq: string | null
  ttl_seconds: number | null
  expires_at: string | null
  created_at: number
  last_accessed_at: number
  closed: boolean
  closed_by_producer_id: string | null
  closed_by_epoch: number | null
  closed_by_seq: number | null
  forked_from: string | null
  fork_offset: string | null
  fork_sub_offset: number | null
  ref_count: number
  soft_deleted: boolean
  producers: Record<string, SerializableProducerState>
}

export interface PostgresStreamStoreOptions {
  /**
   * Postgres connection string (e.g. `postgres://user:pass@host:5432/db`).
   */
  connectionString: string
  /**
   * Maximum number of clients in the pool. Default: 10.
   */
  max?: number
  /**
   * Optional schema to create the tables in. Default: `public`.
   */
  schema?: string
}

function byteToOffset(byte: number): string {
  return `0000000000000000_${String(byte).padStart(16, `0`)}`
}

function offsetToByte(offset: string): number {
  return Number(offset.split(`_`)[1] ?? 0)
}

/**
 * Postgres-backed implementation of the stream store.
 */
export class PostgresStreamStore {
  private pool: Pool
  private schema: string
  private pendingLongPolls: Array<PendingLongPoll> = []

  constructor(options: PostgresStreamStoreOptions) {
    this.schema = options.schema ?? `public`
    this.pool = new Pool({
      connectionString: options.connectionString,
      max: options.max ?? 10,
    })
    this.pool.on(`error`, (err: Error) => {
      serverLog.error(`[PostgresStreamStore] idle client error:`, err)
    })
  }

  /**
   * Create the schema if it does not exist. Call once at startup.
   */
  async init(): Promise<void> {
    const streams = `${this.schema}.durable_streams`
    const messages = `${this.schema}.durable_stream_messages`
    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS ${streams} (
        path TEXT PRIMARY KEY,
        content_type TEXT,
        current_offset TEXT NOT NULL,
        last_seq TEXT,
        ttl_seconds INTEGER,
        expires_at TEXT,
        created_at BIGINT NOT NULL,
        last_accessed_at BIGINT NOT NULL,
        closed BOOLEAN NOT NULL DEFAULT FALSE,
        closed_by_producer_id TEXT,
        closed_by_epoch INTEGER,
        closed_by_seq INTEGER,
        forked_from TEXT,
        fork_offset TEXT,
        fork_sub_offset INTEGER,
        ref_count INTEGER NOT NULL DEFAULT 0,
        soft_deleted BOOLEAN NOT NULL DEFAULT FALSE,
        producers JSONB NOT NULL DEFAULT '{}'::jsonb
      )
    `)
    await this.pool.query(`
      CREATE TABLE IF NOT EXISTS ${messages} (
        id BIGSERIAL PRIMARY KEY,
        stream_path TEXT NOT NULL REFERENCES ${streams}(path) ON DELETE CASCADE,
        data BYTEA NOT NULL,
        byte_offset BIGINT NOT NULL,
        timestamp BIGINT NOT NULL
      )
    `)
    await this.pool.query(`
      CREATE INDEX IF NOT EXISTS idx_dsm_stream_offset
      ON ${messages}(stream_path, byte_offset)
    `)
  }

  /**
   * Close the connection pool.
   */
  async close(): Promise<void> {
    await this.pool.end()
  }

  // ============================================================================
  // Row / Stream conversion
  // ============================================================================

  private rowToMeta(row: Record<string, unknown>): StreamRow {
    return {
      path: row.path as string,
      content_type: (row.content_type as string | null) ?? null,
      current_offset: row.current_offset as string,
      last_seq: (row.last_seq as string | null) ?? null,
      ttl_seconds: (row.ttl_seconds as number | null) ?? null,
      expires_at: (row.expires_at as string | null) ?? null,
      created_at: Number(row.created_at),
      last_accessed_at: Number(row.last_accessed_at),
      closed: Boolean(row.closed),
      closed_by_producer_id:
        (row.closed_by_producer_id as string | null) ?? null,
      closed_by_epoch: (row.closed_by_epoch as number | null) ?? null,
      closed_by_seq: (row.closed_by_seq as number | null) ?? null,
      forked_from: (row.forked_from as string | null) ?? null,
      fork_offset: (row.fork_offset as string | null) ?? null,
      fork_sub_offset: (row.fork_sub_offset as number | null) ?? null,
      ref_count: Number(row.ref_count ?? 0),
      soft_deleted: Boolean(row.soft_deleted),
      producers:
        (row.producers as Record<string, SerializableProducerState> | null) ??
        {},
    }
  }

  private metaToStream(meta: StreamRow): Stream {
    let producers: Map<string, ProducerState> | undefined
    const entries = Object.entries(meta.producers)
    if (entries.length > 0) {
      producers = new Map()
      for (const [id, state] of entries) {
        producers.set(id, { ...state })
      }
    }

    return {
      path: meta.path,
      contentType: meta.content_type ?? undefined,
      messages: [],
      currentOffset: meta.current_offset,
      lastSeq: meta.last_seq ?? undefined,
      ttlSeconds: meta.ttl_seconds ?? undefined,
      expiresAt: meta.expires_at ?? undefined,
      createdAt: meta.created_at,
      lastAccessedAt: meta.last_accessed_at,
      producers,
      closed: meta.closed,
      closedBy:
        meta.closed_by_producer_id !== null
          ? {
              producerId: meta.closed_by_producer_id,
              epoch: meta.closed_by_epoch!,
              seq: meta.closed_by_seq!,
            }
          : undefined,
      forkedFrom: meta.forked_from ?? undefined,
      forkOffset: meta.fork_offset ?? undefined,
      forkSubOffset: meta.fork_sub_offset ?? undefined,
      refCount: meta.ref_count,
      softDeleted: meta.soft_deleted,
    }
  }

  // ============================================================================
  // Expiry
  // ============================================================================

  private isExpired(meta: StreamRow): boolean {
    const now = Date.now()

    if (meta.expires_at) {
      const expiryTime = new Date(meta.expires_at).getTime()
      if (!Number.isFinite(expiryTime) || now >= expiryTime) {
        return true
      }
    }

    if (meta.ttl_seconds !== null) {
      const expiryTime = meta.last_accessed_at + meta.ttl_seconds * 1000
      if (now >= expiryTime) {
        return true
      }
    }

    return false
  }

  /**
   * Get stream metadata for a read (no row lock). Deletes expired streams.
   */
  private async getMetaIfNotExpired(
    path: string
  ): Promise<StreamRow | undefined> {
    const res = await this.pool.query(
      `SELECT * FROM ${this.schema}.durable_streams WHERE path = $1`,
      [path]
    )
    if (res.rows.length === 0) return undefined
    const meta = this.rowToMeta(res.rows[0])
    if (this.isExpired(meta)) {
      if (meta.ref_count > 0) {
        if (!meta.soft_deleted) {
          await this.pool.query(
            `UPDATE ${this.schema}.durable_streams SET soft_deleted = TRUE WHERE path = $1`,
            [path]
          )
          return { ...meta, soft_deleted: true }
        }
        return meta
      }
      await this.delete(path)
      return undefined
    }
    return meta
  }

  /**
   * Get stream metadata for a write, locking the row. Deletes expired streams.
   */
  private async getMetaForUpdate(
    client: PoolClient,
    path: string
  ): Promise<StreamRow | undefined> {
    const res = await client.query(
      `SELECT * FROM ${this.schema}.durable_streams WHERE path = $1 FOR UPDATE`,
      [path]
    )
    if (res.rows.length === 0) return undefined
    const meta = this.rowToMeta(res.rows[0])
    if (this.isExpired(meta)) {
      if (meta.ref_count > 0) {
        if (!meta.soft_deleted) {
          await client.query(
            `UPDATE ${this.schema}.durable_streams SET soft_deleted = TRUE WHERE path = $1`,
            [path]
          )
          return { ...meta, soft_deleted: true }
        }
        return meta
      }
      await this.deleteInTx(client, path)
      return undefined
    }
    return meta
  }

  // ============================================================================
  // Producer validation
  // ============================================================================

  private validateProducer(
    meta: StreamRow,
    producerId: string,
    epoch: number,
    seq: number
  ): ProducerValidationResult {
    const state = meta.producers[producerId]
    const now = Date.now()

    if (!state) {
      if (seq !== 0) {
        return {
          status: `sequence_gap`,
          expectedSeq: 0,
          receivedSeq: seq,
        }
      }
      return {
        status: `accepted`,
        isNew: true,
        producerId,
        proposedState: { epoch, lastSeq: 0, lastUpdated: now },
      }
    }

    if (epoch < state.epoch) {
      return { status: `stale_epoch`, currentEpoch: state.epoch }
    }

    if (epoch > state.epoch) {
      if (seq !== 0) {
        return { status: `invalid_epoch_seq` }
      }
      return {
        status: `accepted`,
        isNew: true,
        producerId,
        proposedState: { epoch, lastSeq: 0, lastUpdated: now },
      }
    }

    if (seq <= state.lastSeq) {
      return { status: `duplicate`, lastSeq: state.lastSeq }
    }

    if (seq === state.lastSeq + 1) {
      return {
        status: `accepted`,
        isNew: false,
        producerId,
        proposedState: { epoch, lastSeq: seq, lastUpdated: now },
      }
    }

    return {
      status: `sequence_gap`,
      expectedSeq: state.lastSeq + 1,
      receivedSeq: seq,
    }
  }

  // ============================================================================
  // Create
  // ============================================================================

  async create(
    path: string,
    options: {
      contentType?: string
      ttlSeconds?: number
      expiresAt?: string
      initialData?: Uint8Array
      closed?: boolean
      forkedFrom?: string
      forkOffset?: string
      forkSubOffset?: number
    } = {}
  ): Promise<Stream> {
    const client = await this.pool.connect()
    try {
      await client.query(`BEGIN`)

      const existingRes = await client.query(
        `SELECT * FROM ${this.schema}.durable_streams WHERE path = $1 FOR UPDATE`,
        [path]
      )
      if (existingRes.rows.length > 0) {
        const existing = this.rowToMeta(existingRes.rows[0])
        if (this.isExpired(existing)) {
          await this.deleteInTx(client, path)
        } else if (existing.soft_deleted) {
          throw new Error(
            `Stream has active forks — path cannot be reused until all forks are removed: ${path}`
          )
        } else {
          if (this.configMatches(existing, options)) {
            await client.query(`COMMIT`)
            return this.metaToStream(existing)
          }
          throw new Error(
            `Stream already exists with different configuration: ${path}`
          )
        }
      }

      // Fork handling
      const isFork = !!options.forkedFrom
      let forkOffset = ZERO_OFFSET
      let sourceContentType: string | undefined
      let sourceMeta: StreamRow | undefined
      let forkSubOffsetPrefix: Uint8Array | undefined

      if (isFork) {
        const sourceRes = await client.query(
          `SELECT * FROM ${this.schema}.durable_streams WHERE path = $1 FOR UPDATE`,
          [options.forkedFrom]
        )
        if (sourceRes.rows.length === 0) {
          throw new Error(`Source stream not found: ${options.forkedFrom}`)
        }
        sourceMeta = this.rowToMeta(sourceRes.rows[0])
        if (sourceMeta.soft_deleted) {
          throw new Error(
            `Source stream is soft-deleted: ${options.forkedFrom}`
          )
        }
        if (this.isExpired(sourceMeta)) {
          throw new Error(`Source stream not found: ${options.forkedFrom}`)
        }

        sourceContentType = sourceMeta.content_type ?? undefined

        if (
          options.contentType &&
          options.contentType.trim() !== `` &&
          normalizeContentType(options.contentType) !==
            normalizeContentType(sourceContentType)
        ) {
          throw new Error(`Content type mismatch with source stream`)
        }

        forkOffset = options.forkOffset ?? sourceMeta.current_offset

        if (
          forkOffset < ZERO_OFFSET ||
          sourceMeta.current_offset < forkOffset
        ) {
          throw new Error(`Invalid fork offset: ${forkOffset}`)
        }

        if (options.forkSubOffset && options.forkSubOffset > 0) {
          forkSubOffsetPrefix = await this.resolveForkSubOffset(
            client,
            options.forkedFrom!,
            forkOffset,
            options.forkSubOffset,
            normalizeContentType(sourceContentType) === `application/json`
          )
        }
      }

      let contentType = options.contentType
      if (!contentType || contentType.trim() === ``) {
        if (isFork) contentType = sourceContentType
      }

      let effectiveExpiresAt = options.expiresAt
      let effectiveTtlSeconds = options.ttlSeconds
      if (isFork) {
        const resolved = this.resolveForkExpiry(options, sourceMeta!)
        effectiveExpiresAt = resolved.expiresAt
        effectiveTtlSeconds = resolved.ttlSeconds
      }

      const now = Date.now()
      let currentOffset = isFork ? forkOffset : ZERO_OFFSET

      // Insert with closed=FALSE so the initial append below is not rejected.
      await client.query(
        `INSERT INTO ${this.schema}.durable_streams
          (path, content_type, current_offset, last_seq, ttl_seconds, expires_at,
           created_at, last_accessed_at, closed, forked_from, fork_offset,
           fork_sub_offset, ref_count, soft_deleted, producers)
         VALUES ($1,$2,$3,NULL,$4,$5,$6,$7,FALSE,$8,$9,$10,0,FALSE,'{}'::jsonb)`,
        [
          path,
          contentType ?? null,
          currentOffset,
          effectiveTtlSeconds ?? null,
          effectiveExpiresAt ?? null,
          now,
          now,
          isFork ? options.forkedFrom : null,
          isFork ? forkOffset : null,
          options.forkSubOffset ?? null,
        ]
      )

      // Increment the source's refCount (the fork itself has refCount 0).
      if (isFork) {
        await client.query(
          `UPDATE ${this.schema}.durable_streams SET ref_count = ref_count + 1 WHERE path = $1`,
          [options.forkedFrom]
        )
      }

      // Materialize the sub-offset prefix as the fork's first own message.
      if (forkSubOffsetPrefix && forkSubOffsetPrefix.length > 0) {
        const newByte =
          offsetToByte(currentOffset) +
          forkSubOffsetPrefix.length +
          FRAME_OVERHEAD
        const newOffset = byteToOffset(newByte)
        await client.query(
          `INSERT INTO ${this.schema}.durable_stream_messages
            (stream_path, data, byte_offset, timestamp) VALUES ($1,$2,$3,$4)`,
          [path, Buffer.from(forkSubOffsetPrefix), newByte, now]
        )
        currentOffset = newOffset
        await client.query(
          `UPDATE ${this.schema}.durable_streams SET current_offset = $1 WHERE path = $2`,
          [newOffset, path]
        )
      }

      // Append initial data if provided.
      if (options.initialData && options.initialData.length > 0) {
        const appended = await this.appendInTx(
          client,
          path,
          options.initialData,
          {
            contentType: options.contentType,
            isInitialCreate: true,
          }
        )
        if (appended && typeof appended === `object` && `offset` in appended) {
          currentOffset = appended.offset
        }
      }

      // Set closed after the initial append succeeded.
      if (options.closed) {
        await client.query(
          `UPDATE ${this.schema}.durable_streams SET closed = TRUE WHERE path = $1`,
          [path]
        )
      }

      await client.query(`COMMIT`)

      const finalRes = await client.query(
        `SELECT * FROM ${this.schema}.durable_streams WHERE path = $1`,
        [path]
      )
      return this.metaToStream(this.rowToMeta(finalRes.rows[0]))
    } catch (err) {
      await client.query(`ROLLBACK`).catch(() => {})
      throw err
    } finally {
      client.release()
    }
  }

  private configMatches(
    existing: StreamRow,
    options: {
      contentType?: string
      ttlSeconds?: number
      expiresAt?: string
      closed?: boolean
      forkedFrom?: string
      forkOffset?: string
      forkSubOffset?: number
    }
  ): boolean {
    const normalizeMimeType = (ct: string | undefined) =>
      (ct ?? `application/octet-stream`).toLowerCase()
    const contentTypeMatches =
      normalizeMimeType(options.contentType) ===
      normalizeMimeType(existing.content_type ?? undefined)
    const ttlMatches = (options.ttlSeconds ?? null) === existing.ttl_seconds
    const expiresMatches = (options.expiresAt ?? null) === existing.expires_at
    const closedMatches = (options.closed ?? false) === existing.closed
    const forkedFromMatches =
      (options.forkedFrom ?? undefined) === (existing.forked_from ?? undefined)
    const forkOffsetMatches =
      options.forkOffset === undefined ||
      options.forkOffset === existing.fork_offset
    const requestedSub = options.forkSubOffset ?? 0
    const existingSub = existing.fork_sub_offset ?? 0
    const forkSubOffsetMatches = requestedSub === existingSub

    return (
      contentTypeMatches &&
      ttlMatches &&
      expiresMatches &&
      closedMatches &&
      forkedFromMatches &&
      forkOffsetMatches &&
      forkSubOffsetMatches
    )
  }

  private resolveForkExpiry(
    opts: { ttlSeconds?: number; expiresAt?: string },
    sourceMeta: StreamRow
  ): { ttlSeconds?: number; expiresAt?: string } {
    if (opts.ttlSeconds !== undefined) {
      return { ttlSeconds: opts.ttlSeconds }
    }
    if (opts.expiresAt) {
      return { expiresAt: opts.expiresAt }
    }
    if (sourceMeta.ttl_seconds !== null) {
      return { ttlSeconds: sourceMeta.ttl_seconds }
    }
    if (sourceMeta.expires_at) {
      return { expiresAt: sourceMeta.expires_at }
    }
    return {}
  }

  // ============================================================================
  // Read helpers
  // ============================================================================

  private async readOwnMessages(
    client: PoolClient | undefined,
    path: string,
    startByte: number
  ): Promise<Array<StreamMessage>> {
    const res = await (client
      ? client.query(
          `SELECT data, byte_offset, timestamp FROM ${this.schema}.durable_stream_messages
           WHERE stream_path = $1 AND byte_offset > $2 ORDER BY byte_offset`,
          [path, startByte]
        )
      : this.pool.query(
          `SELECT data, byte_offset, timestamp FROM ${this.schema}.durable_stream_messages
           WHERE stream_path = $1 AND byte_offset > $2 ORDER BY byte_offset`,
          [path, startByte]
        ))
    return res.rows.map(
      (r: { data: Buffer; byte_offset: number; timestamp: number }) => ({
        data: new Uint8Array(r.data),
        offset: byteToOffset(Number(r.byte_offset)),
        timestamp: Number(r.timestamp),
      })
    )
  }

  /**
   * Recursively read messages from a fork's source chain, capped at capByte.
   */
  private async readForkedMessages(
    client: PoolClient | undefined,
    sourcePath: string,
    startByte: number,
    capByte: number
  ): Promise<Array<StreamMessage>> {
    const res = await (client
      ? client.query(
          `SELECT * FROM ${this.schema}.durable_streams WHERE path = $1`,
          [sourcePath]
        )
      : this.pool.query(
          `SELECT * FROM ${this.schema}.durable_streams WHERE path = $1`,
          [sourcePath]
        ))
    if (res.rows.length === 0) return []
    const sourceMeta = this.rowToMeta(res.rows[0])

    const messages: Array<StreamMessage> = []

    if (sourceMeta.forked_from && sourceMeta.fork_offset) {
      const sourceForkByte = offsetToByte(sourceMeta.fork_offset)
      if (startByte < sourceForkByte) {
        const inheritedCap = Math.min(sourceForkByte, capByte)
        const inherited = await this.readForkedMessages(
          client,
          sourceMeta.forked_from,
          startByte,
          inheritedCap
        )
        messages.push(...inherited)
      }
    }

    const own = await (client
      ? client.query(
          `SELECT data, byte_offset, timestamp FROM ${this.schema}.durable_stream_messages
           WHERE stream_path = $1 AND byte_offset > $2 AND byte_offset <= $3 ORDER BY byte_offset`,
          [sourcePath, startByte, capByte]
        )
      : this.pool.query(
          `SELECT data, byte_offset, timestamp FROM ${this.schema}.durable_stream_messages
           WHERE stream_path = $1 AND byte_offset > $2 AND byte_offset <= $3 ORDER BY byte_offset`,
          [sourcePath, startByte, capByte]
        ))
    for (const r of own.rows) {
      messages.push({
        data: new Uint8Array(r.data as Buffer),
        offset: byteToOffset(Number(r.byte_offset)),
        timestamp: Number(r.timestamp),
      })
    }

    return messages
  }

  private async resolveForkSubOffset(
    client: PoolClient,
    sourcePath: string,
    forkOffset: string,
    subOffset: number,
    isJSON: boolean
  ): Promise<Uint8Array> {
    const forkByte = offsetToByte(forkOffset)
    const sourceRes = await client.query(
      `SELECT * FROM ${this.schema}.durable_streams WHERE path = $1`,
      [sourcePath]
    )
    if (sourceRes.rows.length === 0) {
      throw new Error(`Source stream not found: ${sourcePath}`)
    }
    const sourceMeta = this.rowToMeta(sourceRes.rows[0])
    const currentByte = offsetToByte(sourceMeta.current_offset)
    const messages = await this.readForkedMessages(
      client,
      sourcePath,
      forkByte,
      currentByte
    )
    if (messages.length === 0) {
      throw new Error(`Invalid fork sub-offset: no data past forkOffset`)
    }
    const first = messages[0]!
    if (isJSON) {
      const text = new TextDecoder().decode(first.data)
      const trimmed = text.endsWith(`,`) ? text.slice(0, -1) : text
      let values: Array<unknown>
      try {
        values = JSON.parse(`[${trimmed}]`)
      } catch {
        throw new Error(`Invalid fork sub-offset: source JSON is unparseable`)
      }
      if (subOffset > values.length) {
        throw new Error(
          `Invalid fork sub-offset: overshoots source message count`
        )
      }
      const prefix = values.slice(0, subOffset).map((v) => JSON.stringify(v))
      return new TextEncoder().encode(prefix.join(`,`) + `,`)
    }
    if (subOffset > first.data.length) {
      throw new Error(
        `Invalid fork sub-offset: overshoots source message length`
      )
    }
    return first.data.slice(0, subOffset)
  }

  // ============================================================================
  // Append
  // ============================================================================

  async append(
    path: string,
    data: Uint8Array,
    options: AppendOptions & { isInitialCreate?: boolean } = {}
  ): Promise<StreamMessage | AppendResult | null> {
    const client = await this.pool.connect()
    try {
      await client.query(`BEGIN`)
      const result = await this.appendInTx(client, path, data, options)
      await client.query(`COMMIT`)
      // Await the read-based notification so pending long-polls are resolved
      // with the new messages BEFORE the close notification (which resolves
      // with empty). Otherwise an append-and-close could wake readers with an
      // empty close signal and lose the final data.
      await this.notifyLongPolls(path)
      if (options.close) this.notifyLongPollsClosed(path)
      return result
    } catch (err) {
      await client.query(`ROLLBACK`).catch(() => {})
      throw err
    } finally {
      client.release()
    }
  }

  private async appendInTx(
    client: PoolClient,
    path: string,
    data: Uint8Array,
    options: AppendOptions & { isInitialCreate?: boolean } = {}
  ): Promise<StreamMessage | AppendResult | null> {
    const meta = await this.getMetaForUpdate(client, path)
    if (!meta) {
      throw new Error(`Stream not found: ${path}`)
    }
    if (meta.soft_deleted) {
      throw new Error(`Stream is soft-deleted: ${path}`)
    }

    if (meta.closed) {
      if (
        options.producerId &&
        meta.closed_by_producer_id === options.producerId &&
        meta.closed_by_epoch === options.producerEpoch &&
        meta.closed_by_seq === options.producerSeq
      ) {
        return {
          message: null,
          streamClosed: true,
          producerResult: {
            status: `duplicate`,
            lastSeq: options.producerSeq,
          },
        }
      }
      return { message: null, streamClosed: true }
    }

    if (options.contentType && meta.content_type) {
      if (
        normalizeContentType(options.contentType) !==
        normalizeContentType(meta.content_type)
      ) {
        throw new Error(
          `Content-type mismatch: expected ${meta.content_type}, got ${options.contentType}`
        )
      }
    }

    let producerResult: ProducerValidationResult | undefined
    if (
      options.producerId !== undefined &&
      options.producerEpoch !== undefined &&
      options.producerSeq !== undefined
    ) {
      producerResult = this.validateProducer(
        meta,
        options.producerId,
        options.producerEpoch,
        options.producerSeq
      )
      if (producerResult.status !== `accepted`) {
        return { message: null, producerResult }
      }
    }

    if (options.seq !== undefined) {
      if (meta.last_seq != null && options.seq <= meta.last_seq) {
        throw new Error(`Sequence conflict: ${options.seq} <= ${meta.last_seq}`)
      }
    }

    let processedData = data
    if (
      normalizeContentType(meta.content_type ?? undefined) ===
      `application/json`
    ) {
      processedData = processJsonAppend(data, options.isInitialCreate ?? false)
      if (processedData.length === 0) {
        return null
      }
    }

    const currentByte = offsetToByte(meta.current_offset)
    const newByte = currentByte + processedData.length + FRAME_OVERHEAD
    const newOffset = byteToOffset(newByte)
    const now = Date.now()

    await client.query(
      `INSERT INTO ${this.schema}.durable_stream_messages
        (stream_path, data, byte_offset, timestamp) VALUES ($1,$2,$3,$4)`,
      [path, Buffer.from(processedData), newByte, now]
    )

    const producers = { ...meta.producers }
    if (producerResult && producerResult.status === `accepted`) {
      producers[producerResult.producerId] = producerResult.proposedState
    }

    let closedBy: { producerId: string; epoch: number; seq: number } | undefined
    if (options.close && options.producerId) {
      closedBy = {
        producerId: options.producerId,
        epoch: options.producerEpoch!,
        seq: options.producerSeq!,
      }
    }

    await client.query(
      `UPDATE ${this.schema}.durable_streams
        SET current_offset = $1, last_seq = $2, producers = $3, closed = $4,
            closed_by_producer_id = $5, closed_by_epoch = $6, closed_by_seq = $7
        WHERE path = $8`,
      [
        newOffset,
        options.seq ?? meta.last_seq ?? undefined,
        JSON.stringify(producers),
        options.close ? true : meta.closed,
        closedBy?.producerId ?? null,
        closedBy?.epoch ?? null,
        closedBy?.seq ?? null,
        path,
      ]
    )

    const message: StreamMessage = {
      data: processedData,
      offset: newOffset,
      timestamp: now,
    }

    if (producerResult || options.close) {
      return {
        message,
        producerResult,
        streamClosed: options.close,
      }
    }
    return message
  }

  /**
   * Append with producer serialization. The per-stream row lock already
   * serializes validation+append atomically, so this is equivalent to append.
   */
  async appendWithProducer(
    path: string,
    data: Uint8Array,
    options: AppendOptions
  ): Promise<AppendResult> {
    const result = await this.append(path, data, options)
    if (result && `message` in result) {
      return result
    }
    return { message: result }
  }

  // ============================================================================
  // Close
  // ============================================================================

  async closeStream(
    path: string
  ): Promise<{ finalOffset: string; alreadyClosed: boolean } | null> {
    const client = await this.pool.connect()
    try {
      await client.query(`BEGIN`)
      const meta = await this.getMetaForUpdate(client, path)
      if (!meta) {
        await client.query(`COMMIT`)
        return null
      }
      const alreadyClosed = meta.closed
      await client.query(
        `UPDATE ${this.schema}.durable_streams SET closed = TRUE WHERE path = $1`,
        [path]
      )
      await client.query(`COMMIT`)
      this.notifyLongPollsClosed(path)
      return { finalOffset: meta.current_offset, alreadyClosed }
    } catch (err) {
      await client.query(`ROLLBACK`).catch(() => {})
      throw err
    } finally {
      client.release()
    }
  }

  async closeStreamWithProducer(
    path: string,
    options: {
      producerId: string
      producerEpoch: number
      producerSeq: number
    }
  ): Promise<{
    finalOffset: string
    alreadyClosed: boolean
    producerResult?: ProducerValidationResult
  } | null> {
    const client = await this.pool.connect()
    try {
      await client.query(`BEGIN`)
      const meta = await this.getMetaForUpdate(client, path)
      if (!meta) {
        await client.query(`COMMIT`)
        return null
      }

      if (meta.closed) {
        if (
          meta.closed_by_producer_id === options.producerId &&
          meta.closed_by_epoch === options.producerEpoch &&
          meta.closed_by_seq === options.producerSeq
        ) {
          await client.query(`COMMIT`)
          return {
            finalOffset: meta.current_offset,
            alreadyClosed: true,
            producerResult: {
              status: `duplicate`,
              lastSeq: options.producerSeq,
            },
          }
        }
        await client.query(`COMMIT`)
        return {
          finalOffset: meta.current_offset,
          alreadyClosed: true,
          producerResult: { status: `stream_closed` },
        }
      }

      const producerResult = this.validateProducer(
        meta,
        options.producerId,
        options.producerEpoch,
        options.producerSeq
      )
      if (producerResult.status !== `accepted`) {
        await client.query(`COMMIT`)
        return {
          finalOffset: meta.current_offset,
          alreadyClosed: meta.closed,
          producerResult,
        }
      }

      const producers = { ...meta.producers }
      producers[producerResult.producerId] = producerResult.proposedState

      await client.query(
        `UPDATE ${this.schema}.durable_streams
          SET closed = TRUE, closed_by_producer_id = $1, closed_by_epoch = $2,
              closed_by_seq = $3, producers = $4
          WHERE path = $5`,
        [
          options.producerId,
          options.producerEpoch,
          options.producerSeq,
          JSON.stringify(producers),
          path,
        ]
      )
      await client.query(`COMMIT`)
      this.notifyLongPollsClosed(path)
      return {
        finalOffset: meta.current_offset,
        alreadyClosed: false,
        producerResult,
      }
    } catch (err) {
      await client.query(`ROLLBACK`).catch(() => {})
      throw err
    } finally {
      client.release()
    }
  }

  // ============================================================================
  // Read
  // ============================================================================

  async read(
    path: string,
    offset?: string
  ): Promise<{ messages: Array<StreamMessage>; upToDate: boolean }> {
    const meta = await this.getMetaIfNotExpired(path)
    if (!meta) {
      throw new Error(`Stream not found: ${path}`)
    }

    const startOffset = offset ?? ZERO_OFFSET
    const startByte = offsetToByte(startOffset)
    const currentByte = offsetToByte(meta.current_offset)

    if (meta.current_offset === ZERO_OFFSET) {
      return { messages: [], upToDate: true }
    }
    if (startByte >= currentByte) {
      return { messages: [], upToDate: true }
    }

    const messages: Array<StreamMessage> = []

    if (meta.forked_from && meta.fork_offset) {
      const forkByte = offsetToByte(meta.fork_offset)
      if (startByte < forkByte) {
        const inherited = await this.readForkedMessages(
          undefined,
          meta.forked_from,
          startByte,
          forkByte
        )
        messages.push(...inherited)
      }
      const own = await this.readOwnMessages(undefined, path, startByte)
      messages.push(...own)
    } else {
      const own = await this.readOwnMessages(undefined, path, startByte)
      messages.push(...own)
    }

    return { messages, upToDate: true }
  }

  async waitForMessages(
    path: string,
    offset: string,
    timeoutMs: number
  ): Promise<{
    messages: Array<StreamMessage>
    timedOut: boolean
    streamClosed?: boolean
  }> {
    const meta = await this.getMetaIfNotExpired(path)
    if (!meta) {
      throw new Error(`Stream not found: ${path}`)
    }

    if (meta.forked_from && meta.fork_offset && offset < meta.fork_offset) {
      const { messages } = await this.read(path, offset)
      return { messages, timedOut: false }
    }

    if (meta.closed && offset === meta.current_offset) {
      return { messages: [], timedOut: false, streamClosed: true }
    }

    const { messages } = await this.read(path, offset)
    if (messages.length > 0) {
      return { messages, timedOut: false, streamClosed: meta.closed }
    }

    if (meta.closed) {
      return { messages: [], timedOut: false, streamClosed: true }
    }

    return new Promise((resolve) => {
      const timeoutId = setTimeout(() => {
        this.removePendingLongPoll(pending)
        this.getMetaIfNotExpired(path)
          .then((currentMeta) => {
            resolve({
              messages: [],
              timedOut: true,
              streamClosed: currentMeta?.closed,
            })
          })
          .catch(() => {
            resolve({ messages: [], timedOut: true })
          })
      }, timeoutMs)

      const pending: PendingLongPoll = {
        path,
        offset,
        resolve: (msgs) => {
          clearTimeout(timeoutId)
          this.removePendingLongPoll(pending)
          this.getMetaIfNotExpired(path)
            .then((currentMeta) => {
              resolve({
                messages: msgs,
                timedOut: false,
                streamClosed: currentMeta?.closed,
              })
            })
            .catch(() => {
              resolve({ messages: msgs, timedOut: false })
            })
        },
        timeoutId,
      }

      this.pendingLongPolls.push(pending)
    })
  }

  async formatResponse(
    path: string,
    messages: Array<StreamMessage>
  ): Promise<Uint8Array> {
    const meta = await this.getMetaIfNotExpired(path)
    if (!meta) {
      throw new Error(`Stream not found: ${path}`)
    }

    if (
      normalizeContentType(meta.content_type ?? undefined) ===
      `application/json`
    ) {
      return formatJsonMessages(messages)
    }

    const totalSize = messages.reduce((sum, m) => sum + m.data.length, 0)
    const concatenated = new Uint8Array(totalSize)
    let offset = 0
    for (const msg of messages) {
      concatenated.set(msg.data, offset)
      offset += msg.data.length
    }
    return concatenated
  }

  // ============================================================================
  // Metadata accessors
  // ============================================================================

  async get(path: string): Promise<Stream | undefined> {
    const meta = await this.getMetaIfNotExpired(path)
    if (!meta) return undefined
    return this.metaToStream(meta)
  }

  async has(path: string): Promise<boolean> {
    const meta = await this.getMetaIfNotExpired(path)
    if (!meta) return false
    if (meta.soft_deleted) return false
    return true
  }

  async getCurrentOffset(path: string): Promise<string | undefined> {
    const meta = await this.getMetaIfNotExpired(path)
    return meta?.current_offset
  }

  async getProducerEpoch(
    path: string,
    producerId: string
  ): Promise<number | undefined> {
    const meta = await this.getMetaIfNotExpired(path)
    return meta?.producers[producerId]?.epoch
  }

  async touchAccess(path: string): Promise<void> {
    await this.pool.query(
      `UPDATE ${this.schema}.durable_streams SET last_accessed_at = $1 WHERE path = $2`,
      [Date.now(), path]
    )
  }

  // ============================================================================
  // Delete
  // ============================================================================

  async delete(path: string): Promise<boolean> {
    const client = await this.pool.connect()
    try {
      await client.query(`BEGIN`)
      const result = await this.deleteInTx(client, path)
      await client.query(`COMMIT`)
      this.cancelLongPollsForStream(path)
      return result
    } catch (err) {
      await client.query(`ROLLBACK`).catch(() => {})
      throw err
    } finally {
      client.release()
    }
  }

  private async deleteInTx(client: PoolClient, path: string): Promise<boolean> {
    const res = await client.query(
      `SELECT * FROM ${this.schema}.durable_streams WHERE path = $1 FOR UPDATE`,
      [path]
    )
    if (res.rows.length === 0) return false
    const meta = this.rowToMeta(res.rows[0])

    if (meta.soft_deleted) return true

    if (meta.ref_count > 0) {
      await client.query(
        `UPDATE ${this.schema}.durable_streams SET soft_deleted = TRUE WHERE path = $1`,
        [path]
      )
      return true
    }

    await this.deleteWithCascadeInTx(client, path)
    return true
  }

  private async deleteWithCascadeInTx(
    client: PoolClient,
    path: string
  ): Promise<void> {
    const res = await client.query(
      `SELECT * FROM ${this.schema}.durable_streams WHERE path = $1 FOR UPDATE`,
      [path]
    )
    if (res.rows.length === 0) return
    const meta = this.rowToMeta(res.rows[0])
    const forkedFrom = meta.forked_from

    // DELETE cascades to messages via the FK.
    await client.query(
      `DELETE FROM ${this.schema}.durable_streams WHERE path = $1`,
      [path]
    )

    if (forkedFrom) {
      const parentRes = await client.query(
        `SELECT * FROM ${this.schema}.durable_streams WHERE path = $1 FOR UPDATE`,
        [forkedFrom]
      )
      if (parentRes.rows.length > 0) {
        const parent = this.rowToMeta(parentRes.rows[0])
        const refCount = Math.max(0, parent.ref_count - 1)
        await client.query(
          `UPDATE ${this.schema}.durable_streams SET ref_count = $1 WHERE path = $2`,
          [refCount, forkedFrom]
        )
        if (refCount === 0 && parent.soft_deleted) {
          await this.deleteWithCascadeInTx(client, forkedFrom)
        }
      }
    }
  }

  // ============================================================================
  // Lifecycle / misc
  // ============================================================================

  async clear(): Promise<void> {
    for (const pending of this.pendingLongPolls) {
      clearTimeout(pending.timeoutId)
      pending.resolve([])
    }
    this.pendingLongPolls = []
    await this.pool.query(`DELETE FROM ${this.schema}.durable_stream_messages`)
    await this.pool.query(`DELETE FROM ${this.schema}.durable_streams`)
  }

  cancelAllWaits(): void {
    for (const pending of this.pendingLongPolls) {
      clearTimeout(pending.timeoutId)
      pending.resolve([])
    }
    this.pendingLongPolls = []
  }

  async list(): Promise<Array<string>> {
    const res = await this.pool.query(
      `SELECT path FROM ${this.schema}.durable_streams`
    )
    return res.rows.map((r: { path: string }) => r.path)
  }

  // ============================================================================
  // Long-poll helpers
  // ============================================================================

  private async notifyLongPolls(path: string): Promise<void> {
    const toNotify = this.pendingLongPolls.filter((p) => p.path === path)
    for (const pending of toNotify) {
      const { messages } = await this.read(path, pending.offset)
      if (messages.length > 0) {
        pending.resolve(messages)
      }
    }
  }

  private notifyLongPollsClosed(path: string): void {
    const toNotify = this.pendingLongPolls.filter((p) => p.path === path)
    for (const pending of toNotify) {
      pending.resolve([])
    }
  }

  private cancelLongPollsForStream(path: string): void {
    const toCancel = this.pendingLongPolls.filter((p) => p.path === path)
    for (const pending of toCancel) {
      clearTimeout(pending.timeoutId)
      pending.resolve([])
    }
    this.pendingLongPolls = this.pendingLongPolls.filter((p) => p.path !== path)
  }

  private removePendingLongPoll(pending: PendingLongPoll): void {
    const index = this.pendingLongPolls.indexOf(pending)
    if (index !== -1) {
      this.pendingLongPolls.splice(index, 1)
    }
  }
}
