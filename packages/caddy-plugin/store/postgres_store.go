package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Frame overhead per message: 4-byte length prefix + 1-byte newline.
// Matches the file store's framing so offsets are byte-exact.
const postgresFrameOverhead = 5

// pgProducerState is the serializable producer state stored in the `producers`
// jsonb column.
type pgProducerState struct {
	Epoch       int64 `json:"epoch"`
	LastSeq     int64 `json:"last_seq"`
	LastUpdated int64 `json:"last_updated"`
}

// streamRow is a row from the `durable_streams` table.
type streamRow struct {
	Path                string
	ContentType         *string
	CurrentOffset       string
	LastSeq             *string
	TTLSeconds          *int64
	ExpiresAt           *string
	CreatedAt           int64
	LastAccessedAt      int64
	Closed              bool
	ClosedBy            *ClosedByProducer
	ForkedFrom          *string
	ForkOffset          *string
	ForkOffsetRequested *string
	ForkSubOffset       *int64
	RefCount            int32
	SoftDeleted         bool
	Producers           map[string]pgProducerState
}

// PostgresStoreConfig contains configuration for the postgres store.
type PostgresStoreConfig struct {
	ConnectionString string
	Max              int
	Schema           string
}

// PostgresStore is a Postgres-backed implementation of the Store interface.
//
// Stream metadata lives in a `durable_streams` table and message payloads in a
// `durable_stream_messages` table. Offsets are byte-based
// (`0000000000000000_<byteOffset>`), matching the file store's framing so
// byte-exact resumption holds.
//
// Concurrency: appends, closes, creates and deletes serialize per stream using
// `SELECT ... FOR UPDATE` row locks inside a transaction, so the
// read-modify-write of `current_offset` (and producer state) is atomic.
type PostgresStore struct {
	pool            *pgxpool.Pool
	schema          string
	longPoll        *longPollManager
	producerLocks   map[string]*sync.Mutex
	producerLocksMu sync.Mutex
}

// queryer is satisfied by both *pgxpool.Pool and pgx.Tx so helpers can run
// against either a pooled connection or an in-flight transaction.
type queryer interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// NewPostgresStore creates a new Postgres-backed store and initializes the
// schema.
func NewPostgresStore(cfg PostgresStoreConfig) (*PostgresStore, error) {
	if cfg.ConnectionString == "" {
		return nil, fmt.Errorf("connection string is required")
	}
	schema := cfg.Schema
	if schema == "" {
		schema = "public"
	}
	max := cfg.Max
	if max <= 0 {
		max = 10
	}

	poolCfg, err := pgxpool.ParseConfig(cfg.ConnectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse postgres connection string: %w", err)
	}
	poolCfg.MaxConns = int32(max)

	pool, err := pgxpool.NewWithConfig(context.Background(), poolCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create postgres pool: %w", err)
	}

	s := &PostgresStore{
		pool:          pool,
		schema:        schema,
		longPoll:      &longPollManager{waiters: make(map[string][]chan struct{})},
		producerLocks: make(map[string]*sync.Mutex),
	}

	if err := s.init(context.Background()); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to initialize postgres schema: %w", err)
	}

	return s, nil
}

// init creates the schema if it does not exist. Call once at startup.
func (s *PostgresStore) init(ctx context.Context) error {
	streams := s.schema + ".durable_streams"
	messages := s.schema + ".durable_stream_messages"

	if _, err := s.pool.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS `+s.schema); err != nil {
		return err
	}

	if _, err := s.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS `+streams+` (
			path TEXT PRIMARY KEY,
			content_type TEXT,
			current_offset TEXT NOT NULL,
			last_seq TEXT,
			ttl_seconds BIGINT,
			expires_at TEXT,
			created_at BIGINT NOT NULL,
			last_accessed_at BIGINT NOT NULL,
			closed BOOLEAN NOT NULL DEFAULT FALSE,
			closed_by_producer_id TEXT,
			closed_by_epoch BIGINT,
			closed_by_seq BIGINT,
			forked_from TEXT,
			fork_offset TEXT,
			fork_offset_requested TEXT,
			fork_sub_offset BIGINT,
			ref_count INTEGER NOT NULL DEFAULT 0,
			soft_deleted BOOLEAN NOT NULL DEFAULT FALSE,
			producers JSONB NOT NULL DEFAULT '{}'::jsonb
		)`); err != nil {
		return err
	}

	if _, err := s.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS `+messages+` (
			id BIGSERIAL PRIMARY KEY,
			stream_path TEXT NOT NULL REFERENCES `+streams+`(path) ON DELETE CASCADE,
			data BYTEA NOT NULL,
			byte_offset BIGINT NOT NULL,
			timestamp BIGINT NOT NULL
		)`); err != nil {
		return err
	}

	_, err := s.pool.Exec(ctx, `
		CREATE INDEX IF NOT EXISTS idx_dsm_stream_offset
		ON `+messages+`(stream_path, byte_offset)`)
	return err
}

// Close releases the connection pool.
func (s *PostgresStore) Close() error {
	s.pool.Close()
	return nil
}

// ============================================================================
// Row helpers
// ============================================================================

func byteToOffset(byte int64) Offset {
	return Offset{ReadSeq: 0, ByteOffset: uint64(byte)}
}

func offsetToByte(offset Offset) int64 {
	return int64(offset.ByteOffset)
}

func (r *streamRow) ContentTypeStr() string {
	if r.ContentType == nil {
		return "application/octet-stream"
	}
	return *r.ContentType
}

func (r *streamRow) CurrentOffsetOffset() Offset {
	o, _ := ParseOffset(r.CurrentOffset)
	return o
}

// ForkOffsetOffset parses the stored fork offset, returning false if unset or
// unparseable.
func (r *streamRow) ForkOffsetOffset() (Offset, bool) {
	if r.ForkOffset == nil {
		return Offset{}, false
	}
	o, err := ParseOffset(*r.ForkOffset)
	if err != nil {
		return Offset{}, false
	}
	return o, true
}

func (r *streamRow) toMeta() *StreamMetadata {
	meta := &StreamMetadata{
		Path:           r.Path,
		ContentType:    r.ContentTypeStr(),
		CurrentOffset:  r.CurrentOffsetOffset(),
		TTLSeconds:     r.TTLSeconds,
		CreatedAt:      time.Unix(r.CreatedAt, 0),
		LastAccessedAt: time.Unix(r.LastAccessedAt, 0),
		Closed:         r.Closed,
		ClosedBy:       r.ClosedBy,
		RefCount:       r.RefCount,
		SoftDeleted:    r.SoftDeleted,
	}
	if r.LastSeq != nil {
		meta.LastSeq = *r.LastSeq
	}
	if r.ExpiresAt != nil {
		if t, err := time.Parse(time.RFC3339, *r.ExpiresAt); err == nil {
			meta.ExpiresAt = &t
		}
	}
	if r.ForkedFrom != nil {
		meta.ForkedFrom = *r.ForkedFrom
	}
	if r.ForkOffset != nil {
		if o, err := ParseOffset(*r.ForkOffset); err == nil {
			meta.ForkOffset = o
		}
	}
	if r.ForkOffsetRequested != nil {
		if o, err := ParseOffset(*r.ForkOffsetRequested); err == nil {
			meta.ForkOffsetRequested = &o
		}
	}
	if r.ForkSubOffset != nil {
		meta.ForkSubOffset = uint64(*r.ForkSubOffset)
	}
	if len(r.Producers) > 0 {
		meta.Producers = make(map[string]*ProducerState, len(r.Producers))
		for id, st := range r.Producers {
			meta.Producers[id] = &ProducerState{
				Epoch:       st.Epoch,
				LastSeq:     st.LastSeq,
				LastUpdated: st.LastUpdated,
			}
		}
	}
	return meta
}

func (s *PostgresStore) scanRow(row pgx.Row) (*streamRow, error) {
	var r streamRow
	var contentType, lastSeq, expiresAt, closedByPID, forkedFrom, forkOffset, forkOffsetRequested *string
	var ttlSeconds, closedByEpoch, closedBySeq, forkSubOffset *int64
	var producersJSON []byte

	err := row.Scan(
		&r.Path, &contentType, &r.CurrentOffset, &lastSeq, &ttlSeconds, &expiresAt,
		&r.CreatedAt, &r.LastAccessedAt, &r.Closed, &closedByPID, &closedByEpoch, &closedBySeq,
		&forkedFrom, &forkOffset, &forkOffsetRequested, &forkSubOffset, &r.RefCount, &r.SoftDeleted,
		&producersJSON,
	)
	if err != nil {
		return nil, err
	}

	r.ContentType = contentType
	r.LastSeq = lastSeq
	r.TTLSeconds = ttlSeconds
	r.ExpiresAt = expiresAt
	if closedByPID != nil {
		r.ClosedBy = &ClosedByProducer{
			ProducerId: *closedByPID,
			Epoch:      *closedByEpoch,
			Seq:        *closedBySeq,
		}
	}
	r.ForkedFrom = forkedFrom
	r.ForkOffset = forkOffset
	r.ForkOffsetRequested = forkOffsetRequested
	r.ForkSubOffset = forkSubOffset
	if len(producersJSON) > 0 {
		_ = json.Unmarshal(producersJSON, &r.Producers)
	}
	return &r, nil
}

func (s *PostgresStore) queryRow(ctx context.Context, q queryer, path string) (*streamRow, error) {
	row := q.QueryRow(ctx, `
		SELECT path, content_type, current_offset, last_seq, ttl_seconds, expires_at,
		       created_at, last_accessed_at, closed, closed_by_producer_id, closed_by_epoch,
		       closed_by_seq, forked_from, fork_offset, fork_offset_requested, fork_sub_offset,
		       ref_count, soft_deleted, producers
		FROM `+s.schema+`.durable_streams WHERE path = $1`, path)
	return s.scanRow(row)
}

func (s *PostgresStore) queryRowForUpdate(ctx context.Context, q queryer, path string) (*streamRow, error) {
	row := q.QueryRow(ctx, `
		SELECT path, content_type, current_offset, last_seq, ttl_seconds, expires_at,
		       created_at, last_accessed_at, closed, closed_by_producer_id, closed_by_epoch,
		       closed_by_seq, forked_from, fork_offset, fork_offset_requested, fork_sub_offset,
		       ref_count, soft_deleted, producers
		FROM `+s.schema+`.durable_streams WHERE path = $1 FOR UPDATE`, path)
	return s.scanRow(row)
}

// ============================================================================
// Expiry
// ============================================================================

func (s *PostgresStore) isExpired(meta *streamRow) bool {
	now := time.Now()

	if meta.ExpiresAt != nil {
		if t, err := time.Parse(time.RFC3339, *meta.ExpiresAt); err == nil && now.After(t) {
			return true
		}
	}

	if meta.TTLSeconds != nil {
		expiry := time.Unix(meta.LastAccessedAt, 0).Add(time.Duration(*meta.TTLSeconds) * time.Second)
		if now.After(expiry) {
			return true
		}
	}

	return false
}

// getMetaIfNotExpired returns stream metadata for a read (no row lock),
// cleaning up expired streams. Returns nil if the stream is missing or expired.
func (s *PostgresStore) getMetaIfNotExpired(ctx context.Context, q queryer, path string) (*streamRow, error) {
	row, err := s.queryRow(ctx, q, path)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	if s.isExpired(row) {
		if row.RefCount > 0 {
			if !row.SoftDeleted {
				if err := s.softDelete(ctx, q, path); err != nil {
					return nil, err
				}
			}
			return nil, nil
		}
		if err := s.deleteWithCascade(ctx, q, path); err != nil {
			return nil, err
		}
		return nil, nil
	}
	return row, nil
}

// getMetaForUpdate returns stream metadata for a write, locking the row.
// Returns nil if the stream does not exist. Expiry is handled by callers.
func (s *PostgresStore) getMetaForUpdate(ctx context.Context, q queryer, path string) (*streamRow, error) {
	row, err := s.queryRowForUpdate(ctx, q, path)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return row, nil
}

// ============================================================================
// Producer validation
// ============================================================================

func (s *PostgresStore) validateProducer(meta *streamRow, opts AppendOptions) (AppendResult, *ProducerState, error) {
	epoch := *opts.ProducerEpoch
	seq := *opts.ProducerSeq

	var state *ProducerState
	if st, ok := meta.Producers[opts.ProducerId]; ok {
		state = &ProducerState{Epoch: st.Epoch, LastSeq: st.LastSeq, LastUpdated: st.LastUpdated}
	}

	if state == nil {
		if seq != 0 {
			return AppendResult{
				ProducerResult: ProducerResultNone,
				ExpectedSeq:    0,
				ReceivedSeq:    seq,
			}, nil, ErrProducerSeqGap
		}
		return AppendResult{
			ProducerResult: ProducerResultAccepted,
			LastSeq:        0,
		}, &ProducerState{Epoch: epoch, LastSeq: 0, LastUpdated: time.Now().Unix()}, nil
	}

	if epoch < state.Epoch {
		return AppendResult{
			ProducerResult: ProducerResultNone,
			CurrentEpoch:   state.Epoch,
		}, nil, ErrStaleEpoch
	}

	if epoch > state.Epoch {
		if seq != 0 {
			return AppendResult{ProducerResult: ProducerResultNone}, nil, ErrInvalidEpochSeq
		}
		return AppendResult{
			ProducerResult: ProducerResultAccepted,
			LastSeq:        0,
		}, &ProducerState{Epoch: epoch, LastSeq: 0, LastUpdated: time.Now().Unix()}, nil
	}

	if seq <= state.LastSeq {
		return AppendResult{
			ProducerResult: ProducerResultDuplicate,
			LastSeq:        state.LastSeq,
		}, nil, nil
	}

	if seq == state.LastSeq+1 {
		return AppendResult{
			ProducerResult: ProducerResultAccepted,
			LastSeq:        seq,
		}, &ProducerState{Epoch: epoch, LastSeq: seq, LastUpdated: time.Now().Unix()}, nil
	}

	return AppendResult{
		ProducerResult: ProducerResultNone,
		ExpectedSeq:    state.LastSeq + 1,
		ReceivedSeq:    seq,
	}, nil, ErrProducerSeqGap
}

// ============================================================================
// Create
// ============================================================================

func (s *PostgresStore) Create(path string, opts CreateOptions) (*StreamMetadata, bool, error) {
	ctx := context.Background()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback(ctx)

	existing, err := s.getMetaForUpdate(ctx, tx, path)
	if err != nil {
		return nil, false, err
	}
	if existing != nil {
		if s.isExpired(existing) {
			if existing.RefCount > 0 {
				existing.SoftDeleted = true
				if err := s.softDelete(ctx, tx, path); err != nil {
					return nil, false, err
				}
				return nil, false, ErrStreamExists
			}
			if err := s.deleteWithCascade(ctx, tx, path); err != nil {
				return nil, false, err
			}
		} else if existing.SoftDeleted {
			return nil, false, ErrStreamExists
		} else if configMatches(existing, opts) {
			return existing.toMeta(), false, nil
		} else {
			return nil, false, ErrConfigMismatch
		}
	}

	// Fork creation: validate source stream and resolve fork parameters.
	var forkOffset Offset
	var sourceContentType string
	var sourceMeta *streamRow
	var binarySubOffsetPrefix []byte
	isFork := opts.ForkedFrom != ""

	if isFork {
		source, err := s.getMetaForUpdate(ctx, tx, opts.ForkedFrom)
		if err != nil {
			return nil, false, err
		}
		if source == nil {
			return nil, false, ErrStreamNotFound
		}
		if source.SoftDeleted {
			return nil, false, ErrStreamSoftDeleted
		}
		if s.isExpired(source) {
			return nil, false, ErrStreamNotFound
		}

		sourceMeta = source
		sourceContentType = source.ContentTypeStr()

		// Reject a content-type mismatch up front, before taking a reference on
		// the source.
		if opts.ContentType != "" && !strings.EqualFold(opts.ContentType, sourceContentType) {
			return nil, false, ErrContentTypeMismatch
		}

		if opts.ForkOffset != nil {
			forkOffset = *opts.ForkOffset
		} else {
			forkOffset = source.CurrentOffsetOffset()
		}

		if forkOffset.LessThan(ZeroOffset) || source.CurrentOffsetOffset().LessThan(forkOffset) {
			return nil, false, ErrInvalidForkOffset
		}

		if opts.ForkSubOffset != nil && *opts.ForkSubOffset > 0 {
			resolvedOffset, prefixBytes, err := s.resolveForkSubOffset(
				ctx, tx, opts.ForkedFrom, forkOffset, *opts.ForkSubOffset,
				IsJSONContentType(sourceContentType),
			)
			if err != nil {
				return nil, false, err
			}
			if IsJSONContentType(sourceContentType) {
				forkOffset = resolvedOffset
			} else {
				binarySubOffsetPrefix = prefixBytes
			}
		}

		if err := s.incrementRefCount(ctx, tx, opts.ForkedFrom); err != nil {
			return nil, false, err
		}
	}

	contentType := opts.ContentType
	if contentType == "" {
		if isFork {
			contentType = sourceContentType
		} else {
			contentType = "application/octet-stream"
		}
	}

	now := time.Now()
	meta := &StreamMetadata{
		Path:           path,
		ContentType:    contentType,
		CreatedAt:      now,
		LastAccessedAt: now,
		Closed:         opts.Closed,
	}

	var currentOffset Offset
	if isFork {
		forkTTL, forkExpiresAt := resolveForkExpiry(opts, sourceMeta)
		meta.CurrentOffset = forkOffset
		meta.ForkOffset = forkOffset
		meta.ForkedFrom = opts.ForkedFrom
		meta.TTLSeconds = forkTTL
		meta.ExpiresAt = forkExpiresAt
		if opts.ForkOffset != nil {
			requested := *opts.ForkOffset
			meta.ForkOffsetRequested = &requested
		}
		if opts.ForkSubOffset != nil {
			meta.ForkSubOffset = *opts.ForkSubOffset
		}
		currentOffset = forkOffset
	} else {
		meta.CurrentOffset = ZeroOffset
		meta.TTLSeconds = opts.TTLSeconds
		meta.ExpiresAt = opts.ExpiresAt
		currentOffset = ZeroOffset
	}

	if err := s.insertStream(ctx, tx, meta, currentOffset); err != nil {
		if isFork {
			_ = s.decrementRefCount(ctx, tx, opts.ForkedFrom)
		}
		return nil, false, err
	}

	// Materialize binary sub-offset prefix as the fork's first own message.
	if len(binarySubOffsetPrefix) > 0 {
		newByte := offsetToByte(currentOffset) + int64(len(binarySubOffsetPrefix)) + postgresFrameOverhead
		newOffset := byteToOffset(newByte)
		if err := s.insertMessage(ctx, tx, path, binarySubOffsetPrefix, newByte, now); err != nil {
			return nil, false, err
		}
		currentOffset = newOffset
		meta.CurrentOffset = newOffset
		if err := s.updateOffset(ctx, tx, path, newOffset); err != nil {
			return nil, false, err
		}
	}

	// Append initial data if provided.
	if len(opts.InitialData) > 0 {
		newOffset, err := s.writeMessages(ctx, tx, path, contentType, currentOffset, opts.InitialData, true)
		if err != nil {
			return nil, false, err
		}
		currentOffset = newOffset
		meta.CurrentOffset = newOffset
		if err := s.updateOffset(ctx, tx, path, newOffset); err != nil {
			return nil, false, err
		}
	}

	// Set closed after the initial append succeeded.
	if opts.Closed {
		if err := s.setClosed(ctx, tx, path, true, nil, meta.LastAccessedAt.Unix()); err != nil {
			return nil, false, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, false, err
	}

	return meta, true, nil
}

func configMatches(m *streamRow, opts CreateOptions) bool {
	if !ContentTypeMatches(m.ContentTypeStr(), opts.ContentType) {
		return false
	}

	if (m.TTLSeconds == nil) != (opts.TTLSeconds == nil) {
		return false
	}
	if m.TTLSeconds != nil && opts.TTLSeconds != nil && *m.TTLSeconds != *opts.TTLSeconds {
		return false
	}

	if (m.ExpiresAt == nil) != (opts.ExpiresAt == nil) {
		return false
	}
	if m.ExpiresAt != nil && opts.ExpiresAt != nil {
		existing, err1 := time.Parse(time.RFC3339, *m.ExpiresAt)
		if err1 != nil || !existing.Equal(*opts.ExpiresAt) {
			return false
		}
	}

	if m.Closed != opts.Closed {
		return false
	}

	if (m.ForkedFrom == nil) != (opts.ForkedFrom == "") {
		return false
	}
	if m.ForkedFrom != nil && *m.ForkedFrom != opts.ForkedFrom {
		return false
	}

	if opts.ForkedFrom != "" {
		if opts.ForkOffset != nil {
			storedRequested := m.ForkOffsetRequested
			if storedRequested == nil {
				storedRequested = m.ForkOffset
			}
			if storedRequested == nil {
				return false
			}
			req, err := ParseOffset(*storedRequested)
			if err != nil || !req.Equal(*opts.ForkOffset) {
				return false
			}
		}
		var requestedSub uint64
		if opts.ForkSubOffset != nil {
			requestedSub = *opts.ForkSubOffset
		}
		var storedSub uint64
		if m.ForkSubOffset != nil {
			storedSub = uint64(*m.ForkSubOffset)
		}
		if storedSub != requestedSub {
			return false
		}
	}

	return true
}

func resolveForkExpiry(opts CreateOptions, sourceMeta *streamRow) (*int64, *time.Time) {
	if opts.TTLSeconds != nil {
		return opts.TTLSeconds, nil
	}
	if opts.ExpiresAt != nil {
		return nil, opts.ExpiresAt
	}
	if sourceMeta.TTLSeconds != nil {
		ttl := *sourceMeta.TTLSeconds
		return &ttl, nil
	}
	if sourceMeta.ExpiresAt != nil {
		if t, err := time.Parse(time.RFC3339, *sourceMeta.ExpiresAt); err == nil {
			return nil, &t
		}
	}
	return nil, nil
}

// ============================================================================
// Read helpers
// ============================================================================

func (s *PostgresStore) readOwnMessages(ctx context.Context, q queryer, path string, startByte int64, capByte ...int64) ([]Message, error) {
	sql := `SELECT data, byte_offset, timestamp FROM ` + s.schema + `.durable_stream_messages
	        WHERE stream_path = $1 AND byte_offset > $2`
	args := []any{path, startByte}
	if len(capByte) > 0 {
		sql += ` AND byte_offset <= $3`
		args = append(args, capByte[0])
	}
	sql += ` ORDER BY byte_offset`

	rows, err := q.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var messages []Message
	for rows.Next() {
		var data []byte
		var byteOffset int64
		var ts int64
		if err := rows.Scan(&data, &byteOffset, &ts); err != nil {
			return nil, err
		}
		messages = append(messages, Message{
			Data:   data,
			Offset: byteToOffset(byteOffset),
		})
	}
	return messages, rows.Err()
}

// readForkedMessages recursively reads messages from a fork's source chain,
// capped at capByte.
func (s *PostgresStore) readForkedMessages(ctx context.Context, q queryer, sourcePath string, startByte, capByte int64) ([]Message, error) {
	source, err := s.getMetaIfNotExpired(ctx, q, sourcePath)
	if err != nil {
		return nil, err
	}
	if source == nil {
		return nil, nil
	}

	var messages []Message

	if source.ForkedFrom != nil {
		if sourceFork, ok := source.ForkOffsetOffset(); ok {
			sourceForkByte := offsetToByte(sourceFork)
			if startByte < sourceForkByte {
				inheritedCap := sourceForkByte
				if capByte < inheritedCap {
					inheritedCap = capByte
				}
				inherited, err := s.readForkedMessages(ctx, q, *source.ForkedFrom, startByte, inheritedCap)
				if err != nil {
					return nil, err
				}
				messages = append(messages, inherited...)
			}
		}
	}

	own, err := s.readOwnMessages(ctx, q, sourcePath, startByte, capByte)
	if err != nil {
		return nil, err
	}
	messages = append(messages, own...)

	return messages, nil
}

func (s *PostgresStore) resolveForkSubOffset(ctx context.Context, q queryer, sourcePath string, forkOffset Offset, subOffset uint64, isJSON bool) (Offset, []byte, error) {
	forkByte := offsetToByte(forkOffset)

	source, err := s.getMetaIfNotExpired(ctx, q, sourcePath)
	if err != nil {
		return Offset{}, nil, err
	}
	if source == nil {
		return Offset{}, nil, ErrStreamNotFound
	}

	currentByte := offsetToByte(source.CurrentOffsetOffset())
	messages, err := s.readForkedMessages(ctx, q, sourcePath, forkByte, currentByte)
	if err != nil {
		return Offset{}, nil, err
	}
	if len(messages) == 0 {
		return Offset{}, nil, ErrInvalidForkSubOffset
	}

	if isJSON {
		if uint64(len(messages)) < subOffset {
			return Offset{}, nil, ErrInvalidForkSubOffset
		}
		return messages[subOffset-1].Offset, nil, nil
	}

	first := messages[0]
	if uint64(len(first.Data)) < subOffset {
		return Offset{}, nil, ErrInvalidForkSubOffset
	}
	prefix := make([]byte, subOffset)
	copy(prefix, first.Data[:subOffset])
	return forkOffset, prefix, nil
}

// ============================================================================
// Append
// ============================================================================

func (s *PostgresStore) Append(path string, data []byte, opts AppendOptions) (AppendResult, error) {
	if opts.HasProducerHeaders() && !opts.HasAllProducerHeaders() {
		return AppendResult{}, ErrPartialProducer
	}

	if opts.HasAllProducerHeaders() {
		lock := s.getProducerLock(path, opts.ProducerId)
		lock.Lock()
		defer lock.Unlock()
	}

	ctx := context.Background()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return AppendResult{}, err
	}
	defer tx.Rollback(ctx)

	meta, err := s.getMetaForUpdate(ctx, tx, path)
	if err != nil {
		return AppendResult{}, err
	}
	if meta == nil {
		return AppendResult{}, ErrStreamNotFound
	}
	if s.isExpired(meta) {
		return AppendResult{}, ErrStreamNotFound
	}
	if meta.SoftDeleted {
		return AppendResult{}, ErrStreamSoftDeleted
	}

	// Refresh TTL sliding window.
	meta.LastAccessedAt = time.Now().Unix()

	if meta.Closed {
		if opts.HasAllProducerHeaders() && meta.ClosedBy != nil &&
			meta.ClosedBy.ProducerId == opts.ProducerId &&
			meta.ClosedBy.Epoch == *opts.ProducerEpoch &&
			meta.ClosedBy.Seq == *opts.ProducerSeq {
			return AppendResult{
				Offset:         meta.CurrentOffsetOffset(),
				ProducerResult: ProducerResultDuplicate,
				LastSeq:        *opts.ProducerSeq,
				StreamClosed:   true,
			}, nil
		}
		return AppendResult{
			Offset:       meta.CurrentOffsetOffset(),
			StreamClosed: true,
		}, ErrStreamClosed
	}

	if opts.ContentType != "" && !ContentTypeMatches(meta.ContentTypeStr(), opts.ContentType) {
		return AppendResult{}, ErrContentTypeMismatch
	}

	var producerStatePtr *ProducerState
	var producerResult ProducerResult = ProducerResultNone
	var producerLastSeq int64
	if opts.HasAllProducerHeaders() {
		result, newState, err := s.validateProducer(meta, opts)
		if err != nil {
			result.Offset = meta.CurrentOffsetOffset()
			return result, err
		}
		if result.ProducerResult == ProducerResultDuplicate {
			return AppendResult{
				Offset:         meta.CurrentOffsetOffset(),
				ProducerResult: ProducerResultDuplicate,
				LastSeq:        result.LastSeq,
			}, nil
		}
		producerStatePtr = newState
		producerResult = result.ProducerResult
		producerLastSeq = result.LastSeq
	}

	if opts.Seq != "" {
		if meta.LastSeq != nil && *meta.LastSeq != "" && opts.Seq <= *meta.LastSeq {
			return AppendResult{}, ErrSequenceConflict
		}
	}

	newOffset, err := s.writeMessages(ctx, tx, path, meta.ContentTypeStr(), meta.CurrentOffsetOffset(), data, false)
	if err != nil {
		return AppendResult{}, err
	}

	streamClosed := false
	if opts.Close {
		meta.Closed = true
		streamClosed = true
		if opts.HasAllProducerHeaders() {
			meta.ClosedBy = &ClosedByProducer{
				ProducerId: opts.ProducerId,
				Epoch:      *opts.ProducerEpoch,
				Seq:        *opts.ProducerSeq,
			}
		}
	}

	if producerStatePtr != nil {
		if meta.Producers == nil {
			meta.Producers = make(map[string]pgProducerState)
		}
		meta.Producers[opts.ProducerId] = pgProducerState{
			Epoch:       producerStatePtr.Epoch,
			LastSeq:     producerStatePtr.LastSeq,
			LastUpdated: producerStatePtr.LastUpdated,
		}
	}

	newLastSeq := meta.LastSeq
	if opts.Seq != "" {
		seq := opts.Seq
		newLastSeq = &seq
	}

	if err := s.updateAppendState(ctx, tx, path, newOffset, newLastSeq, meta.Producers, opts.Close, meta.ClosedBy, meta.LastAccessedAt); err != nil {
		return AppendResult{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return AppendResult{}, err
	}

	if streamClosed {
		s.longPoll.notifyClosed(path)
	} else {
		s.longPoll.notify(path)
	}

	return AppendResult{
		Offset:         newOffset,
		ProducerResult: producerResult,
		LastSeq:        producerLastSeq,
		StreamClosed:   streamClosed,
	}, nil
}

// writeMessages writes data to the stream, flattening JSON arrays into
// individual message rows. Returns the new tail offset.
func (s *PostgresStore) writeMessages(ctx context.Context, q queryer, path, contentType string, currentOffset Offset, data []byte, allowEmpty bool) (Offset, error) {
	if IsJSONContentType(contentType) {
		messages, err := processJSONAppend(data, allowEmpty)
		if err != nil {
			return Offset{}, err
		}
		cur := currentOffset
		now := time.Now()
		for _, msgData := range messages {
			newByte := offsetToByte(cur) + int64(len(msgData)) + postgresFrameOverhead
			newOffset := byteToOffset(newByte)
			if err := s.insertMessage(ctx, q, path, msgData, newByte, now); err != nil {
				return Offset{}, err
			}
			cur = newOffset
		}
		return cur, nil
	}

	newByte := offsetToByte(currentOffset) + int64(len(data)) + postgresFrameOverhead
	newOffset := byteToOffset(newByte)
	if err := s.insertMessage(ctx, q, path, data, newByte, time.Now()); err != nil {
		return Offset{}, err
	}
	return newOffset, nil
}

// ============================================================================
// Close
// ============================================================================

func (s *PostgresStore) CloseStream(path string) (*CloseResult, error) {
	ctx := context.Background()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	meta, err := s.getMetaForUpdate(ctx, tx, path)
	if err != nil {
		return nil, err
	}
	if meta == nil {
		return nil, ErrStreamNotFound
	}
	if s.isExpired(meta) {
		return nil, ErrStreamNotFound
	}

	alreadyClosed := meta.Closed
	meta.Closed = true
	meta.LastAccessedAt = time.Now().Unix()

	if err := s.setClosed(ctx, tx, path, true, nil, meta.LastAccessedAt); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}

	s.longPoll.notifyClosed(path)

	return &CloseResult{
		FinalOffset:   meta.CurrentOffsetOffset(),
		AlreadyClosed: alreadyClosed,
	}, nil
}

func (s *PostgresStore) CloseStreamWithProducer(path string, opts CloseProducerOptions) (*CloseProducerResult, error) {
	lock := s.getProducerLock(path, opts.ProducerId)
	lock.Lock()
	defer lock.Unlock()

	ctx := context.Background()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	meta, err := s.getMetaForUpdate(ctx, tx, path)
	if err != nil {
		return nil, err
	}
	if meta == nil {
		return nil, ErrStreamNotFound
	}
	if s.isExpired(meta) {
		return nil, ErrStreamNotFound
	}

	if meta.Closed {
		if meta.ClosedBy != nil &&
			meta.ClosedBy.ProducerId == opts.ProducerId &&
			meta.ClosedBy.Epoch == opts.ProducerEpoch &&
			meta.ClosedBy.Seq == opts.ProducerSeq {
			return &CloseProducerResult{
				FinalOffset:    meta.CurrentOffsetOffset(),
				ProducerResult: ProducerResultDuplicate,
				LastSeq:        opts.ProducerSeq,
				StreamClosed:   true,
				AlreadyClosed:  true,
			}, nil
		}
		return &CloseProducerResult{
			FinalOffset:   meta.CurrentOffsetOffset(),
			StreamClosed:  true,
			AlreadyClosed: true,
		}, ErrStreamClosed
	}

	appendOpts := AppendOptions{
		ProducerId:    opts.ProducerId,
		ProducerEpoch: &opts.ProducerEpoch,
		ProducerSeq:   &opts.ProducerSeq,
	}
	result, newState, err := s.validateProducer(meta, appendOpts)
	if err != nil {
		return &CloseProducerResult{
			FinalOffset:    meta.CurrentOffsetOffset(),
			ProducerResult: result.ProducerResult,
			CurrentEpoch:   result.CurrentEpoch,
			ExpectedSeq:    result.ExpectedSeq,
			ReceivedSeq:    result.ReceivedSeq,
			LastSeq:        result.LastSeq,
			StreamClosed:   meta.Closed,
		}, err
	}

	if result.ProducerResult == ProducerResultDuplicate {
		return &CloseProducerResult{
			FinalOffset:    meta.CurrentOffsetOffset(),
			ProducerResult: ProducerResultDuplicate,
			LastSeq:        result.LastSeq,
			StreamClosed:   meta.Closed,
			AlreadyClosed:  meta.Closed,
		}, nil
	}

	if meta.Producers == nil {
		meta.Producers = make(map[string]pgProducerState)
	}
	meta.Producers[opts.ProducerId] = pgProducerState{
		Epoch:       newState.Epoch,
		LastSeq:     newState.LastSeq,
		LastUpdated: newState.LastUpdated,
	}
	meta.Closed = true
	meta.ClosedBy = &ClosedByProducer{
		ProducerId: opts.ProducerId,
		Epoch:      opts.ProducerEpoch,
		Seq:        opts.ProducerSeq,
	}
	meta.LastAccessedAt = time.Now().Unix()

	producersJSON, _ := json.Marshal(meta.Producers)
	if err := s.updateClosedWithProducers(ctx, tx, path, meta.ClosedBy, producersJSON, meta.LastAccessedAt); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}

	s.longPoll.notifyClosed(path)

	return &CloseProducerResult{
		FinalOffset:    meta.CurrentOffsetOffset(),
		ProducerResult: result.ProducerResult,
		LastSeq:        result.LastSeq,
		StreamClosed:   true,
		AlreadyClosed:  false,
	}, nil
}

// ============================================================================
// Read
// ============================================================================

func (s *PostgresStore) Read(path string, offset Offset) ([]Message, bool, error) {
	ctx := context.Background()

	meta, err := s.getMetaIfNotExpired(ctx, s.pool, path)
	if err != nil {
		return nil, false, err
	}
	if meta == nil {
		return nil, false, ErrStreamNotFound
	}
	if meta.SoftDeleted {
		return nil, false, ErrStreamNotFound
	}

	// Refresh TTL sliding window (best effort).
	_ = s.touchAccess(ctx, s.pool, path, time.Now().Unix())

	startByte := offsetToByte(offset)
	currentByte := offsetToByte(meta.CurrentOffsetOffset())

	if meta.CurrentOffsetOffset().IsZero() {
		return nil, true, nil
	}
	if startByte >= currentByte {
		return nil, true, nil
	}

	var messages []Message
	if meta.ForkedFrom != nil {
		if forkOff, ok := meta.ForkOffsetOffset(); ok {
			forkByte := offsetToByte(forkOff)
			if startByte < forkByte {
				inherited, err := s.readForkedMessages(ctx, s.pool, *meta.ForkedFrom, startByte, forkByte)
				if err != nil {
					return nil, false, err
				}
				messages = append(messages, inherited...)
			}
			own, err := s.readOwnMessages(ctx, s.pool, path, startByte)
			if err != nil {
				return nil, false, err
			}
			messages = append(messages, own...)
		}
	} else {
		own, err := s.readOwnMessages(ctx, s.pool, path, startByte)
		if err != nil {
			return nil, false, err
		}
		messages = append(messages, own...)
	}

	var upToDate bool
	if len(messages) > 0 {
		upToDate = messages[len(messages)-1].Offset.Equal(meta.CurrentOffsetOffset())
	} else {
		upToDate = offset.Equal(meta.CurrentOffsetOffset()) || meta.CurrentOffsetOffset().IsZero()
	}

	return messages, upToDate, nil
}

func (s *PostgresStore) WaitForMessages(ctx context.Context, path string, offset Offset, timeout time.Duration) ([]Message, bool, bool, error) {
	meta, err := s.getMetaIfNotExpired(ctx, s.pool, path)
	if err != nil {
		return nil, false, false, err
	}
	if meta != nil && meta.Closed && offset.Equal(meta.CurrentOffsetOffset()) {
		return nil, false, true, nil
	}

	messages, _, err := s.Read(path, offset)
	if err != nil {
		return nil, false, false, err
	}
	if len(messages) > 0 {
		return messages, false, false, nil
	}

	// For forks: if offset is in the inherited range (< ForkOffset), inherited
	// data exists in the source. The Read call above should have returned it
	// already, but if the source is missing/empty, don't wait — inherited data
	// will never arrive via long-poll notifications.
	if meta != nil && meta.ForkedFrom != nil {
		if forkOff, ok := meta.ForkOffsetOffset(); ok && offset.LessThan(forkOff) {
			return nil, false, false, nil
		}
	}

	ch := make(chan struct{}, 1)
	s.longPoll.register(path, ch)
	defer s.longPoll.unregister(path, ch)

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-ch:
		currentMeta, _ := s.getMetaIfNotExpired(ctx, s.pool, path)
		if currentMeta != nil && currentMeta.Closed {
			currentOffset := currentMeta.CurrentOffsetOffset()
			messages, _, err := s.Read(path, offset)
			if err != nil {
				return nil, false, false, err
			}
			if len(messages) == 0 && offset.Equal(currentOffset) {
				return nil, false, true, nil
			}
			return messages, false, false, nil
		}
		messages, _, err := s.Read(path, offset)
		return messages, false, false, err
	case <-timer.C:
		currentMeta, _ := s.getMetaIfNotExpired(ctx, s.pool, path)
		streamClosed := currentMeta != nil && currentMeta.Closed
		return nil, true, streamClosed, nil
	case <-ctx.Done():
		return nil, false, false, ctx.Err()
	}
}

func (s *PostgresStore) GetCurrentOffset(path string) (Offset, error) {
	meta, err := s.getMetaIfNotExpired(context.Background(), s.pool, path)
	if err != nil {
		return Offset{}, err
	}
	if meta == nil {
		return Offset{}, ErrStreamNotFound
	}
	return meta.CurrentOffsetOffset(), nil
}

// ============================================================================
// Metadata accessors
// ============================================================================

func (s *PostgresStore) Get(path string) (*StreamMetadata, error) {
	meta, err := s.getMetaIfNotExpired(context.Background(), s.pool, path)
	if err != nil {
		return nil, err
	}
	if meta == nil {
		return nil, ErrStreamNotFound
	}
	if meta.SoftDeleted {
		return nil, ErrStreamSoftDeleted
	}
	return meta.toMeta(), nil
}

func (s *PostgresStore) Has(path string) bool {
	meta, err := s.getMetaIfNotExpired(context.Background(), s.pool, path)
	if err != nil || meta == nil {
		return false
	}
	return !meta.SoftDeleted
}

// ============================================================================
// Delete
// ============================================================================

func (s *PostgresStore) Delete(path string) error {
	ctx := context.Background()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	meta, err := s.getMetaForUpdate(ctx, tx, path)
	if err != nil {
		return err
	}
	if meta == nil {
		return ErrStreamNotFound
	}
	if meta.SoftDeleted {
		return ErrStreamSoftDeleted
	}

	if meta.RefCount > 0 {
		meta.SoftDeleted = true
		if err := s.softDelete(ctx, tx, path); err != nil {
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		s.longPoll.notify(path)
		return nil
	}

	if err := s.deleteWithCascade(ctx, tx, path); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	s.longPoll.notify(path)
	return nil
}

func (s *PostgresStore) deleteWithCascade(ctx context.Context, q queryer, path string) error {
	meta, err := s.getMetaForUpdate(ctx, q, path)
	if err != nil {
		return err
	}
	if meta == nil {
		return nil
	}

	forkedFrom := meta.ForkedFrom

	// DELETE cascades to messages via the FK.
	if _, err := q.Exec(ctx, `DELETE FROM `+s.schema+`.durable_streams WHERE path = $1`, path); err != nil {
		return err
	}

	if forkedFrom != nil {
		parent, err := s.getMetaForUpdate(ctx, q, *forkedFrom)
		if err != nil {
			return err
		}
		if parent != nil {
			newRefCount := parent.RefCount - 1
			if newRefCount < 0 {
				newRefCount = 0
			}
			if err := s.setRefCount(ctx, q, *forkedFrom, newRefCount); err != nil {
				return err
			}
			if newRefCount == 0 && parent.SoftDeleted {
				return s.deleteWithCascade(ctx, q, *forkedFrom)
			}
		}
	}

	return nil
}

// ============================================================================
// Formatting
// ============================================================================

func (s *PostgresStore) FormatResponse(path string, messages []Message) ([]byte, error) {
	meta, err := s.getMetaIfNotExpired(context.Background(), s.pool, path)
	if err != nil {
		return nil, err
	}
	if meta == nil {
		return nil, ErrStreamNotFound
	}

	if IsJSONContentType(meta.ContentTypeStr()) {
		return FormatJSONResponse(messages), nil
	}

	var buf bytes.Buffer
	for _, msg := range messages {
		buf.Write(msg.Data)
	}
	return buf.Bytes(), nil
}

// ============================================================================
// SQL helpers
// ============================================================================

func (s *PostgresStore) insertStream(ctx context.Context, q queryer, meta *StreamMetadata, currentOffset Offset) error {
	var expiresAt *string
	if meta.ExpiresAt != nil {
		es := meta.ExpiresAt.Format(time.RFC3339)
		expiresAt = &es
	}
	var forkedFrom, forkOffset, forkOffsetRequested *string
	var forkSubOffset *int64
	if meta.ForkedFrom != "" {
		forkedFrom = &meta.ForkedFrom
		fo := meta.ForkOffset.String()
		forkOffset = &fo
		if meta.ForkOffsetRequested != nil {
			fr := meta.ForkOffsetRequested.String()
			forkOffsetRequested = &fr
		}
		fs := int64(meta.ForkSubOffset)
		forkSubOffset = &fs
	}

	_, err := q.Exec(ctx, `
		INSERT INTO `+s.schema+`.durable_streams
			(path, content_type, current_offset, last_seq, ttl_seconds, expires_at,
			 created_at, last_accessed_at, closed, forked_from, fork_offset,
			 fork_offset_requested, fork_sub_offset, ref_count, soft_deleted, producers)
		VALUES ($1,$2,$3,NULL,$4,$5,$6,$7,FALSE,$8,$9,$10,$11,0,FALSE,'{}'::jsonb)`,
		meta.Path, meta.ContentType, currentOffset.String(), meta.TTLSeconds, expiresAt,
		meta.CreatedAt.Unix(), meta.LastAccessedAt.Unix(), forkedFrom, forkOffset,
		forkOffsetRequested, forkSubOffset)
	return err
}

func (s *PostgresStore) insertMessage(ctx context.Context, q queryer, path string, data []byte, byteOffset int64, ts time.Time) error {
	_, err := q.Exec(ctx, `
		INSERT INTO `+s.schema+`.durable_stream_messages
			(stream_path, data, byte_offset, timestamp) VALUES ($1,$2,$3,$4)`,
		path, data, byteOffset, ts.UnixMilli())
	return err
}

func (s *PostgresStore) updateOffset(ctx context.Context, q queryer, path string, offset Offset) error {
	_, err := q.Exec(ctx, `
		UPDATE `+s.schema+`.durable_streams SET current_offset = $1 WHERE path = $2`,
		offset.String(), path)
	return err
}

func (s *PostgresStore) updateAppendState(ctx context.Context, q queryer, path string, offset Offset, lastSeq *string, producers map[string]pgProducerState, closed bool, closedBy *ClosedByProducer, lastAccessedAt int64) error {
	producersJSON, _ := json.Marshal(producers)
	var closedByPID *string
	var closedByEpoch, closedBySeq *int64
	if closedBy != nil {
		closedByPID = &closedBy.ProducerId
		ce := closedBy.Epoch
		closedByEpoch = &ce
		cs := closedBy.Seq
		closedBySeq = &cs
	}

	_, err := q.Exec(ctx, `
		UPDATE `+s.schema+`.durable_streams
			SET current_offset = $1, last_seq = $2, producers = $3, closed = $4,
			    closed_by_producer_id = $5, closed_by_epoch = $6, closed_by_seq = $7,
			    last_accessed_at = $8
			WHERE path = $9`,
		offset.String(), lastSeq, producersJSON, closed, closedByPID, closedByEpoch,
		closedBySeq, lastAccessedAt, path)
	return err
}

func (s *PostgresStore) setClosed(ctx context.Context, q queryer, path string, closed bool, closedBy *ClosedByProducer, lastAccessedAt int64) error {
	var closedByPID *string
	var closedByEpoch, closedBySeq *int64
	if closedBy != nil {
		closedByPID = &closedBy.ProducerId
		ce := closedBy.Epoch
		closedByEpoch = &ce
		cs := closedBy.Seq
		closedBySeq = &cs
	}

	_, err := q.Exec(ctx, `
		UPDATE `+s.schema+`.durable_streams
			SET closed = $1, closed_by_producer_id = $2, closed_by_epoch = $3,
			    closed_by_seq = $4, last_accessed_at = $5
			WHERE path = $6`,
		closed, closedByPID, closedByEpoch, closedBySeq, lastAccessedAt, path)
	return err
}

func (s *PostgresStore) updateClosedWithProducers(ctx context.Context, q queryer, path string, closedBy *ClosedByProducer, producersJSON []byte, lastAccessedAt int64) error {
	var closedByPID *string
	var closedByEpoch, closedBySeq *int64
	if closedBy != nil {
		closedByPID = &closedBy.ProducerId
		ce := closedBy.Epoch
		closedByEpoch = &ce
		cs := closedBy.Seq
		closedBySeq = &cs
	}

	_, err := q.Exec(ctx, `
		UPDATE `+s.schema+`.durable_streams
			SET closed = TRUE, closed_by_producer_id = $1, closed_by_epoch = $2,
			    closed_by_seq = $3, producers = $4, last_accessed_at = $5
			WHERE path = $6`,
		closedByPID, closedByEpoch, closedBySeq, producersJSON, lastAccessedAt, path)
	return err
}

func (s *PostgresStore) incrementRefCount(ctx context.Context, q queryer, path string) error {
	_, err := q.Exec(ctx, `
		UPDATE `+s.schema+`.durable_streams SET ref_count = ref_count + 1 WHERE path = $1`, path)
	return err
}

func (s *PostgresStore) decrementRefCount(ctx context.Context, q queryer, path string) error {
	_, err := q.Exec(ctx, `
		UPDATE `+s.schema+`.durable_streams SET ref_count = GREATEST(ref_count - 1, 0) WHERE path = $1`, path)
	return err
}

func (s *PostgresStore) setRefCount(ctx context.Context, q queryer, path string, refCount int32) error {
	_, err := q.Exec(ctx, `
		UPDATE `+s.schema+`.durable_streams SET ref_count = $1 WHERE path = $2`, refCount, path)
	return err
}

func (s *PostgresStore) softDelete(ctx context.Context, q queryer, path string) error {
	_, err := q.Exec(ctx, `
		UPDATE `+s.schema+`.durable_streams SET soft_deleted = TRUE WHERE path = $1`, path)
	return err
}

func (s *PostgresStore) touchAccess(ctx context.Context, q queryer, path string, lastAccessedAt int64) error {
	_, err := q.Exec(ctx, `
		UPDATE `+s.schema+`.durable_streams SET last_accessed_at = $1 WHERE path = $2`,
		lastAccessedAt, path)
	return err
}

// getProducerLock returns a per-producer mutex for serializing validation+append.
func (s *PostgresStore) getProducerLock(streamPath, producerId string) *sync.Mutex {
	key := streamPath + ":" + producerId
	s.producerLocksMu.Lock()
	defer s.producerLocksMu.Unlock()

	if mu, ok := s.producerLocks[key]; ok {
		return mu
	}
	mu := &sync.Mutex{}
	s.producerLocks[key] = mu
	return mu
}
