package store

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"
)

// newTestPostgresStore returns a PostgresStore when POSTGRES_URL is set, or
// skips the test otherwise. Each test uses a unique schema so runs are isolated.
func newTestPostgresStore(t *testing.T) *PostgresStore {
	t.Helper()
	url := os.Getenv("POSTGRES_URL")
	if url == "" {
		t.Skip("POSTGRES_URL not set; skipping postgres store test")
	}

	schema := "ds_test_" + sanitizeSchema(t.Name())
	s, err := NewPostgresStore(PostgresStoreConfig{
		ConnectionString: url,
		Schema:           schema,
	})
	if err != nil {
		t.Fatalf("failed to create postgres store: %v", err)
	}
	t.Cleanup(func() {
		s.pool.Exec(context.Background(), `DROP SCHEMA IF EXISTS `+schema+` CASCADE`)
		s.Close()
	})
	return s
}

func sanitizeSchema(name string) string {
	out := make([]byte, 0, len(name))
	for i := 0; i < len(name); i++ {
		c := name[i]
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' {
			out = append(out, c)
		} else if c >= 'A' && c <= 'Z' {
			out = append(out, c+('a'-'A'))
		} else {
			out = append(out, '_')
		}
	}
	return string(out)
}

func TestPostgresStore_CreateAndGet(t *testing.T) {
	s := newTestPostgresStore(t)

	meta, created, err := s.Create("/test/stream", CreateOptions{ContentType: "application/json"})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if !created {
		t.Error("expected created=true for new stream")
	}
	if meta.Path != "/test/stream" {
		t.Errorf("path mismatch: %q", meta.Path)
	}
	if meta.ContentType != "application/json" {
		t.Errorf("content type mismatch: %q", meta.ContentType)
	}

	gotMeta, err := s.Get("/test/stream")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if gotMeta.Path != meta.Path {
		t.Errorf("path mismatch on get")
	}
	if !s.Has("/test/stream") {
		t.Error("Has returned false for existing stream")
	}

	if _, err := s.Get("/nonexistent"); err != ErrStreamNotFound {
		t.Errorf("expected ErrStreamNotFound, got %v", err)
	}
}

func TestPostgresStore_CreateIdempotent(t *testing.T) {
	s := newTestPostgresStore(t)

	opts := CreateOptions{ContentType: "text/plain"}
	if _, created1, err := s.Create("/test", opts); err != nil || !created1 {
		t.Fatalf("first Create failed: %v (created=%v)", err, created1)
	}
	if _, created2, err := s.Create("/test", opts); err != nil || created2 {
		t.Fatalf("idempotent Create should return created=false, got created=%v err=%v", created2, err)
	}
	opts.ContentType = "application/json"
	if _, _, err := s.Create("/test", opts); err != ErrConfigMismatch {
		t.Errorf("expected ErrConfigMismatch, got %v", err)
	}
}

func TestPostgresStore_AppendAndRead(t *testing.T) {
	s := newTestPostgresStore(t)

	if _, _, err := s.Create("/test", CreateOptions{ContentType: "text/plain"}); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	data := []byte("hello world")
	result, err := s.Append("/test", data, AppendOptions{})
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	if result.Offset.ByteOffset == 0 {
		t.Error("offset should be non-zero after append")
	}

	messages, upToDate, err := s.Read("/test", ZeroOffset)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if len(messages) != 1 {
		t.Errorf("expected 1 message, got %d", len(messages))
	}
	if !bytes.Equal(messages[0].Data, data) {
		t.Errorf("data mismatch")
	}
	if !upToDate {
		t.Error("should be up to date")
	}

	messages, upToDate, err = s.Read("/test", result.Offset)
	if err != nil {
		t.Fatalf("Read from tail failed: %v", err)
	}
	if len(messages) != 0 {
		t.Errorf("expected 0 messages at tail, got %d", len(messages))
	}
	if !upToDate {
		t.Error("should be up to date at tail")
	}
}

func TestPostgresStore_AppendJSON(t *testing.T) {
	s := newTestPostgresStore(t)

	if _, _, err := s.Create("/json", CreateOptions{ContentType: "application/json"}); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	if _, err := s.Append("/json", []byte(`[{"id":1},{"id":2}]`), AppendOptions{}); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	messages, _, err := s.Read("/json", ZeroOffset)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if len(messages) != 2 {
		t.Errorf("expected 2 messages (flattened array), got %d", len(messages))
	}

	resp, err := s.FormatResponse("/json", messages)
	if err != nil {
		t.Fatalf("FormatResponse failed: %v", err)
	}
	if string(resp) != `[{"id":1},{"id":2}]` {
		t.Errorf("formatted response mismatch: %s", resp)
	}
}

func TestPostgresStore_Delete(t *testing.T) {
	s := newTestPostgresStore(t)

	if _, _, _ = s.Create("/test", CreateOptions{ContentType: "text/plain"}); true {
	}
	if err := s.Delete("/test"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if s.Has("/test") {
		t.Error("stream still exists after delete")
	}
	if err := s.Delete("/nonexistent"); err != ErrStreamNotFound {
		t.Errorf("expected ErrStreamNotFound, got %v", err)
	}
}

func TestPostgresStore_SequenceConflict(t *testing.T) {
	s := newTestPostgresStore(t)

	if _, _, _ = s.Create("/test", CreateOptions{ContentType: "text/plain"}); true {
	}
	if _, err := s.Append("/test", []byte("a"), AppendOptions{Seq: "seq1"}); err != nil {
		t.Fatalf("first append failed: %v", err)
	}
	if _, err := s.Append("/test", []byte("b"), AppendOptions{Seq: "seq1"}); err != ErrSequenceConflict {
		t.Errorf("expected ErrSequenceConflict, got %v", err)
	}
	if _, err := s.Append("/test", []byte("c"), AppendOptions{Seq: "seq2"}); err != nil {
		t.Fatalf("third append failed: %v", err)
	}
}

func TestPostgresStore_ContentTypeMismatch(t *testing.T) {
	s := newTestPostgresStore(t)

	if _, _, _ = s.Create("/test", CreateOptions{ContentType: "text/plain"}); true {
	}
	if _, err := s.Append("/test", []byte("data"), AppendOptions{ContentType: "application/json"}); err != ErrContentTypeMismatch {
		t.Errorf("expected ErrContentTypeMismatch, got %v", err)
	}
}

func TestPostgresStore_Persistence(t *testing.T) {
	url := os.Getenv("POSTGRES_URL")
	if url == "" {
		t.Skip("POSTGRES_URL not set; skipping postgres store test")
	}
	schema := "ds_test_" + sanitizeSchema(t.Name())

	// Create store and add data
	{
		s, err := NewPostgresStore(PostgresStoreConfig{ConnectionString: url, Schema: schema})
		if err != nil {
			t.Fatalf("failed to create store: %v", err)
		}
		if _, _, _ = s.Create("/test", CreateOptions{ContentType: "text/plain"}); true {
		}
		if _, err := s.Append("/test", []byte("hello"), AppendOptions{}); err != nil {
			t.Fatalf("append failed: %v", err)
		}
		s.Close()
	}

	// Reopen and verify
	{
		s, err := NewPostgresStore(PostgresStoreConfig{ConnectionString: url, Schema: schema})
		if err != nil {
			t.Fatalf("failed to reopen store: %v", err)
		}
		defer func() {
			s.pool.Exec(context.Background(), `DROP SCHEMA IF EXISTS `+schema+` CASCADE`)
			s.Close()
		}()

		if !s.Has("/test") {
			t.Error("stream should exist after reopen")
		}
		messages, _, err := s.Read("/test", ZeroOffset)
		if err != nil {
			t.Fatalf("Read failed: %v", err)
		}
		if len(messages) != 1 {
			t.Errorf("expected 1 message, got %d", len(messages))
		}
		if !bytes.Equal(messages[0].Data, []byte("hello")) {
			t.Error("data mismatch after reopen")
		}
	}
}

func TestPostgresStore_LongPoll(t *testing.T) {
	s := newTestPostgresStore(t)

	if _, _, _ = s.Create("/test", CreateOptions{ContentType: "text/plain"}); true {
	}

	done := make(chan struct{})
	var messages []Message
	var timedOut bool
	go func() {
		messages, timedOut, _, _ = s.WaitForMessages(context.Background(), "/test", ZeroOffset, 5*time.Second)
		close(done)
	}()

	time.Sleep(100 * time.Millisecond)
	if _, err := s.Append("/test", []byte("wakeup"), AppendOptions{}); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	select {
	case <-done:
		if timedOut {
			t.Error("long-poll should not have timed out")
		}
		if len(messages) != 1 {
			t.Errorf("expected 1 message, got %d", len(messages))
		}
	case <-time.After(2 * time.Second):
		t.Error("long-poll did not complete in time")
	}
}

func TestPostgresStore_LongPollTimeout(t *testing.T) {
	s := newTestPostgresStore(t)

	if _, _, _ = s.Create("/test", CreateOptions{ContentType: "text/plain"}); true {
	}
	if _, err := s.Append("/test", []byte("initial"), AppendOptions{}); err != nil {
		t.Fatalf("append failed: %v", err)
	}
	offset, _ := s.GetCurrentOffset("/test")

	messages, timedOut, _, err := s.WaitForMessages(context.Background(), "/test", offset, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("WaitForMessages failed: %v", err)
	}
	if !timedOut {
		t.Error("expected timeout")
	}
	if len(messages) != 0 {
		t.Errorf("expected 0 messages on timeout, got %d", len(messages))
	}
}

func TestPostgresStore_InitialData(t *testing.T) {
	s := newTestPostgresStore(t)

	meta, _, err := s.Create("/test", CreateOptions{
		ContentType: "text/plain",
		InitialData: []byte("initial content"),
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if meta.CurrentOffset.ByteOffset == 0 {
		t.Error("offset should be non-zero with initial data")
	}

	messages, _, err := s.Read("/test", ZeroOffset)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if len(messages) != 1 {
		t.Errorf("expected 1 message, got %d", len(messages))
	}
	if !bytes.Equal(messages[0].Data, []byte("initial content")) {
		t.Error("initial data mismatch")
	}
}

func TestPostgresStore_StreamClosure(t *testing.T) {
	s := newTestPostgresStore(t)

	if _, _, err := s.Create("/test", CreateOptions{ContentType: "text/plain"}); err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if _, err := s.Append("/test", []byte("data"), AppendOptions{}); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	closeResult, err := s.CloseStream("/test")
	if err != nil {
		t.Fatalf("CloseStream failed: %v", err)
	}
	if closeResult.AlreadyClosed {
		t.Error("stream should not be already closed")
	}

	meta, err := s.Get("/test")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !meta.Closed {
		t.Error("stream should be closed")
	}

	if _, err := s.Append("/test", []byte("more data"), AppendOptions{}); err != ErrStreamClosed {
		t.Errorf("expected ErrStreamClosed, got: %v", err)
	}

	closeResult, err = s.CloseStream("/test")
	if err != nil {
		t.Fatalf("second CloseStream failed: %v", err)
	}
	if !closeResult.AlreadyClosed {
		t.Error("stream should be already closed")
	}
}

func TestPostgresStore_CreateClosed(t *testing.T) {
	s := newTestPostgresStore(t)

	meta, _, err := s.Create("/closed", CreateOptions{ContentType: "text/plain", Closed: true})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if !meta.Closed {
		t.Error("stream should be created closed")
	}
	if _, err := s.Append("/closed", []byte("data"), AppendOptions{}); err != ErrStreamClosed {
		t.Errorf("expected ErrStreamClosed, got: %v", err)
	}
}

func TestPostgresStore_AppendAndClose(t *testing.T) {
	s := newTestPostgresStore(t)

	if _, _, err := s.Create("/test", CreateOptions{ContentType: "text/plain"}); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	result, err := s.Append("/test", []byte("final"), AppendOptions{Close: true})
	if err != nil {
		t.Fatalf("Append with close failed: %v", err)
	}
	if !result.StreamClosed {
		t.Error("StreamClosed should be true")
	}

	meta, err := s.Get("/test")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !meta.Closed {
		t.Error("stream should be closed after append with close")
	}

	messages, _, err := s.Read("/test", ZeroOffset)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if len(messages) != 1 {
		t.Errorf("expected 1 message, got %d", len(messages))
	}
	if !bytes.Equal(messages[0].Data, []byte("final")) {
		t.Error("data mismatch")
	}
}

func TestPostgresStore_Fork(t *testing.T) {
	s := newTestPostgresStore(t)

	if _, _, err := s.Create("/source", CreateOptions{ContentType: "text/plain"}); err != nil {
		t.Fatalf("Create source failed: %v", err)
	}
	if _, err := s.Append("/source", []byte("one"), AppendOptions{}); err != nil {
		t.Fatalf("Append source failed: %v", err)
	}
	if _, err := s.Append("/source", []byte("two"), AppendOptions{}); err != nil {
		t.Fatalf("Append source failed: %v", err)
	}

	// Fork at the source's current tail (default fork offset).
	forkMeta, created, err := s.Create("/fork", CreateOptions{
		ContentType: "text/plain",
		ForkedFrom:  "/source",
	})
	if err != nil {
		t.Fatalf("Create fork failed: %v", err)
	}
	if !created {
		t.Error("expected fork to be newly created")
	}
	if forkMeta.ForkedFrom != "/source" {
		t.Errorf("forked_from mismatch: %q", forkMeta.ForkedFrom)
	}

	// Fork reads inherited source messages.
	messages, _, err := s.Read("/fork", ZeroOffset)
	if err != nil {
		t.Fatalf("Read fork failed: %v", err)
	}
	if len(messages) != 2 {
		t.Fatalf("expected 2 inherited messages, got %d", len(messages))
	}
	if !bytes.Equal(messages[0].Data, []byte("one")) || !bytes.Equal(messages[1].Data, []byte("two")) {
		t.Errorf("unexpected inherited messages: %q, %q", messages[0].Data, messages[1].Data)
	}

	// Appending to the fork does not affect the source.
	if _, err := s.Append("/fork", []byte("fork-only"), AppendOptions{}); err != nil {
		t.Fatalf("Append fork failed: %v", err)
	}
	sourceMessages, _, err := s.Read("/source", ZeroOffset)
	if err != nil {
		t.Fatalf("Read source failed: %v", err)
	}
	if len(sourceMessages) != 2 {
		t.Errorf("source should still have 2 messages, got %d", len(sourceMessages))
	}

	// Deleting the source soft-deletes it (fork still references it).
	if err := s.Delete("/source"); err != nil {
		t.Fatalf("Delete source failed: %v", err)
	}
	if s.Has("/source") {
		t.Error("source should be gone after delete")
	}
	// Fork can still read through the soft-deleted source.
	forkMessages, _, err := s.Read("/fork", ZeroOffset)
	if err != nil {
		t.Fatalf("Read fork after source delete failed: %v", err)
	}
	if len(forkMessages) != 3 {
		t.Errorf("expected 3 fork messages after source delete, got %d", len(forkMessages))
	}

	// Deleting the fork releases the source reference.
	if err := s.Delete("/fork"); err != nil {
		t.Fatalf("Delete fork failed: %v", err)
	}
}

func TestPostgresStore_ProducerIdempotency(t *testing.T) {
	s := newTestPostgresStore(t)

	if _, _, err := s.Create("/test", CreateOptions{ContentType: "text/plain"}); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	epoch := int64(1)
	seq := int64(0)
	opts := AppendOptions{ProducerId: "p1", ProducerEpoch: &epoch, ProducerSeq: &seq}

	result, err := s.Append("/test", []byte("first"), opts)
	if err != nil {
		t.Fatalf("first append failed: %v", err)
	}
	if result.ProducerResult != ProducerResultAccepted {
		t.Errorf("expected accepted, got %v", result.ProducerResult)
	}

	// Duplicate seq -> idempotent success, no new message.
	dup, err := s.Append("/test", []byte("first"), opts)
	if err != nil {
		t.Fatalf("duplicate append failed: %v", err)
	}
	if dup.ProducerResult != ProducerResultDuplicate {
		t.Errorf("expected duplicate, got %v", dup.ProducerResult)
	}

	messages, _, err := s.Read("/test", ZeroOffset)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if len(messages) != 1 {
		t.Errorf("expected 1 message after duplicate, got %d", len(messages))
	}

	// Gap -> error.
	seq = 5
	if _, err := s.Append("/test", []byte("gap"), opts); err != ErrProducerSeqGap {
		t.Errorf("expected ErrProducerSeqGap, got %v", err)
	}
}
