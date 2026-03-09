package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	redislib "github.com/redis/go-redis/v9"
	"github.com/streamweld/streamweld/internal/journal"
)

func TestNewServerWiresRedisJournalAndRegistryFromConfig(t *testing.T) {
	t.Parallel()
	redisServer := miniredis.RunT(t)
	config := DefaultConfig()
	config.BackendURL = "http://backend.example.test"
	config.JournalBackend = JournalBackendRedis
	config.RedisURL = "redis://" + redisServer.Addr() + "/0"
	config.RedisKeyPrefix = "wiring-test"
	config.ReaderMaxLagBytes = 64

	server, err := NewServer(config, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("NewServer() error: %v", err)
	}
	store, ok := server.durable.journal.(*journal.Redis)
	if !ok {
		t.Fatalf("journal type = %T, want *journal.Redis", server.durable.journal)
	}
	if server.durable.idempotency != store {
		t.Fatalf("idempotency type = %T, want same *journal.Redis", server.durable.idempotency)
	}
	streamID, err := journal.NewStreamID()
	if err != nil {
		t.Fatalf("NewStreamID() error: %v", err)
	}
	if err := store.Open(context.Background(), streamID, journal.Meta{
		Model: "model", BackendID: "backend",
	}); err != nil {
		t.Fatalf("Open() error: %v", err)
	}
	slow, cancelSlow, err := store.Tail(context.Background(), streamID, 1)
	if err != nil {
		t.Fatalf("Tail(slow) error: %v", err)
	}
	defer cancelSlow()
	fast, cancelFast, err := store.Tail(context.Background(), streamID, 1)
	if err != nil {
		t.Fatalf("Tail(fast) error: %v", err)
	}
	defer cancelFast()
	for index := 0; index < 10; index++ {
		seq, appendErr := store.Append(context.Background(), streamID, journal.Entry{
			Kind:    journal.KindChunk,
			Payload: json.RawMessage(`{}`),
		})
		if appendErr != nil {
			t.Fatalf("Append(%d) error: %v", index, appendErr)
		}
		select {
		case entry := <-fast:
			if entry.Err != nil || entry.Seq != seq {
				t.Fatalf("fast Tail() entry = %+v, want sequence %d", entry, seq)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for fast Tail() sequence %d", seq)
		}
	}
	select {
	case entry := <-slow:
		if !errors.Is(entry.Err, journal.ErrReaderLagged) {
			t.Fatalf("Tail() error = %v, want configured ErrReaderLagged", entry.Err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for configured reader lag eviction")
	}

	shutdownContext, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownContext); err != nil {
		t.Fatalf("Shutdown() error: %v", err)
	}
	id, err := journal.NewStreamID()
	if err != nil {
		t.Fatalf("NewStreamID() error: %v", err)
	}
	if _, err := store.ResolveOrCreate(context.Background(), "after-shutdown", id, time.Minute); err == nil {
		t.Fatal("owned Redis client remained usable after Shutdown")
	}
}

func TestNewServerUsesRedisJournalAsCustomIdempotencyRegistry(t *testing.T) {
	t.Parallel()
	redisServer := miniredis.RunT(t)
	client := redislib.NewClient(&redislib.Options{Addr: redisServer.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	redisConfig := journal.DefaultRedisConfig()
	redisConfig.Prefix = "custom-wiring-test"
	store, err := journal.NewRedis(client, redisConfig)
	if err != nil {
		t.Fatalf("journal.NewRedis() error: %v", err)
	}
	config := DefaultConfig()
	config.BackendURL = "http://backend.example.test"
	server, err := NewServer(config, nil, WithJournal(store))
	if err != nil {
		t.Fatalf("NewServer() error: %v", err)
	}
	if server.durable.idempotency != store {
		t.Fatalf("idempotency type = %T, want custom *journal.Redis", server.durable.idempotency)
	}
}
