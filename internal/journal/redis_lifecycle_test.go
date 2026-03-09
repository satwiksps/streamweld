package journal

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	redislib "github.com/redis/go-redis/v9"
)

func TestRedisTouchKeepsSilentActiveJournalAndBindingAlive(t *testing.T) {
	server := miniredis.RunT(t)
	client := redislib.NewClient(&redislib.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	config := DefaultRedisConfig()
	config.Prefix = "test-active-touch"
	config.TTL = 20 * time.Second
	store, err := NewRedis(client, config)
	if err != nil {
		t.Fatalf("NewRedis() error = %v", err)
	}
	ctx := context.Background()
	id := newRedisTestStreamID(t)
	binding, err := store.ResolveOrCreate(ctx, "silent-stream", id, config.TTL)
	if err != nil {
		t.Fatalf("ResolveOrCreate() error = %v", err)
	}
	digest := binding.Digest
	if err := store.Open(ctx, id, Meta{
		Model: "model", BackendID: "backend", Idempotency: &digest,
	}); err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	server.FastForward(15 * time.Second)
	if err := store.Touch(ctx, id); err != nil {
		t.Fatalf("Touch() error = %v", err)
	}
	for _, key := range store.streamKeys(id)[:2] {
		requireRedisTTL(t, server, key, config.TTL)
	}
	requireRedisTTL(t, server, store.streamKeys(id)[2], 2*config.TTL)
	requireRedisTTL(t, server, store.idempotencyKey(digest), config.TTL)

	// The wall clock has now advanced past the keys' original lifetime. Only
	// the atomic active lease keeps the complete journal resolvable.
	server.FastForward(15 * time.Second)
	state, err := store.State(ctx, id)
	if err != nil || state.Status != StatusOpen || state.LastSeq != 1 {
		t.Fatalf("State() beyond original TTL = (%+v, %v), want active sequence 1", state, err)
	}
	if _, err := store.Append(ctx, id, Entry{Kind: KindChunk, Payload: json.RawMessage(`{}`)}); err != nil {
		t.Fatalf("Append() beyond original TTL error = %v", err)
	}
}

func TestRedisTouchRejectsTerminalWithoutExtendingRetention(t *testing.T) {
	store, server := newRedisTestStore(t, "test-terminal-touch")
	ctx := context.Background()
	id := newRedisTestStreamID(t)
	if err := store.Open(ctx, id, Meta{Model: "model", BackendID: "backend"}); err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := store.Close(ctx, id, Entry{Kind: KindDone, Payload: json.RawMessage(`{}`)}); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	server.FastForward(store.config.TTL / 4)
	before := server.TTL(store.streamKeys(id)[0])
	if err := store.Touch(ctx, id); !errors.Is(err, ErrTerminalState) {
		t.Fatalf("Touch(terminal) error = %v, want ErrTerminalState", err)
	}
	if after := server.TTL(store.streamKeys(id)[0]); after != before {
		t.Fatalf("terminal TTL changed from %s to %s after rejected Touch", before, after)
	}
}

func TestRedisPendingReservationExpiresRenewsAndPromotes(t *testing.T) {
	store, server := newRedisTestStore(t, "test-pending-lifecycle")
	ctx := context.Background()
	const rawKey = "pending-lifecycle-key"
	firstID := newRedisTestStreamID(t)
	first, err := store.ResolveOrCreate(ctx, rawKey, firstID, store.config.TTL)
	if err != nil || !first.Created {
		t.Fatalf("first ResolveOrCreate() = (%+v, %v), want creator", first, err)
	}
	mappingKey := store.idempotencyKey(first.Digest)
	requireRedisTTL(t, server, mappingKey, defaultRedisPendingTTL)

	server.FastForward(4 * time.Second)
	if refreshed, err := store.RefreshPending(ctx, first.Digest, firstID, store.config.TTL); err != nil || !refreshed {
		t.Fatalf("RefreshPending() = (%t, %v), want (true, nil)", refreshed, err)
	}
	server.FastForward(4 * time.Second)
	observed, err := store.ResolveOrCreate(ctx, rawKey, newRedisTestStreamID(t), store.config.TTL)
	if err != nil || observed.Created || observed.ID != firstID {
		t.Fatalf("ResolveOrCreate() after original pending TTL = (%+v, %v), want pending %s", observed, err, firstID)
	}

	digest := first.Digest
	if err := store.Open(ctx, firstID, Meta{
		Model: "model", BackendID: "backend", Idempotency: &digest,
	}); err != nil {
		t.Fatalf("Open() promotion error = %v", err)
	}
	requireRedisTTL(t, server, mappingKey, store.config.TTL)
	if got, getErr := server.Get(mappingKey); getErr != nil || got != "r|"+firstID.String() {
		t.Fatalf("promoted mapping = %q, want ready binding", got)
	}
	if refreshed, err := store.RefreshPending(ctx, first.Digest, firstID, store.config.TTL); err != nil || refreshed {
		t.Fatalf("RefreshPending(promoted) = (%t, %v), want (false, nil)", refreshed, err)
	}
}

func TestRedisExpiredPendingReservationCannotOpenAfterReplacement(t *testing.T) {
	store, server := newRedisTestStore(t, "test-pending-fencing")
	ctx := context.Background()
	const rawKey = "crashed-creator-key"
	oldID := newRedisTestStreamID(t)
	oldBinding, err := store.ResolveOrCreate(ctx, rawKey, oldID, store.config.TTL)
	if err != nil {
		t.Fatalf("old ResolveOrCreate() error = %v", err)
	}
	server.FastForward(defaultRedisPendingTTL)

	newID := newRedisTestStreamID(t)
	newBinding, err := store.ResolveOrCreate(ctx, rawKey, newID, store.config.TTL)
	if err != nil || !newBinding.Created || newBinding.ID != newID {
		t.Fatalf("replacement ResolveOrCreate() = (%+v, %v), want new creator %s", newBinding, err, newID)
	}
	oldDigest := oldBinding.Digest
	if err := store.Open(ctx, oldID, Meta{
		Model: "model", BackendID: "backend", Idempotency: &oldDigest,
	}); !errors.Is(err, ErrIdempotencyReservationLost) {
		t.Fatalf("old Open() error = %v, want ErrIdempotencyReservationLost", err)
	}
	if _, err := store.State(ctx, oldID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("State(old ID) error = %v, want ErrNotFound", err)
	}
	newDigest := newBinding.Digest
	if err := store.Open(ctx, newID, Meta{
		Model: "model", BackendID: "backend", Idempotency: &newDigest,
	}); err != nil {
		t.Fatalf("replacement Open() error = %v", err)
	}
}
