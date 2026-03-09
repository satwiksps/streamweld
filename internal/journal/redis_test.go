package journal

import (
	"context"
	"encoding/json"
	"errors"
	"iter"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	redislib "github.com/redis/go-redis/v9"
)

func TestRedisJournalLifecycleAcrossInstances(t *testing.T) {
	t.Parallel()
	server := miniredis.RunT(t)
	clientA := redislib.NewClient(&redislib.Options{Addr: server.Addr()})
	clientB := redislib.NewClient(&redislib.Options{Addr: server.Addr()})
	t.Cleanup(func() {
		_ = clientA.Close()
		_ = clientB.Close()
	})
	config := DefaultRedisConfig()
	config.Prefix = "test-lifecycle"
	config.ReadBlock = 10 * time.Millisecond
	storeA, err := NewRedis(clientA, config)
	if err != nil {
		t.Fatalf("NewRedis(A) error: %v", err)
	}
	storeB, err := NewRedis(clientB, config)
	if err != nil {
		t.Fatalf("NewRedis(B) error: %v", err)
	}

	ctx := context.Background()
	id := newRedisTestStreamID(t)
	version := "fixture-v1"
	if err := storeA.Open(ctx, id, Meta{
		Model:        "fixture-model",
		ModelVersion: &version,
		BackendID:    "backend-a",
		Endpoint:     "/v1/chat/completions",
		Request:      json.RawMessage(`{"model":"fixture-model"}`),
	}); err != nil {
		t.Fatalf("Open() error: %v", err)
	}

	tail, cancel, err := storeB.Tail(ctx, id, 0)
	if err != nil {
		t.Fatalf("Tail() error: %v", err)
	}
	defer cancel()
	if seq, err := storeA.Append(ctx, id, Entry{Kind: KindChunk, Payload: json.RawMessage(`{"data":"one"}`)}); err != nil || seq != 2 {
		t.Fatalf("Append(chunk) = (%d, %v), want (2, nil)", seq, err)
	}
	migration := json.RawMessage(`{"from_backend":"backend-a","to_backend":"backend-b","reason":"eof","rescued_tokens":1,"token_count_estimated":false,"attempt":1}`)
	if seq, err := storeA.Append(ctx, id, Entry{Kind: KindMigration, Payload: migration}); err != nil || seq != 3 {
		t.Fatalf("Append(migration) = (%d, %v), want (3, nil)", seq, err)
	}
	done := json.RawMessage(`{"usage":{"prompt_tokens":4,"completion_tokens":2,"total_tokens":6,"estimated":false}}`)
	if err := storeA.Close(ctx, id, Entry{Kind: KindDone, Payload: done}); err != nil {
		t.Fatalf("Close() error: %v", err)
	}

	entries := collectRedisTail(t, tail)
	requireRedisTestSequences(t, entries, 1, 2, 3, 4)
	if entries[0].Kind != KindOpen || entries[1].Kind != KindChunk || entries[3].Kind != KindDone {
		t.Fatalf("tail kinds = %v, want open/chunk/migration/done", redisTestKinds(entries))
	}
	state, err := storeB.State(ctx, id)
	if err != nil {
		t.Fatalf("State() error: %v", err)
	}
	if state.Status != StatusDone || state.CurrentBackend != "backend-b" || state.LastSeq != 4 || state.Terminal == nil {
		t.Fatalf("State() = %+v", state)
	}
	if state.ModelVersion == nil || *state.ModelVersion != version || state.Usage.TotalTokens != 6 || len(state.Migrations) != 1 {
		t.Fatalf("State() metadata = %+v", state)
	}

	iterator, err := storeB.Read(ctx, id, 1)
	if err != nil {
		t.Fatalf("Read(1) error: %v", err)
	}
	readEntries, readErr := collectRedisIterator(iterator)
	if readErr != nil {
		t.Fatalf("Read(1) iterator error: %v", readErr)
	}
	requireRedisTestSequences(t, readEntries, 2, 3, 4)
	if _, err := storeB.Append(ctx, id, Entry{Kind: KindChunk, Payload: json.RawMessage(`{}`)}); !errors.Is(err, ErrTerminalState) {
		t.Fatalf("Append() after close error = %v, want ErrTerminalState", err)
	}
}

func TestRedisJournalDegradedPrefixAndGap(t *testing.T) {
	t.Parallel()
	store, server := newRedisTestStore(t, "test-degraded")
	ctx := context.Background()
	id := newRedisTestStreamID(t)
	if err := store.Open(ctx, id, Meta{Model: "model", BackendID: "backend"}); err != nil {
		t.Fatalf("Open() error: %v", err)
	}
	if _, err := store.Append(ctx, id, Entry{Kind: KindChunk, Payload: json.RawMessage(`{"data":"prefix"}`)}); err != nil {
		t.Fatalf("Append() error: %v", err)
	}
	if err := store.MarkDegraded(ctx, id); err != nil {
		t.Fatalf("MarkDegraded() error: %v", err)
	}
	server.FastForward(store.config.TTL / 2)
	if err := store.MarkDegraded(ctx, id); err != nil {
		t.Fatalf("second MarkDegraded() error: %v", err)
	}
	if _, err := store.Append(ctx, id, Entry{Kind: KindChunk, Payload: json.RawMessage(`{}`)}); !errors.Is(err, ErrDegraded) {
		t.Fatalf("Append() degraded error = %v, want ErrDegraded", err)
	}
	if err := store.Close(ctx, id, Entry{Kind: KindDone, Payload: json.RawMessage(`{}`)}); !errors.Is(err, ErrDegraded) {
		t.Fatalf("Close() degraded error = %v, want ErrDegraded", err)
	}

	iterator, err := store.Read(ctx, id, 0)
	if err != nil {
		t.Fatalf("Read(0) error: %v", err)
	}
	entries, gap := collectRedisIterator(iterator)
	requireRedisTestSequences(t, entries, 1, 2)
	if !errors.Is(gap, ErrOffsetExpired) {
		t.Fatalf("Read(0) terminal error = %v, want ErrOffsetExpired", gap)
	}
	if _, err := store.Read(ctx, id, 2); !errors.Is(err, ErrOffsetExpired) {
		t.Fatalf("Read(last) error = %v, want ErrOffsetExpired", err)
	}
	if channel, cancel, err := store.Tail(ctx, id, 2); channel != nil || cancel != nil || !errors.Is(err, ErrOffsetExpired) {
		t.Fatalf("Tail(last) returned channel=%v cancel-set=%t err=%v", channel, cancel != nil, err)
	}

	server.FastForward(store.config.TTL / 2)
	if _, err := store.State(ctx, id); err != nil {
		t.Fatalf("State() at original journal TTL after marker refresh error = %v", err)
	}
	server.FastForward(store.config.TTL / 2)
	if _, err := store.State(ctx, id); !errors.Is(err, ErrExpired) {
		t.Fatalf("State() after journal TTL error = %v, want ErrExpired", err)
	}
	server.FastForward(store.config.TTL)
	if _, err := store.State(ctx, id); !errors.Is(err, ErrNotFound) {
		t.Fatalf("State() after tombstone TTL error = %v, want ErrNotFound", err)
	}
}

func TestRedisIdempotencySharedAtomicAndHashed(t *testing.T) {
	t.Parallel()
	server := miniredis.RunT(t)
	clientA := redislib.NewClient(&redislib.Options{Addr: server.Addr()})
	clientB := redislib.NewClient(&redislib.Options{Addr: server.Addr()})
	t.Cleanup(func() {
		_ = clientA.Close()
		_ = clientB.Close()
	})
	config := DefaultRedisConfig()
	config.Prefix = "test-idempotency"
	storeA, _ := NewRedis(clientA, config)
	storeB, _ := NewRedis(clientB, config)
	idA := newRedisTestStreamID(t)
	idB := newRedisTestStreamID(t)
	const rawKey = "do-not-store-this-private-key"

	start := make(chan struct{})
	bindings := make(chan IdempotencyBinding, 2)
	errorsFound := make(chan error, 2)
	var group sync.WaitGroup
	for _, candidate := range []struct {
		store *Redis
		id    StreamID
	}{{storeA, idA}, {storeB, idB}} {
		group.Add(1)
		go func(store *Redis, id StreamID) {
			defer group.Done()
			<-start
			binding, err := store.ResolveOrCreate(context.Background(), rawKey, id, time.Minute)
			if err != nil {
				errorsFound <- err
				return
			}
			bindings <- binding
		}(candidate.store, candidate.id)
	}
	close(start)
	group.Wait()
	close(bindings)
	close(errorsFound)
	for err := range errorsFound {
		t.Fatalf("ResolveOrCreate() error: %v", err)
	}
	var got []IdempotencyBinding
	for binding := range bindings {
		got = append(got, binding)
	}
	if len(got) != 2 || got[0].ID != got[1].ID || got[0].Created == got[1].Created {
		t.Fatalf("bindings = %+v, want one shared ID and exactly one creator", got)
	}
	for _, key := range server.Keys() {
		if key == rawKey || containsRedisTestSubstring(key, rawKey) {
			t.Fatalf("Redis retained raw idempotency key in %q", key)
		}
	}
	digest, _ := DigestKey(rawKey)
	if refreshed, err := storeB.Refresh(context.Background(), digest, 2*time.Minute); err != nil || !refreshed {
		t.Fatalf("Refresh() = (%t, %v), want (true, nil)", refreshed, err)
	}
	if removed, err := storeA.Remove(context.Background(), digest); err != nil || !removed {
		t.Fatalf("Remove() = (%t, %v), want (true, nil)", removed, err)
	}
	if removed, err := storeB.Remove(context.Background(), digest); err != nil || removed {
		t.Fatalf("second Remove() = (%t, %v), want (false, nil)", removed, err)
	}
}

func TestRedisJournalDoesNotConsumeSequenceWhenStreamKeyIsCorrupt(t *testing.T) {
	t.Parallel()
	store, server := newRedisTestStore(t, "test-corrupt")
	ctx := context.Background()
	id := newRedisTestStreamID(t)
	if err := store.Open(ctx, id, Meta{Model: "model", BackendID: "backend"}); err != nil {
		t.Fatalf("Open() error: %v", err)
	}
	eventsKey := store.eventsKey(id)
	server.Del(eventsKey)
	if err := server.Set(eventsKey, "wrong-type"); err != nil {
		t.Fatalf("corrupt events key: %v", err)
	}
	if _, err := store.Append(ctx, id, Entry{Kind: KindChunk, Payload: json.RawMessage(`{}`)}); err == nil {
		t.Fatal("Append() accepted corrupt Redis stream key")
	}
	stateKey := store.streamKeys(id)[0]
	if got := server.HGet(stateKey, "last_seq"); got != "1" {
		t.Fatalf("last_seq = %q after failed XADD, want 1", got)
	}
}

func TestNewRedisValidatesConfiguration(t *testing.T) {
	t.Parallel()
	server := miniredis.RunT(t)
	client := redislib.NewClient(&redislib.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	for _, config := range []RedisConfig{
		{TTL: -time.Second, Prefix: "valid", ReadBlock: time.Second},
		{TTL: time.Second, Prefix: "bad{tag}", ReadBlock: time.Second},
		{TTL: time.Second, Prefix: "valid", ReadBlock: -time.Second},
		{TTL: time.Second, Prefix: "valid", ReadBlock: time.Second, ReaderMaxLagBytes: -1},
		{TTL: time.Second, Prefix: "valid", ReadBlock: time.Second, MutationReceiptTTL: -time.Second},
	} {
		if _, err := NewRedis(client, config); !errors.Is(err, ErrInvalidRedisConfig) {
			t.Fatalf("NewRedis(%+v) error = %v, want ErrInvalidRedisConfig", config, err)
		}
	}
	if _, err := NewRedis(nil, DefaultRedisConfig()); !errors.Is(err, ErrInvalidRedisConfig) {
		t.Fatalf("NewRedis(nil) error = %v, want ErrInvalidRedisConfig", err)
	}
}

func newRedisTestStore(t *testing.T, prefix string) (*Redis, *miniredis.Miniredis) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redislib.NewClient(&redislib.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	config := DefaultRedisConfig()
	config.Prefix = prefix
	config.ReadBlock = 10 * time.Millisecond
	store, err := NewRedis(client, config)
	if err != nil {
		t.Fatalf("NewRedis() error: %v", err)
	}
	return store, server
}

func newRedisTestStreamID(t *testing.T) StreamID {
	t.Helper()
	id, err := NewStreamID()
	if err != nil {
		t.Fatalf("NewStreamID() error: %v", err)
	}
	return id
}

func collectRedisTail(t *testing.T, tail <-chan Entry) []Entry {
	t.Helper()
	var entries []Entry
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	for {
		select {
		case entry, open := <-tail:
			if !open {
				return entries
			}
			if entry.Err != nil {
				t.Fatalf("tail error: %v", entry.Err)
			}
			entries = append(entries, entry)
		case <-timer.C:
			t.Fatal("timed out waiting for Redis tail to close")
		}
	}
}

func collectRedisIterator(iterator iter.Seq2[Entry, error]) ([]Entry, error) {
	var entries []Entry
	var terminal error
	iterator(func(entry Entry, err error) bool {
		if err != nil {
			terminal = err
			return false
		}
		entries = append(entries, entry)
		return true
	})
	return entries, terminal
}

func requireRedisTestSequences(t *testing.T, entries []Entry, want ...uint64) {
	t.Helper()
	if len(entries) != len(want) {
		t.Fatalf("entry count = %d, want %d: %+v", len(entries), len(want), entries)
	}
	for index, sequence := range want {
		if entries[index].Seq != sequence {
			t.Fatalf("entry[%d].Seq = %d, want %d", index, entries[index].Seq, sequence)
		}
	}
}

func redisTestKinds(entries []Entry) []EntryKind {
	kinds := make([]EntryKind, len(entries))
	for index := range entries {
		kinds[index] = entries[index].Kind
	}
	return kinds
}

func containsRedisTestSubstring(value, substring string) bool {
	for index := 0; index+len(substring) <= len(value); index++ {
		if value[index:index+len(substring)] == substring {
			return true
		}
	}
	return false
}
