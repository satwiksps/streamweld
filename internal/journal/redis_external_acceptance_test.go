package journal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	redislib "github.com/redis/go-redis/v9"
)

const redisAcceptanceURLVariable = "STREAMWELD_TEST_REDIS_URL"

// TestRedisExternalAcceptanceCrossInstanceDurability runs against a real Redis
// process when STREAMWELD_TEST_REDIS_URL is set. The always-on proxy acceptance
// tests use an injected shared store; this test verifies that two independent
// Redis clients provide the same cross-process journal, fan-out, idempotency,
// and degradation semantics.
func TestRedisExternalAcceptanceCrossInstanceDurability(t *testing.T) {
	clientA, clientB, prefix := newRedisAcceptanceClients(t)
	config := DefaultRedisConfig()
	config.Prefix = prefix
	config.TTL = time.Minute
	config.ReadBlock = 20 * time.Millisecond
	storeA, err := NewRedis(clientA, config)
	if err != nil {
		t.Fatalf("NewRedis(client A) error = %v", err)
	}
	storeB, err := NewRedis(clientB, config)
	if err != nil {
		t.Fatalf("NewRedis(client B) error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	streamID := mustRedisAcceptanceID(t, "00000000000000000000000101")
	otherID := mustRedisAcceptanceID(t, "00000000000000000000000102")
	if err := storeA.Open(ctx, streamID, Meta{
		Model:     "phase5-model",
		BackendID: "replica-a-backend",
	}); err != nil {
		t.Fatalf("store A Open() error = %v", err)
	}
	if seq, err := storeA.Append(ctx, streamID, Entry{
		Kind:    KindChunk,
		Payload: json.RawMessage(`{"data":"hello","upstream_event":null}`),
	}); err != nil || seq != 2 {
		t.Fatalf("store A Append(first) = (%d, %v), want (2, nil)", seq, err)
	}

	resume, cancelResume, err := storeB.Tail(ctx, streamID, 1)
	if err != nil {
		t.Fatalf("store B Tail(1) error = %v", err)
	}
	defer cancelResume()
	fanoutA, cancelFanoutA, err := storeA.Tail(ctx, streamID, 0)
	if err != nil {
		t.Fatalf("store A Tail(0) error = %v", err)
	}
	defer cancelFanoutA()
	fanoutB, cancelFanoutB, err := storeB.Tail(ctx, streamID, 0)
	if err != nil {
		t.Fatalf("store B Tail(0) error = %v", err)
	}
	defer cancelFanoutB()

	firstBinding, err := storeA.ResolveOrCreate(ctx, "shared-phase5-key", streamID, time.Minute)
	if err != nil {
		t.Fatalf("store A ResolveOrCreate() error = %v", err)
	}
	secondBinding, err := storeB.ResolveOrCreate(ctx, "shared-phase5-key", otherID, time.Minute)
	if err != nil {
		t.Fatalf("store B ResolveOrCreate() error = %v", err)
	}
	if !firstBinding.Created || secondBinding.Created || firstBinding.ID != streamID || secondBinding.ID != streamID {
		t.Fatalf("cross-instance idempotency bindings = first %#v, second %#v", firstBinding, secondBinding)
	}

	if seq, err := storeA.Append(ctx, streamID, Entry{
		Kind:    KindChunk,
		Payload: json.RawMessage(`{"data":" world","upstream_event":null}`),
	}); err != nil || seq != 3 {
		t.Fatalf("store A Append(second) = (%d, %v), want (3, nil)", seq, err)
	}
	if err := storeA.Close(ctx, streamID, Entry{
		Kind:    KindDone,
		Payload: json.RawMessage(`{"finish_reason":"stop","usage":{"prompt_tokens":1,"completion_tokens":2,"total_tokens":3,"estimated":false}}`),
	}); err != nil {
		t.Fatalf("store A Close() error = %v", err)
	}

	resumedEntries := collectRedisAcceptanceTail(t, resume)
	requireRedisAcceptanceSequences(t, resumedEntries, 2, 3, 4)
	fanoutAEntries := collectRedisAcceptanceTail(t, fanoutA)
	fanoutBEntries := collectRedisAcceptanceTail(t, fanoutB)
	requireRedisAcceptanceSequences(t, fanoutAEntries, 1, 2, 3, 4)
	if !reflect.DeepEqual(fanoutAEntries, fanoutBEntries) {
		t.Fatalf("independent Redis fan-out differs:\nA: %#v\nB: %#v", fanoutAEntries, fanoutBEntries)
	}

	state, err := storeB.State(ctx, streamID)
	if err != nil {
		t.Fatalf("store B State() error = %v", err)
	}
	if state.Status != StatusDone || state.LastSeq != 4 || state.Terminal == nil || state.Terminal.Kind != KindDone {
		t.Fatalf("store B terminal state = %#v", state)
	}
}

func TestRedisExternalAcceptanceDegradedPrefixEndsInGap(t *testing.T) {
	clientA, clientB, prefix := newRedisAcceptanceClients(t)
	config := DefaultRedisConfig()
	config.Prefix = prefix
	config.TTL = time.Minute
	config.ReadBlock = 20 * time.Millisecond
	storeA, err := NewRedis(clientA, config)
	if err != nil {
		t.Fatalf("NewRedis(client A) error = %v", err)
	}
	storeB, err := NewRedis(clientB, config)
	if err != nil {
		t.Fatalf("NewRedis(client B) error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	streamID := mustRedisAcceptanceID(t, "00000000000000000000000103")
	if err := storeA.Open(ctx, streamID, Meta{Model: "phase5-model", BackendID: "backend-a"}); err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if seq, err := storeA.Append(ctx, streamID, Entry{
		Kind:    KindChunk,
		Payload: json.RawMessage(`{"data":"committed","upstream_event":null}`),
	}); err != nil || seq != 2 {
		t.Fatalf("Append() = (%d, %v), want (2, nil)", seq, err)
	}
	if err := storeA.MarkDegraded(ctx, streamID); err != nil {
		t.Fatalf("MarkDegraded() error = %v", err)
	}

	sequence, err := storeB.Read(ctx, streamID, 0)
	if err != nil {
		t.Fatalf("Read(0) error = %v", err)
	}
	var readEntries []Entry
	var readGap error
	for entry, readErr := range sequence {
		if readErr != nil {
			readGap = readErr
			continue
		}
		readEntries = append(readEntries, entry)
	}
	requireRedisAcceptanceSequences(t, readEntries, 1, 2)
	if !errors.Is(readGap, ErrOffsetExpired) {
		t.Fatalf("Read(0) terminal error = %v, want ErrOffsetExpired", readGap)
	}

	tail, cancelTail, err := storeB.Tail(ctx, streamID, 0)
	if err != nil {
		t.Fatalf("Tail(0) error = %v", err)
	}
	defer cancelTail()
	tailEntries, tailGap := collectRedisAcceptanceTailWithError(t, tail)
	requireRedisAcceptanceSequences(t, tailEntries, 1, 2)
	if !errors.Is(tailGap, ErrOffsetExpired) {
		t.Fatalf("Tail(0) terminal error = %v, want ErrOffsetExpired", tailGap)
	}

	atGap, cancelAtGap, err := storeB.Tail(ctx, streamID, 2)
	if cancelAtGap != nil {
		defer cancelAtGap()
	}
	if atGap != nil || !errors.Is(err, ErrOffsetExpired) {
		t.Fatalf("Tail(last sequence) = (channel nil: %v, cancel nil: %v, error: %v), want (true, true, ErrOffsetExpired)", atGap == nil, cancelAtGap == nil, err)
	}
}

func newRedisAcceptanceClients(t *testing.T) (*redislib.Client, *redislib.Client, string) {
	t.Helper()
	rawURL := strings.TrimSpace(os.Getenv(redisAcceptanceURLVariable))
	if rawURL == "" {
		t.Skipf("set %s to run real Redis acceptance tests", redisAcceptanceURLVariable)
	}

	var options *redislib.Options
	var err error
	if strings.Contains(rawURL, "://") {
		options, err = redislib.ParseURL(rawURL)
	} else {
		options = &redislib.Options{Addr: rawURL}
	}
	if err != nil {
		t.Fatalf("parse %s: %v", redisAcceptanceURLVariable, err)
	}
	clientA := redislib.NewClient(options)
	optionsB := *options
	clientB := redislib.NewClient(&optionsB)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := clientA.Ping(ctx).Err(); err != nil {
		_ = clientA.Close()
		_ = clientB.Close()
		t.Fatalf("connect to Redis from %s: %v", redisAcceptanceURLVariable, err)
	}

	prefix := fmt.Sprintf("streamweld:acceptance:%d", time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cleanupCancel()
		var cursor uint64
		for {
			keys, next, scanErr := clientA.Scan(cleanupCtx, cursor, prefix+"*", 100).Result()
			if scanErr != nil {
				t.Errorf("scan acceptance Redis keys: %v", scanErr)
				break
			}
			if len(keys) != 0 {
				if err := clientA.Del(cleanupCtx, keys...).Err(); err != nil {
					t.Errorf("delete acceptance Redis keys: %v", err)
					break
				}
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
		if err := clientA.Close(); err != nil {
			t.Errorf("close Redis client A: %v", err)
		}
		if err := clientB.Close(); err != nil {
			t.Errorf("close Redis client B: %v", err)
		}
	})
	return clientA, clientB, prefix
}

func mustRedisAcceptanceID(t *testing.T, value string) StreamID {
	t.Helper()
	id, err := ParseStreamID(value)
	if err != nil {
		t.Fatalf("ParseStreamID(%q) error = %v", value, err)
	}
	return id
}

func collectRedisAcceptanceTail(t *testing.T, tail <-chan Entry) []Entry {
	t.Helper()
	entries, err := collectRedisAcceptanceTailWithError(t, tail)
	if err != nil {
		t.Fatalf("Tail() delivery error = %v", err)
	}
	return entries
}

func collectRedisAcceptanceTailWithError(t *testing.T, tail <-chan Entry) ([]Entry, error) {
	t.Helper()
	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()
	var entries []Entry
	var deliveryErr error
	for {
		select {
		case entry, open := <-tail:
			if !open {
				return entries, deliveryErr
			}
			if entry.Err != nil {
				deliveryErr = entry.Err
				continue
			}
			entries = append(entries, entry)
		case <-timeout.C:
			t.Fatalf("timed out waiting for Redis tail; entries = %#v, error = %v", entries, deliveryErr)
		}
	}
}

func requireRedisAcceptanceSequences(t *testing.T, entries []Entry, sequences ...uint64) {
	t.Helper()
	if len(entries) != len(sequences) {
		t.Fatalf("entry count = %d, want %d: %#v", len(entries), len(sequences), entries)
	}
	for index, sequence := range sequences {
		if entries[index].Seq != sequence {
			t.Fatalf("entries[%d].Seq = %d, want %d: %#v", index, entries[index].Seq, sequence, entries)
		}
	}
}
