package journal

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	redislib "github.com/redis/go-redis/v9"
)

func TestRedisReadStateAndEarlyTailPageLargeRetainedStream(t *testing.T) {
	t.Parallel()
	server := miniredis.RunT(t)
	base := redislib.NewClient(&redislib.Options{Addr: server.Addr()})
	client := &redisUnboundedRangeGuardClient{UniversalClient: base}
	t.Cleanup(func() { _ = base.Close() })
	config := DefaultRedisConfig()
	config.Prefix = "test-bounded-large-replay"
	config.ReadBlock = 5 * time.Millisecond
	config.ReaderMaxLagBytes = 1 << 20
	store, err := NewRedis(client, config)
	if err != nil {
		t.Fatalf("NewRedis() error: %v", err)
	}
	ctx := context.Background()
	id := newRedisTestStreamID(t)
	if err := store.Open(ctx, id, Meta{Model: "model", BackendID: "backend"}); err != nil {
		t.Fatalf("Open() error: %v", err)
	}
	const chunks = 512
	for index := 0; index < chunks; index++ {
		if _, err := store.Append(ctx, id, Entry{
			Kind:    KindChunk,
			Payload: json.RawMessage(fmt.Sprintf(`{"index":%d}`, index)),
		}); err != nil {
			t.Fatalf("Append(%d) error: %v", index, err)
		}
	}
	if err := store.Close(ctx, id, Entry{Kind: KindDone, Payload: json.RawMessage(`{}`)}); err != nil {
		t.Fatalf("Close() error: %v", err)
	}

	iterator, err := store.Read(ctx, id, 0)
	if err != nil {
		t.Fatalf("Read() error: %v", err)
	}
	readCount := 0
	iterator(func(_ Entry, entryErr error) bool {
		if entryErr != nil {
			t.Errorf("Read() iterator error: %v", entryErr)
			return false
		}
		readCount++
		return true
	})
	if readCount != chunks+2 {
		t.Fatalf("Read() count = %d, want %d", readCount, chunks+2)
	}
	state, err := store.State(ctx, id)
	if err != nil {
		t.Fatalf("State() error: %v", err)
	}
	if state.LastSeq != chunks+2 || state.Terminal == nil || state.Terminal.Seq != chunks+2 {
		t.Fatalf("State() = last %d terminal %+v, want terminal sequence %d", state.LastSeq, state.Terminal, chunks+2)
	}

	tail, cancel, err := store.Tail(ctx, id, 1)
	if err != nil {
		t.Fatalf("Tail(early cursor) error: %v", err)
	}
	defer cancel()
	tailCount := 0
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		select {
		case entry, open := <-tail:
			if !open {
				if tailCount != chunks+1 {
					t.Fatalf("Tail() count = %d, want %d", tailCount, chunks+1)
				}
				if client.sawUnboundedRange.Load() {
					t.Fatal("Read, State, or Tail issued unbounded XRANGE - +")
				}
				return
			}
			if entry.Err != nil {
				t.Fatalf("Tail() error: %v", entry.Err)
			}
			tailCount++
		case <-timer.C:
			t.Fatal("timed out waiting for paged terminal replay")
		}
	}
}

func TestRedisReadyTailAcceptsOneEnvelopeOverPayloadSizedLagLimit(t *testing.T) {
	t.Parallel()
	const maxLagBytes = int64(512)
	store := newRedisLagTestStore(t, "test-ready-near-limit", maxLagBytes)
	ctx := context.Background()
	id := newRedisTestStreamID(t)
	if err := store.Open(ctx, id, Meta{Model: "model", BackendID: "backend"}); err != nil {
		t.Fatalf("Open() error: %v", err)
	}
	payload := json.RawMessage(`{"data":"` + strings.Repeat("x", 480) + `"}`)
	_, envelopeBytes, err := prepareEntry(Entry{Kind: KindChunk, Payload: payload}, 2, time.Now().UTC())
	if err != nil {
		t.Fatalf("prepareEntry() error: %v", err)
	}
	if envelopeBytes <= maxLagBytes {
		t.Fatalf("test envelope is %d bytes, want over %d-byte payload-sized limit", envelopeBytes, maxLagBytes)
	}

	tail, cancel, err := store.Tail(ctx, id, 1)
	if err != nil {
		t.Fatalf("Tail() error: %v", err)
	}
	defer cancel()
	ready := make(chan struct{})
	received := make(chan Entry, 1)
	go func() {
		close(ready)
		received <- <-tail
	}()
	<-ready
	seq, err := store.Append(ctx, id, Entry{Kind: KindChunk, Payload: payload})
	if err != nil || seq != 2 {
		t.Fatalf("Append() = (%d, %v), want (2, nil)", seq, err)
	}
	select {
	case entry := <-received:
		if entry.Err != nil || entry.Seq != 2 {
			t.Fatalf("ready Tail() entry = %+v, want sequence 2", entry)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ready reader did not receive near-limit entry")
	}
	if err := store.Close(ctx, id, Entry{Kind: KindDone, Payload: json.RawMessage(`{}`)}); err != nil {
		t.Fatalf("Close() error: %v", err)
	}
	requireRedisTailSequence(t, tail, 3)
}

type redisUnboundedRangeGuardClient struct {
	redislib.UniversalClient
	sawUnboundedRange atomic.Bool
}

func (client *redisUnboundedRangeGuardClient) TxPipelined(
	ctx context.Context,
	operation func(redislib.Pipeliner) error,
) ([]redislib.Cmder, error) {
	return client.UniversalClient.TxPipelined(ctx, func(pipe redislib.Pipeliner) error {
		return operation(&redisUnboundedRangeGuardPipeline{
			Pipeliner:         pipe,
			sawUnboundedRange: &client.sawUnboundedRange,
		})
	})
}

type redisUnboundedRangeGuardPipeline struct {
	redislib.Pipeliner
	sawUnboundedRange *atomic.Bool
}

func (pipe *redisUnboundedRangeGuardPipeline) XRange(
	ctx context.Context,
	stream string,
	start string,
	stop string,
) *redislib.XMessageSliceCmd {
	if start == "-" && stop == "+" {
		pipe.sawUnboundedRange.Store(true)
	}
	return pipe.Pipeliner.XRange(ctx, stream, start, stop)
}
