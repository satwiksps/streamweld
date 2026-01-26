package journal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const testTimeout = 2 * time.Second

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (clock *fakeClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *fakeClock) Add(duration time.Duration) {
	clock.mu.Lock()
	clock.now = clock.now.Add(duration)
	clock.mu.Unlock()
}

func newTestMemory(t *testing.T, clock *fakeClock) *Memory {
	t.Helper()
	config := DefaultConfig()
	config.MaxTotalBytes = 32 << 20
	config.ReaderMaxLagBytes = 1 << 20
	config.Clock = clock.Now
	memory, err := NewMemory(config)
	if err != nil {
		t.Fatalf("NewMemory() error: %v", err)
	}
	return memory
}

func newTestID(t *testing.T) StreamID {
	t.Helper()
	id, err := NewStreamID()
	if err != nil {
		t.Fatalf("NewStreamID() error: %v", err)
	}
	return id
}

func collectRead(t *testing.T, sequence func(func(Entry, error) bool)) ([]Entry, error) {
	t.Helper()
	var entries []Entry
	for entry, err := range sequence {
		if err != nil {
			return entries, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func receiveEntry(t *testing.T, channel <-chan Entry) (Entry, bool) {
	t.Helper()
	select {
	case entry, ok := <-channel:
		return entry, ok
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for tail entry")
		return Entry{}, false
	}
}

func TestMemoryConfigDefaultsAndValidation(t *testing.T) {
	config := Config{MaxTotalBytes: 1, ReaderMaxLagBytes: 1}
	memory, err := NewMemory(config)
	if err != nil {
		t.Fatalf("NewMemory() with omitted defaulted fields error: %v", err)
	}
	if memory.config.TTL != DefaultTTL {
		t.Errorf("TTL = %s, want %s", memory.config.TTL, DefaultTTL)
	}
	if memory.config.MaxBytesPerStream != DefaultMaxBytesPerStream {
		t.Errorf("MaxBytesPerStream = %d, want %d", memory.config.MaxBytesPerStream, DefaultMaxBytesPerStream)
	}
	if memory.config.Clock == nil {
		t.Error("default Clock is nil")
	}

	tests := []Config{
		{},
		{MaxTotalBytes: 1},
		{ReaderMaxLagBytes: 1},
		{TTL: -1, MaxTotalBytes: 1, ReaderMaxLagBytes: 1},
		{MaxBytesPerStream: -1, MaxTotalBytes: 1, ReaderMaxLagBytes: 1},
		{MaxTotalBytes: -1, ReaderMaxLagBytes: 1},
		{MaxTotalBytes: 1, ReaderMaxLagBytes: -1},
	}
	for index, invalid := range tests {
		if _, err := NewMemory(invalid); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("NewMemory(invalid[%d]) error = %v, want ErrInvalidConfig", index, err)
		}
	}
}

func TestMemoryLifecycleReadAndState(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 8, 22, 10, 0, 0, 0, time.FixedZone("test", 5*60*60))}
	memory := newTestMemory(t, clock)
	id := newTestID(t)
	version := "sha256:origin"
	meta := Meta{Model: "model", ModelVersion: &version, BackendID: "backend-a"}

	if err := memory.Open(context.Background(), id, meta); err != nil {
		t.Fatalf("Open() error: %v", err)
	}
	if err := memory.Open(context.Background(), id, meta); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("duplicate Open() error = %v, want ErrAlreadyExists", err)
	}

	state, err := memory.State(context.Background(), id)
	if err != nil {
		t.Fatalf("State() error: %v", err)
	}
	wantCreated := clock.Now().UTC()
	if state.StreamID != id || state.Status != StatusOpen || !state.Resumable || state.LastSeq != 1 || state.EarliestSeq != 1 {
		t.Fatalf("initial State() = %+v", state)
	}
	if state.Model != meta.Model || state.ModelVersion == nil || *state.ModelVersion != version || state.OriginBackend != "backend-a" || state.CurrentBackend != "backend-a" {
		t.Errorf("initial metadata State() = %+v", state)
	}
	if !state.CreatedAt.Equal(wantCreated) || !state.UpdatedAt.Equal(wantCreated) {
		t.Errorf("initial timestamps = (%s, %s), want %s", state.CreatedAt, state.UpdatedAt, wantCreated)
	}

	clock.Add(time.Second)
	chunkPayload := json.RawMessage(`{"data":"hello","upstream_event":null}`)
	seq, err := memory.Append(context.Background(), id, Entry{
		Seq:     999,
		TS:      time.Unix(1, 0),
		Kind:    KindChunk,
		Payload: chunkPayload,
	})
	if err != nil || seq != 2 {
		t.Fatalf("Append() = (%d, %v), want (2, nil)", seq, err)
	}
	copy(chunkPayload, []byte(`{"data":"xxxxx"`))

	clock.Add(time.Second)
	migration := json.RawMessage(`{"from_backend":"backend-a","to_backend":"backend-b","reason":"tcp_reset","rescued_tokens":7,"token_count_estimated":true,"attempt":2}`)
	if seq, err = memory.Append(context.Background(), id, Entry{Kind: KindMigration, Payload: migration}); err != nil || seq != 3 {
		t.Fatalf("Append(migration) = (%d, %v), want (3, nil)", seq, err)
	}

	clock.Add(time.Second)
	done := json.RawMessage(`{"finish_reason":"stop","usage":{"prompt_tokens":2,"completion_tokens":3,"total_tokens":5,"estimated":false}}`)
	if err := memory.Close(context.Background(), id, Entry{Kind: KindDone, Payload: done}); err != nil {
		t.Fatalf("Close() error: %v", err)
	}
	if _, err := memory.Append(context.Background(), id, Entry{Kind: KindChunk, Payload: json.RawMessage(`{}`)}); !errors.Is(err, ErrTerminalState) {
		t.Errorf("Append() after close error = %v, want ErrTerminalState", err)
	}
	if err := memory.Close(context.Background(), id, Entry{Kind: KindError, Payload: json.RawMessage(`{}`)}); !errors.Is(err, ErrTerminalState) {
		t.Errorf("second Close() error = %v, want ErrTerminalState", err)
	}

	state, err = memory.State(context.Background(), id)
	if err != nil {
		t.Fatalf("terminal State() error: %v", err)
	}
	if state.Status != StatusDone || !state.Resumable || state.LastSeq != 4 || state.CurrentBackend != "backend-b" || state.OriginBackend != "backend-a" {
		t.Errorf("terminal State() = %+v", state)
	}
	if state.Usage.TotalTokens != 5 || len(state.Migrations) != 1 || state.Migrations[0].Seq != 3 || state.Migrations[0].Attempt != 2 {
		t.Errorf("derived State() = %+v", state)
	}
	if state.Terminal == nil || state.Terminal.Kind != KindDone || state.Terminal.Seq != 4 {
		t.Errorf("terminal snapshot = %+v", state.Terminal)
	}

	sequence, err := memory.Read(context.Background(), id, 0)
	if err != nil {
		t.Fatalf("Read() error: %v", err)
	}
	entries, err := collectRead(t, sequence)
	if err != nil {
		t.Fatalf("Read() iterator error: %v", err)
	}
	if len(entries) != 4 {
		t.Fatalf("Read() yielded %d entries, want 4", len(entries))
	}
	for index, entry := range entries {
		if entry.Seq != uint64(index+1) {
			t.Errorf("entry[%d].Seq = %d", index, entry.Seq)
		}
	}
	if got := string(entries[1].Payload); got != `{"data":"hello","upstream_event":null}` {
		t.Errorf("stored payload = %s; caller mutation leaked", got)
	}
	entries[1].Payload[0] = '['
	sequence, err = memory.Read(context.Background(), id, 1)
	if err != nil {
		t.Fatal(err)
	}
	again, err := collectRead(t, sequence)
	if err != nil || len(again) == 0 || again[0].Payload[0] != '{' {
		t.Errorf("Read() snapshot mutated storage: entries=%v err=%v", again, err)
	}
	if _, err := memory.Read(context.Background(), id, 5); !errors.Is(err, ErrCursorAhead) {
		t.Errorf("Read(cursor ahead) error = %v, want ErrCursorAhead", err)
	}
}

func TestMemoryRejectsInvalidEntriesAndContexts(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1, 0)}
	memory := newTestMemory(t, clock)
	id := newTestID(t)
	if err := memory.Open(context.Background(), id, Meta{}); err != nil {
		t.Fatal(err)
	}

	for _, entry := range []Entry{
		{Kind: KindOpen, Payload: json.RawMessage(`{}`)},
		{Kind: KindDone, Payload: json.RawMessage(`{}`)},
		{Kind: KindChunk, Payload: json.RawMessage(`null`)},
		{Kind: KindChunk, Payload: json.RawMessage(`{"broken":`)},
		{Kind: KindChunk, Payload: json.RawMessage(`{}`), Err: errors.New("delivery")},
	} {
		if _, err := memory.Append(context.Background(), id, entry); !errors.Is(err, ErrInvalidEntry) {
			t.Errorf("Append(%+v) error = %v, want ErrInvalidEntry", entry, err)
		}
	}
	if err := memory.Close(context.Background(), id, Entry{Kind: KindChunk, Payload: json.RawMessage(`{}`)}); !errors.Is(err, ErrInvalidEntry) {
		t.Errorf("Close(nonterminal) error = %v, want ErrInvalidEntry", err)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := memory.Append(canceled, id, Entry{Kind: KindChunk, Payload: json.RawMessage(`{}`)}); !errors.Is(err, context.Canceled) {
		t.Errorf("Append(canceled) error = %v", err)
	}
	var nilContext context.Context
	if _, err := memory.State(nilContext, id); !errors.Is(err, ErrInvalidContext) {
		t.Errorf("State(nil) error = %v, want ErrInvalidContext", err)
	}

	iteratorContext, iteratorCancel := context.WithCancel(context.Background())
	sequence, err := memory.Read(iteratorContext, id, 0)
	if err != nil {
		t.Fatal(err)
	}
	iteratorCancel()
	_, err = collectRead(t, sequence)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Read iterator error = %v, want context.Canceled", err)
	}
}

func TestMemoryTailReplayLiveTerminalAndCancel(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1, 0)}
	memory := newTestMemory(t, clock)
	id := newTestID(t)
	if err := memory.Open(context.Background(), id, Meta{}); err != nil {
		t.Fatal(err)
	}
	if _, err := memory.Append(context.Background(), id, Entry{Kind: KindChunk, Payload: json.RawMessage(`{"n":1}`)}); err != nil {
		t.Fatal(err)
	}

	channel, cancel, err := memory.Tail(context.Background(), id, 0)
	if err != nil {
		t.Fatalf("Tail() error: %v", err)
	}
	defer cancel()
	if _, err := memory.Append(context.Background(), id, Entry{Kind: KindChunk, Payload: json.RawMessage(`{"n":2}`)}); err != nil {
		t.Fatal(err)
	}
	if err := memory.Close(context.Background(), id, Entry{Kind: KindDone, Payload: json.RawMessage(`{"usage":{"prompt_tokens":0,"completion_tokens":0,"total_tokens":0,"estimated":false}}`)}); err != nil {
		t.Fatal(err)
	}
	for wantSeq := uint64(1); wantSeq <= 4; wantSeq++ {
		entry, ok := receiveEntry(t, channel)
		if !ok || entry.Err != nil || entry.Seq != wantSeq {
			t.Fatalf("tail entry for seq %d = (%+v, %t)", wantSeq, entry, ok)
		}
	}
	if _, ok := receiveEntry(t, channel); ok {
		t.Error("tail channel remained open after terminal entry")
	}

	closed, closeCancel, err := memory.Tail(context.Background(), id, 4)
	if err != nil {
		t.Fatal(err)
	}
	closeCancel()
	closeCancel()
	if _, ok := receiveEntry(t, closed); ok {
		t.Error("canceled tail channel remained open")
	}
}

func TestMemoryConcurrentAppendAndClose(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1, 0)}
	memory := newTestMemory(t, clock)
	id := newTestID(t)
	if err := memory.Open(context.Background(), id, Meta{}); err != nil {
		t.Fatal(err)
	}

	const appenders = 64
	sequences := make(chan uint64, appenders)
	errorsSeen := make(chan error, appenders)
	var wait sync.WaitGroup
	for index := 0; index < appenders; index++ {
		wait.Add(1)
		go func(value int) {
			defer wait.Done()
			seq, err := memory.Append(context.Background(), id, Entry{Kind: KindChunk, Payload: json.RawMessage(fmt.Sprintf(`{"n":%d}`, value))})
			if err != nil {
				errorsSeen <- err
				return
			}
			sequences <- seq
		}(index)
	}
	wait.Wait()
	close(sequences)
	close(errorsSeen)
	for err := range errorsSeen {
		t.Errorf("concurrent Append() error: %v", err)
	}
	seen := make(map[uint64]bool, appenders)
	for seq := range sequences {
		if seen[seq] {
			t.Errorf("duplicate sequence %d", seq)
		}
		seen[seq] = true
	}
	for seq := uint64(2); seq <= appenders+1; seq++ {
		if !seen[seq] {
			t.Errorf("missing committed sequence %d", seq)
		}
	}

	const closers = 32
	var winners atomic.Int32
	wait = sync.WaitGroup{}
	for index := 0; index < closers; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			err := memory.Close(context.Background(), id, Entry{Kind: KindDone, Payload: json.RawMessage(`{}`)})
			if err == nil {
				winners.Add(1)
				return
			}
			if !errors.Is(err, ErrTerminalState) {
				t.Errorf("concurrent Close() error = %v", err)
			}
		}()
	}
	wait.Wait()
	if winners.Load() != 1 {
		t.Errorf("successful closes = %d, want 1", winners.Load())
	}
	state, err := memory.State(context.Background(), id)
	if err != nil || state.LastSeq != appenders+2 {
		t.Errorf("State() after concurrent work = (%+v, %v)", state, err)
	}
}

func TestMemoryRingBoundariesAndOffsetExpiry(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1, 0)}
	memory := newTestMemory(t, clock)
	id := newTestID(t)
	if err := memory.Open(context.Background(), id, Meta{}); err != nil {
		t.Fatal(err)
	}
	payload := json.RawMessage(`{"data":"012345678901234567890123456789"}`)
	_, entrySize, err := prepareEntry(Entry{Kind: KindChunk, Payload: payload}, 2, clock.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	memory.mu.Lock()
	memory.config.MaxBytesPerStream = entrySize
	memory.mu.Unlock()
	if seq, err := memory.Append(context.Background(), id, Entry{Kind: KindChunk, Payload: payload}); err != nil || seq != 2 {
		t.Fatalf("entry exactly at per-stream cap = (%d, %v)", seq, err)
	}
	state, err := memory.State(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if state.EarliestSeq != 2 || state.LastSeq != 2 {
		t.Errorf("ring bounds = [%d,%d], want [2,2]", state.EarliestSeq, state.LastSeq)
	}
	if _, err := memory.Read(context.Background(), id, 0); !errors.Is(err, ErrOffsetExpired) {
		t.Errorf("Read(before ring) error = %v, want ErrOffsetExpired", err)
	}
	sequence, err := memory.Read(context.Background(), id, 1)
	if err != nil {
		t.Fatalf("Read(at ring boundary) error: %v", err)
	}
	entries, err := collectRead(t, sequence)
	if err != nil || len(entries) != 1 || entries[0].Seq != 2 {
		t.Errorf("Read(at ring boundary) = (%+v, %v)", entries, err)
	}

	memory.mu.Lock()
	memory.config.MaxBytesPerStream = entrySize - 1
	memory.mu.Unlock()
	if _, err := memory.Append(context.Background(), id, Entry{Kind: KindChunk, Payload: payload}); !errors.Is(err, ErrCapacity) {
		t.Errorf("entry one byte over cap error = %v, want ErrCapacity", err)
	}
	state, err = memory.State(context.Background(), id)
	if err != nil || state.LastSeq != 2 {
		t.Errorf("failed append changed state: (%+v, %v)", state, err)
	}
}

func TestMemoryGlobalCapacityUsesTerminalLRUAndNeverEvictsActive(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1, 0)}
	memory := newTestMemory(t, clock)
	idA, idB, idC := newTestID(t), newTestID(t), newTestID(t)
	for _, id := range []StreamID{idA, idB} {
		if err := memory.Open(context.Background(), id, Meta{}); err != nil {
			t.Fatal(err)
		}
		if err := memory.Close(context.Background(), id, Entry{Kind: KindDone, Payload: json.RawMessage(`{}`)}); err != nil {
			t.Fatal(err)
		}
		clock.Add(time.Second)
	}
	// A state read makes A most-recently used, so B must be evicted first.
	if _, err := memory.State(context.Background(), idA); err != nil {
		t.Fatal(err)
	}
	memory.mu.Lock()
	memory.config.MaxTotalBytes = memory.totalBytes
	memory.mu.Unlock()
	if err := memory.Open(context.Background(), idC, Meta{}); err != nil {
		t.Fatalf("Open() requiring terminal eviction: %v", err)
	}
	if _, err := memory.State(context.Background(), idA); err != nil {
		t.Errorf("MRU terminal stream was evicted: %v", err)
	}
	if _, err := memory.State(context.Background(), idB); !errors.Is(err, ErrExpired) {
		t.Errorf("LRU terminal State() error = %v, want ErrExpired", err)
	}
	memory.mu.Lock()
	if memory.totalBytes > memory.config.MaxTotalBytes {
		t.Errorf("hard cap exceeded: total=%d cap=%d", memory.totalBytes, memory.config.MaxTotalBytes)
	}
	memory.mu.Unlock()

	activeOnly := newTestMemory(t, clock)
	activeID, rejectedID := newTestID(t), newTestID(t)
	if err := activeOnly.Open(context.Background(), activeID, Meta{}); err != nil {
		t.Fatal(err)
	}
	activeOnly.mu.Lock()
	activeOnly.config.MaxTotalBytes = activeOnly.totalBytes
	activeOnly.mu.Unlock()
	if err := activeOnly.Open(context.Background(), rejectedID, Meta{}); !errors.Is(err, ErrCapacity) {
		t.Errorf("Open() beyond active-only cap error = %v, want ErrCapacity", err)
	}
	if _, err := activeOnly.State(context.Background(), activeID); err != nil {
		t.Errorf("active stream was evicted: %v", err)
	}
	if _, err := activeOnly.State(context.Background(), rejectedID); !errors.Is(err, ErrNotFound) {
		t.Errorf("failed Open() left stream behind: %v", err)
	}
}

func TestMemoryRetentionTombstoneAndStoppedState(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1, 0)}
	memory := newTestMemory(t, clock)
	memory.config.TTL = time.Minute
	id := newTestID(t)
	if err := memory.Open(context.Background(), id, Meta{}); err != nil {
		t.Fatal(err)
	}
	clock.Add(24 * time.Hour)
	if _, err := memory.State(context.Background(), id); err != nil {
		t.Fatalf("active stream expired by age: %v", err)
	}
	if err := memory.Close(context.Background(), id, Entry{Kind: KindStopped, Payload: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	state, err := memory.State(context.Background(), id)
	if err != nil || state.Status != StatusStopped || state.Resumable {
		t.Fatalf("stopped State() = (%+v, %v)", state, err)
	}
	if _, err := memory.Read(context.Background(), id, 0); !errors.Is(err, ErrNotResumable) {
		t.Errorf("Read(stopped) error = %v, want ErrNotResumable", err)
	}
	if _, _, err := memory.Tail(context.Background(), id, 0); !errors.Is(err, ErrNotResumable) {
		t.Errorf("Tail(stopped) error = %v, want ErrNotResumable", err)
	}

	clock.Add(time.Minute - time.Nanosecond)
	if _, err := memory.State(context.Background(), id); err != nil {
		t.Errorf("terminal stream expired early: %v", err)
	}
	clock.Add(time.Nanosecond)
	if _, err := memory.State(context.Background(), id); !errors.Is(err, ErrExpired) {
		t.Errorf("State(at terminal expiry) error = %v, want ErrExpired", err)
	}
	if err := memory.Open(context.Background(), id, Meta{}); !errors.Is(err, ErrAlreadyExists) {
		t.Errorf("Open(during tombstone) error = %v, want ErrAlreadyExists", err)
	}
	clock.Add(time.Minute)
	if _, err := memory.State(context.Background(), id); !errors.Is(err, ErrNotFound) {
		t.Errorf("State(after tombstone) error = %v, want ErrNotFound", err)
	}
}

func TestMemoryDropsOnlyLaggingTail(t *testing.T) {
	clock := &fakeClock{now: time.Unix(1, 0)}
	memory := newTestMemory(t, clock)
	id := newTestID(t)
	if err := memory.Open(context.Background(), id, Meta{}); err != nil {
		t.Fatal(err)
	}
	payload := json.RawMessage(`{"data":"a moderately sized journal entry"}`)
	_, entrySize, err := prepareEntry(Entry{Kind: KindChunk, Payload: payload}, 2, clock.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	memory.mu.Lock()
	memory.config.ReaderMaxLagBytes = entrySize
	memory.mu.Unlock()
	slow, slowCancel, err := memory.Tail(context.Background(), id, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer slowCancel()
	fast, fastCancel, err := memory.Tail(context.Background(), id, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer fastCancel()

	for index := 0; index < 6; index++ {
		if _, err := memory.Append(context.Background(), id, Entry{Kind: KindChunk, Payload: payload}); err != nil {
			t.Fatal(err)
		}
		entry, ok := receiveEntry(t, fast)
		if !ok || entry.Err != nil {
			t.Fatalf("fast tail entry = (%+v, %t)", entry, ok)
		}
	}
	entry, ok := receiveEntry(t, slow)
	if !ok || !errors.Is(entry.Err, ErrReaderLagged) || entry.Seq != 0 {
		t.Fatalf("slow tail signal = (%+v, %t), want unsequenced ErrReaderLagged", entry, ok)
	}
	if _, ok := receiveEntry(t, slow); ok {
		t.Error("lagged tail did not close")
	}
	state, err := memory.State(context.Background(), id)
	if err != nil || state.Status != StatusOpen || state.LastSeq != 7 {
		t.Errorf("lagged reader affected producer: State() = (%+v, %v)", state, err)
	}
}
