package journal

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	redislib "github.com/redis/go-redis/v9"
)

func TestRedisAmbiguousAppendRetrySurvivesInterveningCommit(t *testing.T) {
	t.Parallel()
	server := miniredis.RunT(t)
	baseA := redislib.NewClient(&redislib.Options{Addr: server.Addr(), MaxRetries: -1})
	baseB := redislib.NewClient(&redislib.Options{Addr: server.Addr(), MaxRetries: -1})
	t.Cleanup(func() {
		_ = baseA.Close()
		_ = baseB.Close()
	})
	interleaving := &interleavingExecuteRedisClient{UniversalClient: baseA}
	config := DefaultRedisConfig()
	config.Prefix = "test-intervening-receipt"
	storeA, err := NewRedis(interleaving, config)
	if err != nil {
		t.Fatalf("NewRedis(A) error: %v", err)
	}
	storeB, err := NewRedis(baseB, config)
	if err != nil {
		t.Fatalf("NewRedis(B) error: %v", err)
	}

	ctx := context.Background()
	id := newRedisTestStreamID(t)
	if err := storeB.Open(ctx, id, Meta{Model: "model", BackendID: "backend"}); err != nil {
		t.Fatalf("Open() error: %v", err)
	}
	executed, resume := interleaving.arm()
	var resumeOnce sync.Once
	release := func() { resumeOnce.Do(func() { close(resume) }) }
	t.Cleanup(release)

	type appendResult struct {
		seq uint64
		err error
	}
	resultA := make(chan appendResult, 1)
	go func() {
		seq, appendErr := storeA.Append(ctx, id, Entry{
			Kind:    KindChunk,
			Payload: json.RawMessage(`{"source":"a"}`),
		})
		resultA <- appendResult{seq: seq, err: appendErr}
	}()

	select {
	case <-executed:
	case <-time.After(2 * time.Second):
		t.Fatal("A did not execute its first append")
	}
	seqB, err := storeB.Append(ctx, id, Entry{
		Kind:    KindChunk,
		Payload: json.RawMessage(`{"source":"b"}`),
	})
	if err != nil || seqB != 3 {
		t.Fatalf("B Append() = (%d, %v), want (3, nil)", seqB, err)
	}
	release()

	select {
	case result := <-resultA:
		if result.err != nil || result.seq != 2 {
			t.Fatalf("A retried Append() = (%d, %v), want original result (2, nil)", result.seq, result.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("A retry did not complete")
	}
	state, err := storeB.State(ctx, id)
	if err != nil {
		t.Fatalf("State() error: %v", err)
	}
	if state.LastSeq != 3 {
		t.Fatalf("LastSeq = %d, want 3 (A retry must not append again)", state.LastSeq)
	}
}

func TestRedisOldAppendReceiptDoesNotRefreshInterveningTerminalRetention(t *testing.T) {
	t.Parallel()
	server := miniredis.RunT(t)
	baseA := redislib.NewClient(&redislib.Options{Addr: server.Addr(), MaxRetries: -1})
	baseB := redislib.NewClient(&redislib.Options{Addr: server.Addr(), MaxRetries: -1})
	t.Cleanup(func() {
		_ = baseA.Close()
		_ = baseB.Close()
	})
	interleaving := &interleavingExecuteRedisClient{UniversalClient: baseA}
	config := DefaultRedisConfig()
	config.Prefix = "test-terminal-receipt-retention"
	config.TTL = time.Minute
	storeA, err := NewRedis(interleaving, config)
	if err != nil {
		t.Fatalf("NewRedis(A) error: %v", err)
	}
	storeB, err := NewRedis(baseB, config)
	if err != nil {
		t.Fatalf("NewRedis(B) error: %v", err)
	}

	ctx := context.Background()
	id := newRedisTestStreamID(t)
	binding, err := storeB.ResolveOrCreate(ctx, "terminal-retention-key", id, config.TTL)
	if err != nil {
		t.Fatalf("ResolveOrCreate() error: %v", err)
	}
	if err := storeB.Open(ctx, id, Meta{
		Model:       "model",
		BackendID:   "backend",
		Idempotency: &binding.Digest,
	}); err != nil {
		t.Fatalf("Open() error: %v", err)
	}
	executed, resume := interleaving.arm()
	var resumeOnce sync.Once
	release := func() { resumeOnce.Do(func() { close(resume) }) }
	t.Cleanup(release)

	type appendResult struct {
		seq uint64
		err error
	}
	resultA := make(chan appendResult, 1)
	go func() {
		seq, appendErr := storeA.Append(ctx, id, Entry{
			Kind:    KindChunk,
			Payload: json.RawMessage(`{"source":"a"}`),
		})
		resultA <- appendResult{seq: seq, err: appendErr}
	}()
	select {
	case <-executed:
	case <-time.After(2 * time.Second):
		t.Fatal("A did not execute its first append")
	}
	if err := storeB.Close(ctx, id, Entry{Kind: KindDone, Payload: json.RawMessage(`{}`)}); err != nil {
		t.Fatalf("B Close() error: %v", err)
	}
	server.FastForward(20 * time.Second)
	release()
	select {
	case result := <-resultA:
		if result.err != nil || result.seq != 2 {
			t.Fatalf("A retried Append() = (%d, %v), want original result (2, nil)", result.seq, result.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("A retry did not complete")
	}

	keys := storeB.streamKeys(id)
	requireRedisTTL(t, server, keys[0], 40*time.Second)
	requireRedisTTL(t, server, keys[1], 40*time.Second)
	requireRedisTTL(t, server, keys[2], 100*time.Second)
	requireRedisTTL(t, server, storeB.idempotencyKey(binding.Digest), 40*time.Second)
}

func TestRedisMutationReceiptsExpireBeforeJournal(t *testing.T) {
	t.Parallel()
	server := miniredis.RunT(t)
	client := redislib.NewClient(&redislib.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	config := DefaultRedisConfig()
	config.Prefix = "test-short-receipts"
	config.TTL = time.Minute
	config.MutationReceiptTTL = 5 * time.Second
	store, err := NewRedis(client, config)
	if err != nil {
		t.Fatalf("NewRedis() error: %v", err)
	}
	ctx := context.Background()
	id := newRedisTestStreamID(t)
	if err := store.Open(ctx, id, Meta{Model: "model", BackendID: "backend"}); err != nil {
		t.Fatalf("Open() error: %v", err)
	}
	const nonce = "0123456789abcdef0123456789abcdef"
	store.nonce = func() (string, error) { return nonce, nil }
	if _, err := store.Append(ctx, id, Entry{Kind: KindChunk, Payload: json.RawMessage(`{}`)}); err != nil {
		t.Fatalf("Append() error: %v", err)
	}
	receiptKey := store.mutationReceiptKey(id, nonce)
	if !server.Exists(receiptKey) {
		t.Fatal("mutation receipt was not persisted")
	}
	if got := server.TTL(receiptKey); got != config.MutationReceiptTTL {
		t.Fatalf("receipt TTL = %s, want %s", got, config.MutationReceiptTTL)
	}

	server.FastForward(config.MutationReceiptTTL)
	if server.Exists(receiptKey) {
		t.Fatal("mutation receipt did not expire independently")
	}
	for _, key := range store.streamKeys(id)[:2] {
		if !server.Exists(key) {
			t.Fatalf("journal key %q expired with short-lived receipt", key)
		}
	}
}

func TestRedisMutationReceiptTTLIsCappedByJournalRetention(t *testing.T) {
	t.Parallel()
	server := miniredis.RunT(t)
	client := redislib.NewClient(&redislib.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	config := DefaultRedisConfig()
	config.TTL = 5 * time.Second
	config.MutationReceiptTTL = time.Minute
	store, err := NewRedis(client, config)
	if err != nil {
		t.Fatalf("NewRedis() error: %v", err)
	}
	if store.config.MutationReceiptTTL != config.TTL {
		t.Fatalf(
			"MutationReceiptTTL = %s, want retention cap %s",
			store.config.MutationReceiptTTL,
			config.TTL,
		)
	}
}

func TestRedisMutationReconcilesReceiptAfterCallerContextExpires(t *testing.T) {
	t.Parallel()
	server := miniredis.RunT(t)
	base := redislib.NewClient(&redislib.Options{Addr: server.Addr(), MaxRetries: -1})
	client := &executeThenCancelRedisClient{UniversalClient: base}
	t.Cleanup(func() { _ = base.Close() })
	config := DefaultRedisConfig()
	config.Prefix = "test-context-reconciliation"
	store, err := NewRedis(client, config)
	if err != nil {
		t.Fatalf("NewRedis() error: %v", err)
	}
	id := newRedisTestStreamID(t)
	openContext, cancelOpen := context.WithCancel(context.Background())
	client.arm(cancelOpen)
	if err := store.Open(openContext, id, Meta{Model: "model", BackendID: "backend"}); err != nil {
		t.Fatalf("Open() after execute-then-cancel error: %v", err)
	}
	if openContext.Err() == nil {
		t.Fatal("test client did not cancel Open context")
	}
	appendContext, cancelAppend := context.WithCancel(context.Background())
	client.arm(cancelAppend)
	seq, err := store.Append(appendContext, id, Entry{Kind: KindChunk, Payload: json.RawMessage(`{}`)})
	if err != nil || seq != 2 {
		t.Fatalf("Append() after execute-then-cancel = (%d, %v), want reconciled (2, nil)", seq, err)
	}
	if appendContext.Err() == nil {
		t.Fatal("test client did not cancel Append context")
	}

	closeContext, cancelClose := context.WithCancel(context.Background())
	client.arm(cancelClose)
	if err := store.Close(closeContext, id, Entry{Kind: KindDone, Payload: json.RawMessage(`{}`)}); err != nil {
		t.Fatalf("Close() after execute-then-cancel error: %v", err)
	}
	if got, err := base.XLen(context.Background(), store.eventsKey(id)).Result(); err != nil || got != 3 {
		t.Fatalf("XLEN after reconciled Append/Close = (%d, %v), want (3, nil)", got, err)
	}

	degradedID := newRedisTestStreamID(t)
	if err := store.Open(context.Background(), degradedID, Meta{Model: "model", BackendID: "backend"}); err != nil {
		t.Fatalf("Open(degraded) error: %v", err)
	}
	degradeContext, cancelDegrade := context.WithCancel(context.Background())
	client.arm(cancelDegrade)
	if err := store.MarkDegraded(degradeContext, degradedID); err != nil {
		t.Fatalf("MarkDegraded() after execute-then-cancel error: %v", err)
	}
	if fields := server.HGet(store.streamKeys(degradedID)[0], "degraded"); fields != "1" {
		t.Fatalf("degraded field = %q, want 1", fields)
	}
}

type interleavingExecuteRedisClient struct {
	redislib.UniversalClient
	mu      sync.Mutex
	pending *redisExecutionInterleave
}

type redisExecutionInterleave struct {
	executed chan struct{}
	resume   chan struct{}
}

func (client *interleavingExecuteRedisClient) arm() (<-chan struct{}, chan struct{}) {
	client.mu.Lock()
	defer client.mu.Unlock()
	interleave := &redisExecutionInterleave{
		executed: make(chan struct{}),
		resume:   make(chan struct{}),
	}
	client.pending = interleave
	return interleave.executed, interleave.resume
}

func (client *interleavingExecuteRedisClient) Eval(
	ctx context.Context,
	script string,
	keys []string,
	args ...any,
) *redislib.Cmd {
	command := client.UniversalClient.Eval(ctx, script, keys, args...)
	client.interleaveSuccessfulCommand(command)
	return command
}

func (client *interleavingExecuteRedisClient) EvalSha(
	ctx context.Context,
	sha1 string,
	keys []string,
	args ...any,
) *redislib.Cmd {
	command := client.UniversalClient.EvalSha(ctx, sha1, keys, args...)
	client.interleaveSuccessfulCommand(command)
	return command
}

func (client *interleavingExecuteRedisClient) interleaveSuccessfulCommand(command *redislib.Cmd) {
	if command.Err() != nil {
		return
	}
	client.mu.Lock()
	interleave := client.pending
	client.pending = nil
	client.mu.Unlock()
	if interleave == nil {
		return
	}
	close(interleave.executed)
	<-interleave.resume
	command.SetErr(redisInterleavingDisconnectError{})
}

type redisInterleavingDisconnectError struct{}

func (redisInterleavingDisconnectError) Error() string {
	return "connection lost after interleaved Redis execution"
}

func (redisInterleavingDisconnectError) Timeout() bool   { return false }
func (redisInterleavingDisconnectError) Temporary() bool { return true }

var _ net.Error = redisInterleavingDisconnectError{}

type executeThenCancelRedisClient struct {
	redislib.UniversalClient
	mu     sync.Mutex
	armed  bool
	cancel context.CancelFunc
}

func (client *executeThenCancelRedisClient) arm(cancel context.CancelFunc) {
	client.mu.Lock()
	client.armed = true
	client.cancel = cancel
	client.mu.Unlock()
}

func (client *executeThenCancelRedisClient) Eval(
	ctx context.Context,
	script string,
	keys []string,
	args ...any,
) *redislib.Cmd {
	command := client.UniversalClient.Eval(ctx, script, keys, args...)
	client.cancelSuccessfulCommand(command)
	return command
}

func (client *executeThenCancelRedisClient) EvalSha(
	ctx context.Context,
	sha1 string,
	keys []string,
	args ...any,
) *redislib.Cmd {
	command := client.UniversalClient.EvalSha(ctx, sha1, keys, args...)
	client.cancelSuccessfulCommand(command)
	return command
}

func (client *executeThenCancelRedisClient) cancelSuccessfulCommand(command *redislib.Cmd) {
	if command.Err() != nil {
		return
	}
	client.mu.Lock()
	if !client.armed {
		client.mu.Unlock()
		return
	}
	cancel := client.cancel
	client.armed = false
	client.cancel = nil
	client.mu.Unlock()
	cancel()
	command.SetErr(context.DeadlineExceeded)
}

func TestRedisTailEvictsOnlyByteLaggingReader(t *testing.T) {
	t.Parallel()
	server := miniredis.RunT(t)
	slowBase := redislib.NewClient(&redislib.Options{Addr: server.Addr()})
	fastBase := redislib.NewClient(&redislib.Options{Addr: server.Addr()})
	t.Cleanup(func() {
		_ = slowBase.Close()
		_ = fastBase.Close()
	})
	slowClient := &observedXReadRedisClient{
		UniversalClient: slowBase,
		observed:        make(chan string),
	}
	config := DefaultRedisConfig()
	config.Prefix = "test-reader-lag"
	config.ReadBlock = 5 * time.Millisecond
	config.ReaderMaxLagBytes = 1200
	slowStore, err := NewRedis(slowClient, config)
	if err != nil {
		t.Fatalf("NewRedis(slow) error: %v", err)
	}
	fastStore, err := NewRedis(fastBase, config)
	if err != nil {
		t.Fatalf("NewRedis(fast) error: %v", err)
	}
	ctx := context.Background()
	id := newRedisTestStreamID(t)
	if err := fastStore.Open(ctx, id, Meta{Model: "model", BackendID: "backend"}); err != nil {
		t.Fatalf("Open() error: %v", err)
	}
	slow, cancelSlow, err := slowStore.Tail(ctx, id, 1)
	if err != nil {
		t.Fatalf("Tail(slow) error: %v", err)
	}
	defer cancelSlow()
	// The barrier proves the slow tail is attached at sequence 1 before any
	// payload is committed. Subsequent cursor barriers prove that each payload
	// crossed that reader's unbuffered source handoff; fast-reader progress
	// alone cannot establish this for an independent Redis tail goroutine.
	requireRedisXReadCursor(t, slowClient.observed, 1)

	fast, cancelFast, err := fastStore.Tail(ctx, id, 1)
	if err != nil {
		t.Fatalf("Tail(fast) error: %v", err)
	}
	defer cancelFast()

	for index := 0; index < 3; index++ {
		seq, appendErr := fastStore.Append(ctx, id, Entry{
			Kind: KindChunk,
			Payload: json.RawMessage(`{"data":"` +
				strings.Repeat("x", 400) + `"}`),
		})
		if appendErr != nil {
			t.Fatalf("Append(%d) error: %v", index, appendErr)
		}
		requireRedisTailSequence(t, fast, seq)
		requireRedisXReadCursor(t, slowClient.observed, seq)
	}

	select {
	case entry, open := <-slow:
		if !open || !errors.Is(entry.Err, ErrReaderLagged) {
			t.Fatalf("slow reader terminal = (%+v, open=%t), want ErrReaderLagged", entry, open)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("slow reader was not evicted")
	}

	if err := fastStore.Close(ctx, id, Entry{Kind: KindDone, Payload: json.RawMessage(`{}`)}); err != nil {
		t.Fatalf("Close() error: %v", err)
	}
	requireRedisTailSequence(t, fast, 5)
	select {
	case _, open := <-fast:
		if open {
			t.Fatal("fast reader remained open after terminal entry")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("fast reader did not close")
	}
}

type observedXReadRedisClient struct {
	redislib.UniversalClient
	observed chan string
}

func (client *observedXReadRedisClient) XRead(
	ctx context.Context,
	args *redislib.XReadArgs,
) *redislib.XStreamSliceCmd {
	command := client.UniversalClient.XRead(ctx, args)
	if len(args.Streams) < 2 {
		return command
	}
	select {
	case client.observed <- args.Streams[len(args.Streams)-1]:
	case <-ctx.Done():
	}
	return command
}

func requireRedisXReadCursor(t *testing.T, observed <-chan string, want uint64) {
	t.Helper()
	wantID := redisStreamID(want)
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	for {
		select {
		case got := <-observed:
			if got == wantID {
				return
			}
		case <-timer.C:
			t.Fatalf("timed out waiting for Redis XREAD cursor %s", wantID)
		}
	}
}

func TestRedisTailPumpEvictsBacklogBehindOversizedEnvelope(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	id := newRedisTestStreamID(t)
	source := make(chan Entry)
	out := make(chan Entry)
	go runRedisTailDelivery(ctx, cancel, id, 128, source, out)
	source <- Entry{
		Seq:     2,
		TS:      time.Now().UTC(),
		Kind:    KindChunk,
		Payload: json.RawMessage(`{"data":"` + strings.Repeat("x", 512) + `"}`),
	}
	source <- Entry{
		Seq:     3,
		TS:      time.Now().UTC(),
		Kind:    KindChunk,
		Payload: json.RawMessage(`{}`),
	}
	close(source)
	select {
	case entry := <-out:
		if !errors.Is(entry.Err, ErrReaderLagged) {
			t.Fatalf("oversized backlog error = %v, want ErrReaderLagged", entry.Err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("oversized backlog was not evicted")
	}
}

func newRedisLagTestStore(t *testing.T, prefix string, maxLagBytes int64) *Redis {
	t.Helper()
	server := miniredis.RunT(t)
	client := redislib.NewClient(&redislib.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	config := DefaultRedisConfig()
	config.Prefix = prefix
	config.ReadBlock = 5 * time.Millisecond
	config.ReaderMaxLagBytes = maxLagBytes
	store, err := NewRedis(client, config)
	if err != nil {
		t.Fatalf("NewRedis() error: %v", err)
	}
	return store
}

func requireRedisTailSequence(t *testing.T, tail <-chan Entry, want uint64) {
	t.Helper()
	select {
	case entry, open := <-tail:
		if !open || entry.Err != nil || entry.Seq != want {
			t.Fatalf("Tail() entry = (%+v, open=%t), want sequence %d", entry, open, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for Tail() sequence %d", want)
	}
}
