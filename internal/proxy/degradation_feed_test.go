package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	redislib "github.com/redis/go-redis/v9"
	"github.com/satwiksps/streamweld/internal/journal"
	"github.com/satwiksps/streamweld/internal/proxy/sse"
)

func TestDegradedFeedBoundsAndReclaimsEachSubscriber(t *testing.T) {
	entry := journal.Entry{Seq: 2, Kind: journal.KindChunk, Payload: json.RawMessage(`{"data":"one"}`)}
	feed := newDegradedFeed(degradedEntrySize(entry) + 1)
	initial, ok := feed.subscribe(true)
	if !ok {
		t.Fatal("initial subscription was rejected")
	}
	secondary, ok := feed.subscribe(false)
	if !ok {
		t.Fatal("secondary subscription was rejected")
	}

	if err := feed.publishCommitted(context.Background(), entry); err != nil {
		t.Fatalf("publish first committed frame: %v", err)
	}
	initial.acknowledge(entry.Seq)
	if got := feed.queuedBytes(); got != degradedEntrySize(entry) {
		t.Fatalf("queued bytes after initial acknowledgement = %d, want one secondary frame", got)
	}

	entry.Seq++
	if err := feed.publishCommitted(context.Background(), entry); err != nil {
		t.Fatalf("publish second committed frame: %v", err)
	}
	if _, ok, err := secondary.next(context.Background()); ok || !errors.Is(err, journal.ErrReaderLagged) {
		t.Fatalf("lagging secondary next = (ok %v, err %v), want ErrReaderLagged", ok, err)
	}
	frame, ok, err := initial.next(context.Background())
	if err != nil || !ok || frame.entry == nil || frame.entry.Seq != entry.Seq {
		t.Fatalf("draining initial next = (%+v, %v, %v), want committed sequence %d", frame, ok, err, entry.Seq)
	}
	if got := feed.queuedBytes(); got != 0 {
		t.Fatalf("queued bytes after drain = %d, want 0", got)
	}

	initial.unsubscribe()
	secondary.unsubscribe()
	if got := feed.subscriberCount(); got != 0 {
		t.Fatalf("subscriber count after unsubscribe = %d, want 0", got)
	}
}

func TestDegradedFeedAdmitsOneOversizeCompleteFrameThenEvicts(t *testing.T) {
	feed := newDegradedFeed(1)
	subscription, ok := feed.subscribe(true)
	if !ok {
		t.Fatal("subscription was rejected")
	}
	oversize := degradedChunkFrame(sse.Event{Data: []byte(`{"large":"complete event"}`), HasData: true})
	if err := feed.activate(context.Background(), 1, oversize); err != nil {
		t.Fatalf("activate with oversize frame: %v", err)
	}
	frame, ok, err := subscription.next(context.Background())
	if err != nil || !ok || string(frame.event.Data) != string(oversize.event.Data) {
		t.Fatalf("single oversize next = (%+v, %v, %v), want complete frame", frame, ok, err)
	}
	if err := feed.append(context.Background(), oversize, oversize); err != nil {
		t.Fatalf("append consecutive oversize frames: %v", err)
	}
	if _, ok, err := subscription.next(context.Background()); ok || !errors.Is(err, journal.ErrReaderLagged) {
		t.Fatalf("consecutive oversize next = (ok %v, err %v), want ErrReaderLagged", ok, err)
	}
}

func TestDegradedFeedRetainsNoSuffixWithoutSubscribers(t *testing.T) {
	feed := newDegradedFeed(1024)
	subscription, ok := feed.subscribe(false)
	if !ok {
		t.Fatal("subscription was rejected")
	}
	subscription.unsubscribe()
	if err := feed.activate(context.Background(), 3, degradedWarningFrame()); err != nil {
		t.Fatalf("activate empty feed: %v", err)
	}
	if err := feed.append(context.Background(), degradedChunkFrame(sse.Event{Data: []byte(`{"suffix":true}`), HasData: true})); err != nil {
		t.Fatalf("append empty feed: %v", err)
	}
	if got := feed.queuedBytes(); got != 0 {
		t.Fatalf("orphan suffix bytes = %d, want 0", got)
	}
	if boundary, active := feed.status(); !active || boundary != 3 {
		t.Fatalf("owner-local boundary = (%d, %v), want (3, true)", boundary, active)
	}
}

func TestTailFailureRelayDoesNotSkipCommittedBoundaryEntry(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = io.Copy(io.Discard, request.Body)
		startDurableBackendSSE(writer)
		_ = writeDurableBackendData(writer, chatChunkHello)
		_ = writeDurableBackendData(writer, chatChunkPart)
		_ = writeDurableBackendData(writer, chatChunkWorld)
		_ = writeDurableBackendData(writer, doneSentinelData)
	}))
	t.Cleanup(backend.Close)

	memoryConfig := journal.DefaultConfig()
	memoryConfig.MaxTotalBytes = 8 << 20
	memoryConfig.ReaderMaxLagBytes = 1 << 20
	committed, err := journal.NewMemory(memoryConfig)
	if err != nil {
		t.Fatalf("journal.NewMemory() error = %v", err)
	}
	store := &tailFailingPhase5Journal{phase5FailingJournal: phase5FailingJournal{
		Memory: committed,
		failAt: 3,
		err:    errors.New("injected append outage after committed boundary entry"),
	}}
	replica := newPhase5SharedReplica(t, backend.URL, store, journal.NewMemoryIdempotencyRegistry(nil))
	request := newDurableHTTPRequest(t, http.MethodPost, replica.url+"/v1/chat/completions", `{"model":"m","stream":true,"messages":[]}`)
	response := doDurableHTTPRequest(t, replica.client, request)
	defer closeDurableHTTPBody(t, response.Body)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, body = %q", response.StatusCode, readDurableHTTPBody(t, response.Body))
	}
	events := readAllDurableSSE(t, response.Body)
	if len(events) != 6 {
		t.Fatalf("events = %#v, want open, two committed chunks, warning, failed chunk, sentinel", events)
	}
	requireDurableSSEEvent(t, events[0], "1", streamOpenEvent, "")
	requireDurableSSEEvent(t, events[1], "2", "", chatChunkHello)
	requireDurableSSEEvent(t, events[2], "3", "", chatChunkPart)
	requirePhase5UnsequencedEvent(t, events[3], streamWarningEvent, `"code":"journal_degraded"`)
	requirePhase5UnsequencedEvent(t, events[4], "", chatChunkWorld)
	requirePhase5UnsequencedEvent(t, events[5], "", doneSentinelData)
}

func TestOwnerLocalBoundarySealsResumeBeforeMarkerRecovery(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = io.Copy(io.Discard, request.Body)
		startDurableBackendSSE(writer)
		_ = writeDurableBackendData(writer, chatChunkHello)
		_ = writeDurableBackendData(writer, chatChunkWorld)
		_ = writeDurableBackendData(writer, doneSentinelData)
	}))
	t.Cleanup(backend.Close)

	memoryConfig := journal.DefaultConfig()
	memoryConfig.MaxTotalBytes = 8 << 20
	memoryConfig.ReaderMaxLagBytes = 1 << 20
	committed, err := journal.NewMemory(memoryConfig)
	if err != nil {
		t.Fatalf("journal.NewMemory() error = %v", err)
	}
	store := &neverMarkingPhase5Journal{phase5FailingJournal: phase5FailingJournal{
		Memory: committed,
		failAt: 2,
		err:    errors.New("injected append outage"),
	}}
	replica := newPhase5SharedReplica(t, backend.URL, store, journal.NewMemoryIdempotencyRegistry(nil))
	request := newDurableHTTPRequest(t, http.MethodPost, replica.url+"/v1/chat/completions", `{"model":"m","stream":true,"messages":[]}`)
	response := doDurableHTTPRequest(t, replica.client, request)
	defer closeDurableHTTPBody(t, response.Body)
	streamID := requireDurableResponseHeaders(t, response)
	_ = readAllDurableSSE(t, response.Body)

	atGap := newDurableHTTPRequest(t, http.MethodGet, replica.url+"/v1/streams/"+streamID.String()+"/events", "")
	atGap.Header.Set("Last-Event-ID", "2")
	atGapResponse := doDurableHTTPRequest(t, replica.client, atGap)
	defer func() { _ = atGapResponse.Body.Close() }()
	requireDurableStreamError(t, atGapResponse, http.StatusGone, "stream_offset_expired", streamID.String())

	prefix := newDurableHTTPRequest(t, http.MethodGet, replica.url+"/v1/streams/"+streamID.String()+"/events", "")
	prefixResponse := doDurableHTTPRequest(t, replica.client, prefix)
	defer closeDurableHTTPBody(t, prefixResponse.Body)
	prefixEvents := readAllDurableSSE(t, prefixResponse.Body)
	if len(prefixEvents) != 3 {
		t.Fatalf("prefix events = %#v, want open, committed chunk, offset error", prefixEvents)
	}
	requireDurableSSEEvent(t, prefixEvents[0], "1", streamOpenEvent, "")
	requireDurableSSEEvent(t, prefixEvents[1], "2", "", chatChunkHello)
	requirePhase5UnsequencedEvent(t, prefixEvents[2], streamErrorEvent, `"code":"stream_offset_expired"`)
}

func TestDegradationMarkerLeaseRefreshesWhileActiveAndOnTerminal(t *testing.T) {
	releaseBackend := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseBackend) }) }
	t.Cleanup(release)
	backend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = io.Copy(io.Discard, request.Body)
		startDurableBackendSSE(writer)
		_ = writeDurableBackendData(writer, chatChunkHello)
		_ = writeDurableBackendData(writer, chatChunkWorld)
		select {
		case <-releaseBackend:
		case <-request.Context().Done():
			return
		}
		_ = writeDurableBackendData(writer, doneSentinelData)
	}))
	t.Cleanup(backend.Close)

	memoryConfig := journal.DefaultConfig()
	memoryConfig.MaxTotalBytes = 8 << 20
	memoryConfig.ReaderMaxLagBytes = 1 << 20
	committed, err := journal.NewMemory(memoryConfig)
	if err != nil {
		t.Fatalf("journal.NewMemory() error = %v", err)
	}
	store := &recoveringMarkerJournal{phase5FailingJournal: phase5FailingJournal{
		Memory: committed,
		failAt: 2,
		err:    errors.New("injected append outage"),
	}}
	replica := newPhase5SharedReplica(t, backend.URL, store, journal.NewMemoryIdempotencyRegistry(nil))
	replica.server.durable.config.JournalTTL = 80 * time.Millisecond
	replica.server.durable.config.ReadinessTimeout = 20 * time.Millisecond

	request := newDurableHTTPRequest(t, http.MethodPost, replica.url+"/v1/chat/completions", `{"model":"m","stream":true,"messages":[]}`)
	response := doDurableHTTPRequest(t, replica.client, request)
	defer closeDurableHTTPBody(t, response.Body)
	streamID := requireDurableResponseHeaders(t, response)
	deadline := time.Now().Add(time.Second)
	for store.markerCalls.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if calls := store.markerCalls.Load(); calls < 3 {
		t.Fatalf("active marker calls = %d, want initial call plus TTL/2 refreshes", calls)
	}
	runtime, ok := replica.server.durable.loadRuntime(streamID)
	if !ok {
		t.Fatal("owner runtime disappeared before degraded terminal")
	}
	beforeTerminal := store.markerCalls.Load()
	release()
	_ = readAllDurableSSE(t, response.Body)
	select {
	case <-runtime.degradationTerminalAttempt:
	case <-time.After(time.Second):
		t.Fatal("terminal degradation refresh was not attempted")
	}
	if calls := store.markerCalls.Load(); calls <= beforeTerminal {
		t.Fatalf("marker calls after terminal = %d, want more than active count %d", calls, beforeTerminal)
	}
}

func TestRedisOutageXReadFailsBeforeNextAppendAndLiveReaderCompletes(t *testing.T) {
	redisServer := miniredis.RunT(t)
	redisClient := redislib.NewClient(&redislib.Options{
		Addr:         redisServer.Addr(),
		DialTimeout:  100 * time.Millisecond,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 100 * time.Millisecond,
		MaxRetries:   -1,
	})
	xread := &xreadLifecycleHook{started: make(chan struct{}), finished: make(chan struct{})}
	redisClient.AddHook(xread)
	t.Cleanup(func() { _ = redisClient.Close() })
	redisConfig := journal.DefaultRedisConfig()
	redisConfig.Prefix = "proxy-real-outage"
	redisConfig.TTL = 2 * time.Second
	redisConfig.ReadBlock = 5 * time.Second
	store, err := journal.NewRedis(redisClient, redisConfig)
	if err != nil {
		t.Fatalf("journal.NewRedis() error = %v", err)
	}

	releaseBackend := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseBackend) }) }
	t.Cleanup(release)
	backend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = io.Copy(io.Discard, request.Body)
		startDurableBackendSSE(writer)
		if !writeDurableBackendData(writer, chatChunkHello) {
			return
		}
		select {
		case <-releaseBackend:
		case <-request.Context().Done():
			return
		}
		_ = writeDurableBackendData(writer, chatChunkWorld)
		_ = writeDurableBackendData(writer, doneSentinelData)
	}))
	t.Cleanup(backend.Close)
	replica := newPhase5SharedReplica(t, backend.URL, store, store)

	request := newDurableHTTPRequest(t, http.MethodPost, replica.url+"/v1/chat/completions", `{"model":"m","stream":true,"messages":[]}`)
	response := doDurableHTTPRequest(t, replica.client, request)
	defer closeDurableHTTPBody(t, response.Body)
	streamID := requireDurableResponseHeaders(t, response)
	decoder := sse.NewDecoder(response.Body)
	requireDurableSSEEvent(t, readNextDurableSSE(t, decoder), "1", streamOpenEvent, "")
	requireDurableSSEEvent(t, readNextDurableSSE(t, decoder), "2", "", chatChunkHello)

	waitForPhase5Signal(t, xread.started, "Redis XREAD start")
	redisServer.Close()
	waitForPhase5Signal(t, xread.finished, "Redis XREAD failure")
	release()

	remaining := make([]sse.Event, 0, 3)
	for {
		event, decodeErr := decoder.Decode()
		if errors.Is(decodeErr, io.EOF) {
			break
		}
		if decodeErr != nil {
			t.Fatalf("decode degraded Redis-outage stream: %v", decodeErr)
		}
		remaining = append(remaining, event)
	}
	if len(remaining) != 3 {
		t.Fatalf("post-outage events = %#v, want warning, chunk, sentinel", remaining)
	}
	requirePhase5UnsequencedEvent(t, remaining[0], streamWarningEvent, `"code":"journal_degraded"`)
	requirePhase5UnsequencedEvent(t, remaining[1], "", chatChunkWorld)
	requirePhase5UnsequencedEvent(t, remaining[2], "", doneSentinelData)

	if err := redisServer.Restart(); err != nil {
		t.Fatalf("restart miniredis: %v", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		_, cancel, tailErr := store.Tail(context.Background(), streamID, 2)
		if cancel != nil {
			cancel()
		}
		if errors.Is(tailErr, journal.ErrOffsetExpired) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("recovered Redis never received the terminal degradation marker")
}

type tailFailingPhase5Journal struct {
	phase5FailingJournal
	once sync.Once
}

func (store *tailFailingPhase5Journal) Tail(
	ctx context.Context,
	id journal.StreamID,
	fromSeq uint64,
) (<-chan journal.Entry, func(), error) {
	var injected bool
	if fromSeq == 2 {
		store.once.Do(func() { injected = true })
	}
	if !injected {
		return store.Memory.Tail(ctx, id, fromSeq)
	}
	out := make(chan journal.Entry, 1)
	out <- journal.Entry{Err: errors.New("injected XREAD transport failure")}
	close(out)
	return out, func() {}, nil
}

type neverMarkingPhase5Journal struct {
	phase5FailingJournal
}

func (*neverMarkingPhase5Journal) MarkDegraded(context.Context, journal.StreamID) error {
	return errors.New("injected persistent degradation-marker outage")
}

type xreadLifecycleHook struct {
	startOnce  sync.Once
	finishOnce sync.Once
	started    chan struct{}
	finished   chan struct{}
}

func (hook *xreadLifecycleHook) DialHook(next redislib.DialHook) redislib.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		return next(ctx, network, addr)
	}
}

func (hook *xreadLifecycleHook) ProcessHook(next redislib.ProcessHook) redislib.ProcessHook {
	return func(ctx context.Context, command redislib.Cmder) error {
		if strings.EqualFold(command.Name(), "xread") {
			hook.startOnce.Do(func() { close(hook.started) })
			err := next(ctx, command)
			hook.finishOnce.Do(func() { close(hook.finished) })
			return err
		}
		return next(ctx, command)
	}
}

func (hook *xreadLifecycleHook) ProcessPipelineHook(next redislib.ProcessPipelineHook) redislib.ProcessPipelineHook {
	return func(ctx context.Context, commands []redislib.Cmder) error {
		return next(ctx, commands)
	}
}

func waitForPhase5Signal(t *testing.T, signal <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

var (
	_ journal.Journal           = (*tailFailingPhase5Journal)(nil)
	_ journal.DegradationMarker = (*neverMarkingPhase5Journal)(nil)
	_ redislib.Hook             = (*xreadLifecycleHook)(nil)
)
