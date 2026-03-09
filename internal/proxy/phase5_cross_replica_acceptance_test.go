package proxy

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	redislib "github.com/redis/go-redis/v9"
	"github.com/streamweld/streamweld/internal/journal"
	"github.com/streamweld/streamweld/internal/proxy/sse"
)

func TestPhase5ConcurrentCrossReplicaIdempotencyDoesNotDeletePendingWinner(t *testing.T) {
	var backendCalls atomic.Int64
	backend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		backendCalls.Add(1)
		_, _ = io.Copy(io.Discard, request.Body)
		startDurableBackendSSE(writer)
		_ = writeDurableBackendData(writer, chatChunkHello)
		_ = writeDurableBackendData(writer, "[DONE]")
	}))
	t.Cleanup(backend.Close)

	redisServer := miniredis.RunT(t)
	clientA := redislib.NewClient(&redislib.Options{Addr: redisServer.Addr()})
	clientB := redislib.NewClient(&redislib.Options{Addr: redisServer.Addr()})
	t.Cleanup(func() {
		_ = clientA.Close()
		_ = clientB.Close()
	})
	config := journal.DefaultRedisConfig()
	config.Prefix = "streamweld:phase5:pending-idempotency"
	config.ReadBlock = 5 * time.Millisecond
	storeA, err := journal.NewRedis(clientA, config)
	if err != nil {
		t.Fatalf("journal.NewRedis(A) error = %v", err)
	}
	storeB, err := journal.NewRedis(clientB, config)
	if err != nil {
		t.Fatalf("journal.NewRedis(B) error = %v", err)
	}

	delayed := &phase5DelayedOpenJournal{
		Journal: storeA,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	observed := &phase5ObservingIdempotencyRegistry{
		IdempotencyRegistry: storeB,
		existing:            make(chan struct{}),
	}
	replicaA := newPhase5SharedReplica(t, backend.URL, delayed, storeA)
	replicaB := newPhase5SharedReplica(t, backend.URL, storeB, observed)

	const key = "phase5-simultaneous-key"
	requestA := newDurableHTTPRequest(t, http.MethodPost, replicaA.url+"/v1/chat/completions",
		`{"model":"phase5-model","stream":true,"messages":[]}`)
	requestA.Header.Set(headerIdempotency, key)
	responseA := make(chan phase5HTTPResult, 1)
	go func() {
		// The receiving test goroutine owns and closes the response body.
		//nolint:bodyclose
		response, requestErr := replicaA.client.Do(requestA)
		responseA <- phase5HTTPResult{response: response, err: requestErr}
	}()

	select {
	case <-delayed.started:
	case <-time.After(durableHTTPTestTimeout):
		t.Fatal("replica A did not reserve the idempotency key before Open")
	}
	requestB := newDurableHTTPRequest(t, http.MethodPost, replicaB.url+"/v1/chat/completions",
		`{"model":"must-not-start","stream":true,"messages":[]}`)
	requestB.Header.Set(headerIdempotency, key)
	responseB := make(chan phase5HTTPResult, 1)
	go func() {
		// The receiving test goroutine owns and closes the response body.
		//nolint:bodyclose
		response, requestErr := replicaB.client.Do(requestB)
		responseB <- phase5HTTPResult{response: response, err: requestErr}
	}()

	select {
	case <-observed.existing:
	case <-time.After(durableHTTPTestTimeout):
		close(delayed.release)
		t.Fatal("replica B did not observe replica A's pending reservation")
	}
	// The old fixed eight-attempt loop failed after roughly 80 ms. Hold Open
	// beyond that window and prove the loser continues waiting for the winner
	// within the configured readiness/request bound.
	select {
	case early := <-responseB:
		close(delayed.release)
		if early.response != nil {
			_ = early.response.Body.Close()
		}
		t.Fatalf("replica B stopped waiting for the live reservation: %v", early.err)
	case <-time.After(150 * time.Millisecond):
	}
	close(delayed.release)
	resultA := awaitPhase5HTTPResult(t, responseA)
	resultB := awaitPhase5HTTPResult(t, responseB)
	defer closeDurableHTTPBody(t, resultA.response.Body)
	defer closeDurableHTTPBody(t, resultB.response.Body)
	if resultA.response.StatusCode != http.StatusOK || resultB.response.StatusCode != http.StatusOK {
		t.Fatalf("concurrent statuses = (%d, %d), want both 200",
			resultA.response.StatusCode, resultB.response.StatusCode)
	}
	idA := requireDurableResponseHeaders(t, resultA.response)
	idB := requireDurableResponseHeaders(t, resultB.response)
	if idA != idB {
		t.Fatalf("concurrent stream IDs = (%s, %s), want one shared ID", idA, idB)
	}
	_ = readAllDurableSSE(t, resultA.response.Body)
	_ = readAllDurableSSE(t, resultB.response.Body)
	if calls := backendCalls.Load(); calls != 1 {
		t.Fatalf("concurrent same-key upstream calls = %d, want 1", calls)
	}
}

type phase5DelayedOpenJournal struct {
	journal.Journal
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (backend *phase5DelayedOpenJournal) Open(ctx context.Context, id journal.StreamID, meta journal.Meta) error {
	backend.once.Do(func() { close(backend.started) })
	select {
	case <-backend.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	return backend.Journal.Open(ctx, id, meta)
}

type phase5ObservingIdempotencyRegistry struct {
	journal.IdempotencyRegistry
	existing chan struct{}
	once     sync.Once
}

func (registry *phase5ObservingIdempotencyRegistry) ResolveOrCreate(
	ctx context.Context,
	key string,
	newID journal.StreamID,
	ttl time.Duration,
) (journal.IdempotencyBinding, error) {
	binding, err := registry.IdempotencyRegistry.ResolveOrCreate(ctx, key, newID, ttl)
	if err == nil && !binding.Created {
		registry.once.Do(func() { close(registry.existing) })
	}
	return binding, err
}

type phase5HTTPResult struct {
	response *http.Response
	err      error
}

func awaitPhase5HTTPResult(t *testing.T, results <-chan phase5HTTPResult) phase5HTTPResult {
	t.Helper()
	select {
	case result := <-results:
		if result.err != nil {
			t.Fatalf("concurrent proxy request error = %v", result.err)
		}
		return result
	case <-time.After(durableHTTPTestTimeout):
		t.Fatal("timed out waiting for concurrent proxy response")
		return phase5HTTPResult{}
	}
}

// TestPhase5CrossReplicaResumeFanoutAndIdempotency exercises the proxy-level
// contract using two independently constructed replicas and Redis clients.
// The test Redis process is in-memory, but neither proxy shares a journal
// object, idempotency object, runtime, or producer state with the other.
func TestPhase5CrossReplicaResumeFanoutAndIdempotency(t *testing.T) {
	var backendCalls atomic.Int64
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseBackend := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseBackend)

	backend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		backendCalls.Add(1)
		_, _ = io.Copy(io.Discard, request.Body)
		startDurableBackendSSE(writer)
		if !writeDurableBackendData(writer, chatChunkHello) {
			return
		}
		select {
		case <-release:
		case <-request.Context().Done():
			return
		}
		if !writeDurableBackendData(writer, chatChunkWorld) {
			return
		}
		_ = writeDurableBackendData(writer, "[DONE]")
	}))
	t.Cleanup(backend.Close)

	redisServer := miniredis.RunT(t)
	redisClientA := redislib.NewClient(&redislib.Options{Addr: redisServer.Addr()})
	redisClientB := redislib.NewClient(&redislib.Options{Addr: redisServer.Addr()})
	t.Cleanup(func() {
		if err := redisClientA.Close(); err != nil {
			t.Errorf("close replica A Redis client: %v", err)
		}
		if err := redisClientB.Close(); err != nil {
			t.Errorf("close replica B Redis client: %v", err)
		}
	})
	redisConfig := journal.DefaultRedisConfig()
	redisConfig.Prefix = "streamweld:phase5:cross-replica"
	redisConfig.ReadBlock = 5 * time.Millisecond
	journalA, err := journal.NewRedis(redisClientA, redisConfig)
	if err != nil {
		t.Fatalf("journal.NewRedis(replica A) error = %v", err)
	}
	journalB, err := journal.NewRedis(redisClientB, redisConfig)
	if err != nil {
		t.Fatalf("journal.NewRedis(replica B) error = %v", err)
	}

	replicaA := newPhase5SharedReplica(t, backend.URL, journalA, journalA)
	replicaB := newPhase5SharedReplica(t, backend.URL, journalB, journalB)
	if replicaA.url == replicaB.url || replicaA.server.durable == replicaB.server.durable {
		t.Fatal("phase 5 harness did not construct independent proxy replicas")
	}

	const idempotencyKey = "phase5-cross-replica-generation"
	initialRequest := newDurableHTTPRequest(
		t,
		http.MethodPost,
		replicaA.url+"/v1/chat/completions",
		`{"model":"phase5-model","stream":true,"messages":[]}`,
	)
	initialRequest.Header.Set(headerIdempotency, idempotencyKey)
	initialResponse := doDurableHTTPRequest(t, replicaA.client, initialRequest)
	defer func() { _ = initialResponse.Body.Close() }()
	if initialResponse.StatusCode != http.StatusOK {
		body := readDurableHTTPBody(t, initialResponse.Body)
		closeDurableHTTPBody(t, initialResponse.Body)
		t.Fatalf("replica A initial status = %d, body = %q", initialResponse.StatusCode, body)
	}
	streamID := requireDurableResponseHeaders(t, initialResponse)
	initialDecoder := sse.NewDecoder(initialResponse.Body)
	initialPrefix := []sse.Event{
		readNextDurableSSE(t, initialDecoder),
		readNextDurableSSE(t, initialDecoder),
	}
	requireDurableSSEEvent(t, initialPrefix[0], "1", streamOpenEvent, "")
	requireDurableSSEEvent(t, initialPrefix[1], "2", "", chatChunkHello)
	closeDurableHTTPBody(t, initialResponse.Body)

	resumeRequest := newDurableHTTPRequest(
		t,
		http.MethodGet,
		replicaB.url+"/v1/streams/"+streamID.String()+"/events",
		"",
	)
	resumeRequest.Header.Set("Last-Event-ID", "2")
	resumeResponse := doDurableHTTPRequest(t, replicaB.client, resumeRequest)
	if resumeResponse.StatusCode != http.StatusOK {
		body := readDurableHTTPBody(t, resumeResponse.Body)
		closeDurableHTTPBody(t, resumeResponse.Body)
		t.Fatalf("replica B resume status = %d, body = %q", resumeResponse.StatusCode, body)
	}
	t.Cleanup(func() { closeDurableHTTPBody(t, resumeResponse.Body) })
	if resumedID := requireDurableResponseHeaders(t, resumeResponse); resumedID != streamID {
		t.Fatalf("replica B resumed stream ID = %s, want %s", resumedID, streamID)
	}

	fanoutRequest := newDurableHTTPRequest(
		t,
		http.MethodGet,
		replicaB.url+"/v1/streams/"+streamID.String()+"/events",
		"",
	)
	fanoutResponse := doDurableHTTPRequest(t, replicaB.client, fanoutRequest)
	if fanoutResponse.StatusCode != http.StatusOK {
		body := readDurableHTTPBody(t, fanoutResponse.Body)
		closeDurableHTTPBody(t, fanoutResponse.Body)
		t.Fatalf("replica B fan-out status = %d, body = %q", fanoutResponse.StatusCode, body)
	}
	t.Cleanup(func() { closeDurableHTTPBody(t, fanoutResponse.Body) })
	fanoutDecoder := sse.NewDecoder(fanoutResponse.Body)
	fanoutPrefix := []sse.Event{
		readNextDurableSSE(t, fanoutDecoder),
		readNextDurableSSE(t, fanoutDecoder),
	}
	if !reflect.DeepEqual(fanoutPrefix, initialPrefix) {
		t.Fatalf("replica B full replay prefix = %#v, want %#v", fanoutPrefix, initialPrefix)
	}

	duplicateRequest := newDurableHTTPRequest(
		t,
		http.MethodPost,
		replicaB.url+"/v1/chat/completions",
		`{"model":"must-not-start","stream":true,"messages":[{"role":"user","content":"different"}]}`,
	)
	duplicateRequest.Header.Set(headerIdempotency, idempotencyKey)
	duplicateResponse := doDurableHTTPRequest(t, replicaB.client, duplicateRequest)
	if duplicateResponse.StatusCode != http.StatusOK {
		body := readDurableHTTPBody(t, duplicateResponse.Body)
		closeDurableHTTPBody(t, duplicateResponse.Body)
		t.Fatalf("replica B idempotent replay status = %d, body = %q", duplicateResponse.StatusCode, body)
	}
	t.Cleanup(func() { closeDurableHTTPBody(t, duplicateResponse.Body) })
	if duplicateID := requireDurableResponseHeaders(t, duplicateResponse); duplicateID != streamID {
		t.Fatalf("replica B duplicate stream ID = %s, want %s", duplicateID, streamID)
	}
	duplicateDecoder := sse.NewDecoder(duplicateResponse.Body)
	duplicatePrefix := []sse.Event{
		readNextDurableSSE(t, duplicateDecoder),
		readNextDurableSSE(t, duplicateDecoder),
	}
	if !reflect.DeepEqual(duplicatePrefix, initialPrefix) {
		t.Fatalf("replica B idempotent prefix = %#v, want %#v", duplicatePrefix, initialPrefix)
	}
	if calls := backendCalls.Load(); calls != 1 {
		t.Fatalf("upstream calls before completion = %d, want 1", calls)
	}

	releaseBackend()

	resumedEvents := readAllDurableSSE(t, resumeResponse.Body)
	if len(resumedEvents) != 3 {
		t.Fatalf("exclusive cross-replica resume events = %#v, want chunk, done, sentinel", resumedEvents)
	}
	requireDurableSSEEvent(t, resumedEvents[0], "3", "", chatChunkWorld)
	requireDurableSSEEvent(t, resumedEvents[1], "4", streamDoneEvent, "")
	if resumedEvents[2].HasID || resumedEvents[2].HasType || string(resumedEvents[2].Data) != doneSentinelData {
		t.Fatalf("cross-replica resume sentinel = %#v", resumedEvents[2])
	}

	fanoutEvents := append([]sse.Event(nil), fanoutPrefix...)
	fanoutEvents = append(fanoutEvents, readRemainingDurableSSE(t, fanoutDecoder)...)
	duplicateEvents := append([]sse.Event(nil), duplicatePrefix...)
	duplicateEvents = append(duplicateEvents, readRemainingDurableSSE(t, duplicateDecoder)...)
	if !reflect.DeepEqual(fanoutEvents, duplicateEvents) {
		t.Fatalf("fan-out and idempotent replay differ:\nfan-out: %#v\nidempotent: %#v", fanoutEvents, duplicateEvents)
	}
	if len(fanoutEvents) != 5 {
		t.Fatalf("full cross-replica replay event count = %d, want 5: %#v", len(fanoutEvents), fanoutEvents)
	}
	if calls := backendCalls.Load(); calls != 1 {
		t.Fatalf("upstream calls after completion = %d, want exactly 1", calls)
	}

	stateRequest := newDurableHTTPRequest(
		t,
		http.MethodGet,
		replicaB.url+"/v1/streams/"+streamID.String(),
		"",
	)
	stateResponse := doDurableHTTPRequest(t, replicaB.client, stateRequest)
	defer closeDurableHTTPBody(t, stateResponse.Body)
	if stateResponse.StatusCode != http.StatusOK {
		t.Fatalf("replica B terminal state status = %d, body = %q", stateResponse.StatusCode, readDurableHTTPBody(t, stateResponse.Body))
	}
}

func newPhase5SharedReplica(
	t *testing.T,
	backendURL string,
	sharedJournal journal.Journal,
	sharedIdempotency journal.IdempotencyRegistry,
) *durableHTTPHarness {
	t.Helper()

	config := DefaultConfig()
	config.BackendURL = backendURL
	config.ListenAddress = "127.0.0.1:0"
	config.ReadinessTimeout = time.Second
	server, err := NewServer(
		config,
		nil,
		WithJournal(sharedJournal),
		WithIdempotencyRegistry(sharedIdempotency),
		WithStreamIDGenerator(&durableSequentialIDs{}),
	)
	if err != nil {
		t.Fatalf("NewServer(shared phase 5 store) error = %v", err)
	}
	front := httptest.NewServer(server.Handler())
	transport := &http.Transport{DisableCompression: true}
	client := &http.Client{Transport: transport, Timeout: durableHTTPTestTimeout}
	t.Cleanup(func() {
		server.forceCancel()
		front.CloseClientConnections()
		front.Close()
		transport.CloseIdleConnections()
		server.closeIdleConnections()
	})
	return &durableHTTPHarness{server: server, url: front.URL, client: client}
}
