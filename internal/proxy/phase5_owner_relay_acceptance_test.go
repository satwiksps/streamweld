package proxy

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	miniredis "github.com/alicebob/miniredis/v2"
	"github.com/satwiksps/streamweld/internal/journal"
	"github.com/satwiksps/streamweld/internal/proxy/sse"
)

func TestPhase5OwnerRelayRoutesCrossReplicaStop(t *testing.T) {
	var backendCalls atomic.Int64
	producerCanceled := make(chan struct{})
	var canceledOnce sync.Once
	backend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/health" {
			writer.WriteHeader(http.StatusOK)
			return
		}
		backendCalls.Add(1)
		_, _ = io.Copy(io.Discard, request.Body)
		startDurableBackendSSE(writer)
		if !writeDurableBackendData(writer, chatChunkHello) {
			return
		}
		<-request.Context().Done()
		canceledOnce.Do(func() { close(producerCanceled) })
	}))
	t.Cleanup(backend.Close)

	redisServer := miniredis.RunT(t)
	redisURL := "redis://" + redisServer.Addr() + "/0"
	const redisPrefix = "phase5-owner-relay-stop"
	replicaA := startRelayAcceptanceReplica(t, "stop-owner-a", backend.URL, redisURL, redisPrefix)
	replicaB := startRelayAcceptanceReplica(t, "stop-caller-b", backend.URL, redisURL, redisPrefix)

	initial := newDurableHTTPRequest(t, http.MethodPost, replicaA.url+"/v1/chat/completions",
		`{"model":"phase5-model","stream":true,"messages":[]}`)
	initialResponse := doDurableHTTPRequest(t, replicaA.client, initial)
	defer func() { _ = initialResponse.Body.Close() }()
	if initialResponse.StatusCode != http.StatusOK {
		body := readDurableHTTPBody(t, initialResponse.Body)
		closeDurableHTTPBody(t, initialResponse.Body)
		t.Fatalf("initial status = %d, body = %q", initialResponse.StatusCode, body)
	}
	streamID := requireDurableResponseHeaders(t, initialResponse)
	decoder := sse.NewDecoder(initialResponse.Body)
	_ = readNextDurableSSE(t, decoder)
	requireDurableSSEEvent(t, readNextDurableSSE(t, decoder), "2", "", chatChunkHello)
	closeDurableHTTPBody(t, initialResponse.Body)

	stopRequest := newDurableHTTPRequest(t, http.MethodPost,
		replicaB.url+"/v1/streams/"+streamID.String()+"/stop", "")
	stopRequest.Header.Set("Authorization", "Bearer must-not-cross-relay")
	stopResponseHTTP := doDurableHTTPRequest(t, replicaB.client, stopRequest)
	defer closeDurableHTTPBody(t, stopResponseHTTP.Body)
	if stopResponseHTTP.StatusCode != http.StatusAccepted {
		t.Fatalf("cross-replica stop status = %d, body = %q", stopResponseHTTP.StatusCode, readDurableHTTPBody(t, stopResponseHTTP.Body))
	}
	var stopped stopResponse
	if err := json.NewDecoder(stopResponseHTTP.Body).Decode(&stopped); err != nil {
		t.Fatalf("decode cross-replica stop response: %v", err)
	}
	if stopped.StreamID != streamID || stopped.Outcome != "stopped" || stopped.PartialText != "hello" {
		t.Fatalf("cross-replica stop response = %+v", stopped)
	}
	select {
	case <-producerCanceled:
	case <-time.After(durableHTTPTestTimeout):
		t.Fatal("cross-replica stop did not cancel the owner producer")
	}
	if calls := backendCalls.Load(); calls != 1 {
		t.Fatalf("backend calls = %d, want one", calls)
	}

	// Simulate owner process loss after the stopped terminal was committed.
	// Replica B must reconstruct the identical logical result from Redis even
	// while stale owner presence briefly routes its first relay attempt to a
	// process that no longer has the runtime.
	replicaA.server.durable.streams.Delete(streamID)
	repeatedRequest := newDurableHTTPRequest(t, http.MethodPost,
		replicaB.url+"/v1/streams/"+streamID.String()+"/stop", "")
	repeatedHTTP := doDurableHTTPRequest(t, replicaB.client, repeatedRequest)
	defer closeDurableHTTPBody(t, repeatedHTTP.Body)
	if repeatedHTTP.StatusCode != http.StatusAccepted {
		t.Fatalf("repeated cross-replica stop status = %d, body = %q",
			repeatedHTTP.StatusCode, readDurableHTTPBody(t, repeatedHTTP.Body))
	}
	var repeated stopResponse
	if err := json.NewDecoder(repeatedHTTP.Body).Decode(&repeated); err != nil {
		t.Fatalf("decode repeated cross-replica stop response: %v", err)
	}
	if repeated != stopped {
		t.Fatalf("repeated stop response = %+v, want %+v", repeated, stopped)
	}
	if calls := backendCalls.Load(); calls != 1 {
		t.Fatalf("backend calls after repeated stop = %d, want one", calls)
	}

	stateRequest := newDurableHTTPRequest(t, http.MethodGet,
		replicaB.url+"/v1/streams/"+streamID.String(), "")
	stateResponse := doDurableHTTPRequest(t, replicaB.client, stateRequest)
	defer closeDurableHTTPBody(t, stateResponse.Body)
	if stateResponse.StatusCode != http.StatusOK {
		t.Fatalf("stopped state status = %d", stateResponse.StatusCode)
	}
	var state journal.StreamState
	if err := json.NewDecoder(stateResponse.Body).Decode(&state); err != nil {
		t.Fatalf("decode stopped state: %v", err)
	}
	if state.Status != journal.StatusStopped || state.Resumable {
		t.Fatalf("stopped state = %+v", state)
	}
}

func TestPhase5OwnerRelayCompletesRemoteReaderAfterRedisDisappears(t *testing.T) {
	var backendCalls atomic.Int64
	releaseSuffix := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseSuffix) }) }
	defer release()
	t.Cleanup(release)

	backend := newRelayAcceptanceBackend(t, &backendCalls, releaseSuffix)
	redisServer := miniredis.RunT(t)
	redisURL := "redis://" + redisServer.Addr() + "/0"
	const redisPrefix = "phase5-owner-relay-cut"
	replicaA := startRelayAcceptanceReplica(t, "replica-a", backend.URL, redisURL, redisPrefix)
	replicaB := startRelayAcceptanceReplica(t, "replica-b", backend.URL, redisURL, redisPrefix)

	initialRequest := newDurableHTTPRequest(
		t,
		http.MethodPost,
		replicaA.url+"/v1/chat/completions",
		`{"model":"phase5-model","stream":true,"messages":[]}`,
	)
	initialResponse := doDurableHTTPRequest(t, replicaA.client, initialRequest)
	defer func() { _ = initialResponse.Body.Close() }()
	if initialResponse.StatusCode != http.StatusOK {
		body := readDurableHTTPBody(t, initialResponse.Body)
		closeDurableHTTPBody(t, initialResponse.Body)
		t.Fatalf("replica A initial status = %d, body = %q", initialResponse.StatusCode, body)
	}
	streamID := requireDurableResponseHeaders(t, initialResponse)
	initialDecoder := sse.NewDecoder(initialResponse.Body)
	requireDurableSSEEvent(t, readNextDurableSSE(t, initialDecoder), "1", streamOpenEvent, "")
	requireDurableSSEEvent(t, readNextDurableSSE(t, initialDecoder), "2", "", chatChunkHello)
	closeDurableHTTPBody(t, initialResponse.Body)
	owner, ownerErr := replicaB.server.durable.journal.(journal.OwnerDirectory).LocateOwner(context.Background(), streamID)
	if ownerErr != nil {
		t.Fatalf("LocateOwner() before relay = %v", ownerErr)
	}
	ownerURL, err := url.Parse(owner.RelayURL)
	if err != nil || ownerURL.Port() == "" || ownerURL.Port() == "0" {
		t.Fatalf("owner relay did not publish its bound port: owner=%+v error=%v", owner, err)
	}

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
		t.Fatalf("replica B relay status = %d, body = %q", resumeResponse.StatusCode, body)
	}
	defer closeDurableHTTPBody(t, resumeResponse.Body)
	if resumedID := requireDurableResponseHeaders(t, resumeResponse); resumedID != streamID {
		t.Fatalf("replica B stream ID = %s, want %s", resumedID, streamID)
	}

	runtime, ok := replicaA.server.durable.loadRuntime(streamID)
	if !ok {
		t.Fatal("producer owner lost its local stream runtime")
	}
	waitForRelaySubscriber(t, runtime)
	if _, local := replicaB.server.durable.loadRuntime(streamID); local {
		t.Fatal("replica B unexpectedly shares replica A producer runtime")
	}

	// Close the actual Redis TCP server only after B has a live owner relay.
	// The gated upstream suffix is therefore impossible to persist but remains
	// available through A's bounded process-local feed.
	redisServer.Close()
	release()

	events := readAllDurableSSE(t, resumeResponse.Body)
	if len(events) != 3 {
		t.Fatalf("remote degraded suffix = %#v, want warning, chunk, sentinel", events)
	}
	requirePhase5UnsequencedEvent(t, events[0], streamWarningEvent, `"code":"journal_degraded"`)
	requirePhase5UnsequencedEvent(t, events[1], "", chatChunkWorld)
	if events[2].HasID || events[2].HasType || string(events[2].Data) != doneSentinelData {
		t.Fatalf("remote degraded terminal sentinel = %#v", events[2])
	}
	warningCount, suffixCount, doneCount := 0, 0, 0
	for _, event := range events {
		switch {
		case event.Type == streamWarningEvent && strings.Contains(string(event.Data), `"code":"journal_degraded"`):
			warningCount++
		case string(event.Data) == chatChunkWorld:
			suffixCount++
		case string(event.Data) == doneSentinelData:
			doneCount++
		}
	}
	if warningCount != 1 || suffixCount != 1 || doneCount != 1 {
		t.Fatalf("relay delivery counts = warning:%d suffix:%d done:%d, want exactly one each", warningCount, suffixCount, doneCount)
	}
	if calls := backendCalls.Load(); calls != 1 {
		t.Fatalf("backend calls = %d, want one producer", calls)
	}
}

type relayAcceptanceReplica struct {
	server *Server
	url    string
	client *http.Client
}

func startRelayAcceptanceReplica(
	t *testing.T,
	replicaID, backendURL, redisURL, redisPrefix string,
) *relayAcceptanceReplica {
	t.Helper()
	config := DefaultConfig()
	config.BackendURL = backendURL
	config.ListenAddress = "127.0.0.1:0"
	config.JournalBackend = JournalBackendRedis
	config.RedisURL = redisURL
	config.RedisKeyPrefix = redisPrefix
	config.ReplicaID = replicaID
	config.RelayListenAddress = "127.0.0.1:0"
	config.RelayAdvertiseURL = "http://127.0.0.1:0"
	config.RelayInsecureDevMode = true
	config.RelayHeartbeatInterval = 250 * time.Millisecond
	config.RelayPresenceTTL = 3 * time.Second
	config.ReadinessTimeout = 250 * time.Millisecond
	config.DialTimeout = time.Second
	config.TLSHandshakeTimeout = time.Second
	config.ShutdownTimeout = 3 * time.Second

	server, err := NewServer(
		config,
		slog.New(slog.DiscardHandler),
		WithStreamIDGenerator(&durableSequentialIDs{}),
	)
	if err != nil {
		t.Fatalf("NewServer(%s) error = %v", replicaID, err)
	}
	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for %s public proxy: %v", replicaID, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	serveResult := make(chan error, 1)
	go func() { serveResult <- server.Serve(ctx, listener) }()

	transport := &http.Transport{DisableCompression: true}
	client := &http.Client{Transport: transport, Timeout: durableHTTPTestTimeout}
	url := "http://" + listener.Addr().String()
	waitForRelayAcceptanceHealth(t, client, url)
	t.Cleanup(func() {
		cancel()
		select {
		case serveErr := <-serveResult:
			if serveErr != nil {
				t.Errorf("Serve(%s) error = %v", replicaID, serveErr)
			}
		case <-time.After(5 * time.Second):
			t.Errorf("Serve(%s) did not shut down", replicaID)
		}
		transport.CloseIdleConnections()
	})
	return &relayAcceptanceReplica{server: server, url: url, client: client}
}

func newRelayAcceptanceBackend(
	t *testing.T,
	calls *atomic.Int64,
	release <-chan struct{},
) *httptest.Server {
	t.Helper()
	backend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/health" {
			writer.WriteHeader(http.StatusOK)
			return
		}
		calls.Add(1)
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
	return backend
}

func waitForRelayAcceptanceHealth(t *testing.T, client *http.Client, baseURL string) {
	t.Helper()
	deadline := time.Now().Add(durableHTTPTestTimeout)
	for time.Now().Before(deadline) {
		request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, baseURL+"/healthz", nil)
		if err != nil {
			t.Fatalf("NewRequestWithContext(health) error = %v", err)
		}
		response, err := client.Do(request)
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("proxy at %s did not become healthy", baseURL)
}

func waitForRelaySubscriber(t *testing.T, runtime *streamRuntime) {
	t.Helper()
	deadline := time.Now().Add(durableHTTPTestTimeout)
	for time.Now().Before(deadline) {
		if runtime.degradedFeed.subscriberCount() >= 1 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("owner relay did not subscribe before the Redis cut")
}
