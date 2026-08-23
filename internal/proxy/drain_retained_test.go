package proxy

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/satwiksps/streamweld/internal/backend"
	"github.com/satwiksps/streamweld/internal/conformance"
	"github.com/satwiksps/streamweld/internal/journal"
)

type drainHTTPResult struct {
	response *http.Response
	err      error
}

func executeDrainRequest(client *http.Client, request *http.Request) drainHTTPResult {
	// The receiver owns and closes a non-nil response body.
	response, err := client.Do(request) //nolint:bodyclose
	return drainHTTPResult{response: response, err: err}
}

type blockingDrainWarningJournal struct {
	journal.Journal
	entered chan time.Time
}

func (store *blockingDrainWarningJournal) Append(
	ctx context.Context,
	id journal.StreamID,
	entry journal.Entry,
) (uint64, error) {
	if entry.Kind != journal.KindWarning {
		return store.Journal.Append(ctx, id, entry)
	}
	deadline, _ := ctx.Deadline()
	store.entered <- deadline
	<-ctx.Done()
	return 0, ctx.Err()
}

func newDrainJournalHarness(
	t *testing.T,
	backendURL string,
	pool *backend.Pool,
	store journal.Journal,
) *failoverHTTPHarness {
	t.Helper()
	config := DefaultConfig()
	config.BackendURL = backendURL
	config.ListenAddress = "127.0.0.1:0"
	config.ReadinessTimeout = time.Second
	server, err := NewServer(
		config,
		nil,
		WithBackendPool(pool),
		WithJournal(store),
		WithStreamIDGenerator(&failoverSequentialIDs{}),
	)
	if err != nil {
		t.Fatal(err)
	}
	front := httptest.NewServer(server.Handler())
	transport := &http.Transport{DisableCompression: true}
	client := &http.Client{Transport: transport, Timeout: failoverHTTPTestTimeout}
	t.Cleanup(func() {
		server.forceCancel()
		front.CloseClientConnections()
		front.Close()
		transport.CloseIdleConnections()
		server.closeIdleConnections()
	})
	return &failoverHTTPHarness{server: server, url: front.URL, client: client}
}

func awaitBackendDraining(t *testing.T, pool *backend.Pool, id backend.ID) {
	t.Helper()
	deadline := time.NewTimer(failoverHTTPTestTimeout)
	defer deadline.Stop()
	for {
		changed := pool.Changes()
		for _, state := range pool.ListRetained() {
			if state.ID == id && state.Draining {
				return
			}
		}
		select {
		case <-changed:
		case <-deadline.C:
			t.Fatalf("backend %q was not marked draining", id)
		}
	}
}

func TestConcurrentPodDrainsRetryBothMigrationsUntilTargetAdmission(t *testing.T) {
	t.Parallel()
	type originSignals struct {
		started  chan struct{}
		canceled chan struct{}
	}
	newOrigin := func(label string) (*httptest.Server, originSignals) {
		signals := originSignals{started: make(chan struct{}, 1), canceled: make(chan struct{}, 1)}
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			startFailoverBackendSSE(writer)
			if !writeFailoverBackendData(writer, failoverChatChunk(
				label+"-origin", label+" ", "",
				&failoverUsage{PromptTokens: 1, CompletionTokens: 1, TotalTokens: 2},
			)) {
				return
			}
			signals.started <- struct{}{}
			<-request.Context().Done()
			signals.canceled <- struct{}{}
		}))
		t.Cleanup(server.Close)
		return server, signals
	}
	originAServer, signalsA := newOrigin("A")
	originBServer, signalsB := newOrigin("B")
	targetServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)
			return
		}
		var payload struct {
			Messages []struct {
				Content string `json:"content"`
			} `json:"messages"`
		}
		if json.Unmarshal(body, &payload) != nil || len(payload.Messages) == 0 {
			http.Error(writer, "continuation has no assistant prefix", http.StatusBadRequest)
			return
		}
		prefix := payload.Messages[len(payload.Messages)-1].Content
		writeTriggerTarget(writer, "concurrent-target", prefix+"recovered", 2)
	}))
	t.Cleanup(targetServer.Close)

	originA := newFailoverBackend(t, "route-a/pod-old-a", originAServer.URL, "model-v1", conformance.VerdictSafe)
	originB := newFailoverBackend(t, "route-a/pod-old-b", originBServer.URL, "model-v1", conformance.VerdictSafe)
	config := backend.DefaultConfig()
	var selection atomic.Uint64
	var gateAChoice atomic.Bool
	var blockedAChoice atomic.Bool
	aChoiceEntered := make(chan struct{})
	releaseAChoice := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-releaseAChoice:
		default:
			close(releaseAChoice)
		}
	})
	config.Choose = func(candidates []backend.State) int {
		if gateAChoice.Load() && len(candidates) == 1 && candidates[0].ID == originB.ID &&
			blockedAChoice.CompareAndSwap(false, true) {
			close(aChoiceEntered)
			<-releaseAChoice
		}
		return int((selection.Add(1) - 1) % uint64(len(candidates)))
	}
	pool, err := backend.NewPool(config, originA, originB)
	if err != nil {
		t.Fatal(err)
	}
	for _, candidate := range []backend.Backend{originA, originB} {
		if _, err := pool.SetHealth(candidate.ID, backend.HealthHealthy); err != nil {
			t.Fatal(err)
		}
	}
	harness := newFailoverHTTPHarness(t, originAServer.URL, pool, nil)
	initial := testRouteUpdate("test-model", "uid-a", 1, originA.ID.String(), originAServer.URL)
	initial.Backends[0].ModelVersion = "model-v1"
	initial.Backends[0].PodNamespace = "models"
	initial.Backends[0].PodName = "pod-old-a"
	initial.Backends = append(initial.Backends, routeBackendInput{
		ID: originB.ID.String(), URL: originBServer.URL, ModelVersion: "model-v1",
		TemplateVerdict: conformance.VerdictSafe, PodNamespace: "models", PodName: "pod-old-b",
	})
	if _, err := harness.server.durable.routes.apply("models/route-a", initial); err != nil {
		t.Fatal(err)
	}

	startStream := func(content string) *http.Response {
		request := newFailoverHTTPRequest(t, http.MethodPost, harness.url+"/v1/chat/completions",
			`{"model":"test-model","stream":true,"messages":[{"role":"user","content":"`+content+`"}],"max_tokens":10}`)
		request.Header.Set(headerVerbose, "1")
		return doFailoverHTTPRequest(t, harness.client, request)
	}
	streamA := startStream("concurrent A")
	defer closeFailoverBody(t, streamA.Body)
	awaitFailoverSignal(t, signalsA.started, "origin A response")
	streamB := startStream("concurrent B")
	defer closeFailoverBody(t, streamB.Body)
	awaitFailoverSignal(t, signalsB.started, "origin B response")

	drainDone := make(chan drainHTTPResult, 2)
	startDrain := func(pod string) {
		request := newFailoverHTTPRequest(t, http.MethodPost,
			harness.url+"/internal/backends/by-pod/models/"+pod+"/drain?timeout=2s", "")
		go func() {
			drainDone <- executeDrainRequest(harness.client, request)
		}()
	}
	gateAChoice.Store(true)
	startDrain("pod-old-a")
	awaitBackendDraining(t, pool, originA.ID)
	awaitFailoverSignal(t, aChoiceEntered, "origin A preflight choice of origin B")
	startDrain("pod-old-b")
	awaitBackendDraining(t, pool, originB.ID)
	close(releaseAChoice)
	awaitFailoverSignal(t, signalsA.canceled, "origin A drain cancellation after B became draining")
	awaitFailoverSignal(t, signalsB.canceled, "origin B drain cancellation")
	select {
	case result := <-drainDone:
		if result.response != nil {
			_ = result.response.Body.Close()
		}
		t.Fatalf("concurrent drain returned before target admission: %v", result.err)
	case <-time.After(100 * time.Millisecond):
	}

	replacement := testRouteUpdate("test-model", "uid-a", 1, "route-a/pod-new", targetServer.URL)
	replacement.Backends[0].ModelVersion = "model-v1"
	replacement.Backends[0].PodNamespace = "models"
	replacement.Backends[0].PodName = "pod-new"
	if _, err := harness.server.durable.routes.apply("models/route-a", replacement); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 2; index++ {
		result := awaitFailoverValue(t, drainDone, "concurrent pod drain completion")
		if result.err != nil {
			t.Fatalf("concurrent pod drain request error: %v", result.err)
		}
		body := readFailoverBody(t, result.response.Body)
		_ = result.response.Body.Close()
		if result.response.StatusCode != http.StatusOK {
			t.Fatalf("concurrent pod drain status = %d, body=%s", result.response.StatusCode, body)
		}
	}

	for label, response := range map[string]*http.Response{"A": streamA, "B": streamB} {
		events := readAllFailoverSSE(t, response.Body)
		requireFailoverSequence(t, events)
		requireTriggerMigration(t, events, "drain", 2)
		requireFailoverDone(t, events)
		if got, want := failoverText(events), label+" recovered"; got != want {
			t.Fatalf("stream %s output = %q, want %q", label, got, want)
		}
	}
}

func TestPodDrainRetriesUntilDelayedReplacementAdmission(t *testing.T) {
	t.Parallel()
	originStarted := make(chan struct{}, 1)
	originCanceled := make(chan struct{}, 1)
	originRelease := make(chan struct{})
	t.Cleanup(func() { close(originRelease) })
	originServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		startFailoverBackendSSE(writer)
		if !writeFailoverBackendData(writer, failoverChatChunk(
			"delayed-origin", "old ", "",
			&failoverUsage{PromptTokens: 1, CompletionTokens: 1, TotalTokens: 2},
		)) {
			return
		}
		originStarted <- struct{}{}
		select {
		case <-request.Context().Done():
			originCanceled <- struct{}{}
		case <-originRelease:
		}
	}))
	t.Cleanup(originServer.Close)
	targetServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writeTriggerTarget(writer, "delayed-target", "old recovered", 2)
	}))
	t.Cleanup(targetServer.Close)

	origin := newFailoverBackend(t, "route-a/pod-old", originServer.URL, "model-v1", conformance.VerdictSafe)
	pool := newFailoverBackendPool(t, origin)
	harness := newFailoverHTTPHarness(t, originServer.URL, pool, nil)
	initial := testRouteUpdate("test-model", "uid-a", 1, origin.ID.String(), originServer.URL)
	initial.Backends[0].ModelVersion = "model-v1"
	initial.Backends[0].PodNamespace = "models"
	initial.Backends[0].PodName = "pod-old"
	if _, err := harness.server.durable.routes.apply("models/route-a", initial); err != nil {
		t.Fatal(err)
	}

	streamRequest := newFailoverHTTPRequest(t, http.MethodPost, harness.url+"/v1/chat/completions",
		`{"model":"test-model","stream":true,"messages":[{"role":"user","content":"delayed admission"}],"max_tokens":10}`)
	streamRequest.Header.Set(headerVerbose, "1")
	streamResponse := doFailoverHTTPRequest(t, harness.client, streamRequest)
	defer closeFailoverBody(t, streamResponse.Body)
	awaitFailoverSignal(t, originStarted, "delayed origin response")

	drainRequest := newFailoverHTTPRequest(t, http.MethodPost,
		harness.url+"/internal/backends/by-pod/models/pod-old/drain?timeout=2s", "")
	drainDone := make(chan drainHTTPResult, 1)
	go func() {
		drainDone <- executeDrainRequest(harness.client, drainRequest)
	}()
	awaitBackendDraining(t, pool, origin.ID)
	awaitFailoverSignal(t, originCanceled, "origin cancellation before target admission")

	select {
	case result := <-drainDone:
		if result.response != nil {
			_ = result.response.Body.Close()
		}
		t.Fatalf("pod drain returned before replacement admission: %v", result.err)
	case <-time.After(100 * time.Millisecond):
	}

	replacement := testRouteUpdate("test-model", "uid-a", 1, "route-a/pod-new", targetServer.URL)
	replacement.Backends[0].ModelVersion = "model-v1"
	replacement.Backends[0].PodNamespace = "models"
	replacement.Backends[0].PodName = "pod-new"
	if _, err := harness.server.durable.routes.apply("models/route-a", replacement); err != nil {
		t.Fatal(err)
	}

	result := awaitFailoverValue(t, drainDone, "pod drain after replacement admission")
	if result.err != nil {
		t.Fatalf("pod drain request error: %v", result.err)
	}
	defer closeFailoverBody(t, result.response.Body)
	drainBody := readFailoverBody(t, result.response.Body)
	if result.response.StatusCode != http.StatusOK {
		t.Fatalf("pod drain status = %d, body=%s", result.response.StatusCode, drainBody)
	}
	var drained podDrainResponse
	decodeFailoverJSON(t, drainBody, &drained)
	if drained.InFlight != 0 || strings.Join(drained.Backends, ",") != origin.ID.String() {
		t.Fatalf("pod drain response = %+v", drained)
	}
	events := readAllFailoverSSE(t, streamResponse.Body)
	requireFailoverSequence(t, events)
	requireTriggerMigration(t, events, "drain", 2)
	requireFailoverDone(t, events)
	if got := failoverText(events); got != "old recovered" {
		t.Fatalf("stream output = %q, want %q", got, "old recovered")
	}
}

func TestPodDrainRetryWakesAtTargetQuarantineExpiry(t *testing.T) {
	t.Parallel()
	originStarted := make(chan struct{}, 1)
	originServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		startFailoverBackendSSE(writer)
		if !writeFailoverBackendData(writer, failoverChatChunk(
			"quarantine-origin", "old ", "",
			&failoverUsage{PromptTokens: 1, CompletionTokens: 1, TotalTokens: 2},
		)) {
			return
		}
		originStarted <- struct{}{}
		<-request.Context().Done()
	}))
	t.Cleanup(originServer.Close)
	targetServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writeTriggerTarget(writer, "quarantine-target", "old recovered", 2)
	}))
	t.Cleanup(targetServer.Close)

	origin := newFailoverBackend(t, "route-a/pod-old", originServer.URL, "model-v1", conformance.VerdictSafe)
	target := newFailoverBackend(t, "route-a/pod-target", targetServer.URL, "model-v1", conformance.VerdictSafe)
	config := backend.DefaultConfig()
	config.QuarantineWindow = 150 * time.Millisecond
	config.Choose = func([]backend.State) int { return 0 }
	pool, err := backend.NewPool(config, origin, target)
	if err != nil {
		t.Fatal(err)
	}
	for _, candidate := range []backend.Backend{origin, target} {
		if _, err := pool.SetHealth(candidate.ID, backend.HealthHealthy); err != nil {
			t.Fatal(err)
		}
	}
	harness := newFailoverHTTPHarness(t, originServer.URL, pool, nil)
	initial := testRouteUpdate("test-model", "uid-a", 1, origin.ID.String(), originServer.URL)
	initial.Backends[0].ModelVersion = "model-v1"
	initial.Backends[0].PodNamespace = "models"
	initial.Backends[0].PodName = "pod-old"
	initial.Backends = append(initial.Backends, routeBackendInput{
		ID: target.ID.String(), URL: targetServer.URL, ModelVersion: "model-v1",
		TemplateVerdict: conformance.VerdictSafe, PodNamespace: "models", PodName: "pod-target",
	})
	if _, err := harness.server.durable.routes.apply("models/route-a", initial); err != nil {
		t.Fatal(err)
	}

	streamRequest := newFailoverHTTPRequest(t, http.MethodPost, harness.url+"/v1/chat/completions",
		`{"model":"test-model","stream":true,"messages":[{"role":"user","content":"quarantine"}],"max_tokens":10}`)
	streamRequest.Header.Set(headerVerbose, "1")
	streamResponse := doFailoverHTTPRequest(t, harness.client, streamRequest)
	defer closeFailoverBody(t, streamResponse.Body)
	awaitFailoverSignal(t, originStarted, "quarantine origin response")
	if _, err := pool.MarkPassiveFailure(target.ID); err != nil {
		t.Fatal(err)
	}

	drainRequest := newFailoverHTTPRequest(t, http.MethodPost,
		harness.url+"/internal/backends/by-pod/models/pod-old/drain?timeout=2s", "")
	drainResponse := doFailoverHTTPRequest(t, harness.client, drainRequest)
	defer closeFailoverBody(t, drainResponse.Body)
	drainBody := readFailoverBody(t, drainResponse.Body)
	if drainResponse.StatusCode != http.StatusOK {
		t.Fatalf("quarantine pod drain status = %d, body=%s", drainResponse.StatusCode, drainBody)
	}

	events := readAllFailoverSSE(t, streamResponse.Body)
	requireTriggerMigration(t, events, "drain", 2)
	requireFailoverDone(t, events)
	if got := failoverText(events); got != "old recovered" {
		t.Fatalf("quarantine-expiry output = %q, want old recovered", got)
	}
}

func TestPodDrainRetryStopsAtMaxStreamDuration(t *testing.T) {
	t.Parallel()
	originStarted := make(chan struct{}, 1)
	originServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		startFailoverBackendSSE(writer)
		if !writeFailoverBackendData(writer, failoverChatChunk(
			"duration-origin", "partial", "",
			&failoverUsage{PromptTokens: 1, CompletionTokens: 1, TotalTokens: 2},
		)) {
			return
		}
		originStarted <- struct{}{}
		<-request.Context().Done()
	}))
	t.Cleanup(originServer.Close)

	origin := newFailoverBackend(t, "route-a/pod-old", originServer.URL, "model-v1", conformance.VerdictSafe)
	pool := newFailoverBackendPool(t, origin)
	harness := newFailoverHTTPHarness(t, originServer.URL, pool, nil)
	initial := testRouteUpdate("test-model", "uid-a", 1, origin.ID.String(), originServer.URL)
	initial.Backends[0].ModelVersion = "model-v1"
	initial.Backends[0].PodNamespace = "models"
	initial.Backends[0].PodName = "pod-old"
	if _, err := harness.server.durable.routes.apply("models/route-a", initial); err != nil {
		t.Fatal(err)
	}

	streamRequest := newFailoverHTTPRequest(t, http.MethodPost, harness.url+"/v1/chat/completions",
		`{"model":"test-model","stream":true,"messages":[{"role":"user","content":"duration"}],"max_tokens":10}`)
	streamRequest.Header.Set(headerVerbose, "1")
	streamResponse := doFailoverHTTPRequest(t, harness.client, streamRequest)
	defer closeFailoverBody(t, streamResponse.Body)
	awaitFailoverSignal(t, originStarted, "duration origin response")
	var runtime *streamRuntime
	harness.server.durable.streams.Range(func(_, value any) bool {
		runtime = value.(*streamRuntime)
		return false
	})
	if runtime == nil {
		t.Fatal("active stream runtime not found")
	}
	runtime.mu.Lock()
	runtime.createdAt = time.Now()
	runtime.policy.MaxStreamDuration = 300 * time.Millisecond
	runtime.mu.Unlock()

	started := time.Now()
	drainRequest := newFailoverHTTPRequest(t, http.MethodPost,
		harness.url+"/internal/backends/by-pod/models/pod-old/drain?timeout=2s", "")
	drainResponse := doFailoverHTTPRequest(t, harness.client, drainRequest)
	defer closeFailoverBody(t, drainResponse.Body)
	drainBody := readFailoverBody(t, drainResponse.Body)
	if drainResponse.StatusCode != http.StatusOK {
		t.Fatalf("duration pod drain status = %d, body=%s", drainResponse.StatusCode, drainBody)
	}
	if elapsed := time.Since(started); elapsed < 150*time.Millisecond || elapsed > time.Second {
		t.Fatalf("duration-gated drain elapsed = %s, want threshold wake well before handler deadline", elapsed)
	}

	events := readAllFailoverSSE(t, streamResponse.Body)
	requireFailoverRefusal(t, events, "max_stream_duration", "")
	errorIndex := failoverEventIndex(events, streamErrorEvent)
	var terminal struct {
		Reason string `json:"reason"`
	}
	decodeFailoverJSON(t, events[errorIndex].Data, &terminal)
	if terminal.Reason != "max_stream_duration" {
		t.Fatalf("terminal refusal reason = %q, want max_stream_duration", terminal.Reason)
	}
}

func TestPodDrainDeadlineBoundsConcurrentSlowJournalRefusals(t *testing.T) {
	t.Parallel()
	const streamCount = 4
	originStarted := make(chan struct{}, streamCount)
	originServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		startFailoverBackendSSE(writer)
		if !writeFailoverBackendData(writer, failoverChatChunk(
			"slow-journal-origin", "partial", "",
			&failoverUsage{PromptTokens: 1, CompletionTokens: 1, TotalTokens: 2},
		)) {
			return
		}
		originStarted <- struct{}{}
		<-request.Context().Done()
	}))
	t.Cleanup(originServer.Close)

	origin := newFailoverBackend(t, "route-a/pod-old", originServer.URL, "model-v1", conformance.VerdictSafe)
	pool := newFailoverBackendPool(t, origin)
	blockingStore := &blockingDrainWarningJournal{
		Journal: newCreationMemoryJournal(t),
		entered: make(chan time.Time, streamCount),
	}
	harness := newDrainJournalHarness(t, originServer.URL, pool, blockingStore)
	initial := testRouteUpdate("test-model", "uid-a", 1, origin.ID.String(), originServer.URL)
	initial.Policy.MaxMigrations = 0
	initial.Backends[0].ModelVersion = "model-v1"
	initial.Backends[0].PodNamespace = "models"
	initial.Backends[0].PodName = "pod-old"
	if _, err := harness.server.durable.routes.apply("models/route-a", initial); err != nil {
		t.Fatal(err)
	}

	for index := 0; index < streamCount; index++ {
		streamRequest := newFailoverHTTPRequest(t, http.MethodPost, harness.url+"/v1/chat/completions",
			`{"model":"test-model","stream":true,"messages":[{"role":"user","content":"slow journal"}],"max_tokens":10}`)
		streamRequest.Header.Set(headerVerbose, "1")
		response := doFailoverHTTPRequest(t, harness.client, streamRequest)
		defer closeFailoverBody(t, response.Body)
		awaitFailoverSignal(t, originStarted, "slow-journal origin response")
	}

	started := time.Now()
	drainRequest := newFailoverHTTPRequest(t, http.MethodPost,
		harness.url+"/internal/backends/by-pod/models/pod-old/drain?timeout=150ms", "")
	drainResponse := doFailoverHTTPRequest(t, harness.client, drainRequest)
	defer closeFailoverBody(t, drainResponse.Body)
	drainBody := readFailoverBody(t, drainResponse.Body)
	if drainResponse.StatusCode != http.StatusGatewayTimeout {
		t.Fatalf("slow-journal drain status = %d, body=%s", drainResponse.StatusCode, drainBody)
	}
	if elapsed := time.Since(started); elapsed > 750*time.Millisecond {
		t.Fatalf("slow-journal drain took %s, want handler bounded independently of stream count", elapsed)
	}
	latestAllowed := started.Add(300 * time.Millisecond)
	for index := 0; index < streamCount; index++ {
		deadline := awaitFailoverValue(t, blockingStore.entered, "deadline-capped warning append")
		if deadline.IsZero() || deadline.After(latestAllowed) {
			t.Fatalf("warning append %d deadline = %s, want drain deadline near %s", index, deadline, started.Add(150*time.Millisecond))
		}
	}
}

func TestPodDrainDeadlineFailsLoudlyWithoutReplacement(t *testing.T) {
	t.Parallel()
	originStarted := make(chan struct{}, 1)
	originCanceled := make(chan struct{}, 1)
	originRelease := make(chan struct{})
	t.Cleanup(func() { close(originRelease) })
	originServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		startFailoverBackendSSE(writer)
		if !writeFailoverBackendData(writer, failoverChatChunk(
			"deadline-origin", "partial", "",
			&failoverUsage{PromptTokens: 1, CompletionTokens: 1, TotalTokens: 2},
		)) {
			return
		}
		originStarted <- struct{}{}
		select {
		case <-request.Context().Done():
			originCanceled <- struct{}{}
		case <-originRelease:
		}
	}))
	t.Cleanup(originServer.Close)

	origin := newFailoverBackend(t, "route-a/pod-old", originServer.URL, "model-v1", conformance.VerdictSafe)
	pool := newFailoverBackendPool(t, origin)
	harness := newFailoverHTTPHarness(t, originServer.URL, pool, nil)
	initial := testRouteUpdate("test-model", "uid-a", 1, origin.ID.String(), originServer.URL)
	initial.Backends[0].ModelVersion = "model-v1"
	initial.Backends[0].PodNamespace = "models"
	initial.Backends[0].PodName = "pod-old"
	if _, err := harness.server.durable.routes.apply("models/route-a", initial); err != nil {
		t.Fatal(err)
	}

	streamRequest := newFailoverHTTPRequest(t, http.MethodPost, harness.url+"/v1/chat/completions",
		`{"model":"test-model","stream":true,"messages":[{"role":"user","content":"deadline"}],"max_tokens":10}`)
	streamRequest.Header.Set(headerVerbose, "1")
	streamResponse := doFailoverHTTPRequest(t, harness.client, streamRequest)
	defer closeFailoverBody(t, streamResponse.Body)
	awaitFailoverSignal(t, originStarted, "deadline origin response")

	drainRequest := newFailoverHTTPRequest(t, http.MethodPost,
		harness.url+"/internal/backends/by-pod/models/pod-old/drain?timeout=100ms", "")
	drainResponse := doFailoverHTTPRequest(t, harness.client, drainRequest)
	defer closeFailoverBody(t, drainResponse.Body)
	drainBody := readFailoverBody(t, drainResponse.Body)
	if drainResponse.StatusCode != http.StatusOK && drainResponse.StatusCode != http.StatusGatewayTimeout {
		t.Fatalf("pod drain status = %d, body=%s; want 200 or 504 at the deadline boundary", drainResponse.StatusCode, drainBody)
	}
	var drained podDrainResponse
	decodeFailoverJSON(t, drainBody, &drained)
	if drained.InFlight > 1 || drained.State != "draining" {
		t.Fatalf("pod drain response = %+v", drained)
	}
	if drainResponse.StatusCode == http.StatusOK && drained.InFlight != 0 {
		t.Fatalf("successful pod drain response still has in-flight work: %+v", drained)
	}
	awaitFailoverSignal(t, originCanceled, "deadline refusal cancellation")

	events := readAllFailoverSSE(t, streamResponse.Body)
	requireFailoverSequence(t, events)
	requireFailoverRefusal(t, events, "backend_available", "")
	errorIndex := failoverEventIndex(events, streamErrorEvent)
	var terminal struct {
		Reason string `json:"reason"`
	}
	decodeFailoverJSON(t, events[errorIndex].Data, &terminal)
	if terminal.Reason != "backend_available" {
		t.Fatalf("terminal refusal reason = %q, want backend_available", terminal.Reason)
	}
}

func TestPodDrainMigratesControllerRetiredLeasedBackend(t *testing.T) {
	t.Parallel()
	originStarted := make(chan struct{}, 1)
	originCanceled := make(chan struct{}, 1)
	originServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		startFailoverBackendSSE(writer)
		if !writeFailoverBackendData(writer, failoverChatChunk(
			"retired-origin", "old ", "",
			&failoverUsage{PromptTokens: 1, CompletionTokens: 1, TotalTokens: 2},
		)) {
			return
		}
		originStarted <- struct{}{}
		<-request.Context().Done()
		originCanceled <- struct{}{}
	}))
	t.Cleanup(originServer.Close)
	targetServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writeTriggerTarget(writer, "replacement-target", "old recovered", 2)
	}))
	t.Cleanup(targetServer.Close)

	origin := newFailoverBackend(t, "route-a/pod-old", originServer.URL, "model-v1", conformance.VerdictSafe)
	pool := newFailoverBackendPool(t, origin)
	harness := newFailoverHTTPHarness(t, originServer.URL, pool, nil)
	initial := testRouteUpdate("test-model", "uid-a", 1, origin.ID.String(), originServer.URL)
	initial.Backends[0].ModelVersion = "model-v1"
	initial.Backends[0].PodNamespace = "models"
	initial.Backends[0].PodName = "pod-old"
	if _, err := harness.server.durable.routes.apply("models/route-a", initial); err != nil {
		t.Fatal(err)
	}

	streamRequest := newFailoverHTTPRequest(t, http.MethodPost, harness.url+"/v1/chat/completions",
		`{"model":"test-model","stream":true,"messages":[{"role":"user","content":"retained drain"}],"max_tokens":10}`)
	streamRequest.Header.Set(headerVerbose, "1")
	streamResponse := doFailoverHTTPRequest(t, harness.client, streamRequest)
	defer closeFailoverBody(t, streamResponse.Body)
	awaitFailoverSignal(t, originStarted, "retired origin response")

	replacement := testRouteUpdate("test-model", "uid-a", 1, "route-a/pod-new", targetServer.URL)
	replacement.Backends[0].ModelVersion = "model-v1"
	replacement.Backends[0].PodNamespace = "models"
	replacement.Backends[0].PodName = "pod-new"
	if _, err := harness.server.durable.routes.apply("models/route-a", replacement); err != nil {
		t.Fatal(err)
	}
	if states := pool.List(); len(states) != 1 || states[0].ID != "route-a/pod-new" {
		t.Fatalf("live pool after replacement = %+v", states)
	}
	retained := pool.ListRetained()
	var retainedOrigin *backend.State
	for index := range retained {
		if retained[index].ID == origin.ID {
			retainedOrigin = &retained[index]
			break
		}
	}
	if len(retained) != 2 || retainedOrigin == nil || !retainedOrigin.Draining || retainedOrigin.InFlight != 1 {
		t.Fatalf("retained pool after replacement = %+v", retained)
	}

	drainRequest := newFailoverHTTPRequest(t, http.MethodPost,
		harness.url+"/internal/backends/by-pod/models/pod-old/drain?timeout=2s", "")
	drainResponse := doFailoverHTTPRequest(t, harness.client, drainRequest)
	defer closeFailoverBody(t, drainResponse.Body)
	drainBody := readFailoverBody(t, drainResponse.Body)
	if drainResponse.StatusCode != http.StatusOK {
		t.Fatalf("retired pod drain status = %d, body=%s", drainResponse.StatusCode, drainBody)
	}
	var drained podDrainResponse
	decodeFailoverJSON(t, drainBody, &drained)
	if drained.InFlight != 0 || strings.Join(drained.Backends, ",") != origin.ID.String() {
		t.Fatalf("retired pod drain response = %+v", drained)
	}
	awaitFailoverSignal(t, originCanceled, "retired origin migration cancellation")

	events := readAllFailoverSSE(t, streamResponse.Body)
	requireTriggerMigration(t, events, "drain", 2)
	requireFailoverDone(t, events)
	for _, state := range pool.ListRetained() {
		if state.ID == origin.ID {
			t.Fatalf("retired backend remained after zero-inflight wait: %+v", state)
		}
	}
}
