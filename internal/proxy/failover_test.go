package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/streamweld/streamweld/internal/backend"
	"github.com/streamweld/streamweld/internal/conformance"
	"github.com/streamweld/streamweld/internal/journal"
	"github.com/streamweld/streamweld/internal/proxy/sse"
)

const failoverHTTPTestTimeout = 5 * time.Second

func TestFailoverHTTPContinuesWithoutBreakingClientStream(t *testing.T) {
	originRequests := make(chan failoverBackendRequest, 1)
	originServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		originRequests <- captureFailoverBackendRequest(request)
		startFailoverBackendSSE(writer)
		writeFailoverBackendData(writer, failoverChatChunk(
			"origin-chunk",
			"The quick brown ",
			"",
			&failoverUsage{PromptTokens: 3, CompletionTokens: 4, TotalTokens: 7},
		))
		// Returning without a finish reason or [DONE] simulates a connection that
		// ends after accepted generation bytes have already reached the client.
	}))
	t.Cleanup(originServer.Close)

	targetRequests := make(chan failoverBackendRequest, 1)
	targetServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		targetRequests <- captureFailoverBackendRequest(request)
		startFailoverBackendSSE(writer)
		if !writeFailoverBackendData(writer, failoverChatChunk("target-chunk", "brown fox", "", nil)) {
			return
		}
		if !writeFailoverBackendData(writer, failoverChatChunk(
			"target-chunk",
			"",
			"stop",
			&failoverUsage{PromptTokens: 3, CompletionTokens: 5, TotalTokens: 8},
		)) {
			return
		}
		writeFailoverBackendData(writer, doneSentinelData)
	}))
	t.Cleanup(targetServer.Close)

	origin := newFailoverBackend(t, "origin.test:8000", originServer.URL, "model-v1", conformance.VerdictSafe)
	target := newFailoverBackend(t, "target.test:8000", targetServer.URL, "model-v1", conformance.VerdictSafe)
	pool := newFailoverBackendPool(t, origin, target)
	harness := newFailoverHTTPHarness(t, originServer.URL, pool, func(config *Config) {
		config.SeamWindowBytes = 8
	})

	requestBody := `{
		"model":"test-model",
		"stream":true,
		"messages":[{"role":"user","content":"Finish this sentence"}],
		"max_tokens":10,
		"temperature":0.25,
		"seed":7123,
		"vendor_extension":{"preserve":"exactly"}
	}`
	request := newFailoverHTTPRequest(t, http.MethodPost, harness.url+"/v1/chat/completions", requestBody)
	request.Header.Set(headerVerbose, "1")
	response := doFailoverHTTPRequest(t, harness.client, request)
	defer closeFailoverBody(t, response.Body)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("completion status = %d, body = %q", response.StatusCode, readFailoverBody(t, response.Body))
	}
	requireDurableResponseHeaders(t, response)

	events := readAllFailoverSSE(t, response.Body)
	requireFailoverSequence(t, events)
	requireFailoverDone(t, events)
	if got := failoverText(events); got != "The quick brown fox" {
		t.Fatalf("assembled downstream text = %q, want %q", got, "The quick brown fox")
	}

	migrationIndex := failoverEventIndex(events, streamMigrationEvent)
	if migrationIndex < 0 {
		t.Fatalf("downstream events contain no verbose migration entry: %#v", events)
	}
	if migrationIndex+1 >= len(events) || events[migrationIndex+1].HasType {
		t.Fatalf("event after migration is not the first continuation chunk: %#v", events[migrationIndex+1:])
	}
	if got := failoverChunkText(t, events[migrationIndex+1]); got != "fox" {
		t.Fatalf("first continuation chunk text = %q, want seam-reconciled %q", got, "fox")
	}

	var migration struct {
		FromBackend         string `json:"from_backend"`
		ToBackend           string `json:"to_backend"`
		Reason              string `json:"reason"`
		RescuedTokens       uint64 `json:"rescued_tokens"`
		TokenCountEstimated bool   `json:"token_count_estimated"`
		Attempt             uint64 `json:"attempt"`
	}
	decodeFailoverJSON(t, events[migrationIndex].Data, &migration)
	if migration.FromBackend != origin.ID.String() || migration.ToBackend != target.ID.String() ||
		migration.Reason != "unexpected_eof" || migration.RescuedTokens != 4 ||
		migration.TokenCountEstimated || migration.Attempt != 2 {
		t.Fatalf("migration payload = %+v", migration)
	}

	originRequest := awaitFailoverValue(t, originRequests, "origin request")
	if originRequest.readErr != nil {
		t.Fatalf("read origin request: %v", originRequest.readErr)
	}
	targetRequest := awaitFailoverValue(t, targetRequests, "continuation request")
	if targetRequest.readErr != nil {
		t.Fatalf("read continuation request: %v", targetRequest.readErr)
	}
	if targetRequest.method != http.MethodPost || targetRequest.path != "/v1/chat/completions" {
		t.Fatalf("continuation target = %s %s", targetRequest.method, targetRequest.path)
	}
	requireFailoverContinuationRequest(t, targetRequest.body)
}

func TestFailoverHTTPRefusesInsideToolCallBoundary(t *testing.T) {
	const toolCallFragment = `{"id":"origin-tool","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call-1","type":"function","function":{"name":"lookup_weather","arguments":"{\"city\":"}}]},"finish_reason":null}],"usage":{"prompt_tokens":3,"completion_tokens":1,"total_tokens":4}}`
	if !json.Valid([]byte(toolCallFragment)) {
		t.Fatal("test tool-call fixture is invalid JSON")
	}

	originServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		startFailoverBackendSSE(writer)
		writeFailoverBackendData(writer, toolCallFragment)
	}))
	t.Cleanup(originServer.Close)
	var targetCalls atomic.Int64
	targetServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		targetCalls.Add(1)
		http.Error(writer, "continuation must not be dispatched", http.StatusInternalServerError)
	}))
	t.Cleanup(targetServer.Close)

	origin := newFailoverBackend(t, "origin.test:8000", originServer.URL, "model-v1", conformance.VerdictSafe)
	target := newFailoverBackend(t, "target.test:8000", targetServer.URL, "model-v1", conformance.VerdictSafe)
	harness := newFailoverHTTPHarness(t, originServer.URL, newFailoverBackendPool(t, origin, target), nil)

	request := newFailoverHTTPRequest(t, http.MethodPost, harness.url+"/v1/chat/completions",
		`{"model":"test-model","stream":true,"messages":[{"role":"user","content":"weather"}],"max_tokens":10}`)
	request.Header.Set(headerVerbose, "1")
	response := doFailoverHTTPRequest(t, harness.client, request)
	defer closeFailoverBody(t, response.Body)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("completion status = %d, body = %q", response.StatusCode, readFailoverBody(t, response.Body))
	}
	events := readAllFailoverSSE(t, response.Body)
	requireFailoverSequence(t, events)
	requireFailoverRefusal(t, events, "", "tool_call_boundary")
	if failoverEventIndex(events, streamMigrationEvent) >= 0 {
		t.Fatalf("tool-call refusal emitted a migration: %#v", events)
	}
	if got := targetCalls.Load(); got != 0 {
		t.Fatalf("continuation backend calls = %d, want 0", got)
	}
	if got := failoverText(events); got != "" {
		t.Fatalf("tool-call fragment was misreported as text %q", got)
	}
}

func TestFailoverHTTPEligibilityRefusals(t *testing.T) {
	tests := []struct {
		name            string
		originVersion   string
		targetVersion   string
		targetVerdict   conformance.Verdict
		includeTarget   bool
		configure       func(*Config)
		failedPredicate string
	}{
		{
			name:            "model version",
			originVersion:   "model-v1",
			targetVersion:   "model-v2",
			targetVerdict:   conformance.VerdictSafe,
			includeTarget:   true,
			failedPredicate: "model_version",
		},
		{
			name:            "unsafe template in strict mode",
			originVersion:   "model-v1",
			targetVersion:   "model-v1",
			targetVerdict:   conformance.VerdictUnsafe,
			includeTarget:   true,
			failedPredicate: "template_verdict",
		},
		{
			name:            "maximum migrations",
			originVersion:   "model-v1",
			targetVersion:   "model-v1",
			targetVerdict:   conformance.VerdictSafe,
			includeTarget:   true,
			configure:       func(config *Config) { config.MaxMigrations = 0 },
			failedPredicate: "max_migrations",
		},
		{
			name:            "no target",
			originVersion:   "model-v1",
			targetVersion:   "model-v1",
			targetVerdict:   conformance.VerdictSafe,
			includeTarget:   false,
			failedPredicate: "backend_available",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			originServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				startFailoverBackendSSE(writer)
				writeFailoverBackendData(writer, failoverChatChunk(
					"origin-refusal",
					"partial",
					"",
					&failoverUsage{PromptTokens: 2, CompletionTokens: 1, TotalTokens: 3},
				))
			}))
			t.Cleanup(originServer.Close)

			var targetCalls atomic.Int64
			targetServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				targetCalls.Add(1)
				http.Error(writer, "ineligible continuation was dispatched", http.StatusInternalServerError)
			}))
			t.Cleanup(targetServer.Close)

			origin := newFailoverBackend(t, "origin.test:8000", originServer.URL, test.originVersion, conformance.VerdictSafe)
			backends := []backend.Backend{origin}
			if test.includeTarget {
				backends = append(backends, newFailoverBackend(
					t,
					"target.test:8000",
					targetServer.URL,
					test.targetVersion,
					test.targetVerdict,
				))
			}
			harness := newFailoverHTTPHarness(t, originServer.URL, newFailoverBackendPool(t, backends...), test.configure)

			request := newFailoverHTTPRequest(t, http.MethodPost, harness.url+"/v1/chat/completions",
				`{"model":"test-model","stream":true,"messages":[{"role":"user","content":"continue"}],"max_tokens":10}`)
			request.Header.Set(headerVerbose, "1")
			response := doFailoverHTTPRequest(t, harness.client, request)
			defer closeFailoverBody(t, response.Body)
			if response.StatusCode != http.StatusOK {
				t.Fatalf("completion status = %d, body = %q", response.StatusCode, readFailoverBody(t, response.Body))
			}
			events := readAllFailoverSSE(t, response.Body)
			requireFailoverSequence(t, events)
			requireFailoverRefusal(t, events, test.failedPredicate, "")
			if failoverEventIndex(events, streamMigrationEvent) >= 0 {
				t.Fatalf("ineligible stream emitted a migration: %#v", events)
			}
			if got := targetCalls.Load(); got != 0 {
				t.Fatalf("continuation backend calls = %d, want 0", got)
			}
		})
	}
}

func TestFailoverHTTPDrainMigratesBoundStreamAndWaitsForZero(t *testing.T) {
	originRelease := make(chan struct{})
	var releaseOrigin sync.Once
	t.Cleanup(func() { releaseOrigin.Do(func() { close(originRelease) }) })
	originCanceled := make(chan struct{}, 1)
	originServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = io.Copy(io.Discard, request.Body)
		startFailoverBackendSSE(writer)
		if !writeFailoverBackendData(writer, failoverChatChunk(
			"origin-drain",
			"drain ",
			"",
			&failoverUsage{PromptTokens: 2, CompletionTokens: 1, TotalTokens: 3},
		)) {
			return
		}
		select {
		case <-request.Context().Done():
			originCanceled <- struct{}{}
		case <-originRelease:
		}
	}))
	t.Cleanup(originServer.Close)

	var targetCalls atomic.Int64
	targetServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		targetCalls.Add(1)
		startFailoverBackendSSE(writer)
		if !writeFailoverBackendData(writer, failoverChatChunk("target-drain", "drain complete", "", nil)) {
			return
		}
		if !writeFailoverBackendData(writer, failoverChatChunk(
			"target-drain",
			"",
			"stop",
			&failoverUsage{PromptTokens: 2, CompletionTokens: 2, TotalTokens: 4},
		)) {
			return
		}
		writeFailoverBackendData(writer, doneSentinelData)
	}))
	t.Cleanup(targetServer.Close)

	origin := newFailoverBackend(t, "origin.test:8000", originServer.URL, "model-v1", conformance.VerdictSafe)
	target := newFailoverBackend(t, "target.test:8000", targetServer.URL, "model-v1", conformance.VerdictSafe)
	pool := newFailoverBackendPool(t, origin, target)
	harness := newFailoverHTTPHarness(t, originServer.URL, pool, func(config *Config) {
		config.SeamWindowBytes = 8
	})

	request := newFailoverHTTPRequest(t, http.MethodPost, harness.url+"/v1/chat/completions",
		`{"model":"test-model","stream":true,"messages":[{"role":"user","content":"drain"}],"max_tokens":10}`)
	request.Header.Set(headerVerbose, "1")
	response := doFailoverHTTPRequest(t, harness.client, request)
	defer closeFailoverBody(t, response.Body)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("completion status = %d, body = %q", response.StatusCode, readFailoverBody(t, response.Body))
	}
	decoder := sse.NewDecoder(response.Body)
	initial := []sse.Event{
		readNextFailoverSSE(t, decoder),
		readNextFailoverSSE(t, decoder),
	}
	if !initial[0].HasType || initial[0].Type != streamOpenEvent {
		t.Fatalf("first event = %#v, want open", initial[0])
	}
	if got := failoverChunkText(t, initial[1]); got != "drain " {
		t.Fatalf("origin text = %q, want %q", got, "drain ")
	}

	drainURL := harness.url + "/internal/backends/" + url.PathEscape(origin.ID.String()) + "/drain?timeout=2s"
	drainRequest := newFailoverHTTPRequest(t, http.MethodPost, drainURL, "")
	drainResponse := doFailoverHTTPRequest(t, harness.client, drainRequest)
	defer closeFailoverBody(t, drainResponse.Body)
	drainBody := readFailoverBody(t, drainResponse.Body)
	if drainResponse.StatusCode != http.StatusOK {
		t.Fatalf("drain status = %d, body = %q", drainResponse.StatusCode, drainBody)
	}
	var drainResult struct {
		Backend  string `json:"backend"`
		State    string `json:"state"`
		InFlight int    `json:"in_flight"`
	}
	decodeFailoverJSON(t, drainBody, &drainResult)
	if drainResult.Backend != origin.ID.String() || drainResult.State != "draining" || drainResult.InFlight != 0 {
		t.Fatalf("drain response = %+v", drainResult)
	}
	awaitFailoverSignal(t, originCanceled, "drained origin request cancellation")

	events := make([]sse.Event, 0, 8)
	events = append(events, initial...)
	events = append(events, readRemainingFailoverSSE(t, decoder)...)
	requireFailoverSequence(t, events)
	requireFailoverDone(t, events)
	if got := failoverText(events); got != "drain complete" {
		t.Fatalf("assembled downstream text after drain = %q, want %q", got, "drain complete")
	}
	migrationIndex := failoverEventIndex(events, streamMigrationEvent)
	if migrationIndex < 0 {
		t.Fatalf("drained stream contains no migration event: %#v", events)
	}
	var migration struct {
		FromBackend string `json:"from_backend"`
		ToBackend   string `json:"to_backend"`
		Reason      string `json:"reason"`
		Attempt     uint64 `json:"attempt"`
	}
	decodeFailoverJSON(t, events[migrationIndex].Data, &migration)
	if migration.FromBackend != origin.ID.String() || migration.ToBackend != target.ID.String() ||
		migration.Reason != "drain" || migration.Attempt != 2 {
		t.Fatalf("drain migration payload = %+v", migration)
	}
	if got := targetCalls.Load(); got != 1 {
		t.Fatalf("continuation backend calls = %d, want 1", got)
	}
}

type failoverUsage struct {
	PromptTokens     uint64 `json:"prompt_tokens"`
	CompletionTokens uint64 `json:"completion_tokens"`
	TotalTokens      uint64 `json:"total_tokens"`
}

type failoverBackendRequest struct {
	method  string
	path    string
	header  http.Header
	body    []byte
	readErr error
}

func captureFailoverBackendRequest(request *http.Request) failoverBackendRequest {
	body, err := io.ReadAll(request.Body)
	return failoverBackendRequest{
		method:  request.Method,
		path:    request.URL.Path,
		header:  request.Header.Clone(),
		body:    body,
		readErr: err,
	}
}

func failoverChatChunk(id, content, finishReason string, usage *failoverUsage) string {
	chunk := struct {
		ID      string `json:"id"`
		Object  string `json:"object"`
		Choices []struct {
			Index        int               `json:"index"`
			Delta        map[string]string `json:"delta"`
			FinishReason *string           `json:"finish_reason"`
		} `json:"choices"`
		Usage *failoverUsage `json:"usage,omitempty"`
	}{
		ID:     id,
		Object: "chat.completion.chunk",
		Usage:  usage,
	}
	choice := struct {
		Index        int               `json:"index"`
		Delta        map[string]string `json:"delta"`
		FinishReason *string           `json:"finish_reason"`
	}{Index: 0, Delta: map[string]string{}}
	if content != "" {
		choice.Delta["content"] = content
	}
	if finishReason != "" {
		choice.FinishReason = &finishReason
	}
	chunk.Choices = append(chunk.Choices, choice)
	encoded, err := json.Marshal(chunk)
	if err != nil {
		panic(fmt.Sprintf("encode failover fixture: %v", err))
	}
	return string(encoded)
}

func startFailoverBackendSSE(writer http.ResponseWriter) {
	writer.Header().Set("Content-Type", "text/event-stream")
	writer.WriteHeader(http.StatusOK)
	if flusher, ok := writer.(http.Flusher); ok {
		flusher.Flush()
	}
}

func writeFailoverBackendData(writer http.ResponseWriter, data string) bool {
	if _, err := io.WriteString(writer, "data: "+data+"\n\n"); err != nil {
		return false
	}
	if flusher, ok := writer.(http.Flusher); ok {
		flusher.Flush()
	}
	return true
}

func newFailoverBackend(
	t *testing.T,
	id string,
	serverURL string,
	modelVersion string,
	templateVerdict conformance.Verdict,
) backend.Backend {
	t.Helper()
	parsed, err := url.Parse(serverURL)
	if err != nil {
		t.Fatalf("url.Parse(%q) error = %v", serverURL, err)
	}
	return backend.Backend{
		ID:              backend.ID(id),
		URL:             parsed,
		ModelVersion:    modelVersion,
		TemplateVerdict: templateVerdict,
	}
}

func newFailoverBackendPool(t *testing.T, backends ...backend.Backend) *backend.Pool {
	t.Helper()
	config := backend.DefaultConfig()
	config.Choose = func([]backend.State) int { return 0 }
	pool, err := backend.NewPool(config, backends...)
	if err != nil {
		t.Fatalf("backend.NewPool() error = %v", err)
	}
	for _, candidate := range backends {
		if _, err := pool.SetHealth(candidate.ID, backend.HealthHealthy); err != nil {
			t.Fatalf("pool.SetHealth(%q) error = %v", candidate.ID, err)
		}
	}
	return pool
}

type failoverHTTPHarness struct {
	server *Server
	url    string
	client *http.Client
}

func newFailoverHTTPHarness(
	t *testing.T,
	backendURL string,
	pool *backend.Pool,
	configure func(*Config),
) *failoverHTTPHarness {
	t.Helper()
	config := DefaultConfig()
	config.BackendURL = backendURL
	config.ListenAddress = "127.0.0.1:0"
	config.ReadinessTimeout = time.Second
	if configure != nil {
		configure(&config)
	}
	server, err := NewServer(
		config,
		nil,
		WithBackendPool(pool),
		WithStreamIDGenerator(&failoverSequentialIDs{}),
	)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
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

type failoverSequentialIDs struct {
	next atomic.Uint64
}

func (ids *failoverSequentialIDs) New() (journal.StreamID, error) {
	return journal.ParseStreamID(fmt.Sprintf("%026d", ids.next.Add(1)))
}

func newFailoverHTTPRequest(t *testing.T, method, target, body string) *http.Request {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), failoverHTTPTestTimeout)
	t.Cleanup(cancel)
	request, err := http.NewRequestWithContext(ctx, method, target, strings.NewReader(body))
	if err != nil {
		t.Fatalf("http.NewRequestWithContext() error = %v", err)
	}
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	return request
}

func doFailoverHTTPRequest(t *testing.T, client *http.Client, request *http.Request) *http.Response {
	t.Helper()
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("client.Do(%s %s) error = %v", request.Method, request.URL, err)
	}
	return response
}

func readAllFailoverSSE(t *testing.T, reader io.Reader) []sse.Event {
	t.Helper()
	return readRemainingFailoverSSE(t, sse.NewDecoder(reader))
}

func readRemainingFailoverSSE(t *testing.T, decoder *sse.Decoder) []sse.Event {
	t.Helper()
	events := make([]sse.Event, 0, 12)
	for len(events) < 64 {
		event, err := decoder.Decode()
		if errors.Is(err, io.EOF) {
			return events
		}
		if err != nil {
			t.Fatalf("SSE Decode() error = %v after %d events", err, len(events))
		}
		events = append(events, event)
	}
	t.Fatalf("SSE stream exceeded 64 events without closing")
	return nil
}

func readNextFailoverSSE(t *testing.T, decoder *sse.Decoder) sse.Event {
	t.Helper()
	event, err := decoder.Decode()
	if err != nil {
		t.Fatalf("SSE Decode() error = %v", err)
	}
	return event
}

func requireFailoverContinuationRequest(t *testing.T, body []byte) {
	t.Helper()
	var fields map[string]json.RawMessage
	decodeFailoverJSON(t, body, &fields)
	for name, want := range map[string]string{
		"model":                  `"test-model"`,
		"stream":                 `true`,
		"continue_final_message": `true`,
		"add_generation_prompt":  `false`,
		"max_tokens":             `6`,
		"temperature":            `0.25`,
		"seed":                   `7123`,
		"vendor_extension":       `{"preserve":"exactly"}`,
	} {
		got, ok := fields[name]
		if !ok || string(got) != want {
			t.Errorf("continuation field %q = %s (present %v), want %s", name, got, ok, want)
		}
	}
	var messages []struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	}
	decodeFailoverJSON(t, fields["messages"], &messages)
	if len(messages) != 2 {
		t.Fatalf("continuation messages = %+v, want original plus assistant prefill", messages)
	}
	last := messages[len(messages)-1]
	if last.Role != "assistant" || last.Content != "The quick brown " {
		t.Fatalf("continuation assistant prefill = %+v", last)
	}
}

func requireFailoverSequence(t *testing.T, events []sse.Event) {
	t.Helper()
	want := uint64(1)
	for _, event := range events {
		if !event.HasID {
			continue
		}
		got, err := strconv.ParseUint(event.ID, 10, 64)
		if err != nil {
			t.Fatalf("SSE event ID %q is not a sequence: %v", event.ID, err)
		}
		if got != want {
			t.Fatalf("SSE event ID = %d, want contiguous sequence %d: %#v", got, want, events)
		}
		want++
	}
}

func requireFailoverDone(t *testing.T, events []sse.Event) {
	t.Helper()
	if len(events) < 2 {
		t.Fatalf("events = %#v, want terminal done and compatibility sentinel", events)
	}
	doneIndex := failoverEventIndex(events, streamDoneEvent)
	if doneIndex < 0 {
		t.Fatalf("events contain no done control event: %#v", events)
	}
	last := events[len(events)-1]
	if last.HasID || last.HasType || !last.HasData || string(last.Data) != doneSentinelData {
		t.Fatalf("last event = %#v, want unsequenced [DONE]", last)
	}
	if doneIndex != len(events)-2 {
		t.Fatalf("done event index = %d, want immediately before [DONE]: %#v", doneIndex, events)
	}
}

func requireFailoverRefusal(t *testing.T, events []sse.Event, predicate, warningCode string) {
	t.Helper()
	if len(events) == 0 {
		t.Fatal("refused stream returned no events")
	}
	foundWarning := false
	for _, event := range events {
		if !event.HasType || event.Type != streamWarningEvent {
			continue
		}
		var warning struct {
			Code      string  `json:"code"`
			Predicate *string `json:"predicate"`
		}
		decodeFailoverJSON(t, event.Data, &warning)
		predicateMatches := predicate == "" || (warning.Predicate != nil && *warning.Predicate == predicate)
		codeMatches := warningCode == "" || warning.Code == warningCode
		if predicateMatches && codeMatches {
			foundWarning = true
		}
	}
	if !foundWarning {
		t.Fatalf("events contain no matching refusal warning (predicate %q, code %q): %#v", predicate, warningCode, events)
	}
	errorIndex := failoverEventIndex(events, streamErrorEvent)
	if errorIndex < 0 {
		t.Fatalf("events contain no terminal error: %#v", events)
	}
	var terminal struct {
		Code      string `json:"code"`
		Reason    string `json:"reason"`
		Retriable bool   `json:"retriable"`
	}
	decodeFailoverJSON(t, events[errorIndex].Data, &terminal)
	if terminal.Code != "migration_refused" || terminal.Retriable {
		t.Fatalf("terminal refusal = %+v", terminal)
	}
	if warningCode != "" && terminal.Reason != warningCode {
		t.Fatalf("terminal refusal reason = %q, want %q", terminal.Reason, warningCode)
	}
	if errorIndex != len(events)-1 {
		t.Fatalf("terminal error is not the final event: %#v", events)
	}
	for _, event := range events {
		if !event.HasID && string(event.Data) == doneSentinelData {
			t.Fatalf("refused stream emitted [DONE]: %#v", events)
		}
	}
}

func failoverEventIndex(events []sse.Event, eventType string) int {
	for index, event := range events {
		if event.HasType && event.Type == eventType {
			return index
		}
	}
	return -1
}

func failoverText(events []sse.Event) string {
	var result strings.Builder
	for _, event := range events {
		if event.HasType || !event.HasID || !event.HasData {
			continue
		}
		var chunk struct {
			Choices []struct {
				Index int     `json:"index"`
				Text  *string `json:"text"`
				Delta struct {
					Content *string `json:"content"`
				} `json:"delta"`
			} `json:"choices"`
		}
		if json.Unmarshal(event.Data, &chunk) != nil {
			continue
		}
		for _, choice := range chunk.Choices {
			if choice.Index != 0 {
				continue
			}
			if choice.Text != nil {
				result.WriteString(*choice.Text)
			}
			if choice.Delta.Content != nil {
				result.WriteString(*choice.Delta.Content)
			}
		}
	}
	return result.String()
}

func failoverChunkText(t *testing.T, event sse.Event) string {
	t.Helper()
	if event.HasType || !event.HasID || !event.HasData {
		t.Fatalf("event is not a sequenced chunk: %#v", event)
	}
	return failoverText([]sse.Event{event})
}

func decodeFailoverJSON(t *testing.T, data []byte, destination any) {
	t.Helper()
	if err := json.Unmarshal(data, destination); err != nil {
		t.Fatalf("json.Unmarshal(%s) error = %v", data, err)
	}
}

func closeFailoverBody(t *testing.T, body io.Closer) {
	t.Helper()
	if err := body.Close(); err != nil {
		t.Errorf("response body Close() error = %v", err)
	}
}

func readFailoverBody(t *testing.T, body io.Reader) []byte {
	t.Helper()
	data, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("io.ReadAll(response) error = %v", err)
	}
	return data
}

func awaitFailoverValue[T any](t *testing.T, values <-chan T, description string) T {
	t.Helper()
	select {
	case value := <-values:
		return value
	case <-time.After(failoverHTTPTestTimeout):
		t.Fatalf("timed out waiting for %s", description)
		var zero T
		return zero
	}
}

func awaitFailoverSignal(t *testing.T, signal <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(failoverHTTPTestTimeout):
		t.Fatalf("timed out waiting for %s", description)
	}
}
