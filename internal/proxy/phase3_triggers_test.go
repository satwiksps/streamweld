package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/streamweld/streamweld/internal/conformance"
	"github.com/streamweld/streamweld/internal/proxy/sse"
)

func TestFailoverHTTPExternalHealthFailureMigratesBoundStream(t *testing.T) {
	var failHealth atomic.Bool
	originCanceled := make(chan struct{}, 1)
	originServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/health" {
			if failHealth.Load() {
				http.Error(writer, "unhealthy", http.StatusServiceUnavailable)
				return
			}
			writer.WriteHeader(http.StatusOK)
			return
		}
		startFailoverBackendSSE(writer)
		if !writeFailoverBackendData(writer, failoverChatChunk(
			"origin-health", "health ", "", &failoverUsage{PromptTokens: 1, CompletionTokens: 1, TotalTokens: 2},
		)) {
			return
		}
		<-request.Context().Done()
		originCanceled <- struct{}{}
	}))
	t.Cleanup(originServer.Close)

	targetServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/health" {
			writer.WriteHeader(http.StatusOK)
			return
		}
		writeTriggerTarget(writer, "target-health", "health recovered", 2)
	}))
	t.Cleanup(targetServer.Close)

	origin := newFailoverBackend(t, "origin-health.test:8000", originServer.URL, "model-v1", conformance.VerdictSafe)
	target := newFailoverBackend(t, "target-health.test:8000", targetServer.URL, "model-v1", conformance.VerdictSafe)
	pool := newFailoverBackendPool(t, origin, target)
	harness := newFailoverHTTPHarness(t, originServer.URL, pool, func(config *Config) {
		config.BackendHealthInterval = 10 * time.Millisecond
		config.SeamWindowBytes = len("health ")
	})

	request := newFailoverHTTPRequest(t, http.MethodPost, harness.url+"/v1/chat/completions",
		`{"model":"test-model","stream":true,"messages":[{"role":"user","content":"health"}],"max_tokens":10}`)
	request.Header.Set(headerVerbose, "1")
	response := doFailoverHTTPRequest(t, harness.client, request)
	defer closeFailoverBody(t, response.Body)
	decoder := sse.NewDecoder(response.Body)
	initial := []sse.Event{readNextFailoverSSE(t, decoder), readNextFailoverSSE(t, decoder)}
	if got := failoverChunkText(t, initial[1]); got != "health " {
		t.Fatalf("origin text = %q, want health prefix", got)
	}

	failHealth.Store(true)
	healthContext, cancelHealth := context.WithCancel(context.Background())
	t.Cleanup(cancelHealth)
	go harness.server.durable.runHealthChecks(healthContext)

	events := initial
	events = append(events, readRemainingFailoverSSE(t, decoder)...)
	requireFailoverDone(t, events)
	requireTriggerMigration(t, events, "health", 2)
	if got := failoverText(events); got != "health recovered" {
		t.Fatalf("assembled health migration text = %q", got)
	}
	awaitFailoverSignal(t, originCanceled, "health-failed request cancellation")
}

func TestFailoverHTTPUpstreamFailureTriggers(t *testing.T) {
	tests := []struct {
		name       string
		reason     string
		originText string
		targetText string
		serve      func(http.ResponseWriter)
	}{
		{
			name:       "5xx before stream",
			reason:     "upstream_5xx",
			targetText: "recovered",
			serve: func(writer http.ResponseWriter) {
				http.Error(writer, "backend unavailable", http.StatusServiceUnavailable)
			},
		},
		{
			name:       "error chunk",
			reason:     "error_chunk",
			originText: "partial ",
			targetText: "partial recovered",
			serve: func(writer http.ResponseWriter) {
				startFailoverBackendSSE(writer)
				writeFailoverBackendData(writer, failoverChatChunk(
					"origin-error", "partial ", "", &failoverUsage{PromptTokens: 1, CompletionTokens: 1, TotalTokens: 2},
				))
				writeFailoverBackendData(writer, `{"error":{"message":"backend failed"}}`)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			originServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				test.serve(writer)
			}))
			t.Cleanup(originServer.Close)
			var targetCalls atomic.Int64
			targetServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				targetCalls.Add(1)
				writeTriggerTarget(writer, "target-trigger", test.targetText, 2)
			}))
			t.Cleanup(targetServer.Close)

			origin := newFailoverBackend(t, "origin-trigger.test:8000", originServer.URL, "model-v1", conformance.VerdictSafe)
			target := newFailoverBackend(t, "target-trigger.test:8000", targetServer.URL, "model-v1", conformance.VerdictSafe)
			harness := newFailoverHTTPHarness(t, originServer.URL, newFailoverBackendPool(t, origin, target), func(config *Config) {
				config.SeamWindowBytes = max(1, len(test.originText))
			})

			request := newFailoverHTTPRequest(t, http.MethodPost, harness.url+"/v1/chat/completions",
				`{"model":"test-model","stream":true,"messages":[{"role":"user","content":"recover"}],"max_tokens":10}`)
			request.Header.Set(headerVerbose, "1")
			response := doFailoverHTTPRequest(t, harness.client, request)
			defer closeFailoverBody(t, response.Body)
			if response.StatusCode != http.StatusOK {
				t.Fatalf("completion status = %d, body = %q", response.StatusCode, readFailoverBody(t, response.Body))
			}
			events := readAllFailoverSSE(t, response.Body)
			requireFailoverDone(t, events)
			requireTriggerMigration(t, events, test.reason, 2)
			if got := failoverText(events); got != test.targetText {
				t.Fatalf("assembled trigger text = %q, want %q", got, test.targetText)
			}
			if got := targetCalls.Load(); got != 1 {
				t.Fatalf("continuation calls = %d, want 1", got)
			}
		})
	}
}

func TestFailoverHTTPEnabledStallDetectionMigrates(t *testing.T) {
	originCanceled := make(chan struct{}, 1)
	originServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		startFailoverBackendSSE(writer)
		if !writeFailoverBackendData(writer, failoverChatChunk(
			"origin-stall", "stall ", "", &failoverUsage{PromptTokens: 1, CompletionTokens: 1, TotalTokens: 2},
		)) {
			return
		}
		<-request.Context().Done()
		originCanceled <- struct{}{}
	}))
	t.Cleanup(originServer.Close)
	targetServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writeTriggerTarget(writer, "target-stall", "stall recovered", 2)
	}))
	t.Cleanup(targetServer.Close)

	origin := newFailoverBackend(t, "origin-stall.test:8000", originServer.URL, "model-v1", conformance.VerdictSafe)
	target := newFailoverBackend(t, "target-stall.test:8000", targetServer.URL, "model-v1", conformance.VerdictSafe)
	harness := newFailoverHTTPHarness(t, originServer.URL, newFailoverBackendPool(t, origin, target), func(config *Config) {
		config.StallDetectionEnabled = true
		config.StallTimeout = 40 * time.Millisecond
		config.SeamWindowBytes = len("stall ")
	})

	request := newFailoverHTTPRequest(t, http.MethodPost, harness.url+"/v1/chat/completions",
		`{"model":"test-model","stream":true,"messages":[{"role":"user","content":"stall"}],"max_tokens":10}`)
	request.Header.Set(headerVerbose, "1")
	response := doFailoverHTTPRequest(t, harness.client, request)
	defer closeFailoverBody(t, response.Body)
	events := readAllFailoverSSE(t, response.Body)
	requireFailoverDone(t, events)
	requireTriggerMigration(t, events, "stall", 2)
	if got := failoverText(events); got != "stall recovered" {
		t.Fatalf("assembled stalled stream text = %q", got)
	}
	awaitFailoverSignal(t, originCanceled, "stalled request cancellation")
}

func TestFailoverHTTPDrainTimeoutIsReportedAndRetryIsIdempotent(t *testing.T) {
	backendServer := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(backendServer.Close)
	origin := newFailoverBackend(t, "held-drain.test:8000", backendServer.URL, "model-v1", conformance.VerdictSafe)
	pool := newFailoverBackendPool(t, origin)
	lease, err := pool.Acquire("external-request")
	if err != nil {
		t.Fatalf("pool.Acquire() error = %v", err)
	}
	t.Cleanup(lease.Release)
	harness := newFailoverHTTPHarness(t, backendServer.URL, pool, nil)
	drainURL := harness.url + "/internal/backends/" + url.PathEscape(origin.ID.String()) + "/drain"

	first := newFailoverHTTPRequest(t, http.MethodPost, drainURL+"?timeout=20ms", "")
	firstResponse := doFailoverHTTPRequest(t, harness.client, first)
	defer closeFailoverBody(t, firstResponse.Body)
	firstBody := readFailoverBody(t, firstResponse.Body)
	if firstResponse.StatusCode != http.StatusGatewayTimeout {
		t.Fatalf("first drain status = %d, body = %q", firstResponse.StatusCode, firstBody)
	}
	var firstResult drainResponse
	decodeFailoverJSON(t, firstBody, &firstResult)
	if firstResult.Backend != origin.ID.String() || firstResult.State != "draining" || firstResult.InFlight != 1 {
		t.Fatalf("timed-out drain response = %+v", firstResult)
	}

	lease.Release()
	second := newFailoverHTTPRequest(t, http.MethodPost, drainURL+"?timeout=100ms", "")
	secondResponse := doFailoverHTTPRequest(t, harness.client, second)
	defer closeFailoverBody(t, secondResponse.Body)
	secondBody := readFailoverBody(t, secondResponse.Body)
	if secondResponse.StatusCode != http.StatusOK {
		t.Fatalf("idempotent drain status = %d, body = %q", secondResponse.StatusCode, secondBody)
	}
	var secondResult drainResponse
	decodeFailoverJSON(t, secondBody, &secondResult)
	if secondResult.Backend != origin.ID.String() || secondResult.State != "draining" || secondResult.InFlight != 0 {
		t.Fatalf("idempotent drain response = %+v", secondResult)
	}
}

func TestFailoverHTTPStructuredPrefixGate(t *testing.T) {
	tests := []struct {
		name           string
		originText     string
		targetText     string
		wantText       string
		wantMigration  bool
		wantTargetCall int64
	}{
		{name: "valid prefix", originText: `{"answer":`, targetText: `{"answer":42}`, wantText: `{"answer":42}`, wantMigration: true, wantTargetCall: 1},
		{name: "invalid prefix", originText: `{"answer":}`, wantText: `{"answer":}`, wantMigration: false, wantTargetCall: 0},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			originServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				startFailoverBackendSSE(writer)
				writeFailoverBackendData(writer, failoverChatChunk(
					"origin-structured", test.originText, "", &failoverUsage{PromptTokens: 1, CompletionTokens: 1, TotalTokens: 2},
				))
			}))
			t.Cleanup(originServer.Close)
			var targetCalls atomic.Int64
			targetServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				targetCalls.Add(1)
				writeTriggerTarget(writer, "target-structured", test.targetText, 2)
			}))
			t.Cleanup(targetServer.Close)

			origin := newFailoverBackend(t, "origin-structured.test:8000", originServer.URL, "model-v1", conformance.VerdictSafe)
			target := newFailoverBackend(t, "target-structured.test:8000", targetServer.URL, "model-v1", conformance.VerdictSafe)
			harness := newFailoverHTTPHarness(t, originServer.URL, newFailoverBackendPool(t, origin, target), func(config *Config) {
				config.AllowStructuredResume = true
				config.SeamWindowBytes = len(test.originText)
			})

			request := newFailoverHTTPRequest(t, http.MethodPost, harness.url+"/v1/chat/completions",
				`{"model":"test-model","stream":true,"messages":[{"role":"user","content":"json"}],"response_format":{"type":"json_object"},"max_tokens":10}`)
			request.Header.Set(headerVerbose, "1")
			response := doFailoverHTTPRequest(t, harness.client, request)
			defer closeFailoverBody(t, response.Body)
			events := readAllFailoverSSE(t, response.Body)
			if got := failoverText(events); got != test.wantText {
				t.Fatalf("structured output = %q, want %q", got, test.wantText)
			}
			if test.wantMigration {
				requireFailoverDone(t, events)
				requireTriggerMigration(t, events, "unexpected_eof", 2)
			} else {
				requireFailoverRefusal(t, events, "", "structured_prefix_invalid")
				if failoverEventIndex(events, streamMigrationEvent) >= 0 {
					t.Fatalf("invalid structured prefix emitted a migration: %#v", events)
				}
			}
			if got := targetCalls.Load(); got != test.wantTargetCall {
				t.Fatalf("structured continuation calls = %d, want %d", got, test.wantTargetCall)
			}
		})
	}
}

func TestFailoverHTTPCrossVersionOptInMigrates(t *testing.T) {
	events := runPolicyOptInMigration(t, "model-v1", "model-v2", conformance.VerdictSafe, func(config *Config) {
		config.AllowCrossVersion = true
	})
	requireFailoverDone(t, events)
	requireTriggerMigration(t, events, "unexpected_eof", 2)
	if got := failoverText(events); got != "policy recovered" {
		t.Fatalf("cross-version output = %q", got)
	}
}

func TestFailoverHTTPPermissiveUnsafeTargetEmitsLoudWarning(t *testing.T) {
	events := runPolicyOptInMigration(t, "model-v1", "model-v1", conformance.VerdictUnsafe, func(config *Config) {
		config.TemplateMode = conformance.TemplatePermissive
	})
	requireFailoverDone(t, events)
	migrationIndex := failoverEventIndex(events, streamMigrationEvent)
	warningIndex := triggerWarningIndex(t, events, "template_unsafe_permissive")
	if warningIndex < 0 || migrationIndex < 0 || warningIndex >= migrationIndex {
		t.Fatalf("unsafe-template warning must precede migration: warning=%d migration=%d events=%#v", warningIndex, migrationIndex, events)
	}
	if got := failoverText(events); got != "policy recovered" {
		t.Fatalf("permissive unsafe output = %q", got)
	}
}

func TestFailoverHTTPMultipleMigrationsUseMonotonicAttemptNumbers(t *testing.T) {
	originServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		startFailoverBackendSSE(writer)
		writeFailoverBackendData(writer, failoverChatChunk(
			"attempt-one", "A", "", &failoverUsage{PromptTokens: 1, CompletionTokens: 1, TotalTokens: 2},
		))
	}))
	t.Cleanup(originServer.Close)
	middleServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		startFailoverBackendSSE(writer)
		writeFailoverBackendData(writer, failoverChatChunk(
			"attempt-two", "AB", "", &failoverUsage{PromptTokens: 1, CompletionTokens: 2, TotalTokens: 3},
		))
	}))
	t.Cleanup(middleServer.Close)
	finalServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writeTriggerTarget(writer, "attempt-three", "ABC", 3)
	}))
	t.Cleanup(finalServer.Close)

	origin := newFailoverBackend(t, "a-origin-attempt.test:8000", originServer.URL, "model-v1", conformance.VerdictSafe)
	middle := newFailoverBackend(t, "b-middle-attempt.test:8000", middleServer.URL, "model-v1", conformance.VerdictSafe)
	final := newFailoverBackend(t, "c-final-attempt.test:8000", finalServer.URL, "model-v1", conformance.VerdictSafe)
	harness := newFailoverHTTPHarness(t, originServer.URL, newFailoverBackendPool(t, origin, middle, final), func(config *Config) {
		config.SeamWindowBytes = 2
	})

	request := newFailoverHTTPRequest(t, http.MethodPost, harness.url+"/v1/chat/completions",
		`{"model":"test-model","stream":true,"messages":[{"role":"user","content":"alphabet"}],"max_tokens":10}`)
	request.Header.Set(headerVerbose, "1")
	response := doFailoverHTTPRequest(t, harness.client, request)
	defer closeFailoverBody(t, response.Body)
	events := readAllFailoverSSE(t, response.Body)
	requireFailoverDone(t, events)
	if got := failoverText(events); got != "ABC" {
		t.Fatalf("multi-migration output = %q, want ABC", got)
	}

	type migrationPayload struct {
		FromBackend string `json:"from_backend"`
		ToBackend   string `json:"to_backend"`
		Reason      string `json:"reason"`
		Attempt     uint64 `json:"attempt"`
	}
	migrations := make([]migrationPayload, 0, 2)
	for _, event := range events {
		if event.HasType && event.Type == streamMigrationEvent {
			var payload migrationPayload
			decodeFailoverJSON(t, event.Data, &payload)
			migrations = append(migrations, payload)
		}
	}
	want := []migrationPayload{
		{FromBackend: origin.ID.String(), ToBackend: middle.ID.String(), Reason: "unexpected_eof", Attempt: 2},
		{FromBackend: middle.ID.String(), ToBackend: final.ID.String(), Reason: "unexpected_eof", Attempt: 3},
	}
	if len(migrations) != len(want) {
		t.Fatalf("migration count = %d, want %d: %+v", len(migrations), len(want), migrations)
	}
	for index := range want {
		if migrations[index] != want[index] {
			t.Fatalf("migration %d = %+v, want %+v", index, migrations[index], want[index])
		}
	}
}

func TestFailoverHTTPStopWinsOrSerializesWithMigration(t *testing.T) {
	originServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		startFailoverBackendSSE(writer)
		if !writeFailoverBackendData(writer, failoverChatChunk(
			"origin-race", "race ", "", &failoverUsage{PromptTokens: 1, CompletionTokens: 1, TotalTokens: 2},
		)) {
			return
		}
		<-request.Context().Done()
	}))
	t.Cleanup(originServer.Close)
	targetServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		startFailoverBackendSSE(writer)
		<-request.Context().Done()
	}))
	t.Cleanup(targetServer.Close)

	origin := newFailoverBackend(t, "origin-race.test:8000", originServer.URL, "model-v1", conformance.VerdictSafe)
	target := newFailoverBackend(t, "target-race.test:8000", targetServer.URL, "model-v1", conformance.VerdictSafe)
	harness := newFailoverHTTPHarness(t, originServer.URL, newFailoverBackendPool(t, origin, target), nil)
	streamRequest := newFailoverHTTPRequest(t, http.MethodPost, harness.url+"/v1/chat/completions",
		`{"model":"test-model","stream":true,"messages":[{"role":"user","content":"race"}],"max_tokens":10}`)
	streamRequest.Header.Set(headerVerbose, "1")
	streamResponse := doFailoverHTTPRequest(t, harness.client, streamRequest)
	defer closeFailoverBody(t, streamResponse.Body)
	decoder := sse.NewDecoder(streamResponse.Body)
	initial := []sse.Event{readNextFailoverSSE(t, decoder), readNextFailoverSSE(t, decoder)}
	streamID := streamResponse.Header.Get(headerStreamID)
	if streamID == "" {
		t.Fatal("durable response omitted stream ID")
	}

	type httpResult struct {
		status int
		body   []byte
		err    error
	}
	start := make(chan struct{})
	results := make(chan httpResult, 2)
	requestURLs := []string{
		harness.url + "/internal/backends/" + url.PathEscape(origin.ID.String()) + "/drain?timeout=1s",
		harness.url + "/v1/streams/" + streamID + "/stop",
	}
	for _, requestURL := range requestURLs {
		requestURL := requestURL
		go func() {
			<-start
			request, err := http.NewRequestWithContext(context.Background(), http.MethodPost, requestURL, nil)
			if err != nil {
				results <- httpResult{err: err}
				return
			}
			response, err := harness.client.Do(request)
			if err != nil {
				results <- httpResult{err: err}
				return
			}
			body, readErr := io.ReadAll(response.Body)
			closeErr := response.Body.Close()
			results <- httpResult{status: response.StatusCode, body: body, err: errors.Join(readErr, closeErr)}
		}()
	}
	close(start)
	statuses := map[int]int{}
	for range 2 {
		result := awaitFailoverValue(t, results, "stop/drain race response")
		if result.err != nil {
			t.Fatalf("stop/drain race request error = %v", result.err)
		}
		statuses[result.status]++
	}
	if statuses[http.StatusAccepted] != 1 || statuses[http.StatusOK] != 1 {
		t.Fatalf("stop/drain statuses = %+v, want one 202 and one 200", statuses)
	}

	events := initial
	events = append(events, readRemainingFailoverSSE(t, decoder)...)
	terminalCount := 0
	terminalIndex := -1
	for index, event := range events {
		if event.HasType && (event.Type == streamStoppedEvent || event.Type == streamDoneEvent || event.Type == streamErrorEvent) {
			terminalCount++
			terminalIndex = index
			if event.Type != streamStoppedEvent {
				t.Fatalf("race terminal event = %q, want stopped", event.Type)
			}
		}
	}
	if terminalCount != 1 || terminalIndex != len(events)-1 {
		t.Fatalf("race terminal count/index = %d/%d for %d events: %#v", terminalCount, terminalIndex, len(events), events)
	}
}

func runPolicyOptInMigration(
	t *testing.T,
	originVersion string,
	targetVersion string,
	targetVerdict conformance.Verdict,
	configure func(*Config),
) []sse.Event {
	t.Helper()
	originServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		startFailoverBackendSSE(writer)
		writeFailoverBackendData(writer, failoverChatChunk(
			"origin-policy", "policy ", "", &failoverUsage{PromptTokens: 1, CompletionTokens: 1, TotalTokens: 2},
		))
	}))
	t.Cleanup(originServer.Close)
	targetServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writeTriggerTarget(writer, "target-policy", "policy recovered", 2)
	}))
	t.Cleanup(targetServer.Close)
	origin := newFailoverBackend(t, "origin-policy.test:8000", originServer.URL, originVersion, conformance.VerdictSafe)
	target := newFailoverBackend(t, "target-policy.test:8000", targetServer.URL, targetVersion, targetVerdict)
	harness := newFailoverHTTPHarness(t, originServer.URL, newFailoverBackendPool(t, origin, target), func(config *Config) {
		config.SeamWindowBytes = len("policy ")
		if configure != nil {
			configure(config)
		}
	})
	request := newFailoverHTTPRequest(t, http.MethodPost, harness.url+"/v1/chat/completions",
		`{"model":"test-model","stream":true,"messages":[{"role":"user","content":"policy"}],"max_tokens":10}`)
	request.Header.Set(headerVerbose, "1")
	response := doFailoverHTTPRequest(t, harness.client, request)
	t.Cleanup(func() { closeFailoverBody(t, response.Body) })
	return readAllFailoverSSE(t, response.Body)
}

func writeTriggerTarget(writer http.ResponseWriter, id, text string, completionTokens uint64) {
	startFailoverBackendSSE(writer)
	if !writeFailoverBackendData(writer, failoverChatChunk(
		id, text, "", &failoverUsage{PromptTokens: 1, CompletionTokens: completionTokens, TotalTokens: completionTokens + 1},
	)) {
		return
	}
	if !writeFailoverBackendData(writer, failoverChatChunk(id, "", "stop", nil)) {
		return
	}
	writeFailoverBackendData(writer, doneSentinelData)
}

func requireTriggerMigration(t *testing.T, events []sse.Event, reason string, attempt uint64) {
	t.Helper()
	index := failoverEventIndex(events, streamMigrationEvent)
	if index < 0 {
		t.Fatalf("events contain no migration: %#v", events)
	}
	var payload struct {
		Reason  string `json:"reason"`
		Attempt uint64 `json:"attempt"`
	}
	decodeFailoverJSON(t, events[index].Data, &payload)
	if payload.Reason != reason || payload.Attempt != attempt {
		t.Fatalf("migration = %+v, want reason %q attempt %d", payload, reason, attempt)
	}
}

func triggerWarningIndex(t *testing.T, events []sse.Event, code string) int {
	t.Helper()
	for index, event := range events {
		if !event.HasType || event.Type != streamWarningEvent {
			continue
		}
		var payload struct {
			Code string `json:"code"`
		}
		if err := json.Unmarshal(event.Data, &payload); err != nil {
			t.Fatalf("decode warning payload: %v", err)
		}
		if payload.Code == code {
			return index
		}
	}
	return -1
}
