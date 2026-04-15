package proxy

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/streamweld/streamweld/internal/backend"
	"github.com/streamweld/streamweld/internal/conformance"
	"github.com/streamweld/streamweld/internal/journal"
	"github.com/streamweld/streamweld/internal/telemetry"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestObservabilityRecordsRealFailoverResumeAndGenAISpans(t *testing.T) {
	originTraceparent := make(chan string, 1)
	targetTraceparent := make(chan string, 1)
	originServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		originTraceparent <- request.Header.Get("traceparent")
		startFailoverBackendSSE(writer)
		writeFailoverBackendData(writer, failoverChatChunk(
			"origin", "The quick brown ", "",
			&failoverUsage{PromptTokens: 3, CompletionTokens: 4, TotalTokens: 7},
		))
	}))
	t.Cleanup(originServer.Close)
	targetServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		targetTraceparent <- request.Header.Get("traceparent")
		startFailoverBackendSSE(writer)
		if !writeFailoverBackendData(writer, failoverChatChunk("target", "brown fox", "", nil)) {
			return
		}
		if !writeFailoverBackendData(writer, failoverChatChunk(
			"target", "", "stop",
			&failoverUsage{PromptTokens: 5, CompletionTokens: 5, TotalTokens: 10},
		)) {
			return
		}
		writeFailoverBackendData(writer, doneSentinelData)
	}))
	t.Cleanup(targetServer.Close)

	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	recorder, err := telemetry.New(nil, nil, provider)
	if err != nil {
		t.Fatalf("telemetry.New() error = %v", err)
	}

	origin := newFailoverBackend(t, "origin.telemetry.test:8000", originServer.URL, "model-v1", conformance.VerdictSafe)
	target := newFailoverBackend(t, "target.telemetry.test:8000", targetServer.URL, "model-v1", conformance.VerdictSafe)
	pool := newFailoverBackendPool(t, origin, target)
	config := DefaultConfig()
	config.BackendURL = originServer.URL
	config.ListenAddress = "127.0.0.1:0"
	config.ReadinessTimeout = time.Second
	config.SeamWindowBytes = 8
	var logBuffer bytes.Buffer
	server, err := NewServer(
		config,
		slog.New(slog.NewJSONHandler(&logBuffer, nil)),
		WithBackendPool(pool),
		WithStreamIDGenerator(&failoverSequentialIDs{}),
		WithTelemetry(recorder),
	)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	applyObservedRoute(t, server, origin, target)

	front := httptest.NewServer(server.Handler())
	t.Cleanup(func() {
		server.forceCancel()
		front.Close()
	})
	client := &http.Client{Timeout: failoverHTTPTestTimeout}
	request := newFailoverHTTPRequest(t, http.MethodPost, front.URL+"/v1/chat/completions", `{
		"model":"test-model",
		"stream":true,
		"messages":[{"role":"user","content":"finish"}],
		"max_tokens":10
	}`)
	const incomingTraceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	const incomingSpanID = "00f067aa0ba902b7"
	request.Header.Set("traceparent", "00-"+incomingTraceID+"-"+incomingSpanID+"-01")
	response := doFailoverHTTPRequest(t, client, request)
	defer closeFailoverBody(t, response.Body)
	streamID := response.Header.Get(headerStreamID)
	events := readAllFailoverSSE(t, response.Body)
	closeFailoverBody(t, response.Body)
	if got := failoverText(events); got != "The quick brown fox" {
		t.Fatalf("assembled stream = %q", got)
	}

	resume := newFailoverHTTPRequest(t, http.MethodGet, front.URL+"/v1/streams/"+streamID+"/events", "")
	resumeResponse := doFailoverHTTPRequest(t, client, resume)
	defer closeFailoverBody(t, resumeResponse.Body)
	if resumeResponse.StatusCode != http.StatusOK {
		t.Fatalf("resume status = %d", resumeResponse.StatusCode)
	}
	_ = readAllFailoverSSE(t, resumeResponse.Body)
	closeFailoverBody(t, resumeResponse.Body)

	metricsResponse := doFailoverHTTPRequest(t, client,
		newFailoverHTTPRequest(t, http.MethodGet, front.URL+"/metrics", ""))
	defer closeFailoverBody(t, metricsResponse.Body)
	metricsBody := string(readFailoverBody(t, metricsResponse.Body))
	closeFailoverBody(t, metricsResponse.Body)
	labels := map[string]string{"route": "team-a/chat", "model": "test-model"}
	requireObservedMetric(t, metricsBody, "streamweld_streams_active", labels, 0)
	requireObservedMetric(t, metricsBody, "streamweld_streams_total", mergeObservedLabels(labels, "outcome", "done"), 1)
	requireObservedMetric(t, metricsBody, "streamweld_migrations_total", mergeObservedLabels(labels, "reason", "crash"), 1)
	requireObservedMetric(t, metricsBody, "streamweld_tokens_rescued_total", labels, 4)
	requireObservedMetric(t, metricsBody, "streamweld_prompt_tokens_rebilled_total", labels, 5)
	requireObservedMetric(t, metricsBody, "streamweld_resumes_total", mergeObservedLabels(labels, "trigger", "client"), 1)
	requireObservedMetric(t, metricsBody, "streamweld_resumes_total", mergeObservedLabels(labels, "trigger", "failover"), 1)
	requireObservedMetric(t, metricsBody, "streamweld_seam_overlap_bytes_sum", labels, 6)
	requireObservedMetric(t, metricsBody, "streamweld_ttft_seconds_count", labels, 1)
	requireObservedMetric(t, metricsBody, "streamweld_inter_token_seconds_count", labels, 1)
	requireObservedMetric(t, metricsBody, "streamweld_stream_duration_seconds_count", labels, 1)
	requireObservedMetric(t, metricsBody, "streamweld_journal_degraded", labels, 0)
	requireObservedMetricPositive(t, metricsBody, "streamweld_journal_bytes", labels)
	requireObservedMetric(t, metricsBody, "streamweld_backends",
		mergeObservedLabels(labels, "state", "healthy"), 1)
	requireObservedMetric(t, metricsBody, "streamweld_backends",
		mergeObservedLabels(labels, "state", "quarantined"), 1)
	requireObservedMetric(t, metricsBody, "streamweld_backends",
		mergeObservedLabels(labels, "state", "draining"), 0)
	requireEveryObservedFamilyHasRouteAndModel(t, metricsBody)
	if _, err := pool.MarkDraining(target.ID); err != nil {
		t.Fatalf("mark target draining: %v", err)
	}
	drainingMetricsResponse := doFailoverHTTPRequest(t, client,
		newFailoverHTTPRequest(t, http.MethodGet, front.URL+"/metrics", ""))
	defer closeFailoverBody(t, drainingMetricsResponse.Body)
	drainingMetrics := string(readFailoverBody(t, drainingMetricsResponse.Body))
	closeFailoverBody(t, drainingMetricsResponse.Body)
	requireObservedMetric(t, drainingMetrics, "streamweld_backends",
		mergeObservedLabels(labels, "state", "healthy"), 0)
	requireObservedMetric(t, drainingMetrics, "streamweld_backends",
		mergeObservedLabels(labels, "state", "draining"), 1)

	spans := exporter.GetSpans()
	if len(spans) != 3 {
		t.Fatalf("exported spans = %d, want one stream and two attempts: %+v", len(spans), spans)
	}
	var root *tracetest.SpanStub
	attempts := make([]tracetest.SpanStub, 0, 2)
	for index := range spans {
		span := &spans[index]
		switch span.SpanKind {
		case trace.SpanKindServer:
			root = span
		case trace.SpanKindClient:
			attempts = append(attempts, *span)
		}
	}
	if root == nil || len(attempts) != 2 {
		t.Fatalf("span kinds do not contain one stream and two attempts: %+v", spans)
	}
	if got := observedSpanAttribute(*root, "gen_ai.operation.name"); got != "chat" {
		t.Errorf("root gen_ai.operation.name = %q", got)
	}
	if got := observedSpanAttribute(*root, "gen_ai.request.model"); got != "test-model" {
		t.Errorf("root gen_ai.request.model = %q", got)
	}
	if got := observedSpanAttribute(*root, "streamweld.route"); got != "team-a/chat" {
		t.Errorf("root route = %q", got)
	}
	if root.Parent.TraceID().String() != incomingTraceID ||
		root.Parent.SpanID().String() != incomingSpanID || !root.Parent.IsRemote() {
		t.Errorf("root remote parent = %s/%s remote=%t", root.Parent.TraceID(), root.Parent.SpanID(), root.Parent.IsRemote())
	}
	for _, attempt := range attempts {
		if attempt.Parent.SpanID() != root.SpanContext.SpanID() {
			t.Errorf("attempt parent = %s, want root %s", attempt.Parent.SpanID(), root.SpanContext.SpanID())
		}
		if attempt.EndTime.After(root.EndTime) {
			t.Errorf("attempt ended at %s after root ended at %s", attempt.EndTime, root.EndTime)
		}
	}
	assertAttemptTraceparent(t, <-originTraceparent, *root, attempts)
	assertAttemptTraceparent(t, <-targetTraceparent, *root, attempts)
	foundMigration := false
	for _, event := range root.Events {
		if event.Name == "streamweld.migration" {
			foundMigration = true
		}
	}
	if !foundMigration {
		t.Error("root stream span has no migration event")
	}

	foundStructuredOpenLog := false
	for _, line := range strings.Split(strings.TrimSpace(logBuffer.String()), "\n") {
		var entry map[string]any
		if json.Unmarshal([]byte(line), &entry) != nil || entry["msg"] != "durable stream opened" {
			continue
		}
		foundStructuredOpenLog = entry["stream_id"] == streamID
	}
	if !foundStructuredOpenLog {
		t.Fatalf("JSON logs do not contain the stream-scoped open record: %s", logBuffer.String())
	}
}

func assertAttemptTraceparent(
	t *testing.T,
	value string,
	root tracetest.SpanStub,
	attempts []tracetest.SpanStub,
) {
	t.Helper()
	parts := strings.Split(value, "-")
	if len(parts) != 4 || parts[0] != "00" {
		t.Fatalf("upstream traceparent = %q", value)
	}
	if parts[1] != root.SpanContext.TraceID().String() {
		t.Errorf("upstream trace ID = %q, want %s", parts[1], root.SpanContext.TraceID())
	}
	for _, attempt := range attempts {
		if parts[2] == attempt.SpanContext.SpanID().String() {
			return
		}
	}
	t.Errorf("upstream span ID = %q, want an attempt span ID", parts[2])
}

func TestObservabilityRecordsMigrationRefusalAndErrorOutcome(t *testing.T) {
	const fragment = `{"id":"tool","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call-1","type":"function","function":{"name":"lookup","arguments":"{\"city\":"}}]},"finish_reason":null}],"usage":{"prompt_tokens":2,"completion_tokens":1,"total_tokens":3}}`
	originServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		startFailoverBackendSSE(writer)
		writeFailoverBackendData(writer, fragment)
	}))
	t.Cleanup(originServer.Close)
	targetServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		http.Error(writer, "must not migrate", http.StatusInternalServerError)
	}))
	t.Cleanup(targetServer.Close)
	origin := newFailoverBackend(t, "origin.refusal.test:8000", originServer.URL, "v1", conformance.VerdictSafe)
	target := newFailoverBackend(t, "target.refusal.test:8000", targetServer.URL, "v1", conformance.VerdictSafe)
	harness := newFailoverHTTPHarness(t, originServer.URL, newFailoverBackendPool(t, origin, target), nil)
	response := doFailoverHTTPRequest(t, harness.client, newFailoverHTTPRequest(
		t, http.MethodPost, harness.url+"/v1/chat/completions",
		`{"model":"test-model","stream":true,"messages":[{"role":"user","content":"tool"}]}`,
	))
	defer closeFailoverBody(t, response.Body)
	_ = readAllFailoverSSE(t, response.Body)
	closeFailoverBody(t, response.Body)
	metrics := doFailoverHTTPRequest(t, harness.client,
		newFailoverHTTPRequest(t, http.MethodGet, harness.url+"/metrics", ""))
	defer closeFailoverBody(t, metrics.Body)
	body := string(readFailoverBody(t, metrics.Body))
	closeFailoverBody(t, metrics.Body)
	labels := map[string]string{"route": "default", "model": "test-model"}
	requireObservedMetric(t, body, "streamweld_migrations_refused_total",
		mergeObservedLabels(labels, "predicate", "tool_call_boundary"), 1)
	requireObservedMetric(t, body, "streamweld_streams_total",
		mergeObservedLabels(labels, "outcome", "error"), 1)
}

func TestObservabilityRecordsExplicitStopOutcome(t *testing.T) {
	upstreamCanceled := make(chan struct{})
	backendServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		startFailoverBackendSSE(writer)
		if !writeFailoverBackendData(writer, failoverChatChunk("stop", "partial", "", nil)) {
			return
		}
		<-request.Context().Done()
		close(upstreamCanceled)
	}))
	t.Cleanup(backendServer.Close)
	harness := newDurableHTTPHarness(t, backendServer.URL, nil)
	response := doDurableHTTPRequest(t, harness.client, newDurableHTTPRequest(
		t, http.MethodPost, harness.url+"/v1/chat/completions",
		`{"model":"stop-model","stream":true,"messages":[{"role":"user","content":"stop"}]}`,
	))
	defer closeDurableHTTPBody(t, response.Body)
	streamID := requireDurableResponseHeaders(t, response)
	stopResponse := doDurableHTTPRequest(t, harness.client, newDurableHTTPRequest(
		t, http.MethodPost, harness.url+"/v1/streams/"+streamID.String()+"/stop", "",
	))
	defer closeDurableHTTPBody(t, stopResponse.Body)
	if stopResponse.StatusCode != http.StatusAccepted {
		t.Fatalf("stop status = %d, body = %q", stopResponse.StatusCode, readDurableHTTPBody(t, stopResponse.Body))
	}
	closeDurableHTTPBody(t, stopResponse.Body)
	_ = readAllDurableSSE(t, response.Body)
	closeDurableHTTPBody(t, response.Body)
	select {
	case <-upstreamCanceled:
	case <-time.After(time.Second):
		t.Fatal("stop did not cancel the upstream attempt")
	}
	metricsResponse := doDurableHTTPRequest(t, harness.client,
		newDurableHTTPRequest(t, http.MethodGet, harness.url+"/metrics", ""))
	defer closeDurableHTTPBody(t, metricsResponse.Body)
	metricsBody := string(readDurableHTTPBody(t, metricsResponse.Body))
	closeDurableHTTPBody(t, metricsResponse.Body)
	requireObservedMetric(t, metricsBody, "streamweld_streams_total", map[string]string{
		"route": "default", "model": "stop-model", "outcome": "stopped",
	}, 1)
}

func TestObservabilityExposesJournalDegradedGaugeOnOpenOutage(t *testing.T) {
	traceparent := make(chan string, 1)
	backendServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		traceparent <- request.Header.Get("traceparent")
		startDurableBackendSSE(writer)
		writeDurableBackendData(writer, `{"choices":[{"index":0,"delta":{"content":"live"},"finish_reason":null}]}`)
		writeDurableBackendData(writer, doneSentinelData)
	}))
	t.Cleanup(backendServer.Close)
	memoryConfig := journal.DefaultConfig()
	memoryConfig.MaxTotalBytes = 8 << 20
	memoryConfig.ReaderMaxLagBytes = 1 << 20
	memory, err := journal.NewMemory(memoryConfig)
	if err != nil {
		t.Fatalf("journal.NewMemory() error = %v", err)
	}
	config := DefaultConfig()
	config.BackendURL = backendServer.URL
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	recorder, err := telemetry.New(nil, nil, provider)
	if err != nil {
		t.Fatalf("telemetry.New() error = %v", err)
	}
	server, err := NewServer(config, nil,
		WithJournal(&openFailingJournal{Memory: memory}),
		WithStreamIDGenerator(&durableSequentialIDs{}),
		WithTelemetry(recorder),
	)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	front := httptest.NewServer(server.Handler())
	t.Cleanup(func() {
		server.forceCancel()
		front.Close()
	})
	client := &http.Client{Timeout: durableHTTPTestTimeout}
	response := doDurableHTTPRequest(t, client, newDurableHTTPRequest(
		t, http.MethodPost, front.URL+"/v1/chat/completions",
		`{"model":"degraded-model","stream":true,"messages":[{"role":"user","content":"go"}]}`,
	))
	defer closeDurableHTTPBody(t, response.Body)
	if response.Header.Get(headerStreamID) == "" {
		t.Error("degraded response omitted stream ID")
	}
	_ = readAllDurableSSE(t, response.Body)
	closeDurableHTTPBody(t, response.Body)
	metrics := doDurableHTTPRequest(t, client,
		newDurableHTTPRequest(t, http.MethodGet, front.URL+"/metrics", ""))
	defer closeDurableHTTPBody(t, metrics.Body)
	body := string(readDurableHTTPBody(t, metrics.Body))
	closeDurableHTTPBody(t, metrics.Body)
	requireObservedMetric(t, body, "streamweld_journal_degraded", map[string]string{
		"route": "default", "model": "degraded-model",
	}, 1)
	requireObservedMetric(t, body, "streamweld_streams_active", map[string]string{
		"route": "default", "model": "degraded-model",
	}, 0)
	requireObservedMetric(t, body, "streamweld_streams_total", map[string]string{
		"route": "default", "model": "degraded-model", "outcome": "done",
	}, 1)
	requireObservedMetric(t, body, "streamweld_ttft_seconds_count", map[string]string{
		"route": "default", "model": "degraded-model",
	}, 1)
	requireObservedMetric(t, body, "streamweld_stream_duration_seconds_count", map[string]string{
		"route": "default", "model": "degraded-model",
	}, 1)

	spans := exporter.GetSpans()
	if len(spans) != 2 {
		t.Fatalf("degraded stream spans = %d, want root and attempt: %+v", len(spans), spans)
	}
	var root tracetest.SpanStub
	attempts := make([]tracetest.SpanStub, 0, 1)
	for _, span := range spans {
		switch span.SpanKind {
		case trace.SpanKindServer:
			root = span
		case trace.SpanKindClient:
			attempts = append(attempts, span)
		}
	}
	if !root.SpanContext.IsValid() || len(attempts) != 1 {
		t.Fatalf("degraded span hierarchy = %+v", spans)
	}
	if attempts[0].Parent.SpanID() != root.SpanContext.SpanID() {
		t.Errorf("degraded attempt parent = %s, want %s", attempts[0].Parent.SpanID(), root.SpanContext.SpanID())
	}
	assertAttemptTraceparent(t, <-traceparent, root, attempts)
}

func TestDegradedTelemetryClosesLifecycleWhenReverseProxyAborts(t *testing.T) {
	memoryConfig := journal.DefaultConfig()
	memoryConfig.MaxTotalBytes = 8 << 20
	memoryConfig.ReaderMaxLagBytes = 1 << 20
	memory, err := journal.NewMemory(memoryConfig)
	if err != nil {
		t.Fatalf("journal.NewMemory() error = %v", err)
	}
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	recorder, err := telemetry.New(nil, nil, provider)
	if err != nil {
		t.Fatalf("telemetry.New() error = %v", err)
	}
	transport := observabilityRoundTripperFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.Path == "/health" {
			return &http.Response{
				StatusCode: http.StatusOK,
				Status:     "200 OK",
				Header:     make(http.Header),
				Body:       http.NoBody,
				Request:    request,
			}, nil
		}
		payload := "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"partial\"},\"finish_reason\":null}]}\n\n"
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Header: http.Header{
				"Content-Type": []string{"text/event-stream"},
			},
			Body: io.NopCloser(io.MultiReader(
				strings.NewReader(payload),
				observabilityErrorReader{},
			)),
			Request: request,
		}, nil
	})
	config := DefaultConfig()
	config.BackendURL = "http://backend.abort.test"
	server, err := NewServer(config, nil,
		WithTransport(transport),
		WithJournal(&openFailingJournal{Memory: memory}),
		WithStreamIDGenerator(&durableSequentialIDs{}),
		WithTelemetry(recorder),
	)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	front := httptest.NewServer(server.Handler())
	t.Cleanup(func() {
		server.forceCancel()
		front.Close()
	})
	client := &http.Client{Timeout: durableHTTPTestTimeout}
	response, requestErr := client.Do(newDurableHTTPRequest(
		t,
		http.MethodPost,
		front.URL+"/v1/chat/completions",
		`{"model":"abort-model","stream":true,"messages":[]}`,
	))
	if response != nil {
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
	}
	if requestErr != nil && !errors.Is(requestErr, io.ErrUnexpectedEOF) {
		t.Logf("aborted reverse proxy request returned %v", requestErr)
	}

	metrics := doDurableHTTPRequest(t, client,
		newDurableHTTPRequest(t, http.MethodGet, front.URL+"/metrics", ""))
	defer closeDurableHTTPBody(t, metrics.Body)
	body := string(readDurableHTTPBody(t, metrics.Body))
	closeDurableHTTPBody(t, metrics.Body)
	labels := map[string]string{"route": "default", "model": "abort-model"}
	requireObservedMetric(t, body, "streamweld_streams_active", labels, 0)
	requireObservedMetric(t, body, "streamweld_streams_total",
		mergeObservedLabels(labels, "outcome", "error"), 1)
	if spans := exporter.GetSpans(); len(spans) != 2 {
		t.Fatalf("aborted degraded stream spans = %d, want root and attempt: %+v", len(spans), spans)
	}
}

type observabilityRoundTripperFunc func(*http.Request) (*http.Response, error)

func (roundTrip observabilityRoundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return roundTrip(request)
}

type observabilityErrorReader struct{}

func (observabilityErrorReader) Read([]byte) (int, error) {
	return 0, io.ErrUnexpectedEOF
}

func TestMetricMigrationReasonUsesBoundedProtocolLabels(t *testing.T) {
	tests := map[string]string{
		"unexpected_eof": "crash",
		"tcp_reset":      "crash",
		"upstream_5xx":   "crash",
		"drain":          "drain",
		"stall":          "stall",
		"error_chunk":    "error_chunk",
		"health":         "health",
	}
	for input, want := range tests {
		if got := metricMigrationReason(input); got != want {
			t.Errorf("metricMigrationReason(%q) = %q, want %q", input, got, want)
		}
	}
}

func applyObservedRoute(t *testing.T, server *Server, backends ...backend.Backend) {
	t.Helper()
	inputs := make([]routeBackendInput, 0, len(backends))
	for _, item := range backends {
		inputs = append(inputs, routeBackendInput{
			ID: item.ID.String(), URL: item.URL.String(), ModelVersion: item.ModelVersion,
			TemplateVerdict: item.TemplateVerdict,
		})
	}
	_, err := server.durable.routes.apply("team-a/chat", routeBackendUpdate{
		Model: "test-model", UID: "observed-route", ObservedGeneration: 1,
		Policy: routePolicyInput{
			MaxMigrations: 3, MaxMigrationTokens: 8192, MaxStreamDuration: "15m",
			OrphanPolicy: OrphanContinue, OrphanTimeout: "60s", SeamWindowBytes: 8,
			JournalTTL: "10m",
		},
		Backends: inputs,
	})
	if err != nil {
		t.Fatalf("apply observed route: %v", err)
	}
}

func requireObservedMetric(
	t *testing.T,
	body, name string,
	labels map[string]string,
	want float64,
) {
	t.Helper()
	got, ok := observedMetricValue(body, name, labels)
	if !ok {
		t.Fatalf("metric %s with labels %v is absent:\n%s", name, labels, body)
	}
	if got != want {
		t.Fatalf("metric %s with labels %v = %v, want %v", name, labels, got, want)
	}
}

func requireObservedMetricPositive(t *testing.T, body, name string, labels map[string]string) {
	t.Helper()
	got, ok := observedMetricValue(body, name, labels)
	if !ok || got <= 0 {
		t.Fatalf("metric %s with labels %v = %v (present=%v), want positive", name, labels, got, ok)
	}
}

func observedMetricValue(body, name string, labels map[string]string) (float64, bool) {
	scanner := bufio.NewScanner(strings.NewReader(body))
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "#") ||
			(!strings.HasPrefix(line, name+"{") && !strings.HasPrefix(line, name+" ")) {
			continue
		}
		matches := true
		for key, value := range labels {
			if !strings.Contains(line, fmt.Sprintf(`%s=%q`, key, value)) {
				matches = false
				break
			}
		}
		if !matches {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		value, err := strconv.ParseFloat(fields[len(fields)-1], 64)
		return value, err == nil
	}
	return 0, false
}

func mergeObservedLabels(source map[string]string, key, value string) map[string]string {
	result := make(map[string]string, len(source)+1)
	for sourceKey, sourceValue := range source {
		result[sourceKey] = sourceValue
	}
	result[key] = value
	return result
}

func requireEveryObservedFamilyHasRouteAndModel(t *testing.T, body string) {
	t.Helper()
	families := []string{
		"streamweld_streams_active", "streamweld_streams_total",
		"streamweld_migrations_total", "streamweld_migrations_refused_total",
		"streamweld_tokens_rescued_total", "streamweld_prompt_tokens_rebilled_total",
		"streamweld_resumes_total", "streamweld_seam_overlap_bytes",
		"streamweld_ttft_seconds", "streamweld_inter_token_seconds",
		"streamweld_stream_duration_seconds", "streamweld_journal_bytes",
		"streamweld_journal_degraded", "streamweld_backends",
	}
	scanner := bufio.NewScanner(strings.NewReader(body))
	seen := make(map[string]bool, len(families))
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "#") {
			continue
		}
		for _, family := range families {
			if !strings.HasPrefix(line, family) {
				continue
			}
			seen[family] = true
			if !strings.Contains(line, `route="`) || !strings.Contains(line, `model="`) {
				t.Errorf("metric sample lacks route/model labels: %s", line)
			}
		}
	}
	for _, family := range families {
		if !seen[family] {
			t.Errorf("metrics endpoint has no samples for %s", family)
		}
	}
}

func observedSpanAttribute(span tracetest.SpanStub, key string) string {
	for _, item := range span.Attributes {
		if string(item.Key) == key {
			return item.Value.AsString()
		}
	}
	return ""
}
