package proxy

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/streamweld/streamweld/internal/proxy/sse"
)

func TestPassthroughPreservesRequestAndResponse(t *testing.T) {
	t.Parallel()

	type capturedRequest struct {
		method     string
		requestURI string
		host       string
		header     http.Header
		body       []byte
	}
	captured := make(chan capturedRequest, 1)
	responseBody := []byte(" {\n  \"id\": \"cmpl_raw\", \"unknown\": [1, null, true]\n}\x00\xff")
	backend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Errorf("read upstream request body: %v", err)
		}
		captured <- capturedRequest{
			method:     request.Method,
			requestURI: request.RequestURI,
			host:       request.Host,
			header:     request.Header.Clone(),
			body:       body,
		}

		writer.Header().Set("Content-Type", "application/json; vendor=unchanged")
		writer.Header().Add("Set-Cookie", "first=one; Path=/")
		writer.Header().Add("Set-Cookie", "second=two; Path=/")
		writer.Header().Set("X-Upstream-End-To-End", "preserved")
		writer.Header().Set("Connection", "X-Upstream-Hop")
		writer.Header().Set("X-Upstream-Hop", "remove-me")
		writer.Header().Add("Trailer", "X-Upstream-Checksum")
		writer.WriteHeader(http.StatusMultiStatus)
		_, _ = writer.Write(responseBody)
		writer.Header().Set("X-Upstream-Checksum", "complete")
	}))
	t.Cleanup(backend.Close)

	proxyServer := newTestProxy(t, backend.URL+"/gateway?fixed=one")
	frontend := httptest.NewServer(proxyServer.Handler())
	t.Cleanup(frontend.Close)

	requestBody := []byte(`{"model":"test","stream":false,"vendor_extension":{"order":[3,2,1],"enabled":true}}`)
	request, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		frontend.URL+"/v1/chat/completions?z=last&vendor=%2Fraw%20value&vendor=second",
		bytes.NewReader(requestBody),
	)
	if err != nil {
		t.Fatal(err)
	}
	request.Host = "client.example.test"
	request.Header.Set("Authorization", "Bearer secret-token")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Add("X-Vendor-Option", "first")
	request.Header.Add("X-Vendor-Option", "second")
	request.Header.Set("Connection", "X-Remove-Me")
	request.Header.Set("X-Remove-Me", "hop-by-hop")

	client := &http.Client{Transport: &http.Transport{DisableCompression: true}}
	t.Cleanup(client.CloseIdleConnections)
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("proxy request: %v", err)
	}
	gotResponseBody, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if err != nil {
		t.Fatalf("read proxy response: %v", err)
	}
	if response.StatusCode != http.StatusMultiStatus {
		t.Errorf("status = %d, want %d", response.StatusCode, http.StatusMultiStatus)
	}
	if !bytes.Equal(gotResponseBody, responseBody) {
		t.Errorf("response body changed\n got: %q\nwant: %q", gotResponseBody, responseBody)
	}
	if got := response.Header.Get("Content-Type"); got != "application/json; vendor=unchanged" {
		t.Errorf("Content-Type = %q", got)
	}
	if got := response.Header.Values("Set-Cookie"); len(got) != 2 || got[0] != "first=one; Path=/" || got[1] != "second=two; Path=/" {
		t.Errorf("Set-Cookie headers changed: %q", got)
	}
	if got := response.Header.Get("X-Upstream-End-To-End"); got != "preserved" {
		t.Errorf("end-to-end response header = %q", got)
	}
	if got := response.Header.Get("X-Upstream-Hop"); got != "" {
		t.Errorf("hop-by-hop response header leaked: %q", got)
	}
	if got := response.Trailer.Get("X-Upstream-Checksum"); got != "complete" {
		t.Errorf("response trailer = %q, want complete", got)
	}

	upstream := <-captured
	if upstream.method != http.MethodPost {
		t.Errorf("upstream method = %q", upstream.method)
	}
	parsedURI, err := url.ParseRequestURI(upstream.requestURI)
	if err != nil {
		t.Fatalf("parse upstream RequestURI %q: %v", upstream.requestURI, err)
	}
	if parsedURI.Path != "/gateway/v1/chat/completions" {
		t.Errorf("upstream path = %q", parsedURI.Path)
	}
	wantQuery := url.Values{
		"fixed":  {"one"},
		"z":      {"last"},
		"vendor": {"/raw value", "second"},
	}
	if got := parsedURI.Query(); !reflect.DeepEqual(got, wantQuery) {
		t.Errorf("upstream query = %#v, want %#v (raw %q)", got, wantQuery, parsedURI.RawQuery)
	}
	if !bytes.Equal(upstream.body, requestBody) {
		t.Errorf("request body changed\n got: %q\nwant: %q", upstream.body, requestBody)
	}
	if got := upstream.header.Get("Authorization"); got != "Bearer secret-token" {
		t.Errorf("Authorization = %q", got)
	}
	if got := upstream.header.Values("X-Vendor-Option"); len(got) != 2 || got[0] != "first" || got[1] != "second" {
		t.Errorf("vendor headers changed: %q", got)
	}
	if got := upstream.header.Get("X-Remove-Me"); got != "" {
		t.Errorf("hop-by-hop request header leaked: %q", got)
	}
	if upstream.host != strings.TrimPrefix(backend.URL, "http://") {
		t.Errorf("upstream Host = %q, want backend host", upstream.host)
	}
	if got := upstream.header.Get("X-Forwarded-Host"); got != "client.example.test" {
		t.Errorf("X-Forwarded-Host = %q", got)
	}
	if got := upstream.header.Get("X-Forwarded-Proto"); got != "http" {
		t.Errorf("X-Forwarded-Proto = %q", got)
	}
	if got := upstream.header.Get("X-Forwarded-For"); got == "" {
		t.Error("X-Forwarded-For was not set")
	}
	if got := upstream.header.Get("Accept-Encoding"); got != "" {
		t.Errorf("proxy transport injected Accept-Encoding %q", got)
	}
}

func TestEncodedResponseBytesAreNotTransformed(t *testing.T) {
	t.Parallel()

	var encoded bytes.Buffer
	compressor := gzip.NewWriter(&encoded)
	_, _ = compressor.Write([]byte(`{"id":"compressed","choices":[]}`))
	if err := compressor.Close(); err != nil {
		t.Fatal(err)
	}
	want := append([]byte(nil), encoded.Bytes()...)

	backend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Accept-Encoding") != "gzip" {
			t.Errorf("Accept-Encoding = %q, want gzip", request.Header.Get("Accept-Encoding"))
		}
		writer.Header().Set("Content-Type", "application/json")
		writer.Header().Set("Content-Encoding", "gzip")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write(want)
	}))
	t.Cleanup(backend.Close)

	proxyServer := newTestProxy(t, backend.URL)
	frontend := httptest.NewServer(proxyServer.Handler())
	t.Cleanup(frontend.Close)
	request, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, frontend.URL+"/v1/models", nil)
	request.Header.Set("Accept-Encoding", "gzip")
	client := &http.Client{Transport: &http.Transport{DisableCompression: true}}
	t.Cleanup(client.CloseIdleConnections)
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("compressed response changed: got %d bytes, want %d", len(got), len(want))
	}
	if response.Header.Get("Content-Encoding") != "gzip" {
		t.Errorf("Content-Encoding = %q", response.Header.Get("Content-Encoding"))
	}
}

func TestSupportedRoutesAndLocalStatusEndpoints(t *testing.T) {
	t.Parallel()
	var upstreamCalls atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		upstreamCalls.Add(1)
		_, _ = io.WriteString(writer, request.Method+" "+request.URL.Path)
	}))
	t.Cleanup(backend.Close)
	proxyServer := newTestProxy(t, backend.URL)
	frontend := httptest.NewServer(proxyServer.Handler())
	t.Cleanup(frontend.Close)

	for _, test := range []struct {
		method string
		path   string
		body   string
	}{
		{http.MethodPost, "/v1/chat/completions", "POST /v1/chat/completions"},
		{http.MethodPost, "/v1/completions", "POST /v1/completions"},
		{http.MethodGet, "/v1/models", "GET /v1/models"},
	} {
		request, _ := http.NewRequestWithContext(context.Background(), test.method, frontend.URL+test.path, strings.NewReader("{}"))
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatalf("%s %s: %v", test.method, test.path, err)
		}
		body, _ := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if response.StatusCode != http.StatusOK || string(body) != test.body {
			t.Errorf("%s %s = (%d, %q), want (200, %q)", test.method, test.path, response.StatusCode, body, test.body)
		}
	}

	for path, wantBody := range map[string]string{
		"/healthz": `{"status":"ok"}` + "\n",
		"/readyz":  `{"status":"ready"}` + "\n",
	} {
		request, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, frontend.URL+path, nil)
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if response.StatusCode != http.StatusOK || string(body) != wantBody {
			t.Errorf("GET %s = (%d, %q), want (200, %q)", path, response.StatusCode, body, wantBody)
		}
	}
	if got := upstreamCalls.Load(); got != 4 {
		t.Errorf("upstream calls = %d, want 4 (three proxied calls and one readiness probe)", got)
	}
}

func TestUnsupportedPathsAndMethodsAreRejectedLocally(t *testing.T) {
	t.Parallel()
	var upstreamCalls atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		upstreamCalls.Add(1)
	}))
	t.Cleanup(backend.Close)
	proxyServer := newTestProxy(t, backend.URL)
	frontend := httptest.NewServer(proxyServer.Handler())
	t.Cleanup(frontend.Close)

	tests := []struct {
		method string
		path   string
		status int
		allow  string
		code   string
	}{
		{http.MethodGet, "/v1/chat/completions", http.StatusMethodNotAllowed, http.MethodPost, "method_not_allowed"},
		{http.MethodDelete, "/v1/completions", http.StatusMethodNotAllowed, http.MethodPost, "method_not_allowed"},
		{http.MethodPost, "/v1/models", http.StatusMethodNotAllowed, http.MethodGet, "method_not_allowed"},
		{http.MethodPost, "/healthz", http.StatusMethodNotAllowed, http.MethodGet, "method_not_allowed"},
		{http.MethodGet, "/v1/unknown", http.StatusNotFound, "", "not_found"},
		{http.MethodGet, "/v1/models/", http.StatusNotFound, "", "not_found"},
	}
	for _, test := range tests {
		request, _ := http.NewRequestWithContext(context.Background(), test.method, frontend.URL+test.path, nil)
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatalf("%s %s: %v", test.method, test.path, err)
		}
		var envelope errorEnvelope
		err = json.NewDecoder(response.Body).Decode(&envelope)
		_ = response.Body.Close()
		if err != nil {
			t.Errorf("decode error response for %s %s: %v", test.method, test.path, err)
		}
		if response.StatusCode != test.status || envelope.Error.Code != test.code {
			t.Errorf("%s %s = (%d, %q), want (%d, %q)", test.method, test.path, response.StatusCode, envelope.Error.Code, test.status, test.code)
		}
		if got := response.Header.Get("Allow"); got != test.allow {
			t.Errorf("%s %s Allow = %q, want %q", test.method, test.path, got, test.allow)
		}
	}
	if got := upstreamCalls.Load(); got != 0 {
		t.Errorf("rejected requests reached upstream %d times", got)
	}
}

func TestUpstreamFailureReturnsStructuredBadGatewayAndJSONLog(t *testing.T) {
	t.Parallel()
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	config := DefaultConfig()
	config.BackendURL = "http://backend.example.test"
	config.ListenAddress = "127.0.0.1:0"
	server, err := NewServer(config, logger, WithTransport(roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("dial failed for test")
	})))
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/models", nil)
	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", recorder.Code)
	}
	var envelope errorEnvelope
	if err := json.Unmarshal(recorder.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("response is not JSON: %v", err)
	}
	if envelope.Error.Code != "bad_gateway" {
		t.Errorf("error code = %q", envelope.Error.Code)
	}
	var logRecord map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(logs.Bytes()), &logRecord); err != nil {
		t.Fatalf("log is not JSON: %q: %v", logs.Bytes(), err)
	}
	if logRecord["msg"] != "upstream request failed" {
		t.Errorf("log message = %v", logRecord["msg"])
	}
}

func TestStreamingResponseIsFlushedPromptly(t *testing.T) {
	t.Parallel()
	firstChunk := []byte("data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"first\"}}]}\n\n")
	secondChunk := []byte("data: [DONE]\n\n")
	firstSent := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	defer releaseOnce.Do(func() { close(release) })

	backend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "text/event-stream")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write(firstChunk)
		writer.(http.Flusher).Flush()
		close(firstSent)
		<-release
		_, _ = writer.Write(secondChunk)
		writer.(http.Flusher).Flush()
	}))
	t.Cleanup(backend.Close)
	proxyServer := newTestProxy(t, backend.URL)
	frontend := httptest.NewServer(proxyServer.Handler())
	t.Cleanup(frontend.Close)

	client := &http.Client{Timeout: 3 * time.Second}
	request, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, frontend.URL+"/v1/chat/completions", strings.NewReader(`{"stream":true}`))
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("open streaming response: %v", err)
	}
	defer func() {
		if closeErr := response.Body.Close(); closeErr != nil {
			t.Errorf("close streaming response: %v", closeErr)
		}
	}()
	<-firstSent

	decoder := sse.NewDecoder(response.Body)
	readResult := make(chan struct {
		events []sse.Event
		err    error
	}, 1)
	go func() {
		events := make([]sse.Event, 0, 2)
		for range 2 {
			event, decodeErr := decoder.Decode()
			if decodeErr != nil {
				readResult <- struct {
					events []sse.Event
					err    error
				}{events, decodeErr}
				return
			}
			events = append(events, event)
		}
		readResult <- struct {
			events []sse.Event
			err    error
		}{events, nil}
	}()
	select {
	case result := <-readResult:
		if result.err != nil {
			t.Fatalf("read initial durable events: %v", result.err)
		}
		if result.events[0].Type != streamOpenEvent {
			t.Fatalf("first event type = %q, want %q", result.events[0].Type, streamOpenEvent)
		}
		if !bytes.Contains(result.events[1].Data, []byte(`"content":"first"`)) {
			t.Fatalf("first chunk data changed: %q", result.events[1].Data)
		}
	case <-time.After(time.Second):
		t.Fatal("first upstream chunk was not flushed to the client")
	}

	releaseOnce.Do(func() { close(release) })
	doneEvent, err := decoder.Decode()
	if err != nil || doneEvent.Type != streamDoneEvent {
		t.Fatalf("done event = %+v, error = %v", doneEvent, err)
	}
	sentinel, err := decoder.Decode()
	if err != nil || string(sentinel.Data) != "[DONE]" {
		t.Fatalf("done sentinel = %+v, error = %v", sentinel, err)
	}
}

func TestClientCancellationDoesNotStopProducer(t *testing.T) {
	t.Parallel()
	transport := newBlockingRoundTripper()
	t.Cleanup(transport.Release)
	proxyServer := newTestProxy(t, "http://backend.example.test", WithTransport(transport))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	request := httptest.NewRequestWithContext(ctx, http.MethodPost, "/v1/completions", strings.NewReader(`{"stream":true}`))
	result := make(chan struct{}, 1)
	go func() {
		proxyServer.Handler().ServeHTTP(httptest.NewRecorder(), request)
		result <- struct{}{}
	}()
	select {
	case <-transport.started:
	case <-time.After(2 * time.Second):
		t.Fatal("upstream request did not start")
	}
	cancel()
	select {
	case <-transport.canceled:
		t.Fatal("client cancellation incorrectly canceled the stream-owned producer")
	case <-time.After(100 * time.Millisecond):
	}
	transport.Release()
	select {
	case <-result:
	case <-time.After(2 * time.Second):
		t.Fatal("proxy handler did not return after cancellation")
	}
}

func newTestProxy(t *testing.T, backendURL string, options ...Option) *Server {
	t.Helper()
	config := DefaultConfig()
	config.BackendURL = backendURL
	config.ListenAddress = "127.0.0.1:0"
	server, err := NewServer(config, nil, options...)
	if err != nil {
		t.Fatalf("NewServer(): %v", err)
	}
	t.Cleanup(server.closeIdleConnections)
	return server
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

type blockingRoundTripper struct {
	started     chan struct{}
	canceled    chan struct{}
	release     chan struct{}
	startedOnce sync.Once
	cancelOnce  sync.Once
	releaseOnce sync.Once
}

func newBlockingRoundTripper() *blockingRoundTripper {
	return &blockingRoundTripper{
		started:  make(chan struct{}),
		canceled: make(chan struct{}),
		release:  make(chan struct{}),
	}
}

func (t *blockingRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	t.startedOnce.Do(func() { close(t.started) })
	select {
	case <-request.Context().Done():
		t.cancelOnce.Do(func() { close(t.canceled) })
		return nil, request.Context().Err()
	case <-t.release:
		return &http.Response{
			StatusCode:    http.StatusOK,
			Status:        "200 OK",
			Header:        make(http.Header),
			Body:          io.NopCloser(strings.NewReader("finished")),
			ContentLength: int64(len("finished")),
			Request:       request,
		}, nil
	}
}

func (t *blockingRoundTripper) Release() {
	t.releaseOnce.Do(func() { close(t.release) })
}
