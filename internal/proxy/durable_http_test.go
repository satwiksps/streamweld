package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/streamweld/streamweld/internal/journal"
	"github.com/streamweld/streamweld/internal/proxy/sse"
)

const durableHTTPTestTimeout = 5 * time.Second

const (
	chatChunkHello = `{"id":"chat-1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"hello"},"finish_reason":null}]}`
	chatChunkWorld = `{"id":"chat-1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":" world"},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5}}`
	chatChunkPart  = `{"id":"chat-stop","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"partial"},"finish_reason":null}]}`
	chatChunkMore  = `{"id":"chat-stop","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"more"},"finish_reason":null}]}`
)

func TestDurableHTTPInitialStreamMapsJournalAndHeaders(t *testing.T) {
	t.Parallel()

	requests := make(chan durableBackendRequest, 1)
	backend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests <- snapshotDurableBackendRequest(request)
		startDurableBackendSSE(writer)
		if !writeDurableBackendData(writer, chatChunkHello) {
			return
		}
		if !writeDurableBackendData(writer, chatChunkWorld) {
			return
		}
		_ = writeDurableBackendData(writer, "[DONE]")
	}))
	t.Cleanup(backend.Close)

	harness := newDurableHTTPHarness(t, backend.URL, nil)
	body := "{\n  \"model\": \"test-model\",\n  \"stream\": true,\n  \"messages\": [{\"role\":\"user\",\"content\":\"hi\"}],\n  \"vendor_extension\": {\"mode\":\"exact\"}\n}"
	request := newDurableHTTPRequest(t, http.MethodPost, harness.url+"/v1/chat/completions?vendor=%2Fraw+value", body)
	request.Header.Set("Authorization", "Bearer upstream-secret")
	request.Header.Set(headerVerbose, "1")
	request.Header.Set(headerOrphanPolicy, string(OrphanContinue))

	response := doDurableHTTPRequest(t, harness.client, request)
	defer closeDurableHTTPBody(t, response.Body)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("initial status = %d, body = %q", response.StatusCode, readDurableHTTPBody(t, response.Body))
	}
	id := requireDurableResponseHeaders(t, response)

	events := readAllDurableSSE(t, response.Body)
	if len(events) != 5 {
		t.Fatalf("initial event count = %d, want 5: %#v", len(events), events)
	}
	requireDurableSSEEvent(t, events[0], "1", streamOpenEvent, "")
	var openPayload struct {
		StreamID  journal.StreamID `json:"stream_id"`
		Model     string           `json:"model"`
		BackendID string           `json:"backend_id"`
	}
	if err := json.Unmarshal(events[0].Data, &openPayload); err != nil {
		t.Fatalf("decode open payload: %v", err)
	}
	wantBackendID := strings.TrimPrefix(backend.URL, "http://")
	if openPayload.StreamID != id || openPayload.Model != "test-model" || openPayload.BackendID != wantBackendID {
		t.Fatalf("open payload = %#v, want stream %s, model test-model, backend %s", openPayload, id, wantBackendID)
	}
	requireDurableSSEEvent(t, events[1], "2", "", chatChunkHello)
	requireDurableSSEEvent(t, events[2], "3", "", chatChunkWorld)
	requireDurableSSEEvent(t, events[3], "4", streamDoneEvent, "")
	var donePayload struct {
		FinishReason *string    `json:"finish_reason"`
		Usage        tokenUsage `json:"usage"`
	}
	if err := json.Unmarshal(events[3].Data, &donePayload); err != nil {
		t.Fatalf("decode done payload: %v", err)
	}
	if donePayload.FinishReason == nil || *donePayload.FinishReason != "stop" {
		t.Fatalf("done finish_reason = %v, want stop", donePayload.FinishReason)
	}
	if donePayload.Usage != (tokenUsage{PromptTokens: 3, CompletionTokens: 2, TotalTokens: 5}) {
		t.Fatalf("done usage = %#v", donePayload.Usage)
	}
	if events[4].HasID || events[4].HasType || string(events[4].Data) != doneSentinelData {
		t.Fatalf("compatibility sentinel = %#v", events[4])
	}

	backendRequest := awaitDurableValue(t, requests, "backend request")
	if backendRequest.readErr != nil {
		t.Fatalf("backend request body read error = %v", backendRequest.readErr)
	}
	if backendRequest.method != http.MethodPost || backendRequest.path != "/v1/chat/completions" || backendRequest.rawQuery != "vendor=%2Fraw+value" {
		t.Fatalf("backend request target = %s %s?%s", backendRequest.method, backendRequest.path, backendRequest.rawQuery)
	}
	if backendRequest.header.Get("Authorization") != "Bearer upstream-secret" {
		t.Fatalf("backend authorization = %q", backendRequest.header.Get("Authorization"))
	}
	for _, name := range []string{headerVerbose, headerOrphanPolicy, headerIdempotency, "Last-Event-ID"} {
		if got := backendRequest.header.Get(name); got != "" {
			t.Fatalf("backend received proxy-only header %s = %q", name, got)
		}
	}
	var normalized map[string]json.RawMessage
	if err := json.Unmarshal(backendRequest.body, &normalized); err != nil {
		t.Fatalf("backend body is not JSON: %v", err)
	}
	if _, ok := normalized["vendor_extension"]; !ok {
		t.Fatalf("backend body lost unknown member: %s", backendRequest.body)
	}
}

func TestDurableHTTPDisconnectThenResumeFromExclusiveCursor(t *testing.T) {
	t.Parallel()

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

	harness := newDurableHTTPHarness(t, backend.URL, nil)
	initialRequest := newDurableHTTPRequest(t, http.MethodPost, harness.url+"/v1/chat/completions", `{"model":"m","stream":true,"messages":[]}`)
	initialResponse := doDurableHTTPRequest(t, harness.client, initialRequest)
	if initialResponse.StatusCode != http.StatusOK {
		body := readDurableHTTPBody(t, initialResponse.Body)
		_ = initialResponse.Body.Close()
		t.Fatalf("initial status = %d, body = %q", initialResponse.StatusCode, body)
	}
	id := requireDurableResponseHeaders(t, initialResponse)
	initialDecoder := sse.NewDecoder(initialResponse.Body)
	requireDurableSSEEvent(t, readNextDurableSSE(t, initialDecoder), "1", streamOpenEvent, "")
	requireDurableSSEEvent(t, readNextDurableSSE(t, initialDecoder), "2", "", chatChunkHello)
	closeDurableHTTPBody(t, initialResponse.Body)

	resumeRequest := newDurableHTTPRequest(t, http.MethodGet, harness.url+"/v1/streams/"+id.String()+"/events", "")
	resumeRequest.Header.Set("Last-Event-ID", "2")
	resumeResponse := doDurableHTTPRequest(t, harness.client, resumeRequest)
	defer closeDurableHTTPBody(t, resumeResponse.Body)
	if resumeResponse.StatusCode != http.StatusOK {
		t.Fatalf("resume status = %d, body = %q", resumeResponse.StatusCode, readDurableHTTPBody(t, resumeResponse.Body))
	}
	if resumedID := requireDurableResponseHeaders(t, resumeResponse); resumedID != id {
		t.Fatalf("resumed stream ID = %s, want %s", resumedID, id)
	}

	releaseBackend()
	events := readAllDurableSSE(t, resumeResponse.Body)
	if len(events) != 3 {
		t.Fatalf("resumed event count = %d, want 3: %#v", len(events), events)
	}
	requireDurableSSEEvent(t, events[0], "3", "", chatChunkWorld)
	requireDurableSSEEvent(t, events[1], "4", streamDoneEvent, "")
	if events[2].HasID || string(events[2].Data) != doneSentinelData {
		t.Fatalf("resume sentinel = %#v", events[2])
	}
	if calls := backendCalls.Load(); calls != 1 {
		t.Fatalf("backend generation calls = %d, want 1", calls)
	}
}

func TestDurableHTTPIdempotentDuplicateReplaysOneActiveGeneration(t *testing.T) {
	t.Parallel()

	var backendCalls atomic.Int64
	backendRequests := make(chan durableBackendRequest, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseBackend := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseBackend)
	backend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		backendCalls.Add(1)
		backendRequests <- snapshotDurableBackendRequest(request)
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

	harness := newDurableHTTPHarness(t, backend.URL, nil)
	firstRequest := newDurableHTTPRequest(t, http.MethodPost, harness.url+"/v1/chat/completions", `{"model":"original-model","stream":true,"messages":[]}`)
	firstRequest.Header.Set(headerIdempotency, "opaque-duplicate-key")
	firstResponse := doDurableHTTPRequest(t, harness.client, firstRequest)
	defer closeDurableHTTPBody(t, firstResponse.Body)
	if firstResponse.StatusCode != http.StatusOK {
		t.Fatalf("first status = %d, body = %q", firstResponse.StatusCode, readDurableHTTPBody(t, firstResponse.Body))
	}
	firstID := requireDurableResponseHeaders(t, firstResponse)
	firstDecoder := sse.NewDecoder(firstResponse.Body)
	firstPrefix := []sse.Event{
		readNextDurableSSE(t, firstDecoder),
		readNextDurableSSE(t, firstDecoder),
	}
	requireDurableSSEEvent(t, firstPrefix[0], "1", streamOpenEvent, "")
	requireDurableSSEEvent(t, firstPrefix[1], "2", "", chatChunkHello)

	duplicateRequest := newDurableHTTPRequest(t, http.MethodPost, harness.url+"/v1/chat/completions", `{"model":"must-not-replace-original","stream":true,"messages":[{"role":"user","content":"different"}]}`)
	duplicateRequest.Header.Set(headerIdempotency, "opaque-duplicate-key")
	duplicateResponse := doDurableHTTPRequest(t, harness.client, duplicateRequest)
	defer closeDurableHTTPBody(t, duplicateResponse.Body)
	if duplicateResponse.StatusCode != http.StatusOK {
		t.Fatalf("duplicate status = %d, body = %q", duplicateResponse.StatusCode, readDurableHTTPBody(t, duplicateResponse.Body))
	}
	if duplicateID := requireDurableResponseHeaders(t, duplicateResponse); duplicateID != firstID {
		t.Fatalf("duplicate stream ID = %s, want %s", duplicateID, firstID)
	}
	duplicateDecoder := sse.NewDecoder(duplicateResponse.Body)
	duplicatePrefix := []sse.Event{
		readNextDurableSSE(t, duplicateDecoder),
		readNextDurableSSE(t, duplicateDecoder),
	}
	if !reflect.DeepEqual(duplicatePrefix, firstPrefix) {
		t.Fatalf("duplicate replay prefix = %#v, want %#v", duplicatePrefix, firstPrefix)
	}
	if calls := backendCalls.Load(); calls != 1 {
		t.Fatalf("backend generation calls before release = %d, want 1", calls)
	}
	backendRequest := awaitDurableValue(t, backendRequests, "first idempotent backend request")
	if backendRequest.header.Get(headerIdempotency) != "" {
		t.Fatalf("backend received raw idempotency key %q", backendRequest.header.Get(headerIdempotency))
	}
	if !bytes.Contains(backendRequest.body, []byte(`"original-model"`)) || bytes.Contains(backendRequest.body, []byte(`"must-not-replace-original"`)) {
		t.Fatalf("backend body = %s, want original request only", backendRequest.body)
	}

	releaseBackend()
	firstEvents := append([]sse.Event(nil), firstPrefix...)
	firstEvents = append(firstEvents, readRemainingDurableSSE(t, firstDecoder)...)
	duplicateEvents := append([]sse.Event(nil), duplicatePrefix...)
	duplicateEvents = append(duplicateEvents, readRemainingDurableSSE(t, duplicateDecoder)...)
	if !reflect.DeepEqual(duplicateEvents, firstEvents) {
		t.Fatalf("duplicate full replay = %#v, want %#v", duplicateEvents, firstEvents)
	}
	if len(firstEvents) != 5 {
		t.Fatalf("idempotent stream event count = %d, want 5", len(firstEvents))
	}
	if calls := backendCalls.Load(); calls != 1 {
		t.Fatalf("backend generation calls = %d, want 1", calls)
	}
}

func TestDurableHTTPDisconnectDiffersFromStopAndStateIsRedacted(t *testing.T) {
	t.Parallel()

	allowMore := make(chan struct{})
	var allowMoreOnce sync.Once
	releaseMore := func() { allowMoreOnce.Do(func() { close(allowMore) }) }
	t.Cleanup(releaseMore)
	upstreamCanceled := make(chan struct{})
	var canceledOnce sync.Once
	backend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = io.Copy(io.Discard, request.Body)
		startDurableBackendSSE(writer)
		if !writeDurableBackendData(writer, chatChunkPart) {
			return
		}
		select {
		case <-allowMore:
		case <-request.Context().Done():
			canceledOnce.Do(func() { close(upstreamCanceled) })
			return
		}
		if !writeDurableBackendData(writer, chatChunkMore) {
			return
		}
		<-request.Context().Done()
		canceledOnce.Do(func() { close(upstreamCanceled) })
	}))
	t.Cleanup(backend.Close)

	harness := newDurableHTTPHarness(t, backend.URL, nil)
	initialBody := `{"model":"state-model","stream":true,"messages":[{"role":"user","content":"private prompt phrase"}],"private_vendor_field":"private request extension"}`
	initialRequest := newDurableHTTPRequest(t, http.MethodPost, harness.url+"/v1/chat/completions", initialBody)
	initialRequest.Header.Set("Authorization", "Bearer private-credential")
	initialRequest.Header.Set(headerIdempotency, "private-idempotency-key")
	initialResponse := doDurableHTTPRequest(t, harness.client, initialRequest)
	if initialResponse.StatusCode != http.StatusOK {
		body := readDurableHTTPBody(t, initialResponse.Body)
		_ = initialResponse.Body.Close()
		t.Fatalf("initial status = %d, body = %q", initialResponse.StatusCode, body)
	}
	id := requireDurableResponseHeaders(t, initialResponse)
	decoder := sse.NewDecoder(initialResponse.Body)
	requireDurableSSEEvent(t, readNextDurableSSE(t, decoder), "1", streamOpenEvent, "")
	requireDurableSSEEvent(t, readNextDurableSSE(t, decoder), "2", "", chatChunkPart)
	closeDurableHTTPBody(t, initialResponse.Body)

	waitDurableCondition(t, "initial reader detachment", func() bool {
		runtime, ok := harness.server.durable.loadRuntime(id)
		if !ok {
			return false
		}
		runtime.mu.Lock()
		defer runtime.mu.Unlock()
		return runtime.readers == 0 && runtime.terminal == ""
	})
	select {
	case <-upstreamCanceled:
		t.Fatal("downstream disconnect canceled the producer")
	default:
	}

	releaseMore()
	openState := waitDurableJournalState(t, harness.server, id, func(state journal.StreamState) bool {
		return state.Status == journal.StatusOpen && state.LastSeq >= 3
	})
	if !openState.Resumable || openState.Terminal != nil {
		t.Fatalf("state after disconnect = %#v, want active resumable stream", openState)
	}
	select {
	case <-upstreamCanceled:
		t.Fatal("producer was canceled while disconnected under continue policy")
	default:
	}

	stopRequest := newDurableHTTPRequest(t, http.MethodPost, harness.url+"/v1/streams/"+id.String()+"/stop", "")
	stopResponseHTTP := doDurableHTTPRequest(t, harness.client, stopRequest)
	defer func() { _ = stopResponseHTTP.Body.Close() }()
	stopBody := readDurableHTTPBody(t, stopResponseHTTP.Body)
	closeDurableHTTPBody(t, stopResponseHTTP.Body)
	if stopResponseHTTP.StatusCode != http.StatusAccepted {
		t.Fatalf("stop status = %d, body = %q", stopResponseHTTP.StatusCode, stopBody)
	}
	var firstStop stopResponse
	if err := json.Unmarshal(stopBody, &firstStop); err != nil {
		t.Fatalf("decode stop response: %v", err)
	}
	if firstStop.StreamID != id || firstStop.Outcome != "stopped" || firstStop.PartialText != "partialmore" {
		t.Fatalf("stop response = %#v", firstStop)
	}
	wantUsage := tokenUsage{CompletionTokens: uint64(len("partialmore")), TotalTokens: uint64(len("partialmore")), Estimated: true}
	if firstStop.Usage != wantUsage {
		t.Fatalf("stop usage = %#v, want %#v", firstStop.Usage, wantUsage)
	}
	awaitDurableSignal(t, upstreamCanceled, "upstream cancellation after explicit stop")

	repeatedRequest := newDurableHTTPRequest(t, http.MethodPost, harness.url+"/v1/streams/"+id.String()+"/stop", "")
	repeatedResponseHTTP := doDurableHTTPRequest(t, harness.client, repeatedRequest)
	defer func() { _ = repeatedResponseHTTP.Body.Close() }()
	repeatedBody := readDurableHTTPBody(t, repeatedResponseHTTP.Body)
	closeDurableHTTPBody(t, repeatedResponseHTTP.Body)
	if repeatedResponseHTTP.StatusCode != http.StatusAccepted {
		t.Fatalf("repeated stop status = %d, body = %q", repeatedResponseHTTP.StatusCode, repeatedBody)
	}
	var repeatedStop stopResponse
	if err := json.Unmarshal(repeatedBody, &repeatedStop); err != nil {
		t.Fatalf("decode repeated stop response: %v", err)
	}
	if !reflect.DeepEqual(repeatedStop, firstStop) {
		t.Fatalf("repeated stop = %#v, want %#v", repeatedStop, firstStop)
	}

	stateRequest := newDurableHTTPRequest(t, http.MethodGet, harness.url+"/v1/streams/"+id.String(), "")
	stateResponse := doDurableHTTPRequest(t, harness.client, stateRequest)
	defer func() { _ = stateResponse.Body.Close() }()
	stateBody := readDurableHTTPBody(t, stateResponse.Body)
	closeDurableHTTPBody(t, stateResponse.Body)
	if stateResponse.StatusCode != http.StatusOK {
		t.Fatalf("state status = %d, body = %q", stateResponse.StatusCode, stateBody)
	}
	var state journal.StreamState
	if err := json.Unmarshal(stateBody, &state); err != nil {
		t.Fatalf("decode stream state: %v", err)
	}
	if state.StreamID != id || state.Status != journal.StatusStopped || state.Resumable || state.LastSeq != 4 || state.Terminal == nil || state.Terminal.Kind != journal.KindStopped {
		t.Fatalf("stopped state = %#v", state)
	}
	for _, secret := range []string{
		"private prompt phrase",
		"private request extension",
		"private-credential",
		"private-idempotency-key",
		"partialmore",
	} {
		if bytes.Contains(stateBody, []byte(secret)) {
			t.Fatalf("state response exposed private value %q: %s", secret, stateBody)
		}
	}

	resumeRequest := newDurableHTTPRequest(t, http.MethodGet, harness.url+"/v1/streams/"+id.String()+"/events", "")
	resumeResponse := doDurableHTTPRequest(t, harness.client, resumeRequest)
	defer func() { _ = resumeResponse.Body.Close() }()
	requireDurableStreamError(t, resumeResponse, http.StatusGone, "stream_not_resumable", id.String())
}

func TestDurableHTTPResumeRejectsMalformedAheadAndUnknownCursors(t *testing.T) {
	t.Parallel()

	var backendCalls atomic.Int64
	backend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		backendCalls.Add(1)
		_, _ = io.Copy(io.Discard, request.Body)
		startDurableBackendSSE(writer)
		if !writeDurableBackendData(writer, chatChunkHello) {
			return
		}
		_ = writeDurableBackendData(writer, "[DONE]")
	}))
	t.Cleanup(backend.Close)

	harness := newDurableHTTPHarness(t, backend.URL, nil)
	initialRequest := newDurableHTTPRequest(t, http.MethodPost, harness.url+"/v1/chat/completions", `{"model":"m","stream":true,"messages":[]}`)
	initialResponse := doDurableHTTPRequest(t, harness.client, initialRequest)
	if initialResponse.StatusCode != http.StatusOK {
		body := readDurableHTTPBody(t, initialResponse.Body)
		_ = initialResponse.Body.Close()
		t.Fatalf("initial status = %d, body = %q", initialResponse.StatusCode, body)
	}
	id := requireDurableResponseHeaders(t, initialResponse)
	events := readAllDurableSSE(t, initialResponse.Body)
	closeDurableHTTPBody(t, initialResponse.Body)
	if len(events) != 4 || events[2].Type != streamDoneEvent {
		t.Fatalf("completed stream events = %#v", events)
	}
	terminalSeq, err := strconv.ParseUint(events[2].ID, 10, 64)
	if err != nil {
		t.Fatalf("parse terminal sequence %q: %v", events[2].ID, err)
	}

	malformed := []struct {
		name   string
		values []string
	}{
		{name: "empty", values: []string{""}},
		{name: "negative", values: []string{"-1"}},
		{name: "explicit plus", values: []string{"+1"}},
		{name: "whitespace", values: []string{" 1"}},
		{name: "leading zero", values: []string{"01"}},
		{name: "overflow", values: []string{"18446744073709551616"}},
		{name: "multiple", values: []string{"1", "2"}},
	}
	for _, test := range malformed {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			request := newDurableHTTPRequest(t, http.MethodGet, harness.url+"/v1/streams/"+id.String()+"/events", "")
			request.Header[http.CanonicalHeaderKey("Last-Event-ID")] = append([]string(nil), test.values...)
			var response *http.Response
			if test.name == "whitespace" {
				// net/http correctly removes header optional whitespace on the
				// wire. Invoke the real handler directly to exercise the
				// protocol-level padded-value rejection.
				recorder := httptest.NewRecorder()
				harness.server.Handler().ServeHTTP(recorder, request)
				response = recorder.Result()
			} else {
				response = doDurableHTTPRequest(t, harness.client, request)
			}
			defer func() { _ = response.Body.Close() }()
			requireDurableStreamError(t, response, http.StatusBadRequest, "invalid_resume_cursor", id.String())
		})
	}

	aheadRequest := newDurableHTTPRequest(t, http.MethodGet, harness.url+"/v1/streams/"+id.String()+"/events", "")
	aheadRequest.Header.Set("Last-Event-ID", strconv.FormatUint(terminalSeq+1, 10))
	aheadResponse := doDurableHTTPRequest(t, harness.client, aheadRequest)
	defer func() { _ = aheadResponse.Body.Close() }()
	requireDurableStreamError(t, aheadResponse, http.StatusConflict, "cursor_ahead", id.String())

	unknownID, err := journal.ParseStreamID(fmt.Sprintf("%026d", 999))
	if err != nil {
		t.Fatalf("construct unknown stream ID: %v", err)
	}
	unknownRequest := newDurableHTTPRequest(t, http.MethodGet, harness.url+"/v1/streams/"+unknownID.String()+"/events", "")
	unknownResponse := doDurableHTTPRequest(t, harness.client, unknownRequest)
	defer func() { _ = unknownResponse.Body.Close() }()
	requireDurableStreamError(t, unknownResponse, http.StatusGone, "stream_not_found", unknownID.String())

	if calls := backendCalls.Load(); calls != 1 {
		t.Fatalf("backend calls after resume errors = %d, want 1", calls)
	}
}

func TestDurableHTTPOrphanCancelTerminatesOnlyTheProducer(t *testing.T) {
	var backendCalls atomic.Int64
	upstreamCanceled := make(chan struct{})
	var canceledOnce sync.Once
	backend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		backendCalls.Add(1)
		_, _ = io.Copy(io.Discard, request.Body)
		startDurableBackendSSE(writer)
		if !writeDurableBackendData(writer, chatChunkHello) {
			return
		}
		<-request.Context().Done()
		canceledOnce.Do(func() { close(upstreamCanceled) })
	}))
	t.Cleanup(backend.Close)

	harness := newDurableHTTPHarness(t, backend.URL, nil)
	initialRequest := newDurableHTTPRequest(t, http.MethodPost, harness.url+"/v1/chat/completions", `{"model":"m","stream":true,"messages":[]}`)
	initialRequest.Header.Set(headerOrphanPolicy, string(OrphanCancel))
	initialResponse := doDurableHTTPRequest(t, harness.client, initialRequest)
	if initialResponse.StatusCode != http.StatusOK {
		body := readDurableHTTPBody(t, initialResponse.Body)
		_ = initialResponse.Body.Close()
		t.Fatalf("initial status = %d, body = %q", initialResponse.StatusCode, body)
	}
	id := requireDurableResponseHeaders(t, initialResponse)
	decoder := sse.NewDecoder(initialResponse.Body)
	requireDurableSSEEvent(t, readNextDurableSSE(t, decoder), "1", streamOpenEvent, "")
	requireDurableSSEEvent(t, readNextDurableSSE(t, decoder), "2", "", chatChunkHello)
	closeDurableHTTPBody(t, initialResponse.Body)

	state := waitDurableJournalState(t, harness.server, id, func(candidate journal.StreamState) bool {
		return candidate.Status == journal.StatusError
	})
	if !state.Resumable || state.Terminal == nil || state.Terminal.Kind != journal.KindError || state.Terminal.Seq != 3 {
		t.Fatalf("orphan-canceled state = %#v", state)
	}
	var terminalPayload struct {
		Code   string `json:"code"`
		Reason string `json:"reason"`
	}
	if err := json.Unmarshal(state.Terminal.Payload, &terminalPayload); err != nil {
		t.Fatalf("decode orphan terminal payload: %v", err)
	}
	if terminalPayload.Code != "orphan_cancelled" || terminalPayload.Reason != "orphan_policy" {
		t.Fatalf("orphan terminal payload = %#v", terminalPayload)
	}
	awaitDurableSignal(t, upstreamCanceled, "orphan producer cancellation")

	resumeRequest := newDurableHTTPRequest(t, http.MethodGet, harness.url+"/v1/streams/"+id.String()+"/events", "")
	resumeRequest.Header.Set("Last-Event-ID", "2")
	resumeResponse := doDurableHTTPRequest(t, harness.client, resumeRequest)
	defer closeDurableHTTPBody(t, resumeResponse.Body)
	if resumeResponse.StatusCode != http.StatusOK {
		t.Fatalf("orphan resume status = %d, body = %q", resumeResponse.StatusCode, readDurableHTTPBody(t, resumeResponse.Body))
	}
	requireDurableResponseHeaders(t, resumeResponse)
	events := readAllDurableSSE(t, resumeResponse.Body)
	if len(events) != 1 {
		t.Fatalf("orphan resume events = %#v, want one terminal error", events)
	}
	requireDurableSSEEvent(t, events[0], "3", streamErrorEvent, "")
	if bytes.Contains(events[0].Data, []byte(`"outcome":"stopped"`)) || !bytes.Contains(events[0].Data, []byte(`"code":"orphan_cancelled"`)) {
		t.Fatalf("orphan terminal SSE data = %s", events[0].Data)
	}
	if calls := backendCalls.Load(); calls != 1 {
		t.Fatalf("orphan backend calls = %d, want 1", calls)
	}
}

func TestDurableHTTPCancelAfterReattachCancelsOrphanTimer(t *testing.T) {
	const orphanTimeout = 350 * time.Millisecond

	var backendCalls atomic.Int64
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseBackend := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseBackend)
	upstreamCanceled := make(chan struct{})
	var canceledOnce sync.Once
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
			canceledOnce.Do(func() { close(upstreamCanceled) })
			return
		}
		if !writeDurableBackendData(writer, chatChunkWorld) {
			return
		}
		_ = writeDurableBackendData(writer, "[DONE]")
	}))
	t.Cleanup(backend.Close)

	harness := newDurableHTTPHarness(t, backend.URL, func(config *Config) {
		config.OrphanTimeout = orphanTimeout
	})
	initialRequest := newDurableHTTPRequest(t, http.MethodPost, harness.url+"/v1/chat/completions", `{"model":"m","stream":true,"messages":[]}`)
	initialRequest.Header.Set(headerOrphanPolicy, string(OrphanCancelAfter))
	initialResponse := doDurableHTTPRequest(t, harness.client, initialRequest)
	if initialResponse.StatusCode != http.StatusOK {
		body := readDurableHTTPBody(t, initialResponse.Body)
		_ = initialResponse.Body.Close()
		t.Fatalf("initial status = %d, body = %q", initialResponse.StatusCode, body)
	}
	id := requireDurableResponseHeaders(t, initialResponse)
	decoder := sse.NewDecoder(initialResponse.Body)
	requireDurableSSEEvent(t, readNextDurableSSE(t, decoder), "1", streamOpenEvent, "")
	requireDurableSSEEvent(t, readNextDurableSSE(t, decoder), "2", "", chatChunkHello)
	closeDurableHTTPBody(t, initialResponse.Body)

	waitDurableCondition(t, "cancel_after orphan timer", func() bool {
		runtime, ok := harness.server.durable.loadRuntime(id)
		if !ok {
			return false
		}
		runtime.mu.Lock()
		defer runtime.mu.Unlock()
		return runtime.readers == 0 && runtime.orphanTimer != nil && runtime.terminal == ""
	})

	resumeRequest := newDurableHTTPRequest(t, http.MethodGet, harness.url+"/v1/streams/"+id.String()+"/events", "")
	resumeRequest.Header.Set("Last-Event-ID", "2")
	resumeResponse := doDurableHTTPRequest(t, harness.client, resumeRequest)
	defer closeDurableHTTPBody(t, resumeResponse.Body)
	if resumeResponse.StatusCode != http.StatusOK {
		t.Fatalf("reattach status = %d, body = %q", resumeResponse.StatusCode, readDurableHTTPBody(t, resumeResponse.Body))
	}
	if resumedID := requireDurableResponseHeaders(t, resumeResponse); resumedID != id {
		t.Fatalf("reattached ID = %s, want %s", resumedID, id)
	}
	waitDurableCondition(t, "reattached reader and canceled timer", func() bool {
		runtime, ok := harness.server.durable.loadRuntime(id)
		if !ok {
			return false
		}
		runtime.mu.Lock()
		defer runtime.mu.Unlock()
		return runtime.readers == 1 && runtime.orphanTimer == nil && runtime.terminal == ""
	})

	proofTimer := time.NewTimer(2 * orphanTimeout)
	select {
	case <-upstreamCanceled:
		proofTimer.Stop()
		t.Fatal("cancel_after timer canceled producer after a reader reattached")
	case <-proofTimer.C:
	}
	state := waitDurableJournalState(t, harness.server, id, func(candidate journal.StreamState) bool {
		return candidate.Status == journal.StatusOpen
	})
	if state.Terminal != nil {
		t.Fatalf("state after cancel_after grace = %#v", state)
	}

	releaseBackend()
	events := readAllDurableSSE(t, resumeResponse.Body)
	if len(events) != 3 {
		t.Fatalf("reattached events = %#v, want chunk, done, sentinel", events)
	}
	requireDurableSSEEvent(t, events[0], "3", "", chatChunkWorld)
	requireDurableSSEEvent(t, events[1], "4", streamDoneEvent, "")
	if events[2].HasID || string(events[2].Data) != doneSentinelData {
		t.Fatalf("reattached sentinel = %#v", events[2])
	}
	if calls := backendCalls.Load(); calls != 1 {
		t.Fatalf("cancel_after backend calls = %d, want 1", calls)
	}
}

func TestDurableHTTPNonStreamingRequestAndResponseRemainByteExact(t *testing.T) {
	t.Parallel()

	requestSnapshot := make(chan durableBackendRequest, 1)
	responseBytes := []byte{'{', '"', 'o', 'k', '"', ':', 't', 'r', 'u', 'e', ',', '"', 'r', 'a', 'w', '"', ':', '"', 0xff, 0x00, '"', '}'}
	backend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requestSnapshot <- snapshotDurableBackendRequest(request)
		writer.Header().Set("Content-Type", "application/octet-stream")
		writer.Header().Set("X-Vendor-Response", "preserved")
		writer.Header().Set("Content-Length", strconv.Itoa(len(responseBytes)))
		writer.WriteHeader(http.StatusCreated)
		_, _ = writer.Write(responseBytes)
	}))
	t.Cleanup(backend.Close)

	harness := newDurableHTTPHarness(t, backend.URL, nil)
	requestBytes := []byte("{\r\n  \"model\" : \"legacy-model\",\r\n  \"stream\" : false,\r\n  \"prompt\" : \"retain spacing\",\r\n  \"vendor_extension\" : [3, 2, 1]\r\n}\r\n")
	request := newDurableHTTPRequest(t, http.MethodPost, harness.url+"/v1/completions?fixed=one&vendor=%2Fraw+value", string(requestBytes))
	request.Header.Set("Authorization", "Bearer passthrough-secret")
	request.Header.Set("X-Vendor-Request", "preserved")
	request.Header.Set(headerIdempotency, "must-not-reach-backend")
	request.Header.Set(headerVerbose, "1")
	request.Header.Set(headerOrphanPolicy, string(OrphanCancel))
	request.Header.Set("Last-Event-ID", "42")

	response := doDurableHTTPRequest(t, harness.client, request)
	defer func() { _ = response.Body.Close() }()
	actualResponse := readDurableHTTPBody(t, response.Body)
	closeDurableHTTPBody(t, response.Body)
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("non-stream status = %d, body = %q", response.StatusCode, actualResponse)
	}
	if !bytes.Equal(actualResponse, responseBytes) {
		t.Fatalf("non-stream response bytes = %x, want %x", actualResponse, responseBytes)
	}
	if response.Header.Get("Content-Type") != "application/octet-stream" || response.Header.Get("X-Vendor-Response") != "preserved" {
		t.Fatalf("non-stream response headers = %#v", response.Header)
	}
	if response.Header.Get(headerDurability) != "" || response.Header.Get(headerStreamID) != "" {
		t.Fatalf("non-stream response unexpectedly has durability headers: %#v", response.Header)
	}

	backendRequest := awaitDurableValue(t, requestSnapshot, "non-stream backend request")
	if backendRequest.readErr != nil {
		t.Fatalf("backend body read error = %v", backendRequest.readErr)
	}
	if backendRequest.method != http.MethodPost || backendRequest.path != "/v1/completions" || backendRequest.rawQuery != "fixed=one&vendor=%2Fraw+value" {
		t.Fatalf("backend request target = %s %s?%s", backendRequest.method, backendRequest.path, backendRequest.rawQuery)
	}
	if !bytes.Equal(backendRequest.body, requestBytes) {
		t.Fatalf("backend request bytes = %x, want %x", backendRequest.body, requestBytes)
	}
	if backendRequest.header.Get("Authorization") != "Bearer passthrough-secret" || backendRequest.header.Get("X-Vendor-Request") != "preserved" {
		t.Fatalf("backend passthrough headers = %#v", backendRequest.header)
	}
	if got := backendRequest.header.Get("Content-Length"); got != strconv.Itoa(len(requestBytes)) {
		t.Fatalf("backend Content-Length = %q, want %d", got, len(requestBytes))
	}
	for _, name := range []string{headerIdempotency, headerVerbose, headerOrphanPolicy, "Last-Event-ID"} {
		if got := backendRequest.header.Get(name); got != "" {
			t.Fatalf("backend received proxy-only header %s = %q", name, got)
		}
	}
}

type durableHTTPHarness struct {
	server *Server
	url    string
	client *http.Client
}

func newDurableHTTPHarness(t *testing.T, backendURL string, configure func(*Config)) *durableHTTPHarness {
	t.Helper()

	config := DefaultConfig()
	config.BackendURL = backendURL
	config.ListenAddress = "127.0.0.1:0"
	config.ReadinessTimeout = time.Second
	if configure != nil {
		configure(&config)
	}
	ids := &durableSequentialIDs{}
	server, err := NewServer(config, nil, WithStreamIDGenerator(ids))
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
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

type durableSequentialIDs struct {
	next atomic.Uint64
}

func (ids *durableSequentialIDs) New() (journal.StreamID, error) {
	return journal.ParseStreamID(fmt.Sprintf("%026d", ids.next.Add(1)))
}

type durableBackendRequest struct {
	method   string
	path     string
	rawQuery string
	header   http.Header
	body     []byte
	readErr  error
}

func snapshotDurableBackendRequest(request *http.Request) durableBackendRequest {
	body, err := io.ReadAll(request.Body)
	return durableBackendRequest{
		method:   request.Method,
		path:     request.URL.Path,
		rawQuery: request.URL.RawQuery,
		header:   request.Header.Clone(),
		body:     body,
		readErr:  err,
	}
}

func startDurableBackendSSE(writer http.ResponseWriter) {
	writer.Header().Set("Content-Type", "text/event-stream")
	writer.WriteHeader(http.StatusOK)
	if flusher, ok := writer.(http.Flusher); ok {
		flusher.Flush()
	}
}

func writeDurableBackendData(writer http.ResponseWriter, data string) bool {
	if _, err := io.WriteString(writer, "data: "+data+"\n\n"); err != nil {
		return false
	}
	if flusher, ok := writer.(http.Flusher); ok {
		flusher.Flush()
	}
	return true
}

func newDurableHTTPRequest(t *testing.T, method, target, body string) *http.Request {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), durableHTTPTestTimeout)
	t.Cleanup(cancel)
	request, err := http.NewRequestWithContext(ctx, method, target, strings.NewReader(body))
	if err != nil {
		t.Fatalf("NewRequestWithContext() error = %v", err)
	}
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	return request
}

func doDurableHTTPRequest(t *testing.T, client *http.Client, request *http.Request) *http.Response {
	t.Helper()

	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("client.Do(%s %s) error = %v", request.Method, request.URL, err)
	}
	return response
}

func closeDurableHTTPBody(t *testing.T, body io.Closer) {
	t.Helper()
	if err := body.Close(); err != nil {
		t.Errorf("response body Close() error = %v", err)
	}
}

func readDurableHTTPBody(t *testing.T, body io.Reader) []byte {
	t.Helper()
	data, err := io.ReadAll(body)
	if err != nil {
		t.Fatalf("io.ReadAll(response) error = %v", err)
	}
	return data
}

func readAllDurableSSE(t *testing.T, body io.Reader) []sse.Event {
	t.Helper()

	decoder := sse.NewDecoder(body)
	events := make([]sse.Event, 0, 8)
	for len(events) < 32 {
		event, err := decoder.Decode()
		if errors.Is(err, io.EOF) {
			return events
		}
		if err != nil {
			t.Fatalf("SSE Decode() error = %v after %d events", err, len(events))
		}
		events = append(events, event)
	}
	t.Fatalf("SSE stream exceeded 32 events without closing")
	return nil
}

func readNextDurableSSE(t *testing.T, decoder *sse.Decoder) sse.Event {
	t.Helper()
	event, err := decoder.Decode()
	if err != nil {
		t.Fatalf("SSE Decode() error = %v", err)
	}
	return event
}

func readRemainingDurableSSE(t *testing.T, decoder *sse.Decoder) []sse.Event {
	t.Helper()

	events := make([]sse.Event, 0, 8)
	for len(events) < 32 {
		event, err := decoder.Decode()
		if errors.Is(err, io.EOF) {
			return events
		}
		if err != nil {
			t.Fatalf("SSE Decode() error = %v after %d remaining events", err, len(events))
		}
		events = append(events, event)
	}
	t.Fatalf("SSE stream exceeded 32 remaining events without closing")
	return nil
}

func requireDurableSSEEvent(t *testing.T, event sse.Event, id, eventType, data string) {
	t.Helper()
	if !event.HasID || event.ID != id {
		t.Fatalf("SSE event ID = (%v, %q), want (true, %q): %#v", event.HasID, event.ID, id, event)
	}
	if eventType == "" {
		if event.HasType {
			t.Fatalf("SSE event type = (%v, %q), want omitted", event.HasType, event.Type)
		}
	} else if !event.HasType || event.Type != eventType {
		t.Fatalf("SSE event type = (%v, %q), want (true, %q)", event.HasType, event.Type, eventType)
	}
	if data != "" && string(event.Data) != data {
		t.Fatalf("SSE event data = %q, want %q", event.Data, data)
	}
}

func requireDurableResponseHeaders(t *testing.T, response *http.Response) journal.StreamID {
	t.Helper()
	if got := response.Header.Get(headerDurability); got != durabilityDurable {
		t.Fatalf("%s = %q, want %q", headerDurability, got, durabilityDurable)
	}
	if got := response.Header.Get("Content-Type"); got != "text/event-stream; charset=utf-8" {
		t.Fatalf("Content-Type = %q", got)
	}
	if got := response.Header.Get("Cache-Control"); got != "no-cache, no-store" {
		t.Fatalf("Cache-Control = %q", got)
	}
	if got := response.Header.Get("X-Accel-Buffering"); got != "no" {
		t.Fatalf("X-Accel-Buffering = %q", got)
	}
	id, err := journal.ParseStreamID(response.Header.Get(headerStreamID))
	if err != nil {
		t.Fatalf("%s = %q: %v", headerStreamID, response.Header.Get(headerStreamID), err)
	}
	return id
}

func awaitDurableValue[T any](t *testing.T, values <-chan T, description string) T {
	t.Helper()
	select {
	case value := <-values:
		return value
	case <-time.After(durableHTTPTestTimeout):
		t.Fatalf("timed out waiting for %s", description)
		var zero T
		return zero
	}
}

func awaitDurableSignal(t *testing.T, signal <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(durableHTTPTestTimeout):
		t.Fatalf("timed out waiting for %s", description)
	}
}

func waitDurableCondition(t *testing.T, description string, condition func() bool) {
	t.Helper()
	deadline := time.NewTimer(durableHTTPTestTimeout)
	defer deadline.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatalf("timed out waiting for %s", description)
		}
	}
}

func waitDurableJournalState(
	t *testing.T,
	server *Server,
	id journal.StreamID,
	condition func(journal.StreamState) bool,
) journal.StreamState {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), durableHTTPTestTimeout)
	defer cancel()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	var last journal.StreamState
	var lastErr error
	for {
		last, lastErr = server.durable.journal.State(ctx, id)
		if lastErr == nil && condition(last) {
			return last
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatalf("timed out waiting for journal state: last state = %#v, error = %v", last, lastErr)
		}
	}
}

func requireDurableStreamError(
	t *testing.T,
	response *http.Response,
	wantStatus int,
	wantCode string,
	wantStreamID string,
) {
	t.Helper()
	body := readDurableHTTPBody(t, response.Body)
	closeDurableHTTPBody(t, response.Body)
	if response.StatusCode != wantStatus {
		t.Fatalf("stream error status = %d, want %d; body = %q", response.StatusCode, wantStatus, body)
	}
	var envelope streamErrorEnvelope
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatalf("decode stream error: %v; body = %q", err, body)
	}
	if envelope.Error.Code != wantCode || envelope.Error.Type != "streamweld_error" || envelope.Error.StreamID != wantStreamID {
		t.Fatalf("stream error = %#v, want code %q and stream ID %q", envelope.Error, wantCode, wantStreamID)
	}
}
