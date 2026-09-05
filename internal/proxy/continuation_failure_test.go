package proxy

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/satwiksps/streamweld/internal/conformance"
)

func TestFailoverHTTPRetainsShortContinuationBeforeAnotherFailure(t *testing.T) {
	for _, failure := range []string{"unexpected_eof", "error_chunk"} {
		t.Run(failure, func(t *testing.T) {
			originServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				startFailoverBackendSSE(writer)
				writeFailoverBackendData(writer, failoverChatChunk("origin", "The quick ", "", nil))
			}))
			t.Cleanup(originServer.Close)
			intermediateServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				startFailoverBackendSSE(writer)
				writeFailoverBackendData(writer, failoverChatChunk("intermediate", "quick brown", "", nil))
				if failure == "error_chunk" {
					writeFailoverBackendData(writer, `{"error":{"message":"producer failed"}}`)
				}
			}))
			t.Cleanup(intermediateServer.Close)
			lastRequest := make(chan failoverBackendRequest, 1)
			lastServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				lastRequest <- captureFailoverBackendRequest(request)
				startFailoverBackendSSE(writer)
				writeFailoverBackendData(writer, failoverChatChunk("last", " fox", "stop", nil))
				writeFailoverBackendData(writer, doneSentinelData)
			}))
			t.Cleanup(lastServer.Close)

			origin := newFailoverBackend(t, "a-origin.test:8000", originServer.URL, "model-v1", conformance.VerdictSafe)
			intermediate := newFailoverBackend(t, "b-intermediate.test:8000", intermediateServer.URL, "model-v1", conformance.VerdictSafe)
			last := newFailoverBackend(t, "c-last.test:8000", lastServer.URL, "model-v1", conformance.VerdictSafe)
			harness := newFailoverHTTPHarness(t, originServer.URL, newFailoverBackendPool(t, origin, intermediate, last), nil)
			request := newFailoverHTTPRequest(t, http.MethodPost, harness.url+"/v1/chat/completions",
				`{"model":"test-model","stream":true,"messages":[{"role":"user","content":"continue"}],"max_tokens":100}`)
			request.Header.Set(headerVerbose, "1")
			response := doFailoverHTTPRequest(t, harness.client, request)
			defer closeFailoverBody(t, response.Body)
			events := readAllFailoverSSE(t, response.Body)
			requireFailoverSequence(t, events)
			requireFailoverDone(t, events)
			if got := failoverText(events); got != "The quick brown fox" {
				t.Fatalf("assembled text = %q, want retained intermediate continuation", got)
			}
			replayRequest := newFailoverHTTPRequest(t, http.MethodGet,
				harness.url+"/v1/streams/"+response.Header.Get(headerStreamID)+"/events", "")
			replay := doFailoverHTTPRequest(t, harness.client, replayRequest)
			defer closeFailoverBody(t, replay.Body)
			replayedEvents := readAllFailoverSSE(t, replay.Body)
			requireFailoverDone(t, replayedEvents)
			if got := failoverText(replayedEvents); got != "The quick brown fox" {
				t.Fatalf("replayed text = %q, want retained intermediate continuation", got)
			}
			var body struct {
				Messages []struct {
					Content string `json:"content"`
				} `json:"messages"`
			}
			captured := awaitFailoverValue(t, lastRequest, "second continuation request")
			decodeFailoverJSON(t, captured.body, &body)
			if len(body.Messages) != 2 || body.Messages[1].Content != "The quick brown" {
				t.Fatalf("second continuation did not retain the first continuation: %+v", body.Messages)
			}
		})
	}
}

func TestFailoverHTTPBoundsContinuationFramesWithoutText(t *testing.T) {
	originServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		startFailoverBackendSSE(writer)
		writeFailoverBackendData(writer, failoverChatChunk("origin", "prefix", "", nil))
	}))
	t.Cleanup(originServer.Close)
	targetServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		startFailoverBackendSSE(writer)
		for range 16 {
			if _, err := io.WriteString(writer, ":"+strings.Repeat("x", 200)+"\n"); err != nil {
				return
			}
			if !writeFailoverBackendData(writer, `{"choices":[{"index":0,"delta":{}}]}`) {
				return
			}
		}
		writeFailoverBackendData(writer, doneSentinelData)
	}))
	t.Cleanup(targetServer.Close)
	origin := newFailoverBackend(t, "origin.test:8000", originServer.URL, "model-v1", conformance.VerdictSafe)
	target := newFailoverBackend(t, "target.test:8000", targetServer.URL, "model-v1", conformance.VerdictSafe)
	harness := newFailoverHTTPHarness(t, originServer.URL, newFailoverBackendPool(t, origin, target), func(config *Config) {
		config.MaxSSEEventBytes = 512
	})
	request := newFailoverHTTPRequest(t, http.MethodPost, harness.url+"/v1/chat/completions",
		`{"model":"test-model","stream":true,"messages":[{"role":"user","content":"continue"}],"max_tokens":100}`)
	request.Header.Set(headerVerbose, "1")
	response := doFailoverHTTPRequest(t, harness.client, request)
	defer closeFailoverBody(t, response.Body)
	events := readAllFailoverSSE(t, response.Body)
	requireFailoverSequence(t, events)
	requireFailoverRefusal(t, events, "", "unsupported_continuation_shape")
}

func TestFailoverHTTPRefusesToolCallBufferedDuringContinuation(t *testing.T) {
	originServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		startFailoverBackendSSE(writer)
		writeFailoverBackendData(writer, failoverChatChunk("origin", "Checking ", "", nil))
	}))
	t.Cleanup(originServer.Close)
	const toolFragment = `{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call-1","type":"function","function":{"name":"lookup_weather","arguments":"{\"city\":"}}]},"finish_reason":null}]}`
	intermediateServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		startFailoverBackendSSE(writer)
		writeFailoverBackendData(writer, toolFragment)
	}))
	t.Cleanup(intermediateServer.Close)
	var lastCalls atomic.Int64
	lastServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		lastCalls.Add(1)
		startFailoverBackendSSE(writer)
		writeFailoverBackendData(writer, doneSentinelData)
	}))
	t.Cleanup(lastServer.Close)
	origin := newFailoverBackend(t, "a-origin.test:8000", originServer.URL, "model-v1", conformance.VerdictSafe)
	intermediate := newFailoverBackend(t, "b-intermediate.test:8000", intermediateServer.URL, "model-v1", conformance.VerdictSafe)
	last := newFailoverBackend(t, "c-last.test:8000", lastServer.URL, "model-v1", conformance.VerdictSafe)
	harness := newFailoverHTTPHarness(t, originServer.URL, newFailoverBackendPool(t, origin, intermediate, last), nil)
	request := newFailoverHTTPRequest(t, http.MethodPost, harness.url+"/v1/chat/completions",
		`{"model":"test-model","stream":true,"messages":[{"role":"user","content":"weather"}],"max_tokens":100}`)
	request.Header.Set(headerVerbose, "1")
	response := doFailoverHTTPRequest(t, harness.client, request)
	defer closeFailoverBody(t, response.Body)
	events := readAllFailoverSSE(t, response.Body)
	requireFailoverSequence(t, events)
	requireFailoverRefusal(t, events, "", "tool_call_boundary")
	fragments := 0
	for _, event := range events {
		if string(event.Data) == toolFragment {
			fragments++
		}
	}
	if fragments != 1 {
		t.Fatalf("received %d tool fragments, want exactly one", fragments)
	}
	if calls := lastCalls.Load(); calls != 0 {
		t.Fatalf("another continuation was dispatched %d times across a tool-call boundary", calls)
	}
}
