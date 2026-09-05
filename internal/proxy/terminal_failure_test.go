package proxy

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/satwiksps/streamweld/internal/backend"
	"github.com/satwiksps/streamweld/internal/conformance"
	"github.com/satwiksps/streamweld/internal/proxy/sse"
)

func TestFailoverHTTPDoesNotContinueCompletedGeneration(t *testing.T) {
	tests := []struct {
		name         string
		requestExtra string
		finishReason string
		wantFinish   string
		drain        bool
		noTarget     bool
		omitUsage    bool
		noMigrations bool
	}{
		{name: "max tokens exhausted", requestExtra: `,"max_tokens":2`, wantFinish: "length"},
		{name: "completion tokens exhausted", requestExtra: `,"max_completion_tokens":2`, wantFinish: "length"},
		{name: "either supplied limit exhausted", requestExtra: `,"max_tokens":2,"max_completion_tokens":5`, wantFinish: "length"},
		{name: "upstream finished before done", requestExtra: `,"max_tokens":10`, finishReason: "stop", wantFinish: "stop"},
		{name: "upstream reason preserved at limit", requestExtra: `,"max_tokens":2`, finishReason: "stop", wantFinish: "stop"},
		{name: "upstream finished without usage", finishReason: "stop", wantFinish: "stop", omitUsage: true},
		{name: "drain after exhausted budget", requestExtra: `,"max_tokens":2`, wantFinish: "length", drain: true},
		{name: "drain needs no replacement", requestExtra: `,"max_tokens":2`, wantFinish: "length", drain: true, noTarget: true},
		{name: "drain needs no migration allowance", requestExtra: `,"max_tokens":2`, wantFinish: "length", drain: true, noMigrations: true},
		{name: "finished drain needs no migration allowance", requestExtra: `,"max_tokens":10`, finishReason: "stop", wantFinish: "stop", drain: true, noMigrations: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			originServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				_, _ = io.Copy(io.Discard, request.Body)
				startFailoverBackendSSE(writer)
				for index, piece := range []string{"one ", "two "} {
					usage := &failoverUsage{CompletionTokens: uint64(index + 1), TotalTokens: uint64(index + 1)}
					if test.omitUsage {
						usage = nil
					}
					finishReason := ""
					if index == 1 {
						finishReason = test.finishReason
					}
					writeFailoverBackendData(writer, failoverChatChunk("origin", piece, finishReason, usage))
				}
				// The final generation token has been accepted, but the transport
				// closes before the compatibility [DONE] event arrives.
				if test.drain {
					<-request.Context().Done()
				}
			}))
			t.Cleanup(originServer.Close)
			var targetCalls atomic.Int64
			targetServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				targetCalls.Add(1)
				startFailoverBackendSSE(writer)
				writeFailoverBackendData(writer, failoverChatChunk("target", "three ", "stop", &failoverUsage{CompletionTokens: 1, TotalTokens: 1}))
				writeFailoverBackendData(writer, doneSentinelData)
			}))
			t.Cleanup(targetServer.Close)
			origin := newFailoverBackend(t, "a-origin:8000", originServer.URL, "model-v1", conformance.VerdictSafe)
			target := newFailoverBackend(t, "b-target:8000", targetServer.URL, "model-v1", conformance.VerdictSafe)
			backends := []backend.Backend{origin}
			if !test.noTarget {
				backends = append(backends, target)
			}
			harness := newFailoverHTTPHarness(t, originServer.URL, newFailoverBackendPool(t, backends...), func(config *Config) {
				if test.noMigrations {
					config.MaxMigrations = 0
				}
			})
			request := newFailoverHTTPRequest(t, http.MethodPost, harness.url+"/v1/chat/completions",
				`{"model":"test-model","stream":true,"messages":[{"role":"user","content":"count"}]`+test.requestExtra+`}`)
			request.Header.Set(headerVerbose, "1")
			response := doFailoverHTTPRequest(t, harness.client, request)
			defer closeFailoverBody(t, response.Body)
			decoder := sse.NewDecoder(response.Body)
			var events []sse.Event
			if test.drain {
				for range 3 {
					events = append(events, readNextFailoverSSE(t, decoder))
				}
				drainURL := harness.url + "/internal/backends/" + url.PathEscape(origin.ID.String()) + "/drain?timeout=2s"
				drainResponse := doFailoverHTTPRequest(t, harness.client, newFailoverHTTPRequest(t, http.MethodPost, drainURL, ""))
				defer closeFailoverBody(t, drainResponse.Body)
				body := readFailoverBody(t, drainResponse.Body)
				if drainResponse.StatusCode != http.StatusOK {
					t.Fatalf("drain status = %d, body = %q", drainResponse.StatusCode, body)
				}
				var drain struct {
					InFlight int `json:"in_flight"`
				}
				decodeFailoverJSON(t, body, &drain)
				if drain.InFlight != 0 {
					t.Fatalf("in-flight generations after drain = %d, want 0", drain.InFlight)
				}
			}
			events = append(events, readRemainingFailoverSSE(t, decoder)...)
			if calls := targetCalls.Load(); calls != 0 {
				t.Errorf("continuation requests = %d, want none after generation completed", calls)
			}
			if text := failoverText(events); text != "one two " {
				t.Errorf("downstream text = %q, want %q", text, "one two ")
			}
			requireFailoverSequence(t, events)
			requireFailoverDone(t, events)
			var done struct {
				FinishReason string     `json:"finish_reason"`
				Usage        tokenUsage `json:"usage"`
			}
			decodeFailoverJSON(t, events[failoverEventIndex(events, streamDoneEvent)].Data, &done)
			wantTokens := uint64(2)
			if test.omitUsage {
				wantTokens = uint64(len("one two "))
			}
			if done.FinishReason != test.wantFinish || done.Usage.CompletionTokens != wantTokens || done.Usage.Estimated != test.omitUsage {
				t.Errorf("terminal result = %+v, want finish_reason %q, tokens %d, estimated %t", done, test.wantFinish, wantTokens, test.omitUsage)
			}
			if failoverEventIndex(events, streamMigrationEvent) >= 0 {
				t.Error("completed generation emitted a migration event")
			}
		})
	}
}

func TestFailoverHTTPDoesNotInferUnsafeCompletion(t *testing.T) {
	tests := []struct {
		name         string
		requestExtra string
		requestBody  string
		endpoint     string
		chunk        string
		warning      string
	}{
		{
			name:    "estimated exhausted budget",
			chunk:   failoverChatChunk("origin", "one two ", "", nil),
			warning: "token_budget_exhausted",
		},
		{
			name:    "exact budget inside fragmented tool call",
			chunk:   `{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call-1","type":"function","function":{"name":"lookup","arguments":"{\"key\":"}}]},"finish_reason":null}],"usage":{"completion_tokens":2,"total_tokens":2}}`,
			warning: "tool_call_boundary",
		},
		{
			name:         "invalid structured output marked finished",
			requestExtra: `,"response_format":{"type":"json_object"}`,
			chunk:        failoverChatChunk("origin", "}", "stop", &failoverUsage{CompletionTokens: 2, TotalTokens: 2}),
			warning:      "structured_prefix_invalid",
		},
		{
			name:        "only first legacy batched prompt finished",
			requestBody: `{"model":"test-model","stream":true,"prompt":["a","b"],"max_tokens":2}`,
			endpoint:    "/v1/completions",
			chunk:       `{"choices":[{"index":0,"text":"one two ","finish_reason":"stop"},{"index":1,"text":"unfinished","finish_reason":null}],"usage":{"completion_tokens":2,"total_tokens":2}}`,
			warning:     "unsupported_continuation_shape",
		},
		{
			name:         "only first of multiple choices finished",
			requestExtra: `,"n":2`,
			chunk:        `{"choices":[{"index":0,"delta":{"content":"one two "},"finish_reason":"stop"},{"index":1,"delta":{"content":"unfinished"},"finish_reason":null}],"usage":{"completion_tokens":2,"total_tokens":2}}`,
			warning:      "unsupported_continuation_shape",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			originServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				startFailoverBackendSSE(writer)
				writeFailoverBackendData(writer, test.chunk)
			}))
			t.Cleanup(originServer.Close)
			var targetCalls atomic.Int64
			targetServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				targetCalls.Add(1)
				startFailoverBackendSSE(writer)
				writeFailoverBackendData(writer, failoverChatChunk("target", "three ", "stop", nil))
				writeFailoverBackendData(writer, doneSentinelData)
			}))
			t.Cleanup(targetServer.Close)
			origin := newFailoverBackend(t, "a-origin:8000", originServer.URL, "model-v1", conformance.VerdictSafe)
			target := newFailoverBackend(t, "b-target:8000", targetServer.URL, "model-v1", conformance.VerdictSafe)
			pool := newFailoverBackendPool(t, origin, target)
			harness := newFailoverHTTPHarness(t, originServer.URL, pool, nil)
			endpoint, body := test.endpoint, test.requestBody
			if endpoint == "" {
				endpoint = "/v1/chat/completions"
			}
			if body == "" {
				body = `{"model":"test-model","stream":true,"messages":[{"role":"user","content":"count"}],"max_tokens":2` + test.requestExtra + `}`
			}
			request := newFailoverHTTPRequest(t, http.MethodPost, harness.url+endpoint, body)
			request.Header.Set(headerVerbose, "1")
			response := doFailoverHTTPRequest(t, harness.client, request)
			defer closeFailoverBody(t, response.Body)
			events := readAllFailoverSSE(t, response.Body)
			if calls := targetCalls.Load(); calls != 0 {
				t.Errorf("unsafe continuation requests = %d, want none", calls)
			}
			requireFailoverSequence(t, events)
			requireFailoverRefusal(t, events, "", test.warning)
			state, err := pool.Get(origin.ID)
			if err != nil || !state.Quarantined {
				t.Errorf("failed backend quarantine = %t, error = %v; want quarantine after passive failure", state.Quarantined, err)
			}
		})
	}
}

func TestFailoverHTTPCompletesBufferedContinuationWithoutAnotherAttempt(t *testing.T) {
	for _, finishReason := range []string{"", "stop"} {
		name, limit, wantFinish := "exhausted budget", uint64(2), "length"
		if finishReason != "" {
			name, limit, wantFinish = "upstream finish reason", 10, finishReason
		}
		t.Run(name, func(t *testing.T) {
			originServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				startFailoverBackendSSE(writer)
				writeFailoverBackendData(writer, failoverChatChunk("origin", "one ", "", &failoverUsage{CompletionTokens: 1, TotalTokens: 1}))
			}))
			t.Cleanup(originServer.Close)
			targetRequests := make(chan failoverBackendRequest, 1)
			targetServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				targetRequests <- captureFailoverBackendRequest(request)
				startFailoverBackendSSE(writer)
				writeFailoverBackendData(writer, failoverChatChunk("target", "two ", finishReason, &failoverUsage{CompletionTokens: 1, TotalTokens: 1}))
				// This complete frame is shorter than the seam window and must be
				// accepted before deciding whether generation needs another attempt.
			}))
			t.Cleanup(targetServer.Close)
			var lastCalls atomic.Int64
			lastServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				lastCalls.Add(1)
				startFailoverBackendSSE(writer)
				writeFailoverBackendData(writer, failoverChatChunk("last", "three ", "stop", &failoverUsage{CompletionTokens: 1, TotalTokens: 1}))
				writeFailoverBackendData(writer, doneSentinelData)
			}))
			t.Cleanup(lastServer.Close)
			origin := newFailoverBackend(t, "a-origin:8000", originServer.URL, "model-v1", conformance.VerdictSafe)
			target := newFailoverBackend(t, "b-target:8000", targetServer.URL, "model-v1", conformance.VerdictSafe)
			last := newFailoverBackend(t, "c-last:8000", lastServer.URL, "model-v1", conformance.VerdictSafe)
			harness := newFailoverHTTPHarness(t, originServer.URL, newFailoverBackendPool(t, origin, target, last), nil)
			request := newFailoverHTTPRequest(t, http.MethodPost, harness.url+"/v1/chat/completions",
				`{"model":"test-model","stream":true,"messages":[{"role":"user","content":"count"}],"max_tokens":`+strconv.FormatUint(limit, 10)+`}`)
			request.Header.Set(headerVerbose, "1")
			response := doFailoverHTTPRequest(t, harness.client, request)
			defer closeFailoverBody(t, response.Body)
			events := readAllFailoverSSE(t, response.Body)
			requireFailoverSequence(t, events)
			requireFailoverDone(t, events)
			if calls := lastCalls.Load(); calls != 0 {
				t.Errorf("requests after completed continuation = %d, want none", calls)
			}
			if text := failoverText(events); text != "one two " {
				t.Errorf("accepted text = %q, want one two", text)
			}
			var body struct {
				MaxTokens uint64 `json:"max_tokens"`
			}
			captured := awaitFailoverValue(t, targetRequests, "continuation request")
			decodeFailoverJSON(t, captured.body, &body)
			if captured.readErr != nil || body.MaxTokens != limit-1 {
				t.Errorf("continuation remaining limit = %d, error = %v; want %d", body.MaxTokens, captured.readErr, limit-1)
			}
			var done struct {
				FinishReason string     `json:"finish_reason"`
				Usage        tokenUsage `json:"usage"`
			}
			decodeFailoverJSON(t, events[failoverEventIndex(events, streamDoneEvent)].Data, &done)
			if done.FinishReason != wantFinish || done.Usage.CompletionTokens != 2 || done.Usage.Estimated {
				t.Errorf("completed continuation = %+v, want %q with exactly 2 tokens", done, wantFinish)
			}
			replayRequest := newFailoverHTTPRequest(t, http.MethodGet,
				harness.url+"/v1/streams/"+response.Header.Get(headerStreamID)+"/events", "")
			replayRequest.Header.Set(headerVerbose, "1")
			replay := doFailoverHTTPRequest(t, harness.client, replayRequest)
			defer closeFailoverBody(t, replay.Body)
			if replayed := readAllFailoverSSE(t, replay.Body); !reflect.DeepEqual(events, replayed) {
				t.Errorf("replayed events differ from accepted events: got %#v, want %#v", replayed, events)
			}
		})
	}
}
