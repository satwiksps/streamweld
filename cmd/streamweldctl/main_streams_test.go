package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/satwiksps/streamweld/internal/journal"
)

const streamsTestID = "01arz3ndektsv4rrffq69g5fav"

func TestStreamsFetchesAndPrintsHumanState(t *testing.T) {
	t.Parallel()
	state := streamsTestState()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet || request.URL.EscapedPath() != "/gateway/v1/streams/"+streamsTestID {
			t.Errorf("request = %s %s", request.Method, request.URL.EscapedPath())
			http.NotFound(writer, request)
			return
		}
		if got := request.Header.Get("Accept"); got != "application/json" {
			t.Errorf("Accept = %q, want application/json", got)
		}
		writer.Header().Set("Content-Type", "application/json; charset=utf-8")
		_ = json.NewEncoder(writer).Encode(state)
	}))
	t.Cleanup(server.Close)

	var stdout, stderr bytes.Buffer
	code := run([]string{"streams", "--endpoint", server.URL + "/gateway/", streamsTestID}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("run(streams) = %d, stderr=%q", code, stderr.String())
	}
	for _, want := range []string{
		"Stream: " + streamsTestID,
		"Status: done",
		"Resumable: true",
		"Model: fixture-model",
		"Backend: backend-a -> backend-b",
		"Usage: prompt=2 completion=3 total=5 estimated=false",
		"seq=3 attempt=2 backend-a -> backend-b reason=tcp_reset rescued_tokens=2 estimated=false",
		"Terminal: done at seq=4",
	} {
		if !strings.Contains(stdout.String(), want) {
			t.Errorf("stdout does not contain %q: %q", want, stdout.String())
		}
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestStreamsJSONIsCompleteState(t *testing.T) {
	t.Parallel()
	state := streamsTestState()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(state)
	}))
	t.Cleanup(server.Close)

	var stdout, stderr bytes.Buffer
	code := run([]string{"streams", "--endpoint", server.URL, "--json", streamsTestID}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("run(streams --json) = %d, stderr=%q", code, stderr.String())
	}
	var got journal.StreamState
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatalf("decode JSON output %q: %v", stdout.String(), err)
	}
	if got.StreamID != state.StreamID || got.Status != state.Status || got.Terminal == nil ||
		got.Terminal.Kind != journal.KindDone || len(got.Migrations) != 1 {
		t.Fatalf("JSON state = %#v, want complete state %#v", got, state)
	}
}

func TestStreamsSurfacesStructuredProxyError(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusGone)
		_, _ = writer.Write([]byte(`{"error":{"type":"streamweld_error","code":"stream_expired","message":"stream journal has expired","stream_id":"` + streamsTestID + `"}}`))
	}))
	t.Cleanup(server.Close)

	var stdout, stderr bytes.Buffer
	code := run([]string{"streams", "--endpoint", server.URL, streamsTestID}, &stdout, &stderr)
	if code != 1 || stdout.Len() != 0 {
		t.Fatalf("run(streams expired) = %d, stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	if got := stderr.String(); !strings.Contains(got, "410 Gone (stream_expired): stream journal has expired") {
		t.Fatalf("stderr = %q", got)
	}
}

func TestStreamsDoesNotFollowRedirects(t *testing.T) {
	t.Parallel()
	var followed atomic.Bool
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		followed.Store(true)
	}))
	t.Cleanup(target.Close)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Redirect(writer, request, target.URL, http.StatusTemporaryRedirect)
	}))
	t.Cleanup(server.Close)

	var stdout, stderr bytes.Buffer
	code := run([]string{"streams", "--endpoint", server.URL, streamsTestID}, &stdout, &stderr)
	if code != 1 || !strings.Contains(stderr.String(), "307 Temporary Redirect") {
		t.Fatalf("run(streams redirect) = %d, stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	if followed.Load() {
		t.Fatal("streams request followed a redirect")
	}
}

func TestStreamsRejectsUnsafeArgumentsLocally(t *testing.T) {
	t.Parallel()
	tests := [][]string{
		{"streams"},
		{"streams", streamsTestID, streamsTestID},
		{"streams", strings.ToUpper(streamsTestID)},
		{"streams", "--endpoint", "http://user:secret@example.test", streamsTestID},
		{"streams", "--endpoint", "http://example.test?token=secret", streamsTestID},
		{"streams", "--timeout", "0s", streamsTestID},
	}
	for _, args := range tests {
		var stdout, stderr bytes.Buffer
		if code := run(args, &stdout, &stderr); code != 2 {
			t.Errorf("run(%v) = %d, stdout=%q stderr=%q", args, code, stdout.String(), stderr.String())
		}
	}
}

func TestStreamsRejectsMalformedOrInconsistentState(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		body string
	}{
		{name: "multiple JSON values", body: `{}` + "\n" + `{}`},
		{name: "different stream ID", body: strings.Replace(string(mustJSON(streamsTestState())), streamsTestID, "01arz3ndektsv4rrffq69g5faw", 1)},
		{name: "terminal mismatch", body: strings.Replace(string(mustJSON(streamsTestState())), `"kind":"done"`, `"kind":"error"`, 1)},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				writer.Header().Set("Content-Type", "application/json")
				_, _ = writer.Write([]byte(test.body))
			}))
			t.Cleanup(server.Close)
			var stdout, stderr bytes.Buffer
			if code := run([]string{"streams", "--endpoint", server.URL, streamsTestID}, &stdout, &stderr); code != 1 {
				t.Fatalf("run(streams) = %d, stdout=%q stderr=%q", code, stdout.String(), stderr.String())
			}
			if stdout.Len() != 0 || !strings.Contains(stderr.String(), "proxy response") {
				t.Fatalf("stdout=%q stderr=%q", stdout.String(), stderr.String())
			}
		})
	}
}

func TestRootUsageIncludesStreams(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	if code := run([]string{"--help"}, &stdout, &stderr); code != 0 {
		t.Fatalf("run(--help) = %d, stderr=%q", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "streams  inspect one durable stream's state") {
		t.Fatalf("help does not include streams command: %q", stdout.String())
	}
}

func streamsTestState() journal.StreamState {
	version := "sha256:fixture"
	created := time.Date(2026, time.August, 22, 10, 15, 3, 221000000, time.UTC)
	updated := created.Add(6 * time.Second)
	return journal.StreamState{
		StreamID:       journal.StreamID(streamsTestID),
		Status:         journal.StatusDone,
		Resumable:      true,
		Model:          "fixture-model",
		ModelVersion:   &version,
		OriginBackend:  "backend-a",
		CurrentBackend: "backend-b",
		CreatedAt:      created,
		UpdatedAt:      updated,
		EarliestSeq:    1,
		LastSeq:        4,
		Usage: journal.Usage{
			PromptTokens: 2, CompletionTokens: 3, TotalTokens: 5,
		},
		Migrations: []journal.Migration{{
			Seq: 3, FromBackend: "backend-a", ToBackend: "backend-b",
			Reason: "tcp_reset", RescuedTokens: 2, Attempt: 2,
		}},
		Terminal: &journal.TerminalState{
			Seq: 4, TS: updated, Kind: journal.KindDone,
			Payload: json.RawMessage(`{"usage":{"prompt_tokens":2,"completion_tokens":3,"total_tokens":5,"estimated":false}}`),
		},
	}
}

func mustJSON(value any) []byte {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return encoded
}
