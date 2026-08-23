package chaos

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/satwiksps/streamweld/internal/proxy/sse"
)

func TestFinishRemoteStreamCountsReconnectsInsteadOfDecodedEvents(t *testing.T) {
	t.Parallel()

	initial := strings.Repeat("data: {\"choices\":[{\"index\":0,\"delta\":{}}]}\n\n", 4)
	stream := &attachedStream{
		id:       "stream-reconnect-after-progress",
		response: &http.Response{Body: io.NopCloser(strings.NewReader(initial))},
		decoder:  sse.NewDecoder(strings.NewReader(initial)),
	}
	resumeCalls := 0
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		resumeCalls++
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body: io.NopCloser(strings.NewReader(
				"event: streamweld.stream.done\ndata: {}\n\n",
			)),
			Request: request,
		}, nil
	})}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := finishRemoteStream(ctx, client, "http://proxy.example.test", ScenarioPodKill, time.Millisecond, stream); err != nil {
		t.Fatalf("finishRemoteStream() error = %v", err)
	}
	if resumeCalls != 1 || stream.terminal != "done" {
		t.Fatalf("resume result = (%d calls, terminal %q), want (1, done)", resumeCalls, stream.terminal)
	}
}

func TestFinishRemoteStreamBoundsActualReconnectAttempts(t *testing.T) {
	t.Parallel()

	stream := &attachedStream{
		id:       "stream-bounded-reconnect",
		response: &http.Response{Body: io.NopCloser(strings.NewReader(""))},
		decoder:  sse.NewDecoder(strings.NewReader("")),
	}
	resumeCalls := 0
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		resumeCalls++
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("")),
			Request:    request,
		}, nil
	})}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := finishRemoteStream(ctx, client, "http://proxy.example.test", ScenarioPodKill, time.Millisecond, stream)
	if err == nil || !strings.Contains(err.Error(), "after 3 resume attempts") {
		t.Fatalf("finishRemoteStream() error = %v", err)
	}
	if resumeCalls != 3 {
		t.Fatalf("resume calls = %d, want 3", resumeCalls)
	}
}

func TestFinishRemoteStreamRestoresReconnectBudgetAfterProgress(t *testing.T) {
	t.Parallel()

	stream := &attachedStream{
		id:       "stream-progress-reset",
		response: &http.Response{Body: io.NopCloser(strings.NewReader(""))},
		decoder:  sse.NewDecoder(strings.NewReader("")),
	}
	resumeCalls := 0
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		resumeCalls++
		body := ""
		if resumeCalls == 1 {
			body = "id: 1\ndata: {\"choices\":[{\"index\":0,\"delta\":{}}]}\n\n"
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(body)),
			Request:    request,
		}, nil
	})}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := finishRemoteStream(ctx, client, "http://proxy.example.test", ScenarioPodKill, time.Millisecond, stream)
	if err == nil || !strings.Contains(err.Error(), "after 3 resume attempts") {
		t.Fatalf("finishRemoteStream() error = %v", err)
	}
	if resumeCalls != 4 {
		t.Fatalf("resume calls = %d, want 4 after one progress reset", resumeCalls)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

func TestAttachedStreamObservesMigrationSeamAndPromptRebilling(t *testing.T) {
	t.Parallel()

	stream := &attachedStream{id: "stream-observed"}
	stream.output.WriteString("token-000 token-001 ")
	if err := stream.accept(sse.Event{
		Type:    "streamweld.stream.migration",
		Data:    []byte(`{"rescued_tokens":2}`),
		HasType: true,
		HasData: true,
	}); err != nil {
		t.Fatalf("accept migration: %v", err)
	}
	if err := stream.accept(sse.Event{
		Data: []byte(`{
			"choices":[{"index":0,"delta":{"content":""}}],
			"usage":{"prompt_tokens":16},
			"streamweld_chaos_raw_delta":"token-001 "
		}`),
		HasData: true,
	}); err != nil {
		t.Fatalf("accept continuation seam: %v", err)
	}
	if stream.migrated != 1 || stream.rescued != 2 || stream.promptRebilled != 16 ||
		len(stream.seamOverlaps) != 1 || stream.seamOverlaps[0] != len("token-001 ") {
		t.Fatalf("observed migration evidence = %+v", stream)
	}
	if got := stream.output.String(); got != "token-000 token-001 " {
		t.Fatalf("seam-reconciled output = %q", got)
	}
}

func TestAttachedStreamRejectsUnobservedMigrationEvidence(t *testing.T) {
	t.Parallel()

	stream := &attachedStream{id: "stream-missing"}
	stream.output.WriteString("token-000 ")
	if err := stream.accept(sse.Event{
		Type:    "streamweld.stream.migration",
		Data:    []byte(`{"rescued_tokens":1}`),
		HasType: true,
		HasData: true,
	}); err != nil {
		t.Fatalf("accept migration: %v", err)
	}
	err := stream.accept(sse.Event{
		Data:    []byte(`{"choices":[{"delta":{"content":"token-001 "}}]}`),
		HasData: true,
	})
	if err == nil || !strings.Contains(err.Error(), "omitted observed seam or prompt-usage metadata") {
		t.Fatalf("missing evidence error = %v", err)
	}
}

func TestAttachedStreamObservesReaderLagEviction(t *testing.T) {
	t.Parallel()

	stream := &attachedStream{id: "stream-slow"}
	err := stream.accept(sse.Event{
		Type:    "streamweld.reader.error",
		Data:    []byte(`{"code":"reader_lag_exceeded"}`),
		HasType: true,
		HasData: true,
	})
	if err != nil || !stream.readerLagged {
		t.Fatalf("reader lag observation = (%t, %v)", stream.readerLagged, err)
	}
}

func TestCompletionTerminalMustMatchScenarioOutcome(t *testing.T) {
	t.Parallel()

	redis, ok := definitionFor(ScenarioRedisDown)
	if !ok {
		t.Fatal("redis-down definition is missing")
	}
	canonical := "token-000 token-001 "
	if remoteOutputCorrect(redis, "done", canonical, canonical) {
		t.Fatal("plain done would incorrectly satisfy the redis-down degraded outcome")
	}
	if !remoteOutputCorrect(redis, "done_degraded", canonical, canonical) {
		t.Fatal("done_degraded did not satisfy the redis-down outcome")
	}
	for _, scenario := range []Scenario{ScenarioPodKill, ScenarioClientDrop, ScenarioSlowConsumer} {
		definition, found := definitionFor(scenario)
		if !found {
			t.Fatalf("%s definition is missing", scenario)
		}
		if definition.ExpectedOutcome != "done" {
			t.Fatalf("%s expected outcome = %q, want done", scenario, definition.ExpectedOutcome)
		}
		if remoteOutputCorrect(definition, "done_degraded", canonical, canonical) {
			t.Fatalf("%s accepted an unexpected degraded terminal", scenario)
		}
	}
}
