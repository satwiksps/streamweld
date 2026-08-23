package chaos

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/satwiksps/streamweld/internal/proxy/sse"
)

func TestWaitRemoteDurabilityProvesCreateResumeAndCompletion(t *testing.T) {
	t.Parallel()

	calls := 0
	var rejectedBody *trackingReadCloser
	var idempotencyKeys []string
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		calls++
		if request.Method == http.MethodPost {
			idempotencyKeys = append(idempotencyKeys, request.Header.Get("X-Streamweld-Idempotency-Key"))
		}
		switch calls {
		case 1:
			rejectedBody = newTrackingReadCloser("data: [DONE]\n\n")
			response := chaosHTTPResponse(request, http.StatusOK, http.Header{
				"X-Streamweld-Durability": []string{"degraded"},
			}, "")
			response.Body = rejectedBody
			return response, nil
		case 2:
			return chaosHTTPResponse(request, http.StatusOK, http.Header{
				"X-Streamweld-Durability": []string{"durable"},
				"X-Streamweld-Stream-Id":  []string{"canary-stream"},
			}, strings.Join([]string{
				"id: 1\nevent: streamweld.stream.open\ndata: {}\n\n",
				"id: 2\ndata: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"token-000 \"}}]}\n\n",
			}, "")), nil
		case 3:
			if request.Method != http.MethodGet || request.Header.Get("Last-Event-ID") != "2" {
				t.Fatalf("canary resume request = %s Last-Event-ID %q", request.Method, request.Header.Get("Last-Event-ID"))
			}
			var body strings.Builder
			for token := 1; token < 8; token++ {
				_, _ = fmt.Fprintf(
					&body,
					"id: %d\ndata: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"token-%03d \"}}]}\n\n",
					token+2,
					token,
				)
			}
			_, _ = fmt.Fprint(&body, "id: 10\nevent: streamweld.stream.done\ndata: {}\n\n")
			return chaosHTTPResponse(request, http.StatusOK, nil, body.String()), nil
		default:
			t.Fatalf("unexpected durability canary request %d: %s %s", calls, request.Method, request.URL)
			return nil, nil
		}
	})}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := waitRemoteDurability(ctx, client, "http://proxy.example.test", time.Millisecond); err != nil {
		t.Fatalf("waitRemoteDurability() error = %v", err)
	}
	if calls != 3 {
		t.Fatalf("durability canary requests = %d, want 3", calls)
	}
	if rejectedBody == nil || !rejectedBody.readToEOF || !rejectedBody.closed {
		t.Fatalf("rejected response lifecycle = %#v, want EOF and Close", rejectedBody)
	}
	if len(idempotencyKeys) != 2 || idempotencyKeys[0] == "" || idempotencyKeys[1] == "" ||
		idempotencyKeys[0] == idempotencyKeys[1] {
		t.Fatalf("canary idempotency keys = %q, want two distinct nonempty values", idempotencyKeys)
	}
}

func TestWaitRemoteDurabilityRetriesLifecycleDegradation(t *testing.T) {
	t.Parallel()

	calls := 0
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		calls++
		switch calls {
		case 1, 3:
			streamID := fmt.Sprintf("canary-stream-%d", calls)
			return chaosHTTPResponse(request, http.StatusOK, http.Header{
				"X-Streamweld-Durability": []string{"durable"},
				"X-Streamweld-Stream-Id":  []string{streamID},
			}, "id: 1\ndata: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"token-000 \"}}]}\n\n"), nil
		case 2, 4:
			var body strings.Builder
			if calls == 2 {
				_, _ = fmt.Fprint(&body, "id: 2\nevent: streamweld.stream.warning\ndata: {\"code\":\"journal_degraded\"}\n\n")
			}
			for token := 1; token < 8; token++ {
				_, _ = fmt.Fprintf(
					&body,
					"id: %d\ndata: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"token-%03d \"}}]}\n\n",
					token+2,
					token,
				)
			}
			_, _ = fmt.Fprint(&body, "id: 10\nevent: streamweld.stream.done\ndata: {}\n\n")
			return chaosHTTPResponse(request, http.StatusOK, nil, body.String()), nil
		default:
			t.Fatalf("unexpected durability canary request %d: %s %s", calls, request.Method, request.URL)
			return nil, nil
		}
	})}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := waitRemoteDurability(ctx, client, "http://proxy.example.test", time.Millisecond); err != nil {
		t.Fatalf("waitRemoteDurability() error = %v", err)
	}
	if calls != 4 {
		t.Fatalf("durability canary requests = %d, want degraded lifecycle plus durable retry", calls)
	}
}

func TestWaitRemoteDurabilityPreservesLastFailureAtDeadline(t *testing.T) {
	t.Parallel()

	calls := 0
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		calls++
		return chaosHTTPResponse(request, http.StatusOK, http.Header{
			"X-Streamweld-Durability": []string{"degraded"},
		}, "data: [DONE]\n\n"), nil
	})}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := waitRemoteDurability(ctx, client, "http://proxy.example.test", time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "wait for end-to-end durable proxy recovery") ||
		!strings.Contains(err.Error(), "response is not durable") {
		t.Fatalf("waitRemoteDurability() error = %v", err)
	}
	if calls < 2 {
		t.Fatalf("durability canary requests = %d, want retries", calls)
	}
}

func chaosHTTPResponse(
	request *http.Request,
	status int,
	header http.Header,
	body string,
) *http.Response {
	if header == nil {
		header = make(http.Header)
	}
	return &http.Response{
		StatusCode: status,
		Status:     fmt.Sprintf("%d %s", status, http.StatusText(status)),
		Header:     header,
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    request,
	}
}

func TestWaitRemoteSlowConsumerProductionWaitsForEveryTerminal(t *testing.T) {
	t.Parallel()

	calls := map[string]int{}
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		streamID := strings.TrimPrefix(request.URL.Path, "/v1/streams/")
		calls[streamID]++
		status := "open"
		if calls[streamID] >= 2 {
			status = "done"
		}
		return chaosHTTPResponse(
			request,
			http.StatusOK,
			nil,
			fmt.Sprintf(`{"status":%q,"resumable":true}`, status),
		), nil
	})}
	streams := []*attachedStream{{id: "slow-a"}, {id: "slow-b"}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := waitRemoteSlowConsumerProduction(
		ctx, client, "http://proxy.example.test", streams, time.Millisecond,
	); err != nil {
		t.Fatalf("waitRemoteSlowConsumerProduction() error = %v", err)
	}
	for _, stream := range streams {
		if calls[stream.id] < 2 {
			t.Errorf("state calls for %s = %d, want at least 2", stream.id, calls[stream.id])
		}
	}
}

func TestWaitRemoteSlowConsumerProductionRejectsUnexpectedTerminal(t *testing.T) {
	t.Parallel()

	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return chaosHTTPResponse(
			request,
			http.StatusOK,
			nil,
			`{"status":"error","resumable":true}`,
		), nil
	})}
	err := waitRemoteSlowConsumerProduction(
		context.Background(),
		client,
		"http://proxy.example.test",
		[]*attachedStream{{id: "slow-error"}},
		time.Millisecond,
	)
	if err == nil || !strings.Contains(err.Error(), `unexpected state "error"`) {
		t.Fatalf("waitRemoteSlowConsumerProduction() error = %v", err)
	}
}

type trackingReadCloser struct {
	reader    *strings.Reader
	readToEOF bool
	closed    bool
}

func newTrackingReadCloser(body string) *trackingReadCloser {
	return &trackingReadCloser{reader: strings.NewReader(body)}
}

func (body *trackingReadCloser) Read(buffer []byte) (int, error) {
	read, err := body.reader.Read(buffer)
	if err == io.EOF {
		body.readToEOF = true
	}
	return read, err
}

func (body *trackingReadCloser) Close() error {
	body.closed = true
	return nil
}

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

func TestFinishRemoteStreamRejectsUnexpectedReaderLag(t *testing.T) {
	t.Parallel()

	body := "event: streamweld.reader.error\ndata: {\"code\":\"reader_lag_exceeded\"}\n\n"
	stream := &attachedStream{
		id:       "stream-unexpected-lag",
		response: &http.Response{Body: io.NopCloser(strings.NewReader(body))},
		decoder:  sse.NewDecoder(strings.NewReader(body)),
	}
	err := finishRemoteStream(
		context.Background(), http.DefaultClient, "http://proxy.example.test", ScenarioRedisDown, time.Millisecond, stream,
	)
	if err == nil || !strings.Contains(err.Error(), "unexpectedly received reader-lag eviction during redis-down") {
		t.Fatalf("finishRemoteStream() error = %v", err)
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
