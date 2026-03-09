package proxy

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/streamweld/streamweld/internal/journal"
	"github.com/streamweld/streamweld/internal/proxy/sse"
)

func TestPhase5JournalDisappearanceCompletesLiveStreamAndSealsResumeGap(t *testing.T) {
	var backendCalls atomic.Int64
	backend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		backendCalls.Add(1)
		_, _ = io.Copy(io.Discard, request.Body)
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

	memoryConfig := journal.DefaultConfig()
	memoryConfig.MaxTotalBytes = 8 << 20
	memoryConfig.ReaderMaxLagBytes = 1 << 20
	committedStore, err := journal.NewMemory(memoryConfig)
	if err != nil {
		t.Fatalf("journal.NewMemory() error = %v", err)
	}
	sharedJournal := &phase5FailingJournal{
		Memory: committedStore,
		failAt: 2,
		err:    errors.New("injected Redis disappearance"),
	}
	sharedIdempotency := journal.NewMemoryIdempotencyRegistry(nil)
	replicaA := newPhase5SharedReplica(t, backend.URL, sharedJournal, sharedIdempotency)
	replicaB := newPhase5SharedReplica(t, backend.URL, sharedJournal, sharedIdempotency)

	const idempotencyKey = "phase5-degraded-generation"
	initialRequest := newDurableHTTPRequest(
		t,
		http.MethodPost,
		replicaA.url+"/v1/chat/completions",
		`{"model":"phase5-model","stream":true,"messages":[]}`,
	)
	initialRequest.Header.Set(headerIdempotency, idempotencyKey)
	initialResponse := doDurableHTTPRequest(t, replicaA.client, initialRequest)
	defer closeDurableHTTPBody(t, initialResponse.Body)
	if initialResponse.StatusCode != http.StatusOK {
		t.Fatalf("initial status = %d, body = %q", initialResponse.StatusCode, readDurableHTTPBody(t, initialResponse.Body))
	}
	streamID := requireDurableResponseHeaders(t, initialResponse)
	events := readAllDurableSSE(t, initialResponse.Body)
	if len(events) != 5 {
		t.Fatalf("degraded live event count = %d, want open, committed chunk, warning, unsequenced chunk, sentinel: %#v", len(events), events)
	}
	requireDurableSSEEvent(t, events[0], "1", streamOpenEvent, "")
	requireDurableSSEEvent(t, events[1], "2", "", chatChunkHello)
	requirePhase5UnsequencedEvent(t, events[2], streamWarningEvent, `"code":"journal_degraded"`)
	requirePhase5UnsequencedEvent(t, events[3], "", chatChunkWorld)
	requirePhase5UnsequencedEvent(t, events[4], "", doneSentinelData)

	if got := phase5ChunkText(t, events[1].Data) + phase5ChunkText(t, events[3].Data); got != "hello world" {
		t.Fatalf("live degraded output text = %q, want %q", got, "hello world")
	}
	if calls := backendCalls.Load(); calls != 1 {
		t.Fatalf("upstream calls after degraded completion = %d, want 1", calls)
	}

	state, err := committedStore.State(context.Background(), streamID)
	if err != nil {
		t.Fatalf("committed store State() error = %v", err)
	}
	if state.LastSeq != 2 || state.Status != journal.StatusOpen || state.Terminal != nil {
		t.Fatalf("committed prefix state = %#v, want open journal sealed after sequence 2", state)
	}

	atGapRequest := newDurableHTTPRequest(
		t,
		http.MethodGet,
		replicaB.url+"/v1/streams/"+streamID.String()+"/events",
		"",
	)
	atGapRequest.Header.Set("Last-Event-ID", "2")
	atGapResponse := doDurableHTTPRequest(t, replicaB.client, atGapRequest)
	defer func() { _ = atGapResponse.Body.Close() }()
	requireDurableStreamError(t, atGapResponse, http.StatusGone, "stream_offset_expired", streamID.String())

	prefixRequest := newDurableHTTPRequest(
		t,
		http.MethodGet,
		replicaB.url+"/v1/streams/"+streamID.String()+"/events",
		"",
	)
	prefixResponse := doDurableHTTPRequest(t, replicaB.client, prefixRequest)
	defer closeDurableHTTPBody(t, prefixResponse.Body)
	if prefixResponse.StatusCode != http.StatusOK {
		t.Fatalf("committed-prefix replay status = %d, body = %q", prefixResponse.StatusCode, readDurableHTTPBody(t, prefixResponse.Body))
	}
	if replayedID := requireDurableResponseHeaders(t, prefixResponse); replayedID != streamID {
		t.Fatalf("committed-prefix stream ID = %s, want %s", replayedID, streamID)
	}
	prefixEvents := readAllDurableSSE(t, prefixResponse.Body)
	if len(prefixEvents) != 3 {
		t.Fatalf("committed-prefix events = %#v, want open, chunk, terminal transport error", prefixEvents)
	}
	requireDurableSSEEvent(t, prefixEvents[0], "1", streamOpenEvent, "")
	requireDurableSSEEvent(t, prefixEvents[1], "2", "", chatChunkHello)
	requirePhase5UnsequencedEvent(t, prefixEvents[2], streamErrorEvent, `"code":"stream_offset_expired"`)

	duplicateRequest := newDurableHTTPRequest(
		t,
		http.MethodPost,
		replicaB.url+"/v1/chat/completions",
		`{"model":"must-not-restart","stream":true,"messages":[]}`,
	)
	duplicateRequest.Header.Set(headerIdempotency, idempotencyKey)
	duplicateResponse := doDurableHTTPRequest(t, replicaB.client, duplicateRequest)
	defer closeDurableHTTPBody(t, duplicateResponse.Body)
	if duplicateResponse.StatusCode != http.StatusOK {
		t.Fatalf("degraded idempotent replay status = %d, body = %q", duplicateResponse.StatusCode, readDurableHTTPBody(t, duplicateResponse.Body))
	}
	if duplicateID := requireDurableResponseHeaders(t, duplicateResponse); duplicateID != streamID {
		t.Fatalf("degraded duplicate stream ID = %s, want %s", duplicateID, streamID)
	}
	duplicateEvents := readAllDurableSSE(t, duplicateResponse.Body)
	if len(duplicateEvents) != 3 {
		t.Fatalf("degraded idempotent events = %#v, want committed prefix and gap error", duplicateEvents)
	}
	requirePhase5UnsequencedEvent(t, duplicateEvents[2], streamErrorEvent, `"code":"stream_offset_expired"`)
	if calls := backendCalls.Load(); calls != 1 {
		t.Fatalf("degraded idempotent replay restarted upstream: calls = %d, want 1", calls)
	}
}

type phase5FailingJournal struct {
	*journal.Memory
	failAt int64
	err    error
	calls  atomic.Int64
}

var _ journal.Journal = (*phase5FailingJournal)(nil)
var _ journal.DegradationMarker = (*phase5FailingJournal)(nil)

func (backend *phase5FailingJournal) Append(
	ctx context.Context,
	id journal.StreamID,
	entry journal.Entry,
) (uint64, error) {
	if backend.calls.Add(1) == backend.failAt {
		return 0, backend.err
	}
	return backend.Memory.Append(ctx, id, entry)
}

func requirePhase5UnsequencedEvent(t *testing.T, event sse.Event, eventType, dataFragment string) {
	t.Helper()
	if event.HasID {
		t.Fatalf("event unexpectedly claimed journal sequence %q: %#v", event.ID, event)
	}
	if eventType == "" {
		if event.HasType {
			t.Fatalf("event type = %q, want omitted: %#v", event.Type, event)
		}
	} else if !event.HasType || event.Type != eventType {
		t.Fatalf("event type = (%v, %q), want (true, %q)", event.HasType, event.Type, eventType)
	}
	if !bytes.Contains(event.Data, []byte(dataFragment)) {
		t.Fatalf("event data = %q, want fragment %q", event.Data, dataFragment)
	}
}

func phase5ChunkText(t *testing.T, data []byte) string {
	t.Helper()
	observation, err := observeOpenAIChunk(data)
	if err != nil {
		t.Fatalf("observeOpenAIChunk(%q) error = %v", data, err)
	}
	return observation.TextDelta
}
