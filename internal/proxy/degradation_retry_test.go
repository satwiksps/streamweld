package proxy

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/streamweld/streamweld/internal/journal"
)

func TestJournalDegradationMarkerRetriesUntilRecovery(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = io.Copy(io.Discard, request.Body)
		startDurableBackendSSE(writer)
		_ = writeDurableBackendData(writer, chatChunkHello)
		_ = writeDurableBackendData(writer, chatChunkWorld)
		_ = writeDurableBackendData(writer, doneSentinelData)
	}))
	t.Cleanup(backend.Close)

	memoryConfig := journal.DefaultConfig()
	memoryConfig.MaxTotalBytes = 8 << 20
	memoryConfig.ReaderMaxLagBytes = 1 << 20
	committed, err := journal.NewMemory(memoryConfig)
	if err != nil {
		t.Fatalf("journal.NewMemory() error = %v", err)
	}
	backendJournal := &recoveringMarkerJournal{
		phase5FailingJournal: phase5FailingJournal{
			Memory: committed,
			failAt: 2,
			err:    errors.New("injected append outage"),
		},
		markerFailures: 2,
	}
	replica := newPhase5SharedReplica(
		t,
		backend.URL,
		backendJournal,
		journal.NewMemoryIdempotencyRegistry(nil),
	)

	request := newDurableHTTPRequest(
		t,
		http.MethodPost,
		replica.url+"/v1/chat/completions",
		`{"model":"phase5-model","stream":true,"messages":[]}`,
	)
	response := doDurableHTTPRequest(t, replica.client, request)
	defer func() {
		if err := response.Body.Close(); err != nil {
			t.Errorf("response body Close() error = %v", err)
		}
	}()
	streamID := requireDurableResponseHeaders(t, response)
	_ = readAllDurableSSE(t, response.Body)
	closeDurableHTTPBody(t, response.Body)

	deadline := time.Now().Add(3 * time.Second)
	for backendJournal.markerCalls.Load() < backendJournal.markerFailures+1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if calls := backendJournal.markerCalls.Load(); calls < backendJournal.markerFailures+1 {
		t.Fatalf("MarkDegraded() calls = %d, want at least %d", calls, backendJournal.markerFailures+1)
	}
	if _, _, err := committed.Tail(context.Background(), streamID, 2); !errors.Is(err, journal.ErrOffsetExpired) {
		t.Fatalf("Tail(at gap) error = %v, want ErrOffsetExpired after marker recovery", err)
	}
	if got := replica.server.durable.journalDegradedValue(); got != 1 {
		t.Fatalf("journal degraded gauge = %d, want 1", got)
	}
}

func TestPreOpenJournalFailureSetsDegradedGauge(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = io.Copy(io.Discard, request.Body)
		startDurableBackendSSE(writer)
		_ = writeDurableBackendData(writer, chatChunkHello)
		_ = writeDurableBackendData(writer, doneSentinelData)
	}))
	t.Cleanup(backend.Close)

	memoryConfig := journal.DefaultConfig()
	memoryConfig.MaxTotalBytes = 1 << 20
	memoryConfig.ReaderMaxLagBytes = 1 << 20
	memory, err := journal.NewMemory(memoryConfig)
	if err != nil {
		t.Fatalf("journal.NewMemory() error = %v", err)
	}
	config := DefaultConfig()
	config.BackendURL = backend.URL
	server, err := NewServer(config, nil, WithJournal(&openFailingJournal{Memory: memory}))
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	front := httptest.NewServer(server.Handler())
	t.Cleanup(func() {
		server.forceCancel()
		front.Close()
	})

	request := newDurableHTTPRequest(
		t,
		http.MethodPost,
		front.URL+"/v1/chat/completions",
		`{"model":"phase5-model","stream":true,"messages":[]}`,
	)
	response := doDurableHTTPRequest(t, http.DefaultClient, request)
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", response.StatusCode)
	}
	if got := response.Header.Get(headerDurability); got != durabilityDegraded {
		t.Fatalf("durability header = %q, want %q", got, durabilityDegraded)
	}
	if got := response.Header.Get(headerStreamID); got == "" {
		t.Fatal("degraded generation omitted its stream ID header")
	}
	if got := server.durable.journalDegradedValue(); got != 1 {
		t.Fatalf("journal degraded gauge = %d, want 1", got)
	}
	if events := readAllDurableSSE(t, response.Body); len(events) != 2 {
		t.Fatalf("passthrough event count = %d, want chunk and sentinel", len(events))
	}
}

type recoveringMarkerJournal struct {
	phase5FailingJournal
	markerFailures int64
	markerCalls    atomic.Int64
}

type openFailingJournal struct {
	*journal.Memory
}

func (backend *openFailingJournal) Open(context.Context, journal.StreamID, journal.Meta) error {
	return errors.New("injected open outage")
}

var _ journal.Journal = (*recoveringMarkerJournal)(nil)
var _ journal.DegradationMarker = (*recoveringMarkerJournal)(nil)

func (backend *recoveringMarkerJournal) MarkDegraded(ctx context.Context, id journal.StreamID) error {
	if backend.markerCalls.Add(1) <= backend.markerFailures {
		return errors.New("injected marker outage")
	}
	return backend.Memory.MarkDegraded(ctx, id)
}
