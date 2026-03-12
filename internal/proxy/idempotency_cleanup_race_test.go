package proxy

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/streamweld/streamweld/internal/journal"
)

func TestStaleIdempotencyCleanerCannotDeleteNewReadyWinner(t *testing.T) {
	registry := journal.NewMemoryIdempotencyRegistry(nil)
	staleID, err := journal.NewStreamID()
	if err != nil {
		t.Fatal(err)
	}
	winnerID, err := journal.NewStreamID()
	if err != nil {
		t.Fatal(err)
	}
	const key = "stale-cleaner-race"
	stale, err := registry.ResolveOrCreate(context.Background(), key, staleID, time.Minute)
	if err != nil || !stale.Created {
		t.Fatalf("create stale binding = (%+v, %v)", stale, err)
	}

	memoryConfig := journal.DefaultConfig()
	memoryConfig.MaxTotalBytes = 1 << 20
	memoryConfig.ReaderMaxLagBytes = 1 << 10
	memory, err := journal.NewMemory(memoryConfig)
	if err != nil {
		t.Fatal(err)
	}
	if err := memory.Open(context.Background(), winnerID, journal.Meta{
		Model: "model", BackendID: "backend",
	}); err != nil {
		t.Fatalf("open replacement winner journal: %v", err)
	}
	journalView := &expiredStreamStateJournal{Journal: memory, expiredID: staleID}
	gated := &gatedConditionalIdempotencyRegistry{
		MemoryIdempotencyRegistry: registry,
		staleID:                   staleID,
		cleanupEntered:            make(chan struct{}),
		allowCleanup:              make(chan struct{}),
	}
	t.Cleanup(gated.allow)

	rootContext, cancelRoot := context.WithCancel(context.Background())
	t.Cleanup(cancelRoot)
	service := &durableService{
		rootContext: rootContext,
		config: Config{
			JournalTTL:       time.Minute,
			ReadinessTimeout: 2 * time.Second,
		},
		journal:     journalView,
		idempotency: gated,
		ids:         &durableSequentialIDs{},
		logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	request := httptest.NewRequestWithContext(
		context.Background(), http.MethodPost, "/v1/chat/completions", nil,
	)
	resolutionResult := make(chan struct {
		resolution streamResolution
		err        error
	}, 1)
	go func() {
		resolution, resolveErr := service.resolve(request, normalizedRequest{
			Body: []byte(`{"model":"model","stream":true}`), Model: "model", Stream: true,
		}, service.policyForModel("model"), key)
		resolutionResult <- struct {
			resolution streamResolution
			err        error
		}{resolution: resolution, err: resolveErr}
	}()

	select {
	case <-gated.cleanupEntered:
	case <-time.After(time.Second):
		t.Fatal("proxy did not reach conditional stale-binding cleanup")
	}
	removed, err := registry.RemoveIfBound(context.Background(), stale.Digest, staleID)
	if err != nil || !removed {
		t.Fatalf("replace stale binding cleanup = (%t, %v)", removed, err)
	}
	winner, err := registry.ResolveOrCreate(context.Background(), key, winnerID, time.Minute)
	if err != nil || !winner.Created || winner.ID != winnerID {
		t.Fatalf("create replacement winner = (%+v, %v)", winner, err)
	}
	gated.allow()

	select {
	case result := <-resolutionResult:
		if result.err != nil {
			t.Fatalf("resolve() error = %v", result.err)
		}
		if !result.resolution.existing || result.resolution.id != winnerID {
			t.Fatalf("resolution = %+v, want existing replacement %s", result.resolution, winnerID)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("resolve did not finish through the replacement binding")
	}
	resolved, err := registry.ResolveOrCreate(context.Background(), key, staleID, time.Minute)
	if err != nil || resolved.ID != winnerID || resolved.Created {
		t.Fatalf("final binding = (%+v, %v), want replacement winner preserved", resolved, err)
	}
}

type expiredStreamStateJournal struct {
	journal.Journal
	expiredID journal.StreamID
}

func (backend *expiredStreamStateJournal) State(
	ctx context.Context,
	id journal.StreamID,
) (journal.StreamState, error) {
	if id == backend.expiredID {
		return journal.StreamState{}, fmt.Errorf("%w: %s", journal.ErrExpired, id)
	}
	return backend.Journal.State(ctx, id)
}

type gatedConditionalIdempotencyRegistry struct {
	*journal.MemoryIdempotencyRegistry
	staleID        journal.StreamID
	cleanupEntered chan struct{}
	allowCleanup   chan struct{}
	enteredOnce    sync.Once
	allowOnce      sync.Once
}

func (registry *gatedConditionalIdempotencyRegistry) RemoveIfBound(
	ctx context.Context,
	digest journal.IdempotencyDigest,
	id journal.StreamID,
) (bool, error) {
	if id == registry.staleID {
		registry.enteredOnce.Do(func() { close(registry.cleanupEntered) })
		select {
		case <-registry.allowCleanup:
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
	return registry.MemoryIdempotencyRegistry.RemoveIfBound(ctx, digest, id)
}

func (registry *gatedConditionalIdempotencyRegistry) allow() {
	registry.allowOnce.Do(func() { close(registry.allowCleanup) })
}
