package proxy

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/streamweld/streamweld/internal/backend"
	"github.com/streamweld/streamweld/internal/journal"
)

func TestBlockedIdempotencyReservationDoesNotSerializeUnrelatedCreation(t *testing.T) {
	registry := &keyedBlockingIdempotencyRegistry{
		MemoryIdempotencyRegistry: journal.NewMemoryIdempotencyRegistry(nil),
		blockedKey:                "key-a",
		entered:                   make(chan struct{}),
		release:                   make(chan struct{}),
	}
	t.Cleanup(registry.unblock)
	service := newCreationConcurrencyService(t, registry, nil)

	blocked := resolveConcurrently(service, "key-a")
	select {
	case <-registry.entered:
	case <-time.After(time.Second):
		t.Fatal("key A did not reach its blocked reservation")
	}

	nonIdempotent := resolveConcurrently(service, "")
	keyB := resolveConcurrently(service, "key-b")
	for name, result := range map[string]<-chan creationResolution{
		"non-idempotent": nonIdempotent,
		"key B":          keyB,
	} {
		select {
		case resolved := <-result:
			if resolved.err != nil || resolved.resolution.existing {
				t.Fatalf("%s resolution = (%+v, %v), want an independent new stream", name, resolved.resolution, resolved.err)
			}
		case <-time.After(time.Second):
			t.Fatalf("%s creation was serialized behind key A", name)
		}
	}

	registry.unblock()
	select {
	case resolved := <-blocked:
		if resolved.err != nil || resolved.resolution.existing {
			t.Fatalf("key A resolution = (%+v, %v), want a new stream", resolved.resolution, resolved.err)
		}
	case <-time.After(time.Second):
		t.Fatal("key A did not finish after its reservation was released")
	}
}

func TestConcurrentSameIdempotencyKeyCreatesOneStreamAndProducer(t *testing.T) {
	baseJournal := newCreationMemoryJournal(t)
	gatedJournal := &gatedCreationOpenJournal{
		Journal:     baseJournal,
		openEntered: make(chan struct{}),
		stateSeen:   make(chan struct{}),
		releaseOpen: make(chan struct{}),
	}
	t.Cleanup(gatedJournal.release)
	service := newCreationConcurrencyService(
		t,
		journal.NewMemoryIdempotencyRegistry(nil),
		gatedJournal,
	)

	first := resolveConcurrently(service, "same-key")
	select {
	case <-gatedJournal.openEntered:
	case <-time.After(time.Second):
		t.Fatal("first request did not reserve the key and reach journal Open")
	}
	second := resolveConcurrently(service, "same-key")
	select {
	case <-gatedJournal.stateSeen:
	case <-time.After(time.Second):
		t.Fatal("second request did not resolve the pending stream reservation")
	}
	gatedJournal.release()

	results := []creationResolution{
		awaitCreationResolution(t, first),
		awaitCreationResolution(t, second),
	}
	if results[0].err != nil || results[1].err != nil {
		t.Fatalf("same-key resolution errors = (%v, %v)", results[0].err, results[1].err)
	}
	if results[0].resolution.id != results[1].resolution.id {
		t.Fatalf("same key resolved to %s and %s", results[0].resolution.id, results[1].resolution.id)
	}
	newStreams := 0
	for _, result := range results {
		if !result.resolution.existing {
			newStreams++
		}
	}
	if newStreams != 1 {
		t.Fatalf("same-key requests produced %d new runtimes, want exactly one producer", newStreams)
	}
	if opens := gatedJournal.opens.Load(); opens != 1 {
		t.Fatalf("journal Open calls = %d, want one", opens)
	}
	state, err := baseJournal.State(context.Background(), results[0].resolution.id)
	if err != nil || state.LastSeq != 1 || state.Status != journal.StatusOpen {
		t.Fatalf("created journal state = (%+v, %v), want one open stream", state, err)
	}
}

type creationResolution struct {
	resolution streamResolution
	err        error
}

func resolveConcurrently(service *durableService, idempotencyKey string) <-chan creationResolution {
	result := make(chan creationResolution, 1)
	go func() {
		request := httptest.NewRequestWithContext(
			context.Background(), http.MethodPost, "/v1/chat/completions", nil,
		)
		resolution, err := service.resolve(request, normalizedRequest{
			Body: []byte(`{"model":"model","stream":true}`), Model: "model", Stream: true,
		}, service.policyForModel("model"), idempotencyKey, time.Now())
		result <- creationResolution{resolution: resolution, err: err}
	}()
	return result
}

func awaitCreationResolution(t *testing.T, result <-chan creationResolution) creationResolution {
	t.Helper()
	select {
	case resolved := <-result:
		return resolved
	case <-time.After(time.Second):
		t.Fatal("creation did not finish")
		return creationResolution{}
	}
}

func newCreationConcurrencyService(
	t *testing.T,
	registry journal.IdempotencyRegistry,
	journalBackend journal.Journal,
) *durableService {
	t.Helper()
	if journalBackend == nil {
		journalBackend = newCreationMemoryJournal(t)
	}
	backendURL, err := url.Parse("http://backend.example.test")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := backend.NewPool(backend.DefaultConfig(), backend.Backend{
		ID: "backend-a", URL: backendURL,
	})
	if err != nil {
		t.Fatalf("backend.NewPool() error = %v", err)
	}
	if _, err := pool.SetHealth("backend-a", backend.HealthHealthy); err != nil {
		t.Fatalf("pool.SetHealth() error = %v", err)
	}
	rootContext, cancelRoot := context.WithCancel(context.Background())
	service := &durableService{
		rootContext: rootContext,
		config: Config{
			JournalTTL:        time.Minute,
			ReadinessTimeout:  2 * time.Second,
			ReaderMaxLagBytes: 1 << 10,
		},
		journal:     journalBackend,
		ids:         journal.NewIDGenerator(nil, nil),
		idempotency: registry,
		logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		backends:    pool,
	}
	t.Cleanup(func() {
		cancelRoot()
		service.streams.Range(func(key, value any) bool {
			runtime := value.(*streamRuntime)
			runtime.cancel(context.Canceled)
			if runtime.currentLease != nil {
				runtime.currentLease.Release()
			}
			runtime.activeDone.Do(service.active.Done)
			service.streams.Delete(key)
			return true
		})
	})
	return service
}

func newCreationMemoryJournal(t *testing.T) journal.Journal {
	t.Helper()
	config := journal.DefaultConfig()
	config.MaxTotalBytes = 1 << 20
	config.ReaderMaxLagBytes = 1 << 10
	memory, err := journal.NewMemory(config)
	if err != nil {
		t.Fatalf("journal.NewMemory() error = %v", err)
	}
	return memory
}

type keyedBlockingIdempotencyRegistry struct {
	*journal.MemoryIdempotencyRegistry
	blockedKey  string
	entered     chan struct{}
	release     chan struct{}
	enterOnce   sync.Once
	releaseOnce sync.Once
}

func (registry *keyedBlockingIdempotencyRegistry) ResolveOrCreate(
	ctx context.Context,
	key string,
	newID journal.StreamID,
	ttl time.Duration,
) (journal.IdempotencyBinding, error) {
	if key == registry.blockedKey {
		registry.enterOnce.Do(func() { close(registry.entered) })
		select {
		case <-registry.release:
		case <-ctx.Done():
			return journal.IdempotencyBinding{}, ctx.Err()
		}
	}
	return registry.MemoryIdempotencyRegistry.ResolveOrCreate(ctx, key, newID, ttl)
}

func (registry *keyedBlockingIdempotencyRegistry) unblock() {
	registry.releaseOnce.Do(func() { close(registry.release) })
}

type gatedCreationOpenJournal struct {
	journal.Journal
	openEntered chan struct{}
	stateSeen   chan struct{}
	releaseOpen chan struct{}
	openOnce    sync.Once
	stateOnce   sync.Once
	releaseOnce sync.Once
	opens       atomic.Int64

	mu        sync.Mutex
	blockedID journal.StreamID
}

func (backend *gatedCreationOpenJournal) Open(
	ctx context.Context,
	id journal.StreamID,
	meta journal.Meta,
) error {
	backend.opens.Add(1)
	backend.mu.Lock()
	backend.blockedID = id
	backend.mu.Unlock()
	backend.openOnce.Do(func() { close(backend.openEntered) })
	select {
	case <-backend.releaseOpen:
	case <-ctx.Done():
		return ctx.Err()
	}
	return backend.Journal.Open(ctx, id, meta)
}

func (backend *gatedCreationOpenJournal) State(
	ctx context.Context,
	id journal.StreamID,
) (journal.StreamState, error) {
	backend.mu.Lock()
	blockedID := backend.blockedID
	backend.mu.Unlock()
	if id == blockedID {
		backend.stateOnce.Do(func() { close(backend.stateSeen) })
	}
	return backend.Journal.State(ctx, id)
}

func (backend *gatedCreationOpenJournal) release() {
	backend.releaseOnce.Do(func() { close(backend.releaseOpen) })
}
