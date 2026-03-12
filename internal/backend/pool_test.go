package backend

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/streamweld/streamweld/internal/conformance"
)

const poolTestTimeout = 2 * time.Second

type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func (clock *testClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *testClock) Add(duration time.Duration) {
	clock.mu.Lock()
	clock.now = clock.now.Add(duration)
	clock.mu.Unlock()
}

func testBackend(t *testing.T, id, rawURL string) Backend {
	t.Helper()
	parsed, err := url.Parse(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	return Backend{
		ID:              ID(id),
		URL:             parsed,
		ModelVersion:    "version-1",
		TemplateVerdict: conformance.VerdictSafe,
	}
}

func testPool(t *testing.T, clock *testClock, backends ...Backend) *Pool {
	t.Helper()
	config := DefaultConfig()
	config.Clock = clock.Now
	pool, err := NewPool(config, backends...)
	if err != nil {
		t.Fatalf("NewPool() error: %v", err)
	}
	return pool
}

func markHealthy(t *testing.T, pool *Pool, ids ...ID) {
	t.Helper()
	for _, id := range ids {
		if _, err := pool.SetHealth(id, HealthHealthy); err != nil {
			t.Fatalf("SetHealth(%s) error: %v", id, err)
		}
	}
}

func TestConfigBackendValidationAndSnapshotIsolation(t *testing.T) {
	clock := &testClock{now: time.Date(2026, 8, 22, 12, 0, 0, 0, time.FixedZone("test", 5*60*60))}
	backend := testBackend(t, "backend-a:8000", "HTTP://backend-a:8000/base")
	config := Config{Clock: clock.Now}
	pool, err := NewPool(config, backend)
	if err != nil {
		t.Fatalf("NewPool() error: %v", err)
	}
	if pool.config.QuarantineWindow != DefaultQuarantineWindow || pool.config.ProbeInterval != DefaultProbeInterval || pool.config.ProbeTimeout != DefaultProbeTimeout {
		t.Errorf("defaults were not applied: %+v", pool.config)
	}

	backend.URL.Host = "mutated.invalid"
	state, err := pool.Get("backend-a:8000")
	if err != nil {
		t.Fatal(err)
	}
	if state.URL.Host != "backend-a:8000" || state.URL.Scheme != "http" || state.Health != HealthUnknown {
		t.Errorf("initial State() = %+v", state)
	}
	state.URL.Host = "also-mutated.invalid"
	again, err := pool.Get("backend-a:8000")
	if err != nil || again.URL.Host != "backend-a:8000" {
		t.Errorf("returned URL mutation reached pool: (%+v, %v)", again, err)
	}

	invalidConfigs := []Config{
		{QuarantineWindow: -1},
		{ProbeInterval: -1},
		{ProbeTimeout: -1},
		{ProbeConcurrency: -1},
	}
	for index, invalid := range invalidConfigs {
		if _, err := NewPool(invalid); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("NewPool(invalid config %d) error = %v, want ErrInvalidConfig", index, err)
		}
	}

	invalidBackends := []Backend{
		{ID: "", URL: mustURL(t, "http://valid:8000")},
		{ID: " padded ", URL: mustURL(t, "http://valid:8000")},
		{ID: "backend", URL: nil},
		{ID: "backend", URL: mustURL(t, "ftp://valid:8000")},
		{ID: "backend", URL: mustURL(t, "http://user:secret@valid:8000")},
		{ID: "backend", URL: mustURL(t, "http://valid:8000/#fragment")},
		{ID: "backend", URL: mustURL(t, "http://valid:8000"), TemplateVerdict: "BROKEN"},
	}
	for index, invalid := range invalidBackends {
		if _, err := NewPool(DefaultConfig(), invalid); !errors.Is(err, ErrInvalidBackend) {
			t.Errorf("NewPool(invalid backend %d) error = %v, want ErrInvalidBackend", index, err)
		}
	}
}

func TestReplaceIsAtomicAndPreservesRuntimeState(t *testing.T) {
	clock := &testClock{now: time.Unix(1, 0)}
	first := testBackend(t, "a:8000", "http://a:8000")
	second := testBackend(t, "b:8000", "http://b:8000")
	pool := testPool(t, clock, first, second)
	markHealthy(t, pool, first.ID, second.ID)
	if _, err := pool.MarkPassiveFailure(first.ID); err != nil {
		t.Fatal(err)
	}

	updatedFirst := testBackend(t, "a:8000", "http://a:8000")
	updatedFirst.ModelVersion = "version-2"
	updatedFirst.TemplateVerdict = conformance.VerdictDegraded
	third := testBackend(t, "c:8000", "http://c:8000")
	if err := pool.Replace([]Backend{updatedFirst, third}); err != nil {
		t.Fatalf("Replace() error: %v", err)
	}
	state, err := pool.Get(first.ID)
	if err != nil {
		t.Fatal(err)
	}
	if state.Health != HealthHealthy || !state.Quarantined || state.ModelVersion != "version-2" || state.TemplateVerdict != conformance.VerdictDegraded {
		t.Errorf("updated state = %+v", state)
	}
	if _, err := pool.Get(second.ID); !errors.Is(err, ErrNotFound) {
		t.Errorf("removed backend Get() error = %v, want ErrNotFound", err)
	}
	thirdState, err := pool.Get(third.ID)
	if err != nil || thirdState.Health != HealthUnknown {
		t.Errorf("new backend state = (%+v, %v)", thirdState, err)
	}

	duplicate := append([]Backend{updatedFirst}, updatedFirst)
	if err := pool.Replace(duplicate); !errors.Is(err, ErrInvalidBackend) {
		t.Errorf("Replace(duplicate) error = %v, want ErrInvalidBackend", err)
	}
	if states := pool.List(); len(states) != 2 {
		t.Errorf("failed Replace changed pool length to %d", len(states))
	}

	changedURL := updatedFirst
	changedURL.URL = mustURL(t, "http://replacement:8000")
	state, err = pool.Upsert(changedURL)
	if err != nil || state.Health != HealthUnknown || state.Quarantined == false {
		t.Errorf("URL-changing Upsert() = (%+v, %v)", state, err)
	}
}

func TestAcquireModelFiltersDynamicRoutesAndKeepsStaticWildcard(t *testing.T) {
	t.Parallel()
	clock := &testClock{now: time.Unix(1, 0)}
	llama := testBackend(t, "llama/pod-a", "http://llama:8000")
	llama.Model = "llama-8b"
	llama.PodNamespace = "models"
	llama.PodName = "llama-a"
	mistral := testBackend(t, "mistral/pod-a", "http://mistral:8000")
	mistral.Model = "mistral-7b"
	wildcard := testBackend(t, "standalone", "http://standalone:8000")
	pool := testPool(t, clock, llama, mistral, wildcard)
	markHealthy(t, pool, llama.ID, mistral.ID, wildcard.ID)

	config := pool.config.Choose
	pool.config.Choose = func(candidates []State) int {
		for index, candidate := range candidates {
			if candidate.ID == llama.ID {
				return index
			}
		}
		return 0
	}
	lease, err := pool.AcquireModel("stream-a", "llama-8b")
	if err != nil {
		t.Fatalf("AcquireModel(llama-8b) error: %v", err)
	}
	if got := lease.Backend(); got.ID != llama.ID || got.Model != "llama-8b" ||
		got.PodNamespace != "models" || got.PodName != "llama-a" {
		t.Fatalf("AcquireModel(llama-8b) backend = %+v", got)
	}
	lease.Release()

	pool.config.Choose = func(candidates []State) int {
		for index, candidate := range candidates {
			if candidate.ID == wildcard.ID {
				return index
			}
		}
		return 0
	}
	lease, err = pool.AcquireModel("stream-b", "unknown-model")
	if err != nil || lease.Backend().ID != wildcard.ID {
		t.Fatalf("AcquireModel(unknown) = (%+v, %v), want wildcard", lease, err)
	}
	lease.Release()
	pool.config.Choose = config

	lease, err = pool.AcquireModel("stream-c", "")
	if err != nil || lease.Backend().ID != wildcard.ID {
		t.Fatalf("AcquireModel(empty) = (%+v, %v), want standalone wildcard", lease, err)
	}
	lease.Release()
}

func TestSelectionExclusionsDrainAndQuarantineBoundary(t *testing.T) {
	clock := &testClock{now: time.Unix(1, 0)}
	a := testBackend(t, "a:8000", "http://a:8000")
	b := testBackend(t, "b:8000", "http://b:8000")
	c := testBackend(t, "c:8000", "http://c:8000")
	var pool *Pool
	config := DefaultConfig()
	config.Clock = clock.Now
	config.Choose = func(candidates []State) int {
		// Re-entering the pool proves the chooser is never called under its lock.
		if got := len(pool.List()); got != 3 {
			t.Errorf("List() from chooser returned %d states", got)
		}
		for index := 1; index < len(candidates); index++ {
			if candidates[index-1].ID >= candidates[index].ID {
				t.Errorf("chooser candidates are not ID-sorted: %+v", candidates)
			}
		}
		return len(candidates) - 1
	}
	var err error
	pool, err = NewPool(config, a, b, c)
	if err != nil {
		t.Fatal(err)
	}
	markHealthy(t, pool, a.ID, b.ID, c.ID)

	lease, err := pool.Acquire("stream-1", c.ID)
	if err != nil {
		t.Fatal(err)
	}
	if lease.Backend().ID != b.ID || lease.Owner() != "stream-1" || lease.ID() == 0 {
		t.Errorf("selected lease = id=%d owner=%q backend=%+v", lease.ID(), lease.Owner(), lease.Backend())
	}
	if state, _ := pool.Get(b.ID); state.InFlight != 1 {
		t.Errorf("in-flight count = %d, want 1", state.InFlight)
	}

	if _, err := pool.MarkDraining(b.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.MarkPassiveFailure(c.ID); err != nil {
		t.Fatal(err)
	}
	selected, err := pool.Acquire("stream-2")
	if err != nil || selected.Backend().ID != a.ID {
		t.Fatalf("Acquire() with gates = (%+v, %v), want backend a", selected, err)
	}
	selected.Release()

	clock.Add(DefaultQuarantineWindow - time.Nanosecond)
	if _, err := pool.Acquire("none", a.ID); !errors.Is(err, ErrNoEligibleBackend) {
		t.Errorf("Acquire() before quarantine boundary error = %v", err)
	}
	clock.Add(time.Nanosecond)
	selected, err = pool.Acquire("stream-3", a.ID)
	if err != nil || selected.Backend().ID != c.ID {
		t.Fatalf("Acquire() at quarantine boundary = (%+v, %v), want backend c", selected, err)
	}
	selected.Release()

	lease.Release()
	lease.Release()
	if state, _ := pool.Get(b.ID); state.InFlight != 0 {
		t.Errorf("idempotent Release left in-flight count %d", state.InFlight)
	}
}

func TestDrainSnapshotWaitTimeoutAndUndrain(t *testing.T) {
	clock := &testClock{now: time.Unix(1, 0)}
	backend := testBackend(t, "backend:8000", "http://backend:8000")
	pool := testPool(t, clock, backend)
	markHealthy(t, pool, backend.ID)
	first, err := pool.Acquire("stream-a")
	if err != nil {
		t.Fatal(err)
	}
	second, err := pool.Acquire("stream-b")
	if err != nil {
		t.Fatal(err)
	}
	drain, err := pool.MarkDraining(backend.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !drain.Backend.Draining || drain.Backend.InFlight != 2 || len(drain.Bindings) != 2 || drain.Bindings[0].Owner != "stream-a" || drain.Bindings[1].Owner != "stream-b" {
		t.Fatalf("MarkDraining() = %+v", drain)
	}
	if _, err := pool.Acquire("rejected"); !errors.Is(err, ErrNoEligibleBackend) {
		t.Errorf("Acquire() while draining error = %v", err)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	state, err := pool.WaitDrained(canceled, backend.ID)
	if !errors.Is(err, context.Canceled) || state.InFlight != 2 || !state.Draining {
		t.Errorf("WaitDrained(canceled) = (%+v, %v)", state, err)
	}

	waitResult := make(chan struct {
		state State
		err   error
	}, 1)
	go func() {
		state, waitErr := pool.WaitDrained(context.Background(), backend.ID)
		waitResult <- struct {
			state State
			err   error
		}{state, waitErr}
	}()
	first.Release()
	select {
	case result := <-waitResult:
		t.Fatalf("WaitDrained returned with one lease: %+v", result)
	case <-time.After(20 * time.Millisecond):
	}
	second.Release()
	select {
	case result := <-waitResult:
		if result.err != nil || result.state.InFlight != 0 {
			t.Errorf("WaitDrained() = (%+v, %v)", result.state, result.err)
		}
	case <-time.After(poolTestTimeout):
		t.Fatal("WaitDrained did not wake after final release")
	}

	state, err = pool.Undrain(backend.ID)
	if err != nil || state.Draining {
		t.Fatalf("Undrain() = (%+v, %v)", state, err)
	}
	lease, err := pool.Acquire("after-drain")
	if err != nil {
		t.Fatalf("Acquire() after Undrain: %v", err)
	}
	lease.Release()
}

func TestRemoveRetainsActiveLeaseUntilRelease(t *testing.T) {
	clock := &testClock{now: time.Unix(1, 0)}
	backend := testBackend(t, "backend:8000", "http://backend:8000")
	pool := testPool(t, clock, backend)
	markHealthy(t, pool, backend.ID)
	lease, err := pool.Acquire("active")
	if err != nil {
		t.Fatal(err)
	}
	removed, err := pool.Remove(backend.ID)
	if err != nil || !removed {
		t.Fatalf("Remove() = (%t, %v)", removed, err)
	}
	if _, err := pool.Get(backend.ID); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get(retired) error = %v", err)
	}
	if _, err := pool.Acquire("new"); !errors.Is(err, ErrNoEligibleBackend) {
		t.Errorf("Acquire() after Remove error = %v", err)
	}
	if lease.Backend().URL.Host != "backend:8000" {
		t.Errorf("active lease snapshot changed: %+v", lease.Backend())
	}
	lease.Release()
	removed, err = pool.Remove(backend.ID)
	if err != nil || removed {
		t.Errorf("second Remove() = (%t, %v), want (false, nil)", removed, err)
	}
}

func TestRetainedPodDrainPinsControllerRetiredLease(t *testing.T) {
	t.Parallel()
	clock := &testClock{now: time.Unix(1, 0)}
	candidate := testBackend(t, "route-a/pod-a", "http://pod-a:8000")
	candidate.PodNamespace = "models"
	candidate.PodName = "pod-a"
	pool := testPool(t, clock, candidate)
	markHealthy(t, pool, candidate.ID)
	lease, err := pool.AcquireID(candidate.ID, "stream-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.ReplaceReady(nil); err != nil {
		lease.Release()
		t.Fatal(err)
	}
	if states := pool.List(); len(states) != 0 {
		lease.Release()
		t.Fatalf("List() retained a retired backend: %+v", states)
	}

	drains, err := pool.BeginRetainedPodDrain("models", "pod-a")
	if err != nil {
		lease.Release()
		t.Fatal(err)
	}
	if len(drains) != 1 {
		lease.Release()
		t.Fatalf("BeginRetainedPodDrain() returned %d records, want 1", len(drains))
	}
	drain := drains[0]
	snapshot := drain.Snapshot()
	if !snapshot.Backend.Draining || snapshot.Backend.InFlight != 1 ||
		len(snapshot.Bindings) != 1 || snapshot.Bindings[0].Owner != "stream-a" {
		lease.Release()
		drain.Close()
		t.Fatalf("retained drain snapshot = %+v", snapshot)
	}
	if _, err := pool.AcquireID(candidate.ID, "new-stream"); !errors.Is(err, ErrNotFound) {
		lease.Release()
		drain.Close()
		t.Fatalf("AcquireID(retired) error = %v, want ErrNotFound", err)
	}

	lease.Release()
	state, err := drain.Wait(context.Background())
	if err != nil || state.InFlight != 0 {
		drain.Close()
		t.Fatalf("retained drain Wait() = (%+v, %v)", state, err)
	}
	if states := pool.ListRetained(); len(states) != 1 {
		drain.Close()
		t.Fatalf("retained record was pruned before Close: %+v", states)
	}
	drain.Close()
	drain.Close()
	if states := pool.ListRetained(); len(states) != 0 {
		t.Fatalf("retired record remained after drain Close: %+v", states)
	}
	if _, err := drain.Wait(context.Background()); !errors.Is(err, ErrDrainClosed) {
		t.Fatalf("Wait() after Close error = %v, want ErrDrainClosed", err)
	}
}

func TestReplaceReadyRevivalClearsOnlyRetirementDrain(t *testing.T) {
	t.Parallel()
	clock := &testClock{now: time.Unix(1, 0)}
	candidate := testBackend(t, "backend:8000", "http://backend:8000")

	t.Run("retirement-only drain", func(t *testing.T) {
		pool := testPool(t, clock, candidate)
		markHealthy(t, pool, candidate.ID)
		retainedLease, err := pool.AcquireID(candidate.ID, "existing")
		if err != nil {
			t.Fatal(err)
		}
		if err := pool.ReplaceReady(nil); err != nil {
			retainedLease.Release()
			t.Fatal(err)
		}
		if err := pool.ReplaceReady([]Backend{candidate}); err != nil {
			retainedLease.Release()
			t.Fatal(err)
		}
		state, err := pool.Get(candidate.ID)
		if err != nil || state.Draining || state.InFlight != 1 {
			retainedLease.Release()
			t.Fatalf("revived retirement-only backend = (%+v, %v)", state, err)
		}
		newLease, err := pool.AcquireID(candidate.ID, "new")
		if err != nil {
			retainedLease.Release()
			t.Fatalf("AcquireID(revived) error: %v", err)
		}
		newLease.Release()
		retainedLease.Release()
	})

	t.Run("explicit drain", func(t *testing.T) {
		pool := testPool(t, clock, candidate)
		markHealthy(t, pool, candidate.ID)
		retainedLease, err := pool.AcquireID(candidate.ID, "existing")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.MarkDraining(candidate.ID); err != nil {
			retainedLease.Release()
			t.Fatal(err)
		}
		if err := pool.ReplaceReady(nil); err != nil {
			retainedLease.Release()
			t.Fatal(err)
		}
		if err := pool.ReplaceReady([]Backend{candidate}); err != nil {
			retainedLease.Release()
			t.Fatal(err)
		}
		state, err := pool.Get(candidate.ID)
		if err != nil || !state.Draining || state.InFlight != 1 {
			retainedLease.Release()
			t.Fatalf("revived explicitly drained backend = (%+v, %v)", state, err)
		}
		if _, err := pool.AcquireID(candidate.ID, "blocked"); !errors.Is(err, ErrNoEligibleBackend) {
			retainedLease.Release()
			t.Fatalf("AcquireID(explicitly drained) error = %v, want ErrNoEligibleBackend", err)
		}
		if _, err := pool.Undrain(candidate.ID); err != nil {
			retainedLease.Release()
			t.Fatal(err)
		}
		newLease, err := pool.AcquireID(candidate.ID, "after-undrain")
		if err != nil {
			retainedLease.Release()
			t.Fatalf("AcquireID(after Undrain) error: %v", err)
		}
		newLease.Release()
		retainedLease.Release()
	})
}

func TestConcurrentAcquireReleaseUpdateAndDrain(t *testing.T) {
	clock := &testClock{now: time.Unix(1, 0)}
	backends := make([]Backend, 4)
	for index := range backends {
		backends[index] = testBackend(t, ID(fmt.Sprintf("backend-%d:8000", index)).String(), fmt.Sprintf("http://backend-%d:8000", index))
	}
	pool := testPool(t, clock, backends...)
	for _, backend := range backends {
		markHealthy(t, pool, backend.ID)
	}

	const workers = 16
	const iterations = 200
	var wait sync.WaitGroup
	var acquired atomic.Int64
	for worker := 0; worker < workers; worker++ {
		wait.Add(1)
		go func(worker int) {
			defer wait.Done()
			for iteration := 0; iteration < iterations; iteration++ {
				lease, err := pool.Acquire(fmt.Sprintf("%d/%d", worker, iteration))
				if err == nil {
					acquired.Add(1)
					_ = lease.Backend()
					lease.Release()
				}
				_ = pool.List()
			}
		}(worker)
	}
	wait.Add(1)
	go func() {
		defer wait.Done()
		for iteration := 0; iteration < iterations; iteration++ {
			backend := backends[iteration%len(backends)]
			_, _ = pool.MarkPassiveFailure(backend.ID)
			clock.Add(DefaultQuarantineWindow)
			_, _ = pool.MarkDraining(backend.ID)
			_, _ = pool.Undrain(backend.ID)
			_, _ = pool.SetHealth(backend.ID, HealthHealthy)
		}
	}()
	wait.Wait()
	if acquired.Load() == 0 {
		t.Error("concurrent test acquired no leases")
	}
	for _, state := range pool.List() {
		if state.InFlight != 0 {
			t.Errorf("backend %s retained %d leases", state.ID, state.InFlight)
		}
	}
}

func TestInvalidChooserAndLeaseExhaustion(t *testing.T) {
	clock := &testClock{now: time.Unix(1, 0)}
	backend := testBackend(t, "backend:8000", "http://backend:8000")
	config := DefaultConfig()
	config.Clock = clock.Now
	config.Choose = func([]State) int { return 9 }
	pool, err := NewPool(config, backend)
	if err != nil {
		t.Fatal(err)
	}
	markHealthy(t, pool, backend.ID)
	if _, err := pool.Acquire("invalid"); !errors.Is(err, ErrInvalidSelection) {
		t.Errorf("Acquire(invalid chooser) error = %v", err)
	}
	pool.config.Choose = nil
	pool.mu.Lock()
	pool.nextLeaseID = ^uint64(0)
	pool.mu.Unlock()
	if _, err := pool.Acquire("overflow"); !errors.Is(err, ErrLeaseIDExhausted) {
		t.Errorf("Acquire(exhausted IDs) error = %v", err)
	}
}

func mustURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}
