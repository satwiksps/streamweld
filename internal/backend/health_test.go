package backend

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestHTTPProbeHealthTransitionsAndNoRedirect(t *testing.T) {
	var status atomic.Int32
	status.Store(http.StatusOK)
	var pathSeen atomic.Value
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		pathSeen.Store(request.URL.Path)
		writer.WriteHeader(int(status.Load()))
		_, _ = writer.Write([]byte(`{"status":"ok"}`))
	}))
	defer server.Close()

	clock := &testClock{now: time.Unix(1, 0)}
	backend := testBackend(t, "backend", server.URL+"/api")
	config := DefaultConfig()
	config.Clock = clock.Now
	pool, err := NewPool(config, backend)
	if err != nil {
		t.Fatal(err)
	}
	results, err := pool.ProbeAll(context.Background())
	if err != nil || len(results) != 1 || !results[0].Applied || !results[0].Healthy || results[0].Err != nil {
		t.Fatalf("first ProbeAll() = (%+v, %v)", results, err)
	}
	if got := pathSeen.Load(); got != "/api/health" {
		t.Errorf("health path = %v, want /api/health", got)
	}
	lease, err := pool.Acquire("stream")
	if err != nil {
		t.Fatal(err)
	}

	clock.Add(time.Second)
	status.Store(http.StatusServiceUnavailable)
	results, err = pool.ProbeAll(context.Background())
	if err != nil || len(results) != 1 || results[0].Healthy || results[0].Err == nil || !results[0].Transition.Changed {
		t.Fatalf("failed ProbeAll() = (%+v, %v)", results, err)
	}
	if len(results[0].Transition.Bindings) != 1 || results[0].Transition.Bindings[0].Owner != "stream" {
		t.Errorf("unhealthy transition bindings = %+v", results[0].Transition.Bindings)
	}
	if _, err := pool.Acquire("rejected"); !errors.Is(err, ErrNoEligibleBackend) {
		t.Errorf("Acquire(unhealthy) error = %v", err)
	}
	lease.Release()

	redirectTargetHit := atomic.Bool{}
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		redirectTargetHit.Store(true)
	}))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		http.Redirect(writer, &http.Request{}, target.URL, http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()
	redirectBackend := testBackend(t, "redirect", redirect.URL)
	if _, err := pool.Upsert(redirectBackend); err != nil {
		t.Fatal(err)
	}
	results, err = pool.ProbeAll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range results {
		if result.ID == redirectBackend.ID && result.Healthy {
			t.Error("redirect response was accepted as healthy")
		}
	}
	if redirectTargetHit.Load() {
		t.Error("health client followed a redirect")
	}
}

func TestProbeCallbacksRunOutsideLocksAndStaleResultsAreDiscarded(t *testing.T) {
	clock := &testClock{now: time.Unix(1, 0)}
	backend := testBackend(t, "backend", "http://backend:8000")
	started := make(chan struct{})
	release := make(chan struct{})
	var pool *Pool
	config := DefaultConfig()
	config.Clock = clock.Now
	config.Probe = func(ctx context.Context, value Backend) error {
		if _, err := pool.Get(value.ID); err != nil {
			return err
		}
		select {
		case <-started:
		default:
			close(started)
		}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	var err error
	pool, err = NewPool(config, backend)
	if err != nil {
		t.Fatal(err)
	}
	resultChannel := make(chan []ProbeResult, 1)
	errorChannel := make(chan error, 1)
	go func() {
		results, probeErr := pool.ProbeAll(context.Background())
		resultChannel <- results
		errorChannel <- probeErr
	}()
	select {
	case <-started:
	case <-time.After(poolTestTimeout):
		t.Fatal("probe callback did not start; it may be blocked under the pool lock")
	}
	updated := backend
	updated.URL = mustURL(t, "http://replacement:8000")
	if _, err := pool.Upsert(updated); err != nil {
		t.Fatal(err)
	}
	close(release)
	results := <-resultChannel
	if err := <-errorChannel; err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Applied {
		t.Fatalf("stale probe result = %+v, want Applied false", results)
	}
	state, err := pool.Get(backend.ID)
	if err != nil || state.Health != HealthUnknown || state.URL.Host != "replacement:8000" {
		t.Errorf("state after stale result = (%+v, %v)", state, err)
	}
}

func TestProbeConcurrencyBoundAndParentCancellation(t *testing.T) {
	clock := &testClock{now: time.Unix(1, 0)}
	backends := make([]Backend, 12)
	for index := range backends {
		backends[index] = testBackend(t, fmt.Sprintf("backend-%02d", index), fmt.Sprintf("http://backend-%02d:8000", index))
	}
	var active atomic.Int32
	var maximum atomic.Int32
	release := make(chan struct{})
	config := DefaultConfig()
	config.Clock = clock.Now
	config.ProbeConcurrency = 3
	config.Probe = func(ctx context.Context, _ Backend) error {
		current := active.Add(1)
		defer active.Add(-1)
		for {
			observed := maximum.Load()
			if current <= observed || maximum.CompareAndSwap(observed, current) {
				break
			}
		}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	pool, err := NewPool(config, backends...)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, probeErr := pool.ProbeAll(context.Background())
		done <- probeErr
	}()
	deadline := time.After(poolTestTimeout)
	for maximum.Load() < 3 {
		select {
		case <-deadline:
			t.Fatal("bounded probes did not reach configured concurrency")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if maximum.Load() != 3 {
		t.Errorf("maximum probe concurrency = %d, want 3", maximum.Load())
	}

	cancelConfig := DefaultConfig()
	cancelConfig.Clock = clock.Now
	cancelConfig.Probe = func(ctx context.Context, _ Backend) error {
		<-ctx.Done()
		return ctx.Err()
	}
	canceledPool, err := NewPool(cancelConfig, backends[0])
	if err != nil {
		t.Fatal(err)
	}
	markHealthy(t, canceledPool, backends[0].ID)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := canceledPool.ProbeAll(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("ProbeAll(canceled) error = %v", err)
	}
	state, err := canceledPool.Get(backends[0].ID)
	if err != nil || state.Health != HealthHealthy {
		t.Errorf("parent cancellation changed health: (%+v, %v)", state, err)
	}
}

type manualTicker struct {
	channel chan time.Time
	stopped atomic.Bool
}

func (ticker *manualTicker) Chan() <-chan time.Time { return ticker.channel }

func (ticker *manualTicker) Stop() { ticker.stopped.Store(true) }

func TestRunHealthChecksUsesTickerAndRejectsSecondLoop(t *testing.T) {
	clock := &testClock{now: time.Unix(1, 0)}
	backend := testBackend(t, "backend", "http://backend:8000")
	ticker := &manualTicker{channel: make(chan time.Time, 1)}
	probed := make(chan struct{}, 4)
	config := DefaultConfig()
	config.Clock = clock.Now
	config.TickerFactory = func(time.Duration) Ticker { return ticker }
	config.Probe = func(context.Context, Backend) error {
		probed <- struct{}{}
		return nil
	}
	pool, err := NewPool(config, backend)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- pool.RunHealthChecks(ctx) }()

	select {
	case <-probed:
	case <-time.After(poolTestTimeout):
		t.Fatal("initial health probe did not run")
	}
	if err := pool.RunHealthChecks(context.Background()); !errors.Is(err, ErrHealthChecksRunning) {
		t.Errorf("second RunHealthChecks() error = %v", err)
	}
	ticker.channel <- clock.Now()
	select {
	case <-probed:
	case <-time.After(poolTestTimeout):
		t.Fatal("tick health probe did not run")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("RunHealthChecks() error = %v", err)
		}
	case <-time.After(poolTestTimeout):
		t.Fatal("RunHealthChecks did not stop on cancellation")
	}
	if !ticker.stopped.Load() {
		t.Error("RunHealthChecks did not stop its ticker")
	}
}

func TestProbeErrorIsBounded(t *testing.T) {
	clock := &testClock{now: time.Unix(1, 0)}
	backend := testBackend(t, "backend", "http://backend:8000")
	config := DefaultConfig()
	config.Clock = clock.Now
	config.Probe = func(context.Context, Backend) error {
		return errors.New(string(make([]byte, maxProbeErrorBytes*2)))
	}
	pool, err := NewPool(config, backend)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.ProbeAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	state, err := pool.Get(backend.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.LastProbeError) != maxProbeErrorBytes {
		t.Errorf("LastProbeError length = %d, want %d", len(state.LastProbeError), maxProbeErrorBytes)
	}
}

func TestConcurrentProbeUpdateSelection(t *testing.T) {
	clock := &testClock{now: time.Unix(1, 0)}
	backends := []Backend{
		testBackend(t, "a", "http://a:8000"),
		testBackend(t, "b", "http://b:8000"),
		testBackend(t, "c", "http://c:8000"),
	}
	config := DefaultConfig()
	config.Clock = clock.Now
	config.Probe = func(context.Context, Backend) error { return nil }
	pool, err := NewPool(config, backends...)
	if err != nil {
		t.Fatal(err)
	}
	markHealthy(t, pool, backends[0].ID, backends[1].ID, backends[2].ID)

	const iterations = 200
	var wait sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for range iterations {
				_, _ = pool.ProbeAll(context.Background())
			}
		}()
	}
	for worker := 0; worker < 8; worker++ {
		wait.Add(1)
		go func(worker int) {
			defer wait.Done()
			for iteration := 0; iteration < iterations; iteration++ {
				lease, acquireErr := pool.Acquire(fmt.Sprintf("%d/%d", worker, iteration))
				if acquireErr == nil {
					lease.Release()
				}
				backend := backends[(worker+iteration)%len(backends)]
				_, _ = pool.Upsert(backend)
			}
		}(worker)
	}
	wait.Wait()
	for _, state := range pool.List() {
		if state.InFlight != 0 {
			t.Errorf("backend %s retained %d leases", state.ID, state.InFlight)
		}
	}
}
