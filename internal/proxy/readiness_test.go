package proxy

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestReadinessProbesBackendRootHealth(t *testing.T) {
	t.Parallel()
	requests := make(chan *http.Request, 1)
	backend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests <- request.Clone(context.Background())
		writer.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(backend.Close)

	server := newTestProxy(t, backend.URL+"/openai?fixed=query")
	recorder := httptest.NewRecorder()
	request := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/readyz", nil)
	server.Handler().ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("GET /readyz status = %d, want 200; body = %q", recorder.Code, recorder.Body.String())
	}
	probe := <-requests
	if probe.Method != http.MethodGet || probe.URL.Path != "/health" || probe.URL.RawQuery != "" {
		t.Errorf("backend probe = %s %s, want GET /health with no query", probe.Method, probe.URL.RequestURI())
	}
}

func TestReadinessRejectsBackendNon2xxWithoutFollowingRedirect(t *testing.T) {
	t.Parallel()
	var followed atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/healthy" {
			followed.Add(1)
			writer.WriteHeader(http.StatusOK)
			return
		}
		writer.Header().Set("Location", "/healthy")
		writer.WriteHeader(http.StatusFound)
	}))
	t.Cleanup(backend.Close)

	server := newTestProxy(t, backend.URL)
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/readyz", nil))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("GET /readyz status = %d, want 503", recorder.Code)
	}
	if followed.Load() != 0 {
		t.Error("readiness probe followed a non-2xx redirect")
	}
}

func TestReadinessReturnsUnavailableOnTransportFailure(t *testing.T) {
	t.Parallel()
	transport := roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("backend unreachable")
	})
	server := newTestProxy(t, "http://backend.example.test", WithTransport(transport))
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/readyz", nil))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("GET /readyz status = %d, want 503", recorder.Code)
	}
}

func TestReadinessProbeHasBoundedTimeout(t *testing.T) {
	t.Parallel()
	probeCanceled := make(chan struct{})
	transport := roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		<-request.Context().Done()
		close(probeCanceled)
		return nil, request.Context().Err()
	})
	config := DefaultConfig()
	config.BackendURL = "http://backend.example.test"
	config.ListenAddress = "127.0.0.1:0"
	config.ReadinessTimeout = 25 * time.Millisecond
	server, err := NewServer(config, nil, WithTransport(transport))
	if err != nil {
		t.Fatal(err)
	}

	started := time.Now()
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/readyz", nil))
	elapsed := time.Since(started)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("GET /readyz status = %d, want 503", recorder.Code)
	}
	if elapsed > time.Second {
		t.Fatalf("readiness probe took %s despite a 25ms timeout", elapsed)
	}
	select {
	case <-probeCanceled:
	default:
		t.Error("readiness timeout did not cancel the backend request")
	}
}

func TestHealthzDoesNotConsultBackendReadiness(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	checker := readinessCheckerFunc(func(context.Context) error {
		calls.Add(1)
		return errors.New("backend down")
	})
	server := newTestProxy(t, "http://backend.example.test", WithReadinessChecker(checker))
	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/healthz", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("GET /healthz status = %d, want 200", recorder.Code)
	}
	if calls.Load() != 0 {
		t.Errorf("process health check consulted backend %d times", calls.Load())
	}
}

func TestReadinessReturnsUnavailableAfterShutdownStarts(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	checker := readinessCheckerFunc(func(context.Context) error {
		calls.Add(1)
		return nil
	})
	server := newTestProxy(t, "http://backend.example.test", WithReadinessChecker(checker))
	if err := server.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() error: %v", err)
	}

	recorder := httptest.NewRecorder()
	server.Handler().ServeHTTP(recorder, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/readyz", nil))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("GET /readyz status = %d after shutdown, want 503", recorder.Code)
	}
	if calls.Load() != 0 {
		t.Errorf("backend was probed %d times after shutdown began", calls.Load())
	}
}

func TestReadinessProbeInFlightWhenShutdownStartsReturnsUnavailable(t *testing.T) {
	t.Parallel()
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	checker := readinessCheckerFunc(func(context.Context) error {
		close(started)
		<-release
		return nil
	})
	server := newTestProxy(t, "http://backend.example.test", WithReadinessChecker(checker))
	recorder := httptest.NewRecorder()
	finished := make(chan struct{})
	go func() {
		server.Handler().ServeHTTP(recorder, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/readyz", nil))
		close(finished)
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("readiness checker did not start")
	}
	if err := server.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() error: %v", err)
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("readiness handler did not finish")
	}
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("in-flight GET /readyz status = %d after shutdown, want 503", recorder.Code)
	}
}

func TestDefaultTransportDoesNotUseEnvironmentProxy(t *testing.T) {
	config := DefaultConfig()
	transport := newTransport(config)
	if transport.Proxy != nil {
		t.Fatal("default upstream transport consults environment proxy settings")
	}
}

type readinessCheckerFunc func(context.Context) error

func (f readinessCheckerFunc) Check(ctx context.Context) error {
	return f(ctx)
}
