package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/satwiksps/streamweld/internal/backend"
	"github.com/satwiksps/streamweld/internal/conformance"
)

func TestRouteBackendRegistryFencesGenerationUIDAndDeletion(t *testing.T) {
	t.Parallel()
	registry, pool := newTestRouteRegistry(t)
	update := testRouteUpdate("model-a", "uid-a", 2, "route-a/pod-a", "http://pod-a:8000")
	if result, err := registry.apply("models/route-a", update); err != nil || result.BackendCount != 1 {
		t.Fatalf("apply initial route = (%+v, %v)", result, err)
	}
	requireAcquiredBackend(t, pool, "model-a", "route-a/pod-a")

	// EndpointSlice contents can change without changing InferenceRoute
	// generation. Reconciles for one object key are synchronous, so an exact
	// retry is a no-op while changed same-generation content replaces the set.
	changed := testRouteUpdate("model-a", "uid-a", 2, "route-a/pod-b", "http://pod-b:8000")
	if _, err := registry.apply("models/route-a", changed); err != nil {
		t.Fatalf("apply same-generation EndpointSlice change: %v", err)
	}
	if _, err := registry.apply("models/route-a", changed); err != nil {
		t.Fatalf("apply exact retry: %v", err)
	}
	requireAcquiredBackend(t, pool, "model-a", "route-a/pod-b")

	stale := testRouteUpdate("model-a", "uid-a", 1, "route-a/stale", "http://stale:8000")
	if _, err := registry.apply("models/route-a", stale); !errors.Is(err, errRouteGenerationStale) {
		t.Fatalf("stale apply error = %v, want errRouteGenerationStale", err)
	}
	requireAcquiredBackend(t, pool, "model-a", "route-a/pod-b")

	deleted := changed
	deleted.Deleted = true
	deleted.Backends = nil
	if result, err := registry.apply("models/route-a", deleted); err != nil || result.BackendCount != 0 {
		t.Fatalf("apply deletion tombstone = (%+v, %v)", result, err)
	}
	if _, ok := registry.policyForModel("model-a"); ok {
		t.Fatal("deleted route retained a serving policy")
	}
	if _, err := registry.acquireModel("deleted-model", "model-a"); !errors.Is(err, backend.ErrNoEligibleBackend) {
		t.Fatalf("deleted model admission error = %v, want ErrNoEligibleBackend", err)
	}
	requireAcquiredBackend(t, pool, "unmanaged-model", "standalone")

	resurrection := testRouteUpdate("model-a", "uid-a", 3, "route-a/resurrected", "http://resurrected:8000")
	if _, err := registry.apply("models/route-a", resurrection); !errors.Is(err, errRouteGenerationStale) {
		t.Fatalf("deleted UID resurrection error = %v, want errRouteGenerationStale", err)
	}

	recreated := testRouteUpdate("model-a", "uid-b", 1, "route-a/pod-new", "http://pod-new:8000")
	if _, err := registry.apply("models/route-a", recreated); err != nil {
		t.Fatalf("apply recreated object: %v", err)
	}
	requireAcquiredBackend(t, pool, "model-a", "route-a/pod-new")
	if _, err := registry.apply("models/route-a", resurrection); !errors.Is(err, errRouteUIDConflict) {
		t.Fatalf("old UID after recreation error = %v, want errRouteUIDConflict", err)
	}
}

func TestRouteBackendRegistryRejectsCrossRouteConflictsAtomically(t *testing.T) {
	t.Parallel()
	registry, pool := newTestRouteRegistry(t)
	first := testRouteUpdate("model-a", "uid-a", 1, "route-a/pod", "http://pod-a:8000")
	if _, err := registry.apply("models/route-a", first); err != nil {
		t.Fatal(err)
	}

	duplicateModel := testRouteUpdate("model-a", "uid-b", 1, "route-b/pod", "http://pod-b:8000")
	if _, err := registry.apply("models/route-b", duplicateModel); err == nil {
		t.Fatal("duplicate model across live routes was accepted")
	}
	requireAcquiredBackend(t, pool, "model-a", "route-a/pod")

	duplicateID := testRouteUpdate("model-b", "uid-c", 1, "route-a/pod", "http://pod-c:8000")
	if _, err := registry.apply("models/route-c", duplicateID); err == nil {
		t.Fatal("duplicate backend ID across live routes was accepted")
	}
	if _, ok := registry.policyForModel("model-b"); ok {
		t.Fatal("rejected update mutated the policy registry")
	}
	requireAcquiredBackend(t, pool, "model-a", "route-a/pod")
}

func TestRouteBackendRegistryAccountsForRetiredLeasedBackends(t *testing.T) {
	t.Parallel()
	registry, pool := newTestRouteRegistry(t)
	initial := testRouteUpdate("model-a", "uid-a", 1, "route-a/pod-old", "http://pod-old:8000")
	if _, err := registry.apply("models/route-a", initial); err != nil {
		t.Fatal(err)
	}
	lease, err := pool.AcquireID("route-a/pod-old", "owner-a")
	if err != nil {
		t.Fatal(err)
	}

	replacement := testRouteUpdate("model-a", "uid-a", 1, "route-a/pod-new", "http://pod-new:8000")
	result, err := registry.apply("models/route-a", replacement)
	if err != nil {
		lease.Release()
		t.Fatal(err)
	}
	if result.BackendCount != 1 || result.DrainingBackends != 1 || result.ActiveStreams != 1 ||
		!slices.Equal(result.DrainingBackendIDs, []string{"route-a/pod-old"}) {
		lease.Release()
		t.Fatalf("replacement result = %+v, want one serving and one retained draining backend", result)
	}

	lease.Release()
	result, err = registry.apply("models/route-a", replacement)
	if err != nil {
		t.Fatal(err)
	}
	if result.DrainingBackends != 0 || result.ActiveStreams != 0 {
		t.Fatalf("post-release result = %+v, want retired accounting pruned", result)
	}
}

func TestRouteBackendRegistryBoundsDrainingBackendIdentities(t *testing.T) {
	t.Parallel()
	registry, pool := newTestRouteRegistry(t)
	initial := testRouteUpdate("model-a", "uid-a", 1, "route-a/pod-000", "http://pod-000:8000")
	for index := 1; index <= maxRouteResultDrainingBackendIDs; index++ {
		initial.Backends = append(initial.Backends, routeBackendInput{
			ID: fmt.Sprintf("route-a/pod-%03d", index), URL: fmt.Sprintf("http://pod-%03d:8000", index),
			TemplateVerdict: conformance.VerdictSafe,
		})
	}
	if _, err := registry.apply("models/route-a", initial); err != nil {
		t.Fatal(err)
	}
	leases := make([]*backend.Lease, 0, len(initial.Backends))
	for _, registered := range initial.Backends {
		lease, err := pool.AcquireID(backend.ID(registered.ID), "owner:"+registered.ID)
		if err != nil {
			t.Fatal(err)
		}
		leases = append(leases, lease)
	}
	t.Cleanup(func() {
		for _, lease := range leases {
			lease.Release()
		}
	})

	replacement := testRouteUpdate("model-a", "uid-a", 1, "route-a/current", "http://current:8000")
	result, err := registry.apply("models/route-a", replacement)
	if err != nil {
		t.Fatal(err)
	}
	if result.DrainingBackends != maxRouteResultDrainingBackendIDs+1 ||
		result.ActiveStreams != maxRouteResultDrainingBackendIDs+1 || len(result.DrainingBackendIDs) != 0 {
		t.Fatalf("bounded result = %+v", result)
	}
}

func TestRoutePolicySnapshotsAreImmutableAndModelScoped(t *testing.T) {
	t.Parallel()
	registry, _ := newTestRouteRegistry(t)
	first := testRouteUpdate("model-a", "uid-a", 1, "route-a/pod", "http://pod-a:8000")
	first.Policy.MaxMigrations = 1
	first.Policy.JournalTTL = "2m"
	if _, err := registry.apply("models/route-a", first); err != nil {
		t.Fatal(err)
	}
	oldPolicy, ok := registry.policyForModel("model-a")
	if !ok {
		t.Fatal("route policy was not registered")
	}

	changed := first
	changed.Policy.MaxMigrations = 7
	changed.Policy.JournalTTL = "9m"
	if _, err := registry.apply("models/route-a", changed); err != nil {
		t.Fatalf("apply changed policy: %v", err)
	}
	newPolicy, ok := registry.policyForModel("model-a")
	if !ok || newPolicy.MaxMigrations != 7 || newPolicy.JournalTTL != 9*time.Minute {
		t.Fatalf("new policy = %+v, present=%t", newPolicy, ok)
	}
	if oldPolicy.MaxMigrations != 1 || oldPolicy.JournalTTL != 2*time.Minute {
		t.Fatalf("captured old policy mutated: %+v", oldPolicy)
	}
	if _, ok := registry.policyForModel("model-b"); ok {
		t.Fatal("unregistered model resolved a route policy")
	}
}

func TestRouteAdminRequiresBearerAndRejectsUnknownJSON(t *testing.T) {
	t.Parallel()
	registry, _ := newTestRouteRegistry(t)
	handler := &Handler{
		durable:    &durableService{routes: registry},
		adminToken: "secret-token",
	}
	update := testRouteUpdate("model-a", "uid-a", 1, "route-a/pod", "http://pod-a:8000")
	body, err := json.Marshal(update)
	if err != nil {
		t.Fatal(err)
	}
	target := "/internal/routes/" + url.PathEscape("models/route-a") + "/backends"

	unauthorized := httptest.NewRecorder()
	handler.ServeHTTP(unauthorized, httptest.NewRequestWithContext(context.Background(), http.MethodPut, target, strings.NewReader(string(body))))
	if unauthorized.Code != http.StatusUnauthorized || unauthorized.Header().Get("WWW-Authenticate") != "Bearer" {
		t.Fatalf("unauthorized response = %d, headers=%v", unauthorized.Code, unauthorized.Header())
	}

	authorizedRequest := httptest.NewRequestWithContext(context.Background(), http.MethodPut, target, strings.NewReader(string(body)))
	authorizedRequest.Header.Set("Authorization", "Bearer secret-token")
	authorized := httptest.NewRecorder()
	handler.ServeHTTP(authorized, authorizedRequest)
	if authorized.Code != http.StatusOK {
		t.Fatalf("authorized response = %d, body=%s", authorized.Code, authorized.Body.String())
	}

	unknownBody := strings.TrimSuffix(string(body), "}") + `,"unknown":true}`
	unknownRequest := httptest.NewRequestWithContext(context.Background(), http.MethodPut, target, strings.NewReader(unknownBody))
	unknownRequest.Header.Set("Authorization", "Bearer secret-token")
	unknown := httptest.NewRecorder()
	handler.ServeHTTP(unknown, unknownRequest)
	if unknown.Code != http.StatusBadRequest {
		t.Fatalf("unknown field response = %d, body=%s", unknown.Code, unknown.Body.String())
	}
}

func TestPodDrainMarksEveryBackendForThePodAndWaitsForZero(t *testing.T) {
	t.Parallel()
	registry, pool := newTestRouteRegistry(t)
	update := testRouteUpdate("model-a", "uid-a", 1, "route-a/port-8000", "http://pod-a:8000")
	update.Backends[0].PodNamespace = "models"
	update.Backends[0].PodName = "pod-a"
	update.Backends = append(update.Backends, routeBackendInput{
		ID: "route-a/port-9000", URL: "http://pod-a:9000",
		TemplateVerdict: conformance.VerdictSafe,
		PodNamespace:    "models", PodName: "pod-a",
	})
	if _, err := registry.apply("models/route-a", update); err != nil {
		t.Fatal(err)
	}
	first, err := pool.AcquireID("route-a/port-8000", "owner-a")
	if err != nil {
		t.Fatal(err)
	}
	second, err := pool.AcquireID("route-a/port-9000", "owner-b")
	if err != nil {
		first.Release()
		t.Fatal(err)
	}
	time.AfterFunc(20*time.Millisecond, func() {
		first.Release()
		second.Release()
	})

	handler := &Handler{durable: &durableService{backends: pool}}
	response := httptest.NewRecorder()
	request := httptest.NewRequestWithContext(
		context.Background(),
		http.MethodPost,
		"/internal/backends/by-pod/models/pod-a/drain?timeout=500ms",
		nil,
	)
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("pod drain response = %d, body=%s", response.Code, response.Body.String())
	}
	var result podDrainResponse
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.InFlight != 0 || result.State != "draining" ||
		strings.Join(result.Backends, ",") != "route-a/port-8000,route-a/port-9000" {
		t.Fatalf("pod drain result = %+v", result)
	}
	for _, id := range []backend.ID{"route-a/port-8000", "route-a/port-9000"} {
		state, err := pool.Get(id)
		if err != nil || !state.Draining || state.InFlight != 0 {
			t.Fatalf("backend %s after drain = (%+v, %v)", id, state, err)
		}
	}
}

func TestReadinessTracksTheLiveDynamicBackendSet(t *testing.T) {
	t.Parallel()
	static := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/health" {
			http.Error(writer, "not ready", http.StatusServiceUnavailable)
			return
		}
		writer.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(static.Close)
	dynamic := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/health" {
			http.NotFound(writer, request)
			return
		}
		writer.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(dynamic.Close)

	config := DefaultConfig()
	config.BackendURL = static.URL + "/openai"
	server, err := NewServer(config, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(server.closeIdleConnections)
	update := testRouteUpdate("model-a", "uid-a", 1, "route-a/pod", dynamic.URL)
	if _, err := server.durable.routes.apply("models/route-a", update); err != nil {
		t.Fatal(err)
	}

	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/readyz", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("dynamic readiness = %d, body=%s", response.Code, response.Body.String())
	}

	update.Deleted = true
	update.Backends = nil
	if _, err := server.durable.routes.apply("models/route-a", update); err != nil {
		t.Fatal(err)
	}
	response = httptest.NewRecorder()
	server.Handler().ServeHTTP(response, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/readyz", nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("restored static readiness = %d, want 503", response.Code)
	}
}

func newTestRouteRegistry(t *testing.T) (*routeBackendRegistry, *backend.Pool) {
	t.Helper()
	staticURL, err := url.Parse("http://standalone:8000")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := backend.NewPool(backend.DefaultConfig(), backend.Backend{
		ID: "standalone", URL: staticURL, TemplateVerdict: conformance.VerdictUnknown,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.SetHealth("standalone", backend.HealthHealthy); err != nil {
		t.Fatal(err)
	}
	registry, err := newRouteBackendRegistry(pool)
	if err != nil {
		t.Fatal(err)
	}
	return registry, pool
}

func testRouteUpdate(model, uid string, generation int64, id, rawURL string) routeBackendUpdate {
	return routeBackendUpdate{
		Model: model, UID: uid, ObservedGeneration: generation,
		Policy: routePolicyInput{
			MaxMigrations: 3, MaxMigrationTokens: 8192,
			MaxStreamDuration: "15m", OrphanPolicy: OrphanContinue,
			OrphanTimeout: "60s", SeamWindowBytes: 64, JournalTTL: "10m",
		},
		Backends: []routeBackendInput{{
			ID: id, URL: rawURL, TemplateVerdict: conformance.VerdictSafe,
		}},
	}
}

func requireAcquiredBackend(t *testing.T, pool *backend.Pool, model string, want backend.ID) {
	t.Helper()
	lease, err := pool.AcquireModel("test-owner", model)
	if err != nil {
		t.Fatalf("AcquireModel(%q): %v", model, err)
	}
	defer lease.Release()
	if got := lease.Backend().ID; got != want {
		t.Fatalf("AcquireModel(%q) backend = %s, want %s", model, got, want)
	}
}
