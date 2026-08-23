package proxy

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/satwiksps/streamweld/internal/backend"
	"github.com/satwiksps/streamweld/internal/conformance"
)

func TestPodDrainMigratesControllerRetiredLeasedBackend(t *testing.T) {
	t.Parallel()
	originStarted := make(chan struct{}, 1)
	originCanceled := make(chan struct{}, 1)
	originServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		startFailoverBackendSSE(writer)
		if !writeFailoverBackendData(writer, failoverChatChunk(
			"retired-origin", "old ", "",
			&failoverUsage{PromptTokens: 1, CompletionTokens: 1, TotalTokens: 2},
		)) {
			return
		}
		originStarted <- struct{}{}
		<-request.Context().Done()
		originCanceled <- struct{}{}
	}))
	t.Cleanup(originServer.Close)
	targetServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writeTriggerTarget(writer, "replacement-target", "old recovered", 2)
	}))
	t.Cleanup(targetServer.Close)

	origin := newFailoverBackend(t, "route-a/pod-old", originServer.URL, "model-v1", conformance.VerdictSafe)
	pool := newFailoverBackendPool(t, origin)
	harness := newFailoverHTTPHarness(t, originServer.URL, pool, nil)
	initial := testRouteUpdate("test-model", "uid-a", 1, origin.ID.String(), originServer.URL)
	initial.Backends[0].ModelVersion = "model-v1"
	initial.Backends[0].PodNamespace = "models"
	initial.Backends[0].PodName = "pod-old"
	if _, err := harness.server.durable.routes.apply("models/route-a", initial); err != nil {
		t.Fatal(err)
	}

	streamRequest := newFailoverHTTPRequest(t, http.MethodPost, harness.url+"/v1/chat/completions",
		`{"model":"test-model","stream":true,"messages":[{"role":"user","content":"retained drain"}],"max_tokens":10}`)
	streamRequest.Header.Set(headerVerbose, "1")
	streamResponse := doFailoverHTTPRequest(t, harness.client, streamRequest)
	defer closeFailoverBody(t, streamResponse.Body)
	awaitFailoverSignal(t, originStarted, "retired origin response")

	replacement := testRouteUpdate("test-model", "uid-a", 1, "route-a/pod-new", targetServer.URL)
	replacement.Backends[0].ModelVersion = "model-v1"
	replacement.Backends[0].PodNamespace = "models"
	replacement.Backends[0].PodName = "pod-new"
	if _, err := harness.server.durable.routes.apply("models/route-a", replacement); err != nil {
		t.Fatal(err)
	}
	if states := pool.List(); len(states) != 1 || states[0].ID != "route-a/pod-new" {
		t.Fatalf("live pool after replacement = %+v", states)
	}
	retained := pool.ListRetained()
	var retainedOrigin *backend.State
	for index := range retained {
		if retained[index].ID == origin.ID {
			retainedOrigin = &retained[index]
			break
		}
	}
	if len(retained) != 2 || retainedOrigin == nil || !retainedOrigin.Draining || retainedOrigin.InFlight != 1 {
		t.Fatalf("retained pool after replacement = %+v", retained)
	}

	drainRequest := newFailoverHTTPRequest(t, http.MethodPost,
		harness.url+"/internal/backends/by-pod/models/pod-old/drain?timeout=2s", "")
	drainResponse := doFailoverHTTPRequest(t, harness.client, drainRequest)
	defer closeFailoverBody(t, drainResponse.Body)
	drainBody := readFailoverBody(t, drainResponse.Body)
	if drainResponse.StatusCode != http.StatusOK {
		t.Fatalf("retired pod drain status = %d, body=%s", drainResponse.StatusCode, drainBody)
	}
	var drained podDrainResponse
	decodeFailoverJSON(t, drainBody, &drained)
	if drained.InFlight != 0 || strings.Join(drained.Backends, ",") != origin.ID.String() {
		t.Fatalf("retired pod drain response = %+v", drained)
	}
	awaitFailoverSignal(t, originCanceled, "retired origin migration cancellation")

	events := readAllFailoverSSE(t, streamResponse.Body)
	requireTriggerMigration(t, events, "drain", 2)
	requireFailoverDone(t, events)
	for _, state := range pool.ListRetained() {
		if state.ID == origin.ID {
			t.Fatalf("retired backend remained after zero-inflight wait: %+v", state)
		}
	}
}
