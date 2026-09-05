package controllers

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestPodDrainFanoutRequiresAllProxyReplicasAtZero(t *testing.T) {
	t.Parallel()
	acknowledged := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.EscapedPath() != "/internal/backends/by-pod/models/backend-0/drain" ||
			request.URL.Query().Get("timeout") != "100ms" {
			t.Errorf("downstream request = %s %s", request.Method, request.URL.String())
		}
		if authorization := request.Header.Get("Authorization"); authorization != "Bearer test-admin-token" {
			t.Errorf("authorization = %q", authorization)
		}
		_, _ = io.WriteString(writer, `{"pod_namespace":"models","pod_name":"backend-0","in_flight":0}`)
	}))
	defer acknowledged.Close()
	missing := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusNotFound)
	}))
	defer missing.Close()

	fanout := newTestDrainFanout(t, []string{acknowledged.URL, missing.URL}, 100*time.Millisecond)
	result, err := fanout.DrainPod(context.Background(), "models", "backend-0")
	if err != nil {
		t.Fatal(err)
	}
	if result.ProxyCount != 2 || result.InFlight != 0 || result.State != "drained" {
		t.Fatalf("result = %#v", result)
	}
}

func TestNewPodDrainFanoutRejectsUnsafeBearerToken(t *testing.T) {
	t.Parallel()
	_, err := NewPodDrainFanout(PodDrainFanoutConfig{
		Discovery: &EndpointFanoutAdmin{}, BearerToken: "secret\r\ninjected: value",
		Timeout: 100 * time.Millisecond,
	})
	if err == nil || !strings.Contains(err.Error(), "bearer token") {
		t.Fatalf("error = %v", err)
	}
}

func TestPodDrainFanoutSurfacesReplicaTimeoutWithoutLeakingBody(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusGatewayTimeout)
		_, _ = io.WriteString(writer, `{"pod_namespace":"models","pod_name":"backend-0","in_flight":2,"secret":"do-not-leak"}`)
	}))
	defer server.Close()
	fanout := newTestDrainFanout(t, []string{server.URL}, 100*time.Millisecond)
	result, err := fanout.DrainPod(context.Background(), "models", "backend-0")
	if err == nil || result.InFlight != 2 || strings.Contains(err.Error(), "secret") {
		t.Fatalf("result/error = %#v / %v", result, err)
	}
}

func TestOperatorDrainHandlerReturnsAllReplicaAcknowledgement(t *testing.T) {
	t.Parallel()
	proxy := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(writer, `{"pod_namespace":"models","pod_name":"backend-0","in_flight":0}`)
	}))
	defer proxy.Close()
	server := &OperatorDrainServer{Fanout: newTestDrainFanout(t, []string{proxy.URL}, 100*time.Millisecond)}
	request := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/internal/backends/by-pod/models/backend-0/drain", nil)
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status/body = %d/%s", response.Code, response.Body.String())
	}
	var result PodDrainResult
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.State != "drained" || result.ProxyCount != 1 || result.InFlight != 0 {
		t.Fatalf("result = %#v", result)
	}
}

func TestInjectedHTTPGetHookDrainsAllProxyReplicas(t *testing.T) {
	t.Parallel()
	var requests atomic.Int32
	proxy := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/internal/backends/by-pod/models/backend-0/drain" ||
			request.Header.Get("Authorization") != "Bearer test-admin-token" {
			t.Errorf("downstream request = %s %s, authorization = %q", request.Method, request.URL.Path, request.Header.Get("Authorization"))
		}
		requests.Add(1)
		_, _ = io.WriteString(writer, `{"pod_namespace":"models","pod_name":"backend-0","in_flight":0}`)
	}))
	t.Cleanup(proxy.Close)
	operator := httptest.NewServer(&OperatorDrainServer{
		Fanout: newTestDrainFanout(t, []string{proxy.URL, proxy.URL}, 100*time.Millisecond),
	})
	t.Cleanup(operator.Close)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "models", Name: "backend-0", Labels: map[string]string{"app": "vllm"}},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "backend"}}},
	}
	if changed, err := newTestPodMutator(t).mutate(context.Background(), pod); err != nil || !changed {
		t.Fatalf("inject preStop hook: changed=%t, err=%v", changed, err)
	}
	hook := pod.Spec.Containers[0].Lifecycle.PreStop.HTTPGet
	// A Kubernetes HTTPGet lifecycle hook always issues GET. Exercise the
	// generated path through the operator HTTP handler and a local proxy stub.
	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, operator.URL+hook.Path, nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := operator.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	var result PodDrainResult
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || result.State != "drained" || result.ProxyCount != 2 || requests.Load() != 2 {
		t.Fatalf("HTTPGet drain = status %d, result %#v, downstream requests %d", response.StatusCode, result, requests.Load())
	}
}

func TestPodDrainFanoutBoundsAllWorkerBatches(t *testing.T) {
	t.Parallel()
	var requests atomic.Int32
	proxy := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		<-request.Context().Done()
	}))
	t.Cleanup(proxy.Close)
	fanout := newTestDrainFanout(t, []string{proxy.URL, proxy.URL, proxy.URL}, 10*time.Millisecond)
	fanout.concurrency = 1
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started := time.Now()
	result, err := fanout.DrainPod(ctx, "models", "backend-0")
	if err == nil || result.State != "draining" || result.ProxyCount != 3 {
		t.Fatalf("result/error = %#v / %v", result, err)
	}
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Fatalf("drain exceeded the shared deadline: %s", elapsed)
	}
	if requests.Load() != 1 {
		t.Fatalf("started %d downstream requests; queued requests must stop at the shared deadline", requests.Load())
	}
}

func TestOperatorDrainHandlerRejectsUnsupportedMethodAndBody(t *testing.T) {
	t.Parallel()
	server := &OperatorDrainServer{Fanout: newTestDrainFanout(t, nil, 100*time.Millisecond)}
	for _, method := range []string{http.MethodHead, http.MethodPut, http.MethodDelete} {
		request := httptest.NewRequestWithContext(context.Background(), method, "/internal/backends/by-pod/models/backend-0/drain", nil)
		response := httptest.NewRecorder()
		server.ServeHTTP(response, request)
		if response.Code != http.StatusMethodNotAllowed || response.Header().Get("Allow") != "GET, POST" {
			t.Errorf("%s status/allow = %d/%q", method, response.Code, response.Header().Get("Allow"))
		}
	}
	for _, method := range []string{http.MethodGet, http.MethodPost} {
		request := httptest.NewRequestWithContext(context.Background(), method, "/internal/backends/by-pod/models/backend-0/drain", strings.NewReader("x"))
		response := httptest.NewRecorder()
		server.ServeHTTP(response, request)
		if response.Code != http.StatusBadRequest {
			t.Errorf("%s with body status = %d", method, response.Code)
		}
	}
}

func TestOperatorDrainHandlerFailsClosedWithNoProxyEndpoints(t *testing.T) {
	t.Parallel()
	server := &OperatorDrainServer{Fanout: newTestDrainFanout(t, nil, 100*time.Millisecond)}
	request := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/internal/backends/by-pod/models/backend-0/drain", nil)
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status/body = %d/%s", response.Code, response.Body.String())
	}
	var result PodDrainResult
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.State != "draining" || result.ProxyCount != 0 || result.InFlight != 0 {
		t.Fatalf("result = %#v", result)
	}
}

func TestOperatorDrainServerReadinessAndShutdown(t *testing.T) {
	t.Parallel()
	server := &OperatorDrainServer{
		Address: "127.0.0.1:0", Fanout: newTestDrainFanout(t, nil, 100*time.Millisecond),
		ShutdownTimeout: time.Second,
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Start(ctx) }()
	deadline := time.Now().Add(2 * time.Second)
	for server.Ready(nil) != nil && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if err := server.Ready(nil); err != nil {
		cancel()
		t.Fatalf("server never became ready: %v", err)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("drain server did not shut down")
	}
}

func newTestDrainFanout(t *testing.T, endpoints []string, timeout time.Duration) *PodDrainFanout {
	t.Helper()
	fanout, err := NewPodDrainFanout(PodDrainFanoutConfig{
		Discovery: &EndpointFanoutAdmin{}, BearerToken: "test-admin-token", Timeout: timeout, Concurrency: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	fanout.endpoints = func(context.Context, bool) ([]string, error) {
		return append([]string(nil), endpoints...), nil
	}
	return fanout
}
