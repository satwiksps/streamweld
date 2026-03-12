package controllers

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
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
