package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDrainCallsOperatorBarrierAndPrintsResult(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.EscapedPath() != "/internal/backends/by-pod/models/pod-a/drain" {
			t.Errorf("request = %s %s", request.Method, request.URL.EscapedPath())
			http.NotFound(writer, request)
			return
		}
		if request.ContentLength > 0 {
			t.Errorf("request Content-Length = %d, want empty", request.ContentLength)
		}
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(drainResult{
			PodNamespace: "models", PodName: "pod-a", ProxyCount: 3,
			InFlight: 0, State: "drained",
		})
	}))
	t.Cleanup(server.Close)

	var stdout, stderr bytes.Buffer
	code := run([]string{
		"drain", "--endpoint", server.URL, "--namespace", "models", "pod-a",
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("run(drain) = %d, stderr=%q", code, stderr.String())
	}
	if got := stdout.String(); !strings.Contains(got, "Pod: models/pod-a") ||
		!strings.Contains(got, "Proxies: 3") || !strings.Contains(got, "State: drained") {
		t.Fatalf("stdout = %q", got)
	}
}

func TestDrainSurfacesIncompleteFanoutAsFailure(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusGatewayTimeout)
		_ = json.NewEncoder(writer).Encode(drainResult{
			PodNamespace: "models", PodName: "pod-a", ProxyCount: 2,
			InFlight: 1, State: "draining",
		})
	}))
	t.Cleanup(server.Close)

	var stdout, stderr bytes.Buffer
	code := run([]string{
		"drain", "--endpoint", server.URL, "--namespace", "models", "--json", "pod-a",
	}, &stdout, &stderr)
	if code != 1 || stdout.Len() != 0 || !strings.Contains(stderr.String(), `"in_flight": 1`) {
		t.Fatalf("run(drain timeout) = %d, stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}

func TestDrainRejectsUnsafeArgumentsLocally(t *testing.T) {
	t.Parallel()
	tests := [][]string{
		{"drain"},
		{"drain", "--namespace", "Bad", "pod-a"},
		{"drain", "--endpoint", "http://user:secret@example.test", "pod-a"},
		{"drain", "--timeout", "0s", "pod-a"},
	}
	for _, args := range tests {
		var stdout, stderr bytes.Buffer
		if code := run(args, &stdout, &stderr); code != 2 {
			t.Errorf("run(%v) = %d, stderr=%q", args, code, stderr.String())
		}
	}
}
