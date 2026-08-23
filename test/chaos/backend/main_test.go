package main

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestPhysicalFailureOriginalStaysInFlightUntilCanceled(t *testing.T) {
	t.Parallel()

	handler := newHandler(config{defaultTokens: 8})
	requestContext, cancel := context.WithCancel(context.Background())
	request := httptest.NewRequestWithContext(requestContext, http.MethodPost, "/v1/chat/completions", bytes.NewBufferString(`{
		"model":"streamweld/deterministic-chaos",
		"messages":[{"role":"user","content":"streamweld-chaos:pod-kill:0"}],
		"stream":true,
		"max_tokens":8
	}`))
	response := newFirstTokenRecorder()
	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(response, request)
		close(done)
	}()

	select {
	case <-response.firstToken:
	case <-time.After(time.Second):
		cancel()
		<-done
		t.Fatal("original physical-failure attempt did not flush its first token")
	}
	select {
	case <-done:
		t.Fatal("original physical-failure attempt completed before cancellation")
	case <-time.After(25 * time.Millisecond):
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("original physical-failure attempt did not stop after cancellation")
	}
	body := response.Body.String()
	if !strings.Contains(body, "token-000 ") || strings.Contains(body, "token-001 ") || strings.Contains(body, "[DONE]") {
		t.Fatalf("held original response = %q", body)
	}
}

func TestOnlyOriginalPhysicalFailureAttemptsAreHeld(t *testing.T) {
	t.Parallel()

	for _, scenario := range []string{"pod-kill", "rolling-update", "spot-reclaim", "unsafe-template"} {
		if !holdOriginalForPhysicalFailure(scenario, 0) {
			t.Errorf("original %s attempt is not held", scenario)
		}
		if holdOriginalForPhysicalFailure(scenario, 1) {
			t.Errorf("continuation %s attempt is held", scenario)
		}
	}
	for _, scenario := range []string{"baseline", "backend-oom", "client-drop", "explicit-stop", "redis-down", "slow-consumer"} {
		if holdOriginalForPhysicalFailure(scenario, 0) {
			t.Errorf("original %s attempt is unexpectedly held", scenario)
		}
	}
}

type firstTokenRecorder struct {
	*httptest.ResponseRecorder
	firstToken chan struct{}
	once       sync.Once
}

func newFirstTokenRecorder() *firstTokenRecorder {
	return &firstTokenRecorder{ResponseRecorder: httptest.NewRecorder(), firstToken: make(chan struct{})}
}

func (recorder *firstTokenRecorder) Write(payload []byte) (int, error) {
	written, err := recorder.ResponseRecorder.Write(payload)
	if bytes.Contains(payload, []byte("token-000 ")) {
		recorder.once.Do(func() { close(recorder.firstToken) })
	}
	return written, err
}

func TestBackendOOMFailsOnlyTheOriginalAttempt(t *testing.T) {
	t.Parallel()

	handler := newHandler(config{defaultTokens: 12})
	original := performCompletion(t, handler, `{
		"model":"streamweld/deterministic-chaos",
		"messages":[{"role":"user","content":"streamweld-chaos:backend-oom:0"}],
		"stream":true,
		"max_tokens":8
	}`)
	if !strings.Contains(original.Body.String(), `"code":"backend_oom"`) || strings.Contains(original.Body.String(), "[DONE]") {
		t.Fatalf("original OOM stream = %q", original.Body.String())
	}

	continuation := performCompletion(t, handler, `{
		"model":"streamweld/deterministic-chaos",
		"messages":[
			{"role":"user","content":"streamweld-chaos:backend-oom:0"},
			{"role":"assistant","content":"token-000 token-001 token-002 "}
		],
		"stream":true,
		"max_tokens":5
	}`)
	if strings.Contains(continuation.Body.String(), "backend_oom") ||
		!strings.Contains(continuation.Body.String(), `"streamweld_chaos_raw_delta":"token-002 "`) ||
		!strings.Contains(continuation.Body.String(), "token-002 ") ||
		!strings.Contains(continuation.Body.String(), "token-003 ") ||
		!strings.Contains(continuation.Body.String(), "[DONE]") {
		t.Fatalf("continuation stream = %q", continuation.Body.String())
	}
}

func TestUnsafeTemplateBreaksTheTwoRequiredConformanceProbes(t *testing.T) {
	t.Parallel()

	if got := conformanceOutput([]message{{Role: "assistant", Content: "1 2 3 4"}}, true); got != "1 2 3 4" {
		t.Fatalf("unsafe continuation output = %q", got)
	}
	if got := conformanceOutput([]message{{Role: "assistant", Content: "The capital of France is Par"}}, true); got != "" {
		t.Fatalf("unsafe mid-word output = %q", got)
	}
	if got := conformanceOutput([]message{{Role: "assistant", Content: "1 2 3 4"}}, false); got != "5 6 7 8 9 10" {
		t.Fatalf("safe continuation output = %q", got)
	}
}

func performCompletion(t *testing.T, handler http.Handler, body string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/v1/chat/completions", bytes.NewBufferString(body))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("completion status = %d, body = %q", response.Code, response.Body.String())
	}
	return response
}
