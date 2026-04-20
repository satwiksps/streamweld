package main

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestBackendOOMFailsOnlyTheOriginalAttempt(t *testing.T) {
	t.Parallel()

	handler := newHandler(config{defaultTokens: 12}, slog.New(slog.NewTextHandler(io.Discard, nil)))
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
