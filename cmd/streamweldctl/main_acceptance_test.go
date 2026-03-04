package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/streamweld/streamweld/internal/conformance"
)

func TestDoctorCommandHumanReport(t *testing.T) {
	t.Parallel()

	backend, calls := newDoctorBackend(t, false)
	defer backend.Close()
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	exitCode := run([]string{"doctor", "--backend", backend.URL, "--model", "fixture-model"}, &stdout, &stderr)
	if exitCode != 0 {
		t.Fatalf("run(doctor) exit = %d, want 0; stderr = %q", exitCode, stderr.String())
	}
	if !strings.Contains(stdout.String(), "Verdict: SAFE") {
		t.Fatalf("doctor output does not contain SAFE verdict: %q", stdout.String())
	}
	if got := strings.Count(stdout.String(), "PASS"); got != 4 {
		t.Errorf("doctor output has %d PASS results, want one for each of four probes: %q", got, stdout.String())
	}
	if got := calls.Load(); got != 12 {
		t.Errorf("doctor sent %d completion requests, want 12", got)
	}
}

func TestDoctorCommandBrokenTemplateJSON(t *testing.T) {
	t.Parallel()

	backend, calls := newDoctorBackend(t, true)
	defer backend.Close()
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	exitCode := run([]string{"doctor", "--backend", backend.URL, "--model", "broken-model", "--json"}, &stdout, &stderr)
	if exitCode != 1 {
		t.Fatalf("run(doctor broken template) exit = %d, want 1; stderr = %q", exitCode, stderr.String())
	}
	var report conformance.Report
	if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
		t.Fatalf("decode doctor JSON output %q: %v", stdout.String(), err)
	}
	if report.Verdict != conformance.VerdictUnsafe {
		t.Fatalf("broken template verdict = %s, want UNSAFE", report.Verdict)
	}
	if len(report.Probes) != 4 {
		t.Fatalf("broken template report has %d probes, want 4", len(report.Probes))
	}
	if got := calls.Load(); got != 12 {
		t.Errorf("doctor sent %d completion requests, want 12", got)
	}
}

func TestDoctorCommandUsageError(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if exitCode := run([]string{"doctor", "--backend", "http://backend.invalid"}, &stdout, &stderr); exitCode != 2 {
		t.Fatalf("doctor without --model exit = %d, want 2", exitCode)
	}
	if stderr.Len() == 0 {
		t.Fatal("doctor usage error did not explain the missing argument on stderr")
	}
}

func newDoctorBackend(t *testing.T, brokenContinuation bool) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		var body struct {
			Messages []struct {
				Content string `json:"content"`
			} `json:"messages"`
		}
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)
			return
		}
		prefill := ""
		if len(body.Messages) > 0 {
			prefill = body.Messages[len(body.Messages)-1].Content
		}
		output := " stable continuation"
		switch prefill {
		case "1 2 3 4":
			output = " 5 6 7 8 9 10"
			if brokenContinuation {
				output = " 1 2 3 4 5"
			}
		case "The capital of France is Par":
			output = "is."
		case "The primary colors are red,":
			output = " green and blue."
		case "The deterministic sequence is alpha, beta,":
			output = " gamma."
		}
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"choices": []any{map[string]any{
				"message":       map[string]any{"role": "assistant", "content": output},
				"finish_reason": "stop",
			}},
		})
	}))
	return server, &calls
}
