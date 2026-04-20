package chaos

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRunVLLMUsesExactPairedOutputAsItsGate(t *testing.T) {
	t.Parallel()

	backend := httptest.NewServer(http.HandlerFunc(serveTTFTBackend))
	t.Cleanup(backend.Close)
	report, err := RunVLLM(context.Background(), VLLMConfig{
		ProxyURL:          backend.URL,
		DirectURL:         backend.URL,
		Model:             "real/model",
		Prompt:            "deterministic prompt",
		ConcurrentStreams: 3,
		MaxTokens:         8,
		Now: func() time.Time {
			return time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)
		},
	})
	if err != nil {
		t.Fatalf("RunVLLM() error = %v", err)
	}
	if !report.OutputCorrect || report.ExactOutputMatches != 3 || report.PromptSHA256 == "" {
		t.Fatalf("report = %+v", report)
	}
	directory := t.TempDir()
	if err := WriteVLLMArtifacts(directory, report); err != nil {
		t.Fatalf("WriteVLLMArtifacts() error = %v", err)
	}
	for _, name := range []string{"vllm-results.json", "vllm-results.md"} {
		if info, err := os.Stat(filepath.Join(directory, name)); err != nil || info.Size() == 0 {
			t.Errorf("artifact %s stat = (%v, %v)", name, info, err)
		}
	}
}

func TestRunVLLMRequiresExplicitConnectionInputs(t *testing.T) {
	t.Parallel()

	if _, err := RunVLLM(context.Background(), VLLMConfig{}); err == nil {
		t.Fatal("RunVLLM() accepted an implicit profile")
	}
}
