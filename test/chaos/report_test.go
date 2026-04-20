package chaos

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestReportValidationIsACorrectnessRegressionGate(t *testing.T) {
	t.Parallel()

	report := validTestReport(t)
	report.Results[0].StreamsMigrated--
	if err := report.Validate(); err == nil || !strings.Contains(err.Error(), "migrated 2/3") {
		t.Fatalf("partial migration validation error = %v", err)
	}

	report = validTestReport(t)
	report.Results[4].OutputCorrect = false
	report.Results[4].CorrectStreams--
	if err := report.Validate(); err == nil || !strings.Contains(err.Error(), "output correctness failed") {
		t.Fatalf("incorrect output validation error = %v", err)
	}

	report = validTestReport(t)
	report.Results[5].StreamsStopped = -1
	if err := report.Validate(); err == nil || !strings.Contains(err.Error(), "streams_stopped = -1") {
		t.Fatalf("negative counter validation error = %v", err)
	}

	report = validTestReport(t)
	report.Results[0].SeamOverlapBytesP50 = 0
	if err := report.Validate(); err == nil || !strings.Contains(err.Error(), "no valid observed seam-overlap") {
		t.Fatalf("missing seam observation validation error = %v", err)
	}

	report = validTestReport(t)
	report.Results[0].PromptTokensRebilled--
	if err := report.Validate(); err == nil || !strings.Contains(err.Error(), "re-billing does not match observed migration") {
		t.Fatalf("unobserved prompt re-billing validation error = %v", err)
	}

	report = validTestReport(t)
	report.Results[0].DirectTTFPMilliseconds = 0
	if err := report.Validate(); err == nil || !strings.Contains(err.Error(), "invalid TTFT") {
		t.Fatalf("zero direct TTFT validation error = %v", err)
	}
}

func TestDecodeReportRejectsUnknownFieldsAndTrailingValues(t *testing.T) {
	t.Parallel()

	report := validTestReport(t)
	data, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeReport(append(data, []byte(` {}`)...)); err == nil || !strings.Contains(err.Error(), "trailing") {
		t.Fatalf("trailing JSON error = %v", err)
	}
	withUnknown := strings.Replace(string(data), `"schema_version":`, `"unknown":true,"schema_version":`, 1)
	if _, err := DecodeReport([]byte(withUnknown)); err == nil {
		t.Fatal("DecodeReport() accepted an unknown field")
	}
}

func TestArtifactsRoundTripFromOneJSONSource(t *testing.T) {
	t.Parallel()

	directory := t.TempDir()
	report := validTestReport(t)
	if err := WriteArtifacts(directory, report); err != nil {
		t.Fatalf("WriteArtifacts() error = %v", err)
	}
	if err := VerifyArtifacts(directory); err != nil {
		t.Fatalf("VerifyArtifacts() error = %v", err)
	}
	markdownPath := filepath.Join(directory, resultsMD)
	markdown, err := os.ReadFile(markdownPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(markdown), "| pod-kill | 16 | 3 | 3 | 3 |") {
		t.Fatalf("rendered Markdown omitted matrix counters:\n%s", markdown)
	}
	if err := os.WriteFile(markdownPath, append(markdown, []byte("manual edit\n")...), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := VerifyArtifacts(directory); err == nil || !strings.Contains(err.Error(), "not the canonical rendering") {
		t.Fatalf("modified Markdown verification error = %v", err)
	}
}

func TestREADMEBenchmarkSectionIsGeneratedAndDriftChecked(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "README.md")
	original := "# Project\n\nKeep this introduction.\n\n## Kubernetes operator\n\nKeep this body.\n"
	if err := os.WriteFile(path, []byte(original), 0o644); err != nil {
		t.Fatal(err)
	}
	report := validTestReport(t)
	if err := UpdateREADMEBenchmarkSection(path, report); err != nil {
		t.Fatalf("UpdateREADMEBenchmarkSection() error = %v", err)
	}
	if err := VerifyREADMEBenchmarkSection(path, report); err != nil {
		t.Fatalf("VerifyREADMEBenchmarkSection() error = %v", err)
	}

	generated, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{
		"Keep this introduction.",
		"Keep this body.",
		readmeStart,
		readmeEnd,
		liveDemoURL,
		"| pod-kill | 16 | 3 | 3 | 3 |",
	} {
		if !strings.Contains(string(generated), required) {
			t.Errorf("generated README does not contain %q", required)
		}
	}
	if err := UpdateREADMEBenchmarkSection(path, report); err != nil {
		t.Fatalf("second UpdateREADMEBenchmarkSection() error = %v", err)
	}
	stable, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(stable) != string(generated) {
		t.Fatal("README update is not reproducible")
	}

	drifted := strings.Replace(string(stable), "| pod-kill |", "| manually-edited |", 1)
	if err := os.WriteFile(path, []byte(drifted), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := VerifyREADMEBenchmarkSection(path, report); err == nil || !strings.Contains(err.Error(), "drifted") {
		t.Fatalf("README drift verification error = %v", err)
	}
}

func TestREADMEBenchmarkSectionRejectsMalformedMarkers(t *testing.T) {
	t.Parallel()

	report := validTestReport(t)
	path := filepath.Join(t.TempDir(), "README.md")
	malformed := "# Project\n\n" + readmeStart + "\n" + readmeStart + "\n" + readmeEnd + "\n"
	if err := os.WriteFile(path, []byte(malformed), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := UpdateREADMEBenchmarkSection(path, report); err == nil || !strings.Contains(err.Error(), "exactly once") {
		t.Fatalf("malformed marker update error = %v", err)
	}
}

func validTestReport(t *testing.T) Report {
	t.Helper()
	report, err := RunLocal(context.Background(), LocalConfig{
		ConcurrentStreams: 3,
		OutputTokens:      16,
		Now: func() time.Time {
			return time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)
		},
		MeasureTTFT: func(_ context.Context, _ int) (TTFTMeasurement, error) {
			return TTFTMeasurement{DirectMilliseconds: 1, StreamweldMilliseconds: 2}, nil
		},
	})
	if err != nil {
		t.Fatalf("RunLocal() error = %v", err)
	}
	return report
}
