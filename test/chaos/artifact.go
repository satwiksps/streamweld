package chaos

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	resultsJSON = "results.json"
	resultsMD   = "results.md"
	readmeStart = "<!-- streamweld:benchmarks:start -->"
	readmeEnd   = "<!-- streamweld:benchmarks:end -->"
	liveDemoURL = "https://streamweld-failure-lab.satwiksub.chatgpt.site"
)

// WriteArtifacts validates and writes both committed benchmark formats from
// one in-memory report.
func WriteArtifacts(directory string, report Report) error {
	if err := report.Validate(); err != nil {
		return fmt.Errorf("refuse to write invalid benchmark report: %w", err)
	}
	if directory == "" {
		return errors.New("benchmark output directory is required")
	}
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return fmt.Errorf("create benchmark output directory: %w", err)
	}
	jsonData, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("encode benchmark JSON: %w", err)
	}
	jsonData = append(jsonData, '\n')
	markdown := RenderMarkdown(report)
	if err := os.WriteFile(filepath.Join(directory, resultsJSON), jsonData, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", resultsJSON, err)
	}
	if err := os.WriteFile(filepath.Join(directory, resultsMD), markdown, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", resultsMD, err)
	}
	return nil
}

// VerifyArtifacts checks the committed JSON correctness gate and proves the
// Markdown file was rendered from that exact JSON source.
func VerifyArtifacts(directory string) error {
	report, err := ReadArtifacts(directory)
	if err != nil {
		return err
	}
	markdown, err := os.ReadFile(filepath.Join(directory, resultsMD))
	if err != nil {
		return fmt.Errorf("read %s: %w", resultsMD, err)
	}
	want := RenderMarkdown(report)
	if !bytes.Equal(markdown, want) {
		return errors.New("results.md is not the canonical rendering of results.json; run make bench")
	}
	return nil
}

// ReadArtifacts loads and validates the machine-readable source artifact.
func ReadArtifacts(directory string) (Report, error) {
	jsonData, err := os.ReadFile(filepath.Join(directory, resultsJSON))
	if err != nil {
		return Report{}, fmt.Errorf("read %s: %w", resultsJSON, err)
	}
	report, err := DecodeReport(jsonData)
	if err != nil {
		return Report{}, fmt.Errorf("validate %s: %w", resultsJSON, err)
	}
	return report, nil
}

// RenderMarkdown creates the human-readable evidence table.
func RenderMarkdown(report Report) []byte {
	var output strings.Builder
	output.WriteString("# Streamweld local deterministic chaos model results\n\n")
	output.WriteString("Generated from `benchmarks/results.json` by `make bench`. Do not edit this table by hand.\n\n")
	output.WriteString("This committed default is an in-process model/simulation. It does not claim Kubernetes process disruption; the nightly kind profile is the physical failure-injection gate.\n\n")
	output.WriteString("This is the **")
	output.WriteString(markdownCell(report.Profile.Name))
	output.WriteString("** profile (`")
	output.WriteString(markdownCell(report.Profile.Execution))
	output.WriteString("`), generated at `")
	output.WriteString(report.GeneratedAt.Format("2006-01-02T15:04:05.999999999Z07:00"))
	output.WriteString("`. It uses ")
	output.WriteString(strconv.Itoa(report.Profile.ConcurrentStreams))
	output.WriteString(" concurrent streams per scenario and ")
	output.WriteString(strconv.Itoa(report.Profile.OutputTokensPerStream))
	output.WriteString(" deterministic output tokens per stream.\n\n")
	output.WriteString("Timing scope: ")
	output.WriteString(report.Profile.TimingScope)
	output.WriteString(". Latency is evidence from this run, not a cross-host regression threshold. Correctness is the regression gate.\n\n")
	_, _ = fmt.Fprintf(
		&output,
		"The paired fake backend applies the same %.3f ms first-token delay to direct and Streamweld requests; TTFT values serialize to %.3f ms resolution.\n\n",
		report.Profile.TTFTBackendDelayMilliseconds,
		report.Profile.TTFTSerializationMilliseconds,
	)
	writeResultTable(&output, report)
	output.WriteString("\nScenario-specific expected terminals are recorded in the JSON artifact: explicit stop is `stopped`, unsafe-template is `migration_refused`, and Redis loss is `done_degraded`.\n")
	return []byte(output.String())
}

// RenderREADMEBenchmarkSection renders the marker-owned README section.
func RenderREADMEBenchmarkSection(report Report) []byte {
	var output strings.Builder
	output.WriteString(readmeStart)
	output.WriteString("\n## Local chaos model (simulation) results\n\n")
	output.WriteString("[Open the live failure lab](")
	output.WriteString(liveDemoURL)
	output.WriteString(") to compare the durable and direct paths side by side.\n\n")
	output.WriteString("This table is generated from [`benchmarks/results.json`](benchmarks/results.json) by `make bench`; edits inside these markers are rejected by `make bench-check`. It reports an in-process model/simulation—not Kubernetes process disruption. The non-skippable nightly [`kind` matrix](.github/workflows/nightly.yml) is the physical failure-injection gate. The committed run is the honestly labelled `")
	output.WriteString(markdownCell(report.Profile.Name))
	output.WriteString("` profile, not a kind or GPU claim.\n\n")
	_, _ = fmt.Fprintf(
		&output,
		"TTFT is a wall-clock p50 from the recorded host. Both paths include the fake backend's %.3f ms first-token delay, values serialize to %.3f ms, and CI gates correctness rather than cross-host timing.\n\n",
		report.Profile.TTFTBackendDelayMilliseconds,
		report.Profile.TTFTSerializationMilliseconds,
	)
	writeResultTable(&output, report)
	output.WriteString("\nFull metadata and scenario-specific terminal outcomes are in [`benchmarks/results.md`](benchmarks/results.md).\n")
	output.WriteString(readmeEnd)
	return []byte(output.String())
}

// UpdateREADMEBenchmarkSection inserts or replaces only the marker-owned slice.
func UpdateREADMEBenchmarkSection(path string, report Report) error {
	if err := report.Validate(); err != nil {
		return err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read README: %w", err)
	}
	section := RenderREADMEBenchmarkSection(report)
	updated, err := replaceREADMESection(data, section)
	if err != nil {
		return err
	}
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat README: %w", err)
	}
	if err := os.WriteFile(path, updated, info.Mode().Perm()); err != nil {
		return fmt.Errorf("write README benchmark section: %w", err)
	}
	return nil
}

// VerifyREADMEBenchmarkSection detects drift from the JSON-derived rendering.
func VerifyREADMEBenchmarkSection(path string, report Report) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read README: %w", err)
	}
	start := bytes.Index(data, []byte(readmeStart))
	end := bytes.Index(data, []byte(readmeEnd))
	if start < 0 || end < start || bytes.Count(data, []byte(readmeStart)) != 1 || bytes.Count(data, []byte(readmeEnd)) != 1 {
		return errors.New("README benchmark markers are missing, duplicated, or out of order; run make bench")
	}
	end += len(readmeEnd)
	if !bytes.Equal(data[start:end], RenderREADMEBenchmarkSection(report)) {
		return errors.New("README benchmark section drifted from results.json; run make bench")
	}
	return nil
}

func replaceREADMESection(data, section []byte) ([]byte, error) {
	startCount := bytes.Count(data, []byte(readmeStart))
	endCount := bytes.Count(data, []byte(readmeEnd))
	if startCount == 0 && endCount == 0 {
		anchor := []byte("\n## Kubernetes operator")
		index := bytes.Index(data, anchor)
		if index < 0 {
			return nil, errors.New("README has no benchmark markers or Kubernetes operator insertion anchor")
		}
		updated := make([]byte, 0, len(data)+len(section)+2)
		updated = append(updated, data[:index]...)
		updated = append(updated, '\n')
		updated = append(updated, section...)
		updated = append(updated, '\n')
		updated = append(updated, data[index:]...)
		return updated, nil
	}
	if startCount != 1 || endCount != 1 {
		return nil, errors.New("README benchmark markers must appear exactly once")
	}
	start := bytes.Index(data, []byte(readmeStart))
	end := bytes.Index(data, []byte(readmeEnd))
	if end < start {
		return nil, errors.New("README benchmark markers are out of order")
	}
	end += len(readmeEnd)
	updated := make([]byte, 0, len(data)-end+start+len(section))
	updated = append(updated, data[:start]...)
	updated = append(updated, section...)
	updated = append(updated, data[end:]...)
	return updated, nil
}

func writeResultTable(output *strings.Builder, report Report) {
	output.WriteString("| Scenario | Tokens/stream | Started | Completed | Migrated | Rescued tokens | Prompt tokens re-billed | Seam p50/p99 (bytes) | Direct TTFT p50 (ms) | Streamweld TTFT p50 (ms) | Added TTFT p50 (ms) | Correct |\n")
	output.WriteString("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|:---:|\n")
	for _, result := range report.Results {
		_, _ = fmt.Fprintf(
			output,
			"| %s | %d | %d | %d | %d | %d | %d | %d/%d | %.3f | %.3f | %.3f | %t |\n",
			markdownCell(string(result.Scenario)),
			result.OutputTokensPerStream,
			result.StreamsStarted,
			result.StreamsCompleted,
			result.StreamsMigrated,
			result.TokensRescued,
			result.PromptTokensRebilled,
			result.SeamOverlapBytesP50,
			result.SeamOverlapBytesP99,
			result.DirectTTFPMilliseconds,
			result.StreamweldTTFPMilliseconds,
			result.AddedTTFTMilliseconds,
			result.OutputCorrect,
		)
	}
}

func markdownCell(value string) string {
	value = strings.ReplaceAll(value, "|", "\\|")
	return strings.ReplaceAll(value, "\n", " ")
}
