package chaos

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/satwiksps/streamweld/internal/proxy/sse"
)

const vLLMSchemaVersion = "streamweld.vllm-benchmarks/v1"

// VLLMConfig controls the explicitly opt-in real-model comparison profile.
// The deterministic fake-backend matrix remains the failure correctness gate.
type VLLMConfig struct {
	ProxyURL          string
	DirectURL         string
	Model             string
	Prompt            string
	ConcurrentStreams int
	MaxTokens         int
	Client            *http.Client
	Now               func() time.Time
}

// VLLMReport compares real vLLM output and TTFT through direct and durable paths.
// It deliberately makes no Kubernetes failure claim.
type VLLMReport struct {
	SchemaVersion              string    `json:"schema_version"`
	GeneratedAt                time.Time `json:"generated_at"`
	Profile                    string    `json:"profile"`
	GOOS                       string    `json:"goos"`
	GOARCH                     string    `json:"goarch"`
	GoVersion                  string    `json:"go_version"`
	Model                      string    `json:"model"`
	PromptSHA256               string    `json:"prompt_sha256"`
	ConcurrentStreams          int       `json:"concurrent_streams"`
	MaxTokens                  int       `json:"max_tokens"`
	ExactOutputMatches         int       `json:"exact_output_matches"`
	OutputCorrect              bool      `json:"output_correct"`
	DirectTTFPMilliseconds     float64   `json:"direct_ttft_ms_p50"`
	StreamweldTTFPMilliseconds float64   `json:"streamweld_ttft_ms_p50"`
	AddedTTFTMilliseconds      float64   `json:"added_ttft_ms_p50"`
	Scope                      string    `json:"scope"`
}

// RunVLLM executes paired temperature-zero requests against a real vLLM and
// Streamweld. Exact output equality is required for every pair.
func RunVLLM(ctx context.Context, config VLLMConfig) (VLLMReport, error) {
	if ctx == nil {
		return VLLMReport{}, errors.New("vLLM context is nil")
	}
	if config.ConcurrentStreams == 0 {
		config.ConcurrentStreams = DefaultConcurrentStreams
	}
	if config.MaxTokens == 0 {
		config.MaxTokens = DefaultOutputTokens
	}
	if config.ConcurrentStreams < 1 || config.ConcurrentStreams > 1024 || config.MaxTokens < 1 || config.MaxTokens > 100_000 {
		return VLLMReport{}, errors.New("vLLM stream or token count is outside its safe bound")
	}
	if strings.TrimSpace(config.Model) == "" || config.Model != strings.TrimSpace(config.Model) {
		return VLLMReport{}, errors.New("vLLM model is required and cannot be padded")
	}
	if config.Prompt == "" {
		return VLLMReport{}, errors.New("vLLM prompt is required")
	}
	proxyURL, err := validateRemoteBaseURL(config.ProxyURL)
	if err != nil {
		return VLLMReport{}, fmt.Errorf("proxy URL: %w", err)
	}
	directURL, err := validateRemoteBaseURL(config.DirectURL)
	if err != nil {
		return VLLMReport{}, fmt.Errorf("direct URL: %w", err)
	}
	if config.Client == nil {
		config.Client = &http.Client{}
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	direct, err := runVLLMBatch(ctx, config.Client, directURL, config)
	if err != nil {
		return VLLMReport{}, fmt.Errorf("direct vLLM batch: %w", err)
	}
	durable, err := runVLLMBatch(ctx, config.Client, proxyURL, config)
	if err != nil {
		return VLLMReport{}, fmt.Errorf("streamweld vLLM batch: %w", err)
	}
	matches := 0
	for index := range direct {
		if direct[index].output == durable[index].output && direct[index].output != "" {
			matches++
		}
	}
	directTTFT := make([]time.Duration, len(direct))
	durableTTFT := make([]time.Duration, len(durable))
	for index := range direct {
		directTTFT[index] = direct[index].ttft
		durableTTFT[index] = durable[index].ttft
	}
	digest := sha256.Sum256([]byte(config.Prompt))
	report := VLLMReport{
		SchemaVersion:              vLLMSchemaVersion,
		GeneratedAt:                config.Now().UTC(),
		Profile:                    "real-vllm-opt-in",
		GOOS:                       runtime.GOOS,
		GOARCH:                     runtime.GOARCH,
		GoVersion:                  runtime.Version(),
		Model:                      config.Model,
		PromptSHA256:               hex.EncodeToString(digest[:]),
		ConcurrentStreams:          config.ConcurrentStreams,
		MaxTokens:                  config.MaxTokens,
		ExactOutputMatches:         matches,
		OutputCorrect:              matches == config.ConcurrentStreams,
		DirectTTFPMilliseconds:     roundMilliseconds(durationP50(directTTFT).Seconds() * 1000),
		StreamweldTTFPMilliseconds: roundMilliseconds(durationP50(durableTTFT).Seconds() * 1000),
		Scope:                      "paired temperature=0, seeded direct-vs-Streamweld baseline; no failure resilience claim",
	}
	report.AddedTTFTMilliseconds = roundMilliseconds(
		report.StreamweldTTFPMilliseconds - report.DirectTTFPMilliseconds,
	)
	if err := report.Validate(); err != nil {
		return VLLMReport{}, err
	}
	return report, nil
}

// Validate enforces the opt-in profile's exact-output gate.
func (report VLLMReport) Validate() error {
	if report.SchemaVersion != vLLMSchemaVersion || report.GeneratedAt.IsZero() || report.Profile != "real-vllm-opt-in" ||
		report.GOOS == "" || report.GOARCH == "" || report.GoVersion == "" || report.Model == "" ||
		report.PromptSHA256 == "" || report.Scope == "" {
		return errors.New("vLLM report metadata is incomplete")
	}
	if report.ConcurrentStreams <= 0 || report.MaxTokens <= 0 || report.ExactOutputMatches != report.ConcurrentStreams || !report.OutputCorrect {
		return fmt.Errorf("vLLM exact output correctness failed (%d/%d)", report.ExactOutputMatches, report.ConcurrentStreams)
	}
	if !finiteNonNegative(report.DirectTTFPMilliseconds) || report.DirectTTFPMilliseconds == 0 ||
		!finiteNonNegative(report.StreamweldTTFPMilliseconds) || report.StreamweldTTFPMilliseconds == 0 ||
		math.IsNaN(report.AddedTTFTMilliseconds) || math.IsInf(report.AddedTTFTMilliseconds, 0) {
		return errors.New("vLLM report contains an invalid TTFT measurement")
	}
	if report.AddedTTFTMilliseconds != roundMilliseconds(
		report.StreamweldTTFPMilliseconds-report.DirectTTFPMilliseconds,
	) {
		return errors.New("vLLM report added TTFT does not match its paired measurements")
	}
	return nil
}

type vLLMCompletion struct {
	output string
	ttft   time.Duration
}

func runVLLMBatch(ctx context.Context, client *http.Client, baseURL string, config VLLMConfig) ([]vLLMCompletion, error) {
	results := make(chan struct {
		index      int
		completion vLLMCompletion
		err        error
	}, config.ConcurrentStreams)
	start := make(chan struct{})
	var workers sync.WaitGroup
	workers.Add(config.ConcurrentStreams)
	for index := range config.ConcurrentStreams {
		go func() {
			defer workers.Done()
			select {
			case <-start:
			case <-ctx.Done():
				return
			}
			completion, err := runOneVLLM(ctx, client, baseURL, config, index)
			results <- struct {
				index      int
				completion vLLMCompletion
				err        error
			}{index: index, completion: completion, err: err}
		}()
	}
	close(start)
	go func() {
		workers.Wait()
		close(results)
	}()
	completions := make([]vLLMCompletion, config.ConcurrentStreams)
	received := 0
	for {
		select {
		case result, ok := <-results:
			if !ok {
				if received != config.ConcurrentStreams {
					return nil, fmt.Errorf("received %d vLLM streams, want %d", received, config.ConcurrentStreams)
				}
				return completions, nil
			}
			if result.err != nil {
				return nil, result.err
			}
			completions[result.index] = result.completion
			received++
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func runOneVLLM(
	ctx context.Context,
	client *http.Client,
	baseURL string,
	config VLLMConfig,
	index int,
) (vLLMCompletion, error) {
	body, err := json.Marshal(map[string]any{
		"model": config.Model,
		"messages": []map[string]string{{
			"role": "user", "content": fmt.Sprintf("%s\nDeterministic request index: %d", config.Prompt, index),
		}},
		"stream": true, "temperature": 0, "seed": 712_300 + index, "max_tokens": config.MaxTokens,
	})
	if err != nil {
		return vLLMCompletion{}, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return vLLMCompletion{}, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "text/event-stream")
	request.Header.Set("X-Streamweld-Idempotency-Key", fmt.Sprintf("vllm-%d-%d", index, time.Now().UnixNano()))
	started := time.Now()
	response, err := client.Do(request)
	if err != nil {
		return vLLMCompletion{}, err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		message, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return vLLMCompletion{}, fmt.Errorf("request %d status %s: %s", index, response.Status, bytes.TrimSpace(message))
	}
	decoder := sse.NewDecoder(response.Body)
	var output strings.Builder
	var ttft time.Duration
	sawToken := false
	done := false
	for {
		event, decodeErr := decoder.Decode()
		if errors.Is(decodeErr, io.EOF) {
			break
		}
		if decodeErr != nil {
			return vLLMCompletion{}, decodeErr
		}
		if bytes.Equal(event.Data, []byte("[DONE]")) {
			done = true
			break
		}
		text := completionText(event.Data)
		if text != "" && !sawToken {
			ttft = time.Since(started)
			sawToken = true
		}
		output.WriteString(text)
	}
	if !done || !sawToken || output.Len() == 0 {
		return vLLMCompletion{}, fmt.Errorf("request %d ended without a non-empty terminal stream", index)
	}
	return vLLMCompletion{output: output.String(), ttft: ttft}, nil
}

// WriteVLLMArtifacts writes separately named files so opt-in real-model timing
// can never be mistaken for the committed deterministic chaos matrix.
func WriteVLLMArtifacts(directory string, report VLLMReport) error {
	if err := report.Validate(); err != nil {
		return err
	}
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if err := os.WriteFile(filepath.Join(directory, "vllm-results.json"), data, 0o644); err != nil {
		return err
	}
	markdown := fmt.Sprintf(
		"# Streamweld real-vLLM opt-in results\n\nModel: `%s`  \nPrompt SHA-256: `%s`  \nToolchain: `%s` on `%s/%s`  \nStreams: %d  \nExact output matches: %d/%d  \nDirect TTFT p50: %.3f ms  \nStreamweld TTFT p50: %.3f ms  \nAdded TTFT p50: %.3f ms\n\n%s.\n",
		report.Model,
		report.PromptSHA256,
		report.GoVersion,
		report.GOOS,
		report.GOARCH,
		report.ConcurrentStreams,
		report.ExactOutputMatches,
		report.ConcurrentStreams,
		report.DirectTTFPMilliseconds,
		report.StreamweldTTFPMilliseconds,
		report.AddedTTFTMilliseconds,
		report.Scope,
	)
	return os.WriteFile(filepath.Join(directory, "vllm-results.md"), []byte(markdown), 0o644)
}
