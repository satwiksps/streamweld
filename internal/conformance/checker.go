package conformance

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	// RunsPerProbe is the protocol-mandated number of repetitions for every
	// conformance probe.
	RunsPerProbe = 3

	// ContinuationPrefill is the assistant suffix for the counting probe.
	ContinuationPrefill = "1 2 3 4"
	// MidWordPrefill is the assistant suffix for the partial-token probe.
	MidWordPrefill = "The capital of France is Par"
	// PunctuationPrefill is the assistant suffix for the comma-continuity probe.
	PunctuationPrefill = "The primary colors are red,"
	// IdempotencePrefill is the assistant suffix for the repeatability probe.
	IdempotencePrefill = "The deterministic sequence is alpha, beta,"

	defaultRequestTimeout = 30 * time.Second
	maxResponseBytes      = 4 << 20
)

// ProbeName identifies one normative chat-template probe.
type ProbeName string

const (
	// ProbeContinuation checks that generation continues at five without a restart.
	ProbeContinuation ProbeName = "continuation"
	// ProbeMidWord checks that a partially emitted word remains open.
	ProbeMidWord ProbeName = "mid_word"
	// ProbePunctuation checks that a comma does not begin a new assistant turn.
	ProbePunctuation ProbeName = "punctuation"
	// ProbeIdempotence checks exact temperature-zero repeatability.
	ProbeIdempotence ProbeName = "idempotence"
)

// CheckRequest identifies a backend and, when available, the immutable
// backend artifacts to use for caching. Both artifact fields must be supplied
// together; a request without either field is probed without caching.
type CheckRequest struct {
	BackendURL         string `json:"backend_url"`
	Model              string `json:"model"`
	BackendImageDigest string `json:"backend_image_digest,omitempty"`
	TokenizerHash      string `json:"tokenizer_hash,omitempty"`
}

// CacheKey is deliberately independent of a backend address. Replicas with
// the same immutable image, model, and tokenizer share one verdict.
type CacheKey struct {
	BackendImageDigest string `json:"backend_image_digest"`
	Model              string `json:"model"`
	TokenizerHash      string `json:"tokenizer_hash"`
}

// RunResult records one of the three deterministic attempts for a probe.
type RunResult struct {
	Attempt int    `json:"attempt"`
	Output  string `json:"output"`
	Passed  bool   `json:"passed"`
	Detail  string `json:"detail,omitempty"`
}

// ProbeResult records all attempts and the aggregate outcome of one probe.
type ProbeResult struct {
	Name   ProbeName   `json:"name"`
	Passed bool        `json:"passed"`
	Runs   []RunResult `json:"runs"`
}

// Report is the complete result of one conformance check. Cached reports
// retain their original CheckedAt time and set Cached for the current caller.
type Report struct {
	Verdict    Verdict       `json:"verdict"`
	Cached     bool          `json:"cached"`
	CheckedAt  time.Time     `json:"checked_at"`
	BackendURL string        `json:"backend_url"`
	Model      string        `json:"model"`
	CacheKey   CacheKey      `json:"cache_key"`
	Probes     []ProbeResult `json:"probes"`
}

// Cache stores reports by immutable model artifacts. Implementations must be
// safe for concurrent use.
type Cache interface {
	Get(context.Context, CacheKey) (Report, bool, error)
	Put(context.Context, CacheKey, Report) error
}

// Checker runs the normative probe suite. One Checker coalesces concurrent
// cache misses for an identical CacheKey so a newly Ready replica group is
// probed only once.
type Checker struct {
	client *http.Client
	cache  Cache
	now    func() time.Time

	mu      sync.Mutex
	flights map[CacheKey]*checkFlight
}

type checkFlight struct {
	done   chan struct{}
	report Report
	err    error
}

type probeDefinition struct {
	name      ProbeName
	user      string
	prefill   string
	maxTokens int
	evaluate  func(string) (bool, string)
}

var probeDefinitions = [...]probeDefinition{
	{
		name:      ProbeContinuation,
		user:      "Count from 1 to 10, numbers only.",
		prefill:   ContinuationPrefill,
		maxTokens: 16,
		evaluate:  evaluateContinuation,
	},
	{
		name:      ProbeMidWord,
		user:      "State the capital of France in one short sentence.",
		prefill:   MidWordPrefill,
		maxTokens: 8,
		evaluate:  evaluateMidWord,
	},
	{
		name:      ProbePunctuation,
		user:      "Name the three primary colors in one sentence.",
		prefill:   PunctuationPrefill,
		maxTokens: 16,
		evaluate:  evaluatePunctuation,
	},
	{
		name:      ProbeIdempotence,
		user:      "Continue the deterministic sequence alpha, beta, gamma, delta.",
		prefill:   IdempotencePrefill,
		maxTokens: 16,
		evaluate:  evaluateNonEmpty,
	},
}

// NewChecker constructs a checker. A nil client selects a bounded default
// client; a nil cache disables caching without changing probe behavior.
func NewChecker(client *http.Client, cache Cache) *Checker {
	if client == nil {
		client = &http.Client{Timeout: defaultRequestTimeout}
	}
	return &Checker{
		client:  client,
		cache:   cache,
		now:     time.Now,
		flights: make(map[CacheKey]*checkFlight),
	}
}

// Run executes the suite without artifact caching. Operators that know the
// backend image digest and tokenizer hash should call Check instead.
func (checker *Checker) Run(ctx context.Context, backendURL, model string) (Report, error) {
	return checker.Check(ctx, CheckRequest{BackendURL: backendURL, Model: model})
}

// Check returns a cached report when a complete immutable identity is
// supplied, otherwise it performs all four probes three times each.
func (checker *Checker) Check(ctx context.Context, request CheckRequest) (Report, error) {
	if checker == nil || checker.client == nil {
		return Report{Verdict: VerdictUnknown}, errors.New("conformance: nil checker")
	}
	endpoint, err := chatCompletionsEndpoint(request.BackendURL)
	if err != nil {
		return Report{Verdict: VerdictUnknown}, err
	}
	request.Model = strings.TrimSpace(request.Model)
	if request.Model == "" {
		return Report{Verdict: VerdictUnknown}, errors.New("conformance: model is required")
	}
	key, cacheable, err := request.cacheKey()
	if err != nil {
		return Report{Verdict: VerdictUnknown}, err
	}
	if !cacheable || checker.cache == nil {
		return checker.run(ctx, endpoint, request, key)
	}

	flight, leader := checker.acquireFlight(key)
	if !leader {
		select {
		case <-ctx.Done():
			return Report{Verdict: VerdictUnknown}, ctx.Err()
		case <-flight.done:
			report := cloneReport(flight.report)
			if flight.err == nil {
				report.Cached = true
				report.BackendURL = request.BackendURL
			}
			return report, flight.err
		}
	}

	report, checkErr := checker.checkCached(ctx, endpoint, request, key)
	checker.completeFlight(key, flight, report, checkErr)
	return report, checkErr
}

func (request CheckRequest) cacheKey() (CacheKey, bool, error) {
	digest := strings.TrimSpace(request.BackendImageDigest)
	tokenizer := strings.TrimSpace(request.TokenizerHash)
	if (digest == "") != (tokenizer == "") {
		return CacheKey{}, false, errors.New("conformance: backend image digest and tokenizer hash must be supplied together")
	}
	key := CacheKey{BackendImageDigest: digest, Model: request.Model, TokenizerHash: tokenizer}
	return key, digest != "", nil
}

func (checker *Checker) acquireFlight(key CacheKey) (*checkFlight, bool) {
	checker.mu.Lock()
	defer checker.mu.Unlock()
	if flight, ok := checker.flights[key]; ok {
		return flight, false
	}
	flight := &checkFlight{done: make(chan struct{})}
	checker.flights[key] = flight
	return flight, true
}

func (checker *Checker) completeFlight(key CacheKey, flight *checkFlight, report Report, err error) {
	checker.mu.Lock()
	flight.report = cloneReport(report)
	flight.err = err
	delete(checker.flights, key)
	close(flight.done)
	checker.mu.Unlock()
}

func (checker *Checker) checkCached(
	ctx context.Context,
	endpoint *url.URL,
	request CheckRequest,
	key CacheKey,
) (Report, error) {
	report, found, err := checker.cache.Get(ctx, key)
	if err != nil {
		return Report{Verdict: VerdictUnknown, BackendURL: request.BackendURL, Model: request.Model, CacheKey: key},
			fmt.Errorf("conformance: read verdict cache: %w", err)
	}
	if found {
		report = cloneReport(report)
		report.Cached = true
		report.BackendURL = request.BackendURL
		report.Model = request.Model
		report.CacheKey = key
		return report, nil
	}

	report, err = checker.run(ctx, endpoint, request, key)
	if err != nil {
		return report, err
	}
	if err := checker.cache.Put(ctx, key, cloneReport(report)); err != nil {
		return report, fmt.Errorf("conformance: store verdict cache: %w", err)
	}
	return report, nil
}

func (checker *Checker) run(
	ctx context.Context,
	endpoint *url.URL,
	request CheckRequest,
	key CacheKey,
) (Report, error) {
	report := Report{
		Verdict:    VerdictUnknown,
		CheckedAt:  checker.now().UTC(),
		BackendURL: request.BackendURL,
		Model:      request.Model,
		CacheKey:   key,
		Probes:     make([]ProbeResult, 0, len(probeDefinitions)),
	}
	for _, definition := range probeDefinitions {
		result := ProbeResult{
			Name:   definition.name,
			Passed: true,
			Runs:   make([]RunResult, 0, RunsPerProbe),
		}
		for attempt := 1; attempt <= RunsPerProbe; attempt++ {
			output, err := checker.execute(ctx, endpoint, request.Model, definition)
			if err != nil {
				return report, fmt.Errorf("conformance: %s probe attempt %d: %w", definition.name, attempt, err)
			}
			passed, detail := definition.evaluate(output)
			result.Runs = append(result.Runs, RunResult{
				Attempt: attempt,
				Output:  output,
				Passed:  passed,
				Detail:  detail,
			})
			result.Passed = result.Passed && passed
		}
		if definition.name == ProbeIdempotence {
			evaluateIdempotence(&result)
		}
		report.Probes = append(report.Probes, result)
	}
	report.Verdict = evaluateVerdict(report.Probes)
	return report, nil
}

func (checker *Checker) execute(
	ctx context.Context,
	endpoint *url.URL,
	model string,
	definition probeDefinition,
) (string, error) {
	body, err := json.Marshal(struct {
		Model                string         `json:"model"`
		Messages             []probeMessage `json:"messages"`
		Temperature          int            `json:"temperature"`
		MaxTokens            int            `json:"max_tokens"`
		N                    int            `json:"n"`
		Stream               bool           `json:"stream"`
		ContinueFinalMessage bool           `json:"continue_final_message"`
		AddGenerationPrompt  bool           `json:"add_generation_prompt"`
	}{
		Model: model,
		Messages: []probeMessage{
			{Role: "user", Content: definition.user},
			{Role: "assistant", Content: definition.prefill},
		},
		Temperature:          0,
		MaxTokens:            definition.maxTokens,
		N:                    1,
		Stream:               false,
		ContinueFinalMessage: true,
		AddGenerationPrompt:  false,
	})
	if err != nil {
		return "", fmt.Errorf("encode request: %w", err)
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}
	httpRequest.Header.Set("Accept", "application/json")
	httpRequest.Header.Set("Content-Type", "application/json")
	response, err := checker.client.Do(httpRequest)
	if err != nil {
		return "", fmt.Errorf("request backend: %w", err)
	}
	defer func() {
		_ = response.Body.Close()
	}()
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil {
		return "", fmt.Errorf("read response: %w", err)
	}
	if len(responseBody) > maxResponseBytes {
		return "", fmt.Errorf("response exceeds %d bytes", maxResponseBytes)
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return "", fmt.Errorf("backend returned %s: %s", response.Status, compactBody(responseBody))
	}
	var completion struct {
		Choices []struct {
			Message struct {
				Content *string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(responseBody, &completion); err != nil {
		return "", fmt.Errorf("decode completion response: %w", err)
	}
	if len(completion.Choices) == 0 || completion.Choices[0].Message.Content == nil {
		return "", errors.New("completion response has no first message content")
	}
	if !utf8.ValidString(*completion.Choices[0].Message.Content) {
		return "", errors.New("completion response content is not valid UTF-8")
	}
	return *completion.Choices[0].Message.Content, nil
}

type probeMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

func chatCompletionsEndpoint(raw string) (*url.URL, error) {
	if strings.TrimSpace(raw) != raw || raw == "" {
		return nil, errors.New("conformance: backend URL is required and must not contain surrounding whitespace")
	}
	endpoint, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("conformance: parse backend URL: %w", err)
	}
	if (endpoint.Scheme != "http" && endpoint.Scheme != "https") || endpoint.Host == "" {
		return nil, errors.New("conformance: backend URL must be an absolute HTTP(S) URL")
	}
	if endpoint.RawQuery != "" || endpoint.Fragment != "" {
		return nil, errors.New("conformance: backend URL must not contain a query or fragment")
	}
	path := strings.TrimRight(endpoint.Path, "/")
	switch {
	case strings.HasSuffix(path, "/v1/chat/completions"):
	case strings.HasSuffix(path, "/v1"):
		path += "/chat/completions"
	default:
		path += "/v1/chat/completions"
	}
	endpoint.Path = path
	endpoint.RawPath = ""
	return endpoint, nil
}

func evaluateContinuation(output string) (bool, string) {
	trimmed := strings.TrimLeftFunc(output, unicode.IsSpace)
	if trimmed == "" {
		return false, "continuation was empty"
	}
	first, size := utf8.DecodeRuneInString(trimmed)
	if first == '1' {
		return false, "continuation restarted at 1"
	}
	if first != '5' {
		return false, "continuation did not start with 5"
	}
	if len(trimmed) > size {
		next, _ := utf8.DecodeRuneInString(trimmed[size:])
		if unicode.IsDigit(next) {
			return false, "continuation started with a number other than the token 5"
		}
	}
	return true, ""
}

func evaluateMidWord(output string) (bool, string) {
	if output == "" {
		return false, "mid-word continuation was empty"
	}
	if !strings.HasPrefix(output, "is") {
		return false, "continuation did not complete Par as Paris"
	}
	if len(output) > len("is") {
		next, _ := utf8.DecodeRuneInString(output[len("is"):])
		if unicode.IsLetter(next) {
			return false, "continuation changed Paris into a different word"
		}
	}
	return true, ""
}

func evaluatePunctuation(output string) (bool, string) {
	if strings.TrimSpace(output) == "" {
		return false, "punctuation continuation was empty"
	}
	for _, value := range output {
		if !unicode.IsLetter(value) {
			continue
		}
		if unicode.IsUpper(value) {
			return false, "continuation began with a capitalized new sentence"
		}
		return true, ""
	}
	return false, "punctuation continuation contained no word"
}

func evaluateNonEmpty(output string) (bool, string) {
	if strings.TrimSpace(output) == "" {
		return false, "idempotence continuation was empty"
	}
	return true, ""
}

func evaluateIdempotence(result *ProbeResult) {
	if result == nil || len(result.Runs) != RunsPerProbe {
		if result != nil {
			result.Passed = false
		}
		return
	}
	want := result.Runs[0].Output
	for index := range result.Runs {
		if result.Runs[index].Output != want {
			result.Runs[index].Passed = false
			result.Runs[index].Detail = "temperature-zero continuation differed between repetitions"
			result.Passed = false
		}
	}
}

func evaluateVerdict(results []ProbeResult) Verdict {
	passed := make(map[ProbeName]bool, len(results))
	for _, result := range results {
		passed[result.Name] = result.Passed
	}
	if !passed[ProbeContinuation] || !passed[ProbeMidWord] {
		return VerdictUnsafe
	}
	if !passed[ProbePunctuation] || !passed[ProbeIdempotence] {
		return VerdictDegraded
	}
	return VerdictSafe
}

func compactBody(body []byte) string {
	const maxErrorBytes = 512
	trimmed := strings.TrimSpace(string(body))
	if len(trimmed) > maxErrorBytes {
		return trimmed[:maxErrorBytes] + "..."
	}
	return trimmed
}

func cloneReport(report Report) Report {
	report.Probes = append([]ProbeResult(nil), report.Probes...)
	for index := range report.Probes {
		report.Probes[index].Runs = append([]RunResult(nil), report.Probes[index].Runs...)
	}
	return report
}
