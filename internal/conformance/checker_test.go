package conformance

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestProbeEvaluators(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		evaluate func(string) (bool, string)
		output   string
		want     bool
	}{
		{name: "continuation", evaluate: evaluateContinuation, output: " 5 6", want: true},
		{name: "continuation empty", evaluate: evaluateContinuation, output: "\n\t", want: false},
		{name: "continuation restart", evaluate: evaluateContinuation, output: " 1 2", want: false},
		{name: "continuation wrong", evaluate: evaluateContinuation, output: "6", want: false},
		{name: "continuation number boundary", evaluate: evaluateContinuation, output: "50", want: false},
		{name: "midword", evaluate: evaluateMidWord, output: "is.", want: true},
		{name: "midword empty", evaluate: evaluateMidWord, output: "", want: false},
		{name: "midword whitespace seam", evaluate: evaluateMidWord, output: " is", want: false},
		{name: "midword changed word", evaluate: evaluateMidWord, output: "isian", want: false},
		{name: "punctuation", evaluate: evaluatePunctuation, output: " green and blue", want: true},
		{name: "punctuation ignores quote", evaluate: evaluatePunctuation, output: " \"green", want: true},
		{name: "punctuation capitalized", evaluate: evaluatePunctuation, output: " Green", want: false},
		{name: "punctuation empty", evaluate: evaluatePunctuation, output: "...", want: false},
		{name: "idempotence nonempty", evaluate: evaluateNonEmpty, output: " gamma", want: true},
		{name: "idempotence empty", evaluate: evaluateNonEmpty, output: " \n", want: false},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, _ := test.evaluate(test.output)
			if got != test.want {
				t.Fatalf("evaluate(%q) = %v, want %v", test.output, got, test.want)
			}
		})
	}
}

func TestEvaluateVerdictRequiresNamedCoreProbes(t *testing.T) {
	t.Parallel()
	if got := evaluateVerdict(nil); got != VerdictUnsafe {
		t.Fatalf("empty result verdict = %s, want UNSAFE", got)
	}
	safe := []ProbeResult{
		{Name: ProbeContinuation, Passed: true},
		{Name: ProbeMidWord, Passed: true},
		{Name: ProbePunctuation, Passed: true},
		{Name: ProbeIdempotence, Passed: true},
	}
	if got := evaluateVerdict(safe); got != VerdictSafe {
		t.Fatalf("all-pass verdict = %s, want SAFE", got)
	}
	safe[2].Passed = false
	if got := evaluateVerdict(safe); got != VerdictDegraded {
		t.Fatalf("secondary-failure verdict = %s, want DEGRADED", got)
	}
	safe[1].Passed = false
	if got := evaluateVerdict(safe); got != VerdictUnsafe {
		t.Fatalf("core-failure verdict = %s, want UNSAFE", got)
	}
}

func TestCheckerOperationalFailureIsUnknownAndNotCached(t *testing.T) {
	t.Parallel()
	var calls atomic.Int64
	backend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		http.Error(writer, "fixture unavailable", http.StatusServiceUnavailable)
	}))
	defer backend.Close()
	cache := &recordingCache{}
	checker := NewChecker(backend.Client(), cache)
	request := CheckRequest{
		BackendURL:         backend.URL,
		Model:              "fixture",
		BackendImageDigest: "sha256:image",
		TokenizerHash:      "sha256:tokenizer",
	}
	for attempt := 0; attempt < 2; attempt++ {
		report, err := checker.Check(context.Background(), request)
		if err == nil || !strings.Contains(err.Error(), "503 Service Unavailable") {
			t.Fatalf("Check() error = %v, want backend status", err)
		}
		if report.Verdict != VerdictUnknown {
			t.Errorf("operational failure verdict = %s, want UNKNOWN", report.Verdict)
		}
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("uncached failures sent %d requests, want 2", got)
	}
	if got := cache.puts.Load(); got != 0 {
		t.Fatalf("failed probes wrote cache %d times", got)
	}
}

func TestCheckerRejectsMalformedCompletion(t *testing.T) {
	t.Parallel()
	backend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte(`{"choices":[]}`))
	}))
	defer backend.Close()
	checker := NewChecker(backend.Client(), nil)
	report, err := checker.Run(context.Background(), backend.URL, "fixture")
	if err == nil || !strings.Contains(err.Error(), "no first message content") {
		t.Fatalf("Run() error = %v, want missing content", err)
	}
	if report.Verdict != VerdictUnknown {
		t.Fatalf("malformed response verdict = %s, want UNKNOWN", report.Verdict)
	}
}

func TestCheckerInputValidationDoesNotContactBackend(t *testing.T) {
	t.Parallel()
	checker := NewChecker(nil, NewMemoryCache())
	tests := []CheckRequest{
		{},
		{BackendURL: "backend.local", Model: "fixture"},
		{BackendURL: "http://backend.local?x=1", Model: "fixture"},
		{BackendURL: "http://backend.local", Model: " "},
		{BackendURL: "http://backend.local", Model: "fixture", BackendImageDigest: "sha256:image"},
		{BackendURL: "http://backend.local", Model: "fixture", TokenizerHash: "sha256:tokenizer"},
	}
	for _, request := range tests {
		if _, err := checker.Check(context.Background(), request); err == nil {
			t.Errorf("Check(%+v) unexpectedly succeeded", request)
		}
	}
	var nilChecker *Checker
	if _, err := nilChecker.Run(context.Background(), "http://backend.local", "fixture"); err == nil {
		t.Fatal("nil Checker unexpectedly succeeded")
	}
}

func TestChatCompletionsEndpointVariants(t *testing.T) {
	t.Parallel()
	for input, want := range map[string]string{
		"http://backend.local":                      "http://backend.local/v1/chat/completions",
		"http://backend.local/":                     "http://backend.local/v1/chat/completions",
		"http://backend.local/prefix":               "http://backend.local/prefix/v1/chat/completions",
		"http://backend.local/prefix/v1":            "http://backend.local/prefix/v1/chat/completions",
		"http://backend.local/v1/chat/completions/": "http://backend.local/v1/chat/completions",
	} {
		endpoint, err := chatCompletionsEndpoint(input)
		if err != nil {
			t.Errorf("chatCompletionsEndpoint(%q) error = %v", input, err)
			continue
		}
		if got := endpoint.String(); got != want {
			t.Errorf("chatCompletionsEndpoint(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestMemoryCacheCopiesReportsAndHonorsCancellation(t *testing.T) {
	t.Parallel()
	cache := NewMemoryCache()
	key := CacheKey{BackendImageDigest: "image", Model: "model", TokenizerHash: "tokenizer"}
	report := Report{
		Verdict: VerdictSafe,
		Probes: []ProbeResult{{
			Name: ProbeContinuation,
			Runs: []RunResult{{Attempt: 1, Output: "original", Passed: true}},
		}},
	}
	if err := cache.Put(context.Background(), key, report); err != nil {
		t.Fatal(err)
	}
	report.Probes[0].Runs[0].Output = "caller mutation"
	first, found, err := cache.Get(context.Background(), key)
	if err != nil || !found {
		t.Fatalf("Get() = (%+v, %v, %v), want hit", first, found, err)
	}
	if first.Probes[0].Runs[0].Output != "original" {
		t.Fatalf("cached nested slice was aliased: %+v", first)
	}
	first.Probes[0].Runs[0].Output = "returned mutation"
	second, _, _ := cache.Get(context.Background(), key)
	if second.Probes[0].Runs[0].Output != "original" {
		t.Fatal("Get returned an alias of cached nested slices")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := cache.Get(ctx, key); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Get() error = %v", err)
	}
	if err := cache.Put(ctx, key, report); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Put() error = %v", err)
	}
}

func TestCachedReportRetainsProbeTimeAndUsesCurrentAddress(t *testing.T) {
	t.Parallel()
	cache := NewMemoryCache()
	key := CacheKey{BackendImageDigest: "image", Model: "model", TokenizerHash: "tokenizer"}
	checkedAt := time.Date(2026, 8, 22, 0, 0, 0, 0, time.UTC)
	if err := cache.Put(context.Background(), key, Report{
		Verdict:   VerdictSafe,
		CheckedAt: checkedAt,
		CacheKey:  key,
	}); err != nil {
		t.Fatal(err)
	}
	checker := NewChecker(nil, cache)
	report, err := checker.Check(context.Background(), CheckRequest{
		BackendURL:         "http://replica-b.local",
		Model:              key.Model,
		BackendImageDigest: key.BackendImageDigest,
		TokenizerHash:      key.TokenizerHash,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !report.Cached || !report.CheckedAt.Equal(checkedAt) || report.BackendURL != "http://replica-b.local" {
		t.Fatalf("cached report = %+v", report)
	}
}

type recordingCache struct {
	puts atomic.Int64
}

func (cache *recordingCache) Get(context.Context, CacheKey) (Report, bool, error) {
	return Report{}, false, nil
}

func (cache *recordingCache) Put(context.Context, CacheKey, Report) error {
	cache.puts.Add(1)
	return nil
}
