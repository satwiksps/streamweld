package conformance

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

const (
	continuationPrefill = "1 2 3 4"
	midwordPrefill      = "The capital of France is Par"
	punctuationPrefill  = "The primary colors are red,"
	idempotencePrefill  = "The deterministic sequence is alpha, beta,"
)

func TestCheckerProbeMatrixAndVerdicts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		behavior probeBehavior
		want     Verdict
	}{
		{name: "all probes pass", want: VerdictSafe},
		{
			name: "continuation restarts",
			behavior: probeBehavior{outputs: map[string][]string{
				continuationPrefill: {" 1 2 3 4 5", " 1 2 3 4 5", " 1 2 3 4 5"},
			}},
			want: VerdictUnsafe,
		},
		{
			name: "midword continuation is empty",
			behavior: probeBehavior{outputs: map[string][]string{
				midwordPrefill: {"", "", ""},
			}},
			want: VerdictUnsafe,
		},
		{
			name: "punctuation starts a new sentence",
			behavior: probeBehavior{outputs: map[string][]string{
				punctuationPrefill: {" Green and blue.", " Green and blue.", " Green and blue."},
			}},
			want: VerdictDegraded,
		},
		{
			name: "temperature zero is not idempotent",
			behavior: probeBehavior{outputs: map[string][]string{
				idempotencePrefill: {" gamma.", " delta.", " epsilon."},
			}},
			want: VerdictDegraded,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			backend := newProbeBackend(t, test.behavior)
			defer backend.Close()

			checker := NewChecker(backend.Client(), nil)
			report, err := checker.Run(context.Background(), backend.URL, "fixture-model")
			if err != nil {
				t.Fatalf("Run() error = %v", err)
			}
			if report.Verdict != test.want {
				t.Fatalf("Run() verdict = %s, want %s; probes = %+v", report.Verdict, test.want, report.Probes)
			}

			assertFourProbesRunThreeTimes(t, report)
			assertCanonicalProbeRequests(t, backend.Requests(), "fixture-model")
		})
	}
}

func TestCheckerCacheKeyIsolation(t *testing.T) {
	t.Parallel()

	backend := newProbeBackend(t, probeBehavior{})
	defer backend.Close()
	checker := NewChecker(backend.Client(), NewMemoryCache())
	base := CheckRequest{
		BackendURL:         backend.URL,
		BackendImageDigest: "sha256:image-a",
		Model:              "model-a",
		TokenizerHash:      "sha256:tokenizer-a",
	}

	first, err := checker.Check(context.Background(), base)
	if err != nil {
		t.Fatalf("first Check() error = %v", err)
	}
	if first.Cached || first.Verdict != VerdictSafe {
		t.Fatalf("first Check() = %+v, want uncached SAFE report", first)
	}
	second, err := checker.Check(context.Background(), base)
	if err != nil {
		t.Fatalf("cached Check() error = %v", err)
	}
	if !second.Cached || second.Verdict != VerdictSafe {
		t.Fatalf("second Check() = %+v, want cached SAFE report", second)
	}
	if got := len(backend.Requests()); got != 12 {
		t.Fatalf("same-key checks sent %d requests, want 12", got)
	}

	variants := []CheckRequest{
		withDigest(base, "sha256:image-b"),
		withModel(base, "model-b"),
		withTokenizer(base, "sha256:tokenizer-b"),
	}
	for index, request := range variants {
		report, checkErr := checker.Check(context.Background(), request)
		if checkErr != nil {
			t.Fatalf("isolated Check(%d) error = %v", index, checkErr)
		}
		if report.Cached {
			t.Errorf("isolated Check(%d) unexpectedly hit another key's cache: %+v", index, report.CacheKey)
		}
	}
	if got := len(backend.Requests()); got != 48 {
		t.Fatalf("three isolated keys produced %d total requests, want 48", got)
	}
}

func TestCheckerCoalescesConcurrentCacheMisses(t *testing.T) {
	t.Parallel()

	releaseFirstRequest := make(chan struct{})
	firstRequestStarted := make(chan struct{})
	var signalOnce sync.Once
	backend := newProbeBackend(t, probeBehavior{
		beforeResponse: func() {
			signalOnce.Do(func() { close(firstRequestStarted) })
			<-releaseFirstRequest
		},
	})
	defer backend.Close()
	checker := NewChecker(backend.Client(), NewMemoryCache())
	request := CheckRequest{
		BackendURL:         backend.URL,
		BackendImageDigest: "sha256:shared-image",
		Model:              "shared-model",
		TokenizerHash:      "sha256:shared-tokenizer",
	}

	const callers = 24
	start := make(chan struct{})
	reports := make(chan Report, callers)
	errors := make(chan error, callers)
	var callersReady sync.WaitGroup
	callersReady.Add(callers)
	for range callers {
		go func() {
			callersReady.Done()
			<-start
			report, err := checker.Check(context.Background(), request)
			reports <- report
			errors <- err
		}()
	}
	callersReady.Wait()
	close(start)

	select {
	case <-firstRequestStarted:
	case <-time.After(2 * time.Second):
		close(releaseFirstRequest)
		t.Fatal("no conformance request reached the backend")
	}
	// While the leader is blocked in its first request, all followers must wait
	// on the same cache fill instead of beginning their own probe suites.
	time.Sleep(150 * time.Millisecond)
	requestsBeforeRelease := len(backend.Requests())
	close(releaseFirstRequest)

	for range callers {
		if err := <-errors; err != nil {
			t.Errorf("concurrent Check() error = %v", err)
		}
		if report := <-reports; report.Verdict != VerdictSafe {
			t.Errorf("concurrent Check() verdict = %s, want SAFE", report.Verdict)
		}
	}
	if requestsBeforeRelease != 1 {
		t.Errorf("requests while the cache leader was blocked = %d, want 1", requestsBeforeRelease)
	}
	if got := len(backend.Requests()); got != 12 {
		t.Fatalf("concurrent same-key checks sent %d requests, want one 12-request suite", got)
	}
}

type probeBehavior struct {
	outputs        map[string][]string
	beforeResponse func()
}

type recordedProbeRequest struct {
	Method               string
	Path                 string
	Model                string            `json:"model"`
	Temperature          *float64          `json:"temperature"`
	Stream               *bool             `json:"stream"`
	N                    *int              `json:"n"`
	ContinueFinalMessage *bool             `json:"continue_final_message"`
	AddGenerationPrompt  *bool             `json:"add_generation_prompt"`
	Messages             []recordedMessage `json:"messages"`
}

type recordedMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type probeBackend struct {
	*httptest.Server

	mu       sync.Mutex
	requests []recordedProbeRequest
	attempts map[string]int
	behavior probeBehavior
}

func newProbeBackend(t *testing.T, behavior probeBehavior) *probeBackend {
	t.Helper()
	backend := &probeBackend{attempts: make(map[string]int), behavior: behavior}
	backend.Server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var recorded recordedProbeRequest
		if err := json.NewDecoder(request.Body).Decode(&recorded); err != nil {
			http.Error(writer, fmt.Sprintf("decode request: %v", err), http.StatusBadRequest)
			return
		}
		recorded.Method = request.Method
		recorded.Path = request.URL.Path
		prefill := ""
		if len(recorded.Messages) > 0 {
			prefill = recorded.Messages[len(recorded.Messages)-1].Content
		}

		backend.mu.Lock()
		attempt := backend.attempts[prefill]
		backend.attempts[prefill] = attempt + 1
		backend.requests = append(backend.requests, recorded)
		backend.mu.Unlock()

		if backend.behavior.beforeResponse != nil {
			backend.behavior.beforeResponse()
		}
		output := successfulProbeOutput(prefill)
		if configured := backend.behavior.outputs[prefill]; attempt < len(configured) {
			output = configured[attempt]
		}
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"id":     "chatcmpl-conformance-fixture",
			"object": "chat.completion",
			"choices": []any{map[string]any{
				"index":         0,
				"message":       map[string]any{"role": "assistant", "content": output},
				"finish_reason": "stop",
			}},
		})
	}))
	return backend
}

func (backend *probeBackend) Requests() []recordedProbeRequest {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	return append([]recordedProbeRequest(nil), backend.requests...)
}

func successfulProbeOutput(prefill string) string {
	switch prefill {
	case continuationPrefill:
		return " 5 6 7 8 9 10"
	case midwordPrefill:
		return "is."
	case punctuationPrefill:
		return " green and blue."
	case idempotencePrefill:
		return " gamma."
	default:
		return " fixture output"
	}
}

func assertFourProbesRunThreeTimes(t *testing.T, report Report) {
	t.Helper()
	if len(report.Probes) != 4 {
		t.Fatalf("probe count = %d, want 4", len(report.Probes))
	}
	for _, probe := range report.Probes {
		if len(probe.Runs) != 3 {
			t.Errorf("probe %q ran %d times, want exactly 3", probe.Name, len(probe.Runs))
		}
		for index, run := range probe.Runs {
			if run.Attempt != index+1 {
				t.Errorf("probe %q run[%d].Attempt = %d, want %d", probe.Name, index, run.Attempt, index+1)
			}
		}
	}
}

func assertCanonicalProbeRequests(t *testing.T, requests []recordedProbeRequest, model string) {
	t.Helper()
	if len(requests) != 12 {
		t.Fatalf("backend received %d requests, want 4 probes x 3 runs = 12", len(requests))
	}
	counts := make(map[string]int)
	for index, request := range requests {
		if request.Method != http.MethodPost || request.Path != "/v1/chat/completions" {
			t.Errorf("request[%d] = %s %s, want POST /v1/chat/completions", index, request.Method, request.Path)
		}
		if request.Model != model {
			t.Errorf("request[%d].model = %q, want %q", index, request.Model, model)
		}
		if request.Temperature == nil || *request.Temperature != 0 {
			t.Errorf("request[%d].temperature = %v, want explicit 0", index, request.Temperature)
		}
		if request.Stream == nil || *request.Stream {
			t.Errorf("request[%d].stream = %v, want explicit false", index, request.Stream)
		}
		if request.N == nil || *request.N != 1 {
			t.Errorf("request[%d].n = %v, want explicit 1", index, request.N)
		}
		if request.ContinueFinalMessage == nil || !*request.ContinueFinalMessage {
			t.Errorf("request[%d].continue_final_message = %v, want true", index, request.ContinueFinalMessage)
		}
		if request.AddGenerationPrompt == nil || *request.AddGenerationPrompt {
			t.Errorf("request[%d].add_generation_prompt = %v, want false", index, request.AddGenerationPrompt)
		}
		if len(request.Messages) != 2 || request.Messages[0].Role != "user" || request.Messages[1].Role != "assistant" {
			t.Errorf("request[%d].messages = %#v, want user then assistant prefill", index, request.Messages)
			continue
		}
		if request.Messages[0].Content == "" || request.Messages[1].Content == "" {
			t.Errorf("request[%d] contains an empty prompt or prefill: %#v", index, request.Messages)
		}
		counts[request.Messages[1].Content]++
	}
	for _, prefill := range []string{continuationPrefill, midwordPrefill, punctuationPrefill, idempotencePrefill} {
		if got := counts[prefill]; got != 3 {
			t.Errorf("assistant prefill %q used %d times, want exactly 3", prefill, got)
		}
	}
	if len(counts) != 4 {
		t.Errorf("distinct assistant prefills = %d, want 4: %#v", len(counts), counts)
	}
}

func withDigest(request CheckRequest, digest string) CheckRequest {
	request.BackendImageDigest = digest
	return request
}

func withModel(request CheckRequest, model string) CheckRequest {
	request.Model = model
	return request
}

func withTokenizer(request CheckRequest, tokenizer string) CheckRequest {
	request.TokenizerHash = tokenizer
	return request
}
