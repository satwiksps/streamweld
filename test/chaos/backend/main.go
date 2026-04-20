// Command streamweld-chaos-backend is a deterministic OpenAI-compatible
// backend used by the kind chaos profile. It requires no model or GPU.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	modelName        = "streamweld/deterministic-chaos"
	promptTokenCount = 16
	maxRequestBytes  = 1 << 20
)

var tokenPattern = regexp.MustCompile(`token-(\d+) `)

type config struct {
	listen        string
	tokenDelay    time.Duration
	defaultTokens int
	unsafe        bool
}

type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type completionRequest struct {
	Model               string    `json:"model"`
	Messages            []message `json:"messages"`
	Stream              bool      `json:"stream"`
	MaxTokens           int       `json:"max_tokens"`
	MaxCompletionTokens int       `json:"max_completion_tokens"`
}

func main() {
	os.Exit(run())
}

func run() int {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	settings, err := configFromEnv()
	if err != nil {
		logger.Error("invalid configuration", "error", err)
		return 2
	}
	server := &http.Server{
		Addr:              settings.listen,
		Handler:           newHandler(settings, logger),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	result := make(chan error, 1)
	go func() {
		result <- server.ListenAndServe()
	}()
	select {
	case err := <-result:
		if !errors.Is(err, http.ErrServerClosed) {
			logger.Error("serve failed", "error", err)
			return 1
		}
	case <-ctx.Done():
		shutdownContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownContext); err != nil {
			logger.Error("shutdown failed", "error", err)
			return 1
		}
	}
	return 0
}

func configFromEnv() (config, error) {
	settings := config{listen: ":8000", tokenDelay: 8 * time.Millisecond, defaultTokens: 64}
	if value := os.Getenv("CHAOS_LISTEN"); value != "" {
		settings.listen = value
	}
	if value := os.Getenv("CHAOS_TOKEN_DELAY"); value != "" {
		parsed, err := time.ParseDuration(value)
		if err != nil || parsed < 0 {
			return config{}, errors.New("CHAOS_TOKEN_DELAY must be a non-negative duration")
		}
		settings.tokenDelay = parsed
	}
	if value := os.Getenv("CHAOS_DEFAULT_TOKENS"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 8 || parsed > 100_000 {
			return config{}, errors.New("CHAOS_DEFAULT_TOKENS must be in 8..100000")
		}
		settings.defaultTokens = parsed
	}
	switch os.Getenv("CHAOS_TEMPLATE_MODE") {
	case "", "safe":
	case "unsafe":
		settings.unsafe = true
	default:
		return config{}, errors.New("CHAOS_TEMPLATE_MODE must be safe or unsafe")
	}
	return settings, nil
}

func newHandler(settings config, logger *slog.Logger) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(writer http.ResponseWriter, _ *http.Request) {
		writeJSON(writer, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /v1/models", func(writer http.ResponseWriter, _ *http.Request) {
		writeJSON(writer, http.StatusOK, map[string]any{
			"object": "list",
			"data":   []map[string]string{{"id": modelName, "object": "model"}},
		})
	})
	mux.HandleFunc("POST /v1/chat/completions", func(writer http.ResponseWriter, request *http.Request) {
		handleCompletion(writer, request, settings, logger)
	})
	return mux
}

func handleCompletion(writer http.ResponseWriter, request *http.Request, settings config, logger *slog.Logger) {
	decoder := json.NewDecoder(io.LimitReader(request.Body, maxRequestBytes+1))
	var completion completionRequest
	if err := decoder.Decode(&completion); err != nil {
		writeJSON(writer, http.StatusBadRequest, errorBody("invalid JSON request"))
		return
	}
	if completion.Model != modelName {
		writeJSON(writer, http.StatusBadRequest, errorBody("unsupported deterministic model"))
		return
	}
	limit := completion.MaxCompletionTokens
	if limit == 0 {
		limit = completion.MaxTokens
	}
	if limit == 0 {
		limit = settings.defaultTokens
	}
	if limit < 1 || limit > 100_000 {
		writeJSON(writer, http.StatusBadRequest, errorBody("token limit must be in 1..100000"))
		return
	}
	if !completion.Stream {
		content := conformanceOutput(completion.Messages, settings.unsafe)
		if content == "" {
			content = deterministicRange(continuationStart(completion.Messages), continuationStart(completion.Messages)+limit)
		}
		writeJSON(writer, http.StatusOK, map[string]any{
			"id": "chatcmpl-chaos", "object": "chat.completion", "model": modelName,
			"choices": []map[string]any{{"index": 0, "message": map[string]string{"role": "assistant", "content": content}, "finish_reason": "stop"}},
		})
		return
	}

	start := continuationStart(completion.Messages)
	generationStart := start
	generationLimit := limit
	if start > 0 {
		// Repeat exactly one deterministic token on continuation attempts. The
		// proxy must remove it at the seam. The raw delta is retained as
		// fake-backend-only observation metadata for the kind harness.
		generationStart--
		generationLimit++
	}
	scenario := scenarioFromMessages(completion.Messages)
	tokenDelay := settings.tokenDelay
	if scenario == "slow-consumer" {
		tokenDelay = 0
	}
	failAt := -1
	if start == 0 && (scenario == "backend-oom" || scenario == "unsafe-template") {
		failAt = max(2, min(limit-1, settings.defaultTokens/3))
	}
	writer.Header().Set("Content-Type", "text/event-stream")
	writer.Header().Set("Cache-Control", "no-cache")
	writer.Header().Set("Connection", "close")
	writer.WriteHeader(http.StatusOK)
	flush(writer)
	for offset := 0; offset < generationLimit; offset++ {
		if failAt >= 0 && offset == failAt {
			if scenario == "backend-oom" {
				writeSSE(writer, map[string]any{"error": map[string]string{"code": "backend_oom", "message": "deterministic injected backend OOM"}})
			}
			return
		}
		content := fmt.Sprintf("token-%03d ", generationStart+offset)
		chunk := map[string]any{
			"id": "chatcmpl-chaos", "object": "chat.completion.chunk", "model": modelName,
			"choices": []map[string]any{{"index": 0, "delta": map[string]string{"content": content}, "finish_reason": nil}},
			"usage": map[string]int{
				"prompt_tokens": promptTokenCount, "completion_tokens": offset + 1,
				"total_tokens": promptTokenCount + offset + 1,
			},
		}
		if start > 0 {
			chunk["streamweld_chaos_raw_delta"] = content
		}
		writeSSE(writer, chunk)
		if tokenDelay > 0 {
			select {
			case <-time.After(tokenDelay):
			case <-request.Context().Done():
				return
			}
		}
	}
	writeSSE(writer, map[string]any{
		"id": "chatcmpl-chaos", "object": "chat.completion.chunk", "model": modelName,
		"choices": []map[string]any{{"index": 0, "delta": map[string]string{}, "finish_reason": "stop"}},
	})
	_, _ = io.WriteString(writer, "data: [DONE]\n\n")
	flush(writer)
	logger.DebugContext(request.Context(), "deterministic generation completed", "scenario", scenario, "start", start, "tokens", limit)
}

func continuationStart(messages []message) int {
	if len(messages) == 0 || messages[len(messages)-1].Role != "assistant" {
		return 0
	}
	matches := tokenPattern.FindAllStringSubmatch(messages[len(messages)-1].Content, -1)
	maximum := -1
	for _, match := range matches {
		value, err := strconv.Atoi(match[1])
		if err == nil && value > maximum {
			maximum = value
		}
	}
	return maximum + 1
}

func scenarioFromMessages(messages []message) string {
	for _, item := range messages {
		if item.Role != "user" {
			continue
		}
		const prefix = "streamweld-chaos:"
		if strings.HasPrefix(item.Content, prefix) {
			value := strings.TrimPrefix(item.Content, prefix)
			if separator := strings.IndexByte(value, ':'); separator >= 0 {
				value = value[:separator]
			}
			return value
		}
	}
	return "baseline"
}

func conformanceOutput(messages []message, unsafe bool) string {
	if len(messages) == 0 || messages[len(messages)-1].Role != "assistant" {
		return ""
	}
	prefill := messages[len(messages)-1].Content
	if unsafe {
		switch prefill {
		case "1 2 3 4":
			return "1 2 3 4"
		case "The capital of France is Par":
			return ""
		}
	}
	outputs := map[string]string{
		"1 2 3 4":                                    "5 6 7 8 9 10",
		"The capital of France is Par":               "is.",
		"The primary colors are red,":                " blue, and yellow.",
		"The deterministic sequence is alpha, beta,": " gamma, delta.",
	}
	return outputs[prefill]
}

func deterministicRange(start, end int) string {
	var output strings.Builder
	for token := start; token < end; token++ {
		_, _ = fmt.Fprintf(&output, "token-%03d ", token)
	}
	return output.String()
}

func writeSSE(writer http.ResponseWriter, payload any) {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return
	}
	_, _ = writer.Write([]byte("data: "))
	_, _ = writer.Write(encoded)
	_, _ = writer.Write([]byte("\n\n"))
	flush(writer)
}

func flush(writer http.ResponseWriter) {
	if flusher, ok := writer.(http.Flusher); ok {
		flusher.Flush()
	}
}

func writeJSON(writer http.ResponseWriter, status int, payload any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(payload)
}

func errorBody(message string) map[string]any {
	return map[string]any{"error": map[string]string{"message": message, "type": "invalid_request_error"}}
}
