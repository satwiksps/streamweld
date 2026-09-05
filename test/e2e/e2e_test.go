package e2e_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

const (
	e2eStreams = 8
	e2eTokens  = 128
)

func TestBackendRolloutLosesNoDeterministicStreams(t *testing.T) {
	proxyURL := requireE2ECluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	type streamResult struct {
		index      int
		streamID   string
		output     string
		migrations int
		err        error
	}
	firstChunk := make(chan int, e2eStreams)
	results := make(chan streamResult, e2eStreams)
	client := &http.Client{Transport: &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		MaxIdleConns:          e2eStreams * 2,
		IdleConnTimeout:       30 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
	}}
	defer client.CloseIdleConnections()

	for index := 0; index < e2eStreams; index++ {
		go func() {
			streamID, output, migrations, err := consumeDeterministicStream(ctx, client, proxyURL, index, func() {
				firstChunk <- index
			})
			results <- streamResult{index: index, streamID: streamID, output: output, migrations: migrations, err: err}
		}()
	}

	attached := make(map[int]struct{}, e2eStreams)
	for len(attached) < e2eStreams {
		select {
		case index := <-firstChunk:
			attached[index] = struct{}{}
		case result := <-results:
			t.Fatalf("stream %d ended before rollout: %v", result.index, result.err)
		case <-ctx.Done():
			t.Fatalf("streams did not attach before rollout: %v", ctx.Err())
		}
	}

	namespace := envDefault("STREAMWELD_E2E_NAMESPACE", "streamweld-system")
	runKubectl(ctx, t, "rollout", "restart", "deployment/streamweld-sample-backend", "--namespace", namespace)
	runKubectl(ctx, t, "rollout", "status", "deployment/streamweld-sample-backend", "--namespace", namespace, "--timeout=180s")

	want := deterministicOutput(e2eTokens)
	streamIDs := make(map[string]struct{}, e2eStreams)
	for range e2eStreams {
		select {
		case result := <-results:
			if result.err != nil {
				t.Errorf("stream %d failed during rollout: %v", result.index, result.err)
				continue
			}
			if result.output != want {
				offset := 0
				for offset < len(result.output) && offset < len(want) && result.output[offset] == want[offset] {
					offset++
				}
				t.Errorf("stream %d (%s, %d migrations) output is not the canonical %d-token sequence: got %d bytes, want %d; first difference at byte %d: got %q, want %q",
					result.index, result.streamID, result.migrations, e2eTokens, len(result.output), len(want), offset,
					result.output[offset:min(offset+80, len(result.output))], want[offset:min(offset+80, len(want))])
			}
			if result.streamID == "" {
				t.Errorf("stream %d omitted X-Streamweld-Stream-Id", result.index)
			} else if _, duplicate := streamIDs[result.streamID]; duplicate {
				t.Errorf("stream %d reused stream ID %q", result.index, result.streamID)
			} else {
				streamIDs[result.streamID] = struct{}{}
			}
			if result.migrations == 0 {
				t.Errorf("stream %d (%s) completed rollout without a visible migration event", result.index, result.streamID)
			}
		case <-ctx.Done():
			t.Fatalf("streams did not finish after rollout: %v", ctx.Err())
		}
	}
}

func requireE2ECluster(t *testing.T) string {
	t.Helper()
	enabled, present := os.LookupEnv("STREAMWELD_E2E_CLUSTER")
	if !present {
		t.Skip("set STREAMWELD_E2E_CLUSTER=1 to run the kind acceptance profile")
	}
	if enabled != "1" {
		t.Fatalf("STREAMWELD_E2E_CLUSTER must be exactly 1 when present, got %q", enabled)
	}
	proxyURL := strings.TrimRight(os.Getenv("STREAMWELD_E2E_PROXY_URL"), "/")
	if proxyURL == "" {
		t.Fatal("STREAMWELD_E2E_PROXY_URL is required when the cluster profile is enabled")
	}
	return proxyURL
}

func consumeDeterministicStream(
	ctx context.Context,
	client *http.Client,
	proxyURL string,
	index int,
	onFirstChunk func(),
) (string, string, int, error) {
	// The sample backend holds only marked original attempts after token-000.
	// Every reader is attached before rollout; continuation requests finish normally.
	body, err := json.Marshal(map[string]any{
		"model":       "streamweld/deterministic-vllm",
		"messages":    []map[string]string{{"role": "user", "content": fmt.Sprintf("streamweld-e2e:rolling-update:%d", index)}},
		"stream":      true,
		"temperature": 0,
		"max_tokens":  e2eTokens,
	})
	if err != nil {
		return "", "", 0, fmt.Errorf("encode request: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, proxyURL+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return "", "", 0, fmt.Errorf("create request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "text/event-stream")
	request.Header.Set("X-Streamweld-Idempotency-Key", fmt.Sprintf("kind-rollout-%d-%d", time.Now().UnixNano(), index))
	request.Header.Set("X-Streamweld-Verbose", "1")
	response, err := client.Do(request)
	if err != nil {
		return "", "", 0, fmt.Errorf("start request: %w", err)
	}
	defer func() {
		_ = response.Body.Close()
	}()
	streamID := response.Header.Get("X-Streamweld-Stream-Id")
	if response.StatusCode != http.StatusOK {
		message, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return streamID, "", 0, fmt.Errorf("status %s: %s", response.Status, strings.TrimSpace(string(message)))
	}
	if durability := response.Header.Get("X-Streamweld-Durability"); durability != "durable" {
		return streamID, "", 0, fmt.Errorf("durability header = %q, want durable", durability)
	}

	var output strings.Builder
	migrations := 0
	notified := false
	done := false
	err = scanSSE(response.Body, func(eventType, data string) error {
		if terminalErr := terminalStreamEventError(eventType, data); terminalErr != nil {
			return terminalErr
		}
		if eventType == "streamweld.stream.migration" {
			migrations++
			return nil
		}
		if data == "[DONE]" {
			done = true
			return nil
		}
		if data == "" || eventType != "" {
			return nil
		}
		var chunk struct {
			Choices []struct {
				Delta struct {
					Content string `json:"content"`
				} `json:"delta"`
			} `json:"choices"`
		}
		if decodeErr := json.Unmarshal([]byte(data), &chunk); decodeErr != nil || len(chunk.Choices) == 0 {
			return nil
		}
		content := chunk.Choices[0].Delta.Content
		if content == "" {
			return nil
		}
		output.WriteString(content)
		if !notified {
			notified = true
			onFirstChunk()
		}
		return nil
	})
	if err != nil {
		return streamID, output.String(), migrations, err
	}
	if !notified {
		return streamID, output.String(), migrations, errors.New("stream contained no completion chunks")
	}
	if !done {
		return streamID, output.String(), migrations, errors.New("stream ended without the OpenAI [DONE] sentinel")
	}
	return streamID, output.String(), migrations, nil
}

func terminalStreamEventError(eventType, data string) error {
	switch eventType {
	case "streamweld.stream.error", "streamweld.stream.stopped", "streamweld.reader.error":
	default:
		return nil
	}

	var payload struct {
		Code    string `json:"code"`
		Message string `json:"message"`
		Reason  string `json:"reason"`
	}
	detail := strings.TrimSpace(data)
	if json.Unmarshal([]byte(data), &payload) == nil {
		fields := make([]string, 0, 3)
		if payload.Code != "" {
			fields = append(fields, "code="+payload.Code)
		}
		if payload.Reason != "" {
			fields = append(fields, "reason="+payload.Reason)
		}
		if payload.Message != "" {
			fields = append(fields, "message="+payload.Message)
		}
		if len(fields) != 0 {
			detail = strings.Join(fields, ", ")
		}
	}
	if detail == "" {
		detail = "no details"
	}
	return fmt.Errorf("received terminal SSE event %q (%s)", eventType, detail)
}

func TestTerminalStreamEventError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		eventType string
		data      string
		want      string
	}{
		{
			name:      "stream error",
			eventType: "streamweld.stream.error",
			data:      `{"code":"migration_refused","message":"continuation is not safe","reason":"target_backend_unavailable"}`,
			want:      "code=migration_refused, reason=target_backend_unavailable, message=continuation is not safe",
		},
		{
			name:      "reader error",
			eventType: "streamweld.reader.error",
			data:      `{"code":"reader_lag_exceeded"}`,
			want:      "code=reader_lag_exceeded",
		},
		{
			name:      "malformed terminal payload",
			eventType: "streamweld.stream.stopped",
			data:      "not-json",
			want:      "not-json",
		},
		{name: "migration is not terminal", eventType: "streamweld.stream.migration"},
		{name: "done control event is not an error", eventType: "streamweld.stream.done"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := terminalStreamEventError(test.eventType, test.data)
			if test.want == "" {
				if err != nil {
					t.Fatalf("terminalStreamEventError() = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("terminalStreamEventError() = %v, want error containing %q", err, test.want)
			}
		})
	}
}

func scanSSE(reader io.Reader, yield func(eventType, data string) error) error {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 4096), 2<<20)
	eventType := ""
	data := make([]string, 0, 1)
	flush := func() error {
		if len(data) == 0 && eventType == "" {
			return nil
		}
		err := yield(eventType, strings.Join(data, "\n"))
		eventType = ""
		data = data[:0]
		return err
	}
	for scanner.Scan() {
		line := strings.TrimSuffix(scanner.Text(), "\r")
		if line == "" {
			if err := flush(); err != nil {
				return err
			}
			continue
		}
		if strings.HasPrefix(line, "event:") {
			eventType = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		}
		if strings.HasPrefix(line, "data:") {
			data = append(data, strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("read SSE: %w", err)
	}
	if err := flush(); err != nil {
		return err
	}
	return nil
}

func deterministicOutput(tokens int) string {
	var output strings.Builder
	for index := 0; index < tokens; index++ {
		fmt.Fprintf(&output, "token-%03d ", index)
	}
	return output.String()
}

func runKubectl(ctx context.Context, t *testing.T, args ...string) {
	t.Helper()
	command := exec.CommandContext(ctx, "kubectl", args...)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("kubectl %s: %v\n%s", strings.Join(args, " "), err, output)
	}
}

func envDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
