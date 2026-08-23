package chaos

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"time"

	"github.com/satwiksps/streamweld/internal/proxy"
	"github.com/satwiksps/streamweld/internal/proxy/sse"
)

const (
	ttftProbeTimeout = 20 * time.Second
	// The explicit delay keeps an in-memory handler from measuring as a
	// misleading zero on coarse host clocks. Both compared paths include it.
	ttftBackendDelay = 2 * time.Millisecond
)

func measureLocalTTFT(ctx context.Context, concurrent int) (TTFTMeasurement, error) {
	backend := httptest.NewServer(http.HandlerFunc(serveTTFTBackend))
	defer backend.Close()

	config := proxy.DefaultConfig()
	config.BackendURL = backend.URL
	config.ListenAddress = "127.0.0.1:0"
	config.ShutdownTimeout = 2 * time.Second
	server, err := proxy.NewServer(config, nil)
	if err != nil {
		return TTFTMeasurement{}, err
	}
	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		return TTFTMeasurement{}, fmt.Errorf("listen for TTFT proxy: %w", err)
	}
	serveContext, cancelServe := context.WithCancel(context.Background())
	serveResult := make(chan error, 1)
	go func() {
		serveResult <- server.Serve(serveContext, listener)
	}()
	defer func() {
		cancelServe()
		select {
		case <-serveResult:
		case <-time.After(3 * time.Second):
			_ = listener.Close()
		}
	}()

	transport := &http.Transport{
		DisableCompression:  true,
		MaxIdleConns:        concurrent * 4,
		MaxIdleConnsPerHost: concurrent * 2,
	}
	client := &http.Client{Transport: transport, Timeout: ttftProbeTimeout}
	defer transport.CloseIdleConnections()

	proxyURL := "http://" + listener.Addr().String()
	if _, err := measureOneTTFT(ctx, client, backend.URL, -1); err != nil {
		return TTFTMeasurement{}, fmt.Errorf("warm direct path: %w", err)
	}
	if _, err := measureOneTTFT(ctx, client, proxyURL, -1); err != nil {
		return TTFTMeasurement{}, fmt.Errorf("warm streamweld path: %w", err)
	}
	direct, err := measureTTFTBatch(ctx, client, backend.URL, concurrent)
	if err != nil {
		return TTFTMeasurement{}, fmt.Errorf("direct batch: %w", err)
	}
	streamweld, err := measureTTFTBatch(ctx, client, proxyURL, concurrent)
	if err != nil {
		return TTFTMeasurement{}, fmt.Errorf("streamweld batch: %w", err)
	}
	return TTFTMeasurement{
		DirectMilliseconds:     durationP50(direct).Seconds() * 1000,
		StreamweldMilliseconds: durationP50(streamweld).Seconds() * 1000,
	}, nil
}

func measureTTFTBatch(ctx context.Context, client *http.Client, baseURL string, concurrent int) ([]time.Duration, error) {
	start := make(chan struct{})
	results := make(chan struct {
		duration time.Duration
		err      error
	}, concurrent)
	var workers sync.WaitGroup
	workers.Add(concurrent)
	for index := range concurrent {
		go func() {
			defer workers.Done()
			select {
			case <-start:
			case <-ctx.Done():
				return
			}
			duration, err := measureOneTTFT(ctx, client, baseURL, index)
			results <- struct {
				duration time.Duration
				err      error
			}{duration: duration, err: err}
		}()
	}
	close(start)
	go func() {
		workers.Wait()
		close(results)
	}()

	samples := make([]time.Duration, 0, concurrent)
	for {
		select {
		case result, ok := <-results:
			if !ok {
				if len(samples) != concurrent {
					return nil, fmt.Errorf("received %d TTFT samples, want %d", len(samples), concurrent)
				}
				return samples, nil
			}
			if result.err != nil {
				return nil, result.err
			}
			samples = append(samples, result.duration)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func measureOneTTFT(ctx context.Context, client *http.Client, baseURL string, index int) (time.Duration, error) {
	body, err := json.Marshal(map[string]any{
		"model":       "streamweld/deterministic-chaos",
		"messages":    []map[string]string{{"role": "user", "content": fmt.Sprintf("ttft-%d", index)}},
		"stream":      true,
		"temperature": 0,
		"max_tokens":  1,
	})
	if err != nil {
		return 0, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "text/event-stream")
	request.Header.Set("X-Streamweld-Idempotency-Key", fmt.Sprintf("ttft-%d-%d", index, time.Now().UnixNano()))
	started := time.Now()
	response, err := client.Do(request)
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = response.Body.Close()
	}()
	if response.StatusCode != http.StatusOK {
		message, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return 0, fmt.Errorf("status %s: %s", response.Status, bytes.TrimSpace(message))
	}
	decoder := sse.NewDecoder(response.Body)
	var first time.Duration
	sawToken := false
	for {
		event, decodeErr := decoder.Decode()
		if errors.Is(decodeErr, io.EOF) {
			break
		}
		if decodeErr != nil {
			return 0, decodeErr
		}
		if !sawToken && completionText(event.Data) != "" {
			first = time.Since(started)
			sawToken = true
		}
		if bytes.Equal(event.Data, []byte("[DONE]")) {
			break
		}
	}
	if !sawToken {
		return 0, errors.New("stream ended before a completion token")
	}
	return first, nil
}

func completionText(data []byte) string {
	var chunk struct {
		Choices []struct {
			Delta struct {
				Content string `json:"content"`
			} `json:"delta"`
		} `json:"choices"`
	}
	if json.Unmarshal(data, &chunk) != nil || len(chunk.Choices) == 0 {
		return ""
	}
	return chunk.Choices[0].Delta.Content
}

func durationP50(samples []time.Duration) time.Duration {
	ordered := append([]time.Duration(nil), samples...)
	sort.Slice(ordered, func(left, right int) bool { return ordered[left] < ordered[right] })
	return ordered[(len(ordered)-1)/2]
}

func serveTTFTBackend(writer http.ResponseWriter, request *http.Request) {
	if request.URL.Path == "/health" {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(writer, `{"status":"ok"}`)
		return
	}
	if request.Method != http.MethodPost || request.URL.Path != "/v1/chat/completions" {
		http.NotFound(writer, request)
		return
	}
	_, _ = io.Copy(io.Discard, request.Body)
	timer := time.NewTimer(ttftBackendDelay)
	select {
	case <-timer.C:
	case <-request.Context().Done():
		if !timer.Stop() {
			<-timer.C
		}
		return
	}
	writer.Header().Set("Content-Type", "text/event-stream")
	writer.Header().Set("Cache-Control", "no-cache")
	writer.WriteHeader(http.StatusOK)
	writeTTFTEvent(writer, map[string]any{
		"id": "chatcmpl-ttft", "object": "chat.completion.chunk",
		"choices": []map[string]any{{"index": 0, "delta": map[string]string{"content": "token-000 "}, "finish_reason": nil}},
		"usage":   map[string]int{"prompt_tokens": DeterministicPromptTokens, "completion_tokens": 1, "total_tokens": DeterministicPromptTokens + 1},
	})
	writeTTFTEvent(writer, map[string]any{
		"id": "chatcmpl-ttft", "object": "chat.completion.chunk",
		"choices": []map[string]any{{"index": 0, "delta": map[string]string{}, "finish_reason": "stop"}},
	})
	_, _ = io.WriteString(writer, "data: [DONE]\n\n")
	if flusher, ok := writer.(http.Flusher); ok {
		flusher.Flush()
	}
}

func writeTTFTEvent(writer http.ResponseWriter, payload any) {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return
	}
	_, _ = writer.Write([]byte("data: "))
	_, _ = writer.Write(encoded)
	_, _ = writer.Write([]byte("\n\n"))
	if flusher, ok := writer.(http.Flusher); ok {
		flusher.Flush()
	}
}
