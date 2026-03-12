package proxy

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/streamweld/streamweld/internal/proxy/sse"
)

func TestPassthroughTTFTOverheadBudget(t *testing.T) {
	const (
		warmupSamples = 40
		measurements  = 300
		targetBudget  = 5 * time.Millisecond
	)
	budget := targetBudget
	if runtime.GOOS == "windows" {
		// The Windows loopback scheduler regularly contributes a 10-16ms tail
		// even when the proxy work remains below the cross-platform target. CI
		// enforces the normative 5ms gate on Linux; keep the local gate useful
		// without making it a timer-quantum lottery.
		budget = 20 * time.Millisecond
	}

	releases := releaseRegistry{}
	backend := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/health" {
			writer.WriteHeader(http.StatusOK)
			return
		}
		release, ok := releases.Load(request.Header.Get("X-Streamweld-Timing-Id"))
		if !ok {
			http.Error(writer, "missing timing registration", http.StatusBadRequest)
			return
		}
		writer.Header().Set("Content-Type", "text/event-stream")
		writer.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(writer, "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"d\"}}]}\n\n")
		writer.(http.Flusher).Flush()
		select {
		case <-release:
		case <-request.Context().Done():
		case <-time.After(2 * time.Second):
		}
		_, _ = io.WriteString(writer, "data: [DONE]\n\n")
		writer.(http.Flusher).Flush()
	}))
	t.Cleanup(backend.Close)

	cfg := DefaultConfig()
	cfg.BackendURL = backend.URL
	cfg.ListenAddress = "127.0.0.1:0"
	server, err := NewServer(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("NewServer(): %v", err)
	}
	t.Cleanup(server.closeIdleConnections)
	frontend := httptest.NewServer(server.Handler())
	t.Cleanup(frontend.Close)

	transport := &http.Transport{
		Proxy:               nil,
		MaxIdleConns:        8,
		MaxIdleConnsPerHost: 4,
		IdleConnTimeout:     time.Second,
	}
	t.Cleanup(transport.CloseIdleConnections)
	client := &http.Client{Transport: transport, Timeout: 3 * time.Second}

	measure := func(baseURL, id string) time.Duration {
		release := releases.Store(id)
		defer releases.Delete(id)
		request, requestErr := http.NewRequestWithContext(context.Background(), http.MethodPost, baseURL+"/v1/chat/completions", strings.NewReader(`{"stream":true}`))
		if requestErr != nil {
			t.Fatalf("create timing request: %v", requestErr)
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("X-Streamweld-Timing-Id", id)

		started := time.Now()
		response, requestErr := client.Do(request)
		if requestErr != nil {
			close(release)
			t.Fatalf("timing request: %v", requestErr)
		}
		decoder := sse.NewDecoder(response.Body)
		var first sse.Event
		var readErr error
		for {
			first, readErr = decoder.Decode()
			if readErr != nil || first.Type != streamOpenEvent {
				break
			}
		}
		elapsed := time.Since(started)
		close(release)
		_, drainErr := io.Copy(io.Discard, response.Body)
		closeErr := response.Body.Close()
		if readErr != nil {
			t.Fatalf("read first response byte for %s: %v", id, readErr)
		}
		if drainErr != nil {
			t.Fatalf("drain timing response: %v", drainErr)
		}
		if closeErr != nil {
			t.Fatalf("close timing response: %v", closeErr)
		}
		if !strings.Contains(string(first.Data), `"content":"d"`) {
			t.Fatalf("first token event = %q, want content d", first.Data)
		}
		return elapsed
	}

	sequence := 0
	for range warmupSamples {
		sequence++
		_ = measure(backend.URL, fmt.Sprintf("warm-direct-%d", sequence))
		sequence++
		_ = measure(frontend.URL, fmt.Sprintf("warm-proxy-%d", sequence))
	}

	directSamples := make([]time.Duration, 0, measurements)
	proxySamples := make([]time.Duration, 0, measurements)
	for index := range measurements {
		// Alternate order so gradual host-load or scheduler drift affects both
		// distributions evenly.
		if index%2 == 0 {
			sequence++
			directSamples = append(directSamples, measure(backend.URL, fmt.Sprintf("direct-%d", sequence)))
			sequence++
			proxySamples = append(proxySamples, measure(frontend.URL, fmt.Sprintf("proxy-%d", sequence)))
		} else {
			sequence++
			proxySamples = append(proxySamples, measure(frontend.URL, fmt.Sprintf("proxy-%d", sequence)))
			sequence++
			directSamples = append(directSamples, measure(backend.URL, fmt.Sprintf("direct-%d", sequence)))
		}
	}

	directP99 := percentile99(directSamples)
	proxyP99 := percentile99(proxySamples)
	overheadP99 := max(time.Duration(0), proxyP99-directP99)
	t.Logf("loopback TTFT p99: direct=%s proxy=%s added=%s across %d requests per path", directP99, proxyP99, overheadP99, measurements)
	if overheadP99 >= budget {
		t.Fatalf("passthrough TTFT p99 overhead %s exceeds %s budget", overheadP99, budget)
	}
}

func percentile99(samples []time.Duration) time.Duration {
	sorted := append([]time.Duration(nil), samples...)
	sort.Slice(sorted, func(left, right int) bool { return sorted[left] < sorted[right] })
	return sorted[(len(sorted)*99+99)/100-1]
}

type releaseRegistry struct {
	values sync.Map
}

func (registry *releaseRegistry) Store(id string) chan struct{} {
	release := make(chan struct{})
	registry.values.Store(id, release)
	return release
}

func (registry *releaseRegistry) Load(id string) (<-chan struct{}, bool) {
	value, ok := registry.values.Load(id)
	if !ok {
		return nil, false
	}
	release, ok := value.(chan struct{})
	return release, ok
}

func (registry *releaseRegistry) Delete(id string) {
	registry.values.Delete(id)
}
