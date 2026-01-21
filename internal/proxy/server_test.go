package proxy

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestNewServerRejectsInvalidOptions(t *testing.T) {
	t.Parallel()
	config := DefaultConfig()
	config.BackendURL = "http://backend.example.test"
	config.ListenAddress = "127.0.0.1:0"
	if _, err := NewServer(config, nil, WithTransport(nil)); err == nil {
		t.Fatal("NewServer accepted a nil transport")
	}
	if _, err := NewServer(config, nil, WithReadinessChecker(nil)); err == nil {
		t.Fatal("NewServer accepted a nil readiness checker")
	}
	if _, err := NewServer(config, nil, nil); err == nil {
		t.Fatal("NewServer accepted a nil option")
	}
}

func TestServeGracefullyWaitsForActiveRequest(t *testing.T) {
	t.Parallel()
	transport := newBlockingRoundTripper()
	config := DefaultConfig()
	config.BackendURL = "http://backend.example.test"
	config.ListenAddress = "127.0.0.1:0"
	config.ShutdownTimeout = 2 * time.Second
	server, err := NewServer(config, nil, WithTransport(transport))
	if err != nil {
		t.Fatal(err)
	}
	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(context.Background(), "tcp", config.ListenAddress)
	if err != nil {
		t.Fatal(err)
	}
	serveContext, cancelServe := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancelServe()
		transport.Release()
		_ = server.httpServer.Close()
	})
	serveResult := make(chan error, 1)
	go func() { serveResult <- server.Serve(serveContext, listener) }()

	requestResult := make(chan struct {
		status int
		body   string
		err    error
	}, 1)
	go func() {
		client := &http.Client{Timeout: 3 * time.Second}
		request, requestErr := http.NewRequestWithContext(context.Background(), http.MethodPost, "http://"+listener.Addr().String()+"/v1/completions", strings.NewReader(`{"stream":false}`))
		if requestErr == nil {
			request.Header.Set("Content-Type", "application/json")
		}
		var response *http.Response
		if requestErr == nil {
			response, requestErr = client.Do(request)
		}
		if requestErr != nil {
			requestResult <- struct {
				status int
				body   string
				err    error
			}{err: requestErr}
			return
		}
		body, readErr := io.ReadAll(response.Body)
		_ = response.Body.Close()
		requestResult <- struct {
			status int
			body   string
			err    error
		}{response.StatusCode, string(body), readErr}
	}()

	select {
	case <-transport.started:
	case <-time.After(2 * time.Second):
		t.Fatal("active request did not reach backend")
	}
	cancelServe()
	select {
	case err := <-serveResult:
		t.Fatalf("Serve returned before the active request completed: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	transport.Release()

	select {
	case result := <-requestResult:
		if result.err != nil || result.status != http.StatusOK || result.body != "finished" {
			t.Errorf("active request result = (%d, %q, %v)", result.status, result.body, result.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("active request did not finish")
	}
	select {
	case err := <-serveResult:
		if err != nil {
			t.Errorf("Serve() error after graceful shutdown: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not finish after active request completed")
	}
}

func TestServeForcesCloseAfterShutdownDeadline(t *testing.T) {
	t.Parallel()
	transport := newBlockingRoundTripper()
	config := DefaultConfig()
	config.BackendURL = "http://backend.example.test"
	config.ListenAddress = "127.0.0.1:0"
	config.ShutdownTimeout = 100 * time.Millisecond
	server, err := NewServer(config, nil, WithTransport(transport))
	if err != nil {
		t.Fatal(err)
	}
	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(context.Background(), "tcp", config.ListenAddress)
	if err != nil {
		t.Fatal(err)
	}
	serveContext, cancelServe := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancelServe()
		transport.Release()
		_ = server.httpServer.Close()
	})
	serveResult := make(chan error, 1)
	go func() { serveResult <- server.Serve(serveContext, listener) }()
	clientResult := make(chan error, 1)
	go func() {
		client := &http.Client{Timeout: 3 * time.Second}
		request, requestErr := http.NewRequestWithContext(context.Background(), http.MethodPost, "http://"+listener.Addr().String()+"/v1/chat/completions", strings.NewReader(`{"stream":true}`))
		if requestErr == nil {
			request.Header.Set("Content-Type", "application/json")
		}
		var response *http.Response
		if requestErr == nil {
			response, requestErr = client.Do(request)
		}
		if response != nil {
			_ = response.Body.Close()
		}
		clientResult <- requestErr
	}()
	select {
	case <-transport.started:
	case <-time.After(2 * time.Second):
		t.Fatal("active request did not reach backend")
	}
	cancelServe()
	select {
	case err := <-serveResult:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Serve() error = %v, want shutdown deadline exceeded", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not force-close after its shutdown timeout")
	}
	select {
	case <-transport.canceled:
	case <-time.After(2 * time.Second):
		t.Fatal("forced proxy shutdown did not cancel the upstream request")
	}
	select {
	case <-clientResult:
	case <-time.After(2 * time.Second):
		t.Fatal("client request remained blocked after forced shutdown")
	}
}

func TestServeRejectsNilInputs(t *testing.T) {
	t.Parallel()
	server := newTestProxy(t, "http://backend.example.test")
	if err := server.Serve(nil, nil); err == nil { //nolint:staticcheck // Intentionally verify the public nil-context contract.
		t.Error("Serve(nil, nil) returned nil")
	}
	if err := server.Serve(context.Background(), nil); err == nil {
		t.Error("Serve(context, nil) returned nil")
	}
	if err := server.Shutdown(nil); err == nil { //nolint:staticcheck // Intentionally verify the public nil-context contract.
		t.Error("Shutdown(nil) returned nil")
	}
}
