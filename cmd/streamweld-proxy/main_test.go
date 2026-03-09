package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunRejectsMissingBackendWithJSONLog(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	code := run(nil, mapEnvironment(nil), &stdout, &stderr)
	if code != 2 {
		t.Fatalf("run() code = %d, want 2", code)
	}
	var record map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(stderr.Bytes()), &record); err != nil {
		t.Fatalf("startup error is not JSON: %q: %v", stderr.Bytes(), err)
	}
	if record["msg"] != "proxy configuration rejected" {
		t.Errorf("message = %v", record["msg"])
	}
	if stdout.Len() != 0 {
		t.Errorf("unexpected stdout: %q", stdout.Bytes())
	}
}

func TestRunRejectsMalformedEnvironment(t *testing.T) {
	t.Parallel()
	var stderr bytes.Buffer
	code := run(nil, mapEnvironment(map[string]string{
		"STREAMWELD_DIAL_TIMEOUT": "eventually",
	}), &bytes.Buffer{}, &stderr)
	if code != 2 {
		t.Fatalf("run() code = %d, want 2", code)
	}
	if !strings.Contains(stderr.String(), `"msg":"invalid environment configuration"`) {
		t.Errorf("stderr = %q", stderr.String())
	}
}

func TestRunHelp(t *testing.T) {
	t.Parallel()
	var stderr bytes.Buffer
	code := run([]string{"--help"}, mapEnvironment(nil), &bytes.Buffer{}, &stderr)
	if code != 0 {
		t.Fatalf("run(--help) code = %d, want 0", code)
	}
	if !strings.Contains(stderr.String(), "Usage: streamweld-proxy --backend URL") {
		t.Errorf("help output = %q", stderr.String())
	}
	for _, flagName := range []string{
		"-journal-backend", "-redis-url", "-redis-key-prefix", "-replica-id",
		"-relay-listen", "-relay-advertise-url", "-relay-ca-file",
		"-relay-cert-file", "-relay-key-file", "-relay-insecure-dev-mode",
		"-reader-write-timeout",
	} {
		if !strings.Contains(stderr.String(), flagName) {
			t.Errorf("help output does not contain %q: %q", flagName, stderr.String())
		}
	}
}

func TestRunHelpDoesNotExposeConfiguredCredentials(t *testing.T) {
	t.Parallel()
	const secret = "very-private-password"
	var stderr bytes.Buffer
	code := run([]string{"--help"}, mapEnvironment(map[string]string{
		"STREAMWELD_BACKEND":             "http://user:" + secret + "@backend.example.test",
		"STREAMWELD_REDIS_URL":           "redis://user:" + secret + "@redis.example.test:6379/0",
		"STREAMWELD_RELAY_ADVERTISE_URL": "https://user:" + secret + "@relay.example.test:8443",
	}), &bytes.Buffer{}, &stderr)
	if code != 0 {
		t.Fatalf("run(--help) code = %d, want 0", code)
	}
	if strings.Contains(stderr.String(), secret) {
		t.Fatalf("help output exposed configured credentials: %q", stderr.String())
	}
}

func TestParseLogLevel(t *testing.T) {
	t.Parallel()
	for text, want := range map[string]slog.Level{
		"debug": slog.LevelDebug,
		"info":  slog.LevelInfo,
		"warn":  slog.LevelWarn,
		"error": slog.LevelError,
	} {
		got, err := parseLogLevel(text)
		if err != nil || got != want {
			t.Errorf("parseLogLevel(%q) = (%s, %v), want (%s, nil)", text, got, err, want)
		}
	}
	if _, err := parseLogLevel("verbose"); err == nil {
		t.Error("parseLogLevel accepted invalid level")
	}
}

func TestShutdownContextRestoresSignalHandlingBeforeCancellation(t *testing.T) {
	t.Parallel()
	var subscription chan<- os.Signal
	var stopped atomic.Bool
	var stopCalls atomic.Int32
	ctx, stop := newShutdownContext(
		context.Background(),
		func(events chan<- os.Signal) { subscription = events },
		func(chan<- os.Signal) {
			stopped.Store(true)
			stopCalls.Add(1)
		},
	)
	t.Cleanup(stop)

	subscription <- os.Interrupt
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("shutdown context was not canceled by the first signal")
	}
	if !stopped.Load() {
		t.Fatal("signal handling was not restored before shutdown began")
	}
	stop()
	if got := stopCalls.Load(); got != 1 {
		t.Errorf("stop notifications called %d times, want once", got)
	}
}

func mapEnvironment(values map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) {
		value, ok := values[name]
		return value, ok
	}
}
