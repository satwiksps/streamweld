package main

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/satwiksps/streamweld/internal/version"
)

func TestVersionDoesNotReadEnvironment(t *testing.T) {
	t.Parallel()
	for _, argument := range []string{"--version", "-version"} {
		t.Run(argument, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			lookup := func(key string) (string, bool) {
				t.Fatalf("version request unexpectedly read environment variable %s", key)
				return "", false
			}
			if code := run([]string{argument}, lookup, &stdout, &stderr); code != 0 {
				t.Fatalf("version exit = %d, stderr = %q", code, stderr.String())
			}
			for _, field := range []string{"streamweld-proxy " + version.Version, "commit " + version.Commit, "date " + version.Date} {
				if !strings.Contains(stdout.String(), field) {
					t.Errorf("version output %q does not contain %q", stdout.String(), field)
				}
			}
			if stderr.Len() != 0 || strings.Count(stdout.String(), "\n") != 1 {
				t.Fatalf("stdout/stderr = %q/%q, want one version line only", stdout.String(), stderr.String())
			}
		})
	}
}

func TestHelpIgnoresMalformedEnvironmentAndUsesBuiltinDefaults(t *testing.T) {
	t.Parallel()
	for _, argument := range []string{"--help", "-help", "-h", "--h"} {
		t.Run(argument, func(t *testing.T) {
			const secret = "private-environment-value"
			var stdout, stderr bytes.Buffer
			code := run([]string{argument}, mapEnvironment(map[string]string{
				"STREAMWELD_DIAL_TIMEOUT": "eventually",
				"STREAMWELD_LOG_LEVEL":    secret,
				"STREAMWELD_LISTEN":       secret,
				"STREAMWELD_BACKEND":      "http://user:" + secret + "@backend.example.test",
				"STREAMWELD_REDIS_URL":    "redis://user:" + secret + "@redis.example.test:6379/0",
			}), &stdout, &stderr)
			if code != 0 || stdout.Len() != 0 {
				t.Fatalf("help exit/stdout/stderr = %d/%q/%q", code, stdout.String(), stderr.String())
			}
			if !strings.Contains(stderr.String(), "Usage: streamweld-proxy --backend URL") ||
				!strings.Contains(stderr.String(), "-version") || !strings.Contains(stderr.String(), `default ":8080"`) {
				t.Fatalf("help did not show builtin usage and defaults: %q", stderr.String())
			}
			if strings.Contains(stderr.String(), secret) || strings.Contains(stderr.String(), "invalid environment") {
				t.Fatalf("help exposed environment configuration: %q", stderr.String())
			}
		})
	}
}

func TestVersionReportsOutputFailure(t *testing.T) {
	t.Parallel()
	var stderr bytes.Buffer
	if code := run([]string{"--version"}, mapEnvironment(nil), closedVersionWriter{}, &stderr); code != 1 {
		t.Fatalf("version write failure exit = %d, stderr = %q", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "write version") {
		t.Fatalf("missing version output error: %q", stderr.String())
	}
}

func TestVersionRejectsUnexpectedArguments(t *testing.T) {
	t.Parallel()
	var stdout, stderr bytes.Buffer
	if code := run([]string{"--version", "unexpected"}, mapEnvironment(nil), &stdout, &stderr); code != 2 || stdout.Len() != 0 {
		t.Fatalf("version misuse exit/stdout/stderr = %d/%q/%q", code, stdout.String(), stderr.String())
	}
}

type closedVersionWriter struct{}

func (closedVersionWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }
