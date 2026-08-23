package proxy

import (
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestOwnedRedisClientOptionsHonorContextsAndDisableOpaqueRetries(t *testing.T) {
	t.Parallel()
	config := DefaultConfig()
	config.RedisURL = "rediss://user:password@redis.example.test:6380/2"
	config.DialTimeout = 137 * time.Millisecond
	options, err := ownedRedisClientOptions(config)
	if err != nil {
		t.Fatalf("ownedRedisClientOptions() error: %v", err)
	}
	if !options.ContextTimeoutEnabled {
		t.Fatal("ContextTimeoutEnabled = false, want true")
	}
	if options.MaxRetries != -1 {
		t.Fatalf("MaxRetries = %d, want -1", options.MaxRetries)
	}
	if options.DialerRetries != 1 {
		t.Fatalf("DialerRetries = %d, want 1", options.DialerRetries)
	}
	if options.DialTimeout != config.DialTimeout {
		t.Fatalf("DialTimeout = %s, want %s", options.DialTimeout, config.DialTimeout)
	}
}

func TestRedisConfigurationErrorsRedactCredentials(t *testing.T) {
	t.Parallel()
	const secret = "do-not-echo-this-password"

	malformed := DefaultConfig()
	malformed.BackendURL = "http://backend.example.test"
	malformed.JournalBackend = JournalBackendRedis
	malformed.RedisURL = "redis://user:" + secret + "%zz@redis.example.test/0"
	err := malformed.Validate()
	if err == nil {
		t.Fatal("Validate() accepted malformed Redis URL")
	}
	if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), "user:") {
		t.Fatalf("Validate() leaked Redis credentials: %v", err)
	}

	invalidOptions := DefaultConfig()
	invalidOptions.BackendURL = "http://backend.example.test"
	invalidOptions.JournalBackend = JournalBackendRedis
	invalidOptions.RedisURL = "redis://user:" + secret + "@redis.example.test/not-a-database"
	_, err = NewServer(invalidOptions, slog.New(slog.DiscardHandler))
	if err == nil {
		t.Fatal("NewServer() accepted Redis URL with invalid database")
	}
	if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), "user:") {
		t.Fatalf("NewServer() leaked Redis credentials: %v", err)
	}
	if !strings.Contains(err.Error(), "redis://<redacted>") {
		t.Fatalf("NewServer() error = %v, want redacted Redis URL", err)
	}
}
