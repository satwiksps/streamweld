package proxy

import (
	"strings"
	"testing"
	"time"
)

func TestConfigValidate(t *testing.T) {
	t.Parallel()

	valid := DefaultConfig()
	valid.BackendURL = "https://backend.example.test/base"
	valid.ListenAddress = "127.0.0.1:0"
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{"missing backend", func(c *Config) { c.BackendURL = "" }, "backend URL: is required"},
		{"relative backend", func(c *Config) { c.BackendURL = "/api" }, "scheme must be http or https"},
		{"unsupported backend scheme", func(c *Config) { c.BackendURL = "ftp://backend.example.test" }, "scheme must be http or https"},
		{"backend without host", func(c *Config) { c.BackendURL = "http:///api" }, "absolute URL with a host"},
		{"backend credentials", func(c *Config) { c.BackendURL = "http://user:secret@backend.example.test" }, "must not contain user credentials"},
		{"backend fragment", func(c *Config) { c.BackendURL = "http://backend.example.test/#fragment" }, "must not contain a fragment"},
		{"listen without port", func(c *Config) { c.ListenAddress = "localhost" }, "listen address"},
		{"non-numeric port", func(c *Config) { c.ListenAddress = "localhost:http" }, "port must be numeric"},
		{"negative read timeout", func(c *Config) { c.ReadHeaderTimeout = -time.Second }, "read header timeout must be positive"},
		{"zero idle timeout", func(c *Config) { c.IdleTimeout = 0 }, "idle timeout must be positive"},
		{"zero readiness timeout", func(c *Config) { c.ReadinessTimeout = 0 }, "readiness timeout must be positive"},
		{"negative response timeout", func(c *Config) { c.ResponseHeaderTimeout = -time.Second }, "response header timeout cannot be negative"},
		{"zero max header bytes", func(c *Config) { c.MaxHeaderBytes = 0 }, "max header bytes must be positive"},
		{"zero max request bytes", func(c *Config) { c.MaxRequestBytes = 0 }, "max request bytes must be positive"},
		{"zero journal TTL", func(c *Config) { c.JournalTTL = 0 }, "journal TTL must be positive"},
		{"invalid journal backend", func(c *Config) { c.JournalBackend = "disk" }, "journal backend must be memory or redis"},
		{"redis without URL", func(c *Config) { c.JournalBackend = JournalBackendRedis }, "redis URL: is required"},
		{"redis URL scheme", func(c *Config) { c.JournalBackend = JournalBackendRedis; c.RedisURL = "http://redis.test" }, "scheme must be redis"},
		{"blank redis prefix", func(c *Config) { c.RedisKeyPrefix = " " }, "redis key prefix cannot be blank"},
		{"zero stream journal cap", func(c *Config) { c.JournalMaxBytesPerStream = 0 }, "journal max bytes per stream must be positive"},
		{"stream cap exceeds total", func(c *Config) { c.JournalMaxTotalBytes = 1 }, "cannot exceed total bytes"},
		{"zero reader lag cap", func(c *Config) { c.ReaderMaxLagBytes = 0 }, "reader max lag bytes must be positive"},
		{"zero reader write timeout", func(c *Config) { c.ReaderWriteTimeout = 0 }, "reader write timeout must be positive"},
		{"invalid orphan policy", func(c *Config) { c.OrphanPolicy = "finish_later" }, "orphan policy must be"},
		{"negative migrations", func(c *Config) { c.MaxMigrations = -1 }, "max migrations cannot be negative"},
		{"zero seam window", func(c *Config) { c.SeamWindowBytes = 0 }, "seam window bytes must be positive"},
		{"invalid template mode", func(c *Config) { c.TemplateMode = "risky" }, "template mode must be"},
		{"zero SSE event limit", func(c *Config) { c.MaxSSEEventBytes = 0 }, "max SSE event bytes must be positive"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := valid
			test.mutate(&config)
			err := config.Validate()
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Validate() error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestConfigValidateReportsAllProblems(t *testing.T) {
	t.Parallel()
	config := Config{}
	err := config.Validate()
	if err == nil {
		t.Fatal("Validate() returned nil for zero config")
	}
	for _, want := range []string{"backend URL", "listen address", "read header timeout", "max header bytes"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("Validate() error %q does not contain %q", err, want)
		}
	}
}

func TestConfigFromEnv(t *testing.T) {
	t.Parallel()
	values := map[string]string{
		"STREAMWELD_BACKEND":                          "http://backend.example.test:9000/base",
		"STREAMWELD_LISTEN":                           "127.0.0.1:9090",
		"STREAMWELD_READ_HEADER_TIMEOUT":              "3s",
		"STREAMWELD_IDLE_TIMEOUT":                     "4s",
		"STREAMWELD_SHUTDOWN_TIMEOUT":                 "5s",
		"STREAMWELD_READINESS_TIMEOUT":                "3500ms",
		"STREAMWELD_DIAL_TIMEOUT":                     "6s",
		"STREAMWELD_TLS_HANDSHAKE_TIMEOUT":            "7s",
		"STREAMWELD_RESPONSE_HEADER_TIMEOUT":          "8s",
		"STREAMWELD_UPSTREAM_IDLE_CONNECTION_TIMEOUT": "9s",
		"STREAMWELD_MAX_HEADER_BYTES":                 "12345",
		"STREAMWELD_MAX_REQUEST_BYTES":                "23456",
		"STREAMWELD_JOURNAL_TTL":                      "11m",
		"STREAMWELD_JOURNAL_BACKEND":                  "redis",
		"STREAMWELD_JOURNAL_MAX_BYTES_PER_STREAM":     "34567",
		"STREAMWELD_JOURNAL_MAX_TOTAL_BYTES":          "45678",
		"STREAMWELD_READER_MAX_LAG_BYTES":             "5678",
		"STREAMWELD_READER_WRITE_TIMEOUT":             "10s",
		"STREAMWELD_REDIS_URL":                        "rediss://redis.example.test:6380/2",
		"STREAMWELD_REDIS_KEY_PREFIX":                 "tenant-a",
		"STREAMWELD_ORPHAN_POLICY":                    "cancel_after",
		"STREAMWELD_ORPHAN_TIMEOUT":                   "12s",
		"STREAMWELD_BACKEND_HEALTH_INTERVAL":          "13s",
		"STREAMWELD_BACKEND_QUARANTINE_WINDOW":        "14s",
		"STREAMWELD_MAX_MIGRATIONS":                   "4",
		"STREAMWELD_MAX_MIGRATION_TOKENS":             "9000",
		"STREAMWELD_MAX_STREAM_DURATION":              "16m",
		"STREAMWELD_ALLOW_CROSS_VERSION":              "true",
		"STREAMWELD_ALLOW_STRUCTURED_RESUME":          "true",
		"STREAMWELD_SEAM_WINDOW_BYTES":                "96",
		"STREAMWELD_TEMPLATE_MODE":                    "permissive",
		"STREAMWELD_STALL_DETECTION_ENABLED":          "true",
		"STREAMWELD_STALL_TIMEOUT":                    "17s",
		"STREAMWELD_MAX_SSE_EVENT_BYTES":              "1048577",
	}
	config, err := ConfigFromEnv(mapLookup(values))
	if err != nil {
		t.Fatalf("ConfigFromEnv() error: %v", err)
	}
	if config.BackendURL != values["STREAMWELD_BACKEND"] || config.ListenAddress != values["STREAMWELD_LISTEN"] {
		t.Fatalf("string environment values not applied: %+v", config)
	}
	for name, got := range map[string]time.Duration{
		"read":          config.ReadHeaderTimeout,
		"idle":          config.IdleTimeout,
		"shutdown":      config.ShutdownTimeout,
		"readiness":     config.ReadinessTimeout,
		"dial":          config.DialTimeout,
		"TLS":           config.TLSHandshakeTimeout,
		"response":      config.ResponseHeaderTimeout,
		"upstream idle": config.UpstreamIdleConnectionTimeout,
	} {
		if got < 3*time.Second || got > 9*time.Second {
			t.Errorf("%s duration not overlaid: %s", name, got)
		}
	}
	if config.MaxHeaderBytes != 12345 {
		t.Errorf("MaxHeaderBytes = %d, want 12345", config.MaxHeaderBytes)
	}
	if config.MaxRequestBytes != 23456 || config.JournalMaxBytesPerStream != 34567 || config.JournalMaxTotalBytes != 45678 || config.ReaderMaxLagBytes != 5678 {
		t.Errorf("size environment values not applied: %+v", config)
	}
	if config.ReaderWriteTimeout != 10*time.Second {
		t.Errorf("ReaderWriteTimeout = %s, want 10s", config.ReaderWriteTimeout)
	}
	if config.JournalTTL != 11*time.Minute || config.OrphanPolicy != OrphanCancelAfter || config.OrphanTimeout != 12*time.Second {
		t.Errorf("durability environment values not applied: %+v", config)
	}
	if config.JournalBackend != JournalBackendRedis || config.RedisURL != values["STREAMWELD_REDIS_URL"] || config.RedisKeyPrefix != "tenant-a" {
		t.Errorf("redis environment values not applied: %+v", config)
	}
	if config.BackendHealthInterval != 13*time.Second || config.BackendQuarantineWindow != 14*time.Second || config.MaxStreamDuration != 16*time.Minute || config.StallTimeout != 17*time.Second {
		t.Errorf("migration duration values not applied: %+v", config)
	}
	if config.MaxMigrations != 4 || config.MaxMigrationTokens != 9000 || config.SeamWindowBytes != 96 || config.MaxSSEEventBytes != 1048577 {
		t.Errorf("migration limit values not applied: %+v", config)
	}
	if !config.AllowCrossVersion || !config.AllowStructuredResume || !config.StallDetectionEnabled || config.TemplateMode != "permissive" {
		t.Errorf("migration policy values not applied: %+v", config)
	}
}

func TestConfigFromEnvRejectsMalformedValues(t *testing.T) {
	t.Parallel()
	for _, values := range []map[string]string{
		{"STREAMWELD_DIAL_TIMEOUT": "soon"},
		{"STREAMWELD_READER_WRITE_TIMEOUT": "eventually"},
		{"STREAMWELD_MAX_HEADER_BYTES": "many"},
		{"STREAMWELD_JOURNAL_MAX_TOTAL_BYTES": "limitless"},
		{"STREAMWELD_MAX_MIGRATIONS": "several"},
		{"STREAMWELD_MAX_MIGRATION_TOKENS": "all"},
		{"STREAMWELD_ALLOW_CROSS_VERSION": "sometimes"},
	} {
		if _, err := ConfigFromEnv(mapLookup(values)); err == nil {
			t.Fatalf("ConfigFromEnv(%v) returned nil error", values)
		}
	}
	if _, err := ConfigFromEnv(nil); err == nil {
		t.Fatal("ConfigFromEnv(nil) returned nil error")
	}
}

func mapLookup(values map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) {
		value, ok := values[name]
		return value, ok
	}
}
