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
		{"zero stream journal cap", func(c *Config) { c.JournalMaxBytesPerStream = 0 }, "journal max bytes per stream must be positive"},
		{"stream cap exceeds total", func(c *Config) { c.JournalMaxTotalBytes = 1 }, "cannot exceed total bytes"},
		{"zero reader lag cap", func(c *Config) { c.ReaderMaxLagBytes = 0 }, "reader max lag bytes must be positive"},
		{"invalid orphan policy", func(c *Config) { c.OrphanPolicy = "finish_later" }, "orphan policy must be"},
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
		"STREAMWELD_JOURNAL_MAX_BYTES_PER_STREAM":     "34567",
		"STREAMWELD_JOURNAL_MAX_TOTAL_BYTES":          "45678",
		"STREAMWELD_READER_MAX_LAG_BYTES":             "5678",
		"STREAMWELD_ORPHAN_POLICY":                    "cancel_after",
		"STREAMWELD_ORPHAN_TIMEOUT":                   "12s",
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
	if config.JournalTTL != 11*time.Minute || config.OrphanPolicy != OrphanCancelAfter || config.OrphanTimeout != 12*time.Second {
		t.Errorf("durability environment values not applied: %+v", config)
	}
}

func TestConfigFromEnvRejectsMalformedValues(t *testing.T) {
	t.Parallel()
	for _, values := range []map[string]string{
		{"STREAMWELD_DIAL_TIMEOUT": "soon"},
		{"STREAMWELD_MAX_HEADER_BYTES": "many"},
		{"STREAMWELD_JOURNAL_MAX_TOTAL_BYTES": "limitless"},
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
