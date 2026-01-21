// Package proxy implements Streamweld's OpenAI-compatible data-plane proxy.
package proxy

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	defaultListenAddress       = ":8080"
	defaultReadHeaderTimeout   = 10 * time.Second
	defaultIdleTimeout         = 2 * time.Minute
	defaultShutdownTimeout     = 15 * time.Second
	defaultReadinessTimeout    = 2 * time.Second
	defaultDialTimeout         = 10 * time.Second
	defaultTLSHandshakeTimeout = 10 * time.Second
	defaultUpstreamIdleTimeout = 90 * time.Second
	defaultMaxHeaderBytes      = 1 << 20
)

// Config contains the process and upstream transport settings for a proxy.
// ReadinessTimeout bounds each backend GET /health probe.
// ResponseHeaderTimeout may be zero to allow models with an unbounded time to
// first token. All other durations must be positive.
type Config struct {
	BackendURL                    string
	ListenAddress                 string
	ReadHeaderTimeout             time.Duration
	IdleTimeout                   time.Duration
	ShutdownTimeout               time.Duration
	ReadinessTimeout              time.Duration
	DialTimeout                   time.Duration
	TLSHandshakeTimeout           time.Duration
	ResponseHeaderTimeout         time.Duration
	UpstreamIdleConnectionTimeout time.Duration
	MaxHeaderBytes                int
}

// DefaultConfig returns production-safe listener and transport defaults. A
// backend is intentionally not supplied: callers must choose one explicitly.
func DefaultConfig() Config {
	return Config{
		ListenAddress:                 defaultListenAddress,
		ReadHeaderTimeout:             defaultReadHeaderTimeout,
		IdleTimeout:                   defaultIdleTimeout,
		ShutdownTimeout:               defaultShutdownTimeout,
		ReadinessTimeout:              defaultReadinessTimeout,
		DialTimeout:                   defaultDialTimeout,
		TLSHandshakeTimeout:           defaultTLSHandshakeTimeout,
		UpstreamIdleConnectionTimeout: defaultUpstreamIdleTimeout,
		MaxHeaderBytes:                defaultMaxHeaderBytes,
	}
}

// ConfigFromEnv overlays STREAMWELD_* environment variables on DefaultConfig.
// It reports malformed values instead of silently replacing them with defaults.
func ConfigFromEnv(lookup func(string) (string, bool)) (Config, error) {
	if lookup == nil {
		return Config{}, errors.New("environment lookup function is nil")
	}

	cfg := DefaultConfig()
	if value, ok := lookup("STREAMWELD_BACKEND"); ok {
		cfg.BackendURL = value
	}
	if value, ok := lookup("STREAMWELD_LISTEN"); ok {
		cfg.ListenAddress = value
	}

	durations := []struct {
		name string
		dst  *time.Duration
	}{
		{"STREAMWELD_READ_HEADER_TIMEOUT", &cfg.ReadHeaderTimeout},
		{"STREAMWELD_IDLE_TIMEOUT", &cfg.IdleTimeout},
		{"STREAMWELD_SHUTDOWN_TIMEOUT", &cfg.ShutdownTimeout},
		{"STREAMWELD_READINESS_TIMEOUT", &cfg.ReadinessTimeout},
		{"STREAMWELD_DIAL_TIMEOUT", &cfg.DialTimeout},
		{"STREAMWELD_TLS_HANDSHAKE_TIMEOUT", &cfg.TLSHandshakeTimeout},
		{"STREAMWELD_RESPONSE_HEADER_TIMEOUT", &cfg.ResponseHeaderTimeout},
		{"STREAMWELD_UPSTREAM_IDLE_CONNECTION_TIMEOUT", &cfg.UpstreamIdleConnectionTimeout},
	}
	for _, item := range durations {
		value, ok := lookup(item.name)
		if !ok {
			continue
		}
		parsed, err := time.ParseDuration(value)
		if err != nil {
			return Config{}, fmt.Errorf("parse %s: %w", item.name, err)
		}
		*item.dst = parsed
	}

	if value, ok := lookup("STREAMWELD_MAX_HEADER_BYTES"); ok {
		parsed, err := strconv.Atoi(value)
		if err != nil {
			return Config{}, fmt.Errorf("parse STREAMWELD_MAX_HEADER_BYTES: %w", err)
		}
		cfg.MaxHeaderBytes = parsed
	}

	return cfg, nil
}

// Validate checks all configuration before the listener is opened.
func (c Config) Validate() error {
	var problems []error
	if _, err := parseBackendURL(c.BackendURL); err != nil {
		problems = append(problems, fmt.Errorf("backend URL: %w", err))
	}
	if err := validateListenAddress(c.ListenAddress); err != nil {
		problems = append(problems, fmt.Errorf("listen address: %w", err))
	}

	positiveDurations := []struct {
		name  string
		value time.Duration
	}{
		{"read header timeout", c.ReadHeaderTimeout},
		{"idle timeout", c.IdleTimeout},
		{"shutdown timeout", c.ShutdownTimeout},
		{"readiness timeout", c.ReadinessTimeout},
		{"dial timeout", c.DialTimeout},
		{"TLS handshake timeout", c.TLSHandshakeTimeout},
		{"upstream idle connection timeout", c.UpstreamIdleConnectionTimeout},
	}
	for _, item := range positiveDurations {
		if item.value <= 0 {
			problems = append(problems, fmt.Errorf("%s must be positive", item.name))
		}
	}
	if c.ResponseHeaderTimeout < 0 {
		problems = append(problems, errors.New("response header timeout cannot be negative"))
	}
	if c.MaxHeaderBytes <= 0 {
		problems = append(problems, errors.New("max header bytes must be positive"))
	}

	return errors.Join(problems...)
}

func parseBackendURL(raw string) (*url.URL, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, errors.New("is required")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	u.Scheme = strings.ToLower(u.Scheme)
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("scheme must be http or https, got %q", u.Scheme)
	}
	if u.Host == "" || u.Hostname() == "" {
		return nil, errors.New("must be an absolute URL with a host")
	}
	if u.User != nil {
		return nil, errors.New("must not contain user credentials")
	}
	if u.Fragment != "" {
		return nil, errors.New("must not contain a fragment")
	}
	return u, nil
}

func validateListenAddress(address string) error {
	if strings.TrimSpace(address) == "" {
		return errors.New("is required")
	}
	_, port, err := net.SplitHostPort(address)
	if err != nil {
		return err
	}
	value, err := strconv.Atoi(port)
	if err != nil {
		return fmt.Errorf("port must be numeric: %w", err)
	}
	if value < 0 || value > 65535 {
		return fmt.Errorf("port %d is outside 0..65535", value)
	}
	return nil
}
