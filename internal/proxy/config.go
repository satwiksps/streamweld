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

	"github.com/streamweld/streamweld/internal/conformance"
)

const (
	defaultListenAddress         = ":8080"
	defaultReadHeaderTimeout     = 10 * time.Second
	defaultIdleTimeout           = 2 * time.Minute
	defaultShutdownTimeout       = 15 * time.Second
	defaultReadinessTimeout      = 2 * time.Second
	defaultDialTimeout           = 10 * time.Second
	defaultTLSHandshakeTimeout   = 10 * time.Second
	defaultUpstreamIdleTimeout   = 90 * time.Second
	defaultMaxHeaderBytes        = 1 << 20
	defaultMaxRequestBytes       = 8 << 20
	defaultJournalTTL            = 10 * time.Minute
	defaultJournalStreamBytes    = 4 << 20
	defaultJournalTotalBytes     = 256 << 20
	defaultReaderLagBytes        = 1 << 20
	defaultOrphanTimeout         = 60 * time.Second
	defaultBackendHealthInterval = 5 * time.Second
	defaultBackendQuarantine     = 5 * time.Second
	defaultMaxMigrations         = 3
	defaultMaxMigrationTokens    = 8192
	defaultMaxStreamDuration     = 15 * time.Minute
	defaultSeamWindowBytes       = 64
	defaultStallTimeout          = 30 * time.Second
	defaultMaxSSEEventBytes      = 1 << 20
)

// OrphanPolicy controls what happens to a producer after its final reader
// disconnects. A disconnect is never treated as an explicit stop.
type OrphanPolicy string

const (
	// OrphanContinue lets generation continue with zero attached readers.
	OrphanContinue OrphanPolicy = "continue"
	// OrphanCancelAfter cancels generation after the reattachment grace period.
	OrphanCancelAfter OrphanPolicy = "cancel_after"
	// OrphanCancel cancels generation as soon as the final reader disconnects.
	OrphanCancel OrphanPolicy = "cancel"
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
	MaxRequestBytes               int64
	JournalTTL                    time.Duration
	JournalMaxBytesPerStream      int64
	JournalMaxTotalBytes          int64
	ReaderMaxLagBytes             int64
	OrphanPolicy                  OrphanPolicy
	OrphanTimeout                 time.Duration
	BackendHealthInterval         time.Duration
	BackendQuarantineWindow       time.Duration
	MaxMigrations                 int
	MaxMigrationTokens            uint64
	MaxStreamDuration             time.Duration
	AllowCrossVersion             bool
	AllowStructuredResume         bool
	SeamWindowBytes               int
	TemplateMode                  conformance.TemplateMode
	StallDetectionEnabled         bool
	StallTimeout                  time.Duration
	MaxSSEEventBytes              int
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
		MaxRequestBytes:               defaultMaxRequestBytes,
		JournalTTL:                    defaultJournalTTL,
		JournalMaxBytesPerStream:      defaultJournalStreamBytes,
		JournalMaxTotalBytes:          defaultJournalTotalBytes,
		ReaderMaxLagBytes:             defaultReaderLagBytes,
		OrphanPolicy:                  OrphanContinue,
		OrphanTimeout:                 defaultOrphanTimeout,
		BackendHealthInterval:         defaultBackendHealthInterval,
		BackendQuarantineWindow:       defaultBackendQuarantine,
		MaxMigrations:                 defaultMaxMigrations,
		MaxMigrationTokens:            defaultMaxMigrationTokens,
		MaxStreamDuration:             defaultMaxStreamDuration,
		SeamWindowBytes:               defaultSeamWindowBytes,
		TemplateMode:                  conformance.TemplateStrict,
		StallTimeout:                  defaultStallTimeout,
		MaxSSEEventBytes:              defaultMaxSSEEventBytes,
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
		{"STREAMWELD_JOURNAL_TTL", &cfg.JournalTTL},
		{"STREAMWELD_ORPHAN_TIMEOUT", &cfg.OrphanTimeout},
		{"STREAMWELD_BACKEND_HEALTH_INTERVAL", &cfg.BackendHealthInterval},
		{"STREAMWELD_BACKEND_QUARANTINE_WINDOW", &cfg.BackendQuarantineWindow},
		{"STREAMWELD_MAX_STREAM_DURATION", &cfg.MaxStreamDuration},
		{"STREAMWELD_STALL_TIMEOUT", &cfg.StallTimeout},
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
	int64Values := []struct {
		name string
		dst  *int64
	}{
		{"STREAMWELD_MAX_REQUEST_BYTES", &cfg.MaxRequestBytes},
		{"STREAMWELD_JOURNAL_MAX_BYTES_PER_STREAM", &cfg.JournalMaxBytesPerStream},
		{"STREAMWELD_JOURNAL_MAX_TOTAL_BYTES", &cfg.JournalMaxTotalBytes},
		{"STREAMWELD_READER_MAX_LAG_BYTES", &cfg.ReaderMaxLagBytes},
	}
	for _, item := range int64Values {
		value, ok := lookup(item.name)
		if !ok {
			continue
		}
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return Config{}, fmt.Errorf("parse %s: %w", item.name, err)
		}
		*item.dst = parsed
	}
	if value, ok := lookup("STREAMWELD_ORPHAN_POLICY"); ok {
		cfg.OrphanPolicy = OrphanPolicy(value)
	}
	if value, ok := lookup("STREAMWELD_TEMPLATE_MODE"); ok {
		cfg.TemplateMode = conformance.TemplateMode(value)
	}
	intValues := []struct {
		name string
		dst  *int
	}{
		{"STREAMWELD_MAX_MIGRATIONS", &cfg.MaxMigrations},
		{"STREAMWELD_SEAM_WINDOW_BYTES", &cfg.SeamWindowBytes},
		{"STREAMWELD_MAX_SSE_EVENT_BYTES", &cfg.MaxSSEEventBytes},
	}
	for _, item := range intValues {
		value, ok := lookup(item.name)
		if !ok {
			continue
		}
		parsed, err := strconv.Atoi(value)
		if err != nil {
			return Config{}, fmt.Errorf("parse %s: %w", item.name, err)
		}
		*item.dst = parsed
	}
	if value, ok := lookup("STREAMWELD_MAX_MIGRATION_TOKENS"); ok {
		parsed, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return Config{}, fmt.Errorf("parse STREAMWELD_MAX_MIGRATION_TOKENS: %w", err)
		}
		cfg.MaxMigrationTokens = parsed
	}
	boolValues := []struct {
		name string
		dst  *bool
	}{
		{"STREAMWELD_ALLOW_CROSS_VERSION", &cfg.AllowCrossVersion},
		{"STREAMWELD_ALLOW_STRUCTURED_RESUME", &cfg.AllowStructuredResume},
		{"STREAMWELD_STALL_DETECTION_ENABLED", &cfg.StallDetectionEnabled},
	}
	for _, item := range boolValues {
		value, ok := lookup(item.name)
		if !ok {
			continue
		}
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			return Config{}, fmt.Errorf("parse %s: %w", item.name, err)
		}
		*item.dst = parsed
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
		{"journal TTL", c.JournalTTL},
		{"orphan timeout", c.OrphanTimeout},
		{"backend health interval", c.BackendHealthInterval},
		{"backend quarantine window", c.BackendQuarantineWindow},
		{"max stream duration", c.MaxStreamDuration},
		{"stall timeout", c.StallTimeout},
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
	positiveSizes := []struct {
		name  string
		value int64
	}{
		{"max request bytes", c.MaxRequestBytes},
		{"journal max bytes per stream", c.JournalMaxBytesPerStream},
		{"journal max total bytes", c.JournalMaxTotalBytes},
		{"reader max lag bytes", c.ReaderMaxLagBytes},
	}
	for _, item := range positiveSizes {
		if item.value <= 0 {
			problems = append(problems, fmt.Errorf("%s must be positive", item.name))
		}
	}
	if c.JournalMaxBytesPerStream > c.JournalMaxTotalBytes {
		problems = append(problems, errors.New("journal max bytes per stream cannot exceed total bytes"))
	}
	if !c.OrphanPolicy.valid() {
		problems = append(problems, fmt.Errorf("orphan policy must be continue, cancel_after, or cancel, got %q", c.OrphanPolicy))
	}
	if c.MaxMigrations < 0 {
		problems = append(problems, errors.New("max migrations cannot be negative"))
	}
	if c.SeamWindowBytes <= 0 {
		problems = append(problems, errors.New("seam window bytes must be positive"))
	}
	if c.MaxSSEEventBytes <= 0 {
		problems = append(problems, errors.New("max SSE event bytes must be positive"))
	}
	if err := c.TemplateMode.Validate(); err != nil {
		problems = append(problems, err)
	}

	return errors.Join(problems...)
}

func (policy OrphanPolicy) valid() bool {
	return policy == OrphanContinue || policy == OrphanCancelAfter || policy == OrphanCancel
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
