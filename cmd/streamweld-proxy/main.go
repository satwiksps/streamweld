// Package main runs the Streamweld data-plane proxy command.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/streamweld/streamweld/internal/proxy"
)

func main() {
	os.Exit(run(os.Args[1:], os.LookupEnv, os.Stdout, os.Stderr))
}

func run(args []string, lookup func(string) (string, bool), stdout, stderr io.Writer) int {
	config, err := proxy.ConfigFromEnv(lookup)
	if err != nil {
		startupLogger(stderr).Error("invalid environment configuration", "error", err)
		return 2
	}

	logLevel := envOrDefault(lookup, "STREAMWELD_LOG_LEVEL", "info")
	flags := flag.NewFlagSet("streamweld-proxy", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.Func("backend", "absolute URL of the OpenAI-compatible backend (or STREAMWELD_BACKEND; omitted from help defaults)", func(value string) error {
		config.BackendURL = value
		return nil
	})
	flags.StringVar(&config.ListenAddress, "listen", config.ListenAddress, "TCP address to listen on")
	flags.DurationVar(&config.ReadHeaderTimeout, "read-header-timeout", config.ReadHeaderTimeout, "maximum time to read incoming request headers")
	flags.DurationVar(&config.IdleTimeout, "idle-timeout", config.IdleTimeout, "incoming keep-alive idle timeout")
	flags.DurationVar(&config.ShutdownTimeout, "shutdown-timeout", config.ShutdownTimeout, "graceful shutdown deadline")
	flags.DurationVar(&config.ReadinessTimeout, "readiness-timeout", config.ReadinessTimeout, "maximum time for a backend readiness probe")
	flags.DurationVar(&config.DialTimeout, "dial-timeout", config.DialTimeout, "upstream connection timeout")
	flags.DurationVar(&config.TLSHandshakeTimeout, "tls-handshake-timeout", config.TLSHandshakeTimeout, "upstream TLS handshake timeout")
	flags.DurationVar(&config.ResponseHeaderTimeout, "response-header-timeout", config.ResponseHeaderTimeout, "upstream response-header timeout; zero disables it")
	flags.DurationVar(&config.UpstreamIdleConnectionTimeout, "upstream-idle-connection-timeout", config.UpstreamIdleConnectionTimeout, "upstream keep-alive idle timeout")
	flags.IntVar(&config.MaxHeaderBytes, "max-header-bytes", config.MaxHeaderBytes, "maximum incoming request-header size")
	flags.Int64Var(&config.MaxRequestBytes, "max-request-bytes", config.MaxRequestBytes, "maximum completion request-body size")
	flags.Var((*stringValue)(&config.JournalBackend), "journal-backend", "journal persistence backend: memory or redis")
	flags.DurationVar(&config.JournalTTL, "journal-ttl", config.JournalTTL, "retention time for terminal stream journals")
	flags.Int64Var(&config.JournalMaxBytesPerStream, "journal-max-bytes-per-stream", config.JournalMaxBytesPerStream, "memory journal byte cap per stream")
	flags.Int64Var(&config.JournalMaxTotalBytes, "journal-max-total-bytes", config.JournalMaxTotalBytes, "memory journal global byte cap")
	flags.Int64Var(&config.ReaderMaxLagBytes, "reader-max-lag-bytes", config.ReaderMaxLagBytes, "maximum queued bytes for one stream reader")
	flags.DurationVar(&config.ReaderWriteTimeout, "reader-write-timeout", config.ReaderWriteTimeout, "maximum time for each downstream stream write or flush")
	flags.StringVar(&config.AdminTokenFile, "admin-token-file", config.AdminTokenFile, "file containing the bearer token required by route administration")
	flags.Func("redis-url", "Redis connection URL (or STREAMWELD_REDIS_URL; omitted from help defaults)", func(value string) error {
		config.RedisURL = value
		return nil
	})
	flags.StringVar(&config.RedisKeyPrefix, "redis-key-prefix", config.RedisKeyPrefix, "namespace prefix for Redis journal keys")
	flags.StringVar(&config.ReplicaID, "replica-id", config.ReplicaID, "unique relay identity; generated when omitted")
	flags.StringVar(&config.RelayListenAddress, "relay-listen", config.RelayListenAddress, "private owner-relay TCP listen address")
	flags.Func("relay-advertise-url", "private owner-relay base URL (or STREAMWELD_RELAY_ADVERTISE_URL; empty disables relay)", func(value string) error {
		config.RelayAdvertiseURL = value
		return nil
	})
	flags.StringVar(&config.RelayCAFile, "relay-ca-file", config.RelayCAFile, "PEM CA used to verify relay peers")
	flags.StringVar(&config.RelayCertificateFile, "relay-cert-file", config.RelayCertificateFile, "PEM certificate used for relay mutual TLS")
	flags.StringVar(&config.RelayPrivateKeyFile, "relay-key-file", config.RelayPrivateKeyFile, "PEM private key used for relay mutual TLS")
	flags.BoolVar(&config.RelayInsecureDevMode, "relay-insecure-dev-mode", config.RelayInsecureDevMode, "allow plaintext relay on loopback for development and tests")
	flags.DurationVar(&config.RelayHeartbeatInterval, "relay-heartbeat-interval", config.RelayHeartbeatInterval, "interval between relay owner-presence heartbeats")
	flags.DurationVar(&config.RelayPresenceTTL, "relay-presence-ttl", config.RelayPresenceTTL, "lifetime of a relay owner-presence lease")
	flags.Var(orphanPolicyValue{value: &config.OrphanPolicy}, "orphan-policy", "producer policy after the final reader disconnects: continue, cancel_after, or cancel")
	flags.DurationVar(&config.OrphanTimeout, "orphan-timeout", config.OrphanTimeout, "reattachment grace period for cancel_after")
	flags.DurationVar(&config.BackendHealthInterval, "backend-health-interval", config.BackendHealthInterval, "interval between active backend health probes")
	flags.DurationVar(&config.BackendQuarantineWindow, "backend-quarantine-window", config.BackendQuarantineWindow, "passive-failure backend quarantine period")
	flags.IntVar(&config.MaxMigrations, "max-migrations", config.MaxMigrations, "maximum continuation attempts per stream")
	flags.Uint64Var(&config.MaxMigrationTokens, "max-migration-tokens", config.MaxMigrationTokens, "maximum emitted tokens eligible for continuation")
	flags.DurationVar(&config.MaxStreamDuration, "max-stream-duration", config.MaxStreamDuration, "maximum stream age eligible for continuation")
	flags.BoolVar(&config.AllowCrossVersion, "allow-cross-version", config.AllowCrossVersion, "allow continuation across unequal model versions")
	flags.BoolVar(&config.AllowStructuredResume, "allow-structured-resume", config.AllowStructuredResume, "allow validated JSON-prefix continuation")
	flags.IntVar(&config.SeamWindowBytes, "seam-window-bytes", config.SeamWindowBytes, "leading continuation bytes held for overlap removal")
	flags.Var((*stringValue)(&config.TemplateMode), "template-mode", "chat-template policy: strict or permissive")
	flags.BoolVar(&config.StallDetectionEnabled, "stall-detection", config.StallDetectionEnabled, "enable inter-token stall migration")
	flags.DurationVar(&config.StallTimeout, "stall-timeout", config.StallTimeout, "inter-token stall threshold when enabled")
	flags.IntVar(&config.MaxSSEEventBytes, "max-sse-event-bytes", config.MaxSSEEventBytes, "maximum complete upstream SSE frame size")
	flags.StringVar(&logLevel, "log-level", logLevel, "JSON log level: debug, info, warn, or error")
	flags.Usage = func() {
		_, _ = fmt.Fprintf(stderr, "Usage: streamweld-proxy --backend URL [options]\n\nOptions:\n")
		flags.PrintDefaults()
	}

	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 {
		startupLogger(stderr).Error("unexpected positional arguments", "arguments", flags.Args())
		flags.Usage()
		return 2
	}

	level, err := parseLogLevel(logLevel)
	if err != nil {
		startupLogger(stderr).Error("invalid log level", "value", logLevel, "error", err)
		return 2
	}
	logger := slog.New(slog.NewJSONHandler(stderr, &slog.HandlerOptions{Level: level}))
	server, err := proxy.NewServer(config, logger)
	if err != nil {
		logger.Error("proxy configuration rejected", "error", err)
		return 2
	}

	ctx, stop := newShutdownContext(
		context.Background(),
		func(events chan<- os.Signal) { signal.Notify(events, os.Interrupt, syscall.SIGTERM) },
		signal.Stop,
	)
	defer stop()
	if err := server.ListenAndServe(ctx); err != nil {
		logger.Error("proxy stopped with an error", "error", err)
		return 1
	}
	_ = stdout // Reserved for command output; operational logs remain on stderr.
	return 0
}

type orphanPolicyValue struct {
	value *proxy.OrphanPolicy
}

type stringValue string

func (v *stringValue) String() string {
	if v == nil {
		return ""
	}
	return string(*v)
}

func (v *stringValue) Set(value string) error {
	if v == nil {
		return errors.New("string flag destination is nil")
	}
	*v = stringValue(value)
	return nil
}

func (v orphanPolicyValue) String() string {
	if v.value == nil {
		return ""
	}
	return string(*v.value)
}

func (v orphanPolicyValue) Set(value string) error {
	if v.value == nil {
		return errors.New("orphan policy destination is nil")
	}
	*v.value = proxy.OrphanPolicy(value)
	return nil
}

func newShutdownContext(
	parent context.Context,
	notify func(chan<- os.Signal),
	stopNotifications func(chan<- os.Signal),
) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	events := make(chan os.Signal, 1)
	notify(events)

	var once sync.Once
	stop := func() {
		once.Do(func() {
			// Restore the operating system's default handling before waking the
			// server. A second termination signal can therefore force an exit.
			stopNotifications(events)
			cancel()
		})
	}
	go func() {
		select {
		case <-events:
			stop()
		case <-ctx.Done():
			stop()
		}
	}()
	return ctx, stop
}

func parseLogLevel(value string) (slog.Level, error) {
	var level slog.Level
	if err := level.UnmarshalText([]byte(value)); err != nil {
		return 0, err
	}
	return level, nil
}

func startupLogger(writer io.Writer) *slog.Logger {
	return slog.New(slog.NewJSONHandler(writer, nil))
}

func envOrDefault(lookup func(string) (string, bool), name, fallback string) string {
	if value, ok := lookup(name); ok {
		return value
	}
	return fallback
}
