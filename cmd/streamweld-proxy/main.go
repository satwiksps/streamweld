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
	flags.StringVar(&config.BackendURL, "backend", config.BackendURL, "absolute URL of the OpenAI-compatible backend (or STREAMWELD_BACKEND)")
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
