package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/streamweld/streamweld/internal/backend"
	"github.com/streamweld/streamweld/internal/conformance"
	"github.com/streamweld/streamweld/internal/journal"
)

// Option customizes a Server without expanding its core configuration surface.
type Option func(*serverOptions) error

type serverOptions struct {
	transport        http.RoundTripper
	readinessChecker ReadinessChecker
	journal          journal.Journal
	ids              StreamIDGenerator
	idempotency      journal.IdempotencyRegistry
	backendPool      *backend.Pool
}

// StreamIDGenerator supplies collision-resistant stream identifiers.
type StreamIDGenerator interface {
	New() (journal.StreamID, error)
}

// WithReadinessChecker supplies the policy used by /readyz.
func WithReadinessChecker(checker ReadinessChecker) Option {
	return func(options *serverOptions) error {
		if checker == nil {
			return errors.New("readiness checker cannot be nil")
		}
		options.readinessChecker = checker
		return nil
	}
}

// WithTransport supplies the shared upstream HTTP transport.
func WithTransport(transport http.RoundTripper) Option {
	return func(options *serverOptions) error {
		if transport == nil {
			return errors.New("proxy transport cannot be nil")
		}
		options.transport = transport
		return nil
	}
}

// WithJournal supplies the durable journal implementation. The default is a
// bounded in-memory journal intended for a single proxy replica.
func WithJournal(backend journal.Journal) Option {
	return func(options *serverOptions) error {
		if backend == nil {
			return errors.New("journal cannot be nil")
		}
		options.journal = backend
		return nil
	}
}

// WithStreamIDGenerator supplies stream identities, primarily for deterministic
// tests. Production callers normally use the cryptographic default.
func WithStreamIDGenerator(generator StreamIDGenerator) Option {
	return func(options *serverOptions) error {
		if generator == nil {
			return errors.New("stream ID generator cannot be nil")
		}
		options.ids = generator
		return nil
	}
}

// WithIdempotencyRegistry supplies the key-to-stream registry.
func WithIdempotencyRegistry(registry journal.IdempotencyRegistry) Option {
	return func(options *serverOptions) error {
		if registry == nil {
			return errors.New("idempotency registry cannot be nil")
		}
		options.idempotency = registry
		return nil
	}
}

// WithBackendPool supplies a dynamically updateable backend pool. The default
// pool contains Config.BackendURL as an immediately healthy standalone backend.
func WithBackendPool(pool *backend.Pool) Option {
	return func(options *serverOptions) error {
		if pool == nil {
			return errors.New("backend pool cannot be nil")
		}
		options.backendPool = pool
		return nil
	}
}

// Server is an OpenAI-compatible passthrough proxy with graceful lifecycle
// management. Its Handler can be embedded in an existing HTTP server for tests.
type Server struct {
	config      Config
	target      *url.URL
	transport   http.RoundTripper
	handler     *Handler
	httpServer  *http.Server
	logger      *slog.Logger
	forceCancel context.CancelFunc
	readiness   *readinessGate
	durable     *durableService
	backendPool *backend.Pool
}

// NewServer validates config and constructs a proxy without opening a listener.
func NewServer(config Config, logger *slog.Logger, options ...Option) (*Server, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid proxy configuration: %w", err)
	}
	target, err := parseBackendURL(config.BackendURL)
	if err != nil {
		return nil, fmt.Errorf("parse backend URL: %w", err)
	}
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}

	settings := serverOptions{}
	for _, option := range options {
		if option == nil {
			return nil, errors.New("proxy option cannot be nil")
		}
		if err := option(&settings); err != nil {
			return nil, fmt.Errorf("apply proxy option: %w", err)
		}
	}
	if settings.transport == nil {
		settings.transport = newTransport(config)
	}
	if settings.backendPool == nil {
		poolConfig := backend.DefaultConfig()
		poolConfig.QuarantineWindow = config.BackendQuarantineWindow
		poolConfig.ProbeInterval = config.BackendHealthInterval
		poolConfig.ProbeTimeout = config.ReadinessTimeout
		poolConfig.HTTPClient = &http.Client{
			Transport: settings.transport,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}
		standalone := backend.Backend{
			ID:              backend.ID(target.Host),
			URL:             target,
			TemplateVerdict: conformance.VerdictUnknown,
		}
		settings.backendPool, err = backend.NewPool(poolConfig, standalone)
		if err != nil {
			return nil, fmt.Errorf("create backend pool: %w", err)
		}
		if _, err := settings.backendPool.SetHealth(standalone.ID, backend.HealthHealthy); err != nil {
			return nil, fmt.Errorf("admit standalone backend: %w", err)
		}
	}
	if settings.readinessChecker == nil {
		settings.readinessChecker = newBackendReadinessChecker(target, settings.transport, config.ReadinessTimeout)
	}
	if settings.journal == nil {
		memoryConfig := journal.DefaultConfig()
		memoryConfig.TTL = config.JournalTTL
		memoryConfig.MaxBytesPerStream = config.JournalMaxBytesPerStream
		memoryConfig.MaxTotalBytes = config.JournalMaxTotalBytes
		memoryConfig.ReaderMaxLagBytes = config.ReaderMaxLagBytes
		settings.journal, err = journal.NewMemory(memoryConfig)
		if err != nil {
			return nil, fmt.Errorf("create memory journal: %w", err)
		}
	}
	if settings.ids == nil {
		settings.ids = journal.NewIDGenerator(nil, nil)
	}
	if settings.idempotency == nil {
		settings.idempotency = journal.NewMemoryIdempotencyRegistry(nil)
	}

	readiness := newReadinessGate(settings.readinessChecker)
	serverContext, forceCancel := context.WithCancel(context.Background())
	durable := newDurableService(
		serverContext,
		config,
		target,
		settings.transport,
		settings.journal,
		settings.ids,
		settings.idempotency,
		logger,
		settings.backendPool,
	)
	handler := newHandler(readiness, durable)
	httpServer := &http.Server{
		Addr:              config.ListenAddress,
		Handler:           handler,
		ReadHeaderTimeout: config.ReadHeaderTimeout,
		IdleTimeout:       config.IdleTimeout,
		MaxHeaderBytes:    config.MaxHeaderBytes,
		ErrorLog:          log.New(&slogWriter{logger: logger}, "", 0),
		BaseContext: func(net.Listener) context.Context {
			return serverContext
		},
	}

	return &Server{
		config:      config,
		target:      target,
		transport:   settings.transport,
		handler:     handler,
		httpServer:  httpServer,
		logger:      logger,
		forceCancel: forceCancel,
		readiness:   readiness,
		durable:     durable,
		backendPool: settings.backendPool,
	}, nil
}

// Handler returns the server's routing handler.
func (s *Server) Handler() http.Handler {
	return s.handler
}

// ListenAndServe opens Config.ListenAddress and serves until ctx is canceled or
// the listener fails. Cancellation starts a bounded graceful shutdown.
func (s *Server) ListenAndServe(ctx context.Context) error {
	if ctx == nil {
		return errors.New("serve context cannot be nil")
	}
	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(ctx, "tcp", s.config.ListenAddress)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", s.config.ListenAddress, err)
	}
	return s.Serve(ctx, listener)
}

// Serve runs on an existing listener until ctx is canceled. The HTTP server
// takes ownership of listener, matching net/http.Server.Serve semantics.
func (s *Server) Serve(ctx context.Context, listener net.Listener) error {
	if ctx == nil {
		return errors.New("serve context cannot be nil")
	}
	if listener == nil {
		return errors.New("listener cannot be nil")
	}
	healthContext, stopHealth := context.WithCancel(ctx)
	defer stopHealth()
	defer s.forceCancel()
	defer s.readiness.BeginShutdown()
	go s.durable.runHealthChecks(healthContext)

	s.logger.Info("proxy listening",
		"address", listener.Addr().String(),
		"backend", safeBackendAddress(s.target),
	)
	serveResult := make(chan error, 1)
	go func() {
		serveResult <- s.httpServer.Serve(listener)
	}()

	select {
	case err := <-serveResult:
		s.closeIdleConnections()
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("serve proxy: %w", err)
	case <-ctx.Done():
	}

	s.logger.Info("proxy shutting down")
	shutdownContext, cancel := context.WithTimeout(context.Background(), s.config.ShutdownTimeout)
	defer cancel()
	shutdownErr := s.Shutdown(shutdownContext)
	if shutdownErr != nil {
		s.logger.Error("graceful proxy shutdown failed", "error", shutdownErr)
		s.forceCancel()
		closeErr := s.httpServer.Close()
		if closeErr != nil && !errors.Is(closeErr, http.ErrServerClosed) {
			shutdownErr = errors.Join(shutdownErr, fmt.Errorf("force close proxy: %w", closeErr))
		}
	}

	serveErr := <-serveResult
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		serveErr = fmt.Errorf("serve proxy: %w", serveErr)
	} else {
		serveErr = nil
	}
	return errors.Join(shutdownErr, serveErr)
}

// Shutdown gracefully stops accepting requests and waits for active handlers.
func (s *Server) Shutdown(ctx context.Context) error {
	if ctx == nil {
		return errors.New("shutdown context cannot be nil")
	}
	s.readiness.BeginShutdown()
	httpErr := s.httpServer.Shutdown(ctx)
	producerErr := s.durable.wait(ctx)
	s.closeIdleConnections()
	return errors.Join(httpErr, producerErr)
}

func (s *Server) closeIdleConnections() {
	if closer, ok := s.transport.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
	}
}

func newTransport(config Config) *http.Transport {
	dialer := &net.Dialer{
		Timeout:   config.DialTimeout,
		KeepAlive: 30 * time.Second,
	}
	return &http.Transport{
		DialContext:           dialer.DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   100,
		IdleConnTimeout:       config.UpstreamIdleConnectionTimeout,
		TLSHandshakeTimeout:   config.TLSHandshakeTimeout,
		ResponseHeaderTimeout: config.ResponseHeaderTimeout,
		ExpectContinueTimeout: time.Second,
		DisableCompression:    true,
	}
}

func safeBackendAddress(target *url.URL) string {
	return target.Scheme + "://" + target.Host + target.EscapedPath()
}
