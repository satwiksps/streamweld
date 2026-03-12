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
	"os"
	"strings"
	"sync"
	"time"

	redislib "github.com/redis/go-redis/v9"
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
	relay       *relayCoordinator

	journalClose     func() error
	journalCloseOnce sync.Once
	journalCloseErr  error
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
		healthURL := *target
		healthURL.Path = "/health"
		healthURL.RawPath = ""
		healthURL.RawQuery = ""
		healthURL.ForceQuery = false
		healthURL.Fragment = ""
		standalone.HealthURL = &healthURL
		settings.backendPool, err = backend.NewPool(poolConfig, standalone)
		if err != nil {
			return nil, fmt.Errorf("create backend pool: %w", err)
		}
		if _, err := settings.backendPool.SetHealth(standalone.ID, backend.HealthHealthy); err != nil {
			return nil, fmt.Errorf("admit standalone backend: %w", err)
		}
	}
	var journalClose func() error
	if settings.journal == nil {
		switch config.JournalBackend {
		case JournalBackendMemory:
			memoryConfig := journal.DefaultConfig()
			memoryConfig.TTL = config.JournalTTL
			memoryConfig.MaxBytesPerStream = config.JournalMaxBytesPerStream
			memoryConfig.MaxTotalBytes = config.JournalMaxTotalBytes
			memoryConfig.ReaderMaxLagBytes = config.ReaderMaxLagBytes
			settings.journal, err = journal.NewMemory(memoryConfig)
			if err != nil {
				return nil, fmt.Errorf("create memory journal: %w", err)
			}
		case JournalBackendRedis:
			redisOptions, parseErr := ownedRedisClientOptions(config)
			if parseErr != nil {
				return nil, parseErr
			}
			client := redislib.NewClient(redisOptions)
			redisConfig := journal.DefaultRedisConfig()
			redisConfig.TTL = config.JournalTTL
			redisConfig.Prefix = config.RedisKeyPrefix
			redisConfig.ReaderMaxLagBytes = config.ReaderMaxLagBytes
			settings.journal, err = journal.NewRedis(client, redisConfig)
			if err != nil {
				_ = client.Close()
				return nil, fmt.Errorf("create Redis journal: %w", err)
			}
			journalClose = client.Close
		}
	}
	if settings.ids == nil {
		settings.ids = journal.NewIDGenerator(nil, nil)
	}
	if settings.idempotency == nil {
		if registry, ok := settings.journal.(journal.IdempotencyRegistry); ok {
			settings.idempotency = registry
		} else {
			settings.idempotency = journal.NewMemoryIdempotencyRegistry(nil)
		}
	}

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
	if settings.readinessChecker == nil {
		settings.readinessChecker = newBackendPoolReadinessChecker(
			settings.backendPool,
			func(result backend.ProbeResult) {
				durable.triggerBindings("health", result.ID, result.Transition.Bindings)
			},
		)
	}
	readiness := newReadinessGate(settings.readinessChecker)
	durable.routes, err = newRouteBackendRegistry(settings.backendPool)
	if err != nil {
		forceCancel()
		if journalClose != nil {
			_ = journalClose()
		}
		return nil, fmt.Errorf("create route backend registry: %w", err)
	}
	directory, _ := settings.journal.(journal.OwnerDirectory)
	relay, err := newRelayCoordinator(relayConfig{
		ReplicaID:        config.ReplicaID,
		ListenAddress:    config.RelayListenAddress,
		AdvertiseURL:     config.RelayAdvertiseURL,
		CAFile:           config.RelayCAFile,
		CertificateFile:  config.RelayCertificateFile,
		PrivateKeyFile:   config.RelayPrivateKeyFile,
		InsecureDevMode:  config.RelayInsecureDevMode,
		Heartbeat:        config.RelayHeartbeatInterval,
		PresenceTTL:      config.RelayPresenceTTL,
		DialTimeout:      config.DialTimeout,
		HandshakeTimeout: config.TLSHandshakeTimeout,
	}, directory, durable, logger)
	if err != nil {
		forceCancel()
		if journalClose != nil {
			_ = journalClose()
		}
		return nil, fmt.Errorf("create owner relay: %w", err)
	}
	durable.relay = relay
	durable.owner = relay.ownerRecord()
	adminToken, err := loadAdminToken(config.AdminTokenFile)
	if err != nil {
		forceCancel()
		if journalClose != nil {
			_ = journalClose()
		}
		return nil, err
	}
	handler := newHandler(readiness, durable, adminToken)
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
		config:       config,
		target:       target,
		transport:    settings.transport,
		handler:      handler,
		httpServer:   httpServer,
		logger:       logger,
		forceCancel:  forceCancel,
		readiness:    readiness,
		durable:      durable,
		backendPool:  settings.backendPool,
		relay:        relay,
		journalClose: journalClose,
	}, nil
}

func loadAdminToken(path string) (string, error) {
	if path == "" {
		return "", nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read admin token file: %w", err)
	}
	if len(data) == 0 || len(data) > 4096 {
		return "", errors.New("admin token file must contain 1-4096 bytes")
	}
	token := strings.TrimSpace(string(data))
	if token == "" || len(strings.Fields(token)) != 1 || strings.ContainsAny(token, "\r\n\x00") {
		return "", errors.New("admin token file must contain one non-empty bearer token")
	}
	return token, nil
}

func ownedRedisClientOptions(config Config) (*redislib.Options, error) {
	options, err := redislib.ParseURL(config.RedisURL)
	if err != nil {
		return nil, fmt.Errorf(
			"parse Redis URL %s: invalid connection options",
			redactedRedisURL(config.RedisURL),
		)
	}
	// Mutating journal operations perform one nonce-safe retry themselves. An
	// owned go-redis client therefore disables opaque command retries, limits
	// connection establishment to one attempt, and lets request contexts bound
	// socket I/O as well as pool waits.
	options.MaxRetries = -1
	options.DialerRetries = 1
	options.ContextTimeoutEnabled = true
	options.DialTimeout = config.DialTimeout
	return options, nil
}

func redactedRedisURL(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "<redacted>"
	}
	switch strings.ToLower(parsed.Scheme) {
	case "redis", "rediss", "unix":
		return strings.ToLower(parsed.Scheme) + "://<redacted>"
	default:
		return "<redacted>"
	}
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
	defer func() {
		if err := s.closeOwnedJournal(); err != nil {
			s.logger.Warn("close journal client", "error", err)
		}
	}()
	healthContext, stopHealth := context.WithCancel(ctx)
	defer stopHealth()
	defer s.forceCancel()
	defer s.readiness.BeginShutdown()
	go s.durable.runHealthChecks(healthContext)
	relayResult, err := s.relay.start(s.durable.rootContext)
	if err != nil {
		_ = listener.Close()
		return err
	}
	defer func() {
		shutdownContext, cancel := context.WithTimeout(context.Background(), s.config.ShutdownTimeout)
		defer cancel()
		if err := s.relay.shutdown(shutdownContext); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.logger.Warn("shutdown owner relay", "error", err)
		}
	}()

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
	case err := <-relayResult:
		_ = s.httpServer.Close()
		s.closeIdleConnections()
		if err == nil {
			return errors.New("owner relay stopped unexpectedly")
		}
		return fmt.Errorf("serve owner relay: %w", err)
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
	producerErr := s.durable.waitActive(ctx)
	if producerErr != nil {
		s.forceCancel()
	}
	relayErr := s.relay.shutdown(ctx)
	s.forceCancel()
	degradationErr := s.durable.waitDegradation(ctx)
	s.closeIdleConnections()
	journalErr := s.closeOwnedJournal()
	return errors.Join(httpErr, producerErr, relayErr, degradationErr, journalErr)
}

func (s *Server) closeOwnedJournal() error {
	s.journalCloseOnce.Do(func() {
		if s.journalClose != nil {
			s.journalCloseErr = s.journalClose()
		}
	})
	return s.journalCloseErr
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
