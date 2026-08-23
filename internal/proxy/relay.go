package proxy

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/satwiksps/streamweld/internal/journal"
)

const (
	relayEventsPrefix            = "/internal/relay/v1/streams/"
	maxRelayControlResponseBytes = 64 << 10
)

type relayCoordinator struct {
	replicaID    string
	owner        journal.OwnerRecord
	directory    journal.OwnerDirectory
	durable      *durableService
	logger       *slog.Logger
	client       *http.Client
	server       *http.Server
	tlsConfig    *tls.Config
	listen       string
	heartbeat    time.Duration
	ttl          time.Duration
	writeTimeout time.Duration
	enabled      bool
	insecure     bool

	mu              sync.Mutex
	listener        net.Listener
	heartbeatCancel context.CancelFunc
	heartbeatDone   chan struct{}
	shuttingDown    bool
}

type relayConfig struct {
	ReplicaID        string
	ListenAddress    string
	AdvertiseURL     string
	CAFile           string
	CertificateFile  string
	PrivateKeyFile   string
	InsecureDevMode  bool
	Heartbeat        time.Duration
	PresenceTTL      time.Duration
	DialTimeout      time.Duration
	HandshakeTimeout time.Duration
}

func newReplicaID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("generate replica identity: %w", err)
	}
	return hex.EncodeToString(value[:]), nil
}

func newRelayCoordinator(
	config relayConfig,
	directory journal.OwnerDirectory,
	durable *durableService,
	logger *slog.Logger,
) (*relayCoordinator, error) {
	replicaID := config.ReplicaID
	if replicaID == "" {
		var err error
		replicaID, err = newReplicaID()
		if err != nil {
			return nil, err
		}
	}
	coordinator := &relayCoordinator{
		replicaID: replicaID,
		directory: directory,
		durable:   durable,
		logger:    logger,
		listen:    config.ListenAddress,
		heartbeat: config.Heartbeat,
		ttl:       config.PresenceTTL,
		enabled:   config.AdvertiseURL != "",
		insecure:  config.InsecureDevMode,
	}
	if durable != nil {
		coordinator.writeTimeout = durable.config.ReaderWriteTimeout
	}
	if !coordinator.enabled {
		return coordinator, nil
	}
	coordinator.owner = journal.OwnerRecord{ReplicaID: replicaID, RelayURL: config.AdvertiseURL}
	if err := coordinator.owner.Validate(); err != nil {
		return nil, fmt.Errorf("invalid relay owner: %w", err)
	}
	if directory == nil {
		return nil, errors.New("relay requires a journal owner directory")
	}

	tlsConfig, transport, err := buildRelayTLS(config)
	if err != nil {
		return nil, err
	}
	coordinator.tlsConfig = tlsConfig
	coordinator.client = &http.Client{
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	coordinator.server = &http.Server{
		Addr:              config.ListenAddress,
		Handler:           coordinator,
		ReadHeaderTimeout: config.HandshakeTimeout,
		IdleTimeout:       2 * time.Minute,
		MaxHeaderBytes:    16 << 10,
		TLSConfig:         tlsConfig,
	}
	return coordinator, nil
}

func buildRelayTLS(config relayConfig) (*tls.Config, *http.Transport, error) {
	dialer := &net.Dialer{Timeout: config.DialTimeout, KeepAlive: 30 * time.Second}
	transport := &http.Transport{
		DialContext:           dialer.DialContext,
		ForceAttemptHTTP2:     true,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   config.HandshakeTimeout,
		ResponseHeaderTimeout: config.HandshakeTimeout,
		DisableCompression:    true,
	}
	if config.InsecureDevMode {
		return nil, transport, nil
	}
	certificate, err := tls.LoadX509KeyPair(config.CertificateFile, config.PrivateKeyFile)
	if err != nil {
		return nil, nil, fmt.Errorf("load relay certificate: %w", err)
	}
	caPEM, err := os.ReadFile(config.CAFile)
	if err != nil {
		return nil, nil, fmt.Errorf("read relay CA: %w", err)
	}
	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(caPEM) {
		return nil, nil, errors.New("relay CA file contains no certificates")
	}
	serverTLS := &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{certificate},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    caPool,
	}
	transport.TLSClientConfig = &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{certificate},
		RootCAs:      caPool,
	}
	return serverTLS, transport, nil
}

func (coordinator *relayCoordinator) ownerRecord() *journal.OwnerRecord {
	if coordinator == nil || !coordinator.enabled {
		return nil
	}
	return &coordinator.owner
}

func (coordinator *relayCoordinator) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if coordinator == nil || !coordinator.enabled || coordinator.durable == nil {
		writeAPIError(writer, http.StatusNotFound, "not_found", "the requested endpoint is not supported")
		return
	}
	if !relayTLSVerified(request, coordinator.tlsConfig) {
		writeAPIError(writer, http.StatusUnauthorized, "relay_authentication_required", "a verified relay client certificate is required")
		return
	}
	rest := strings.TrimPrefix(request.URL.Path, relayEventsPrefix)
	if rest == request.URL.Path {
		writeAPIError(writer, http.StatusNotFound, "not_found", "the requested endpoint is not supported")
		return
	}
	var rawID, operation string
	switch {
	case strings.HasSuffix(rest, "/events"):
		rawID, operation = strings.TrimSuffix(rest, "/events"), "events"
	case strings.HasSuffix(rest, "/stop"):
		rawID, operation = strings.TrimSuffix(rest, "/stop"), "stop"
	default:
		writeAPIError(writer, http.StatusNotFound, "not_found", "the requested endpoint is not supported")
		return
	}
	if rawID == "" || strings.Contains(rawID, "/") {
		writeAPIError(writer, http.StatusNotFound, "not_found", "the requested endpoint is not supported")
		return
	}
	id, err := journal.ParseStreamID(rawID)
	if err != nil {
		writeStreamError(writer, http.StatusBadRequest, "invalid_stream_id", "stream ID is not canonical", rawID)
		return
	}
	runtime, local := coordinator.durable.loadRuntime(id)
	if !local {
		writeStreamError(writer, http.StatusNotFound, "stream_owner_not_local", "stream is not owned by this proxy", id.String())
		return
	}
	handler := &Handler{durable: coordinator.durable}
	if operation == "stop" {
		if requireMethod(writer, request, http.MethodPost) {
			handler.handleLocalStreamStop(writer, request, id, runtime)
		}
		return
	}
	if !requireMethod(writer, request, http.MethodGet) {
		return
	}
	cursor, err := parseResumeCursor(request.Header)
	if err != nil {
		writeStreamError(writer, http.StatusBadRequest, "invalid_resume_cursor", err.Error(), id.String())
		return
	}
	verbose, err := parseVerboseHeader(request.Header)
	if err != nil {
		writeStreamError(writer, http.StatusBadRequest, "invalid_verbose", err.Error(), id.String())
		return
	}
	handler.serveLocalJournal(writer, request, id, cursor, verbose, runtime)
}

func (coordinator *relayCoordinator) tryServeRemoteStop(
	writer http.ResponseWriter,
	request *http.Request,
	id journal.StreamID,
) bool {
	if coordinator == nil || !coordinator.enabled || coordinator.directory == nil {
		return false
	}
	owner, err := coordinator.directory.LocateOwner(request.Context(), id)
	if err != nil || owner.ReplicaID == coordinator.replicaID {
		return false
	}
	if err := coordinator.validateLocatedOwner(owner); err != nil {
		coordinator.logger.WarnContext(request.Context(), "reject unsafe stream owner stop relay",
			"stream_id", safeLogString(id.String()),
			"owner", safeLogString(owner.ReplicaID),
			"error", safeLogError(err))
		return false
	}
	endpoint, err := relayOperationURL(owner.RelayURL, id, "stop")
	if err != nil {
		return false
	}
	relayRequest, err := http.NewRequestWithContext(request.Context(), http.MethodPost, endpoint, nil)
	if err != nil {
		return false
	}
	relayRequest.Header.Set("User-Agent", "")
	response, err := coordinator.client.Do(relayRequest)
	if err != nil {
		coordinator.logger.WarnContext(request.Context(), "connect stream owner stop relay",
			"stream_id", safeLogString(id.String()),
			"owner", safeLogString(owner.ReplicaID),
			"error", safeLogError(err))
		return false
	}
	defer func() {
		if closeErr := response.Body.Close(); closeErr != nil {
			coordinator.logger.DebugContext(request.Context(), "close owner stop relay response",
				"stream_id", safeLogString(id.String()),
				"error", safeLogError(closeErr))
		}
	}()
	if !relayStopStatus(response.StatusCode) || !relayMediaType(response.Header, "application/json") {
		return false
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxRelayControlResponseBytes+1))
	if err != nil || len(body) > maxRelayControlResponseBytes {
		return false
	}
	if !validRelayStopBody(response.StatusCode, body, id) {
		return false
	}
	for _, name := range []string{"Cache-Control", "Content-Type"} {
		if value := response.Header.Get(name); value != "" {
			writer.Header().Set(name, value)
		}
	}
	writer.WriteHeader(response.StatusCode)
	_, _ = writer.Write(body)
	return true
}

func relayTLSVerified(request *http.Request, config *tls.Config) bool {
	if config == nil {
		return true
	}
	return request.TLS != nil && len(request.TLS.VerifiedChains) != 0
}

func (coordinator *relayCoordinator) tryServeRemoteEvents(
	writer http.ResponseWriter,
	request *http.Request,
	id journal.StreamID,
	cursor uint64,
	verbose bool,
) bool {
	if coordinator == nil || !coordinator.enabled || coordinator.directory == nil {
		return false
	}
	owner, err := coordinator.directory.LocateOwner(request.Context(), id)
	if err != nil || owner.ReplicaID == coordinator.replicaID {
		return false
	}
	if err := coordinator.validateLocatedOwner(owner); err != nil {
		coordinator.logger.WarnContext(request.Context(), "reject unsafe stream owner relay",
			"stream_id", safeLogString(id.String()),
			"owner", safeLogString(owner.ReplicaID),
			"error", safeLogError(err))
		return false
	}
	endpoint, err := relayEventsURL(owner.RelayURL, id)
	if err != nil {
		coordinator.logger.WarnContext(request.Context(), "reject invalid stream owner relay",
			"stream_id", safeLogString(id.String()),
			"error", safeLogError(err))
		return false
	}
	relayRequest, err := http.NewRequestWithContext(request.Context(), http.MethodGet, endpoint, nil)
	if err != nil {
		return false
	}
	relayRequest.Header.Set("User-Agent", "")
	if cursor != 0 {
		relayRequest.Header.Set("Last-Event-ID", strconv.FormatUint(cursor, 10))
	}
	if verbose {
		relayRequest.Header.Set(headerVerbose, "1")
	}
	response, err := coordinator.client.Do(relayRequest)
	if err != nil {
		coordinator.logger.WarnContext(request.Context(), "connect stream owner relay",
			"stream_id", safeLogString(id.String()),
			"owner", safeLogString(owner.ReplicaID),
			"error", safeLogError(err))
		return false
	}
	defer func() {
		if closeErr := response.Body.Close(); closeErr != nil {
			coordinator.logger.DebugContext(request.Context(), "close owner relay response",
				"stream_id", safeLogString(id.String()),
				"error", safeLogError(closeErr))
		}
	}()
	if response.StatusCode != http.StatusOK {
		return coordinator.tryServeRelayCursorError(writer, response, id)
	}
	if !validRelayEventHeaders(response.Header, id) {
		return false
	}
	streamWriter := newStreamResponseWriter(writer, coordinator.writeTimeout)
	defer streamWriter.clearDeadline()
	copyRelayResponseHeaders(writer.Header(), response.Header)
	writer.WriteHeader(response.StatusCode)
	flusher, _ := writer.(http.Flusher)
	if flusher != nil {
		if err := streamWriter.Flush(); err != nil {
			return true
		}
	}
	if request.Method == http.MethodHead {
		return true
	}
	buffer := make([]byte, 32<<10)
	for {
		read, readErr := response.Body.Read(buffer)
		if read != 0 {
			if _, writeErr := streamWriter.Write(buffer[:read]); writeErr != nil {
				return true
			}
			if flusher != nil {
				if flushErr := streamWriter.Flush(); flushErr != nil {
					return true
				}
			}
		}
		if readErr != nil {
			if !errors.Is(readErr, io.EOF) && !errors.Is(request.Context().Err(), context.Canceled) {
				coordinator.logger.WarnContext(request.Context(), "read stream owner relay",
					"stream_id", safeLogString(id.String()),
					"owner", safeLogString(owner.ReplicaID),
					"error", safeLogError(readErr))
			}
			return true
		}
	}
}

func (coordinator *relayCoordinator) tryServeRelayCursorError(
	writer http.ResponseWriter,
	response *http.Response,
	id journal.StreamID,
) bool {
	if response.StatusCode != http.StatusConflict && response.StatusCode != http.StatusGone {
		return false
	}
	if !relayMediaType(response.Header, "application/json") {
		return false
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxRelayControlResponseBytes+1))
	if err != nil || len(body) > maxRelayControlResponseBytes ||
		!validRelayCursorError(response.StatusCode, body, id) {
		return false
	}
	copyRelayControlResponseHeaders(writer.Header(), response.Header)
	writer.WriteHeader(response.StatusCode)
	_, _ = writer.Write(body)
	return true
}

func validRelayEventHeaders(header http.Header, id journal.StreamID) bool {
	return relayHeaderExactly(header, headerStreamID, id.String()) &&
		relayHeaderExactly(header, headerDurability, durabilityDurable) &&
		relayMediaType(header, "text/event-stream")
}

func relayHeaderExactly(header http.Header, name, expected string) bool {
	values := header.Values(name)
	return len(values) == 1 && values[0] == expected
}

func relayMediaType(header http.Header, expected string) bool {
	values := header.Values("Content-Type")
	if len(values) != 1 {
		return false
	}
	mediaType, _, err := mime.ParseMediaType(values[0])
	return err == nil && strings.EqualFold(mediaType, expected)
}

func validRelayCursorError(status int, body []byte, id journal.StreamID) bool {
	var envelope streamErrorEnvelope
	if err := json.Unmarshal(body, &envelope); err != nil ||
		envelope.Error.Type != "streamweld_error" ||
		envelope.Error.StreamID != id.String() ||
		envelope.Error.Message == "" {
		return false
	}
	if status == http.StatusConflict {
		return envelope.Error.Code == "cursor_ahead" || envelope.Error.Code == "stream_closing"
	}
	switch envelope.Error.Code {
	case "stream_expired", "stream_not_found", "stream_offset_expired", "stream_not_resumable":
		return true
	default:
		return false
	}
}

func relayStopStatus(status int) bool {
	return status == http.StatusAccepted || status == http.StatusConflict || status == http.StatusGone
}

func validRelayStopBody(status int, body []byte, id journal.StreamID) bool {
	if status == http.StatusAccepted {
		var result stopResponse
		return json.Unmarshal(body, &result) == nil && result.StreamID == id && result.Outcome == "stopped"
	}
	var envelope streamErrorEnvelope
	if err := json.Unmarshal(body, &envelope); err != nil ||
		envelope.Error.Type != "streamweld_error" ||
		envelope.Error.StreamID != id.String() ||
		envelope.Error.Message == "" {
		return false
	}
	if status == http.StatusConflict {
		return envelope.Error.Code == "stream_already_terminal"
	}
	switch envelope.Error.Code {
	case "stream_expired", "stream_not_found", "stream_not_resumable":
		return true
	default:
		return false
	}
}

func copyRelayControlResponseHeaders(destination, source http.Header) {
	for _, name := range []string{"Cache-Control", "Content-Type"} {
		if value := source.Get(name); value != "" {
			destination.Set(name, value)
		}
	}
}

func (coordinator *relayCoordinator) validateLocatedOwner(owner journal.OwnerRecord) error {
	if err := owner.Validate(); err != nil {
		return err
	}
	parsed, _ := url.Parse(owner.RelayURL)
	if coordinator.insecure {
		if parsed.Scheme != "http" || !isLoopbackRelayHost(parsed.Hostname()) {
			return errors.New("insecure development owner relay must use HTTP on loopback")
		}
		return nil
	}
	if parsed.Scheme != "https" {
		return errors.New("production owner relay must use HTTPS")
	}
	return nil
}

func relayEventsURL(base string, id journal.StreamID) (string, error) {
	return relayOperationURL(base, id, "events")
}

func relayOperationURL(base string, id journal.StreamID, operation string) (string, error) {
	owner := journal.OwnerRecord{ReplicaID: "validation", RelayURL: base}
	if err := owner.Validate(); err != nil {
		return "", err
	}
	parsed, _ := url.Parse(base)
	parsed.Path = relayEventsPrefix + id.String() + "/" + operation
	return parsed.String(), nil
}

func copyRelayResponseHeaders(destination, source http.Header) {
	for _, name := range []string{
		"Cache-Control", "Content-Type", headerDurability, headerStreamID, "X-Accel-Buffering",
	} {
		if value := source.Get(name); value != "" {
			destination.Set(name, value)
		}
	}
}

func (coordinator *relayCoordinator) start(ctx context.Context) (<-chan error, error) {
	if coordinator == nil || !coordinator.enabled {
		return nil, nil
	}
	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(ctx, "tcp", coordinator.listen)
	if err != nil {
		return nil, fmt.Errorf("listen for owner relay on %s: %w", coordinator.listen, err)
	}
	if coordinator.tlsConfig != nil {
		listener = tls.NewListener(listener, coordinator.tlsConfig)
	}
	if err := coordinator.resolveDynamicAdvertiseURL(listener.Addr()); err != nil {
		_ = listener.Close()
		return nil, err
	}
	heartbeatContext, cancelHeartbeat := context.WithCancel(ctx)
	heartbeatDone := make(chan struct{})
	coordinator.mu.Lock()
	if coordinator.shuttingDown {
		coordinator.mu.Unlock()
		cancelHeartbeat()
		_ = listener.Close()
		return nil, errors.New("owner relay is shutting down")
	}
	coordinator.listener = listener
	coordinator.heartbeatCancel = cancelHeartbeat
	coordinator.heartbeatDone = heartbeatDone
	coordinator.mu.Unlock()
	if err := coordinator.heartbeatOnce(heartbeatContext); err != nil && heartbeatContext.Err() == nil {
		coordinator.logger.Warn("publish initial relay presence", "replica_id", coordinator.replicaID, "error", err)
	}
	go func() {
		defer close(heartbeatDone)
		coordinator.runHeartbeats(heartbeatContext)
	}()
	result := make(chan error, 1)
	go func() {
		err := coordinator.server.Serve(listener)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		result <- err
	}()
	return result, nil
}

func (coordinator *relayCoordinator) resolveDynamicAdvertiseURL(address net.Addr) error {
	parsed, err := url.Parse(coordinator.owner.RelayURL)
	if err != nil || parsed.Port() != "0" {
		return err
	}
	_, port, err := net.SplitHostPort(address.String())
	if err != nil {
		return fmt.Errorf("resolve dynamic relay port: %w", err)
	}
	parsed.Host = net.JoinHostPort(parsed.Hostname(), port)
	coordinator.owner.RelayURL = parsed.String()
	return nil
}

func (coordinator *relayCoordinator) runHeartbeats(ctx context.Context) {
	ticker := time.NewTicker(coordinator.heartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := coordinator.heartbeatOnce(ctx); err != nil && ctx.Err() == nil {
				coordinator.logger.Warn("refresh relay presence", "replica_id", coordinator.replicaID, "error", err)
			}
		case <-ctx.Done():
			return
		}
	}
}

func (coordinator *relayCoordinator) heartbeatOnce(parent context.Context) error {
	ctx, cancel := context.WithTimeout(parent, coordinator.heartbeat)
	defer cancel()
	return coordinator.directory.HeartbeatOwner(ctx, coordinator.owner, coordinator.ttl)
}

func (coordinator *relayCoordinator) shutdown(ctx context.Context) error {
	if coordinator == nil {
		return nil
	}
	if ctx == nil {
		return errors.New("relay shutdown context cannot be nil")
	}
	coordinator.mu.Lock()
	coordinator.shuttingDown = true
	cancelHeartbeat := coordinator.heartbeatCancel
	heartbeatDone := coordinator.heartbeatDone
	coordinator.mu.Unlock()
	if cancelHeartbeat != nil {
		cancelHeartbeat()
	}

	var serverErr error
	if coordinator.server != nil {
		serverErr = coordinator.server.Shutdown(ctx)
	}
	var heartbeatErr error
	if heartbeatDone != nil {
		select {
		case <-heartbeatDone:
		case <-ctx.Done():
			heartbeatErr = fmt.Errorf("wait for owner relay heartbeat shutdown: %w", ctx.Err())
		}
	}
	return errors.Join(serverErr, heartbeatErr)
}
