package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	defaultDrainReadHeaderTimeout = 5 * time.Second
	defaultDrainShutdownTimeout   = 3 * time.Second
	maxDrainRequestBytes          = 1
)

// OperatorDrainServer exposes the unauthenticated in-cluster preStop target.
// Its Service must be isolated with NetworkPolicy; the handler itself accepts
// only an exact namespaced Pod drain path and never exposes proxy credentials.
type OperatorDrainServer struct {
	Address           string
	Fanout            *PodDrainFanout
	ReadHeaderTimeout time.Duration
	ShutdownTimeout   time.Duration

	started atomic.Bool
}

// NeedLeaderElection keeps the drain endpoint available on every operator
// replica, including followers during leader transitions.
func (*OperatorDrainServer) NeedLeaderElection() bool { return false }

// Start implements manager.Runnable with bounded HTTP and shutdown behavior.
func (server *OperatorDrainServer) Start(ctx context.Context) error {
	if server == nil || server.Fanout == nil {
		return errors.New("operator drain server is not configured")
	}
	if strings.TrimSpace(server.Address) == "" {
		return errors.New("operator drain server address is required")
	}
	readHeaderTimeout := server.ReadHeaderTimeout
	if readHeaderTimeout <= 0 {
		readHeaderTimeout = defaultDrainReadHeaderTimeout
	}
	shutdownTimeout := server.ShutdownTimeout
	if shutdownTimeout <= 0 {
		shutdownTimeout = defaultDrainShutdownTimeout
	}
	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(ctx, "tcp", server.Address)
	if err != nil {
		return err
	}
	httpServer := &http.Server{
		Handler: server, ReadHeaderTimeout: readHeaderTimeout,
		WriteTimeout: server.Fanout.timeout + 3*time.Second,
		IdleTimeout:  30 * time.Second, MaxHeaderBytes: 16 << 10,
	}
	server.started.Store(true)
	serveContext, cancelServe := context.WithCancel(ctx)
	defer cancelServe()
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		<-serveContext.Done()
		shutdownContext, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		_ = httpServer.Shutdown(shutdownContext)
	}()
	serveErr := httpServer.Serve(listener)
	server.started.Store(false)
	cancelServe()
	<-shutdownDone
	if errors.Is(serveErr, http.ErrServerClosed) {
		return nil
	}
	_ = listener.Close()
	return serveErr
}

// Ready is a controller-runtime health checker for the drain listener.
func (server *OperatorDrainServer) Ready(*http.Request) error {
	if server != nil && server.started.Load() {
		return nil
	}
	return errors.New("operator drain listener has not started")
}

// ServeHTTP handles POST /internal/backends/by-pod/{namespace}/{name}/drain.
func (server *OperatorDrainServer) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.Header().Set("Cache-Control", "no-store")
	if request.Method != http.MethodPost {
		writer.Header().Set("Allow", http.MethodPost)
		writeDrainError(writer, http.StatusMethodNotAllowed, "method_not_allowed", "only POST is supported")
		return
	}
	request.Body = http.MaxBytesReader(writer, request.Body, maxDrainRequestBytes)
	if body, err := io.ReadAll(request.Body); err != nil || len(body) != 0 {
		writeDrainError(writer, http.StatusBadRequest, "invalid_body", "drain request body must be empty")
		return
	}
	namespace, name, ok := parseOperatorDrainPath(request.URL.EscapedPath())
	if !ok {
		writeDrainError(writer, http.StatusNotFound, "not_found", "the requested drain endpoint is not supported")
		return
	}
	result, err := server.Fanout.DrainPod(request.Context(), namespace, name)
	if err != nil {
		status := http.StatusServiceUnavailable
		if result.InFlight > 0 {
			status = http.StatusGatewayTimeout
		}
		writer.WriteHeader(status)
		_ = json.NewEncoder(writer).Encode(result)
		return
	}
	writer.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(writer).Encode(result)
}

func parseOperatorDrainPath(escapedPath string) (string, string, bool) {
	const prefix = "/internal/backends/by-pod/"
	const suffix = "/drain"
	if !strings.HasPrefix(escapedPath, prefix) || !strings.HasSuffix(escapedPath, suffix) {
		return "", "", false
	}
	identity := strings.Split(strings.TrimSuffix(strings.TrimPrefix(escapedPath, prefix), suffix), "/")
	if len(identity) != 2 {
		return "", "", false
	}
	namespace, namespaceErr := url.PathUnescape(identity[0])
	name, nameErr := url.PathUnescape(identity[1])
	if namespaceErr != nil || nameErr != nil || len(validation.IsDNS1123Label(namespace)) != 0 ||
		len(validation.IsDNS1123Subdomain(name)) != 0 {
		return "", "", false
	}
	return namespace, name, true
}

func writeDrainError(writer http.ResponseWriter, status int, code, message string) {
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}{Error: struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}{Code: code, Message: message}})
}
