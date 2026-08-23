package proxy

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"

	"github.com/satwiksps/streamweld/internal/backend"
	"github.com/satwiksps/streamweld/internal/telemetry"
)

const immediateFlushInterval = -1

// Handler owns the public and administrative HTTP routing surface.
type Handler struct {
	readiness  *readinessGate
	durable    *durableService
	adminToken string
}

func newHandler(readiness *readinessGate, durable *durableService, adminToken string) *Handler {
	return &Handler{readiness: readiness, durable: durable, adminToken: adminToken}
}

func (h *Handler) authorizeAdmin(writer http.ResponseWriter, request *http.Request) bool {
	if h.adminToken == "" {
		return true
	}
	values := request.Header.Values("Authorization")
	const prefix = "Bearer "
	if len(values) != 1 || !strings.HasPrefix(values[0], prefix) ||
		subtle.ConstantTimeCompare([]byte(strings.TrimPrefix(values[0], prefix)), []byte(h.adminToken)) != 1 {
		writer.Header().Set("WWW-Authenticate", "Bearer")
		writeAPIError(writer, http.StatusUnauthorized, "admin_unauthorized", "administration requires a valid bearer token")
		return false
	}
	return true
}

func newReverseProxy(target *url.URL, transport http.RoundTripper, logger *slog.Logger) *httputil.ReverseProxy {
	proxy := &httputil.ReverseProxy{
		Rewrite: func(request *httputil.ProxyRequest) {
			request.SetURL(target)
			request.SetXForwarded()
		},
		Transport:     transport,
		FlushInterval: immediateFlushInterval,
		ErrorLog:      log.New(&slogWriter{logger: logger}, "", 0),
	}
	proxy.ErrorHandler = func(writer http.ResponseWriter, request *http.Request, err error) {
		if errors.Is(err, context.Canceled) || errors.Is(request.Context().Err(), context.Canceled) {
			logger.DebugContext(request.Context(), "upstream request canceled",
				"method", request.Method,
				"path", safeLogString(request.URL.Path),
			)
			return
		}
		logger.ErrorContext(request.Context(), "upstream request failed",
			"method", request.Method,
			"path", safeLogString(request.URL.Path),
			"error", safeLogError(err),
		)
		writeAPIError(writer, http.StatusBadGateway, "bad_gateway", "the upstream backend could not complete the request")
	}

	return proxy
}

func (h *Handler) proxyTo(writer http.ResponseWriter, request *http.Request, target *url.URL) {
	h.proxyToWithLogger(writer, request, target, h.durable.logger)
}

func (h *Handler) proxyToWithLogger(
	writer http.ResponseWriter,
	request *http.Request,
	target *url.URL,
	logger *slog.Logger,
) {
	newReverseProxy(target, h.durable.transport, logger).ServeHTTP(writer, request)
}

func (h *Handler) proxyFromPool(writer http.ResponseWriter, request *http.Request, model string) {
	owner := "passthrough:" + request.Method + ":" + request.URL.Path
	var lease *backend.Lease
	var err error
	if model == "" {
		if h.durable.routes != nil {
			lease, err = h.durable.routes.acquireModel(owner, "")
		} else {
			lease, err = h.durable.backends.Acquire(owner)
		}
	} else {
		lease, err = h.durable.acquireBackend(owner, model)
	}
	if err != nil {
		writeAPIError(writer, http.StatusServiceUnavailable, "no_healthy_backend", "no healthy backend can serve the request")
		return
	}
	defer lease.Release()
	h.proxyTo(writer, request, lease.Backend().URL)
}

// ServeHTTP dispatches OpenAI-compatible data-plane and Streamweld endpoints.
func (h *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if strings.HasPrefix(request.URL.Path, "/internal/routes/") {
		h.handleRouteBackends(writer, request)
		return
	}
	if strings.HasPrefix(request.URL.Path, "/internal/backends/") {
		h.handleBackendDrainRoute(writer, request)
		return
	}
	switch request.URL.Path {
	case "/healthz":
		if requireMethod(writer, request, http.MethodGet) {
			writeStatus(writer, http.StatusOK, "ok")
		}
	case "/readyz":
		if requireMethod(writer, request, http.MethodGet) {
			if err := h.readiness.Check(request.Context()); err != nil {
				writeStatus(writer, http.StatusServiceUnavailable, "not_ready")
				return
			}
			writeStatus(writer, http.StatusOK, "ready")
		}
	case "/metrics":
		if requireMethod(writer, request, http.MethodGet) {
			h.durable.refreshBackendMetrics()
			h.durable.telemetry.Handler().ServeHTTP(writer, request)
		}
	case "/v1/chat/completions", "/v1/completions":
		if requireMethod(writer, request, http.MethodPost) {
			h.handleCompletion(writer, request)
		}
	case "/v1/models":
		if requireMethod(writer, request, http.MethodGet) {
			h.proxyFromPool(writer, request, "")
		}
	default:
		if strings.HasPrefix(request.URL.Path, "/v1/streams/") {
			h.handleStreamRoute(writer, request)
			return
		}
		writeAPIError(writer, http.StatusNotFound, "not_found", "the requested endpoint is not supported")
	}
}

func (s *durableService) refreshBackendMetrics() {
	type sampleKey struct {
		labels telemetry.Labels
		state  string
	}
	counts := make(map[sampleKey]float64)
	for _, current := range s.backends.List() {
		labels := telemetry.Labels{
			Route: s.routeForModel(current.Model),
			Model: current.Model,
		}
		for _, state := range []string{"healthy", "draining", "quarantined"} {
			counts[sampleKey{labels: labels, state: state}] += 0
		}
		state := ""
		switch {
		case current.Draining:
			state = "draining"
		case current.Quarantined:
			state = "quarantined"
		case current.Health == backend.HealthHealthy:
			state = "healthy"
		}
		if state != "" {
			counts[sampleKey{labels: labels, state: state}]++
		}
	}
	samples := make([]telemetry.BackendCount, 0, len(counts))
	for key, count := range counts {
		samples = append(samples, telemetry.BackendCount{
			Labels: key.labels,
			State:  key.state,
			Count:  count,
		})
	}
	s.telemetry.ReplaceBackends(samples)
}

func requireMethod(writer http.ResponseWriter, request *http.Request, allowed string) bool {
	if request.Method == allowed {
		return true
	}
	writer.Header().Set("Allow", allowed)
	writeAPIError(writer, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed for this endpoint")
	return false
}

type errorEnvelope struct {
	Error apiError `json:"error"`
}

type apiError struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Code    string `json:"code"`
}

func writeAPIError(writer http.ResponseWriter, status int, code, message string) {
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.Header().Set("Cache-Control", "no-store")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(errorEnvelope{Error: apiError{
		Message: message,
		Type:    "invalid_request_error",
		Code:    code,
	}})
}

func writeStatus(writer http.ResponseWriter, statusCode int, status string) {
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.Header().Set("Cache-Control", "no-store")
	writer.WriteHeader(statusCode)
	_, _ = io.WriteString(writer, `{"status":"`+status+`"}`+"\n")
}

type slogWriter struct {
	logger *slog.Logger
}

func (w *slogWriter) Write(data []byte) (int, error) {
	w.logger.Error("reverse proxy transport error", "detail", safeLogString(strings.TrimSpace(string(data))))
	return len(data), nil
}
