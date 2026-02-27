package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
)

const immediateFlushInterval = -1

// Handler owns the public and administrative HTTP routing surface.
type Handler struct {
	readiness *readinessGate
	durable   *durableService
}

func newHandler(readiness *readinessGate, durable *durableService) *Handler {
	return &Handler{readiness: readiness, durable: durable}
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
				"path", request.URL.Path,
			)
			return
		}
		logger.ErrorContext(request.Context(), "upstream request failed",
			"method", request.Method,
			"path", request.URL.Path,
			"error", err,
		)
		writeAPIError(writer, http.StatusBadGateway, "bad_gateway", "the upstream backend could not complete the request")
	}

	return proxy
}

func (h *Handler) proxyTo(writer http.ResponseWriter, request *http.Request, target *url.URL) {
	newReverseProxy(target, h.durable.transport, h.durable.logger).ServeHTTP(writer, request)
}

func (h *Handler) proxyFromPool(writer http.ResponseWriter, request *http.Request) {
	lease, err := h.durable.backends.Acquire("passthrough:" + request.Method + ":" + request.URL.Path)
	if err != nil {
		writeAPIError(writer, http.StatusServiceUnavailable, "no_healthy_backend", "no healthy backend can serve the request")
		return
	}
	defer lease.Release()
	h.proxyTo(writer, request, lease.Backend().URL)
}

// ServeHTTP dispatches OpenAI-compatible data-plane and Streamweld endpoints.
func (h *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
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
	case "/v1/chat/completions", "/v1/completions":
		if requireMethod(writer, request, http.MethodPost) {
			h.handleCompletion(writer, request)
		}
	case "/v1/models":
		if requireMethod(writer, request, http.MethodGet) {
			h.proxyFromPool(writer, request)
		}
	default:
		if strings.HasPrefix(request.URL.Path, "/v1/streams/") {
			h.handleStreamRoute(writer, request)
			return
		}
		writeAPIError(writer, http.StatusNotFound, "not_found", "the requested endpoint is not supported")
	}
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
	w.logger.Error("reverse proxy transport error", "detail", strings.TrimSpace(string(data)))
	return len(data), nil
}
