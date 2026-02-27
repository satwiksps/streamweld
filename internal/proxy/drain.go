package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/streamweld/streamweld/internal/backend"
)

const defaultDrainTimeout = 10 * time.Second

type drainResponse struct {
	Backend  string `json:"backend"`
	State    string `json:"state"`
	InFlight int    `json:"in_flight"`
}

func (h *Handler) handleBackendDrainRoute(writer http.ResponseWriter, request *http.Request) {
	if !requireMethod(writer, request, http.MethodPost) {
		return
	}
	escaped := strings.TrimPrefix(request.URL.EscapedPath(), "/internal/backends/")
	if !strings.HasSuffix(escaped, "/drain") {
		writeAPIError(writer, http.StatusNotFound, "not_found", "the requested endpoint is not supported")
		return
	}
	escaped = strings.TrimSuffix(escaped, "/drain")
	rawID, err := url.PathUnescape(escaped)
	if err != nil || rawID == "" || strings.Contains(rawID, "/") {
		writeAPIError(writer, http.StatusBadRequest, "invalid_backend", "backend address is not canonical")
		return
	}
	id := backend.ID(rawID)
	if err := id.Validate(); err != nil {
		writeAPIError(writer, http.StatusBadRequest, "invalid_backend", "backend address is not canonical")
		return
	}

	timeout := defaultDrainTimeout
	if values, ok := request.URL.Query()["timeout"]; ok {
		if len(values) != 1 {
			writeAPIError(writer, http.StatusBadRequest, "invalid_timeout", "timeout must contain one duration")
			return
		}
		timeout, err = time.ParseDuration(values[0])
		if err != nil || timeout <= 0 {
			writeAPIError(writer, http.StatusBadRequest, "invalid_timeout", "timeout must be a positive duration")
			return
		}
	}

	snapshot, err := h.durable.backends.MarkDraining(id)
	if err != nil {
		if errors.Is(err, backend.ErrNotFound) {
			writeAPIError(writer, http.StatusNotFound, "backend_not_found", "backend is not registered")
			return
		}
		writeAPIError(writer, http.StatusBadRequest, "invalid_backend", "backend could not be drained")
		return
	}
	h.durable.triggerBindings("drain", id, snapshot.Bindings)

	waitContext, cancel := context.WithTimeout(request.Context(), timeout)
	state, waitErr := h.durable.backends.WaitDrained(waitContext, id)
	cancel()
	status := http.StatusOK
	if waitErr != nil {
		if !errors.Is(waitErr, context.DeadlineExceeded) && !errors.Is(waitErr, context.Canceled) {
			writeAPIError(writer, http.StatusServiceUnavailable, "drain_failed", "backend drain could not be observed")
			return
		}
		status = http.StatusGatewayTimeout
	}
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.Header().Set("Cache-Control", "no-store")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(drainResponse{
		Backend: id.String(), State: "draining", InFlight: state.InFlight,
	})
}
