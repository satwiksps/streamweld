package proxy

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/satwiksps/streamweld/internal/backend"
)

const defaultDrainTimeout = 10 * time.Second

type drainResponse struct {
	Backend  string `json:"backend"`
	State    string `json:"state"`
	InFlight int    `json:"in_flight"`
}

type podDrainResponse struct {
	PodNamespace string   `json:"pod_namespace"`
	PodName      string   `json:"pod_name"`
	Backends     []string `json:"backends"`
	State        string   `json:"state"`
	InFlight     int      `json:"in_flight"`
}

func (h *Handler) handleBackendDrainRoute(writer http.ResponseWriter, request *http.Request) {
	if !requireMethod(writer, request, http.MethodPost) || !h.authorizeAdmin(writer, request) {
		return
	}
	if strings.HasPrefix(request.URL.EscapedPath(), "/internal/backends/by-pod/") {
		h.handlePodDrain(writer, request)
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

	timeout, err := parseDrainTimeout(request)
	if err != nil {
		writeAPIError(writer, http.StatusBadRequest, "invalid_timeout", err.Error())
		return
	}

	drain, err := h.durable.backends.BeginRetainedDrain(id)
	if err != nil {
		if errors.Is(err, backend.ErrNotFound) {
			writeAPIError(writer, http.StatusNotFound, "backend_not_found", "backend is not registered")
			return
		}
		writeAPIError(writer, http.StatusBadRequest, "invalid_backend", "backend could not be drained")
		return
	}
	defer drain.Close()
	snapshot := drain.Snapshot()
	h.durable.triggerBindings("drain", id, snapshot.Bindings)

	waitContext, cancel := context.WithTimeout(request.Context(), timeout)
	state, waitErr := drain.Wait(waitContext)
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

func (h *Handler) handlePodDrain(writer http.ResponseWriter, request *http.Request) {
	escaped := strings.TrimPrefix(request.URL.EscapedPath(), "/internal/backends/by-pod/")
	if !strings.HasSuffix(escaped, "/drain") {
		writeAPIError(writer, http.StatusNotFound, "not_found", "the requested endpoint is not supported")
		return
	}
	identity := strings.Split(strings.TrimSuffix(escaped, "/drain"), "/")
	if len(identity) != 2 {
		writeAPIError(writer, http.StatusBadRequest, "invalid_pod", "pod identity must contain namespace and name")
		return
	}
	namespace, namespaceErr := url.PathUnescape(identity[0])
	name, nameErr := url.PathUnescape(identity[1])
	if namespaceErr != nil || nameErr != nil || !routeNamespacePattern.MatchString(namespace) ||
		!routeNamePattern.MatchString(name) {
		writeAPIError(writer, http.StatusBadRequest, "invalid_pod", "pod identity is not canonical")
		return
	}
	timeout, err := parseDrainTimeout(request)
	if err != nil {
		writeAPIError(writer, http.StatusBadRequest, "invalid_timeout", err.Error())
		return
	}

	drains, err := h.durable.backends.BeginRetainedPodDrain(namespace, name)
	if err != nil {
		writeAPIError(writer, http.StatusServiceUnavailable, "drain_failed", "pod backends could not be marked draining")
		return
	}
	defer func() {
		for _, drain := range drains {
			drain.Close()
		}
	}()
	if len(drains) == 0 {
		writeAPIError(writer, http.StatusNotFound, "backend_not_found", "pod has no registered backends")
		return
	}
	for _, drain := range drains {
		snapshot := drain.Snapshot()
		h.durable.triggerBindings("drain", snapshot.Backend.ID, snapshot.Bindings)
	}

	waitContext, cancel := context.WithTimeout(request.Context(), timeout)
	defer cancel()
	status := http.StatusOK
	inFlight := 0
	backendNames := make([]string, 0, len(drains))
	for _, drain := range drains {
		snapshot := drain.Snapshot()
		state, waitErr := drain.Wait(waitContext)
		inFlight += state.InFlight
		backendNames = append(backendNames, snapshot.Backend.ID.String())
		if waitErr == nil {
			continue
		}
		if errors.Is(waitErr, context.DeadlineExceeded) || errors.Is(waitErr, context.Canceled) {
			status = http.StatusGatewayTimeout
			continue
		}
		writeAPIError(writer, http.StatusServiceUnavailable, "drain_failed", "pod backend drain could not be observed")
		return
	}
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.Header().Set("Cache-Control", "no-store")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(podDrainResponse{
		PodNamespace: namespace,
		PodName:      name,
		Backends:     backendNames,
		State:        "draining",
		InFlight:     inFlight,
	})
}

func parseDrainTimeout(request *http.Request) (time.Duration, error) {
	timeout := defaultDrainTimeout
	if values, ok := request.URL.Query()["timeout"]; ok {
		if len(values) != 1 {
			return 0, errors.New("timeout must contain one duration")
		}
		parsed, err := time.ParseDuration(values[0])
		if err != nil || parsed <= 0 {
			return 0, errors.New("timeout must be a positive duration")
		}
		timeout = parsed
	}
	return timeout, nil
}
