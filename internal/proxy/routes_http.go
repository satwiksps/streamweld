package proxy

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
)

const maxRouteUpdateBytes = 4 << 20

type routeBackendHTTPResponse struct {
	routeApplyResult
	Error *apiError `json:"error,omitempty"`
}

func (h *Handler) handleRouteBackends(writer http.ResponseWriter, request *http.Request) {
	if !requireMethod(writer, request, http.MethodPut) || !h.authorizeAdmin(writer, request) {
		return
	}
	if h.durable.routes == nil {
		writeAPIError(writer, http.StatusServiceUnavailable, "route_admin_unavailable", "route administration is unavailable")
		return
	}
	escaped := strings.TrimPrefix(request.URL.EscapedPath(), "/internal/routes/")
	if !strings.HasSuffix(escaped, "/backends") {
		writeAPIError(writer, http.StatusNotFound, "not_found", "the requested endpoint is not supported")
		return
	}
	rawRoute, err := url.PathUnescape(strings.TrimSuffix(escaped, "/backends"))
	if err != nil || validateRouteIdentity(rawRoute) != nil {
		writeAPIError(writer, http.StatusBadRequest, "invalid_route", "route must be a canonical escaped namespace/name")
		return
	}

	request.Body = http.MaxBytesReader(writer, request.Body, maxRouteUpdateBytes)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	var update routeBackendUpdate
	if err := decoder.Decode(&update); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeAPIError(writer, http.StatusRequestEntityTooLarge, "route_update_too_large", "route backend update exceeds the configured limit")
			return
		}
		writeAPIError(writer, http.StatusBadRequest, "invalid_route_update", "route backend update must be one valid JSON object")
		return
	}
	if err := requireJSONEOF(decoder); err != nil {
		writeAPIError(writer, http.StatusBadRequest, "invalid_route_update", "route backend update must contain one JSON object")
		return
	}

	result, applyErr := h.durable.routes.apply(rawRoute, update)
	if applyErr != nil {
		switch {
		case errors.Is(applyErr, errRouteGenerationStale):
			writeRouteBackendResponse(writer, http.StatusConflict, result,
				&apiError{Type: "streamweld_error", Code: "stale_route_generation", Message: "a newer route generation is already applied"})
		case errors.Is(applyErr, errRouteUIDConflict):
			writeRouteBackendResponse(writer, http.StatusConflict, result,
				&apiError{Type: "streamweld_error", Code: "route_uid_conflict", Message: "a different live route object already owns this name"})
		default:
			writeAPIError(writer, http.StatusBadRequest, "invalid_route_update", "route backend update was rejected")
		}
		return
	}
	writeRouteBackendResponse(writer, http.StatusOK, result, nil)
}

func requireJSONEOF(decoder *json.Decoder) error {
	var extra json.RawMessage
	err := decoder.Decode(&extra)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err == nil {
		return errors.New("unexpected trailing JSON value")
	}
	return err
}

func writeRouteBackendResponse(
	writer http.ResponseWriter,
	status int,
	result routeApplyResult,
	apiErr *apiError,
) {
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.Header().Set("Cache-Control", "no-store")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(routeBackendHTTPResponse{
		routeApplyResult: result,
		Error:            apiErr,
	})
}
