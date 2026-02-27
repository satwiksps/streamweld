package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/streamweld/streamweld/internal/journal"
)

const (
	headerDurability   = "X-Streamweld-Durability"
	headerIdempotency  = "X-Streamweld-Idempotency-Key"
	headerOrphanPolicy = "X-Streamweld-Orphan-Policy"
	headerStreamID     = "X-Streamweld-Stream-Id"
	headerVerbose      = "X-Streamweld-Verbose"
	durabilityDurable  = "durable"
	durabilityDegraded = "degraded"
)

func (h *Handler) handleCompletion(writer http.ResponseWriter, request *http.Request) {
	original, err := readRequestBody(request.Body, h.durable.config.MaxRequestBytes)
	if err != nil {
		if errors.Is(err, errRequestBodyTooLarge) {
			writeAPIError(writer, http.StatusRequestEntityTooLarge, "request_too_large", "request body exceeds the configured limit")
			return
		}
		writeAPIError(writer, http.StatusBadRequest, "invalid_request_body", "request body could not be read")
		return
	}
	normalized, err := normalizeRequest(original)
	if err != nil {
		writeAPIError(writer, http.StatusBadRequest, "invalid_request_body", err.Error())
		return
	}
	if !normalized.Stream {
		restoreRequestBody(request, original)
		stripStreamweldHeaders(request.Header)
		h.proxyFromPool(writer, request)
		return
	}

	verbose, err := parseVerboseHeader(request.Header)
	if err != nil {
		writeAPIError(writer, http.StatusBadRequest, "invalid_verbose", err.Error())
		return
	}
	policy, err := h.effectiveOrphanPolicy(request.Header)
	if err != nil {
		writeAPIError(writer, http.StatusBadRequest, "invalid_orphan_policy", err.Error())
		return
	}
	idempotencyKey, err := parseIdempotencyHeader(request.Header)
	if err != nil {
		writeAPIError(writer, http.StatusBadRequest, "invalid_idempotency_key", err.Error())
		return
	}

	resolution, err := h.durable.resolve(request, normalized, policy, idempotencyKey)
	if err != nil {
		writeAPIError(writer, http.StatusServiceUnavailable, "durability_unavailable", "durable stream creation is temporarily unavailable")
		return
	}
	if resolution.degraded {
		degradedBackend := resolution.degradedLease.Backend()
		defer resolution.degradedLease.Release()
		writer.Header().Set(headerDurability, durabilityDegraded)
		restoreRequestBody(request, normalized.Body)
		stripStreamweldHeaders(request.Header)
		h.proxyTo(writer, request, degradedBackend.URL)
		return
	}

	id := resolution.runtimeID()
	writer.Header().Set(headerStreamID, id.String())
	writer.Header().Set(headerDurability, durabilityDurable)
	if resolution.existing {
		h.serveJournal(writer, request, id, 0, verbose)
		return
	}

	resolution.runtime.attachReader()
	var detachOnce sync.Once
	detach := func() { detachOnce.Do(resolution.runtime.detachReader) }
	stopDisconnectWatch := context.AfterFunc(request.Context(), detach)
	rejection, startErr := h.durable.start(resolution.runtime, request.URL.RawQuery)
	if !stopDisconnectWatch() || request.Context().Err() != nil {
		detach()
		return
	}
	if startErr != nil {
		detach()
		writeAPIError(writer, http.StatusBadGateway, "upstream_error", "the upstream stream could not be started")
		return
	}
	if rejection != nil {
		detach()
		writeUpstreamRejection(writer, rejection)
		return
	}
	defer detach()
	h.serveInitialJournal(writer, request, resolution.runtime, verbose)
}

func (h *Handler) handleStreamRoute(writer http.ResponseWriter, request *http.Request) {
	rest := strings.TrimPrefix(request.URL.Path, "/v1/streams/")
	var rawID, operation string
	switch {
	case strings.HasSuffix(rest, "/events"):
		rawID = strings.TrimSuffix(rest, "/events")
		operation = "events"
	case strings.HasSuffix(rest, "/stop"):
		rawID = strings.TrimSuffix(rest, "/stop")
		operation = "stop"
	default:
		rawID = rest
		operation = "state"
	}
	if rawID == "" || strings.Contains(rawID, "/") {
		writeAPIError(writer, http.StatusNotFound, "not_found", "the requested endpoint is not supported")
		return
	}
	id, err := journal.ParseStreamID(rawID)
	if err != nil {
		writeStreamError(writer, http.StatusBadRequest, "invalid_stream_id", "stream ID is not a canonical lowercase ULID", rawID)
		return
	}

	switch operation {
	case "events":
		if !requireMethod(writer, request, http.MethodGet) {
			return
		}
		cursor, cursorErr := parseResumeCursor(request.Header)
		if cursorErr != nil {
			writeStreamError(writer, http.StatusBadRequest, "invalid_resume_cursor", cursorErr.Error(), id.String())
			return
		}
		verbose, verboseErr := parseVerboseHeader(request.Header)
		if verboseErr != nil {
			writeStreamError(writer, http.StatusBadRequest, "invalid_verbose", verboseErr.Error(), id.String())
			return
		}
		h.serveJournal(writer, request, id, cursor, verbose)
	case "state":
		if requireMethod(writer, request, http.MethodGet) {
			h.handleStreamState(writer, request, id)
		}
	case "stop":
		if requireMethod(writer, request, http.MethodPost) {
			h.handleStreamStop(writer, request, id)
		}
	}
}

func (h *Handler) serveJournal(
	writer http.ResponseWriter,
	request *http.Request,
	id journal.StreamID,
	cursor uint64,
	verbose bool,
) {
	flusher, ok := writer.(http.Flusher)
	if !ok {
		writeAPIError(writer, http.StatusInternalServerError, "streaming_unsupported", "HTTP response streaming is unavailable")
		return
	}
	state, err := h.durable.journal.State(request.Context(), id)
	if err != nil {
		writeJournalHTTPError(writer, err, id)
		return
	}
	events, cancel, err := h.durable.journal.Tail(request.Context(), id, cursor)
	if err != nil {
		writeJournalHTTPError(writer, err, id)
		return
	}
	defer cancel()

	if runtime, exists := h.durable.loadRuntime(id); exists {
		runtime.attachReader()
		defer runtime.detachReader()
	}

	writer.Header().Set(headerStreamID, id.String())
	writer.Header().Set(headerDurability, durabilityDurable)
	writer.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	writer.Header().Set("Cache-Control", "no-cache, no-store")
	writer.Header().Set("X-Accel-Buffering", "no")
	writer.WriteHeader(http.StatusOK)
	if request.Method == http.MethodGet {
		flusher.Flush()
	}

	sseWriter := NewJournalSSEWriter(writer, verbose)
	wroteTerminal := false
	for entry := range events {
		if entry.Err != nil {
			if errors.Is(entry.Err, journal.ErrReaderLagged) {
				_ = sseWriter.WriteReaderLagError()
				flusher.Flush()
			}
			return
		}
		result, writeErr := sseWriter.WriteEntry(entry)
		if writeErr != nil {
			h.durable.logger.ErrorContext(request.Context(), "write journal SSE entry",
				"stream_id", id,
				"seq", entry.Seq,
				"error", writeErr,
			)
			return
		}
		// On an initial POST, keep the small open control frame in the same
		// write batch as the first token. Besides reducing syscalls, this avoids
		// delayed-ACK latency between two tiny chunks while preserving open as
		// the first SSE event.
		if result.Visible && (request.Method != http.MethodPost || entry.Kind != journal.KindOpen) {
			flusher.Flush()
		}
		if result.Terminal {
			wroteTerminal = true
			break
		}
	}
	if !wroteTerminal && state.Terminal != nil && state.Terminal.Kind == journal.KindDone && cursor == state.Terminal.Seq {
		_ = sseWriter.WriteDoneSentinel()
		flusher.Flush()
	}
}

func (h *Handler) serveInitialJournal(
	writer http.ResponseWriter,
	request *http.Request,
	runtime *streamRuntime,
	verbose bool,
) {
	flusher, ok := writer.(http.Flusher)
	if !ok {
		writeAPIError(writer, http.StatusInternalServerError, "streaming_unsupported", "HTTP response streaming is unavailable")
		return
	}
	writer.Header().Set(headerStreamID, runtime.id.String())
	writer.Header().Set(headerDurability, durabilityDurable)
	writer.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	writer.Header().Set("Cache-Control", "no-cache, no-store")
	writer.Header().Set("X-Accel-Buffering", "no")
	writer.WriteHeader(http.StatusOK)

	sseWriter := NewJournalSSEWriter(writer, verbose)
	if _, err := sseWriter.WriteEntry(runtime.openEntry); err != nil {
		h.durable.logger.ErrorContext(request.Context(), "write initial open event", "stream_id", runtime.id, "error", err)
		return
	}

	var first journal.Entry
	select {
	case first = <-runtime.firstEntry:
	case <-request.Context().Done():
		return
	}
	result, err := sseWriter.WriteEntry(first)
	if err != nil {
		h.durable.logger.ErrorContext(request.Context(), "write initial journal event",
			"stream_id", runtime.id, "seq", first.Seq, "error", err)
		return
	}
	if result.Visible {
		flusher.Flush()
	}
	if result.Terminal {
		return
	}

	events, cancel, err := h.durable.journal.Tail(request.Context(), runtime.id, first.Seq)
	if err != nil {
		h.durable.logger.ErrorContext(request.Context(), "tail initial durable stream", "stream_id", runtime.id, "error", err)
		return
	}
	defer cancel()
	for entry := range events {
		if entry.Err != nil {
			if errors.Is(entry.Err, journal.ErrReaderLagged) {
				_ = sseWriter.WriteReaderLagError()
				flusher.Flush()
			}
			return
		}
		mapped, writeErr := sseWriter.WriteEntry(entry)
		if writeErr != nil {
			h.durable.logger.ErrorContext(request.Context(), "write initial journal SSE entry",
				"stream_id", runtime.id, "seq", entry.Seq, "error", writeErr)
			return
		}
		if mapped.Visible {
			flusher.Flush()
		}
		if mapped.Terminal {
			return
		}
	}
}

func (h *Handler) handleStreamState(writer http.ResponseWriter, request *http.Request, id journal.StreamID) {
	state, err := h.durable.journal.State(request.Context(), id)
	if err != nil {
		writeJournalHTTPError(writer, err, id)
		return
	}
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.Header().Set("Cache-Control", "no-store")
	if err := json.NewEncoder(writer).Encode(state); err != nil {
		h.durable.logger.WarnContext(request.Context(), "write stream state", "stream_id", id, "error", err)
	}
}

func (h *Handler) handleStreamStop(writer http.ResponseWriter, request *http.Request, id journal.StreamID) {
	runtime, exists := h.durable.loadRuntime(id)
	if !exists {
		state, err := h.durable.journal.State(request.Context(), id)
		if err != nil {
			writeJournalHTTPError(writer, err, id)
			return
		}
		if state.Status == journal.StatusDone || state.Status == journal.StatusError {
			writeStreamError(writer, http.StatusConflict, "stream_already_terminal", "stream already completed", id.String())
			return
		}
		if state.Status == journal.StatusStopped {
			writeStreamError(writer, http.StatusServiceUnavailable, "stream_owner_unavailable", "stopped stream result is unavailable on this proxy", id.String())
			return
		}
		writeStreamError(writer, http.StatusServiceUnavailable, "stream_owner_unavailable", "active stream is owned by another proxy", id.String())
		return
	}
	result, err := runtime.stop()
	if err != nil {
		if errors.Is(err, journal.ErrTerminalState) {
			writeStreamError(writer, http.StatusConflict, "stream_already_terminal", "stream already completed", id.String())
			return
		}
		writeStreamError(writer, http.StatusServiceUnavailable, "stop_failed", "stream could not be stopped durably", id.String())
		return
	}
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.Header().Set("Cache-Control", "no-store")
	writer.WriteHeader(http.StatusAccepted)
	if err := json.NewEncoder(writer).Encode(result); err != nil {
		h.durable.logger.WarnContext(request.Context(), "write stop response", "stream_id", id, "error", err)
	}
}

var errRequestBodyTooLarge = errors.New("request body too large")

func readRequestBody(body io.Reader, maxBytes int64) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(body, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxBytes {
		return nil, errRequestBodyTooLarge
	}
	return data, nil
}

func restoreRequestBody(request *http.Request, body []byte) {
	request.Body = io.NopCloser(bytes.NewReader(body))
	request.ContentLength = int64(len(body))
	request.Header.Set("Content-Length", strconv.Itoa(len(body)))
}

func stripStreamweldHeaders(header http.Header) {
	header.Del(headerIdempotency)
	header.Del(headerOrphanPolicy)
	header.Del(headerVerbose)
	header.Del("Last-Event-ID")
}

func parseVerboseHeader(header http.Header) (bool, error) {
	values, present := headerValues(header, headerVerbose)
	if !present {
		return false, nil
	}
	if len(values) != 1 || values[0] != "1" {
		return false, errors.New("X-Streamweld-Verbose must be exactly 1 when present")
	}
	return true, nil
}

func parseIdempotencyHeader(header http.Header) (string, error) {
	values, present := headerValues(header, headerIdempotency)
	if !present {
		return "", nil
	}
	if len(values) != 1 || values[0] == "" {
		return "", errors.New("X-Streamweld-Idempotency-Key must contain one non-empty value")
	}
	return values[0], nil
}

func (h *Handler) effectiveOrphanPolicy(header http.Header) (OrphanPolicy, error) {
	values, present := headerValues(header, headerOrphanPolicy)
	if !present {
		return h.durable.config.OrphanPolicy, nil
	}
	if len(values) != 1 {
		return "", errors.New("X-Streamweld-Orphan-Policy must contain one value")
	}
	policy := OrphanPolicy(values[0])
	if !policy.valid() {
		return "", fmt.Errorf("unsupported orphan policy %q", values[0])
	}
	return policy, nil
}

func parseResumeCursor(header http.Header) (uint64, error) {
	values, present := headerValues(header, "Last-Event-ID")
	if !present {
		return 0, nil
	}
	if len(values) != 1 || values[0] == "" {
		return 0, errors.New("Last-Event-ID must contain one canonical unsigned decimal integer")
	}
	value := values[0]
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil || strconv.FormatUint(parsed, 10) != value {
		return 0, errors.New("Last-Event-ID must be a canonical unsigned decimal integer")
	}
	return parsed, nil
}

func headerValues(header http.Header, name string) ([]string, bool) {
	values, ok := header[http.CanonicalHeaderKey(name)]
	return values, ok
}

func (resolution streamResolution) runtimeID() journal.StreamID {
	return resolution.id
}

func writeJournalHTTPError(writer http.ResponseWriter, err error, id journal.StreamID) {
	switch {
	case errors.Is(err, journal.ErrExpired):
		writeStreamError(writer, http.StatusGone, "stream_expired", "stream journal has expired", id.String())
	case errors.Is(err, journal.ErrNotFound):
		writeStreamError(writer, http.StatusGone, "stream_not_found", "stream was not found", id.String())
	case errors.Is(err, journal.ErrOffsetExpired):
		writeStreamError(writer, http.StatusGone, "stream_offset_expired", "requested stream events are no longer retained", id.String())
	case errors.Is(err, journal.ErrNotResumable):
		writeStreamError(writer, http.StatusGone, "stream_not_resumable", "stream was explicitly stopped", id.String())
	case errors.Is(err, journal.ErrCursorAhead):
		writeStreamError(writer, http.StatusConflict, "cursor_ahead", "resume cursor is ahead of the stream", id.String())
	default:
		writeStreamError(writer, http.StatusServiceUnavailable, "journal_unavailable", "stream journal is temporarily unavailable", id.String())
	}
}

type streamErrorEnvelope struct {
	Error streamAPIError `json:"error"`
}

type streamAPIError struct {
	Type     string `json:"type"`
	Code     string `json:"code"`
	Message  string `json:"message"`
	StreamID string `json:"stream_id"`
}

func writeStreamError(writer http.ResponseWriter, status int, code, message, streamID string) {
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.Header().Set("Cache-Control", "no-store")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(streamErrorEnvelope{Error: streamAPIError{
		Type: "streamweld_error", Code: code, Message: message, StreamID: streamID,
	}})
}

func writeUpstreamRejection(writer http.ResponseWriter, rejection *upstreamRejection) {
	if len(rejection.body) == 0 {
		writeAPIError(writer, rejection.status, "upstream_rejected", "the upstream backend rejected the request")
		return
	}
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.Header().Set("Cache-Control", "no-store")
	writer.WriteHeader(rejection.status)
	_, _ = writer.Write(rejection.body)
}
