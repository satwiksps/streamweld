package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/streamweld/streamweld/internal/journal"
	"github.com/streamweld/streamweld/internal/proxy/sse"
)

const maxUpstreamErrorBytes = 64 << 10

var (
	errExplicitStop       = errors.New("stream explicitly stopped")
	errGenerationComplete = errors.New("stream generation completed")
)

type streamIDSource interface {
	New() (journal.StreamID, error)
}

type durableService struct {
	rootContext context.Context
	config      Config
	target      *url.URL
	transport   http.RoundTripper
	journal     journal.Journal
	ids         streamIDSource
	idempotency journal.IdempotencyRegistry
	logger      *slog.Logger

	createMu sync.Mutex
	streams  sync.Map // journal.StreamID -> *streamRuntime
	active   sync.WaitGroup
}

type streamRuntime struct {
	service       *durableService
	id            journal.StreamID
	endpoint      string
	requestBody   []byte
	requestHeader http.Header
	orphanPolicy  OrphanPolicy
	idemDigest    *journal.IdempotencyDigest

	context    context.Context
	cancel     context.CancelCauseFunc
	done       chan struct{}
	firstEntry chan journal.Entry
	openEntry  journal.Entry

	writeMu      sync.Mutex
	mu           sync.Mutex
	terminating  bool
	terminal     journal.EntryKind
	readers      int
	orphanTimer  *time.Timer
	progress     streamProgress
	finishReason string
	lastSeq      uint64
	stopResult   *stopResponse
	activeDone   sync.Once
	firstOnce    sync.Once
}

type streamResolution struct {
	id       journal.StreamID
	runtime  *streamRuntime
	existing bool
	degraded bool
}

type upstreamRejection struct {
	status int
	body   []byte
}

type stopResponse struct {
	StreamID    journal.StreamID `json:"stream_id"`
	Outcome     string           `json:"outcome"`
	PartialText string           `json:"partial_text"`
	Usage       tokenUsage       `json:"usage"`
}

func newDurableService(
	rootContext context.Context,
	config Config,
	target *url.URL,
	transport http.RoundTripper,
	journalBackend journal.Journal,
	ids streamIDSource,
	idempotency journal.IdempotencyRegistry,
	logger *slog.Logger,
) *durableService {
	return &durableService{
		rootContext: rootContext,
		config:      config,
		target:      target,
		transport:   transport,
		journal:     journalBackend,
		ids:         ids,
		idempotency: idempotency,
		logger:      logger,
	}
}

func (s *durableService) resolve(
	request *http.Request,
	normalized normalizedRequest,
	policy OrphanPolicy,
	idempotencyKey string,
) (streamResolution, error) {
	s.createMu.Lock()
	defer s.createMu.Unlock()

	for attempt := 0; attempt < 8; attempt++ {
		id, err := s.ids.New()
		if err != nil {
			return streamResolution{}, fmt.Errorf("generate stream ID: %w", err)
		}

		var digest *journal.IdempotencyDigest
		if idempotencyKey != "" {
			binding, resolveErr := s.idempotency.ResolveOrCreate(
				s.rootContext,
				idempotencyKey,
				id,
				s.config.JournalTTL,
			)
			if resolveErr != nil {
				s.logDegraded(request, "idempotency resolution failed", resolveErr)
				return streamResolution{degraded: true}, nil
			}
			digestValue := binding.Digest
			digest = &digestValue
			if !binding.Created {
				if _, stateErr := s.journal.State(s.rootContext, binding.ID); stateErr == nil {
					runtime, _ := s.loadRuntime(binding.ID)
					return streamResolution{id: binding.ID, runtime: runtime, existing: true}, nil
				} else if !errors.Is(stateErr, journal.ErrNotFound) && !errors.Is(stateErr, journal.ErrExpired) {
					s.logDegraded(request, "idempotent journal lookup failed", stateErr)
					return streamResolution{degraded: true}, nil
				}
				if _, removeErr := s.idempotency.Remove(s.rootContext, binding.Digest); removeErr != nil {
					s.logDegraded(request, "stale idempotency cleanup failed", removeErr)
					return streamResolution{degraded: true}, nil
				}
				continue
			}
		}

		meta := journal.Meta{
			Model:     normalized.Model,
			BackendID: safeBackendAddress(s.target),
			Endpoint:  request.URL.Path,
			Request:   bytes.Clone(normalized.Body),
		}
		if err := s.journal.Open(s.rootContext, id, meta); err != nil {
			if digest != nil {
				_, _ = s.idempotency.Remove(s.rootContext, *digest)
			}
			if errors.Is(err, journal.ErrAlreadyExists) {
				continue
			}
			s.logDegraded(request, "durable journal unavailable", err)
			return streamResolution{degraded: true}, nil
		}

		producerContext, cancel := context.WithCancelCause(s.rootContext)
		openPayload, marshalErr := marshalOpenPayload(id, meta)
		if marshalErr != nil {
			cancel(marshalErr)
			if digest != nil {
				_, _ = s.idempotency.Remove(s.rootContext, *digest)
			}
			return streamResolution{}, marshalErr
		}
		runtime := &streamRuntime{
			service:       s,
			id:            id,
			endpoint:      request.URL.Path,
			requestBody:   bytes.Clone(normalized.Body),
			requestHeader: cloneUpstreamHeaders(request.Header),
			orphanPolicy:  policy,
			idemDigest:    digest,
			context:       producerContext,
			cancel:        cancel,
			done:          make(chan struct{}),
			firstEntry:    make(chan journal.Entry, 1),
			lastSeq:       1,
		}
		runtime.openEntry = journal.Entry{Seq: 1, Kind: journal.KindOpen, Payload: openPayload}
		s.active.Add(1)
		s.streams.Store(id, runtime)
		if digest != nil {
			go runtime.maintainIdempotencyLease()
		}
		return streamResolution{id: id, runtime: runtime}, nil
	}
	return streamResolution{}, errors.New("could not allocate a collision-free stream ID")
}

func (s *durableService) start(runtime *streamRuntime, rawQuery string) (*upstreamRejection, error) {
	upstreamURL := joinUpstreamURL(s.target, runtime.endpoint, rawQuery)
	request, err := http.NewRequestWithContext(
		runtime.context,
		http.MethodPost,
		upstreamURL.String(),
		bytes.NewReader(runtime.requestBody),
	)
	if err != nil {
		return nil, fmt.Errorf("construct upstream request: %w", err)
	}
	request.Header = runtime.requestHeader.Clone()
	request.ContentLength = int64(len(runtime.requestBody))
	request.Host = s.target.Host

	client := &http.Client{
		Transport: s.transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	response, err := client.Do(request)
	if err != nil {
		if context.Cause(runtime.context) != nil {
			runtime.finishCanceledProducer()
			return nil, nil
		}
		if closeErr := runtime.finishError("upstream_error", "upstream request failed", "tcp_reset"); closeErr != nil {
			return nil, errors.Join(fmt.Errorf("dispatch upstream: %w", err), closeErr)
		}
		return nil, nil
	}

	if response.StatusCode >= http.StatusBadRequest && response.StatusCode < http.StatusInternalServerError {
		body := readBoundedErrorBody(response.Body)
		if closeErr := response.Body.Close(); closeErr != nil {
			s.logger.Warn("close rejected upstream response", "stream_id", runtime.id, "error", closeErr)
		}
		if closeErr := runtime.finishError("upstream_error", "upstream rejected the request", "upstream_rejected"); closeErr != nil {
			return nil, closeErr
		}
		return &upstreamRejection{status: response.StatusCode, body: body}, nil
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		_, _ = io.CopyN(io.Discard, response.Body, maxUpstreamErrorBytes)
		if closeErr := response.Body.Close(); closeErr != nil {
			s.logger.Warn("close failed upstream response", "stream_id", runtime.id, "error", closeErr)
		}
		if closeErr := runtime.finishError("upstream_error", "upstream backend failed", "upstream_5xx"); closeErr != nil {
			return nil, closeErr
		}
		return nil, nil
	}

	contentType, _, contentTypeErr := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if contentTypeErr != nil || !strings.EqualFold(contentType, "text/event-stream") {
		_, _ = io.CopyN(io.Discard, response.Body, maxUpstreamErrorBytes)
		if closeErr := response.Body.Close(); closeErr != nil {
			s.logger.Warn("close invalid upstream response", "stream_id", runtime.id, "error", closeErr)
		}
		if closeErr := runtime.finishError("upstream_error", "upstream response was not an event stream", "invalid_content_type"); closeErr != nil {
			return nil, closeErr
		}
		return nil, nil
	}

	decoder := sse.NewDecoder(response.Body)
	for {
		accepted, terminal := runtime.consumeNext(decoder)
		if terminal {
			if closeErr := response.Body.Close(); closeErr != nil {
				s.logger.Warn("close upstream stream", "stream_id", runtime.id, "error", closeErr)
			}
			return nil, nil
		}
		if accepted {
			break
		}
	}
	go runtime.consumeRemaining(response.Body, decoder)
	return nil, nil
}

func (r *streamRuntime) consumeRemaining(body io.ReadCloser, decoder *sse.Decoder) {
	defer func() {
		if err := body.Close(); err != nil {
			r.service.logger.Warn("close upstream stream", "stream_id", r.id, "error", err)
		}
	}()

	for {
		_, terminal := r.consumeNext(decoder)
		if terminal {
			return
		}
	}
}

func (r *streamRuntime) consumeNext(decoder *sse.Decoder) (bool, bool) {
	event, err := decoder.Decode()
	if err != nil {
		if context.Cause(r.context) != nil {
			r.finishCanceledProducer()
			return false, true
		}
		reason := "upstream_read_error"
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, sse.ErrIncompleteEvent) {
			reason = "unexpected_eof"
		}
		if closeErr := r.finishError("upstream_error", "upstream stream ended before completion", reason); closeErr != nil {
			r.service.logger.Error("close failed stream", "stream_id", r.id, "error", errors.Join(err, closeErr))
		}
		return false, true
	}
	if !event.HasData {
		return false, false
	}
	if bytes.Equal(bytes.TrimSpace(event.Data), []byte("[DONE]")) {
		if closeErr := r.finishDone(); closeErr != nil {
			r.service.logger.Error("finish durable stream", "stream_id", r.id, "error", closeErr)
		}
		return true, true
	}
	observation, observeErr := observeOpenAIChunk(event.Data)
	if observeErr != nil {
		if closeErr := r.finishError("upstream_error", "upstream emitted an invalid chunk", "invalid_chunk"); closeErr != nil {
			r.service.logger.Error("close invalid upstream stream", "stream_id", r.id, "error", errors.Join(observeErr, closeErr))
		}
		return true, true
	}
	if len(observation.ErrorPayload) != 0 {
		if closeErr := r.finishError("upstream_error", "upstream emitted an error chunk", "error_chunk"); closeErr != nil {
			r.service.logger.Error("close errored upstream stream", "stream_id", r.id, "error", closeErr)
		}
		return true, true
	}
	if appendErr := r.appendChunk(event, observation); appendErr != nil {
		if context.Cause(r.context) == nil {
			r.service.logger.Error("append durable chunk", "stream_id", r.id, "error", appendErr)
			_ = r.finishError("journal_capacity_exceeded", "durable journal could not accept a chunk", "journal_capacity")
		}
		return true, true
	}
	return true, false
}

func (r *streamRuntime) appendChunk(event sse.Event, observation chunkObservation) error {
	payload, err := json.Marshal(struct {
		Data          string  `json:"data"`
		UpstreamEvent *string `json:"upstream_event"`
	}{
		Data:          string(event.Data),
		UpstreamEvent: optionalString(event.Type, event.HasType),
	})
	if err != nil {
		return fmt.Errorf("marshal chunk entry: %w", err)
	}

	r.writeMu.Lock()
	defer r.writeMu.Unlock()
	r.mu.Lock()
	if r.terminal != "" || r.terminating {
		r.mu.Unlock()
		return journal.ErrTerminalState
	}
	r.mu.Unlock()
	seq, err := r.service.journal.Append(r.context, r.id, journal.Entry{Kind: journal.KindChunk, Payload: payload})
	if err != nil {
		return err
	}
	r.mu.Lock()
	r.lastSeq = seq
	r.progress.Apply(observation)
	if observation.FinishReason != "" {
		r.finishReason = observation.FinishReason
	}
	r.mu.Unlock()
	r.publishFirst(journal.Entry{Seq: seq, Kind: journal.KindChunk, Payload: payload})
	return nil
}

func (r *streamRuntime) finishDone() error {
	return r.closeTerminal(journal.KindDone, errGenerationComplete, nil, func() ([]byte, *stopResponse, error) {
		_, usage := r.progress.Snapshot()
		payload, err := json.Marshal(struct {
			FinishReason *string    `json:"finish_reason"`
			Usage        tokenUsage `json:"usage"`
		}{
			FinishReason: optionalString(r.finishReason, r.finishReason != ""),
			Usage:        usage,
		})
		return payload, nil, err
	})
}

func (r *streamRuntime) finishError(code, message, reason string) error {
	return r.finishErrorWhen(code, message, reason, nil)
}

func (r *streamRuntime) finishErrorWhen(code, message, reason string, guard func() bool) error {
	return r.closeTerminal(journal.KindError, errors.New(reason), guard, func() ([]byte, *stopResponse, error) {
		_, usage := r.progress.Snapshot()
		payload, err := json.Marshal(struct {
			Code      string     `json:"code"`
			Message   string     `json:"message"`
			Reason    string     `json:"reason"`
			Retriable bool       `json:"retriable"`
			Usage     tokenUsage `json:"usage"`
		}{
			Code: code, Message: message, Reason: reason, Usage: usage,
		})
		return payload, nil, err
	})
}

func (r *streamRuntime) stop() (stopResponse, error) {
	r.mu.Lock()
	if r.terminal == journal.KindStopped && r.stopResult != nil {
		result := *r.stopResult
		r.mu.Unlock()
		return result, nil
	}
	if r.terminal != "" || r.terminating {
		r.mu.Unlock()
		return stopResponse{}, journal.ErrTerminalState
	}
	r.mu.Unlock()
	var result stopResponse
	err := r.closeTerminal(journal.KindStopped, errExplicitStop, nil, func() ([]byte, *stopResponse, error) {
		partialText, usage := r.progress.Snapshot()
		payload, marshalErr := json.Marshal(struct {
			Usage tokenUsage `json:"usage"`
		}{Usage: usage})
		result = stopResponse{StreamID: r.id, Outcome: "stopped", PartialText: partialText, Usage: usage}
		return payload, &result, marshalErr
	})
	return result, err
}

func (r *streamRuntime) closeTerminal(
	kind journal.EntryKind,
	cause error,
	guard func() bool,
	buildPayload func() ([]byte, *stopResponse, error),
) error {
	r.mu.Lock()
	if r.terminal != "" || r.terminating {
		r.mu.Unlock()
		return journal.ErrTerminalState
	}
	if guard != nil && !guard() {
		r.mu.Unlock()
		return nil
	}
	r.terminating = true
	r.cancel(cause)
	r.mu.Unlock()

	r.writeMu.Lock()
	defer r.writeMu.Unlock()
	r.mu.Lock()
	payload, stopResult, buildErr := buildPayload()
	r.mu.Unlock()
	if buildErr != nil {
		r.failTerminalTransition()
		return fmt.Errorf("marshal %s entry: %w", kind, buildErr)
	}
	ctx, cancel := context.WithTimeout(context.Background(), r.service.config.ReadinessTimeout)
	defer cancel()
	if err := r.service.journal.Close(ctx, r.id, journal.Entry{Kind: kind, Payload: payload}); err != nil {
		r.failTerminalTransition()
		return err
	}
	r.mu.Lock()
	r.lastSeq++
	terminalEntry := journal.Entry{Seq: r.lastSeq, Kind: kind, Payload: payload}
	r.terminal = kind
	r.terminating = false
	r.stopResult = stopResult
	if r.orphanTimer != nil {
		r.orphanTimer.Stop()
		r.orphanTimer = nil
	}
	close(r.done)
	r.activeDone.Do(r.service.active.Done)
	r.mu.Unlock()
	r.publishFirst(terminalEntry)
	r.refreshIdempotency()
	time.AfterFunc(r.service.config.JournalTTL, func() {
		r.service.streams.CompareAndDelete(r.id, r)
	})
	return nil
}

func (r *streamRuntime) failTerminalTransition() {
	r.mu.Lock()
	r.terminating = false
	r.mu.Unlock()
}

func (r *streamRuntime) publishFirst(entry journal.Entry) {
	r.firstOnce.Do(func() {
		r.firstEntry <- entry
	})
}

func (r *streamRuntime) attachReader() {
	r.mu.Lock()
	if r.orphanTimer != nil {
		r.orphanTimer.Stop()
		r.orphanTimer = nil
	}
	r.readers++
	r.mu.Unlock()
}

func (r *streamRuntime) detachReader() {
	r.mu.Lock()
	if r.readers > 0 {
		r.readers--
	}
	if r.readers != 0 || r.terminal != "" {
		r.mu.Unlock()
		return
	}
	switch r.orphanPolicy {
	case OrphanCancel:
		r.mu.Unlock()
		go r.cancelOrphan()
	case OrphanCancelAfter:
		r.orphanTimer = time.AfterFunc(r.service.config.OrphanTimeout, r.cancelOrphan)
		r.mu.Unlock()
	default:
		r.mu.Unlock()
	}
}

func (r *streamRuntime) cancelOrphan() {
	if err := r.finishErrorWhen(
		"orphan_cancelled",
		"producer canceled after its final reader disconnected",
		"orphan_policy",
		func() bool { return r.readers == 0 },
	); err != nil && !errors.Is(err, journal.ErrTerminalState) {
		r.service.logger.Error("close orphaned stream", "stream_id", r.id, "error", err)
	}
}

func (r *streamRuntime) refreshIdempotency() {
	if r.idemDigest == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), r.service.config.ReadinessTimeout)
	defer cancel()
	if _, err := r.service.idempotency.Refresh(ctx, *r.idemDigest, r.service.config.JournalTTL); err != nil {
		r.service.logger.Warn("refresh idempotency mapping", "stream_id", r.id, "error", err)
	}
}

func (r *streamRuntime) maintainIdempotencyLease() {
	interval := r.service.config.JournalTTL / 2
	if interval <= 0 {
		interval = time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			r.refreshIdempotency()
		case <-r.done:
			return
		case <-r.service.rootContext.Done():
			return
		}
	}
}

func (r *streamRuntime) finishCanceledProducer() {
	r.mu.Lock()
	needsTerminal := r.terminal == "" && !r.terminating
	r.mu.Unlock()
	if !needsTerminal {
		return
	}
	if err := r.finishError(
		"proxy_shutdown_timeout",
		"proxy shutdown canceled the upstream producer",
		"proxy_shutdown",
	); err != nil && !errors.Is(err, journal.ErrTerminalState) {
		r.service.logger.Error("close producer canceled by proxy shutdown", "stream_id", r.id, "error", err)
	}
}

func (s *durableService) wait(ctx context.Context) error {
	finished := make(chan struct{})
	go func() {
		s.active.Wait()
		close(finished)
	}()
	select {
	case <-finished:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *durableService) logDegraded(request *http.Request, message string, err error) {
	s.logger.ErrorContext(request.Context(), message+"; degrading stream",
		"error", err,
		"backend", safeBackendAddress(s.target),
	)
}

func (s *durableService) loadRuntime(id journal.StreamID) (*streamRuntime, bool) {
	value, ok := s.streams.Load(id)
	if !ok {
		return nil, false
	}
	runtime, ok := value.(*streamRuntime)
	return runtime, ok
}

func optionalString(value string, present bool) *string {
	if !present || value == "" {
		return nil
	}
	cloned := value
	return &cloned
}

func marshalOpenPayload(id journal.StreamID, meta journal.Meta) ([]byte, error) {
	payload, err := json.Marshal(struct {
		StreamID     journal.StreamID `json:"stream_id"`
		Model        string           `json:"model"`
		ModelVersion *string          `json:"model_version"`
		BackendID    string           `json:"backend_id"`
	}{
		StreamID: id, Model: meta.Model, ModelVersion: meta.ModelVersion, BackendID: meta.BackendID,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal open event: %w", err)
	}
	return payload, nil
}

func cloneUpstreamHeaders(source http.Header) http.Header {
	header := source.Clone()
	for _, name := range []string{
		"Connection", "Proxy-Connection", "Keep-Alive", "Proxy-Authenticate",
		"Proxy-Authorization", "Te", "Trailer", "Transfer-Encoding", "Upgrade",
		"X-Streamweld-Idempotency-Key", "X-Streamweld-Verbose", "X-Streamweld-Orphan-Policy",
		"Last-Event-ID",
	} {
		header.Del(name)
	}
	if connection := source.Get("Connection"); connection != "" {
		for _, name := range strings.Split(connection, ",") {
			header.Del(strings.TrimSpace(name))
		}
	}
	header.Set("Content-Type", "application/json")
	header.Del("Content-Length")
	return header
}

func joinUpstreamURL(target *url.URL, requestPath, rawQuery string) *url.URL {
	result := *target
	switch {
	case strings.HasSuffix(result.Path, "/") && strings.HasPrefix(requestPath, "/"):
		result.Path += strings.TrimPrefix(requestPath, "/")
	case !strings.HasSuffix(result.Path, "/") && !strings.HasPrefix(requestPath, "/"):
		result.Path += "/" + requestPath
	default:
		result.Path += requestPath
	}
	result.RawPath = ""
	if result.RawQuery != "" && rawQuery != "" {
		result.RawQuery += "&" + rawQuery
	} else if rawQuery != "" {
		result.RawQuery = rawQuery
	}
	return &result
}

func readBoundedErrorBody(body io.Reader) []byte {
	limited, err := io.ReadAll(io.LimitReader(body, maxUpstreamErrorBytes+1))
	if err != nil || len(limited) > maxUpstreamErrorBytes {
		return nil
	}
	var envelope struct {
		Error json.RawMessage `json:"error"`
	}
	if json.Unmarshal(limited, &envelope) != nil || len(envelope.Error) == 0 {
		return nil
	}
	return bytes.Clone(limited)
}
