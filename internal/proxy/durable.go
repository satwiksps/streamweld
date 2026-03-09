package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/streamweld/streamweld/internal/backend"
	"github.com/streamweld/streamweld/internal/journal"
	"github.com/streamweld/streamweld/internal/migrate"
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
	backends    *backend.Pool
	relay       *relayCoordinator
	owner       *journal.OwnerRecord

	streams           sync.Map // journal.StreamID -> *streamRuntime
	active            sync.WaitGroup
	degradationLeases sync.WaitGroup
	journalDegraded   atomic.Int64
}

type streamRuntime struct {
	service         *durableService
	id              journal.StreamID
	endpoint        string
	requestBody     []byte
	requestHeader   http.Header
	orphanPolicy    OrphanPolicy
	idemDigest      *journal.IdempotencyDigest
	currentLease    *backend.Lease
	currentBackend  backend.State
	originBackend   backend.State
	createdAt       time.Time
	requestKind     migrate.RequestKind
	structured      bool
	multipleChoice  bool
	migrationsUsed  uint64
	estimateWarned  bool
	toolCalls       migrate.ToolCallTracker
	toolTrackFailed bool
	attemptCancel   context.CancelCauseFunc
	pendingTrigger  string

	context      context.Context
	cancel       context.CancelCauseFunc
	done         chan struct{}
	firstEntry   chan journal.Entry
	openEntry    journal.Entry
	degradedFeed *degradedFeed

	writeMu                 sync.Mutex
	mu                      sync.Mutex
	terminating             bool
	terminal                journal.EntryKind
	readers                 int
	orphanTimer             *time.Timer
	orphanClaim             string
	remoteReaderWasAttached bool
	progress                streamProgress
	finishReason            string
	lastSeq                 uint64
	stopResult              *stopResponse
	stopRequested           bool
	stopWait                chan struct{}
	degraded                bool
	activeDone              sync.Once
	firstOnce               sync.Once

	degradationMarkerOnce      sync.Once
	degradationTerminalOnce    sync.Once
	degradationAttemptOnce     sync.Once
	degradationTerminal        chan struct{}
	degradationTerminalAttempt chan struct{}
}

type streamResolution struct {
	id            journal.StreamID
	runtime       *streamRuntime
	existing      bool
	degraded      bool
	degradedLease *backend.Lease
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
	backends *backend.Pool,
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
		backends:    backends,
	}
}

func (s *durableService) resolve(
	request *http.Request,
	normalized normalizedRequest,
	policy OrphanPolicy,
	idempotencyKey string,
) (streamResolution, error) {
	waitContext, cancelWait := context.WithTimeout(request.Context(), s.config.ReadinessTimeout)
	defer cancelWait()

	for {
		if err := waitContext.Err(); err != nil {
			return streamResolution{}, fmt.Errorf("allocate durable stream: %w", err)
		}
		id, err := s.ids.New()
		if err != nil {
			return streamResolution{}, fmt.Errorf("generate stream ID: %w", err)
		}

		var digest *journal.IdempotencyDigest
		if idempotencyKey != "" {
			binding, resolveErr := s.idempotency.ResolveOrCreate(
				waitContext,
				idempotencyKey,
				id,
				s.config.JournalTTL,
			)
			if resolveErr != nil {
				s.logDegraded(request, "idempotency resolution failed", resolveErr)
				return s.resolveDegraded(id)
			}
			digestValue := binding.Digest
			digest = &digestValue
			if !binding.Created {
				if _, stateErr := s.journal.State(waitContext, binding.ID); stateErr == nil {
					runtime, _ := s.loadRuntime(binding.ID)
					return streamResolution{id: binding.ID, runtime: runtime, existing: true}, nil
				} else if errors.Is(stateErr, journal.ErrNotFound) {
					// A different replica may have won the idempotency reservation
					// and still be between ResolveOrCreate and Open. Never delete
					// that live reservation: doing so can admit two producers for
					// one key. Wait up to the request/readiness bound. Redis uses a
					// much shorter renewable pending lease, so a creator crash does
					// not poison this key for the journal retention period.
					select {
					case <-waitContext.Done():
						return streamResolution{}, fmt.Errorf(
							"wait for idempotency reservation: %w", waitContext.Err(),
						)
					case <-time.After(10 * time.Millisecond):
					}
					continue
				} else if !errors.Is(stateErr, journal.ErrExpired) {
					s.logDegraded(request, "idempotent journal lookup failed", stateErr)
					return s.resolveDegraded(id)
				}
				_, removeErr := s.removeIdempotencyIfBound(
					waitContext, binding.Digest, binding.ID,
				)
				if removeErr != nil {
					s.logDegraded(request, "stale idempotency cleanup failed", removeErr)
					return s.resolveDegraded(id)
				}
				// Resolve again whether this caller removed the stale binding or
				// another replica already replaced it. Conditional removal ensures
				// that replacement winner is never deleted.
				continue
			}
		}

		meta := journal.Meta{
			Model:       normalized.Model,
			Endpoint:    request.URL.Path,
			Request:     bytes.Clone(normalized.Body),
			Owner:       s.owner,
			Idempotency: digest,
		}
		stopPendingLease := s.maintainPendingIdempotencyLease(digest, id)
		lease, acquireErr := s.backends.Acquire(id.String())
		if acquireErr != nil {
			stopPendingLease()
			if digest != nil {
				s.releasePendingIdempotency(*digest, id)
			}
			return streamResolution{}, fmt.Errorf("select initial backend: %w", acquireErr)
		}
		selected := lease.Backend()
		meta.BackendID = selected.ID.String()
		meta.ModelVersion = optionalString(selected.ModelVersion, selected.ModelVersion != "")
		if err := s.journal.Open(waitContext, id, meta); err != nil {
			stopPendingLease()
			if digest != nil {
				s.releasePendingIdempotency(*digest, id)
			}
			if errors.Is(err, journal.ErrAlreadyExists) ||
				errors.Is(err, journal.ErrIdempotencyReservationLost) {
				lease.Release()
				continue
			}
			s.logDegraded(request, "durable journal unavailable", err)
			return streamResolution{degraded: true, degradedLease: lease}, nil
		}
		stopPendingLease()

		producerContext, cancel := context.WithCancelCause(s.rootContext)
		openPayload, marshalErr := marshalOpenPayload(id, meta)
		if marshalErr != nil {
			cancel(marshalErr)
			lease.Release()
			if digest != nil {
				_, _ = s.removeIdempotencyIfBound(s.rootContext, *digest, id)
			}
			return streamResolution{}, marshalErr
		}
		structured, _ := migrate.IsStructuredRequest(normalized.Body)
		runtime := &streamRuntime{
			service:                    s,
			id:                         id,
			endpoint:                   request.URL.Path,
			requestBody:                bytes.Clone(normalized.Body),
			requestHeader:              cloneUpstreamHeaders(request.Header),
			orphanPolicy:               policy,
			idemDigest:                 digest,
			currentLease:               lease,
			currentBackend:             selected,
			originBackend:              selected,
			createdAt:                  time.Now().UTC(),
			requestKind:                requestKindForEndpoint(request.URL.Path),
			structured:                 structured,
			multipleChoice:             requestHasMultipleChoices(normalized.Body),
			context:                    producerContext,
			cancel:                     cancel,
			done:                       make(chan struct{}),
			firstEntry:                 make(chan journal.Entry, 1),
			degradedFeed:               newDegradedFeed(s.config.ReaderMaxLagBytes),
			degradationTerminal:        make(chan struct{}),
			degradationTerminalAttempt: make(chan struct{}),
			stopWait:                   make(chan struct{}),
			lastSeq:                    1,
		}
		runtime.openEntry = journal.Entry{Seq: 1, Kind: journal.KindOpen, Payload: openPayload}
		s.active.Add(1)
		s.streams.Store(id, runtime)
		if digest != nil {
			go runtime.maintainIdempotencyLease()
		}
		if activeLease, ok := s.journal.(journal.ActiveJournalLease); ok {
			go runtime.maintainJournalLease(activeLease)
		}
		return streamResolution{id: id, runtime: runtime}, nil
	}
}

func (s *durableService) resolveDegraded(id journal.StreamID) (streamResolution, error) {
	lease, err := s.backends.Acquire(id.String())
	if err != nil {
		return streamResolution{}, fmt.Errorf("select degraded backend: %w", err)
	}
	return streamResolution{degraded: true, degradedLease: lease}, nil
}

func (s *durableService) start(runtime *streamRuntime, rawQuery string) (*upstreamRejection, error) {
	result := make(chan producerStartResult, 1)
	go runtime.runProducer(rawQuery, result)
	started := <-result
	return started.rejection, started.err
}

func (r *streamRuntime) appendChunk(event sse.Event, observation chunkObservation) error {
	if r.requestKind == migrate.RequestChatCompletion {
		r.mu.Lock()
		if err := r.toolCalls.ObserveChunk(event.Data); err != nil {
			r.toolTrackFailed = true
			r.service.logger.Warn("track tool-call boundary", "stream_id", r.id, "error", err)
		}
		r.mu.Unlock()
	}
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
	if r.degraded {
		r.progress.Apply(observation)
		if observation.FinishReason != "" {
			r.finishReason = observation.FinishReason
		}
		r.mu.Unlock()
		_ = r.degradedFeed.append(r.context, degradedChunkFrame(event))
		return nil
	}
	r.mu.Unlock()
	mutationContext, cancelMutation := r.journalMutationContext()
	seq, err := r.service.journal.Append(
		mutationContext,
		r.id,
		journal.Entry{Kind: journal.KindChunk, Payload: payload},
	)
	cancelMutation()
	if err != nil {
		r.mu.Lock()
		r.progress.Apply(observation)
		if observation.FinishReason != "" {
			r.finishReason = observation.FinishReason
		}
		r.mu.Unlock()
		r.enterJournalDegradedLocked(err, degradedChunkFrame(event))
		return nil
	}
	r.mu.Lock()
	r.lastSeq = seq
	r.progress.Apply(observation)
	if observation.FinishReason != "" {
		r.finishReason = observation.FinishReason
	}
	r.mu.Unlock()
	committed := journal.Entry{Seq: seq, Kind: journal.KindChunk, Payload: payload}
	if err := r.degradedFeed.publishCommitted(r.context, committed); err != nil {
		return err
	}
	r.publishFirst(committed)
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
	for {
		r.mu.Lock()
		if r.terminal == journal.KindStopped && r.stopResult != nil {
			result := *r.stopResult
			r.mu.Unlock()
			return result, nil
		}
		if r.stopRequested {
			wait := r.stopWait
			r.mu.Unlock()
			<-wait
			continue
		}
		if r.terminal != "" || r.terminating {
			r.mu.Unlock()
			return stopResponse{}, journal.ErrTerminalState
		}
		r.stopRequested = true
		r.mu.Unlock()
		break
	}
	var result stopResponse
	err := r.closeTerminal(journal.KindStopped, errExplicitStop, nil, func() ([]byte, *stopResponse, error) {
		partialText, usage := r.progress.Snapshot()
		payload, marshalErr := json.Marshal(struct {
			PartialText string     `json:"partial_text"`
			Usage       tokenUsage `json:"usage"`
		}{PartialText: partialText, Usage: usage})
		result = stopResponse{StreamID: r.id, Outcome: "stopped", PartialText: partialText, Usage: usage}
		return payload, &result, marshalErr
	})
	r.mu.Lock()
	wait := r.stopWait
	r.stopWait = make(chan struct{})
	r.stopRequested = false
	close(wait)
	r.mu.Unlock()
	return result, err
}

func (r *streamRuntime) closeTerminal(
	kind journal.EntryKind,
	cause error,
	guard func() bool,
	buildPayload func() ([]byte, *stopResponse, error),
) error {
	r.writeMu.Lock()
	defer r.writeMu.Unlock()

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
	r.mu.Lock()
	payload, stopResult, buildErr := buildPayload()
	degraded := r.degraded
	r.mu.Unlock()
	if buildErr != nil {
		r.failTerminalTransition()
		return fmt.Errorf("marshal %s entry: %w", kind, buildErr)
	}
	if degraded {
		return r.finishDegradedLocked(kind, payload, stopResult)
	}
	ctx, cancel := context.WithTimeout(context.Background(), r.service.config.ReadinessTimeout)
	defer cancel()
	if err := r.service.journal.Close(ctx, r.id, journal.Entry{Kind: kind, Payload: payload}); err != nil {
		r.enterJournalDegradedLocked(err)
		return r.finishDegradedLocked(kind, payload, stopResult)
	}
	r.mu.Lock()
	r.lastSeq++
	terminalEntry := journal.Entry{Seq: r.lastSeq, Kind: kind, Payload: payload}
	r.terminal = kind
	r.terminating = false
	r.stopResult = stopResult
	lease := r.currentLease
	r.currentLease = nil
	if r.orphanTimer != nil {
		r.orphanTimer.Stop()
		r.orphanTimer = nil
	}
	close(r.done)
	r.activeDone.Do(r.service.active.Done)
	r.mu.Unlock()
	if lease != nil {
		lease.Release()
	}
	if err := r.degradedFeed.publishCommitted(r.service.rootContext, terminalEntry); err != nil {
		r.service.logger.Warn("relay committed terminal event", "stream_id", r.id, "error", err)
	}
	r.degradedFeed.close()
	r.publishFirst(terminalEntry)
	r.refreshIdempotency()
	time.AfterFunc(r.service.config.JournalTTL, func() {
		r.service.streams.CompareAndDelete(r.id, r)
	})
	return nil
}

// journalMutationContext preserves request-scoped values while decoupling a
// committed journal mutation from producer cancellation. Terminal transitions
// serialize on writeMu before canceling the producer, and each mutation is
// independently bounded by ReadinessTimeout, so an execute-then-cancel result
// is always observed and mirrored before terminal sequence allocation.
func (r *streamRuntime) journalMutationContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(r.context), r.service.config.ReadinessTimeout)
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

func (r *streamRuntime) attachReader() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.terminating || (r.orphanClaim != "" && r.terminal == "") {
		return false
	}
	r.remoteReaderWasAttached = false
	if r.orphanTimer != nil {
		r.orphanTimer.Stop()
		r.orphanTimer = nil
	}
	r.readers++
	return true
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
	r.mu.Lock()
	if r.readers != 0 || r.terminal != "" || r.terminating {
		r.mu.Unlock()
		return
	}
	r.orphanTimer = nil

	claimID, claimSupported, claimed, claimErr := r.service.tryClaimDistributedOrphan(r.id)
	if claimSupported {
		if claimErr != nil {
			r.mu.Unlock()
			// A journal outage or an ambiguous claim result must not be
			// interpreted as proof that a remote reader disappeared.
			r.service.logger.Debug("claim distributed orphan cancellation",
				"stream_id", r.id, "error", claimErr)
			r.scheduleOrphanCheck(remoteReaderPollPeriod)
			return
		}
		if !claimed {
			r.remoteReaderWasAttached = true
			r.mu.Unlock()
			r.scheduleOrphanCheck(remoteReaderPollPeriod)
			return
		}
		if r.orphanPolicy == OrphanCancelAfter && r.remoteReaderWasAttached {
			r.remoteReaderWasAttached = false
			r.mu.Unlock()
			if err := r.service.releaseDistributedOrphanClaim(r.id, claimID); err != nil {
				r.service.logger.Debug("release orphan claim before grace period",
					"stream_id", r.id, "error", err)
			}
			r.scheduleOrphanCheck(r.service.config.OrphanTimeout)
			return
		}
		r.remoteReaderWasAttached = false
		r.orphanClaim = claimID
		r.mu.Unlock()
		r.finishClaimedOrphan(claimID)
		return
	}
	r.mu.Unlock()

	remoteReaders, countErr := r.service.activeDistributedReaders(r.id)
	if countErr != nil {
		// A journal outage must not be interpreted as proof that a remote
		// reader disappeared. Retry without canceling the producer.
		r.service.logger.Debug("count distributed readers before orphan cancellation",
			"stream_id", r.id, "error", countErr)
		r.scheduleOrphanCheck(remoteReaderPollPeriod)
		return
	}
	if remoteReaders != 0 {
		r.mu.Lock()
		r.remoteReaderWasAttached = true
		r.mu.Unlock()
		r.scheduleOrphanCheck(remoteReaderPollPeriod)
		return
	}

	r.mu.Lock()
	needsRemoteGrace := r.orphanPolicy == OrphanCancelAfter && r.remoteReaderWasAttached
	r.remoteReaderWasAttached = false
	r.mu.Unlock()
	if needsRemoteGrace {
		r.scheduleOrphanCheck(r.service.config.OrphanTimeout)
		return
	}
	if err := r.finishErrorWhen(
		"orphan_cancelled",
		"producer canceled after its final reader disconnected",
		"orphan_policy",
		func() bool { return r.readers == 0 },
	); err != nil && !errors.Is(err, journal.ErrTerminalState) {
		r.service.logger.Error("close orphaned stream", "stream_id", r.id, "error", err)
	}
}

func (r *streamRuntime) finishClaimedOrphan(claimID string) {
	err := r.finishErrorWhen(
		"orphan_cancelled",
		"producer canceled after its final reader disconnected",
		"orphan_policy",
		func() bool { return r.readers == 0 && r.orphanClaim == claimID },
	)

	r.mu.Lock()
	owned := r.orphanClaim == claimID
	release := owned && r.terminal == ""
	if owned {
		r.orphanClaim = ""
	}
	r.mu.Unlock()
	if release {
		if releaseErr := r.service.releaseDistributedOrphanClaim(r.id, claimID); releaseErr != nil {
			r.service.logger.Debug("release abandoned orphan cancellation claim",
				"stream_id", r.id, "error", releaseErr)
		}
	}
	if err != nil && !errors.Is(err, journal.ErrTerminalState) {
		r.service.logger.Error("close claimed orphaned stream", "stream_id", r.id, "error", err)
	}
}

func (r *streamRuntime) scheduleOrphanCheck(delay time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.readers != 0 || r.terminal != "" || r.terminating || r.orphanClaim != "" {
		return
	}
	if r.orphanTimer != nil {
		r.orphanTimer.Stop()
	}
	r.orphanTimer = time.AfterFunc(delay, r.cancelOrphan)
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

func (s *durableService) maintainPendingIdempotencyLease(
	digest *journal.IdempotencyDigest,
	id journal.StreamID,
) func() {
	if digest == nil {
		return func() {}
	}
	registry, ok := s.idempotency.(journal.PendingIdempotencyRegistry)
	if !ok {
		return func() {}
	}
	leaseContext, cancelLease := context.WithCancel(s.rootContext)
	done := make(chan struct{})
	interval := s.config.JournalTTL / 4
	if interval > time.Second {
		interval = time.Second
	}
	if interval <= 0 {
		interval = time.Millisecond
	}
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				refreshContext, cancel := context.WithTimeout(leaseContext, s.config.ReadinessTimeout)
				refreshed, err := registry.RefreshPending(
					refreshContext, *digest, id, s.config.JournalTTL,
				)
				cancel()
				if err != nil {
					s.logger.Warn("refresh pending idempotency reservation",
						"stream_id", id, "error", err)
					continue
				}
				if !refreshed {
					return
				}
			case <-leaseContext.Done():
				return
			}
		}
	}()
	var stopOnce sync.Once
	return func() {
		stopOnce.Do(func() {
			cancelLease()
			<-done
		})
	}
}

func (s *durableService) releasePendingIdempotency(
	digest journal.IdempotencyDigest,
	id journal.StreamID,
) {
	ctx, cancel := context.WithTimeout(context.Background(), s.config.ReadinessTimeout)
	defer cancel()
	if registry, ok := s.idempotency.(journal.PendingIdempotencyRegistry); ok {
		_, _ = registry.ReleasePending(ctx, digest, id)
		return
	}
	_, _ = s.removeIdempotencyIfBound(ctx, digest, id)
}

func (s *durableService) removeIdempotencyIfBound(
	ctx context.Context,
	digest journal.IdempotencyDigest,
	id journal.StreamID,
) (bool, error) {
	registry, ok := s.idempotency.(journal.ConditionalIdempotencyRegistry)
	if !ok {
		return false, errors.New("idempotency registry does not support conditional cleanup")
	}
	return registry.RemoveIfBound(ctx, digest, id)
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

func (r *streamRuntime) maintainJournalLease(lease journal.ActiveJournalLease) {
	interval := r.service.config.JournalTTL / 2
	if interval <= 0 {
		interval = time.Millisecond
	}
	touchTimeout := r.service.config.ReadinessTimeout
	if touchTimeout > interval {
		touchTimeout = interval
	}
	if touchTimeout <= 0 {
		touchTimeout = time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(r.service.rootContext, touchTimeout)
			err := lease.Touch(ctx, r.id)
			cancel()
			if errors.Is(err, journal.ErrTerminalState) ||
				errors.Is(err, journal.ErrExpired) ||
				errors.Is(err, journal.ErrNotFound) {
				return
			}
			if err != nil && r.service.rootContext.Err() == nil {
				r.service.logger.Warn("refresh active journal lease",
					"stream_id", r.id, "error", err)
			}
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

func (s *durableService) waitActive(ctx context.Context) error {
	return waitForGroup(ctx, &s.active)
}

func (s *durableService) waitDegradation(ctx context.Context) error {
	return waitForGroup(ctx, &s.degradationLeases)
}

func waitForGroup(ctx context.Context, group *sync.WaitGroup) error {
	finished := make(chan struct{})
	go func() {
		group.Wait()
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
	s.journalDegraded.Store(1)
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
