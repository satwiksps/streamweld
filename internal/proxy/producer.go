package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/satwiksps/streamweld/internal/backend"
	"github.com/satwiksps/streamweld/internal/conformance"
	"github.com/satwiksps/streamweld/internal/journal"
	"github.com/satwiksps/streamweld/internal/migrate"
	"github.com/satwiksps/streamweld/internal/proxy/sse"
	"go.opentelemetry.io/otel/propagation"
)

type migrationCause struct {
	reason   string
	deadline time.Time
}

func (cause *migrationCause) Error() string { return "migrate producer: " + cause.reason }

type producerStartResult struct {
	rejection *upstreamRejection
	err       error
}

type attemptSpec struct {
	body             []byte
	continuation     bool
	seamBase         string
	migrationEntries []journal.Entry
	estimateWarning  bool
	migrationReason  string
	fromBackend      string
	toBackend        string
	rescuedTokens    uint64
	attempt          uint64
}

type attemptOutcome struct {
	terminal  bool
	trigger   string
	deadline  time.Time
	passive   bool
	rejection *upstreamRejection
	err       error
}

type bufferedAttemptFrame struct {
	event       sse.Event
	observation chunkObservation
}

func requestKindForEndpoint(endpoint string) migrate.RequestKind {
	if endpoint == "/v1/completions" {
		return migrate.RequestCompletion
	}
	return migrate.RequestChatCompletion
}

func requestHasMultipleChoices(body []byte) bool {
	var fields map[string]json.RawMessage
	if json.Unmarshal(body, &fields) != nil {
		return false
	}
	raw, ok := fields["n"]
	if !ok {
		return false
	}
	var choices uint64
	return json.Unmarshal(raw, &choices) == nil && choices != 1
}

func (r *streamRuntime) runProducer(rawQuery string, started chan<- producerStartResult) {
	defer r.endProducerSpan()
	signaled := false
	signal := func(result producerStartResult) {
		if signaled {
			return
		}
		signaled = true
		started <- result
	}
	defer func() { signal(producerStartResult{}) }()

	spec := attemptSpec{body: bytes.Clone(r.requestBody)}
	for {
		outcome := r.runAttempt(rawQuery, spec, signal)
		if outcome.err != nil {
			if terminalErr := r.finishError(
				"upstream_error",
				"the upstream producer failed before the stream could continue",
				"producer_failure",
			); terminalErr != nil && !errors.Is(terminalErr, journal.ErrTerminalState) {
				outcome.err = errors.Join(outcome.err, terminalErr)
			}
			signal(producerStartResult{err: outcome.err})
			return
		}
		if outcome.rejection != nil {
			signal(producerStartResult{rejection: outcome.rejection})
			return
		}
		if outcome.terminal {
			return
		}

		next, migrated := r.prepareMigration(outcome.trigger, outcome.passive, outcome.deadline)
		if !migrated {
			// Refusal commits its warning/error sequence before returning.
			signal(producerStartResult{})
			return
		}
		spec = next
	}
}

func (r *streamRuntime) runAttempt(
	rawQuery string,
	spec attemptSpec,
	signal func(producerStartResult),
) (outcome attemptOutcome) {
	r.mu.Lock()
	selected := r.currentBackend
	attempt := r.migrationsUsed + 1
	r.mu.Unlock()

	traceContext, attemptSpan := r.service.telemetry.StartAttempt(
		r.context,
		r.telemetryLabels(),
		operationForEndpoint(r.endpoint),
		selected.ID.String(),
		selected.URL.Hostname(),
		upstreamServerPort(selected.URL.Scheme, selected.URL.Port()),
		attempt,
		spec.continuation,
	)
	defer func() {
		trigger := outcome.trigger
		if outcome.rejection != nil && trigger == "" {
			trigger = "upstream_rejected"
		}
		r.service.telemetry.EndAttempt(attemptSpan, outcome.err, trigger)
	}()

	attemptContext, attemptCancel := context.WithCancelCause(traceContext)
	r.mu.Lock()
	r.attemptCancel = attemptCancel
	pending := r.pendingTrigger
	pendingDeadline := r.pendingDeadline
	r.pendingTrigger = ""
	r.pendingDeadline = time.Time{}
	r.pendingFallback = false
	r.mu.Unlock()
	if pending != "" {
		attemptCancel(&migrationCause{reason: pending, deadline: pendingDeadline})
	}
	if context.Cause(attemptContext) != nil {
		return r.outcomeForAttemptError(attemptContext, "tcp_reset")
	}

	var stallTimer *time.Timer
	resetStall := func() {
		if !r.service.config.StallDetectionEnabled {
			return
		}
		if stallTimer == nil {
			stallTimer = time.AfterFunc(r.service.config.StallTimeout, func() {
				attemptCancel(&migrationCause{reason: "stall"})
			})
			return
		}
		stallTimer.Reset(r.service.config.StallTimeout)
	}
	resetStall()
	defer func() {
		if stallTimer != nil {
			stallTimer.Stop()
		}
		attemptCancel(nil)
	}()

	upstreamURL := joinUpstreamURL(selected.URL, r.endpoint, rawQuery)
	request, err := http.NewRequestWithContext(
		attemptContext,
		http.MethodPost,
		upstreamURL.String(),
		bytes.NewReader(spec.body),
	)
	if err != nil {
		return attemptOutcome{err: fmt.Errorf("construct upstream request: %w", err)}
	}
	request.Header = r.requestHeader.Clone()
	propagation.TraceContext{}.Inject(attemptContext, propagation.HeaderCarrier(request.Header))
	request.ContentLength = int64(len(spec.body))
	request.Host = selected.URL.Host

	client := &http.Client{
		Transport: r.service.transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	if spec.continuation && !r.markAttemptDispatched() {
		return attemptOutcome{terminal: true}
	}
	response, err := client.Do(request)
	if spec.continuation {
		if commitErr := r.commitDispatchedAttempt(spec); commitErr != nil {
			if context.Cause(r.context) == nil {
				_ = r.finishError("journal_capacity_exceeded", "durable journal could not record migration", "journal_capacity")
			}
			if response != nil && response.Body != nil {
				_ = response.Body.Close()
			}
			return attemptOutcome{terminal: true, trigger: "journal_capacity"}
		}
		signal(producerStartResult{})
	}
	if err != nil {
		return r.outcomeForAttemptError(attemptContext, "tcp_reset")
	}
	defer func() {
		if closeErr := response.Body.Close(); closeErr != nil {
			r.service.logger.Warn("close upstream stream", "stream_id", r.id, "error", closeErr)
		}
	}()

	if response.StatusCode >= http.StatusBadRequest && response.StatusCode < http.StatusInternalServerError {
		body := readBoundedErrorBody(response.Body)
		if !spec.continuation {
			if closeErr := r.finishError("upstream_error", "upstream rejected the request", "upstream_rejected"); closeErr != nil {
				return attemptOutcome{err: closeErr}
			}
			return attemptOutcome{terminal: true, trigger: "upstream_rejected", rejection: &upstreamRejection{status: response.StatusCode, body: body}}
		}
		if closeErr := r.finishError("upstream_error", "continuation request was rejected", "upstream_rejected"); closeErr != nil {
			return attemptOutcome{err: closeErr}
		}
		return attemptOutcome{terminal: true, trigger: "upstream_rejected"}
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		_, _ = io.CopyN(io.Discard, response.Body, maxUpstreamErrorBytes)
		return attemptOutcome{trigger: "upstream_5xx", passive: true}
	}

	contentType, _, contentTypeErr := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if contentTypeErr != nil || !strings.EqualFold(contentType, "text/event-stream") {
		_, _ = io.CopyN(io.Discard, response.Body, maxUpstreamErrorBytes)
		if closeErr := r.finishError("upstream_error", "upstream response was not an event stream", "invalid_content_type"); closeErr != nil {
			return attemptOutcome{err: closeErr}
		}
		return attemptOutcome{terminal: true, trigger: "invalid_content_type"}
	}

	decoder, decoderErr := sse.NewDecoderWithOptions(
		response.Body,
		sse.WithMaxEventBytes(r.service.config.MaxSSEEventBytes),
	)
	if decoderErr != nil {
		return attemptOutcome{err: fmt.Errorf("configure upstream SSE decoder: %w", decoderErr)}
	}

	frames := make([]bufferedAttemptFrame, 0, 4)
	var continuationText []byte
	var bufferedBytes int64
	maxBufferBytes := int64(max(r.service.config.MaxSSEEventBytes, r.policy.SeamWindowBytes))
	flushContinuation := func() bool {
		if !spec.continuation || len(frames) == 0 {
			return true
		}
		if err := r.flushSeam(spec.seamBase, frames, continuationText); err != nil {
			if context.Cause(r.context) == nil {
				r.service.logger.Error("reconcile continuation seam", "stream_id", r.id, "error", err)
				r.recordMigrationRefusal("unsupported_continuation_shape")
				_ = r.finishError("migration_refused", "continuation seam could not be reconciled", "unsupported_continuation_shape")
			}
			return false
		}
		frames = nil
		continuationText = nil
		spec.continuation = false
		signal(producerStartResult{})
		return true
	}
	for {
		event, decodeErr := decoder.Decode()
		if decodeErr != nil {
			// A failed attempt may have produced fewer text bytes than the seam
			// window. Commit its complete frames before evaluating the next
			// migration so text, usage, and tool-call boundaries are retained.
			if context.Cause(r.context) == nil && !flushContinuation() {
				return attemptOutcome{terminal: true, trigger: "unsupported_continuation_shape"}
			}
			return r.outcomeForAttemptError(attemptContext, classifyReadFailure(decodeErr))
		}
		if !event.HasData {
			continue
		}
		resetStall()
		if bytes.Equal(bytes.TrimSpace(event.Data), []byte(doneSentinelData)) {
			if !flushContinuation() {
				return attemptOutcome{terminal: true, trigger: "unsupported_continuation_shape"}
			}
			if closeErr := r.finishDone(); closeErr != nil {
				return attemptOutcome{err: closeErr}
			}
			signal(producerStartResult{})
			return attemptOutcome{terminal: true}
		}

		observation, observeErr := observeOpenAIChunk(event.Data)
		if observeErr != nil {
			if !flushContinuation() {
				return attemptOutcome{terminal: true, trigger: "unsupported_continuation_shape"}
			}
			if closeErr := r.finishError("upstream_error", "upstream emitted an invalid chunk", "invalid_chunk"); closeErr != nil {
				return attemptOutcome{err: errors.Join(observeErr, closeErr)}
			}
			return attemptOutcome{terminal: true, trigger: "invalid_chunk"}
		}
		if len(observation.ErrorPayload) != 0 {
			if !flushContinuation() {
				return attemptOutcome{terminal: true, trigger: "unsupported_continuation_shape"}
			}
			return attemptOutcome{trigger: "error_chunk", passive: true}
		}

		if spec.continuation {
			frames = append(frames, bufferedAttemptFrame{event: event, observation: observation})
			continuationText = append(continuationText, observation.TextDelta...)
			if len(continuationText) < r.policy.SeamWindowBytes {
				// Textless metadata and tool deltas must not grow the seam
				// buffer indefinitely. Include retained metadata and a frame
				// allowance so tiny data payloads cannot bypass this bound.
				bufferedBytes += int64(len(event.Data)+len(event.Type)+len(event.ID)+len(observation.TextDelta)) + 256
				for _, comment := range event.Comments {
					bufferedBytes += int64(len(comment)) + 16
				}
				if bufferedBytes > maxBufferBytes {
					r.recordMigrationRefusal("unsupported_continuation_shape")
					_ = r.appendNonTerminal([]journal.Entry{warningEntry(
						"unsupported_continuation_shape", "continuation exceeded the seam buffering limit before producing enough text", nil,
					)})
					_ = r.finishError("migration_refused", "continuation seam buffer exceeded its limit", "unsupported_continuation_shape")
					return attemptOutcome{terminal: true, trigger: "unsupported_continuation_shape"}
				}
				continue
			}
			if !flushContinuation() {
				return attemptOutcome{terminal: true, trigger: "unsupported_continuation_shape"}
			}
			continue
		}

		if appendErr := r.appendChunk(event, observation); appendErr != nil {
			if context.Cause(r.context) == nil {
				r.service.logger.Error("append durable chunk", "stream_id", r.id, "error", appendErr)
				_ = r.finishError("journal_capacity_exceeded", "durable journal could not accept a chunk", "journal_capacity")
			}
			return attemptOutcome{terminal: true, trigger: "journal_capacity"}
		}
		signal(producerStartResult{})
	}
}

func upstreamServerPort(scheme, value string) int {
	if value != "" {
		port, err := strconv.Atoi(value)
		if err == nil && port > 0 && port <= 65535 {
			return port
		}
		return 0
	}
	if scheme == "https" {
		return 443
	}
	if scheme == "http" {
		return 80
	}
	return 0
}

func (r *streamRuntime) outcomeForAttemptError(
	attemptContext context.Context,
	fallbackReason string,
) attemptOutcome {
	if context.Cause(r.context) != nil {
		r.finishCanceledProducer()
		return attemptOutcome{terminal: true}
	}
	var cause *migrationCause
	if errors.As(context.Cause(attemptContext), &cause) {
		return attemptOutcome{trigger: cause.reason, deadline: cause.deadline, passive: cause.reason == "stall"}
	}
	return attemptOutcome{trigger: fallbackReason, passive: true, err: nil}
}

func classifyReadFailure(err error) string {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, sse.ErrIncompleteEvent) {
		return "unexpected_eof"
	}
	return "tcp_reset"
}

func (r *streamRuntime) prepareMigration(
	reason string,
	passive bool,
	deadline time.Time,
) (attemptSpec, bool) {
	r.mu.Lock()
	if r.terminal != "" || r.terminating || context.Cause(r.context) != nil {
		r.mu.Unlock()
		return attemptSpec{}, false
	}
	failedLease := r.currentLease
	failedBackend := r.currentBackend
	accumulated, usage := r.progress.Snapshot()
	insideToolCall := r.toolCalls.InsideToolCall() || r.toolTrackFailed
	migrationsUsed := r.migrationsUsed
	createdAt := r.createdAt
	estimateWarning := usage.Estimated && !r.estimateWarned
	finishReason := r.finishReason
	r.mu.Unlock()

	correctness := migrate.EvaluateCorrectness(migrate.CorrectnessSnapshot{
		ToolCallInProgress: insideToolCall,
		StructuredResponse: r.structured,
		AccumulatedText:    []byte(accumulated),
		MultipleChoices:    r.multipleChoice,
	})

	// A completed choice or an exact exhausted budget needs no new producer.
	// Resolve this before target selection: a rollout can interrupt the small
	// window between the final token/finish reason and the [DONE] sentinel.
	rewritten, rewriteErr := migrate.RewriteContinuation(r.requestKind, r.requestBody, migrate.ContinuationOptions{
		AccumulatedText:      accumulated,
		TokensAlreadyEmitted: usage.CompletionTokens,
	})
	if completedReason, completed := completedGenerationReason(finishReason, usage, correctness, rewriteErr); completed {
		if err := r.finishDoneUntil(deadline, completedReason); err != nil && !errors.Is(err, journal.ErrTerminalState) {
			r.service.logger.Error("close completed generation", "stream_id", r.id, "error", err)
		}
		return attemptSpec{}, false
	}
	if passive {
		_, _ = r.service.backends.MarkPassiveFailure(failedBackend.ID)
	}
	if errors.Is(rewriteErr, migrate.ErrTokenBudgetExhausted) {
		if !correctness.Eligible() {
			r.refuseMigrationUntil(deadline, estimateWarning, nil, correctness.Failures)
		} else {
			r.refuseMigrationUntil(deadline, estimateWarning, nil, []migrate.CorrectnessFailure{migrate.FailureTokenBudgetExhausted})
		}
		return attemptSpec{}, false
	}

	policy := migrate.Policy{
		MaxMigrations:         uint64(r.policy.MaxMigrations),
		MaxMigrationTokens:    r.policy.MaxMigrationTokens,
		MaxStreamDuration:     r.policy.MaxStreamDuration,
		AllowStructuredResume: r.policy.AllowStructuredResume,
		AllowCrossVersion:     r.policy.AllowCrossVersion,
		SeamWindowBytes:       r.policy.SeamWindowBytes,
		TemplateMode:          r.policy.TemplateMode,
	}
	if reason == "drain" {
		preflight, preflightErr := migrate.EvaluateEligibility(policy, migrate.EligibilitySnapshot{
			MigrationsUsed:         migrationsUsed,
			AccumulatedTokens:      usage.CompletionTokens,
			Elapsed:                time.Since(createdAt),
			TemplateVerdict:        conformance.VerdictSafe,
			StructuredResponse:     r.structured,
			OriginModelVersion:     "drain-preflight",
			TargetModelVersion:     "drain-preflight",
			TargetBackendAvailable: true,
		})
		if preflightErr != nil {
			r.service.logger.Error("evaluate migration policy", "stream_id", r.id, "error", preflightErr)
			r.recordMigrationRefusal("invalid_policy")
			_ = r.finishError("migration_refused", "migration policy could not be evaluated", "invalid_policy")
			return attemptSpec{}, false
		}
		if !preflight.Eligible() || !correctness.Eligible() {
			r.refuseMigrationUntil(deadline, estimateWarning, preflight.Failures, correctness.Failures)
			return attemptSpec{}, false
		}
	}

	retryDeadline := deadline
	if reason == "drain" && !retryDeadline.IsZero() {
		maxDurationDeadline := createdAt.Add(r.policy.MaxStreamDuration)
		if maxDurationDeadline.Before(retryDeadline) {
			retryDeadline = maxDurationDeadline
		}
	}
	targetLease, acquireErr := r.acquireMigrationTarget(reason, retryDeadline, failedBackend.ID)
	if acquireErr != nil && context.Cause(r.context) != nil {
		r.finishCanceledProducer()
		return attemptSpec{}, false
	}
	targetAvailable := acquireErr == nil
	target := backend.State{
		Backend: backend.Backend{
			ModelVersion:    failedBackend.ModelVersion,
			TemplateVerdict: failedBackend.TemplateVerdict,
		},
	}
	if targetAvailable {
		target = targetLease.Backend()
	}

	eligibility, eligibilityErr := migrate.EvaluateEligibility(policy, migrate.EligibilitySnapshot{
		MigrationsUsed:         migrationsUsed,
		AccumulatedTokens:      usage.CompletionTokens,
		Elapsed:                time.Since(createdAt),
		TemplateVerdict:        target.TemplateVerdict,
		StructuredResponse:     r.structured,
		OriginModelVersion:     r.originBackend.ModelVersion,
		TargetModelVersion:     target.ModelVersion,
		TargetBackendAvailable: targetAvailable,
	})
	if eligibilityErr != nil {
		if targetLease != nil {
			targetLease.Release()
		}
		r.service.logger.Error("evaluate migration policy", "stream_id", r.id, "error", eligibilityErr)
		r.recordMigrationRefusal("invalid_policy")
		_ = r.finishError("migration_refused", "migration policy could not be evaluated", "invalid_policy")
		return attemptSpec{}, false
	}

	if !eligibility.Eligible() || !correctness.Eligible() {
		if targetLease != nil {
			targetLease.Release()
		}
		r.refuseMigrationUntil(deadline, estimateWarning, eligibility.Failures, correctness.Failures)
		return attemptSpec{}, false
	}

	if rewriteErr != nil {
		targetLease.Release()
		r.refuseMigrationUntil(deadline, estimateWarning, nil, []migrate.CorrectnessFailure{migrate.FailureUnsupportedContinuationShape})
		return attemptSpec{}, false
	}

	entries := make([]journal.Entry, 0, len(eligibility.Warnings)+2)
	if estimateWarning {
		entries = append(entries, warningEntry("token_count_estimated", "completion token count is a conservative estimate", nil))
	}
	for _, warning := range eligibility.Warnings {
		entries = append(entries, warningEntry(string(warning), warningMessage(string(warning)), nil))
	}
	migrationPayload, marshalErr := json.Marshal(struct {
		FromBackend         string `json:"from_backend"`
		ToBackend           string `json:"to_backend"`
		Reason              string `json:"reason"`
		RescuedTokens       uint64 `json:"rescued_tokens"`
		TokenCountEstimated bool   `json:"token_count_estimated"`
		Attempt             uint64 `json:"attempt"`
	}{
		FromBackend:         failedBackend.ID.String(),
		ToBackend:           target.ID.String(),
		Reason:              reason,
		RescuedTokens:       usage.CompletionTokens,
		TokenCountEstimated: usage.Estimated,
		Attempt:             migrationsUsed + 2,
	})
	if marshalErr != nil {
		targetLease.Release()
		r.recordMigrationRefusal("unsupported_continuation_shape")
		_ = r.finishError("migration_refused", "migration metadata could not be encoded", "unsupported_continuation_shape")
		return attemptSpec{}, false
	}
	entries = append(entries, journal.Entry{Kind: journal.KindMigration, Payload: migrationPayload})

	if err := r.reserveMigrationTarget(failedLease, targetLease, target); err != nil {
		targetLease.Release()
		return attemptSpec{}, false
	}
	return attemptSpec{
		body:             rewritten,
		continuation:     true,
		seamBase:         accumulated,
		migrationEntries: entries,
		estimateWarning:  estimateWarning,
		migrationReason:  reason,
		fromBackend:      failedBackend.ID.String(),
		toBackend:        target.ID.String(),
		rescuedTokens:    usage.CompletionTokens,
		attempt:          migrationsUsed + 2,
	}, true
}

func completedGenerationReason(
	finishReason string,
	usage tokenUsage,
	correctness migrate.CorrectnessResult,
	rewriteErr error,
) (string, bool) {
	if !correctness.Eligible() || (rewriteErr != nil && !errors.Is(rewriteErr, migrate.ErrTokenBudgetExhausted)) {
		return "", false
	}
	if finishReason != "" {
		return finishReason, true
	}
	if !usage.Estimated && errors.Is(rewriteErr, migrate.ErrTokenBudgetExhausted) {
		return "length", true
	}
	return "", false
}

func (r *streamRuntime) acquireMigrationTarget(
	reason string,
	deadline time.Time,
	excluded backend.ID,
) (*backend.Lease, error) {
	for {
		changed := r.service.backends.Changes()
		var quarantineTimer *time.Timer
		var quarantineExpired <-chan time.Time
		if delay, exists, delayErr := r.service.backends.NextQuarantineExpiry(r.model, excluded); delayErr == nil && exists {
			quarantineTimer = time.NewTimer(delay)
			quarantineExpired = quarantineTimer.C
		}
		lease, err := r.service.acquireBackend(r.id.String(), r.model, excluded)
		if err == nil || reason != "drain" || !errors.Is(err, backend.ErrNoEligibleBackend) || deadline.IsZero() {
			if quarantineTimer != nil {
				quarantineTimer.Stop()
			}
			return lease, err
		}
		if !time.Now().Before(deadline) {
			if quarantineTimer != nil {
				quarantineTimer.Stop()
			}
			// Recheck once at the boundary so simultaneous admission wins over
			// the timeout rather than producing a false backend_available refusal.
			return r.service.acquireBackend(r.id.String(), r.model, excluded)
		}
		deadlineTimer := time.NewTimer(time.Until(deadline))
		select {
		case <-r.context.Done():
		case <-changed:
		case <-quarantineExpired:
		case <-deadlineTimer.C:
		}
		if quarantineTimer != nil && !quarantineTimer.Stop() {
			select {
			case <-quarantineTimer.C:
			default:
			}
		}
		if !deadlineTimer.Stop() {
			select {
			case <-deadlineTimer.C:
			default:
			}
		}
		if context.Cause(r.context) != nil {
			return nil, context.Cause(r.context)
		}
	}
}

func (r *streamRuntime) reserveMigrationTarget(
	failedLease *backend.Lease,
	targetLease *backend.Lease,
	target backend.State,
) error {
	r.mu.Lock()
	if r.terminal != "" || r.terminating || r.currentLease != failedLease {
		r.mu.Unlock()
		return journal.ErrTerminalState
	}
	r.currentLease = targetLease
	r.currentBackend = target
	r.attemptCancel = nil
	r.mu.Unlock()
	failedLease.Release()

	latest, err := r.service.backends.Get(target.ID)
	reason := ""
	if err != nil || latest.Health != backend.HealthHealthy || latest.Quarantined {
		reason = "health"
	} else if latest.Draining {
		reason = "drain"
	}
	if reason != "" {
		r.mu.Lock()
		if r.currentLease == targetLease {
			deadline := time.Time{}
			fallback := false
			if reason == "drain" {
				deadline = time.Now().Add(defaultDrainTimeout)
				fallback = true
			}
			r.mergePendingTriggerLocked(reason, deadline, fallback)
		}
		r.mu.Unlock()
	}
	return nil
}

func (r *streamRuntime) markAttemptDispatched() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.terminal != "" || r.terminating || context.Cause(r.context) != nil {
		return false
	}
	r.migrationsUsed++
	r.progress.BeginAttempt()
	r.attemptPromptObserved = 0
	return true
}

func (r *streamRuntime) commitDispatchedAttempt(spec attemptSpec) error {
	if err := r.appendNonTerminal(spec.migrationEntries); err != nil {
		return err
	}
	if spec.estimateWarning {
		r.mu.Lock()
		r.estimateWarned = true
		r.mu.Unlock()
	}
	r.service.telemetry.Migration(
		r.telemetryLabels(), metricMigrationReason(spec.migrationReason), spec.rescuedTokens,
	)
	r.service.telemetry.RecordMigration(
		r.span,
		spec.fromBackend,
		spec.toBackend,
		spec.migrationReason,
		spec.rescuedTokens,
		spec.attempt,
	)
	return nil
}

func metricMigrationReason(reason string) string {
	switch reason {
	case "drain", "stall", "error_chunk", "health":
		return reason
	default:
		return "crash"
	}
}

func (r *streamRuntime) refuseMigrationUntil(
	deadline time.Time,
	estimateWarning bool,
	predicates []migrate.Predicate,
	correctness []migrate.CorrectnessFailure,
) {
	r.recordMigrationRefusals(predicates, correctness)
	entries, reason := buildRefusalEntries(estimateWarning, predicates, correctness)
	if err := r.appendNonTerminalUntil(entries, deadline); err != nil && context.Cause(r.context) == nil {
		r.service.logger.Error("append migration refusal warnings", "stream_id", r.id, "error", err)
	} else if estimateWarning {
		r.mu.Lock()
		r.estimateWarned = true
		r.mu.Unlock()
	}
	if err := r.finishErrorUntil(deadline, "migration_refused", "continuation is not safe", reason); err != nil &&
		!errors.Is(err, journal.ErrTerminalState) {
		r.service.logger.Error("close refused migration", "stream_id", r.id, "error", err)
	}
}

func (r *streamRuntime) recordMigrationRefusals(
	predicates []migrate.Predicate,
	correctness []migrate.CorrectnessFailure,
) {
	for _, predicate := range predicates {
		r.recordMigrationRefusal(string(predicate))
	}
	for _, failure := range correctness {
		r.recordMigrationRefusal(string(failure))
	}
	if len(predicates) == 0 && len(correctness) == 0 {
		r.recordMigrationRefusal("migration_ineligible")
	}
}

func (r *streamRuntime) recordMigrationRefusal(predicate string) {
	r.service.telemetry.MigrationRefused(r.telemetryLabels(), predicate)
}

func buildRefusalEntries(
	estimateWarning bool,
	predicates []migrate.Predicate,
	correctness []migrate.CorrectnessFailure,
) ([]journal.Entry, string) {
	entries := make([]journal.Entry, 0, len(predicates)+len(correctness)+1)
	if estimateWarning {
		entries = append(entries, warningEntry("token_count_estimated", "completion token count is a conservative estimate", nil))
	}
	for _, predicate := range predicates {
		value := string(predicate)
		entries = append(entries, warningEntry(
			"eligibility_failed",
			"migration eligibility predicate failed",
			&value,
		))
	}
	for _, failure := range correctness {
		entries = append(entries, warningEntry(string(failure), warningMessage(string(failure)), nil))
	}
	reason := "migration_ineligible"
	if len(correctness) != 0 {
		reason = string(correctness[0])
	} else if len(predicates) != 0 {
		reason = string(predicates[0])
	}
	return entries, reason
}

func warningEntry(code, message string, predicate *string) journal.Entry {
	payload, _ := json.Marshal(struct {
		Code      string         `json:"code"`
		Message   string         `json:"message"`
		Predicate *string        `json:"predicate"`
		Details   map[string]any `json:"details"`
	}{Code: code, Message: message, Predicate: predicate, Details: map[string]any{}})
	return journal.Entry{Kind: journal.KindWarning, Payload: payload}
}

func warningMessage(code string) string {
	switch code {
	case "tool_call_boundary":
		return "migration would split a fragmented tool call"
	case "structured_prefix_invalid":
		return "accumulated structured output is not a valid JSON prefix"
	case "unsupported_continuation_shape":
		return "the request cannot be represented as one continuation"
	case "token_budget_exhausted":
		return "the estimated completion-token count leaves no safe continuation budget"
	case "template_degraded":
		return "the target chat template has degraded continuation conformance"
	case "template_unsafe_permissive":
		return "an unsafe target chat template was admitted by permissive policy"
	case "seam_anomaly":
		return "continuation may have restarted instead of continuing"
	default:
		return code
	}
}

func (r *streamRuntime) appendNonTerminal(entries []journal.Entry) error {
	return r.appendNonTerminalUntil(entries, time.Time{})
}

func (r *streamRuntime) appendNonTerminalUntil(entries []journal.Entry, deadline time.Time) error {
	if len(entries) == 0 {
		return nil
	}
	r.writeMu.Lock()
	defer r.writeMu.Unlock()
	r.mu.Lock()
	if r.terminal != "" || r.terminating {
		r.mu.Unlock()
		return journal.ErrTerminalState
	}
	degraded := r.degraded
	r.mu.Unlock()
	for index, entry := range entries {
		if degraded {
			frames, err := degradedFramesForEntries(entries[index:])
			if err != nil {
				return err
			}
			return r.degradedFeed.append(r.context, frames...)
		}
		mutationContext, cancelMutation := r.journalMutationContextUntil(deadline)
		seq, err := r.service.journal.Append(mutationContext, r.id, entry)
		cancelMutation()
		if err != nil {
			frames, frameErr := degradedFramesForEntries(entries[index:])
			if frameErr != nil {
				return errors.Join(err, frameErr)
			}
			r.enterJournalDegradedLocked(err, frames...)
			return nil
		}
		entry.Seq = seq
		r.mu.Lock()
		r.lastSeq = seq
		r.mu.Unlock()
		r.recordJournalCommit(int64(len(entry.Payload)))
		if err := r.degradedFeed.publishCommitted(r.context, entry); err != nil {
			return err
		}
		r.publishFirst(entry)
	}
	return nil
}

func degradedFramesForEntries(entries []journal.Entry) ([]degradedFrame, error) {
	frames := make([]degradedFrame, 0, len(entries))
	for _, entry := range entries {
		frame, err := degradedEntryFrame(entry)
		if err != nil {
			return nil, err
		}
		frames = append(frames, frame)
	}
	return frames, nil
}

func (r *streamRuntime) flushSeam(
	accumulated string,
	frames []bufferedAttemptFrame,
	continuation []byte,
) error {
	result, err := migrate.ReconcileSeam(
		[]byte(accumulated),
		continuation,
		r.policy.SeamWindowBytes,
	)
	if err != nil {
		return err
	}
	r.service.telemetry.SeamOverlap(r.telemetryLabels(), result.OverlapBytes)
	if result.Anomaly {
		if err := r.appendNonTerminal([]journal.Entry{
			warningEntry("seam_anomaly", warningMessage("seam_anomaly"), nil),
		}); err != nil {
			return err
		}
	}
	remaining := result.OverlapBytes
	for _, frame := range frames {
		event := frame.event
		if remaining != 0 {
			rewritten, rewriteErr := rewriteLeadingAssistantText(event.Data, &remaining)
			if rewriteErr != nil {
				return rewriteErr
			}
			event.Data = rewritten
		}
		observation, observeErr := observeOpenAIChunk(event.Data)
		if observeErr != nil {
			return observeErr
		}
		if err := r.appendChunk(event, observation); err != nil {
			return err
		}
	}
	if remaining != 0 {
		return errors.New("seam overlap exceeded buffered assistant text")
	}
	return nil
}

func rewriteLeadingAssistantText(data []byte, remaining *int) ([]byte, error) {
	var root map[string]json.RawMessage
	if err := json.Unmarshal(data, &root); err != nil {
		return nil, err
	}
	var choices []map[string]json.RawMessage
	if raw, ok := root["choices"]; ok {
		if err := json.Unmarshal(raw, &choices); err != nil {
			return nil, err
		}
	}
	for _, choice := range choices {
		var index int
		if raw, ok := choice["index"]; !ok || json.Unmarshal(raw, &index) != nil || index != 0 {
			continue
		}
		if raw, ok := choice["text"]; ok && !bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			var text string
			if err := json.Unmarshal(raw, &text); err != nil {
				return nil, err
			}
			text, err := stripTextPrefix(text, remaining)
			if err != nil {
				return nil, err
			}
			choice["text"], _ = json.Marshal(text)
		}
		if raw, ok := choice["delta"]; ok && !bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			var delta map[string]json.RawMessage
			if err := json.Unmarshal(raw, &delta); err != nil {
				return nil, err
			}
			if contentRaw, ok := delta["content"]; ok && !bytes.Equal(bytes.TrimSpace(contentRaw), []byte("null")) {
				var content string
				if err := json.Unmarshal(contentRaw, &content); err != nil {
					return nil, err
				}
				content, err := stripTextPrefix(content, remaining)
				if err != nil {
					return nil, err
				}
				delta["content"], _ = json.Marshal(content)
			}
			choice["delta"], _ = json.Marshal(delta)
		}
	}
	encodedChoices, err := json.Marshal(choices)
	if err != nil {
		return nil, err
	}
	root["choices"] = encodedChoices
	return json.Marshal(root)
}

func stripTextPrefix(text string, remaining *int) (string, error) {
	if *remaining == 0 {
		return text, nil
	}
	remove := min(len(text), *remaining)
	if !isUTF8Boundary(text, remove) {
		return "", errors.New("seam overlap ended inside a UTF-8 rune")
	}
	*remaining -= remove
	return text[remove:], nil
}

func isUTF8Boundary(value string, offset int) bool {
	return offset == 0 || offset == len(value) || (value[offset]&0xc0) != 0x80
}

func (r *streamRuntime) triggerMigrationUntil(reason string, id backend.ID, deadline time.Time) bool {
	if reason == "drain" || reason == "health" {
		if r.refuseExternalTriggerIfIneligible(id, reason == "drain", deadline) {
			return true
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.terminal != "" || r.terminating || r.currentBackend.ID != id {
		return false
	}
	if r.attemptCancel == nil {
		r.mergePendingTriggerLocked(reason, deadline, false)
		return true
	}
	r.attemptCancel(&migrationCause{reason: reason, deadline: deadline})
	return true
}

func (r *streamRuntime) mergePendingTriggerLocked(reason string, deadline time.Time, fallback bool) {
	if r.pendingTrigger == "" {
		r.pendingTrigger = reason
		r.pendingDeadline = deadline
		r.pendingFallback = fallback
		return
	}
	if r.pendingTrigger != "drain" || reason != "drain" {
		return
	}
	if r.pendingFallback && !fallback {
		r.pendingDeadline = deadline
		r.pendingFallback = false
		return
	}
	if r.pendingFallback == fallback &&
		(r.pendingDeadline.IsZero() || (!deadline.IsZero() && deadline.Before(r.pendingDeadline))) {
		r.pendingDeadline = deadline
	}
}

// refuseExternalTriggerIfIneligible handles the drain/health ordering rule:
// an attempt that cannot migrate receives its durable refusal before its
// upstream context is canceled. Eligible attempts return to the normal
// attempt-cancel path so the producer goroutine owns the handoff.
func (r *streamRuntime) refuseExternalTriggerIfIneligible(
	id backend.ID,
	deferUnavailable bool,
	deadline time.Time,
) bool {
	r.mu.Lock()
	if r.terminal != "" || r.terminating || r.currentBackend.ID != id {
		r.mu.Unlock()
		return false
	}
	failed := r.currentBackend
	accumulated, usage := r.progress.Snapshot()
	insideToolCall := r.toolCalls.InsideToolCall() || r.toolTrackFailed
	migrationsUsed := r.migrationsUsed
	createdAt := r.createdAt
	estimateWarning := usage.Estimated && !r.estimateWarned
	finishReason := r.finishReason
	r.mu.Unlock()

	correctness := migrate.EvaluateCorrectness(migrate.CorrectnessSnapshot{
		ToolCallInProgress: insideToolCall,
		StructuredResponse: r.structured,
		AccumulatedText:    []byte(accumulated),
		MultipleChoices:    r.multipleChoice,
	})
	_, rewriteErr := migrate.RewriteContinuation(r.requestKind, r.requestBody, migrate.ContinuationOptions{
		AccumulatedText: accumulated, TokensAlreadyEmitted: usage.CompletionTokens,
	})
	if _, completed := completedGenerationReason(finishReason, usage, correctness, rewriteErr); completed {
		// Let the producer cancel its attempt, flush any accepted continuation
		// frames, and confirm completion without requiring migration eligibility.
		return false
	}

	targetLease, acquireErr := r.service.acquireBackend(r.id.String(), r.model, failed.ID)
	targetAvailable := acquireErr == nil
	target := backend.State{Backend: backend.Backend{
		ModelVersion: failed.ModelVersion, TemplateVerdict: failed.TemplateVerdict,
	}}
	if targetAvailable {
		target = targetLease.Backend()
		targetLease.Release()
	}
	policy := migrate.Policy{
		MaxMigrations:         uint64(r.policy.MaxMigrations),
		MaxMigrationTokens:    r.policy.MaxMigrationTokens,
		MaxStreamDuration:     r.policy.MaxStreamDuration,
		AllowStructuredResume: r.policy.AllowStructuredResume,
		AllowCrossVersion:     r.policy.AllowCrossVersion,
		SeamWindowBytes:       r.policy.SeamWindowBytes,
		TemplateMode:          r.policy.TemplateMode,
	}
	eligibility, err := migrate.EvaluateEligibility(policy, migrate.EligibilitySnapshot{
		MigrationsUsed:         migrationsUsed,
		AccumulatedTokens:      usage.CompletionTokens,
		Elapsed:                time.Since(createdAt),
		TemplateVerdict:        target.TemplateVerdict,
		StructuredResponse:     r.structured,
		OriginModelVersion:     r.originBackend.ModelVersion,
		TargetModelVersion:     target.ModelVersion,
		TargetBackendAvailable: targetAvailable,
	})
	if err != nil {
		return false
	}
	if deferUnavailable && errors.Is(acquireErr, backend.ErrNoEligibleBackend) &&
		correctness.Eligible() && len(eligibility.Failures) == 1 &&
		eligibility.Failures[0] == migrate.PredicateBackendAvailable {
		return false
	}
	if eligibility.Eligible() && correctness.Eligible() {
		return false
	}
	entries, refusalReason := buildRefusalEntries(estimateWarning, eligibility.Failures, correctness.Failures)
	closed := r.closeExternalMigrationRefusal(id, entries, refusalReason, estimateWarning, deadline)
	if closed {
		r.recordMigrationRefusals(eligibility.Failures, correctness.Failures)
	}
	return closed
}

func (r *streamRuntime) closeExternalMigrationRefusal(
	id backend.ID,
	warnings []journal.Entry,
	reason string,
	estimateWarning bool,
	deadline time.Time,
) bool {
	r.mu.Lock()
	if r.terminal != "" || r.terminating || r.currentBackend.ID != id {
		r.mu.Unlock()
		return false
	}
	r.terminating = true
	r.mu.Unlock()

	r.writeMu.Lock()
	committed := make([]journal.Entry, 0, len(warnings))
	for _, entry := range warnings {
		mutationContext, cancelMutation := r.journalMutationContextUntil(deadline)
		seq, err := r.service.journal.Append(mutationContext, r.id, entry)
		cancelMutation()
		if err != nil {
			r.writeMu.Unlock()
			r.failTerminalTransition()
			return false
		}
		entry.Seq = seq
		committed = append(committed, entry)
		r.mu.Lock()
		r.lastSeq = seq
		r.mu.Unlock()
		r.recordJournalCommit(int64(len(entry.Payload)))
		if relayErr := r.degradedFeed.publishCommitted(r.context, entry); relayErr != nil {
			r.writeMu.Unlock()
			r.failTerminalTransition()
			return false
		}
	}
	r.mu.Lock()
	_, usage := r.progress.Snapshot()
	r.mu.Unlock()
	payload, err := json.Marshal(struct {
		Code      string     `json:"code"`
		Message   string     `json:"message"`
		Reason    string     `json:"reason"`
		Retriable bool       `json:"retriable"`
		Usage     tokenUsage `json:"usage"`
	}{
		Code: "migration_refused", Message: "continuation is not safe", Reason: reason, Usage: usage,
	})
	if err != nil {
		r.writeMu.Unlock()
		r.failTerminalTransition()
		return false
	}
	closeContext, cancel := boundedContext(context.Background(), r.service.config.ReadinessTimeout, deadline)
	err = r.service.journal.Close(closeContext, r.id, journal.Entry{Kind: journal.KindError, Payload: payload})
	cancel()
	if err != nil {
		r.writeMu.Unlock()
		r.failTerminalTransition()
		return false
	}
	r.recordJournalCommit(int64(len(payload)))
	r.mu.Lock()
	r.lastSeq++
	terminal := journal.Entry{Seq: r.lastSeq, Kind: journal.KindError, Payload: payload}
	r.terminal = journal.KindError
	r.terminating = false
	lease := r.currentLease
	r.currentLease = nil
	if estimateWarning {
		r.estimateWarned = true
	}
	if r.orphanTimer != nil {
		r.orphanTimer.Stop()
		r.orphanTimer = nil
	}
	close(r.done)
	r.activeDone.Do(r.service.active.Done)
	r.mu.Unlock()
	if relayErr := r.degradedFeed.publishCommitted(r.service.rootContext, terminal); relayErr != nil {
		r.service.logger.Warn("relay committed migration refusal", "stream_id", r.id, "error", relayErr)
	}
	r.degradedFeed.close()
	r.writeMu.Unlock()

	// Cancellation happens only after the terminal error is durable.
	r.cancel(errors.New(reason))
	lease.Release()
	r.recordTerminal(journal.KindError)
	for _, entry := range committed {
		r.publishFirst(entry)
	}
	r.publishFirst(terminal)
	r.refreshIdempotency()
	r.scheduleRetentionExpiry()
	return true
}

func (s *durableService) triggerBindings(reason string, id backend.ID, bindings []backend.Binding) {
	s.triggerBindingsUntil(reason, id, bindings, time.Time{})
}

func (s *durableService) triggerBindingsUntil(
	reason string,
	id backend.ID,
	bindings []backend.Binding,
	deadline time.Time,
) {
	for _, binding := range bindings {
		streamID, err := journal.ParseStreamID(binding.Owner)
		if err != nil {
			continue
		}
		if value, ok := s.streams.Load(streamID); ok {
			runtime := value.(*streamRuntime)
			if reason == "drain" {
				go runtime.triggerMigrationUntil(reason, id, deadline)
				continue
			}
			runtime.triggerMigrationUntil(reason, id, deadline)
		}
	}
}
