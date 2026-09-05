// Package telemetry owns Streamweld's Prometheus and OpenTelemetry surface.
package telemetry

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
)

const (
	// DefaultRoute is the stable label for streams served by the standalone
	// backend rather than an operator-managed InferenceRoute.
	DefaultRoute = "default"
	// AnyModel is used for a standalone backend that can serve any model.
	AnyModel = "*"
)

// Labels are the bounded identity labels shared by every Streamweld metric.
type Labels struct {
	Route string
	Model string
}

// BackendCount is one current backend-state gauge sample.
type BackendCount struct {
	Labels Labels
	State  string
	Count  float64
}

// Recorder owns a private or caller-supplied Prometheus registry and an OTel
// tracer. Its methods are concurrency-safe.
type Recorder struct {
	gatherer prometheus.Gatherer
	handler  http.Handler
	tracer   trace.Tracer

	streamsActive        *prometheus.GaugeVec
	streamsTotal         *prometheus.CounterVec
	migrationsTotal      *prometheus.CounterVec
	migrationsRefused    *prometheus.CounterVec
	tokensRescued        *prometheus.CounterVec
	promptTokensRebilled *prometheus.CounterVec
	resumesTotal         *prometheus.CounterVec
	seamOverlap          *prometheus.HistogramVec
	ttft                 *prometheus.HistogramVec
	interToken           *prometheus.HistogramVec
	streamDuration       *prometheus.HistogramVec
	journalBytes         *prometheus.GaugeVec
	journalDegraded      *prometheus.GaugeVec
	backends             *prometheus.GaugeVec

	backendMu    sync.Mutex
	journalMu    sync.Mutex
	journalState map[Labels]*journalHealth
}

type journalHealth struct {
	degradedStreams        map[string]struct{}
	successfulSinceFailure bool
	degraded               bool
}

// NewOTLPTraceProvider creates the production SDK pipeline used when an OTLP
// HTTP endpoint is configured. The exporter also honors the standard
// OTEL_EXPORTER_OTLP_* environment variables for headers, compression, and
// timeout. Call Shutdown during graceful process termination.
func NewOTLPTraceProvider(
	ctx context.Context,
	endpoint, serviceName, serviceVersion string,
) (*sdktrace.TracerProvider, error) {
	if ctx == nil {
		return nil, errors.New("OTLP trace provider context cannot be nil")
	}
	if endpoint == "" {
		return nil, errors.New("OTLP trace endpoint cannot be empty")
	}
	endpoint, err := exactOTLPTraceEndpoint(endpoint)
	if err != nil {
		return nil, err
	}
	exporter, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(endpoint))
	if err != nil {
		return nil, fmt.Errorf("create OTLP HTTP trace exporter: %w", err)
	}
	serviceResource, err := resource.Merge(resource.Default(), resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(serviceName),
		semconv.ServiceVersion(serviceVersion),
	))
	if err != nil {
		return nil, errors.Join(
			fmt.Errorf("create OpenTelemetry resource: %w", err),
			exporter.Shutdown(ctx),
		)
	}
	return sdktrace.NewTracerProvider(
		sdktrace.WithResource(serviceResource),
		sdktrace.WithBatcher(exporter),
	), nil
}

func exactOTLPTraceEndpoint(endpoint string) (string, error) {
	parsed, err := url.ParseRequestURI(endpoint)
	if err != nil || parsed.Host == "" ||
		(parsed.Scheme != "http" && parsed.Scheme != "https") {
		return "", fmt.Errorf("invalid OTLP HTTP trace endpoint %q", endpoint)
	}
	// The signal-specific OTLP endpoint is exact. The SDK otherwise replaces an
	// empty path with /v1/traces, so make the specified root path explicit.
	if parsed.Path == "" {
		parsed.Path = "/"
	}
	return parsed.String(), nil
}

// New constructs a recorder. When registerer and gatherer are both nil, New
// creates a private registry, avoiding collector collisions between tests and
// independently embedded proxy servers. They must otherwise be supplied as a
// pair. A nil tracer provider uses the process-global OpenTelemetry provider.
func New(
	registerer prometheus.Registerer,
	gatherer prometheus.Gatherer,
	provider trace.TracerProvider,
) (*Recorder, error) {
	if (registerer == nil) != (gatherer == nil) {
		return nil, errors.New("telemetry registerer and gatherer must be supplied together")
	}
	if registerer == nil {
		registry := prometheus.NewRegistry()
		registerer = registry
		gatherer = registry
	}
	if provider == nil {
		provider = otel.GetTracerProvider()
	}

	labels := []string{"route", "model"}
	recorder := &Recorder{
		gatherer:     gatherer,
		tracer:       provider.Tracer("github.com/satwiksps/streamweld/internal/proxy"),
		journalState: make(map[Labels]*journalHealth),
		streamsActive: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "streamweld_streams_active",
			Help: "Current durable streams owned by this proxy.",
		}, labels),
		streamsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "streamweld_streams_total",
			Help: "Durable streams completed by terminal outcome.",
		}, append(labels, "outcome")),
		migrationsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "streamweld_migrations_total",
			Help: "Producer migrations dispatched by normalized failure reason.",
		}, append(labels, "reason")),
		migrationsRefused: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "streamweld_migrations_refused_total",
			Help: "Producer migrations refused by safety or eligibility predicate.",
		}, append(labels, "predicate")),
		tokensRescued: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "streamweld_tokens_rescued_total",
			Help: "Previously emitted completion tokens preserved across failover.",
		}, labels),
		promptTokensRebilled: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "streamweld_prompt_tokens_rebilled_total",
			Help: "Prompt tokens reported by continuation attempts.",
		}, labels),
		resumesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "streamweld_resumes_total",
			Help: "Stream continuation operations by client reattachment or producer failover.",
		}, append(labels, "trigger")),
		seamOverlap: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "streamweld_seam_overlap_bytes",
			Help:    "Continuation bytes removed because they overlap the accepted stream tail.",
			Buckets: []float64{0, 1, 2, 4, 8, 16, 32, 64, 128, 256},
		}, labels),
		ttft: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "streamweld_ttft_seconds",
			Help:    "Time from client stream request arrival to its first text chunk.",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 16),
		}, labels),
		interToken: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "streamweld_inter_token_seconds",
			Help:    "Time between consecutive non-empty streamed text chunks.",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 16),
		}, labels),
		streamDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "streamweld_stream_duration_seconds",
			Help:    "Elapsed time from client stream request arrival to terminal outcome.",
			Buckets: prometheus.ExponentialBuckets(0.01, 2, 18),
		}, labels),
		journalBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "streamweld_journal_bytes",
			Help: "Journal payload bytes retained for locally owned streams.",
		}, labels),
		journalDegraded: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "streamweld_journal_degraded",
			Help: "Whether the latest journal operation failed and streaming degraded (0 or 1).",
		}, labels),
		backends: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "streamweld_backends",
			Help: "Current backends by serving state.",
		}, append(labels, "state")),
	}

	collectors := []prometheus.Collector{
		recorder.streamsActive,
		recorder.streamsTotal,
		recorder.migrationsTotal,
		recorder.migrationsRefused,
		recorder.tokensRescued,
		recorder.promptTokensRebilled,
		recorder.resumesTotal,
		recorder.seamOverlap,
		recorder.ttft,
		recorder.interToken,
		recorder.streamDuration,
		recorder.journalBytes,
		recorder.journalDegraded,
		recorder.backends,
	}
	registered := make([]prometheus.Collector, 0, len(collectors))
	for _, collector := range collectors {
		if err := registerer.Register(collector); err != nil {
			for _, previous := range registered {
				registerer.Unregister(previous)
			}
			return nil, fmt.Errorf("register Streamweld telemetry: %w", err)
		}
		registered = append(registered, collector)
	}
	recorder.handler = promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	})
	return recorder, nil
}

// Handler exposes this recorder's Prometheus gatherer.
func (r *Recorder) Handler() http.Handler {
	if r == nil {
		return http.NotFoundHandler()
	}
	return r.handler
}

// Gatherer returns the underlying gatherer for tests and embedders.
func (r *Recorder) Gatherer() prometheus.Gatherer {
	if r == nil {
		return nil
	}
	return r.gatherer
}

func normalized(labels Labels) Labels {
	if labels.Route == "" {
		labels.Route = DefaultRoute
	}
	if labels.Model == "" {
		labels.Model = AnyModel
	}
	return labels
}

func values(labels Labels) (string, string) {
	labels = normalized(labels)
	return labels.Route, labels.Model
}

// StreamStarted records ownership of one durable stream and initializes its
// zero-valued gauges so they are visible before an exceptional transition.
func (r *Recorder) StreamStarted(labels Labels) {
	if r == nil {
		return
	}
	route, model := values(labels)
	r.streamsActive.WithLabelValues(route, model).Inc()
	for _, outcome := range []string{"done", "stopped", "error"} {
		r.streamsTotal.WithLabelValues(route, model, outcome).Add(0)
	}
	for _, reason := range []string{"crash", "drain", "stall", "error_chunk", "health"} {
		r.migrationsTotal.WithLabelValues(route, model, reason).Add(0)
	}
	for _, predicate := range []string{
		"max_migrations", "max_migration_tokens", "max_stream_duration",
		"template_verdict", "allow_structured_resume", "model_version",
		"backend_available", "tool_call_boundary", "structured_prefix_invalid",
		"unsupported_continuation_shape", "token_budget_exhausted", "invalid_policy", "migration_ineligible",
	} {
		r.migrationsRefused.WithLabelValues(route, model, predicate).Add(0)
	}
	r.tokensRescued.WithLabelValues(route, model).Add(0)
	r.promptTokensRebilled.WithLabelValues(route, model).Add(0)
	for _, trigger := range []string{"client", "failover"} {
		r.resumesTotal.WithLabelValues(route, model, trigger).Add(0)
	}
	r.seamOverlap.WithLabelValues(route, model)
	r.ttft.WithLabelValues(route, model)
	r.interToken.WithLabelValues(route, model)
	r.streamDuration.WithLabelValues(route, model)
	r.journalBytes.WithLabelValues(route, model).Add(0)
	r.journalDegraded.WithLabelValues(route, model).Add(0)
}

// StreamFinished records exactly one terminal outcome for a stream.
func (r *Recorder) StreamFinished(
	labels Labels,
	streamID, outcome string,
	duration time.Duration,
) {
	if r == nil {
		return
	}
	route, model := values(labels)
	r.streamsActive.WithLabelValues(route, model).Dec()
	r.streamsTotal.WithLabelValues(route, model, outcome).Inc()
	r.streamDuration.WithLabelValues(route, model).Observe(max(0, duration.Seconds()))
	r.journalStreamFinished(normalized(labels), streamID)
}

// Migration records a successfully dispatched continuation.
func (r *Recorder) Migration(labels Labels, reason string, rescuedTokens uint64) {
	if r == nil {
		return
	}
	route, model := values(labels)
	r.migrationsTotal.WithLabelValues(route, model, reason).Inc()
	r.tokensRescued.WithLabelValues(route, model).Add(float64(rescuedTokens))
	r.resumesTotal.WithLabelValues(route, model, "failover").Inc()
}

// MigrationRefused records one failed predicate. Call once per predicate when
// a refusal has multiple independent causes.
func (r *Recorder) MigrationRefused(labels Labels, predicate string) {
	if r == nil {
		return
	}
	route, model := values(labels)
	r.migrationsRefused.WithLabelValues(route, model, predicate).Inc()
}

// PromptTokensRebilled records prompt usage reported by continuation attempts.
func (r *Recorder) PromptTokensRebilled(labels Labels, count uint64) {
	if r == nil || count == 0 {
		return
	}
	route, model := values(labels)
	r.promptTokensRebilled.WithLabelValues(route, model).Add(float64(count))
}

// Resume records a successful client or failover resume.
func (r *Recorder) Resume(labels Labels, trigger string) {
	if r == nil {
		return
	}
	route, model := values(labels)
	r.resumesTotal.WithLabelValues(route, model, trigger).Inc()
}

// SeamOverlap records the exact overlap stripped from a continuation.
func (r *Recorder) SeamOverlap(labels Labels, count int) {
	if r == nil {
		return
	}
	route, model := values(labels)
	r.seamOverlap.WithLabelValues(route, model).Observe(float64(max(0, count)))
}

// TTFT records time to the first non-empty text chunk.
func (r *Recorder) TTFT(labels Labels, duration time.Duration) {
	if r == nil {
		return
	}
	route, model := values(labels)
	r.ttft.WithLabelValues(route, model).Observe(max(0, duration.Seconds()))
}

// InterToken records time between non-empty text chunks.
func (r *Recorder) InterToken(labels Labels, duration time.Duration) {
	if r == nil {
		return
	}
	route, model := values(labels)
	r.interToken.WithLabelValues(route, model).Observe(max(0, duration.Seconds()))
}

// AddJournalBytes adjusts retained payload bytes after a committed mutation or
// local retention expiry.
func (r *Recorder) AddJournalBytes(labels Labels, bytes int64) {
	if r == nil {
		return
	}
	route, model := values(labels)
	r.journalBytes.WithLabelValues(route, model).Add(float64(bytes))
}

// JournalDegraded records the result of a journal operation. A successful
// operation cannot clear the gauge while a different stream with the same
// labels is still degraded.
func (r *Recorder) JournalDegraded(labels Labels, streamID string, degraded bool) {
	if r == nil {
		return
	}
	labels = normalized(labels)
	r.journalMu.Lock()
	state := r.journalState[labels]
	if state == nil {
		state = &journalHealth{degradedStreams: make(map[string]struct{})}
		r.journalState[labels] = state
	}
	if degraded {
		state.degradedStreams[streamID] = struct{}{}
		state.successfulSinceFailure = false
		state.degraded = true
	} else {
		state.successfulSinceFailure = true
		if len(state.degradedStreams) == 0 {
			state.degraded = false
		}
	}
	r.setJournalDegradedLocked(labels, state.degraded)
	r.journalMu.Unlock()
}

func (r *Recorder) journalStreamFinished(labels Labels, streamID string) {
	r.journalMu.Lock()
	defer r.journalMu.Unlock()
	state := r.journalState[labels]
	if state == nil {
		return
	}
	delete(state.degradedStreams, streamID)
	if len(state.degradedStreams) == 0 && state.successfulSinceFailure {
		state.degraded = false
	}
	r.setJournalDegradedLocked(labels, state.degraded)
}

func (r *Recorder) setJournalDegradedLocked(labels Labels, degraded bool) {
	value := 0.0
	if degraded {
		value = 1
	}
	r.journalDegraded.WithLabelValues(labels.Route, labels.Model).Set(value)
}

// ReplaceBackends atomically serializes scrape-time replacement of all
// backend samples. The Prometheus vectors themselves remain concurrency-safe.
func (r *Recorder) ReplaceBackends(samples []BackendCount) {
	if r == nil {
		return
	}
	r.backendMu.Lock()
	defer r.backendMu.Unlock()
	r.backends.Reset()
	for _, sample := range samples {
		route, model := values(sample.Labels)
		r.backends.WithLabelValues(route, model, sample.State).Set(sample.Count)
	}
}

// StartStream starts the long-lived GenAI stream span. The span follows the
// GenAI naming and attribute conventions while retaining Streamweld identity.
func (r *Recorder) StartStream(
	ctx context.Context,
	streamID string,
	labels Labels,
	operation string,
	requestStartedAt time.Time,
) (context.Context, trace.Span) {
	if r == nil {
		return ctx, trace.SpanFromContext(ctx)
	}
	labels = normalized(labels)
	options := []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(
			attribute.String("gen_ai.operation.name", operation),
			attribute.String("gen_ai.request.model", labels.Model),
			attribute.String("gen_ai.response.id", streamID),
			attribute.String("streamweld.stream.id", streamID),
			attribute.String("streamweld.route", labels.Route),
		),
	}
	if !requestStartedAt.IsZero() {
		options = append(options, trace.WithTimestamp(requestStartedAt))
	}
	return r.tracer.Start(ctx, operation+" "+labels.Model, options...)
}

// StartAttempt starts one child client span for an upstream backend attempt.
func (r *Recorder) StartAttempt(
	ctx context.Context,
	labels Labels,
	operation, backendID, backendAddress string,
	backendPort int,
	attempt uint64,
	continuation bool,
) (context.Context, trace.Span) {
	if r == nil {
		return ctx, trace.SpanFromContext(ctx)
	}
	labels = normalized(labels)
	attributes := []attribute.KeyValue{
		attribute.String("gen_ai.operation.name", operation),
		attribute.String("gen_ai.request.model", labels.Model),
		attribute.String("server.address", backendAddress),
		attribute.String("streamweld.backend.id", backendID),
		attribute.Int64("streamweld.attempt", boundedTokenCount(attempt)),
		attribute.Bool("streamweld.continuation", continuation),
	}
	if backendPort > 0 {
		attributes = append(attributes, attribute.Int("server.port", backendPort))
	}
	return r.tracer.Start(ctx, operation+" "+labels.Model,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attributes...),
	)
}

// EndAttempt records an attempt failure trigger, when present, and ends it.
func (r *Recorder) EndAttempt(span trace.Span, err error, trigger string) {
	if r == nil || span == nil {
		return
	}
	if err != nil {
		span.RecordError(err)
		span.SetAttributes(semconv.ErrorType(err))
		span.SetStatus(codes.Error, err.Error())
	} else if trigger != "" {
		span.SetAttributes(attribute.String("error.type", trigger))
		span.SetStatus(codes.Error, trigger)
		span.AddEvent("streamweld.attempt.failed", trace.WithAttributes(
			attribute.String("streamweld.migration.reason", trigger),
		))
	}
	span.End()
}

// RecordMigration adds producer handoff metadata to the root stream span.
func (r *Recorder) RecordMigration(
	span trace.Span,
	fromBackend, toBackend, reason string,
	rescuedTokens, attempt uint64,
) {
	if r == nil || span == nil {
		return
	}
	span.AddEvent("streamweld.migration", trace.WithAttributes(
		attribute.String("streamweld.backend.from", fromBackend),
		attribute.String("streamweld.backend.to", toBackend),
		attribute.String("streamweld.migration.reason", reason),
		attribute.Int64("streamweld.tokens.rescued", boundedTokenCount(rescuedTokens)),
		attribute.Int64("streamweld.attempt", boundedTokenCount(attempt)),
	))
}

// EndStream records the protocol outcome and ends the root stream span.
func (r *Recorder) EndStream(
	span trace.Span,
	outcome string,
	promptTokens, completionTokens uint64,
	usageEstimated bool,
	finishReason string,
) {
	if r == nil || span == nil {
		return
	}
	attributes := []attribute.KeyValue{
		attribute.String("streamweld.outcome", outcome),
		attribute.Int64("gen_ai.usage.input_tokens", boundedTokenCount(promptTokens)),
		attribute.Int64("gen_ai.usage.output_tokens", boundedTokenCount(completionTokens)),
		attribute.Bool("streamweld.usage.estimated", usageEstimated),
	}
	if finishReason != "" {
		attributes = append(attributes,
			attribute.StringSlice("gen_ai.response.finish_reasons", []string{finishReason}),
		)
	}
	span.SetAttributes(attributes...)
	if outcome == "error" {
		span.SetAttributes(attribute.String("error.type", "stream_error"))
		span.SetStatus(codes.Error, outcome)
	} else {
		span.SetStatus(codes.Ok, outcome)
	}
	span.End()
}

func boundedTokenCount(count uint64) int64 {
	if count > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(count)
}
