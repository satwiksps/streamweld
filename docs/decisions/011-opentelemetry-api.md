# ADR 011: Use the OpenTelemetry Go API for stream traces

- Status: accepted
- Date: 2026-08-22

## Context

Streamweld must emit one GenAI span for each durable stream, child spans for
backend attempts, and migration events. Recreating OpenTelemetry's context and
span contracts locally would produce an incompatible tracing surface and make
exporter integration the application's responsibility in a non-standard way.
The build otherwise limits Go dependencies to the standard library,
controller-runtime, the Prometheus client, and the Redis client.

## Decision

Use `go.opentelemetry.io/otel` and its trace API directly. Use v1.38.x, the
newest OpenTelemetry Go line whose module still declares Go 1.23 support; newer
lines require a newer Go language baseline. The standalone proxy installs the
v1.38 OTLP/HTTP exporter and SDK only when
`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` or `OTEL_EXPORTER_OTLP_ENDPOINT` is set;
otherwise its tracer provider remains a no-op. Embedded servers may inject any
standard tracer provider. Every backend attempt propagates its W3C trace
context upstream, and incoming W3C trace context becomes the remote parent of
the stream span. The development GenAI conventions recommend
`gen_ai.provider.name`, but Streamweld deliberately omits it: the backend API
does not carry provider identity, and an OpenAI-compatible endpoint is not
evidence that OpenAI is the provider. A future explicit backend-provider field
can populate it without misattributing telemetry.

The generic `OTEL_EXPORTER_OTLP_ENDPOINT` is treated as a base URL and has
`/v1/traces` appended, including after a path prefix. The signal-specific
`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` is used as the exact traces URL.

Prometheus remains the operational metrics path. Each proxy owns a private
registry by default so embedded servers and tests cannot collide through global
collector registration.

## Consequences

- Applications can inject any standard OpenTelemetry tracer provider without a
  Streamweld-specific bridge, while the standalone binary works directly with
  an OTLP/HTTP collector.
- The data plane adds the stable OpenTelemetry API, SDK, and OTLP/HTTP exporter
  as runtime dependencies.
- Tracing is zero-export and low-overhead unless a tracer provider is installed.
- Updating past v1.38 requires raising the repository's Go baseline first.
