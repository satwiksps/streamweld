---
title: Observe and debug
description: Metrics, traces, logs, dashboards, and a practical migration investigation.
---

# Observe and debug

## Prometheus metrics

The proxy exposes stream outcomes, migrations and refusals, rescued and re-billed
tokens, resumes, seam overlap, TTFT, inter-token time, stream duration, journal
bytes, journal degradation, and backend state. Stream metrics use bounded
`route` and `model` labels; trigger, reason, outcome, and predicate labels use
defined enumerations.

Useful starting queries:

```text
sum by (route, outcome) (rate(streamweld_streams_total[5m]))
sum by (route, reason) (rate(streamweld_migrations_total[5m]))
sum by (route, predicate) (rate(streamweld_migrations_refused_total[5m]))
histogram_quantile(0.99, sum by (le, route) (rate(streamweld_ttft_seconds_bucket[5m])))
max by (route) (streamweld_journal_degraded)
```

## Traces and logs

Each generation has one OpenTelemetry stream span, with a child span for each
backend attempt and migration recorded as a span event. Configure the chart with
an OTLP/HTTP base endpoint:

```yaml
observability:
  tracing:
    otlpEndpoint: http://otel-collector.observability:4318
```

Structured JSON logs include `stream_id` for stream-scoped records. Correlate a
terminal error by stream ID, inspect its attempt spans, then use the migration or
refusal metric label to group the incident.

## Dashboard

Set `grafanaDashboard.enabled=true` to install the supplied dashboard ConfigMap.
Set `serviceMonitor.enabled=true` when the Prometheus Operator CRDs are available.

When investigating a migration, check in this order: trigger, source and target
backend states, compatibility verdict, refusal predicate, attempt span status,
seam-overlap histogram, terminal outcome, and full deterministic output when a
fixture is available.
