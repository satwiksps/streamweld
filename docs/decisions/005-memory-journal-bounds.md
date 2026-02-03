# ADR 005: Give the standalone memory journal explicit bounds

- Status: Accepted
- Date: 2026-08-22

## Context

The durable proxy must run safely with no external services for development,
yet an unbounded in-process journal lets active streams exhaust the proxy. The
protocol defines a 10-minute TTL and 4 MiB per-stream ring but deliberately
leaves deployment-wide and per-reader sizing to operators.

## Decision

The standalone command uses these conservative defaults:

- terminal journal TTL: 10 minutes;
- retained bytes per stream: 4 MiB;
- total memory-journal bytes: 256 MiB;
- lag allowance per reader: 1 MiB;
- orphan policy: `continue`, with a 60-second timeout when `cancel_after` is
  selected;
- completion request body: 8 MiB.

Every value has a flag and `STREAMWELD_*` environment counterpart. Kubernetes
deployments render explicit values so capacity is visible in release manifests.

## Consequences

Local startup is safe without a configuration file and memory mode remains a
single-replica development choice. A stream can outlive readers by default.
Production multi-replica installs use Redis and size the limits explicitly.
