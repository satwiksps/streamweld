# ADR 007: Route live remote readers to the producer owner

- Status: accepted
- Date: 2026-08-22

## Context

Redis makes committed journal entries visible to every proxy replica, but a
post-open Redis outage creates an intentionally unsequenced suffix. That suffix
exists only in the producer owner's bounded process-local reader feed. A reader
attached through another replica must already have a path to that feed when
Redis disappears; discovering the owner after the outage is too late.

The relay is an internal data-plane capability. Publishing its address or
credentials in public stream state would expose topology, and serving the
relay route on the public listener would turn a correctness mechanism into an
unnecessary attack surface.

## Decision

Each relay-enabled replica has a unique opaque identity and advertises a
private base URL in the Redis stream metadata. The metadata is omitted from
the open event and public `StreamState`. A short Redis presence lease, refreshed
by the owner, distinguishes a live advertised endpoint from a stale record.

Before a non-owner replica commits public response headers for an events
request, it resolves the live owner and establishes a streaming request to the
owner's separate relay listener. The owner serves the same journal-to-SSE
mapping used locally and subscribes the relay reader to the existing bounded
degradation feed. If Redis later fails, the established connection carries the
single `journal_degraded` warning and every subsequent unsequenced event. A
failure to establish the relay before headers are committed falls back to the
shared Redis journal.

Production relay listeners require TLS 1.3 with certificates verified in both
directions. Plain HTTP is available only through an explicitly named insecure
development mode restricted to loopback addresses. Relay requests are built
from scratch. Event reads forward only the stream identity, cursor, and verbose
bit. Stop controls forward the stream identity with an empty body. Client
authorization, idempotency, cookies, request bodies, and arbitrary headers
never cross the internal hop. Relay responses expose only an allowlist of
public streaming or JSON control headers.

## Consequences

- A reader connected through replica B can finish from owner A when Redis dies,
  provided the owner connection was established while the directory was live.
- Relay buffering remains bounded per reader; a slow remote reader is evicted
  without blocking producer progress.
- Owner process loss still ends its uncommitted suffix. The presence lease
  prevents new readers from being routed to the dead owner, but cannot recreate
  data which was never committed.
- The relay listener and presence heartbeat share the proxy's graceful
  lifecycle. It remains available while active producers drain and closes
  before owned Redis resources are released.
