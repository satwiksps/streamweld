# ADR 006: Bind leases to backend attempts, not durable streams

- Status: accepted
- Date: 2026-08-22

## Context

A durable stream outlives any one inference-server connection. Backend health,
passive connection failure, and an administrative drain must be able to stop a
single producer attempt without turning a reader disconnect into a stop or
canceling the stream-wide journal owner.

Selection also races with drain and health transitions. Selecting an address
and incrementing an in-flight counter as separate operations could admit work
after drain has started or let the drain endpoint return while an attempt is
still bound.

## Decision

The backend pool grants an atomic, idempotently releasable lease for every
upstream attempt. A lease binds an opaque owner (the stream ID for durable
generation), an immutable backend snapshot, and the backend's in-flight count.
Healthy, non-draining, non-quarantined selection and lease creation happen in
one critical section.

The durable stream has a stream-wide cancellation context and a replaceable
attempt context. Stop, orphan cancellation, and forced proxy shutdown cancel
the stream context. Crash, health, drain, and optional stall triggers cancel
only the current attempt with a typed migration reason.

On a successful handoff, Streamweld commits all required warnings and the
`migration` journal entry, swaps the current lease, and only then releases the
failed lease. On refusal, the failed lease remains bound until the terminal
error is durable. This makes a successful `200` drain response mean exactly
zero remaining attempts on that backend.

## Consequences

- A drain snapshot can identify and proactively signal every bound stream.
- Stop-versus-migration ordering is serialized by the journal writer lock and
  exactly one terminal entry wins.
- Non-streaming requests also hold leases, so drain waits for them to finish
  even though there is no migration mechanism for their response bodies.
- Backend metadata is immutable for an active attempt; endpoint updates affect
  only later leases.
