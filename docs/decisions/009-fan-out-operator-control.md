# ADR 009: Fan out route and drain control to every proxy replica

- Status: accepted
- Date: 2026-08-22

## Context

Backend pools and active stream leases are deliberately process-local hot-path
state. A Kubernetes Service distributes one HTTP request to one proxy Pod; it
does not broadcast. Sending an EndpointSlice snapshot or a backend pre-stop
drain to the proxy ClusterIP once would therefore leave other replicas stale.
Those replicas could route to deleted Pods, reject a same-name recreated route,
or keep generations on a Pod that Kubernetes is terminating.

EndpointSlice changes also do not increment `InferenceRoute.metadata.generation`.
The controller must be able to replace endpoint content at the same route
generation, while delayed work from a deleted object must not resurrect its
backends after a new object reuses the namespace/name.

## Decision

The operator discovers proxy Pods through the proxy Service EndpointSlices and
sends every complete route snapshot directly to every non-terminating Ready
replica with bounded concurrency. An update succeeds only when all discovered
replicas acknowledge a consistent serving-backend count. Draining backend
identities are unioned across replicas and active-stream counts are
replica-local and summed; neither local counter is required to match. Proxy
EndpointSlice changes enqueue
all routes, so a new or restarted replica receives every snapshot before the
operator reports the routes fully reconciled.

Each snapshot carries the route UID, observed generation, complete backend set,
materialized policy, and a deletion bit. The proxy accepts changed content at
the same generation because controller-runtime serializes reconciles for one
object key and each fanout call is synchronous. It rejects lower generations.
Deletion installs a UID/generation tombstone on every reachable non-terminating
replica, including temporarily NotReady replicas, before the finalizer is
removed. A different UID may replace only a tombstone; the old UID cannot be
resurrected. Dual-stack addresses are deduplicated by proxy Pod UID.

Backend Pod pre-stop hooks call a NetworkPolicy-restricted operator drain
listener, not the load-balanced proxy Service. The listener fans the pod drain
request out to every proxy replica and returns success only when all local
in-flight counts reach zero. The manual CLI calls the same operator barrier.
No bearer credential is placed in backend Pods; the operator listener is an
in-cluster administrative surface and must remain network restricted. The
operator authenticates its downstream per-proxy drain calls with the same
release-scoped bearer token used for route snapshots.

## Consequences

- Redis-backed proxy replicas receive identical route and deletion state
  without adding a shared lookup or lock to request selection.
- Rolling backend termination drains streams owned by every proxy, rather than
  whichever replica a ClusterIP happens to select.
- A route finalizer can remain while an unreachable proxy prevents a safe
  tombstone. This favors fail-closed deletion over stale backend resurrection.
- Normal EndpointSlice churn may repeat conformance-cache lookup and snapshot
  fanout, but exact retries are idempotent and concurrency is bounded.
- The proxy admin token protects route mutation. The unauthenticated drain
  barrier depends on namespace isolation and NetworkPolicy as an explicit
  operational trust boundary.
