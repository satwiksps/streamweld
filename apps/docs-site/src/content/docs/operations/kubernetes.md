---
title: Kubernetes
description: Route programming, compatibility admission, rollout drain, and safe chart layouts.
---

## Custom resources

An `InferenceRoute` selects backend Pods and maps an OpenAI model name to those
endpoints. A referenced `DurabilityPolicy` controls migration budget, template
strictness, version matching, structured-output behavior, and orphan handling.

The operator watches EndpointSlices and backend Pods, probes compatibility, and
publishes a complete generation-fenced route snapshot to every proxy replica.
Backends that are unhealthy, draining, or incompatible are excluded from new
attempts.

## Drain before termination

The sample backend uses a pre-stop hook to notify the operator by Pod identity.
Drain has this order:

1. mark the backend draining on every proxy;
2. prevent new attempt leases;
3. wait for existing leases to finish or migrate;
4. let Kubernetes terminate the Pod within its grace period.

The optional mutating webhook can add the hook when a workload does not already
own one. It refuses to replace a different hook. Production webhook use requires
a serving certificate and CA bundle or the chart's cert-manager integration.

## Chart safety checks

The chart fails rendering instead of accepting these unsafe layouts:

- memory journal with more than one proxy replica;
- Redis journal without a Redis URL source;
- owner relay without TLS material;
- operator drain listener without the namespace NetworkPolicy.

Enable Redis before horizontal proxy scaling. Keep operator leader election on
for high availability. See the
[sample manifests](https://github.com/satwiksps/streamweld/tree/main/deploy/samples)
for a deterministic CPU-only route.

## Verify a rollout

Run a long deterministic request, then update the backend image with
`maxUnavailable: 1`. Watch the request, the route conditions, and
`streamweld_migrations_total{reason="drain"}` together. A successful rollout is
one whose complete output matches the deterministic expected sequence—not one
that merely kept an HTTP connection open.
