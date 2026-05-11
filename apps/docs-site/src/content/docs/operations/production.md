---
title: Production journals
description: Choose memory or Redis, configure owner relay, and understand degraded mode.
---

## Memory mode

Memory is the development default. It is bounded, process-local, and valid for
exactly one proxy replica. Streams and idempotency bindings disappear when the
process exits. Never place multiple memory-mode replicas behind a load balancer.

## Redis mode

Redis Streams provide shared append, replay, blocking tail, terminal state, and
idempotency across proxy replicas. All replicas must use the same Redis URL and a
deployment-unique key prefix. Inject credential-bearing URLs from a Secret.

```yaml
journal:
  backend: redis
  redis:
    existingSecret: streamweld-redis
    secretKey: redis-url
    keyPrefix: streamweld-production
proxy:
  replicaCount: 2
```

Use TLS (`rediss`) unless a trusted private network or sidecar provides transport
security. Retention begins after terminal closure; active streams refresh their
expiry while entries are committed.

## Owner relay

Redis is authoritative for committed replay. The optional private owner relay
lets a reader attached through another proxy keep receiving an uncommitted suffix
if Redis fails after that relay is established. It uses a separate direct-to-Pod
listener with TLS 1.3 and mutual certificate verification.

The relay deliberately cannot discover a new owner during a Redis outage, and it
cannot recover uncommitted bytes after the owner process dies. Plaintext relay is
accepted only in an explicit loopback development mode.

## Degraded behavior

Journal loss does not reject an otherwise serviceable inference request. If open
fails, the proxy omits the stream ID and marks the response
`X-Streamweld-Durability: degraded`. If the journal fails after open, already
attached owner feeds receive one warning and then complete, unsequenced SSE
events. That suffix is never resumable, even after Redis recovers.

Alert on `streamweld_journal_degraded == 1` and treat the request as completed but
without the durability guarantee.
