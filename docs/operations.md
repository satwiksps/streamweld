# Operations

## Draining inference backends

Register backends by their canonical `host:port` identity. To exclude one from
new selection, proactively migrate its durable streams, and wait for every
lease to leave it, call:

```sh
curl --fail-with-body -X POST \
  'http://streamweld:8080/internal/backends/10.0.0.12%3A8000/drain?timeout=10s'
```

`200 OK` means `in_flight` reached zero. `504 Gateway Timeout` reports the
remaining count and deliberately leaves the backend in the draining state.
Repeating the request is safe. A stream that cannot be continued receives its
verbose refusal warning followed by a terminal error; the backend lease is not
released before that error is committed.

The pre-stop budget must cover the drain request and ordinary process exit.
With Streamweld, set `terminationGracePeriodSeconds: 15`, not 300. The Helm
hook and reproducible rollout measurements are maintained with the operator
and chaos harness; deployment documentation does not publish an unmeasured
savings number.

## Proxy shutdown

Proxy shutdown is intentionally different from backend drain. Readiness turns
false and new HTTP work stops, but existing backend attempts are not migrated
merely because this proxy is shutting down. The server waits up to its shutdown
deadline, then cancels remaining producers. Multi-replica reattachment requires
the Redis journal; memory journals are single-replica only.
