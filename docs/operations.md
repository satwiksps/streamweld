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

## Journal deployment modes

### Memory journal

`memory` is the development default and is valid only with one proxy replica.
Its bounded ring, idempotency bindings, and active producer ownership cannot be
shared across processes and are lost on process exit. Do not run multiple
memory-mode replicas behind a load balancer, including with sticky sessions.

### Redis journal

Production deployments that need proxy failover or cross-replica resume must
give every replica the same Redis settings:

```text
STREAMWELD_JOURNAL_BACKEND=redis
STREAMWELD_REDIS_URL=redis://redis.streamweld.svc:6379/0
STREAMWELD_REDIS_KEY_PREFIX=streamweld
```

Redis data is namespaced by the configured prefix. Use a distinct prefix for
each independent Streamweld installation sharing a Redis database. A terminal
stream and its idempotency binding remain available for `journal.ttl`. Redis
URLs may use `redis`, `rediss`, or `unix`; use `rediss` unless transport security
is provided by a trusted private network or sidecar.

Treat a credential-bearing Redis URL as a secret. Inject
`STREAMWELD_REDIS_URL` from a secret store instead of putting it in
`--redis-url`, a manifest literal, or a shell history. CLI help and
configuration errors redact Redis credentials, but command-line arguments may
still be visible to other processes on the host.

Redis alone provides cross-replica access to committed entries while Redis is
reachable. It does not carry the uncommitted suffix created by a Redis outage.
Enable the owner relay when a reader connected through a different proxy must
remain live through that outage.

### Owner relay for Redis outages

The owner relay is disabled when `STREAMWELD_RELAY_ADVERTISE_URL` is empty. When
enabled, it listens separately from the public inference API. Each stream stores
a private owner replica ID and relay URL in Redis; neither value is included in
the public open event or stream-state JSON, and relay certificates and keys are
never stored in Redis.

Each production replica needs a unique identity, a directly routable advertise
URL for that exact replica, and mutual-TLS material:

```text
STREAMWELD_REPLICA_ID=streamweld-proxy-0
STREAMWELD_RELAY_LISTEN=0.0.0.0:8081
STREAMWELD_RELAY_ADVERTISE_URL=https://proxy-0.relay.internal:8081
STREAMWELD_RELAY_CA_FILE=/var/run/streamweld-relay/ca.crt
STREAMWELD_RELAY_CERT_FILE=/var/run/streamweld-relay/tls.crt
STREAMWELD_RELAY_KEY_FILE=/var/run/streamweld-relay/tls.key
```

The advertise URL must use `https`, contain no credentials, query, fragment,
or path, and route directly to the owning replica rather than a load-balanced
Service. The server certificate must be valid for the advertise hostname. The
CA must verify every relay peer, and each certificate must be usable for both
server and client authentication. Relays require TLS 1.3. Files are loaded at
process startup, so restart a replica to load rotated material. Restrict the
relay port with network policy even though mutual TLS is mandatory.

The defaults refresh owner presence every `2s` with a `10s` lease. The presence
TTL must be greater than twice the heartbeat interval. If
`STREAMWELD_REPLICA_ID` is omitted, the process generates a unique identity at
startup; an explicit pod identity is easier to correlate operationally.

For a two-replica local test only, plaintext relay mode is available. Both the
listen and advertised hosts are required to be loopback addresses:

```sh
go run ./cmd/streamweld-proxy \
  --backend http://127.0.0.1:8000 \
  --listen 127.0.0.1:8080 \
  --journal-backend redis \
  --redis-url redis://127.0.0.1:6379/0 \
  --replica-id proxy-a \
  --relay-listen 127.0.0.1:18081 \
  --relay-advertise-url http://127.0.0.1:18081 \
  --relay-insecure-dev-mode
```

Use different public and relay ports and a different replica ID for the second
process. Never enable insecure development mode on a shared host or production
network; configuration validation rejects non-loopback plaintext endpoints.

When a client resumes through a non-owner proxy, that proxy tries to establish
the relay before committing public response headers. If owner discovery or the
relay connection fails while Redis is healthy, it falls back to the shared
Redis journal. Once established, the relay carries both committed events and a
later unsequenced degraded suffix. It forwards only the resume cursor and
verbose flag for an events request. It also allows a non-owner public replica
to forward an empty stop operation to the producer owner. It never forwards the
public request body, client authorization, cookies, idempotency keys, or
arbitrary request headers. Remote stop requires Redis owner discovery and a
reachable relay; a non-owner does not guess at producer ownership.

The relay has deliberate limits:

- A reader must establish its owner relay while Redis owner discovery is still
  available. A new remote reader cannot discover the owner during an outage.
- Owner process loss destroys any suffix that Redis never committed.
- Per-reader relay buffering is bounded by `reader.max_lag_bytes`; a lagging
  reader is closed without blocking the producer or other readers.

### Redis outage behavior

Redis loss is a durability degradation, not an inference outage. Before
`Open`, the proxy omits the stream ID and responds with
`X-Streamweld-Durability: degraded`. After `Open`, it emits one unsequenced
`streamweld.stream.warning` event with code `journal_degraded`, keeps the
producer running, and forwards subsequent complete SSE events without IDs to
readers already attached to the owner's local feed. This includes a remote
reader with an already-established owner relay. It does not include a new
remote reader created after Redis became unavailable.

The degraded suffix, including its terminal event, is never resumable. Redis
recovery does not restart sequence allocation for that stream. Once the shared
journal is readable again, a resume at its committed boundary receives HTTP
`410 stream_offset_expired`; a resume before the boundary receives the
committed prefix followed by an unsequenced `stream_offset_expired` SSE error.
Neither representation means the prefix is a complete generation.

### Phase 5 configuration reference

For the standalone proxy, built-in defaults are overlaid by environment
variables and then command-line flags. These are the journal and relay settings:

| Environment variable | CLI flag | Default or requirement |
|---|---|---|
| `STREAMWELD_JOURNAL_BACKEND` | `--journal-backend` | `memory`; choose `redis` for more than one replica |
| `STREAMWELD_JOURNAL_TTL` | `--journal-ttl` | `10m` |
| `STREAMWELD_JOURNAL_MAX_BYTES_PER_STREAM` | `--journal-max-bytes-per-stream` | `4194304`; memory journal only |
| `STREAMWELD_JOURNAL_MAX_TOTAL_BYTES` | `--journal-max-total-bytes` | `268435456`; memory journal only |
| `STREAMWELD_READER_MAX_LAG_BYTES` | `--reader-max-lag-bytes` | `1048576`; applies independently to each reader |
| `STREAMWELD_READER_WRITE_TIMEOUT` | `--reader-write-timeout` | `30s`; maximum duration of each downstream stream write or flush |
| `STREAMWELD_REDIS_URL` | `--redis-url` | Required for Redis; omitted from help defaults to avoid credential disclosure |
| `STREAMWELD_REDIS_KEY_PREFIX` | `--redis-key-prefix` | `streamweld`; must be deployment-unique and cannot contain braces or control characters |
| `STREAMWELD_REPLICA_ID` | `--replica-id` | Generated at startup when omitted |
| `STREAMWELD_RELAY_LISTEN` | `--relay-listen` | `127.0.0.1:8081`; no listener opens while relay is disabled |
| `STREAMWELD_RELAY_ADVERTISE_URL` | `--relay-advertise-url` | Unset, which disables the relay |
| `STREAMWELD_RELAY_CA_FILE` | `--relay-ca-file` | Required for a production relay |
| `STREAMWELD_RELAY_CERT_FILE` | `--relay-cert-file` | Required for a production relay |
| `STREAMWELD_RELAY_KEY_FILE` | `--relay-key-file` | Required for a production relay; keep the file secret |
| `STREAMWELD_RELAY_INSECURE_DEV_MODE` | `--relay-insecure-dev-mode` | `false`; loopback development only |
| `STREAMWELD_RELAY_HEARTBEAT_INTERVAL` | `--relay-heartbeat-interval` | `2s` |
| `STREAMWELD_RELAY_PRESENCE_TTL` | `--relay-presence-ttl` | `10s`; must exceed twice the heartbeat interval |

### Reproduce the Phase 5 acceptance tests

The always-on tests use an in-process Redis-compatible server. The external
acceptance tests use the same Redis 7.4 image and environment variable as CI.
Start Redis in one terminal:

```sh
docker run --rm --name streamweld-phase5-redis \
  -p 6379:6379 redis:7.4-alpine
```

Then run these commands from the repository root in another terminal:

```sh
STREAMWELD_TEST_REDIS_URL=redis://127.0.0.1:6379/0 \
  go test ./internal/journal -run '^TestRedisExternalAcceptance' -count=1

go test ./internal/proxy -run '^TestPhase5' -count=1
```

The first command verifies two independent clients against the real Redis
process. The second verifies cross-replica resume, shared idempotency, remote
reader orphan leases, and completion through a mid-stream Redis loss, including
the owner-relay path and cross-replica explicit stop. Stop the foreground Redis
container with `Ctrl-C` after the tests.
