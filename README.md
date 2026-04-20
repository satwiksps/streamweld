# Streamweld

> Your token stream shouldn't die because a pod got evicted or a phone switched to cellular.

Streamweld is a durable stream layer for LLM inference: an OpenAI-compatible reverse proxy and Kubernetes operator that give every generation an identity and an append-only event log.

Non-streaming responses use the direct pass-through path with journaling
disabled. The bounded request body is inspected only to select streaming mode;
its original bytes are then restored before proxy forwarding.

The protocol is defined in [`docs/protocol.md`](docs/protocol.md). The implementation is being built in the ordered phases recorded in [`streamweld-build-spec.md`](streamweld-build-spec.md); claims and performance numbers are published only when backed by reproducible tests.

Before allowing producer migration for a model, probe its chat-template
continuation behavior with `streamweldctl doctor --backend URL --model NAME`.
The checker and the honestly scoped results table are documented in
[`docs/compatibility.md`](docs/compatibility.md).

## Development prerequisites

- Go 1.23 or newer
- Node.js 20 or newer
- pnpm 11 or newer
- GNU Make 4 or newer

Run the repository checks with:

```sh
make test
```

## Local durable proxy

Start any OpenAI-compatible backend on port 8000, then run:

```sh
go run ./cmd/streamweld-proxy --backend http://127.0.0.1:8000 --listen :8080
```

The proxy accepts OpenAI-compatible chat and legacy completions plus
`GET /v1/models`. A streaming response includes
`X-Streamweld-Stream-Id`; complete upstream SSE chunks are committed before
delivery and can be replayed from an exclusive cursor. Unknown JSON request
fields survive normalization. Health probes are available at `/healthz` and
`/readyz`.

When the configured pool contains a compatible healthy target, an unexpected
producer EOF, reset, 5xx, error chunk, failed health probe, explicit backend
drain, or enabled stall detector starts a bounded continuation attempt. The
proxy journals the handoff before continuation chunks and removes only a
UTF-8-safe leading overlap. Migration remains disabled for unknown or unsafe
chat-template verdicts under the default strict policy.

```sh
curl -N http://127.0.0.1:8080/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"your-model","messages":[{"role":"user","content":"Count to five."}],"stream":true}'
```

Resume a disconnected reader with the returned stream ID and its last SSE
`id`:

```sh
curl -N http://127.0.0.1:8080/v1/streams/STREAM_ID/events \
  -H 'Last-Event-ID: 41'
```

Only an explicit stop cancels generation:

```sh
curl -X POST http://127.0.0.1:8080/v1/streams/STREAM_ID/stop
```

Drain one registered backend and wait for its leases to reach zero:

```sh
curl -X POST 'http://127.0.0.1:8080/internal/backends/127.0.0.1%3A8000/drain?timeout=10s'
```

## TypeScript clients

`@streamweld/client` keeps one async iterable alive across transport failures
and exposes independent typed-event and text views:

```ts
import { createDurableStream } from "@streamweld/client";

const stream = createDurableStream({
  url: "http://127.0.0.1:8080/v1/chat/completions",
  body: { model: "your-model", messages, stream: true },
});

for await (const delta of stream.text) render(delta);
```

`await stream.stop()` explicitly cancels generation. An `AbortSignal` only
detaches the local reader and leaves the identified generation resumable.
`@streamweld/ai-sdk` implements Vercel AI SDK v5 `ChatTransport`, allowing a
`useChat` app to switch its transport while retaining durable cursor resume.
See [`docs/client.md`](docs/client.md) for typed outcomes, persistence, exact
`uint64` cursor handling, and the adapter contract.

<!-- streamweld:benchmarks:start -->
## Local chaos model (simulation) results

[Open the live failure lab](https://streamweld-failure-lab.satwiksub.chatgpt.site) to compare the durable and direct paths side by side.

This table is generated from [`benchmarks/results.json`](benchmarks/results.json) by `make bench`; edits inside these markers are rejected by `make bench-check`. It reports an in-process model/simulation—not Kubernetes process disruption. The non-skippable nightly [`kind` matrix](.github/workflows/nightly.yml) is the physical failure-injection gate. The committed run is the honestly labelled `deterministic-local` profile, not a kind or GPU claim.

TTFT is a wall-clock p50 from the recorded host. Both paths include the fake backend's 2.000 ms first-token delay, values serialize to 0.001 ms, and CI gates correctness rather than cross-host timing.

| Scenario | Tokens/stream | Started | Completed | Migrated | Rescued tokens | Prompt tokens re-billed | Seam p50/p99 (bytes) | Direct TTFT p50 (ms) | Streamweld TTFT p50 (ms) | Added TTFT p50 (ms) | Correct |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|:---:|
| pod-kill | 64 | 8 | 8 | 8 | 175 | 128 | 10/20 | 3.375 | 7.838 | 4.463 | true |
| rolling-update | 64 | 8 | 8 | 8 | 175 | 128 | 10/20 | 3.375 | 7.838 | 4.463 | true |
| spot-reclaim | 64 | 8 | 8 | 8 | 175 | 128 | 10/20 | 3.375 | 7.838 | 4.463 | true |
| backend-oom | 64 | 8 | 8 | 8 | 175 | 128 | 10/20 | 3.375 | 7.838 | 4.463 | true |
| client-drop | 64 | 8 | 8 | 0 | 0 | 0 | 0/0 | 3.375 | 7.838 | 4.463 | true |
| explicit-stop | 64 | 8 | 0 | 0 | 0 | 0 | 0/0 | 3.375 | 7.838 | 4.463 | true |
| redis-down | 64 | 8 | 8 | 0 | 0 | 0 | 0/0 | 3.375 | 7.838 | 4.463 | true |
| slow-consumer | 64 | 8 | 8 | 0 | 0 | 0 | 0/0 | 3.375 | 7.838 | 4.463 | true |
| unsafe-template | 64 | 8 | 0 | 0 | 0 | 0 | 0/0 | 3.375 | 7.838 | 4.463 | true |

Full metadata and scenario-specific terminal outcomes are in [`benchmarks/results.md`](benchmarks/results.md).
<!-- streamweld:benchmarks:end -->

## Kubernetes operator

Install the single-replica memory profile and apply the deterministic CPU-only
sample:

```sh
helm upgrade --install streamweld deploy/helm/streamweld \
  --namespace streamweld-system --create-namespace

kubectl apply -f deploy/samples/deterministic-backend.yaml
kubectl apply -f deploy/samples/durability-policy.yaml
kubectl apply -f deploy/samples/inference-route.yaml
```

`InferenceRoute` binds an exact model to selected backend Pods and a
`DurabilityPolicy`. The operator probes each newly Ready backend, then pushes a
UID- and generation-fenced snapshot directly to every proxy Pod. EndpointSlice
changes do not restart proxies. Route deletion completes only after every
non-terminating proxy acknowledges the tombstone.

For multiple proxy replicas, use a shared Redis journal:

```sh
helm upgrade --install streamweld deploy/helm/streamweld \
  --namespace streamweld-system --create-namespace \
  --set journal.backend=redis \
  --set redis.enabled=true \
  --set proxy.replicaCount=2
```

Backend rollout drains are Pod-scoped and fanned out by the operator to every
proxy replica. A manual drain uses the same all-replica barrier:

```sh
kubectl -n streamweld-system port-forward service/streamweld-operator 8082:8082
streamweldctl drain --endpoint http://127.0.0.1:8082 --namespace models POD_NAME
```

Do not point an HA pre-stop hook at the proxy ClusterIP: one request reaches
only one process. Keep the chart NetworkPolicy enabled because Kubernetes HTTP
lifecycle hooks cannot attach the route-admin bearer token; the operator adds
that token to its downstream per-proxy drain calls. The optional Pod
webhook is disabled by default and requires an explicit serving certificate.
See [`deploy/helm/streamweld/README.md`](deploy/helm/streamweld/README.md) for
validated install profiles.

### Redis and multi-replica mode

The default memory journal is bounded, process-local, and supports exactly one
proxy replica. A process restart loses its journals and idempotency bindings.
Do not place multiple memory-mode proxies behind a load balancer, even with
sticky sessions.

For cross-replica resume, run Redis and configure every proxy replica with the
same URL and a deployment-unique key prefix:

```sh
go run ./cmd/streamweld-proxy \
  --backend http://127.0.0.1:8000 \
  --journal-backend redis \
  --redis-url redis://127.0.0.1:6379/0 \
  --redis-key-prefix streamweld
```

Redis alone lets any replica replay or tail committed events while Redis is
reachable. For a remote reader to keep receiving an uncommitted suffix if
Redis fails, every replica must also enable the private owner relay. Production
relay traffic uses a separate listener with TLS 1.3 mutual authentication; see
[`docs/operations.md`](docs/operations.md#owner-relay-for-redis-outages) for the
complete configuration and certificate requirements. The same private relay
routes an explicit stop to the producer owner when the public request reaches a
different replica.

If Redis is unavailable before a stream opens, the request proceeds as a
non-resumable pass-through response with `X-Streamweld-Durability: degraded`
and no stream ID. If Redis disappears after a durable stream opens, readers
already attached to the producer owner's bounded local feed—including readers
already connected through its relay—receive an always-visible
`journal_degraded` warning and the remaining complete SSE events without
sequence IDs. That suffix and its terminal outcome cannot be replayed, even if
Redis later recovers. A new remote reader cannot discover the owner while
Redis is unavailable.

The Phase 5 Redis and outage acceptance commands are documented in
[`docs/operations.md`](docs/operations.md#reproduce-the-phase-5-acceptance-tests).

Streamweld is licensed under Apache-2.0.
