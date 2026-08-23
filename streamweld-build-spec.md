# Streamweld — Build Specification


## 0. Instructions to the agent

You are building a production-grade open-source infrastructure project from scratch, end to end. Read this entire document before writing any code.

**Rules for the whole build:**

1. Work in the phase order given in §14. Do not start a phase until the previous phase's acceptance criteria pass.
2. Every phase ends with: tests green, lint clean, docs updated, one commit per logical unit with conventional-commit messages.
3. No `TODO`, no `unimplemented!()`, no stub functions that silently return zero values. If something is out of scope, it is out of scope in the docs too.
4. No invented benchmark numbers anywhere. Numbers in the README come from `make bench` output committed to the repo, or they don't exist.
5. Errors are values. Every error path in the data plane is either handled or explicitly surfaced as a journal event. Never swallow.
6. When you hit a design ambiguity that this spec doesn't resolve, write the decision and its rationale into `docs/decisions/NNN-slug.md` (ADR format) and proceed. Do not stall.
7. Do not add dependencies casually. Go: stdlib + controller-runtime + prometheus client + redis client only, unless justified in an ADR. TypeScript: zero runtime dependencies in the published SDK.

---

## 1. What this is

**Streamweld is a durable stream layer for LLM inference.**

An OpenAI-compatible reverse proxy plus a Kubernetes operator that treats a generation as a *resource with an identity and an append-only log*, rather than as an HTTP connection.

One line for the README:

> Your token stream shouldn't die because a pod got evicted or a phone switched to cellular.

### The problem

When an LLM streams a response and something breaks mid-stream, everything already generated is lost and the request restarts from zero — re-billing the entire prompt. This happens on:

| Failure | Who breaks | Status quo |
|---|---|---|
| Backend pod OOMs or crashes | producer | stream dies, client sees truncated output |
| Rolling update / model version bump | producer | in-flight generations killed at `terminationGracePeriodSeconds` |
| Spot GPU node reclaimed | producer | same |
| Client WiFi → cellular, tab backgrounded | consumer | stream dies, server may keep burning GPU |
| Proxy/LB idle timeout | transport | stream dies |
| User presses stop | intentional | frequently indistinguishable from a disconnect |

### Prior art — read this before you design anything

Two half-solutions exist and they are mutually incompatible:

- **NVIDIA Dynamo** implements request migration: on worker failure it preserves accumulated tokens and continues generation on a healthy worker, with a configurable max-migrations-per-disconnect and a sequence-length eligibility cap. But it requires adopting the whole Dynamo runtime, and a client disconnect *cancels* the request across all workers.
- **Vercel `resumable-stream`** buffers tokens in Redis so a client can reconnect after a page reload. But it is single-device, framework-coupled, assumes the server stays alive, and is incompatible with `stop()` (their issue #8390, still open).

Dynamo protects the producer and kills on consumer disconnect. Vercel protects the consumer and assumes the producer lives. **Streamweld's thesis: one stream identity that survives both, as a thin layer over any OpenAI-compatible backend.**

Put this comparison in the README. It is the project's reason to exist and it must be stated honestly, including what Streamweld does *not* do better.

### Non-goals — do not build these

- Not an inference engine. vLLM/SGLang/TGI do that.
- Not a scheduler, autoscaler, or GPU placement system. Kueue/KEDA/llm-d do that.
- Not a KV-cache-aware router. AIBrix/production-stack do that. Streamweld may *co-exist* behind one.
- Not an eval or benchmark harness.
- Not a prompt manager, observability SaaS, or cost dashboard.
- Not a multi-tenant control plane with auth/RBAC/billing in v1.

---

## 2. Language split (non-negotiable)

| Component | Language | Why |
|---|---|---|
| Data plane proxy | **Go** | long-lived streaming connections, goroutine-per-stream, low GC pressure |
| Kubernetes operator | **Go** | controller-runtime is the only serious option |
| `streamweldctl` CLI | **Go** | shares the protocol package |
| Client SDK | **TypeScript** | the consumers are JS/TS apps; must ship on npm |
| Demo + control UI | **TypeScript + React** | the failure-injection demo is the project's shop window |

The TypeScript is load-bearing, not decorative. The SDK is how anyone actually adopts this.

---

## 3. Repository layout

```
streamweld/
├── cmd/
│   ├── streamweld-proxy/            # data plane binary
│   ├── streamweld-operator/         # controller manager
│   └── streamweldctl/               # CLI: doctor, drain, streams, bench
├── internal/
│   ├── proxy/                 # HTTP handlers, SSE codec, request normalization
│   ├── journal/               # Journal interface + memory/redis backends
│   ├── migrate/               # eligibility, continuation, seam reconciliation
│   ├── backend/               # pool, health, drain state, selection
│   ├── conformance/           # chat-template probe suite
│   ├── telemetry/             # prometheus + otel
│   └── apis/v1alpha1/         # CRD types
├── controllers/               # reconcilers
├── packages/
│   ├── client/                # @streamweld/client  (npm, zero deps)
│   └── ai-sdk/                # @streamweld/ai-sdk  (Vercel AI SDK v5 ChatTransport)
├── apps/
│   └── demo/                  # Vite + React failure-injection demo
├── deploy/
│   ├── helm/streamweld/             # Helm chart
│   └── samples/               # example CRs, vLLM Deployment
├── infra/terraform/           # demo cluster + GPU/spot node pool
├── test/
│   ├── e2e/                   # kind-based end-to-end
│   └── chaos/                 # failure injection harness
├── benchmarks/                # committed results: results.md + results.json
├── docs/
│   ├── protocol.md            # THE spec — write this before the code
│   ├── decisions/             # ADRs
│   └── operations.md
├── Makefile
├── go.mod
├── pnpm-workspace.yaml
└── README.md
```

Go module `github.com/<user>/streamweld`. npm scope `@streamweld`.

---

## 4. Protocol specification

Write `docs/protocol.md` first. The implementation follows the document, not the other way round.

### 4.1 Stream identity

- Every generation gets a `stream_id` (ULID, lowercase).
- Returned on the response as header `X-Streamweld-Stream-Id` and as the first SSE event (`event: streamweld.stream.open`).
- Client may supply `X-Streamweld-Idempotency-Key`. Same key within the journal TTL returns the *existing* stream rather than starting a new generation. This is what makes a naive client retry safe.

### 4.2 Journal

Append-only, monotonic `seq` starting at 1. Entry:

```json
{
  "seq": 42,
  "ts": "2026-08-22T10:15:03.221Z",
  "kind": "chunk",
  "payload": { }
}
```

`kind` is one of:

| kind | meaning | client-visible by default |
|---|---|---|
| `open` | stream created; carries model, backend id | yes |
| `chunk` | verbatim upstream SSE data payload | yes |
| `migration` | producer failover occurred; carries from/to backend, reason, rescued token count | no (opt-in via `X-Streamweld-Verbose: 1`) |
| `warning` | degraded continuation, seam anomaly, eligibility downgrade | opt-in |
| `error` | terminal failure | yes |
| `done` | terminal success; carries usage totals | yes |
| `stopped` | terminal, user-initiated | yes |

Terminal entries (`done`, `error`, `stopped`) close the journal. Nothing may be appended after.

**Journal interface** (Go), with two implementations:

```go
type Journal interface {
    Open(ctx context.Context, id StreamID, meta Meta) error
    Append(ctx context.Context, id StreamID, e Entry) (seq uint64, err error)
    Read(ctx context.Context, id StreamID, fromSeq uint64) (iter.Seq2[Entry, error], error)
    Tail(ctx context.Context, id StreamID, fromSeq uint64) (<-chan Entry, func(), error)
    State(ctx context.Context, id StreamID) (StreamState, error)
    Close(ctx context.Context, id StreamID, terminal Entry) error
}
```

- `memory` backend: dev/single-replica. Bounded ring per stream + global byte cap with LRU eviction of terminal streams.
- `redis` backend: production. Streams stored as Redis Streams (`XADD`/`XRANGE`/`XREAD BLOCK`) with per-stream TTL. Optimise the common path: no extra round trips when nobody is resuming.

Config: `journal.ttl` (default `10m`), `journal.max_bytes_per_stream` (default `4MiB`), `journal.max_total_bytes`.

### 4.3 Resume

```
GET /v1/streams/{stream_id}/events
Last-Event-ID: 41
```

- Replays entries from `seq > 41`, then tails live. Standard SSE `id:` field carries `seq`, so browsers reconnect automatically with no SDK.
- If the stream is already terminal, replay to the terminal entry and close.
- If `stream_id` is unknown or the TTL has expired: `410 Gone` with a structured body distinguishing *expired* from *never existed*.
- Multiple concurrent readers on one stream are supported (fan-out). Second reader from `seq=0` gets the full replay.

### 4.4 Producer failover — the core mechanism

Detection triggers, in order of confidence:

1. Upstream TCP connection reset / EOF without terminal SSE event
2. Upstream emits an error chunk inside the stream
3. Upstream `5xx` before or during stream
4. Backend health check fails while a stream is bound to it
5. Backend marked `draining` by the drain protocol (§4.6) — proactive, not reactive
6. Inter-token stall exceeds `migration.stall_timeout` (default `30s`, off by default — false positives on slow models)

On trigger, evaluate **eligibility**:

```
migrations_used        <  policy.max_migrations            (default 3)
accumulated_tokens     <  policy.max_migration_tokens      (default 8192)
elapsed                <  policy.max_stream_duration       (default 15m)
template_verdict(model) == SAFE                            (see §6)
response_format is text  OR  policy.allow_structured_resume
target model version    == origin model version  OR  policy.allow_cross_version
a healthy non-excluded backend exists
```

If ineligible → append `warning` explaining which predicate failed, then `error`. Never migrate silently into a degraded result.

If eligible → **continuation request**:

- Take the normalized original request.
- Append `{"role": "assistant", "content": <accumulated_text>}` as the final message.
- Set `continue_final_message: true` and `add_generation_prompt: false`.
- Carry over sampling params, but pin `seed` if it was set; recompute `max_tokens` as `original_max_tokens - tokens_already_emitted` (floor at 1).
- Dispatch to a healthy backend, excluding the failed one for `backend.quarantine_window` (default `5s`).
- Append a `migration` journal entry *before* the first continuation chunk.

**Seam reconciliation.** The continuation may repeat the tail of what was already emitted. Before appending continuation chunks:

- Hold the first `policy.seam_window` bytes (default 64) of continuation output.
- Find the longest suffix of `accumulated_text` that is a prefix of the held bytes; strip it.
- Record the overlap length as a metric (`streamweld_seam_overlap_bytes`).
- If overlap is 0 *and* the continuation starts with a capital letter after a non-terminal character, emit a `warning` — likely a restart rather than a continuation.

**Correctness constraints you must handle explicitly:**

- Never split a UTF-8 rune across a journal entry boundary. Buffer partial runes.
- Never split an SSE `data:` frame. Parse and re-emit whole frames.
- Tool-call deltas arrive as fragmented JSON across chunks. In v1, if a migration would land mid-tool-call, refuse the migration and emit `error` with reason `tool_call_boundary`. Do not attempt to stitch.
- If `response_format` is `json_object`/`json_schema`, migration requires `policy.allow_structured_resume`, and the accumulated text must be validated by a streaming JSON parser as a valid *prefix*. If it isn't, refuse.

### 4.5 Stop vs. disconnect

This distinction is the thing everyone else gets wrong. Implement it precisely.

- `POST /v1/streams/{id}/stop` → cancel upstream, append `stopped`, mark journal non-resumable. Returns `202` with the partial text and token usage.
- **Client disconnect is not a stop.** Behaviour governed by `policy.orphan_policy`:
  - `continue` (default) — generation proceeds, journal keeps filling, client may resume
  - `cancel_after` — cancel upstream if no reader reattaches within `policy.orphan_timeout` (default `60s`)
  - `cancel` — Dynamo-like behaviour, cancel immediately
- Config is per-route via the CRD, overridable per-request with `X-Streamweld-Orphan-Policy`.

### 4.6 Drain protocol

The reason this matters: current guidance is to set `terminationGracePeriodSeconds` to 300 so in-flight generations can finish — you pay for an idle GPU for five minutes on every rollout. Streamweld migrates instead of waiting, so the grace period can drop to seconds.

- `POST /internal/backends/{addr}/drain` marks a backend draining.
- Draining backend: receives no new requests; all in-flight streams bound to it are *proactively* migrated immediately.
- Endpoint returns `200` once in-flight count for that backend hits zero, with a timeout.
- The operator injects a `preStop` hook on managed backend pods that calls this endpoint on the proxy Service.
- `streamweldctl drain <pod>` does the same manually.

Document in `docs/operations.md`: "with Streamweld, set `terminationGracePeriodSeconds: 15`, not 300" — and show the measured rollout impact from the chaos suite.

### 4.7 API surface

```
POST   /v1/chat/completions          OpenAI-compatible; stream and non-stream
POST   /v1/completions               legacy completions
GET    /v1/streams/{id}/events       resume (SSE, Last-Event-ID)
GET    /v1/streams/{id}              state, usage, migration history
POST   /v1/streams/{id}/stop         explicit cancel
GET    /v1/models                    proxied from backends
GET    /healthz  /readyz
GET    /metrics                      prometheus
POST   /internal/backends/{addr}/drain
```

Non-streaming requests pass through with journaling disabled — zero added latency. Say so in the README.

---

## 5. Custom resources

`streamweld.io/v1alpha1`, two kinds.

**`InferenceRoute`** — binds a model name to a backend pool and a durability policy.

```yaml
apiVersion: streamweld.io/v1alpha1
kind: InferenceRoute
metadata:
  name: llama-8b
spec:
  model: meta-llama/Llama-3.1-8B-Instruct
  backends:
    selector:
      matchLabels: { app: vllm-llama-8b }
    port: 8000
  policyRef: { name: default-durable }
status:
  healthyBackends: 3
  drainingBackends: 0
  templateVerdict: SAFE          # from the conformance probe
  templateProbedAt: "..."
  activeStreams: 12
  conditions: [...]
```

**`DurabilityPolicy`** — everything in §4.4/§4.5 as a typed spec: `maxMigrations`, `maxMigrationTokens`, `maxStreamDuration`, `orphanPolicy`, `orphanTimeout`, `allowCrossVersion`, `allowStructuredResume`, `seamWindowBytes`, `journalTTL`.

Reconciler responsibilities:

1. Watch EndpointSlices for the selector; push backend set to the proxy via its admin API (not by restarting it).
2. Run the conformance probe (§6) on every newly-Ready backend before admitting it to the pool. A backend that fails the probe is admitted for serving but marked migration-ineligible.
3. Inject the `preStop` drain hook via a mutating webhook on pods matching a managed selector. Make the webhook optional — `--enable-pod-mutation=false` must produce a fully working system where the user adds the hook themselves.
4. Set status conditions: `Ready`, `TemplateSafe`, `Degraded`. Emit Kubernetes Events on verdict changes.

---

## 6. Chat-template conformance checker

**This is the single largest correctness risk in the project.** Some chat templates unconditionally append the end-of-turn token after assistant turns, which closes the message before generation starts — the model then emits an empty continuation or starts a fresh turn. Migration into such a model silently corrupts output.

Build `streamweldctl doctor --backend <url> --model <name>` and the same code path in the operator.

Probes, each run 3× at `temperature=0`:

1. **Continuation** — user: `Count from 1 to 10, numbers only.` assistant prefill: `1 2 3 4` → output must continue with `5`, must not restart at `1`, must not be empty.
2. **Mid-word** — prefill ends mid-token (`The capital of France is Par`) → output must complete the word.
3. **Mid-sentence with punctuation** — prefill ends after a comma → output must not begin with a capitalized new sentence.
4. **Idempotence** — same prefill twice yields the same continuation at `temperature=0`.

Verdicts:

- `SAFE` — all probes pass
- `DEGRADED` — probes 1 and 2 pass, 3 or 4 fail → migration allowed, seam warnings elevated
- `UNSAFE` — probe 1 or 2 fails → migration refused under `strict`, allowed with loud `warning` under `permissive`

Cache verdicts per `(backend_image_digest, model, tokenizer_hash)`. Persist in the `InferenceRoute` status.

Ship a `docs/compatibility.md` table of models you actually probed, with verdicts and dates. Only models you ran. This table is one of the most useful things the project can offer, and it is entirely honest work.

---

## 7. Go data plane — implementation notes

- One goroutine per stream, cancellable via `context`. No global locks on the hot path.
- SSE parsing: write a real incremental parser (`internal/proxy/sse`). Handle `data:` continuation lines, comments, `retry:`, and multi-line payloads. Fuzz it (`go test -fuzz`).
- Backpressure: if a client reader is slower than the producer, keep journaling at full speed and let the reader lag. Never block the producer on a slow consumer. Bounded per-reader channel; drop the reader with `error` if it exceeds `reader.max_lag_bytes`.
- Request normalization: canonicalize the incoming body once, store the normalized form for continuation. Preserve unknown fields verbatim so backend-specific params survive.
- Backend pool: health via `GET /health` at `backend.health_interval`, plus passive ejection on connection failure with a quarantine window.
- Redis failure must degrade, not fail: if the journal backend is unreachable, serve the stream in pass-through mode and expose `streamweld_journal_degraded 1`. Never drop a user's request because Redis is down.
- Graceful shutdown of the proxy itself: drain, migrate nothing (backends are fine), let readers reattach to another proxy replica via Redis. This is why Redis-backed journals allow >1 proxy replica; document that `memory` mode is single-replica only and have the Helm chart refuse `replicas > 1` with `journal.backend=memory`.

---

## 8. `@streamweld/client` (TypeScript)

Published to npm. **Zero runtime dependencies.** ESM + CJS via `tsup`. `strict: true`, `noUncheckedIndexedAccess: true`. Tests in `vitest`. Works in browser, Node ≥20, Deno, Bun, and edge runtimes — no Node-only APIs.

```ts
import { createDurableStream } from "@streamweld/client";

const stream = createDurableStream({
  url: "https://streamweld.example.com/v1/chat/completions",
  body: { model: "llama-8b", messages, stream: true },
  resume: {
    maxAttempts: 5,
    backoff: { initialMs: 250, maxMs: 5000, jitter: true },
  },
  onMigration: (e) => console.debug("producer failover", e),
});

for await (const delta of stream.text) {
  render(delta);
}

// later, from a stop button — NOT an abort
await stream.stop();
```

Requirements:

- Exposes `stream.text` (async iterable of string deltas), `stream.events` (async iterable of typed protocol events), `stream.id`, `stream.state`.
- Tracks the last seen `seq`. On any transport error, reconnects to `/v1/streams/{id}/events` with `Last-Event-ID` and continues the *same* async iterable. The consumer's `for await` loop never breaks.
- Distinguishes three terminations cleanly, with distinct typed outcomes: `done`, `stopped`, `error`.
- `stop()` calls the stop endpoint. Passing an `AbortSignal` aborts the *local* connection only and leaves the generation resumable — document this distinction prominently, since it is the exact bug in Vercel's library.
- Handles `410 Gone` by surfacing a typed `StreamExpiredError`, never by silently restarting the generation.
- Persistence hook: `persist?: { get(): string | null; set(id: string, seq: number): void }` so an app can survive a full page reload by storing the stream id and offset. Ship a `localStorage` helper but don't depend on it.

`@streamweld/ai-sdk` is a thin adapter exporting `StreamweldChatTransport implements ChatTransport` for Vercel AI SDK v5, so existing `useChat` apps adopt Streamweld by swapping one line. Keep it in a separate package so the core SDK stays dependency-free.

---

## 9. Demo application (TypeScript + React)

`apps/demo` — Vite + React + TS + Tailwind. This is the project's shop window; it must be genuinely convincing, not a toy.

Two panes:

- **Left** — a normal chat UI streaming from Streamweld.
- **Right** — a failure injection console with buttons that hit a small demo-only backend:
  - Kill the serving pod (SIGKILL)
  - Trigger a rolling update to a new model version
  - Simulate spot node reclaim (cordon + drain)
  - Drop the client connection
  - Press stop (to show it behaves differently from a drop)
- **Below** — a live stream timeline: seq numbers, chunk arrival, migration markers annotated with backend id and rescued token count, seam overlap, resume points.

Include a toggle: **Streamweld off** routes directly to vLLM. The same button press then truncates the response. The side-by-side is the entire pitch.

Deploy it publicly. A URL an interviewer can click is worth more than the README.

---

## 10. Observability

Prometheus metrics (all with `route`, `model` labels):

```
streamweld_streams_active
streamweld_streams_total{outcome="done|stopped|error"}
streamweld_migrations_total{reason="crash|drain|stall|error_chunk|health"}
streamweld_migrations_refused_total{predicate="..."}
streamweld_tokens_rescued_total
streamweld_prompt_tokens_rebilled_total
streamweld_resumes_total{trigger="client|failover"}
streamweld_seam_overlap_bytes           (histogram)
streamweld_ttft_seconds                 (histogram)
streamweld_inter_token_seconds          (histogram)
streamweld_stream_duration_seconds      (histogram)
streamweld_journal_bytes
streamweld_journal_degraded             (gauge, 0/1)
streamweld_backends{state="healthy|draining|quarantined"}
```

OpenTelemetry traces following the GenAI semantic conventions. One span per stream, child spans per backend attempt, migration as a span event.

Ship a Grafana dashboard JSON in `deploy/helm/streamweld/dashboards/`. Structured logging via `log/slog`, JSON, with `stream_id` on every line.

---

## 11. Infrastructure

- **Helm chart** `deploy/helm/streamweld`: proxy Deployment + HPA + PDB, operator Deployment, CRDs, ServiceMonitor, optional Redis subchart, values schema (`values.schema.json`) with validation. Chart must lint clean and install on a bare kind cluster.
- **Terraform** `infra/terraform`: a demo cluster with one GPU node pool and one spot GPU node pool, so the spot-reclaim scenario is real and not simulated. Modularized, remote state configurable, `terraform destroy` must fully clean up. Include a cost note in the README.
- **Samples**: a working vLLM Deployment + InferenceRoute + DurabilityPolicy that a user can `kubectl apply` after `helm install`.

---

## 12. Chaos harness

`test/chaos` — Go, drives a kind cluster with a fake OpenAI-compatible backend (deterministic token generator, configurable failure injection) so the suite runs in CI without GPUs. A separate opt-in profile runs against real vLLM.

Scenarios, each with N concurrent streams:

| Scenario | Injection |
|---|---|
| `pod-kill` | SIGKILL a backend mid-generation |
| `rolling-update` | `kubectl set image`, maxUnavailable=1 |
| `spot-reclaim` | cordon + drain a node |
| `backend-oom` | backend returns error chunk mid-stream |
| `client-drop` | client closes TCP, reconnects after delay |
| `explicit-stop` | client calls stop |
| `redis-down` | journal backend unavailable |
| `slow-consumer` | reader lags beyond limit |
| `unsafe-template` | backend with a non-conforming chat template |

For each: streams started, streams completed, streams migrated, tokens rescued, prompt tokens re-billed, seam overlap p50/p99, added TTFT overhead vs. direct passthrough, and — critically — **output correctness** verified against the deterministic backend's expected token sequence.

`make bench` writes `benchmarks/results.md` and `results.json`. The README table is generated from that file. Nightly CI run; regression gate on the correctness column.

---

## 13. CI/CD

GitHub Actions:

- `go test ./... -race`, `golangci-lint`, `go vet`, fuzz seed corpus
- `pnpm typecheck`, `pnpm test`, `pnpm build` across the TS workspace
- kind-based e2e on every PR
- chaos suite nightly + on release tags
- Helm chart lint + install test
- `terraform validate` + `tflint`
- Trivy on images, CodeQL on Go and TypeScript
- Release: `goreleaser` for binaries, multi-arch images to ghcr.io, Helm chart to OCI, npm publish with provenance
- Docs site from `docs/` (Astro Starlight or MkDocs Material)

Everything reproducible via `make`: `make dev`, `make test`, `make e2e`, `make bench`, `make demo`.

---

## 14. Build order

Do not skip ahead. Each phase must satisfy its acceptance criteria.

### Phase 0 — Foundations
Repo scaffolding, Go module, pnpm workspace, Makefile, CI skeleton, `docs/protocol.md` written in full, ADR 001 recording the language split.
**Accept:** `make test` passes on an empty test suite; protocol doc is complete enough that someone else could implement the proxy from it.

### Phase 1 — Passthrough proxy
OpenAI-compatible proxy, SSE parser + fuzz corpus, single backend, no journal. Non-streaming passthrough.
**Accept:** `curl` against Streamweld returns byte-identical output to `curl` against vLLM. TTFT overhead < 5ms p99, measured.

### Phase 2 — Journal + resume
Journal interface, memory backend, stream ids, idempotency keys, `Last-Event-ID` resume, `410 Gone` semantics, stop endpoint, orphan policy.
**Accept:** kill the client mid-stream, reconnect with `Last-Event-ID`, receive the remainder exactly once. Explicit stop is distinguishable from a drop in the journal.

### Phase 3 — Producer failover
Backend pool, health checks, eligibility evaluation, continuation requests, seam reconciliation, UTF-8 and SSE frame safety, tool-call refusal, migration journal entries.
**Accept:** SIGKILL the backend mid-generation; the client's `for await` loop never breaks and final output matches the deterministic expected sequence. Every refusal predicate has a test.

### Phase 4 — Conformance checker
`streamweldctl doctor`, four probes, verdict caching, `docs/compatibility.md` with real probed models.
**Accept:** a deliberately broken chat template yields `UNSAFE` and migration is refused under strict policy.

### Phase 5 — Redis journal + multi-replica
Redis Streams backend, fan-out readers, cross-replica resume, degraded-mode fallback.
**Accept:** resume a stream through a *different* proxy replica than the one that started it. Kill Redis mid-stream; the request completes in degraded mode.

### Phase 6 — Operator
CRDs, reconcilers, EndpointSlice watching, conformance gating, drain protocol, optional pod-mutating webhook, Helm chart.
**Accept:** `helm install` on kind, apply the sample CRs, roll the backend Deployment with `terminationGracePeriodSeconds: 15` and lose zero streams.

### Phase 7 — TypeScript SDK
`@streamweld/client` and `@streamweld/ai-sdk`, full test suite including simulated transport failures, published to npm.
**Accept:** an app using `useChat` adopts Streamweld by changing one line and survives all Phase 3 and 6 failure modes.

### Phase 8 — Observability + chaos + demo
Metrics, OTel spans, Grafana dashboard, full chaos harness, `benchmarks/results.md`, demo app deployed.
**Accept:** README numbers are generated from committed benchmark output. Demo URL is live.

### Phase 9 — Release
Terraform module, docs site, `goreleaser`, container images, Helm OCI, CONTRIBUTING, SECURITY, Apache-2.0, issue templates, `good-first-issue` set.
**Accept:** a stranger can go from zero to a working durable stream in under ten minutes following the quickstart, on a machine you've never touched.

---

## 15. README requirements

Structure it in this order:

1. One-sentence thesis + a 20-second asciinema/GIF of the demo: pod dies, stream survives.
2. The problem table from §1.
3. **Honest prior-art comparison** — Dynamo, Vercel `resumable-stream`, LLM gateways. What each does well. What Streamweld adds. What Streamweld does *not* do.
4. Quickstart: `helm install` → `kubectl apply -f samples/` → `curl` that survives a `kubectl delete pod`.
5. Results table generated from `benchmarks/results.md`.
6. Compatibility table from the conformance probes.
7. Architecture diagram.
8. Configuration reference.
9. Non-goals.

Never claim resilience — show the injected-failure table. That evidence discipline is the point.

---

## 16. Definition of done

- Stream survives backend crash, rolling update, spot reclaim, and client disconnect, with verified output correctness under each.
- Stop and disconnect are unambiguously different, at protocol level and in the SDK.
- Unsafe chat templates are detected and refuse migration rather than corrupting output.
- Journal backend failure degrades to passthrough instead of dropping requests.
- Non-streaming and non-resumed streaming paths add under 5ms p99.
- One command installs it; one command tears it down.
- Every number published is reproducible by `make bench`.
