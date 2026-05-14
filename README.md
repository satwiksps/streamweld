# Streamweld

> Your token stream shouldn't die because a pod got evicted or a phone switched to cellular.

Streamweld is a durable stream layer for self-hosted LLM inference: one OpenAI-compatible stream identity and append-only journal that can outlive both its reader connection and its current backend attempt.

[▶ Play the 20-second terminal cast](docs/assets/streamweld-demo.cast) · [Open the live failure lab](https://streamweld-failure-lab.satwiksub.chatgpt.site) · [Read the documentation](https://streamweld-docs.satwiksub.chatgpt.site)

```sh
asciinema play docs/assets/streamweld-demo.cast
```

The cast is a scripted walkthrough of the deterministic pod-kill scenario. The
committed local benchmark is an in-process fault model; the non-skippable nightly
kind job is the physical Kubernetes failure gate.

## The problem

When an LLM streams a response and something breaks mid-stream, everything
already generated is commonly lost and the request restarts from zero, re-billing
the prompt.

| Failure | Who breaks | Status quo |
|---|---|---|
| Backend pod OOMs or crashes | producer | stream dies, client sees truncated output |
| Rolling update / model version bump | producer | in-flight generations killed at `terminationGracePeriodSeconds` |
| Spot GPU node reclaimed | producer | same |
| Client WiFi → cellular, tab backgrounded | consumer | stream dies, server may keep burning GPU |
| Proxy/LB idle timeout | transport | stream dies |
| User presses stop | intentional | frequently indistinguishable from a disconnect |

Streamweld treats a generation as a resource with a lowercase ULID, an ordered
journal, and an exact exclusive cursor. A socket close detaches one reader; only
the stop endpoint cancels generation.

## Prior art, honestly

Streamweld does not claim to have invented request migration, Redis-backed
resume, or gateway fallback. It combines a deliberately narrow subset of those
ideas for OpenAI-compatible self-hosted inference.

| Project | What it does well | What Streamweld adds | What Streamweld does not do better |
|---|---|---|---|
| [NVIDIA Dynamo](https://docs.nvidia.com/dynamo/dev/kubernetes/fault-tolerance/request-migration) | In-flight request migration inside a distributed inference runtime, including transparent continuation | A small standalone proxy/operator, externally replayable SSE cursor, explicit stop protocol, TypeScript client, and CRD policy | Dynamo is the broader inference platform; Streamweld does not replace its scheduling, serving runtime, or cache-aware architecture |
| [Vercel `resumable-stream`](https://github.com/vercel/resumable-stream) | Redis-backed application streams that can keep producing after the original reader leaves and resume later | Backend-producer failover, continuation safety gates, model/template admission, and Kubernetes drain coordination | Streamweld is not a general serverless stream primitive and does not match its framework-native application ergonomics |
| [LiteLLM](https://docs.litellm.ai/) | Broad provider unification, retries and fallbacks, routing, authentication, budgets, and cost controls | A generation journal with exact reader replay and conservative continuation of an already-started self-hosted stream | Streamweld is not a multi-provider gateway, auth plane, or spend-management product |
| [Envoy AI Gateway](https://aigateway.envoyproxy.io/docs/capabilities/traffic/provider-fallback/) | Gateway-native routing, retry, and provider fallback | In-flight token continuation with explicit seam, template, request-shape, and terminal-state checks | Streamweld is not a general Envoy traffic platform and has a much smaller routing surface |

The differentiator is not “retry.” It is one protocol that separates reader
resume, producer migration, and user stop while refusing a migration when the
continuation proof does not hold.

## Quickstart

Prerequisites: Kubernetes 1.27 or newer, Helm 3.14 or newer, and `kubectl`.
Tagged releases install from the OCI chart:

```sh
helm upgrade --install streamweld oci://ghcr.io/streamweld/charts/streamweld \
  --namespace streamweld-system \
  --create-namespace
```

From a source checkout before the first tagged release, use the local chart:

```sh
helm upgrade --install streamweld ./deploy/helm/streamweld \
  --namespace streamweld-system \
  --create-namespace
```

Apply the deterministic CPU-only OpenAI fixture and Streamweld policy:

```sh
kubectl apply -f deploy/samples/
kubectl -n streamweld-system wait --for=condition=Available \
  deployment/streamweld-proxy deployment/streamweld-operator \
  deployment/streamweld-sample-backend --timeout=180s
```

Port-forward the proxy in one terminal:

```sh
kubectl -n streamweld-system port-forward service/streamweld-proxy 8080:8080
```

Start a long deterministic stream in a second terminal:

```sh
curl -N http://127.0.0.1:8080/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"streamweld/deterministic-vllm","messages":[{"role":"user","content":"Count steadily."}],"max_tokens":512,"stream":true}'
```

While `curl` is printing, delete exactly one serving backend from a third
terminal:

```sh
POD_TO_KILL=$(kubectl -n streamweld-system get pods \
  -l app.kubernetes.io/name=streamweld-sample-backend \
  -o jsonpath='{.items[0].metadata.name}')
kubectl -n streamweld-system delete pod "$POD_TO_KILL" --wait=false
```

The same `curl` remains attached while the proxy records a migration and
continues on the other compatible backend. The fixture is for protocol and
rollout validation, not a production-model compatibility or performance claim.
See the [ten-minute guide](https://streamweld-docs.satwiksub.chatgpt.site/getting-started/)
for teardown and the release/source distinction.

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

## Compatibility

Migration requires the target backend's chat template to continue an existing
assistant message. `streamweldctl doctor` repeats continuation, mid-word,
punctuation, and temperature-zero idempotence probes and keys its cache by the
immutable `(backend image digest, model, tokenizer hash)` tuple.

```sh
streamweldctl doctor --backend http://127.0.0.1:8000 --model MODEL --json
```

| Backend | Model | Verdict | Probe date (UTC) | Evidence |
|---|---|:---:|---:|---|
| In-process deterministic safe fixture | `fixture-model` | `SAFE` | 2026-08-22 | `TestCheckerProbeMatrixAndVerdicts/all_probes_pass` and `TestDoctorCommandHumanReport` |
| In-process deliberately broken-template fixture | `broken-model` | `UNSAFE` | 2026-08-22 | `TestDoctorCommandBrokenTemplateJSON` |

These are protocol fixtures, not claims about real production models. No
production row is published because no exact production image/model/tokenizer
tuple was probed during this build. Add one only from a captured doctor report;
strict policy refuses `UNSAFE` targets. See the
[compatibility methodology](https://streamweld-docs.satwiksub.chatgpt.site/reference/compatibility/).

## Architecture

```text
                                 Kubernetes control plane
                         ┌──────────────────────────────────┐
                         │ InferenceRoute + DurabilityPolicy│
                         │ operator + conformance cache     │
                         └───────────────┬──────────────────┘
                                         │ snapshots / drain
                                         ▼
client ── OpenAI HTTP + SSE ──▶ ┌──────────────────────┐
                                 │ Streamweld proxy     │
resume cursor ◀────────────────▶ │ stream + attempts    │
                                 └──────┬────────┬──────┘
                                        │        │
                              commit    │        │ OpenAI-compatible
                                        ▼        ▼
                                  ┌─────────┐  ┌─────────────────┐
                                  │ journal │  │ inference pool  │
                                  │ memory  │  │ vLLM / SGLang / │
                                  │ or Redis│  │ TGI / fixtures  │
                                  └─────────┘  └─────────────────┘
```

The Go proxy commits complete UTF-8-safe upstream SSE events before publishing
them. Readers replay and tail independently, so a slow reader cannot block the
producer. Non-streaming requests use the direct pass-through path with
journaling disabled. The TypeScript client reconnects one async iterable from
its exact cursor; the AI SDK package supplies a Vercel AI SDK v5 transport.

On producer failure, Streamweld checks migration budget, model version,
chat-template verdict, request shape, token budget, structured-output prefix,
and tool-call boundaries. A successful continuation removes only the bounded
UTF-8-safe seam overlap. A failed predicate becomes a visible warning and
terminal `migration_refused`, never a silent restart.

The normative surface is in [`docs/protocol.md`](docs/protocol.md); operational
details are in [`docs/operations.md`](docs/operations.md), and TypeScript usage
is in [`docs/client.md`](docs/client.md).

## Configuration

| Helm value | Default | Meaning |
|---|---:|---|
| `proxy.replicaCount` | `1` | Requires Redis before it can exceed one |
| `journal.backend` | `memory` | Use `redis` for shared cross-replica durability |
| `journal.ttl` | `10m` | Terminal journal and idempotency retention |
| `journal.maxBytesPerStream` | `4194304` | Memory ring cap per stream |
| `journal.maxTotalBytes` | `268435456` | Memory journal global cap |
| `reader.maxLagBytes` | `1048576` | Independent reader backlog before eviction |
| `reader.writeTimeout` | `30s` | Downstream write/flush deadline |
| `migration.maxMigrations` | `3` | Continuation-attempt budget |
| `migration.maxTokens` | `8192` | Accepted-token limit before migration refusal |
| `migration.maxStreamDuration` | `15m` | Stream-age limit before migration refusal |
| `migration.allowCrossVersion` | `false` | Permit target model-version mismatch |
| `migration.allowStructuredResume` | `false` | Permit validated structured-prefix continuation |
| `migration.seamWindowBytes` | `64` | Maximum overlap-reconciliation window |
| `migration.templateMode` | `strict` | Required conformance verdict |
| `orphan.policy` | `continue` | Behavior when the last reader detaches |
| `orphan.timeout` | `60s` | Grace period for `cancel_after` |

The chart schema rejects unsafe layouts: memory journals cannot scale beyond one
proxy, Redis mode needs a URL source, and the production owner relay needs mutual
TLS. See the [full configuration reference](https://streamweld-docs.satwiksub.chatgpt.site/reference/configuration/)
and [`deploy/helm/streamweld/values.yaml`](deploy/helm/streamweld/values.yaml).

The Terraform demo creates a small CPU system pool plus on-demand and real Spot
GPU pools. It incurs GKE control-plane, VM/GPU, disk, and network charges; Spot is
discounted, not free. Prices and quotas vary, so the module intentionally links
the official calculators instead of freezing a cost estimate. Review
[`infra/terraform/README.md`](infra/terraform/README.md), configure remote state,
and run `terraform destroy` when finished. The project, billing setup, API
enablement, quota, and versioned state bucket are documented prerequisites
outside that destroy boundary.

## Non-goals

- **Not an inference engine.** vLLM, SGLang, and TGI execute models; Streamweld
  remains an OpenAI-compatible layer in front of them.
- **Not a scheduler, autoscaler, or GPU placement system.** It can coexist with
  Kueue, KEDA, llm-d, or a cache-aware router.
- **Not a KV-cache migration system.** Continuation can re-bill prompt tokens;
  the metrics make that cost explicit.
- **Not a general gateway.** Provider catalogs, authentication, tenant RBAC,
  quotas, billing, and cost controls are outside protocol v1.
- **Not a semantic repair engine.** It refuses unsafe tool, structured-output,
  template, or request-shape boundaries and cannot make stochastic sampling
  bit-for-bit deterministic.
- **Not a model-quality benchmark product.** The chaos harness exists to prove
  Streamweld protocol correctness under injected failures.
- **Not durably multi-replica without external state.** Memory mode is
  single-process; Redis loss is surfaced as explicit, non-resumable degradation.

Apache-2.0 licensed. See [`CONTRIBUTING.md`](CONTRIBUTING.md),
[`SECURITY.md`](SECURITY.md), and the scoped
[`good-first-issue` candidates](docs/good-first-issues.md).
