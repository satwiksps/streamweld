---
title: Prior art and non-goals
description: Where Streamweld overlaps with established systems and where its scope deliberately ends.
---

# Prior art and non-goals

Streamweld builds on ideas already proven in serving runtimes, resumable
transports, and gateways. Its reason to exist is the combination of an external
replay cursor, explicit stream journal, conservative continuation policy, and
Kubernetes lifecycle coordination in a small standalone layer.

| Project | What it does well | What Streamweld adds in its scope |
|---|---|---|
| [NVIDIA Dynamo](https://docs.nvidia.com/dynamo/dev/kubernetes/fault-tolerance/request-migration) | In-flight request migration inside a distributed inference runtime, including transparent continuation | A standalone OpenAI-compatible proxy, externally replayable SSE journal/cursor, typed stop semantics, client SDK, and CRD-driven policy |
| [Vercel resumable-stream](https://github.com/vercel/resumable-stream) | Redis-backed resumable streams that can outlive the original reader in serverless apps | Backend-producer failover, continuation safety checks, model/template admission, and Kubernetes drain coordination |
| [LiteLLM](https://docs.litellm.ai/) | Broad provider unification, retries/fallbacks, routing, auth, budgets, and observability | Ordered generation journals and exact reader replay around self-hosted inference attempts |
| [Envoy AI Gateway](https://aigateway.envoyproxy.io/docs/capabilities/traffic/provider-fallback/) | Gateway-native provider routing, retry, and fallback | Continuation of an already-started token stream with explicit seam and template checks |

This is not a claim that the other projects are incapable of adjacent features,
nor that Streamweld is a better general gateway or serving runtime. Dynamo is a
broader inference platform; LiteLLM and Envoy AI Gateway have much broader
gateway concerns; resumable-stream is a focused application transport primitive.

## Non-goals

- training, model execution, batching, KV-cache management, or GPU scheduling;
- authentication, tenant isolation, quotas, billing, or a provider catalog;
- semantic reconciliation of divergent stochastic continuations;
- automatic migration through unsafe tool-call or structured-output boundaries;
- bit-for-bit determinism across backends, versions, or sampling runs;
- durable multi-replica operation without an external journal;
- pretending a journal outage is still resumable.

Streamweld refuses a migration when its proof obligations do not hold and exposes
degraded durability when the journal is unavailable. Those limits are part of the
protocol, not implementation footnotes.
