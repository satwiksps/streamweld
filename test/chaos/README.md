# Chaos harness

The harness has three deliberately separate profiles:

- `local` is the reproducible default used by `make bench`. It runs all nine scenarios with N concurrent deterministic streams, exercises the production seam reconciler, and measures TTFT through an actual in-process Streamweld HTTP server versus its direct fake backend. Kubernetes failures are modeled in this profile; the artifact does not claim that a cluster was used.
- `kind` builds the proxy, operator, and deterministic OpenAI-compatible backend, creates a fresh three-worker kind cluster (two inference workers plus an isolated system worker) pinned to Kubernetes 1.35.0, and physically injects every required failure. It cannot silently skip: `make chaos-kind` exits non-zero when Docker, kind, kubectl, Helm, cluster setup, any injection, or any output check fails. For each migrated stream, the harness observes the raw repeated continuation delta carried by the test backend, reconciles it against the accepted prefix, and reads prompt usage from the first continuation chunk; missing seam or re-billing evidence fails the run.
- `vllm` is opt-in and sends paired, seeded, temperature-zero requests through a real vLLM endpoint and Streamweld. It requires exact output equality and records separately named artifacts. It is a real-model compatibility/TTFT profile and makes no failure-resilience claim.

The nine deterministic scenarios are `pod-kill`, `rolling-update`, `spot-reclaim`, `backend-oom`, `client-drop`, `explicit-stop`, `redis-down`, `slow-consumer`, and `unsafe-template`. The first four migrate every injected stream. Explicit stop must terminate as `stopped`; an unsafe continuation must terminate as `migration_refused`; Redis loss must complete as `done_degraded`. Two proxy replicas use the chart's mTLS owner relay so live operations remain owner-directed behind the NodePort. The slow-consumer case uses the system worker's local NodePort directly, combines a constrained client receive window with a pod-scoped 4 KiB proxy TCP send bound, waits for the batched deterministic producer to finish, and must observe `reader_lag_exceeded` before resuming; completing without that eviction fails the case. A disposable NetworkPolicy admits only the runner's detected kind bridge gateway to that NodePort. The policy, short-lived relay certificate, and TCP sysctl are confined to the kind fixture; they do not alter production chart defaults.

Run the local correctness suite and regenerate committed evidence:

```sh
make chaos
make bench
make bench-check
```

Run the kind profile on a Linux host (Docker must be available):

```sh
make chaos-kind
```

Keep the cluster for inspection with `STREAMWELD_CHAOS_KEEP_CLUSTER=true`. A later run requires that retained cluster to be deleted or a fresh `KIND_CLUSTER_NAME` to be supplied. Override concurrency and output length with `STREAMWELD_CHAOS_STREAMS` and `STREAMWELD_CHAOS_TOKENS`. The generated kind artifacts default to a temporary directory; set `STREAMWELD_CHAOS_RESULTS_DIR` to retain them.

Run the real-vLLM profile against already reachable endpoints:

```sh
go run ./cmd/streamweldctl bench \
  --profile vllm \
  --direct-url http://127.0.0.1:8000 \
  --proxy-url http://127.0.0.1:8080 \
  --model meta-llama/Llama-3.1-8B-Instruct \
  --streams 8 \
  --tokens 64 \
  --output benchmarks/vllm-local
```

Latency fields are wall-clock samples scoped to their recorded host and profile. CI gates the correctness fields and artifact provenance, not latency stability across unlike runners.
