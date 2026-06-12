# Streamweld local deterministic chaos model results

Generated from `benchmarks/results.json` by `make bench`. Do not edit this table by hand.

This committed default is an in-process model/simulation. It does not claim Kubernetes process disruption; the nightly kind profile is the physical failure-injection gate.

This is the **deterministic-local** profile (`in-process concurrent fault model plus paired HTTP TTFT probe`), generated at `2026-08-22T20:08:56.4308824Z`. It uses 8 concurrent streams per scenario and 64 deterministic output tokens per stream.

Timing scope: one paired N-stream wall-clock p50 baseline measured before the matrix and joined to every scenario row; compare only within this host/profile. Latency is evidence from this run, not a cross-host regression threshold. Correctness is the regression gate.

The paired fake backend applies the same 2.000 ms first-token delay to direct and Streamweld requests; TTFT values serialize to 0.001 ms resolution.

## Local rollout grace-window model

The `deterministic-local` profile measured an amortized mean of **0.085 ms per cohort** across 32 sequential local `rolling-update` cohorts; every cohort ended with all 8 simulated streams terminal (8 migrated, 8 completed).

| Grace-window comparison | Value |
|---|---:|
| Legacy configured grace period | 300 s |
| Streamweld configured grace period | 15 s |
| Configured grace-window reduction | 285 s |
| Modelled headroom after the measured local mean inside the 15 s window | 14999.915 ms |
| Measured local completion fits the 15 s window | true |

The measured value is an in-process migration-model interval, not physical Kubernetes rollout timing. The configured-window arithmetic does not measure Kubernetes control-plane, scheduling, image-pull, readiness, process-exit, GPU-idle, cost, or end-to-end rollout duration. Measurement scope: amortized monotonic wall-clock mean across a batch of sequential in-process rolling-update cohorts, from before each harness cohort setup through every simulated stream reaching its terminal result; repeated cohorts overcome host clock granularity and include harness overhead; excludes Kubernetes control-plane, scheduling, image-pull, readiness, process-exit, GPU-idle, and cost timing.

| Scenario | Tokens/stream | Started | Completed | Migrated | Rescued tokens | Prompt tokens re-billed | Seam p50/p99 (bytes) | Direct TTFT p50 (ms) | Streamweld TTFT p50 (ms) | Added TTFT p50 (ms) | Correct |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|:---:|
| pod-kill | 64 | 8 | 8 | 8 | 175 | 128 | 10/20 | 3.625 | 5.585 | 1.960 | true |
| rolling-update | 64 | 8 | 8 | 8 | 175 | 128 | 10/20 | 3.625 | 5.585 | 1.960 | true |
| spot-reclaim | 64 | 8 | 8 | 8 | 175 | 128 | 10/20 | 3.625 | 5.585 | 1.960 | true |
| backend-oom | 64 | 8 | 8 | 8 | 175 | 128 | 10/20 | 3.625 | 5.585 | 1.960 | true |
| client-drop | 64 | 8 | 8 | 0 | 0 | 0 | 0/0 | 3.625 | 5.585 | 1.960 | true |
| explicit-stop | 64 | 8 | 0 | 0 | 0 | 0 | 0/0 | 3.625 | 5.585 | 1.960 | true |
| redis-down | 64 | 8 | 8 | 0 | 0 | 0 | 0/0 | 3.625 | 5.585 | 1.960 | true |
| slow-consumer | 64 | 8 | 8 | 0 | 0 | 0 | 0/0 | 3.625 | 5.585 | 1.960 | true |
| unsafe-template | 64 | 8 | 0 | 0 | 0 | 0 | 0/0 | 3.625 | 5.585 | 1.960 | true |

Scenario-specific expected terminals are recorded in the JSON artifact: explicit stop is `stopped`, unsafe-template is `migration_refused`, and Redis loss is `done_degraded`.
