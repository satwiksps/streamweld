# Streamweld local deterministic chaos model results

Generated from `benchmarks/results.json` by `make bench`. Do not edit this table by hand.

This committed default is an in-process model/simulation. It does not claim Kubernetes process disruption; the nightly kind profile is the physical failure-injection gate.

This is the **deterministic-local** profile (`in-process concurrent fault model plus paired HTTP TTFT probe`), generated at `2026-08-22T18:13:20.9191317Z`. It uses 8 concurrent streams per scenario and 64 deterministic output tokens per stream.

Timing scope: one paired N-stream wall-clock p50 baseline measured before the matrix and joined to every scenario row; compare only within this host/profile. Latency is evidence from this run, not a cross-host regression threshold. Correctness is the regression gate.

The paired fake backend applies the same 2.000 ms first-token delay to direct and Streamweld requests; TTFT values serialize to 0.001 ms resolution.

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

Scenario-specific expected terminals are recorded in the JSON artifact: explicit stop is `stopped`, unsafe-template is `migration_refused`, and Redis loss is `done_degraded`.
