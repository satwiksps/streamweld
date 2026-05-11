---
title: Chaos results
description: Reproducible correctness and latency evidence from the committed deterministic profile.
---

`make bench` writes both a machine-readable artifact and the Markdown table used
by the repository README. The committed default is an in-process deterministic
fault model with a paired HTTP TTFT probe. It does **not** claim physical
Kubernetes disruption; nightly kind CI is the physical failure-injection gate.

The committed run used eight concurrent streams and 64 deterministic output
tokens per stream. All nine scenario-specific correctness checks passed.

| Scenario | Started | Completed | Migrated | Expected terminal | Correct |
|---|---:|---:|---:|---|:---:|
| pod kill | 8 | 8 | 8 | done | yes |
| rolling update | 8 | 8 | 8 | done | yes |
| spot reclaim | 8 | 8 | 8 | done | yes |
| backend OOM/error chunk | 8 | 8 | 8 | done | yes |
| client drop | 8 | 8 | 0 | done | yes |
| explicit stop | 8 | 0 | 0 | stopped | yes |
| Redis down | 8 | 8 | 0 | done, degraded | yes |
| slow consumer | 8 | 8 | 0 | done | yes |
| unsafe template | 8 | 0 | 0 | migration refused | yes |

The paired local run measured 3.375 ms direct TTFT p50 and 7.838 ms through
Streamweld, an added 4.463 ms p50 on that host/profile. Treat these as run
evidence, not a cross-host threshold.

Reproduce and verify:

```sh
make bench
go run ./test/chaos/cmd/bench verify
```

Review the exact
[`results.json`](https://github.com/streamweld/streamweld/blob/main/benchmarks/results.json)
before comparing runs. Correctness—not a portable wall-clock number—is the
regression gate.
