---
title: Chaos results
description: Reproducible correctness and latency evidence from the committed deterministic profile.
---

# Chaos results

`make bench` writes the machine-readable artifact and both published Markdown
tables from that artifact. The docs build imports the committed
`benchmarks/results.md` directly, so timings and scenario outcomes are never
copied into a second hand-maintained page.

[Read the generated chaos evidence](../source/benchmarks.md). The committed default
is an in-process deterministic fault model with a paired HTTP TTFT probe. It
does **not** claim physical Kubernetes disruption; nightly kind CI is the
physical failure-injection gate.

Reproduce and verify:

```sh
make bench
go run ./cmd/streamweldctl bench --verify
```

Review the exact machine-readable
[`results.json`](https://github.com/satwiksps/streamweld/blob/main/benchmarks/results.json)
before comparing runs. Correctness—not a portable wall-clock number—is the
regression gate.
