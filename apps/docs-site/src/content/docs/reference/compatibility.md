---
title: Compatibility probes
description: How the checker evaluates chat templates and what has actually been tested.
---

# Compatibility probes

Safe producer migration depends on a target backend's ability to continue an
existing assistant message. Run the same checker used by the operator:

```sh
streamweldctl doctor --backend http://127.0.0.1:8000 --model MODEL
streamweldctl doctor --backend http://127.0.0.1:8000 --model MODEL --json
```

The checker repeats continuation, mid-word completion, punctuation continuity,
and temperature-zero idempotence probes three times. Reports are cached only for
the complete immutable tuple `(backend image digest, model, tokenizer hash)`.

| Backend | Model | Verdict | Probe date | Evidence |
|---|---|:---:|---:|---|
| In-process deterministic safe fixture | `fixture-model` | `SAFE` | 2026-08-22 | `TestCheckerProbeMatrixAndVerdicts/all_probes_pass` |
| In-process deliberately broken-template fixture | `broken-model` | `UNSAFE` | 2026-08-22 | `TestDoctorCommandBrokenTemplateJSON` |

These are protocol fixtures, not production inference images. No real model
family is listed because one was not run for the committed build. Add a
production row only from a captured JSON report against the exact immutable
tuple being claimed; re-run after any upgrade.

- `SAFE`: all four probe classes passed.
- `DEGRADED`: both core continuation probes passed, but punctuation or
  idempotence did not.
- `UNSAFE`: continuation or mid-word behavior failed; strict policy refuses it.
