# Chat-template compatibility

Streamweld only permits producer migration when the target backend's chat
template can continue an existing assistant message. The verdict comes from
four probes—continuation, mid-word completion, punctuation continuity, and
temperature-zero idempotence—each repeated three times.

Run the same checker used by the operator:

```console
streamweldctl doctor --backend http://127.0.0.1:8000 --model MODEL
streamweldctl doctor --backend http://127.0.0.1:8000 --model MODEL --json
```

The operator caches a report only when it has the complete immutable key
`(backend image digest, model, tokenizer hash)`. An address is intentionally
not part of the key, so identical replicas share a result; an incomplete key
is always probed and is never cached.

## Probed compatibility

| Backend | Model | Verdict | Probe date (UTC) | Evidence |
|---|---|---:|---:|---|
| In-process deterministic safe fixture | `fixture-model` | `SAFE` | 2026-08-22 | `TestCheckerProbeMatrixAndVerdicts/all_probes_pass` and `TestDoctorCommandHumanReport` |
| In-process deliberately broken-template fixture | `broken-model` | `UNSAFE` | 2026-08-22 | `TestDoctorCommandBrokenTemplateJSON` |

These are protocol fixtures, not production inference images or claims about
real model families. No production backend has been added to this table
because one was not actually run during this build. Add a row only from a
captured `doctor --json` report against the exact image digest, model, and
tokenizer hash being claimed; upgrades require a new probe and a new row.

`SAFE` means all four probes passed. `DEGRADED` means the two core continuation
probes passed while punctuation or idempotence failed. `UNSAFE` means the
continuation or mid-word probe failed; strict template policy refuses
migration to that backend.
