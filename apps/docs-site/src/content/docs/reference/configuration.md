---
title: Configuration
description: Safe defaults and the chart values that materially change durability behavior.
---

The standalone proxy resolves built-in defaults, then environment variables,
then command-line flags. Helm renders those settings from validated values.

| Helm value | Default | Effect |
|---|---:|---|
| `proxy.replicaCount` | `1` | Requires Redis before it can exceed one |
| `journal.backend` | `memory` | Choose `redis` for shared durability |
| `journal.ttl` | `10m` | Terminal journal and idempotency retention |
| `journal.maxBytesPerStream` | `4194304` | Memory ring cap per stream |
| `journal.maxTotalBytes` | `268435456` | Memory journal global cap |
| `reader.maxLagBytes` | `1048576` | Independent reader backlog before eviction |
| `reader.writeTimeout` | `30s` | Maximum downstream write or flush duration |
| `migration.maxMigrations` | `3` | Continuation-attempt budget |
| `migration.maxTokens` | `8192` | Maximum accepted tokens before refusing a move |
| `migration.maxStreamDuration` | `15m` | Maximum stream age before refusing a move |
| `migration.allowCrossVersion` | `false` | Permit target model-version mismatch |
| `migration.allowStructuredResume` | `false` | Permit validated structured-prefix continuation |
| `migration.seamWindowBytes` | `64` | Bounded overlap reconciliation window |
| `migration.templateMode` | `strict` | Required conformance verdict |
| `orphan.policy` | `continue` | Behavior when the last reader detaches |
| `orphan.timeout` | `60s` | Grace period for `cancel_after` |

## Production Redis profile

```yaml
journal:
  backend: redis
  redis:
    existingSecret: streamweld-redis
    secretKey: redis-url
    keyPrefix: production
proxy:
  replicaCount: 2
autoscaling:
  enabled: true
  minReplicas: 2
  maxReplicas: 5
```

## Migration policy posture

Start with strict template checks, matching model versions, structured resume
disabled, and stall detection disabled. Relax one predicate only after running
the checker against the immutable backend image digest, exact model, and
tokenizer hash.

The exhaustive source of chart defaults and validation is
[`values.yaml`](https://github.com/satwiksps/streamweld/blob/main/deploy/helm/streamweld/values.yaml)
plus
[`values.schema.json`](https://github.com/satwiksps/streamweld/blob/main/deploy/helm/streamweld/values.schema.json).
