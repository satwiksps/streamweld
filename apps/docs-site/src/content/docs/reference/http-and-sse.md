---
title: HTTP and SSE
description: Public endpoints, Streamweld headers, journal entry events, and terminal behavior.
---

Streamweld preserves the OpenAI-compatible request surface and adds durable
stream operations.

| Method and path | Purpose |
|---|---|
| `POST /v1/chat/completions` | Streaming durability or non-streaming pass-through |
| `POST /v1/completions` | Legacy completion durability for supported shapes |
| `GET /v1/models` | Pass-through model list |
| `GET /v1/streams/{id}/events` | Replay after an exclusive cursor, then tail |
| `GET /v1/streams/{id}` | Metadata, status, cursor bounds, usage, and migrations |
| `POST /v1/streams/{id}/stop` | Explicitly cancel generation and commit `stopped` |
| `GET /healthz` | Process liveness |
| `GET /readyz` | Admission readiness and serviceable backend availability |
| `GET /metrics` | Prometheus exposition |

## Request and response headers

| Header | Meaning |
|---|---|
| `X-Streamweld-Idempotency-Key` | Bind retries of an initial streaming POST to one stream |
| `X-Streamweld-Verbose: 1` | Include migration and warning journal entries |
| `X-Streamweld-Orphan-Policy` | Override `continue`, `cancel_after`, or `cancel` |
| `Last-Event-ID` | Exact exclusive resume cursor |
| `X-Streamweld-Stream-Id` | Durable identity returned after journal creation |
| `X-Streamweld-Durability` | `durable` or explicit `degraded` behavior |

## SSE mapping

| Journal kind | SSE event | Terminal |
|---|---|:---:|
| `open` | `streamweld.stream.open` | no |
| `chunk` | default message event | no |
| `migration` | `streamweld.stream.migration` | no |
| `warning` | `streamweld.stream.warning` | no |
| `done` | `streamweld.stream.done`, then `[DONE]` | yes |
| `error` | `streamweld.stream.error` | yes |
| `stopped` | `streamweld.stream.stopped` | yes |

The proxy journals a complete decoded upstream SSE event, never a partial frame
or split UTF-8 rune. Heartbeat comments are allowed but are not journaled and do
not advance the cursor.

The administrative `/internal/*` surface is not an end-user API. Bind or
network-restrict it separately from public inference endpoints.
