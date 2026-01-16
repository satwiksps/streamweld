# ADR 003: Bound upstream SSE frames

- Status: Accepted
- Date: 2026-08-22

## Context

The protocol requires an incremental SSE parser that buffers a complete event before journaling or re-emitting it. The build specification does not define an individual event-size limit. Without one, a faulty or hostile backend can send an unterminated field and make the proxy allocate memory until the process is exhausted.

OpenAI-compatible text and tool-call delta frames are normally far smaller than a megabyte. A limit should nevertheless be configurable for backends that intentionally emit larger extension payloads.

## Decision

Limit upstream SSE frames to 1MiB of non-delimiter wire content by default. Count recognized fields, comments, and unknown fields so ignored input cannot bypass the bound; exclude CR and LF delimiters. Allow deployments to select another positive limit through `sse.max_event_bytes`.

Exceeding the limit is an explicit producer error, `sse_event_too_large`. The partial event is neither journaled nor exposed to readers. The data plane may evaluate the failure for migration under the same correctness gates as another malformed upstream event.

## Consequences

Memory use per active parser is bounded independently of backend behavior. Exceptionally large custom SSE events require an explicit configuration change. The default can reject a nonstandard backend frame even when the overall stream would fit within the journal's per-stream capacity; that failure is visible rather than silently truncated.
