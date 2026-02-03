# ADR 004: Bound and buffer completion requests for stream selection

- Status: Accepted
- Date: 2026-08-22

## Context

OpenAI-compatible completion endpoints use the JSON body field `stream` to select streaming behavior. That field may appear anywhere in the object. Streamweld must know whether to create a journal before it dispatches an upstream request, while non-streaming requests must keep their original bytes and bypass journaling.

No correct general-purpose JSON parser can decide that a missing or false `stream` field will not appear later without reaching the end of the object. Selecting durability from a nonstandard header would make ordinary OpenAI clients incompatible.

## Decision

Read each chat/completions request into a bounded buffer before upstream dispatch. Validate the top-level JSON object and inspect `stream`. For durable streaming requests, canonicalize the object once and retain that normalized form for continuation. For non-streaming requests, restore the exact original bytes and use the direct passthrough path with journaling disabled.

The body-size bound is a proxy configuration value with an `8 MiB` standalone
default. Exceeding it returns a structured `413` response before an upstream
request or journal is created.

## Consequences

Non-streaming response bytes remain untouched and no journal operations occur, but request dispatch begins only after the bounded body has been received. This is a small and explicit request-side cost rather than a claim of literal zero proxy latency. Streaming requests gain one immutable normalized representation for retries and producer migration. Clients need no Streamweld-specific opt-in header.
