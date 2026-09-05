# `@streamweld/client`

Dependency-free durable SSE streaming for Streamweld. The package uses only
web-platform APIs and ships ESM, CommonJS, and TypeScript declarations.

```sh
npm install @streamweld/client
```

```ts
import { createDurableStream } from "@streamweld/client";

const stream = createDurableStream({
  url: "https://streamweld.example.com/v1/chat/completions",
  body: {
    model: "llama-8b",
    messages: [{ role: "user", content: "Explain journaled SSE." }],
    stream: true,
  },
  resume: {
    maxAttempts: 5,
    backoff: { initialMs: 250, maxMs: 5_000, jitter: true },
  },
  onMigration: (event) => console.debug("producer moved", event),
});

for await (const delta of stream.text) {
  render(delta);
}

const outcome = await stream.result;
```

`stream.events` exposes typed `open`, `chunk`, `migration`, `warning`, `done`,
`stopped`, and `error` events. `stream.text` extracts choice-zero chat or legacy
completion text. Both are independent cursors over one network pump, so they
may be consumed concurrently or started at different times without competing
for events. Each view is intentionally single-consumer; a second iterator is
rejected. If an unused or slow view exceeds its bounded replay buffer, only that
view throws `StreamBufferLimitError`; the other view and generation continue.

Journal sequence values are exposed as canonical decimal strings, never as
JavaScript numbers. Transport failures reconnect to the same stream with the
last committed `Last-Event-ID`. An initial POST is protected by one generated
idempotency key, reused across pre-header retries. `410 Gone` and an in-band
retention-gap error throw `StreamExpiredError`; the client never starts a new
generation silently. The retry budget resets when the durable cursor advances;
repeated delivery of already processed events does not extend it.

## Stop is not abort

`await stream.stop()` sends `POST /v1/streams/{id}/stop` and explicitly ends the
remote generation. By contrast, the `signal` option only detaches this local
HTTP connection. It never calls the stop endpoint, and the identified stream
remains resumable from another client or a persisted checkpoint. Local detach
rejects with `LocalAbortError`, whose `name` is `AbortError`.

The stop request has a 30-second timeout covering both headers and the response
body. A timeout rejects with `StreamTransportError`; it does not prove whether
the server committed the stop, so the caller may retry `stop()`.

Protocol termination resolves `stream.result` to a discriminated `done`,
`stopped`, or `error` outcome. The text view ends for `done`/`stopped` and throws
`StreamGenerationError` for a terminal protocol error; the events view yields
the terminal entry before closing.

## Reload persistence

```ts
import {
  createDurableStream,
  createLocalStoragePersistence,
} from "@streamweld/client";

const stream = createDurableStream({
  url: "/v1/chat/completions",
  body: { model: "llama-8b", messages, stream: true },
  persist: createLocalStoragePersistence("active-generation"),
});
```

`StreamPersistence.get()` returns the opaque checkpoint previously produced by
`set(id, seq)`. Custom stores can use `encodePersistedCursor` and
`decodePersistedCursor`. Implement optional `setExact(id, seqString)` to support
the entire uint64 sequence range without rounding; the localStorage helper does.
Pass `resumeFrom: { id, lastEventId }` for an explicit checkpoint.

Authorization and other caller headers are preserved for resume and stop
requests. Streamweld-specific initial-only headers are stripped from GET/stop
requests, while `X-Streamweld-Verbose: 1` is always enabled so typed migration
and warning events are observable.

## Runtime verification

The published package supports Node 20+ and Web API runtimes. CI checks the
built ESM entry point with the following runtime suites before publishing:

| Runtime | Checks |
|---|---|
| Node 20, 22, 24 | Real HTTP cancellation after garbage collection and the complete stop response timeout |
| Deno 2.9.6, Bun 1.4.2 | Native HTTP servers: UTF-8 SSE, exact reconnect cursors, duplicate suppression, local detach, explicit stop, and expiration |
| Browser/edge targets | Browser-mode bundling rejects Node dependencies; the bundle imports in a context exposing only Web APIs |

The browser check is an import guard; it does not run streaming in a browser
or an edge provider. Those environments also need streaming `fetch`,
`ReadableStream`, `TextDecoder`, and `AbortController`. Cross-origin requests
need a server CORS policy that permits the request headers and exposes the
Streamweld response headers.

To reproduce these checks from a checkout, install the development toolchain
and the exact Deno/Bun versions above, then follow the
[runtime test commands](https://github.com/satwiksps/streamweld/blob/main/CONTRIBUTING.md#development-setup).
