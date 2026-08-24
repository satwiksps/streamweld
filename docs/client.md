# TypeScript clients

Streamweld ships two public packages:

- `@streamweld/client` is a zero-runtime-dependency client for browsers,
  Node.js 20+, Deno, Bun, and edge runtimes.
- `@streamweld/ai-sdk` adapts the durable client to Vercel AI SDK v5's
  `ChatTransport` interface.

```sh
npm install @streamweld/client
npm install @streamweld/ai-sdk ai@^5
```

## Durable client

```ts
import { createDurableStream } from "@streamweld/client";

const stream = createDurableStream({
  url: "https://streamweld.example.com/v1/chat/completions",
  body: {
    model: "llama-8b",
    messages: [{ role: "user", content: "Count to five." }],
    stream: true,
  },
  resume: {
    maxAttempts: 5,
    backoff: { initialMs: 250, maxMs: 5_000, jitter: true },
  },
  onMigration(event) {
    console.debug("producer moved", event.fromBackend, event.toBackend);
  },
});

for await (const delta of stream.text) {
  render(delta);
}

const outcome = await stream.result;
```

`stream.text` and `stream.events` are independent async iterables backed by one
multicast connection. They can be consumed concurrently without splitting or
stealing events. `stream.events` exposes typed `open`, `chunk`, `migration`,
`warning`, `done`, `stopped`, and `error` entries. Sequence values remain exact
decimal strings internally and on events, including values larger than
JavaScript's safe integer range.

The stream ID is `null` until the initial response establishes a durable
identity. Await `stream.idReady` when a caller needs the ID immediately. The
current client-side lifecycle is available from `stream.state`, and
`stream.result` resolves to a distinct `done`, `stopped`, or protocol `error`
outcome.

Transport failures do not end the iterables. Once an identity is known, the
client reconnects to `/v1/streams/{id}/events` using the last exact SSE ID and
continues the same iterables. Before an identity is known it retries the initial
POST with one stable idempotency key, so a lost response cannot create a second
generation. A server-side `410 Gone`, including expired, evicted-offset,
explicitly stopped, and never-seen outcomes, rejects with
`StreamExpiredError`; the client never starts a replacement generation.

### Stop is not abort

```ts
// Cancels generation and commits a typed `stopped` terminal entry.
const stopped = await stream.stop();
```

`stop()` is the only client operation that calls
`POST /v1/streams/{id}/stop`. In contrast, an `AbortSignal` passed as
`signal` detaches this local reader and cancels its pending reconnect/backoff.
It does not call the stop endpoint, so the generation remains available for a
later resume.

### Resume after a reload

Pass an explicit checkpoint when application state already contains one:

```ts
const stream = createDurableStream({
  url,
  resumeFrom: { id: savedStreamId, lastEventId: savedSequence },
});
```

The optional `persist` hook receives every durable checkpoint. Its required
compatibility method is `set(id, seq: number)`; implementations that need the
entire protocol `uint64` range should also implement
`setExact(id, seq: string)`. The packaged helper preserves the exact form:

```ts
import { createLocalStoragePersistence } from "@streamweld/client";

const stream = createDurableStream({
  url,
  body,
  persist: createLocalStoragePersistence("current-generation"),
});
```

`encodePersistedCursor` and `decodePersistedCursor` are available for custom
stores. A local detach rejects with `LocalAbortError`, reconnect exhaustion
rejects with `StreamTransportError` and its attempt count, and retention
failures reject with `StreamExpiredError` and the server's `expirationCode`.

## Vercel AI SDK v5

Replace the transport in an existing `useChat` call:

```ts
import { useChat } from "@ai-sdk/react";
import { StreamweldChatTransport } from "@streamweld/ai-sdk";

const chat = useChat({
  transport: new StreamweldChatTransport({
    api: "https://streamweld.example.com/v1/chat/completions",
    model: "llama-8b",
  }),
});
```

The adapter implements the AI SDK v5 `ChatTransport` contract and translates
Streamweld's OpenAI-compatible chunks into object-form `UIMessageChunk`
lifecycle events. Its reconnect path reuses the durable stream associated with
the chat ID. Because AI SDK v5 reconstructs a fresh assistant message after a
page reload, the adapter deliberately replays that journal from sequence zero;
the core client still resumes ordinary network interruptions from its exact
last cursor. AI SDK abort signals detach the local reader only; call the
adapter's explicit `stop(chatId)` method when the user intends to cancel the
generation.

The adapter currently accepts text message parts. It rejects tool, reasoning,
file, and other unsupported UI parts before starting a request instead of
silently dropping them.
