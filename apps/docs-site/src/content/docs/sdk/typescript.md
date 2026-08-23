---
title: TypeScript clients
description: Durable async iteration and the Vercel AI SDK v5 transport adapter.
---

# TypeScript clients

## Core client

`@streamweld/client` has no runtime dependency. It supports browsers, Node.js,
Deno, Bun, and edge runtimes with standard Fetch APIs.

```ts
import { createDurableStream } from '@streamweld/client';

const stream = createDurableStream({
  url: 'https://inference.example.com/v1/chat/completions',
  body: {
    model: 'llama-8b',
    messages: [{ role: 'user', content: 'Count to five.' }],
    stream: true,
  },
  resume: { maxAttempts: 5 },
  onMigration(event) {
    console.debug('producer moved', event.fromBackend, event.toBackend);
  },
});

for await (const delta of stream.text) render(delta);
const outcome = await stream.result;
```

`stream.text` and `stream.events` are independent iterables over one multicast
connection. Transport failure reconnects from the exact last processed event ID.
Before the server reveals a stream ID, retries reuse one stable idempotency key so
a lost initial response cannot create a second generation.

Call `await stream.stop()` only when the user intends to cancel generation. An
`AbortSignal` detaches this reader and leaves the producer resumable.

## Persist across page reloads

```ts
import {
  createDurableStream,
  createLocalStoragePersistence,
} from '@streamweld/client';

const stream = createDurableStream({
  url,
  body,
  persist: createLocalStoragePersistence('current-generation'),
});
```

The helper preserves 64-bit cursor values as decimal strings. Applications with
their own store can pass `resumeFrom: { id, lastEventId }`.

## Vercel AI SDK v5

Change the `useChat` transport:

```ts
import { useChat } from '@ai-sdk/react';
import { StreamweldChatTransport } from '@streamweld/ai-sdk';

const chat = useChat({
  transport: new StreamweldChatTransport({
    api: 'https://inference.example.com/v1/chat/completions',
    model: 'llama-8b',
  }),
});
```

The adapter accepts text message parts and rejects unsupported tool, reasoning,
and file parts before opening a request. Its abort signal detaches locally; use
the adapter's explicit `stop(chatId)` operation for cancellation.
