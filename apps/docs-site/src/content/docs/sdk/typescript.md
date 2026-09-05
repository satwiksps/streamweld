---
title: TypeScript clients
description: Durable async iteration and the Vercel AI SDK v5 transport adapter.
---

# TypeScript clients

## Core client

`@streamweld/client` has no runtime dependency. It supports browsers, Node.js,
Deno, Bun, and edge runtimes with standard Fetch APIs.

```sh
npm install @streamweld/client
```

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
The stop request times out after 30 seconds, including its response body. A
`StreamTransportError` does not establish whether the remote stop completed;
retrying `stop()` is safe.

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

```sh
npm install @streamweld/ai-sdk ai@^5
```

The [adapter installation notes](https://github.com/satwiksps/streamweld/blob/main/packages/ai-sdk/README.md)
cover strict TypeScript declaration dependencies and the tested npm dependency
override for AI SDK v5.

For React's `useChat`, also install the v2 integration:

```sh
npm install @ai-sdk/react@^2
```

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

With `StreamweldChatPersistence` configured, `stop(chatId)` also works after a
page reload. It stops the saved generation without replaying its response and
keeps the checkpoint if the request fails, so you can retry.
