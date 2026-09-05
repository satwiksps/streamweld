# `@streamweld/ai-sdk`

Vercel AI SDK v5 `ChatTransport` backed by `@streamweld/client` and Streamweld's
durable OpenAI-compatible stream protocol.

```sh
npm install @streamweld/ai-sdk ai@^5
```

When TypeScript checks dependency declarations (`skipLibCheck: false`), the
tested `ai@5.0.244` release also needs its referenced JSON Schema and Node types:

```sh
npm install --save-dev @types/json-schema @types/node
```

For the `useChat` example, also install the v2 integration in your React app:

```sh
npm install @ai-sdk/react@^2
```

```ts
import { useChat } from "@ai-sdk/react";
import { StreamweldChatTransport } from "@streamweld/ai-sdk";

const transport = new StreamweldChatTransport({
  // Relative URLs work in browsers; use an absolute URL in Node.
  api: "https://streamweld.example.com/v1/chat/completions",
  model: "llama-8b",
});

const chat = useChat({ transport, resume: true });
```

The transport maps text-only AI SDK `UIMessage` objects to OpenAI chat messages
and returns AI SDK UI message chunks. It rejects file, reasoning, data, and tool
parts instead of silently dropping or stringifying them.
Unsupported response payloads also fail explicitly and detach the local reader;
the saved generation remains available for reconnection or explicit stop.

## Stop is not disconnect

`useChat().stop()` aborts the local HTTP reader. Streamweld keeps generating and
the same stream remains resumable. To explicitly stop generation on the server,
call the transport with the AI SDK chat ID:

```ts
await transport.stop(chatId);
```

The explicit call resolves with `@streamweld/client`'s typed `stopped` outcome.
A stopped UI stream also emits a transient `data-streamweld` chunk before its
normal text-end/finish sequence, so `onData` can distinguish it from model
completion.

With a `StreamweldChatPersistence` implementation, `stop(chatId)` also works
immediately after a reload, before reconnecting. It uses the saved stream ID
and current configured headers and credentials to send only the stop request.
A successful saved stop removes that generation's checkpoint; a failed request
keeps it available for retry. A newer generation's checkpoint is preserved if
the chat changes while the stop request is pending.

For reload-safe resume, provide a `StreamweldChatPersistence` implementation.
It stores the mapping from AI SDK `chatId` to Streamweld stream identity and
cursor; the two IDs are not interchangeable. AI SDK v5 does not give a
reconnecting transport its previously materialized streaming-message state, so
the adapter deliberately requests a full replay from sequence zero and rebuilds
that message. Once attached, `@streamweld/client` resumes later network failures
from its exact cursor without duplicating chunks.

Terminal Streamweld errors become AI SDK `error` chunks after any open text part
is closed. This is the v5-native error path: `useChat` retains partial text,
enters its error state, and invokes `onError`. A preceding transient
`data-streamweld` chunk carries the structured Streamweld error fields.

The package has one runtime dependency, `@streamweld/client`. Vercel's `ai`
package is a v5 peer dependency and is used only for its transport/message types.

## AI SDK dependency overrides

AI SDK v5's provider utilities install Undici 5.x. For npm applications, add
this override to your application's `package.json` and run `npm install`:

```json
{
  "overrides": {
    "@ai-sdk/provider-utils": {
      "undici": "6.28.0"
    }
  }
}
```

This matches the repository's pnpm override. With `ai@5.0.253`, the installed
adapter passed its HTTP streaming checks with this override, and `npm audit`
reported no high or moderate advisories. A low-severity
[provider-utils resource-consumption advisory](https://github.com/advisories/GHSA-866g-f22w-33x8)
remains without a published v3 fix. It affects AI SDK JSON response handlers;
this adapter uses the durable client's Fetch implementation. Applications
using other AI SDK provider operations should assess that advisory separately.
