# `@streamweld/ai-sdk`

Vercel AI SDK v5 `ChatTransport` backed by `@streamweld/client` and Streamweld's
durable OpenAI-compatible stream protocol.

```sh
npm install @streamweld/ai-sdk ai@^5
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
