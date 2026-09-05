import type { UIMessage, UIMessageChunk } from "ai";
import { StreamHTTPError, StreamProtocolError, StreamTransportError } from "@streamweld/client";
import { describe, expect, it, vi } from "vitest";
import {
  StreamweldChatTransport,
  type StreamweldChatCheckpoint,
} from "../src/index";

const streamId = "01k4a000000000000000000000";
const encoder = new TextEncoder();

describe("@streamweld/client integration", () => {
  it("survives a transport failure and resumes the same useChat object stream", async () => {
    const requests: Array<{ url: string; init: RequestInit }> = [];
    let attempt = 0;
    const fetch = vi.fn(async (input: RequestInfo | URL, init: RequestInit = {}) => {
      requests.push({ url: String(input), init });
      attempt += 1;
      if (attempt === 1) {
        return sseResponse(
          failingBody(
            sse(
              [
                "1",
                "streamweld.stream.open",
                {
                  stream_id: streamId,
                  model: "test-model",
                  model_version: "v1",
                  backend_id: "backend-a",
                },
              ],
              [
                "2",
                undefined,
                { choices: [{ index: 0, delta: { content: "Hel" } }] },
              ],
            ),
          ),
          { "X-Streamweld-Stream-Id": streamId },
        );
      }
      if (attempt === 2) {
        return sseResponse(
          sse(
            [
              "3",
              undefined,
              { choices: [{ index: 0, delta: { content: "lo" } }] },
            ],
            [
              "4",
              "streamweld.stream.done",
              {
                finish_reason: "stop",
                usage: {
                  prompt_tokens: 1,
                  completion_tokens: 2,
                  total_tokens: 3,
                  estimated: false,
                },
              },
            ],
          ),
        );
      }
      throw new Error("unexpected request");
    }) as typeof globalThis.fetch;

    const transport = new StreamweldChatTransport({
      api: "https://proxy.example/v1/chat/completions",
      model: "test-model",
      fetch,
      resume: {
        maxAttempts: 2,
        backoff: { initialMs: 1, maxMs: 1, jitter: false },
      },
    });
    const stream = await transport.sendMessages({
      trigger: "submit-message",
      chatId: "chat-integration",
      messageId: undefined,
      messages: [
        {
          id: "user-integration",
          role: "user",
          parts: [{ type: "text", text: "Say hello" }],
        } satisfies UIMessage,
      ],
      abortSignal: undefined,
    });

    const chunks = await readAll(stream);

    expect(chunks).toEqual([
      {
        type: "start",
        messageId: "streamweld:chat-integration:user-integration:assistant",
      },
      {
        type: "text-start",
        id: "streamweld:chat-integration:user-integration:assistant:text",
      },
      {
        type: "text-delta",
        id: "streamweld:chat-integration:user-integration:assistant:text",
        delta: "Hel",
      },
      {
        type: "text-delta",
        id: "streamweld:chat-integration:user-integration:assistant:text",
        delta: "lo",
      },
      {
        type: "text-end",
        id: "streamweld:chat-integration:user-integration:assistant:text",
      },
      { type: "finish", finishReason: "stop" },
    ]);
    expect(requests).toHaveLength(2);
    expect(requests[0]?.url).toBe("https://proxy.example/v1/chat/completions");
    expect(requests[0]?.init.method).toBe("POST");
    const initialHeaders = new Headers(requests[0]?.init.headers);
    const idempotencyKey = initialHeaders.get("x-streamweld-idempotency-key");
    expect(idempotencyKey).toMatch(/^sdk-/);
    expect(JSON.parse(String(requests[0]?.init.body))).toMatchObject({
      model: "test-model",
      stream: true,
      messages: [{ role: "user", content: "Say hello" }],
    });

    expect(requests[1]?.url).toBe(
      `https://proxy.example/v1/streams/${streamId}/events`,
    );
    expect(requests[1]?.init.method).toBe("GET");
    const resumeHeaders = new Headers(requests[1]?.init.headers);
    expect(resumeHeaders.get("last-event-id")).toBe("2");
    expect(resumeHeaders.has("x-streamweld-idempotency-key")).toBe(false);
  });
});

describe("stopping a persisted chat generation", () => {
  it("stops after reload with only a POST and current configured authentication", async () => {
    const { checkpoints, persistence } = savedChat();
    let authorization = "Bearer before-reload";
    const fetch = vi.fn(async (_input: RequestInfo | URL, _init?: RequestInit) => stoppedResponse());
    const transport = new StreamweldChatTransport({
      api: "https://proxy.example/v1/chat/completions",
      model: "test-model",
      persistence,
      headers: async () => ({ Authorization: authorization }),
      credentials: async () => "include" as const,
      fetch,
    });
    authorization = "Bearer current";

    await expect(transport.stop("saved-chat")).resolves.toMatchObject({
      type: "stopped", streamId, partialText: "partial",
    });

    expect(fetch).toHaveBeenCalledOnce();
    const [input, init] = fetch.mock.calls[0]!;
    expect(String(input)).toBe(`https://proxy.example/v1/streams/${streamId}/stop`);
    expect(init).toMatchObject({ method: "POST", credentials: "include", redirect: "error" });
    expect(init?.body).toBeUndefined();
    expect(init?.signal?.aborted).toBe(false);
    const headers = new Headers(init?.headers);
    expect(headers.get("authorization")).toBe("Bearer current");
    expect(headers.get("accept")).toBe("application/json");
    expect(headers.has("last-event-id")).toBe(false);
    expect(checkpoints.has("saved-chat")).toBe(false);
    expect(persistence.remove).toHaveBeenCalledExactlyOnceWith("saved-chat");
  });

  it.each([
    { name: "HTTP failure", response: () => new Response("unavailable", { status: 503 }), error: StreamHTTPError },
    { name: "invalid stop response", response: () => new Response("{}", { status: 202 }), error: StreamProtocolError },
    { name: "network failure", response: () => { throw new TypeError("offline"); }, error: StreamTransportError },
  ])("preserves the checkpoint after $name so stop can be retried", async ({ response, error }) => {
    const { checkpoints, persistence } = savedChat();
    const original = checkpoints.get("saved-chat");
    const fetch = vi.fn(async () => response());
    const transport = new StreamweldChatTransport({
      api: "https://proxy.example/v1/chat/completions", model: "test-model", persistence, fetch,
    });

    await expect(transport.stop("saved-chat")).rejects.toBeInstanceOf(error);
    expect(checkpoints.get("saved-chat")).toEqual(original);
    expect(persistence.remove).not.toHaveBeenCalled();
    fetch.mockImplementation(async () => stoppedResponse());
    await expect(transport.stop("saved-chat")).resolves.toMatchObject({ type: "stopped", streamId });
    expect(fetch).toHaveBeenCalledTimes(2);
    expect(checkpoints.has("saved-chat")).toBe(false);
  });

  it("preserves a newer checkpoint written while the saved stop is pending", async () => {
    const { checkpoints, persistence } = savedChat();
    let respond!: (response: Response) => void;
    let started!: () => void;
    const requested = new Promise<void>((resolve) => { started = resolve; });
    const transport = new StreamweldChatTransport({
      api: "https://proxy.example/v1/chat/completions", model: "test-model", persistence,
      fetch: async () => {
        started();
        return new Promise<Response>((resolve) => { respond = resolve; });
      },
    });
    const stopping = transport.stop("saved-chat");
    await Promise.race([requested, stopping]);
    const replacement = { ...checkpoints.get("saved-chat")!, streamId: "01k4b000000000000000000000" };
    checkpoints.set("saved-chat", replacement);
    respond(stoppedResponse());

    await expect(stopping).resolves.toMatchObject({ type: "stopped", streamId });
    expect(checkpoints.get("saved-chat")).toEqual(replacement);
    expect(persistence.remove).not.toHaveBeenCalled();
  });

  it("keeps a new chat request usable when an older saved stop finishes", async () => {
    const { checkpoints, persistence } = savedChat();
    const replacementId = "01k4b000000000000000000000";
    let respond!: (response: Response) => void;
    let started!: () => void;
    const requested = new Promise<void>((resolve) => { started = resolve; });
    const cancel = vi.fn();
    const requests: string[] = [];
    const transport = new StreamweldChatTransport({
      api: "https://proxy.example/v1/chat/completions", model: "test-model", persistence,
      fetch: async (input) => {
        const url = String(input);
        requests.push(url);
        if (url.endsWith(`/${streamId}/stop`)) {
          started();
          return new Promise<Response>((resolve) => { respond = resolve; });
        }
        if (url.endsWith(`/${replacementId}/stop`)) return stoppedResponse(replacementId);
        return sseResponse(new ReadableStream<Uint8Array>({
          start(controller) {
            controller.enqueue(encoder.encode(sse(
              ["1", "streamweld.stream.open", {
                stream_id: replacementId, model: "test-model", model_version: null, backend_id: "test",
              }],
              ["2", undefined, { choices: [{ index: 0, delta: { content: "new" } }] }],
            )));
          },
          cancel,
        }), { "X-Streamweld-Stream-Id": replacementId });
      },
    });
    const stopping = transport.stop("saved-chat");
    await Promise.race([requested, stopping]);
    const replacement = await transport.sendMessages({
      trigger: "submit-message", chatId: "saved-chat", messageId: undefined, abortSignal: undefined,
      messages: [{ id: "new-user", role: "user", parts: [{ type: "text", text: "new request" }] }],
    });
    const reader = replacement.getReader();
    try {
      expect((await reader.read()).value?.type).toBe("start");
      expect((await reader.read()).value?.type).toBe("text-start");
      expect((await reader.read()).value?.type).toBe("text-delta");
      expect(checkpoints.get("saved-chat")?.streamId).toBe(replacementId);
      respond(stoppedResponse());
      await expect(stopping).resolves.toMatchObject({ type: "stopped", streamId });
      expect(persistence.remove).not.toHaveBeenCalled();
      expect(checkpoints.get("saved-chat")?.streamId).toBe(replacementId);
      await expect(transport.stop("saved-chat")).resolves.toMatchObject({ streamId: replacementId });
      expect(requests).toEqual([
        `https://proxy.example/v1/streams/${streamId}/stop`,
        "https://proxy.example/v1/chat/completions",
        `https://proxy.example/v1/streams/${replacementId}/stop`,
      ]);
    } finally {
      await reader.cancel();
      reader.releaseLock();
    }
    expect(cancel).toHaveBeenCalledOnce();
  });
});

function savedChat() {
  const checkpoints = new Map<string, StreamweldChatCheckpoint>([["saved-chat", {
    streamId, lastEventId: "9007199254740993", messageId: "saved-assistant", textPartId: "saved-text",
  }]]);
  return {
    checkpoints,
    persistence: {
      get: (chatId: string) => checkpoints.get(chatId) ?? null,
      set: (chatId: string, checkpoint: StreamweldChatCheckpoint) => { checkpoints.set(chatId, checkpoint); },
      remove: vi.fn((chatId: string) => { checkpoints.delete(chatId); }),
    },
  };
}

function stoppedResponse(id = streamId): Response {
  return new Response(JSON.stringify({
    stream_id: id, outcome: "stopped", partial_text: "partial",
    usage: { prompt_tokens: 1, completion_tokens: 2, total_tokens: 3, estimated: false },
  }), { status: 202, headers: { "Content-Type": "application/json" } });
}

async function readAll(stream: ReadableStream<UIMessageChunk>): Promise<UIMessageChunk[]> {
  const chunks: UIMessageChunk[] = [];
  const reader = stream.getReader();
  try {
    while (true) {
      const next = await reader.read();
      if (next.done) return chunks;
      chunks.push(next.value);
    }
  } finally {
    reader.releaseLock();
  }
}

type Frame = readonly [
  id: string,
  event: string | undefined,
  data: Record<string, unknown>,
];

function sse(...frames: Frame[]): string {
  return frames
    .map(([id, event, data]) =>
      `${[
        `id: ${id}`,
        ...(event === undefined ? [] : [`event: ${event}`]),
        `data: ${JSON.stringify(data)}`,
      ].join("\n")}\n\n`,
    )
    .join("");
}

function failingBody(prefix: string): ReadableStream<Uint8Array> {
  let read = false;
  return new ReadableStream<Uint8Array>({
    pull(controller) {
      if (!read) {
        read = true;
        controller.enqueue(encoder.encode(prefix));
        return;
      }
      controller.error(new TypeError("network connection was interrupted"));
    },
  });
}

function sseResponse(
  body: BodyInit,
  extraHeaders: Record<string, string> = {},
): Response {
  return new Response(body, {
    status: 200,
    headers: {
      "Content-Type": "text/event-stream; charset=utf-8",
      "X-Streamweld-Durability": "durable",
      ...extraHeaders,
    },
  });
}
