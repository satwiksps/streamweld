import type { UIMessage, UIMessageChunk } from "ai";
import { describe, expect, it, vi } from "vitest";
import { StreamweldChatTransport } from "../src/index";

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
