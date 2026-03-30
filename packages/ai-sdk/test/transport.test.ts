import {
  AbstractChat,
  type ChatInit,
  type ChatState,
  type ChatStatus,
  type UIMessage,
  type UIMessageChunk,
} from "ai";
import { beforeEach, describe, expect, it, vi } from "vitest";

const createDurableStream = vi.hoisted(() => vi.fn());

vi.mock("@streamweld/client", async (importOriginal) => ({
  ...(await importOriginal<typeof import("@streamweld/client")>()),
  createDurableStream,
}));

import {
  LocalAbortError,
  StreamExpiredError,
  StreamTransportError,
} from "@streamweld/client";

import {
  NoActiveStreamError,
  StreamweldChatTransport,
  UnsupportedStreamPayloadError,
  UnsupportedUIMessagePartError,
  type StreamweldChatCheckpoint,
  type StreamweldChatPersistence,
} from "../src/index";

type FakeEvent = Record<string, unknown> & { type: string };

const usage = {
  promptTokens: 1,
  completionTokens: 2,
  totalTokens: 3,
  estimated: false,
};

function events(...values: FakeEvent[]): AsyncIterable<FakeEvent> {
  return {
    async *[Symbol.asyncIterator]() {
      yield* values;
    },
  };
}

function deferredEvents(signal: AbortSignal): AsyncIterable<FakeEvent> {
  return {
    async *[Symbol.asyncIterator]() {
      yield {
        type: "chunk",
        data: { choices: [{ index: 0, delta: { content: "partial" } }] },
      };
      await new Promise<void>((_resolve, reject) => {
        signal.addEventListener(
          "abort",
          () => reject(new DOMException("Local reader aborted", "AbortError")),
          { once: true },
        );
      });
    },
  };
}

function failingEvents(error: Error): AsyncIterable<FakeEvent> {
  return {
    async *[Symbol.asyncIterator]() {
      throw error;
    },
  };
}

function fakeStream(
  streamEvents: AsyncIterable<FakeEvent>,
  stop = vi.fn(async () => ({ type: "stopped", usage })),
) {
  return {
    events: streamEvents,
    text: events(),
    id: "01k4a000000000000000000000",
    idReady: Promise.resolve("01k4a000000000000000000000"),
    state: { status: "open", lastEventId: "1" },
    result: new Promise(() => {}),
    stop,
  };
}

function message(
  id: string,
  role: UIMessage["role"],
  text: string,
): UIMessage {
  return { id, role, parts: [{ type: "text", text }] };
}

function sendOptions(overrides: Partial<Parameters<StreamweldChatTransport["sendMessages"]>[0]> = {}) {
  return {
    trigger: "submit-message" as const,
    chatId: "chat-1",
    messageId: undefined,
    messages: [message("system-1", "system", "Be terse."), message("user-1", "user", "Hi")],
    abortSignal: undefined,
    ...overrides,
  };
}

async function readAll(stream: ReadableStream<UIMessageChunk>): Promise<UIMessageChunk[]> {
  const output: UIMessageChunk[] = [];
  const reader = stream.getReader();
  try {
    while (true) {
      const next = await reader.read();
      if (next.done) return output;
      output.push(next.value);
    }
  } finally {
    reader.releaseLock();
  }
}

describe("StreamweldChatTransport", () => {
  beforeEach(() => {
    createDurableStream.mockReset();
  });

  it("maps AI SDK requests and returns the exact text UI chunk lifecycle", async () => {
    createDurableStream.mockImplementation((options: Record<string, any>) => {
      options.persist.set("01k4a000000000000000000000", 4);
      return fakeStream(
        events(
          { type: "open" },
          {
            type: "chunk",
            data: { choices: [{ index: 0, delta: { role: "assistant" } }] },
          },
          {
            type: "chunk",
            data: { choices: [{ index: 0, delta: { content: "Hel" } }] },
          },
          {
            type: "chunk",
            data: {
              choices: [
                { index: 0, delta: { content: "lo" }, finish_reason: "stop" },
              ],
            },
          },
          { type: "done", finishReason: "stop", usage },
        ),
      );
    });

    const transport = new StreamweldChatTransport({
      api: "https://proxy.example/v1/chat/completions",
      model: "base-model",
      headers: async () => ({ Authorization: "Bearer token", "X-Shared": "base" }),
      credentials: async (): Promise<RequestCredentials> => "include",
      body: async () => ({ temperature: 0.2, top_p: 0.9 }),
      resume: { maxAttempts: 7 },
    });
    const stream = await transport.sendMessages(
      sendOptions({
        headers: { "X-Shared": "request", "X-Request": "yes" },
        body: { model: "request-model", temperature: 0.7, stream: false },
      }),
    );

    const call = createDurableStream.mock.calls[0]?.[0] as Record<string, any>;
    expect(call.url).toBe("https://proxy.example/v1/chat/completions");
    expect(call.credentials).toBe("include");
    expect(call.resume).toEqual({ maxAttempts: 7 });
    expect(call.body).toEqual({
      model: "request-model",
      temperature: 0.7,
      top_p: 0.9,
      stream: true,
      messages: [
        { role: "system", content: "Be terse." },
        { role: "user", content: "Hi" },
      ],
    });
    const headers = call.headers as Headers;
    expect(headers.get("authorization")).toBe("Bearer token");
    expect(headers.get("x-shared")).toBe("request");
    expect(headers.get("x-request")).toBe("yes");
    expect(headers.get("x-streamweld-verbose")).toBe("1");
    expect(headers.has("x-streamweld-idempotency-key")).toBe(false);

    const chunks = await readAll(stream);
    const textId = "streamweld:chat-1:user-1:assistant:text";
    expect(chunks).toEqual([
      { type: "start", messageId: "streamweld:chat-1:user-1:assistant" },
      { type: "text-start", id: textId },
      { type: "text-delta", id: textId, delta: "Hel" },
      { type: "text-delta", id: textId, delta: "lo" },
      { type: "text-end", id: textId },
      { type: "finish", finishReason: "stop" },
    ]);
  });

  it("uses an explicit idempotency header without overwriting it", async () => {
    createDurableStream.mockReturnValue(
      fakeStream(events({ type: "done", finishReason: "stop", usage })),
    );
    const transport = new StreamweldChatTransport({ model: "m" });
    await transport.sendMessages(
      sendOptions({ headers: { "X-Streamweld-Idempotency-Key": "caller-key" } }),
    );
    const headers = createDurableStream.mock.calls[0]?.[0].headers as Headers;
    expect(headers.get("x-streamweld-idempotency-key")).toBe("caller-key");
  });

  it("does not synthesize a reusable idempotency key across separate generations", async () => {
    createDurableStream.mockReturnValue(
      fakeStream(events({ type: "done", finishReason: "stop", usage })),
    );
    const transport = new StreamweldChatTransport({ model: "m" });
    await transport.sendMessages(sendOptions());
    await transport.sendMessages(
      sendOptions({
        trigger: "regenerate-message",
        messageId: "assistant-old",
      }),
    );

    expect(createDurableStream).toHaveBeenCalledTimes(2);
    for (const [call] of createDurableStream.mock.calls) {
      expect((call.headers as Headers).has("x-streamweld-idempotency-key")).toBe(false);
    }
  });

  it("rejects file, reasoning, data, and tool parts instead of dropping them", async () => {
    const unsupported = [
      { type: "file", mediaType: "image/png", url: "data:image/png;base64,AA==" },
      { type: "reasoning", text: "secret" },
      { type: "data-result", data: { ok: true } },
      { type: "dynamic-tool", toolName: "weather", toolCallId: "t1", state: "input-available", input: {} },
    ];
    const transport = new StreamweldChatTransport({ model: "m" });

    for (const part of unsupported) {
      await expect(
        transport.sendMessages(
          sendOptions({
            messages: [{ id: "bad", role: "user", parts: [part] } as UIMessage],
          }),
        ),
      ).rejects.toMatchObject({
        name: "UnsupportedUIMessagePartError",
        messageId: "bad",
        partType: part.type,
      } satisfies Partial<UnsupportedUIMessagePartError>);
    }
    expect(createDurableStream).not.toHaveBeenCalled();
  });

  it("reconnects from the chat checkpoint and forwards the v5 reconnect abort signal", async () => {
    const checkpoint: StreamweldChatCheckpoint = {
      streamId: "01k4a000000000000000000000",
      lastEventId: "41",
      messageId: "assistant-existing",
      textPartId: "text-existing",
    };
    const persistence: StreamweldChatPersistence = {
      get: vi.fn(() => checkpoint),
      set: vi.fn(),
      remove: vi.fn(),
    };
    createDurableStream.mockReturnValue(
      fakeStream(
        events(
          {
            type: "chunk",
            data: { choices: [{ index: 0, delta: { content: "again" } }] },
          },
          { type: "done", finishReason: "length", usage },
        ),
      ),
    );
    const abortController = new AbortController();
    const transport = new StreamweldChatTransport({
      model: "m",
      headers: { Authorization: "Bearer resume" },
      credentials: "same-origin",
      persistence,
    });

    const stream = await transport.reconnectToStream({
      chatId: "chat-resume",
      abortSignal: abortController.signal,
      headers: { "X-Reconnect": "1" },
    });
    expect(stream).not.toBeNull();
    const call = createDurableStream.mock.calls[0]?.[0] as Record<string, any>;
    expect(call.resumeFrom).toEqual({
      id: checkpoint.streamId,
      lastEventId: "0",
    });
    expect(call.body).toBeUndefined();
    expect(call.credentials).toBe("same-origin");
    expect(call.headers.get("authorization")).toBe("Bearer resume");
    expect(call.headers.get("x-reconnect")).toBe("1");
    expect(call.signal.aborted).toBe(false);
    abortController.abort("tab closed");
    expect(call.signal.aborted).toBe(true);
  });

  it("persists uint64 cursors exactly through setExact", async () => {
    const persistence: StreamweldChatPersistence = {
      get: vi.fn(() => null),
      set: vi.fn(),
      remove: vi.fn(),
    };
    createDurableStream.mockImplementation((options: Record<string, any>) => {
      options.persist.setExact(
        "01k4a000000000000000000000",
        "18446744073709551615",
      );
      return fakeStream(deferredEvents(options.signal));
    });
    const transport = new StreamweldChatTransport({ model: "m", persistence });
    const stream = await transport.sendMessages(sendOptions());

    expect(persistence.set).toHaveBeenCalledWith("chat-1", {
      streamId: "01k4a000000000000000000000",
      lastEventId: "18446744073709551615",
      messageId: "streamweld:chat-1:user-1:assistant",
      textPartId: "streamweld:chat-1:user-1:assistant:text",
    });
    await stream.cancel();
  });

  it("returns null when the AI chat has no active Streamweld generation", async () => {
    const transport = new StreamweldChatTransport({ model: "m" });
    await expect(transport.reconnectToStream({ chatId: "missing" })).resolves.toBeNull();
    await expect(transport.stop("missing")).rejects.toBeInstanceOf(NoActiveStreamError);
    expect(createDurableStream).not.toHaveBeenCalled();
  });

  it("resumes the same generation after a local reader drop", async () => {
    createDurableStream
      .mockImplementationOnce((options: Record<string, any>) => {
        options.persist.setExact("01k4a000000000000000000000", "9007199254740993");
        return fakeStream(deferredEvents(options.signal));
      })
      .mockImplementationOnce(() =>
        fakeStream(events({ type: "done", finishReason: "stop", usage })),
      );
    const transport = new StreamweldChatTransport({ model: "m" });
    const first = await transport.sendMessages(sendOptions());
    const reader = first.getReader();
    await reader.read();
    await reader.cancel("offline");

    const resumed = await transport.reconnectToStream({ chatId: "chat-1" });

    expect(resumed).not.toBeNull();
    expect(createDurableStream.mock.calls[1]?.[0].resumeFrom).toEqual({
      id: "01k4a000000000000000000000",
      lastEventId: "0",
    });
  });

  it("fully replays on page reload so AI SDK replaces a partial prefix exactly once", async () => {
    const checkpoint: StreamweldChatCheckpoint = {
      streamId: "01k4a000000000000000000000",
      lastEventId: "2",
      messageId: "assistant-existing",
      textPartId: "text-existing",
    };
    const persistence: StreamweldChatPersistence = {
      get: vi.fn(() => checkpoint),
      set: vi.fn(),
      remove: vi.fn(),
    };
    createDurableStream.mockReturnValue(
      fakeStream(
        events(
          {
            type: "chunk",
            data: { choices: [{ index: 0, delta: { content: "Hel" } }] },
          },
          {
            type: "chunk",
            data: { choices: [{ index: 0, delta: { content: "lo" } }] },
          },
          { type: "done", finishReason: "stop", usage },
        ),
      ),
    );
    const transport = new StreamweldChatTransport({ model: "m", persistence });
    const chat = new TestChat({
      id: "reload-chat",
      messages: [
        message("user-existing", "user", "Say hello"),
        {
          id: "assistant-existing",
          role: "assistant",
          parts: [{ type: "text", text: "Hel", state: "streaming" }],
        },
      ],
      transport,
    });

    await chat.resumeStream();

    expect(createDurableStream.mock.calls[0]?.[0].resumeFrom).toEqual({
      id: checkpoint.streamId,
      lastEventId: "0",
    });
    expect(chat.messages).toHaveLength(2);
    expect(chat.messages[1]).toEqual({
      id: "assistant-existing",
      role: "assistant",
      metadata: undefined,
      parts: [
        {
          type: "text",
          text: "Hello",
          state: "done",
          providerMetadata: undefined,
        },
      ],
    });
    expect(persistence.remove).toHaveBeenCalledWith("reload-chat");
  });

  it("keeps local cancellation distinct from explicit server stop", async () => {
    const stop = vi.fn(async () => ({ type: "stopped", usage }));
    let clientSignal: AbortSignal | undefined;
    createDurableStream.mockImplementation((options: Record<string, any>) => {
      clientSignal = options.signal;
      options.persist.set("01k4a000000000000000000000", 2);
      return fakeStream(deferredEvents(options.signal), stop);
    });
    const transport = new StreamweldChatTransport({ model: "m" });
    const stream = await transport.sendMessages(sendOptions());
    const reader = stream.getReader();
    await reader.read();
    await reader.cancel("drop connection");

    expect(clientSignal?.aborted).toBe(true);
    expect(stop).not.toHaveBeenCalled();
    await expect(transport.stop("chat-1")).resolves.toMatchObject({ type: "stopped" });
    expect(stop).toHaveBeenCalledOnce();
  });

  it("maps a remote stopped outcome without losing the partial text", async () => {
    createDurableStream.mockReturnValue(
      fakeStream(
        events(
          {
            type: "chunk",
            data: { choices: [{ index: 0, delta: { content: "partial" } }] },
          },
          { type: "stopped", usage },
        ),
      ),
    );
    const transport = new StreamweldChatTransport({ model: "m" });
    const chunks = await readAll(await transport.sendMessages(sendOptions()));
    expect(chunks.slice(-3)).toEqual([
      {
        type: "data-streamweld",
        data: { outcome: "stopped", usage },
        transient: true,
      },
      { type: "text-end", id: "streamweld:chat-1:user-1:assistant:text" },
      { type: "finish", finishReason: "stop" },
    ]);
  });

  it("maps structured Streamweld errors to the AI SDK error protocol", async () => {
    createDurableStream.mockReturnValue(
      fakeStream(
        events({
          type: "error",
          code: "migration_refused",
          message: "continuation is unsafe",
          reason: "tool_call_boundary",
          retriable: false,
          usage,
        }),
      ),
    );
    const transport = new StreamweldChatTransport({ model: "m" });
    const chunks = await readAll(await transport.sendMessages(sendOptions()));
    expect(chunks).toEqual([
      { type: "start", messageId: "streamweld:chat-1:user-1:assistant" },
      {
        type: "data-streamweld",
        data: {
          outcome: "error",
          code: "migration_refused",
          reason: "tool_call_boundary",
          retriable: false,
          usage,
        },
        transient: true,
      },
      { type: "error", errorText: "continuation is unsafe" },
    ]);
  });

  it("rejects unsupported OpenAI response shapes instead of silently truncating", async () => {
    createDurableStream.mockReturnValue(
      fakeStream(
        events({
          type: "chunk",
          data: {
            choices: [
              { index: 0, delta: { tool_calls: [{ id: "call-1" }] } },
            ],
          },
        }),
      ),
    );
    const transport = new StreamweldChatTransport({ model: "m" });
    const reader = (await transport.sendMessages(sendOptions())).getReader();
    await reader.read();
    await expect(reader.read()).rejects.toBeInstanceOf(UnsupportedStreamPayloadError);
  });

  it("clears the current checkpoint after a typed stream expiration", async () => {
    const checkpoint: StreamweldChatCheckpoint = {
      streamId: "01k4a000000000000000000000",
      lastEventId: "8",
      messageId: "assistant-existing",
      textPartId: "text-existing",
    };
    let saved: StreamweldChatCheckpoint | null = checkpoint;
    const persistence: StreamweldChatPersistence = {
      get: vi.fn(() => saved),
      set: vi.fn((_chatId, value) => {
        saved = value;
      }),
      remove: vi.fn(() => {
        saved = null;
      }),
    };
    const expiration = new StreamExpiredError(
      "stream expired",
      410,
      "stream_expired",
      checkpoint.streamId,
    );
    createDurableStream.mockReturnValue(fakeStream(failingEvents(expiration)));
    const transport = new StreamweldChatTransport({ model: "m", persistence });
    const reader = (await transport.reconnectToStream({ chatId: "chat-resume" }))!.getReader();
    await reader.read();
    await expect(reader.read()).rejects.toBe(expiration);
    expect(persistence.remove).toHaveBeenCalledWith("chat-resume");
    await expect(
      transport.reconnectToStream({ chatId: "chat-resume" }),
    ).resolves.toBeNull();
  });

  it.each([
    ["transport failure", new StreamTransportError("network unavailable", 4)],
    ["local abort", new LocalAbortError("tab disconnected")],
  ])("retains the checkpoint after a transient %s", async (_label, failure) => {
    const checkpoint: StreamweldChatCheckpoint = {
      streamId: "01k4a000000000000000000000",
      lastEventId: "8",
      messageId: "assistant-existing",
      textPartId: "text-existing",
    };
    const persistence: StreamweldChatPersistence = {
      get: vi.fn(() => checkpoint),
      set: vi.fn(),
      remove: vi.fn(),
    };
    createDurableStream.mockReturnValue(fakeStream(failingEvents(failure)));
    const transport = new StreamweldChatTransport({ model: "m", persistence });
    const reader = (await transport.reconnectToStream({ chatId: "chat-resume" }))!.getReader();
    await reader.read();
    await expect(reader.read()).rejects.toBe(failure);
    expect(persistence.remove).not.toHaveBeenCalled();
    expect(persistence.get("chat-resume")).toEqual(checkpoint);
  });

  it("is consumed by AI SDK v5 AbstractChat exactly like a useChat transport", async () => {
    createDurableStream.mockReturnValue(
      fakeStream(
        events(
          {
            type: "chunk",
            data: { choices: [{ index: 0, delta: { content: "Hello" } }] },
          },
          {
            type: "chunk",
            data: { choices: [{ index: 0, delta: { content: "!" } }] },
          },
          { type: "done", finishReason: "stop", usage },
        ),
      ),
    );
    const finish = vi.fn();
    const chat = new TestChat({
      id: "actual-use-chat",
      transport: new StreamweldChatTransport({ model: "m" }),
      onFinish: finish,
    });

    await chat.sendMessage({ text: "Hi" });

    expect(chat.status).toBe("ready");
    expect(chat.error).toBeUndefined();
    expect(chat.messages).toHaveLength(2);
    expect(chat.messages[1]).toMatchObject({
      id: "streamweld:actual-use-chat:user-generated:assistant",
      role: "assistant",
      parts: [{ type: "text", text: "Hello!", state: "done" }],
    });
    expect(finish).toHaveBeenCalledWith(
      expect.objectContaining({
        isAbort: false,
        isError: false,
        finishReason: "stop",
      }),
    );
  });

  it("lets AI SDK v5 retain partial text and enter error state on a terminal error", async () => {
    const persistence: StreamweldChatPersistence = {
      get: vi.fn(() => null),
      set: vi.fn(),
      remove: vi.fn(),
    };
    createDurableStream.mockImplementation((options: Record<string, any>) => {
      options.persist.set("01k4a000000000000000000000", 8);
      return fakeStream(
        events(
          {
            type: "chunk",
            data: { choices: [{ index: 0, delta: { content: "kept" } }] },
          },
          {
            type: "error",
            code: "migration_refused",
            message: "cannot continue safely",
            reason: "tool_call_boundary",
            retriable: false,
            usage,
          },
        ),
      );
    });
    const onError = vi.fn();
    const chat = new TestChat({
      id: "error-chat",
      transport: new StreamweldChatTransport({ model: "m", persistence }),
      onError,
    });

    await chat.sendMessage({ text: "Go" });

    expect(chat.status).toBe("error");
    expect(chat.error?.message).toBe("cannot continue safely");
    expect(chat.messages[1]?.parts).toEqual([
      { type: "text", text: "kept", state: "done", providerMetadata: undefined },
    ]);
    expect(onError).toHaveBeenCalledWith(
      expect.objectContaining({ message: "cannot continue safely" }),
    );
    expect(persistence.remove).toHaveBeenCalledWith("error-chat");
  });
});

class MemoryChatState implements ChatState<UIMessage> {
  status: ChatStatus = "ready";
  error: Error | undefined;
  messages: UIMessage[];

  constructor(messages: UIMessage[] = []) {
    this.messages = structuredClone(messages);
  }
  pushMessage = (value: UIMessage) => {
    this.messages.push(structuredClone(value));
  };
  popMessage = () => {
    this.messages.pop();
  };
  replaceMessage = (index: number, value: UIMessage) => {
    this.messages[index] = structuredClone(value);
  };
  snapshot = <T>(value: T): T => structuredClone(value);
}

class TestChat extends AbstractChat<UIMessage> {
  constructor({ messages, ...options }: ChatInit<UIMessage>) {
    super({
      ...options,
      state: new MemoryChatState(messages),
      generateId: () => "user-generated",
    });
  }
}
