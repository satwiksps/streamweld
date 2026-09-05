import {
  createDurableStream,
  StreamExpiredError,
  type DurableStream,
  type DurableStreamOptions,
  type StoppedOutcome,
  type StreamEvent,
  type StreamPersistence,
} from "@streamweld/client";
import type { ChatTransport, UIMessage, UIMessageChunk } from "ai";

export type Resolvable<T> = T | (() => T | PromiseLike<T>);

export interface StreamweldChatCheckpoint {
  streamId: string;
  lastEventId?: string;
  messageId: string;
  textPartId: string;
}

/**
 * Persists the active Streamweld generation for an AI SDK chat. The chat ID and
 * Streamweld stream ID are deliberately separate identities.
 */
export interface StreamweldChatPersistence {
  get(chatId: string): StreamweldChatCheckpoint | null;
  set(chatId: string, checkpoint: StreamweldChatCheckpoint): void;
  remove(chatId: string): void;
}

export interface StreamweldChatTransportOptions<
  UI_MESSAGE extends UIMessage = UIMessage,
> {
  /** OpenAI-compatible Streamweld chat-completions endpoint. */
  api?: string | URL;
  /** Default model. A per-request `body.model` may override it. */
  model: string;
  headers?: Resolvable<HeadersInit>;
  credentials?: Resolvable<RequestCredentials>;
  body?: Resolvable<Record<string, unknown>>;
  fetch?: typeof globalThis.fetch;
  resume?: DurableStreamOptions["resume"];
  persistence?: StreamweldChatPersistence;
}

export class UnsupportedUIMessagePartError extends Error {
  readonly messageId: string;
  readonly partType: string;

  constructor(messageId: string, partType: string) {
    super(
      `UI message ${JSON.stringify(messageId)} contains unsupported part ${JSON.stringify(partType)}; ` +
        "StreamweldChatTransport currently accepts text-only messages",
    );
    this.name = "UnsupportedUIMessagePartError";
    this.messageId = messageId;
    this.partType = partType;
  }
}

export class UnsupportedStreamPayloadError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "UnsupportedStreamPayloadError";
  }
}

export class NoActiveStreamError extends Error {
  readonly chatId: string;

  constructor(chatId: string) {
    super(`No active Streamweld generation is known for chat ${JSON.stringify(chatId)}`);
    this.name = "NoActiveStreamError";
    this.chatId = chatId;
  }
}

type ActiveStream = {
  stream: DurableStream;
  abortController: AbortController;
  checkpoint: StreamweldChatCheckpoint;
  token: symbol;
};

type ParsedChunk = {
  deltas: string[];
  finishReason?: UIMessageFinishReason;
};

type UIMessageFinishReason = NonNullable<
  Extract<UIMessageChunk, { type: "finish" }>["finishReason"]
>;

const allowedFinishReasons = new Set<UIMessageFinishReason>([
  "stop",
  "length",
  "content-filter",
  "tool-calls",
  "error",
  "other",
  "unknown",
]);

/**
 * Vercel AI SDK v5 transport backed by a durable Streamweld generation.
 *
 * `useChat().stop()` aborts only the local reader. Call `transport.stop(chatId)`
 * when the user explicitly wants to stop generation at the Streamweld server.
 */
export class StreamweldChatTransport<
    UI_MESSAGE extends UIMessage = UIMessage,
  >
  implements ChatTransport<UI_MESSAGE>
{
  readonly #api: string | URL;
  readonly #model: string;
  readonly #headers: Resolvable<HeadersInit> | undefined;
  readonly #credentials: Resolvable<RequestCredentials> | undefined;
  readonly #body: Resolvable<Record<string, unknown>> | undefined;
  readonly #fetch: typeof globalThis.fetch | undefined;
  readonly #resume: DurableStreamOptions["resume"];
  readonly #persistence: StreamweldChatPersistence | undefined;
  readonly #active = new Map<string, ActiveStream>();
  readonly #memory = new Map<string, StreamweldChatCheckpoint>();
  readonly #tokens = new Map<string, symbol>();

  constructor(options: StreamweldChatTransportOptions<UI_MESSAGE>) {
    if (options.model.trim() === "") {
      throw new TypeError("StreamweldChatTransport model must not be empty");
    }
    this.#api = options.api ?? "/v1/chat/completions";
    this.#model = options.model;
    this.#headers = options.headers;
    this.#credentials = options.credentials;
    this.#body = options.body;
    this.#fetch = options.fetch;
    this.#resume = options.resume;
    this.#persistence = options.persistence;
  }

  async sendMessages(
    options: Parameters<ChatTransport<UI_MESSAGE>["sendMessages"]>[0],
  ): Promise<ReadableStream<UIMessageChunk>> {
    const configuredBody = await resolve(this.#body);
    const requestBody = asRecord(options.body, "request body");
    const messages = options.messages.map(toOpenAIMessage);
    const model = readModel(requestBody.model ?? configuredBody?.model ?? this.#model);
    const body: Record<string, unknown> = {
      ...configuredBody,
      ...requestBody,
      model,
      messages,
      stream: true,
    };

    const requestKey =
      options.messageId ?? options.messages.at(-1)?.id ?? options.trigger;
    const messageId = `streamweld:${options.chatId}:${requestKey}:assistant`;
    const textPartId = `${messageId}:text`;
    const headers = await this.#requestHeaders(options.headers);
    const credentials = await resolve(this.#credentials);
    const localAbort = linkedAbortController(options.abortSignal);

    this.#active.get(options.chatId)?.abortController.abort(
      new DOMException("Superseded by a new chat request", "AbortError"),
    );

    const checkpoint: StreamweldChatCheckpoint = {
      streamId: "",
      messageId,
      textPartId,
    };
    const token = Symbol(options.chatId);
    this.#tokens.set(options.chatId, token);
    let stream: DurableStream;
    try {
      stream = createDurableStream(
        compactDurableOptions({
          url: this.#api,
          body,
          headers,
          credentials,
          signal: localAbort.signal,
          fetch: this.#fetch,
          resume: this.#resume,
          persist: this.#streamPersistence(options.chatId, checkpoint, token),
        }),
      );
    } catch (error) {
      if (this.#tokens.get(options.chatId) === token) {
        this.#tokens.delete(options.chatId);
      }
      throw error;
    }
    const active = { stream, abortController: localAbort, checkpoint, token };
    this.#active.set(options.chatId, active);

    return this.#toUIMessageStream(options.chatId, active);
  }

  async reconnectToStream(
    options: Parameters<ChatTransport<UI_MESSAGE>["reconnectToStream"]>[0],
  ): Promise<ReadableStream<UIMessageChunk> | null> {
    const checkpoint = this.#checkpoint(options.chatId);
    if (checkpoint === null || checkpoint.streamId === "") {
      return null;
    }

    const headers = await this.#requestHeaders(options.headers);
    const credentials = await resolve(this.#credentials);
    const localAbort = linkedAbortController(options.abortSignal);
    this.#active.get(options.chatId)?.abortController.abort(
      new DOMException("Superseded by stream reconnection", "AbortError"),
    );

    const token = Symbol(options.chatId);
    this.#tokens.set(options.chatId, token);
    let stream: DurableStream;
    try {
      stream = createDurableStream(
        compactDurableOptions({
          url: this.#api,
          headers,
          credentials,
          signal: localAbort.signal,
          fetch: this.#fetch,
          resume: this.#resume,
          resumeFrom: {
            id: checkpoint.streamId,
            // AI SDK v5 reconstructs a fresh streaming message on reconnect and
            // does not pass the previously materialized UI message to the
            // transport. Replay the complete journal so its text part is rebuilt
            // instead of replacing the saved prefix with only the suffix. The
            // core client still advances exact cursors for transport retries
            // after this attachment starts.
            lastEventId: "0",
          },
          persist: this.#streamPersistence(options.chatId, checkpoint, token),
        }),
      );
    } catch (error) {
      if (this.#tokens.get(options.chatId) === token) {
        this.#tokens.delete(options.chatId);
      }
      throw error;
    }
    const active = { stream, abortController: localAbort, checkpoint, token };
    this.#active.set(options.chatId, active);
    return this.#toUIMessageStream(options.chatId, active);
  }

  /** Explicitly stops an active or persisted generation on the Streamweld server. */
  async stop(chatId: string): Promise<StoppedOutcome> {
    const active = this.#active.get(chatId);
    if (active !== undefined) return active.stream.stop();

    const checkpoint = this.#checkpoint(chatId);
    if (checkpoint === null || checkpoint.streamId === "") {
      throw new NoActiveStreamError(chatId);
    }
    const headers = await this.#requestHeaders();
    const credentials = await resolve(this.#credentials);
    const detached = new AbortController();
    detached.abort();
    // A detached client never opens an events reader. Its independent stop
    // request still provides the normal response validation and timeout.
    const stream = createDurableStream(compactDurableOptions({
      url: this.#api,
      resumeFrom: { id: checkpoint.streamId },
      headers,
      credentials,
      signal: detached.signal,
      fetch: this.#fetch,
    }));
    const outcome = await stream.stop();

    // A new local attachment owns its cleanup. Shared persistence may also
    // have changed in another tab while the request was in flight.
    if (!this.#active.has(chatId)) {
      if (this.#memory.get(chatId)?.streamId === checkpoint.streamId) {
        this.#memory.delete(chatId);
      }
      if (this.#persistence?.get(chatId)?.streamId === checkpoint.streamId) {
        this.#persistence.remove(chatId);
      }
    }
    return outcome;
  }

  #checkpoint(chatId: string): StreamweldChatCheckpoint | null {
    const checkpoint =
      this.#active.get(chatId)?.checkpoint ??
      this.#memory.get(chatId) ??
      this.#persistence?.get(chatId);
    return checkpoint === undefined || checkpoint === null
      ? null
      : { ...checkpoint };
  }

  #saveCheckpoint(chatId: string, checkpoint: StreamweldChatCheckpoint): void {
    this.#memory.set(chatId, checkpoint);
    this.#persistence?.set(chatId, checkpoint);
  }

  #removeCheckpoint(chatId: string, active: ActiveStream): void {
    if (this.#active.get(chatId) === active) {
      this.#active.delete(chatId);
      if (this.#tokens.get(chatId) === active.token) {
        this.#tokens.delete(chatId);
      }
      this.#memory.delete(chatId);
      this.#persistence?.remove(chatId);
    }
  }

  #streamPersistence(
    chatId: string,
    checkpoint: StreamweldChatCheckpoint,
    token: symbol,
  ): StreamPersistence {
    return {
      get: () => null,
      set: (id: string, seq: number) => {
        if (this.#tokens.get(chatId) !== token) return;
        checkpoint.streamId = id;
        checkpoint.lastEventId = String(seq);
        this.#saveCheckpoint(chatId, { ...checkpoint });
      },
      setExact: (id: string, seq: string) => {
        if (this.#tokens.get(chatId) !== token) return;
        checkpoint.streamId = id;
        checkpoint.lastEventId = String(seq);
        this.#saveCheckpoint(chatId, { ...checkpoint });
      },
    };
  }

  async #requestHeaders(
    requestHeaders?: Record<string, string> | Headers,
  ): Promise<Headers> {
    const configured = await resolve(this.#headers);
    const headers = mergeHeaders(configured, requestHeaders);
    headers.set("X-Streamweld-Verbose", "1");
    return headers;
  }

  #toUIMessageStream(chatId: string, active: ActiveStream): ReadableStream<UIMessageChunk> {
    const removeCheckpoint = () => this.#removeCheckpoint(chatId, active);
    const iterator = adaptEvents(
      active.stream.events,
      active.checkpoint,
      removeCheckpoint,
    )[Symbol.asyncIterator]();
    return new ReadableStream<UIMessageChunk>({
      async pull(controller) {
        try {
          const next = await iterator.next();
          if (next.done) {
            controller.close();
          } else {
            controller.enqueue(next.value);
          }
        } catch (error) {
          // An adapter failure ends this local reader too. Keep the generation
          // and checkpoint available for explicit stop or later reconnection.
          active.abortController.abort(error);
          if (error instanceof StreamExpiredError) {
            removeCheckpoint();
          }
          controller.error(error);
        }
      },
      async cancel(reason) {
        active.abortController.abort(reason);
        await iterator.return?.();
      },
    });
  }
}

async function* adaptEvents(
  events: AsyncIterable<StreamEvent>,
  checkpoint: StreamweldChatCheckpoint,
  onTerminal: () => void,
): AsyncGenerator<UIMessageChunk, void> {
  let textStarted = false;
  let finishReason: UIMessageFinishReason | undefined;

  yield { type: "start", messageId: checkpoint.messageId };

  for await (const event of events) {
    if (event.type === "chunk") {
      const parsed = parseOpenAIChunk(event.data);
      finishReason = parsed.finishReason ?? finishReason;
      for (const delta of parsed.deltas) {
        if (!textStarted) {
          textStarted = true;
          yield { type: "text-start", id: checkpoint.textPartId };
        }
        yield { type: "text-delta", id: checkpoint.textPartId, delta };
      }
      continue;
    }

    if (event.type === "done") {
      onTerminal();
      if (textStarted) {
        yield { type: "text-end", id: checkpoint.textPartId };
      }
      const terminalFinishReason =
        normalizeFinishReason(event.finishReason) ?? finishReason;
      yield terminalFinishReason === undefined
        ? { type: "finish" }
        : { type: "finish", finishReason: terminalFinishReason };
      return;
    }

    if (event.type === "stopped") {
      onTerminal();
      yield {
        type: "data-streamweld",
        data: { outcome: "stopped", usage: event.usage },
        transient: true,
      };
      if (textStarted) {
        yield { type: "text-end", id: checkpoint.textPartId };
      }
      yield { type: "finish", finishReason: "stop" };
      return;
    }

    if (event.type === "error") {
      onTerminal();
      yield {
        type: "data-streamweld",
        data: {
          outcome: "error",
          code: event.code,
          reason: event.reason,
          retriable: event.retriable,
          usage: event.usage,
        },
        transient: true,
      };
      if (textStarted) {
        yield { type: "text-end", id: checkpoint.textPartId };
      }
      yield { type: "error", errorText: event.message };
      return;
    }
  }

  throw new UnsupportedStreamPayloadError(
    "Streamweld event stream ended without a terminal done, stopped, or error event",
  );
}

function parseOpenAIChunk(value: unknown): ParsedChunk {
  const record = asRecord(value, "OpenAI stream chunk");
  if ("error" in record) {
    throw new UnsupportedStreamPayloadError("OpenAI stream chunk contained an error object");
  }
  const choices = record.choices;
  if (!Array.isArray(choices)) {
    throw new UnsupportedStreamPayloadError("OpenAI stream chunk is missing a choices array");
  }

  const deltas: string[] = [];
  let finishReason: UIMessageFinishReason | undefined;
  for (const [position, choiceValue] of choices.entries()) {
    const choice = asRecord(choiceValue, `OpenAI stream choice ${position}`);
    const choiceIndex = typeof choice.index === "number" ? choice.index : position;
    const delta = asRecord(choice.delta, `OpenAI stream choice ${choiceIndex} delta`);
    const content = delta.content;

    if (choiceIndex !== 0 && content !== undefined && content !== null && content !== "") {
      throw new UnsupportedStreamPayloadError(
        "StreamweldChatTransport supports only the first OpenAI completion choice",
      );
    }
    if (choiceIndex === 0 && content !== undefined && content !== null) {
      if (typeof content !== "string") {
        throw new UnsupportedStreamPayloadError(
          "OpenAI streamed delta.content must be a string",
        );
      }
      if (content !== "") {
        deltas.push(content);
      }
    }
    if (choiceIndex === 0) {
      if (delta.tool_calls !== undefined || delta.function_call !== undefined) {
        throw new UnsupportedStreamPayloadError(
          "Streaming tool calls are not supported by StreamweldChatTransport",
        );
      }
      finishReason = normalizeFinishReason(choice.finish_reason) ?? finishReason;
    }
  }
  return finishReason === undefined ? { deltas } : { deltas, finishReason };
}

function toOpenAIMessage(message: UIMessage): Record<string, unknown> {
  const text: string[] = [];
  for (const part of message.parts) {
    if (part.type !== "text") {
      throw new UnsupportedUIMessagePartError(message.id, part.type);
    }
    text.push(part.text);
  }
  return { role: message.role, content: text.join("") };
}

function normalizeFinishReason(value: unknown): UIMessageFinishReason | undefined {
  if (value === "content_filter") return "content-filter";
  if (value === "tool_calls" || value === "function_call") return "tool-calls";
  if (typeof value === "string" && allowedFinishReasons.has(value as UIMessageFinishReason)) {
    return value as UIMessageFinishReason;
  }
  if (value === null || value === undefined) return undefined;
  return "unknown";
}

function readModel(value: unknown): string {
  if (typeof value !== "string" || value.trim() === "") {
    throw new TypeError("Streamweld chat request model must be a non-empty string");
  }
  return value;
}

function asRecord(value: unknown, label: string): Record<string, unknown> {
  if (value === undefined) return {};
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError(`${label} must be an object`);
  }
  return value as Record<string, unknown>;
}

function mergeHeaders(...values: Array<HeadersInit | undefined>): Headers {
  const headers = new Headers();
  for (const value of values) {
    if (value === undefined) continue;
    new Headers(value).forEach((headerValue, name) => headers.set(name, headerValue));
  }
  return headers;
}

async function resolve<T>(value: Resolvable<T> | undefined): Promise<T | undefined> {
  return typeof value === "function"
    ? await (value as () => T | PromiseLike<T>)()
    : value;
}

function linkedAbortController(signal: AbortSignal | undefined): AbortController {
  const controller = new AbortController();
  if (signal === undefined) return controller;
  if (signal.aborted) {
    controller.abort(signal.reason);
  } else {
    signal.addEventListener("abort", () => controller.abort(signal.reason), {
      once: true,
    });
  }
  return controller;
}

function compactDurableOptions(
  options: Record<string, unknown>,
): DurableStreamOptions {
  return Object.fromEntries(
    Object.entries(options).filter(([, value]) => value !== undefined),
  ) as unknown as DurableStreamOptions;
}
