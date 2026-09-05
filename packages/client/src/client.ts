import {
  LocalAbortError,
  StreamExpiredError,
  StreamGenerationError,
  StreamHTTPError,
  StreamNotIdentifiedError,
  StreamPersistenceError,
  StreamProtocolError,
  StreamTransportError,
} from "./errors.js";
import { ReplayMulticast } from "./multicast.js";
import {
  compareSequences,
  decodePersistedCursor,
  validateSequence,
  validateStreamId,
} from "./persistence.js";
import { decodeProtocolFrame, extractTextDelta } from "./protocol.js";
import { IncrementalSSEParser } from "./sse.js";
import type {
  DoneOutcome,
  DurableStream,
  DurableStreamOptions,
  DurableStreamState,
  ErrorOutcome,
  StoppedOutcome,
  StreamDoneEvent,
  StreamErrorEvent,
  StreamEvent,
  StreamOutcome,
  StreamSequence,
  StreamStoppedEvent,
  TokenUsage,
} from "./types.js";

const defaultMaxAttempts = 5;
const defaultInitialBackoffMs = 250;
const defaultMaxBackoffMs = 5_000;
const defaultMaxRequestBytes = 1 << 20;
const defaultMaxEventBytes = 1 << 20;
const defaultMaxErrorBytes = 64 << 10;
const defaultMaxReplayEvents = 65_536;
const defaultMaxReplayBytes = 16 << 20;
const maxHeaderBytes = 32 << 10;
const maxURLBytes = 8 << 10;
const expirationCodes = new Set([
  "stream_expired",
  "stream_not_found",
  "stream_offset_expired",
  "stream_not_resumable",
]);

interface ResolvedOptions {
  readonly url: URL;
  readonly fetch: typeof globalThis.fetch;
  readonly credentials?: RequestCredentials;
  readonly headers: Headers;
  readonly serializedBody: string | null;
  readonly maxAttempts: number;
  readonly initialBackoffMs: number;
  readonly maxBackoffMs: number;
  readonly jitter: boolean;
  readonly maxEventBytes: number;
  readonly maxErrorBytes: number;
  readonly persist: DurableStreamOptions["persist"];
  readonly onMigration: DurableStreamOptions["onMigration"];
  readonly onWarning: DurableStreamOptions["onWarning"];
}

interface Deferred<T> {
  readonly promise: Promise<T>;
  readonly settled: boolean;
  resolve(value: T): void;
  reject(error: unknown): void;
}

interface ResponseReadResult {
  readonly terminal: boolean;
}

class RetryableReaderError extends Error {}

class DurableStreamImplementation implements DurableStream {
  readonly #options: ResolvedOptions;
  readonly #controller = new AbortController();
  readonly #hub: ReplayMulticast<StreamEvent>;
  readonly #idDeferred = deferred<string>();
  readonly #resultDeferred = deferred<StreamOutcome>();
  readonly #externalSignal: AbortSignal | undefined;
  readonly #externalAbort = (): void => this.#controller.abort(this.#externalSignal?.reason);
  #id: string | null = null;
  #lastSequence: StreamSequence = "0";
  #state: DurableStreamState = "connecting";
  #durability: "durable" | "degraded" | null = null;
  #terminal = false;
  #connectionEventCount = 0;

  readonly events: AsyncIterable<StreamEvent>;
  readonly text: AsyncIterable<string>;
  readonly idReady: Promise<string>;
  readonly result: Promise<StreamOutcome>;

  constructor(options: DurableStreamOptions) {
    const limits = resolveLimits(options);
    this.#options = resolveOptions(options, limits.maxRequestBytes);
    this.#hub = new ReplayMulticast<StreamEvent>({
      maxEvents: limits.maxReplayEvents,
      maxBytes: limits.maxReplayBytes,
      sizeOf: eventSize,
      sequenceOf: (event) => event.seq ?? this.#lastSequence,
    });
    // Reserve both public views before the eager pump starts. One unused view
    // can be evicted independently at its configured bound without competing
    // with or truncating the other view's cursor.
    this.events = singleUseIterable(this.#hub.subscribe(), "events");
    this.text = this.#textIterable(this.#hub.subscribe());
    this.idReady = this.#idDeferred.promise;
    this.result = this.#resultDeferred.promise;
    // Prevent optional promises from producing process-level unhandled
    // rejection noise; callers still observe the original rejected promises.
    void this.idReady.catch(() => undefined);
    void this.result.catch(() => undefined);

    const cursor = resolveInitialCursor(options);
    if (cursor !== null) {
      this.#id = cursor.id;
      this.#lastSequence = cursor.lastEventId;
      this.#idDeferred.resolve(cursor.id);
    }

    this.#externalSignal = options.signal;
    if (this.#externalSignal !== undefined) {
      if (this.#externalSignal.aborted) {
        this.#controller.abort(this.#externalSignal.reason);
      } else {
        this.#externalSignal.addEventListener("abort", this.#externalAbort, { once: true });
      }
    }
    void Promise.resolve().then(async () => this.#run());
  }

  get id(): string | null {
    return this.#id;
  }

  get state(): DurableStreamState {
    return this.#state;
  }

  async stop(): Promise<StoppedOutcome> {
    const id = await this.idReady;
    const url = streamURL(this.#options.url, id, "stop");
    const headers = resumeHeaders(this.#options.headers);
    headers.set("Accept", "application/json");
    headers.delete("Last-Event-ID");

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 30_000);
    try {
      const response = await this.#options.fetch(url, this.#requestInit("POST", headers, controller.signal));
      if (!response.ok) throw await decodeHTTPError(response, this.#options.maxErrorBytes, id);
      const raw = await readBoundedText(response, this.#options.maxErrorBytes);
      return decodeStopResult(raw, id);
    } catch (error) {
      if (controller.signal.aborted) {
        throw new StreamTransportError("stop request timed out", 1, { cause: error });
      }
      if (error instanceof StreamHTTPError || error instanceof StreamProtocolError) throw error;
      throw new StreamTransportError("stop request failed", 1, { cause: error });
    } finally {
      clearTimeout(timeout);
    }
  }

  async #run(): Promise<void> {
    try {
      await this.#pump();
    } catch (error) {
      if (this.#terminal) return;
      const failure = this.#controller.signal.aborted
        ? new LocalAbortError("the local stream attachment was aborted", { cause: error })
        : error;
      this.#fail(failure);
    }
  }

  async #pump(): Promise<void> {
    let failures = 0;
    while (!this.#terminal) {
      this.#throwIfAborted();
      const isResume = this.#id !== null;
      this.#state = isResume ? (failures === 0 ? "connecting" : "reconnecting") : "connecting";
      const cursorBefore = this.#lastSequence;
      let response: Response;
      try {
        response = await this.#connect(isResume);
      } catch (error) {
        this.#throwIfAborted(error);
        if (error instanceof StreamProtocolError) throw error;
        failures = await this.#retry(failures, error);
        continue;
      }

      try {
        const responseID = response.headers.get("X-Streamweld-Stream-Id");
        if (responseID !== null) this.#setID(responseID);
        if (response.ok) this.#validateStreamingResponse(response, isResume);
      } catch (error) {
        // Rejecting headers still owns the response body and must release its
        // connection, including failures while persisting the stream identity.
        try {
          await response.body?.cancel();
        } catch {
          // Preserve the validation failure if the connection already failed.
        }
        throw error;
      }
      if (!response.ok) {
        const failure = await decodeHTTPError(response, this.#options.maxErrorBytes, this.#id);
        if (isRetryableStatus(response.status) && !(failure instanceof StreamExpiredError)) {
          failures = await this.#retry(failures, failure);
          continue;
        }
        throw failure;
      }

      this.#state = "streaming";
      this.#connectionEventCount = 0;
      try {
        const read = await this.#readResponse(response);
        if (read.terminal) return;
        if (compareSequences(this.#lastSequence, cursorBefore) > 0) failures = 0;
        if (this.#durability === "degraded" && this.#id === null) {
          throw new StreamTransportError(
            "the degraded stream disconnected and has no resumable identity",
            1,
          );
        }
        failures = await this.#retry(
          failures,
          new RetryableReaderError("SSE response ended before a terminal event"),
        );
      } catch (error) {
        this.#throwIfAborted(error);
        if (
          error instanceof StreamProtocolError ||
          error instanceof StreamExpiredError ||
          error instanceof StreamPersistenceError
        ) throw error;
        if (this.#durability === "degraded" && this.#id === null) {
          throw new StreamTransportError(
            "the degraded stream disconnected and has no resumable identity",
            1,
            { cause: error },
          );
        }
        if (compareSequences(this.#lastSequence, cursorBefore) > 0) failures = 0;
        failures = await this.#retry(failures, error);
      }
    }
  }

  async #connect(isResume: boolean): Promise<Response> {
    if (isResume) {
      const id = this.#id;
      if (id === null) throw new StreamNotIdentifiedError("cannot resume an unidentified stream");
      const headers = resumeHeaders(this.#options.headers);
      headers.set("Accept", "text/event-stream");
      headers.set("X-Streamweld-Verbose", "1");
      headers.set("Last-Event-ID", this.#lastSequence);
      return this.#options.fetch(
        streamURL(this.#options.url, id, "events"),
        this.#requestInit("GET", headers, this.#controller.signal),
      );
    }
    const body = this.#options.serializedBody;
    if (body === null) throw new StreamProtocolError("body is required to start a new durable stream");
    const headers = new Headers(this.#options.headers);
    headers.set("Accept", "text/event-stream");
    headers.set("Content-Type", "application/json");
    headers.set("X-Streamweld-Verbose", "1");
    return this.#options.fetch(
      this.#options.url,
      this.#requestInit("POST", headers, this.#controller.signal, body),
    );
  }

  #requestInit(method: string, headers: Headers, signal: AbortSignal, body?: string): RequestInit {
    return {
      method,
      headers,
      signal,
      redirect: "error",
      cache: "no-store",
      ...(body === undefined ? {} : { body }),
      ...(this.#options.credentials === undefined ? {} : { credentials: this.#options.credentials }),
    };
  }

  #validateStreamingResponse(response: Response, isResume: boolean): void {
    const contentType = response.headers.get("Content-Type")?.toLowerCase() ?? "";
    if (!/^text\/event-stream(?:\s*;|$)/.test(contentType)) {
      throw new StreamProtocolError("successful stream response must use text/event-stream");
    }
    const durability = response.headers.get("X-Streamweld-Durability");
    if (durability !== "durable" && durability !== "degraded") {
      throw new StreamProtocolError("successful stream response has an invalid durability header");
    }
    if (isResume && durability !== "durable") {
      throw new StreamProtocolError("a resume response cannot be durability-degraded");
    }
    if (durability === "durable" && this.#id === null) {
      throw new StreamProtocolError("durable response is missing X-Streamweld-Stream-Id");
    }
    if (durability === "degraded" && this.#id !== null) {
      throw new StreamProtocolError("degraded response must not claim a resumable stream ID");
    }
    this.#durability = durability;
  }

  async #readResponse(response: Response): Promise<ResponseReadResult> {
    if (response.body === null) throw new RetryableReaderError("SSE response has no body");
    const reader = response.body.getReader();
    const parser = new IncrementalSSEParser(this.#options.maxEventBytes);
    let shouldCancel = true;
    try {
      while (true) {
        const read = await reader.read();
        if (read.done) {
          shouldCancel = false;
          break;
        }
        for (const frame of parser.push(read.value)) {
          if (this.#handleFrame(frame)) {
            await cancelReader(reader);
            shouldCancel = false;
            return { terminal: true };
          }
          await Promise.resolve();
        }
      }
      for (const frame of parser.finish()) {
        if (this.#handleFrame(frame)) {
          await cancelReader(reader);
          shouldCancel = false;
          return { terminal: true };
        }
        await Promise.resolve();
      }
      return { terminal: false };
    } finally {
      if (shouldCancel) {
        await cancelReader(reader);
      }
      reader.releaseLock();
    }
  }

  #handleFrame(frame: import("./sse.js").ParsedSSEEvent): boolean {
    let seq: StreamSequence | null = null;
    if (frame.id !== undefined) {
      seq = validateSequence(frame.id);
      if (compareSequences(seq, this.#lastSequence) <= 0) return false;
    }
    const decoded = decodeProtocolFrame(frame, seq);
    switch (decoded.kind) {
      case "reader-error":
        throw new RetryableReaderError(`server detached this reader: ${decoded.code}`);
      case "expired":
        throw new StreamExpiredError(decoded.message, 410, decoded.code, decoded.streamId ?? this.#id);
      case "done-sentinel": {
        if (
          this.#durability === "durable" &&
          (this.#lastSequence === "0" || this.#connectionEventCount > 0)
        ) {
          throw new StreamProtocolError(
            "a durable [DONE]-only response is valid only when resuming at the terminal sequence",
          );
        }
        const event: StreamDoneEvent = {
          type: "done",
          seq: this.#id === null ? null : this.#lastSequence,
          finishReason: null,
          replayedTerminal: true,
        };
        this.#complete(event);
        return true;
      }
      case "event":
        break;
    }

    const event = decoded.value;
    this.#connectionEventCount += 1;
    if (this.#durability === "durable" && this.#lastSequence === "0" && event.type !== "open") {
      throw new StreamProtocolError("the first durable journal event must be open");
    }
    if (event.type === "open" && (event.seq !== "1" || this.#lastSequence !== "0")) {
      throw new StreamProtocolError("open must be journal sequence 1 and may appear only once");
    }
    // A response that began durable may acquire an unjournaled degraded suffix.
    // Those frames remain useful to this attachment but never advance/persist
    // the last durable cursor.
    if (event.seq === null && this.#durability === "durable") {
      if (event.type === "warning" && event.code === "journal_degraded") {
        this.#durability = "degraded";
      } else {
        throw new StreamProtocolError(
          "durable stream emitted unsequenced data before a journal_degraded warning",
        );
      }
    } else if (event.seq !== null && this.#durability === "degraded") {
      throw new StreamProtocolError("a degraded stream cannot resume sequence allocation");
    }
    if (event.type === "open") this.#setID(event.streamId);
    if (event.seq !== null) {
      this.#persist(event.seq);
      this.#lastSequence = event.seq;
    }
    if (event.type === "migration") this.#safeCallback(this.#options.onMigration, event);
    if (event.type === "warning") {
      if (event.code === "journal_degraded") this.#durability = "degraded";
      this.#safeCallback(this.#options.onWarning, event);
    }

    if (event.type === "done" || event.type === "stopped" || event.type === "error") {
      this.#complete(event);
      return true;
    }
    this.#hub.publish(event);
    return false;
  }

  #complete(event: StreamDoneEvent | StreamStoppedEvent | StreamErrorEvent): void {
    if (this.#terminal) return;
    this.#terminal = true;
    this.#hub.publish(event);
    this.#hub.close();
    const outcome = terminalOutcome(event, this.#id);
    this.#state = outcome.type;
    this.#resultDeferred.resolve(outcome);
    if (this.#id === null) {
      this.#idDeferred.reject(new StreamNotIdentifiedError("the stream completed without a durable identity"));
    }
    this.#cleanup();
  }

  #setID(rawID: string): void {
    const id = validateStreamId(rawID);
    if (this.#id !== null && this.#id !== id) {
      throw new StreamProtocolError("stream ID changed across transport connections");
    }
    if (this.#id === null) {
      this.#id = id;
      this.#idDeferred.resolve(id);
      this.#persist(this.#lastSequence);
    }
  }

  #persist(seq: StreamSequence): void {
    const persistence = this.#options.persist;
    const id = this.#id;
    if (persistence === undefined || id === null) return;
    try {
      if (persistence.setExact !== undefined) {
        persistence.setExact(id, seq);
        return;
      }
      const numeric = Number(seq);
      if (!Number.isSafeInteger(numeric)) {
        throw new StreamPersistenceError(
          "persistence must implement setExact for sequences above Number.MAX_SAFE_INTEGER",
        );
      }
      persistence.set(id, numeric);
    } catch (error) {
      if (error instanceof StreamPersistenceError) throw error;
      throw new StreamPersistenceError("persisting the stream checkpoint failed", { cause: error });
    }
  }

  async #retry(failures: number, cause: unknown): Promise<number> {
    const next = failures + 1;
    if (next > this.#options.maxAttempts) {
      throw new StreamTransportError(
        `stream transport failed after ${String(next)} consecutive connection attempts`,
        next,
        { cause },
      );
    }
    this.#state = "reconnecting";
    const exponential = Math.min(
      this.#options.maxBackoffMs,
      this.#options.initialBackoffMs * 2 ** Math.max(0, next - 1),
    );
    const delay = this.#options.jitter ? Math.floor(Math.random() * (exponential + 1)) : exponential;
    await abortableDelay(delay, this.#controller.signal);
    return next;
  }

  #throwIfAborted(cause?: unknown): void {
    if (this.#controller.signal.aborted) {
      throw new LocalAbortError("the local stream attachment was aborted", { cause });
    }
  }

  #safeCallback<T>(callback: ((value: T) => void) | undefined, value: T): void {
    if (callback === undefined) return;
    try {
      callback(value);
    } catch {
      // Observational hooks never own or interrupt the transport pump.
    }
  }

  #textIterable(iterator: AsyncIterator<StreamEvent>): AsyncIterable<string> {
    const events = singleUseIterable(iterator, "text");
    return {
      async *[Symbol.asyncIterator](): AsyncIterator<string> {
        for await (const event of events) {
          if (event.type === "chunk") {
            const delta = extractTextDelta(event);
            if (delta !== null && delta.length > 0) yield delta;
          } else if (event.type === "error") {
            throw new StreamGenerationError(event);
          }
        }
      },
    };
  }

  #fail(error: unknown): void {
    if (this.#terminal) return;
    this.#terminal = true;
    const failure = error instanceof Error
      ? error
      : new StreamTransportError("stream failed with a non-Error cause", 1);
    this.#state = failure instanceof LocalAbortError ||
      failure instanceof StreamTransportError ||
      failure instanceof StreamExpiredError
      ? "disconnected"
      : "error";
    this.#hub.fail(failure);
    this.#resultDeferred.reject(failure);
    if (this.#id === null) this.#idDeferred.reject(failure);
    this.#cleanup();
  }

  #cleanup(): void {
    this.#externalSignal?.removeEventListener("abort", this.#externalAbort);
  }
}

export function createDurableStream(options: DurableStreamOptions): DurableStream {
  return new DurableStreamImplementation(options);
}

function resolveOptions(options: DurableStreamOptions, maxRequestBytes: number): ResolvedOptions {
  const url = normalizeURL(options.url);
  const fetchImplementation = options.fetch ?? globalThis.fetch;
  if (fetchImplementation === undefined) {
    throw new StreamProtocolError("this runtime does not provide fetch; pass options.fetch");
  }
  const headers = new Headers(options.headers);
  headers.delete("Last-Event-ID");
  headers.set("X-Streamweld-Verbose", "1");
  if (headerBytes(headers) > maxHeaderBytes) {
    throw new StreamProtocolError(`request headers exceed ${String(maxHeaderBytes)} bytes`);
  }

  let serializedBody: string | null = null;
  if (options.body !== undefined) {
    const record = typeof options.body === "object" && options.body !== null && !Array.isArray(options.body)
      ? options.body as Record<string, unknown>
      : null;
    if (record === null || record["stream"] !== true) {
      throw new StreamProtocolError("body must be a JSON object with stream: true");
    }
    try {
      const encoded = JSON.stringify(options.body);
      if (typeof encoded !== "string") {
        throw new StreamProtocolError("body JSON serialization produced no value");
      }
      serializedBody = encoded;
    } catch (error) {
      throw new StreamProtocolError("body is not JSON-serializable", { cause: error });
    }
    if (new TextEncoder().encode(serializedBody).byteLength > maxRequestBytes) {
      throw new StreamProtocolError(`serialized request exceeds ${String(maxRequestBytes)} bytes`);
    }
  }

  if (!headers.has("X-Streamweld-Idempotency-Key") && serializedBody !== null) {
    headers.set("X-Streamweld-Idempotency-Key", generateIdempotencyKey());
  }
  const key = headers.get("X-Streamweld-Idempotency-Key");
  if (key !== null && (key.length === 0 || key.length > 256)) {
    throw new StreamProtocolError("X-Streamweld-Idempotency-Key must contain 1 to 256 characters");
  }
  if (headerBytes(headers) > maxHeaderBytes) {
    throw new StreamProtocolError(`request headers exceed ${String(maxHeaderBytes)} bytes`);
  }

  const maxAttempts = boundedInteger(options.resume?.maxAttempts, defaultMaxAttempts, 0, 100, "maxAttempts");
  const initialBackoffMs = boundedInteger(
    options.resume?.backoff?.initialMs,
    defaultInitialBackoffMs,
    0,
    60_000,
    "initialMs",
  );
  const maxBackoffMs = boundedInteger(
    options.resume?.backoff?.maxMs,
    defaultMaxBackoffMs,
    initialBackoffMs,
    60_000,
    "maxMs",
  );

  return {
    url,
    fetch: fetchImplementation,
    ...(options.credentials === undefined ? {} : { credentials: options.credentials }),
    headers,
    serializedBody,
    maxAttempts,
    initialBackoffMs,
    maxBackoffMs,
    jitter: options.resume?.backoff?.jitter ?? true,
    maxEventBytes: boundedInteger(
      options.limits?.maxEventBytes,
      defaultMaxEventBytes,
      1,
      64 << 20,
      "maxEventBytes",
    ),
    maxErrorBytes: boundedInteger(
      options.limits?.maxErrorBytes,
      defaultMaxErrorBytes,
      1,
      1 << 20,
      "maxErrorBytes",
    ),
    persist: options.persist,
    onMigration: options.onMigration,
    onWarning: options.onWarning,
  };
}

function resolveLimits(options: DurableStreamOptions): {
  maxRequestBytes: number;
  maxReplayEvents: number;
  maxReplayBytes: number;
} {
  return {
    maxRequestBytes: boundedInteger(
      options.limits?.maxRequestBytes,
      defaultMaxRequestBytes,
      1,
      64 << 20,
      "maxRequestBytes",
    ),
    maxReplayEvents: boundedInteger(
      options.limits?.maxReplayEvents,
      defaultMaxReplayEvents,
      1,
      1_000_000,
      "maxReplayEvents",
    ),
    maxReplayBytes: boundedInteger(
      options.limits?.maxReplayBytes,
      defaultMaxReplayBytes,
      1,
      256 << 20,
      "maxReplayBytes",
    ),
  };
}

function resolveInitialCursor(options: DurableStreamOptions): {
  id: string;
  lastEventId: StreamSequence;
} | null {
  if (options.resumeFrom !== undefined) {
    return {
      id: validateStreamId(options.resumeFrom.id),
      lastEventId: validateSequence(options.resumeFrom.lastEventId ?? "0"),
    };
  }
  if (options.persist === undefined) return null;
  let raw: string | null;
  try {
    raw = options.persist.get();
  } catch (error) {
    throw new StreamPersistenceError("loading the persisted stream checkpoint failed", { cause: error });
  }
  return raw === null ? null : decodePersistedCursor(raw);
}

function normalizeURL(input: string | URL): URL {
  let url: URL;
  try {
    if (input instanceof URL) {
      url = new URL(input.href);
    } else {
      try {
        url = new URL(input);
      } catch {
        const base = globalThis.location?.href;
        if (base === undefined) throw new StreamProtocolError("relative URL requires a browser location");
        url = new URL(input, base);
      }
    }
  } catch (error) {
    if (error instanceof StreamProtocolError) throw error;
    throw new StreamProtocolError("url is invalid", { cause: error });
  }
  if (url.protocol !== "http:" && url.protocol !== "https:") {
    throw new StreamProtocolError("url must use http or https");
  }
  if (url.username !== "" || url.password !== "") {
    throw new StreamProtocolError("url must not contain credentials");
  }
  if (url.hash !== "") throw new StreamProtocolError("url must not contain a fragment");
  if (new TextEncoder().encode(url.href).byteLength > maxURLBytes) {
    throw new StreamProtocolError(`url exceeds ${String(maxURLBytes)} bytes`);
  }
  return url;
}

function streamURL(base: URL, id: string, operation: "events" | "stop"): URL {
  const match = /^(.*\/v1)\/(?:chat\/completions|completions)\/?$/.exec(base.pathname);
  if (match === null || match[1] === undefined) {
    throw new StreamProtocolError("url path must end in /v1/chat/completions or /v1/completions");
  }
  const url = new URL(base.href);
  url.pathname = `${match[1]}/streams/${encodeURIComponent(id)}/${operation}`;
  url.search = "";
  return url;
}

function resumeHeaders(source: Headers): Headers {
  const headers = new Headers(source);
  headers.delete("Content-Type");
  headers.delete("Content-Length");
  headers.delete("X-Streamweld-Idempotency-Key");
  headers.delete("X-Streamweld-Orphan-Policy");
  return headers;
}

function generateIdempotencyKey(): string {
  const crypto = globalThis.crypto;
  if (crypto === undefined) {
    throw new StreamProtocolError("secure randomness is required to generate an idempotency key");
  }
  if (typeof crypto.randomUUID === "function") return `sdk-${crypto.randomUUID()}`;
  const bytes = new Uint8Array(16);
  crypto.getRandomValues(bytes);
  return `sdk-${[...bytes].map((value) => value.toString(16).padStart(2, "0")).join("")}`;
}

function headerBytes(headers: Headers): number {
  const encoder = new TextEncoder();
  let total = 0;
  headers.forEach((value, name) => {
    total += encoder.encode(name).byteLength + encoder.encode(value).byteLength + 4;
  });
  return total;
}

function boundedInteger(
  value: number | undefined,
  fallback: number,
  minimum: number,
  maximum: number,
  name: string,
): number {
  const resolved = value ?? fallback;
  if (!Number.isSafeInteger(resolved) || resolved < minimum || resolved > maximum) {
    throw new StreamProtocolError(`${name} must be an integer between ${String(minimum)} and ${String(maximum)}`);
  }
  return resolved;
}

function eventSize(event: StreamEvent): number {
  return new TextEncoder().encode(JSON.stringify(event)).byteLength;
}

function terminalOutcome(
  event: StreamDoneEvent | StreamStoppedEvent | StreamErrorEvent,
  streamId: string | null,
): StreamOutcome {
  switch (event.type) {
    case "done":
      return {
        type: "done",
        streamId,
        seq: event.seq,
        finishReason: event.finishReason,
        ...(event.usage === undefined ? {} : { usage: event.usage }),
      } satisfies DoneOutcome;
    case "stopped":
      return {
        type: "stopped",
        streamId: streamId ?? "",
        seq: event.seq,
        usage: event.usage,
        ...(event.partialText === undefined ? {} : { partialText: event.partialText }),
      } satisfies StoppedOutcome;
    case "error":
      return {
        type: "error",
        streamId,
        seq: event.seq,
        code: event.code,
        message: event.message,
        reason: event.reason,
        retriable: false,
        usage: event.usage,
      } satisfies ErrorOutcome;
  }
}

async function decodeHTTPError(
  response: Response,
  maxBytes: number,
  fallbackStreamID: string | null,
): Promise<StreamHTTPError> {
  const raw = await readBoundedText(response, maxBytes);
  let code: string | null = null;
  let message = `Streamweld returned HTTP ${String(response.status)}`;
  let streamId = fallbackStreamID;
  try {
    const decoded = JSON.parse(raw) as unknown;
    const root = asRecord(decoded);
    const error = asRecord(root?.["error"]);
    if (typeof error?.["code"] === "string") code = error["code"];
    if (typeof error?.["message"] === "string" && error["message"].length > 0) message = error["message"];
    if (typeof error?.["stream_id"] === "string") streamId = error["stream_id"];
  } catch {
    // Status and bounded generic text are sufficient when the body is not JSON.
  }
  if (response.status === 410 && code !== null && expirationCodes.has(code)) {
    return new StreamExpiredError(
      message,
      response.status,
      code as "stream_expired" | "stream_not_found" | "stream_offset_expired" | "stream_not_resumable",
      streamId,
    );
  }
  return new StreamHTTPError(message, response.status, code, streamId);
}

async function readBoundedText(response: Response, maxBytes: number): Promise<string> {
  if (response.body === null) return "";
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  try {
    while (true) {
      const read = await reader.read();
      if (read.done) break;
      if (total + read.value.byteLength > maxBytes) {
        const remaining = maxBytes - total;
        if (remaining > 0) chunks.push(read.value.slice(0, remaining));
        await reader.cancel();
        break;
      }
      chunks.push(read.value);
      total += read.value.byteLength;
    }
  } finally {
    reader.releaseLock();
  }
  const joined = new Uint8Array(chunks.reduce((sum, chunk) => sum + chunk.byteLength, 0));
  let offset = 0;
  for (const chunk of chunks) {
    joined.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return new TextDecoder().decode(joined);
}

function decodeStopResult(raw: string, expectedID: string): StoppedOutcome {
  let decoded: unknown;
  try {
    decoded = JSON.parse(raw) as unknown;
  } catch (error) {
    throw new StreamProtocolError("stop response is not valid JSON", { cause: error });
  }
  const object = asRecord(decoded);
  if (object === null || object["stream_id"] !== expectedID || object["outcome"] !== "stopped") {
    throw new StreamProtocolError("stop response does not identify the stopped stream");
  }
  const partialText = object["partial_text"];
  if (typeof partialText !== "string") throw new StreamProtocolError("stop response is missing partial_text");
  const usage = decodeUsage(object["usage"]);
  return { type: "stopped", streamId: expectedID, partialText, usage };
}

function decodeUsage(value: unknown): TokenUsage {
  const object = asRecord(value);
  if (object === null) throw new StreamProtocolError("usage must be an object");
  const integer = (name: string): number => {
    const field = object[name];
    if (!Number.isSafeInteger(field) || (field as number) < 0) {
      throw new StreamProtocolError(`usage.${name} must be a non-negative safe integer`);
    }
    return field as number;
  };
  if (typeof object["estimated"] !== "boolean") {
    throw new StreamProtocolError("usage.estimated must be a boolean");
  }
  return {
    promptTokens: integer("prompt_tokens"),
    completionTokens: integer("completion_tokens"),
    totalTokens: integer("total_tokens"),
    estimated: object["estimated"],
  };
}

function asRecord(value: unknown): Record<string, unknown> | null {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? value as Record<string, unknown>
    : null;
}

function singleUseIterable<T>(iterator: AsyncIterator<T>, name: string): AsyncIterable<T> {
  let claimed = false;
  return {
    [Symbol.asyncIterator](): AsyncIterator<T> {
      if (claimed) {
        return {
          async next(): Promise<IteratorResult<T>> {
            throw new StreamProtocolError(`${name} is a single-consumer async iterable`);
          },
        };
      }
      claimed = true;
      return iterator;
    },
  };
}

function isRetryableStatus(status: number): boolean {
  return status === 408 || status === 425 || status === 429 || status >= 500;
}

async function abortableDelay(milliseconds: number, signal: AbortSignal): Promise<void> {
  if (signal.aborted) throw new LocalAbortError();
  if (milliseconds === 0) return;
  await new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(done, milliseconds);
    signal.addEventListener("abort", aborted, { once: true });
    function done(): void {
      signal.removeEventListener("abort", aborted);
      resolve();
    }
    function aborted(): void {
      clearTimeout(timeout);
      reject(new LocalAbortError());
    }
  });
}

async function cancelReader(reader: ReadableStreamDefaultReader<Uint8Array>): Promise<void> {
  try {
    await reader.cancel();
  } catch {
    // The transport may already be failed; cancellation remains best-effort.
  }
}

function deferred<T>(): Deferred<T> {
  let isSettled = false;
  let resolvePromise!: (value: T) => void;
  let rejectPromise!: (error: unknown) => void;
  const promise = new Promise<T>((resolve, reject) => {
    resolvePromise = resolve;
    rejectPromise = reject;
  });
  return {
    promise,
    get settled() {
      return isSettled;
    },
    resolve(value) {
      if (isSettled) return;
      isSettled = true;
      resolvePromise(value);
    },
    reject(error) {
      if (isSettled) return;
      isSettled = true;
      rejectPromise(error);
    },
  };
}
