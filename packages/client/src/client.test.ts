import { describe, expect, it, vi } from "vitest";

import {
  LocalAbortError,
  StreamBufferLimitError,
  StreamExpiredError,
  StreamGenerationError,
  StreamNotIdentifiedError,
  StreamPersistenceError,
  StreamProtocolError,
  StreamTransportError,
} from "./errors.js";
import { createDurableStream } from "./client.js";
import { encodePersistedCursor } from "./persistence.js";
import type { StreamEvent } from "./types.js";

const id = "01arz3ndektsv4rrffq69g5fav";
const baseURL = "https://example.test/v1/chat/completions";
const usage = {
  prompt_tokens: 2,
  completion_tokens: 3,
  total_tokens: 5,
  estimated: false,
};
const open = {
  stream_id: id,
  model: "llama-test",
  model_version: null,
  backend_id: "backend-a",
};

describe("createDurableStream", () => {
  it("reports a missing request body without transport retries", async () => {
    vi.useFakeTimers();
    try {
      const fetcher = vi.fn();
      const stream = createDurableStream({ url: baseURL, fetch: fetcher });
      const result = stream.result.catch((error: unknown) => error);
      await vi.runAllTimersAsync();
      expect(await result).toMatchObject({
        name: "StreamProtocolError",
        message: "body is required to start a new durable stream",
      });
      expect(fetcher).not.toHaveBeenCalled();
    } finally {
      vi.useRealTimers();
    }
  });

  it("exhausts retries when reconnects only replay already committed events", async () => {
    let calls = 0;
    const stream = createDurableStream({
      url: baseURL,
      resumeFrom: { id, lastEventId: "2" },
      resume: { maxAttempts: 2, backoff: { initialMs: 0, maxMs: 0 } },
      fetch: async () => {
        calls += 1;
        // Bound the broken implementation too, so this regression never hangs.
        if (calls > 4) return new Response("", { status: 400 });
        return sseResponse([frame("message", "2", chatChunk("duplicate"))], id);
      },
    });
    await expect(stream.result).rejects.toMatchObject({
      name: "StreamTransportError",
      attempts: 3,
    });
    expect(calls).toBe(3);
    await expect(collect(stream.text)).rejects.toBeInstanceOf(StreamTransportError);
  });

  it("multicasts one pump to typed events and text with callbacks and a done outcome", async () => {
    const wire = joinFrames(
      frame("streamweld.stream.open", "1", open),
      frame("message", "2", chatChunk("hello ")),
      frame("streamweld.stream.migration", "3", {
        from_backend: "backend-a",
        to_backend: "backend-b",
        reason: "tcp_reset",
        rescued_tokens: 1,
        token_count_estimated: false,
        attempt: 2,
      }),
      frame("streamweld.stream.warning", "4", {
        code: "seam_anomaly",
        message: "checked seam",
        predicate: null,
        details: {},
      }),
      frame("message", "5", chatChunk("world")),
      frame("streamweld.stream.done", "6", { finish_reason: "stop", usage }),
      "data: [DONE]\n\n",
    );
    const calls: FetchCall[] = [];
    const migrations: string[] = [];
    const warnings: string[] = [];
    const exactCursors: string[] = [];
    const stream = createDurableStream({
      url: baseURL,
      body: requestBody(),
      fetch: recordingFetch(calls, async () => sseResponse([wire], id)),
      persist: {
        get: () => null,
        set: () => undefined,
        setExact: (_streamID, seq) => exactCursors.push(seq),
      },
      onMigration: (event) => migrations.push(event.toBackend),
      onWarning: (event) => warnings.push(event.code),
    });

    const eventsPromise = collect(stream.events);
    const textPromise = collect(stream.text);
    const [events, text, outcome] = await Promise.all([eventsPromise, textPromise, stream.result]);

    expect(events.map((event) => event.type)).toEqual([
      "open", "chunk", "migration", "warning", "chunk", "done",
    ]);
    expect(text).toEqual(["hello ", "world"]);
    expect(outcome).toMatchObject({ type: "done", streamId: id, seq: "6", finishReason: "stop" });
    expect(stream.id).toBe(id);
    expect(stream.state).toBe("done");
    expect(migrations).toEqual(["backend-b"]);
    expect(warnings).toEqual(["seam_anomaly"]);
    expect(exactCursors.at(-1)).toBe("6");
    expect(calls).toHaveLength(1);
    expect(header(calls[0], "X-Streamweld-Verbose")).toBe("1");
    expect(header(calls[0], "X-Streamweld-Idempotency-Key")).toMatch(/^sdk-/);
  });

  it("reserves independent view cursors before staggered consumption", async () => {
    const stream = createDurableStream({
      url: baseURL,
      body: requestBody(),
      fetch: async () => sseResponse([joinFrames(
        frame("streamweld.stream.open", "1", open),
        frame("message", "2", chatChunk("first")),
        frame("message", "3", chatChunk("second")),
        frame("streamweld.stream.done", "4", { finish_reason: "stop", usage }),
      )], id),
    });
    const eventIterator = stream.events[Symbol.asyncIterator]();
    expect((await eventIterator.next()).value?.type).toBe("open");
    const textPromise = collect(stream.text);
    const remaining: StreamEvent[] = [];
    while (true) {
      const next = await eventIterator.next();
      if (next.done) break;
      remaining.push(next.value);
    }
    expect(await textPromise).toEqual(["first", "second"]);
    expect(remaining.at(-1)?.type).toBe("done");
  });

  it("retries the initial POST with one stable key, then resumes the same loop exactly once", async () => {
    const calls: FetchCall[] = [];
    let attempt = 0;
    const fetcher = recordingFetch(calls, async () => {
      attempt += 1;
      if (attempt === 1) throw new TypeError("connection reset before headers");
      if (attempt === 2) {
        return sseResponse([joinFrames(
          frame("streamweld.stream.open", "1", open),
          frame("message", "2", chatChunk("A")),
          // A truncated id=3 frame must be discarded and replayed.
          "id: 3\ndata: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"lost\"}}]}\n",
        )], id);
      }
      return sseResponse([joinFrames(
        frame("message", "3", chatChunk("B")),
        frame("streamweld.stream.done", "4", { finish_reason: "stop", usage }),
      )], id);
    });
    const stream = createDurableStream({
      url: baseURL,
      body: requestBody(),
      fetch: fetcher,
      resume: { maxAttempts: 3, backoff: { initialMs: 0, maxMs: 0, jitter: false } },
    });

    const text = await collect(stream.text);
    expect(text.join("")).toBe("AB");
    expect((await stream.result).type).toBe("done");
    expect(calls.map((call) => call.init.method)).toEqual(["POST", "POST", "GET"]);
    expect(header(calls[0], "X-Streamweld-Idempotency-Key")).toBe(
      header(calls[1], "X-Streamweld-Idempotency-Key"),
    );
    expect(header(calls[2], "Last-Event-ID")).toBe("2");
    expect(header(calls[2], "X-Streamweld-Idempotency-Key")).toBeNull();
    expect(String(calls[2]?.input)).toBe(`https://example.test/v1/streams/${id}/events`);
  });

  it.each([
    "stream_expired",
    "stream_not_found",
    "stream_offset_expired",
    "stream_not_resumable",
  ] as const)("maps HTTP 410 %s to StreamExpiredError without retry", async (code) => {
    let calls = 0;
    const stream = createDurableStream({
      url: baseURL,
      resumeFrom: { id, lastEventId: "41" },
      fetch: async () => {
        calls += 1;
        return new Response(JSON.stringify({
          error: { type: "streamweld_error", code, message: "gone", stream_id: id },
        }), { status: 410, headers: { "Content-Type": "application/json" } });
      },
      resume: { maxAttempts: 5, backoff: { initialMs: 0, maxMs: 0 } },
    });
    await expect(stream.result).rejects.toMatchObject({
      name: "StreamExpiredError",
      expirationCode: code,
      status: 410,
    });
    expect(calls).toBe(1);
  });

  it("maps an unsequenced in-band offset failure to StreamExpiredError", async () => {
    const stream = createDurableStream({
      url: baseURL,
      resumeFrom: { id, lastEventId: "9" },
      fetch: async () => sseResponse([frame("streamweld.stream.error", null, {
        code: "stream_offset_expired",
        message: "unjournaled gap",
        stream_id: id,
      })], id),
    });
    await expect(stream.result).rejects.toBeInstanceOf(StreamExpiredError);
  });

  it("keeps exact uint64 cursors in headers and persistence", async () => {
    const exact: string[] = [];
    const calls: FetchCall[] = [];
    const stream = createDurableStream({
      url: baseURL,
      resumeFrom: { id, lastEventId: "9007199254740993" },
      persist: {
        get: () => null,
        set: () => undefined,
        setExact: (_id, seq) => exact.push(seq),
      },
      fetch: recordingFetch(calls, async () => sseResponse([
        frame("streamweld.stream.done", "18446744073709551615", { finish_reason: null, usage }),
      ], id)),
    });
    await expect(stream.result).resolves.toMatchObject({ seq: "18446744073709551615" });
    expect(header(calls[0], "Last-Event-ID")).toBe("9007199254740993");
    expect(exact).toEqual(["18446744073709551615"]);
  });

  it("loads an opaque persisted page-reload checkpoint and resumes with its offset", async () => {
    const calls: FetchCall[] = [];
    const stream = createDurableStream({
      url: baseURL,
      body: requestBody(),
      persist: {
        get: () => encodePersistedCursor(id, "41"),
        set: () => undefined,
        setExact: () => undefined,
      },
      fetch: recordingFetch(calls, async () => sseResponse([
        frame("streamweld.stream.done", "42", { finish_reason: "stop", usage }),
      ], id)),
    });
    await stream.result;
    expect(calls[0]?.init.method).toBe("GET");
    expect(header(calls[0], "Last-Event-ID")).toBe("41");
  });

  it("completes a terminal-cursor resume from a lone [DONE] sentinel without reconnecting", async () => {
    const terminalSequence = "18446744073709551615";
    const calls: FetchCall[] = [];
    const stream = createDurableStream({
      url: baseURL,
      resumeFrom: { id, lastEventId: terminalSequence },
      resume: { maxAttempts: 3, backoff: { initialMs: 0, maxMs: 0, jitter: false } },
      fetch: recordingFetch(calls, async () => sseResponse(["data: [DONE]\n\n"], id)),
    });

    const [events, outcome] = await Promise.all([collect(stream.events), stream.result]);
    expect(events).toEqual([{
      type: "done",
      seq: terminalSequence,
      finishReason: null,
      replayedTerminal: true,
    }]);
    expect(outcome).toMatchObject({
      type: "done",
      streamId: id,
      seq: terminalSequence,
      finishReason: null,
    });
    expect(calls).toHaveLength(1);
    expect(calls[0]?.init.method).toBe("GET");
    expect(header(calls[0], "Last-Event-ID")).toBe(terminalSequence);
  });

  it("fails explicitly rather than rounding an unsafe numeric persistence cursor", async () => {
    let calls = 0;
    const stream = createDurableStream({
      url: baseURL,
      resumeFrom: { id, lastEventId: "9007199254740991" },
      persist: { get: () => null, set: () => undefined },
      fetch: async () => {
        calls += 1;
        return sseResponse([
          frame("streamweld.stream.done", "9007199254740992", { finish_reason: null, usage }),
        ], id);
      },
    });
    await expect(stream.result).rejects.toBeInstanceOf(StreamPersistenceError);
    expect(calls).toBe(1);
  });

  it("treats AbortSignal as local detach and stop() as a separate remote action", async () => {
    const controller = new AbortController();
    const calls: FetchCall[] = [];
    const fetcher = recordingFetch(calls, async (input, init) => {
      if (String(input).endsWith("/stop")) {
        expect(init.signal).not.toBe(controller.signal);
        expect(init.signal?.aborted).toBe(false);
        return new Response(JSON.stringify({
          stream_id: id,
          outcome: "stopped",
          partial_text: "partial",
          usage,
        }), { status: 202, headers: { "Content-Type": "application/json" } });
      }
      const signal = init.signal;
      return new Response(new ReadableStream<Uint8Array>({
        start(streamController) {
          streamController.enqueue(new TextEncoder().encode(frame("streamweld.stream.open", "1", open)));
          signal?.addEventListener("abort", () => streamController.error(new Error("detached")), { once: true });
        },
      }), { headers: streamHeaders(id) });
    });
    const stream = createDurableStream({
      url: baseURL,
      body: requestBody(),
      fetch: fetcher,
      signal: controller.signal,
    });
    await stream.idReady;
    controller.abort();
    await expect(stream.result).rejects.toSatisfy((error: unknown) =>
      error instanceof LocalAbortError && error.name === "AbortError",
    );
    expect(calls).toHaveLength(1);

    await expect(stream.stop()).resolves.toMatchObject({
      type: "stopped",
      streamId: id,
      partialText: "partial",
    });
    expect(calls).toHaveLength(2);
  });

  it.each([202, 503])("cancels a stalled HTTP %s stop body without Fetch abort propagation", async (status) => {
    vi.useFakeTimers();
    const stopSignals: AbortSignal[] = [];
    const cancel = vi.fn();
    let stalledBody: ReadableStreamDefaultController<Uint8Array> | undefined;
    try {
      const stream = createDurableStream({
        url: baseURL,
        resumeFrom: { id, lastEventId: "2" },
        fetch: async (input, init) => {
          if (!String(input).endsWith("/stop")) return sseResponse(["data: [DONE]\n\n"], id);
          const stopSignal = init?.signal;
          if (!stopSignal) throw new Error("stop request is missing its timeout signal");
          stopSignals.push(stopSignal);
          return new Response(new ReadableStream<Uint8Array>({
            start(controller) {
              stalledBody = controller;
            },
            cancel,
          }), { status });
        },
      });
      await stream.result;
      const stopResult = stream.stop();
      const failure = stopResult.catch((error: unknown) => error);
      await vi.advanceTimersByTimeAsync(30_000);
      expect(stopSignals[0]?.aborted).toBe(true);
      expect(cancel).toHaveBeenCalledOnce();
      expect(await failure).toMatchObject({
        name: "StreamTransportError",
        message: "stop request timed out",
      });
    } finally {
      stalledBody?.error(new Error("test cleanup"));
      vi.useRealTimers();
    }
  });

  it.each([200, 503])("detaches a stalled HTTP %s reader without Fetch abort propagation or retries", async (status) => {
    const controller = new AbortController();
    const cancel = vi.fn();
    const calls: FetchCall[] = [];
    let stalledBody: ReadableStreamDefaultController<Uint8Array> | undefined;
    let readStarted!: () => void;
    const reading = new Promise<void>((resolve) => { readStarted = resolve; });
    const stream = createDurableStream({
      url: baseURL,
      body: requestBody(),
      signal: controller.signal,
      resume: { maxAttempts: 2, backoff: { initialMs: 0, maxMs: 0 } },
      fetch: recordingFetch(calls, async () => new Response(new ReadableStream<Uint8Array>({
        start(bodyController) {
          stalledBody = bodyController;
          if (status === 200) {
            bodyController.enqueue(new TextEncoder().encode(frame("streamweld.stream.open", "1", open)));
          }
        },
        pull() { readStarted(); },
        cancel,
      }, { highWaterMark: 0 }), {
        status,
        headers: status === 200 ? streamHeaders(id) : { "Content-Type": "application/json" },
      })),
    });
    const result = stream.result.catch((error: unknown) => error);
    try {
      await reading;
      controller.abort();
      await Promise.resolve();
      expect(cancel).toHaveBeenCalledOnce();
      expect(await result).toBeInstanceOf(LocalAbortError);
      await expect(collect(stream.text)).rejects.toBeInstanceOf(LocalAbortError);
      expect(stream.state).toBe("disconnected");
      expect(calls).toHaveLength(1);
      expect(String(calls[0]?.input)).toBe(baseURL);
    } finally {
      stalledBody?.error(new Error("test cleanup"));
    }
  });

  it("does not process a buffered terminal frame after a callback detaches the reader", async () => {
    const controller = new AbortController();
    const stream = createDurableStream({
      url: baseURL,
      body: requestBody(),
      signal: controller.signal,
      onWarning: () => controller.abort(),
      fetch: async () => sseResponse([joinFrames(
        frame("streamweld.stream.open", "1", open),
        frame("streamweld.stream.warning", "2", { code: "notice", message: "detach now" }),
        frame("streamweld.stream.done", "3", { finish_reason: "stop", usage }),
      )], id),
    });
    await expect(stream.result).rejects.toBeInstanceOf(LocalAbortError);
    expect(stream.state).toBe("disconnected");
  });

  it("cancels an error response body returned after the caller already detached", async () => {
    const controller = new AbortController();
    const cancel = vi.fn();
    const fetcher = vi.fn(async () => {
      controller.abort();
      return new Response(new ReadableStream<Uint8Array>({ cancel }), {
        status: 503,
        headers: { "Content-Type": "application/json" },
      });
    });
    const stream = createDurableStream({
      url: baseURL,
      body: requestBody(),
      signal: controller.signal,
      fetch: fetcher,
    });
    await expect(stream.result).rejects.toBeInstanceOf(LocalAbortError);
    expect(cancel).toHaveBeenCalledOnce();
    expect(fetcher).toHaveBeenCalledOnce();
  });

  it("yields a terminal error event while text throws a typed generation error", async () => {
    const stream = createDurableStream({
      url: baseURL,
      body: requestBody(),
      fetch: async () => sseResponse([joinFrames(
        frame("streamweld.stream.open", "1", open),
        frame("message", "2", chatChunk("partial")),
        frame("streamweld.stream.error", "3", {
          code: "migration_refused",
          message: "unsafe continuation",
          reason: "tool_call_boundary",
          retriable: false,
          usage,
        }),
      )], id),
    });
    const eventsPromise = collect(stream.events);
    await expect(collect(stream.text)).rejects.toBeInstanceOf(StreamGenerationError);
    expect((await eventsPromise).at(-1)).toMatchObject({ type: "error", code: "migration_refused" });
    await expect(stream.result).resolves.toMatchObject({ type: "error", code: "migration_refused" });
  });

  it("continues an unsequenced degraded suffix and types unsequenced stopped", async () => {
    const persisted: string[] = [];
    const stream = createDurableStream({
      url: baseURL,
      body: requestBody(),
      persist: {
        get: () => null,
        set: () => undefined,
        setExact: (_id, seq) => persisted.push(seq),
      },
      fetch: async () => sseResponse([joinFrames(
        frame("streamweld.stream.open", "1", open),
        frame("streamweld.stream.warning", null, {
          code: "journal_degraded",
          message: "stream journaling failed; subsequent events are not resumable",
          details: {},
        }),
        frame("message", null, chatChunk("live suffix")),
        frame("streamweld.stream.stopped", null, {
          partial_text: "live suffix",
          usage,
        }),
      )], id),
    });
    const eventsPromise = collect(stream.events);
    expect(await collect(stream.text)).toEqual(["live suffix"]);
    const events = await eventsPromise;
    expect(events.slice(1).map((event) => event.seq)).toEqual([null, null, null]);
    await expect(stream.result).resolves.toMatchObject({ type: "stopped", seq: null });
    expect(persisted.at(-1)).toBe("1");
  });

  it("completes an initially degraded stream on [DONE] without inventing an id", async () => {
    const stream = createDurableStream({
      url: baseURL,
      body: requestBody(),
      fetch: async () => sseResponse([
        joinFrames(frame("message", null, chatChunk("degraded")), "data: [DONE]\n\n"),
      ], null, "degraded"),
    });
    expect(await collect(stream.text)).toEqual(["degraded"]);
    await expect(stream.result).resolves.toMatchObject({ type: "done", streamId: null, seq: null });
    await expect(stream.idReady).rejects.toBeInstanceOf(StreamNotIdentifiedError);
    expect(stream.id).toBeNull();
  });

  it("rejects an initially degraded response that claims a resumable id", async () => {
    const stream = createDurableStream({
      url: baseURL,
      body: requestBody(),
      fetch: async () => sseResponse([
        joinFrames(frame("message", null, chatChunk("degraded")), "data: [DONE]\n\n"),
      ], id, "degraded"),
    });
    await expect(stream.result).rejects.toBeInstanceOf(StreamProtocolError);
  });

  it("never retries a dropped initially degraded generation", async () => {
    let calls = 0;
    const stream = createDurableStream({
      url: baseURL,
      body: requestBody(),
      fetch: async () => {
        calls += 1;
        return sseResponse([frame("message", null, chatChunk("partial"))], null, "degraded");
      },
      resume: { maxAttempts: 5, backoff: { initialMs: 0, maxMs: 0 } },
    });
    await expect(stream.result).rejects.toBeInstanceOf(StreamTransportError);
    expect(calls).toBe(1);
  });

  it("does not leak a nonzero indexed choice into stream.text", async () => {
    const stream = createDurableStream({
      url: baseURL,
      body: requestBody(),
      fetch: async () => sseResponse([joinFrames(
        frame("streamweld.stream.open", "1", open),
        frame("message", "2", { choices: [{ index: 1, delta: { content: "wrong" } }] }),
        frame("streamweld.stream.done", "3", { finish_reason: "stop", usage }),
      )], id),
    });
    expect(await collect(stream.text)).toEqual([]);
    await stream.result;
  });

  it("fails an unused bounded view explicitly without interrupting the active view", async () => {
    const stream = createDurableStream({
      url: baseURL,
      body: requestBody(),
      limits: { maxReplayEvents: 2 },
      fetch: async () => sseResponse([joinFrames(
        frame("streamweld.stream.open", "1", open),
        frame("message", "2", chatChunk("A")),
        frame("message", "3", chatChunk("B")),
        frame("streamweld.stream.done", "4", { finish_reason: "stop", usage }),
      )], id),
    });
    expect(await collect(stream.text)).toEqual(["A", "B"]);
    const delayedEvents = stream.events[Symbol.asyncIterator]();
    await expect(delayedEvents.next()).rejects.toBeInstanceOf(StreamBufferLimitError);
  });

  it("rejects near-match content types", async () => {
    const stream = createDurableStream({
      url: baseURL,
      body: requestBody(),
      fetch: async () => new Response("", {
        headers: {
          "Content-Type": "text/event-streaming",
          "X-Streamweld-Durability": "durable",
          "X-Streamweld-Stream-Id": id,
        },
      }),
    });
    await expect(stream.result).rejects.toBeInstanceOf(StreamProtocolError);
  });

  it("cancels an unread response body after a protocol decoding failure", async () => {
    let canceled = 0;
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(new TextEncoder().encode("id: 1\ndata: not-json\n\n"));
      },
      cancel() {
        canceled += 1;
      },
    });
    const stream = createDurableStream({
      url: baseURL,
      body: requestBody(),
      fetch: async () => new Response(body, { headers: streamHeaders(id) }),
    });
    await expect(stream.result).rejects.toBeInstanceOf(StreamProtocolError);
    expect(canceled).toBe(1);
  });

  it.each([
    { "Content-Type": "text/plain" },
    { "X-Streamweld-Durability": "unknown" },
    { "X-Streamweld-Stream-Id": "invalid" },
  ])("cancels the unread response when headers are invalid: %j", async (invalidHeaders) => {
    const cancel = vi.fn();
    const headers = streamHeaders(id);
    for (const [name, value] of Object.entries(invalidHeaders)) headers.set(name, value);
    const stream = createDurableStream({
      url: baseURL,
      body: requestBody(),
      fetch: async () => new Response(new ReadableStream<Uint8Array>({ cancel }), { headers }),
    });
    await expect(stream.result).rejects.toBeInstanceOf(StreamProtocolError);
    expect(cancel).toHaveBeenCalledOnce();
  });

  it("cancels a reader-error body before reconnecting", async () => {
    let canceled = 0;
    let calls = 0;
    const stream = createDurableStream({
      url: baseURL,
      resumeFrom: { id, lastEventId: "5" },
      resume: { maxAttempts: 1, backoff: { initialMs: 0, maxMs: 0, jitter: false } },
      fetch: async () => {
        calls += 1;
        if (calls === 1) {
          return new Response(new ReadableStream<Uint8Array>({
            start(controller) {
              controller.enqueue(new TextEncoder().encode(frame("streamweld.reader.error", null, {
                code: "reader_lag_exceeded",
              })));
            },
            cancel() {
              canceled += 1;
            },
          }), { headers: streamHeaders(id) });
        }
        return sseResponse([
          frame("streamweld.stream.done", "6", { finish_reason: "stop", usage }),
        ], id);
      },
    });
    await expect(stream.result).resolves.toMatchObject({ type: "done", seq: "6" });
    expect(calls).toBe(2);
    expect(canceled).toBe(1);
  });

  it("rejects unsequenced data until a journal_degraded marker is observed", async () => {
    const stream = createDurableStream({
      url: baseURL,
      resumeFrom: { id, lastEventId: "1" },
      fetch: async () => sseResponse([frame("message", null, chatChunk("unsafe"))], id),
    });
    await expect(stream.result).rejects.toBeInstanceOf(StreamProtocolError);
  });
});

interface FetchCall {
  readonly input: RequestInfo | URL;
  readonly init: RequestInit;
}

function recordingFetch(
  calls: FetchCall[],
  implementation: (input: RequestInfo | URL, init: RequestInit) => Promise<Response>,
): typeof globalThis.fetch {
  return async (input, init = {}) => {
    calls.push({ input, init });
    return implementation(input, init);
  };
}

function header(call: FetchCall | undefined, name: string): string | null {
  return new Headers(call?.init.headers).get(name);
}

function requestBody(): Record<string, unknown> {
  return { model: "llama-test", messages: [{ role: "user", content: "hello" }], stream: true };
}

function chatChunk(content: string): Record<string, unknown> {
  return { choices: [{ index: 0, delta: { content }, finish_reason: null }] };
}

function frame(event: string, sequence: string | null, payload: unknown): string {
  const eventLine = event === "message" ? "" : `event: ${event}\n`;
  const idLine = sequence === null ? "" : `id: ${sequence}\n`;
  const data = JSON.stringify(payload).split("\n").map((line) => `data: ${line}\n`).join("");
  return `${eventLine}${idLine}${data}\n`;
}

function joinFrames(...frames: string[]): string {
  return frames.join("");
}

function streamHeaders(
  streamID: string | null,
  durability: "durable" | "degraded" = "durable",
): Headers {
  const headers = new Headers({
    "Content-Type": "text/event-stream; charset=utf-8",
    "X-Streamweld-Durability": durability,
  });
  if (streamID !== null) headers.set("X-Streamweld-Stream-Id", streamID);
  return headers;
}

function sseResponse(
  chunks: string[],
  streamID: string | null,
  durability: "durable" | "degraded" = "durable",
): Response {
  return new Response(new ReadableStream<Uint8Array>({
    start(controller) {
      const encoder = new TextEncoder();
      for (const chunk of chunks) controller.enqueue(encoder.encode(chunk));
      controller.close();
    },
  }), { headers: streamHeaders(streamID, durability) });
}

async function collect<T>(iterable: AsyncIterable<T>): Promise<T[]> {
  const values: T[] = [];
  for await (const value of iterable) values.push(value);
  return values;
}
