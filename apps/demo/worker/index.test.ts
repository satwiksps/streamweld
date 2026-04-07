import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createDurableStream, LocalAbortError, type StreamEvent } from "@streamweld/client";
import worker, { resetDemoStateForTests } from "./index.js";

interface TestContext {
  readonly pending: Promise<unknown>[];
  readonly context: ExecutionContext;
}

describe("demo worker", () => {
  beforeEach(() => {
    resetDemoStateForTests();
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("serves a complete Streamweld protocol stream", async () => {
    const harness = testContext();
    const response = await dispatch(
      new Request("https://demo.test/v1/chat/completions", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ model: "llama-3.1-8b", stream: true }),
      }),
      harness,
    );
    const bodyPromise = response.text();
    await vi.runAllTimersAsync();
    const body = await bodyPromise;

    expect(response.status).toBe(200);
    expect(response.headers.get("X-Streamweld-Durability")).toBe("durable");
    expect(response.headers.get("X-Streamweld-Stream-Id")).toMatch(/^[0-9a-hjkmnp-tv-z]{26}$/);
    expect(body).toContain("event: streamweld.stream.open");
    expect(body).toContain("event: streamweld.stream.done");
    expect(body).toContain("Durable ");
    expect(body).not.toContain("data: [DONE]");
    await Promise.all(harness.pending);
  });

  it("continues a durable stream through a backend failure with migration and seam events", async () => {
    const harness = testContext();
    const response = await dispatch(
      new Request("https://demo.test/v1/chat/completions", {
        method: "POST",
        body: JSON.stringify({ model: "llama-3.1-8b" }),
      }),
      harness,
    );
    const id = response.headers.get("X-Streamweld-Stream-Id");
    expect(id).not.toBeNull();
    const injection = await dispatch(
      new Request("https://demo.test/api/demo/inject", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ stream_id: id, scenario: "pod-kill" }),
      }),
      harness,
    );
    expect(injection.status).toBe(200);

    const bodyPromise = response.text();
    await vi.runAllTimersAsync();
    const body = await bodyPromise;
    expect(body).toContain("event: streamweld.stream.migration");
    expect(body).toContain('"reason":"crash"');
    expect(body).toContain('"code":"seam_reconciled"');
    expect(body).toContain("event: streamweld.stream.done");
  });

  it("drives the dependency-free client through migration without breaking its event loop", async () => {
    const harness = testContext();
    const demoFetch = fetchThroughWorker(harness);
    const stream = createDurableStream({
      url: "https://demo.test/v1/chat/completions",
      body: { model: "llama-3.1-8b", stream: true },
      fetch: demoFetch,
      resume: { maxAttempts: 0 },
    });
    const eventsPromise = collectEvents(stream.events);
    const id = await stream.idReady;
    await dispatch(
      new Request("https://demo.test/api/demo/inject", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ stream_id: id, scenario: "rolling-update" }),
      }),
      harness,
    );
    await vi.runAllTimersAsync();
    const events = await eventsPromise;

    expect(events.map((event) => event.type)).toContain("migration");
    expect(events.at(-1)?.type).toBe("done");
    const text = events
      .filter((event) => event.type === "chunk")
      .map((event) => chunkText(event.data))
      .join("");
    expect(text).toContain("without duplicated or missing text.");
  });

  it("resumes from the exact client cursor after a local connection drop", async () => {
    const harness = testContext();
    const demoFetch = fetchThroughWorker(harness);
    const controller = new AbortController();
    const first = createDurableStream({
      url: "https://demo.test/v1/chat/completions",
      body: { model: "llama-3.1-8b", stream: true },
      fetch: demoFetch,
      signal: controller.signal,
      resume: { maxAttempts: 0 },
    });
    let cursor = "0";
    let prefix = "";
    const firstReader = (async () => {
      try {
        for await (const event of first.events) {
          if (event.seq !== null) cursor = event.seq;
          if (event.type === "chunk") prefix += chunkText(event.data);
        }
      } catch (error) {
        expect(error).toBeInstanceOf(LocalAbortError);
      }
    })();
    const id = await first.idReady;
    await vi.advanceTimersByTimeAsync(340);
    controller.abort();
    await firstReader;
    await expect(first.result).rejects.toBeInstanceOf(LocalAbortError);
    expect(Number(cursor)).toBeGreaterThan(1);

    const resumed = createDurableStream({
      url: "https://demo.test/v1/chat/completions",
      resumeFrom: { id, lastEventId: cursor },
      fetch: demoFetch,
      resume: { maxAttempts: 0 },
    });
    const resumedEventsPromise = collectEvents(resumed.events);
    await vi.runAllTimersAsync();
    const resumedEvents = await resumedEventsPromise;
    const suffix = resumedEvents
      .filter((event) => event.type === "chunk")
      .map((event) => chunkText(event.data))
      .join("");
    expect(prefix + suffix).toContain("without duplicated or missing text.");
    expect(resumedEvents.at(-1)?.type).toBe("done");
  });

  it("truncates the equivalent direct stream after the same backend failure", async () => {
    const harness = testContext();
    const response = await dispatch(
      new Request("https://demo.test/api/demo/direct", {
        method: "POST",
        body: JSON.stringify({ model: "llama-3.1-8b" }),
      }),
      harness,
    );
    const id = response.headers.get("X-Demo-Stream-Id");
    expect(id).not.toBeNull();
    await dispatch(
      new Request("https://demo.test/api/demo/inject", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ stream_id: id, scenario: "pod-kill" }),
      }),
      harness,
    );

    const bodyPromise = response.text();
    await vi.runAllTimersAsync();
    const body = await bodyPromise;
    expect(body).not.toContain("data: [DONE]");
    expect(body).not.toContain("streamweld.stream.migration");
  });

  it("keeps explicit stop distinct and supports cursor-exclusive replay", async () => {
    const harness = testContext();
    const response = await dispatch(
      new Request("https://demo.test/v1/chat/completions", {
        method: "POST",
        body: JSON.stringify({ model: "llama-3.1-8b" }),
      }),
      harness,
    );
    const id = response.headers.get("X-Streamweld-Stream-Id");
    expect(id).not.toBeNull();
    const bodyPromise = response.text();
    await vi.advanceTimersByTimeAsync(200);

    const stop = await dispatch(
      new Request(`https://demo.test/v1/streams/${id}/stop`, { method: "POST" }),
      harness,
    );
    expect(stop.status).toBe(202);
    expect(await stop.json()).toMatchObject({ outcome: "stopped", stream_id: id });
    await vi.runAllTimersAsync();
    const body = await bodyPromise;
    expect(body).toContain("event: streamweld.stream.stopped");
    expect(body).not.toContain("event: streamweld.stream.done");

    const replay = await dispatch(
      new Request(`https://demo.test/v1/streams/${id}/events`, {
        headers: { "Last-Event-ID": "1" },
      }),
      harness,
    );
    const replayBody = await replay.text();
    expect(replayBody).not.toContain("id: 1\n");
    expect(replayBody).toContain("event: streamweld.stream.stopped");
  });
});

function testContext(): TestContext {
  const pending: Promise<unknown>[] = [];
  return {
    pending,
    context: {
      waitUntil(promise: Promise<unknown>) {
        pending.push(promise);
      },
      passThroughOnException() {},
      props: {},
    } as unknown as ExecutionContext,
  };
}

function dispatch(request: Request, harness: TestContext): Promise<Response> {
  return worker.fetch(request, {}, harness.context);
}

function fetchThroughWorker(harness: TestContext): typeof globalThis.fetch {
  return (async (input: RequestInfo | URL, init?: RequestInit) => {
    const request = new Request(input, init);
    const response = await dispatch(request, harness);
    if (response.body === null) return response;
    const reader = response.body.getReader();
    const body = new ReadableStream<Uint8Array>({
      start(controller) {
        let aborted = false;
        const abort = () => {
          aborted = true;
          void reader.cancel();
          controller.error(new DOMException("The operation was aborted", "AbortError"));
        };
        request.signal.addEventListener("abort", abort, { once: true });
        void (async () => {
          try {
            for (;;) {
              const result = await reader.read();
              if (result.done) break;
              controller.enqueue(result.value);
            }
            request.signal.removeEventListener("abort", abort);
            if (!aborted) controller.close();
          } catch (error) {
            if (!aborted) controller.error(error);
          }
        })();
      },
      cancel() {
        return reader.cancel();
      },
    });
    return new Response(body, {
      status: response.status,
      statusText: response.statusText,
      headers: response.headers,
    });
  }) as typeof globalThis.fetch;
}

async function collectEvents(events: AsyncIterable<StreamEvent>): Promise<StreamEvent[]> {
  const collected: StreamEvent[] = [];
  for await (const event of events) collected.push(event);
  return collected;
}

function chunkText(data: unknown): string {
  if (typeof data !== "object" || data === null) return "";
  const choices = (data as { choices?: unknown }).choices;
  if (!Array.isArray(choices)) return "";
  const first = choices[0] as { delta?: { content?: unknown } } | undefined;
  return typeof first?.delta?.content === "string" ? first.delta.content : "";
}
