import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createDurableStream, LocalAbortError, type StreamEvent } from "@streamweld/client";
import vercelFunction from "../api/streamweld.js";
import { createTestWorker } from "./index.js";

interface TestContext {
  readonly pending: Promise<unknown>[];
  readonly context: ExecutionContext;
}

let worker: ReturnType<typeof createTestWorker>;

describe("demo worker", () => {
  beforeEach(() => {
    worker = createTestWorker();
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.unstubAllEnvs();
    vi.unstubAllGlobals();
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

  it("adapts Vercel rewrites without claiming durable storage", async () => {
    const response = await vercelFunction.fetch(new Request(
      "https://demo.test/api/streamweld?__streamweld_path=/api/demo/health",
    ));

    expect(response.status).toBe(200);
    expect(await response.json()).toMatchObject({ status: "ready", storage: "ephemeral" });
  });

  it("streams and injects a failure through the Vercel adapter", async () => {
    const harness = testContext();
    const response = await vercelFunction.fetch(
      new Request(
        "https://demo.test/api/streamweld?__streamweld_path=/v1/chat/completions",
        { method: "POST", body: JSON.stringify({ model: "llama-3.1-8b" }) },
      ),
      harness.context,
    );
    const id = response.headers.get("X-Streamweld-Stream-Id");
    expect(id).not.toBeNull();

    const injection = await vercelFunction.fetch(
      new Request(
        "https://demo.test/api/streamweld?__streamweld_path=/api/demo/inject",
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ stream_id: id, scenario: "pod-kill" }),
        },
      ),
      harness.context,
    );
    expect(injection.status).toBe(200);

    const bodyPromise = response.text();
    await vi.runAllTimersAsync();
    expect(await bodyPromise).toContain("event: streamweld.stream.migration");
    await Promise.all(harness.pending);
  });

  it("proxies Vercel API calls to a configured shared Worker backend", async () => {
    const upstreamResponse = new Response(JSON.stringify({ marker: "relayed-body" }), {
      headers: { "X-Upstream-Response": "preserved" },
    });
    const upstreamFetch = vi.fn(async (_request: Request) => upstreamResponse);
    vi.stubEnv("STREAMWELD_DEMO_UPSTREAM_ORIGIN", "https://worker.demo.test");
    vi.stubGlobal("fetch", upstreamFetch);

    const response = await vercelFunction.fetch(new Request(
      "https://demo.test/api/streamweld?__streamweld_path=/api/demo/health",
      {
        headers: {
          Accept: "application/json",
          Authorization: "Bearer must-not-leak",
          Cookie: "session=must-not-leak",
          "X-Streamweld-Verbose": "1",
          "X-Vercel-Forwarded-For": "must-not-leak",
        },
      },
    ));

    expect(response.status).toBe(200);
    expect(response).not.toBe(upstreamResponse);
    expect(await response.json()).toEqual({ marker: "relayed-body" });
    expect(response.headers.get("X-Upstream-Response")).toBe("preserved");
    const forwarded = upstreamFetch.mock.calls[0]?.[0];
    expect(forwarded).toBeInstanceOf(Request);
    expect(forwarded?.url).toBe("https://worker.demo.test/api/demo/health");
    expect(Object.fromEntries(forwarded!.headers.entries())).toEqual({
      accept: "application/json",
      "x-streamweld-verbose": "1",
    });
    expect(upstreamFetch).toHaveBeenCalledOnce();
  });

  it("rejects a normalized path that escapes the public API prefixes", async () => {
    const upstreamFetch = vi.fn();
    vi.stubEnv("STREAMWELD_DEMO_UPSTREAM_ORIGIN", "https://worker.demo.test");
    vi.stubGlobal("fetch", upstreamFetch);

    const response = await vercelFunction.fetch(new Request(
      "https://demo.test/api/streamweld?__streamweld_path=%2Fv1%2F..%2Fadmin",
    ));

    expect(response.status).toBe(404);
    expect(upstreamFetch).not.toHaveBeenCalled();
  });

  it("requires a shared upstream in the hosted Vercel runtime", async () => {
    vi.stubEnv("VERCEL_ENV", "production");
    vi.stubEnv("STREAMWELD_DEMO_UPSTREAM_ORIGIN", "");

    const response = await vercelFunction.fetch(new Request(
      "https://demo.test/api/streamweld?__streamweld_path=/api/demo/health",
    ));

    expect(response.status).toBe(503);
    expect(await response.json()).toMatchObject({ error: { code: "upstream_required" } });
  });

  it("bounds public demo input and only accepts the visible model", async () => {
    const harness = testContext();
    const unsupported = await dispatch(new Request("https://demo.test/v1/chat/completions", {
      method: "POST",
      body: JSON.stringify({ model: "arbitrary-expensive-model" }),
    }), harness);
    expect(unsupported.status).toBe(400);
    expect(await unsupported.json()).toMatchObject({ error: { code: "unsupported_model" } });

    const oversized = await dispatch(new Request("https://demo.test/v1/chat/completions", {
      method: "POST",
      body: JSON.stringify({ model: "llama-3.1-8b", padding: "x".repeat(20_000) }),
    }), harness);
    expect(oversized.status).toBe(413);
    expect(await oversized.json()).toMatchObject({ error: { code: "request_too_large" } });
  });

  it("does not expose the demo API to arbitrary browser origins", async () => {
    const harness = testContext();
    const response = await dispatch(new Request("https://demo.test/api/demo/health", {
      headers: { Origin: "https://untrusted.example" },
    }), harness);

    expect(response.status).toBe(200);
    expect(response.headers.has("Access-Control-Allow-Origin")).toBe(false);
  });

  it("releases journal entries before the producer reaches a terminal event", async () => {
    const harness = testContext();
    const response = await dispatch(
      new Request("https://demo.test/v1/chat/completions", {
        method: "POST",
        body: JSON.stringify({ model: "llama-3.1-8b" }),
      }),
      harness,
    );
    const reader = response.body?.getReader();
    expect(reader).toBeDefined();
    let firstReadResolved = false;
    const firstRead = reader!.read().then((result) => {
      firstReadResolved = true;
      return result;
    });

    await vi.advanceTimersByTimeAsync(150);
    await Promise.resolve();
    expect(firstReadResolved).toBe(true);
    const first = await firstRead;
    expect(new TextDecoder().decode(first.value)).toContain("event: streamweld.stream.open");

    await reader!.cancel();
    await vi.runAllTimersAsync();
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
