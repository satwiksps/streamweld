import assert from "node:assert/strict";
import { once } from "node:events";
import { createServer } from "node:http";
import test from "node:test";
import { setTimeout as delay } from "node:timers/promises";
import { createDurableStream, LocalAbortError, StreamTransportError } from "../dist/index.js";

const id = "01arz3ndektsv4rrffq69g5fav";
const streamHeaders = {
  "content-type": "text/event-stream",
  "x-streamweld-durability": "durable",
  "x-streamweld-stream-id": id,
};

async function fixture(t, handler) {
  const server = createServer(handler);
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  t.after(async () => {
    server.closeAllConnections();
    await new Promise((resolve) => server.close(resolve));
  });
  return `http://127.0.0.1:${server.address().port}/v1/chat/completions`;
}

async function collectGarbage() {
  assert.equal(typeof global.gc, "function", "run this test with --expose-gc");
  // Allow Fetch to release its request after headers, then collect the internal
  // abort controller that older Node runtimes fail to retain for redirect:error.
  for (let i = 0; i < 3; i += 1) {
    await delay(50);
    global.gc();
  }
}

async function within(promise, milliseconds) {
  const controller = new AbortController();
  try {
    return await Promise.race([
      promise,
      delay(milliseconds, undefined, { signal: controller.signal }).then(() => {
        throw new Error(`operation did not finish within ${milliseconds}ms`);
      }),
    ]);
  } finally {
    controller.abort();
  }
}

test("aborting a stalled SSE reader closes HTTP after garbage collection", async (t) => {
  let requests = 0;
  let closed;
  const disconnected = new Promise((resolve) => { closed = resolve; });
  const url = await fixture(t, (request, response) => {
    requests += 1;
    request.resume();
    response.on("close", closed);
    response.writeHead(200, streamHeaders);
    response.write(`id: 1\nevent: streamweld.stream.open\ndata: ${JSON.stringify({
      stream_id: id, model: "test", model_version: null, backend_id: "fixture",
    })}\n\n`);
  });
  const controller = new AbortController();
  const stream = createDurableStream({
    url, body: { model: "test", stream: true }, signal: controller.signal,
  });
  const events = stream.events[Symbol.asyncIterator]();
  assert.equal((await events.next()).value.type, "open");
  await collectGarbage();
  controller.abort();
  await within(assert.rejects(stream.result, LocalAbortError), 2_000);
  await within(disconnected, 2_000);
  assert.equal(requests, 1, "detach must not reconnect or send a stop request");
});

test("stop bounds stalled success and error bodies after garbage collection", { concurrency: true }, async (t) => {
  await Promise.all([202, 503].map((status) => t.test(`HTTP ${status}`, { concurrency: true }, async (t) => {
    let stopOpened;
    const opened = new Promise((resolve) => { stopOpened = resolve; });
    let stopClosed;
    const disconnected = new Promise((resolve) => { stopClosed = resolve; });
    const url = await fixture(t, (request, response) => {
      request.resume();
      if (request.url.endsWith("/stop")) {
        response.on("close", stopClosed);
        response.writeHead(status, { "content-type": "application/json" });
        response.flushHeaders();
        stopOpened();
      } else {
        response.writeHead(200, streamHeaders);
        response.end("data: [DONE]\n\n");
      }
    });
    const stream = createDurableStream({ url, resumeFrom: { id, lastEventId: "2" } });
    await stream.result;
    const started = Date.now();
    const stopped = assert.rejects(stream.stop(), (error) =>
      error instanceof StreamTransportError && error.message === "stop request timed out");
    await opened;
    await collectGarbage();
    await within(stopped, 35_000);
    assert.ok(Date.now() - started >= 29_000, "the normal 30-second timeout must apply");
    await within(disconnected, 2_000);
  })));
});
