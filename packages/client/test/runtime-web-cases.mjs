import {
  createDurableStream,
  LocalAbortError,
  StreamExpiredError,
} from "../dist/index.js";

const id = "01arz3ndektsv4rrffq69g5fav";
const usage = { prompt_tokens: 2, completion_tokens: 2, total_tokens: 4, estimated: false };
const headers = {
  "content-type": "text/event-stream",
  "x-streamweld-durability": "durable",
  "x-streamweld-stream-id": id,
};
const open = { stream_id: id, model: "test", model_version: null, backend_id: "fixture" };

function equal(actual, expected, label) {
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    throw new Error(`${label}: got ${JSON.stringify(actual)}, expected ${JSON.stringify(expected)}`);
  }
}

function frame(type, seq, data) {
  return `id: ${seq}\nevent: ${type}\ndata: ${JSON.stringify(data)}\n\n`;
}

function chunk(text) {
  return { choices: [{ index: 0, delta: { content: text }, finish_reason: null }] };
}

function fragmentedResponse(wire) {
  const bytes = new TextEncoder().encode(wire);
  let offset = 0;
  return new Response(new ReadableStream({
    pull(controller) {
      if (offset === bytes.length) {
        controller.close();
        return;
      }
      // Split UTF-8 code points and SSE lines across writes to the HTTP server.
      controller.enqueue(bytes.slice(offset, offset + 3));
      offset = Math.min(offset + 3, bytes.length);
    },
  }), { headers });
}

async function collect(iterable) {
  const values = [];
  for await (const value of iterable) values.push(value);
  return values;
}

async function within(promise, milliseconds = 3_000) {
  let timeout;
  try {
    return await Promise.race([
      promise,
      new Promise((_, reject) => {
        timeout = setTimeout(() => reject(new Error(`operation exceeded ${milliseconds}ms`)), milliseconds);
      }),
    ]);
  } finally {
    clearTimeout(timeout);
  }
}

async function expectRejection(promise, ErrorType) {
  try {
    await within(promise);
  } catch (error) {
    if (error instanceof ErrorType) return error;
    throw error;
  }
  throw new Error(`expected ${ErrorType.name}`);
}

export const httpCases = [
  {
    name: "built ESM resumes fragmented UTF-8 SSE with an exact cursor",
    async run(listen) {
      const requests = [];
      const cursor = "9007199254740993";
      const server = await listen(async (request) => {
        const recorded = { method: request.method, path: new URL(request.url).pathname,
          cursor: request.headers.get("last-event-id"), authorization: request.headers.get("authorization") };
        if (request.method === "POST") recorded.body = await request.json();
        requests.push(recorded);
        if (requests.length === 1) {
          return fragmentedResponse(frame("streamweld.stream.open", "1", open) + frame("message", cursor, chunk("hé")));
        }
        return fragmentedResponse(
          frame("message", cursor, chunk("duplicate")) +
          frame("message", "9007199254740994", chunk("🙂llo")) +
          frame("streamweld.stream.done", "9007199254740995", { finish_reason: "stop", usage }),
        );
      });
      const controller = new AbortController();
      try {
        const body = { model: "test", messages: [{ role: "user", content: "hello" }], stream: true };
        const stream = createDurableStream({
          url: server.url, body, signal: controller.signal, headers: { Authorization: "Bearer fixture" },
          resume: { maxAttempts: 1, backoff: { initialMs: 0, maxMs: 0, jitter: false } },
        });
        const [text, events, outcome] = await within(Promise.all([collect(stream.text), collect(stream.events), stream.result]));
        equal(text.join(""), "hé🙂llo", "complete text without duplicate replay");
        equal(events.map((event) => event.seq), ["1", cursor, "9007199254740994", "9007199254740995"], "exact event cursors");
        equal(outcome.type, "done", "terminal outcome");
        equal(outcome.seq, "9007199254740995", "terminal cursor");
        equal(requests, [
          { method: "POST", path: "/v1/chat/completions", cursor: null, authorization: "Bearer fixture", body },
          { method: "GET", path: `/v1/streams/${id}/events`, cursor, authorization: "Bearer fixture" },
        ], "initial request and reconnect");
      } finally {
        controller.abort();
        await server.close();
      }
    },
  },
  {
    name: "built ESM detaches locally and sends stop only when explicitly requested",
    async run(listen) {
      const requests = [];
      let closeReader;
      const disconnected = new Promise((resolve) => { closeReader = resolve; });
      const server = await listen((request) => {
        const path = new URL(request.url).pathname;
        requests.push({ method: request.method, path });
        if (path.endsWith("/stop")) {
          return Response.json({ stream_id: id, outcome: "stopped", partial_text: "hello", usage });
        }
        return new Response(new ReadableStream({
          start(controller) {
            controller.enqueue(new TextEncoder().encode(frame("streamweld.stream.open", "1", open)));
          },
          cancel() { closeReader(); },
        }), { headers });
      });
      const controller = new AbortController();
      try {
        const stream = createDurableStream({ url: server.url, body: { model: "test", stream: true }, signal: controller.signal });
        const events = stream.events[Symbol.asyncIterator]();
        equal((await within(events.next())).value.type, "open", "initial event");
        controller.abort();
        await expectRejection(stream.result, LocalAbortError);
        await within(disconnected);
        equal(requests.length, 1, "detach must not reconnect or stop");
        const stopped = await within(stream.stop());
        equal(stopped.type, "stopped", "explicit stop outcome");
        equal(stopped.partialText, "hello", "stopped prefix");
        equal(requests[1], { method: "POST", path: `/v1/streams/${id}/stop` }, "explicit stop request");
      } finally {
        controller.abort();
        await server.close();
      }
    },
  },
  {
    name: "built ESM reports HTTP expiration without starting another generation",
    async run(listen) {
      let requests = 0;
      const server = await listen(() => {
        requests += 1;
        return Response.json({ error: { code: "stream_expired", message: "fixture expired", stream_id: id } }, { status: 410 });
      });
      const controller = new AbortController();
      try {
        const stream = createDurableStream({ url: server.url, resumeFrom: { id, lastEventId: "2" }, signal: controller.signal });
        const error = await expectRejection(stream.result, StreamExpiredError);
        equal(error.code, "stream_expired", "expiration code");
        equal(requests, 1, "expired streams must not retry");
      } finally {
        controller.abort();
        await server.close();
      }
    },
  },
];
