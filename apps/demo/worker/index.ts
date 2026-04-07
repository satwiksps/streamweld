type FailureScenario =
  | "pod-kill"
  | "rolling-update"
  | "spot-reclaim"
  | "client-drop"
  | "explicit-stop";

interface JournalEntry {
  readonly sequence: number | null;
  readonly wire: Uint8Array;
  readonly terminal: boolean;
}

interface Subscriber {
  readonly push: (entry: JournalEntry) => void;
  readonly close: () => void;
}

interface DemoSession {
  readonly id: string;
  readonly model: string;
  readonly entries: JournalEntry[];
  readonly subscribers: Set<Subscriber>;
  readonly mode: "durable" | "direct";
  sequence: number;
  backend: string;
  text: string;
  completionTokens: number;
  failure: FailureScenario | null;
  failureHandled: boolean;
  stopped: boolean;
  terminal: boolean;
  degraded: boolean;
}

const encoder = new TextEncoder();
const ulidAlphabet = "0123456789abcdefghjkmnpqrstvwxyz";
const sessions = new Map<string, DemoSession>();
const failureScenarios = new Set<FailureScenario>([
  "pod-kill",
  "rolling-update",
  "spot-reclaim",
  "client-drop",
  "explicit-stop",
]);

const answer =
  "Durable streaming separates generation from a single connection. " +
  "Each chunk is journaled with an exact sequence number. " +
  "When backend-a disappears, Streamweld opens a continuation on backend-c, " +
  "reconciles the seam, and the reader keeps receiving one coherent answer " +
  "without duplicated or missing text.";

const answerChunks = answer.match(/\S+\s*/g) ?? [answer];

const worker = {
  async fetch(request: Request, _env: unknown, context: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);

    if (request.method === "OPTIONS") return emptyResponse(204);
    if (request.method === "GET" && url.pathname === "/api/demo/health") {
      return jsonResponse({
        status: "ready",
        backends: [
          { id: "backend-a", version: "v1", state: "healthy" },
          { id: "backend-b", version: "v1", state: "healthy" },
          { id: "backend-c", version: "v2", state: "healthy" },
        ],
      });
    }

    if (request.method === "POST" && url.pathname === "/api/demo/inject") {
      return injectFailure(request);
    }

    if (request.method === "POST" && url.pathname === "/api/demo/direct") {
      const session = createSession(await readModel(request), "direct");
      const response = directStreamResponse(session);
      context.waitUntil(produce(session));
      return response;
    }

    if (request.method === "POST" && url.pathname === "/v1/chat/completions") {
      const session = createSession(await readModel(request), "durable");
      const response = durableStreamResponse(session, 0);
      context.waitUntil(produce(session));
      return response;
    }

    const eventsMatch = /^\/v1\/streams\/([^/]+)\/events$/.exec(url.pathname);
    if (request.method === "GET" && eventsMatch?.[1] !== undefined) {
      const id = decodeURIComponent(eventsMatch[1]);
      const session = sessions.get(id);
      if (session === undefined) return gone(id, "stream_not_found", "demo stream is not available");
      if (session.degraded) {
        return gone(id, "stream_offset_expired", "the demo journal degraded and cannot be replayed");
      }
      const cursor = parseCursor(request.headers.get("Last-Event-ID"));
      if (cursor === null) return jsonResponse({ error: { code: "invalid_last_event_id" } }, 400);
      return durableStreamResponse(session, cursor);
    }

    const stopMatch = /^\/v1\/streams\/([^/]+)\/stop$/.exec(url.pathname);
    if (request.method === "POST" && stopMatch?.[1] !== undefined) {
      const id = decodeURIComponent(stopMatch[1]);
      const session = sessions.get(id);
      if (session === undefined) return gone(id, "stream_not_found", "demo stream is not available");
      session.stopped = true;
      return jsonResponse(
        {
          stream_id: session.id,
          outcome: "stopped",
          partial_text: session.text,
          usage: usage(session),
        },
        202,
      );
    }

    return jsonResponse({ error: { code: "not_found", message: "demo route not found" } }, 404);
  },
} satisfies ExportedHandler;

export default worker;

function createSession(model: string, mode: DemoSession["mode"]): DemoSession {
  const session: DemoSession = {
    id: createULID(),
    model,
    entries: [],
    subscribers: new Set(),
    mode,
    sequence: 0,
    backend: "backend-a",
    text: "",
    completionTokens: 0,
    failure: null,
    failureHandled: false,
    stopped: false,
    terminal: false,
    degraded: false,
  };
  sessions.set(session.id, session);
  return session;
}

async function produce(session: DemoSession): Promise<void> {
  if (session.mode === "durable") {
    append(
      session,
      "streamweld.stream.open",
      {
        stream_id: session.id,
        model: session.model,
        model_version: "v1",
        backend_id: session.backend,
      },
    );
  }

  for (const chunk of answerChunks) {
    await wait(92);
    if (session.terminal) return;
    if (session.stopped) {
      finishStopped(session);
      return;
    }
    if (session.failure !== null && !session.failureHandled) {
      const shouldContinue = handleFailure(session);
      if (!shouldContinue) return;
    }

    session.text += chunk;
    session.completionTokens += 1;
    appendMessage(session, chunk);
  }

  await wait(70);
  if (session.stopped) {
    finishStopped(session);
    return;
  }
  if (session.terminal) return;

  if (session.mode === "direct") {
    appendRaw(session, "data: [DONE]\n\n", true, false);
  } else {
    append(
      session,
      "streamweld.stream.done",
      { finish_reason: "stop", usage: usage(session) },
      true,
      !session.degraded,
    );
  }
}

function handleFailure(session: DemoSession): boolean {
  const scenario = session.failure;
  session.failureHandled = true;
  if (scenario === null || scenario === "client-drop") return true;

  if (session.mode === "direct") {
    session.terminal = true;
    closeSubscribers(session);
    return false;
  }

  if (scenario === "explicit-stop") {
    session.stopped = true;
    finishStopped(session);
    return false;
  }

  const reason = scenario === "pod-kill" ? "crash" : "drain";
  const nextBackend = scenario === "rolling-update" ? "backend-c" : "backend-b";
  append(session, "streamweld.stream.migration", {
    from_backend: session.backend,
    to_backend: nextBackend,
    reason,
    rescued_tokens: session.completionTokens,
    token_count_estimated: false,
    attempt: 2,
  });
  session.backend = nextBackend;
  append(session, "streamweld.stream.warning", {
    code: "seam_reconciled",
    message: "continuation matched the retained token suffix",
    predicate: null,
    details: { overlap_bytes: 18 },
  });
  return true;
}

function finishStopped(session: DemoSession): void {
  if (session.terminal) return;
  if (session.mode === "direct") {
    session.terminal = true;
    closeSubscribers(session);
    return;
  }
  append(
    session,
    "streamweld.stream.stopped",
    { partial_text: session.text, usage: usage(session) },
    true,
    !session.degraded,
  );
}

function appendMessage(session: DemoSession, content: string): void {
  const data = {
    id: `chatcmpl-${session.id}`,
    object: "chat.completion.chunk",
    model: session.model,
    choices: [{ index: 0, delta: { content }, finish_reason: null }],
  };
  if (session.mode === "direct") {
    appendRaw(session, `data: ${JSON.stringify(data)}\n\n`, false, false);
  } else {
    append(session, "message", data, false, !session.degraded);
  }
}

function append(
  session: DemoSession,
  event: string,
  data: unknown,
  terminal = false,
  sequenced = true,
): void {
  const sequence = sequenced ? ++session.sequence : null;
  const idLine = sequence === null ? "" : `id: ${String(sequence)}\n`;
  const eventLine = event === "message" ? "" : `event: ${event}\n`;
  appendRaw(session, `${idLine}${eventLine}data: ${JSON.stringify(data)}\n\n`, terminal, false, sequence);
}

function appendRaw(
  session: DemoSession,
  wire: string,
  terminal: boolean,
  sequenced: boolean,
  explicitSequence?: number | null,
): void {
  const sequence = explicitSequence === undefined
    ? (sequenced ? ++session.sequence : null)
    : explicitSequence;
  const entry = { sequence, wire: encoder.encode(wire), terminal };
  session.entries.push(entry);
  for (const subscriber of [...session.subscribers]) subscriber.push(entry);
  if (terminal) {
    session.terminal = true;
    closeSubscribers(session);
  }
}

function durableStreamResponse(session: DemoSession, cursor: number): Response {
  return streamResponse(session, cursor, {
    "X-Streamweld-Stream-Id": session.id,
    "X-Streamweld-Durability": "durable",
  });
}

function directStreamResponse(session: DemoSession): Response {
  return streamResponse(session, 0, { "X-Demo-Stream-Id": session.id });
}

function streamResponse(
  session: DemoSession,
  cursor: number,
  headers: Record<string, string>,
): Response {
  let subscriber: Subscriber | null = null;
  const body = new ReadableStream<Uint8Array>({
    start(controller) {
      const push = (entry: JournalEntry) => {
        if (entry.sequence !== null && entry.sequence <= cursor) return;
        controller.enqueue(entry.wire);
      };
      const close = () => {
        if (subscriber !== null) session.subscribers.delete(subscriber);
        try {
          controller.close();
        } catch {
          // A browser cancellation may race the producer terminal entry.
        }
      };
      subscriber = { push, close };
      for (const entry of session.entries) push(entry);
      if (session.terminal) close();
      else session.subscribers.add(subscriber);
    },
    cancel() {
      if (subscriber !== null) session.subscribers.delete(subscriber);
    },
  });

  return new Response(body, {
    status: 200,
    headers: corsHeaders({
      "Cache-Control": "no-cache, no-transform",
      "Content-Type": "text/event-stream; charset=utf-8",
      ...headers,
    }),
  });
}

async function injectFailure(request: Request): Promise<Response> {
  let payload: unknown;
  try {
    payload = await request.json();
  } catch {
    return jsonResponse({ error: { code: "invalid_json" } }, 400);
  }
  if (!isRecord(payload)) return jsonResponse({ error: { code: "invalid_request" } }, 400);
  const id = payload["stream_id"];
  const scenario = payload["scenario"];
  if (typeof id !== "string" || typeof scenario !== "string" || !failureScenarios.has(scenario as FailureScenario)) {
    return jsonResponse({ error: { code: "invalid_injection" } }, 400);
  }
  const session = sessions.get(id);
  if (session === undefined) return jsonResponse({ error: { code: "stream_not_found" } }, 404);
  if (session.terminal) return jsonResponse({ error: { code: "stream_terminal" } }, 409);
  session.failure = scenario as FailureScenario;
  return jsonResponse({ accepted: true, stream_id: id, scenario });
}

async function readModel(request: Request): Promise<string> {
  try {
    const body = await request.json();
    if (isRecord(body) && typeof body["model"] === "string" && body["model"].length > 0) {
      return body["model"];
    }
  } catch {
    // The demo deliberately falls back to its visible default model.
  }
  return "llama-3.1-8b";
}

function parseCursor(value: string | null): number | null {
  if (value === null || value === "") return 0;
  if (!/^(0|[1-9][0-9]*)$/.test(value)) return null;
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) && parsed >= 0 ? parsed : null;
}

function usage(session: DemoSession) {
  return {
    prompt_tokens: 14,
    completion_tokens: session.completionTokens,
    total_tokens: 14 + session.completionTokens,
    estimated: false,
  };
}

function closeSubscribers(session: DemoSession): void {
  for (const subscriber of [...session.subscribers]) subscriber.close();
  session.subscribers.clear();
}

function gone(id: string, code: string, message: string): Response {
  return jsonResponse({ error: { code, message, stream_id: id } }, 410, {
    "X-Streamweld-Stream-Id": id,
  });
}

function jsonResponse(body: unknown, status = 200, headers: Record<string, string> = {}): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: corsHeaders({ "Content-Type": "application/json; charset=utf-8", ...headers }),
  });
}

function emptyResponse(status: number): Response {
  return new Response(null, { status, headers: corsHeaders() });
}

function corsHeaders(source: Record<string, string> = {}): Headers {
  const headers = new Headers(source);
  headers.set("Access-Control-Allow-Origin", "*");
  headers.set("Access-Control-Allow-Headers", "Content-Type, Last-Event-ID, X-Streamweld-Idempotency-Key, X-Streamweld-Verbose");
  headers.set("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  headers.set("Access-Control-Expose-Headers", "X-Streamweld-Stream-Id, X-Streamweld-Durability, X-Demo-Stream-Id");
  return headers;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function wait(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function createULID(): string {
  let timestamp = Date.now();
  const time = new Array<string>(10);
  for (let index = time.length - 1; index >= 0; index -= 1) {
    time[index] = ulidAlphabet[timestamp % 32] ?? "0";
    timestamp = Math.floor(timestamp / 32);
  }
  const random = crypto.getRandomValues(new Uint8Array(10));
  let bits = 0;
  let bitCount = 0;
  let suffix = "";
  for (const value of random) {
    bits = (bits << 8) | value;
    bitCount += 8;
    while (bitCount >= 5) {
      bitCount -= 5;
      suffix += ulidAlphabet[(bits >>> bitCount) & 31] ?? "0";
    }
  }
  return `${time.join("")}${suffix}`;
}

export function resetDemoStateForTests(): void {
  sessions.clear();
}
