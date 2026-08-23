import { demoSchemaStatements } from "../db/schema.js";

type FailureScenario =
  | "pod-kill"
  | "rolling-update"
  | "spot-reclaim"
  | "client-drop"
  | "explicit-stop";

type SessionMode = "durable" | "direct";
type StorageKind = "shared" | "ephemeral";

interface Env {
  readonly DB?: D1Database;
}

interface JournalEntry {
  readonly ordinal: number;
  readonly sequence: number | null;
  readonly wire: string;
  readonly terminal: boolean;
}

interface DemoSession {
  readonly id: string;
  readonly model: string;
  readonly mode: SessionMode;
  sequence: number;
  ordinal: number;
  backend: string;
  text: string;
  completionTokens: number;
  failure: FailureScenario | null;
  failureHandled: boolean;
  stopped: boolean;
  terminal: boolean;
  degraded: boolean;
  readonly createdAt: number;
}

interface SessionControl {
  readonly failure: FailureScenario | null;
  readonly failureHandled: boolean;
  readonly stopped: boolean;
  readonly terminal: boolean;
  readonly degraded: boolean;
}

interface ReplayBatch {
  readonly exists: boolean;
  readonly terminal: boolean;
  readonly degraded: boolean;
  readonly entries: readonly JournalEntry[];
  readonly more: boolean;
}

type FailureUpdate = "accepted" | "missing" | "terminal";

interface DemoStore {
  initialize(): Promise<void>;
  cleanup(before: number): Promise<void>;
  create(session: DemoSession): Promise<void>;
  get(id: string): Promise<DemoSession | null>;
  control(id: string): Promise<SessionControl | null>;
  setFailure(id: string, scenario: FailureScenario): Promise<FailureUpdate>;
  requestStop(id: string): Promise<DemoSession | null>;
  append(session: DemoSession, entry: JournalEntry): Promise<void>;
  markTerminal(session: DemoSession): Promise<void>;
  replay(id: string, afterOrdinal: number): Promise<ReplayBatch>;
}

interface DemoHandler {
  fetch(request: Request, env: Env, context: ExecutionContext): Promise<Response>;
}

interface SessionRow {
  readonly id: string;
  readonly model: string;
  readonly mode: SessionMode;
  readonly sequence: number;
  readonly ordinal: number;
  readonly backend: string;
  readonly text: string;
  readonly completion_tokens: number;
  readonly failure: string | null;
  readonly failure_handled: number;
  readonly stopped: number;
  readonly terminal: number;
  readonly degraded: number;
  readonly created_at: number;
}

interface ControlRow {
  readonly failure: string | null;
  readonly failure_handled: number;
  readonly stopped: number;
  readonly terminal: number;
  readonly degraded: number;
}

interface ReplayRow {
  readonly ordinal: number | null;
  readonly sequence: number | null;
  readonly wire: string | null;
  readonly entry_terminal: number | null;
  readonly session_terminal: number;
  readonly degraded: number;
}

const encoder = new TextEncoder();
const ulidAlphabet = "0123456789abcdefghjkmnpqrstvwxyz";
const demoModel = "llama-3.1-8b";
const maxDemoRequestBytes = 16 * 1024;
const initializedDatabases = new WeakMap<object, Promise<void>>();
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

const answerWords = answer.match(/\S+\s*/g) ?? [answer];
const answerChunks = Array.from(
  { length: Math.ceil(answerWords.length / 3) },
  (_, index) => answerWords.slice(index * 3, index * 3 + 3).join(""),
);

const worker: ExportedHandler<Env> = createWorker(
  (env) => {
    if (env.DB === undefined) throw new Error("D1 binding DB is unavailable");
    return new D1DemoStore(env.DB);
  },
  "shared",
);

export default worker;

export function createTestWorker(): DemoHandler {
  return createEphemeralWorker();
}

export function createEphemeralWorker(): DemoHandler {
  const store = new MemoryDemoStore();
  return createWorker(() => store, "ephemeral");
}

function createWorker(
  resolveStore: (env: Env) => DemoStore,
  storage: StorageKind,
): DemoHandler {
  return {
    async fetch(request, env, context) {
      let store: DemoStore;
      try {
        store = resolveStore(env);
        await store.initialize();
      } catch (error) {
        console.error(JSON.stringify({
          level: "error",
          event: "demo_store_unavailable",
          message: error instanceof Error ? error.message : "unknown storage error",
        }));
        return jsonResponse(
          { error: { code: "storage_unavailable", message: "shared demo state is unavailable" } },
          503,
        );
      }
      return route(request, context, store, storage);
    },
  };
}

async function route(
  request: Request,
  context: ExecutionContext,
  store: DemoStore,
  storage: StorageKind,
): Promise<Response> {
  const url = new URL(request.url);

  if (request.method === "OPTIONS") return emptyResponse(204);
  if (request.method === "GET" && url.pathname === "/api/demo/health") {
    return jsonResponse({
      status: "ready",
      storage,
      backends: [
        { id: "backend-a", version: "v1", state: "healthy" },
        { id: "backend-b", version: "v1", state: "healthy" },
        { id: "backend-c", version: "v2", state: "healthy" },
      ],
    });
  }

  if (request.method === "POST" && url.pathname === "/api/demo/inject") {
    return injectFailure(request, store);
  }

  if (request.method === "POST" && url.pathname === "/api/demo/direct") {
    const model = await readModel(request);
    if (model instanceof Response) return model;
    const session = await createSession(model, "direct", store);
    const liveStore = new LiveDemoStore(store);
    const response = liveStore.response({ "X-Demo-Stream-Id": session.id });
    context.waitUntil(produce(session, liveStore));
    return response;
  }

  if (request.method === "POST" && url.pathname === "/v1/chat/completions") {
    const model = await readModel(request);
    if (model instanceof Response) return model;
    const session = await createSession(model, "durable", store);
    const liveStore = new LiveDemoStore(store);
    const response = liveStore.response({
      "X-Streamweld-Stream-Id": session.id,
      "X-Streamweld-Durability": "durable",
    });
    context.waitUntil(produce(session, liveStore));
    return response;
  }

  const eventsMatch = /^\/v1\/streams\/([^/]+)\/events$/.exec(url.pathname);
  if (request.method === "GET" && eventsMatch?.[1] !== undefined) {
    const id = decodeURIComponent(eventsMatch[1]);
    const session = await store.get(id);
    if (session === null) return gone(id, "stream_not_found", "demo stream is not available");
    if (session.degraded) {
      return gone(id, "stream_offset_expired", "the demo journal degraded and cannot be replayed");
    }
    const cursor = parseCursor(request.headers.get("Last-Event-ID"));
    if (cursor === null) return jsonResponse({ error: { code: "invalid_last_event_id" } }, 400);
    return durableStreamResponse(session, cursor, store);
  }

  const stopMatch = /^\/v1\/streams\/([^/]+)\/stop$/.exec(url.pathname);
  if (request.method === "POST" && stopMatch?.[1] !== undefined) {
    const id = decodeURIComponent(stopMatch[1]);
    const session = await store.requestStop(id);
    if (session === null) return gone(id, "stream_not_found", "demo stream is not available");
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
}

async function createSession(
  model: string,
  mode: SessionMode,
  store: DemoStore,
): Promise<DemoSession> {
  await store.cleanup(Date.now() - 60 * 60 * 1000);
  const session: DemoSession = {
    id: createULID(),
    model,
    mode,
    sequence: 0,
    ordinal: 0,
    backend: "backend-a",
    text: "",
    completionTokens: 0,
    failure: null,
    failureHandled: false,
    stopped: false,
    terminal: false,
    degraded: false,
    createdAt: Date.now(),
  };
  await store.create(session);
  return session;
}

async function produce(session: DemoSession, store: DemoStore): Promise<void> {
  if (session.mode === "durable") {
    await append(
      session,
      "streamweld.stream.open",
      {
        stream_id: session.id,
        model: session.model,
        model_version: "v1",
        backend_id: session.backend,
      },
      store,
    );
  }

  for (const chunk of answerChunks) {
    await wait(92);
    if (!await refreshControl(session, store)) return;
    if (session.stopped) {
      await finishStopped(session, store);
      return;
    }
    if (session.failure !== null && !session.failureHandled) {
      const shouldContinue = await handleFailure(session, store);
      if (!shouldContinue) return;
    }

    session.text += chunk;
    session.completionTokens += 1;
    await appendMessage(session, chunk, store);
  }

  await wait(70);
  if (!await refreshControl(session, store)) return;
  if (session.stopped) {
    await finishStopped(session, store);
    return;
  }

  if (session.mode === "direct") {
    await appendRaw(session, "data: [DONE]\n\n", true, false, store);
  } else {
    await append(
      session,
      "streamweld.stream.done",
      { finish_reason: "stop", usage: usage(session) },
      store,
      true,
      !session.degraded,
    );
  }
}

async function refreshControl(session: DemoSession, store: DemoStore): Promise<boolean> {
  const control = await store.control(session.id);
  if (control === null) return false;
  session.failure = control.failure;
  session.failureHandled = control.failureHandled;
  session.stopped = control.stopped;
  session.degraded = control.degraded;
  if (control.terminal) {
    session.terminal = true;
    return false;
  }
  return true;
}

async function handleFailure(session: DemoSession, store: DemoStore): Promise<boolean> {
  const scenario = session.failure;
  session.failureHandled = true;
  if (scenario === null || scenario === "client-drop") return true;

  if (session.mode === "direct") {
    session.terminal = true;
    await store.markTerminal(session);
    return false;
  }

  if (scenario === "explicit-stop") {
    session.stopped = true;
    await finishStopped(session, store);
    return false;
  }

  const reason = scenario === "pod-kill" ? "crash" : "drain";
  const nextBackend = scenario === "rolling-update" ? "backend-c" : "backend-b";
  await append(session, "streamweld.stream.migration", {
    from_backend: session.backend,
    to_backend: nextBackend,
    reason,
    rescued_tokens: session.completionTokens,
    token_count_estimated: false,
    attempt: 2,
  }, store);
  session.backend = nextBackend;
  await append(session, "streamweld.stream.warning", {
    code: "seam_reconciled",
    message: "continuation matched the retained token suffix",
    predicate: null,
    details: { overlap_bytes: 18 },
  }, store);
  return true;
}

async function finishStopped(session: DemoSession, store: DemoStore): Promise<void> {
  if (session.terminal) return;
  if (session.mode === "direct") {
    session.terminal = true;
    await store.markTerminal(session);
    return;
  }
  await append(
    session,
    "streamweld.stream.stopped",
    { partial_text: session.text, usage: usage(session) },
    store,
    true,
    !session.degraded,
  );
}

async function appendMessage(
  session: DemoSession,
  content: string,
  store: DemoStore,
): Promise<void> {
  const data = {
    id: `chatcmpl-${session.id}`,
    object: "chat.completion.chunk",
    model: session.model,
    choices: [{ index: 0, delta: { content }, finish_reason: null }],
  };
  if (session.mode === "direct") {
    await appendRaw(session, `data: ${JSON.stringify(data)}\n\n`, false, false, store);
  } else {
    await append(session, "message", data, store, false, !session.degraded);
  }
}

async function append(
  session: DemoSession,
  event: string,
  data: unknown,
  store: DemoStore,
  terminal = false,
  sequenced = true,
): Promise<void> {
  const sequence = sequenced ? ++session.sequence : null;
  const idLine = sequence === null ? "" : `id: ${String(sequence)}\n`;
  const eventLine = event === "message" ? "" : `event: ${event}\n`;
  const flushLine = event === "streamweld.stream.open" ? `: ${createFlushPadding()}\n` : "";
  await appendRaw(
    session,
    `${idLine}${eventLine}${flushLine}data: ${JSON.stringify(data)}\n\n`,
    terminal,
    false,
    store,
    sequence,
  );
}

async function appendRaw(
  session: DemoSession,
  wire: string,
  terminal: boolean,
  sequenced: boolean,
  store: DemoStore,
  explicitSequence?: number | null,
): Promise<void> {
  const sequence = explicitSequence === undefined
    ? (sequenced ? ++session.sequence : null)
    : explicitSequence;
  if (terminal) session.terminal = true;
  session.ordinal += 1;
  await store.append(session, {
    ordinal: session.ordinal,
    sequence,
    wire,
    terminal,
  });
}

function durableStreamResponse(
  session: DemoSession,
  cursor: number,
  store: DemoStore,
): Response {
  return streamResponse(session.id, cursor, store, {
    "X-Streamweld-Stream-Id": session.id,
    "X-Streamweld-Durability": "durable",
  });
}

function streamResponse(
  streamID: string,
  cursor: number,
  store: DemoStore,
  headers: Record<string, string>,
): Response {
  let cancelled = false;
  let ordinal = 0;
  const body = new ReadableStream<Uint8Array>({
    start(controller) {
      void (async () => {
        try {
          for (;;) {
            if (cancelled) return;
            const batch = await store.replay(streamID, ordinal);
            if (!batch.exists) {
              controller.close();
              return;
            }

            let sawTerminal = false;
            for (const entry of batch.entries) {
              ordinal = entry.ordinal;
              if (entry.sequence === null || entry.sequence > cursor) {
                controller.enqueue(encoder.encode(entry.wire));
              }
              if (entry.terminal) sawTerminal = true;
            }
            if (sawTerminal || (batch.terminal && !batch.more)) {
              controller.close();
              return;
            }
            if (batch.entries.length === 0) await wait(50);
          }
        } catch (error) {
          if (!cancelled) controller.error(error);
        }
      })();
    },
    cancel() {
      cancelled = true;
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

async function injectFailure(request: Request, store: DemoStore): Promise<Response> {
  const parsed = await readJSONBody(request);
  if (!parsed.ok) return parsed.response;
  const payload = parsed.value;
  if (!isRecord(payload)) return jsonResponse({ error: { code: "invalid_request" } }, 400);
  const id = payload["stream_id"];
  const scenario = payload["scenario"];
  if (typeof id !== "string" || typeof scenario !== "string" || !isFailureScenario(scenario)) {
    return jsonResponse({ error: { code: "invalid_injection" } }, 400);
  }
  const update = await store.setFailure(id, scenario);
  if (update === "missing") return jsonResponse({ error: { code: "stream_not_found" } }, 404);
  if (update === "terminal") return jsonResponse({ error: { code: "stream_terminal" } }, 409);
  return jsonResponse({ accepted: true, stream_id: id, scenario });
}

class LiveDemoStore implements DemoStore {
  private controller: ReadableStreamDefaultController<Uint8Array> | null = null;
  private readonly pending: JournalEntry[] = [];
  private cancelled = false;
  private closed = false;

  constructor(private readonly delegate: DemoStore) {}

  response(headers: Record<string, string>): Response {
    const body = new ReadableStream<Uint8Array>({
      start: (controller) => {
        this.controller = controller;
        for (const entry of this.pending.splice(0)) this.push(entry);
      },
      cancel: () => {
        this.cancelled = true;
        this.controller = null;
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

  initialize(): Promise<void> {
    return this.delegate.initialize();
  }

  cleanup(before: number): Promise<void> {
    return this.delegate.cleanup(before);
  }

  create(session: DemoSession): Promise<void> {
    return this.delegate.create(session);
  }

  get(id: string): Promise<DemoSession | null> {
    return this.delegate.get(id);
  }

  control(id: string): Promise<SessionControl | null> {
    return this.delegate.control(id);
  }

  setFailure(id: string, scenario: FailureScenario): Promise<FailureUpdate> {
    return this.delegate.setFailure(id, scenario);
  }

  requestStop(id: string): Promise<DemoSession | null> {
    return this.delegate.requestStop(id);
  }

  async append(session: DemoSession, entry: JournalEntry): Promise<void> {
    await this.delegate.append(session, entry);
    this.push(entry);
  }

  async markTerminal(session: DemoSession): Promise<void> {
    await this.delegate.markTerminal(session);
    this.close();
  }

  replay(id: string, afterOrdinal: number): Promise<ReplayBatch> {
    return this.delegate.replay(id, afterOrdinal);
  }

  private push(entry: JournalEntry): void {
    if (this.cancelled || this.closed) return;
    if (this.controller === null) {
      this.pending.push(entry);
      return;
    }
    this.controller.enqueue(encoder.encode(entry.wire));
    if (entry.terminal) this.close();
  }

  private close(): void {
    if (this.cancelled || this.closed) return;
    this.closed = true;
    try {
      this.controller?.close();
    } catch {
      // A browser cancellation can race a terminal producer write.
    }
    this.controller = null;
  }
}

class D1DemoStore implements DemoStore {
  constructor(private readonly db: D1Database) {}

  async initialize(): Promise<void> {
    let ready = initializedDatabases.get(this.db as object);
    if (ready === undefined) {
      ready = this.db.batch(demoSchemaStatements.map((sql) => this.db.prepare(sql)))
        .then(() => undefined)
        .catch((error: unknown) => {
          initializedDatabases.delete(this.db as object);
          throw error;
        });
      initializedDatabases.set(this.db as object, ready);
    }
    await ready;
  }

  async cleanup(before: number): Promise<void> {
    await this.db.batch([
      this.db.prepare(
        "DELETE FROM demo_entries WHERE stream_id IN (SELECT id FROM demo_sessions WHERE created_at < ?)",
      ).bind(before),
      this.db.prepare("DELETE FROM demo_sessions WHERE created_at < ?").bind(before),
    ]);
  }

  async create(session: DemoSession): Promise<void> {
    await this.db.prepare(
      `INSERT INTO demo_sessions (
        id, model, mode, sequence, backend, text, completion_tokens, failure,
        failure_handled, stopped, terminal, degraded, created_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    ).bind(
      session.id,
      session.model,
      session.mode,
      session.sequence,
      session.backend,
      session.text,
      session.completionTokens,
      session.failure,
      booleanInteger(session.failureHandled),
      booleanInteger(session.stopped),
      booleanInteger(session.terminal),
      booleanInteger(session.degraded),
      session.createdAt,
    ).run();
  }

  async get(id: string): Promise<DemoSession | null> {
    const row = await this.db.prepare(
      `SELECT
        s.id, s.model, s.mode, s.sequence, s.backend, s.text, s.completion_tokens,
        s.failure, s.failure_handled, s.stopped, s.terminal, s.degraded, s.created_at,
        COALESCE((SELECT MAX(e.ordinal) FROM demo_entries e WHERE e.stream_id = s.id), 0) AS ordinal
      FROM demo_sessions s
      WHERE s.id = ?`,
    ).bind(id).first<SessionRow>();
    return row === null ? null : sessionFromRow(row);
  }

  async control(id: string): Promise<SessionControl | null> {
    const row = await this.db.prepare(
      "SELECT failure, failure_handled, stopped, terminal, degraded FROM demo_sessions WHERE id = ?",
    ).bind(id).first<ControlRow>();
    return row === null ? null : controlFromRow(row);
  }

  async setFailure(id: string, scenario: FailureScenario): Promise<FailureUpdate> {
    const result = await this.db.prepare(
      "UPDATE demo_sessions SET failure = ? WHERE id = ? AND terminal = 0",
    ).bind(scenario, id).run();
    if (Number(result.meta.changes) > 0) return "accepted";
    const session = await this.get(id);
    if (session === null) return "missing";
    return session.terminal ? "terminal" : "accepted";
  }

  async requestStop(id: string): Promise<DemoSession | null> {
    await this.db.prepare(
      "UPDATE demo_sessions SET stopped = 1 WHERE id = ? AND terminal = 0",
    ).bind(id).run();
    return this.get(id);
  }

  async append(session: DemoSession, entry: JournalEntry): Promise<void> {
    await this.db.batch([
      this.db.prepare(
        "INSERT INTO demo_entries (stream_id, ordinal, sequence, wire, terminal) VALUES (?, ?, ?, ?, ?)",
      ).bind(
        session.id,
        entry.ordinal,
        entry.sequence,
        entry.wire,
        booleanInteger(entry.terminal),
      ),
      this.db.prepare(
        `UPDATE demo_sessions
        SET sequence = ?, backend = ?, text = ?, completion_tokens = ?,
            failure_handled = ?, terminal = ?, degraded = ?
        WHERE id = ?`,
      ).bind(
        session.sequence,
        session.backend,
        session.text,
        session.completionTokens,
        booleanInteger(session.failureHandled),
        booleanInteger(session.terminal),
        booleanInteger(session.degraded),
        session.id,
      ),
    ]);
  }

  async markTerminal(session: DemoSession): Promise<void> {
    await this.db.prepare(
      `UPDATE demo_sessions
      SET sequence = ?, backend = ?, text = ?, completion_tokens = ?,
          failure_handled = ?, terminal = 1, degraded = ?
      WHERE id = ?`,
    ).bind(
      session.sequence,
      session.backend,
      session.text,
      session.completionTokens,
      booleanInteger(session.failureHandled),
      booleanInteger(session.degraded),
      session.id,
    ).run();
  }

  async replay(id: string, afterOrdinal: number): Promise<ReplayBatch> {
    const result = await this.db.prepare(
      `SELECT
        e.ordinal, e.sequence, e.wire, e.terminal AS entry_terminal,
        s.terminal AS session_terminal, s.degraded
      FROM demo_sessions s
      LEFT JOIN demo_entries e
        ON e.stream_id = s.id AND e.ordinal > ?
      WHERE s.id = ?
      ORDER BY e.ordinal
      LIMIT 129`,
    ).bind(afterOrdinal, id).all<ReplayRow>();
    const rows = result.results;
    if (rows.length === 0) {
      return { exists: false, terminal: false, degraded: false, entries: [], more: false };
    }
    const more = rows.length > 128;
    const entries = rows.slice(0, 128).flatMap((row): JournalEntry[] => {
      if (row.ordinal === null || row.wire === null) return [];
      return [{
        ordinal: row.ordinal,
        sequence: row.sequence,
        wire: row.wire,
        terminal: row.entry_terminal === 1,
      }];
    });
    return {
      exists: true,
      terminal: rows[0]?.session_terminal === 1,
      degraded: rows[0]?.degraded === 1,
      entries,
      more,
    };
  }
}

class MemoryDemoStore implements DemoStore {
  private readonly sessions = new Map<string, DemoSession>();
  private readonly entries = new Map<string, JournalEntry[]>();

  async initialize(): Promise<void> {}

  async cleanup(before: number): Promise<void> {
    for (const [id, session] of this.sessions) {
      if (session.createdAt < before) {
        this.sessions.delete(id);
        this.entries.delete(id);
      }
    }
  }

  async create(session: DemoSession): Promise<void> {
    this.sessions.set(session.id, cloneSession(session));
    this.entries.set(session.id, []);
  }

  async get(id: string): Promise<DemoSession | null> {
    const session = this.sessions.get(id);
    return session === undefined ? null : cloneSession(session);
  }

  async control(id: string): Promise<SessionControl | null> {
    const session = this.sessions.get(id);
    if (session === undefined) return null;
    return {
      failure: session.failure,
      failureHandled: session.failureHandled,
      stopped: session.stopped,
      terminal: session.terminal,
      degraded: session.degraded,
    };
  }

  async setFailure(id: string, scenario: FailureScenario): Promise<FailureUpdate> {
    const session = this.sessions.get(id);
    if (session === undefined) return "missing";
    if (session.terminal) return "terminal";
    session.failure = scenario;
    return "accepted";
  }

  async requestStop(id: string): Promise<DemoSession | null> {
    const session = this.sessions.get(id);
    if (session === undefined) return null;
    if (!session.terminal) session.stopped = true;
    return cloneSession(session);
  }

  async append(session: DemoSession, entry: JournalEntry): Promise<void> {
    const stored = this.sessions.get(session.id);
    const entries = this.entries.get(session.id);
    if (stored === undefined || entries === undefined) throw new Error("demo session disappeared");
    entries.push({ ...entry });
    copyProducerState(stored, session);
  }

  async markTerminal(session: DemoSession): Promise<void> {
    const stored = this.sessions.get(session.id);
    if (stored === undefined) throw new Error("demo session disappeared");
    copyProducerState(stored, session);
  }

  async replay(id: string, afterOrdinal: number): Promise<ReplayBatch> {
    const session = this.sessions.get(id);
    const allEntries = this.entries.get(id);
    if (session === undefined || allEntries === undefined) {
      return { exists: false, terminal: false, degraded: false, entries: [], more: false };
    }
    const matching = allEntries.filter((entry) => entry.ordinal > afterOrdinal);
    return {
      exists: true,
      terminal: session.terminal,
      degraded: session.degraded,
      entries: matching.slice(0, 128).map((entry) => ({ ...entry })),
      more: matching.length > 128,
    };
  }
}

async function readModel(request: Request): Promise<string | Response> {
  const parsed = await readJSONBody(request);
  if (!parsed.ok) return parsed.response;
  if (!isRecord(parsed.value)) {
    return jsonResponse({ error: { code: "invalid_request", message: "request body must be an object" } }, 400);
  }

  const model = parsed.value["model"];
  if (model === undefined) return demoModel;
  if (model !== demoModel) {
    return jsonResponse(
      { error: { code: "unsupported_model", message: `the public demo only supports ${demoModel}` } },
      400,
    );
  }
  return demoModel;
}

type JSONBodyResult =
  | { readonly ok: true; readonly value: unknown }
  | { readonly ok: false; readonly response: Response };

async function readJSONBody(request: Request): Promise<JSONBodyResult> {
  const contentLength = request.headers.get("Content-Length");
  if (contentLength !== null && Number(contentLength) > maxDemoRequestBytes) {
    return {
      ok: false,
      response: jsonResponse({ error: { code: "request_too_large" } }, 413),
    };
  }

  let text: string;
  try {
    text = await request.text();
  } catch {
    return { ok: false, response: jsonResponse({ error: { code: "invalid_body" } }, 400) };
  }
  if (encoder.encode(text).byteLength > maxDemoRequestBytes) {
    return {
      ok: false,
      response: jsonResponse({ error: { code: "request_too_large" } }, 413),
    };
  }
  if (text.trim() === "") return { ok: true, value: {} };

  try {
    return { ok: true, value: JSON.parse(text) as unknown };
  } catch {
    return { ok: false, response: jsonResponse({ error: { code: "invalid_json" } }, 400) };
  }
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

function sessionFromRow(row: SessionRow): DemoSession {
  return {
    id: row.id,
    model: row.model,
    mode: row.mode,
    sequence: Number(row.sequence),
    ordinal: Number(row.ordinal),
    backend: row.backend,
    text: row.text,
    completionTokens: Number(row.completion_tokens),
    failure: isFailureScenario(row.failure) ? row.failure : null,
    failureHandled: row.failure_handled === 1,
    stopped: row.stopped === 1,
    terminal: row.terminal === 1,
    degraded: row.degraded === 1,
    createdAt: Number(row.created_at),
  };
}

function controlFromRow(row: ControlRow): SessionControl {
  return {
    failure: isFailureScenario(row.failure) ? row.failure : null,
    failureHandled: row.failure_handled === 1,
    stopped: row.stopped === 1,
    terminal: row.terminal === 1,
    degraded: row.degraded === 1,
  };
}

function cloneSession(session: DemoSession): DemoSession {
  return { ...session };
}

function copyProducerState(target: DemoSession, source: DemoSession): void {
  target.sequence = source.sequence;
  target.ordinal = source.ordinal;
  target.backend = source.backend;
  target.text = source.text;
  target.completionTokens = source.completionTokens;
  target.failureHandled = source.failureHandled;
  target.terminal = source.terminal;
  target.degraded = source.degraded;
}

function booleanInteger(value: boolean): number {
  return value ? 1 : 0;
}

function isFailureScenario(value: unknown): value is FailureScenario {
  return typeof value === "string" && failureScenarios.has(value as FailureScenario);
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

function createFlushPadding(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(1024));
  return Array.from(bytes, (value) => value.toString(16).padStart(2, "0")).join("");
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
