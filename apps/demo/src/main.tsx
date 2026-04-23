import { StrictMode, useEffect, useRef, useState, type FormEvent } from "react";
import { createRoot } from "react-dom/client";
import {
  createDurableStream,
  IncrementalSSEParser,
  LocalAbortError,
  type DurableStream,
  type StreamEvent,
} from "@streamweld/client";
import "./styles.css";

type FailureScenario =
  | "pod-kill"
  | "rolling-update"
  | "spot-reclaim"
  | "client-drop"
  | "explicit-stop";

type RunStatus =
  | "idle"
  | "connecting"
  | "streaming"
  | "migrating"
  | "resuming"
  | "stopping"
  | "done"
  | "stopped"
  | "truncated"
  | "error";

interface TimelineItem {
  readonly key: string;
  readonly sequence: string;
  readonly kind: "open" | "chunk" | "fault" | "migration" | "resume" | "seam" | "terminal";
  readonly title: string;
  readonly detail: string;
  readonly tone: "neutral" | "mint" | "amber" | "red" | "blue";
}

interface DemoHealth {
  readonly backends: ReadonlyArray<{ readonly id: string; readonly state: string; readonly version: string }>;
}

const model = "llama-3.1-8b";
const defaultPrompt = "Describe a zero-downtime model rollout in three steps.";
const terminalStates = new Set<RunStatus>(["idle", "done", "stopped", "truncated", "error"]);
const controls: ReadonlyArray<{
  readonly scenario: FailureScenario;
  readonly label: string;
  readonly meta: string;
  readonly tone: string;
  readonly glyph: string;
}> = [
  { scenario: "pod-kill", label: "Kill serving pod", meta: "SIGKILL", tone: "danger", glyph: "×" },
  { scenario: "rolling-update", label: "Rolling update", meta: "v2 model", tone: "warm", glyph: "↻" },
  { scenario: "spot-reclaim", label: "Spot reclaim", meta: "cordon + drain", tone: "warm", glyph: "↘" },
  { scenario: "client-drop", label: "Drop client", meta: "TCP close", tone: "cool", glyph: "⌁" },
  { scenario: "explicit-stop", label: "Press stop", meta: "explicit cancel", tone: "neutral", glyph: "■" },
];

function App() {
  const [durableMode, setDurableMode] = useState(true);
  const [prompt, setPrompt] = useState(defaultPrompt);
  const [submittedPrompt, setSubmittedPrompt] = useState(defaultPrompt);
  const [answer, setAnswer] = useState("");
  const [status, setStatus] = useState<RunStatus>("idle");
  const [streamId, setStreamId] = useState<string | null>(null);
  const [lastSequence, setLastSequence] = useState("0");
  const [timeline, setTimeline] = useState<TimelineItem[]>([]);
  const [backendCount, setBackendCount] = useState(3);
  const [rescuedTokens, setRescuedTokens] = useState(0);
  const [seamBytes, setSeamBytes] = useState(0);
  const [resumeCount, setResumeCount] = useState(0);
  const [chunkCount, setChunkCount] = useState(0);
  const durableRef = useRef<DurableStream | null>(null);
  const abortRef = useRef<AbortController | null>(null);
  const streamIdRef = useRef<string | null>(null);
  const lastSequenceRef = useRef("0");
  const plannedDropRef = useRef(false);
  const directActionRef = useRef<"drop" | "stop" | null>(null);
  const runRef = useRef(0);

  useEffect(() => {
    void fetch("/api/demo/health")
      .then(async (response) => response.ok ? response.json() as Promise<DemoHealth> : Promise.reject(new Error("health")))
      .then((health) => setBackendCount(health.backends.filter((backend) => backend.state === "healthy").length))
      .catch(() => setBackendCount(3));
  }, []);

  const isRunning = !terminalStates.has(status);
  const canInject = isRunning && streamId !== null && status !== "stopping";

  function addTimeline(item: Omit<TimelineItem, "key">): void {
    setTimeline((current) => [
      ...current,
      { ...item, key: `${performance.now().toFixed(3)}-${current.length}` },
    ].slice(-80));
  }

  function resetRun(): number {
    abortRef.current?.abort();
    const run = runRef.current + 1;
    runRef.current = run;
    durableRef.current = null;
    abortRef.current = null;
    streamIdRef.current = null;
    lastSequenceRef.current = "0";
    plannedDropRef.current = false;
    directActionRef.current = null;
    setAnswer("");
    setStreamId(null);
    setLastSequence("0");
    setTimeline([]);
    setRescuedTokens(0);
    setSeamBytes(0);
    setResumeCount(0);
    setChunkCount(0);
    return run;
  }

  async function start(event: FormEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault();
    if (isRunning || prompt.trim() === "") return;
    const run = resetRun();
    const nextPrompt = prompt.trim();
    setSubmittedPrompt(nextPrompt);
    setStatus("connecting");
    if (durableMode) await startDurable(run, nextPrompt);
    else await startDirect(run, nextPrompt);
  }

  async function startDurable(run: number, nextPrompt: string): Promise<void> {
    const controller = new AbortController();
    abortRef.current = controller;
    const stream = createDurableStream({
      url: new URL("/v1/chat/completions", window.location.href),
      body: {
        model,
        stream: true,
        messages: [{ role: "user", content: nextPrompt }],
      },
      signal: controller.signal,
      resume: { maxAttempts: 5, backoff: { initialMs: 150, maxMs: 800, jitter: false } },
    });
    durableRef.current = stream;
    void stream.result.catch(() => undefined);
    void stream.idReady.then((id) => {
      if (runRef.current !== run) return;
      streamIdRef.current = id;
      setStreamId(id);
    }).catch(() => undefined);
    await consumeDurable(stream, run, false);
  }

  async function resumeDurable(run: number): Promise<void> {
    const id = streamIdRef.current;
    if (id === null || runRef.current !== run) return;
    setStatus("resuming");
    addTimeline({
      sequence: lastSequenceRef.current,
      kind: "resume",
      title: "Reader resumed",
      detail: `Last-Event-ID ${lastSequenceRef.current}`,
      tone: "blue",
    });
    setResumeCount((value) => value + 1);
    await wait(520);
    if (runRef.current !== run) return;
    const controller = new AbortController();
    abortRef.current = controller;
    const stream = createDurableStream({
      url: new URL("/v1/chat/completions", window.location.href),
      resumeFrom: { id, lastEventId: lastSequenceRef.current },
      signal: controller.signal,
      resume: { maxAttempts: 5, backoff: { initialMs: 150, maxMs: 800, jitter: false } },
    });
    durableRef.current = stream;
    void stream.result.catch(() => undefined);
    await consumeDurable(stream, run, true);
  }

  async function consumeDurable(stream: DurableStream, run: number, resumed: boolean): Promise<void> {
    try {
      for await (const event of stream.events) {
        if (runRef.current !== run) return;
        handleDurableEvent(event, resumed);
      }
    } catch (error) {
      if (runRef.current !== run) return;
      if (error instanceof LocalAbortError && plannedDropRef.current) {
        plannedDropRef.current = false;
        await resumeDurable(run);
        return;
      }
      if (error instanceof LocalAbortError) return;
      setStatus("error");
      addTimeline({
        sequence: lastSequenceRef.current,
        kind: "terminal",
        title: "Reader error",
        detail: error instanceof Error ? error.message : "unknown transport error",
        tone: "red",
      });
    }
  }

  function handleDurableEvent(event: StreamEvent, resumed: boolean): void {
    if (event.seq !== null) {
      lastSequenceRef.current = event.seq;
      setLastSequence(event.seq);
    }
    switch (event.type) {
      case "open":
        setStatus("streaming");
        addTimeline({ sequence: event.seq, kind: "open", title: "Journal opened", detail: event.backendId, tone: "mint" });
        break;
      case "chunk": {
        const delta = textDelta(event.data);
        if (delta !== null) setAnswer((current) => current + delta);
        setChunkCount((value) => value + 1);
        setStatus("streaming");
        addTimeline({
          sequence: event.seq ?? "live",
          kind: "chunk",
          title: resumed ? "Replayed chunk" : "Chunk arrived",
          detail: delta === null ? "structured payload" : `“${delta.trim() || "space"}”`,
          tone: "neutral",
        });
        break;
      }
      case "migration":
        setStatus("migrating");
        setRescuedTokens((value) => value + event.rescuedTokens);
        addTimeline({
          sequence: event.seq ?? "live",
          kind: "migration",
          title: `${event.fromBackend} → ${event.toBackend}`,
          detail: `${event.reason} · ${event.rescuedTokens} tokens rescued`,
          tone: "amber",
        });
        break;
      case "warning": {
        const overlap = overlapBytes(event.details);
        if (overlap !== null) setSeamBytes(overlap);
        addTimeline({
          sequence: event.seq ?? "live",
          kind: "seam",
          title: overlap === null ? "Journal warning" : "Seam reconciled",
          detail: overlap === null ? event.message : `${overlap} overlapping bytes removed`,
          tone: "blue",
        });
        break;
      }
      case "done":
        setStatus("done");
        addTimeline({ sequence: event.seq ?? "live", kind: "terminal", title: "Generation complete", detail: "finish reason: stop", tone: "mint" });
        break;
      case "stopped":
        setStatus("stopped");
        addTimeline({ sequence: event.seq ?? "live", kind: "terminal", title: "Explicitly stopped", detail: `${event.usage.completionTokens} tokens retained`, tone: "red" });
        break;
      case "error":
        setStatus("error");
        addTimeline({ sequence: event.seq ?? "live", kind: "terminal", title: event.code, detail: event.message, tone: "red" });
        break;
    }
  }

  async function startDirect(run: number, nextPrompt: string): Promise<void> {
    const controller = new AbortController();
    abortRef.current = controller;
    try {
      const response = await fetch("/api/demo/direct", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ model, stream: true, messages: [{ role: "user", content: nextPrompt }] }),
        signal: controller.signal,
      });
      if (!response.ok || response.body === null) throw new Error(`direct backend returned ${response.status}`);
      const id = response.headers.get("X-Demo-Stream-Id");
      if (id === null) throw new Error("direct backend omitted its demo stream id");
      if (runRef.current !== run) return;
      streamIdRef.current = id;
      setStreamId(id);
      setStatus("streaming");
      addTimeline({ sequence: "—", kind: "open", title: "Direct socket opened", detail: "backend-a · no journal", tone: "neutral" });

      const reader = response.body.getReader();
      const parser = new IncrementalSSEParser();
      let sawDone = false;
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        for (const frame of parser.push(value)) {
          if (frame.data === "[DONE]") {
            sawDone = true;
            continue;
          }
          const delta = directDelta(frame.data);
          if (delta === null) continue;
          setAnswer((current) => current + delta);
          setChunkCount((value) => value + 1);
          addTimeline({ sequence: "—", kind: "chunk", title: "Unjournaled chunk", detail: `“${delta.trim() || "space"}”`, tone: "neutral" });
        }
      }
      for (const frame of parser.finish()) if (frame.data === "[DONE]") sawDone = true;
      if (runRef.current !== run) return;
      if (sawDone) {
        setStatus("done");
        addTimeline({ sequence: "—", kind: "terminal", title: "Generation complete", detail: "direct connection stayed open", tone: "mint" });
      } else {
        setStatus("truncated");
        addTimeline({ sequence: "—", kind: "terminal", title: "Connection lost", detail: "no cursor · response truncated", tone: "red" });
      }
    } catch (error) {
      if (runRef.current !== run) return;
      if (error instanceof DOMException && error.name === "AbortError") {
        const action = directActionRef.current;
        directActionRef.current = null;
        if (action === "drop") {
          setStatus("truncated");
          addTimeline({ sequence: "—", kind: "terminal", title: "Client disconnected", detail: "no resume cursor · response truncated", tone: "red" });
        } else if (action === "stop") {
          setStatus("stopped");
          addTimeline({ sequence: "—", kind: "terminal", title: "Explicitly stopped", detail: "local cancellation sent", tone: "red" });
        }
        return;
      }
      setStatus("error");
      addTimeline({ sequence: "—", kind: "terminal", title: "Direct backend error", detail: error instanceof Error ? error.message : "unknown error", tone: "red" });
    }
  }

  async function inject(scenario: FailureScenario): Promise<void> {
    const id = streamIdRef.current;
    if (id === null || !canInject) return;
    const label = controls.find((control) => control.scenario === scenario)?.label ?? scenario;
    addTimeline({
      sequence: durableMode ? lastSequenceRef.current : "—",
      kind: "fault",
      title: label,
      detail: durableMode ? "injection accepted by demo backend" : "direct backend connection at risk",
      tone: scenario === "client-drop" ? "blue" : "red",
    });

    if (scenario === "explicit-stop") {
      setStatus("stopping");
      if (durableMode) {
        try {
          await durableRef.current?.stop();
        } catch (error) {
          setStatus("error");
          addTimeline({ sequence: lastSequenceRef.current, kind: "terminal", title: "Stop failed", detail: error instanceof Error ? error.message : "unknown error", tone: "red" });
        }
      } else {
        directActionRef.current = "stop";
        await postInjection(id, scenario).catch(() => undefined);
        abortRef.current?.abort();
      }
      return;
    }

    await postInjection(id, scenario);
    if (scenario === "client-drop") {
      if (durableMode) {
        plannedDropRef.current = true;
        abortRef.current?.abort();
      } else {
        directActionRef.current = "drop";
        abortRef.current?.abort();
      }
    } else if (durableMode) {
      setStatus("migrating");
    }
  }

  return (
    <main className="min-h-screen px-4 py-5 sm:px-6 lg:px-8">
      <div className="mx-auto max-w-[1500px]">
        <header className="mb-5 flex flex-wrap items-center justify-between gap-4 border-b border-white/10 pb-5">
          <div className="flex items-center gap-3">
            <span className="brand-mark" aria-hidden="true">SW</span>
            <div>
              <p className="eyebrow">Failure laboratory</p>
              <h1 className="text-xl font-semibold tracking-[-0.03em] text-white">Break the backend. Keep the answer.</h1>
            </div>
          </div>
          <div className="flex items-center gap-3">
            <span className="status-pill"><span className="status-dot" /> {backendCount} backends healthy</span>
            <label className={`mode-toggle ${isRunning ? "opacity-60" : ""}`}>
              <span>Streamweld {durableMode ? "on" : "off"}</span>
              <input
                checked={durableMode}
                disabled={isRunning}
                type="checkbox"
                aria-label="Toggle Streamweld durability"
                onChange={(event) => setDurableMode(event.target.checked)}
              />
              <span aria-hidden="true" className="toggle-track"><span /></span>
            </label>
          </div>
        </header>

        <section className="grid gap-4 xl:grid-cols-[minmax(0,1.35fr)_minmax(360px,.65fr)]">
          <article className="panel min-h-[590px]">
            <div className="panel-heading">
              <div><p className="eyebrow">Live generation</p><h2>Ask the model</h2></div>
              <span className="mono max-w-[52%] truncate text-xs text-slate-400">{model} · {shortId(streamId)}</span>
            </div>

            <div className="chat-space" aria-live="polite">
              {status === "idle" ? (
                <div className="empty-state">
                  <span className="empty-orbit" aria-hidden="true"><i /></span>
                  <strong>Start a stream, then break it.</strong>
                  <p>Watch the same fault survive with an exact journal cursor—or truncate with Streamweld off.</p>
                </div>
              ) : (
                <>
                  <div className="message message-user">{submittedPrompt}</div>
                  <div className="message message-assistant">
                    <span className="message-label">Assistant · {statusLabel(status)}</span>
                    {answer || "Connecting to the serving backend…"}
                    {isRunning && <span className="typing-caret" aria-hidden="true" />}
                  </div>
                  {status === "truncated" && (
                    <div className="outcome-banner failed"><strong>Response truncated.</strong> The direct connection had no journal cursor to resume.</div>
                  )}
                  {status === "done" && rescuedTokens > 0 && (
                    <div className="outcome-banner survived"><strong>Failure survived.</strong> The reader received one continuous answer after migration.</div>
                  )}
                </>
              )}
            </div>

            <form className="prompt-box" onSubmit={(event) => void start(event)}>
              <label htmlFor="prompt" className="sr-only">Message</label>
              <textarea
                id="prompt"
                rows={2}
                value={prompt}
                disabled={isRunning}
                onChange={(event) => setPrompt(event.target.value)}
              />
              <button type="submit" disabled={isRunning || prompt.trim() === ""}>
                {isRunning ? statusLabel(status) : "Run stream"} <span aria-hidden="true">↗</span>
              </button>
            </form>
          </article>

          <aside className="panel">
            <div className="panel-heading">
              <div><p className="eyebrow">Failure injection</p><h2>Break something</h2></div>
              <span className={canInject ? "armed-badge" : "armed-badge disarmed"}>{canInject ? "Armed" : "Waiting"}</span>
            </div>
            <p className="mb-4 text-sm leading-6 text-slate-400">Inject a lifecycle failure while the answer streams. Every control calls the demo backend; durability decides what the reader sees.</p>
            <div className="failure-grid">
              {controls.map((control) => (
                <button
                  key={control.scenario}
                  type="button"
                  className={`failure-button ${control.tone}`}
                  disabled={!canInject}
                  onClick={() => void inject(control.scenario)}
                >
                  <span className="failure-label"><i aria-hidden="true">{control.glyph}</i>{control.label}</span>
                  <small>{control.meta}</small>
                </button>
              ))}
            </div>
            <div className="signal-card" role="status" aria-live="polite">
              <span className={`signal-pulse status-${status}`} />
              <div><strong>{statusSummary(status, durableMode)}</strong><small>{chunkCount} chunks · cursor {durableMode ? lastSequence.padStart(4, "0") : "none"}</small></div>
              <span className={`mono text-xs ${status === "truncated" || status === "error" ? "text-red-300" : "text-emerald-300"}`}>
                {durableMode ? `${rescuedTokens} rescued` : "direct"}
              </span>
            </div>
            <div className="stat-grid">
              <div><span>Resumes</span><strong>{resumeCount}</strong></div>
              <div><span>Seam overlap</span><strong>{seamBytes} B</strong></div>
              <div><span>Lost chunks</span><strong className={status === "truncated" ? "text-red-300" : "text-emerald-300"}>{status === "truncated" ? "unknown" : "0"}</strong></div>
            </div>
          </aside>
        </section>

        <section className="timeline-shell mt-4">
          <div className="panel-heading">
            <div><p className="eyebrow">Live stream timeline</p><h2>What the client actually saw</h2></div>
            <span className="mono text-xs text-slate-500">{durableMode ? "sequenced journal" : "raw socket"}</span>
          </div>
          {timeline.length === 0 ? (
            <p className="timeline-empty">Timeline events appear here with their real cursor, backend, migration, seam, and resume details.</p>
          ) : (
            <div className="timeline-scroll">
              <ol className="timeline-events">
                {timeline.map((item) => (
                  <li key={item.key} className={`timeline-card tone-${item.tone}`}>
                    <span className="timeline-sequence">{item.sequence}</span>
                    <i aria-hidden="true" />
                    <strong>{item.title}</strong>
                    <small>{item.detail}</small>
                  </li>
                ))}
              </ol>
            </div>
          )}
        </section>
        <p className="demo-note">Deterministic demo backend · no GPU or API key required · toggle off to expose the failure</p>
      </div>
    </main>
  );
}

function textDelta(data: unknown): string | null {
  if (!isRecord(data) || !Array.isArray(data["choices"])) return null;
  const choice = data["choices"].find((candidate) => isRecord(candidate) && candidate["index"] === 0);
  if (!isRecord(choice) || !isRecord(choice["delta"])) return null;
  const content = choice["delta"]["content"];
  return typeof content === "string" ? content : null;
}

function directDelta(data: string): string | null {
  try {
    return textDelta(JSON.parse(data) as unknown);
  } catch {
    return null;
  }
}

function overlapBytes(details: unknown): number | null {
  if (!isRecord(details)) return null;
  const value = details["overlap_bytes"];
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0 ? value : null;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function shortId(id: string | null): string {
  if (id === null) return "new stream";
  return id.length <= 18 ? id : `${id.slice(0, 12)}…${id.slice(-5)}`;
}

function statusLabel(status: RunStatus): string {
  const labels: Record<RunStatus, string> = {
    idle: "Ready",
    connecting: "Connecting",
    streaming: "Streaming",
    migrating: "Migrating",
    resuming: "Resuming",
    stopping: "Stopping",
    done: "Complete",
    stopped: "Stopped",
    truncated: "Truncated",
    error: "Error",
  };
  return labels[status];
}

function statusSummary(status: RunStatus, durableMode: boolean): string {
  if (status === "idle") return durableMode ? "Durability ready" : "Direct mode ready";
  if (status === "truncated") return "Stream interrupted";
  if (status === "done") return "Generation complete";
  if (status === "stopped") return "Generation stopped";
  if (status === "error") return "Transport error";
  if (status === "migrating") return "Moving generation";
  if (status === "resuming") return "Replaying journal";
  return durableMode ? "Stream intact" : "Socket connected";
}

async function postInjection(streamId: string, scenario: FailureScenario): Promise<void> {
  const response = await fetch("/api/demo/inject", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ stream_id: streamId, scenario }),
  });
  if (!response.ok) throw new Error(`injection rejected with ${response.status}`);
}

function wait(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

const rootElement = document.querySelector<HTMLElement>("#root");
if (rootElement === null) throw new Error("Demo root element is missing");
createRoot(rootElement).render(<StrictMode><App /></StrictMode>);
