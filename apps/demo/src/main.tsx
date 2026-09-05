import { StrictMode, useEffect, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import "./styles.css";

const REPOSITORY_URL = "https://github.com/satwiksps/streamweld";
const DOCUMENTATION_URL = "https://streamweld.readthedocs.io/";
const GETTING_STARTED_URL = `${DOCUMENTATION_URL}en/latest/getting-started/`;
const ARCHITECTURE_URL = `${DOCUMENTATION_URL}en/latest/concepts/architecture/`;

const guarantees = [
  {
    id: "SW001",
    title: "Exact reader resume",
    description: "Replay strictly after the committed cursor, then rejoin the live tail.",
    property: "No gap or duplicate",
  },
  {
    id: "SW002",
    title: "Commit before visibility",
    description: "An event enters the journal before any reader can observe it.",
    property: "Replayable",
  },
  {
    id: "SW003",
    title: "Guarded migration",
    description: "Continue on a new backend only when compatibility and seam checks pass.",
    property: "Eligibility gated",
  },
  {
    id: "SW004",
    title: "Explicit stop",
    description: "A stop request cancels generation and records a distinct terminal state.",
    property: "Terminal",
  },
  {
    id: "SW005",
    title: "Reader detach",
    description: "A dropped client socket detaches one reader without cancelling the stream.",
    property: "Recoverable",
  },
] as const;

const integrations = [
  {
    name: "HTTP proxy",
    title: "Keep the OpenAI contract",
    description: "Point existing chat-completion traffic at the Streamweld proxy.",
    file: "terminal",
    command: [
      "curl http://localhost:8080/v1/chat/completions \\",
      "  -H 'Content-Type: application/json' \\",
      "  -d '{\"model\":\"llama-3.1-8b\",\"messages\":[{\"role\":\"user\",\"content\":\"Hello\"}],\"stream\":true}'",
    ].join("\n"),
  },
  {
    name: "Kubernetes",
    title: "Route and drain safely",
    description: "Install the operator and describe eligible inference backends declaratively.",
    file: "terminal",
    command: [
      "helm upgrade --install streamweld \\",
      "  oci://ghcr.io/satwiksps/charts/streamweld \\",
      "  --namespace streamweld-system --create-namespace \\",
      "  --version 1.0.0 --wait --timeout 3m",
      "kubectl -n streamweld-system get inferenceroutes",
    ].join("\n"),
  },
  {
    name: "TypeScript",
    title: "Resume from the client",
    description: "Persist the stream identity and cursor, then reconnect after transport loss.",
    file: "app.ts",
    command: [
      "import { createDurableStream, createLocalStoragePersistence } from '@streamweld/client';",
      "const stream = createDurableStream({",
      "  url: 'http://localhost:8080/v1/chat/completions',",
      "  body: { model: 'llama-3.1-8b', messages: [{ role: 'user', content: 'Hello' }], stream: true },",
      "  persist: createLocalStoragePersistence('active-stream'),",
      "});",
      "for await (const text of stream.text) render(text);",
    ].join("\n"),
  },
] as const;

const installCommands = [
  "helm upgrade --install streamweld \\",
  "  oci://ghcr.io/satwiksps/charts/streamweld \\",
  "  --namespace streamweld-system \\",
  "  --create-namespace --version 1.0.0 \\",
  "  --wait --timeout 3m",
].join("\n");

function CopyButton({ value, label }: { value: string; label: string }): React.JSX.Element {
  const [copied, setCopied] = useState(false);

  async function copy(): Promise<void> {
    try {
      await navigator.clipboard.writeText(value);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1800);
    } catch {
      setCopied(false);
    }
  }

  return (
    <button
      className="inline-flex h-7 items-center rounded border border-white/[0.08] bg-white/[0.025] px-2 font-mono text-[9px] font-medium text-zinc-400 transition-colors hover:border-white/15 hover:text-white"
      type="button"
      onClick={copy}
      aria-label={label}
    >
      <span aria-live="polite">{copied ? "Copied" : "Copy"}</span>
    </button>
  );
}

function MobileMenu(): React.JSX.Element {
  const [open, setOpen] = useState(false);
  const menuRef = useRef<HTMLDivElement>(null);
  const triggerRef = useRef<HTMLButtonElement>(null);

  useEffect(() => {
    function closeOnEscape(event: KeyboardEvent): void {
      if (event.key === "Escape" && open) {
        setOpen(false);
        triggerRef.current?.focus();
      }
    }

    function closeOutside(event: PointerEvent): void {
      if (!menuRef.current?.contains(event.target as Node)) setOpen(false);
    }

    document.addEventListener("keydown", closeOnEscape);
    document.addEventListener("pointerdown", closeOutside);
    return () => {
      document.removeEventListener("keydown", closeOnEscape);
      document.removeEventListener("pointerdown", closeOutside);
    };
  }, [open]);

  return (
    <div className="relative md:hidden" ref={menuRef}>
      <button
        ref={triggerRef}
        className="inline-flex h-11 items-center rounded-md border border-white/10 bg-white/[0.035] px-3 text-xs font-medium text-zinc-300 transition-colors hover:border-white/20 hover:bg-white/[0.07]"
        type="button"
        aria-expanded={open}
        aria-controls="mobile-navigation"
        onClick={() => setOpen((current) => !current)}
      >
        {open ? "Close" : "Menu"}
      </button>
      {open ? (
        <div className="absolute right-0 top-12 w-56 overflow-hidden rounded-lg border border-white/10 bg-[#111114] p-1.5 shadow-2xl shadow-black/50" id="mobile-navigation">
          {[
            ["Product", "#product"],
            ["Guarantees", "#guarantees"],
            ["Integrations", "#integrations"],
            ["How it works", "#workflow"],
          ].map(([label, href]) => (
            <a className="block rounded-md px-3 py-2.5 text-sm text-zinc-400 transition-colors hover:bg-white/[0.05] hover:text-white" href={href} key={href} onClick={() => setOpen(false)}>
              {label}
            </a>
          ))}
          <a className="mt-1 block border-t border-white/[0.07] px-3 py-3 text-sm font-medium text-zinc-200" href={DOCUMENTATION_URL} onClick={() => setOpen(false)}>Documentation</a>
          <a className="block px-3 py-3 text-sm font-medium text-zinc-200" href={REPOSITORY_URL} onClick={() => setOpen(false)}>Repository</a>
        </div>
      ) : null}
    </div>
  );
}

function App(): React.JSX.Element {
  const externalLinkProps = { target: "_blank" as const, rel: "noreferrer" };

  return (
    <div className="min-h-screen overflow-x-hidden bg-[#09090b] text-zinc-100">
      <a className="fixed left-4 top-4 z-[100] -translate-y-24 rounded-md bg-white px-4 py-2 text-sm font-semibold text-zinc-950 transition-transform focus:translate-y-0" href="#main">
        Skip to content
      </a>

      <header className="sticky top-0 z-50 border-b border-white/[0.07] bg-[#09090b]/90 backdrop-blur-xl">
        <nav className="mx-auto flex h-16 max-w-7xl items-center justify-between px-5 sm:px-6 lg:px-8" aria-label="Main navigation">
          <a className="flex items-center gap-2.5 font-semibold tracking-tight" href="#top">
            <span>Streamweld</span>
          </a>
          <div className="hidden items-center gap-7 text-sm text-zinc-400 md:flex">
            <a className="transition-colors hover:text-white" href="#product">Product</a>
            <a className="transition-colors hover:text-white" href="#guarantees">Guarantees</a>
            <a className="transition-colors hover:text-white" href="#integrations">Integrations</a>
            <a className="transition-colors hover:text-white" href={DOCUMENTATION_URL} {...externalLinkProps}>Docs</a>
          </div>
          <div className="flex items-center gap-2">
            <a className="hidden h-9 items-center rounded-md border border-white/10 bg-white/[0.035] px-3.5 text-sm font-medium text-zinc-200 transition-colors hover:border-white/20 hover:bg-white/[0.07] sm:inline-flex" href={REPOSITORY_URL} {...externalLinkProps}>GitHub</a>
            <MobileMenu />
          </div>
        </nav>
      </header>

      <main id="main">
        <section id="top" className="relative">
          <div className="hero-grid absolute inset-x-0 top-0 h-[720px] opacity-60" aria-hidden="true" />
          <div className="relative mx-auto max-w-7xl px-5 pb-16 pt-24 sm:px-6 sm:pt-28 lg:px-8 lg:pb-20 lg:pt-32">
            <div className="mx-auto max-w-4xl text-center">
              <p className="mb-5 font-mono text-xs font-medium uppercase tracking-[0.18em] text-blue-300">Open source, Kubernetes-native, OpenAI-compatible</p>
              <h1 className="text-balance text-5xl font-semibold tracking-[-0.045em] text-white sm:text-6xl lg:text-[72px] lg:leading-[1.04]">
                Keep one LLM response alive
                <span className="block text-zinc-400">through connection and backend failure.</span>
              </h1>
              <p className="mx-auto mt-6 max-w-2xl text-pretty text-base leading-7 text-zinc-400 sm:text-lg sm:leading-8">
                Streamweld sits between your OpenAI-compatible client and self-hosted model servers. It journals each generation so readers can reconnect and eligible backends can continue it safely.
              </p>
              <div className="mt-8 flex flex-col items-center justify-center gap-3 sm:flex-row">
                <a className="inline-flex h-11 w-full items-center justify-center rounded-md bg-white px-5 text-sm font-semibold text-zinc-950 transition-colors hover:bg-zinc-200 sm:w-auto" href="#get-started">Get started</a>
                <a className="inline-flex h-11 w-full items-center justify-center rounded-md border border-white/12 bg-white/[0.035] px-5 text-sm font-medium text-zinc-200 transition-colors hover:border-white/20 hover:bg-white/[0.07] sm:w-auto" href={REPOSITORY_URL} {...externalLinkProps}>View source</a>
              </div>
              <p className="mt-5 text-sm text-zinc-400">Self-hosted. Keep your inference runtime and OpenAI-compatible clients.</p>
            </div>

            <div id="product" className="mt-14 scroll-mt-24 lg:mt-16">
              <figure className="overflow-hidden rounded-xl border border-white/10 bg-[#0c0c0f] shadow-[0_32px_100px_rgba(0,0,0,0.55)]">
                <figcaption className="sr-only">How Streamweld journals one logical response and recovers it after client or backend failure</figcaption>
                <div className="flex h-12 items-center justify-between border-b border-white/[0.07] bg-[#111114] px-4 sm:px-5">
                  <div className="flex min-w-0 items-center gap-3">
                    <span className="grid size-6 shrink-0 place-items-center rounded border border-white/10 bg-white/[0.03] font-mono text-[9px] font-bold text-blue-300">SW</span>
                    <span className="truncate text-xs font-medium text-zinc-300">Where Streamweld fits</span>
                  </div>
                  <span className="flex shrink-0 items-center gap-2 font-mono text-[10px] text-emerald-300"><i className="size-1.5 rounded-full bg-emerald-400" aria-hidden="true" />ONE LOGICAL STREAM</span>
                </div>

                <div className="p-4 sm:p-6 lg:p-8">
                  <p className="max-w-3xl text-sm leading-6 text-zinc-400">Your application keeps the OpenAI HTTP + SSE contract. Streamweld adds the durable boundary; it is not another model server.</p>

                  <div className="mt-6 grid items-stretch gap-3 lg:grid-cols-[1fr_auto_1.25fr_auto_1fr]">
                    <div className="rounded-lg border border-white/[0.08] bg-black/20 p-4">
                      <span className="font-mono text-[9px] font-semibold uppercase tracking-[0.14em] text-zinc-400">Client</span>
                      <h3 className="mt-3 text-sm font-semibold text-zinc-100">Your application</h3>
                      <p className="mt-2 text-xs leading-5 text-zinc-400">Sends a normal streaming chat-completion request and stores the returned stream ID + cursor.</p>
                    </div>

                    <div className="hidden items-center font-mono text-lg text-blue-300 lg:flex" aria-hidden="true">→</div>

                    <div className="rounded-lg border border-blue-400/30 bg-blue-400/[0.055] p-4">
                      <span className="font-mono text-[9px] font-semibold uppercase tracking-[0.14em] text-blue-300">Durability layer</span>
                      <h3 className="mt-3 text-sm font-semibold text-zinc-100">Streamweld proxy</h3>
                      <p className="mt-2 text-xs leading-5 text-zinc-400">Assigns one logical identity, commits complete SSE events, and orchestrates backend attempts.</p>
                      <div className="mt-4 rounded-md border border-white/[0.08] bg-black/25 px-3 py-2.5 font-mono text-[10px] text-zinc-400"><span className="text-emerald-300">Journal</span><span className="mx-2 text-zinc-700">·</span>event 40 → 41 → 42</div>
                    </div>

                    <div className="hidden items-center font-mono text-lg text-blue-300 lg:flex" aria-hidden="true">→</div>

                    <div className="rounded-lg border border-white/[0.08] bg-black/20 p-4">
                      <span className="font-mono text-[9px] font-semibold uppercase tracking-[0.14em] text-zinc-400">Inference pool</span>
                      <div className="mt-3 grid gap-2">
                        <div className="flex items-center justify-between rounded border border-rose-400/15 bg-rose-400/[0.04] px-3 py-2 text-xs"><span className="text-zinc-400">Backend attempt 01</span><span className="font-mono text-rose-300">failed ×</span></div>
                        <div className="flex items-center justify-between rounded border border-emerald-400/20 bg-emerald-400/[0.05] px-3 py-2 text-xs"><span className="text-zinc-300">Compatible attempt 02</span><span className="font-mono text-emerald-300">continues ✓</span></div>
                      </div>
                    </div>
                  </div>

                  <div className="mt-4 grid overflow-hidden rounded-lg border border-white/[0.08] bg-white/[0.07] md:grid-cols-3">
                    <div className="bg-[#0a0a0d] p-4"><span className="font-mono text-[9px] text-rose-300">01 · FAILURE</span><p className="mt-2 text-xs leading-5 text-zinc-400">A reader disconnects or a backend attempt ends unexpectedly.</p></div>
                    <div className="border-y border-white/[0.08] bg-[#0a0a0d] p-4 md:border-x md:border-y-0"><span className="font-mono text-[9px] text-blue-300">02 · RECOVERY</span><p className="mt-2 text-xs leading-5 text-zinc-400">The proxy replays committed events or moves production only after every safety gate passes.</p></div>
                    <div className="bg-[#0a0a0d] p-4"><span className="font-mono text-[9px] text-emerald-300">03 · SAME RESPONSE</span><p className="mt-2 text-xs leading-5 text-zinc-400"><code className="text-zinc-300">Last-Event-ID: 41</code> starts at event 42, then rejoins the live tail.</p></div>
                  </div>
                </div>
              </figure>
            </div>

            <div className="grid gap-px border-x border-b border-white/[0.07] bg-white/[0.07] sm:grid-cols-2 lg:grid-cols-4">
              {[
                ["Compatible", "OpenAI HTTP + SSE"],
                ["Durable", "memory or Redis journal"],
                ["Recoverable", "reader + producer resume"],
                ["Kubernetes-native", "proxy + operator"],
              ].map(([label, detail]) => (
                <div className="bg-[#0b0b0e] px-5 py-4" key={label}><strong className="block text-xs font-medium text-zinc-200">{label}</strong><span className="mt-1 block font-mono text-[10px] text-zinc-400">{detail}</span></div>
              ))}
            </div>
          </div>
        </section>

        <section className="border-y border-white/[0.07] bg-white/[0.012]">
          <div className="mx-auto grid max-w-7xl gap-12 px-5 py-20 sm:px-6 lg:grid-cols-[0.9fr_1.1fr] lg:gap-24 lg:px-8 lg:py-28">
            <div><p className="font-mono text-xs font-medium uppercase tracking-[0.16em] text-blue-300">The gap in direct streaming</p><h2 className="mt-4 max-w-xl text-3xl font-semibold tracking-[-0.035em] text-white sm:text-4xl">A healthy model cannot rescue a broken response.</h2></div>
            <div className="space-y-8 text-base leading-7 text-zinc-400">
              <p>A direct SSE connection binds the generation, backend process, and reader to one fragile request. When any part disappears, useful output is usually discarded.</p>
              <dl className="divide-y divide-white/[0.07] border-y border-white/[0.07]">
                <div className="grid gap-2 py-4 sm:grid-cols-[150px_1fr]"><dt className="text-sm font-medium text-zinc-200">Direct proxy</dt><dd className="text-sm text-zinc-400">Retry the prompt and generate a different response.</dd></div>
                <div className="grid gap-2 py-4 sm:grid-cols-[150px_1fr]"><dt className="text-sm font-medium text-zinc-200">Streamweld</dt><dd className="text-sm text-zinc-400">Resume the same logical stream from its committed cursor.</dd></div>
              </dl>
              <p className="text-sm text-zinc-400">Streamweld does not replace the model server. It adds identity, journaling, and recovery around the streaming boundary.</p>
            </div>
          </div>
        </section>

        <section id="guarantees" className="scroll-mt-24">
          <div className="mx-auto max-w-7xl px-5 py-20 sm:px-6 lg:px-8 lg:py-28">
            <div className="grid gap-6 lg:grid-cols-[1fr_420px] lg:items-end">
              <div><p className="font-mono text-xs font-medium uppercase tracking-[0.16em] text-blue-300">A narrow durability contract</p><h2 className="mt-4 text-3xl font-semibold tracking-[-0.035em] text-white sm:text-4xl">Explicit guarantees at the stream boundary.</h2></div>
              <p className="text-sm leading-6 text-zinc-400">Every guarantee maps to a protocol rule that can be tested under deterministic failure.</p>
            </div>
            <div className="mt-10 overflow-hidden rounded-lg border border-white/[0.08]">
              <div className="overflow-x-auto">
                <table className="w-full min-w-[760px] border-collapse text-left">
                  <thead className="bg-white/[0.025] font-mono text-[10px] uppercase tracking-[0.12em] text-zinc-400"><tr><th className="w-28 px-5 py-3 font-medium">Rule</th><th className="w-56 px-5 py-3 font-medium">Guarantee</th><th className="px-5 py-3 font-medium">Contract</th><th className="w-44 px-5 py-3 font-medium">Property</th></tr></thead>
                  <tbody className="divide-y divide-white/[0.07]">
                    {guarantees.map((guarantee) => (
                      <tr className="bg-[#0b0b0e] transition-colors hover:bg-white/[0.025]" key={guarantee.id}><td className="px-5 py-4 font-mono text-xs font-semibold text-blue-300">{guarantee.id}</td><td className="px-5 py-4 text-sm font-medium text-zinc-200">{guarantee.title}</td><td className="px-5 py-4 text-sm text-zinc-400">{guarantee.description}</td><td className="px-5 py-4 font-mono text-[10px] font-semibold uppercase text-emerald-300">{guarantee.property}</td></tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
            <a className="mt-5 inline-flex text-sm font-medium text-zinc-400 transition-colors hover:text-white" href={`${DOCUMENTATION_URL}en/latest/protocol/resume-and-stop/`} {...externalLinkProps}>Read the protocol contract</a>
          </div>
        </section>

        <section id="integrations" className="scroll-mt-24 border-y border-white/[0.07] bg-white/[0.012]">
          <div className="mx-auto max-w-7xl px-5 py-20 sm:px-6 lg:px-8 lg:py-28">
            <div className="max-w-2xl"><p className="font-mono text-xs font-medium uppercase tracking-[0.16em] text-blue-300">One stream, three integration surfaces</p><h2 className="mt-4 text-3xl font-semibold tracking-[-0.035em] text-white sm:text-4xl">Add durability without replacing inference.</h2><p className="mt-4 text-base leading-7 text-zinc-400">The proxy owns the data path, the operator owns routing policy, and the client carries the exact resume cursor.</p></div>
            <div className="mt-10 grid overflow-hidden rounded-lg border border-white/[0.08] lg:grid-cols-3">
              {integrations.map((integration) => (
                <article className="border-b border-white/[0.08] bg-[#0b0b0e] p-5 last:border-b-0 lg:border-b-0 lg:border-r lg:last:border-r-0" key={integration.name}>
                  <span className="font-mono text-[10px] font-semibold uppercase tracking-[0.14em] text-blue-300">{integration.name}</span>
                  <h3 className="mt-3 text-base font-semibold text-zinc-100">{integration.title}</h3>
                  <p className="mt-2 min-h-12 text-sm leading-6 text-zinc-400">{integration.description}</p>
                  <div className="mt-5 overflow-hidden rounded-md border border-white/[0.07] bg-black/25"><div className="flex h-9 items-center justify-between border-b border-white/[0.06] px-3"><span className="truncate font-mono text-[10px] text-zinc-400">{integration.file}</span><CopyButton value={integration.command} label={`Copy ${integration.name} example`} /></div><pre className="min-h-32 overflow-x-auto p-3 font-mono text-[11px] leading-5 text-zinc-400"><code>{integration.command}</code></pre></div>
                </article>
              ))}
            </div>
          </div>
        </section>

        <section id="workflow" className="scroll-mt-24">
          <div className="mx-auto max-w-7xl px-5 py-20 sm:px-6 lg:px-8 lg:py-28">
            <div className="grid gap-12 lg:grid-cols-[0.82fr_1.18fr] lg:gap-24">
              <div><p className="font-mono text-xs font-medium uppercase tracking-[0.16em] text-blue-300">Small, explicit system boundary</p><h2 className="mt-4 text-3xl font-semibold tracking-[-0.035em] text-white sm:text-4xl">Commit the event. Then show it.</h2><p className="mt-5 text-base leading-7 text-zinc-400">The logical stream survives because its state is independent from any reader socket or backend attempt.</p><a className="mt-6 inline-flex text-sm font-medium text-zinc-300 transition-colors hover:text-white" href={ARCHITECTURE_URL} {...externalLinkProps}>Read the architecture</a></div>
              <div>
                <ol className="grid gap-px overflow-hidden rounded-lg border border-white/[0.08] bg-white/[0.08] sm:grid-cols-2">
                  {[
                    ["01", "Create the stream", "Assign one logical identity before choosing a backend attempt."],
                    ["02", "Journal each event", "Commit ordered SSE events before exposing them to readers."],
                    ["03", "Detach failures", "Treat reader loss and producer loss as independent recovery paths."],
                    ["04", "Resume exactly", "Replay after the cursor, then tail the same logical response live."],
                  ].map(([number, title, description]) => (
                    <li className="bg-[#0b0b0e] p-5" key={number}><span className="font-mono text-[10px] text-zinc-400">{number}</span><h3 className="mt-5 text-sm font-semibold text-zinc-200">{title}</h3><p className="mt-2 text-xs leading-5 text-zinc-400">{description}</p></li>
                  ))}
                </ol>
                <div className="mt-6 grid grid-cols-2 gap-x-6 gap-y-3 border-t border-white/[0.07] pt-6 text-xs text-zinc-400 sm:grid-cols-4">
                  {["No client-side restart", "No hidden duplicate", "No model lock-in", "No hosted control plane"].map((fact) => (<span className="flex items-center gap-2" key={fact}><i className="size-1.5 rounded-full bg-emerald-400" aria-hidden="true" />{fact}</span>))}
                </div>
              </div>
            </div>
          </div>
        </section>

        <section id="get-started" className="scroll-mt-24 border-t border-white/[0.07]">
          <div className="mx-auto max-w-7xl px-5 py-20 sm:px-6 lg:px-8 lg:py-28">
            <div className="grid gap-10 lg:grid-cols-[0.8fr_1.2fr] lg:items-center lg:gap-20">
              <div><p className="font-mono text-xs font-medium uppercase tracking-[0.16em] text-blue-300">Install v1.0.0</p><h2 className="mt-4 text-3xl font-semibold tracking-[-0.035em] text-white sm:text-4xl">Add the durable boundary to Kubernetes.</h2><p className="mt-4 text-sm leading-6 text-zinc-400">Install the versioned proxy and operator, then follow the ten-minute walkthrough with the CPU-only sample backend.</p></div>
              <div className="overflow-hidden rounded-lg border border-white/[0.09] bg-[#0b0b0e]"><div className="flex h-11 items-center justify-between border-b border-white/[0.07] px-4"><span className="font-mono text-[10px] text-zinc-400">terminal</span><CopyButton value={installCommands} label="Copy Helm install command" /></div><pre className="overflow-x-auto p-4 font-mono text-[11px] leading-6 text-zinc-300 sm:p-5 sm:text-xs"><code>{installCommands}</code></pre></div>
            </div>
            <div className="mt-14 flex flex-col gap-4 border-t border-white/[0.07] pt-8 sm:flex-row sm:items-center sm:justify-between"><p className="text-sm text-zinc-400">Apache-2.0, Go + TypeScript, v1.0.0</p><a className="inline-flex h-10 items-center justify-center rounded-md bg-white px-4 text-sm font-semibold text-zinc-950 transition-colors hover:bg-zinc-200" href={GETTING_STARTED_URL} {...externalLinkProps}>Open installation guide</a></div>
          </div>
        </section>
      </main>

      <footer className="border-t border-white/[0.07] bg-[#070708]">
        <div className="mx-auto flex max-w-7xl flex-col gap-8 px-5 py-10 sm:px-6 md:flex-row md:items-end md:justify-between lg:px-8">
          <div><a className="inline-flex items-center gap-2.5 font-semibold tracking-tight" href="#top"><span className="grid size-7 place-items-center rounded-md border border-white/15 bg-white/[0.04] font-mono text-[10px] font-bold text-blue-300" aria-hidden="true">SW</span>Streamweld</a><p className="mt-3 max-w-sm text-xs leading-5 text-zinc-400">Durable token streams for self-hosted LLM inference.</p></div>
          <div className="flex flex-wrap gap-x-6 gap-y-3 text-xs text-zinc-400"><a className="hover:text-zinc-200" href={DOCUMENTATION_URL} {...externalLinkProps}>Docs</a><a className="hover:text-zinc-200" href={`${DOCUMENTATION_URL}en/latest/protocol/resume-and-stop/`} {...externalLinkProps}>Protocol</a><a className="hover:text-zinc-200" href={`${REPOSITORY_URL}/blob/main/SECURITY.md`} {...externalLinkProps}>Security</a><a className="hover:text-zinc-200" href={`${REPOSITORY_URL}/blob/main/CONTRIBUTING.md`} {...externalLinkProps}>Contributing</a><a className="hover:text-zinc-200" href={REPOSITORY_URL} {...externalLinkProps}>GitHub</a></div>
        </div>
        <div className="mx-auto flex max-w-7xl flex-col gap-1 border-t border-white/[0.05] px-5 py-5 font-mono text-[11px] text-zinc-400 sm:flex-row sm:items-center sm:justify-between sm:px-6 lg:px-8"><span>Apache License 2.0</span><span>v1.0.0 · self-hosted · open source</span></div>
      </footer>

      <script type="application/ld+json" dangerouslySetInnerHTML={{ __html: JSON.stringify({ "@context": "https://schema.org", "@type": "SoftwareApplication", name: "Streamweld", applicationCategory: "DeveloperApplication", operatingSystem: "Kubernetes", url: "https://streamweld.vercel.app/", license: "https://www.apache.org/licenses/LICENSE-2.0", codeRepository: REPOSITORY_URL, description: "Durable token streams for self-hosted LLM inference." }).replace(/</g, "\\u003c") }} />
    </div>
  );
}

createRoot(document.getElementById("root")!).render(<StrictMode><App /></StrictMode>);
