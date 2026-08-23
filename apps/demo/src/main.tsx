import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import "./styles.css";

const repository = "https://github.com/satwiksps/streamweld";
const documentation = "https://streamweld.readthedocs.io/en/latest/";

function Arrow(): React.JSX.Element {
  return <span aria-hidden="true">↗</span>;
}

function App(): React.JSX.Element {
  return (
    <div className="site-shell">
      <header className="site-header">
        <a className="brand" href="#top" aria-label="Streamweld home">
          <span className="brand-mark" aria-hidden="true">SW</span>
          <span>Streamweld</span>
        </a>
        <nav aria-label="Primary navigation">
          <a href="#architecture">Architecture</a>
          <a href={documentation}>Documentation</a>
          <a className="nav-cta" href={repository}>GitHub <Arrow /></a>
        </nav>
      </header>

      <main id="top">
        <section className="hero section-wrap">
          <div className="eyebrow"><span /> Pre-release · Apache 2.0</div>
          <h1>Durable streams for<br />self-hosted inference.</h1>
          <p className="hero-copy">
            Streamweld keeps one logical LLM response alive when a backend pod,
            node, or client connection disappears.
          </p>
          <div className="hero-actions">
            <a className="button button-primary" href={documentation}>Read the docs <Arrow /></a>
            <a className="button button-secondary" href={repository}>View on GitHub <Arrow /></a>
          </div>
          <ul className="proof-list" aria-label="Project characteristics">
            <li>OpenAI-compatible</li>
            <li>Kubernetes-native</li>
            <li>Redis-backed</li>
            <li>Client-resumable</li>
          </ul>
        </section>

        <section className="problem section-wrap" aria-labelledby="problem-title">
          <div>
            <p className="section-label">The failure between tokens</p>
            <h2 id="problem-title">A generation should outlive its connection.</h2>
          </div>
          <p>
            A pod restart or mobile network change should not discard useful output
            and start an expensive prompt again. Streamweld separates the generation,
            its producer, and every reader into independently recoverable resources.
          </p>
        </section>

        <section className="architecture section-wrap" id="architecture" aria-labelledby="architecture-title">
          <div className="section-heading">
            <p className="section-label">Architecture</p>
            <h2 id="architecture-title">A narrow layer in the request path.</h2>
            <p>Keep your inference runtime. Point OpenAI-compatible traffic at Streamweld.</p>
          </div>

          <div className="flow" role="img" aria-label="Application traffic flows through the Streamweld proxy to inference backends while events are committed to a journal and policies are managed by the Kubernetes operator">
            <div className="flow-node">
              <span>01</span>
              <strong>Application</strong>
              <small>OpenAI HTTP + SSE</small>
            </div>
            <div className="flow-connector" aria-hidden="true"><i />→</div>
            <div className="flow-node flow-node-accent">
              <span>02</span>
              <strong>Streamweld</strong>
              <small>Proxy + journal</small>
            </div>
            <div className="flow-connector" aria-hidden="true"><i />→</div>
            <div className="flow-node">
              <span>03</span>
              <strong>Inference pool</strong>
              <small>vLLM · SGLang · TGI</small>
            </div>
          </div>

          <div className="capability-grid">
            <article>
              <span className="capability-number">01</span>
              <h3>Resume readers exactly</h3>
              <p>Replay strictly after the last committed SSE event, then rejoin the live tail without a gap or duplicate.</p>
            </article>
            <article>
              <span className="capability-number">02</span>
              <h3>Migrate producers safely</h3>
              <p>Continue on another backend only when request, model, template, and seam checks prove migration is eligible.</p>
            </article>
            <article>
              <span className="capability-number">03</span>
              <h3>Stop intentionally</h3>
              <p>A dropped socket detaches a reader. An explicit stop cancels generation and records a distinct terminal state.</p>
            </article>
          </div>
        </section>

        <section className="install section-wrap" aria-labelledby="install-title">
          <div className="install-copy">
            <p className="section-label">Try the source</p>
            <h2 id="install-title">Built in public. Tested under failure.</h2>
            <p>
              Streamweld is pre-release. Clone the repository to run the deterministic
              suites today; versioned images and the Helm OCI chart will arrive with v0.1.0.
            </p>
            <a className="text-link" href={`${documentation}getting-started/`}>Open the installation guide <Arrow /></a>
          </div>
          <div className="terminal" aria-label="Source quickstart commands">
            <div className="terminal-bar"><span /><span /><span /><small>quickstart</small></div>
            <pre><code><b>$</b> git clone https://github.com/satwiksps/streamweld.git{"\n"}<b>$</b> cd streamweld{"\n"}<b>$</b> make bootstrap{"\n"}<b>$</b> make test</code></pre>
          </div>
        </section>

        <section className="closing section-wrap">
          <p className="section-label">Keep the stream</p>
          <h2>Make inference failures recoverable.</h2>
          <div className="hero-actions">
            <a className="button button-primary" href={documentation}>Get started <Arrow /></a>
            <a className="button button-secondary" href={repository}>Explore the source <Arrow /></a>
          </div>
        </section>
      </main>

      <footer className="site-footer section-wrap">
        <div className="brand"><span className="brand-mark" aria-hidden="true">SW</span><span>Streamweld</span></div>
        <p>Durable streaming infrastructure for self-hosted LLM inference.</p>
        <div><a href={documentation}>Docs</a><a href={repository}>GitHub</a><a href={`${repository}/blob/main/LICENSE`}>Apache 2.0</a></div>
      </footer>
    </div>
  );
}

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
