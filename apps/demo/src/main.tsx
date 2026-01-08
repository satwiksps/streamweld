import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import "./styles.css";

const rootElement = document.querySelector<HTMLElement>("#root");

if (rootElement === null) {
  throw new Error("Demo root element is missing");
}

createRoot(rootElement).render(
  <StrictMode>
    <main>
      <h1>Streamweld</h1>
      <p>Your token stream shouldn&rsquo;t die because a pod got evicted or a phone switched to cellular.</p>
    </main>
  </StrictMode>,
);
