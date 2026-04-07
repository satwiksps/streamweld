# Streamweld failure lab

This Vite, React, TypeScript, and Tailwind application is a deterministic,
GPU-free demonstration of the Streamweld client protocol. The same-origin
Worker exposes a tiny OpenAI-shaped streaming backend plus five failure controls.

- Streamweld on uses `@streamweld/client`, exact SSE cursors, migration markers,
  replay after a local disconnect, and the explicit stop endpoint.
- Streamweld off uses a raw direct stream. Backend failure or client disconnect
  closes the response without a replay cursor, so the visible answer truncates.

From the repository root, run `make demo` or `pnpm --filter @streamweld/demo dev`.
No API key, model download, GPU, or external service is required.
