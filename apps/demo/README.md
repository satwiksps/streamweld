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

## Deploy to Vercel

Import the repository with these project settings so Vercel uses the app's
checked-in `vercel.json` instead of treating the monorepo root as the site:

- Root Directory: `apps/demo`
- Framework Preset: Vite
- Include source files outside the Root Directory: enabled
- Environment variable: `STREAMWELD_DEMO_UPSTREAM_ORIGIN` set to the HTTPS
  origin of a deployed Streamweld demo Worker with shared D1 storage

The upstream must expose the `/api/demo/*` and `/v1/*` routes implemented in
`worker/index.ts`. Vercel serves the React application and same-origin proxy;
the Worker/D1 service owns durable session and journal state. Hosted Vercel
deployments intentionally return `503 upstream_required` if that shared origin
is absent rather than claiming durability from process-local memory.
