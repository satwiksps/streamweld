import { createEphemeralWorker } from "../worker/index.js";

declare const process: { readonly env: Record<string, string | undefined> };

interface VercelFunctionContext {
  waitUntil(promise: Promise<unknown>): void;
}

const worker = createEphemeralWorker();

export const maxDuration = 60;

export default {
  async fetch(request: Request, context?: VercelFunctionContext): Promise<Response> {
    const url = new URL(request.url);
    const pathname = url.searchParams.get("__streamweld_path");
    url.searchParams.delete("__streamweld_path");

    if (pathname === null) {
      return Promise.resolve(Response.json(
        { error: { code: "not_found", message: "demo route not found" } },
        { status: 404 },
      ));
    }

    url.pathname = pathname;
    if (!isDemoPath(url.pathname)) {
      return Promise.resolve(Response.json(
        { error: { code: "not_found", message: "demo route not found" } },
        { status: 404 },
      ));
    }

    let upstream: URL | null;
    try {
      upstream = upstreamURL(url);
    } catch (error) {
      console.error(JSON.stringify({
        level: "error",
        event: "demo_upstream_invalid",
        message: error instanceof Error ? error.message : "invalid upstream origin",
      }));
      return Promise.resolve(Response.json(
        { error: { code: "upstream_invalid", message: "demo upstream is misconfigured" } },
        { status: 503 },
      ));
    }

    if (upstream !== null) {
      const headers = new Headers();
      for (const [header, value] of request.headers.entries()) {
        if (shouldForwardHeader(header)) headers.set(header, value);
      }
      const body = request.method === "GET" || request.method === "HEAD"
        ? undefined
        : await request.arrayBuffer();
      // Construct an independent request instead of cloning the inbound one.
      // Vercel aborts the inbound signal after the handler resolves; inheriting
      // that signal cancels the still-streaming upstream response body.
      const upstreamRequest = body === undefined
        ? new Request(upstream, { method: request.method, headers })
        : new Request(upstream, { method: request.method, headers, body });
      const upstreamResponse = await fetch(upstreamRequest);
      // Re-wrap cross-origin fetch responses at the Vercel function boundary.
      // Returning the undici Response object directly can preserve its status
      // and headers while the platform adapter loses the body stream.
      return new Response(upstreamResponse.body, {
        status: upstreamResponse.status,
        statusText: upstreamResponse.statusText,
        headers: upstreamResponse.headers,
      });
    }

    if (process.env["VERCEL_ENV"] !== undefined) {
      return Promise.resolve(Response.json(
        {
          error: {
            code: "upstream_required",
            message: "the hosted demo requires a shared Streamweld upstream",
          },
        },
        { status: 503 },
      ));
    }

    const forwarded = new Request(url, request);
    return worker.fetch(forwarded, {}, {
      waitUntil(promise: Promise<unknown>) {
        const logged = promise.catch((error: unknown) => {
          console.error(JSON.stringify({
            level: "error",
            event: "demo_background_task_failed",
            message: error instanceof Error ? error.message : "unknown background task error",
          }));
        });
        if (context === undefined) void logged;
        else context.waitUntil(logged);
      },
      passThroughOnException() {},
      props: {},
    } as unknown as ExecutionContext);
  },
};

function isDemoPath(pathname: string): boolean {
  return pathname.startsWith("/api/demo/") || pathname.startsWith("/v1/");
}

function shouldForwardHeader(header: string): boolean {
  const normalized = header.toLowerCase();
  return normalized === "accept"
    || normalized === "content-type"
    || normalized === "last-event-id"
    || normalized.startsWith("x-streamweld-");
}

function upstreamURL(requestURL: URL): URL | null {
  const value = process.env["STREAMWELD_DEMO_UPSTREAM_ORIGIN"]?.trim();
  if (value === undefined || value === "") return null;

  const base = new URL(value);
  if (base.protocol !== "https:" && base.protocol !== "http:") {
    throw new Error("STREAMWELD_DEMO_UPSTREAM_ORIGIN must use HTTPS or HTTP");
  }
  if (base.origin === requestURL.origin) {
    throw new Error("STREAMWELD_DEMO_UPSTREAM_ORIGIN would create a proxy loop");
  }

  const upstream = new URL(requestURL.pathname, `${base.origin}/`);
  upstream.search = requestURL.search;
  return upstream;
}
