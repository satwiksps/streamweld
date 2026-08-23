import { createEphemeralWorker } from "../worker/index.js";

declare const process: { readonly env: Record<string, string | undefined> };

interface VercelFunctionContext {
  waitUntil(promise: Promise<unknown>): void;
}

const worker = createEphemeralWorker();

export const maxDuration = 60;

export default {
  fetch(request: Request, context?: VercelFunctionContext): Promise<Response> {
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
      const upstreamRequest = new Request(upstream, request);
      for (const header of Array.from(upstreamRequest.headers.keys())) {
        if (!shouldForwardHeader(header)) upstreamRequest.headers.delete(header);
      }
      return fetch(upstreamRequest);
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
