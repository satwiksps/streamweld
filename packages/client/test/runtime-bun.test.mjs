import { test } from "bun:test";
import { httpCases } from "./runtime-web-cases.mjs";

async function listen(handler) {
  const server = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: handler });
  return {
    url: `http://127.0.0.1:${server.port}/v1/chat/completions`,
    async close() { await server.stop(true); },
  };
}

for (const { name, run } of httpCases) {
  test(name, () => run(listen), 10_000);
}
