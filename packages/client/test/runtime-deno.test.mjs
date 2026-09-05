import { httpCases } from "./runtime-web-cases.mjs";

async function listen(handler) {
  const controller = new AbortController();
  const server = Deno.serve({ hostname: "127.0.0.1", port: 0, signal: controller.signal, onListen() {} }, handler);
  return {
    url: `http://127.0.0.1:${server.addr.port}/v1/chat/completions`,
    async close() {
      controller.abort();
      await server.finished;
    },
  };
}

for (const { name, run } of httpCases) {
  Deno.test(name, () => run(listen));
}
