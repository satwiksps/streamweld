import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import { createContext, Script } from "node:vm";
import test from "node:test";

// Resolve esbuild through the already-declared tsup build dependency.
const require = createRequire(import.meta.url);
const { build } = createRequire(require.resolve("tsup"))("esbuild");
const entry = fileURLToPath(new URL("../dist/index.js", import.meta.url));

function bundleBrowser(input) {
  return build({ ...input, bundle: true, platform: "browser", format: "iife", globalName: "StreamweldClient",
    target: "es2022", treeShaking: false, write: false, metafile: true, logLevel: "silent" });
}

test("built ESM bundles and imports with only Web API globals", async () => {
  const bundled = await bundleBrowser({ entryPoints: [entry] });
  for (const output of Object.values(bundled.metafile.outputs)) {
    assert.deepEqual(output.imports, [], "browser bundle must not retain external imports");
  }
  const context = createContext({
    AbortController, AbortSignal, DOMException, Headers, Request, Response, ReadableStream,
    TextEncoder, TextDecoder, URL, URLSearchParams, crypto: globalThis.crypto,
    fetch: globalThis.fetch, setTimeout, clearTimeout, queueMicrotask,
  });
  new Script(bundled.outputFiles[0].text).runInContext(context, { timeout: 1_000 });
  assert.equal(typeof context.StreamweldClient.createDurableStream, "function");
  assert.equal(typeof context.StreamweldClient.createLocalStoragePersistence, "function");
});

test("browser import guard rejects Node builtin dependencies", async () => {
  await assert.rejects(bundleBrowser({ stdin: {
    contents: 'import { readFileSync } from "node:fs"; globalThis.readFile = readFileSync;',
  } }), /Could not resolve "node:fs"/);
});
