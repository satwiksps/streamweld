import assert from "node:assert/strict";

const baseURL = (
  process.argv.slice(2).find((argument) => /^https?:\/\//.test(argument)) ??
  "http://127.0.0.1:5173"
).replace(/\/$/, "");
const startedAt = performance.now();
const model = "llama-3.1-8b";
const expectedAnswer =
  "Durable streaming separates generation from a single connection. " +
  "Each chunk is journaled with an exact sequence number. " +
  "When backend-a disappears, Streamweld opens a continuation on backend-c, " +
  "reconciles the seam, and the reader keeps receiving one coherent answer " +
  "without duplicated or missing text.";

const health = await request("/api/demo/health");
assert.equal(health.status, 200);
assert.equal((await health.json()).storage, "shared");

const migrated = await start("/v1/chat/completions");
const migratedID = requiredHeader(migrated, "X-Streamweld-Stream-Id");
const injection = await request("/api/demo/inject", {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({ stream_id: migratedID, scenario: "pod-kill" }),
});
assert.equal(injection.status, 200);
const migratedBody = await migrated.text();
assert.match(migratedBody, /event: streamweld\.stream\.migration/);
assert.match(migratedBody, /"code":"seam_reconciled"/);
assert.match(migratedBody, /event: streamweld\.stream\.done/);
assert.equal(messageText(parseRecords(migratedBody)), expectedAnswer);

const interrupted = await start("/v1/chat/completions");
const interruptedID = requiredHeader(interrupted, "X-Streamweld-Stream-Id");
const prefix = await readPrefix(interrupted, 4);
assert.ok(prefix.cursor >= 4);
const resumed = await request(`/v1/streams/${interruptedID}/events`, {
  headers: { "Last-Event-ID": String(prefix.cursor) },
});
assert.equal(resumed.status, 200);
const resumedRecords = parseRecords(await resumed.text());
assert.ok(resumedRecords.every((record) => record.id === null || record.id > prefix.cursor));
assert.equal(prefix.text + messageText(resumedRecords), expectedAnswer);
assert.equal(resumedRecords.at(-1)?.event, "streamweld.stream.done");

const stopped = await start("/v1/chat/completions");
const stoppedID = requiredHeader(stopped, "X-Streamweld-Stream-Id");
const stop = await request(`/v1/streams/${stoppedID}/stop`, { method: "POST" });
assert.equal(stop.status, 202);
assert.equal((await stop.json()).outcome, "stopped");
const stoppedBody = await stopped.text();
assert.match(stoppedBody, /event: streamweld\.stream\.stopped/);
assert.doesNotMatch(stoppedBody, /event: streamweld\.stream\.done/);

const direct = await start("/api/demo/direct");
const directID = requiredHeader(direct, "X-Demo-Stream-Id");
const directInjection = await request("/api/demo/inject", {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({ stream_id: directID, scenario: "pod-kill" }),
});
assert.equal(directInjection.status, 200);
const directBody = await direct.text();
assert.doesNotMatch(directBody, /data: \[DONE\]/);
assert.doesNotMatch(directBody, /streamweld\.stream\.migration/);

console.log(JSON.stringify({
  base_url: baseURL,
  shared_storage: true,
  migration: "passed",
  exact_resume: "passed",
  explicit_stop: "passed",
  direct_truncation: "passed",
  elapsed_ms: Math.round(performance.now() - startedAt),
}));

function start(path) {
  return request(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ model, stream: true }),
  });
}

function request(path, init) {
  return fetch(`${baseURL}${path}`, {
    ...init,
    signal: AbortSignal.timeout(60_000),
  });
}

function requiredHeader(response, name) {
  assert.equal(response.status, 200);
  const value = response.headers.get(name);
  assert.ok(value, `missing ${name}`);
  return value;
}

async function readPrefix(response, minimumCursor) {
  assert.ok(response.body);
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  let cursor = 0;
  const records = [];
  while (cursor < minimumCursor) {
    const result = await reader.read();
    assert.equal(result.done, false, "stream ended before a resumable prefix was observed");
    buffer += decoder.decode(result.value, { stream: true }).replace(/\r\n/g, "\n");
    for (;;) {
      const boundary = buffer.indexOf("\n\n");
      if (boundary < 0) break;
      const record = parseRecord(buffer.slice(0, boundary));
      buffer = buffer.slice(boundary + 2);
      if (record !== null) {
        records.push(record);
        if (record.id !== null) cursor = record.id;
      }
    }
  }
  await reader.cancel();
  return { cursor, text: messageText(records) };
}

function parseRecords(body) {
  return body.replace(/\r\n/g, "\n").split("\n\n").flatMap((block) => {
    const record = parseRecord(block);
    return record === null ? [] : [record];
  });
}

function parseRecord(block) {
  if (block.trim() === "") return null;
  let id = null;
  let event = "message";
  const data = [];
  for (const line of block.split("\n")) {
    if (line.startsWith("id:")) id = Number(line.slice(3).trim());
    else if (line.startsWith("event:")) event = line.slice(6).trim();
    else if (line.startsWith("data:")) data.push(line.slice(5).trimStart());
  }
  return { id, event, data: data.join("\n") };
}

function messageText(records) {
  return records
    .filter((record) => record.event === "message")
    .map((record) => JSON.parse(record.data).choices?.[0]?.delta?.content ?? "")
    .join("");
}
