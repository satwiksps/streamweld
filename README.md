# Streamweld

> Your token stream shouldn't die because a pod got evicted or a phone switched to cellular.

Streamweld is a durable stream layer for LLM inference: an OpenAI-compatible reverse proxy and Kubernetes operator that give every generation an identity and an append-only event log.

Non-streaming responses use the direct pass-through path with journaling
disabled. The bounded request body is inspected only to select streaming mode;
its original bytes are then restored before proxy forwarding.

The protocol is defined in [`docs/protocol.md`](docs/protocol.md). The implementation is being built in the ordered phases recorded in [`streamweld-build-spec.md`](streamweld-build-spec.md); claims and performance numbers are published only when backed by reproducible tests.

Before allowing producer migration for a model, probe its chat-template
continuation behavior with `streamweldctl doctor --backend URL --model NAME`.
The checker and the honestly scoped results table are documented in
[`docs/compatibility.md`](docs/compatibility.md).

## Development prerequisites

- Go 1.23 or newer
- Node.js 20 or newer
- pnpm 11 or newer
- GNU Make 4 or newer

Run the repository checks with:

```sh
make test
```

## Local durable proxy

Start any OpenAI-compatible backend on port 8000, then run:

```sh
go run ./cmd/streamweld-proxy --backend http://127.0.0.1:8000 --listen :8080
```

The proxy accepts OpenAI-compatible chat and legacy completions plus
`GET /v1/models`. A streaming response includes
`X-Streamweld-Stream-Id`; complete upstream SSE chunks are committed before
delivery and can be replayed from an exclusive cursor. Unknown JSON request
fields survive normalization. Health probes are available at `/healthz` and
`/readyz`.

When the configured pool contains a compatible healthy target, an unexpected
producer EOF, reset, 5xx, error chunk, failed health probe, explicit backend
drain, or enabled stall detector starts a bounded continuation attempt. The
proxy journals the handoff before continuation chunks and removes only a
UTF-8-safe leading overlap. Migration remains disabled for unknown or unsafe
chat-template verdicts under the default strict policy.

```sh
curl -N http://127.0.0.1:8080/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"your-model","messages":[{"role":"user","content":"Count to five."}],"stream":true}'
```

Resume a disconnected reader with the returned stream ID and its last SSE
`id`:

```sh
curl -N http://127.0.0.1:8080/v1/streams/STREAM_ID/events \
  -H 'Last-Event-ID: 41'
```

Only an explicit stop cancels generation:

```sh
curl -X POST http://127.0.0.1:8080/v1/streams/STREAM_ID/stop
```

Drain one registered backend and wait for its leases to reach zero:

```sh
curl -X POST 'http://127.0.0.1:8080/internal/backends/127.0.0.1%3A8000/drain?timeout=10s'
```

The default memory journal is bounded and intended for one proxy replica.
Redis-backed cross-replica durability is configured for production installs.

Streamweld is licensed under Apache-2.0.
