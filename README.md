# Streamweld

> Your token stream shouldn't die because a pod got evicted or a phone switched to cellular.

Streamweld is a durable stream layer for LLM inference: an OpenAI-compatible reverse proxy and Kubernetes operator that give every generation an identity and an append-only event log.

Non-streaming requests are direct pass-through requests with journaling disabled, so Streamweld adds no data-plane work beyond proxy forwarding on that path.

The protocol is defined in [`docs/protocol.md`](docs/protocol.md). The implementation is being built in the ordered phases recorded in [`streamweld-build-spec.md`](streamweld-build-spec.md); claims and performance numbers are published only when backed by reproducible tests.

## Development prerequisites

- Go 1.23 or newer
- Node.js 20 or newer
- pnpm 11 or newer
- GNU Make 4 or newer

Run the repository checks with:

```sh
make test
```

## Local passthrough proxy

Start any OpenAI-compatible backend on port 8000, then run:

```sh
go run ./cmd/streamweld-proxy --backend http://127.0.0.1:8000 --listen :8080
```

The proxy currently accepts `POST /v1/chat/completions`, `POST /v1/completions`, and `GET /v1/models`. Streaming response bytes are forwarded and flushed as they arrive; request bodies and backend-specific JSON fields are not rewritten. Health probes are available at `/healthz` and `/readyz`.

```sh
curl -N http://127.0.0.1:8080/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"your-model","messages":[{"role":"user","content":"Count to five."}],"stream":true}'
```

Streamweld is licensed under Apache-2.0.
