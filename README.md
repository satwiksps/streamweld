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

Streamweld is licensed under Apache-2.0.
