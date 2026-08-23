# ADR 001: Split Go infrastructure from TypeScript consumers

- Status: Accepted
- Date: 2026-08-22
- Decision owners: Streamweld maintainers

## Context

Streamweld spans two materially different environments.

The data plane holds long-lived upstream and downstream streaming connections,
maintains per-stream state, and must continue producer work independently of
consumer connections. The Kubernetes control plane watches cluster resources,
reconciles backend pools, and manages pod-drain integration. The CLI shares
protocol types and operational behavior with those services.

Adoption happens primarily in JavaScript and TypeScript applications. The
durable client must expose async iterables, reconnect transparently in browsers
and server runtimes, and integrate with the Vercel AI SDK. The demonstration UI
is also a web application.

A single implementation language would optimize one of these environments at
the expense of the other. Cross-language behavior must instead be kept aligned
through the normative wire protocol and shared conformance fixtures.

## Decision

Use the following fixed language boundary:

| Component | Language | Boundary rationale |
|---|---|---|
| Data plane proxy | Go | Suits long-lived network streams, cancellable goroutines, bounded concurrency, and low-overhead services |
| Kubernetes operator | Go | Uses the Kubernetes controller-runtime ecosystem and shares operational types with the proxy |
| `streamweldctl` CLI | Go | Reuses protocol, backend, conformance, and drain code from the Go implementation |
| `@streamweld/client` | TypeScript | Provides the native API surface for browser, Node 20+, Deno, Bun, and edge consumers |
| `@streamweld/ai-sdk` | TypeScript | Implements the Vercel AI SDK v5 `ChatTransport` boundary directly |
| Demo and control UI | TypeScript with React | Shares SDK types and targets the browser deployment environment |

The Go components live under `cmd/`, `internal/`, and `controllers/`. The
published TypeScript packages live under `packages/`, and the demo lives under
`apps/demo/`.

The language boundary is the versioned HTTP/SSE protocol in
`docs/protocol.md`. Go implementation structs are not the public contract, and
the TypeScript SDK does not import generated Go artifacts. Both sides validate
the same checked-in protocol examples and failure cases.

Dependency policy follows the same boundary:

- Go uses the standard library plus controller-runtime, the Prometheus client,
  and a Redis client. Any additional production dependency requires a separate
  ADR with its operational and supply-chain cost.
- `@streamweld/client` has zero runtime dependencies. It uses web-platform APIs
  available in every supported runtime and publishes ESM and CommonJS builds.
- The AI SDK adapter remains a separate package so its peer integration does
  not add runtime weight to the core client.
- UI-only dependencies remain in the demo and never enter the client package or
  Go data plane.

## Consequences

### Positive

- The proxy, operator, and CLI share one implementation of protocol primitives,
  conformance probes, backend state, and operational commands.
- Kubernetes reconciliation follows established Go controller patterns instead
  of reproducing that ecosystem in another runtime.
- Application developers receive an idiomatic typed client rather than a thin
  generated binding around a server-oriented language.
- Browser and edge support can be tested without Node-only compatibility
  shims.
- The dependency-free core SDK can be adopted without pulling UI or AI SDK
  packages into an application.

### Costs

- Protocol types and codecs exist in both languages and can drift.
- CI must build, lint, and test Go and TypeScript workspaces.
- Contributors may need proficiency in two ecosystems.
- Releases coordinate Go binaries and images with npm packages.

The drift cost is controlled by treating `docs/protocol.md` as normative,
keeping language-neutral wire fixtures, and running equivalent protocol tests
in both workspaces. Neither implementation may redefine protocol behavior
solely through language-specific types.

## Alternatives considered

### All TypeScript

This would simplify the web SDK and demo but make the Kubernetes operator and
high-concurrency streaming data plane depend on a less natural control-plane
ecosystem. It would also prevent direct code sharing with a Go operational CLI.

### All Go

This would simplify repository tooling but leave browser adoption dependent on
generated or hand-written bindings that are not idiomatic TypeScript. It would
also weaken the required Vercel AI SDK integration and browser-focused demo.

### Go services with no maintained client SDK

Plain SSE is sufficient for basic reconnection, but it does not provide the
required distinction among local abort, explicit stop, expiration, and
terminal error, nor the persistence hook and uninterrupted async iterable.
The TypeScript SDK is therefore a core product component.

### Rust data plane with Go operator

Rust could provide a capable streaming data plane, but it would introduce a
third language without removing the need for Go controller-runtime or
TypeScript consumers. The additional build, contributor, and shared-type cost
does not provide a necessary protocol capability.

## Revisit criteria

This decision may be revisited only if a component's deployment environment or
public integration boundary changes materially. Performance preference alone
is insufficient; a replacement must preserve the normative protocol, supported
runtimes, dependency limits, and operational behavior.
