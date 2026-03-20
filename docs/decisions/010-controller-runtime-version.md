# ADR 010: Pin controller-runtime to the Kubernetes 1.32 library line

- Status: accepted
- Date: 2026-08-22

## Context

The operator requires controller-runtime, Kubernetes API types, admission
webhooks, EndpointSlice watches, leader election, status patching, and envtest
compatible schemas. These packages must remain on one supported dependency
line. Selecting whatever versions happen to be newest independently can create
incompatible `k8s.io/*` modules or raise the minimum Go version beyond this
repository's Go 1.23 contract.

## Decision

Pin `sigs.k8s.io/controller-runtime` to `v0.20.4` and its Kubernetes `v0.32.1`
module line. This is the controller-runtime compatibility line for Kubernetes
1.32 libraries and supports Go 1.23. Kubernetes API, apimachinery, and
client-go imports are treated as the transitive platform surface of the one
approved controller-runtime dependency, not as independently versioned
features.

Upgrade controller-runtime and all `k8s.io/*` modules together after checking
the upstream compatibility matrix, supported Go version, CRD validation, Helm
install test, webhook admission test, and kind acceptance suite.

## Consequences

- Operator builds and generated schemas use a coherent Kubernetes API set.
- The repository gains controller-runtime's substantial indirect dependency
  graph, but no alternative Kubernetes client stack.
- Supporting a newer Kubernetes API library or controller-runtime feature is a
  deliberate coordinated upgrade rather than an incidental module update.
