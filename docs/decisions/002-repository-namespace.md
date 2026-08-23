# ADR 002: Use the Streamweld project namespace

- Status: Accepted
- Date: 2026-08-22

## Context

The build specification leaves the GitHub module owner as `<user>`, while internal import paths and published build metadata require a real package owner. The public repository and its release credentials are owned by `satwiksps`; a `streamweld` GitHub organization is not available.

## Decision

Use `github.com/satwiksps/streamweld` as the Go module path and `ghcr.io/satwiksps` for container and Helm artifacts. Retain `@streamweld` as the npm scope because npm package ownership is configured independently from GitHub Container Registry.

## Consequences

All Go imports, release metadata, examples, and generated manifests point to namespaces that the current release workflow can publish to. If the project later moves to an organization, that migration is a deliberate module-path and package-distribution change, with repository redirects where possible.
