# ADR 002: Use the Streamweld project namespace

- Status: Accepted
- Date: 2026-08-22

## Context

The build specification leaves the GitHub module owner as `<user>`, while internal import paths and published build metadata require a stable module path from the first commit. No personal GitHub account or alternative organization is supplied.

## Decision

Use `github.com/streamweld/streamweld` as the Go module path and `@streamweld` as the npm scope. The repository name and package namespace therefore represent the project rather than an individual maintainer.

## Consequences

All Go imports, release metadata, examples, and generated manifests use the same canonical project identity. If the project is later hosted under a different owner, that move is a deliberate breaking module-path change or is handled with repository redirects.
