# ADR 012: Raise the Go security baseline

- Status: accepted
- Date: 2026-08-23

## Context

The original Go 1.23 compatibility contract held the OpenTelemetry, gRPC, and
`golang.org/x/*` modules below releases containing fixes for published HIGH and
CRITICAL vulnerabilities. Container binaries also embed the Go standard
library, so updating only the runtime image cannot repair those findings.

## Decision

Streamweld now declares Go 1.25 as its minimum module version and selects Go
1.26.6 as the repository toolchain and release/container compiler. The affected
OpenTelemetry, gRPC, `x/net`, `x/sys`, and `x/text` module families are updated
together to their patched, compatible release lines.

## Consequences

- Contributors need Go 1.25 or newer; standard Go toolchain selection installs
  Go 1.26.6 when permitted.
- CI and release images compile with the patched standard library.
- Older Go installations no longer build the repository.
- Security updates may raise the minimum again when a safe dependency release
  no longer supports the current baseline.
