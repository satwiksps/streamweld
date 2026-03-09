# ADR 008: Test Redis semantics with both an emulator and Redis

- Status: accepted
- Date: 2026-08-22

## Context

Phase 5 needs deterministic tests for Redis Streams, Lua scripts, expiry,
ambiguous mutation receipts, reader leases, and cross-client races. Starting an
external Redis process for every unit test makes local and package-level tests
slow and dependent on host tooling. The build specification otherwise limits
Go dependencies to the production Redis client and a small approved set.

An in-process Redis emulator is an additional test dependency and does not
perfectly reproduce Redis networking, blocking reads, server time, or every Lua
edge case. It therefore cannot be the only compatibility oracle.

## Decision

Use `github.com/alicebob/miniredis/v2` only from test files for fast,
deterministic package and proxy acceptance tests. Production binaries use
`github.com/redis/go-redis/v9` and do not link the emulator.

CI also starts Redis 7.4 and runs the external acceptance suite through
`STREAMWELD_TEST_REDIS_URL`. Tests that require real blocking or transport
behavior must use that suite; emulator coverage must not be used to claim Redis
wire or server-version compatibility by itself.

## Consequences

- Unit and concurrency tests remain reproducible without Docker or a local
  Redis installation.
- The repository carries one test-only Go dependency outside the production
  allowlist.
- Real Redis remains a required CI gate, catching differences in Streams, Lua,
  expiry, and connection behavior that the emulator may miss.
