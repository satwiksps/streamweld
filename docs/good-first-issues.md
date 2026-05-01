# Good first issue candidates

This page is a maintainer-ready backlog of bounded starter tasks. Each section
can be copied into a GitHub issue and labeled `good first issue` plus the
suggested area label. Before assigning one, confirm that no open pull request
already covers it. Contributors should comment on the GitHub issue before
starting; this document by itself does not reserve work.

These tasks deliberately avoid changing Streamweld's core durability protocol.
If investigation reveals a wire-contract or migration-safety decision, stop
and ask a maintainer to rescope the issue.

## 1. Normalize localStorage read and write failures

Suggested labels: `good first issue`, `typescript`, `client`

The `@streamweld/client` localStorage helper wraps failures while obtaining
`globalThis.localStorage`, but exceptions thrown later by `getItem` or
`setItem` currently escape as runtime-specific errors. Callers should receive a
consistent `StreamPersistenceError` with the original exception as its cause.

Scope:

- update `packages/client/src/persistence.ts` to wrap `getItem` and `setItem`
  failures;
- add focused cases to `packages/client/src/persistence.test.ts` using a fake
  storage object that throws on reads and writes; and
- keep successful cursor encoding and exact uint64 behavior unchanged.

Acceptance criteria:

- read, numeric write, and exact-string write failures throw
  `StreamPersistenceError`;
- the original error is available as `cause`;
- existing persistence tests continue to pass; and
- `pnpm --filter @streamweld/client test` and
  `pnpm --filter @streamweld/client run typecheck` pass.

Non-goals: retries, fallback storage, quota management, or changing the
`StreamPersistence` interface.

## 2. Add a persistent Go SSE decoder fuzz regression corpus

Suggested labels: `good first issue`, `go`, `testing`

`internal/proxy/sse/fuzz_test.go` has useful inline seeds for chunk boundaries,
mixed line endings, invalid UTF-8, comments, and oversized events. Add a small
persistent `testdata/fuzz/FuzzDecoder` corpus so future fuzz-discovered edge
cases can be reviewed as named repository fixtures.

Scope:

- add 6-10 minimal corpus entries covering CR, LF, and CRLF framing, a split
  multibyte rune, an invalid rune, an unterminated event, NUL in `id`, and an
  event at the configured size boundary;
- avoid redundant large or random corpus files; and
- add a short comment in `fuzz_test.go` explaining how to minimize and retain a
  newly discovered regression.

Acceptance criteria:

- `go test ./internal/proxy/sse -run=FuzzDecoder` loads every corpus entry;
- `go test ./internal/proxy/sse` passes;
- a short local `go test ./internal/proxy/sse -fuzz=FuzzDecoder -fuzztime=10s`
  run finds no failure; and
- no production parser behavior changes.

Non-goals: redesigning the decoder or committing an unbounded generated fuzz
cache.

## 3. Add a documented Redis multi-replica Helm values example

Suggested labels: `good first issue`, `helm`, `documentation`

The root README shows the minimum `--set` flags for Redis and two proxies, while
the chart exposes additional bounded reader, journal, PDB, and resource values.
Add a checked-in example values file that users can inspect and pass to Helm
without copying a long command.

Scope:

- add `deploy/helm/streamweld/examples/redis-ha-values.yaml` with two proxy
  replicas, the bundled Redis enabled, explicit resource requests/limits, a
  PDB, and comments that distinguish the bundled development profile from an
  externally managed production Redis;
- link the example from `deploy/helm/streamweld/README.md`; and
- extend an existing Helm render check or add a small test proving the example
  renders with the current values schema.

Acceptance criteria:

- `helm lint deploy/helm/streamweld --strict` passes;
- `helm template streamweld deploy/helm/streamweld -f deploy/helm/streamweld/examples/redis-ha-values.yaml`
  succeeds;
- no credentials or environment-specific hostnames appear in the example; and
- comments do not describe the bundled single Redis Pod as highly available.

Non-goals: deploying a Redis operator, adding persistence defaults, or changing
chart behavior.

## 4. Add a copyable `streamweldctl doctor --json` documentation example

Suggested labels: `good first issue`, `cli`, `documentation`

`docs/compatibility.md` explains SAFE, DEGRADED, and UNSAFE verdicts but does not
show the JSON shape that automation consumes. Add a sanitized deterministic
example tied to the existing CLI acceptance fixture.

Scope:

- document one complete but compact `doctor --json` response in
  `docs/compatibility.md`;
- explain which fields form the immutable cache identity and which fields are
  evidence rather than configuration; and
- add or refine a test in `cmd/streamweldctl/main_acceptance_test.go` so the
  documented top-level field names cannot silently drift.

Acceptance criteria:

- the example is valid JSON and contains the current four probe results;
- it uses fixture names and no production compatibility claim;
- `go test ./cmd/streamweldctl` passes; and
- the prose tells operators to capture a new report after an image, tokenizer,
  or model change.

Non-goals: changing probe algorithms, verdict thresholds, or the CLI JSON
schema.

## 5. Document the metrics catalog with starter PromQL

Suggested labels: `good first issue`, `observability`, `documentation`

Metric definitions live in `internal/telemetry/telemetry.go` and the Helm chart
ships a Grafana dashboard, but operators need a compact text reference for
names, labels, units, and cardinality. Add it to the operations guide without
inventing alert thresholds.

Scope:

- add a metrics-reference section to `docs/operations.md` covering stream,
  migration, rescued/re-billed token, resume, seam, TTFT/inter-token, journal,
  and backend metrics;
- state the unit and bounded label set for each metric family; and
- include 3-5 starter PromQL queries for rates or distributions, clearly
  labeled as examples rather than universal alert thresholds.

Acceptance criteria:

- every documented metric name exists in `internal/telemetry/telemetry.go`;
- label names match the implementation exactly;
- histogram examples use `rate(..._bucket[...])` and `histogram_quantile`
  correctly where applicable; and
- no fabricated production baseline or SLO is added.

Non-goals: changing instrumentation, dashboard JSON, or recommending a single
alert threshold for every deployment.

## 6. Smoke-test the published ESM and CommonJS package entry points

Suggested labels: `good first issue`, `typescript`, `packaging`

The TypeScript packages build ESM, CommonJS, and declaration outputs, while
their unit tests import source modules. Add a small post-build smoke test that
would catch a broken `exports`, `main`, `module`, or declaration path before
publication.

Scope:

- add minimal package-local smoke scripts for `@streamweld/client` and
  `@streamweld/ai-sdk` that import the built ESM entry and require the built
  CommonJS entry;
- assert only stable exported symbols, without contacting a backend;
- wire the smoke scripts into an explicit package script that runs after
  `pnpm --recursive run build`; and
- document the command in the pull request validation notes.

Acceptance criteria:

- both import styles resolve from each package's declared entry points;
- the test runs against `dist`, not TypeScript source aliases;
- `pnpm --recursive run build`, the new smoke command, and `make typecheck`
  pass; and
- package tarball contents remain limited to the intended `files` entries.

Non-goals: publishing to npm, adding a bundler, or expanding the public API.
