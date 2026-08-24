# Contributing to Streamweld

Thank you for helping make durable LLM streams easier to operate. Streamweld
spans a Go proxy and Kubernetes operator, TypeScript clients, a Helm chart,
Terraform, and deterministic failure-injection tests. Small, focused changes
are easier to review and safer to release than broad rewrites.

Participation in the project is governed by the
[`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md). General usage and deployment
questions belong in [GitHub Discussions](https://github.com/satwiksps/streamweld/discussions);
the issue tracker is reserved for actionable defects and proposals. See
[`SUPPORT.md`](SUPPORT.md) for the complete routing guide.

## Before you start

- Search existing issues and pull requests before opening a duplicate.
- Use a bug report for reproducible incorrect behavior and a feature request
  for a proposed protocol or product change.
- Report suspected vulnerabilities privately as described in
  [`SECURITY.md`](SECURITY.md). Never include exploit details or secrets in a
  public issue.
- For protocol, persistence, migration-safety, CRD, or public SDK changes,
  open an issue before implementation. Describe compatibility and rollout
  implications, not only the desired API.
- Starter-sized tasks are listed in
  [`docs/good-first-issues.md`](docs/good-first-issues.md).

## Development setup

The supported development toolchain is:

- Go 1.25 or newer (the repository selects the patched Go 1.26.6 toolchain);
- Node.js 20 or newer;
- pnpm 11 (the exact package-manager version is declared in `package.json`);
- GNU Make 4 or newer;
- Docker, kind, kubectl, and Helm for cluster tests; and
- Terraform and TFLint for infrastructure changes.

Bootstrap locked dependencies from the repository root:

```sh
make bootstrap
```

Run the ordinary unit suites with:

```sh
make test
```

The full local gate is `make ci`. Some checks require Docker, Redis, kind, or
platform tooling; run every check relevant to the files you changed and state
clearly in the pull request when an environment prevented a check.

## Making a change

1. Fork the repository and create a topic branch from the current `main`.
2. Keep commits reviewable and use imperative commit subjects.
3. Add or update tests with behavioral changes. A regression fix should
   normally contain a test that fails without the fix.
4. Update the protocol, operations documentation, examples, or decision record
   when a user-visible contract changes.
5. Run the relevant checks below.
6. Open a pull request using the repository template and link the issue it
   addresses.

Do not mix dependency upgrades, formatting sweeps, generated-file churn, and a
behavioral change in one pull request. Preserve unrelated work in a dirty
working tree.

## Engineering expectations

### Protocol and correctness

`docs/protocol.md` is the normative wire contract. Changes must preserve these
core invariants unless an accepted design explicitly versions the protocol:

- committed SSE sequence numbers are ordered and never represented as lossy
  JavaScript numbers;
- disconnect and explicit stop remain different operations;
- migration must refuse unsafe continuation rather than risk corrupt output;
- bounded journals and readers must fail explicitly; and
- degraded Redis behavior must not be presented as durable or replayable.

Never add benchmark or compatibility claims by hand. `make bench` owns
`benchmarks/results.json`, `benchmarks/results.md`, and the marker-delimited
README results section. Only publish results from a run that actually
executed, with its profile and environment recorded.

### Go

- Prefer standard-library types and explicit bounds at network and storage
  boundaries.
- Keep errors actionable and preserve causes with `%w` or `errors.Join`.
- Use `gofmt`; do not hand-edit generated Go such as
  `internal/apis/v1alpha1/zz_generated.deepcopy.go`.
- Add table-driven and concurrency tests where appropriate. Code touching
  journals, readers, ownership, or lifecycle synchronization must be exercised
  with the race detector in CI.

### TypeScript

- Preserve exact cursor strings and the zero-runtime-dependency contract of
  `@streamweld/client`.
- Keep public types discriminated and avoid `any` at protocol boundaries.
- Test ESM-facing behavior with Vitest and update package documentation for
  public API changes.
- Use pnpm and commit `pnpm-lock.yaml` only when dependency metadata changes.

### Kubernetes and infrastructure

- Keep CRD validation, Go API types, Helm values, and the values schema in
  sync.
- Render and lint Helm changes; test both valid profiles and intended render
  failures.
- Treat drain hooks, admin credentials, Redis URLs, relay certificates, and
  Terraform state as sensitive. Never commit live credentials or state files.
- Changes to failure handling should include deterministic tests; cluster
  behavior should be exercised with kind when possible.

## Test matrix

Run the smallest applicable set while developing, then the complete relevant
gate before requesting review.

| Area changed | Required checks |
|---|---|
| Go proxy, journal, migration, operator, or CLI | `go test ./...`, `make vet`, `make lint-go`, `make build-go` |
| Concurrency or lifecycle logic | `go test -race ./...` on a CGO-capable host |
| TypeScript clients or website | `make typecheck`, `make test-ts`, `make build-ts` |
| Helm chart or CRDs | `make helm-lint`; run `make e2e` for runtime behavior |
| Chaos harness or benchmark rendering | `make chaos`, `make bench-check`; use `make chaos-kind` for physical injections |
| Terraform | `make terraform-validate terraform-lint` |
| Documentation or governance only | Check links, commands, Markdown rendering, and `git diff --check` |

Before pushing, also run:

```sh
git diff --check
```

## Developer Certificate of Origin

Streamweld uses the [Developer Certificate of Origin 1.1](https://developercertificate.org/).
Every commit must carry a `Signed-off-by` trailer certifying that you have the
right to submit the contribution under the project's Apache-2.0 license. Add
the trailer automatically with:

```sh
git commit -s
```

The name and email in the trailer must identify the contributor and match an
author or committer identity for that commit. If a commit is missing the
trailer, amend it rather than adding a separate sign-off-only commit. This is a
sign-off, not a copyright assignment.

## Pull request review

Maintainers review correctness, compatibility, bounded resource behavior,
security, operational impact, and evidence. Address review comments with new
commits while review is active; maintainers may squash when merging. A pull
request is ready to merge when required checks pass, documentation and tests
match the behavior, every commit is signed off, and reviewer concerns are
resolved.

By contributing, you agree that your contribution is licensed under the
[Apache License 2.0](LICENSE).
