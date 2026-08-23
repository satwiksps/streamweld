# Security policy

Streamweld sits between clients, inference backends, Redis, and Kubernetes
control paths. A vulnerability can affect generated content, credentials,
stream isolation, or cluster workloads. Please report suspected security
issues privately and give maintainers time to investigate before disclosure.

## Supported versions

| Version | Security support |
|---|---|
| `main` and unreleased builds | Investigated to prepare the first release; not a stable deployment target |
| Tagged releases | None published yet; after `v0.1.0`, the latest release will be supported |
| Superseded releases | Not routinely patched; upgrade to the latest release |

If a fix can be backported safely, maintainers may publish an additional patch
release. This table does not promise long-term support for any minor version.

## Reporting a vulnerability

Use GitHub's private vulnerability reporting form:

[Report a Streamweld vulnerability privately](https://github.com/satwiksps/streamweld/security/advisories/new)

Do not open a public issue, discussion, or pull request containing vulnerability
details. If GitHub private reporting is unavailable, open a public issue that
only asks maintainers to establish a private channel; include no technical
details, affected endpoints, credentials, or reproduction steps in that issue.

Include as much of the following as you can:

- the affected Streamweld version, image digest, Helm chart version, npm
  package version, or commit;
- deployment shape, including journal mode, proxy replica count, Kubernetes
  version, and whether the private owner relay is enabled;
- a clear impact statement and the trust boundary that is crossed;
- minimal reproduction steps or a proof of concept using synthetic data;
- relevant logs with tokens, prompts, generated content, Redis URLs, bearer
  tokens, certificates, and cloud identifiers removed;
- whether the issue is already public or under an external disclosure
  deadline; and
- suggested mitigations or fixes, if known.

Never send live API keys, model credentials, private keys, production prompts,
generated customer content, Terraform state, or a database dump. Maintainers
will ask for additional sanitized evidence if needed.

## What to expect

Maintainers aim to:

- acknowledge a complete report within three business days;
- provide an initial severity and scope assessment within seven business days;
- send progress updates at least every fourteen days while remediation is in
  progress; and
- coordinate a release and public disclosure with the reporter.

Complex cross-project or supply-chain reports may take longer. Timelines are
targets, not a service-level agreement. Please avoid disclosure until a fix and
reasonable upgrade window are available, unless maintainers stop responding or
an active exploitation risk requires faster coordination.

When appropriate, the project will credit the reporter, request a CVE, publish
a GitHub security advisory, document affected versions and mitigations, and
release patched artifacts. Tell us if you prefer not to be credited.

## Scope and testing boundaries

Security-relevant areas include:

- public OpenAI-compatible and stream resume/stop HTTP endpoints;
- journal isolation, idempotency, retention, and degraded-mode behavior;
- Redis credentials and owner-relay discovery;
- relay mTLS identity and cross-replica stop or event routing;
- operator route administration, drain hooks, webhook mutation, and RBAC;
- Helm defaults, container security contexts, and network policies;
- TypeScript cursor persistence, header forwarding, and resume behavior;
- release artifacts, dependency integrity, and CI/CD configuration; and
- the hosted failure-lab application's session and API boundaries.

The following are normally out of scope unless they demonstrate a concrete
Streamweld vulnerability:

- automated scanner output without a reproducible impact;
- denial-of-service testing against the public demo or any system you do not
  own;
- social engineering, phishing, physical attacks, or maintainer harassment;
- attacks requiring compromised cluster-admin, host-root, or inference-backend
  control when no additional boundary is crossed;
- vulnerabilities solely in an upstream dependency, Kubernetes, Redis, a model
  server, or a cloud provider (report those to the responsible project first);
  and
- findings that depend on intentionally documented insecure development modes.

Test only against systems you own or have explicit permission to assess. Use
the deterministic local or kind fixtures where possible. Do not access,
modify, retain, or publish another user's data.

## Safe harbor

The project will not pursue legal action for good-faith research that follows
this policy, avoids privacy violations and service disruption, and gives
maintainers a reasonable opportunity to remediate. If you are unsure whether a
test is safe or in scope, ask through the private reporting channel before
proceeding. This safe-harbor statement does not authorize testing of third-party
systems and cannot bind third parties.
