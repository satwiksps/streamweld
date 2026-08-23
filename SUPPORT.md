# Support

Streamweld is a pre-release open-source project maintained on a best-effort
basis. There is currently no paid support channel or guaranteed response time.
The routes below help questions reach the right place and keep security reports
private.

## Choose the right channel

| Need | Channel |
|---|---|
| Usage, deployment, or architecture question | [GitHub Discussions](https://github.com/satwiksps/streamweld/discussions) |
| Reproducible defect | [Bug report](https://github.com/satwiksps/streamweld/issues/new?template=bug_report.yml) |
| Focused product or protocol proposal | [Feature request](https://github.com/satwiksps/streamweld/issues/new?template=feature_request.yml) |
| Incorrect or missing documentation | [Documentation issue](https://github.com/satwiksps/streamweld/issues/new?template=documentation.yml) |
| Suspected vulnerability | Follow the private process in [SECURITY.md](SECURITY.md) |
| Conduct incident | Follow the confidential process in [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md#reporting-and-enforcement) |

Please search the [documentation](apps/docs-site/src/content/docs/index.mdx),
existing Discussions, issues, and pull requests before opening something new.
Do not open a support request solely to request an expedited review.

## Information that helps

For technical questions and bug reports, include:

- the Streamweld commit, release, image digest, chart version, or npm package
  version;
- the component and deployment profile involved;
- Kubernetes, Redis, Go, Node.js, browser, or runtime versions as applicable;
- the smallest configuration and sequence of commands that reproduces the
  behavior;
- the expected outcome and the observed terminal state or error; and
- relevant sanitized logs.

Remove API keys, tokens, prompts, generated user content, Redis URLs,
certificates, cloud identifiers, Terraform state, and other private data. If a
report may reveal a security boundary or exploit, stop and use the private
security channel instead.

## Version policy

Support is focused on current `main` while the project is pre-release. After
versioned releases begin, support will focus on the latest release. Before
reporting a problem on an older revision, verify whether it still occurs on the
current release or branch. See
[SECURITY.md](SECURITY.md#supported-versions) for the separate security support
policy.
