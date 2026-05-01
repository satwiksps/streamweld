## Summary

<!-- Explain the user-visible or operational outcome. Keep this focused. -->

## Related issue

<!-- Use "Fixes #123" when merge should close an issue. -->

## Validation

<!-- List exact commands and results. Explain any check you could not run. -->

```text
make test
```

## Compatibility and risk

<!--
Cover protocol/wire changes, mixed-version rollout, persistence, bounded
resources, CRDs/Helm, TypeScript public APIs, security boundaries, and rollback
as applicable. Write "Not applicable" with a reason for a docs-only change.
-->

## Checklist

- [ ] The change is scoped to the linked problem and contains no unrelated churn.
- [ ] Tests cover new behavior or reproduce the fixed regression.
- [ ] Protocol, operations, examples, API docs, and decision records are updated where needed.
- [ ] I ran every relevant check from `CONTRIBUTING.md` and listed the results above.
- [ ] I did not add secrets, production data, Terraform state, or invented benchmark/compatibility claims.
- [ ] Generated benchmark and README result content came from `make bench`, if applicable.
- [ ] Every commit has a DCO `Signed-off-by` trailer (`git commit -s`).
