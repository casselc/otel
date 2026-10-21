# Explicit-default consumer review follow-up

Measured source is preserved in commit `5b15abe05ebb4bf45b81e445985ba4001dc2a488`,
based on SDK `26186ce79941e84e0cc67eb148dd8dd43b4e1880`.
The independent main extraction merged in PR #65 at
`5381ecc0a0b9bc74697d280c81a4498140095ce3`.

The integration source/evidence review requested two follow-ups:

1. Copy main's deterministic shortcut-use test into this consumer branch.
   Done: `explicit-default-scalars-avoid-generic-walk-only-for-original-policy`
   proves zero generic walker calls for default scalars and positive calls for
   structured values and rebound-but-equal defaults. The generic oracle and
   tagged-error containment tests remain alongside it.
2. Confirm that `catch Throwable` catches `ex-info` on actual Jolt rather than
   infer that from JVM behavior. This catch is **pre-existing**, not a change in
   this optimization: `git show 26186ce:src/otel/any_value.clj` has it at line 210.
   Its earlier portability change was intentionally excluded from the main-only
   optimization. Do not replace a working inherited catch merely for convention.

Codex ran the full consumer SDK suite on official stock Jolt 0.8.23 through
the mandatory Chez 10.4.1 wrapper, after adding the missing mechanism test:

```text
jolt -A:test -M -m otel.test-runner
Ran 474 tests. 4197 assertions passed, 0 failures, 0 errors.
0 failures, 0 errors
```

The authoritative workspace log is
`evidence/otel-default-scalar-options-full-tests-20261020.log` and the process
exited zero. `explicit-default-path-keeps-known-policy-errors-nonthrowing`
deliberately throws a tagged `ex-info` from `string-bytes` and asserts a returned
byte-limit error map through this two-argument catch. Thus this is actual-host
containment evidence, not just a source inference or a test comparing two equally
throwing paths. Custom-budget/structured regression tests ran in the same suite.
This is not qualification of all hosts or deliberate concurrent Var redefinition.

The four-process socket evidence remains allocation-only qualification: 28.3%
lower estimated allocation, independent recovery of all four stores, substantial
control CPU/throughput drift, no claimed throughput or population-tail gain.
No production source changed after the timed campaign; these follow-ups add only
tests and this review receipt.
