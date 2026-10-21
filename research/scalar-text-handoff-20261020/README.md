# SDK-owned scalar-text encoder handoff (unintegrated prototype)

BaseSDK `14f58e5a57d6cd1760451a426e40d8cddcf53949`. Companion codec prototype
`dbbe399df68c4eb817ab0fbd8f21db9dc578002e`, branch
`perf/scalar-text-encoder-view-20261020` in the owned exporter repository.
Both run on official stock Jolt0.8.23 /Chez10.4.1 without compiler/JSON patches.

`scalar-text-payload-projector` is a SEPARATE method, not a new return type for
`scalar-string-map-payload-converter`. It may produce either a fresh plain map
or the selected codec's opaque view, or nil to decline to the caller's original
normalization path. Never use a view as an ordinary map or data.json input.
No exporter has adopted it yet; no defaults, capture, persistence or queueing
semantics changed. The prototype is not ready to repin into an application.

Follow-up: the exporter now has an explicit opt-in with independent recovery of
350,000 rows and exact-head stock CI; see exporter checkpoint `68ee1ca` and its
review/docs follow-up `a373c1b`. This remains separate from enabling it in Oscope.
The SDK branch merged consumer target `5aff604` (CI/deps changes only; SDK source
unchanged), and adds `:stock-scalar-views-test` against published codec `68ee1ca`.
That actual Git-component gate passes 5/67 and checks the selected source path.

Claude read-only review of exact implementation `d11752a`: APPROVE (prototype
scope only), no blockers. Review did not execute tests; Codex owns the actual
new/regression/baseline runs. Fixed budgets deliberately have no caller override
in this first API. See `claude-review.txt` for the bounded source review.

The defining SDK supplies all eight original policy roots, scalar limits,
trusted sorted prototype and fixed per-payload budgets. The codec receives no
application-owned substitutes for those private anchors. Conversion order is:

1. Existing source-root cache hit, with fresh plain wrapper.
2. Original plain-map scalar helper and existing store admission.
3. Learned sorted-map view, with original policy checks before/after.
4. Original sorted-map fallback, learn after admission, then existing store.

Opaque views never enter the ordinary map cache. That cache may therefore fill
differently from the old converter; no equivalence of insertion counts or cache
hit rates is claimed. Shared roots already stored by the fallback retain their
first-position cache hit. Plain maps never run the learned-view lookup.

The closure remains serial and payload-owned. Fixed bounds:64 root insertions /
65536 retained String characters;8 key schemas /8192 key characters /128keys per
schema, plus bounded template overhead. Plans must retain no input values or
source trees. Views own fresh ephemeral slots. Native factory/folder bodies and
immutable input Strings remain trusted; no cross-thread redefinition lock or
general third-party callback validation is claimed.

## Bounded qualification

New focused gate:5tests/67assertions, covering root-cache/plain-map precedence,
fresh wrappers, distinct-root opaque output, identical encoded bytes, all eight
changed policies, unsupported inputs, full shape budget/fallback, unavailable
factories and invalid options. Existing plain-map converter remains map-returning.
New +existing injected-plan regression:11tests/140assertions, all pass. The
default SDK aggregate also passes476tests/5484assertions on stock0.8.23. The
older no-argument native-cache gate is NOT stock-qualified: a combined run had
7failures/57errors in that namespace. The unchanged SDK14f58e5 baseline reproduces
exactly7failures/57errors (16tests/27passing assertions), and confirms stock lacks
global `pmap-fold-seq-order`. This optional compiler-helper gate intentionally
fails rather than skips when unavailable. Its tests were not weakened; use the
injected library path here. Neither result is inferred from the codec's own
43/596 source/standalone gate. No compiler rebuild or old-version test was used.

Run from this SDK WT through the required Chez10.4.1 wrapper:

```sh
jolt -Srepro -Sdeps '{:paths ["src" "resources" "test" "/absolute/codec-component/src"]}' -m otel.scalar-text-payload-projector-test
```

Only tests select the codec source path; SDK production dependencies do not gain
a codec/exporter dependency. No native stores or heavyweight captures were made.
No performance is measured by this SDK test. Earlier codec17-field micro results
exclude admission/root-cache and real spans and cannot qualify this new handoff.

Next: independent review, exporter opt-in/preflight adapter under unchanged
policy/fallback/effect ordering, then cumulative rich component comparison and
real local-Durable independent recovery. Do not install this factory into the
existing map-returning API. Keep earlier optimized paths and cross-runtime
baselines intact; no unchanged exhaustive model run is needed for this pure
synchronous representation handoff.
