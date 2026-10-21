## Summary

Add a separate, opt-in `scalar-text-payload-projector` for encoder owners that
can consume an opaque scalar-text view. Repeated attribute shapes currently
produce a fresh normalized HAMT and numeric Strings immediately before encoding.
This handoff lets a trusted codec avoid those intermediates while the SDK still
owns normalization policy and admission.

The existing map-returning converter is unchanged. The new method returns a
fresh plain map, an encoder-specific view, or nil to require the caller's normal
fallback. Preserve root-cache-first/plain-map-first order, all eight original
policy anchors, and bounded per-payload key-only plans. Views never enter the
ordinary map cache. No default capture, export queue or confirmation change.

Stacked on `perf/default-scalar-options-20261020`, not SDK main. The target's
published-shape CI changes were merged without changing the reviewed SDK source.

## Validation

- Stock Jolt 0.8.23 / Chez 10.4.1: default aggregate **476 tests / 5,484 assertions**.
- New published-codec gate **5 / 67**, including actual source-coordinate
  selection, original policy changes, bounded schemas, missing capabilities and
  unsupported input. Add that gate to stock-Jolt CI; no production codec dependency.
- New + existing injected-plan regression **11 / 140**.
- Red/green control: the old method always returns plain maps; after learning,
  the new method returns an opaque view for a distinct sorted root with identical
  encoded bytes. Already-cached roots still return fresh map wrappers first;
  plain inputs do not invoke the learned planner. Changed policy roots decline.
- Companion library source/standalone **43 / 596**. Its learned-lookup control
  counts one pmap + five numeric Strings before, zero of each with views.
- Actual exporter integration independently recovered **350,000 local-Durable
  rows**, all physical fields/types/timestamps/multiplicities checked. Estimated
  managed allocation fell about **8.9%**, but the sequential pair does **not**
  establish a throughput or tail gain. No S3/live-app claim.
  [Exporter evidence](https://github.com/chucklehead-dev/jolt-otel-clickhouse/tree/a373c1b4a1c02c6972e64bb05fc7333836e82347/research/scalar-view-exporter-20261020).
- Claude read-only implementation review approved. Final CI/dependency/test-entry
  follow-up is reviewed separately; exact-head hosted checks must pass before merge.

The older optional no-argument cache gate assumes a global compiler helper
absent on stock Jolt; its failures reproduce on the untouched base. Its tests
were not weakened. This feature uses the independently supplied native folder.

Changelog included. Unsupported hosts still decline; callers must explicitly
select a compatible codec and keep inputs immutable.
