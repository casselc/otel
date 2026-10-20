# Explicit-default AnyValue normalization

The OTLP decoder supplies normalized options explicitly. That previously missed
the existing one-argument scalar shortcut and rebuilt default option maps and
generic traversal state. Reuse the same scalar policy when both the supplied
normalized options and current defaults identify the immutable original map.
Structured/special values, custom budgets and changed defaults remain generic.
Known policy errors still pass through the existing nonthrowing result boundary.

## Measured pinned-consumer screen

Four fresh processes ran in baseline/candidate/candidate/baseline order on
official stock Jolt 0.8.23 and Chez 10.4.1. Each decoded a preconstructed 512-span
OTLP map: two warmups and 12 samples for each of two ID shapes, alternating
their order. This excludes JSON parsing, HTTP, exporter and Durable storage.

| Process | Padded p50 ms | p90 ms | p95/p99 ms | Estimated bytes/span |
|---|---:|---:|---:|---:|
| 1 baseline | 110.50 | 119.48 | 181.41 | 169,050 |
| 2 candidate | 75.47 | 109.55 | 246.65 | 94,270 |
| 3 candidate | 75.78 | 103.85 | 157.80 | 94,280 |
| 4 baseline | 110.59 | 126.73 | 172.48 | 169,049 |

Estimated allocation falls about 44%, with about 32% lower median decoder time.
Candidate process 2 has a worse maximum than either baseline: no tail improvement
is claimed. Twelve samples make p95 and p99 the sample maximum, not production
tail estimates. The nonzero-prefix ID variant shows the same allocation reduction
and lower medians; full samples and CPU/GC deltas are in the adjacent EDN files.

Allocation estimates combine coarse live-heap and GC-byte deltas. Separate
counter reads are not atomic; these are not per-object allocation profiles.
Each sample checks accepted/rejected counts, absence of errors, false-valued
attributes and the expected first trace ID. This is not whole-field equivalence
or persistence verification; semantic regression tests are separate.

### Provenance and reproduction

- Oscope benchmark fixture: `dfb2f6788df1189ccff5600d17ad034da4246571`,
  `benchmark/oscope/socket_ingest.clj` in `chucklehead-dev/oscope`.
- Baseline SDK: `casselc/otel@26186ce79941e84e0cc67eb148dd8dd43b4e1880`.
- Candidate: the dirty two-file prototype in
  `/home/chuck/ai-src/worktrees/otel-default-scalar-options-20261020`.
  `sources.sha256` and `source-after.log` attest the measured production source
  and script did not change across the campaign. Each EDN records actual source URIs.
- Stock executable SHA256:
  `7bf7acae4b4bcb5c27b3651fa4a94d2d4e6a7fc43b5b821def2f8be3d6f00e94`.

From that Oscope fixture checkout, run the adjacent script through the workspace
Chez wrapper and stock executable. For candidate processes add:

```clojure
-Sdeps '{:paths ["src" "benchmark"]
         :deps {io.github.casselc/otel
                {:local/root "/home/chuck/ai-src/worktrees/otel-default-scalar-options-20261020"}}}'
```

Use `JOLT_AOT_CACHE=0`, and baseline processes without the SDK override. Preserve
the four-process ordering and capture stdout to distinct EDN files. No local
review, native build or other timed campaign ran concurrently with this screen.

## Main-based extraction

The production change and focused tests are now independently extracted onto
`casselc/otel` main `ecce82c427be527b8778cf8dd26a86ff9e22d9e6`.
This extraction excludes the pinned branch's native payload-cache helpers and
portable catch change. The initial focused gate passed 29 tests / 538 assertions;
a deterministic shortcut-versus-generic mechanism control was added afterward.
The pinned-consumer timings above do not claim a measurement of this main tree
or an end-to-end ingestion improvement. Application integration and independent
Durable recovery must be rerun with the final published SDK pin.
