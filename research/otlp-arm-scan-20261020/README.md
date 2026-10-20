# Bounded OTLP AnyValue arm scan

Each wire AnyValue selects zero or one of seven fixed fields. The previous
decoder allocated a predicate closure and lazy filtered sequence to classify
them. The candidate uses a fixed-order scan over shared immutable field names.
It still visits all seven fields before reporting a conflict, then uses the
same selected-value lookup, type checks and normalization. Unknown fields are
not traversed: classification stays bounded independently of object size.

## Checks

Official stock Jolt 0.8.23 / Chez 10.4.1:

- Focused AnyValue and trace decoder namespaces: 32 tests / 1,835 assertions.
- Full SDK suite: **474 tests / 5,472 assertions**, zero failures/errors.
- Complete-result comparison against the original lazy selector for all 128
  subsets of fields, with keyword/string keys and five budget configurations,
  plus conflicting key spellings, false, null, sentinel and nested values.
- A controlled replacement of `clojure.core/filter` throws if the old mechanism
  is used. The new scalar decode succeeds without it. The retained legacy oracle
  still uses the real old filter; normal type/budget guards are not bypassed.

These are parser regression controls, not a new concurrency/lifecycle model or
proof of all malformed inputs. No queues, acknowledgement rules, persistence
boundaries, private runtime layouts, compiler patches or library dependencies change.

## Decoder-only performance

Two fresh processes ran baseline/candidate, followed by two fresh processes in
reverse order. Each alternated two 512-span fixtures with two warmups and 12
measured samples per fixture. Normalizer and trace-decoder source files compare
byte-for-byte; only wire AnyValue classification differs. The added source URI
metadata identifies all three selected SDK modules. Source hashes match after
each pair. No local build, test suite or Claude review ran during timed windows.

| Order / process | Padded p50 ms | Padded p90 ms | Padded p95/p99 ms | Estimated bytes/span |
|---|---:|---:|---:|---:|
| Forward baseline | 66.61 | 101.47 | 113.16 | 94,269 |
| Forward candidate | 54.49 | 73.45 | 77.70 | 82,024 |
| Reverse candidate | 52.04 | 57.51 | 59.24 | 82,022 |
| Reverse baseline | 80.92 | 126.41 | 143.54 | 94,269 |

| Order / process | Nonzero-prefix p50 ms | p90 ms | p95/p99 ms | Estimated bytes/span |
|---|---:|---:|---:|---:|
| Forward baseline | 68.09 | 95.74 | 117.38 | 91,040 |
| Forward candidate | 52.59 | 87.76 | 94.00 | 78,786 |
| Reverse candidate | 51.15 | 55.80 | 124.01 | 78,783 |
| Reverse baseline | 67.92 | 107.11 | 108.51 | 91,041 |

Estimated allocation drops about **13%** in both shapes and both orders. Candidate
medians are lower in both orders; nonzero-prefix medians improve about 23–25%
with nearly stable baseline medians. Padded baseline medians drift about 21%:
do not turn those cells into a precise general speedup. The reverse nonzero-prefix
candidate has a worse maximum than its baseline. No tail improvement is claimed;
with 12 samples, p95/p99 are maxima, not population-tail estimates.

Allocation estimates combine non-atomic coarse live-heap and GC-byte counters,
not object profiling. Full samples retain CPU/GC deltas. This screen starts with
an already-parsed OTLP map: it excludes JSON, HTTP, exporter and Durable work,
and does not qualify application throughput or the direct-SDK 25k/20k targets.
Each decode checks acceptance counts, no errors, false attribute retention and
the first trace ID. Full regression or persistence equivalence is not inferred
from those timing-loop assertions.

## Provenance and reproduction

- Baseline main tree: `64966fd9ac26953596561134bbfa2b32382b6a6f`, shared by
  `e19c1badd1b525946502cb1984d1a717a62fd050` and merged main `5381ecc0a0b9bc74697d280c81a4498140095ce3`.
- Candidate implementation: `b4368de9fdd3993137b74fc6a096bb0d24f4b5be`.
- Fixture: `chucklehead-dev/oscope@a9345e91393f754a7c91b99851894df60b88144b`,
  `benchmark/oscope/socket_ingest.clj`; no fixture body differs between arms.
- Runtime SHA256: `7bf7acae4b4bcb5c27b3651fa4a94d2d4e6a7fc43b5b821def2f8be3d6f00e94`.

Run the adjacent `run-pair.sh NEW_ABSOLUTE_DIR forward` and then another new
directory with `reverse`, from that Oscope fixture checkout. The runner records
exact local source roots, runtime hash and source hashes, then checks all selected
module URIs. Its `task_script` points at the equivalent workspace copy of
`profile.clj`; adapt explicit paths to your checkouts, not runtime/compiler policy.
Raw reports are under `forward/` and `reverse/` here. Recompute the compact table
with `bb research/otlp-arm-scan-20261020/summarize.clj`. No stores or large binary
artifacts are produced by this screen.
