# Changelog

## Unreleased

- Ignore non-numeric or non-finite histogram measurements before attribute
  normalization or series mutation, preserving convertible finite numbers and
  existing negative-value behavior (Refs #3). This histogram-only guard does
  not guarantee against aggregate overflow from multiple finite measurements.

- Parse ratio-sampler trace-ID halves using two signed-Long-safe 32-bit chunks
  and exact reconstruction, preserving all signed 64-bit sampling decisions.
  This removes reliance on permissive out-of-range `Long/parseLong` behavior;
  ratio formulas and endpoint semantics are unchanged.
  Malformed low halves, including internal chunk signs, reject with a fixed
  safe diagnostic; this is not an expanded full trace-ID validation policy.

- Canonicalize histogram boundaries to finite doubles before instrument
  registration and reject unordered, duplicate or precision-colliding bounds
  without publishing an instrument. Nil retains the default boundaries and an
  explicit empty vector retains one bucket. This aligns SDK collection with
  the OTLP explicit-bound wire domain (existing metric-domain work in #3).

- Allow tracer providers to use an explicit ID generator for deterministic
  replay while retaining OS entropy by default. Generated root trace IDs carry
  per-ID random provenance, so the W3C random flag is set only for random IDs;
  children inherit their parent's provenance, and invalid IDs fail before
  sampling or context construction. The no-OpenSSL uniqueness fallback now
  depends explicitly on supported host time and fails closed if it is
  unavailable rather than substituting a repeatable zero seed.

- Preserve metric data-point flags as exact unsigned 32-bit values, including
  the distinction between an absent flag and an explicitly present zero, and
  carry positive `droppedAttributesCount` values across SDK gauge, sum, and
  explicit-histogram records plus OTLP JSON encode/decode. SDK measurement
  attributes are normalized and counted once per measurement, and aggregation
  remains keyed only by the normalized attribute set. If merged measurements
  report different loss counts, the point retains their maximum rather than
  fragmenting the series or summing a repeated diagnostic. Malformed inbound
  metadata rejects only that point. Exemplars and reservoirs remain outside
  this slice, and no ClickHouse projection is added.

- Preserve log-record attribute losses reported by the shared bounded
  normalizer as canonical `droppedAttributesCount` across direct SDK export and
  OTLP JSON relay. Zero remains omitted, recursive value truncation is not
  misreported as a dropped top-level attribute, and caller-provided internal
  count metadata remains untrusted.

- Make owned batch shutdown promptly cancellable without changing force-flush:
  after retiring admission, each span/log processor or metric reader grants a
  250 ms cooperative grace, interrupts only its own worker while that worker
  still owns exporter I/O, and requires termination within a further 2,000 ms
  before exporter close. Healthy sibling workers are not interrupted, bounded
  join failure leaves the exporter open, and an interrupted OTLP POST remains a
  failed non-replayed delivery with per-destination lifecycle results. Accepted
  span batches behind a cancelled in-flight export are counted as attempted
  failures without starting another request; other batch pipelines likewise
  start no new export. Log and metric readers persist an owned export failure
  through shutdown so a successful close cannot replace the failed delivery.
  A sent owned interrupt is conservatively a failed destination result even
  when the exporter clears interruption and returns true.
  Metric retirement now requires a worker-owned final collection, closing the
   race that could lose a measurement accepted after the last scheduled snapshot.

- Converge OTLP transport on `casselc/http-client`
  merge commit `eab6b78d5957f88690faf6768360572a3f185341`, whose parents are
  prior `main` `8e8f8f2268fd8625116f9b9a7e4766d65ffd218a` and reviewed provider
  head `89e2084598bd485dfc81268f67ee1cff5b723aa0`. The provider head descends
  from append-only integration merge `8f449006eb8c679755fa1dae6aeb933cfb51211c` of request-aspect
  fork parent `9cb5801e8c5929387715aa6713c33b2c21fd9a2a` and
  `jolt-lang/http-client` v0.0.10 parent
  `b98833b8338b66d435cdbffa480ba2b59c005a2e`. Samizdat-style consumer graph
  tests require one shared source root for every `jolt.http.*` namespace and a
  discoverable exact-one request manifest; a hermetic stalled peer verifies
  that thread interruption returns within the provider's read slice rather
  than the socket timeout. The minimum Jolt runtime is now 0.8.1. OTel's public
  API and OTLP wire behavior are unchanged.
  Complete framed TLS responses do not wait for shutdown, while incomplete or
  close-delimited responses reject a raw close without `close_notify`.

- Retain bounded scalar delivery counts for batch span processors so a failed
  background export remains visible through later per-destination force-flush
  and shutdown results. Healthy pipelines continue independently, and no
  exporter exception or transport content is retained.

- Add strict `:environment? false` closed configuration to the OTLP/HTTP span,
  metric, and log exporters. Closed exporters ignore ambient OTLP base and
  signal endpoints, headers, and timeout, while the default remains compatible
  with the standard environment-aware behavior.
- Add one-provider composition for named, independently bounded span pipelines,
  reusing the existing batch processor per destination. Per-destination flush,
  shutdown, queue and drop results preserve cleanup after failures, while the
  same canonical span identity and parentage fan out without coupling exporter
  latency or overflow. `init!` can own an explicit replacement span-processor
  sequence without coupling it to log or metric routing. Span exporter callbacks
  now run under generic instrumentation suppression. Lifecycle results redact
  thrown exporter details, and batch shutdown still joins its worker and closes
  its exporter after a failed flush.
- Preserve representable log bodies as canonical AnyValues across direct SDK
  export and OTLP JSON relay, including keyword and symbol strings, maps,
  arrays, byte strings, and explicit empty values. Empty maps and arrays stay
  distinct from empty strings and explicit empty values, while nil or omitted
  bodies retain the established empty-string behavior and malformed or
  unsupported values retain their readable compatibility fallback.
- Bound discovery EDN to a conservative shared budget of 64 open delimiters and
  discard prefixes before recursive parsing, rejecting core-only reader macros
  and tagged values while preserving sets and namespaced maps. Also enforce
  aggregate character, UTF-8 byte, decoded-entry and evidence budgets across all
  indexes and selected fragments. Over-budget discovery stops before later
  resource reads or schema merging with redacted diagnostics.
- Add a literate Quint model and shared runtime trace vocabulary for batch-span,
  periodic-metric, and batch-log worker ownership. Corrected traces require
  worker termination before the exactly-once exporter-close call, retain
  ownership after an interrupted wait, share close failures across callers,
  reject post-shutdown owner work (enqueue for spans/logs and
  collection/export for metrics), exercise real signal workers, and kill timed-join,
  swallowed-wait, late-export, and double-close mutants in a path-sensitive CI
  gate.
- Add explicit `otel.attribute-schema.index/v1` discovery with canonical
  package, repository, immutable revision/version, resource path and SHA-256
  identity. Deterministic artifact ordering, include/exclude selection,
  collision and digest rejection, pinned semantic-convention checking, and
  privacy-safe diagnostics produce a pure `otel.attribute-schema.bundle/v1`
  build artifact while retaining unknown evidence and per-artifact provenance.
  A bounded exact-one-value `read-bundle` / `validate-bundle` / `render`
  boundary supports canonical persisted build artifacts without claiming new
  digest attestation.
- Add a canonical `otel.semantic-conventions/v1` registry pinned to official
  OpenTelemetry semantic conventions v1.44.0 source files, with exact commit and
  SHA-256 provenance. Its checker rejects concrete standard resource/exception
  type mismatches without widening int64, while dynamic and unregistered
  evidence retains the existing fallback behavior and sanitized diagnostics do
  not disclose source paths or observed values.
- Align decoded log and metric shapes with their canonical SDK exporter inputs:
  absent log correlation fields, gauge start time, and default-zero resource,
  scope, or log dropped counts stay absent, while positive counts survive.
  Focused fixtures cover representable log bodies, all AnyValue attribute arms,
  and gauge, sum, and explicit-histogram collections.
- Make decoded trace records match the canonical SDK-ended span shape while
  preserving typed AnyValue distinctions. Span and link contexts use the shared
  immutable context value, default-zero nested dropped counts stay absent, and a
  malformed typed sibling is rejected without losing a valid span.
- Converge the direct and HTTP-client-transitive `jolt-crypto` dependency on the
  canonical upstream repository and reviewed Jolt 0.8 revision, with a selected
  graph guard for local and hosted cache layouts.
- Test pull requests and `main` on hosted Linux with checksum-pinned Jolt 0.8.3,
  isolated reproducible caches, least-privilege permissions, and a bounded job.
- Make batch span, periodic metric, and batch log shutdown wait for actual worker
  termination before releasing the exporter. An interrupted worker wait is now
  a shared terminal failure and never silently permits exporter shutdown.
- Preserve exactly-once SDK shutdown and post-shutdown rejection while retaining
  canonical typed instrumentation-scope attributes.
- Add deterministic, non-evaluating source inference for storage-neutral
  attribute schema hints, including instrumentation-scope attributes at tracer,
  meter, and logger acquisition. Calls respect namespace aliases, referred vars,
  lexical shadowing, destructuring, and source-order definitions. Dynamic and
  conflicting evidence remains explicit.
- Add one bounded, immutable OpenTelemetry AnyValue representation shared by
  attribute normalization and OTLP encoding/decoding. It preserves signed
  64-bit integers, explicit present-empty values, byte strings, mixed arrays
  and nested maps without string coercion. Ordinary application `nil` remains
  an invalid, dropped attribute for compatibility.
