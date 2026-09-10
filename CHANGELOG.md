# Changelog

## Unreleased

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
