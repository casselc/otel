# Changelog

## Unreleased

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
  meter, and logger acquisition. Dynamic and conflicting evidence remains
  explicit.
- Add one bounded, immutable OpenTelemetry AnyValue representation shared by
  attribute normalization and OTLP encoding/decoding. It preserves signed
  64-bit integers, explicit present-empty values, byte strings, mixed arrays
  and nested maps without string coercion. Ordinary application `nil` remains
  an invalid, dropped attribute for compatibility.
