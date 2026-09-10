# Changelog

## Unreleased

- Test pull requests and `main` on hosted Linux with checksum-pinned Jolt 0.8.3,
  isolated reproducible caches, least-privilege permissions, and a bounded job.
- Preserve exactly-once SDK shutdown and post-shutdown rejection while retaining
  canonical typed instrumentation-scope attributes.
- Add deterministic, non-evaluating source inference for storage-neutral
  attribute schema hints. Dynamic and conflicting evidence remains explicit.
- Add one bounded, immutable OpenTelemetry AnyValue representation shared by
  attribute normalization and OTLP encoding/decoding. It preserves signed
  64-bit integers, explicit present-empty values, byte strings, mixed arrays
  and nested maps without string coercion. Ordinary application `nil` remains
  an invalid, dropped attribute for compatibility.
