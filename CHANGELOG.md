# Changelog

## Unreleased

- Add deterministic, non-evaluating source inference for storage-neutral
  attribute schema hints. Dynamic and conflicting evidence remains explicit.
- Add one bounded, immutable OpenTelemetry AnyValue representation shared by
  attribute normalization and OTLP encoding/decoding. It preserves signed
  64-bit integers, explicit present-empty values, byte strings, mixed arrays
  and nested maps without string coercion. Ordinary application `nil` remains
  an invalid, dropped attribute for compatibility.
