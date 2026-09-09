# Changelog

## Unreleased

- Add one bounded, immutable OpenTelemetry AnyValue representation shared by
  attribute normalization and OTLP encoding/decoding. It preserves signed
  64-bit integers, explicit present-empty values, byte strings, mixed arrays
  and nested maps without string coercion. Ordinary application `nil` remains
  an invalid, dropped attribute for compatibility.
