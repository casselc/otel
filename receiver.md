# OTLP receiver seam

`otel.otlp.http-receiver/handler` is the reusable Ring boundary for an
OTLP/HTTP JSON trace, log, and metric receiver. It owns request policy but not
sockets, JSON parsing, threads, authentication, or storage. A caller supplies a
body parser that reports the actual encoded byte count and the matching signal
exporters; the resulting handler enforces `POST /v1/traces`, `POST /v1/logs`,
or `POST /v1/metrics`, JSON content type, the encoded-body cap, and bounded
concurrency. A host may supply a timeout wrapper for its own bounded executor.

`otel.otlp.trace-decode/decode-request` is the reusable data boundary beneath
the Ring adapter. It accepts an already parsed JSON-compatible map
with the JSON-Protobuf `ExportTraceServiceRequest` shape and returns:

```clojure
{:spans [...]             ; canonical immutable ended-span maps
 :rejected-spans 0        ; individual wire spans not accepted
 :errors []}              ; structured, path-aware diagnostics
```

Accepted values can be handed directly to an `otel.sdk.export/SpanExporter`.
The decoder preserves resource and instrumentation-scope grouping metadata on
each span, including schema URLs; identifiers, parents, trace state and flags;
timestamps, kind and status; attributes, events and links; and every dropped
count represented by those values. One bad span is rejected without discarding
valid sibling spans. Container errors reject the spans contained by that
container when they can be counted. Decoded span contexts use the same immutable
`otel.trace/SpanContext` value as SDK-ended spans. A default-zero dropped count
is omitted from resource, scope, event, and link maps, matching the SDK's
canonical shape; a positive wire count remains explicit.

`otel.otlp.signal-decode/decode-logs` and `decode-metrics` provide the equivalent
transport-neutral boundaries for the canonical log and metric models. The Ring
handler routes their accepted records or collections through the corresponding
SDK exporter protocols and reports signal-specific rejected counts.

Within the current canonical domain, SDK log records and gauge, sum, and
explicit-histogram collections compare equal before encoding and after decoding.
Absent correlation fields and gauge start times remain absent, and default-zero
dropped counts remain absent from resources, scopes, and logs. Positive wire
dropped counts remain explicit. SDK metric points explicitly carry zero flags;
decoded wire points preserve the distinction between an absent flag and a
present zero, and accept the complete unsigned 32-bit flag range. Positive
point attribute-loss counts survive as `:dropped-attributes-count`, while zero
remains absent. Points are identified only by their normalized attribute set.
When several measurements for one point report different loss counts, the SDK
retains the maximum: the diagnostic is monotone and order-independent without
incorrectly summing the same discarded attribute across measurements. A
malformed flag or point count rejects only its owning point.

This is not a universal round-trip claim. A nil or omitted application log body
retains the established empty-string representation. Representable log bodies
use the canonical AnyValue scalar, map, array, byte and explicit-empty arms;
arbitrary host objects, malformed values, and integers outside signed int64
retain the documented readable string fallback for compatibility. Metric
values outside signed int64,
non-finite values, and integer histogram boundaries are outside exact record
equality: OTLP constrains integer points and represents bounds as doubles.
Exemplars and exemplar reservoirs are not modeled by this SDK and remain
explicit receiver rejections rather than invented defaults.

Neither namespace is an HTTP server or JSON parser. The complete receiver
stack owns, in this order:

1. Authenticate and authorize the request before buffering or parsing it.
2. Accept only the three signal paths above with `POST` and
   `Content-Type: application/json` (ignoring media-type parameters); return
   `415` for other representations.
3. Enforce a configurable encoded-body cap before parsing. Start at 4 MiB. Do
   not enable compressed requests until decompression has its own output and
   ratio limits, so compression cannot bypass the cap.
4. Parse with bounded depth/collection/string limits and preserve OTLP `int64`
   and `uint64` decimal strings. Then call `decode-request` with the map.
5. Export accepted signal records using a caller-supplied exporter and map the
   decoder's rejected count to the matching standard partial-success field:
   `rejectedSpans`, `rejectedLogRecords`, or `rejectedDataPoints`.
   Receiver/exporter failures remain HTTP failures rather than partial success.
6. Bound concurrency and exporter time. `wrap-suppress-receiver-telemetry` and
   `telemetry-suppressed?` form the explicit outer-middleware contract that
   prevents an ingest-observe-ingest feedback loop.

The decoders do not accept a JSON string because this project has no explicit
safe JSON-reader dependency. The shared AnyValue decoder preserves strings,
Booleans, signed int64 values, doubles (including the protobuf JSON special
spellings), explicit empty values, bytes, arrays, and nested string-key maps.
It rejects multiple oneof arms, invalid base64 or int64 values, duplicate keys,
and values that exceed the canonical depth, node, or byte limits. Signal
decoders reject the owning record or data point without discarding valid
siblings and retain path-aware diagnostics for partial success.

The representative fixture at `test/fixtures/otlp/traces-v1.edn` is pinned to
OpenTelemetry protobuf commit
`dfd0b0e8974eac54c4d99a84a86d6098e11ad1bd`. The vendored OTLP specification
in `index.md` links the v1.11.0 protobuf definitions; the exact fixture commit,
not a moving branch, is the provenance for the checked trace shape.
