# OTLP trace receiver seam

`otel.otlp.trace-decode/decode-request` is the reusable data boundary for an
eventual OTLP/HTTP receiver. It accepts an already parsed JSON-compatible map
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
container when they can be counted.

This namespace is deliberately not an HTTP server and not a JSON parser. The
next receiver layer owns, in this order:

1. Authenticate and authorize the request before buffering or parsing it.
2. Accept only `POST /v1/traces` and `Content-Type: application/json` (ignoring
   media-type parameters); return `415` for other representations.
3. Enforce a configurable encoded-body cap before parsing. Start at 4 MiB. Do
   not enable compressed requests until decompression has its own output and
   ratio limits, so compression cannot bypass the cap.
4. Parse with bounded depth/collection/string limits and preserve OTLP `int64`
   and `uint64` decimal strings. Then call `decode-request` with the map.
5. Export accepted spans using a caller-supplied exporter and map
   `:rejected-spans` to the standard JSON response
   `{"partialSuccess":{"rejectedSpans":"N","errorMessage":"..."}}`.
   Receiver/exporter failures remain HTTP failures rather than partial success.
6. Bound concurrency and exporter time. Exclude or separately mark the receiver
   endpoint's own telemetry to prevent an ingest-observe-ingest feedback loop.

The initial decoder does not accept a JSON string because this project has no
explicit safe JSON-reader dependency. It also rejects AnyValue `kvlistValue`
and `bytesValue`, heterogeneous or nested arrays, duplicate attribute keys,
non-finite doubles, malformed trace state, and invalid/all-zero IDs. Those
values cannot be represented losslessly by the SDK's current canonical
attribute model; rejecting the owning span is safer than silently changing it.

The representative fixture at `test/fixtures/otlp/traces-v1.edn` is pinned to
OpenTelemetry protobuf commit
`dfd0b0e8974eac54c4d99a84a86d6098e11ad1bd`.
