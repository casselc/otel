# Attribute values

OpenTelemetry attributes are not limited to strings. This library normalizes
attribute values once, when they enter the SDK, so every signal and OTLP path
sees the same immutable value.

| Application value | Canonical value | OTLP field |
| --- | --- | --- |
| string | string | `stringValue` |
| boolean | boolean | `boolValue` |
| signed 64-bit integer | integer | `intValue` (a decimal JSON string) |
| floating-point number | double | `doubleValue` |
| `otel.any-value/empty-value` | present-empty sentinel | no oneof field |
| `(otel.any-value/bytes [0 255])` | immutable byte string | `bytesValue` |
| sequential collection | vector, recursively normalized | `arrayValue` |
| map with string-like keys | sorted string-key map, recursively normalized | `kvlistValue` |

Keywords and symbols are converted to strings. Sets, functions, integers
outside the signed 64-bit range, invalid map keys and other unsupported host
objects are dropped at the attribute boundary. A bad instrumentation value does
not change the observed application's return value or exception behavior.

An ordinary application `nil` keeps the established SDK behavior: the attribute
is invalid and is dropped. `otel.any-value/empty-value` means **present but
empty**. It is different from omitting the key, and also different from `""`,
`[]` and `{}`. A received OTLP AnyValue with no selected oneof field decodes to
the same explicit sentinel.

## Byte strings

Host byte arrays are mutable and differ between Jolt and the JVM. Construct an
OpenTelemetry byte string explicitly:

```clojure
(require '[otel.any-value :as any])

(any/bytes [0 1 254 255])
```

The constructor copies the octets into an immutable vector. Two byte strings
with the same octets compare as equal. Each octet must be an integer from 0 to
255.

## Determinism and limits

Map keys may be strings, keywords or symbols. They are converted to strings and
sorted. If two source keys produce the same string, every conflicting entry is
dropped; map traversal order never chooses a winner.

Other key types are dropped rather than converted with `str`. When a count limit
is reached, the lexicographically first valid canonical keys win; this makes the
result independent of hash-map traversal order.

Normalization is bounded by depth, node and byte budgets. The default 1 MiB
budget charges four bytes for every observed string character, a conservative
cross-host upper bound for UTF-8. String and byte-string length limits apply
recursively. The defaults are:

```clojure
{:count-limit 128
 :value-length-limit nil
 :max-depth 16
 :max-nodes 1024
 :max-bytes 1048576}
```

`otel.attributes/normalize` retains the existing map-only API. Diagnostic code
can call `otel.attributes/normalize-result` to receive the canonical attributes,
dropped and truncated counts, and at most sixteen structured errors.

The shared `otel.otlp.any-value` codec is used by trace, log and metric receiver
paths and by attribute encoding. OTLP special double spellings (`NaN`,
`Infinity`, and `-Infinity`) and base64 byte strings round-trip without a
string fallback. Log bodies retain their older human-readable fallback for
arbitrary Clojure objects; this contract applies to attribute collections.

This representation follows the OpenTelemetry common attribute model and the
OTLP JSON protobuf mapping. Safety bounds are an SDK policy layered on that wire
model.

Static type inference, generated ClickHouse schemas, schema widening/evolution,
and oscope query or UI changes are separate later phases. This runtime contract
is the validation and fallback layer those features can consume; it does not
infer a schema by observing values.
