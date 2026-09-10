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

A focused trace relay fixture compares a canonical SDK-ended span directly with
the record produced by `traces-request` followed by `decode-request`, including
every value arm above and the absent-key distinction. Default-zero dropped
counts on resources, scopes, events, and links stay absent in both records;
positive counts remain explicit and survive the wire path.

Equivalent focused fixtures cover SDK-ended logs with representable scalar
bodies and SDK gauge, sum, and explicit-histogram collections. Their attribute
maps use this complete value algebra. The record-level qualification in
`receiver.md` documents fields outside exact log or metric equality; those
limitations do not cause attribute values to be stringified or inferred.

This representation follows the OpenTelemetry common attribute model and the
OTLP JSON protobuf mapping. Safety bounds are an SDK policy layered on that wire
model. The representative trace fixture is pinned to opentelemetry-proto commit
`dfd0b0e8974eac54c4d99a84a86d6098e11ad1bd`; `index.md` carries the vendored
OTLP text and links the v1.11.0 protobuf definitions.

Static type inference, generated ClickHouse schemas, schema widening/evolution,
and oscope query or UI changes are separate later phases. This runtime contract
is the validation and fallback layer those features can consume; it does not
infer a schema by observing values.

## Source-inferred hints

`otel.attribute-schema` can inspect explicitly listed Clojure source files and
return a deterministic, storage-neutral EDN fragment. It recognizes exact OTel
API calls, literal attribute maps and keys, the canonical byte and empty-value
constructors, and ordinary scalar casts. It also distinguishes attributes on
spans, metrics, logs, and resources from instrumentation-scope attributes passed
to `get-tracer`, `get-meter`, and `get-logger`. Dynamic forms remain visibly
unknown; conflicting evidence is retained rather than widened or guessed.
Dynamic acquisition option maps are likewise recorded as unknown scope evidence,
including computed option keys that could resolve to `:attributes` at runtime.

```sh
jolt -M -m otel.attribute-schema.main --root . src/my/app.clj \
  > target/META-INF/otel/attribute-schema/my-app.edn
```

The analyzer does not evaluate source, mutate macro-expansion state, infer a
ClickHouse type, or apply a database migration. Source paths in the artifact are
project-relative, and there are no timestamps or checkout paths, so running the
same analysis twice produces byte-identical EDN. The initial analyzer is
deliberately shallow: helper-built or otherwise dynamic maps need an explicit
future declaration or remain unknown. Calls resolve through namespace aliases,
referred vars, source-order top-level definitions, and lexical bindings in the
standard `let`/`loop`, conditional binding, `fn`/`defn`, `letfn`, and
comprehension forms. Destructured locals shadow referred vars; a qualified alias
remains a namespace reference, matching Clojure call resolution. The analyzer
does not expand arbitrary user macros, so it does not claim evidence for calls
that exist only after macro expansion. Bare auto-resolved keyword keys (`::key`)
are reported as dynamic. Alias-qualified auto-resolved keys (`::alias/key`) are
unsupported and fail closed with a sanitized diagnostic: resolving them
correctly requires the compiler's namespace environment, which this source-only
analyzer deliberately does not invent.

The source list and `--root` are trusted build inputs, not a filesystem sandbox.
Each source name is validated before any read and cannot be absolute or contain
`.` or `..` components, but the portable CLI does not resolve or police symlinks
inside the supplied root.

## Pinned semantic conventions

`otel.semantic-conventions` checks an existing `otel.attribute-schema/v1`
fragment against a small immutable registry of standard resource and exception
attributes that this library emits. The registry is
`resources/otel/semantic-conventions.edn`; it pins OpenTelemetry semantic
conventions v1.44.0 at full commit
`e10a930844c6951757a43b849d364f7d056ac32b` and records the SHA-256 digest of
each upstream registry source file used to transcribe its entries.

The checker requires exact type equality at a registered signal/location.
In particular, an `:int64` convention is never widened to `:double`. A known
literal, constructor, or cast with a conflicting type fails with deterministic,
sanitized exception data. Dynamic values, unregistered keys (including this
library's `exception.data` extension), and uses outside a registered location
remain on the existing fallback path. Conflicts between literals for an
unregistered key remain visible in the original fragment exactly as before.

The registry contains metadata only: it does not inspect observed attribute
values, environment variables, host details, or absolute paths, and checking
does not mutate a runtime schema. Build-resource discovery, advice-pack
discovery, database schema generation and schema installation remain separate
later phases.
