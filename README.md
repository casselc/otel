# otel

An [OpenTelemetry](https://opentelemetry.io) implementation for
[Jolt](https://github.com/jolt-lang/jolt) — the API instrumentation is written
against, and the SDK that records and exports it.

All three signals are implemented — traces, metrics and logs — with OTLP/HTTP
export over http and https, W3C context propagation, runtime metrics read
straight off Chez Scheme's collector, and a `clojure.tools.logging` bridge that
correlates log lines with the span they were written inside.

## Dependencies

| Library | Why |
| --- | --- |
| [casselc/http-client](https://github.com/casselc/http-client) | OTLP transport, including TLS |
| [jolt-lang/jolt-crypto](https://github.com/jolt-lang/jolt-crypto) | the OpenSSL (`libssl`/`libcrypto`) declarations TLS needs |
| [jolt-lang/logging](https://github.com/jolt-lang/logging) | `clojure.tools.logging`, for the logs bridge |

All three are git coordinates in `deps.edn`; https also needs the system OpenSSL
(`brew install openssl@3` on macOS, the distro `libssl3` on Linux).
The direct crypto coordinate deliberately matches the HTTP client's transitive
URL and full revision, so Jolt selects one canonical checkout rather than loading
the same native providers from fork and upstream identities. CI checks the
selected graph under both known local and hosted cache layouts.

## Requirements

jolt v0.8.1 or newer. The telemetry implementation uses the `jolt.host`
primitives (`wall-nanos`, `mono-nanos`, and the gc and memory counters) and its
HTTP/TLS dependencies use Jolt 0.8's value-first FFI write API and the executor
interfaces added in 0.8.1.

The SDK checks at startup and says so plainly if the primitives are missing. It
will not silently fall back to a millisecond clock: that is the exact defect the
two-clock design exists to avoid, and a quiet degradation would make every span
duration wrong in a way nothing downstream could detect.

The HTTP provider is `casselc/http-client` at
`e77cba3bc4ea0e421d4e107383ba09ffa4294837`. It descends from integration merge
`8f449006eb8c679755fa1dae6aeb933cfb51211c`, whose parents are
`9cb5801e8c5929387715aa6713c33b2c21fd9a2a` (the request-aspect, relative
redirect, and canonical-provider lineage) and
`b98833b8338b66d435cdbffa480ba2b59c005a2e` (`jolt-lang/http-client` v0.0.10,
including interruptible reads and its later framing, byte-pipeline, address,
and pollfd fixes). The head adds fail-closed compiler/cardinality and
wait-boundary cancellation gates, and accepts only TLS `close_notify` as clean
EOF while allowing complete Content-Length, chunked, and bodyless responses to
finish without reading a later raw transport close.

Consumers that already declare `jolt-lang/http-client` must move that coordinate
to the same casselc URL and exact SHA when enabling OTel. Do not retain the old
upstream checkout or downgrade to OTel's former fork revision: either produces
two possible sources for the same `jolt.http.*` namespaces, and the latter loses
prompt blocked-read cancellation. The `:consumer-resolution` alias in this
repository is the executable model of that migration.

Pull requests and `main` are tested on hosted Linux with the released Jolt
v0.8.3 binary. The workflow pins the Jolt source revision, installer checksum,
release archive checksum, and checkout action revision; it verifies the runtime
version and prints `-Srepro -Sdescribe` before running the complete suite in a
repository-local cache. The matching provenance record lives in
`resources/otel/ci-toolchain.edn` and is enforced by the test suite.

## Install

```clojure
;; deps.edn
{:deps {io.github.jolt-lang/otel {:git/tag "v0.1.0" :git/sha "bbbf33d"}}}
```

A `:git/sha` must be the full 40-character sha, or a prefix alongside a
`:git/tag` as above — jolt rejects a bare abbreviated sha.

## Use

```clojure
(require '[otel.sdk :as sdk]
         '[otel.trace :as trace]
         '[otel.metrics :as metrics])

;; Once, at startup. Reads the standard OTEL_* environment variables.
(def otel (sdk/init! {:service-name "checkout"}))

(def tracer (sdk/tracer "checkout.http"))

(trace/with-span [sp tracer "GET /cart" {:kind :server}]
  (trace/set-attribute! sp :http.route "/cart")
  (trace/add-event! sp "cache.miss" {:key "user:42"})
  (handle-request))

;; Before the process exits — a batch processor is still holding spans.
(sdk/shutdown! otel)
```

Shutdown rejects new processor/reader work, drains accepted work, waits for each
background worker to terminate, and only then shuts down its exporter. For
spans and logs that means no new enqueue; an existing metric instrument may
still record locally, but its shut-down reader rejects later collection/export.
Shutdown has no separate
worker-wait timeout: exporter-specific timeouts or cancellation own any bound
on exporter work, while treating an arbitrary join timeout as quiescence could
close an exporter still in use. If the waiting thread is interrupted, every
concurrent or later shutdown caller observes the same failure and the exporter
remains open.

The executable [shutdown ownership model](formal/quint/shutdown-lifecycle.md)
documents the shared worker/exporter state machine, its abstraction boundary,
and the causal mutation controls used to keep this ordering check meaningful.

Attribute values keep their OpenTelemetry types, including nested maps and
arrays, byte strings and an explicit present-empty value. Invalid values are
dropped without escaping into the instrumented application. See
[Attribute values](any-values.md) for the supported values and safety limits.
Builds that consume source-inferred schema fragments from dependencies or local
advice packs can use the explicit, digest-checked process in
[Attribute-schema artifact discovery](attribute-schema-discovery.md), including
the bounded `render` / `read-bundle` boundary for persisted build artifacts.

`with-span` makes the span current for the body, ends it on the way out, and on
a throw records the exception and sets the span's status to `:error` before
rethrowing. A span started inside another automatically becomes its child.

Metrics work the same way:

```clojure
(def meter (sdk/meter "checkout"))
(def requests (metrics/counter meter "http.server.requests" {:unit "{request}"}))
(def latency  (metrics/histogram meter "http.server.duration" {:unit "ms"}))

(metrics/add! requests 1 {:route "/cart"})
(metrics/record! latency 42.0 {:route "/cart"})
```

### Without an SDK

Every API operation has a working no-op, so a library can instrument itself
unconditionally. `(sdk/tracer "my.lib")` returns a no-op tracer when no SDK has
been installed, and `with-span` around it costs a protocol dispatch. A library
should never call `init!` — that is the application's decision.

## Distributed tracing

Inject the active context into outgoing requests and extract it from incoming
ones. The wire format is W3C Trace Context, so traces join up across services in
any language.

```clojure
(require '[otel.propagation :as propagation])

;; outgoing
(http-post url {:headers (propagation/inject-current {})})

;; incoming
(let [ctx (propagation/extract-context (:headers request))]
  (trace/with-span [sp tracer "GET /cart" {:kind :server :parent ctx}]
    ...))
```

`otel.baggage` carries application key/value pairs along the same path. Baggage
crosses trust boundaries in a plain header — every hop can read and modify it,
so nothing sensitive belongs in it.

## Configuration

`init!` options, each falling back to the standard environment variable:

| Option | Environment variable | Default |
| --- | --- | --- |
| `:service-name` | `OTEL_SERVICE_NAME` | `unknown_service:jolt` |
| `:sampler` | `OTEL_TRACES_SAMPLER`, `OTEL_TRACES_SAMPLER_ARG` | parent-based, always on |
| `:endpoint` | `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4318` |
| `:headers` | `OTEL_EXPORTER_OTLP_HEADERS` | none |
| `:exporter` | — | `:otlp` (also `:console`, `:json`, `:none`, or an exporter instance) |
| `:processor` | — | `:batch` (also `:simple`) |
| `:span-processors` | — | optional replacement tracing processor sequence |
| `:metrics?` / `:runtime-metrics?` | — | true |
| `:metric-interval-ms` | — | 60000 |
| `:logs?` / `:bridge-logging?` | — | false / true |
| `:insecure?` | — | false (skip TLS verification) |
| — | `OTEL_SDK_DISABLED=true` | installs nothing |
| — | `OTEL_RESOURCE_ATTRIBUTES` | merged into the resource |

An `:exporter` may also be an exporter instance, which is used for whichever
signals it implements — handing `init!` a `memory/exporter` collects spans in a
test without metrics reaching the network. An unrecognised value is an error
rather than a silent fall back to OTLP.

Samplers, processors and exporters can also be built directly and passed to
`otel.sdk.tracer/tracer-provider` when `init!` is too opinionated.

The direct OTLP span, metric, and log exporter constructors accept
`:environment? false` for closed configuration. In that mode they do not read
any `OTEL_EXPORTER_OTLP_*` endpoint, signal-endpoint, header, or timeout
variables: supplied options win without ambient headers being added, and
omitted values use the library defaults. Omitting `:environment?` (or setting it
to `true`) retains the standard environment-aware behavior. The option must be
a boolean when present.

### Independent span destinations

One tracer provider can send the same canonical spans to several independently
bounded destinations. `independent-batch-pipelines` composes the existing batch
processor rather than adding another queue: each destination has its own worker,
queue capacity, batch size, schedule, drop count and exporter lifecycle.

```clojure
(require '[otel.sdk.export :as export]
         '[otel.sdk.tracer :as sdk-tracer])

(def pipelines
  (export/independent-batch-pipelines
    {:local  {:exporter local-exporter
              :config {:max-queue-size 2048}}
     :remote {:exporter remote-exporter
              :config {:max-queue-size 512
                       :schedule-delay-ms 1000}}}))

(def provider
  (sdk-tracer/tracer-provider
    {:processors [pipelines]}))

;; Detailed lifecycle results retain the caller's destination names. Every
;; destination is attempted even if an earlier one returns false or throws.
(export/force-flush-pipelines! pipelines)
;; => {:local {:ok? true}, :remote {:ok? true}}

(export/shutdown-pipelines! pipelines)
(export/pipeline-stats pipelines)
;; => {:local {:queue-size 0
;;             :attempted-span-count 1
;;             :exported-span-count 1
;;             :failed-span-count 0
;;             :dropped-count 0}, ...}
```

Ending a span only admits it independently to the bounded queues, so exporter
latency in one destination does not enter the application path or stop another
worker. A full destination queue drops only that destination's copy. Export,
flush and shutdown callbacks run under generic instrumentation suppression to
avoid recursively observing exporter work. The ordinary provider
`force-flush!` and `shutdown!` methods still return one aggregate boolean; keep
the `pipelines` value when per-destination results or queue diagnostics matter.
Failed lifecycle results use only safe `:returned-false` or `:threw` markers;
raw exporter exceptions are never returned because they may contain credentials.
Background export failures are retained as scalar processor-lifetime counts, so
a stateless exporter's later successful flush cannot erase a failed batch from
named force-flush or shutdown results. `pipeline-stats` exposes attempted,
exported, failed, and dropped span counts without retaining exporter values,
throwables, response bodies, endpoints, headers, or payloads. This lifetime
window is deliberately conservative: once a processor loses an accepted span,
later lifecycle barriers for that processor continue to report failure.

This primitive composes traces only. Logs and metrics keep their own explicit
processor and reader configuration. Applications using the global SDK can pass
`{:span-processors [pipelines]}` to `sdk/init!`; that replaces only its tracing
processor sequence, and transfers lifecycle ownership of those processors to
the returned SDK handle. The ordinary `:exporter` remains available to configure
metric and log export independently.

## Runtime metrics

Chez Scheme already tracks everything worth reporting about the running process.
`otel.instrument.runtime` maps it onto OpenTelemetry instruments, registered by
default:

| Instrument | Kind | Source |
| --- | --- | --- |
| `process.runtime.jolt.memory.heap` | gauge | `bytes-allocated` |
| `process.runtime.jolt.memory.reserved{,.peak}` | gauge | `current/maximum-memory-bytes` |
| `process.runtime.jolt.gc.count` | counter | `sstats-gc-count` |
| `process.runtime.jolt.gc.duration` | counter | `sstats-gc-real` |
| `process.runtime.jolt.gc.cpu.time` | counter | `sstats-gc-cpu` |
| `process.runtime.jolt.gc.reclaimed` | counter | `sstats-gc-bytes` |
| `process.runtime.jolt.cpu.time` | counter | `sstats-cpu` |
| `process.runtime.jolt.uptime` | counter | `sstats-real` |
| `system.cpu.logical.count` | gauge | host CPU count |

These are asynchronous instruments: their callbacks run once per collection, on
the reader's thread, so nothing is tracked on the application's hot path.

The primitives behind them are exposed by jolt as `jolt.host/wall-nanos`,
`mono-nanos`, `cpu-nanos`, `real-nanos`, `gc-count`, `gc-cpu-nanos`,
`gc-real-nanos`, `gc-bytes`, `bytes-allocated`, `current-memory-bytes`,
`maximum-memory-bytes`, `thread-id`, `scheme-version` and `machine-type`.

## Clocks

Spans need two clocks and neither one alone will do. `wall-nanos` (Chez's
`time-utc`) is the only clock a collector can interpret, but ntp can step it
backwards; `mono-nanos` (`time-monotonic`) never steps but has an arbitrary
origin. The SDK anchors one to the other at startup and derives every timestamp
as `anchor-wall + (mono-now - anchor-mono)`, so timestamps stay epoch-based while
durations come entirely from the monotonic clock. A clock adjustment in the
middle of a span cannot make it end before it started.

## Logs

Logs are the one signal you do not normally emit by hand. Turn the signal on and
keep using `clojure.tools.logging`:

```clojure
(sdk/init! {:service-name "checkout" :logs? true})

(trace/with-span [sp tracer "GET /cart"]
  (log/info "handling the cart request"))
```

The bridge is **additive** — it wraps whatever logger factory was already
installed, so stderr (or anything else configured) keeps working exactly as
before, and `shutdown!` puts the original back. Set `:bridge-logging? false` to
enable the signal without touching the application's logging.

What OpenTelemetry adds over the line you were already writing is correlation:
a record emitted inside a span carries that span's trace and span ids, so a
backend can show a request's logs beside that same request's trace. Levels map
onto the spec's severity ranges (`:trace` 1, `:debug` 5, `:info` 9, `:warn` 13,
`:error` 17, `:fatal` 21), and a throwable passed to `log/error` becomes
`exception.type` / `exception.message` / `exception.data` attributes.

`otel.logs/emit!` is there for a bridge from another logging library, or for
emitting structured records directly.

## Export

The exporter speaks **OTLP/HTTP with the JSON Protobuf encoding**, which is a
first-class encoding in the OTLP spec and interoperates with the OpenTelemetry
Collector and every backend that accepts OTLP/HTTP. Traces go to `/v1/traces`,
metrics to `/v1/metrics`, logs to `/v1/logs`.

Transport is [casselc/http-client](https://github.com/casselc/http-client),
so **https endpoints work** — TLS comes from the system OpenSSL. `:insecure?`
skips certificate verification for a collector with a self-signed cert; do not
use it across an untrusted network.

**gRPC is not implemented** — it needs HTTP/2 and binary protobuf, neither of
which exists on this host. A non-http(s) endpoint is rejected at construction
rather than being posted to as if it were HTTP.

### Receiver seam

`otel.otlp.http-receiver/handler` builds a pure Ring adapter for
`POST /v1/traces`; callers provide bounded JSON parsing and a span exporter.
`otel.otlp.trace-decode/decode-request` decodes an already parsed OTLP/JSON
`ExportTraceServiceRequest` into the same immutable ended-span maps consumed by
the exporters. It isolates invalid individual spans and returns path-aware
errors plus a rejected-span count suitable for an OTLP partial-success response.
It intentionally owns no Ring, socket, JSON-reader, authentication, or storage
dependency. See [receiver.md](receiver.md) for the HTTP boundary, caps,
feedback-loop guard, and current lossless-decoding limits.

## Namespaces

**API** — what instrumentation uses, and safe without an SDK:

- `otel.trace` — span contexts, the Span/Tracer protocols, `with-span`
- `otel.metrics` — meters and instruments
- `otel.logs` — loggers and log records
- `otel.context` — the propagation context and the active-context slot
- `otel.baggage`, `otel.propagation` — W3C Baggage and Trace Context
- `otel.attributes`, `otel.resource` — the common data model

**SDK** — what an application configures:

- `otel.sdk` — `init!`, `shutdown!`, and the global tracer/meter registry
- `otel.sdk.tracer`, `otel.sdk.span`, `otel.sdk.sampler`, `otel.sdk.export`
- `otel.sdk.metrics`, `otel.sdk.logs`, `otel.sdk.clock`, `otel.id`
- `otel.bridge.tools-logging` — the clojure.tools.logging appender
- `otel.otlp.trace-decode` — transport-neutral strict OTLP/JSON trace decoding
- `otel.otlp.http-receiver` — bounded Ring policy and exporter adapter

**Exporters** — `otel.exporter.otlp`, `otel.exporter.stdout`,
`otel.exporter.memory` (for tests).

## Testing your instrumentation

`otel.exporter.memory` records spans in memory so a test can assert on what
instrumentation produced:

```clojure
(let [exporter (memory/exporter)
      provider (sdk-tracer/tracer-provider
                 {:processors [(export/simple-processor exporter)]})]
  (trace/with-span [sp (sdk-tracer/get-tracer provider {:name "t"}) "op"]
    (do-the-work))
  (is (= ["op"] (map :name (memory/spans exporter)))))
```

`memory/metric-exporter` and `memory/log-exporter` do the same for the other two
signals, and `otel.sdk.clock/fake-clock` drives time explicitly for assertions on
durations.

## Tests

```bash
jolt test                                  # everything
jolt -M:test -m otel.test-runner trace     # one suite
jolt -M:test -m otel.test-runner dependency-resolution # dependency graph only
```

## License

Apache 2.0. See [LICENSE](LICENSE).
