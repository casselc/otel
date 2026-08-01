# otel

An [OpenTelemetry](https://opentelemetry.io) implementation for
[Jolt](https://github.com/jolt-lang/jolt) — the API instrumentation is written
against, and the SDK that records and exports it.

Traces and metrics are implemented, including OTLP/HTTP export, W3C context
propagation, and runtime metrics read straight off Chez Scheme's collector. Logs
are not implemented yet.

## Requirements

A jolt whose `jolt.host` exposes the telemetry primitives (`wall-nanos`,
`mono-nanos`, the gc and memory counters). These landed in jolt alongside this
library and are not in the released binaries yet, so for now build jolt from a
checkout that includes them. The SDK checks at startup and says so plainly if
they are missing — it will not silently fall back to a millisecond clock, since
that is the exact defect the two-clock design exists to avoid.

## Install

```clojure
;; deps.edn
{:deps {io.github.jolt-lang/otel {:git/sha "..."}}}
```

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
| `:exporter` | — | `:otlp` (also `:console`, `:json`, `:none`) |
| `:processor` | — | `:batch` (also `:simple`) |
| `:metrics?` / `:runtime-metrics?` | — | true |
| `:metric-interval-ms` | — | 60000 |
| — | `OTEL_SDK_DISABLED=true` | installs nothing |
| — | `OTEL_RESOURCE_ATTRIBUTES` | merged into the resource |

Samplers, processors and exporters can also be built directly and passed to
`otel.sdk.tracer/tracer-provider` when `init!` is too opinionated.

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

## Export

The exporter speaks **OTLP/HTTP with the JSON Protobuf encoding**, which is a
first-class encoding in the OTLP spec and interoperates with the OpenTelemetry
Collector and every backend that accepts OTLP/HTTP.

Two things are deliberately out of scope:

- **gRPC** — needs HTTP/2 and binary protobuf, neither of which exists on this
  host. Use OTLP/HTTP.
- **TLS** — the transport is cleartext `http://` only. An `https://` endpoint is
  rejected at construction with a clear message rather than failing later. Send
  to a collector on localhost or a sidecar and let it forward over TLS, which is
  the recommended OTLP topology anyway.

## Namespaces

**API** — what instrumentation uses, and safe without an SDK:

- `otel.trace` — span contexts, the Span/Tracer protocols, `with-span`
- `otel.metrics` — meters and instruments
- `otel.context` — the propagation context and the active-context slot
- `otel.baggage`, `otel.propagation` — W3C Baggage and Trace Context
- `otel.attributes`, `otel.resource` — the common data model

**SDK** — what an application configures:

- `otel.sdk` — `init!`, `shutdown!`, and the global tracer/meter registry
- `otel.sdk.tracer`, `otel.sdk.span`, `otel.sdk.sampler`, `otel.sdk.export`
- `otel.sdk.metrics`, `otel.sdk.clock`, `otel.id`

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

`otel.sdk.clock/fake-clock` drives time explicitly for assertions on durations.

## Tests

```bash
jolt test                                  # everything
jolt -M:test -m otel.test-runner trace     # one suite
```

## License

Apache 2.0. See [LICENSE](LICENSE).
