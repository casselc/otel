(ns otel.metrics
  "The metrics API: meters and the instruments they create.

  An instrument is chosen by what the measurement *means*, not by what it looks
  like, because the choice determines how a backend is allowed to aggregate it:

    counter          a value that only ever goes up (requests served, bytes sent).
                     Summing and rate-of-change are meaningful.
    up-down-counter  a value that goes up and down (queue depth, open connections).
                     Summing is meaningful; rate-of-change is not.
    histogram        a distribution of individual measurements (request latency).
                     Percentiles are meaningful.
    gauge            a value read as-is at a point in time (temperature, heap size).
                     Only the latest value is meaningful.

  Each has an asynchronous form taking a callback, for values that are cheaper to
  *read* on demand than to track on every change — runtime counters like heap
  bytes or GC totals are the canonical case.

  As with tracing, every operation here works with no SDK installed: the API's
  instruments are no-ops, so a library can instrument itself unconditionally."
  (:refer-clojure :exclude [count]))

(defprotocol Counter
  (add! [instrument value] [instrument value attributes]
    "Add to a monotonic counter. `value` must not be negative."))

(defprotocol UpDownCounter
  (add-delta! [instrument value] [instrument value attributes]
    "Add a signed delta to an up/down counter."))

(defprotocol Histogram
  (record! [instrument value] [instrument value attributes]
    "Record one measurement into the distribution."))

(defprotocol Gauge
  (set-value! [instrument value] [instrument value attributes]
    "Set the gauge's current value."))

(defprotocol Meter
  (counter [meter name] [meter name opts]
    "A monotonic counter. `opts` may carry :description and :unit.")
  (up-down-counter [meter name] [meter name opts]
    "A counter that can decrease.")
  (histogram [meter name] [meter name opts]
    "A distribution of measurements. `opts` may carry :boundaries.")
  (gauge [meter name] [meter name opts]
    "A synchronous gauge.")
  (observable-counter [meter name callback] [meter name callback opts]
    "A monotonic counter read on demand. `callback` is passed an observer and
    calls `observe!` on it.")
  (observable-up-down-counter [meter name callback] [meter name callback opts]
    "An up/down counter read on demand.")
  (observable-gauge [meter name callback] [meter name callback opts]
    "A gauge read on demand."))

(defprotocol Observer
  (observe! [observer value] [observer value attributes]
    "Report the current value of an asynchronous instrument."))

;; --- no-op implementations --------------------------------------------------

(defrecord NoopInstrument []
  Counter
  (add! [this _] this)
  (add! [this _ _] this)
  UpDownCounter
  (add-delta! [this _] this)
  (add-delta! [this _ _] this)
  Histogram
  (record! [this _] this)
  (record! [this _ _] this)
  Gauge
  (set-value! [this _] this)
  (set-value! [this _ _] this))

(def noop-instrument (->NoopInstrument))

(defrecord NoopMeter []
  Meter
  (counter [_ _] noop-instrument)
  (counter [_ _ _] noop-instrument)
  (up-down-counter [_ _] noop-instrument)
  (up-down-counter [_ _ _] noop-instrument)
  (histogram [_ _] noop-instrument)
  (histogram [_ _ _] noop-instrument)
  (gauge [_ _] noop-instrument)
  (gauge [_ _ _] noop-instrument)
  (observable-counter [_ _ _] noop-instrument)
  (observable-counter [_ _ _ _] noop-instrument)
  (observable-up-down-counter [_ _ _] noop-instrument)
  (observable-up-down-counter [_ _ _ _] noop-instrument)
  (observable-gauge [_ _ _] noop-instrument)
  (observable-gauge [_ _ _ _] noop-instrument))

(def noop-meter (->NoopMeter))
