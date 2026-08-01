(ns otel.instrument.runtime
  "Runtime metrics for the Chez Scheme process.

  Chez already maintains everything worth reporting — the collector's counters
  and two clocks — and jolt exposes them through `jolt.host`. This maps them onto
  OpenTelemetry instruments following the `process.runtime.*` semantic
  conventions, so a Jolt service shows up in a runtime dashboard next to JVM and
  Go services rather than needing a bespoke one.

  Every instrument here is asynchronous. These are values that are cheap to read
  on demand and expensive to track continuously — there is no hook on each
  allocation, and there does not need to be — so the SDK reads them once per
  collection instead.

  The instrument kinds are chosen from what each number actually is, which is
  where a runtime integration usually goes wrong:

    heap usage             a gauge. It rises and falls with collection, and only
                           the latest reading means anything.
    collection count/time  monotonic counters. They only ever increase, so a
                           backend can turn them into a rate — collections per
                           second, or fraction of time spent collecting.
    cpu time               a monotonic counter, for the same reason."
  (:require [otel.metrics :as metrics]))

(defn- observe-value
  "An observable callback reporting one value with fixed attributes."
  ([f] (observe-value f {}))
  ([f attrs] (fn [observer] (metrics/observe! observer (f) attrs))))

(defn register!
  "Register the Chez runtime instruments on `meter`. Returns the instruments, so
  a caller can hold them; the SDK reads them through the meter regardless.

  Called once at startup, after the meter provider exists."
  [meter]
  [;; --- memory ---------------------------------------------------------------
   ;; Live bytes: what survived the last collection plus what has been allocated
   ;; since. This is the number that answers \"is this process leaking\".
   (metrics/observable-gauge
     meter "process.runtime.jolt.memory.heap"
     (observe-value #(jolt.host/bytes-allocated))
     {:unit "By" :description "Bytes currently allocated on the Chez heap"})

   ;; Bytes Chez holds from the operating system. Always >= heap: it includes
   ;; space the collector has reserved but not handed out, so the gap between the
   ;; two is fragmentation and headroom rather than live data.
   (metrics/observable-gauge
     meter "process.runtime.jolt.memory.reserved"
     (observe-value #(jolt.host/current-memory-bytes))
     {:unit "By" :description "Bytes obtained from the OS by the Chez allocator"})

   (metrics/observable-gauge
     meter "process.runtime.jolt.memory.reserved.peak"
     (observe-value #(jolt.host/maximum-memory-bytes))
     {:unit "By" :description "Peak bytes obtained from the OS by the Chez allocator"})

   ;; --- garbage collection ---------------------------------------------------
   (metrics/observable-counter
     meter "process.runtime.jolt.gc.count"
     (observe-value #(jolt.host/gc-count))
     {:unit "{collection}" :description "Collections performed since process start"})

   ;; Reported in seconds because that is what the OTel conventions use for
   ;; durations; the host counter is nanoseconds.
   (metrics/observable-counter
     meter "process.runtime.jolt.gc.duration"
     (observe-value #(/ (double (jolt.host/gc-real-nanos)) 1e9))
     {:unit "s" :description "Wall-clock time spent collecting since process start"})

   (metrics/observable-counter
     meter "process.runtime.jolt.gc.cpu.time"
     (observe-value #(/ (double (jolt.host/gc-cpu-nanos)) 1e9))
     {:unit "s" :description "CPU time spent collecting since process start"})

   (metrics/observable-counter
     meter "process.runtime.jolt.gc.reclaimed"
     (observe-value #(jolt.host/gc-bytes))
     {:unit "By" :description "Bytes reclaimed by the collector since process start"})

   ;; --- cpu ------------------------------------------------------------------
   (metrics/observable-counter
     meter "process.runtime.jolt.cpu.time"
     (observe-value #(/ (double (jolt.host/cpu-nanos)) 1e9))
     {:unit "s" :description "CPU time consumed by the process"})

   (metrics/observable-counter
     meter "process.runtime.jolt.uptime"
     (observe-value #(/ (double (jolt.host/real-nanos)) 1e9))
     {:unit "s" :description "Wall-clock time since process start"})

   ;; --- host -----------------------------------------------------------------
   (metrics/observable-gauge
     meter "system.cpu.logical.count"
     (observe-value #(jolt.host/available-processors))
     {:unit "{cpu}" :description "Logical CPUs visible to the process"})])
