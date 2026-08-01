(ns otel.sdk.clock
  "The two clocks telemetry needs, and the anchor that combines them.

  OpenTelemetry timestamps and OpenTelemetry durations have different
  requirements, and no single clock satisfies both:

    wall  (jolt.host/wall-nanos, Chez 'time-utc) — nanoseconds since the Unix
          epoch. The only clock a remote collector can interpret, so a span's
          start/end timestamps must be expressed in it. It is not monotonic: ntp
          can step it, forwards or backwards, at any moment.

    mono  (jolt.host/mono-nanos, Chez 'time-monotonic) — nanoseconds from an
          arbitrary origin that never steps. Meaningful only as a difference
          between two readings, which is exactly what a duration is.

  Timing a span with the wall clock lets an ntp step land inside it and produce a
  negative or absurd duration; timestamping with the monotonic clock produces a
  number no collector can place in time. `anchored` resolves this the way the
  OpenTelemetry SDKs do: read both clocks once, then derive every later wall
  timestamp as anchor-wall + (mono-now - anchor-mono). Timestamps stay
  epoch-based and comparable across processes, while intervals within one
  anchored clock come entirely from the monotonic clock and cannot go backwards.

  The Clock protocol exists so tests can drive time explicitly — see fake-clock."
  (:refer-clojure :exclude [set!]))

(defprotocol Clock
  (wall-nanos [clock]
    "Nanoseconds since the Unix epoch. Use for timestamps, never for durations.")
  (mono-nanos [clock]
    "Nanoseconds from an arbitrary fixed origin. Use for durations, never for timestamps."))

;; --- the real clock ---------------------------------------------------------

(defrecord SystemClock []
  Clock
  (wall-nanos [_] (jolt.host/wall-nanos))
  (mono-nanos [_] (jolt.host/mono-nanos)))

(def system
  "The process clock, reading Chez's 'time-utc and 'time-monotonic directly."
  (->SystemClock))

;; --- anchored clock ---------------------------------------------------------

(defrecord AnchoredClock [source anchor-wall anchor-mono]
  Clock
  (wall-nanos [_]
    ;; The wall reading is reconstructed from the monotonic delta, so a step of
    ;; the underlying wall clock after anchoring is invisible here.
    (+ anchor-wall (- (mono-nanos source) anchor-mono)))
  (mono-nanos [_] (mono-nanos source)))

(defn require-host-clock!
  "Fail early, and legibly, on a jolt that predates the telemetry primitives.

  Unlike a resource attribute, the clock has no meaningful fallback: deriving it
  from System/currentTimeMillis would make every span millisecond-granular and
  wall-clock-timed, which is the exact defect the two-clock design exists to
  avoid. So this is a hard requirement, and the useful thing to do is say so once
  at startup rather than raise `No such var` from inside the first span."
  []
  (try
    (jolt.host/wall-nanos)
    (catch :default _
      (throw (ex-info (str "otel: this jolt build does not expose jolt.host/wall-nanos. "
                           "The SDK needs the jolt.host telemetry primitives (wall-nanos, "
                           "mono-nanos, the gc and memory counters); build jolt from a "
                           "checkout that includes them.")
                      {:missing 'jolt.host/wall-nanos})))))

(defn anchored
  "Pin `clock`'s current wall reading to its current monotonic reading.

  The returned clock reports wall timestamps that advance strictly with the
  monotonic clock, so durations measured across it are immune to wall-clock
  steps. Anchor once per trace-provider (or per span, for a long-running one):
  the anchor's accuracy against true wall time drifts with the monotonic clock,
  so it is a trade of absolute accuracy for interval correctness."
  ([] (anchored system))
  ([clock]
   ;; Checked here because every provider builds its clock through this call, so
   ;; one check covers traces and metrics both.
   (when (instance? SystemClock clock) (require-host-clock!))
   (->AnchoredClock clock (wall-nanos clock) (mono-nanos clock))))

;; --- test clock -------------------------------------------------------------

(defrecord FakeClock [state]
  Clock
  (wall-nanos [_] (:wall @state))
  (mono-nanos [_] (:mono @state)))

(defn fake-clock
  "A clock under the caller's control, for tests. Starts at the given :wall and
  :mono nanos (both default 0) and only moves when advance!/set-wall!/set-mono!
  is called."
  ([] (fake-clock {}))
  ([{:keys [wall mono] :or {wall 0 mono 0}}]
   (->FakeClock (atom {:wall wall :mono mono}))))

(defn advance!
  "Move a fake clock forward by the given :wall and/or :mono nanos."
  [fake {:keys [wall mono] :or {wall 0 mono 0}}]
  (swap! (:state fake) (fn [s] (-> s (update :wall + wall) (update :mono + mono))))
  fake)

(defn set-wall!
  "Set a fake clock's wall reading absolutely — models an ntp step."
  [fake nanos]
  (swap! (:state fake) assoc :wall nanos)
  fake)

(defn set-mono!
  "Set a fake clock's monotonic reading absolutely."
  [fake nanos]
  (swap! (:state fake) assoc :mono nanos)
  fake)
