(ns otel.sdk.export
  "Span processors and the exporter contract.

  A span processor sits between the SDK and an exporter and decides *when*
  telemetry leaves the process. Two are provided:

    simple-processor  exports each span the moment it ends, on the thread that
                      ended it. Predictable and immediate — the right choice for
                      tests, for a CLI, and for debugging. In a server it puts
                      network latency directly in the request path.

    batch-processor   queues ended spans and hands them to the exporter in bulk
                      from a background thread. This is what production wants.
                      The queue is bounded and *drops* on overflow: telemetry
                      must never block or crash the workload it is observing.

  An exporter is the thing that actually writes them somewhere."
  (:require [otel.sdk.lifecycle :as lifecycle]
            [otel.trace :as trace]))

(defprotocol SpanExporter
  (export-spans! [exporter spans]
    "Write a batch of finished span maps. Returns truthy on success. Must not
    throw — a failed export is reported, never propagated into the application.")
  (flush-exporter! [exporter]
    "Block until anything buffered inside the exporter has been written.")
  (shutdown-exporter! [exporter]
    "Release the exporter's resources. Later exports are no-ops."))

(defprotocol SpanProcessor
  (on-start [processor span parent-context]
    "Called when a recording span starts, on the starting thread.")
  (on-end [processor span]
    "Called when a recording span ends, on the ending thread.")
  (force-flush! [processor]
    "Block until everything already ended has been handed to the exporter.")
  (shutdown! [processor]
    "Flush, stop, and shut the exporter down."))

;; --- simple processor -------------------------------------------------------

(defn- export-quietly!
  "Export, swallowing anything the exporter throws. An exporter that throws must
  not propagate into application code that merely ended a span."
  [exporter spans]
  (try
    (export-spans! exporter spans)
    (catch :default _ false)))

(defrecord SimpleSpanProcessor [exporter state terminal]
  SpanProcessor
  (on-start [_ _ _] nil)
  (on-end [_ span]
    ;; Only sampled spans are exported. A :record-only span is recorded locally
    ;; for in-process consumers but is deliberately not sent onward.
    (locking state
      (when (and (not (:shutdown? @state))
                 (trace/sampled? (:span-context span)))
        (export-quietly! exporter [span])))
    nil)
  (force-flush! [_] (boolean (flush-exporter! exporter)))
  (shutdown! [_]
    (lifecycle/run-terminal!
      terminal
      #(locking state
         (swap! state assoc :shutdown? true)
         (shutdown-exporter! exporter)))))

(defn simple-processor
  "Export each span as it ends, synchronously."
  [exporter]
  (->SimpleSpanProcessor exporter (atom {:shutdown? false})
                         (lifecycle/terminal-action)))

;; --- batch processor --------------------------------------------------------

(def default-batch-config
  {:max-queue-size 2048
   :max-export-batch-size 512
   :schedule-delay-ms 5000})

(defn- take-batch!
  "Atomically remove up to `n` spans from the front of the queue and return them."
  [state n]
  (let [[old new] (swap-vals! state
                              (fn [s]
                                (let [q (:queue s)]
                                  (assoc s :queue (subvec q (min n (count q)))))))]
    (subvec (:queue old) 0 (- (count (:queue old)) (count (:queue new))))))

(defn- drain!
  "Export everything currently queued, in batches of at most `batch-size`."
  [exporter state batch-size]
  (loop [ok true]
    (let [batch (take-batch! state batch-size)]
      (if (empty? batch)
        ok
        (recur (and (export-quietly! exporter batch) ok))))))

(def ^:private shutdown-poll-ms
  "How often the worker checks for shutdown while waiting out its export interval.
  Sleeping the whole interval in one call would make shutdown take up to a full
  schedule-delay, which for the default 5s config is a visible stall at exit."
  50)

(defn- sleep-until-due
  "Sleep for `delay-ms`, in slices, returning early once shutdown is requested."
  [state delay-ms]
  (loop [remaining delay-ms]
    (when (and (pos? remaining) (not (:shutdown? @state)))
      (let [slice (min shutdown-poll-ms remaining)]
        (Thread/sleep slice)
        (recur (- remaining slice))))))

(defrecord BatchSpanProcessor [exporter state config worker terminal]
  SpanProcessor
  (on-start [_ _ _] nil)
  (on-end [_ span]
    (when (trace/sampled? (:span-context span))
      ;; Bounded queue: when it is full the span is dropped and counted. Blocking
      ;; here would make a slow collector into application latency, and growing
      ;; without bound would turn it into an out-of-memory crash.
      (swap! state
             (fn [s]
               (if (or (:shutdown? s) (>= (count (:queue s)) (:max-queue-size config)))
                 (update s :dropped inc)
                 (update s :queue conj span)))))
    nil)
  (force-flush! [_]
    ;; The worker may already have removed a batch from :queue while its export
    ;; is still in flight. Serializing every drain makes acquiring this monitor
    ;; the completion barrier for that batch; checking an empty queue alone is
    ;; not sufficient. Using the atom object only as a monitor still leaves its
    ;; lock-free swap path available to nonblocking producers.
    (locking state
      (boolean (and (drain! exporter state (:max-export-batch-size config))
                    (flush-exporter! exporter)))))
  (shutdown! [this]
    (lifecycle/run-terminal!
      terminal
      (fn []
        ;; Mark shutdown first so nothing new is queued, then drain what is
        ;; already there. The state swap is the acceptance boundary shared with
        ;; on-end: every span accepted before it is drained, and later spans are
        ;; rejected.
        (swap! state assoc :shutdown? true)
        (force-flush! this)
        (when worker
          (try (.join worker 5000) (catch :default _ nil)))
        (shutdown-exporter! exporter)))))

(defn dropped-count
  "How many spans this processor has dropped because its queue was full."
  [processor]
  (:dropped @(:state processor)))

(defn queue-size
  "How many spans are waiting to be exported."
  [processor]
  (count (:queue @(:state processor))))

(defn batch-processor
  "Queue ended spans and export them from a background thread.

  Options: :max-queue-size (2048), :max-export-batch-size (512),
  :schedule-delay-ms (5000)."
  ([exporter] (batch-processor exporter {}))
  ([exporter opts]
   (let [config (merge default-batch-config opts)
         state (atom {:queue [] :dropped 0 :shutdown? false})
         worker (Thread.
                  (fn []
                    (loop []
                      (sleep-until-due state (:schedule-delay-ms config))
                      ;; Drain unconditionally, including on the way out: spans
                      ;; ended just before shutdown must still be exported.
                      (locking state
                        (drain! exporter state (:max-export-batch-size config)))
                      (when-not (:shutdown? @state) (recur)))))]
     ;; A daemon thread: a background exporter must never be the reason a process
     ;; refuses to exit.
     (.setDaemon worker true)
     (.start worker)
     (->BatchSpanProcessor exporter state config worker
                           (lifecycle/terminal-action)))))

;; --- composite --------------------------------------------------------------

(defrecord CompositeSpanProcessor [processors terminal]
  SpanProcessor
  (on-start [_ span parent-context]
    (doseq [p processors] (on-start p span parent-context))
    nil)
  (on-end [_ span]
    (doseq [p processors] (on-end p span))
    nil)
  (force-flush! [_]
    ;; reduce, not `every?`: every processor must be flushed even if an earlier
    ;; one failed, so short-circuiting would silently skip the rest.
    (reduce (fn [ok p] (and (force-flush! p) ok)) true processors))
  (shutdown! [_]
    (lifecycle/run-terminal!
      terminal
      #(lifecycle/run-all! (mapv (fn [p] (fn [] (shutdown! p)))
                                 processors)))))

(defn composite-processor
  "One processor that fans out to several."
  [processors]
  (->CompositeSpanProcessor (vec processors) (lifecycle/terminal-action)))

;; --- metrics ----------------------------------------------------------------

(defprotocol MetricExporter
  (export-metrics! [exporter resource collected]
    "Write one collection. `collected` is a sequence of {:scope … :metrics […]}.
    Returns truthy on success and must not throw.")
  (shutdown-metric-exporter! [exporter]
    "Release the exporter's resources."))
