(ns otel.sdk.lifecycle
  "Small shared lifecycle primitives used by SDK owners.")

(defn terminal-action
  "Return the state cell for one exactly-once terminal action."
  []
  (atom nil))

(defn await-worker!
  "Wait until `worker` has terminated before releasing resources it may own.

  There is deliberately no internal timeout. A timeout cannot establish worker
  quiescence, so treating one as success would allow resource shutdown to race
  an in-flight operation. If the wait is interrupted or otherwise fails, the
  exception propagates and the owning terminal action must leave the resource
  open."
  [worker]
  (when worker
    (.join worker))
  true)

(def shutdown-cancel-grace-ms
  "Cooperative grace before shutdown interrupts a worker that still owns
  exporter I/O. Healthy workers normally observe their retired source and exit
  during this interval without receiving an interrupt."
  250)

(def shutdown-join-bound-ms
  "Maximum wait after the cooperative grace for an owned worker to terminate.
  Exceeding the bound is a terminal failure; the exporter remains open because
  bounded waiting is not evidence of worker quiescence."
  2000)

(def ^:private shutdown-join-poll-ms 25)

(defn await-owned-worker!
  "Retire-aware wait for one processor's worker.

  `state` must already have crossed its `:shutdown?` admission boundary. After
  a cooperative grace, only a call site marked `:worker-export-active?` may
  trigger interruption of the exact `worker` argument. The worker must then
  terminate within `shutdown-join-bound-ms`; otherwise this throws and the
  caller must not close the exporter. No sibling or caller-owned force-flush
  thread is interrupted."
  [worker state]
  (when-not (:shutdown? @state)
    (throw (ex-info "worker cancellation requires retired admission"
                    {:otel.sdk/error :worker-admission-active})))
  (when worker
    (.join worker shutdown-cancel-grace-ms)
    (let [deadline (+ (System/nanoTime)
                      (* shutdown-join-bound-ms 1000000))]
      (loop [interrupted? false]
        (when (.isAlive worker)
          (let [owned? (:worker-export-active? @state)
                interrupt-now? (and owned? (not interrupted?))
                interrupted? (or interrupted? interrupt-now?)
                remaining-ns (- deadline (System/nanoTime))]
            (when interrupt-now?
              (swap! state update :worker-interrupt-count (fnil inc 0))
              (.interrupt worker))
            (when-not (pos? remaining-ns)
              (throw (ex-info "export worker did not terminate after cancellation"
                              {:otel.sdk/error :worker-join-timeout
                               :timeout-ms shutdown-join-bound-ms})))
            (.join worker
                   (max 1
                        (min shutdown-join-poll-ms
                             (quot (+ remaining-ns 999999) 1000000))))
            (recur interrupted?))))))
  true)

(defn- observe!
  [outcome]
  (let [{:keys [value throwable]} @outcome]
    (if throwable
      (throw throwable)
      value)))

(defn run-terminal!
  "Run `action` at most once. Concurrent and later callers wait for and observe
  the same returned object, or rethrow the same Throwable object."
  [terminal action]
  (loop []
    (if-let [outcome @terminal]
      (observe! outcome)
      (let [outcome (promise)]
        (if (compare-and-set! terminal nil outcome)
          (do
            (try
              (deliver outcome {:value (action)})
              (catch :default throwable
                (deliver outcome {:throwable throwable})))
            (observe! outcome))
          (recur))))))

(defn run-all!
  "Run every zero-argument action, even after false or a throw. Return their
  aggregate truth value, or rethrow the first Throwable after all have run."
  [actions]
  (loop [remaining (seq actions)
         ok true
         failure nil]
    (if-let [action (first remaining)]
      (let [outcome (try
                      {:value (action)}
                      (catch :default throwable
                        {:throwable throwable}))]
        (recur (next remaining)
               (and (boolean (:value outcome)) ok)
               (or failure (:throwable outcome))))
      (if failure (throw failure) ok))))
