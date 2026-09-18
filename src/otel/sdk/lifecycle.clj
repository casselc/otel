(ns otel.sdk.lifecycle
  "Small shared lifecycle primitives used by SDK owners.")

(defn terminal-action
  "Return the state cell for one exactly-once terminal action."
  []
  (atom nil))

(defprotocol SettlementWitness
  (settlement-status [component]
    "Return a closed ownership witness, never a resource or Throwable.
    This MUST be a bounded, nonblocking, nonwaiting snapshot observation:
    never join a worker, await exporter I/O, or start resource operations.
    The SDK invokes trusted custom witnesses synchronously; this contract is
    not sandbox isolation or enforcement of foreign implementation latency.
    :quiescence :confirmed declares stable retirement of admission and that
    every owned caller/background resource-using operation has settled, not
    momentary idleness. Implementers must never reopen that admission. An
    exporter must include its internal resource users in this declaration."))

(defn terminal-status
  "Observe an exactly-once action without waiting or exposing its result."
  [terminal]
  (if-let [outcome (some-> terminal deref)]
    (if (realized? outcome)
      (let [{:keys [value throwable]} @outcome]
        (cond throwable :threw value :returned-true :else :returned-false))
      :running)
    :not-started))

(defn admitted-operation!
  "Count caller-owned exporter operations across the atomic retirement gate.
  Return rejected-value without running action after admission retires."
  [state rejected-value action]
  (let [admitted? (loop []
                    (let [old @state]
                      (cond
                        (:shutdown? old) false
                        (compare-and-set! state old
                                          (update old :caller-export-count (fnil inc 0))) true
                        :else (recur))))]
    (if admitted?
      (try (action)
           (finally (swap! state update :caller-export-count dec)))
      rejected-value)))

(declare component-settlement)

(defn release-exporter!
  "Record only the release contract's scalar outcome; preserve value/identity.
  Failure is not resource settlement proof."
  [state action]
  (swap! state assoc :exporter-release :running)
  (try
    (let [value (action)]
      (swap! state assoc :exporter-release
             (cond (true? value) :returned-true
                   (false? value) :returned-false
                   :else :returned-unconfirmed))
      value)
    (catch :default error
      (swap! state assoc :exporter-release :threw)
      (throw error))))

(defn owned-settlement
  "Fresh witness for a maintained owner. Reading it never waits for a worker.
  Retirement prevents new counted callers; an existing caller blocks proof."
  [state worker terminal exporter]
  (let [snapshot @state
        retired? (true? (:shutdown? snapshot))
        callers-settled? (zero? (get snapshot :caller-export-count 0))
        worker-settled? (try (or (nil? worker) (not (.isAlive worker)))
                            (catch :default _ false))
        outcome (terminal-status terminal)
        action-settled? (contains? #{:returned-true :returned-false :threw} outcome)
        sdk-settled? (and retired? callers-settled? worker-settled? action-settled?)
        exporter-settled? (or (= :returned-true (:exporter-release snapshot))
                              (= :confirmed (:quiescence (component-settlement exporter))))]
    {:admission-retired? retired?
     :operations-settled? callers-settled?
     :worker-settled? worker-settled?
     :terminal outcome
     :sdk-quiescence (if sdk-settled? :confirmed :unconfirmed)
     :exporter-quiescence (if exporter-settled? :confirmed :unconfirmed)
     :quiescence (if (and sdk-settled? exporter-settled?)
                   :confirmed :unconfirmed)}))

(defn component-settlement
  "Unknown/custom owners must never gain cleanup permission by default."
  [component]
  (if (satisfies? SettlementWitness component)
    (try (let [status (settlement-status component)]
           ;; A protocol implementation can carry arbitrary values. Never copy
           ;; those into the operator-visible SDK result.
           {:quiescence (if (= :confirmed (:quiescence status)) :confirmed :unconfirmed)
            :sdk-quiescence (if (= :confirmed (:sdk-quiescence status)) :confirmed :unconfirmed)
            :exporter-quiescence (if (= :confirmed (:exporter-quiescence status)) :confirmed :unconfirmed)
            :terminal (if (contains? #{:not-started :running :returned-true :returned-false :threw}
                                     (:terminal status))
                        (:terminal status) :unknown)})
         (catch :default _ {:quiescence :unconfirmed}))
    {:quiescence :unconfirmed}))

(defn combined-settlement
  [components terminal]
  (let [statuses (mapv component-settlement components)
        outcome (terminal-status terminal)]
    {:terminal outcome
     :sdk-quiescence (if (and (contains? #{:returned-true :returned-false :threw} outcome)
                              (every? #(= :confirmed (:sdk-quiescence %)) statuses))
                       :confirmed :unconfirmed)
     :exporter-quiescence (if (every? #(= :confirmed (:exporter-quiescence %)) statuses)
                            :confirmed :unconfirmed)
     :quiescence (if (and (contains? #{:returned-true :returned-false :threw} outcome)
                          (every? #(= :confirmed (:quiescence %)) statuses))
                   :confirmed :unconfirmed)
     :components statuses}))

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
  bounded waiting is not evidence of worker quiescence. This public policy seam
  may be rebound by causal tests to exercise timeout handling without a real
  two-second delay."
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
              (swap! state
                     (fn [current]
                       (-> current
                           (assoc :shutdown-cancelled? true)
                           (update :worker-interrupt-count (fnil inc 0)))))
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

(defrecord ^:private ConstructionReceipt [state lock])

(defn construction-receipt
  "Opaque per-invocation acquisition receipt; never reuse it for another call."
  []
  (->ConstructionReceipt (atom {:phase :new :owners []}) (Object.)))

(defn construction-settlement
  "Closed fresh observation of known acquired owners, not arbitrary advice.
  Custom component witnesses retain their trusted bounded/nonblocking contract."
  [receipt]
  (if (instance? ConstructionReceipt receipt)
    (let [snapshot @(:state receipt)
          owners (:owners snapshot)
          confirmed? (and (:retirement-requested? snapshot)
                          (contains? #{:failed :returned} (:phase snapshot))
                          (every? (fn [{:keys [owner terminal]}]
                                    (and (contains? #{:returned-true :returned-false :threw}
                                                    (terminal-status terminal))
                                         (= :confirmed
                                            (:quiescence (component-settlement owner)))))
                                  owners))]
      {:otel.sdk.construction/version 1
       :quiescence (if confirmed? :confirmed :unconfirmed)})
    {:otel.sdk.construction/version 1 :quiescence :unconfirmed}))

(defn construction-failure-status
  "Attest only this invocation's exact reported failure. No error is returned.
  Missing receipts, reuse errors, successful-return advice throws and replacement
  errors are unknown even when some previously acquired owners have retired."
  [receipt error]
  (if (and (some? error) (instance? ConstructionReceipt receipt)
           (= :failed (:phase @(:state receipt)))
           (or (identical? error (:failure @(:state receipt)))
               (identical? error (:reported-error @(:state receipt)))))
    (assoc (construction-settlement receipt) :outcome :failed)
    {:otel.sdk.construction/version 1 :outcome :unknown :quiescence :unconfirmed}))

(defn acquire-owner!
  "Register a known owner before starting its resource users; retain it privately."
  ([receipt owner close!] (acquire-owner! receipt owner close! nil))
  ([receipt owner close! resource]
  (locking (:lock receipt)
    (when-not (= :acquiring (:phase @(:state receipt)))
      (throw (ex-info "construction receipt is not acquiring"
                      {:otel.sdk/error :invalid-construction-receipt})))
    (swap! (:state receipt) update :owners conj
           {:owner owner :close! close! :resource resource
            :terminal (terminal-action)}))
  owner))

(defn construction-resource-status
  "Closed transfer classification for an exact built-in invocation/resource.
  Claims are a trusted constructor contract, not hostile-forgery protection."
  [receipt error resource signal]
  (let [snapshot (when (instance? ConstructionReceipt receipt) @(:state receipt))
        failed? (and (some? error) (= :failed (:phase snapshot))
                     (or (identical? error (:failure snapshot))
                         (identical? error (:reported-error snapshot))))
        returned? (and (nil? error) (= :returned (:phase snapshot)))
        claimed? (and (some? resource)
                      (some #(and (identical? resource (:exporter (:resource %)))
                                  (= signal (:signal (:resource %))))
                            (:owners snapshot)))
        ownership (cond
                    (and (or failed? returned?) claimed?) :child-owned
                    (and failed? (empty? (:owners snapshot))) :orphan
                    :else :unknown)]
    {:otel.sdk.construction/version 1 :ownership ownership}))

(defn retire-construction!
  "Serialized exactly-once retirement, with fresh proof on every retry.
  A terminal throw/false does not itself grant cleanup permission."
  [receipt]
  (locking (:lock receipt)
    ;; Never close a record while its constructor might still start the worker.
    ;; Acquisition is sealed by return/failure before retirement can begin.
    (when (contains? #{:failed :returned} (:phase @(:state receipt)))
      (swap! (:state receipt) assoc :retirement-requested? true)
      (doseq [{:keys [close! terminal]} (reverse (:owners @(:state receipt)))]
        (try (run-terminal! terminal close!) (catch :default _ nil))))
    (let [settlement (construction-settlement receipt)]
      (assoc settlement :status
             (if (= :confirmed (:quiescence settlement)) :closed :closing)))))

(defn with-construction!
  "One acquisition attempt. Rethrow the original failure only after retirement;
  otherwise transfer an opaque retry owner through closed exception data.
  Original errors/resources/configuration are never copied into diagnostic data."
  [receipt action]
  (when-not (and (instance? ConstructionReceipt receipt)
                (locking (:lock receipt)
                  (when (= :new (:phase @(:state receipt)))
                    (swap! (:state receipt) assoc :phase :acquiring)
                    true)))
    (throw (ex-info "construction receipt was missing or reused"
                    {:otel.sdk/error :invalid-construction-receipt})))
  (try
    (let [result (action)]
      (swap! (:state receipt) assoc :phase :returned)
      result)
    (catch :default original
      (swap! (:state receipt) assoc :phase :failed :failure original)
      (let [retry-stop! #(retire-construction! receipt)
            result (retry-stop!)]
        (if (= :closed (:status result))
          (throw original)
          (let [reported (ex-info "SDK constructor cleanup remains incomplete"
                                  {:otel.sdk/error :construction-cleanup-incomplete
                                   :retry-stop! retry-stop!})]
            (swap! (:state receipt) assoc :reported-error reported)
            (throw reported)))))))

(defn acquire-child!
  "Preinstall a child receipt before invoking its constructor. A throw without
  matching built-in failure evidence stays unknown, not an empty-owner proof."
  [receipt construct]
  (let [child (construction-receipt) failure (atom nil)
        observer (reify SettlementWitness
                   (settlement-status [_]
                     (if-let [error @failure]
                       (construction-failure-status child error)
                       (construction-settlement child))))]
    (acquire-owner! receipt observer #(retire-construction! child))
    (try (construct child)
         (catch :default error
           (reset! failure error)
           (throw error)))))

(defn start-owned-worker!
  "Start only after its full owner/terminal record has been acquired privately."
  [receipt owner worker]
  (when-not (and (= :acquiring (:phase @(:state receipt)))
                 (identical? worker (:worker owner))
                 (some #(identical? owner (:owner %)) (:owners @(:state receipt))))
    (throw (ex-info "worker owner was not acquired before start"
                    {:otel.sdk/error :unacquired-worker-owner})))
  (.setDaemon worker true)
  (.start worker)
  owner)
