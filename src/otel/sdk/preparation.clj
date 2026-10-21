(ns otel.sdk.preparation
  "Internal opt-in two-slot span preparation engine. No transport dependency.

  The adapter is a trusted resource-owner contract, not arbitrary async export:
  reserve before materializing, prepare serially, submit without acknowledging,
  await the exact settled result through interruption, then release and retire.
  SQL plus precomputed WAL byte limits belong to the exporter reservation.
  Neither this engine nor the raw span count claims an RSS bound.")

(def ^:private operations
  [:reserve! :prepare! :submit! :await! :abort! :release! :retire! :fence!])

(defn checked-adapter
  "Validate before resource acquisition. Nil preserves ordinary export.
  :fence! must synchronously and boundedly prevent NEW exporter AND writer admission, without
  waiting for publication or acquiring the SDK preparer's drain monitor.
  All phase callbacks return literal true only on completion; other values fail.
  :await! returns true ONLY for confirmed publication, and must not return/throw
  until its admitted request actually settles. reserve! instead returns a
  non-nil capability, or nil without acquiring a reservation.
  Unexpected cleanup exceptions quarantine the SDK owner rather than authorize
  exporter close. The supported exporter supplies these callbacks explicitly."
  [adapter]
  (when-not (or (nil? adapter)
                (and (map? adapter) (every? #(fn? (get adapter %)) operations)))
    (throw (ex-info "Invalid experimental span preparation adapter"
                    {:otel.sdk/error :invalid-preparation-adapter})))
  adapter)

(defn require-settled!
  [state]
  (when (:preparation-unsettled? @state)
    (throw (ex-info "Span preparation resource retirement is unconfirmed"
                    {:otel.sdk/error :preparation-unsettled}))))

(defn- prepare!
  [adapter state batch]
  (let [n (count batch)]
    (swap! state update :attempted-span-count + n)
    (let [reservation (try {:value ((:reserve! adapter) n)}
                           (catch :default _ {:failed? true}))]
      (if (or (:failed? reservation) (nil? (:value reservation)))
        {:count n :failed? true}
        (let [cap (:value reservation)
              prepared? (try (true? ((:prepare! adapter) cap batch))
                             (catch :default _ false))]
          {:count n :cap cap :reserved? true :failed? (not prepared?)})))))

(defn- retire!
  [adapter state batch]
  (when (:reserved? batch)
    (try
      (when-not (and (true? ((:release! adapter) (:cap batch)))
                     (true? ((:retire! adapter) (:cap batch))))
        (throw (ex-info "Incomplete preparation retirement" {})))
      (catch :default _
        ;; No throwable/message, SQL, credential or exporter result in state.
        ;; The exporter must retain its unretired capability on this failure.
        (swap! state assoc :preparation-unsettled? true)
        (require-settled! state)))))

(defn- settle!
  [adapter state batch]
  (if-not batch
    true
    (let [abort-settled? (if (or (:submitted? batch) (not (:reserved? batch)))
                           true
                           (try (true? ((:abort! adapter) (:cap batch)))
                                (catch :default _ false)))
          exported? (if (:submitted? batch)
                      (try (true? ((:await! adapter) (:cap batch)))
                           (catch :default _ false))
                      false)]
      (when-not abort-settled? (swap! state assoc :preparation-unsettled? true))
      ;; Confirmation/error settlement precedes exporter admission release and
      ;; slot retirement, including failure before writer admission.
      (swap! state update (if exported? :exported-span-count :failed-span-count)
             + (:count batch))
      ;; Another batch's cleanup failure withholds global proof, but must NOT
      ;; prevent this batch's independent abort/release/retirement attempt.
      (when abort-settled?
        (retire! adapter state batch))
      (require-settled! state)
      exported?)))

(defn- submit!
  [adapter state batch]
  (if (or (:failed? batch) (:shutdown-cancelled? @state))
    batch
    (try
      (if (true? ((:submit! adapter) (:cap batch)))
        (assoc batch :submitted? true)
        (assoc batch :failed? true))
      (catch :default _ (assoc batch :failed? true)))))

(defn- drain-internal!
  "Called under the existing SDK drain monitor and suppression binding.
  Prepare B while the existing writer publishes A. Settle/retire A before
  submitting B or preparing C. There is no second SDK worker and no admission
  receipt is interpreted as delivery. take-batch!/fail-queued! use the owner's
  existing bounded queue. Only closed scalar failure evidence enters state."
  [adapter state batch-size take-batch! fail-queued!]
  (require-settled! state)
  (loop [pending nil ok true]
    (if (:shutdown-cancelled? @state)
      (try
        (settle! adapter state pending)
        false
        (finally (fail-queued! state)))
      (let [batch (take-batch! state batch-size)]
        (if (empty? batch)
          (let [settled? (settle! adapter state pending)]
            (and settled? ok))
          (let [prepared (prepare! adapter state batch)
                ;; Even if B preparation fails, never abandon A's admitted
                ;; request. A cleanup failure must not lose B's reservation.
                settled? (try (settle! adapter state pending)
                              (catch :default error
                                (try (settle! adapter state prepared)
                                     (catch :default _ nil))
                                (throw error)))
                submitted (submit! adapter state prepared)]
            (if (:submitted? submitted)
              (recur submitted (and settled? ok))
              (let [failed? (settle! adapter state submitted)]
                (recur nil (and failed? settled? ok))))))))))

(defn drain!
  [adapter state batch-size take-batch! fail-queued!]
  (try
    (drain-internal! adapter state batch-size take-batch! fail-queued!)
    (catch :default _
      ;; Unexpected engine/queue failure cannot turn worker termination into
      ;; permission to close resources. The adapter retains unretired owners.
      (swap! state assoc :preparation-unsettled? true)
      (require-settled! state))))
