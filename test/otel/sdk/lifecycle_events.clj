(ns otel.sdk.lifecycle-events
  "Test-only lifecycle trace vocabulary shared by every background SDK owner.

  Owner-work rejection is signal-specific: span/log enqueue is rejected, while
  a stopped metric reader rejects collection/export rather than recording on an
  already-acquired instrument. Terminal-completion entries are reconstructed by
  the test after shutdown returns or throws; direct assertions, not these
  entries, establish returned-value and Throwable identity.")

(def vocabulary
  #{:export-started
    :export-finished
    :shutdown-requested
    :owner-work-rejected
    :worker-terminal
    :worker-wait-succeeded
    :worker-wait-failed
    :exporter-close-called
    :exporter-close-returned
    :exporter-close-failed
    :terminal-completed-return
    :terminal-completed-failure
    :terminal-observed-return
    :terminal-observed-failure})

(def initial-state
  {:accepting? true
   :export-active? false
   :worker-terminal? false
   :wait-outcome :not-waiting
   :exporter-close-called? false
   :close-outcome :not-called
   :close-count 0
   :terminal-result :pending
   :shutdown-requests 0
   :owner-work-rejected-count 0})

(defn record!
  "Append one sequenced lifecycle event to a shared trace atom."
  [trace event]
  (when-not (contains? vocabulary event)
    (throw (ex-info "unknown lifecycle event"
                    {:event event :known-events vocabulary})))
  (locking trace
    (let [entry {:seq (count @trace) :event event}]
      (swap! trace conj entry)
      entry)))

(defn- invalid!
  [state entry reason]
  (throw (ex-info "invalid SDK lifecycle trace"
                  {:reason reason :entry entry :state state})))

(defn transition
  "Apply one trace entry to the test oracle used by all signal owners."
  [state {:keys [event] :as entry}]
  (case event
    :export-started
    (if (and (not (:export-active? state))
             (not (:worker-terminal? state))
             (not (:exporter-close-called? state)))
      (assoc state :export-active? true)
      (invalid! state entry :export-started-after-ownership-release))

    :export-finished
    (if (:export-active? state)
      (assoc state :export-active? false)
      (invalid! state entry :export-finished-while-inactive))

    :shutdown-requested
    (-> state
        (assoc :accepting? false)
        (update :shutdown-requests inc)
        (update :wait-outcome #(if (= :not-waiting %) :waiting %)))

    :owner-work-rejected
    (if-not (:accepting? state)
      (update state :owner-work-rejected-count inc)
      (invalid! state entry :owner-work-rejected-before-shutdown))

    :worker-terminal
    (if (and (not (:accepting? state))
             (not (:export-active? state))
             (not (:worker-terminal? state)))
      (assoc state :worker-terminal? true)
      (invalid! state entry :worker-terminal-while-export-active))

    :worker-wait-succeeded
    (if (and (= :waiting (:wait-outcome state))
             (:worker-terminal? state)
             (= :pending (:terminal-result state)))
      (assoc state :wait-outcome :succeeded)
      (invalid! state entry :worker-wait-succeeded-without-terminal-worker))

    :worker-wait-failed
    (if (and (= :waiting (:wait-outcome state))
             (= :pending (:terminal-result state)))
      (assoc state :wait-outcome :failed)
      (invalid! state entry :worker-wait-failure-after-terminal-result))

    :exporter-close-called
    (if (and (= :succeeded (:wait-outcome state))
             (:worker-terminal? state)
             (not (:export-active? state))
             (zero? (:close-count state))
             (= :pending (:terminal-result state)))
      (assoc state
             :exporter-close-called? true
             :close-count 1
             :close-outcome :in-flight)
      (invalid! state entry :exporter-close-without-worker-ownership))

    :exporter-close-returned
    (if (= :in-flight (:close-outcome state))
      (assoc state :close-outcome :returned)
      (invalid! state entry :exporter-return-without-close-call))

    :exporter-close-failed
    (if (= :in-flight (:close-outcome state))
      (assoc state :close-outcome :failed)
      (invalid! state entry :exporter-failure-without-close-call))

    :terminal-completed-return
    (if (and (= :returned (:close-outcome state))
             (= :pending (:terminal-result state)))
      (assoc state :terminal-result :return)
      (invalid! state entry :terminal-return-without-exporter-return))

    :terminal-completed-failure
    (if (and (= :pending (:terminal-result state))
             (or (= :failed (:wait-outcome state))
                 (= :failed (:close-outcome state))))
      (assoc state :terminal-result :failure)
      (invalid! state entry :terminal-failure-without-failed-owner-step))

    :terminal-observed-return
    (if (= :return (:terminal-result state))
      state
      (invalid! state entry :terminal-return-does-not-match))

    :terminal-observed-failure
    (if (= :failure (:terminal-result state))
      state
      (invalid! state entry :terminal-failure-does-not-match))

    (invalid! state entry :unknown-event)))

(defn validate!
  "Validate a complete or in-progress ownership trace and return its state."
  [trace]
  (reduce transition initial-state trace))

(defn valid?
  "Return false when the lifecycle oracle rejects `trace`."
  [trace]
  (try
    (validate! trace)
    true
    (catch :default _ false)))

;; Separate from the legacy lifecycle journal: these are observation points,
;; NOT a claim that callback entry/return is the admission CAS linearization.
(defn settlement-record! [trace owner operation event data]
  (locking trace
    (swap! trace conj (merge data {:seq (count @trace) :owner owner
                                  :operation operation :event event}))))

(defn validate-settlement!
  "Check bounded, scalar settlement observations against independent users.
  Custom witness truthfulness remains a trusted contract, not this oracle's
  ability to inspect arbitrary foreign resource users."
  [trace]
  (when (> (count trace) 128)
    (throw (ex-info "settlement journal exceeded bound" {:reason :bound})))
  (let [owners (reduce
   (fn [owners [index entry]]
     (let [{:keys [owner operation event] sequence-number :seq} entry
           old (get owners owner {:callers #{} :retired? false :worker :absent
                                  :release :not-started})
           reject! (fn [reason]
                     (throw (ex-info "invalid settlement observation"
                                     {:reason reason :entry entry})))
           fields (case event
                    :retired #{:worker :retired?}
                    :release-outcome #{:outcome}
                    :caller-proof #{:operations-settled?}
                    :proof #{:sdk :exporter :overall :internal-users :exporter-retired?}
                    #{})
           _ (when-not (and (= (set (keys entry))
                               (into #{:seq :owner :operation :event} fields))
                            (= index sequence-number) (keyword? owner)
                            (keyword? operation)
                            (every? #(or (nil? %) (boolean? %) (keyword? %)
                                         (and (integer? %) (<= 0 % 128)))
                                    (vals entry)))
               (reject! :envelope))
           next-state
           (case event
             :caller-enter
             (do (when (or (:retired? old) (contains? (:callers old) operation))
                   (reject! :late-or-duplicate-caller))
                 (update old :callers conj operation))
             :caller-exit
             (do (when-not (contains? (:callers old) operation)
                   (reject! :unowned-caller))
                 (update old :callers disj operation))
             :retired (do (when-not (true? (:retired? entry))
                            (reject! :observed-admission-active))
                          (when-not (contains? #{:absent :running} (:worker entry))
                            (reject! :worker-envelope))
                          (assoc old :retired? true :worker (:worker entry)))
             :worker-exited (do (when-not (:retired? old) (reject! :worker-before-retirement))
                                (assoc old :worker :exited))
             :release-start
             (do (when (or (not (:retired? old)) (seq (:callers old))
                           (= :running (:worker old)))
                   (reject! :release-before-sdk-settlement))
                 (when-not (= :not-started (:release old))
                   (reject! :duplicate-release))
                 (assoc old :release :running :release-operation operation))
             :release-outcome
             (do (when-not (and (= :running (:release old))
                               (= operation (:release-operation old)))
                   (reject! :unowned-release-outcome))
                 (when-not (contains? #{:returned-true :returned-false
                                       :returned-unconfirmed :threw} (:outcome entry))
                   (reject! :release-outcome-envelope))
                 (assoc old :release (:outcome entry)))
             :caller-proof
             (do (when-not (and (boolean? (:operations-settled? entry))
                               (= (empty? (:callers old)) (:operations-settled? entry)))
                   (reject! :caller-bookkeeping-disagreement))
                 old)
             :proof
             (do
               (when-not (and (every? #(contains? #{:confirmed :unconfirmed} %)
                                      [(:sdk entry) (:exporter entry) (:overall entry)])
                              (integer? (:internal-users entry))
                              (boolean? (:exporter-retired? entry)))
                 (reject! :proof-envelope))
               (when (and (= :confirmed (:sdk entry))
                          (or (not (:retired? old)) (seq (:callers old))
                              (= :running (:worker old))))
                 (reject! :sdk-proof-before-settlement))
               (when (and (= :confirmed (:exporter entry))
                          (or (pos? (:internal-users entry))
                              (not (or (= :returned-true (:release old))
                                       (and (:exporter-retired? entry)
                                            (zero? (:internal-users entry)))))))
                 (reject! :exporter-proof-before-settlement))
               (when-not (= (= :confirmed (:overall entry))
                            (and (= :confirmed (:sdk entry))
                                 (= :confirmed (:exporter entry))))
                 (reject! :factor-disagreement))
               old)
             (reject! :unknown-event))]
       (assoc owners owner next-state)))
   {} (map-indexed vector trace))]
    (doseq [[owner state] owners]
      (when (= :running (:release state))
        (throw (ex-info "release observation has no outcome"
                        {:reason :incomplete-release :owner owner}))))
    owners))

(defn settlement-valid? [trace]
  (try (validate-settlement! trace) true (catch :default _ false)))
