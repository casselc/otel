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
