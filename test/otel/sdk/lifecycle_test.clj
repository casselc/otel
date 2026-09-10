(ns otel.sdk.lifecycle-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [hegel.clojure-test :as ht]
            [hegel.generator :as g]
            [hegel.history :as history]
            [otel.metrics :as metrics-api]
            [otel.resource :as res]
            [otel.sdk :as sdk]
            [otel.sdk.export :as export]
            [otel.sdk.lifecycle :as lifecycle]
            [otel.sdk.lifecycle-events :as lifecycle-events]
            [otel.sdk.logs :as logs]
            [otel.sdk.metrics :as metrics]
            [otel.sdk.tracer :as tracer]))

(deftest terminal-action-preserves-result-and-throwable-identity
  (testing "a returned object is memoized without boolean coercion"
    (let [terminal (lifecycle/terminal-action)
          calls (atom 0)
          marker (atom :marker)
          run #(lifecycle/run-terminal!
                 terminal
                 (fn [] (swap! calls inc) marker))]
      (is (identical? marker (run)))
      (is (identical? marker (run)))
      (is (= 1 @calls))))
  (testing "the first Throwable object is rethrown to every caller"
    (let [terminal (lifecycle/terminal-action)
          calls (atom 0)
          failure (ex-info "terminal failure" {:expected true})
          run #(try
                 (lifecycle/run-terminal!
                   terminal
                   (fn [] (swap! calls inc) (throw failure)))
                 nil
                 (catch :default throwable throwable))]
      (is (identical? failure (run)))
      (is (identical? failure (run)))
      (is (= 1 @calls)))))

(defn- caught
  [f]
  (try
    {:value (f)}
    (catch :default throwable
      {:throwable throwable})))

(defrecord CountingExporter [span-calls log-calls metric-calls
                             span-marker log-marker metric-marker]
  export/SpanExporter
  (export-spans! [_ _] true)
  (flush-exporter! [_] true)
  (shutdown-exporter! [_] (swap! span-calls inc) span-marker)

  logs/LogRecordExporter
  (export-logs! [_ _] true)
  (shutdown-log-exporter! [_] (swap! log-calls inc) log-marker)

  export/MetricExporter
  (export-metrics! [_ _ _] true)
  (shutdown-metric-exporter! [_] (swap! metric-calls inc) metric-marker))

(defn- blocked-export!
  [events export-calls export-started release-export]
  (lifecycle-events/record! events :export-started)
  (swap! export-calls inc)
  (deliver export-started true)
  @release-export
  (lifecycle-events/record! events :export-finished)
  true)

(defn- close-exporter!
  [events shutdown-calls marker failure]
  (lifecycle-events/record! events :exporter-close-called)
  (swap! shutdown-calls inc)
  (if failure
    (do
      (lifecycle-events/record! events :exporter-close-failed)
      (throw failure))
    (do
      (lifecycle-events/record! events :exporter-close-returned)
      marker)))

(defrecord LifecycleExporter [events shutdown-calls marker failure
                              export-calls export-started release-export]
  export/SpanExporter
  (export-spans! [_ _]
    (blocked-export! events export-calls export-started release-export))
  (flush-exporter! [_] true)
  (shutdown-exporter! [_]
    (close-exporter! events shutdown-calls marker failure))

  logs/LogRecordExporter
  (export-logs! [_ _]
    (blocked-export! events export-calls export-started release-export))
  (shutdown-log-exporter! [_]
    (close-exporter! events shutdown-calls marker failure))

  export/MetricExporter
  (export-metrics! [_ _ _]
    (blocked-export! events export-calls export-started release-export))
  (shutdown-metric-exporter! [_]
    (close-exporter! events shutdown-calls marker failure)))

(defn- worker-owner-case
  [signal events close-failure?]
  (let [shutdown-calls (atom 0)
        marker (atom [signal :shutdown-result])
        failure (when close-failure?
                  (ex-info "exporter close failed"
                           {:signal signal :expected true}))
        export-started (promise)
        release-export (promise)
        export-calls (atom 0)
        exporter (->LifecycleExporter events shutdown-calls marker failure
                                      export-calls export-started release-export)
        ownership {:shutdown-calls shutdown-calls
                   :marker marker
                   :failure failure
                   :export-calls export-calls
                   :export-started export-started
                   :release-export release-export}]
    (case signal
      :span
      (let [owner (export/batch-processor exporter {:schedule-delay-ms 60000})]
        (merge ownership
               {:owner owner
                :background-worker (:worker owner)
                :shutdown #(export/shutdown! %)
                :run-export! #(export/export-spans! exporter [{}])}))

      :log
      (let [owner (logs/batch-processor exporter {:schedule-delay-ms 60000})]
        (merge ownership
               {:owner owner
                :background-worker (:worker owner)
                :shutdown #(export/shutdown! %)
                :run-export! #(logs/export-logs! exporter [{}])}))

      :metric
      (let [provider (metrics/meter-provider {:resource res/empty-resource})
            owner (metrics/periodic-reader provider exporter {:interval-ms 60000})]
        (merge ownership
               {:owner owner
                :background-worker (:worker owner)
                :shutdown #(export/shutdown! %)
                :run-export! #(export/export-metrics! exporter {} [{}])})))))

(defn- exercise-blocked-worker-shutdown!
  [signal interrupt? close-failure?]
  (let [events (atom [])
        {:keys [owner background-worker shutdown shutdown-calls marker
                failure export-started release-export run-export!]}
        (worker-owner-case signal events close-failure?)
        worker (Thread. run-export!)
        owner (assoc owner :worker worker)
        terminal-completed? (atom false)
        awaiting-worker (promise)
        await-worker! lifecycle/await-worker!
        observe-shutdown
        (fn []
          (lifecycle-events/record! events :shutdown-requested)
          (let [result (caught #(shutdown owner))]
            ;; run-terminal! publishes before returning. Serialize this
            ;; test-only observation so the completion event necessarily
            ;; precedes every caller-observation event in the trace.
            (locking terminal-completed?
              (when-not @terminal-completed?
                (reset! terminal-completed? true)
                (lifecycle-events/record!
                  events
                  (if (:throwable result)
                    :terminal-completed-failure
                    :terminal-completed-return)))
              (lifecycle-events/record!
                events
                (if (:throwable result)
                  :terminal-observed-failure
                  :terminal-observed-return)))
            result))
        invoked (repeatedly 3 promise)
        outcomes (repeatedly 3 promise)
        callers (mapv (fn [invoked outcome]
                        (Thread. (fn []
                                   (deliver invoked true)
                                   (deliver outcome (observe-shutdown)))))
                      invoked outcomes)]
    (try
      (.start worker)
      (is (= true (deref export-started 2000 ::timeout)))
      (with-redefs [lifecycle/await-worker!
                    (fn [worker]
                      (deliver awaiting-worker true)
                      (try
                        (let [result (await-worker! worker)]
                          (lifecycle-events/record! events :worker-terminal)
                          (lifecycle-events/record!
                            events :worker-wait-succeeded)
                          result)
                        (catch :default throwable
                          (lifecycle-events/record!
                            events :worker-wait-failed)
                          (throw throwable))))]
        ;; Start the terminal owner first so the interruption case targets the
        ;; thread actually blocked in join, not a follower awaiting its result.
        (.start (first callers))
        (is (= true (deref (first invoked) 2000 ::timeout)))
        (is (= true (deref awaiting-worker 2000 ::timeout)))
        (doseq [caller (next callers)] (.start caller))
        (is (every? #(= true (deref % 2000 ::timeout)) invoked))
        (is (every? #(= ::timeout (deref % 50 ::timeout)) outcomes)
            (str signal " shutdown callers wait for worker quiescence"))
        (is (zero? @shutdown-calls)
            (str signal " exporter remains open while worker is live"))
        (if interrupt?
          (.interrupt (first callers))
          (deliver release-export true))
        (let [concurrent-results (mapv #(deref % 2000 ::timeout) outcomes)
              repeated-result (observe-shutdown)
              results (conj concurrent-results repeated-result)]
          (if (or interrupt? close-failure?)
            (let [failures (mapv :throwable results)]
              (is (every? some? failures))
              (is (every? #(identical? (first failures) %) failures)
                  (str signal " callers share the terminal failure"))
              (if interrupt?
                (do
                  (is (= java.lang.InterruptedException
                         (class (first failures))))
                  (is (zero? @shutdown-calls)
                      (str signal
                           " interrupted wait cannot call exporter close")))
                (do
                  (is (identical? failure (first failures)))
                  (is (= 1 @shutdown-calls)
                      (str signal " calls the throwing exporter once")))))
            (do
              (is (every? #(identical? marker (:value %)) results))
              (is (= 1 @shutdown-calls)
                  (str signal " exporter shuts down exactly once")))))
        (let [final-state (lifecycle-events/validate! @events)]
          (is (= 4 (:shutdown-requests final-state)))
          (is (= (if (or interrupt? close-failure?) :failure :return)
                 (:terminal-result final-state)))
          (is (= (if interrupt? 0 1) (:close-count final-state)))
          (is (= (cond
                   interrupt? :not-called
                   close-failure? :failed
                   :else :returned)
                 (:close-outcome final-state)))
          (is (zero? (:owner-work-rejected-count final-state)))))
      (finally
        (deliver release-export true)
        (swap! (:state owner) assoc :shutdown? true)
        (doseq [caller callers]
          (try (.join caller 2000) (catch :default _ nil)))
        (try (.join worker 2000) (catch :default _ nil))
        (try (.join background-worker 2000) (catch :default _ nil))))))

(deftest worker-quiescence-precedes-exporter-shutdown
  (doseq [signal [:span :metric :log]]
    (testing (name signal)
      (exercise-blocked-worker-shutdown! signal false false))))

(deftest interrupted-worker-wait-is-a-shared-fail-safe-result
  (doseq [signal [:span :metric :log]]
    (testing (name signal)
      (exercise-blocked-worker-shutdown! signal true false))))

(deftest exporter-close-failure-is-one-shared-terminal-result
  (doseq [signal [:span :metric :log]]
    (testing (name signal)
      (exercise-blocked-worker-shutdown! signal false true))))

(defn- real-worker-owner-case
  [signal events]
  (let [shutdown-calls (atom 0)
        export-calls (atom 0)
        marker (atom [signal :real-worker-shutdown-result])
        export-started (promise)
        release-export (promise)
        exporter (->LifecycleExporter events shutdown-calls marker nil
                                      export-calls export-started release-export)
        common {:shutdown-calls shutdown-calls
                :export-calls export-calls
                :marker marker
                :exporter exporter
                :export-started export-started
                :release-export release-export}]
    (case signal
      :span
      (let [owner (export/batch-processor exporter {:schedule-delay-ms 1})]
        (merge common
               {:owner owner
                :start-owner-work!
                #(export/on-end owner {:span-context {:trace-flags 1}})
                :probe-late-owner-work!
                #(let [before (export/queue-size owner)]
                   (export/on-end owner {:span-context {:trace-flags 1}})
                   {:rejected? (= before (export/queue-size owner))})}))

      :log
      (let [owner (logs/batch-processor exporter {:schedule-delay-ms 1})]
        (merge common
               {:owner owner
                :start-owner-work! #(export/on-end owner {:body "accepted"})
                :probe-late-owner-work!
                #(let [before (logs/dropped-count owner)]
                   (export/on-end owner {:body "late"})
                   {:rejected? (= (inc before)
                                  (logs/dropped-count owner))})}))

      :metric
      (let [provider (metrics/meter-provider {:resource res/empty-resource})
            meter (metrics/get-meter provider {:name "lifecycle-test"})
            counter (metrics-api/counter meter "lifecycle.owner.work")
            owner (metrics/periodic-reader provider exporter {:interval-ms 1})]
        (merge common
               {:owner owner
                :provider provider
                :counter counter
                :start-owner-work! #(metrics-api/add! counter 1)
                ;; Recording remains valid on an existing instrument. The
                ;; reader owns collection/export and must reject that work.
                :probe-late-owner-work!
                #(do
                   (metrics-api/add! counter 1)
                   (let [before @export-calls
                         result (export/force-flush! owner)]
                     {:rejected? (false? result)
                      :flush-result result
                      :export-calls-before before
                      :export-calls-after @export-calls}))})))))

(defn- exercise-real-background-worker!
  [signal]
  (let [events (atom [])
        {:keys [owner exporter provider counter shutdown-calls export-calls
                marker export-started release-export start-owner-work!
                probe-late-owner-work!]}
        (real-worker-owner-case signal events)
        await-worker! lifecycle/await-worker!
        result (promise)
        caller (Thread.
                 (fn []
                   (lifecycle-events/record! events :shutdown-requested)
                   (deliver result (caught #(export/shutdown! owner)))))]
    (try
      (start-owner-work!)
      (is (= true (deref export-started 2000 ::timeout)))
      (with-redefs [lifecycle/await-worker!
                    (fn [worker]
                      (try
                        (let [joined (await-worker! worker)]
                          (lifecycle-events/record! events :worker-terminal)
                          (lifecycle-events/record!
                            events :worker-wait-succeeded)
                          joined)
                        (catch :default throwable
                          (lifecycle-events/record! events :worker-wait-failed)
                          (throw throwable))))]
        (.start caller)
        (is (= ::timeout (deref result 50 ::timeout))
            (str signal " real worker keeps shutdown incomplete"))
        (is (zero? @shutdown-calls)
            (str signal " real worker retains exporter ownership"))
        (deliver release-export true)
        (let [outcome (deref result 2000 ::timeout)]
          (is (identical? marker (:value outcome)))
          ;; run-terminal! publishes internally before returning. These two
          ;; events reconstruct that boundary in the test trace; identity is
          ;; asserted directly above rather than inferred from event order.
          (lifecycle-events/record! events :terminal-completed-return)
          (lifecycle-events/record! events :terminal-observed-return))
        (is (= 1 @shutdown-calls))
        (let [{:keys [rejected? flush-result export-calls-before
                      export-calls-after]}
              (probe-late-owner-work!)]
          (is rejected?
              (str signal " rejects owner work after shutdown"))
          (when (= :metric signal)
            (is (false? flush-result)
                "the shut-down reader explicitly rejects force-flush")
            (is (= export-calls-before export-calls-after)
                "the rejected metric collection never reaches the exporter")))
        (lifecycle-events/record! events :owner-work-rejected)
        (is (= :return
               (:terminal-result (lifecycle-events/validate! @events))))
        (when (= :metric signal)
          (let [before @export-calls
                unguarded-collect-and-export!
                (ns-resolve 'otel.sdk.metrics 'collect-and-export!)]
            (is (some? unguarded-collect-and-export!))
            ;; Causal runtime mutant: this is the force-flush body with its
            ;; reader :shutdown? guard removed. The fresh post-shutdown counter
            ;; value reaches the exporter and the ownership trace turns red.
            (is (true? (boolean
                         (unguarded-collect-and-export! provider exporter))))
            (is (= (inc before) @export-calls))
            (is (not (lifecycle-events/valid? @events))))))
      (finally
        (deliver release-export true)
        (swap! (:state owner) assoc :shutdown? true)
        (try (.join caller 2000) (catch :default _ nil))
        (try (.join (:worker owner) 2000) (catch :default _ nil))))))

(deftest real-background-worker-retains-exporter-through-owner-work
  (doseq [signal [:span :metric :log]]
    (testing (name signal)
      (exercise-real-background-worker! signal))))

(deftest lifecycle-trace-validator-kills-ownership-mutants
  (let [entries #(map-indexed (fn [seq event] {:seq seq :event event}) %)]
    (testing "a timed join cannot stand in for worker termination"
      (is (not (lifecycle-events/valid?
                 (entries [:shutdown-requested
                           :worker-wait-succeeded
                           :exporter-close-called])))))
    (testing "a failed wait cannot be swallowed before exporter close"
      (is (not (lifecycle-events/valid?
                 (entries [:shutdown-requested
                           :worker-wait-failed
                           :exporter-close-called])))))
    (testing "no export begins after worker ownership is released"
      (is (not (lifecycle-events/valid?
                 (entries [:shutdown-requested
                           :worker-terminal
                           :export-started])))))
    (testing "exporter close is exactly once"
      (is (not (lifecycle-events/valid?
                 (entries [:shutdown-requested
                           :worker-terminal
                           :worker-wait-succeeded
                           :exporter-close-called
                           :exporter-close-returned
                           :exporter-close-called])))))))

(deftest lifecycle-trace-validator-allows-accepted-shutdown-drain
  (let [entries (map-indexed
                  (fn [seq event] {:seq seq :event event})
                  [:shutdown-requested
                   :export-started
                   :export-finished
                   :worker-terminal
                   :worker-wait-succeeded
                   :exporter-close-called
                   :exporter-close-returned
                   :terminal-completed-return
                   :terminal-observed-return])]
    (is (= {:close-count 1
            :close-outcome :returned
            :terminal-result :return}
           (select-keys (lifecycle-events/validate! entries)
                        [:close-count :close-outcome :terminal-result])))))

(defn- path-sensitive-lifecycle-workflow?
  [workflow]
  (and
    (every? #(str/includes? workflow (str "      - " %))
            ["formal/quint/shutdown-lifecycle.md"
             "scripts/check-shutdown-lifecycle-quint.sh"
             "src/otel/sdk/lifecycle.clj"
             "src/otel/sdk/export.clj"
             "src/otel/sdk/logs.clj"
             "src/otel/sdk/metrics.clj"
             "test/otel/sdk/lifecycle_events.clj"
             "test/otel/sdk/lifecycle_test.clj"])
    (not (str/includes? workflow "      - src/**"))
    (str/includes? workflow
                   "run: scripts/check-shutdown-lifecycle-quint.sh")
    (str/includes? workflow "@informalsystems/quint@0.32.0")
    (str/includes? workflow
                   "github.com/driusan/lmt@62fe18f2f6a6e11c158ff2b2209e1082a4fcd59c")
    (str/includes? workflow
                   "actions/setup-node@49933ea5288caeca8642d1e84afbd3f7d6820020")
    (str/includes? workflow
                   "actions/setup-go@924ae3a1cded613372ab5595356fb5720e22ba16")
    (str/includes? workflow "permissions:\n  contents: read")
    (str/includes? workflow "persist-credentials: false")))

(deftest shutdown-lifecycle-model-workflow-is-path-sensitive
  (let [workflow (slurp ".github/workflows/shutdown-lifecycle-quint.yml")]
    (is (path-sensitive-lifecycle-workflow? workflow))
    (testing "broadening or dropping a lifecycle path turns the guard red"
      (is (not (path-sensitive-lifecycle-workflow?
                 (str/replace workflow
                              "      - src/otel/sdk/logs.clj\n"
                              ""))))
      (is (not (path-sensitive-lifecycle-workflow?
                 (str/replace workflow
                              "      - src/otel/sdk/logs.clj\n"
                              "      - src/**\n")))))))

(deftest each-signal-owner-invokes-its-exporter-once
  (let [span-calls (atom 0)
        log-calls (atom 0)
        metric-calls (atom 0)
        markers [(atom :span) (atom :log) (atom :metric)]
        exporter (->CountingExporter span-calls log-calls metric-calls
                                     (nth markers 0) (nth markers 1) (nth markers 2))
        span-processor (export/simple-processor exporter)
        log-processor (logs/simple-processor exporter)
        provider (metrics/meter-provider {:resource res/empty-resource})
        reader (metrics/periodic-reader provider exporter {:interval-ms 60000})]
    (is (identical? (nth markers 0) (export/shutdown! span-processor)))
    (is (identical? (nth markers 0) (export/shutdown! span-processor)))
    (is (identical? (nth markers 1) (export/shutdown! log-processor)))
    (is (identical? (nth markers 1) (export/shutdown! log-processor)))
    (is (identical? (nth markers 2) (export/shutdown! reader)))
    (is (identical? (nth markers 2) (export/shutdown! reader)))
    (is (= [1 1 1] [@span-calls @log-calls @metric-calls]))))

(deftest sdk-handle-shutdown-is-one-terminal-action
  (let [span-calls (atom 0)
        log-calls (atom 0)
        metric-calls (atom 0)
        markers [(atom :span) (atom :log) (atom :metric)]
        exporter (->CountingExporter span-calls log-calls metric-calls
                                     (nth markers 0) (nth markers 1) (nth markers 2))
        handle (sdk/init! {:exporter exporter
                           :processor :simple
                           :runtime-metrics? false
                           :logs? true
                           :bridge-logging? false})
        first-result (sdk/shutdown! handle)]
    (is (identical? first-result (sdk/shutdown! handle)))
    (is (= [1 1 1] [@span-calls @log-calls @metric-calls]))
    (is (= [] (metrics/collect! (:meter-provider handle))))
    (is (identical? metrics-api/noop-meter
                    (metrics/get-meter (:meter-provider handle) {:name "late"})))))

(defrecord TerminalProcessor [calls marker failure]
  export/SpanProcessor
  (on-start [_ _ _] nil)
  (on-end [_ _] nil)
  (force-flush! [_] true)
  (shutdown! [_]
    (swap! calls inc)
    (if failure (throw failure) marker)))

(deftest provider-shutdown-memoizes-a-child-failure
  (let [calls (atom 0)
        later-calls (atom 0)
        failure (ex-info "processor shutdown failed" {:expected true})
        processor (->TerminalProcessor calls nil failure)
        later-processor (->TerminalProcessor later-calls (atom :later) nil)
        provider (tracer/tracer-provider {:resource res/empty-resource
                                          :processors [processor later-processor]})
        shutdown #(try (tracer/shutdown! provider)
                       nil
                       (catch :default throwable throwable))]
    (is (identical? failure (shutdown)))
    (is (identical? failure (shutdown)))
    (is (= 1 @calls))
    (is (= 1 @later-calls)
        "a failed child does not skip later owned shutdowns")))

(defrecord HistoryExporter [accepted shutdown-calls marker failure]
  export/SpanExporter
  (export-spans! [_ spans]
    (swap! accepted into (map :history/id spans))
    true)
  (flush-exporter! [_] true)
  (shutdown-exporter! [_]
    (swap! shutdown-calls inc)
    (if failure (throw failure) marker)))

(defn- append-event!
  [events event]
  (locking events
    (let [event (assoc event :seq (count @events))]
      (swap! events conj event)
      event)))

(defn- history-step
  [accepted expected-outcome state operation]
  (case (:operation operation)
    :emit
    (let [accepted? (contains? accepted (:input operation))]
      (cond
        (:closed? state) (when-not accepted? {:state state})
        accepted? (when (= :return (:outcome operation))
                    {:state (update state :accepted conj (:input operation))})
        :else nil))

    :shutdown
    (when (and (= expected-outcome (:outcome operation))
               (= expected-outcome (:value operation)))
      {:state (assoc state :closed? true)})

    nil))

(defn- run-concurrent-history!
  [emit-count shutdown-count throwing? batch?]
  (let [events (atom [])
        accepted (atom #{})
        shutdown-calls (atom 0)
        marker (atom :shutdown-result)
        failure (when throwing? (ex-info "shutdown failed" {:expected true}))
        exporter (->HistoryExporter accepted shutdown-calls marker failure)
        processor (if batch?
                    (export/batch-processor exporter {:schedule-delay-ms 60000})
                    (export/simple-processor exporter))
        start (promise)
        results (atom [])
        operation
        (fn [operation-id operation input call]
          (let [done (promise)
                thread
                (Thread.
                  (fn []
                    @start
                    (append-event! events
                                   {:operation-id operation-id
                                    :phase :invoke
                                    :operation operation
                                    :input input})
                    (try
                      (let [value (call)]
                        (append-event! events
                                       {:operation-id operation-id
                                        :phase :return
                                        :value (if (= :shutdown operation)
                                                 :return
                                                 value)})
                        (when (= :shutdown operation)
                          (swap! results conj value)))
                      (catch :default throwable
                        (append-event! events
                                       {:operation-id operation-id
                                        :phase :throw
                                        :value :throw})
                        (when (= :shutdown operation)
                          (swap! results conj throwable)))
                      (finally (deliver done true)))))]
            (.setDaemon thread true)
            {:thread thread :done done}))
        emit-workers
        (mapv (fn [id]
                (operation [:emit id] :emit id
                           #(export/on-end processor
                                           {:history/id id
                                            :span-context {:trace-flags 1}})))
              (range emit-count))
        shutdown-workers
        (mapv (fn [id]
                (operation [:shutdown id] :shutdown nil
                           #(export/shutdown! processor)))
              (range shutdown-count))
        workers (into emit-workers shutdown-workers)]
    (doseq [{:keys [thread]} workers] (.start thread))
    (deliver start true)
    ;; hegel.history deliberately requires a complete history. Join every
    ;; generated worker before taking the snapshot; a missing terminal is a
    ;; harness failure, not a history for the checker to reinterpret.
    (doseq [{:keys [thread]} workers] (.join thread 5000))
    (when-not (every? #(= true (deref (:done %) 0 false)) workers)
      (throw (ex-info "generated lifecycle worker did not complete"
                      {:hegel/origin "otel.shutdown-history/worker-completion"})))
    (let [expected-outcome (if throwing? :throw :return)
          snapshot @events
          witness (history/check!
                    {:closed? false :accepted #{}}
                    (partial history-step @accepted expected-outcome)
                    snapshot
                    {:name :otel-shutdown-linearizable
                     :max-operations 6})]
      (is (= 1 @shutdown-calls))
      (is (= @accepted (get-in witness [:final-state :accepted])))
      (is (= shutdown-count (count @results)))
      (is (every? #(identical? (if throwing? failure marker) %) @results))
      witness)))

(deftest concurrent-shutdown-history-is-linearizable
  (ht/with {:test-cases 30
            :database ""
            :derandomize? true
            :verbosity :quiet}
    [emit-count (g/integer 1 3)
     shutdown-count (g/integer 1 3)
     throwing? (g/boolean)
     batch? (g/boolean)]
    (run-concurrent-history! emit-count shutdown-count throwing? batch?)))
