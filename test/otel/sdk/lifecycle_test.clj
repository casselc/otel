(ns otel.sdk.lifecycle-test
  (:require [clojure.test :refer [deftest is testing]]
            [hegel.clojure-test :as ht]
            [hegel.generator :as g]
            [hegel.history :as history]
            [otel.metrics :as metrics-api]
            [otel.resource :as res]
            [otel.sdk :as sdk]
            [otel.sdk.export :as export]
            [otel.sdk.lifecycle :as lifecycle]
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

(defn- worker-owner-case
  [signal]
  (let [span-calls (atom 0)
        log-calls (atom 0)
        metric-calls (atom 0)
        markers {:span (atom :span-result)
                 :log (atom :log-result)
                 :metric (atom :metric-result)}
        exporter (->CountingExporter span-calls log-calls metric-calls
                                     (:span markers) (:log markers)
                                     (:metric markers))]
    (case signal
      :span
      (let [owner (export/batch-processor exporter {:schedule-delay-ms 60000})]
        {:owner owner
         :background-worker (:worker owner)
         :shutdown #(export/shutdown! %)
         :shutdown-calls span-calls
         :marker (:span markers)
         :late-accepted? #(do
                            (export/on-end % {:span-context {:trace-flags 1}})
                            (seq (:queue @(:state %))))})

      :log
      (let [owner (logs/batch-processor exporter {:schedule-delay-ms 60000})]
        {:owner owner
         :background-worker (:worker owner)
         :shutdown #(export/shutdown! %)
         :shutdown-calls log-calls
         :marker (:log markers)
         :late-accepted? #(do
                            (export/on-end % {:body "late"})
                            (seq (:queue @(:state %))))})

      :metric
      (let [provider (metrics/meter-provider {:resource res/empty-resource})
            owner (metrics/periodic-reader provider exporter {:interval-ms 60000})]
        {:owner owner
         :background-worker (:worker owner)
         :shutdown #(export/shutdown! %)
         :shutdown-calls metric-calls
         :marker (:metric markers)
         :late-accepted? #(export/force-flush! %)}))))

(defn- exercise-blocked-worker-shutdown!
  [signal interrupt?]
  (let [{:keys [owner background-worker shutdown shutdown-calls marker
                late-accepted?]}
        (worker-owner-case signal)
        worker-started (promise)
        release-worker (promise)
        worker (Thread. (fn []
                          (deliver worker-started true)
                          @release-worker))
        owner (assoc owner :worker worker)
        awaiting-worker (promise)
        await-worker! lifecycle/await-worker!
        invoked (repeatedly 3 promise)
        outcomes (repeatedly 3 promise)
        callers (mapv (fn [invoked outcome]
                        (Thread. (fn []
                                   (deliver invoked true)
                                   (deliver outcome (caught #(shutdown owner))))))
                      invoked outcomes)]
    (try
      (.start worker)
      (is (= true (deref worker-started 2000 ::timeout)))
      (with-redefs [lifecycle/await-worker!
                    (fn [worker]
                      (deliver awaiting-worker true)
                      (await-worker! worker))]
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
          (deliver release-worker true))
        (let [results (mapv #(deref % 2000 ::timeout) outcomes)]
          (if interrupt?
            (let [failures (mapv :throwable results)]
              (is (every? some? failures))
              (is (every? #(identical? (first failures) %) failures)
                  (str signal " callers share the interrupted terminal failure"))
              (is (= java.lang.InterruptedException (class (first failures))))
              (is (zero? @shutdown-calls)
                  (str signal " interrupted wait cannot release exporter")))
            (do
              (is (every? #(identical? marker (:value %)) results))
              (is (= 1 @shutdown-calls)
                  (str signal " exporter shuts down exactly once"))
              (is (not (late-accepted? owner))
                  (str signal " rejects work after shutdown"))))))
      (finally
        (deliver release-worker true)
        (swap! (:state owner) assoc :shutdown? true)
        (doseq [caller callers]
          (try (.join caller 2000) (catch :default _ nil)))
        (try (.join worker 2000) (catch :default _ nil))
        (try (.join background-worker 2000) (catch :default _ nil))))))

(deftest worker-quiescence-precedes-exporter-shutdown
  (doseq [signal [:span :metric :log]]
    (testing (name signal)
      (exercise-blocked-worker-shutdown! signal false))))

(deftest interrupted-worker-wait-is-a-shared-fail-safe-result
  (doseq [signal [:span :metric :log]]
    (testing (name signal)
      (exercise-blocked-worker-shutdown! signal true))))

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
