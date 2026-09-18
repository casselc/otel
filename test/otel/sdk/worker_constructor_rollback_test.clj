(ns otel.sdk.worker-constructor-rollback-test
  (:require [clojure.test :refer [deftest is]]
            [otel.sdk.export :as export]
            [otel.sdk.lifecycle :as lifecycle]
            [otel.sdk.metrics :as metrics]))

(defn- controlled-exporter [counts release-value]
  (let [retired (atom false)]
    (reify export/SpanExporter
      (export-spans! [_ _] true) (flush-exporter! [_] true)
      (shutdown-exporter! [_]
        (swap! counts inc) (reset! retired true) release-value)
      export/MetricExporter
      (export-metrics! [_ _ _] true)
      (shutdown-metric-exporter! [_]
        (swap! counts inc) (reset! retired true) release-value)
      lifecycle/SettlementWitness
      (settlement-status [_] {:quiescence (if @retired :confirmed :unconfirmed)}))))

(defn- worker-retired? [owner]
  (and (not (.isAlive (:worker owner)))
       (= :confirmed (:quiescence (lifecycle/component-settlement owner)))))

(deftest worker-record-is-acquired-before-post-start-failure
  (doseq [kind [:spans :metrics] release-value [true false]]
    (let [receipt (lifecycle/construction-receipt) owner (atom nil)
          alive-at-failure (atom false) calls (atom 0)
          exporter (controlled-exporter calls release-value)
          failure (ex-info "controlled post-start failure" {})
          start lifecycle/start-owned-worker!]
      (try
        (let [error
              (with-redefs [lifecycle/start-owned-worker!
                            (fn [receipt acquired worker]
                              (reset! owner (start receipt acquired worker))
                              (reset! alive-at-failure (.isAlive worker))
                              (throw failure))]
                (try
                  (if (= :spans kind)
                    (export/batch-processor exporter {:schedule-delay-ms 60000} receipt)
                    (metrics/periodic-reader (metrics/meter-provider {}) exporter
                                             {:interval-ms 60000} receipt))
                  nil (catch Throwable error error)))]
          (is @alive-at-failure)
          (is (identical? failure error))
          (is (not (.isAlive (:worker @owner))))
          (is (= :confirmed (:quiescence (lifecycle/component-settlement @owner))))
          (is (worker-retired? @owner))
          (is (= {:otel.sdk.construction/version 1 :outcome :failed :quiescence :confirmed}
                 (lifecycle/construction-failure-status receipt error)))
          (is (= 1 @calls)))
        (finally
          (when @owner
            (export/shutdown! @owner) (.join (:worker @owner) 2000)
            (is (not (.isAlive (:worker @owner)))))
          (is (= 1 @calls)))))))

(deftest worker-record-is-acquired-before-bypassed-start-helper-failure
  ;; The wrapper starts the SDK-created worker without delegating. The same
  ;; retirement oracle must hold: ownership precedes this replaceable seam.
  (doseq [kind [:spans :metrics]]
    (let [receipt (lifecycle/construction-receipt) owner (atom nil)
          alive-at-failure (atom false) calls (atom 0)
          exporter (controlled-exporter calls true)
          failure (ex-info "controlled bypass-start failure" {})]
      (try
        (let [error
              (with-redefs [lifecycle/start-owned-worker!
                            (fn [_ acquired worker]
                              (reset! owner acquired)
                              (.setDaemon worker true)
                              (.start worker)
                              (reset! alive-at-failure (.isAlive worker))
                              (throw failure))]
                (try
                  (if (= :spans kind)
                    (export/batch-processor exporter {:schedule-delay-ms 60000} receipt)
                    (metrics/periodic-reader (metrics/meter-provider {}) exporter
                                             {:interval-ms 60000} receipt))
                  nil (catch Throwable error error)))]
          (is @alive-at-failure)
          (is (identical? failure error))
          (is (worker-retired? @owner))
          (is (= :confirmed
                 (:quiescence (lifecycle/construction-failure-status receipt error))))
          (is (= 1 @calls)))
        (finally
          (when @owner
            (export/shutdown! @owner) (.join (:worker @owner) 2000)
            (is (worker-retired? @owner)))
          (is (= 1 @calls)))))))

(deftest aggregate-failure-retires-every-incrementally-acquired-worker
  (let [receipt (lifecycle/construction-receipt) acquired (atom [])
        calls [(atom 0) (atom 0)] count-starts (atom 0)
        failure (ex-info "controlled second owner failure" {})
        start lifecycle/start-owned-worker!]
    (try
      (let [error
            (with-redefs [lifecycle/start-owned-worker!
                          (fn [receipt owner worker]
                            (let [result (start receipt owner worker)]
                              (swap! acquired conj result)
                              (when (= 2 (swap! count-starts inc)) (throw failure))
                              result))]
              (try
                (export/independent-batch-pipelines
                 (array-map :local {:exporter (controlled-exporter (first calls) true)}
                            :remote {:exporter (controlled-exporter (second calls) false)})
                 receipt)
                nil (catch Throwable error error)))]
        (is (= 2 (count @acquired)))
        (is (identical? failure error))
        (is (= :confirmed (:quiescence (lifecycle/construction-failure-status receipt error))))
        (doseq [owner @acquired]
          (is (not (.isAlive (:worker owner))))
          (is (= :confirmed (:quiescence (lifecycle/component-settlement owner)))))
        (is (= [1 1] (mapv deref calls))))
      (finally
        (doseq [owner @acquired]
          (export/shutdown! owner) (.join (:worker owner) 2000)
          (is (not (.isAlive (:worker owner)))))
        (is (= [1 1] (mapv deref calls)))))))

(deftest advice-throw-after-successful-child-return-is-not-failure-proof
  (let [receipt (lifecycle/construction-receipt) owners (atom []) calls (atom 0)
        wrapped-error (ex-info "outside successful constructor" {})
        batch export/batch-processor]
    (try
      (let [error
            (with-redefs [export/batch-processor
                          (fn [exporter config child]
                            (let [owner (batch exporter config child)]
                              (swap! owners conj owner)
                              (throw wrapped-error)))]
              (try
                (export/independent-batch-pipelines
                 {:local {:exporter (controlled-exporter calls true)}} receipt)
                nil (catch Throwable error error)))
            retry-stop! (:retry-stop! (ex-data error))]
        (is (= :construction-cleanup-incomplete (:otel.sdk/error (ex-data error))))
        (is (= :unconfirmed (:quiescence (lifecycle/construction-failure-status receipt error))))
        (is (= :closing (:status (retry-stop!))))
        (is (= 1 @calls))
        (is (every? #(not (.isAlive (:worker %))) @owners)))
      (finally
        (doseq [owner @owners]
          (export/shutdown! owner) (.join (:worker owner) 2000)
          (is (not (.isAlive (:worker owner)))))
        (is (= 1 @calls))))))
