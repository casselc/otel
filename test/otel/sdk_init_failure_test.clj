(ns otel.sdk-init-failure-test
  (:require [clojure.test :refer [deftest is]]
            [otel.instrument.runtime :as runtime]
            [otel.sdk :as sdk]
            [otel.sdk.export :as export]
            [otel.sdk.lifecycle :as lifecycle]
            [otel.sdk.metrics :as metrics]))

(defn- initialization-rollback-observation [registration-fails?]
  (let [owners (atom {}) handle (atom nil) registrations (atom 0)
        registration-observation (atom nil)
        releases (atom {:spans 0 :metrics 0})
        failure (ex-info "controlled runtime registration failure" {})
        exporter (reify export/SpanExporter
                   (export-spans! [_ _] true)
                   (flush-exporter! [_] true)
                   (shutdown-exporter! [_]
                     (swap! releases update :spans inc) true)
                   export/MetricExporter
                   (export-metrics! [_ _ _] true)
                   (shutdown-metric-exporter! [_]
                     (swap! releases update :metrics inc) true))
        original-batch export/batch-processor
        original-reader metrics/periodic-reader
        snapshot (fn []
                   (into {} (map (fn [[kind owner]]
                                   [kind {:worker-alive? (.isAlive (:worker owner))
                                          :settlement (lifecycle/component-settlement owner)}])
                                 @owners)))]
    ;; This fixture must not discard another application's configured SDK.
    (is (nil? (sdk/tracer-provider)))
    (is (nil? (sdk/meter-provider)))
    (is (nil? (sdk/logger-provider)))
    (try
      (with-redefs [export/batch-processor
                    (fn [& args]
                      (let [owner (apply original-batch args)]
                        (swap! owners assoc :spans owner)
                        owner))
                    metrics/periodic-reader
                    (fn [& args]
                      (let [owner (apply original-reader args)]
                        (swap! owners assoc :metrics owner)
                        owner))
                    runtime/register!
                    (fn [_]
                      ;; No assertions inside constructor/registration observers.
                      ;; Retain only actual acquired references and scalar facts.
                      (swap! registrations inc)
                      (reset! registration-observation (snapshot))
                      (when registration-fails? (throw failure)))]
        (let [error (try
                      (reset! handle
                              (sdk/init! {:service-name "init-rollback-control"
                                          :exporter exporter :processor :batch
                                          :schedule-delay-ms 60000
                                          :metric-interval-ms 60000
                                          :metrics? true :runtime-metrics? true
                                          :logs? false :bridge-logging? false}))
                      nil
                      (catch Throwable error error))
              before-cleanup (snapshot)]
          (is (= 1 @registrations))
          (is (= #{:spans :metrics} (set (keys @owners))))
          (is (every? :worker-alive? (vals @registration-observation))
              "both maintained workers exist before injected registration failure")
          (if registration-fails?
            (do
              (is (identical? failure error))
              (is (nil? @handle))
              (is (nil? (sdk/tracer-provider)))
              (is (nil? (sdk/meter-provider)))
              (is (nil? (sdk/logger-provider))))
            (do
              (is (nil? error))
              (is (map? @handle))
              (is (identical? (:tracer-provider @handle) (sdk/tracer-provider)))
              (is (identical? (:meter-provider @handle) (sdk/meter-provider)))
              (sdk/shutdown! @handle)))
          (let [observation (if registration-fails? before-cleanup (snapshot))]
            (prn {:phase :sdk-init-rollback :registration-fails? registration-fails?
                  :owners observation :releases @releases
                  :returned-handle? (some? @handle)})
            observation)))
      (finally
        ;; Always use real maintained retirement, even after expected RED. Never
        ;; interrupt a worker as a substitute for its owner's shutdown proof.
        (if @handle
          (sdk/shutdown! @handle)
          (doseq [[_ owner] @owners] (export/shutdown! owner)))
        (doseq [[_ owner] @owners]
          (.join (:worker owner) 2000)
          (is (not (.isAlive (:worker owner)))))
        (is (= {:spans 1 :metrics 1} @releases))
        (is (nil? (sdk/tracer-provider)))
        (is (nil? (sdk/meter-provider)))
        (is (nil? (sdk/logger-provider)))))))

(defn- retired-before-return? [owners]
  (and (= #{:spans :metrics} (set (keys owners)))
       (every? (fn [[_ {:keys [worker-alive? settlement]}]]
                 (and (not worker-alive?) (= :confirmed (:quiescence settlement))))
               owners)))

(deftest sdk-init-registration-failure-retires-acquired-workers
  (is (retired-before-return? (initialization-rollback-observation false))
      "same oracle accepts real successful initialization followed by SDK shutdown")
  ;; Expected old-path RED: real init! throws without returning its already
  ;; acquired span/metric owners or retiring their independently live workers.
  (is (retired-before-return? (initialization-rollback-observation true))
      "SDK must retire acquired workers before constructor failure escapes"))
