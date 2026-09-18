(ns otel.sdk.init-failure-receipt-test
  (:require [clojure.test :refer [deftest is]]
            [otel.bridge.tools-logging :as bridge]
            [otel.instrument.runtime :as runtime]
            [otel.sdk :as sdk]
            [otel.sdk.export :as export]
            [otel.sdk.lifecycle :as lifecycle]
            [otel.sdk.logs :as logs]
            [otel.sdk.metrics :as metrics]))

(defn- controlled-exporter [releases]
  (reify export/SpanExporter
    (export-spans! [_ _] true) (flush-exporter! [_] true)
    (shutdown-exporter! [_] (swap! releases update :spans inc) true)
    export/MetricExporter
    (export-metrics! [_ _ _] true)
    (shutdown-metric-exporter! [_] (swap! releases update :metrics inc) true)
    logs/LogRecordExporter
    (export-logs! [_ _] true)
    (shutdown-log-exporter! [_] (swap! releases update :logs inc) true)))

(defn- retired? [owners]
  (every? #(and (not (.isAlive (:worker %)))
                (= :confirmed (:quiescence (lifecycle/component-settlement %)))) owners))

(deftest invocation-receipt-observes-runtime-and-bridge-rollback
  (doseq [seam [:runtime :bridge]]
    (let [receipt (lifecycle/construction-receipt) owners (atom [])
          releases (atom {:spans 0 :metrics 0 :logs 0})
          exporter (controlled-exporter releases)
          failure (ex-info "controlled initialization seam" {})
          batch export/batch-processor reader metrics/periodic-reader
          log-batch logs/batch-processor install bridge/install-owned!]
      (try
        (let [error
              (with-redefs [export/batch-processor
                            (fn [& args] (let [owner (apply batch args)]
                                          (swap! owners conj owner) owner))
                            metrics/periodic-reader
                            (fn [& args] (let [owner (apply reader args)]
                                          (swap! owners conj owner) owner))
                            logs/batch-processor
                            (fn [& args] (let [owner (apply log-batch args)]
                                          (swap! owners conj owner) owner))
                            runtime/register! (fn [_] (when (= seam :runtime) (throw failure)))
                            bridge/install-owned!
                            (fn [token] (let [previous (install token)]
                                          (when (= seam :bridge) (throw failure)) previous))]
                (try (sdk/init! {:exporter exporter :metrics? true :logs? true
                                 :runtime-metrics? true :bridge-logging? true
                                 :schedule-delay-ms 60000 :metric-interval-ms 60000
                                 :construction-receipt receipt})
                     nil (catch Throwable error error)))]
          (is (identical? failure error))
          (is (= 3 (count @owners)))
          (is (every? #(identical? exporter (:exporter %)) @owners))
          (is (retired? @owners))
          (is (= {:spans 1 :metrics 1 :logs 1} @releases))
          (is (= :confirmed (:quiescence (lifecycle/construction-failure-status receipt error))))
          (is (= :unknown (:outcome (lifecycle/construction-failure-status receipt nil))))
          (is (= :unknown (:outcome (lifecycle/construction-failure-status receipt
                                                                  (ex-info "replacement" {})))))
          (is (nil? (sdk/tracer-provider)))
          (is (nil? (sdk/meter-provider)))
          (is (nil? (sdk/logger-provider))))
        (finally
          (doseq [owner @owners]
            (export/shutdown! owner) (.join (:worker owner) 2000)
            (is (not (.isAlive (:worker owner))))))))))

(deftest orphan-validation-failure-releases-only-the-unclaimed-signal
  (let [receipt (lifecycle/construction-receipt)
        releases (atom {:spans 0 :metrics 0 :logs 0})
        exporter (controlled-exporter releases)
        error (try (sdk/init! {:exporter exporter :metrics? true :logs? true
                              :max-queue-size 0 :construction-receipt receipt})
                   nil (catch Throwable error error))]
    (is (some? error))
    (is (not= :construction-cleanup-incomplete (:otel.sdk/error (ex-data error))))
    (is (= :confirmed (:quiescence (lifecycle/construction-failure-status receipt error))))
    (is (= {:spans 1 :metrics 0 :logs 0} @releases))
    (is (nil? (sdk/tracer-provider)))))

(deftest resource-claims-bind-exporter-identity-and-signal
  (let [receipt (lifecycle/construction-receipt)
        releases (atom {:spans 0 :metrics 0 :logs 0})
        exporter (controlled-exporter releases)
        owner (export/simple-processor exporter receipt)]
    (try
      (is (= :child-owned (:ownership
                           (lifecycle/construction-resource-status receipt nil exporter :spans))))
      (doseq [signal [:metrics :logs]]
        (is (= :unknown (:ownership
                         (lifecycle/construction-resource-status receipt nil exporter signal)))))
      (is (= :unknown (:ownership
                       (lifecycle/construction-resource-status receipt nil
                         (controlled-exporter (atom {:spans 0 :metrics 0 :logs 0})) :spans))))
      (is (= :unknown (:ownership
                       (lifecycle/construction-resource-status receipt (ex-info "outside" {})
                                                              exporter :spans))))
      (finally (export/shutdown! owner)))
    (is (= {:spans 1 :metrics 0 :logs 0} @releases))))

(deftest foreign-constructor-errors-do-not-authorize-orphan-release
  (doseq [after-return? [false true]]
    (let [receipt (lifecycle/construction-receipt) owner (atom nil)
          releases (atom {:spans 0 :metrics 0 :logs 0})
          exporter (controlled-exporter releases)
          failure (ex-info "controlled foreign wrapper" {})
          batch export/batch-processor]
      (try
        (let [error
              (with-redefs [export/batch-processor
                            (fn [& args]
                              (when after-return? (reset! owner (apply batch args)))
                              (throw failure))]
                (try (sdk/init! {:exporter exporter :metrics? false
                                 :construction-receipt receipt})
                     nil (catch Throwable error error)))
              retry-stop! (:retry-stop! (ex-data error))]
          (is (= #{:otel.sdk/error :retry-stop!} (set (keys (ex-data error)))))
          (is (= :construction-cleanup-incomplete (:otel.sdk/error (ex-data error))))
          (is (nil? (.getCause error)))
          (is (= :unconfirmed (:quiescence (lifecycle/construction-failure-status receipt error))))
          (is (= :closing (:status (retry-stop!))))
          (is (= (if after-return? 1 0) (:spans @releases)))
          (when @owner (is (retired? [@owner])))
          (is (nil? (sdk/tracer-provider))))
        (finally
          (when @owner
            (export/shutdown! @owner) (.join (:worker @owner) 2000)
            (is (not (.isAlive (:worker @owner))))))))))

(deftest actual-init-later-signal-start-failure-retires-all-acquired-workers
  (doseq [signal [:metrics :logs]]
    (let [receipt (lifecycle/construction-receipt) owners (atom [])
          seam-observation (atom nil) handle (atom nil)
          releases (atom {:spans 0 :metrics 0 :logs 0})
          exporter (controlled-exporter releases)
          failure (ex-info "controlled later signal start failure" {})
          start lifecycle/start-owned-worker!]
      (try
        (let [error
              (with-redefs [lifecycle/start-owned-worker!
                            (fn [child acquired worker]
                              (let [result (start child acquired worker)]
                                (swap! owners conj acquired)
                                (when (if (= :metrics signal)
                                        (instance? otel.sdk.metrics.PeriodicReader acquired)
                                        (instance? otel.sdk.logs.BatchLogProcessor acquired))
                                  ;; Original start really ran; earlier owners
                                  ;; are observed live before the builtin catch.
                                  (reset! seam-observation
                                    {:count (count @owners)
                                     :all-live? (every? #(.isAlive (:worker %)) @owners)
                                     :all-original-exporters?
                                     (every? #(identical? exporter (:exporter %)) @owners)})
                                  (throw failure))
                                result))]
                (try (reset! handle
                       (sdk/init! {:exporter exporter :metrics? true :logs? true
                                   :runtime-metrics? false :bridge-logging? false
                                   :schedule-delay-ms 60000 :metric-interval-ms 60000
                                   :construction-receipt receipt}))
                     nil (catch Throwable error error)))
              expected-count (if (= :metrics signal) 2 3)
              expected-releases {:spans 1 :metrics 1 :logs (if (= :logs signal) 1 0)}]
          (is (= {:count expected-count :all-live? true :all-original-exporters? true}
                 @seam-observation))
          (is (identical? failure error))
          (is (nil? @handle))
          (is (= expected-count (count @owners)))
          (is (every? #(identical? exporter (:exporter %)) @owners))
          (is (retired? @owners))
          (is (= expected-releases @releases))
          (is (= :confirmed (:quiescence (lifecycle/construction-failure-status receipt error))))
          (is (= :unknown (:outcome (lifecycle/construction-failure-status receipt
                                                       (ex-info "outside replacement" {})))))
          (is (nil? (sdk/tracer-provider)))
          (is (nil? (sdk/meter-provider)))
          (is (nil? (sdk/logger-provider)))
          (lifecycle/retire-construction! receipt)
          (is (= expected-releases @releases)))
        (finally
          (when @handle (sdk/shutdown! @handle))
          (doseq [acquired @owners]
            (try (export/shutdown! acquired) (catch Throwable _ nil))
            (.join (:worker acquired) 2000)
            (is (not (.isAlive (:worker acquired)))))
          (lifecycle/retire-construction! receipt)
          (is (= {:spans 1 :metrics 1 :logs (if (= :logs signal) 1 0)} @releases)))))))
