(ns otel.sdk.init-publication-rollback-test
  (:require [clojure.test :refer [deftest is]]
            [clojure.tools.logging :as logging]
            [otel.bridge.tools-logging :as bridge]
            [otel.sdk :as sdk]
            [otel.sdk.export :as export]
            [otel.sdk.lifecycle :as lifecycle]
            [otel.sdk.logs :as logs]))

(deftest log-owner-precedes-bypassed-start-helper
  (let [receipt (lifecycle/construction-receipt) owner (atom nil)
        observed-alive (atom false) closes (atom 0)
        failure (ex-info "controlled log start seam failure" {})
        exporter (reify logs/LogRecordExporter
                   (export-logs! [_ _] true)
                   (shutdown-log-exporter! [_] (swap! closes inc) true))]
    (try
      (let [error
            (with-redefs [lifecycle/start-owned-worker!
                          (fn [_ acquired worker]
                            (reset! owner acquired)
                            (.setDaemon worker true)
                            (.start worker)
                            (reset! observed-alive (.isAlive worker))
                            (throw failure))]
              (try (logs/batch-processor exporter {:schedule-delay-ms 60000} receipt)
                   nil (catch Throwable error error)))]
        (is @observed-alive)
        (is (identical? failure error))
        (is (not (.isAlive (:worker @owner))))
        (is (= :confirmed (:quiescence (lifecycle/component-settlement @owner))))
        (is (= :confirmed (:quiescence (lifecycle/construction-failure-status receipt error))))
        (is (= 1 @closes)))
      (finally
        (when @owner
          (export/shutdown! @owner)
          (.join (:worker @owner) 2000)
          (is (not (.isAlive (:worker @owner)))))
        (is (= 1 @closes))))))

(deftest owned-bridge-retirement-preserves-later-replacement
  (let [previous logging/*logger-factory*
        provider (logs/logger-provider {})
        token (bridge/installation provider)]
    (try
      (is (identical? previous (bridge/install-owned! token)))
      (is (not (identical? previous logging/*logger-factory*)))
      (is (= :unconfirmed (:quiescence (lifecycle/component-settlement token))))
      (is (true? (bridge/retire-installation! token)))
      (is (identical? previous logging/*logger-factory*))
      (is (= :confirmed (:quiescence (lifecycle/component-settlement token))))
      (is (true? (bridge/retire-installation! token)))
      (let [second-token (bridge/installation provider)
            replacement (bridge/factory previous provider)]
        (bridge/install-owned! second-token)
        (alter-var-root #'logging/*logger-factory* (constantly replacement))
        (is (true? (bridge/retire-installation! second-token)))
        (is (identical? replacement logging/*logger-factory*))
        (is (= :confirmed (:quiescence (lifecycle/component-settlement second-token)))))
      (finally
        (alter-var-root #'logging/*logger-factory* (constantly previous))
        (logs/shutdown! provider)))))

(deftest sdk-shutdown-preserves-a-later-global-installation
  (is (nil? (sdk/tracer-provider)))
  (let [first-handle (sdk/init! {:exporter :none :metrics? false})
        second-handle (atom nil)]
    (try
      (reset! second-handle (sdk/init! {:exporter :none :metrics? false}))
      (is (identical? (:tracer-provider @second-handle) (sdk/tracer-provider)))
      (sdk/shutdown! first-handle)
      (is (identical? (:tracer-provider @second-handle) (sdk/tracer-provider)))
      (sdk/shutdown! @second-handle)
      (is (nil? (sdk/tracer-provider)))
      (finally
        (when @second-handle (sdk/shutdown! @second-handle))
        (sdk/shutdown! first-handle)))))
